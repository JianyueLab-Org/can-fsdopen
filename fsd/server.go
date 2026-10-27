package fsd

import (
	"context"
	"crypto/sha256"
	"crypto/tls"
	"errors"
	"fmt"
	"github.com/JianyueLab-Org/can-fsdopen/auth"
	"github.com/JianyueLab-Org/can-fsdopen/config"
	"log/slog"
	"net"
	"sync"
	"syscall"
	"time"
)

// Authenticator verifies a transient FSD login and its requested rating.
type Authenticator interface {
	Authenticate(ctx context.Context, cid, password string, requestedRating int, sourceIP string) (*auth.Identity, error)
}

// Server is the FSD daemon.
type Server struct {
	config *config.Store
	users  Authenticator

	postOffice *postOffice
	metar      *metarService
	tracks     *trackRegistry

	// identPacket is the $DI greeting, built once at startup.
	identPacket string

	// kicked records CIDs a supervisor removed, blocking immediate
	// reconnection. Mirrors the Python server's kill_user cooldown.
	kickedMu sync.Mutex
	kicked   map[string]time.Time

	// offlinePlans keeps flight plans alive briefly after a pilot drops so a
	// reconnect does not require refiling.
	plansMu      sync.Mutex
	offlinePlans map[string]offlinePlan

	wallopBudget       *tokenBucket
	memberWallopBudget *tokenBucket
	memberWallopInit   sync.Once

	// conns counts the per-connection goroutines, so shutdown can wait for
	// them. See WaitConnections.
	conns sync.WaitGroup

	// preAuth limits work that can be forced by unauthenticated sockets. The
	// lease is released after registration, or by deferred cleanup on failure.
	preAuth *preAuthAdmission
}

// NewServer builds a server. Only Config and Users are required.
func NewServer(opts Options) (*Server, error) {
	if opts.Config == nil {
		return nil, errors.New("fsd: config is required")
	}
	if opts.Users == nil {
		return nil, errors.New("fsd: authenticator is required")
	}

	cfg := opts.Config.Get()

	s := &Server{
		config:       opts.Config,
		users:        opts.Users,
		postOffice:   newPostOffice(cfg.MaxClients),
		tracks:       newTrackRegistry(),
		identPacket:  buildIdentPacket(cfg),
		kicked:       make(map[string]time.Time),
		offlinePlans: make(map[string]offlinePlan),
		wallopBudget: newTokenBucket(6, time.Minute),
		preAuth:      newPreAuthAdmission(),
	}

	s.metar = newMetarService(opts.Config, 4)

	return s, nil
}

// buildIdentPacket renders the public protocol greeting and server challenge seed.
func buildIdentPacket(cfg *config.Config) string {
	return fmt.Sprintf("$DISERVER:CLIENT:%s:%s\r\n",
		cfg.ServerIdentString(), cfg.ServerChallenge())
}

// StartBackgroundJobs starts weather workers and protocol maintenance.
// Run calls it; callers using Serve directly must arrange it when needed.
func (s *Server) StartBackgroundJobs(ctx context.Context) {
	go s.preAuth.report(ctx)
	go s.metar.run(ctx)
	go s.runReaper(ctx)

}

// Run starts every listener and blocks until cancellation or listener failure.
func (s *Server) Run(ctx context.Context) error {
	return s.run(ctx, (&net.ListenConfig{}).Listen)
}

func (s *Server) run(ctx context.Context, listen func(context.Context, string, string) (net.Listener, error)) error {
	runCtx, cancel := context.WithCancel(ctx)
	defer cancel()

	cfg := s.config.Get()

	s.StartBackgroundJobs(runCtx)

	// The plaintext listeners, unless policy says otherwise. Turning them off
	// is how a deployment closes the downgrade: a client told there is a TLS
	// port must not accept plaintext, and with nothing bound there is nothing
	// to downgrade to. See fsd/tls.go.
	var addrs []string
	if cfg.ATCPlaintextPolicy != PlaintextOff {
		addrs = cfg.AllListenAddrs()
	} else {
		slog.Warn("plaintext FSD disabled by policy; TLS only")
	}

	var tlsConfig *tls.Config
	tlsAddrs := cfg.AllTLSListenAddrs()
	if len(tlsAddrs) > 0 {
		var err error
		tlsConfig, err = TLSConfig(cfg.TLSCertFile, cfg.TLSKeyFile)
		if err != nil {
			return err
		}
	}

	var wg sync.WaitGroup
	errCh := make(chan error, len(addrs)+len(tlsAddrs))

	bind := func(addr string, secure bool) error {
		listener, err := listen(runCtx, "tcp", addr)
		if err != nil {
			// Fail fast rather than coming up half-listening: a server bound
			// to only one of its two ports looks healthy but silently refuses
			// half its clients.
			return fmt.Errorf("fsd: listening on %s: %w", addr, err)
		}
		if secure {
			listener = WrapTLS(listener, tlsConfig)
		}

		slog.Info("FSD listening", "addr", addr, "tls", secure)

		wg.Add(1)
		go func(l net.Listener) {
			defer wg.Done()
			s.Serve(runCtx, l, errCh)
		}(listener)
		return nil
	}

	for _, addr := range addrs {
		if err := bind(addr, false); err != nil {
			return err
		}
	}
	for _, addr := range tlsAddrs {
		if err := bind(addr, true); err != nil {
			return err
		}
	}

	var serveErr error
	select {
	case <-ctx.Done():
	case serveErr = <-errCh:
		cancel()
	}
	wg.Wait()

	// The accept loops are stopped; now drain the connections they started.
	// Returning before they finish hands the caller a server whose goroutines
	// are still writing into the stores it is about to close.
	if !s.WaitConnections(connectionDrainGrace) {
		slog.Warn("connection goroutines did not finish before shutdown",
			"grace", connectionDrainGrace)
	}

	return serveErr
}

// connectionDrainGrace bounds the wait for connection goroutines at shutdown.
//
// They exit as soon as their contexts are cancelled — eventLoop trips the read
// deadline to unblock the socket — so this is a backstop against a goroutine
// wedged somewhere unexpected, not the normal path. Bounded rather than
// unbounded because a shutdown that never completes is worse than one that
// completes with a warning.
const connectionDrainGrace = 10 * time.Second

// Serve accepts connections on listener until ctx is cancelled.
//
// Exported so a caller can supply its own listener — Run builds them from
// configuration, tests bind an ephemeral port. Fatal accept errors are sent to
// errCh, which may be nil.
func (s *Server) Serve(ctx context.Context, listener net.Listener, errCh chan<- error) {
	serveDone := make(chan struct{})
	defer close(serveDone)
	defer listener.Close()

	go func() {
		select {
		case <-ctx.Done():
			listener.Close()
		case <-serveDone:
		}
	}()

	var tempDelay time.Duration
	for {
		conn, err := listener.Accept()
		if err != nil {
			if ctx.Err() != nil {
				return
			}

			errorClass, retry := classifyAcceptError(err)
			if retry {
				firstFailure := tempDelay == 0
				// Resource exhaustion and timeout errors are transient. Back off
				// so a busy process does not spin, and reset after a successful
				// accept so a later failure recovers quickly.
				if tempDelay == 0 {
					tempDelay = 5 * time.Millisecond
				} else {
					tempDelay *= 2
				}
				if max := time.Second; tempDelay > max {
					tempDelay = max
				}
				level := slog.LevelDebug
				if firstFailure {
					level = slog.LevelWarn
				}
				slog.Log(ctx, level, "FSD accept failed; retrying",
					"error_class", errorClass, "backoff", tempDelay, "err", err)
				timer := time.NewTimer(tempDelay)
				select {
				case <-ctx.Done():
					if !timer.Stop() {
						<-timer.C
					}
					return
				case <-timer.C:
				}
				continue
			}

			slog.Error("FSD listener failed", "error_class", errorClass, "err", err)
			if errCh != nil {
				select {
				case errCh <- fmt.Errorf("fsd: accept: %w", err):
				case <-ctx.Done():
				}
			}
			return
		}
		if tempDelay > 0 {
			slog.Info("FSD accept recovered")
		}
		tempDelay = 0

		acceptedAt := time.Now()
		cfg := s.config.Get()
		lease := s.preAuth.acquire(hostOf(conn.RemoteAddr().String()), cfg)
		if lease == nil {
			conn.Close()
			continue
		}

		loginCtx, cancelLogin := context.WithDeadline(ctx,
			acceptedAt.Add(time.Duration(cfg.LoginTimeoutSeconds)*time.Second))
		s.conns.Add(1)
		go func() {
			defer s.conns.Done()
			defer cancelLogin()
			s.handleConn(ctx, loginCtx, cancelLogin, conn, lease)
		}()
	}
}

func classifyAcceptError(err error) (class string, retry bool) {
	for _, resourceErr := range []error{syscall.EMFILE, syscall.ENFILE, syscall.ENOBUFS, syscall.ENOMEM} {
		if errors.Is(err, resourceErr) {
			return "resource", true
		}
	}
	if errors.Is(err, net.ErrClosed) {
		return "closed", false
	}
	var netErr net.Error
	if errors.As(err, &netErr) {
		if netErr.Timeout() {
			return "timeout", true
		}
		if netErr.Temporary() {
			return "temporary", true
		}
	}
	return "terminal", false
}

func (s *Server) WaitConnections(grace time.Duration) bool {
	done := make(chan struct{})
	go func() {
		s.conns.Wait()
		close(done)
	}()

	select {
	case <-done:
		return true
	case <-time.After(grace):
		return false
	}
}

func (s *Server) Config() *config.Store { return s.config }

// OnlineCount returns the number of registered clients.
func (s *Server) OnlineCount() int { return s.postOffice.count() }

// Kick disconnects a callsign, as a supervisor action.
func (s *Server) Kick(callsign, reason string) error {
	client, err := s.postOffice.find(callsign)
	if err != nil {
		return err
	}

	s.markKicked(client.CID)
	client.SendServerMessage("You have been disconnected: " + reason)
	client.Disconnect("kicked: " + reason)

	return nil
}

// kickCooldown is how long a kicked CID is refused. Matches the 300 seconds
// the Python server used.
const kickCooldown = 5 * time.Minute

func (s *Server) markKicked(cid string) {
	s.kickedMu.Lock()
	s.kicked[cid] = time.Now()
	s.kickedMu.Unlock()
}

func (s *Server) recentlyKicked(cid string) bool {
	s.kickedMu.Lock()
	defer s.kickedMu.Unlock()

	at, ok := s.kicked[cid]
	if !ok {
		return false
	}
	if time.Since(at) > kickCooldown {
		delete(s.kicked, cid)
		return false
	}
	return true
}

func (s *Server) restoreFlightPlan(client *Client) {
	if client.IsATC {
		return
	}

	fp := s.takeOfflinePlan(client.Callsign, client.CID)

	if fp == nil {
		return
	}

	client.setFlightPlan(fp)
	s.broadcastAllATC(client, fp.BuildFilePacket(client.Callsign, "*A"))
}

// onDisconnect runs under postOffice.mu before the callsign is released.
func (s *Server) onDisconnect(client *Client) {
	if !client.IsATC {
		if fp := client.FlightPlanData(); fp != nil {
			s.holdOfflinePlan(client.Callsign, client.CID, fp)
		}
	}

	s.releaseTracked(client.Callsign)
}

// flightPlanGrace is how long a filed plan outlives its filer. 300 seconds,
// matching Status.Delete_flight_plan in the Python server.
const flightPlanGrace = 5 * time.Minute

// tokenBucket is a simple refilling rate limiter.
type tokenBucket struct {
	mu       sync.Mutex
	tokens   int
	capacity int
	interval time.Duration
	lastFill time.Time
}

func newTokenBucket(capacity int, interval time.Duration) *tokenBucket {
	return &tokenBucket{
		tokens:   capacity,
		capacity: capacity,
		interval: interval,
		lastFill: time.Now(),
	}
}

// take consumes one token, reporting whether one was available.
func (b *tokenBucket) take() bool {
	return b.takeN(1)
}

func (b *tokenBucket) takeN(count int) bool {
	b.mu.Lock()
	defer b.mu.Unlock()

	if elapsed := time.Since(b.lastFill); elapsed >= b.interval {
		b.tokens = b.capacity
		b.lastFill = time.Now()
	}

	if count <= 0 || b.tokens < count {
		return false
	}
	b.tokens -= count
	return true
}

// Options supplies the live configuration and standalone identity verifier.
type Options struct {
	Config *config.Store
	Users  Authenticator
}

type offlinePlan struct {
	*FlightPlan
	identity [32]byte
}

func (s *Server) holdOfflinePlan(callsign, identity string, fp *FlightPlan) {
	held := *fp
	held.OfflineAt = time.Now().UnixNano()
	s.plansMu.Lock()
	defer s.plansMu.Unlock()
	if _, exists := s.offlinePlans[callsign]; !exists && len(s.offlinePlans) >= s.config.Get().MaxClients {
		oldest := ""
		var at int64
		for name, plan := range s.offlinePlans {
			if oldest == "" || plan.OfflineAt < at {
				oldest, at = name, plan.OfflineAt
			}
		}
		delete(s.offlinePlans, oldest)
	}
	s.offlinePlans[callsign] = offlinePlan{FlightPlan: &held, identity: sha256.Sum256([]byte(identity))}
}

func (s *Server) takeOfflinePlan(callsign, identity string) *FlightPlan {
	s.plansMu.Lock()
	defer s.plansMu.Unlock()
	held, ok := s.offlinePlans[callsign]
	if !ok {
		return nil
	}
	delete(s.offlinePlans, callsign)
	if held.identity != sha256.Sum256([]byte(identity)) || time.Since(time.Unix(0, held.OfflineAt)) > flightPlanGrace {
		return nil
	}
	held.OfflineAt = 0
	return held.FlightPlan
}

// lookupFlightPlan queries a live aircraft or an unclaimed reconnect plan.
func (s *Server) lookupFlightPlan(callsign string) *FlightPlan {
	if client, err := s.postOffice.find(callsign); err == nil {
		return client.FlightPlanData()
	}
	s.plansMu.Lock()
	defer s.plansMu.Unlock()
	held, ok := s.offlinePlans[callsign]
	if !ok || time.Since(time.Unix(0, held.OfflineAt)) > flightPlanGrace {
		return nil
	}
	return held.FlightPlan
}
