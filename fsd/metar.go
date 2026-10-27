package fsd

import (
	"context"
	"fmt"
	"github.com/JianyueLab-Org/can-fsdopen/config"
	"io"
	"log/slog"
	"net/http"
	"strings"
	"sync"
	"time"
)

type metarService struct {
	config     *config.Store
	client     *http.Client
	numWorkers int

	mu      sync.Mutex
	queue   []*metarLookup
	wake    chan struct{}
	lookups map[string]*metarLookup
	pending map[*Client]*metarLookup
	closed  bool

	cacheMu sync.RWMutex
	cache   map[string]metarEntry
	limitMu sync.Mutex
	limits  map[string]metarLimit
}

type metarLookup struct {
	icao    string
	ctx     context.Context
	cancel  context.CancelFunc
	clients map[*Client]func() bool
	queued  bool
}

type metarEntry struct {
	report    string
	ok        bool
	fetchedAt time.Time
}

type metarLimit struct {
	started time.Time
	count   int
}

// metarTTL is how long an observation is reused. METARs are issued hourly, so
// a short cache removes almost all upstream traffic without serving stale
// weather.
const metarTTL = 5 * time.Minute
const metarNegativeTTL = 30 * time.Second
const metarPerClientLimit = 8
const metarLimitWindow = time.Minute
const metarQueueLimit = 256

func newMetarService(cfg *config.Store, numWorkers int) *metarService {
	return &metarService{
		config:     cfg,
		client:     &http.Client{Timeout: 8 * time.Second},
		numWorkers: numWorkers,
		wake:       make(chan struct{}, 1),
		lookups:    make(map[string]*metarLookup),
		pending:    make(map[*Client]*metarLookup),
		cache:      make(map[string]metarEntry),
		limits:     make(map[string]metarLimit),
	}
}

func (m *metarService) run(ctx context.Context) {
	var workers sync.WaitGroup
	defer func() {
		m.shutdown()
		workers.Wait()
	}()
	for range m.numWorkers {
		workers.Add(1)
		go func() {
			defer workers.Done()
			m.worker(ctx)
		}()
	}

	// Evict stale cache entries so the map does not grow without bound.
	ticker := time.NewTicker(metarTTL)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			m.evict()
		}
	}
}

func (m *metarService) worker(ctx context.Context) {
	for {
		m.mu.Lock()
		if m.closed || ctx.Err() != nil {
			m.mu.Unlock()
			return
		}
		if len(m.queue) == 0 {
			m.mu.Unlock()
			select {
			case <-ctx.Done():
				return
			case <-m.wake:
			}
			continue
		}
		req := m.queue[0]
		m.removeQueued(req)
		if len(m.queue) > 0 {
			m.signalWork()
		}
		m.mu.Unlock()
		m.serve(ctx, req)
	}
}

// request queues a lookup. It never blocks: if the queue is full the client is
// told immediately rather than having its connection stall.
// Each connection has at most one queued or active request. Clients asking
// for the same ICAO share one lookup and one worker.
func (m *metarService) request(client *Client, icao string) {
	if client.Context().Err() != nil {
		return
	}
	if !m.allow(client.Callsign) {
		sendMetarError(client, "Weather service rate limit exceeded")
		return
	}
	icao = strings.ToUpper(strings.TrimSpace(icao))
	m.mu.Lock()
	if m.closed || m.pending[client] != nil {
		m.mu.Unlock()
		sendMetarError(client, "Weather service busy, try again")
		return
	}
	if entry, ok := m.cached(icao); ok {
		m.mu.Unlock()
		m.reply(client, icao, entry.report, entry.ok)
		return
	}
	req := m.lookups[icao]
	if req == nil {
		if len(m.queue) >= metarQueueLimit {
			m.mu.Unlock()
			sendMetarError(client, "Weather service busy, try again")
			return
		}
		ctx, cancel := context.WithCancel(context.Background())
		req = &metarLookup{icao: icao, ctx: ctx, cancel: cancel, clients: make(map[*Client]func() bool), queued: true}
		m.lookups[icao] = req
		m.queue = append(m.queue, req)
		m.signalWork()
	}
	m.pending[client] = req
	req.clients[client] = context.AfterFunc(client.Context(), func() { m.detach(req, client) })
	m.mu.Unlock()
}

// signalWork and removeQueued run with m.mu held.
func (m *metarService) signalWork() {
	select {
	case m.wake <- struct{}{}:
	default:
	}
}

func (m *metarService) removeQueued(req *metarLookup) {
	if !req.queued {
		return
	}
	for i, queued := range m.queue {
		if queued == req {
			copy(m.queue[i:], m.queue[i+1:])
			m.queue[len(m.queue)-1] = nil
			m.queue = m.queue[:len(m.queue)-1]
			break
		}
	}
	req.queued = false
}

func (m *metarService) detach(req *metarLookup, client *Client) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.pending[client] != req {
		return
	}
	req.clients[client]()
	delete(req.clients, client)
	delete(m.pending, client)
	if len(req.clients) == 0 {
		m.removeQueued(req)
		if m.lookups[req.icao] == req {
			delete(m.lookups, req.icao)
		}
		req.cancel()
	}
}

func (m *metarService) serve(ctx context.Context, req *metarLookup) {
	stop := context.AfterFunc(ctx, req.cancel)
	report, ok := m.lookup(req.ctx, req.icao)
	stop()

	m.mu.Lock()
	clients := make([]*Client, 0, len(req.clients))
	for client, stop := range req.clients {
		stop()
		delete(m.pending, client)
		clients = append(clients, client)
	}
	clear(req.clients)
	if m.lookups[req.icao] == req {
		delete(m.lookups, req.icao)
	}
	m.mu.Unlock()
	req.cancel()
	for _, client := range clients {
		m.reply(client, req.icao, report, ok)
	}
}

func (m *metarService) shutdown() {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.closed = true
	for _, req := range m.lookups {
		for _, stop := range req.clients {
			stop()
		}
		clear(req.clients)
		req.cancel()
	}
	clear(m.lookups)
	clear(m.pending)
	m.queue = nil
}

func (m *metarService) reply(client *Client, icao, report string, ok bool) {
	if client.Context().Err() != nil {
		return
	}
	if !ok {
		sendMetarError(client, "No METAR available for "+icao)
		return
	}
	client.SendBestEffort(fmt.Sprintf("$ARserver:%s:METAR:%s\r\n", client.Callsign, report))
}

func sendMetarError(client *Client, message string) {
	if client.Context().Err() == nil {
		client.SendBestEffort(errorPacket(client.Callsign, ErrCodeNoWeatherProfile, message))
	}
}

func (m *metarService) lookup(ctx context.Context, icao string) (string, bool) {
	if entry, ok := m.cached(icao); ok {
		return entry.report, entry.ok
	}

	cfg := m.config.Get()

	for _, base := range []string{cfg.Weather, cfg.WeatherFallback} {
		if ctx.Err() != nil {
			return "", false
		}
		if base == "" {
			continue
		}
		if report, ok := m.fetch(ctx, base, icao); ok {
			m.store(icao, report, true)
			return report, true
		}
	}

	if ctx.Err() == nil {
		m.store(icao, "", false)
	}
	return "", false
}

func (m *metarService) fetch(ctx context.Context, base, icao string) (string, bool) {
	reqCtx, cancel := context.WithTimeout(ctx, 8*time.Second)
	defer cancel()

	req, err := http.NewRequestWithContext(reqCtx, http.MethodGet, base+icao, nil)
	if err != nil {
		return "", false
	}
	req.Header.Set("User-Agent", "can-fsdopen (+https://github.com/JianyueLab-Org/can-fsdopen)")
	req.Header.Set("Accept", "text/plain")

	res, err := m.client.Do(req)
	if err != nil {
		slog.Debug("METAR fetch failed", "event", "upstream_request_failed")
		return "", false
	}
	defer res.Body.Close()

	if res.StatusCode != http.StatusOK {
		return "", false
	}

	// Bound the read: a provider returning an HTML error page should not be
	// copied into a packet.
	body, err := io.ReadAll(io.LimitReader(res.Body, 8*1024))
	if err != nil {
		return "", false
	}

	report := extractMetar(string(body), icao)
	if report == "" {
		return "", false
	}

	return report, true
}

// extractMetar pulls the observation line out of a provider response.
//
// VATSIM returns the bare METAR. NOAA's plain-text endpoint prefixes it with a
// timestamp line. Either way the wanted line is the one starting with the
// station identifier.
func extractMetar(body, icao string) string {
	icao = strings.ToUpper(icao)

	for line := range strings.SplitSeq(body, "\n") {
		line = strings.TrimSpace(line)
		if line == "" {
			continue
		}
		if strings.HasPrefix(strings.ToUpper(line), icao) {
			// A METAR must never contain a colon: it would be parsed as extra
			// packet fields by the receiving client.
			return strings.ReplaceAll(line, ":", " ")
		}
	}

	return ""
}

func (m *metarService) cached(icao string) (metarEntry, bool) {
	m.cacheMu.RLock()
	entry, ok := m.cache[icao]
	m.cacheMu.RUnlock()

	ttl := metarTTL
	if !entry.ok {
		ttl = metarNegativeTTL
	}
	if !ok || time.Since(entry.fetchedAt) > ttl {
		return metarEntry{}, false
	}
	return entry, true
}

func (m *metarService) store(icao, report string, ok bool) {
	m.cacheMu.Lock()
	m.cache[icao] = metarEntry{report: report, ok: ok, fetchedAt: time.Now()}
	m.cacheMu.Unlock()
}

func (m *metarService) evict() {
	m.cacheMu.Lock()

	for icao, entry := range m.cache {
		ttl := metarTTL
		if !entry.ok {
			ttl = metarNegativeTTL
		}
		if time.Since(entry.fetchedAt) > ttl {
			delete(m.cache, icao)
		}
	}
	m.cacheMu.Unlock()

	m.limitMu.Lock()
	defer m.limitMu.Unlock()
	for client, limit := range m.limits {
		if time.Since(limit.started) >= metarLimitWindow {
			delete(m.limits, client)
		}
	}
}

func (m *metarService) allow(client string) bool {
	now := time.Now()
	m.limitMu.Lock()
	defer m.limitMu.Unlock()
	l := m.limits[client]
	if l.started.IsZero() || now.Sub(l.started) >= metarLimitWindow {
		l = metarLimit{started: now}
	}
	if l.count >= metarPerClientLimit {
		m.limits[client] = l
		return false
	}
	l.count++
	m.limits[client] = l
	return true
}

// handleMetarRequest serves $AX.
//
// Layout: $AXcallsign:SERVER:METAR:ICAO
func (s *Server) handleMetarRequest(client *Client, packet []byte) {
	if FieldString(packet, 1) != recipientServer || FieldString(packet, 2) != "METAR" {
		return
	}

	icao := strings.ToUpper(strings.TrimSpace(FieldString(packet, 3)))
	if !isValidICAO(icao) {
		sendMetarError(client, "Invalid ICAO code")
		return
	}

	s.metar.request(client, icao)
}

// isValidICAO reports whether s is four letters. Rejecting anything else keeps
// arbitrary client input out of an outbound URL.
func isValidICAO(s string) bool {
	if len(s) != 4 {
		return false
	}
	for i := range len(s) {
		if s[i] < 'A' || s[i] > 'Z' {
			return false
		}
	}
	return true
}
