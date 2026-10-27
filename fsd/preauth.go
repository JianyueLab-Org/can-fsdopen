package fsd

import (
	"context"
	"errors"
	"github.com/JianyueLab-Org/can-fsdopen/config"
	"log/slog"
	"net/netip"
	"sync"
	"time"
)

var (
	errAuthCapacity = errors.New("fsd: authentication capacity exhausted")
	errAuthAttempts = errors.New("fsd: authentication attempt rate exceeded")
)

// preAuthAdmission is shared by all FSD listeners. Active counts survive
// configuration reloads; each new admission reads the current limits.
type preAuthAdmission struct {
	mu        sync.Mutex
	active    int
	byIP      map[string]int
	queries   int
	attempts  map[string]authAttemptBucket
	rejected  admissionRejections
	now       func() time.Time
	nextSweep time.Time
}

type authAttemptBucket struct {
	tokens    float64
	updatedAt time.Time
	expiresAt time.Time
}

type admissionRejections struct {
	global, perIP, queries, rate, state, timeout uint64
}

type preAuthLease struct {
	admission *preAuthAdmission
	ip        string
	once      sync.Once
}

func newPreAuthAdmission() *preAuthAdmission {
	return &preAuthAdmission{
		byIP:     make(map[string]int),
		attempts: make(map[string]authAttemptBucket),
		now:      time.Now,
	}
}

func normalizedPeerIP(ip string) string {
	if parsed, err := netip.ParseAddr(ip); err == nil {
		return parsed.Unmap().WithZone("").String()
	}
	return "unknown"
}

// acquire runs before TLS or allocation of a connection goroutine.
func (a *preAuthAdmission) acquire(ip string, cfg *config.Config) *preAuthLease {
	ip = normalizedPeerIP(ip)
	a.mu.Lock()
	defer a.mu.Unlock()

	if a.active >= cfg.MaxPreAuthConnections {
		a.rejected.global++
		return nil
	}
	if a.byIP[ip] >= cfg.MaxPreAuthPerIP {
		a.rejected.perIP++
		return nil
	}
	a.active++
	a.byIP[ip]++
	return &preAuthLease{admission: a, ip: ip}
}

func (l *preAuthLease) release() {
	if l == nil {
		return
	}
	l.once.Do(func() {
		a := l.admission
		a.mu.Lock()
		defer a.mu.Unlock()

		a.active--
		if count := a.byIP[l.ip]; count == 1 {
			delete(a.byIP, l.ip)
		} else {
			a.byIP[l.ip] = count - 1
		}
	})
}

func (a *preAuthAdmission) beginAuth(ip string, cfg *config.Config) (func(), error) {
	ip = normalizedPeerIP(ip)
	a.mu.Lock()
	defer a.mu.Unlock()
	if a.queries >= cfg.AuthConcurrency {
		a.rejected.queries++
		return nil, errAuthCapacity
	}
	now := a.now()
	if !now.Before(a.nextSweep) {
		a.sweepLocked(now)
		a.nextSweep = now.Add(time.Minute)
	}
	b, found := a.attempts[ip]
	if !found || !now.Before(b.expiresAt) {
		if !found && len(a.attempts) >= cfg.AuthIPStateLimit {
			a.rejected.state++
			return nil, errAuthCapacity
		}
		b = authAttemptBucket{tokens: float64(cfg.AuthAttemptBurst), updatedAt: now}
	}
	refilled := b.tokens + now.Sub(b.updatedAt).Seconds()*float64(cfg.AuthAttemptsPerMinute)/60
	b.tokens = min(float64(cfg.AuthAttemptBurst), refilled)
	b.updatedAt = now
	// Retain exhausted buckets at least until their full burst refills.
	ttl := max(time.Minute, time.Duration(float64(cfg.AuthAttemptBurst)*float64(time.Minute)/float64(cfg.AuthAttemptsPerMinute)))
	b.expiresAt = now.Add(ttl)
	if b.tokens < 1 {
		a.attempts[ip] = b
		a.rejected.rate++
		return nil, errAuthAttempts
	}
	b.tokens--
	a.attempts[ip] = b
	a.queries++
	return sync.OnceFunc(func() {
		a.mu.Lock()
		a.queries--
		a.mu.Unlock()
	}), nil
}

func (a *preAuthAdmission) sweepLocked(now time.Time) {
	for ip, bucket := range a.attempts {
		if !now.Before(bucket.expiresAt) {
			delete(a.attempts, ip)
		}
	}
}

func (a *preAuthAdmission) timedOut() {
	a.mu.Lock()
	a.rejected.timeout++
	a.mu.Unlock()
}

// report emits at most one aggregate event per minute, with no peer or
// credential data. Expired attempt buckets are removed without new traffic.
func (a *preAuthAdmission) report(ctx context.Context) {
	ticker := time.NewTicker(time.Minute)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			a.mu.Lock()
			a.sweepLocked(a.now())
			r, pending, queries := a.rejected, a.active, a.queries
			a.rejected = admissionRejections{}
			a.mu.Unlock()
			if r != (admissionRejections{}) {
				slog.Warn("pre-auth admission limits",
					"global_rejections", r.global, "per_ip_rejections", r.perIP,
					"auth_capacity_rejections", r.queries, "attempt_rate_rejections", r.rate,
					"ip_state_rejections", r.state, "login_timeouts", r.timeout,
					"pending", pending, "auth_queries", queries)
			}
		}
	}
}
