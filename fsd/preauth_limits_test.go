package fsd

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"github.com/JianyueLab-Org/can-fsdopen/auth"
	"github.com/JianyueLab-Org/can-fsdopen/config"
	"net"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestPreAuthConcurrentMappedIPAndLiveLimits(t *testing.T) {
	cfg := config.Defaults()
	cfg.MaxPreAuthConnections, cfg.MaxPreAuthPerIP = 4, 2
	a := newPreAuthAdmission()
	leases := make(chan *preAuthLease, 100)
	var wg sync.WaitGroup
	for i := range 100 {
		wg.Go(func() {
			ip := "192.0.2.1"
			if i%2 == 0 {
				ip = "::ffff:192.0.2.1"
			}
			if lease := a.acquire(ip, &cfg); lease != nil {
				leases <- lease
			}
		})
	}
	wg.Wait()
	close(leases)
	if len(leases) != 2 {
		t.Fatalf("admitted %d mapped/native peers, want 2", len(leases))
	}
	cfg.MaxPreAuthConnections, cfg.MaxPreAuthPerIP = 1, 1
	if lease := a.acquire("192.0.2.2", &cfg); lease != nil {
		t.Fatal("lowered global limit reset active usage")
	}
	for lease := range leases {
		for range 10 {
			wg.Go(lease.release)
		}
	}
	wg.Wait()
	if a.active != 0 || len(a.byIP) != 0 {
		t.Fatal("released leases retained active peer state")
	}
	lease := a.acquire("192.0.2.1", &cfg)
	if lease == nil {
		t.Fatal("released capacity did not respect the reloaded limit")
	}
	lease.release()
}

func TestAuthBudgetKeepsInFlightUsageAcrossLimitChanges(t *testing.T) {
	cfg := config.Defaults()
	cfg.AuthConcurrency = 2
	a := newPreAuthAdmission()
	first, err := a.beginAuth("192.0.2.1", &cfg)
	if err != nil {
		t.Fatal(err)
	}
	second, err := a.beginAuth("192.0.2.2", &cfg)
	if err != nil {
		t.Fatal(err)
	}
	cfg.AuthConcurrency = 1
	if _, err := a.beginAuth("192.0.2.3", &cfg); !errors.Is(err, errAuthCapacity) {
		t.Fatalf("lowered limit accepted work: %v", err)
	}
	first()
	first()
	if _, err := a.beginAuth("192.0.2.3", &cfg); !errors.Is(err, errAuthCapacity) {
		t.Fatalf("existing workflow was not counted: %v", err)
	}
	second()
	third, err := a.beginAuth("192.0.2.3", &cfg)
	if err != nil {
		t.Fatalf("released query slot: %v", err)
	}
	third()
	if a.queries != 0 {
		t.Fatal("query reservation leaked")
	}
}

func TestAuthAttemptsRefillExpireAndBoundIPState(t *testing.T) {
	cfg := config.Defaults()
	cfg.AuthAttemptBurst, cfg.AuthAttemptsPerMinute, cfg.AuthIPStateLimit = 2, 60, 1
	a := newPreAuthAdmission()
	now := time.Date(2026, 10, 11, 0, 0, 0, 0, time.UTC)
	a.now = func() time.Time { return now }
	for range 2 {
		release, err := a.beginAuth("192.0.2.1", &cfg)
		if err != nil {
			t.Fatal(err)
		}
		release()
	}
	if _, err := a.beginAuth("::ffff:192.0.2.1", &cfg); !errors.Is(err, errAuthAttempts) {
		t.Fatalf("mapped peer escaped exhausted burst: %v", err)
	}
	if _, err := a.beginAuth("192.0.2.2", &cfg); !errors.Is(err, errAuthCapacity) {
		t.Fatalf("new peer exceeded IP-state cap: %v", err)
	}
	now = now.Add(time.Second)
	release, err := a.beginAuth("192.0.2.1", &cfg)
	if err != nil {
		t.Fatalf("one token should refill: %v", err)
	}
	release()
	// Reloading does not grant a fresh burst.
	cfg.AuthAttemptBurst = 3
	if _, err := a.beginAuth("192.0.2.1", &cfg); !errors.Is(err, errAuthAttempts) {
		t.Fatalf("reload reset attempt history: %v", err)
	}
	now = now.Add(time.Minute)
	release, err = a.beginAuth("192.0.2.2", &cfg)
	if err != nil {
		t.Fatalf("expired state was not reclaimed: %v", err)
	}
	release()
	if len(a.attempts) != 1 {
		t.Fatalf("retained %d IP buckets", len(a.attempts))
	}
	now = now.Add(time.Minute)
	a.sweepLocked(now)
	if len(a.attempts) != 0 {
		t.Fatal("expired peer address retained")
	}
}

func TestAuthAttemptStateCannotExpireBeforeSlowBurstRefill(t *testing.T) {
	cfg := config.Defaults()
	cfg.AuthAttemptBurst, cfg.AuthAttemptsPerMinute, cfg.AuthIPStateLimit = 2, 1, 1
	a := newPreAuthAdmission()
	now := time.Date(2026, 10, 11, 0, 0, 0, 0, time.UTC)
	a.now = func() time.Time { return now }
	for range 2 {
		release, err := a.beginAuth("192.0.2.1", &cfg)
		if err != nil {
			t.Fatal(err)
		}
		release()
	}
	now = now.Add(time.Minute)
	if _, err := a.beginAuth("192.0.2.2", &cfg); !errors.Is(err, errAuthCapacity) {
		t.Fatalf("bucket expired before a full refill: %v", err)
	}
	now = now.Add(time.Minute)
	release, err := a.beginAuth("192.0.2.2", &cfg)
	if err != nil {
		t.Fatalf("full refill should expire the old bucket: %v", err)
	}
	release()
}

type admissionCountingAuth struct{ calls atomic.Int32 }

func (a *admissionCountingAuth) Authenticate(ctx context.Context, cid, password string, rating int, ip string) (*auth.Identity, error) {
	a.calls.Add(1)
	return (&fakeAuth{users: map[string]*testIdentity{
		"1000": {Username: "1000", Password: "secret", Rating: 5},
	}}).Authenticate(ctx, cid, password, rating, ip)
}

func admissionPipeClient(t *testing.T, callsign string) *Client {
	t.Helper()
	conn, peer := net.Pipe()
	t.Cleanup(func() { conn.Close(); peer.Close() })
	go func() { _, _ = peer.Read(make([]byte, 512)) }()
	client := newClient(context.Background(), conn, NewScanner(bufio.NewReader(conn)), LoginData{
		Callsign: callsign, CID: "1000", NetworkRating: 1,
	})
	t.Cleanup(client.cancel)
	return client
}

func TestRepeatedValidAuthenticationConsumesPeerBurst(t *testing.T) {
	cfg := config.Defaults()
	cfg.AuthAttemptBurst = 2
	auth := &admissionCountingAuth{}
	s, err := NewServer(Options{Config: config.NewStore("unused", &cfg), Users: auth})
	if err != nil {
		t.Fatal(err)
	}
	for i := range 3 {
		client := admissionPipeClient(t, fmt.Sprintf("CCA%d", i+100))
		err := s.authenticate(context.Background(), client, "secret")
		if i < 2 && err != nil {
			t.Fatal(err)
		}
		if i == 2 && !errors.Is(err, errAuthAttempts) {
			t.Fatalf("valid reconnect escaped limit: %v", err)
		}
	}
	if auth.calls.Load() != 2 {
		t.Fatal("limited attempt reached authenticator")
	}
}
