package fsd

import (
	"bufio"
	"context"
	"errors"
	"github.com/JianyueLab-Org/can-fsdopen/auth"
	"github.com/JianyueLab-Org/can-fsdopen/config"
	"net"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestPreAuthAdmissionEnforcesPerIPAndGlobalCaps(t *testing.T) {
	cfg := config.Defaults()
	cfg.MaxPreAuthConnections, cfg.MaxPreAuthPerIP = 2, 1
	admission := newPreAuthAdmission()

	first := admission.acquire("198.51.100.1", &cfg)
	if first == nil {
		t.Fatal("first connection was refused")
	}
	if got := admission.acquire("198.51.100.1", &cfg); got != nil {
		got.release()
		t.Fatal("second connection from one IP bypassed the per-IP cap")
	}
	second := admission.acquire("198.51.100.2", &cfg)
	if second == nil {
		t.Fatal("different IP was refused before the global cap")
	}
	if got := admission.acquire("198.51.100.3", &cfg); got != nil {
		got.release()
		t.Fatal("connection bypassed the global cap")
	}

	first.release()
	if got := admission.acquire("198.51.100.2", &cfg); got != nil {
		got.release()
		t.Fatal("released IP slot did not remain protected by the existing per-IP lease")
	}
	second.release()
	if got := admission.acquire("198.51.100.2", &cfg); got == nil {
		t.Fatal("released global and per-IP slots were not reusable")
	} else {
		got.release()
	}
}

func TestPreAuthAdmissionReleaseIsIdempotent(t *testing.T) {
	cfg := config.Defaults()
	cfg.MaxPreAuthConnections, cfg.MaxPreAuthPerIP = 1, 1
	admission := newPreAuthAdmission()
	lease := admission.acquire("203.0.113.10", &cfg)
	if lease == nil {
		t.Fatal("connection was refused")
	}
	lease.release()
	lease.release()

	if got := admission.acquire("203.0.113.10", &cfg); got == nil {
		t.Fatal("idempotent release did not return the slot")
	} else {
		got.release()
	}
}

func TestAuthenticateBoundsConcurrentQueriesAndUsesConfiguredTimeout(t *testing.T) {
	auth := &blockingAuth{}
	cfg := config.Defaults()
	cfg.FSDMode = 1
	cfg.IdentDelayMS = 0
	cfg.AuthConcurrency = 1
	server, err := NewServer(Options{
		Config: config.NewStore("config.json", &cfg),
		Users:  auth,
	})
	if err != nil {
		t.Fatalf("NewServer: %v", err)
	}

	start := make(chan struct{})
	results := make(chan error, 2)
	for range 2 {
		go func() {
			serverConn, peer := net.Pipe()
			defer serverConn.Close()
			defer peer.Close()
			go func() {
				_, _ = peer.Read(make([]byte, 512))
			}()
			client := newClient(context.Background(), serverConn, NewScanner(bufio.NewReader(serverConn)), LoginData{
				Callsign: "CCA101",
				CID:      "1000",
			})
			defer client.cancel()
			<-start
			ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
			defer cancel()
			results <- server.authenticate(ctx, client, "secret")
		}()
	}
	close(start)

	deadline := time.After(time.Second)
	for range 2 {
		select {
		case err := <-results:
			if !errors.Is(err, context.DeadlineExceeded) && !errors.Is(err, errAuthCapacity) {
				t.Errorf("authenticate error = %v, want deadline or capacity rejection", err)
			}
		case <-deadline:
			t.Fatal("authentication did not respect the configured timeout")
		}
	}

	if active := auth.maxActive.Load(); active > 1 {
		t.Fatalf("%d authentication queries ran concurrently, want at most 1", active)
	}
}

type blockingAuth struct {
	active    atomic.Int32
	maxActive atomic.Int32
	mu        sync.Mutex
}

func (a *blockingAuth) Authenticate(ctx context.Context, _ string, _ string, _ int, _ string) (*auth.Identity, error) {
	active := a.active.Add(1)
	for {
		previous := a.maxActive.Load()
		if active <= previous || a.maxActive.CompareAndSwap(previous, active) {
			break
		}
	}
	defer a.active.Add(-1)
	<-ctx.Done()
	return nil, ctx.Err()
}
