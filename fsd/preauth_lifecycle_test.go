package fsd

import (
	"bufio"
	"context"
	"crypto/tls"
	"github.com/JianyueLab-Org/can-fsdopen/config"
	"net"
	"path/filepath"
	"testing"
	"time"
)

type admissionTestHarness struct {
	*testHarness
	configPath string
}

func admissionHarness(t *testing.T, cfg config.Config) *admissionTestHarness {
	t.Helper()
	cfg.FSDMode, cfg.IdentDelayMS = 1, 0
	cfg.AuthAttemptBurst, cfg.AuthAttemptsPerMinute = 100, 6000
	path := filepath.Join(t.TempDir(), "config.json")
	s, err := NewServer(Options{
		Config: config.NewStore(path, &cfg),
		Users:  &fakeAuth{users: defaultAdmissionUsers()},
	})
	if err != nil {
		t.Fatal(err)
	}
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	serveDone := make(chan struct{})
	go func() { s.Serve(ctx, listener, nil); close(serveDone) }()
	t.Cleanup(func() {
		cancel()
		listener.Close()
		<-serveDone
		if !s.WaitConnections(time.Second) {
			t.Error("pending connection did not drain")
		}
	})
	return &admissionTestHarness{
		testHarness: &testHarness{server: s, addr: listener.Addr().String(), cancel: cancel},
		configPath:  path,
	}
}

func defaultAdmissionUsers() map[string]*testIdentity {
	return map[string]*testIdentity{
		"1000": {Username: "1000", Password: "secret", Rating: 5},
	}
}

func waitAdmission(t *testing.T, s *Server, count int) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		s.preAuth.mu.Lock()
		got := s.preAuth.active
		s.preAuth.mu.Unlock()
		if got == count {
			return
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatalf("pending connection count did not become %d", count)
}

func expectAdmissionClose(t *testing.T, c *testClient, within time.Duration) {
	t.Helper()
	timer := time.NewTimer(within)
	defer timer.Stop()
	for {
		select {
		case _, ok := <-c.packets:
			if !ok {
				return
			}
		case <-timer.C:
			t.Fatal("connection did not close within the login budget")
		}
	}
}

func TestPreAuthCapacityReleasedOnEveryConnectionOutcome(t *testing.T) {
	for _, outcome := range []string{"success", "credentials", "syntax", "duplicate", "full", "disconnect", "timeout", "shutdown"} {
		t.Run(outcome, func(t *testing.T) {
			cfg := config.Defaults()
			cfg.MaxPreAuthConnections, cfg.MaxPreAuthPerIP, cfg.LoginTimeoutSeconds = 1, 1, 1
			cfg.MaxClients = 1
			h := admissionHarness(t, cfg)
			switch outcome {
			case "success":
				c := h.loginPilot(t, "CCA101", "1000", 100)
				waitAdmission(t, h.server, 0)
				// The completed handshake must not retain a timeout callback or
				// derive the online client's context from the login deadline.
				time.Sleep(1100 * time.Millisecond)
				c.send("$PICCA101:SERVER:alive")
				if got := c.expect("$PO"); got != "$POSERVER:CCA101:alive" {
					t.Fatalf("pong: %q", got)
				}
				if h.server.OnlineCount() != 1 {
					t.Fatal("login deadline killed an online client")
				}
			case "duplicate", "full":
				h.loginPilot(t, "CCA101", "1000", 100)
				c := h.dial(t)
				callsign := "CCA102"
				if outcome == "duplicate" {
					callsign = "CCA101"
				}
				c.send("#AP" + callsign + ":SERVER:1000:secret:1:100:1:Test Pilot")
				c.expect("$ER")
				expectAdmissionClose(t, c, time.Second)
				waitAdmission(t, h.server, 0)
			case "credentials", "syntax":
				c := h.dial(t)
				if outcome == "credentials" {
					c.send("#APCCA101:SERVER:1000:wrong:1:100:1:Test Pilot")
				} else {
					c.send("invalid")
				}
				c.expect("$ER")
				expectAdmissionClose(t, c, time.Second)
				waitAdmission(t, h.server, 0)
			case "disconnect":
				c := h.dial(t)
				waitAdmission(t, h.server, 1)
				c.conn.Close()
				waitAdmission(t, h.server, 0)
			case "timeout":
				c := h.dial(t)
				expectAdmissionClose(t, c, 1500*time.Millisecond)
				waitAdmission(t, h.server, 0)
			case "shutdown":
				c := h.dial(t)
				h.cancel()
				expectAdmissionClose(t, c, 500*time.Millisecond)
				waitAdmission(t, h.server, 0)
			}
			h.server.preAuth.mu.Lock()
			defer h.server.preAuth.mu.Unlock()
			if len(h.server.preAuth.byIP) != 0 || h.server.preAuth.queries != 0 {
				t.Fatal("outcome retained peer or authentication capacity")
			}
		})
	}
}

func TestLoginDeadlineBoundsGreetingTLSAndErrorWrites(t *testing.T) {
	cert, key := writeTestCert(t)
	tlsConfig, err := TLSConfig(cert, key)
	if err != nil {
		t.Fatal(err)
	}
	for _, phase := range []string{"greeting write", "greeting peek", "TLS handshake", "error write"} {
		t.Run(phase, func(t *testing.T) {
			cfg := config.Defaults()
			cfg.IdentDelayMS = 0
			if phase == "greeting peek" {
				cfg.IdentDelayMS = 5000
			}
			auth := &admissionCountingAuth{}
			s, err := NewServer(Options{Config: config.NewStore("unused", &cfg), Users: auth})
			if err != nil {
				t.Fatal(err)
			}
			conn, peer := net.Pipe()
			defer peer.Close()
			defer conn.Close()
			var serverConn net.Conn = conn
			if phase == "TLS handshake" {
				serverConn = tls.Server(conn, tlsConfig)
			}
			if phase == "error write" {
				go func() {
					reader := bufio.NewReader(peer)
					_, _ = reader.ReadString('\n')
					_, _ = peer.Write([]byte("invalid\r\n"))
					// Do not read the synchronous error response.
				}()
			}
			lease := s.preAuth.acquire("192.0.2.1", &cfg)
			ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
			defer cancel()
			done := make(chan struct{})
			go func() { s.handleConn(context.Background(), ctx, cancel, serverConn, lease); close(done) }()
			select {
			case <-done:
			case <-time.After(500 * time.Millisecond):
				t.Fatal("phase extended the absolute login deadline")
			}
			if auth.calls.Load() != 0 {
				t.Fatal("incomplete login reached user store")
			}
			if s.preAuth.active != 0 || len(s.preAuth.byIP) != 0 {
				t.Fatal("timeout retained admission")
			}
		})
	}
}

func TestPreAuthLimitsAreSharedAcrossPlaintextAndTLSListeners(t *testing.T) {
	cert, key := writeTestCert(t)
	tlsConfig, err := TLSConfig(cert, key)
	if err != nil {
		t.Fatal(err)
	}
	for _, limit := range []string{"global", "per-IP"} {
		t.Run(limit, func(t *testing.T) {
			cfg := config.Defaults()
			if limit == "global" {
				cfg.MaxPreAuthConnections = 1
			} else {
				cfg.MaxPreAuthPerIP = 1
			}
			h := admissionHarness(t, cfg)
			pending := h.dial(t)
			waitAdmission(t, h.server, 1)
			listener, err := net.Listen("tcp", "127.0.0.1:0")
			if err != nil {
				t.Fatal(err)
			}
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			done := make(chan struct{})
			go func() { h.server.Serve(ctx, tls.NewListener(listener, tlsConfig), nil); close(done) }()
			defer func() { cancel(); listener.Close(); <-done }()
			// An excess plaintext peer on the TLS port must close before TLS.
			peer, err := net.Dial("tcp", listener.Addr().String())
			if err != nil {
				t.Fatal(err)
			}
			defer peer.Close()
			peer.SetReadDeadline(time.Now().Add(500 * time.Millisecond))
			if _, err := peer.Read(make([]byte, 1)); err == nil || isTimeout(err) {
				t.Fatalf("shared %s cap allowed a TLS handshake: %v", limit, err)
			}
			waitAdmission(t, h.server, 1)
			pending.conn.Close()
			waitAdmission(t, h.server, 0)
			peer2, err := tls.Dial("tcp", listener.Addr().String(), &tls.Config{InsecureSkipVerify: true, MinVersion: tls.VersionTLS12})
			if err != nil {
				t.Fatalf("released shared capacity did not admit TLS: %v", err)
			}
			peer2.Close()
			waitAdmission(t, h.server, 0)
		})
	}
}
