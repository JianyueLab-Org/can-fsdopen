package fsd

import (
	"context"
	"github.com/JianyueLab-Org/can-fsdopen/config"
	"net"
	"runtime"
	"testing"
	"time"
)

// opaqueContext hides the fact that its parent is a *context.cancelCtx.
//
// context.WithCancel has two ways to attach a child: registering it in the
// parent's `children` map when the parent is a recognisable cancelCtx, or —
// when it is not — spawning a goroutine that waits on both Done channels.
// Overriding Value forces the second, which turns a child that is never
// cancelled into a goroutine that never exits, and so makes the leak countable
// from a test. Both shapes leak the same child; only this one is observable.
type opaqueContext struct{ ctx context.Context }

func (o opaqueContext) Deadline() (time.Time, bool) { return o.ctx.Deadline() }
func (o opaqueContext) Done() <-chan struct{}       { return o.ctx.Done() }
func (o opaqueContext) Err() error                  { return o.ctx.Err() }
func (o opaqueContext) Value(any) any               { return nil }

// settledGoroutines waits for the count to stop moving, so a connection still
// tearing down is not mistaken for a leak.
func settledGoroutines() int {
	previous := -1
	for range 50 {
		time.Sleep(20 * time.Millisecond)
		current := runtime.NumGoroutine()
		if current == previous {
			return current
		}
		previous = current
	}
	return runtime.NumGoroutine()
}

// **Every rejected login used to leak a context.cancelCtx for the life of the
// process.**
//
// newClient derives a child of the process-lifetime Serve context, and only
// cancelling removes that child from the parent. The three rejection paths in
// handleConn — bad credentials, an ATIS rating violation, a callsign already
// in use — all closed the socket and returned instead. It is reachable before
// authentication with any well-formed #AA/#AP. The store's limiter combines
// the CID with the socket peer and also caps failures from that peer across
// CIDs.
func TestRejectedLoginLeavesNoChildContext(t *testing.T) {
	cfg := config.Defaults()
	cfg.FSDMode = 1
	cfg.IdentDelayMS = 0

	server, err := NewServer(Options{
		Config: config.NewStore("config.json", &cfg),
		Users: &fakeAuth{users: map[string]*testIdentity{
			"1001": {Username: "1001", Password: "secret", Rating: 5},
		}},
	})
	if err != nil {
		t.Fatalf("NewServer: %v", err)
	}

	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	defer listener.Close()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go server.Serve(opaqueContext{ctx}, listener, nil)

	addr := listener.Addr().String()

	// One rejected login first, so the accept path's own goroutines exist
	// before the baseline is taken.
	rejectedLogin(t, addr, "1002")
	baseline := settledGoroutines()

	const attempts = 40
	for i := range attempts {
		rejectedLogin(t, addr, "9"+string(rune('0'+i%10))+"00")
	}

	if grew := settledGoroutines() - baseline; grew > attempts/4 {
		t.Fatalf("%d rejected logins left %d extra goroutines; each leaked its client context",
			attempts, grew)
	}
}

// rejectedLogin opens a connection, offers credentials that will be refused,
// and waits for the server to hang up.
func rejectedLogin(t *testing.T, addr, cid string) {
	t.Helper()

	conn, err := net.DialTimeout("tcp", addr, 5*time.Second)
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	defer conn.Close()

	conn.SetWriteDeadline(time.Now().Add(3 * time.Second))
	if _, err := conn.Write([]byte("#APCCA101:SERVER:" + cid + ":wrong:1:100:1:Test Pilot\r\n")); err != nil {
		t.Fatalf("write: %v", err)
	}

	// Read until the server closes, which is how a rejection ends.
	conn.SetReadDeadline(time.Now().Add(5 * time.Second))
	buf := make([]byte, 256)
	for {
		if _, err := conn.Read(buf); err != nil {
			return
		}
	}
}
