package fsd

import (
	"context"
	"net"
	"testing"
	"time"
)

// newIdleClient builds a registered-shaped Client with no writer goroutine, so
// its outbound buffer can be filled and left full.
func newIdleClient(t *testing.T, callsign string) *Client {
	t.Helper()

	local, remote := net.Pipe()
	t.Cleanup(func() {
		local.Close()
		remote.Close()
	})

	c := newClient(context.Background(), local, nil, LoginData{
		Callsign: callsign, CID: "1000", ProtoRevision: ProtoRevisionClassic,
	})
	t.Cleanup(c.cancel)

	return c
}

// **A client that has stopped reading must not stall position handling for the
// whole network.**
//
// postOffice.search used to run its visit callback inside treeMu.RLock, and
// every ranged broadcast calls Client.Send from that callback. Send waits up to
// two seconds when the recipient's 64-slot buffer is full, so one client with a
// closed TCP receive window held the read lock for two seconds; updatePosition
// wants the write lock, and Go's RWMutex blocks new readers once a writer
// queues, so every position packet on the network stopped for that long.
// Nothing caps connections per CID, so it was repeatable at will.
func TestStalledRecipientDoesNotBlockPositionUpdates(t *testing.T) {
	p := newPostOffice(16)

	sender := newIdleClient(t, "CCA101")
	stalled := newIdleClient(t, "CCA102")

	if err := p.register(sender); err != nil {
		t.Fatalf("register sender: %v", err)
	}
	if err := p.register(stalled); err != nil {
		t.Fatalf("register stalled: %v", err)
	}

	// Fill the stalled client's outbound buffer exactly. Nothing drains it:
	// there is no writer goroutine, which is what a closed receive window
	// looks like from in here.
	for len(stalled.sendCh) < cap(stalled.sendCh) {
		stalled.sendCh <- "#TMSERVER:CCA102:filler\r\n"
	}

	broadcasting := make(chan struct{})
	done := make(chan struct{})

	go func() {
		defer close(done)
		p.search(sender, func(recipient *Client) bool {
			close(broadcasting)
			// Blocks for the full two-second grace period before giving up.
			recipient.Send("@N:CCA101:1200:1:0.0:0.0:1000:0:0:0\r\n")
			return true
		})
	}()

	<-broadcasting

	start := time.Now()
	p.updatePosition(sender, [2]float64{31.2, 121.3}, pilotVisRangeNM*MetresPerNauticalMile)
	elapsed := time.Since(start)

	// The stalled Send is still running; it must not have been in the way.
	if elapsed > 500*time.Millisecond {
		t.Fatalf("updatePosition waited %v on a stalled recipient; the R-tree lock was held across Send", elapsed)
	}

	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("the broadcast never finished")
	}
}

// The search must still visit every in-range client, and still skip the
// sender — collecting recipients under the lock changes when they are visited,
// not which.
func TestSearchVisitsEveryInRangeClientExceptTheSender(t *testing.T) {
	p := newPostOffice(16)

	sender := newIdleClient(t, "CCA101")
	near := newIdleClient(t, "CCA102")
	far := newIdleClient(t, "CCA103")

	for _, c := range []*Client{sender, near, far} {
		if err := p.register(c); err != nil {
			t.Fatalf("register %s: %v", c.Callsign, err)
		}
	}
	p.updatePosition(far, [2]float64{60, 60}, pilotVisRangeNM*MetresPerNauticalMile)

	var visited []string
	p.search(sender, func(recipient *Client) bool {
		visited = append(visited, recipient.Callsign)
		return true
	})

	if len(visited) != 1 || visited[0] != "CCA102" {
		t.Fatalf("visited %v, want only CCA102", visited)
	}
}

// A callback that stops early still stops the visiting.
func TestSearchStopsWhenTheCallbackReturnsFalse(t *testing.T) {
	p := newPostOffice(16)

	sender := newIdleClient(t, "CCA101")
	for _, callsign := range []string{"CCA102", "CCA103", "CCA104"} {
		if err := p.register(newIdleClient(t, callsign)); err != nil {
			t.Fatalf("register %s: %v", callsign, err)
		}
	}
	if err := p.register(sender); err != nil {
		t.Fatalf("register sender: %v", err)
	}

	seen := 0
	p.search(sender, func(*Client) bool {
		seen++
		return false
	})

	if seen != 1 {
		t.Fatalf("visited %d clients after the callback said stop, want 1", seen)
	}
}
