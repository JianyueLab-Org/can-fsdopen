package fsd

import (
	"fmt"
	"sync"
	"testing"
	"time"
)

func registerSpatialClient(t *testing.T, p *postOffice, callsign string, centre [2]float64, rangeNM float64) *Client {
	t.Helper()
	c := newIdleClient(t, callsign)
	c.setLatLon(centre[0], centre[1])
	c.setVisRange(rangeNM * MetresPerNauticalMile)
	if err := p.register(c); err != nil {
		t.Fatal(err)
	}
	return c
}

func assertSpatialVisits(t *testing.T, p *postOffice, sender *Client, want ...*Client) {
	t.Helper()
	seen := make(map[*Client]int)
	p.search(sender, func(found *Client) bool {
		seen[found]++
		return true
	})
	if len(seen) != len(want) {
		t.Fatalf("search found %d different clients, want %d: %v", len(seen), len(want), seen)
	}
	for _, c := range want {
		if seen[c] != 1 {
			t.Fatalf("search visited %s %d times, want exactly once", c.Callsign, seen[c])
		}
	}
}

func TestSpatialInsertionWrapsAndDeduplicates(t *testing.T) {
	for _, tc := range []struct {
		name          string
		sender, peer  [2]float64
		senderRangeNM float64
		peerRangeNM   float64
	}{
		{"east_to_west", [2]float64{0, 179.9}, [2]float64{0, -179.9}, 40, 40},
		{"west_to_east", [2]float64{0, -179.9}, [2]float64{0, 179.9}, 40, 40},
		{"receiver_only_wraps_west", [2]float64{0, 178.5}, [2]float64{0, -179.5}, 1, 200},
		{"receiver_only_wraps_east", [2]float64{0, -178.5}, [2]float64{0, 179.5}, 1, 200},
		{"positive_endpoint", [2]float64{0, 180}, [2]float64{0, -180}, 0, 0},
		{"negative_endpoint", [2]float64{0, -180}, [2]float64{0, 180}, 0, 0},
		{"full_sender_split_receiver", [2]float64{0, 0}, [2]float64{0, 179.9}, 12000, 40},
		{"split_sender_full_receiver", [2]float64{0, 179.9}, [2]float64{0, 0}, 40, 12000},
		{"both_full_longitude", [2]float64{0, 179.9}, [2]float64{0, -179.9}, 12000, 12000},
		{"north_polar_sender", [2]float64{89, 0}, [2]float64{89.9, 180}, 70, 1},
		{"north_polar_receiver", [2]float64{89.9, 180}, [2]float64{89, 0}, 1, 70},
		{"south_polar_sender", [2]float64{-89, 0}, [2]float64{-89.9, 180}, 70, 1},
		{"south_polar_receiver", [2]float64{-89.9, 180}, [2]float64{-89, 0}, 1, 70},
		{"north_polar_arc", [2]float64{89.25, -150}, [2]float64{89, 170}, 1, 40},
		{"south_polar_arc", [2]float64{-89.25, -150}, [2]float64{-89, 170}, 1, 40},
		{"north_pole", [2]float64{90, 0}, [2]float64{90, 180}, 1, 1},
		{"south_pole", [2]float64{-90, 0}, [2]float64{-90, 180}, 1, 1},
	} {
		t.Run(tc.name, func(t *testing.T) {
			p := newPostOffice(8)
			sender := registerSpatialClient(t, p, "TEST1", tc.sender, tc.senderRangeNM)
			peer := registerSpatialClient(t, p, "TEST2", tc.peer, tc.peerRangeNM)
			assertSpatialVisits(t, p, sender, peer)
			assertSpatialVisits(t, p, peer, sender)
		})
	}
}

func TestSpatialMovementRangeChangesAndRelease(t *testing.T) {
	p := newPostOffice(8)
	sender := registerSpatialClient(t, p, "TEST1", [2]float64{0, -179.9}, 1)
	peer := registerSpatialClient(t, p, "TEST2", [2]float64{0, 0}, 40)
	for range 8 {
		for _, step := range []struct {
			centre  [2]float64
			rangeNM float64
			want    bool
		}{
			{[2]float64{0, 179.9}, 40, true},
			{[2]float64{0, -179.9}, 40, true},
			{[2]float64{0, 179}, 100, true},
			{[2]float64{0, 179}, 1, false},
			{[2]float64{0, 179}, 100, true},
			{[2]float64{0, 0}, 12000, true},
			{[2]float64{0, 0}, 1, false},
			{[2]float64{0, 180}, 40, true},
			{[2]float64{0, -180}, 40, true},
		} {
			p.updatePosition(peer, step.centre, step.rangeNM*MetresPerNauticalMile)
			if step.want {
				assertSpatialVisits(t, p, sender, peer)
			} else {
				assertSpatialVisits(t, p, sender)
			}
		}
	}
	p.release(peer, func(*Client) {})
	p.updatePosition(sender, [2]float64{0, 0}, 12000*MetresPerNauticalMile)
	assertSpatialVisits(t, p, sender)

	replacement := registerSpatialClient(t, p, peer.Callsign, [2]float64{0, 179.9}, 40)
	// Late packets and repeated cleanup from the departed connection must not
	// resurrect its spatial entry or remove the replacement's entry.
	p.updatePosition(peer, [2]float64{0, -179.9}, 40*MetresPerNauticalMile)
	p.release(peer, func(*Client) { t.Fatal("old session cleaned up its replacement") })
	assertSpatialVisits(t, p, sender, replacement)
	p.release(replacement, func(*Client) {})
	assertSpatialVisits(t, p, sender)
}

func TestSpatialWrappedReceiverRangeChangesAffectDelivery(t *testing.T) {
	s := &Server{postOffice: newPostOffice(8)}
	pilot := registerSpatialClient(t, s.postOffice, "TEST1", [2]float64{0, 178.5}, 40)
	receiver := registerSpatialClient(t, s.postOffice, "TEST_CTR", [2]float64{0, -179.5}, 200)
	pilot.MaxRating = RatingObserver
	receiver.IsATC = true
	for _, step := range []struct {
		rangeNM float64
		want    int
	}{{200, 1}, {80, 0}, {200, 1}, {40, 0}, {200, 1}} {
		s.postOffice.updatePosition(receiver, [2]float64{0, -179.5}, step.rangeNM*MetresPerNauticalMile)
		packet := sendVisibilityPosition(s, pilot, "@", [2]float64{0, 178.5})
		assertSpatialPacketCount(t, receiver, packet, step.want)
	}
}

func TestSpatialWrappedSearchKeepsClosestVelocityPeerAndEarlyStop(t *testing.T) {
	p := newPostOffice(8)
	sender := registerSpatialClient(t, p, "TEST1", [2]float64{0, 179.9}, 200)
	near := registerSpatialClient(t, p, "TEST2", [2]float64{0, -179.9}, 200)
	far := registerSpatialClient(t, p, "TEST3", [2]float64{0, 179}, 200)
	controller := registerSpatialClient(t, p, "TEST_CTR", [2]float64{0, 179.91}, 200)
	for _, c := range []*Client{sender, near, far, controller} {
		c.ProtoRevision = ProtoRevisionVelocity
	}
	controller.IsATC = true
	seen := 0
	result := p.search(sender, func(*Client) bool {
		seen++
		return false
	})
	if seen != 1 {
		t.Fatalf("early stop visited %d clients, want 1", seen)
	}
	// The peer across the antimeridian is about 12 NM away. The closer
	// controller must not participate in the velocity-peer calculation.
	if result.closestVelocityPeer < 22000 || result.closestVelocityPeer > 22500 {
		t.Fatalf("closest velocity peer = %v metres, want about 22239", result.closestVelocityPeer)
	}
}

func TestSpatialWrappedCallbacksCanUpdateIndex(t *testing.T) {
	p := newPostOffice(8)
	sender := registerSpatialClient(t, p, "TEST1", [2]float64{0, 179.9}, 40)
	peer := registerSpatialClient(t, p, "TEST2", [2]float64{0, -179.9}, 40)
	done := make(chan struct{})
	go func() {
		defer close(done)
		p.search(sender, func(found *Client) bool {
			p.updatePosition(found, [2]float64{0, 0}, 40*MetresPerNauticalMile)
			return true
		})
	}()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("wrapped search held the index lock across its callback")
	}
	assertSpatialVisits(t, p, sender)
	p.updatePosition(peer, [2]float64{0, -179.9}, 40*MetresPerNauticalMile)
	assertSpatialVisits(t, p, sender, peer)
}

func TestSpatialConcurrentWrapUpdatesAndReleaseLeaveNoStaleEntries(t *testing.T) {
	p := newPostOffice(8)
	sender := registerSpatialClient(t, p, "TEST1", [2]float64{0, 0}, 12000)
	peer := registerSpatialClient(t, p, "TEST2", [2]float64{0, 179.9}, 40)
	start := make(chan struct{})
	failures := make(chan string, 1)
	var workers sync.WaitGroup
	for worker := range 3 {
		workers.Add(1)
		go func(worker int) {
			defer workers.Done()
			<-start
			for step := range 100 {
				if worker < 2 {
					longitude := 179.9
					if (step+worker)%2 == 0 {
						longitude = -179.9
					}
					p.updatePosition(peer, [2]float64{0, longitude}, float64(1+step%3)*40*MetresPerNauticalMile)
					continue
				}
				seen := 0
				p.search(sender, func(found *Client) bool {
					seen++
					return true
				})
				if seen != 1 {
					select {
					case failures <- fmt.Sprintf("concurrent search visited %d peers, want 1", seen):
					default:
					}
					return
				}
			}
		}(worker)
	}
	close(start)
	workers.Wait()
	select {
	case failure := <-failures:
		t.Fatal(failure)
	default:
	}
	assertSpatialVisits(t, p, sender, peer)

	workers.Add(1)
	go func() {
		defer workers.Done()
		for range 100 {
			p.updatePosition(peer, [2]float64{0, 179.9}, 40*MetresPerNauticalMile)
		}
	}()
	p.release(peer, func(*Client) {})
	workers.Wait()
	assertSpatialVisits(t, p, sender)
}

func TestSpatialControllerPresenceCrossesAntimeridian(t *testing.T) {
	s := &Server{postOffice: newPostOffice(8)}
	controller := registerSpatialClient(t, s.postOffice, "EAST_CTR", [2]float64{0, 179.9}, 40)
	peer := registerSpatialClient(t, s.postOffice, "WEST_CTR", [2]float64{0, -179.9}, 40)
	for _, c := range []*Client{controller, peer} {
		c.IsATC = true
		c.touchPosition()
	}
	s.broadcastAddPacket(controller)
	assertQueuedPrefix(t, peer, "#AAEAST_CTR:")
	assertQueuedPrefix(t, controller, "#AAWEST_CTR:")
	s.broadcastAddPacket(controller)
	assertNoQueuedPackets(t, peer)
	assertNoQueuedPackets(t, controller)

	s.postOffice.updatePosition(controller, [2]float64{0, 0}, 40*MetresPerNauticalMile)
	s.broadcastAddPacket(controller)
	assertQueuedPrefix(t, peer, "#DAEAST_CTR:")
	assertQueuedPrefix(t, controller, "#DAWEST_CTR:")
}
