package fsd

import (
	"context"
	"reflect"
	"runtime"
	"sync"
	"testing"
	"time"

	"github.com/JianyueLab-Org/can-fsdopen/config"
)

func newTrackLifecycleServer(t *testing.T) (*Server, *Client, *Client, *Client) {
	t.Helper()
	cfg := config.Defaults()
	s, err := NewServer(Options{Config: config.NewStore("unused", &cfg), Users: &fakeAuth{}})
	if err != nil {
		t.Fatal(err)
	}
	controller := newIdleClient(t, "ZSSS_CTR")
	peer := newIdleClient(t, "ZSSS_APP")
	pilot := newIdleClient(t, "CCA101")
	for _, c := range []*Client{controller, peer} {
		c.IsATC = true
		c.NetworkRating = RatingController1
		c.MaxRating = RatingController1
		c.setFacilityType(FacilityCentre)
	}
	for _, c := range []*Client{controller, peer, pilot} {
		if err := s.postOffice.register(c); err != nil {
			t.Fatal(err)
		}
	}
	return s, controller, peer, pilot
}

func awaitTrackLifecycle(t *testing.T, done <-chan struct{}) {
	t.Helper()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("track lifecycle operation did not finish")
	}
}

// A pending writer blocks new readers. Observe that state rather than relying
// on a sleep to guess whether disconnect has reached the membership lock.
func awaitRegistryWriter(t *testing.T, p *postOffice) {
	t.Helper()
	deadline := time.After(5 * time.Second)
	for {
		if !p.mu.TryRLock() {
			return
		}
		p.mu.RUnlock()
		select {
		case <-deadline:
			t.Fatal("disconnect did not reach the membership lock")
		default:
			runtime.Gosched()
		}
	}
}

func TestTrackMutationDisconnectAndReuseAreSerialized(t *testing.T) {
	for _, departingRole := range []string{"pilot", "owner", "handoff sender", "handoff peer"} {
		t.Run(departingRole, func(t *testing.T) {
			s, controller, peer, pilot := newTrackLifecycleServer(t)
			departing := pilot
			switch departingRole {
			case "owner", "handoff sender":
				departing = controller
			case "handoff peer":
				departing = peer
			}
			pilot.setFlightPlan(&FlightPlan{Route: "DISCONNECT-PLAN"})

			entered := make(chan struct{})
			resume := make(chan struct{})
			unblock := sync.OnceFunc(func() { close(resume) })
			defer unblock()
			mutationDone := make(chan struct{})
			var changed bool
			go func() {
				defer close(mutationDone)
				changed = s.changeTrack(controller, pilot.Callsign, peer.Callsign, true, func() error {
					close(entered)
					<-resume
					if departingRole == "pilot" || departingRole == "owner" {
						return s.tracks.claim(pilot.Callsign, controller.Callsign)
					}
					return s.tracks.proposeHandoff(pilot.Callsign, controller.Callsign, peer.Callsign)
				})
			}()
			awaitTrackLifecycle(t, entered)

			removed := make(chan struct{})
			go func() {
				defer close(removed)
				s.postOffice.release(departing, s.onDisconnect)
			}()
			awaitRegistryWriter(t, s.postOffice)

			replacement := newIdleClient(t, departing.Callsign)
			replacement.LoginData = departing.LoginData
			replacement.setFacilityType(departing.FacilityType())
			registered := make(chan struct{})
			var registerErr error
			go func() {
				defer close(registered)
				registerErr = s.postOffice.register(replacement)
				if registerErr == nil {
					s.restoreFlightPlan(replacement)
				}
			}()
			unblock()
			awaitTrackLifecycle(t, mutationDone)
			awaitTrackLifecycle(t, removed)
			awaitTrackLifecycle(t, registered)
			if !changed || registerErr != nil {
				t.Fatalf("mutation=%t, reuse error=%v", changed, registerErr)
			}
			if state := s.tracks.states(); len(state) != 0 {
				t.Fatalf("departed %s left stale tracks after reuse: %+v", departingRole, state)
			}
			if departing == pilot {
				fp := replacement.FlightPlanData()
				if fp == nil || fp.Route != "DISCONNECT-PLAN" {
					t.Fatalf("callsign reused before its reconnect plan was saved: %+v", fp)
				}
			}

			// Repeating the old connection's cleanup must not touch ownership or
			// pending handoffs established by the replacement connection.
			if departing == controller {
				if err := s.tracks.claim(pilot.Callsign, replacement.Callsign); err != nil {
					t.Fatal(err)
				}
			} else if departing == peer {
				if err := s.tracks.proposeHandoff(pilot.Callsign, controller.Callsign, replacement.Callsign); err != nil {
					t.Fatal(err)
				}
			} else if err := s.tracks.claim(replacement.Callsign, controller.Callsign); err != nil {
				t.Fatal(err)
			}
			want := s.tracks.states()
			s.postOffice.release(departing, s.onDisconnect)
			if got := s.tracks.states(); !reflect.DeepEqual(got, want) {
				t.Fatalf("old cleanup changed replacement tracks: got %+v, want %+v", got, want)
			}
		})
	}
}

func TestDepartedControllerCannotMutateReplacementTracks(t *testing.T) {
	for _, tc := range []struct {
		name   string
		packet string
	}{
		{"claim", "$CQZSSS_CTR:@94835:IT:CCA101\r\n"},
		{"take handoff", "$CQZSSS_CTR:@94835:HT:CCA101\r\n"},
		{"offer handoff", "$HOZSSS_CTR:ZSSS_APP:CCA101\r\n"},
		{"accept handoff", "$HAZSSS_CTR:ZSSS_APP:CCA101\r\n"},
		{"drop", "$CQZSSS_CTR:@94835:DR:CCA101\r\n"},
		{"cancel handoff", "#PCZSSS_CTR:ZSSS_APP:CCP:HC:CCA101\r\n"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			s, old, peer, pilot := newTrackLifecycleServer(t)
			s.postOffice.release(old, s.onDisconnect)
			replacement := newIdleClient(t, old.Callsign)
			replacement.LoginData = old.LoginData
			replacement.setFacilityType(FacilityCentre)
			if err := s.postOffice.register(replacement); err != nil {
				t.Fatal(err)
			}
			switch tc.name {
			case "accept handoff", "cancel handoff":
				if err := s.tracks.proposeHandoff(pilot.Callsign, peer.Callsign, replacement.Callsign); err != nil {
					t.Fatal(err)
				}
			case "drop":
				if err := s.tracks.claim(pilot.Callsign, replacement.Callsign); err != nil {
					t.Fatal(err)
				}
			}
			want := s.tracks.states()
			packet := []byte(tc.packet)
			s.dispatch(old, GetPacketType(packet), packet)
			if got := s.tracks.states(); !reflect.DeepEqual(got, want) {
				t.Fatalf("departed controller changed replacement state: got %+v, want %+v", got, want)
			}
			assertNoQueuedPackets(t, peer)
		})
	}
}

func TestHandoffToDepartedPeerLeavesNoPendingTrack(t *testing.T) {
	s, controller, peer, _ := newTrackLifecycleServer(t)
	s.postOffice.release(peer, s.onDisconnect)
	packet := []byte("$HOZSSS_CTR:ZSSS_APP:CCA101\r\n")
	s.dispatch(controller, PacketHandoffRequest, packet)
	if state := s.tracks.states(); len(state) != 0 {
		t.Fatalf("departed peer left a pending handoff: %+v", state)
	}
	assertQueuedPrefix(t, controller, "$ERserver:ZSSS_CTR:7:")
}

func TestOldDisconnectCannotReplaceNewOfflinePlan(t *testing.T) {
	s, _, _, old := newTrackLifecycleServer(t)
	old.setFlightPlan(&FlightPlan{Route: "OLD"})
	s.postOffice.release(old, s.onDisconnect)
	replacement := newIdleClient(t, old.Callsign)
	replacement.setFlightPlan(&FlightPlan{Route: "CURRENT"})
	if err := s.postOffice.register(replacement); err != nil {
		t.Fatal(err)
	}
	s.postOffice.release(replacement, s.onDisconnect)
	s.postOffice.release(old, s.onDisconnect)
	if fp := s.takeOfflinePlan(replacement.Callsign, replacement.CID); fp == nil || fp.Route != "CURRENT" {
		t.Fatalf("old cleanup replaced the current reconnect plan: %+v", fp)
	}
}

type trackReplyContext struct {
	context.Context
	entered chan struct{}
	resume  chan struct{}
	once    sync.Once
}

func (c *trackReplyContext) Done() <-chan struct{} {
	c.once.Do(func() { close(c.entered) })
	<-c.resume
	return c.Context.Done()
}

func TestTrackRefusalSendsAfterReleasingMembershipLock(t *testing.T) {
	s, controller, _, pilot := newTrackLifecycleServer(t)
	gate := &trackReplyContext{Context: controller.ctx, entered: make(chan struct{}), resume: make(chan struct{})}
	controller.ctx = gate
	for len(controller.sendCh) < cap(controller.sendCh) {
		controller.sendCh <- "filler"
	}
	unblock := sync.OnceFunc(func() { close(gate.resume) })
	defer unblock()
	done := make(chan struct{})
	go func() {
		defer close(done)
		s.changeTrack(controller, pilot.Callsign, "", true, func() error { return errTrackOwned })
	}()
	awaitTrackLifecycle(t, gate.entered)
	if !s.postOffice.mu.TryLock() {
		t.Fatal("track refusal held the membership lock while sending")
	}
	s.postOffice.mu.Unlock()
	controller.cancel()
	unblock()
	awaitTrackLifecycle(t, done)
}
