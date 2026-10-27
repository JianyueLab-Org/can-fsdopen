package fsd

import (
	"errors"
	"fmt"
	"testing"
	"time"
)

// clientByCallsign returns the online Client with the given callsign.
func clientByCallsign(t *testing.T, s *Server, callsign string) *Client {
	t.Helper()

	for _, c := range s.postOffice.snapshot() {
		if c.Callsign == callsign {
			return c
		}
	}
	t.Fatalf("client %s is not online", callsign)
	return nil
}

// positionATC posts a controller % position packet so the station counts as an
// active controller rather than an observer.
func positionATC(c *testClient) {
	c.send("%" + c.callsign + ":28550:4:50:5:31.19670:121.33500:0")
}

// TestTrackHandoffLifecycle drives the server-owned track state machine through
// a complete handoff: IT (initiate), $HO (propose), $HA (accept), HT (broadcast
// accept) and DR (drop).
//
// This is the behaviour the ATC workstation presents: the client never owns the
// state, it reads owner/handoff_peer/status from the feed.
func TestTrackHandoffLifecycle(t *testing.T) {
	h := newHarness(t, harnessOptions{
		users: map[string]*testIdentity{
			"1001": {Username: "1001", Password: "secret", Rating: 5},
			"1002": {Username: "1002", Password: "secret", Rating: 5},
			"1000": {Username: "1000", Password: "secret", Rating: int(RatingObserver)},
		},
	})

	ctr := h.loginATC(t, "ZSSS_CTR", "1001", 5)
	app := h.loginATC(t, "ZSSS_APP", "1002", 5)
	h.loginPilot(t, "CCA101", "1000", 101)
	h.waitForOnline(t, 3)

	if owners := testTrackOwners(h.server); len(owners) != 0 {
		t.Fatalf("untracked aircraft reported an owner: %v", owners)
	}

	// Initiate: ZSSS_CTR owns CCA101.
	positionATC(ctr)
	ctr.send("$CQZSSS_CTR:@94835:IT:CCA101")
	time.Sleep(200 * time.Millisecond)

	if got := testTrackOwners(h.server)["CCA101"]; got != "ZSSS_CTR" {
		t.Fatalf("TrackOwners = %q, want ZSSS_CTR", got)
	}
	if state := h.server.tracks.states()["CCA101"]; state.Owner != "ZSSS_CTR" || state.Status != TrackStatusOwned {
		t.Fatalf("after IT: state = %+v, want owned by ZSSS_CTR", state)
	}

	// Propose a handoff to ZSSS_APP; the packet must also be relayed.
	positionATC(app)
	ctr.send("$HOZSSS_CTR:ZSSS_APP:CCA101")
	app.expect("$HOZSSS_CTR:ZSSS_APP:CCA101")
	time.Sleep(200 * time.Millisecond)

	state := h.server.tracks.states()["CCA101"]
	if state.HandoffPeer != "ZSSS_APP" || state.Status != TrackStatusPendingHandoff {
		t.Fatalf("after $HO: state = %+v, want pending handoff to ZSSS_APP", state)
	}
	if got := testTrackOwners(h.server)["CCA101"]; got != "ZSSS_CTR" {
		t.Fatalf("TrackOwners moved on $HO = %q, want ZSSS_CTR", got)
	}

	// Accept.
	app.send("$HAZSSS_APP:ZSSS_CTR:CCA101")
	ctr.expect("$HAZSSS_APP:ZSSS_CTR:CCA101")
	time.Sleep(200 * time.Millisecond)

	if state := h.server.tracks.states()["CCA101"]; state.Status != TrackStatusHandoffAccepted {
		t.Fatalf("after $HA: state = %+v, want handoff_accepted", state)
	}

	// The accepting controller broadcasts the change of owner.
	app.send("$CQZSSS_APP:@94835:HT:CCA101")
	time.Sleep(200 * time.Millisecond)

	state = h.server.tracks.states()["CCA101"]
	if state.Owner != "ZSSS_APP" || state.HandoffPeer != "" || state.Status != TrackStatusOwned {
		t.Fatalf("after HT: state = %+v, want owned by ZSSS_APP", state)
	}
	if got := testTrackOwners(h.server)["CCA101"]; got != "ZSSS_APP" {
		t.Fatalf("TrackOwners = %q, want ZSSS_APP", got)
	}

	ctr.send("$CQZSSS_CTR:@94835:DR:CCA101")
	ctr.expect("$ER")
	time.Sleep(200 * time.Millisecond)

	if got := testTrackOwners(h.server)["CCA101"]; got != "ZSSS_APP" {
		t.Fatalf("a non-owner's DR released the track: %v", testTrackOwners(h.server))
	}

	// The owner may.
	app.send("$CQZSSS_APP:@94835:DR:CCA101")
	time.Sleep(200 * time.Millisecond)

	if got := testTrackOwners(h.server)["CCA101"]; got != "" {
		t.Fatalf("track not released on DR: %v", testTrackOwners(h.server))
	}
}

// TestHandoffToUntrackedAircraft keeps a pending handoff visible even before
// the owner's initiate-track arrives, without surfacing an owner to the lock.
func TestHandoffToUntrackedAircraft(t *testing.T) {
	h := newHarness(t, harnessOptions{
		users: map[string]*testIdentity{
			"1001": {Username: "1001", Password: "secret", Rating: 5},
			"1002": {Username: "1002", Password: "secret", Rating: 5},
			"1000": {Username: "1000", Password: "secret", Rating: int(RatingObserver)},
		},
	})

	ctr := h.loginATC(t, "ZSSS_CTR", "1001", 5)
	h.loginATC(t, "ZSSS_APP", "1002", 5)
	h.loginPilot(t, "CCA101", "1000", 101)
	h.waitForOnline(t, 3)

	positionATC(ctr)
	ctr.send("$HOZSSS_CTR:ZSSS_APP:CCA101")
	time.Sleep(200 * time.Millisecond)

	if got := testTrackOwners(h.server)["CCA101"]; got != "" {
		t.Fatalf("TrackOwners = %q, want empty (no owner yet)", got)
	}
	if state := h.server.tracks.states()["CCA101"]; state.HandoffPeer != "ZSSS_APP" || state.Status != TrackStatusPendingHandoff {
		t.Fatalf("after $HO: state = %+v, want pending handoff to ZSSS_APP", state)
	}
}

// twoControllersAndAPilot is the shape every ownership test needs: two active
// controllers and one aircraft.
func twoControllersAndAPilot(t *testing.T) (*testHarness, *testClient, *testClient) {
	t.Helper()

	h := newHarness(t, harnessOptions{
		users: map[string]*testIdentity{
			"1001": {Username: "1001", Password: "secret", Rating: 5},
			"1002": {Username: "1002", Password: "secret", Rating: 5},
			"1011": {Username: "1011", Password: "secret", Rating: int(RatingSupervisor)},
			"1000": {Username: "1000", Password: "secret", Rating: int(RatingObserver)},
		},
	})

	ctr := h.loginATC(t, "ZSSS_CTR", "1001", 5)
	app := h.loginATC(t, "ZSSS_APP", "1002", 5)
	h.loginPilot(t, "CCA101", "1000", 101)
	h.waitForOnline(t, 3)

	positionATC(ctr)
	positionATC(app)
	time.Sleep(200 * time.Millisecond)

	return h, ctr, app
}

func TestInitiateTrackCannotStealAnOwnedAircraft(t *testing.T) {
	h, ctr, app := twoControllersAndAPilot(t)

	ctr.send("$CQZSSS_CTR:@94835:IT:CCA101")
	time.Sleep(200 * time.Millisecond)
	drain(app)

	app.send("$CQZSSS_APP:@94835:IT:CCA101")
	app.expect("$ER")
	time.Sleep(200 * time.Millisecond)

	if got := testTrackOwners(h.server)["CCA101"]; got != "ZSSS_CTR" {
		t.Fatalf("track owner = %q, want ZSSS_CTR", got)
	}
	// A refused query must not be relayed: another scope acting on it would
	// disagree with the server about who holds the aircraft.
	ctr.expectNone("$CQZSSS_APP", time.Second)
}

// $CQ DR may not release somebody else's track. The registry is the only
// source of tracked_by and of the 409 protecting a controller's amendments, so
// a stray DR silently unlocked the web form for an aircraft the owner was
// still working.
func TestDropTrackRequiresOwnership(t *testing.T) {
	h, ctr, app := twoControllersAndAPilot(t)

	ctr.send("$CQZSSS_CTR:@94835:IT:CCA101")
	time.Sleep(200 * time.Millisecond)
	drain(app)

	app.send("$CQZSSS_APP:@94835:DR:CCA101")
	app.expect("$ER")
	time.Sleep(200 * time.Millisecond)

	if got := testTrackOwners(h.server)["CCA101"]; got != "ZSSS_CTR" {
		t.Fatalf("a non-owner's DR released the track; owner = %q", got)
	}
}

// A supervisor may unstick a track. This is the one override, and it exists
// because a track whose owner is gone but whose entry survived is otherwise
// untouchable by anybody.
func TestSupervisorMayDropAnyTrack(t *testing.T) {
	h, ctr, _ := twoControllersAndAPilot(t)

	sup := h.loginATC(t, "ZSSS_SUP", "1011", int(RatingSupervisor))
	h.waitForOnline(t, 4)
	positionATC(sup)

	ctr.send("$CQZSSS_CTR:@94835:IT:CCA101")
	time.Sleep(200 * time.Millisecond)

	sup.send("$CQZSSS_SUP:@94835:DR:CCA101")
	time.Sleep(200 * time.Millisecond)

	if got := testTrackOwners(h.server)["CCA101"]; got != "" {
		t.Fatalf("a supervisor could not release a stuck track; owner = %q", got)
	}
}

// $CQ HT is "I have taken this from another controller", and the server has to
// check that somebody actually offered it. Without that any active controller
// could take any aircraft with one packet.
func TestAcceptTransferRequiresAnOfferedHandoff(t *testing.T) {
	h, ctr, app := twoControllersAndAPilot(t)

	ctr.send("$CQZSSS_CTR:@94835:IT:CCA101")
	time.Sleep(200 * time.Millisecond)
	drain(app)

	app.send("$CQZSSS_APP:@94835:HT:CCA101")
	app.expect("$ER")
	time.Sleep(200 * time.Millisecond)

	if got := testTrackOwners(h.server)["CCA101"]; got != "ZSSS_CTR" {
		t.Fatalf("HT took a track that was never offered; owner = %q", got)
	}

	// Offered, it goes through.
	ctr.send("$HOZSSS_CTR:ZSSS_APP:CCA101")
	time.Sleep(200 * time.Millisecond)
	app.send("$CQZSSS_APP:@94835:HT:CCA101")
	time.Sleep(200 * time.Millisecond)

	if got := testTrackOwners(h.server)["CCA101"]; got != "ZSSS_APP" {
		t.Fatalf("an offered handoff did not complete; owner = %q", got)
	}
}

// $HA from a controller the handoff was never offered to is refused. Accepting
// somebody else's handoff was the first half of taking their aircraft, since
// the accepter could then complete it with $CQ HT.
func TestAcceptHandoffRequiresBeingThePeer(t *testing.T) {
	h, ctr, _ := twoControllersAndAPilot(t)

	third := h.loginATC(t, "ZSSS_TWR", "1011", 5)
	h.waitForOnline(t, 4)
	positionATC(third)

	ctr.send("$CQZSSS_CTR:@94835:IT:CCA101")
	ctr.send("$HOZSSS_CTR:ZSSS_APP:CCA101")
	time.Sleep(200 * time.Millisecond)
	drain(third)

	third.send("$HAZSSS_TWR:ZSSS_CTR:CCA101")
	third.expect("$ER")
	time.Sleep(200 * time.Millisecond)

	if state := h.server.tracks.states()["CCA101"]; state.Status != TrackStatusPendingHandoff {
		t.Fatalf("state = %+v, want the handoff still merely pending", state)
	}
	// The refused $HA must not have been relayed to the offering controller
	// either, or they would believe ZSSS_TWR was taking the aircraft.
	ctr.expectNone("$HAZSSS_TWR", time.Second)
}

func TestHandoffClearsWhenThePeerDisconnects(t *testing.T) {
	h, ctr, app := twoControllersAndAPilot(t)

	ctr.send("$CQZSSS_CTR:@94835:IT:CCA101")
	ctr.send("$HOZSSS_CTR:ZSSS_APP:CCA101")
	time.Sleep(200 * time.Millisecond)

	if state := h.server.tracks.states()["CCA101"]; state.HandoffPeer != "ZSSS_APP" {
		t.Fatalf("state = %+v, want a pending handoff to ZSSS_APP", state)
	}

	app.conn.Close()
	h.waitForOnline(t, 2)
	time.Sleep(200 * time.Millisecond)

	state := h.server.tracks.states()["CCA101"]
	if state.HandoffPeer != "" || state.Status != TrackStatusOwned {
		t.Fatalf("state = %+v, want the handoff cleared and the track still owned", state)
	}
	if state.Owner != "ZSSS_CTR" {
		t.Errorf("owner = %q, want the offering controller to keep the aircraft", state.Owner)
	}
}

// The offering controller may recall a handoff with #PC … HC. It is routed
// peer to peer and the track state machine is otherwise entirely $CQ-driven,
// so the recall never used to reach the registry at all.
func TestHandoffCancelClearsThePendingHandoff(t *testing.T) {
	h, ctr, _ := twoControllersAndAPilot(t)

	ctr.send("$CQZSSS_CTR:@94835:IT:CCA101")
	ctr.send("$HOZSSS_CTR:ZSSS_APP:CCA101")
	time.Sleep(200 * time.Millisecond)

	ctr.send("#PCZSSS_CTR:ZSSS_APP:CCP:HC:CCA101")
	time.Sleep(200 * time.Millisecond)

	state := h.server.tracks.states()["CCA101"]
	if state.HandoffPeer != "" || state.Status != TrackStatusOwned {
		t.Fatalf("state = %+v, want the handoff recalled", state)
	}
	if state.Owner != "ZSSS_CTR" {
		t.Errorf("owner = %q, want the track kept by ZSSS_CTR", state.Owner)
	}
}

// A recall for an aircraft neither party is working is refused rather than
// applied, on the same rule as everything else here.
func TestHandoffCancelRequiresBeingAParty(t *testing.T) {
	h, ctr, _ := twoControllersAndAPilot(t)

	third := h.loginATC(t, "ZSSS_TWR", "1011", 5)
	h.waitForOnline(t, 4)
	positionATC(third)

	ctr.send("$CQZSSS_CTR:@94835:IT:CCA101")
	ctr.send("$HOZSSS_CTR:ZSSS_APP:CCA101")
	time.Sleep(200 * time.Millisecond)
	drain(third)

	third.send("#PCZSSS_TWR:ZSSS_CTR:CCP:HC:CCA101")
	third.expect("$ER")
	time.Sleep(200 * time.Millisecond)

	if state := h.server.tracks.states()["CCA101"]; state.HandoffPeer != "ZSSS_APP" {
		t.Fatalf("state = %+v, want the handoff untouched", state)
	}
}

// A handoff nobody ever completed is swept. Neither #PC … HC nor a client
// simply forgetting is guaranteed to reach the registry, and pending_handoff
// has no other exit.
func TestStaleHandoffsAreSwept(t *testing.T) {
	h, ctr, _ := twoControllersAndAPilot(t)

	ctr.send("$CQZSSS_CTR:@94835:IT:CCA101")
	ctr.send("$HOZSSS_CTR:ZSSS_APP:CCA101")
	time.Sleep(200 * time.Millisecond)

	// Nothing is old enough yet.
	if cleared := h.server.tracks.expireHandoffs(time.Hour); cleared != 0 {
		t.Fatalf("swept %d fresh handoffs, want 0", cleared)
	}
	if cleared := h.server.tracks.expireHandoffs(time.Nanosecond); cleared != 1 {
		t.Fatalf("swept %d stale handoffs, want 1", cleared)
	}

	state := h.server.tracks.states()["CCA101"]
	if state.HandoffPeer != "" || state.Status != TrackStatusOwned {
		t.Fatalf("state = %+v, want the stale handoff cleared", state)
	}
}

func TestSweepingAnUnownedHandoffRemovesTheEntry(t *testing.T) {
	h, ctr, _ := twoControllersAndAPilot(t)

	ctr.send("$HOZSSS_CTR:ZSSS_APP:CCA101")
	time.Sleep(200 * time.Millisecond)

	if h.server.tracks.size() != 1 {
		t.Fatalf("registry holds %d entries, want 1", h.server.tracks.size())
	}
	h.server.tracks.expireHandoffs(time.Nanosecond)

	if n := h.server.tracks.size(); n != 0 {
		t.Fatalf("registry holds %d entries after the sweep, want 0", n)
	}
}

// TestHandoffTargetsAreValidatedAndMustExist rejects invented aircraft.
func TestHandoffTargetsAreValidatedAndMustExist(t *testing.T) {
	h, ctr, _ := twoControllersAndAPilot(t)

	for _, target := range []string{
		"cca101",                         // lower case: not a legal callsign
		"CCA/101",                        // illegal character
		"AAAAAAAAAAAAAAAAAAAAAAAAAAAAAA", // over the length limit
		"NOBODY1",                        // well-formed, but nobody is connected as it
		"ZSSS_APP",                       // a controller is not an aircraft
	} {
		ctr.send("$HOZSSS_CTR:ZSSS_APP:" + target)
		ctr.expect("$ER")
	}
	time.Sleep(200 * time.Millisecond)

	if n := h.server.tracks.size(); n != 0 {
		t.Fatalf("registry holds %d entries after five rejected handoffs, want 0", n)
	}
}

// The same validation guards $CQ IT and HT, which are the other two insertion
// paths.
func TestInitiateTrackTargetsAreValidatedAndMustExist(t *testing.T) {
	h, ctr, _ := twoControllersAndAPilot(t)

	for _, query := range []string{"IT", "HT"} {
		ctr.send("$CQZSSS_CTR:@94835:" + query + ":NOBODY1")
		ctr.expect("$ER")
		ctr.send("$CQZSSS_CTR:@94835:" + query + ":not-a-callsign")
		ctr.expect("$ER")
	}
	time.Sleep(200 * time.Millisecond)

	if n := h.server.tracks.size(); n != 0 {
		t.Fatalf("registry holds %d entries after four rejected queries, want 0", n)
	}
}

func TestTrackRegistryIsBounded(t *testing.T) {
	r := newTrackRegistry()

	for i := range maxTrackedAircraft {
		if err := r.claim(fmt.Sprintf("CCA%d", i), "ZSSS_CTR"); err != nil {
			t.Fatalf("claim %d: %v", i, err)
		}
	}
	if err := r.claim("CCA99999", "ZSSS_CTR"); !errors.Is(err, errTrackRegistryFull) {
		t.Fatalf("claim past the bound returned %v, want errTrackRegistryFull", err)
	}
	// An aircraft already in the registry is still claimable: the bound is on
	// growth, not on operating.
	if err := r.claim("CCA0", "ZSSS_CTR"); err != nil {
		t.Fatalf("re-claiming an existing track at the bound: %v", err)
	}
}
