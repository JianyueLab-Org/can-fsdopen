package fsd

import (
	"testing"
	"time"
)

// **A controller sending `@` is not a pilot reporting a position.**
//
// handleFastPilotPosition opens with a class check and handleATCPosition
// rejects non-ATC, but handlePilotPosition had neither: a controller's `@` moved
// its indexed position and overwrote its visibility with the 40 NM pilot
// default until the next `%` — up to fifteen seconds of silently losing
// in-range traffic — and the packet was relayed to everyone as a pilot position
// under a controller callsign.
func TestControllerPilotPositionIsIgnored(t *testing.T) {
	h := newHarness(t, harnessOptions{
		users: map[string]*testIdentity{
			"1001": {Username: "1001", Password: "secret", Rating: 5},
			"1000": {Username: "1000", Password: "secret", Rating: int(RatingObserver)},
		},
	})

	ctr := h.loginATC(t, "ZSSS_CTR", "1001", 5)
	pilot := h.loginPilot(t, "CCA101", "1000", 101)
	h.waitForOnline(t, 2)

	// Shanghai, 50 NM of visibility. The pilot is left at the login default of
	// 0,0 with 40 NM, so the two are nowhere near each other.
	positionATC(ctr)
	time.Sleep(300 * time.Millisecond)

	c := clientByCallsign(t, h.server, "ZSSS_CTR")
	wantLatLon, wantRange := c.LatLon(), c.VisRange()
	if wantRange == pilotVisRangeNM*MetresPerNauticalMile {
		t.Fatalf("the controller's visibility is already the pilot default (%v); the test proves nothing", wantRange)
	}

	drain(pilot)

	// A pilot position claiming to be at the pilot's own location. If the
	// server honours it, the controller's box moves on top of the pilot and
	// the packet is relayed there as traffic.
	ctr.send("@N:ZSSS_CTR:1200:1:0.00000:0.00000:1000:0:0:0")
	time.Sleep(300 * time.Millisecond)

	c = clientByCallsign(t, h.server, "ZSSS_CTR")
	if got := c.LatLon(); got != wantLatLon {
		t.Errorf("controller position moved to %v, want %v", got, wantLatLon)
	}
	if got := c.VisRange(); got != wantRange {
		t.Errorf("controller visibility became %v, want %v", got, wantRange)
	}

	pilot.expectNone("@N:ZSSS_CTR", time.Second)
}
