package fsd

import (
	"fmt"
	"github.com/JianyueLab-Org/can-fsdopen/config"
	"strings"
	"testing"
	"time"
)

func forRatingModes(t *testing.T, run func(*testing.T, harnessOptions)) {
	t.Helper()
	for _, live := range []bool{true, false} {
		for _, enforcement := range []config.FacilityEnforcement{
			config.EnforcementOff, config.EnforcementWarn, config.EnforcementStrict,
		} {
			t.Run(fmt.Sprintf("live=%t/%s", live, enforcement), func(t *testing.T) {
				run(t, harnessOptions{liveMode: live, enforcement: enforcement})
			})
		}
	}
}

func expectRatingError(t *testing.T, c *testClient, code int) {
	t.Helper()
	if packet := c.expect("$ER"); !strings.Contains(packet, fmt.Sprintf(":%d:", code)) {
		t.Fatalf("error = %q, want code %d", packet, code)
	}
}

func TestAccountLoginRatingAuthorizationAllModes(t *testing.T) {
	forRatingModes(t, func(t *testing.T, opts harnessOptions) {
		for _, account := range []int{-7, -1, 0, 1, 2, 5, 10} {
			for _, atc := range []bool{false, true} {
				t.Run(fmt.Sprintf("account=%d/atc=%t", account, atc), func(t *testing.T) {
					opts.users = map[string]*testIdentity{
						"1000": {Username: "1000", Password: "secret", Rating: account},
					}
					h := newHarness(t, opts)
					c := h.dial(t)
					requested, code := account+1, ErrCodeRequestedLevelTooHigh
					if account < 1 {
						requested, code = 1, ErrCodeCertificateSuspended
					}
					if atc {
						c.send(fmt.Sprintf("#AAZSSS_OBS:SERVER:Test Controller:1000:secret:%d:100", requested))
					} else {
						c.send(fmt.Sprintf("#APCCA101:SERVER:1000:secret:%d:100:10:Test Pilot", requested))
					}
					expectRatingError(t, c, code)
					h.waitForOnline(t, 0)
				})
			}
		}
	})
}

func TestObserverFacilityAuthorizationAllModes(t *testing.T) {
	forRatingModes(t, func(t *testing.T, opts harnessOptions) {
		for _, callsign := range []string{"ZSSS_OBS", "ZSSS_CTR"} {
			for _, facility := range []int{-1, FacilityFSS, FacilityDelivery, FacilityGround,
				FacilityTower, FacilityApproach, FacilityCentre, FacilityATIS, 8} {
				t.Run(fmt.Sprintf("%s/%d", callsign, facility), func(t *testing.T) {
					h := newHarness(t, opts)
					c := h.loginATC(t, callsign, "1000", 1)
					h.waitForOnline(t, 1)
					station := clientByCallsign(t, h.server, callsign)
					c.send(fmt.Sprintf("%%%s:28550:%d:50:1:31.19670:121.33500:0", callsign, facility))
					expectRatingError(t, c, ErrCodeInvalidPositionForRating)
					h.waitForOnline(t, 0)
					if station.FacilityType() != FacilityObserver {
						t.Fatal("rejected facility was applied")
					}
				})
			}
		}
		for _, callsign := range []string{"ZSSS_ATIS", "ZSSS_A_ATIS"} {
			t.Run(callsign, func(t *testing.T) {
				h := newHarness(t, opts)
				c := h.dial(t)
				c.send(fmt.Sprintf("#AA%s:SERVER:Test Controller:1000:secret:1:100", callsign))
				expectRatingError(t, c, ErrCodeInvalidPositionForRating)
				h.waitForOnline(t, 0)
			})
		}
	})
}

func TestPositionRatingCeilingAllModes(t *testing.T) {
	forRatingModes(t, func(t *testing.T, opts harnessOptions) {
		for _, account := range []int{1, 5} {
			for _, atc := range []bool{false, true} {
				t.Run(fmt.Sprintf("account=%d/atc=%t", account, atc), func(t *testing.T) {
					opts.users = map[string]*testIdentity{
						"1000": {Username: "1000", Password: "secret", Rating: account},
					}
					h := newHarness(t, opts)
					var c *testClient
					if atc {
						c = h.loginATC(t, "ZSSS_OBS", "1000", 1)
					} else {
						c = h.loginPilot(t, "CCA101", "1000", 100)
					}
					h.waitForOnline(t, 1)
					station := clientByCallsign(t, h.server, c.callsign)
					if station.MaxRating != NetworkRating(account) {
						t.Fatalf("authenticated rating = %d, want %d", station.MaxRating, account)
					}
					if atc {
						c.send(fmt.Sprintf("%%ZSSS_OBS:99998:0:50:%d:31.19670:121.33500:0", account+1))
					} else {
						c.send(fmt.Sprintf("@N:CCA101:1200:%d:31.19670:121.33500:1000:0:0:0", account+1))
					}
					expectRatingError(t, c, ErrCodeRequestedLevelTooHigh)
					h.waitForOnline(t, 0)
					if station.NetworkRating != RatingObserver || station.LatLon() != [2]float64{} {
						t.Fatal("rejected position changed rating or coordinates")
					}
				})
			}
		}
	})
}

func TestObserverControllerOperationsAllModes(t *testing.T) {
	forRatingModes(t, func(t *testing.T, opts harnessOptions) {
		h := newHarness(t, opts)
		pilot := h.loginPilot(t, "CCA101", "1000", 100)
		obs := h.loginATC(t, "ZSSS_CTR", "1000", 1)
		h.waitForOnline(t, 2)
		target := clientByCallsign(t, h.server, pilot.callsign)
		operations := []string{
			"$AMZSSS_CTR:*A:CCA101:I:B738:400:ZSSS:1000:1000:35000:ZBAA:2:0:3:0:ZSPD:RMK:TEST",
			"$CQZSSS_CTR:@94835:IT:CCA101", "$CQZSSS_CTR:@94835:HT:CCA101",
			"$CQZSSS_CTR:@94835:DR:CCA101",
			"$CQZSSS_CTR:@94835:BC:CCA101:1234",
			"#PCZSSS_CTR:CCA101:CCP:BC:CCA101:1234",
			"#PCZSSS_CTR:CCA101:CCP:HC:CCA101",
			"$HOZSSS_CTR:CCA101:CCA101", "$HAZSSS_CTR:CCA101:CCA101",
		}
		for _, positioned := range []bool{false, true} {
			if positioned {
				obs.send("%ZSSS_CTR:99998:0:50:1:31.19670:121.33500:0")
			}
			for _, packet := range operations {
				obs.send(packet)
				expectRatingError(t, obs, ErrCodeInvalidControl)
			}
		}
		// A rejected declaration followed immediately by mutations must not
		// apply the facility or process buffered operations as a controller.
		obs.sendRaw([]byte("%ZSSS_CTR:28550:6:50:1:31.19670:121.33500:0\r\n" +
			strings.Join(operations, "\r\n") + "\r\n"))
		expectRatingError(t, obs, ErrCodeInvalidPositionForRating)
		h.waitForOnline(t, 1)
		if len(h.server.tracks.states()) != 0 || target.FlightPlanData() != nil || target.BeaconCode() != "0" {
			t.Fatal("observer changed aircraft state")
		}
		pilot.expectNone("$AMZSSS_CTR", 20*time.Millisecond)
	})
}

func TestWithinAccountRatingsRemainAllowedAllModes(t *testing.T) {
	forRatingModes(t, func(t *testing.T, opts harnessOptions) {
		h := newHarness(t, opts)
		obs := h.loginATC(t, "ZSSS_OBS", "1000", 1)
		ctr := h.loginATC(t, "ZSSS_TWR", "1001", 3)
		pilot := h.loginPilot(t, "CCA101", "1000", 100)
		h.waitForOnline(t, 3)
		obs.send("%ZSSS_OBS:99998:0:50:1:31.19670:121.33500:0")
		ctr.send("%ZSSS_TWR:28550:4:50:3:31.19670:121.33500:0")
		ctr.send("$CQZSSS_TWR:@94835:IT:CCA101")
		// A server ping is processed after the claim and acts as a barrier.
		ctr.send("$PIZSSS_TWR:SERVER:rating-check")
		ctr.expect("$POSERVER")
		if testTrackOwners(h.server)[pilot.callsign] != ctr.callsign {
			t.Fatal("legitimate controller could not claim a track")
		}
		if station := clientByCallsign(t, h.server, obs.callsign); station.FacilityType() != FacilityObserver {
			t.Fatal("valid observer position changed role")
		}
		if station := clientByCallsign(t, h.server, ctr.callsign); station.MaxRating != RatingController1 || station.NetworkRating != RatingStudent2 {
			t.Fatal("account rating and chosen lower login rating were conflated")
		}
	})
}

func TestActiveControllerUsesAuthenticatedRating(t *testing.T) {
	client := &Client{LoginData: LoginData{
		IsATC: true, MaxRating: RatingObserver, NetworkRating: RatingSupervisor,
	}}
	client.setFacilityType(FacilityCentre)
	if client.activeController() {
		t.Fatal("client claims overrode the authenticated OBS account")
	}
	client.MaxRating = RatingController1
	if !client.activeController() {
		t.Fatal("legitimate controller account was refused")
	}
}

func TestControllerRatingFloorKeepsConfiguredEnforcement(t *testing.T) {
	forRatingModes(t, func(t *testing.T, opts harnessOptions) {
		opts.users = map[string]*testIdentity{
			"1000": {Username: "1000", Password: "secret", Rating: 1},
			"1001": {Username: "1001", Password: "secret", Rating: 2},
		}
		h := newHarness(t, opts)
		ctr := h.loginATC(t, "ZSSS_CTR", "1001", 2)
		pilot := h.loginPilot(t, "CCA101", "1000", 100)
		h.waitForOnline(t, 2)
		ctr.send("%ZSSS_CTR:28550:6:50:2:31.19670:121.33500:0")
		if opts.enforcement == config.EnforcementStrict {
			expectRatingError(t, ctr, ErrCodeInvalidPositionForRating)
			h.waitForOnline(t, 1)
			return
		}
		ctr.send("$CQZSSS_CTR:@94835:IT:CCA101")
		ctr.send("$PIZSSS_CTR:SERVER:rating-floor-check")
		ctr.expect("$POSERVER")
		if testTrackOwners(h.server)[pilot.callsign] != ctr.callsign {
			t.Fatal("optional rating floor became a mandatory controller restriction")
		}
	})
}
