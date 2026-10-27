package fsd

import (
	"strings"
	"testing"
)

func TestControllerLoginIsNotAnnouncedBeforePosition(t *testing.T) {
	s := &Server{postOffice: newPostOffice(16)}
	controller := newIdleClient(t, "ZBAA_CTR")
	controller.IsATC = true
	remote := newIdleClient(t, "KJFK_CTR")
	remote.IsATC = true
	for _, c := range []*Client{controller, remote} {
		if err := s.postOffice.register(c); err != nil {
			t.Fatal(err)
		}
	}
	s.postOffice.updatePosition(remote, [2]float64{40.64, -73.78}, 200*MetresPerNauticalMile)
	remote.touchPosition()

	s.broadcastAddPacket(controller)
	select {
	case packet := <-remote.sendCh:
		t.Fatalf("unpositioned controller leaked to remote facility: %q", packet)
	default:
	}
}

func TestControllerPresenceFollowsVisibility(t *testing.T) {
	s := &Server{postOffice: newPostOffice(16)}
	controller := newIdleClient(t, "ZBAA_CTR")
	controller.IsATC = true
	near := newIdleClient(t, "ZBAA_APP")
	near.IsATC = true
	far := newIdleClient(t, "KJFK_CTR")
	far.IsATC = true
	for _, c := range []*Client{controller, near, far} {
		if err := s.postOffice.register(c); err != nil {
			t.Fatal(err)
		}
	}
	s.postOffice.updatePosition(near, [2]float64{40.08, 116.60}, 200*MetresPerNauticalMile)
	near.touchPosition()
	s.postOffice.updatePosition(far, [2]float64{40.64, -73.78}, 200*MetresPerNauticalMile)
	far.touchPosition()
	s.postOffice.updatePosition(controller, [2]float64{40.08, 116.60}, 200*MetresPerNauticalMile)
	controller.touchPosition()

	s.broadcastAddPacket(controller)
	assertQueuedPrefix(t, near, "#AAZBAA_CTR:")
	assertQueuedPrefix(t, controller, "#AAZBAA_APP:")
	assertNoQueuedPackets(t, far)

	// Repeated position updates must not reset an already-known controller.
	s.broadcastAddPacket(controller)
	assertNoQueuedPackets(t, near)
	assertNoQueuedPackets(t, controller)

	s.postOffice.updatePosition(controller, [2]float64{40.64, -73.78}, 200*MetresPerNauticalMile)
	s.broadcastAddPacket(controller)
	assertQueuedPrefix(t, near, "#DAZBAA_CTR:")
	assertQueuedPrefix(t, far, "#AAZBAA_CTR:")
	seen := map[string]bool{}
	for len(controller.sendCh) > 0 {
		seen[<-controller.sendCh] = true
	}
	if !seen["#DAZBAA_APP:1000\r\n"] || !seen["#AAKJFK_CTR:SERVER::1000::0:100\r\n"] {
		t.Fatalf("moving controller visibility = %v", seen)
	}

	s.broadcastDeletePacket(controller)
	assertQueuedPrefix(t, far, "#DAZBAA_CTR:")
	assertNoQueuedPackets(t, near)
}

func TestPositionedPilotReceivesNearbyControllerRoster(t *testing.T) {
	h := newHarness(t, harnessOptions{})
	controller := h.loginATC(t, "ZSSS_CTR", "1001", 5)
	positionATC(controller)
	controller.send("$PIZSSS_CTR:SERVER:positioned")
	controller.expect("$POSERVER:ZSSS_CTR:positioned")

	pilot := h.loginPilot(t, "CCA101", "1000", ProtoRevisionClassic)
	pilot.send("@N:CCA101:1200:1:31.19670:121.33500:1000:0:0:0")
	announcement := pilot.expect("#AAZSSS_CTR:")
	if strings.Contains(announcement, "secret") {
		t.Fatalf("controller announcement leaked credentials: %q", announcement)
	}
	pilot.expect("%ZSSS_CTR:28550:4:50:5:31.19670:121.33500:0")
}

func assertQueuedPrefix(t *testing.T, client *Client, prefix string) {
	t.Helper()
	select {
	case packet := <-client.sendCh:
		if !strings.HasPrefix(packet, prefix) {
			t.Fatalf("%s received %q, want prefix %q", client.Callsign, packet, prefix)
		}
	default:
		t.Fatalf("%s did not receive %q", client.Callsign, prefix)
	}
}

func assertNoQueuedPackets(t *testing.T, client *Client) {
	t.Helper()
	select {
	case packet := <-client.sendCh:
		t.Fatalf("%s received unexpected packet %q", client.Callsign, packet)
	default:
	}
}
