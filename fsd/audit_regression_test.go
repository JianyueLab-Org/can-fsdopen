package fsd

import (
	"strconv"
	"strings"
	"testing"
)

func TestFlightPlanAcknowledgementIsConsumed(t *testing.T) {
	s := &Server{postOffice: newPostOffice(16)}
	controller := newIdleClient(t, "ZSSS_CTR")
	controller.IsATC = true
	peer := newIdleClient(t, "ZBAA_CTR")
	peer.IsATC = true
	for _, client := range []*Client{controller, peer} {
		if err := s.postOffice.register(client); err != nil {
			t.Fatal(err)
		}
	}

	s.dispatch(controller, PacketTextMessage, []byte("#TMZSSS_CTR:FP:CCA101\r\n"))
	assertNoQueuedPackets(t, controller)
	assertNoQueuedPackets(t, peer)

	s.dispatch(controller, PacketPing, []byte("$PIZSSS_CTR:SERVER:123\r\n"))
	assertQueuedPrefix(t, controller, "$POSERVER:ZSSS_CTR:123\r\n")
	assertNoQueuedPackets(t, controller)
}

func TestTextToMissingCallsignStillReturnsError(t *testing.T) {
	s := &Server{postOffice: newPostOffice(16)}
	controller := newIdleClient(t, "ZSSS_CTR")
	controller.IsATC = true

	s.dispatch(controller, PacketTextMessage, []byte("#TMZSSS_CTR:MISSING:hello\r\n"))
	select {
	case packet := <-controller.sendCh:
		if !strings.HasPrefix(packet, "$ER") || FieldString([]byte(packet), 1) != "ZSSS_CTR" || FieldString([]byte(packet), 2) != strconv.Itoa(ErrCodeNoSuchCallsign) {
			t.Fatalf("missing recipient returned the wrong FSD error: %q", packet)
		}
	default:
		t.Fatal("missing recipient did not return an FSD error")
	}
	assertNoQueuedPackets(t, controller)
}
