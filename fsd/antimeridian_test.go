package fsd

import (
	"fmt"
	"testing"
	"time"
)

func TestAntimeridianPilotPositionDelivery(t *testing.T) {
	for _, kind := range []string{"@", "^", "#SL", "#ST"} {
		for _, tc := range []struct {
			name        string
			from, to    float64
			rangeNM     float64
			wantPackets int
		}{
			{"east_to_west", 179.9, -179.9, 40, 1},
			{"west_to_east", -179.9, 179.9, 40, 1},
			{"east_endpoint", 180, -180, 1, 1},
			{"west_endpoint", -180, 180, 1, 1},
			{"larger_wrapped_receiver_west", 178.5, -179.5, 200, 1},
			{"larger_wrapped_receiver_east", -178.5, 179.5, 200, 1},
			{"outside_receiver_radius", 179, -179.9, 40, 0},
		} {
			t.Run(kind+"/"+tc.name, func(t *testing.T) {
				s := &Server{postOffice: newPostOffice(8)}
				pilot := newIdleClient(t, "TEST1")
				receiver := newIdleClient(t, "TEST2")
				pilot.MaxRating = RatingObserver
				pilot.ProtoRevision = ProtoRevisionVelocity
				receiver.ProtoRevision = ProtoRevisionVelocity
				receiver.IsATC = true
				for _, client := range []*Client{pilot, receiver} {
					if err := s.postOffice.register(client); err != nil {
						t.Fatal(err)
					}
				}
				s.postOffice.updatePosition(receiver, [2]float64{0, tc.to}, tc.rangeNM*MetresPerNauticalMile)
				s.postOffice.updatePosition(pilot, [2]float64{0, tc.from}, 40*MetresPerNauticalMile)
				packet := sendVisibilityPosition(s, pilot, kind, [2]float64{0, tc.from})
				assertSpatialPacketCount(t, receiver, packet, tc.wantPackets)
			})
		}
	}
}

func assertSpatialPacketCount(t *testing.T, receiver *Client, packet string, want int) {
	t.Helper()
	got := 0
	for len(receiver.sendCh) > 0 {
		if received := <-receiver.sendCh; received == packet {
			got++
		}
	}
	if got != want {
		t.Fatalf("%s received position %q %d times, want %d", receiver.Callsign, packet, got, want)
	}
}

func TestAntimeridianPilotPositionDeliveryOverTCP(t *testing.T) {
	for _, tc := range []struct {
		name     string
		from, to float64
	}{
		{"east_to_west", 179.9, -179.9},
		{"west_to_east", -179.9, 179.9},
	} {
		t.Run(tc.name, func(t *testing.T) {
			h := newHarness(t, harnessOptions{})
			receiver := h.loginATC(t, "TEST_CTR", "1001", 5)
			pilot := h.loginPilot(t, "TEST1", "1000", ProtoRevisionClassic)
			receiver.send(fmt.Sprintf("%%TEST_CTR:28550:4:40:5:0:%.6f:0", tc.to))
			receiver.send("$PITEST_CTR:SERVER:positioned")
			receiver.expect("$POSERVER:TEST_CTR:positioned")

			packet := fmt.Sprintf("@N:TEST1:1200:1:0:%.6f:1000:0:0:0", tc.from)
			pilot.send(packet)
			if got := receiver.expect("@N:TEST1:"); got != packet {
				t.Fatalf("position = %q, want %q", got, packet)
			}
			pilot.send("$PITEST1:SERVER:positioned")
			pilot.expect("$POSERVER:TEST1:positioned")
			receiver.expectNone("@N:TEST1:", 100*time.Millisecond)
		})
	}
}
