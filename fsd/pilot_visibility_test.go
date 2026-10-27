package fsd

import (
	"fmt"
	"strings"
	"testing"
)

// These fixtures are near the equator: 0.816 degrees is about 49 NM,
// 1.166 degrees is about 70 NM, and (0.7, 0.7) is about 59 NM away.
func TestPilotPositionsHonorReceiverVisibility(t *testing.T) {
	for _, kind := range []string{"@", "^", "#SL", "#ST"} {
		for _, tc := range []struct {
			name     string
			position [2]float64
			rangeNM  float64
			revision int
			isATC    bool
			want     bool
		}{
			{"inside", [2]float64{0.816, 0}, 50, 101, true, true},
			{"outside", [2]float64{1.166, 0}, 50, 101, true, false},
			{"box_corner", [2]float64{0.7, 0.7}, 50, 101, true, false},
			{"larger_receiver", [2]float64{1.166, 0}, 100, 101, true, true},
			{"classic_receiver", [2]float64{0.816, 0}, 50, 100, true, kind == "@"},
			{"pilot_receiver", [2]float64{0.75, 0}, 40, 101, false, false},
		} {
			t.Run(kind+"/"+tc.name, func(t *testing.T) {
				s := &Server{postOffice: newPostOffice(8)}
				pilot := newIdleClient(t, "CCA101")
				pilot.MaxRating = 1
				receiver := newIdleClient(t, "ZSSS_OBS")
				receiver.IsATC = tc.isATC
				receiver.ProtoRevision = tc.revision
				for _, c := range []*Client{pilot, receiver} {
					if err := s.postOffice.register(c); err != nil {
						t.Fatal(err)
					}
				}
				s.postOffice.updatePosition(receiver, [2]float64{0, 0}, tc.rangeNM*MetresPerNauticalMile)
				s.postOffice.updatePosition(pilot, tc.position, 40*MetresPerNauticalMile)
				packet := sendVisibilityPosition(s, pilot, kind, tc.position)
				assertPositionDelivery(t, receiver, packet, tc.want)
			})
		}
	}
}

func TestPilotPositionsHonorReceiverRangeChanges(t *testing.T) {
	for _, kind := range []string{"@", "^", "#SL", "#ST"} {
		t.Run(kind, func(t *testing.T) {
			s := &Server{postOffice: newPostOffice(8)}
			pilot := newIdleClient(t, "CCA101")
			pilot.MaxRating = 1
			receiver := newIdleClient(t, "ZSSS_OBS")
			receiver.IsATC = true
			receiver.ProtoRevision = 101
			for _, c := range []*Client{pilot, receiver} {
				if err := s.postOffice.register(c); err != nil {
					t.Fatal(err)
				}
			}
			position := [2]float64{1.166, 0}
			s.postOffice.updatePosition(pilot, position, 40*MetresPerNauticalMile)
			for _, step := range []struct {
				rangeNM float64
				want    bool
			}{{100, true}, {50, false}, {100, true}} {
				s.postOffice.updatePosition(receiver, [2]float64{0, 0}, step.rangeNM*MetresPerNauticalMile)
				packet := sendVisibilityPosition(s, pilot, kind, position)
				assertPositionDelivery(t, receiver, packet, step.want)
			}
		})
	}
}

func sendVisibilityPosition(s *Server, pilot *Client, kind string, position [2]float64) string {
	var packet string
	if kind == "@" {
		packet = fmt.Sprintf("@N:%s:1200:1:%.6f:%.6f:1000:0:0:0\r\n", pilot.Callsign, position[0], position[1])
		s.handlePilotPosition(pilot, []byte(packet))
	} else {
		packet = fmt.Sprintf("%s%s:%.6f:%.6f:1000:1000:0:0", kind, pilot.Callsign, position[0], position[1])
		if kind != "#ST" {
			packet += ":0:0:0:0:0:0"
		}
		packet += "\r\n"
		s.handleFastPilotPosition(pilot, []byte(packet))
	}
	return packet
}

func assertPositionDelivery(t *testing.T, receiver *Client, packet string, want bool) {
	t.Helper()
	got := false
	for len(receiver.sendCh) > 0 {
		if received := <-receiver.sendCh; received == packet {
			got = true
		}
	}
	if got != want {
		t.Fatalf("position %s delivered = %v, want %v", strings.TrimSpace(packet), got, want)
	}
}
