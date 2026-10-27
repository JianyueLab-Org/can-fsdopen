package fsd

import (
	"testing"
	"time"
)

func TestFastPositionHandlersPreserveCoordinateTime(t *testing.T) {
	c := newIdleClient(t, "CCA201")
	position, at := c.LatLonTime()
	if position != [2]float64{} || !at.IsZero() {
		t.Fatalf("login position = %v at %v, want unset coordinates", position, at)
	}
	// A fixed authoritative sample makes preservation independent of sleeps.
	want := time.Unix(1_700_000_000, 123_000_000)
	c.coordinates.Store(&coordinatePosition{latLon: [2]float64{35.55, 139.78}, at: want})
	s := &Server{postOffice: newPostOffice(8)}
	for _, packet := range []string{
		"^CCA201:36:140:2000:0:0:0:0:0:0:0:0:0",
		"#SLCCA201:36:140:2000:0:0:0:0:0:0:0:0:0",
		"#STCCA201:36:140:2000:0:0:0:0:0:0:0:0:0",
	} {
		c.lastPosition.Store(want.UnixNano())
		s.handleFastPilotPosition(c, []byte(packet))
		position, at = c.LatLonTime()
		if position != [2]float64{35.55, 139.78} || !at.Equal(want) {
			t.Fatalf("%s renewed coordinates: %v at %v", packet, position, at)
		}
		if !c.LastPosition().After(want) {
			t.Fatalf("%s did not preserve generic activity refresh", packet)
		}
	}
}
