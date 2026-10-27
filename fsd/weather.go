package fsd

import (
	"fmt"
	"strings"
)

// Weather profiles.
//
// A client sends #WX to request the wind, cloud and temperature layers for an
// airport and expects #WD, #CD and #TD in reply. The Python server did not
// implement any of this, so clients that ask (notably older pilot clients on
// protocol 100) waited for a response that never came.
//
// The layers served here are a standard-atmosphere profile rather than real
// forecast data: it is well-formed, deterministic, and does not contradict the
// METAR a client fetches separately. Wiring a real upper-air source in later
// only means replacing buildWindLayers.

// handleWeatherRequest serves #WX.
//
// Layout: #WXcallsign:SERVER:ICAO
func (s *Server) handleWeatherRequest(client *Client, packet []byte) {
	if FieldString(packet, 1) != recipientServer {
		return
	}

	icao := strings.ToUpper(strings.TrimSpace(FieldString(packet, 2)))
	if !isValidICAO(icao) {
		client.SendError(ErrCodeNoWeatherProfile, "Invalid ICAO code")
		return
	}

	client.Send(buildWindPacket(client.Callsign))
	client.Send(buildCloudPacket(client.Callsign))
	client.Send(buildTemperaturePacket(client.Callsign))
}

// windLayer is one entry of a #WD packet.
type windLayer struct {
	ceiling    int // feet
	floor      int // feet
	direction  int // degrees true
	speed      int // knots
	gusting    int
	turbulence int
}

// standardWindLayers is a calm, shear-free profile.
func standardWindLayers() []windLayer {
	return []windLayer{
		{ceiling: 10400, floor: 2500, direction: 0, speed: 0},
		{ceiling: 22600, floor: 10400, direction: 0, speed: 0},
		{ceiling: 90000, floor: 22700, direction: 0, speed: 0},
		{ceiling: -1, floor: -1},
	}
}

// buildWindPacket renders #WD.
//
// Each layer contributes six fields: ceiling, floor, direction, speed,
// gusting and turbulence.
func buildWindPacket(callsign string) string {
	var b strings.Builder
	b.Grow(160)
	b.WriteString("#WDSERVER:")
	b.WriteString(callsign)

	for _, layer := range standardWindLayers() {
		fmt.Fprintf(&b, ":%d:%d:%d:%d:%d:%d",
			layer.ceiling, layer.floor, layer.direction,
			layer.speed, layer.gusting, layer.turbulence)
	}

	b.WriteString("\r\n")
	return b.String()
}

// buildCloudPacket renders #CD: three cloud layers plus a visibility figure.
// All layers are empty, which is what a clear sky looks like on the wire.
func buildCloudPacket(callsign string) string {
	var b strings.Builder
	b.Grow(128)
	b.WriteString("#CDSERVER:")
	b.WriteString(callsign)

	// Two standard layers and one storm layer, each: ceiling, floor,
	// coverage, icing, turbulence.
	for range 3 {
		b.WriteString(":-1:-1:0:0:0")
	}

	// Visibility in statute miles.
	b.WriteString(":10.00\r\n")
	return b.String()
}

// buildTemperaturePacket renders #TD: four temperature layers and a barometric
// pressure in hundredths of an inch of mercury.
func buildTemperaturePacket(callsign string) string {
	// International Standard Atmosphere: 15 degrees at sea level, falling
	// about 2 degrees per thousand feet through the troposphere.
	layers := [][2]int{
		{100, 15},
		{10000, -5},
		{18000, -21},
		{35000, -54},
	}

	var b strings.Builder
	b.Grow(96)
	b.WriteString("#TDSERVER:")
	b.WriteString(callsign)

	for _, layer := range layers {
		fmt.Fprintf(&b, ":%d:%d", layer[0], layer[1])
	}

	// 29.92 inHg, the standard altimeter setting.
	b.WriteString(":2992\r\n")
	return b.String()
}
