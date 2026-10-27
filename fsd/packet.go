package fsd

import (
	"bufio"
	"bytes"
	"math"
	"strconv"
)

// PacketType identifies an FSD packet by its prefix.
type PacketType int

const (
	PacketUnknown PacketType = iota
	PacketTextMessage
	PacketPilotPosition
	PacketFastPilotPosition
	PacketFastPilotPositionSlow
	PacketFastPilotPositionStopped
	PacketATCPosition
	PacketDeleteATC
	PacketDeletePilot
	PacketClientQuery
	PacketClientQueryResponse
	PacketProController
	PacketSquawkbox
	PacketMetarRequest
	PacketWeatherRequest
	PacketKillRequest
	PacketAuthChallenge
	PacketHandoffRequest
	PacketHandoffAccept
	PacketFlightPlan
	PacketFlightPlanAmendment
	PacketPing
	PacketPong
)

// MaxPacketSize bounds a single line.
//
// The Python server read fixed 1024-byte chunks and discarded whatever did not
// end on a delimiter, so long flight plan routes and multi-line ATIS text were
// silently truncated or dropped. Framing here is delimiter-based with a
// generous ceiling; only a packet genuinely larger than this is refused, and
// it is refused loudly.
const MaxPacketSize = 16 * 1024

// ScanPackets is a bufio.SplitFunc that frames FSD packets on CRLF.
//
// A bare LF is also accepted as a terminator: some clients and most test
// harnesses omit the carriage return, and the previous server tolerated it by
// accident of using split("\r\n") on decoded text.
//
// Partial data is retained until a full line arrives, which is the whole point
// of this function — packets that straddle a TCP segment boundary must not be
// lost.
func ScanPackets(data []byte, atEOF bool) (advance int, token []byte, err error) {
	if atEOF && len(data) == 0 {
		return 0, nil, nil
	}

	if i := bytes.IndexByte(data, '\n'); i >= 0 {
		line := data[:i]
		// Trim the CR of a CRLF pair.
		line = bytes.TrimSuffix(line, []byte("\r"))
		return i + 1, line, nil
	}

	if atEOF {
		return len(data), bytes.TrimSuffix(data, []byte("\r")), nil
	}

	// Ask for more data.
	return 0, nil, nil
}

// NewScanner returns a scanner that frames FSD packets from r.
func NewScanner(r *bufio.Reader) *bufio.Scanner {
	scanner := bufio.NewScanner(r)
	scanner.Buffer(make([]byte, 4096), MaxPacketSize)
	scanner.Split(ScanPackets)
	return scanner
}

// GetPacketType parses a packet's type from its prefix.
func GetPacketType(packet []byte) PacketType {
	if len(packet) == 0 {
		return PacketUnknown
	}

	switch packet[0] {
	case '^':
		return PacketFastPilotPosition
	case '@':
		return PacketPilotPosition
	case '%':
		return PacketATCPosition
	}

	if len(packet) < 3 {
		return PacketUnknown
	}

	switch string(packet[:3]) {
	case "#DA":
		return PacketDeleteATC
	case "#DP":
		return PacketDeletePilot
	case "#TM":
		return PacketTextMessage
	case "#SL":
		return PacketFastPilotPositionSlow
	case "#ST":
		return PacketFastPilotPositionStopped
	case "#PC":
		return PacketProController
	case "#SB":
		return PacketSquawkbox
	case "#WX":
		return PacketWeatherRequest
	case "$CQ":
		return PacketClientQuery
	case "$CR":
		return PacketClientQueryResponse
	case "$AX":
		return PacketMetarRequest
	case "$!!":
		return PacketKillRequest
	case "$ZC":
		return PacketAuthChallenge
	case "$HO":
		return PacketHandoffRequest
	case "$HA":
		return PacketHandoffAccept
	case "$FP":
		return PacketFlightPlan
	case "$AM":
		return PacketFlightPlanAmendment
	case "$PI":
		return PacketPing
	case "$PO":
		return PacketPong
	default:
		return PacketUnknown
	}
}

// PacketPrefix returns the wire prefix for a packet type.
func PacketPrefix(t PacketType) string {
	switch t {
	case PacketFastPilotPosition:
		return "^"
	case PacketPilotPosition:
		return "@"
	case PacketATCPosition:
		return "%"
	case PacketDeleteATC:
		return "#DA"
	case PacketDeletePilot:
		return "#DP"
	case PacketTextMessage:
		return "#TM"
	case PacketFastPilotPositionSlow:
		return "#SL"
	case PacketFastPilotPositionStopped:
		return "#ST"
	case PacketProController:
		return "#PC"
	case PacketSquawkbox:
		return "#SB"
	case PacketWeatherRequest:
		return "#WX"
	case PacketClientQuery:
		return "$CQ"
	case PacketClientQueryResponse:
		return "$CR"
	case PacketMetarRequest:
		return "$AX"
	case PacketKillRequest:
		return "$!!"
	case PacketAuthChallenge:
		return "$ZC"
	case PacketHandoffRequest:
		return "$HO"
	case PacketHandoffAccept:
		return "$HA"
	case PacketFlightPlan:
		return "$FP"
	case PacketFlightPlanAmendment:
		return "$AM"
	case PacketPing:
		return "$PI"
	case PacketPong:
		return "$PO"
	default:
		return ""
	}
}

// minFields is the smallest field count a packet type can carry and still be
// interpretable. -1 means the type is unknown.
func minFields(t PacketType) int {
	switch t {
	case PacketPilotPosition:
		return 9
	case PacketFastPilotPosition, PacketFastPilotPositionSlow:
		return 13
	case PacketFastPilotPositionStopped:
		return 7
	case PacketATCPosition:
		return 7
	case PacketDeleteATC, PacketDeletePilot:
		return 1
	case PacketTextMessage:
		return 3
	case PacketProController:
		return 4
	case PacketSquawkbox:
		return 3
	case PacketClientQuery, PacketClientQueryResponse:
		return 3
	case PacketMetarRequest:
		return 4
	case PacketWeatherRequest:
		return 3
	case PacketKillRequest:
		return 2
	case PacketAuthChallenge:
		return 3
	case PacketHandoffRequest, PacketHandoffAccept:
		return 3
	case PacketFlightPlan:
		return 17
	case PacketFlightPlanAmendment:
		return 18
	case PacketPing, PacketPong:
		return 3
	default:
		return -1
	}
}

// sourceCallsignField is the field index holding the sender's callsign.
//
// Pilot position packets lead with the transponder mode, so the callsign is
// the second field; every other type puts it first.
func sourceCallsignField(t PacketType) int {
	if t == PacketPilotPosition {
		return 1
	}
	return 0
}

// CountFields returns the number of colon-separated fields in a packet.
func CountFields(packet []byte) int {
	return bytes.Count(packet, []byte(":")) + 1
}

// Field returns the field at index, or an empty slice if it does not exist.
// The returned slice aliases packet.
func Field(packet []byte, index int) []byte {
	for range index {
		i := bytes.IndexByte(packet, ':')
		if i < 0 {
			return nil
		}
		packet = packet[i+1:]
	}

	if i := bytes.IndexByte(packet, ':'); i >= 0 {
		packet = packet[:i]
	}

	return bytes.TrimSuffix(packet, []byte("\r\n"))
}

// FieldString is Field with a string result.
func FieldString(packet []byte, index int) string {
	return string(Field(packet, index))
}

// SourceCallsign returns the sender's callsign with the packet prefix removed.
func SourceCallsign(packet []byte, t PacketType) []byte {
	callsign, _ := bytes.CutPrefix(Field(packet, sourceCallsignField(t)), []byte(PacketPrefix(t)))
	return callsign
}

// ParseLatLon reads two decimal degree values from the given field indices.
func ParseLatLon(packet []byte, latIndex, lonIndex int) (lat, lon float64, ok bool) {
	lat, err := strconv.ParseFloat(string(Field(packet, latIndex)), 64)
	if err != nil {
		return 0, 0, false
	}
	lon, err = strconv.ParseFloat(string(Field(packet, lonIndex)), 64)
	if err != nil {
		return 0, 0, false
	}
	if math.IsNaN(lat) || math.IsInf(lat, 0) || math.IsNaN(lon) || math.IsInf(lon, 0) {
		return 0, 0, false
	}

	// Reject values outside the globe rather than letting them poison the
	// spatial index, where a bounding box built from them matches everything.
	if lat < -90 || lat > 90 || lon < -180 || lon > 180 {
		return 0, 0, false
	}

	return lat, lon, true
}

// MetresPerNauticalMile converts the protocol's nautical miles to metres,
// which is what the post office indexes in.
const MetresPerNauticalMile = 1852.0

// ParseVisRange reads a visibility range in nautical miles and returns metres.
func ParseVisRange(packet []byte, index int) (metres float64, ok bool) {
	nm, err := strconv.ParseFloat(string(Field(packet, index)), 64)
	if err != nil || math.IsNaN(nm) || math.IsInf(nm, 0) || nm < 0 {
		return 0, false
	}

	// A misreported range should not turn one station into a global
	// broadcaster; 1500 NM comfortably exceeds any real centre sector.
	const maxVisRangeNM = 1500
	if nm > maxVisRangeNM {
		nm = maxVisRangeNM
	}

	return nm * MetresPerNauticalMile, true
}

// PitchBankHeading decodes the packed attitude field: pitch in bits 22–31,
// bank in bits 12–21, and heading in bits 2–11, at 360/1024 degrees per unit.
func PitchBankHeading(packed uint32) (pitch, bank, heading float64) {
	const ratio = 360.0 / 1024.0
	const mask = 0x3FF

	pitch = normaliseSigned(float64(packed>>22&mask) * ratio)
	bank = normaliseSigned(float64(packed>>12&mask) * ratio)

	heading = float64(packed>>2&mask) * ratio
	for heading < 0 {
		heading += 360
	}
	for heading >= 360 {
		heading -= 360
	}

	return pitch, bank, heading
}

// normaliseSigned folds 0..360 onto -180..180 without negating the angle:
// nose-up pitch and right bank remain positive.
func normaliseSigned(v float64) float64 {
	if v > 180 {
		v -= 360
	} else if v <= -180 {
		v += 360
	}
	return v
}

var reservedCallsigns = map[string]struct{}{
	"SERVER": {},
	"CLIENT": {},
	"FP":     {},
}

// Callsign length bounds.
//
// The upper bound used to be 10, matching VATSIM. That is one character short
// of the split-ATIS names vATIS generates: an airport running separate
// departure and arrival broadcasts uses ZSPD_D_ATIS and ZSPD_A_ATIS, both 11
// characters, so those stations could not log in at all. 12 covers them with a
// little room, and the field stays short enough that a callsign can never
// dominate a packet.
const (
	MinCallsignLength = 2
	MaxCallsignLength = 12
)

// IsValidCallsign reports whether a client may register under this callsign.
//
// Length, character set and the reserved names are all checked. The Python
// server accepted anything, so a client could register as SERVER and
// impersonate system messages.
func IsValidCallsign(callsign []byte) bool {
	if len(callsign) < MinCallsignLength || len(callsign) > MaxCallsignLength {
		return false
	}

	for _, b := range callsign {
		switch {
		case b >= '0' && b <= '9',
			b >= 'A' && b <= 'Z',
			b == '-', b == '_':
		default:
			return false
		}
	}

	_, reserved := reservedCallsigns[string(callsign)]
	return !reserved
}

// IsATISCallsign reports whether a callsign denotes an ATIS station.
func IsATISCallsign(callsign string) bool {
	i := lastIndexByte(callsign, '_')
	return i >= 0 && callsign[i+1:] == "ATIS"
}

func lastIndexByte(s string, b byte) int {
	for i := len(s) - 1; i >= 0; i-- {
		if s[i] == b {
			return i
		}
	}
	return -1
}
