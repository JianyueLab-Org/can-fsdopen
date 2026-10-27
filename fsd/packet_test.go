package fsd

import (
	"bufio"
	"bytes"
	"io"
	"math"
	"strings"
	"testing"
)

func TestCountFields(t *testing.T) {
	tests := []struct {
		packet string
		want   int
	}{
		{"", 1},
		{"abc", 1},
		{"a:b", 2},
		{"a:b:c", 3},
		{"a:b:", 3},
		{":a:b", 3},
		{":", 2},
	}

	for _, tt := range tests {
		if got := CountFields([]byte(tt.packet)); got != tt.want {
			t.Errorf("CountFields(%q) = %d, want %d", tt.packet, got, tt.want)
		}
	}
}

func TestField(t *testing.T) {
	tests := []struct {
		packet string
		index  int
		want   string
	}{
		{"a:b:c", 0, "a"},
		{"a:b:c", 1, "b"},
		{"a:b:c", 2, "c"},
		{"a:", 0, "a"},
		{"a:", 1, ""},
		{"a:", 2, ""},
		{":a", 0, ""},
		{":a", 1, "a"},
		{"", 0, ""},
		{"", 1, ""},
		{"a", 0, "a"},
		// Reading past the end must yield nothing rather than repeating the
		// last field, which would make a short packet look well-formed.
		{"a", 1, ""},
		{"a", 5, ""},
	}

	for _, tt := range tests {
		if got := FieldString([]byte(tt.packet), tt.index); got != tt.want {
			t.Errorf("Field(%q, %d) = %q, want %q", tt.packet, tt.index, got, tt.want)
		}
	}
}

// slowReader hands out data in fixed-size chunks, simulating a stream that
// arrives in TCP segments unrelated to packet boundaries.
type slowReader struct {
	data      []byte
	chunkSize int
	pos       int
}

func (r *slowReader) Read(p []byte) (int, error) {
	if r.pos >= len(r.data) {
		return 0, io.EOF
	}
	end := min(r.pos+r.chunkSize, len(r.data))
	n := copy(p, r.data[r.pos:end])
	r.pos += n
	return n, nil
}

// TestScannerReassemblesSplitPackets is the regression test for the defect
// that motivated this rewrite.
//
// The Python server did recv(1024), split the result on "\r\n" and discarded
// the trailing element. Any packet straddling a read boundary was lost and any
// packet over 1024 bytes was truncated. Framing must be independent of how the
// bytes happen to arrive.
func TestScannerReassemblesSplitPackets(t *testing.T) {
	longRoute := strings.Repeat("WAYPT ", 900) // well over the old 1024 limit
	packets := []string{
		"$IDN172SP:SERVER:88e4:vPilot:3:8:123456:-582057156:6d6973746176",
		"#APN172SP:SERVER:1000:secret:1:101:2:John Doe",
		"@N:N172SP:2000:1:40.65906:-73.79891:26:0:4290776072:359",
		"$FPN172SP:SERVER:I:A320:420:ZSPD:1200:1200:FL350:ZBAA:0:0:0:0::/V/:" + longRoute,
		"#TMN172SP:@22800:short one",
	}

	var raw bytes.Buffer
	for _, p := range packets {
		raw.WriteString(p)
		raw.WriteString("\r\n")
	}

	// Chunk sizes chosen to land mid-packet, including one byte at a time.
	for _, chunk := range []int{1, 7, 64, 1024, 4096} {
		reader := bufio.NewReader(&slowReader{data: raw.Bytes(), chunkSize: chunk})
		scanner := NewScanner(reader)

		var got []string
		for scanner.Scan() {
			got = append(got, scanner.Text())
		}
		if err := scanner.Err(); err != nil {
			t.Fatalf("chunk=%d: scanner error: %v", chunk, err)
		}

		if len(got) != len(packets) {
			t.Fatalf("chunk=%d: got %d packets, want %d", chunk, len(got), len(packets))
		}
		for i := range packets {
			if got[i] != packets[i] {
				t.Errorf("chunk=%d packet %d: got %d bytes, want %d bytes",
					chunk, i, len(got[i]), len(packets[i]))
			}
		}
	}
}

func TestScannerAcceptsBareLF(t *testing.T) {
	reader := bufio.NewReader(strings.NewReader("#TMA:B:one\n#TMA:B:two\r\n"))
	scanner := NewScanner(reader)

	var got []string
	for scanner.Scan() {
		got = append(got, scanner.Text())
	}

	want := []string{"#TMA:B:one", "#TMA:B:two"}
	if len(got) != len(want) {
		t.Fatalf("got %v, want %v", got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Errorf("packet %d: got %q, want %q", i, got[i], want[i])
		}
	}
}

func TestGetPacketType(t *testing.T) {
	tests := []struct {
		packet string
		want   PacketType
	}{
		{"@N:N172SP:2000", PacketPilotPosition},
		{"^DAL1151:40.6:-73.7", PacketFastPilotPosition},
		{"%EWR_P_APP:28550:5:150:4:40.6:-74.1:0", PacketATCPosition},
		{"#SLPRM4211:41.0:-73.1", PacketFastPilotPositionSlow},
		{"#STDAL2119:40.6:-73.7", PacketFastPilotPositionStopped},
		{"#TMN172SP:@22800:hi", PacketTextMessage},
		{"#PCSCT_APP:SAN_APP:CCP:VER", PacketProController},
		{"#SBN7938C:DAL625:PIR", PacketSquawkbox},
		{"#WXAAL365:SERVER:KDFW", PacketWeatherRequest},
		{"#DAsan_twr:1555555", PacketDeleteATC},
		{"#DPAAL325:1400000", PacketDeletePilot},
		{"$CQN172SP:SERVER:ATC:SAN_TWR", PacketClientQuery},
		{"$CRSERVER:N172SP:ATC:Y:SAN_TWR", PacketClientQueryResponse},
		{"$AXN172SP:SERVER:METAR:KSAN", PacketMetarRequest},
		{"$!!SUP:N172SP:1:reason", PacketKillRequest},
		{"$ZCSERVER:N172SP:abcdef", PacketAuthChallenge},
		{"$HOSAN_APP:LAX_CTR:ROU1887", PacketHandoffRequest},
		{"$HALAX_CTR:SAN_APP:ROU1887", PacketHandoffAccept},
		{"$FPN172SP:SERVER:I:A320", PacketFlightPlan},
		{"$AMSAN_APP:SERVER:ROU1887:I:A320", PacketFlightPlanAmendment},
		{"$PIN172SP:N7938C:1736029820", PacketPing},
		{"$PON7938C:N172SP:1736029820", PacketPong},
		{"", PacketUnknown},
		{"##", PacketUnknown},
		{"$XX", PacketUnknown},
		{"garbage", PacketUnknown},
	}

	for _, tt := range tests {
		if got := GetPacketType([]byte(tt.packet)); got != tt.want {
			t.Errorf("GetPacketType(%q) = %v, want %v", tt.packet, got, tt.want)
		}
	}
}

func TestSourceCallsign(t *testing.T) {
	tests := []struct {
		packet string
		want   string
	}{
		// The pilot position packet leads with the transponder mode, so the
		// callsign is in the second field, not the first.
		{"@N:N172SP:2000:1:40.6:-73.7:26:0:4290776072:359", "N172SP"},
		{"%EWR_P_APP:28550:5:150:4:40.6:-74.1:0", "EWR_P_APP"},
		{"#TMN172SP:@22800:hi", "N172SP"},
		{"$FPN172SP:SERVER:I:A320", "N172SP"},
	}

	for _, tt := range tests {
		typ := GetPacketType([]byte(tt.packet))
		if got := string(SourceCallsign([]byte(tt.packet), typ)); got != tt.want {
			t.Errorf("SourceCallsign(%q) = %q, want %q", tt.packet, got, tt.want)
		}
	}
}

func TestParseLatLon(t *testing.T) {
	tests := []struct {
		packet   string
		wantOK   bool
		wantLat  float64
		wantLong float64
	}{
		{"@N:C:2000:1:40.65906:-73.79891:26:0:0:0", true, 40.65906, -73.79891},
		{"@N:C:2000:1:notanumber:-73.79891:26:0:0:0", false, 0, 0},
		{"@N:C:2000:1:NaN:-73.79891:26:0:0:0", false, 0, 0},
		{"@N:C:2000:1:+Inf:-73.79891:26:0:0:0", false, 0, 0},
		{"@N:C:2000:1:40.65906:-Inf:26:0:0:0", false, 0, 0},
		// Out-of-range coordinates would build a bounding box that matches the
		// entire index, turning one bad client into a global broadcaster.
		{"@N:C:2000:1:91.0:0.0:26:0:0:0", false, 0, 0},
		{"@N:C:2000:1:0.0:181.0:26:0:0:0", false, 0, 0},
	}

	for _, tt := range tests {
		lat, lon, ok := ParseLatLon([]byte(tt.packet), 4, 5)
		if ok != tt.wantOK {
			t.Errorf("ParseLatLon(%q) ok = %v, want %v", tt.packet, ok, tt.wantOK)
			continue
		}
		if ok && (math.Abs(lat-tt.wantLat) > 1e-9 || math.Abs(lon-tt.wantLong) > 1e-9) {
			t.Errorf("ParseLatLon(%q) = %v,%v want %v,%v", tt.packet, lat, lon, tt.wantLat, tt.wantLong)
		}
	}
}

func TestParseVisRange(t *testing.T) {
	metres, ok := ParseVisRange([]byte("%A:28550:5:150:4:40.6:-74.1:0"), 3)
	if !ok || math.Abs(metres-150*MetresPerNauticalMile) > 1e-6 {
		t.Errorf("ParseVisRange = %v, %v; want %v, true", metres, ok, 150*MetresPerNauticalMile)
	}

	// An absurd range is clamped rather than rejected: the position itself is
	// still useful, it just must not cover the planet.
	metres, ok = ParseVisRange([]byte("%A:28550:5:999999:4:40.6:-74.1:0"), 3)
	if !ok || metres > 1500*MetresPerNauticalMile {
		t.Errorf("ParseVisRange did not clamp: got %v, ok=%v", metres, ok)
	}

	if _, ok := ParseVisRange([]byte("%A:28550:5:xyz:4"), 3); ok {
		t.Error("ParseVisRange accepted a non-numeric range")
	}

	for _, value := range []string{"NaN", "+Inf", "-Inf"} {
		if _, ok := ParseVisRange([]byte("%A:28550:5:"+value+":4"), 3); ok {
			t.Errorf("ParseVisRange accepted non-finite range %q", value)
		}
	}
}

func TestPitchBankHeading(t *testing.T) {
	// Heading occupies bits 2..11. Encoding a known heading and decoding it
	// must round-trip within the encoding's resolution (~0.35 degrees).
	for _, wantHeading := range []float64{0, 90, 180, 270, 359} {
		encoded := uint32(math.Round(wantHeading/(360.0/1024.0))) << 2
		_, _, heading := PitchBankHeading(encoded)
		if math.Abs(heading-wantHeading) > 0.5 {
			t.Errorf("PitchBankHeading(%d) heading = %v, want ~%v", encoded, heading, wantHeading)
		}
	}

	// Every axis must stay in range for arbitrary input.
	for _, packed := range []uint32{0, 1, 4261294148, 4290776072, math.MaxUint32} {
		pitch, bank, heading := PitchBankHeading(packed)
		if pitch < -180 || pitch > 180 {
			t.Errorf("PitchBankHeading(%d) pitch %v out of range", packed, pitch)
		}
		if bank < -180 || bank > 180 {
			t.Errorf("PitchBankHeading(%d) bank %v out of range", packed, bank)
		}
		if heading < 0 || heading >= 360 {
			t.Errorf("PitchBankHeading(%d) heading %v out of range", packed, heading)
		}
	}
}

func TestIsValidCallsign(t *testing.T) {
	valid := []string{
		"N172SP", "SAN_TWR", "LAX_25_CTR", "ZSSS_ATIS", "AB", "A-B_1234",
		// vATIS splits departure and arrival broadcasts into their own
		// stations; both names are 11 characters and used to be rejected.
		"ZSPD_D_ATIS", "ZSPD_A_ATIS",
		"ABCDEFGHIJKL", // 12 characters, the limit
	}
	for _, cs := range valid {
		if !IsValidCallsign([]byte(cs)) {
			t.Errorf("IsValidCallsign(%q) = false, want true", cs)
		}
	}

	invalid := []string{
		"",                       // empty
		"A",                      // too short
		"ABCDEFGHIJKLM",          // 13 characters, one over the limit
		"lax_twr",                // lowercase
		"SAN TWR",                // space
		"SAN:TWR",                // field separator would forge packet structure
		"SERVER", "CLIENT", "FP", // reserved: would let a client forge system messages
	}
	for _, cs := range invalid {
		if IsValidCallsign([]byte(cs)) {
			t.Errorf("IsValidCallsign(%q) = true, want false", cs)
		}
	}
}

func TestIsATISCallsign(t *testing.T) {
	tests := map[string]bool{
		"ZSSS_ATIS":  true,
		"KSAN_ATIS":  true,
		"SAN_TWR":    false,
		"ATIS":       false, // no underscore: not a station suffix
		"LAX_25_CTR": false,
	}

	for callsign, want := range tests {
		if got := IsATISCallsign(callsign); got != want {
			t.Errorf("IsATISCallsign(%q) = %v, want %v", callsign, got, want)
		}
	}
}

func TestScannerRejectsOversizePacket(t *testing.T) {
	// A line with no terminator that exceeds the cap must surface an error
	// rather than being silently truncated into a valid-looking packet.
	huge := strings.Repeat("A", MaxPacketSize+1)
	scanner := NewScanner(bufio.NewReader(strings.NewReader(huge)))

	if scanner.Scan() {
		t.Fatalf("scanner accepted a %d-byte packet", len(scanner.Bytes()))
	}
	if scanner.Err() == nil {
		t.Error("oversize packet produced no error")
	}
}

// These literal vectors follow the FSD wire layout: pitch in bits 22–31,
// bank in bits 12–21, heading in bits 2–11, at 360/1024 degrees per unit.
// No client encoder is used to generate the expected values.
func TestAttitudeWireVectors(t *testing.T) {
	for _, tc := range []struct {
		name                 string
		packed               uint32
		pitch, bank, heading float64
	}{
		{"level", 0x00000000, 0, 0, 0},
		{"nose_up", 0x20000000, 45, 0, 0},
		{"nose_down", 0xe0000000, -45, 0, 0},
		{"right_bank", 0x00080000, 0, 45, 0},
		{"left_bank", 0x00380000, 0, -45, 0},
		{"east", 0x00000400, 0, 0, 90},
		{"south", 0x00000800, 0, 0, 180},
		{"west", 0x00000c00, 0, 0, 270},
		{"heading_below_360", 0x00000ffc, 0, 0, 359.6484375},
		{"climbing_turn", 0x053c0c00, 7.03125, -22.5, 270},
		{"low_flags_do_not_change_attitude", 0x053c0c03, 7.03125, -22.5, 270},
	} {
		t.Run(tc.name, func(t *testing.T) {
			pitch, bank, heading := PitchBankHeading(tc.packed)
			if pitch != tc.pitch || bank != tc.bank || heading != tc.heading {
				t.Fatalf("decoded %08x as (%v, %v, %v), want (%v, %v, %v)",
					tc.packed, pitch, bank, heading, tc.pitch, tc.bank, tc.heading)
			}
		})
	}
}
