package fsd

import (
	"strings"
)

type FlightPlan struct {
	FlightRules      string
	Aircraft         string
	CruiseTAS        string
	Departure        string
	DepartureTime    string
	CruisingAltitude string
	Arrival          string
	Alternate        string
	Remarks          string
	Route            string

	ActualDepartureTime string
	HoursEnroute        string
	MinutesEnroute      string
	FuelHours           string
	FuelMinutes         string

	// Raw is the canonical $FP packet body, re-broadcast verbatim to
	// controllers that request the plan. Empty for a plan assembled from
	// stored fields rather than parsed off the wire.
	Raw string

	// OfflineAt records when the filer disconnected. Plans are kept for a
	// grace period so a pilot who drops and reconnects does not have to
	// refile; see reaper.go.
	OfflineAt int64
}

// $FP field layout, after the "$FPcallsign" and recipient fields:
//
//	2  flight rules      8  cruising altitude   14 alternate
//	3  aircraft          9  arrival             15 remarks
//	4  cruise TAS       10  hours enroute       16 route
//	5  departure        11  minutes enroute
//	6  departure time   12  fuel hours
//	7  actual departure 13  fuel minutes
const (
	fpFieldRules      = 2
	fpFieldAircraft   = 3
	fpFieldCruiseTAS  = 4
	fpFieldDeparture  = 5
	fpFieldDepTime    = 6
	fpFieldActualDep  = 7
	fpFieldAltitude   = 8
	fpFieldArrival    = 9
	fpFieldHoursEnr   = 10
	fpFieldMinutesEnr = 11
	fpFieldFuelHours  = 12
	fpFieldFuelMins   = 13
	fpFieldAlternate  = 14
	fpFieldRemarks    = 15
	fpFieldRoute      = 16
)

// ParseFlightPlan reads a $FP packet body into a FlightPlan.
//
// offset shifts every field index, which is how $AM is handled: an amendment
// carries an extra target-callsign field before the plan proper.
func ParseFlightPlan(packet []byte, offset int) *FlightPlan {
	fp := &FlightPlan{
		FlightRules:         FieldString(packet, fpFieldRules+offset),
		Aircraft:            FieldString(packet, fpFieldAircraft+offset),
		CruiseTAS:           FieldString(packet, fpFieldCruiseTAS+offset),
		Departure:           FieldString(packet, fpFieldDeparture+offset),
		DepartureTime:       FieldString(packet, fpFieldDepTime+offset),
		ActualDepartureTime: FieldString(packet, fpFieldActualDep+offset),
		CruisingAltitude:    FieldString(packet, fpFieldAltitude+offset),
		Arrival:             FieldString(packet, fpFieldArrival+offset),
		HoursEnroute:        FieldString(packet, fpFieldHoursEnr+offset),
		MinutesEnroute:      FieldString(packet, fpFieldMinutesEnr+offset),
		FuelHours:           FieldString(packet, fpFieldFuelHours+offset),
		FuelMinutes:         FieldString(packet, fpFieldFuelMins+offset),
		Alternate:           FieldString(packet, fpFieldAlternate+offset),
		Remarks:             FieldString(packet, fpFieldRemarks+offset),
		Route:               strings.TrimRight(remainderFrom(packet, fpFieldRoute+offset), "\r\n"),
	}

	return fp
}

// remainderFrom returns everything from the given field index to the end of
// the packet, colons included.
//
// The route is the last field but may itself contain colons, so it cannot be
// read with Field.
func remainderFrom(packet []byte, index int) string {
	rest := packet
	for range index {
		i := indexByte(rest, ':')
		if i < 0 {
			return ""
		}
		rest = rest[i+1:]
	}
	return string(rest)
}

func indexByte(b []byte, c byte) int {
	for i := range b {
		if b[i] == c {
			return i
		}
	}
	return -1
}

// BuildFilePacket renders a $FP packet advertising this plan for a callsign.
func (fp *FlightPlan) BuildFilePacket(callsign, recipient string) string {
	var b strings.Builder
	b.Grow(len(fp.Raw) + 32)
	b.WriteString("$FP")
	b.WriteString(callsign)
	b.WriteByte(':')
	b.WriteString(recipient)
	b.WriteByte(':')
	b.WriteString(fp.Body())
	b.WriteString("\r\n")
	return b.String()
}

// BuildAmendPacket renders an $AM packet for this plan.
func (fp *FlightPlan) BuildAmendPacket(source, recipient, target string) string {
	var b strings.Builder
	b.Grow(len(fp.Raw) + 48)
	b.WriteString("$AM")
	b.WriteString(source)
	b.WriteByte(':')
	b.WriteString(recipient)
	b.WriteByte(':')
	b.WriteString(target)
	b.WriteByte(':')
	b.WriteString(fp.Body())
	b.WriteString("\r\n")
	return b.String()
}

func (fp *FlightPlan) Body() string {
	if fp.Raw != "" {
		return fp.Raw
	}

	return strings.Join([]string{
		fp.FlightRules,                 // 2
		fp.Aircraft,                    // 3
		fp.CruiseTAS,                   // 4
		fp.Departure,                   // 5
		fp.DepartureTime,               // 6
		orZero(fp.ActualDepartureTime), // 7
		fp.CruisingAltitude,            // 8
		fp.Arrival,                     // 9
		orZero(fp.HoursEnroute),        // 10
		orZero(fp.MinutesEnroute),      // 11
		orZero(fp.FuelHours),           // 12
		orZero(fp.FuelMinutes),         // 13
		fp.Alternate,                   // 14
		sanitiseField(fp.Remarks),      // 15
		sanitiseField(fp.Route),        // 16
	}, ":")
}

// orZero substitutes "0" for an empty numeric field, which is what clients
// send and what controller software expects to parse.
func orZero(v string) string {
	if v == "" {
		return "0"
	}
	return v
}

// sanitiseField strips characters that would change the packet's structure.
//
// Remarks and route are free text supplied by the pilot. A colon in either
// would be read as a field separator by every receiving client, shifting
// everything after it; a newline would split one packet into two.
func sanitiseField(v string) string {
	return strings.Map(func(r rune) rune {
		switch r {
		case ':', '\r', '\n':
			return ' '
		}
		return r
	}, v)
}

// extractBody returns the portion of a $FP or $AM packet that follows the
// header fields, which is what gets re-broadcast.
func extractBody(packet []byte, headerFields int) string {
	rest := packet
	for range headerFields {
		i := indexByte(rest, ':')
		if i < 0 {
			return ""
		}
		rest = rest[i+1:]
	}
	return string(rest)
}
