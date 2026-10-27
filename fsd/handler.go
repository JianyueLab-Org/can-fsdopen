package fsd

import (
	"fmt"
	"strconv"
	"strings"
)

// dispatch routes a verified packet to its handler.
func (s *Server) dispatch(client *Client, packetType PacketType, packet []byte) {
	switch packetType {
	case PacketTextMessage:
		s.handleTextMessage(client, packet)
	case PacketPilotPosition:
		s.handlePilotPosition(client, packet)
	case PacketFastPilotPosition, PacketFastPilotPositionSlow, PacketFastPilotPositionStopped:
		s.handleFastPilotPosition(client, packet)
	case PacketATCPosition:
		s.handleATCPosition(client, packet)
	case PacketDeleteATC, PacketDeletePilot:
		s.handleDelete(client, packet)
	case PacketClientQuery, PacketClientQueryResponse:
		s.handleClientQuery(client, packetType, packet)
	case PacketProController:
		s.handleProController(client, packet)
	case PacketSquawkbox:
		s.handleSquawkbox(client, packet)
	case PacketMetarRequest:
		s.handleMetarRequest(client, packet)
	case PacketWeatherRequest:
		s.handleWeatherRequest(client, packet)
	case PacketKillRequest:
		s.handleKillRequest(client, packet)
	case PacketAuthChallenge:
		s.handleAuthChallenge(client, packet)
	case PacketHandoffRequest, PacketHandoffAccept:
		s.handleHandoff(client, packet)
	case PacketFlightPlan:
		s.handleFileFlightPlan(client, packet)
	case PacketFlightPlanAmendment:
		s.handleAmendFlightPlan(client, packet)
	case PacketPing:
		s.handlePing(client, packet)
	case PacketPong:
		s.handlePong(client, packet)
	}
}

// --- text messages ---

func (s *Server) handleTextMessage(client *Client, packet []byte) {
	recipient := FieldString(packet, 1)
	raw := string(packet)

	switch {
	case recipient == recipientFlightPlan:
		// Legacy ATC clients acknowledge received flight plans through FP.
		return

	case recipient == recipientATCChat:
		// The controller chat channel is controllers only.
		if !client.IsATC {
			client.SendError(ErrCodeInvalidControl, "ATC chat is for controllers only")
			return
		}
		s.broadcastRangedATC(client, raw)

	case recipient == recipientSupervisors:
		s.handleWallop(client, packet)

	case recipient == recipientBroadcast:
		// Server-wide broadcast. The Python server let anyone do this.
		if !client.NetworkRating.IsSupervisor() {
			client.SendError(ErrCodeInvalidControl, "Broadcast requires supervisor")
			return
		}
		s.broadcastAll(client, raw)

	case recipient == recipientServer:
		s.handleServerMessage(client, packet)

	case strings.HasPrefix(recipient, "@"):
		// Frequency message: everyone in range hears it.
		s.broadcastRanged(client, raw)

	default:
		s.sendDirect(client, recipient, raw)
	}
}

// handleWallop relays a message to supervisors.
func (s *Server) handleWallop(client *Client, packet []byte) {
	if !s.allowWallop(client) {
		return
	}
	s.broadcastSupervisors(client, string(packet))
	client.SendServerMessage("Your wallop has been sent to all online supervisors.")

}

// handleServerMessage handles text addressed to SERVER, which is where the
// dot-commands live.
func (s *Server) handleServerMessage(client *Client, packet []byte) {
	body := remainderFrom(packet, 2)
	body = strings.TrimSpace(strings.TrimSuffix(body, "\r\n"))

	// .wallop was handled here by the Python server, so keep it working.
	if after, found := strings.CutPrefix(body, ".wallop "); found {
		message := strings.TrimSpace(after)
		if message == "" {
			return
		}
		if !s.allowWallop(client) {
			return
		}
		s.broadcastSupervisors(client, fmt.Sprintf("#TM%s:*S:%s\r\n", client.Callsign, message))
		client.SendServerMessage("Your wallop has been sent to all online supervisors.")

		return
	}
}

// --- positions ---

// handlePilotPosition processes the 0.2 Hz `@` update.
//
// Layout: @mode:callsign:squawk:rating:lat:lon:altitude:groundspeed:pbh:correction
func (s *Server) handlePilotPosition(client *Client, packet []byte) {
	// **A controller is not an aircraft.** handleFastPilotPosition and
	// handleATCPosition both check the client's class and this one did not, so
	// a controller sending `@` had its indexed position moved and its
	// visibility overwritten with the 40 NM pilot default until its next `%` —
	// up to fifteen seconds of silently losing in-range traffic — and the
	// packet was relayed to everyone as a pilot position under a controller
	// callsign. Dropped silently rather than answered with an $ER, matching
	// handleFastPilotPosition: at 0.2 Hz a misbehaving client would otherwise
	// be sent a steady stream of errors.
	if client.IsATC {
		return
	}
	if !checkPositionRating(client, FieldString(packet, 3)) {
		return
	}

	lat, lon, ok := ParseLatLon(packet, 4, 5)
	if !ok {
		client.SendError(ErrCodeSyntax, "Invalid latitude/longitude")
		return
	}

	s.postOffice.updatePosition(client, [2]float64{lat, lon}, pilotVisRangeNM*MetresPerNauticalMile)

	client.touchPosition()

	s.syncControllerVisibility(client)

	result := s.broadcastPilotPosition(client, string(packet), false)

	s.updateSendFast(client, result)
}

// checkPositionRating rejects untrusted ratings above the authenticated account.
// The login rating remains fixed; position packets do not grant permissions.
func checkPositionRating(client *Client, field string) bool {
	rating, err := parseRating(field)
	if err != nil {
		client.SendError(ErrCodeSyntax, "Invalid network rating")
		return false
	}
	if rating > client.MaxRating {
		client.SendError(ErrCodeRequestedLevelTooHigh, "Requested level too high")
		client.Disconnect("position rating exceeds authenticated account rating")
		return false
	}
	return true
}

// updateSendFast toggles Velocity fast positions.
//
// A client only needs 5 Hz updates when another Velocity-capable aircraft is
// close enough for the extra fidelity to matter. Hysteresis-free switching at
// exactly 5 NM would flap, so the thresholds differ slightly.
func (s *Server) updateSendFast(client *Client, result searchResult) {
	if client.ProtoRevision != ProtoRevisionVelocity {
		return
	}

	const enableNM = 5.0
	const disableNM = 6.0

	distanceNM := result.closestVelocityPeer / MetresPerNauticalMile

	if client.sendFastEnabled.Load() {
		if distanceNM > disableNM {
			client.sendFastEnabled.Store(false)
			s.sendSendFast(client, false)
		}
		return
	}

	if distanceNM < enableNM {
		client.sendFastEnabled.Store(true)
		s.sendSendFast(client, true)
	}
}

func (s *Server) sendSendFast(client *Client, enabled bool) {
	flag := '0'
	if enabled {
		flag = '1'
	}
	client.Send(fmt.Sprintf("$SFSERVER:%s:%c\r\n", client.Callsign, flag))
}

// handleFastPilotPosition relays `^`, `#SL` and `#ST` updates.
//
// These only mean anything to protocol revision 101 clients, so they are not
// sent to anyone else.
func (s *Server) handleFastPilotPosition(client *Client, packet []byte) {
	if client.IsATC {
		return
	}

	// Fast positions carry coordinates too, but the authoritative position
	// comes from the `@` packet; only the freshness marker is updated here so
	// a stopped aircraft sending only #ST is not reaped.
	client.touchPosition()

	s.broadcastPilotPosition(client, string(packet), true)
}

// handleATCPosition processes the 15-second `%` update.
//
// Layout: %callsign:frequency:facility:visRange:rating:lat:lon:0
func (s *Server) handleATCPosition(client *Client, packet []byte) {
	if !client.IsATC {
		client.SendError(ErrCodeInvalidControl, "Only controllers may send position updates")
		return
	}
	if !checkPositionRating(client, FieldString(packet, 4)) {
		return
	}

	facility, err := strconv.Atoi(FieldString(packet, 2))
	if err != nil {
		client.SendError(ErrCodeSyntax, "Invalid facility type")
		return
	}

	lat, lon, ok := ParseLatLon(packet, 5, 6)
	if !ok {
		client.SendError(ErrCodeSyntax, "Invalid latitude/longitude")
		return
	}

	visRange, ok := ParseVisRange(packet, 3)
	if !ok {
		client.SendError(ErrCodeSyntax, "Invalid visibility range")
		return
	}

	// The facility comes from the packet, not from guessing at the callsign
	// the way the Python server did.
	if !s.checkFacility(client, facility) {
		return
	}

	client.setFacilityType(facility)

	s.postOffice.updatePosition(client, [2]float64{lat, lon}, visRange)
	client.touchPosition()
	position := string(packet)
	client.atcPosition.Store(&position)
	s.syncControllerVisibility(client)

	s.broadcastRanged(client, string(packet))
}

// handleDelete processes a client announcing its own departure.
func (s *Server) handleDelete(client *Client, packet []byte) {
	// The broadcast happens in the deferred handler in handleConn, so all
	// that is needed here is to close the connection.
	client.Disconnect("client sent delete packet")
}

// --- client queries ---

func (s *Server) handleClientQuery(client *Client, packetType PacketType, packet []byte) {
	recipient := FieldString(packet, 1)
	queryType := FieldString(packet, 2)

	// A reply to the server's own ATIS poll is addressed to SERVER, because
	// SERVER is what asked. It has to be claimed before the generic SERVER
	// dispatch below, which has no ATIS case and would drop it.
	if packetType == PacketClientQueryResponse && recipient == recipientServer && queryType == "ATIS" {
		s.handleTextATISResponse(client, packet)
		return
	}

	if recipient == recipientServer {
		s.handleServerQuery(client, queryType, packet)
		return
	}

	switch queryType {
	// Unprivileged controller queries: any controller may send them.
	case "BY", "HI", "HLP", "NOHLP", "WH", "NEWATIS", "NEWINFO":
		if !client.IsATC {
			client.SendError(ErrCodeInvalidControl, "Controllers only")
			return
		}
		s.forwardClientQuery(client, packet)

	// Privileged controller queries: these mutate another aircraft's state,
	// so an observer must not be able to send them.
	case "IT", "DR", "HT", "TA", "FA", "BC", "SC", "VT", "EST", "GD", "IPC":
		if !client.activeController() {
			client.SendError(ErrCodeInvalidControl, "Active controller required")
			return
		}
		// A query the ownership rules refused is not relayed: telling every
		// other scope a track moved when the registry says it did not is the
		// disagreement the server-owned state machine exists to prevent.
		if !s.trackFromQuery(client, queryType, packet) {
			return
		}
		if queryType == "BC" {
			s.assignBeaconCode(FieldString(packet, 3), FieldString(packet, 4))
		}
		s.forwardClientQuery(client, packet)

	// Open to everyone: capability and identity exchanges between clients.
	case "ACC", "CAPS", "C?", "RN", "ATIS", "SV":
		s.forwardClientQuery(client, packet)

	case "INF":
		// Responses may come from anyone; interrogations are a supervisor
		// tool, since the reply carries the target's IP and system details.
		if packetType == PacketClientQueryResponse {
			s.sendDirect(client, recipient, string(packet))
			return
		}
		if !client.NetworkRating.IsSupervisor() {
			client.SendError(ErrCodeInvalidControl, "Supervisor required")
			return
		}
		s.forwardClientQuery(client, packet)

	default:
		s.forwardClientQuery(client, packet)
	}
}

// forwardClientQuery routes a query by its recipient field.
func (s *Server) forwardClientQuery(client *Client, packet []byte) {
	recipient := FieldString(packet, 1)
	raw := string(packet)

	switch recipient {
	case recipientRangedATC:
		s.broadcastRangedATC(client, raw)
	case recipientRangedAll:
		s.broadcastRanged(client, raw)
	default:
		if recipient == "" {
			client.SendError(ErrCodeNoSuchCallsign, "Missing recipient")
			return
		}
		s.sendDirect(client, recipient, raw)
	}
}

// handleServerQuery answers a $CQ addressed to SERVER.
func (s *Server) handleServerQuery(client *Client, queryType string, packet []byte) {
	switch queryType {
	case "ATC":
		s.handleQueryATC(client, packet)
	case "IP":
		client.Send(fmt.Sprintf("$CRSERVER:%s:IP:%s\r\n", client.Callsign, client.RemoteIP()))
	case "FP":
		s.handleQueryFlightPlan(client, packet)
	case "CAPS":
		client.Send(fmt.Sprintf("$CRSERVER:%s:CAPS:ATCINFO=1:MODELDESC=1:ACCONFIG=1\r\n", client.Callsign))
	case "SV":
		// SV asks for the *address* of the server, not its name or version.
		// Answer with the address this client reached us on, which is what a
		// supervisor asking "which server is that client on" wants.
		client.Send(fmt.Sprintf("$CRSERVER:%s:SV:%s\r\n", client.Callsign, client.LocalAddr()))
	}
}

// handleQueryATC answers "is this callsign an active controller?".
func (s *Server) handleQueryATC(client *Client, packet []byte) {
	target := FieldString(packet, 3)
	if target == "" {
		client.SendError(ErrCodeSyntax, "Missing callsign")
		return
	}

	answer := "N"
	if other, err := s.postOffice.find(target); err == nil {
		if other.IsATC && other.FacilityType() > FacilityObserver {
			answer = "Y"
		}
	}

	client.Send(fmt.Sprintf("$CRSERVER:%s:ATC:%s:%s\r\n", client.Callsign, answer, target))
}

// handleQueryFlightPlan returns a filed plan to a controller.
func (s *Server) handleQueryFlightPlan(client *Client, packet []byte) {
	if !client.IsATC {
		client.SendError(ErrCodeInvalidControl, "Controllers only")
		return
	}

	target := FieldString(packet, 3)
	if target == "" {
		client.SendError(ErrCodeSyntax, "Missing callsign")
		return
	}

	fp := s.lookupFlightPlan(target)
	if fp == nil {
		client.SendError(ErrCodeNoFlightPlan, "No flight plan for "+target)
		return
	}

	client.Send(fp.BuildFilePacket(target, "*A"))

	beaconCode := "0"
	if other, err := s.postOffice.find(target); err == nil {
		beaconCode = other.BeaconCode()
	}
	client.Send(fmt.Sprintf("#PCserver:%s:CCP:BC:%s:%s\r\n", client.Callsign, target, beaconCode))
}

func (s *Server) assignBeaconCode(aircraft, code string) {
	if target, err := s.postOffice.find(aircraft); err == nil {
		target.setBeaconCode(code)
	}
}

func (s *Server) handleTextATISResponse(client *Client, packet []byte) {
	if !client.IsATC {
		return
	}

	switch FieldString(packet, 3) {
	case "T":
		line := remainderFrom(packet, 4)
		line = strings.TrimSuffix(line, "\r\n")
		client.receiveTextATIS("T", line)
	case "A", "Z", "V", "E":
		client.receiveTextATIS(FieldString(packet, 3), "")
	}
}

// --- shared state (#PC) ---

func (s *Server) handleProController(client *Client, packet []byte) {
	if !client.IsATC {
		client.SendError(ErrCodeInvalidControl, "Controllers only")
		return
	}

	recipient := FieldString(packet, 1)
	if recipient == "" {
		client.SendError(ErrCodeSyntax, "Missing recipient")
		return
	}

	switch FieldString(packet, 3) {
	// Version checks and landline signalling: harmless between any two
	// controllers, including observers.
	case "VER", "ID", "DI",
		"IC", "IK", "IB", "IO",
		"OC", "OK", "OB", "OO",
		"MC", "MK", "MB", "MO":
		s.routeProController(client, recipient, packet)

	// State mutation: requires an active (non-observer) controller.
	case "IH", "SC", "GD", "TA", "FA", "VT", "BC", "HC", "PT", "DP", "ST":
		if !client.activeController() {
			client.SendError(ErrCodeInvalidControl, "Active controller required")
			return
		}

		switch FieldString(packet, 3) {
		case "BC":
			// Track assigned squawks so they can be handed to the next
			// controller that asks for the flight plan.
			s.assignBeaconCode(FieldString(packet, 4), FieldString(packet, 5))

		case "HC":
			if target := FieldString(packet, 4); target != "" {
				if !s.changeTrack(client, target, "", false, func() error {
					return s.tracks.cancelHandoff(target, client.Callsign)
				}) {
					return
				}
			}
		}

		s.routeProController(client, recipient, packet)
	}
}

func (s *Server) routeProController(client *Client, recipient string, packet []byte) {
	if strings.HasPrefix(recipient, "@") {
		s.broadcastRangedATC(client, string(packet))
		return
	}
	s.sendDirect(client, recipient, string(packet))
}

// --- squawkbox, handoff, ping ---

func (s *Server) handleSquawkbox(client *Client, packet []byte) {
	s.sendDirect(client, FieldString(packet, 1), string(packet))
}

func (s *Server) handleHandoff(client *Client, packet []byte) {
	if !client.activeController() {
		client.SendError(ErrCodeInvalidControl, "Active controller required")
		return
	}

	peer := FieldString(packet, 1)

	if target := FieldString(packet, 2); target != "" {
		if !s.changeTrack(client, target, peer, true, func() error {
			if GetPacketType(packet) == PacketHandoffRequest {
				// An owner-less offer may arrive before the owner's $CQ IT.
				return s.tracks.proposeHandoff(target, client.Callsign, peer)
			}
			// Ownership moves only after the accepting controller's $CQ HT.
			return s.tracks.acceptHandoff(target, client.Callsign)
		}) {
			return
		}
	}

	s.sendDirect(client, peer, string(packet))
}

func (s *Server) handlePing(client *Client, packet []byte) {
	recipient := FieldString(packet, 1)

	// A ping to SERVER is answered here rather than forwarded.
	if recipient == recipientServer {
		client.Send(fmt.Sprintf("$POSERVER:%s:%s\r\n", client.Callsign, FieldString(packet, 2)))
		return
	}

	s.sendDirect(client, recipient, string(packet))
}

func (s *Server) handlePong(client *Client, packet []byte) {
	s.sendDirect(client, FieldString(packet, 1), string(packet))
}

// --- supervisor actions ---

func (s *Server) handleKillRequest(client *Client, packet []byte) {
	// The Python server performed no check at all here, so any connected
	// client could disconnect any other.
	if !client.NetworkRating.IsSupervisor() {
		client.SendError(ErrCodeInvalidControl, "Supervisor required")
		return
	}

	target := FieldString(packet, 1)
	reason := remainderFrom(packet, 2)
	reason = strings.TrimSpace(strings.TrimSuffix(reason, "\r\n"))
	if reason == "" {
		reason = "no reason given"
	}

	if err := s.Kick(target, reason); err != nil {
		client.SendError(ErrCodeNoSuchCallsign, "No such callsign: "+target)
		return
	}

	client.SendServerMessage("Disconnected " + target)
}

// --- authentication ---

func (s *Server) handleAuthChallenge(client *Client, packet []byte) {
	if !client.authState.Initialized() {
		client.SendError(ErrCodeUnauthorizedSoftware,
			"Cannot answer a challenge: no initial challenge was received")
		return
	}

	challenge := Field(packet, 2)
	if len(challenge) == 0 {
		client.SendError(ErrCodeSyntax, "Empty challenge")
		return
	}

	response := client.authState.Response(challenge)
	client.authState.Advance(&response)

	var b strings.Builder
	b.Grow(64)
	b.WriteString("$ZRSERVER:")
	b.WriteString(client.Callsign)
	b.WriteByte(':')
	b.Write(response[:])
	b.WriteString("\r\n")

	client.Send(b.String())
}

// --- flight plans ---

// handleFileFlightPlan stores a plan filed by a pilot and tells controllers.
func (s *Server) handleFileFlightPlan(client *Client, packet []byte) {
	fp := ParseFlightPlan(packet, 0)
	// Two header fields precede the plan body: "$FPcallsign" and the
	// recipient.
	fp.Raw = strings.TrimSuffix(extractBody(packet, 2), "\r\n")

	client.setFlightPlan(fp)

	s.broadcastAllATC(client, fp.BuildFilePacket(client.Callsign, "*A"))

}

// handleAmendFlightPlan applies a controller's amendment.
func (s *Server) handleAmendFlightPlan(client *Client, packet []byte) {
	if !client.activeController() {
		client.SendError(ErrCodeInvalidControl, "Active controller required")
		return
	}

	target := FieldString(packet, 2)
	targetClient, err := s.postOffice.find(target)
	if err != nil {
		client.SendError(ErrCodeNoSuchCallsign, "No such callsign: "+target)
		return
	}
	if track, owned := s.tracks.owner(target); owned && track.Controller != client.Callsign {
		client.SendError(ErrCodeInvalidControl, "Only the tracking controller may amend "+target)
		return
	}

	// $AM carries an extra target-callsign field ahead of the plan proper.
	fp := ParseFlightPlan(packet, 1)
	fp.Raw = strings.TrimSuffix(extractBody(packet, 3), "\r\n")

	targetClient.setFlightPlan(fp)

	s.broadcastAllATC(client, fp.BuildAmendPacket(client.Callsign, "*A", target))
}
