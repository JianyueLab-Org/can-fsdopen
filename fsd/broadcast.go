package fsd

// Broadcast helpers.
//
// Every one of these queues to each recipient's outbound channel rather than
// writing to sockets directly, so a slow client can never stall delivery to
// the rest of the network.

// broadcastRanged sends to every client whose visibility box overlaps the
// sender's. Returns what the search learned about nearby Velocity clients.
func (s *Server) broadcastRanged(sender *Client, packet string) searchResult {
	return s.postOffice.search(sender, func(recipient *Client) bool {
		recipient.SendBestEffort(packet)
		return true
	})
}

// broadcastPilotPosition sends aircraft updates within each recipient's radius.
// Fast position packets also require protocol revision 101.
func (s *Server) broadcastPilotPosition(sender *Client, packet string, velocityOnly bool) searchResult {
	origin := sender.LatLon()
	return s.postOffice.search(sender, func(recipient *Client) bool {
		if velocityOnly && recipient.ProtoRevision != ProtoRevisionVelocity {
			return true
		}
		position := recipient.LatLon()
		if Distance(origin[0], origin[1], position[0], position[1]) <= recipient.VisRange() {
			recipient.SendBestEffort(packet)
		}
		return true
	})
}

// broadcastRangedATC sends to in-range controllers only.
func (s *Server) broadcastRangedATC(sender *Client, packet string) {
	s.postOffice.search(sender, func(recipient *Client) bool {
		if recipient.IsATC {
			recipient.SendBestEffort(packet)
		}
		return true
	})
}

// broadcastAll sends to every client except the sender.
func (s *Server) broadcastAll(sender *Client, packet string) {
	s.postOffice.all(sender, func(recipient *Client) bool {
		recipient.SendBestEffort(packet)
		return true
	})
}

// broadcastAllATC sends to every controller except the sender.
func (s *Server) broadcastAllATC(sender *Client, packet string) {
	s.postOffice.all(sender, func(recipient *Client) bool {
		if recipient.IsATC {
			recipient.SendBestEffort(packet)
		}
		return true
	})
}

// broadcastSupervisors sends to every supervisor and administrator.
//
// The sender is included when it is itself a supervisor, so a supervisor
// sending a wallop sees their own message echoed the way clients expect.
func (s *Server) broadcastSupervisors(sender *Client, packet string) {
	s.postOffice.all(sender, func(recipient *Client) bool {
		if recipient.NetworkRating.IsSupervisor() {
			recipient.SendBestEffort(packet)
		}
		return true
	})
}

// sendDirect forwards a packet to one callsign, telling the sender when no
// such station exists.
func (s *Server) sendDirect(sender *Client, recipient string, packet string) {
	if err := s.postOffice.send(recipient, packet); err != nil {
		sender.SendError(ErrCodeNoSuchCallsign, "No such callsign: "+recipient)
	}
}

// Well-known broadcast recipients from the FSD protocol.
const (
	// recipientATCChat is the all-controllers text channel.
	recipientATCChat = "@49999"
	// recipientRangedATC delivers a client query to in-range controllers.
	recipientRangedATC = "@94835"
	// recipientRangedAll delivers a client query to every in-range client.
	recipientRangedAll = "@94836"
	// recipientSupervisors is the wallop channel.
	recipientSupervisors = "*S"
	// recipientBroadcast is the server-wide channel, supervisors only.
	recipientBroadcast = "*"
	// recipientServer addresses the server itself.
	recipientServer = "SERVER"
	// recipientFlightPlan is the legacy flight plan pseudo-station.
	recipientFlightPlan = "FP"
)
