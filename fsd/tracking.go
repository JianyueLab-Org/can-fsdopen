package fsd

import (
	"errors"
	"log/slog"
	"sync"
	"time"
)

// TrackStatus is the lifecycle phase of a radar track.
type TrackStatus string

const (
	// TrackStatusOwned is a track some controller holds, with no handoff
	// outstanding.
	TrackStatusOwned TrackStatus = "owned"
	// TrackStatusPendingHandoff is a track whose owner proposed a handoff
	// ($HO) that the peer has not accepted yet.
	TrackStatusPendingHandoff TrackStatus = "pending_handoff"
	// TrackStatusHandoffAccepted is a track whose peer accepted the handoff
	// ($HA) but the change of owner has not been broadcast ($CQ HT).
	TrackStatusHandoffAccepted TrackStatus = "handoff_accepted"
)

// trackedAircraft records one aircraft's radar track.
type trackedAircraft struct {
	Controller string
	// HandoffFrom retains the proposer before an owner-less offer is claimed.
	HandoffFrom string
	HandoffPeer string
	Status      TrackStatus

	// HandoffAt is when the pending handoff was proposed, zero when there is
	// none. It exists so a handoff nobody ever completed can be swept: #PC HC
	// travels controller-to-controller and a client may simply forget.
	HandoffAt time.Time
}

// Refusals from the registry. They are errors rather than a bare bool because
// the controller is told which rule they hit, and "somebody else owns this"
// and "that was not offered to you" call for different answers on the scope.
var (
	errTrackOwned        = errors.New("fsd: aircraft is tracked by another controller")
	errNotTrackOwner     = errors.New("fsd: not the controller tracking this aircraft")
	errNotHandoffParty   = errors.New("fsd: no handoff for this aircraft involves this controller")
	errTrackRegistryFull = errors.New("fsd: too many tracked aircraft")
)

// maxTrackedAircraft bounds transient ownership and handoff state.
const maxTrackedAircraft = 4096

// handoffTimeout is how long a proposed handoff may stay pending before it is
// swept. Generous: a controller may reasonably take minutes to answer, and the
// cost of sweeping too early is an offer that silently disappears.
const handoffTimeout = 10 * time.Minute

type trackRegistry struct {
	mu     sync.RWMutex
	tracks map[string]trackedAircraft // aircraft callsign -> track
}

func newTrackRegistry() *trackRegistry {
	return &trackRegistry{tracks: make(map[string]trackedAircraft)}
}

func (r *trackRegistry) claim(aircraft, controller string) error {
	r.mu.Lock()
	defer r.mu.Unlock()

	if t, ok := r.tracks[aircraft]; ok {
		if t.Controller != "" && t.Controller != controller && t.HandoffPeer != controller {
			return errTrackOwned
		}
	} else if len(r.tracks) >= maxTrackedAircraft {
		return errTrackRegistryFull
	}

	r.tracks[aircraft] = trackedAircraft{
		Controller: controller,
		Status:     TrackStatusOwned,
	}
	return nil
}

// drop releases a track on $CQ DR.
//
// The owner may drop it and a supervisor may unstick one, but a controller who
// never held the aircraft may not release another controller's ownership.
//
// Dropping an aircraft nobody owns stays a no-op rather than an error, as it
// has been since the Python server raised a KeyError over it.
func (r *trackRegistry) drop(aircraft, controller string, override bool) error {
	r.mu.Lock()
	defer r.mu.Unlock()

	t, ok := r.tracks[aircraft]
	if !ok {
		return nil
	}
	if !override && t.Controller != "" && t.Controller != controller {
		return errNotTrackOwner
	}

	delete(r.tracks, aircraft)
	return nil
}

// proposeHandoff marks a track as being handed off to a peer controller. The
// aircraft need not be owned yet — a $HO can arrive before the owner's $CQ IT
// is seen — but the pending handoff still matters to the receiving side.
//
// Only the owner may offer it. Offering an aircraft somebody else holds would
// be a takeover with an extra step, since the peer could then claim it.
func (r *trackRegistry) proposeHandoff(aircraft, from, peer string) error {
	r.mu.Lock()
	defer r.mu.Unlock()

	t, ok := r.tracks[aircraft]
	if ok {
		if t.Controller != "" && t.Controller != from {
			return errNotTrackOwner
		}
	} else {
		if len(r.tracks) >= maxTrackedAircraft {
			return errTrackRegistryFull
		}
		t = trackedAircraft{}
	}

	t.HandoffFrom = from
	t.HandoffPeer = peer
	t.HandoffAt = time.Now()
	t.Status = TrackStatusPendingHandoff
	r.tracks[aircraft] = t
	return nil
}

// acceptHandoff marks a pending handoff as accepted by the peer it was offered
// to. Ownership does not change until that peer broadcasts $CQ HT.
//
// A $HA for an aircraft with no pending handoff changes nothing and is not an
// error: the server may have restarted mid-negotiation, and refusing to relay
// the packet would strand the two controllers with no way to finish. Only a
// $HA from somebody the handoff was *not* offered to is refused, which is the
// case that would otherwise let any active controller accept a handoff meant
// for someone else.
func (r *trackRegistry) acceptHandoff(aircraft, peer string) error {
	r.mu.Lock()
	defer r.mu.Unlock()

	t, ok := r.tracks[aircraft]
	if !ok || t.HandoffPeer == "" {
		return nil
	}
	if t.HandoffPeer != peer {
		return errNotHandoffParty
	}

	t.Status = TrackStatusHandoffAccepted
	r.tracks[aircraft] = t
	return nil
}

// cancelHandoff withdraws a pending handoff. Either party may: the offering
// controller with #PC … HC, the peer by rejecting or disconnecting.
func (r *trackRegistry) cancelHandoff(aircraft, controller string) error {
	r.mu.Lock()
	defer r.mu.Unlock()

	t, ok := r.tracks[aircraft]
	if !ok || t.HandoffPeer == "" {
		return nil
	}
	if t.Controller != controller && t.HandoffPeer != controller {
		return errNotHandoffParty
	}

	r.clearHandoffLocked(aircraft, t)
	return nil
}

func (r *trackRegistry) clearHandoffLocked(aircraft string, t trackedAircraft) {
	if t.Controller == "" {
		delete(r.tracks, aircraft)
		return
	}

	t.HandoffFrom = ""
	t.HandoffPeer = ""
	t.HandoffAt = time.Time{}
	t.Status = TrackStatusOwned
	r.tracks[aircraft] = t
}

func (r *trackRegistry) expireHandoffs(maxAge time.Duration) int {
	r.mu.Lock()
	defer r.mu.Unlock()

	now := time.Now()
	cleared := 0
	for aircraft, t := range r.tracks {
		if t.HandoffPeer == "" || t.HandoffAt.IsZero() || now.Sub(t.HandoffAt) <= maxAge {
			continue
		}
		r.clearHandoffLocked(aircraft, t)
		cleared++
	}
	return cleared
}

func (r *trackRegistry) releaseAllBy(controller string) {
	r.mu.Lock()
	defer r.mu.Unlock()

	for aircraft, track := range r.tracks {
		switch {
		case track.Controller == controller:
			delete(r.tracks, aircraft)
		case track.HandoffFrom == controller || track.HandoffPeer == controller:
			r.clearHandoffLocked(aircraft, track)
		}
	}
}

// releaseAircraft drops the track for a departing aircraft.
func (r *trackRegistry) releaseAircraft(aircraft string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	delete(r.tracks, aircraft)
}

func (r *trackRegistry) owner(aircraft string) (trackedAircraft, bool) {
	r.mu.RLock()
	defer r.mu.RUnlock()

	t, ok := r.tracks[aircraft]
	if !ok || t.Controller == "" {
		return trackedAircraft{}, false
	}
	return t, true
}

func (s *Server) trackFromQuery(client *Client, queryType string, packet []byte) bool {
	aircraft := FieldString(packet, 3)
	if aircraft == "" {
		// Nothing to do. Forwarded as before rather than refused: the
		// ownership rules have no opinion on a malformed payload, and the
		// syntax check is not this function's job.
		return true
	}

	switch queryType {
	case "IT", "HT":
		// IT is the unsolicited claim, HT the accepting controller
		// broadcasting a change of owner. Both name the aircraft in field 3
		// and both make the sender the owner, so both go through claim.
		if !s.changeTrack(client, aircraft, "", true, func() error {
			return s.tracks.claim(aircraft, client.Callsign)
		}) {
			return false
		}

	case "DR":
		// A supervisor may unstick a track whose owner is gone but whose
		// entry survived. There is deliberately no such override on IT or HT:
		// the recovery path is to drop and re-initiate, which leaves both
		// steps visible on every scope.
		override := client.NetworkRating.IsSupervisor()
		if !s.changeTrack(client, aircraft, "", false, func() error {
			return s.tracks.drop(aircraft, client.Callsign, override)
		}) {
			return false
		}
	}

	return true
}

// changeTrack serializes membership checks and track mutations with release.
// Lock order is postOffice.mu before tracks.mu. Cleanup holds the membership
// write lock through track removal and callsign release; a mutation either
// finishes before cleanup or observes the departed connection. Replies are
// sent only after the membership read lock is released.
func (s *Server) changeTrack(client *Client, aircraft, peer string, requirePilot bool, change func() error) bool {
	if requirePilot && !IsValidCallsign([]byte(aircraft)) {
		client.SendError(ErrCodeCallsignInvalid, "Invalid callsign: "+aircraft)
		return false
	}
	if peer != "" && !IsValidCallsign([]byte(peer)) {
		client.SendError(ErrCodeCallsignInvalid, "Invalid callsign: "+peer)
		return false
	}

	s.postOffice.mu.RLock()
	registered := s.postOffice.clients[client.Callsign] == client && client.ctx.Err() == nil
	target := s.postOffice.clients[aircraft]
	validTarget := !requirePilot || target != nil && !target.IsATC && target.ctx.Err() == nil
	other := s.postOffice.clients[peer]
	validPeer := peer == "" || other != nil && other.ctx.Err() == nil
	var err error
	if registered && validTarget && validPeer {
		err = change()
	}
	s.postOffice.mu.RUnlock()

	if !registered {
		return false
	}
	if !validTarget {
		client.SendError(ErrCodeNoSuchCallsign, "No such aircraft: "+aircraft)
		return false
	}
	if !validPeer {
		client.SendError(ErrCodeNoSuchCallsign, "No such callsign: "+peer)
		return false
	}
	if err != nil {
		s.refuseTrack(client, aircraft, err)
		return false
	}
	return true
}

// refuseTrack tells a controller which ownership rule they hit.
func (s *Server) refuseTrack(client *Client, aircraft string, err error) {
	var message string
	switch {
	case errors.Is(err, errTrackOwned):
		message = "Another controller is tracking " + aircraft
	case errors.Is(err, errNotTrackOwner):
		message = "You are not tracking " + aircraft
	case errors.Is(err, errNotHandoffParty):
		message = "No handoff for " + aircraft + " involves you"
	case errors.Is(err, errTrackRegistryFull):
		message = "Too many tracked aircraft"
	default:
		message = "Track request refused for " + aircraft
	}

	client.SendError(ErrCodeInvalidControl, message)

	slog.Debug("track request refused",
		"controller", client.Callsign, "err", err)
}

// releaseTracked clears whatever a departing client was involved in.
func (s *Server) releaseTracked(callsign string) {
	s.tracks.releaseAllBy(callsign)
	s.tracks.releaseAircraft(callsign)
}
