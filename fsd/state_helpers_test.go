package fsd

// Protocol tests inspect track state without exposing a datafeed API.
type testTrackState struct {
	Owner       string
	HandoffPeer string
	Status      TrackStatus
}

func (r *trackRegistry) states() map[string]testTrackState {
	r.mu.RLock()
	defer r.mu.RUnlock()
	states := make(map[string]testTrackState, len(r.tracks))
	for callsign, track := range r.tracks {
		states[callsign] = testTrackState{Owner: track.Controller, HandoffPeer: track.HandoffPeer, Status: track.Status}
	}
	return states
}

func (r *trackRegistry) size() int {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return len(r.tracks)
}

func testTrackOwners(s *Server) map[string]string {
	owners := make(map[string]string)
	for callsign, track := range s.tracks.states() {
		if track.Owner != "" {
			owners[callsign] = track.Owner
		}
	}
	return owners
}
