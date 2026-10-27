package fsd

import (
	"fmt"
	"github.com/JianyueLab-Org/can-fsdopen/config"
	"log/slog"
)

// facilityViolation describes why a position was flagged.
type facilityViolation struct {
	// reason is a stable key used for per-session deduplication.
	reason string
	detail string
}

func (s *Server) checkFacility(client *Client, facility int) bool {
	cfg := s.config.Get()
	if client.MaxRating == RatingObserver && (facility != FacilityObserver || client.IsATIS) {
		client.SendError(ErrCodeInvalidPositionForRating, "OBS accounts may use observer positions only")
		client.Disconnect("observer account declared a non-observer position")
		return false
	}
	if cfg.FacilityEnforcement == config.EnforcementOff {
		return true
	}

	violation := ratingViolation(client, facility)
	if violation == nil {
		return true
	}

	if cfg.FacilityEnforcement == config.EnforcementStrict ||
		warnMustRefuseControlPosition(cfg, client, facility, violation) {
		slog.Warn("position refused",
			"callsign", client.Callsign,
			"facility", FacilityName(facility), "rating", int(client.NetworkRating),
			"reason", violation.reason)

		client.SendError(ErrCodeInvalidPositionForRating, violation.detail)
		client.Disconnect("unauthorised position: " + violation.reason)
		return false
	}

	s.warnFacility(client, facility, violation)
	return true
}

func warnMustRefuseControlPosition(cfg *config.Config, client *Client, facility int, violation *facilityViolation) bool {
	if cfg.FacilityEnforcement != config.EnforcementWarn ||
		violation == nil || violation.reason != "rating_too_low" ||
		client.IsATIS || !isControlFacility(facility) {
		return false
	}

	switch client.NetworkRating {
	case RatingInactive, RatingSuspended, RatingObserver:
		return true
	default:
		return false
	}
}

// isControlFacility identifies positions that exercise ATC control authority.
// FSS and ATIS are service positions and keep the existing advisory warn
// behavior for below-floor ratings.
func isControlFacility(facility int) bool {
	return facility >= FacilityDelivery && facility <= FacilityCentre
}

func ratingViolation(client *Client, facility int) *facilityViolation {
	// An _ATIS callsign is judged against the ATIS floor whatever facility it
	// reports, so self-reporting a lower one buys nothing.
	judged := facility
	if client.IsATIS {
		judged = FacilityATIS
	}

	if IsAllowedFacilityType(client.NetworkRating, judged) {
		return nil
	}

	return &facilityViolation{
		reason: "rating_too_low",
		detail: fmt.Sprintf("Rating %d is below the minimum for %s",
			int(client.NetworkRating), FacilityName(judged)),
	}
}

// atisRatingViolation reports an _ATIS login below the ATIS floor, or nil if
// the rating is sufficient. OBS accounts are refused in every mode; other
// violations follow the configured enforcement mode.
//
// An ATIS station is not obliged to send a `%` packet — some clients only
// publish the text and let the position lapse — so the check in
// handleATCPosition cannot be relied on to see it at all. The floor is applied
// at login instead.
func (s *Server) atisRatingViolation(client *Client) *facilityViolation {
	cfg := s.config.Get()
	if !client.IsATIS {
		return nil
	}
	if client.MaxRating == RatingObserver {
		return &facilityViolation{
			reason: "rating_too_low",
			detail: "OBS accounts may use observer positions only",
		}
	}
	if cfg.FacilityEnforcement == config.EnforcementOff {
		return nil
	}

	return ratingViolation(client, FacilityATIS)
}

// warnFacility reports a violation without interrupting the connection.
//
// Deduplicated per connection and per reason: ATC position packets arrive
// every 15 seconds, so an undeduplicated warning would notify the controller
// and every supervisor four times a minute for as long as they stayed logged
// in.
func (s *Server) warnFacility(client *Client, facility int, v *facilityViolation) {
	if !client.warnOnce(v.reason) {
		return
	}

	slog.Warn("unauthorised position",
		"callsign", client.Callsign,
		"facility", FacilityName(facility),
		"rating", int(client.NetworkRating),
		"reason", v.reason)

	client.SendServerMessage(v.detail + " (advisory facility check)")

	// Supervisors get a summary too, but behind a global budget so a batch of
	// legacy positions reconnecting cannot flood them.
	if !s.wallopBudget.take() {
		return
	}

	notice := fmt.Sprintf("Facility check: %s is on %s — %s",
		client.Callsign, FacilityName(facility), v.reason)
	s.broadcastSupervisors(nil, fmt.Sprintf("#TMSERVER:*S:%s\r\n", notice))

}
