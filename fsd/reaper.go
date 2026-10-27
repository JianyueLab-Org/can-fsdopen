package fsd

import (
	"context"
	"log/slog"
	"time"
)

// Periodic maintenance.
//
// The Python server did all of this in one 14-second loop wrapped in half a
// dozen separate try/except blocks, each swallowing its error. Here each job
// is a separate function on its own schedule.

const (
	// heartbeatInterval is how often #DL keepalives go out. Clients treat a
	// silent server as dead.
	heartbeatInterval = 15 * time.Second

	// atisPollInterval is how often controllers are asked to resend their
	// ATIS text.
	atisPollInterval = 60 * time.Second

	// pilotIdleLimit and atcIdleLimit are how long a station may go without a
	// position report before it is dropped. Pilots report every 5 seconds and
	// controllers every 15, so these are generous.
	pilotIdleLimit = 60 * time.Second
	atcIdleLimit   = 150 * time.Second

	// maintenanceInterval is the sweep period for expiry jobs.
	maintenanceInterval = 30 * time.Second
)

// runReaper runs the periodic jobs until ctx is cancelled.
func (s *Server) runReaper(ctx context.Context) {
	heartbeat := time.NewTicker(heartbeatInterval)
	defer heartbeat.Stop()

	atisPoll := time.NewTicker(atisPollInterval)
	defer atisPoll.Stop()

	maintenance := time.NewTicker(maintenanceInterval)
	defer maintenance.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-heartbeat.C:
			s.sendHeartbeats()
		case <-atisPoll.C:
			s.pollTextATIS()
		case <-maintenance.C:
			s.dropIdleClients()
			s.expireOfflinePlans()
			s.expireKicks()
			s.expireStaleHandoffs()
		}
	}
}

// sendHeartbeats broadcasts the #DL keepalive.
func (s *Server) sendHeartbeats() {
	const packet = "#DLSERVER:*:0:0\r\n"

	s.postOffice.all(nil, func(c *Client) bool {
		c.TrySend(packet)
		return true
	})
}

// pollTextATIS asks each controller to resend their ATIS text.
//
// The reply arrives as a series of $CR ATIS packets. Published text stays
// available until a complete response ends with E.
func (s *Server) pollTextATIS() {
	s.postOffice.all(nil, func(c *Client) bool {
		if !c.IsATC {
			return true
		}
		c.queueTextATISRefresh()
		return true
	})
}

// dropIdleClients disconnects stations that have stopped reporting.
//
// The read deadline already covers a peer that stops sending anything at all;
// this catches one that keeps the socket warm without reporting a position.
func (s *Server) dropIdleClients() {
	now := time.Now()

	s.postOffice.all(nil, func(c *Client) bool {
		limit := pilotIdleLimit
		if c.IsATC {
			limit = atcIdleLimit
		}

		// Give a client that has never sent a position the full login grace
		// period before judging it.
		reference := c.LastPosition()
		if !c.HasPosition() && c.LoginTime.After(reference) {
			reference = c.LoginTime
		}

		if now.Sub(reference) > limit {
			slog.Info("dropping idle client",
				"callsign", c.Callsign, "idle", now.Sub(reference).Round(time.Second))
			c.Disconnect("no position updates")
		}

		return true
	})
}

// expireOfflinePlans discards flight plans whose grace period has elapsed.
func (s *Server) expireOfflinePlans() {
	s.plansMu.Lock()
	defer s.plansMu.Unlock()

	for callsign, fp := range s.offlinePlans {
		if time.Since(time.Unix(0, fp.OfflineAt)) > flightPlanGrace {
			delete(s.offlinePlans, callsign)
		}
	}
}

func (s *Server) expireStaleHandoffs() {
	if cleared := s.tracks.expireHandoffs(handoffTimeout); cleared > 0 {
		slog.Info("cleared handoffs nobody completed", "count", cleared)
	}
}

// expireKicks forgets kick cooldowns that have elapsed.
func (s *Server) expireKicks() {
	s.kickedMu.Lock()
	defer s.kickedMu.Unlock()

	for cid, at := range s.kicked {
		if time.Since(at) > kickCooldown {
			delete(s.kicked, cid)
		}
	}
}
