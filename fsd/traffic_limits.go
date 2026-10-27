package fsd

import "time"

// Both wallop spellings share per-member and network-wide relay budgets.
func (s *Server) allowWallop(client *Client) bool {
	cfg := s.config.Get()
	if client.wallopBudget == nil {
		client.wallopBudget = newTokenBucket(cfg.WallopsPerClientMinute, time.Minute)
	}
	s.memberWallopInit.Do(func() {
		s.memberWallopBudget = newTokenBucket(cfg.WallopsPerServerMinute, time.Minute)
	})
	if !client.wallopBudget.take() || !s.memberWallopBudget.take() {
		client.SendServerMessage("Wallop rate limit exceeded; try again later.")
		return false
	}
	return true
}
