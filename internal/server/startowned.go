package server

import (
	"context"
	"fmt"
)

// StartOwned acquires exactly these shards, whatever placement would say. It is how a test
// or the simulator pins a layout; a cluster uses StartCluster and lets leases decide. An
// empty list owns nothing.
func (s *Server) StartOwned(ctx context.Context, shards []int) error {
	for _, id := range shards {
		if _, err := s.shards.Acquire(ctx, id); err != nil {
			return fmt.Errorf("server: %w", err)
		}
	}
	s.refreshShardGaugeMetric()
	s.refreshPendingTimersMetric()
	return nil
}
