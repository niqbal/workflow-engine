package server

import (
	"context"
	"fmt"
)

// StartOwned acquires exactly shards and records their fences. Start acquires every
// shard, which is the single-node case. The cluster simulator places each shard on one
// of N nodes by calling StartOwned; shard leases (WI-050) replace this. An empty list
// owns nothing.
func (s *Server) StartOwned(ctx context.Context, shards []int) error {
	for _, shard := range shards {
		if shard < 0 || shard >= s.cfg.NumShards {
			return fmt.Errorf("server: shard %d not in 0..%d", shard, s.cfg.NumShards-1)
		}
		f, err := s.st.AcquireShard(ctx, shard, s.cfg.Node, s.clk.Now(), s.cfg.LeaseDuration)
		if err != nil {
			return fmt.Errorf("server: acquire shard %d: %w", shard, err)
		}
		s.mu.Lock()
		s.fences[shard] = f
		s.mu.Unlock()
	}
	s.refreshShardGaugeMetric()
	s.refreshPendingTimersMetric()
	return nil
}
