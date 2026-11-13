package server

import (
	"context"

	wfemetrics "github.com/niqbal/workflow-engine/internal/metrics"
	"github.com/niqbal/workflow-engine/internal/store"
)

const pendingTimersReadBatch = 256

func (s *Server) metrics() *wfemetrics.Collector {
	if m := s.sub.metrics.Load(); m != nil {
		return m
	}
	m := &wfemetrics.Collector{}
	if s.sub.metrics.CompareAndSwap(nil, m) {
		return m
	}
	return s.sub.metrics.Load()
}

// Metrics returns this server's metrics collector.
func (s *Server) Metrics() *wfemetrics.Collector {
	return s.metrics()
}

func (s *Server) observeWriteMetrics(written []store.Task, done []store.TaskKey) {
	hasTimerChange := false
	for _, t := range written {
		if t.Kind == store.TimerTask {
			hasTimerChange = true
			break
		}
	}
	if !hasTimerChange {
		for _, k := range done {
			if k.Kind == store.TimerTask {
				hasTimerChange = true
				break
			}
		}
	}
	if hasTimerChange {
		s.refreshPendingTimersMetric()
	}
}

func (s *Server) refreshShardGaugeMetric() {
	s.mu.Lock()
	n := len(s.fences)
	s.mu.Unlock()
	s.metrics().SetShardsOwned(n)
}

func (s *Server) refreshPendingTimersMetric() {
	ctx := context.Background()
	var total int64
	for _, shard := range s.Shards() {
		count, err := s.countPendingTimersShard(ctx, shard)
		if err != nil {
			return
		}
		total += count
	}
	s.metrics().SetPendingTimers(total)
}

func (s *Server) countPendingTimersShard(ctx context.Context, shard int) (int64, error) {
	limit := pendingTimersReadBatch
	for {
		page, err := s.st.ReadTimerTasks(ctx, shard, farFuture, limit)
		if err != nil {
			return 0, err
		}
		if len(page) < limit {
			return int64(len(page)), nil
		}
		limit *= 2
	}
}
