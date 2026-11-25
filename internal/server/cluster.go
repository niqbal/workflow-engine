package server

// WI-050: running as one node of a cluster.
//
// Start acquires every shard, which is right for a single node and wrong for a cluster.
// StartCluster instead lets placement decide: this node takes its even share of the shards,
// renews their leases, picks up a dead node's shards once their leases lapse, and gives up a
// shard it has not managed to renew before the stop-serving margin.
//
// None of that is what makes concurrent writes safe. Every write carries the fence it was
// acquired under and the store checks it inside the transaction, so a node with a paused
// process or a slow clock can do nothing harmful whatever it believes about ownership. The
// leases decide only who tries, and the margin only keeps a node from wasting work it will
// not be allowed to commit.

import (
	"context"
	"errors"
	"fmt"

	"github.com/niqbal/workflow-engine/internal/server/api"
	"github.com/niqbal/workflow-engine/internal/shard"
	"github.com/niqbal/workflow-engine/internal/store"
)

// StartCluster takes this node's share of the shards and returns. Run starts the processing
// loops, and RunShards keeps ownership up to date; a cluster node calls all three.
func (s *Server) StartCluster(ctx context.Context) error {
	if _, err := s.shards.Tick(ctx); err != nil {
		return fmt.Errorf("server: shard placement: %w", err)
	}
	s.refreshShardGaugeMetric()
	s.refreshPendingTimersMetric()
	return nil
}

// RunShards starts the shard loop: one placement pass every interval, until ctx is done or
// StopShards is called. It returns once the loop is running.
func (s *Server) RunShards(ctx context.Context) error {
	sl := &shardLoop{wake: newWakeup(s.rt)}
	if !s.sub.shardLoop.CompareAndSwap(nil, sl) {
		return errors.New("server: the shard loop is already running")
	}
	sl.unwatch = context.AfterFunc(ctx, sl.wake.stop)
	s.rt.Go("shards", func(context.Context) { s.shardLoopRun(ctx, sl) })
	return nil
}

// StopShards ends the shard loop. The node keeps the shards it holds: their leases simply
// stop being renewed, so a peer may take them over once they lapse. Stopping without having
// started is a no-op.
func (s *Server) StopShards() {
	sl := s.sub.shardLoop.Load()
	if sl == nil {
		return
	}
	sl.unwatch()
	sl.wake.stop()
	s.sub.shardLoop.CompareAndSwap(sl, nil)
}

// shardLoop is the running placement loop.
type shardLoop struct {
	wake    *wakeup
	unwatch func() bool
}

func (s *Server) shardLoopRun(ctx context.Context, sl *shardLoop) {
	for {
		ch, err := s.shards.Tick(ctx)
		if err != nil {
			// A pass that could not renew or place everything is normal under a store
			// fault: the margin decides what happens if it keeps failing.
			s.metrics().IncShardPlacementError()
		}
		if ch.Any() {
			s.onShardChanges(ch)
		}
		if !sl.wake.wait(s.clk.Now().Add(s.shards.Config().Interval())) {
			return
		}
	}
}

// onShardChanges reacts to ownership moving. An acquired shard's loops are rung so they scan
// at once rather than waiting out their poll; a dropped shard needs nothing, because its
// loops find no fence and go back to waiting, and its queued tasks are rebuilt by whoever
// takes the shard over (SPEC §8.2-8.3).
func (s *Server) onShardChanges(ch shard.Changes) {
	ls := s.sub.loops.Load()
	for _, id := range ch.Acquired {
		if ls == nil {
			continue
		}
		if w, ok := ls.transfer[id]; ok {
			w.ring()
		}
		if w, ok := ls.timers[id]; ok {
			w.ring()
		}
	}
	if len(ch.Dropped) > 0 {
		// The in-memory queues of a shard we no longer own are stale; whoever takes it over
		// rebuilds them from the outbox (SPEC §8.2-8.3).
		s.match.Drop()
	}
	s.refreshShardGaugeMetric()
}

// lostShard handles a write its fence rejected.
//
// In a cluster the rejection is proof that another node has taken the shard: the node stops
// serving it at once (SPEC §9, "its writes fail with ErrFenced, and it drops the shard") and
// the placement loop is woken, which takes the shard back if it turns out to be free.
//
// When nothing is renewing these leases — a single node that owns every shard, or a pinned
// test layout — there is no peer that could have taken anything, so a rejected write is a
// store anomaly to count rather than ownership to give up. Dropping there would strand the
// shard with nothing to re-place it.
func (s *Server) lostShard(shardID int, err error) {
	if !errors.Is(err, store.ErrFenced) {
		return
	}
	s.metrics().IncErrFenced()
	if !s.shards.Renewing() {
		return
	}
	if s.shards.Invalidate(shardID) {
		s.refreshShardGaugeMetric()
		// The in-memory queues of a shard we no longer own are stale; whoever takes it over
		// rebuilds them from the outbox.
		s.match.Drop()
		if sl := s.sub.shardLoop.Load(); sl != nil {
			sl.wake.ring()
		}
	}
}

// NotOwner is the error a frontend returns for a shard this node does not own, so a client or
// a peer knows to look the owner up again (WI-051).
func notOwner(shardID int) error {
	return fmt.Errorf("%w: shard %d", api.ErrNotOwner, shardID)
}
