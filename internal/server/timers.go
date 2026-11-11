package server

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/niqbal/workflow-engine/internal/history"
	"github.com/niqbal/workflow-engine/internal/store"
	"github.com/niqbal/workflow-engine/internal/xsync"
)

// WI-021: the timer processor (SPEC §10).
//
// Each owned shard's due timer tasks (FireAt <= now) are read in (FireAt, ID) order and fired
// one by one through update: Machine.FireTimer applies the ref if it is still pending at the
// same attempt, and the timer task is deleted in the same write. A ref that is no longer
// pending is a no-op, so a re-fire after a crash writes no event (G5).

// timerBatch is the page size of one ReadTimerTasks call.
const timerBatch = 100

// timerLock returns the shard's timer-scan lock, creating it on first use.
func (s *Server) timerLock(shard int) xsync.Locker {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.sub.timers == nil {
		s.sub.timers = map[int]xsync.Locker{}
	}
	l, ok := s.sub.timers[shard]
	if !ok {
		l = s.rt.NewLocker()
		s.sub.timers[shard] = l
	}
	return l
}

// processTimers fires every timer task of shard that is due at the current time, including
// tasks that firing itself makes due, and returns how many it fired. A task that cannot be
// fired (its write fails) is skipped for the rest of this call and retried on the next one.
func (s *Server) processTimers(ctx context.Context, shard int) (int, error) {
	l := s.timerLock(shard)
	l.Lock()
	defer l.Unlock()

	now := s.clk.Now()
	failed := map[int64]bool{}
	fired := 0
	var firstErr error
	for {
		// Failed tasks are still stored and due, so read past them.
		page, err := s.st.ReadTimerTasks(ctx, shard, now, timerBatch+len(failed))
		if err != nil {
			return fired, storeErr(err)
		}
		progress := false
		for _, t := range page {
			if failed[t.ID] {
				continue
			}
			progress = true
			if err := s.fireTimer(ctx, t); err != nil {
				failed[t.ID] = true
				if firstErr == nil {
					firstErr = err
				}
				continue
			}
			fired++
		}
		if !progress {
			return fired, firstErr
		}
	}
}

// fireTimer applies one timer task to its run and deletes it in the same write.
func (s *Server) fireTimer(ctx context.Context, t store.Task) error {
	var p TimerPayload
	if err := json.Unmarshal(t.Payload, &p); err != nil {
		return fmt.Errorf("server: decode timer task %d on shard %d: %w", t.ID, t.Shard, err)
	}
	_, err := s.updateAndWake(ctx, t.Run, []store.TaskKey{t.Key()}, func(m *history.Machine) error {
		m.FireTimer(p.Ref)
		return nil
	})
	return err
}

// settleTimers fires every due timer task on every owned shard at the current time. It
// reports whether anything was fired.
func (s *Server) settleTimers(ctx context.Context) bool {
	did := false
	for _, shard := range s.Shards() {
		n, _ := s.processTimers(ctx, shard) // a failed task is retried on the next call
		if n > 0 {
			did = true
		}
	}
	return did
}
