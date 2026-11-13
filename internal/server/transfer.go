package server

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/niqbal/workflow-engine/internal/history"
	"github.com/niqbal/workflow-engine/internal/server/matching"
	"github.com/niqbal/workflow-engine/internal/store"
	"github.com/niqbal/workflow-engine/internal/xsync"
)

// WI-020: the transfer processor (SPEC §8.1 outbox reading, §8.2-8.3 redispatch).
//
// Each owned shard's transfer tasks are read from the ack level and pushed to matching. The
// processor never deletes a transfer task: the poll handlers do that through update's done
// tasks when they start the task (WI-018/019).

// transferBatch is the page size of one ReadTransferTasks call.
const transferBatch = 100

// idRange is an inclusive range of task IDs.
type idRange struct{ lo, hi int64 }

// transferShard is one shard's in-memory transfer processor state.
//
// Task IDs are allocated before commit, so an ID the processor has never seen may still
// commit later (SPEC §8.1). The ack level therefore never passes an unseen ID: it is the
// lowest ID that is still present or was skipped over without being seen (a gap). Gaps
// close when their IDs show up. A gap left by an aborted write never closes and only keeps
// the ack level low, which is harmless: the store returns undeleted tasks only.
type transferShard struct {
	mu     xsync.Locker        // one scan at a time
	fence  store.Fence         // ownership epoch the state below was built under (recovery.go)
	ack    int64               // scans read from here
	seen   int64               // highest ID ever read
	gaps   []idRange           // unseen IDs below seen, ascending and disjoint
	pushed map[int64]time.Time // pushed and not yet seen deleted -> earliest re-push
}

// transferShard returns the shard's processor state, creating it on first use.
func (s *Server) transferShard(shard int) *transferShard {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.sub.transfer == nil {
		s.sub.transfer = map[int]*transferShard{}
	}
	ts, ok := s.sub.transfer[shard]
	if !ok {
		ts = &transferShard{mu: s.rt.NewLocker(), ack: 1, pushed: map[int64]time.Time{}}
		s.sub.transfer[shard] = ts
	}
	return ts
}

// processTransfer runs one scan of a shard's transfer tasks at the current time. It pushes
// every task not pushed before and every task whose redispatch time has come, and returns
// how many it pushed.
func (s *Server) processTransfer(ctx context.Context, shard int) (int, error) {
	ts := s.transferShard(shard)
	ts.mu.Lock()
	defer ts.mu.Unlock()
	s.loadTransferShard(shard, ts)

	var present []store.Task
	from := ts.ack
	for {
		page, err := s.st.ReadTransferTasks(ctx, shard, from, transferBatch)
		if err != nil {
			return 0, storeErr(err)
		}
		present = append(present, page...)
		if len(page) < transferBatch {
			break
		}
		from = page[len(page)-1].ID + 1
	}

	now := s.clk.Now()
	live := make(map[int64]bool, len(present))
	pushed := 0
	var firstErr error
	for _, t := range present {
		live[t.ID] = true
		ts.observe(t.ID)
		redispatch := false
		if due, ok := ts.pushed[t.ID]; ok {
			if now.Before(due) {
				continue
			}
			redispatch = true
		}
		// A task that cannot be pushed is retried at its redispatch time like any other.
		ts.pushed[t.ID] = now.Add(s.cfg.RedispatchInterval)
		if err := s.pushTransfer(t); err != nil {
			if firstErr == nil {
				firstErr = err
			}
			continue
		}
		if redispatch {
			s.metrics().IncRedispatch()
		}
		pushed++
	}
	for _, id := range xsync.SortedKeys(ts.pushed) {
		if !live[id] {
			delete(ts.pushed, id) // started and deleted by a poll handler
		}
	}
	ts.ack = ts.seen + 1
	if len(present) > 0 {
		ts.ack = min(ts.ack, present[0].ID)
	}
	if len(ts.gaps) > 0 {
		ts.ack = min(ts.ack, ts.gaps[0].lo)
	}
	return pushed, firstErr
}

// observe records that id has been read: it either extends what has been seen, opening a
// gap for any IDs skipped over, or closes the gap it was in (a late commit).
func (ts *transferShard) observe(id int64) {
	if id > ts.seen {
		if id > ts.seen+1 {
			ts.gaps = append(ts.gaps, idRange{ts.seen + 1, id - 1})
		}
		ts.seen = id
		return
	}
	for i, g := range ts.gaps {
		if id < g.lo || id > g.hi {
			continue
		}
		var rest []idRange
		if g.lo < id {
			rest = append(rest, idRange{g.lo, id - 1})
		}
		if id < g.hi {
			rest = append(rest, idRange{id + 1, g.hi})
		}
		ts.gaps = append(ts.gaps[:i:i], append(rest, ts.gaps[i+1:]...)...)
		return
	}
}

// pushTransfer adds a transfer task to the FIFO its DispatchRef names. The task carries its
// key, so the poll handler can delete it when it starts the task.
func (s *Server) pushTransfer(t store.Task) error {
	var p TransferPayload
	if err := json.Unmarshal(t.Payload, &p); err != nil {
		return fmt.Errorf("server: decode transfer task %d on shard %d: %w", t.ID, t.Shard, err)
	}
	var kind matching.Kind
	switch p.Dispatch.Type {
	case history.WorkflowTaskType:
		kind = matching.WorkflowTasks
	case history.ActivityTaskType:
		kind = matching.ActivityTasks
	default:
		return fmt.Errorf("server: transfer task %d on shard %d has unknown type %q", t.ID, t.Shard, p.Dispatch.Type)
	}
	s.match.Add(kind, p.Dispatch.TaskQueue, t)
	return nil
}

// settleTransfer scans every owned shard until a full pass pushes nothing at the current
// time. It reports whether anything was pushed.
func (s *Server) settleTransfer(ctx context.Context) bool {
	did := false
	for {
		n := 0
		for _, shard := range s.Shards() {
			k, _ := s.processTransfer(ctx, shard) // a failed scan is retried on the next call
			n += k
		}
		if n == 0 {
			return did
		}
		did = true
	}
}
