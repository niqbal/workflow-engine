package server

import (
	"context"
	"encoding/json"
	"slices"
	"testing"
	"time"

	"github.com/niqbal/workflow-engine/internal/clock"
	"github.com/niqbal/workflow-engine/internal/history"
	"github.com/niqbal/workflow-engine/internal/rnd"
	"github.com/niqbal/workflow-engine/internal/server/matching"
	"github.com/niqbal/workflow-engine/internal/simrt"
	"github.com/niqbal/workflow-engine/internal/store"
	"github.com/niqbal/workflow-engine/internal/store/memstore"
	"github.com/niqbal/workflow-engine/internal/xsync"
)

var transferT0 = time.Date(2026, 11, 11, 12, 0, 0, 0, time.UTC)

// readCountingStore counts ReadTransferTasks calls.
type readCountingStore struct {
	store.Store
	reads int
}

func (c *readCountingStore) ReadTransferTasks(ctx context.Context, shard int, fromID int64, limit int) ([]store.Task, error) {
	c.reads++
	return c.Store.ReadTransferTasks(ctx, shard, fromID, limit)
}

func newTransferServer(t *testing.T, cfg Config) (*Server, *readCountingStore, *clock.Manual) {
	t.Helper()
	clk := clock.NewManual(transferT0)
	rt := &xsync.Real{Ctx: context.Background(), C: clk}
	st := &readCountingStore{Store: memstore.New(memstore.WithRuntime(rt))}
	s := New(rt, st, rnd.New(3), cfg)
	if err := s.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	return s, st, clk
}

// drainQueue takes every task queued in one FIFO without waiting.
func drainQueue(s *Server, kind matching.Kind, queue string) []store.Task {
	var out []store.Task
	for {
		t, ok := s.match.Poll(kind, queue, time.Time{})
		if !ok {
			return out
		}
		out = append(out, t)
	}
}

func taskIDs(ts []store.Task) []int64 {
	ids := make([]int64, len(ts))
	for i, t := range ts {
		ids[i] = t.ID
	}
	return ids
}

// createTestRun writes a new run through the write path, bypassing the start lock so two
// creates on one shard can be in flight at once.
func createTestRun(ctx context.Context, s *Server, shard int, id string) error {
	key := store.RunKey{Shard: shard, WorkflowID: id, RunID: id + "-run"}
	st, out := history.NewRun(history.StartInput{WorkflowID: id, RunID: key.RunID, WorkflowType: "W", TaskQueue: "q"}, s.clk.Now())
	_, err := s.createRun(ctx, key, st, out)
	return err
}

// Task IDs are allocated before commit. A write that allocates ID 1 and commits late, after
// ID 2 has committed, been pushed and been deleted, must still be pushed (SPEC §8.1).
func TestTransferLateCommitOfEarlierIDIsNotSkipped(t *testing.T) {
	for seed := uint64(1); seed <= 16; seed++ {
		ctx := context.Background()
		rt := simrt.New(seed, transferT0)
		delayed := false
		st := memstore.New(memstore.WithRuntime(rt), memstore.WithFaults(memstore.Faults{
			CommitDelay: func(op string) time.Duration {
				if op == "CreateRun" && !delayed {
					delayed = true
					return 10 * time.Second
				}
				return 0
			},
		}))
		s := New(rt, st, rt.Rand(), Config{NumShards: 1})
		if err := s.Start(ctx); err != nil {
			t.Fatal(err)
		}

		var errs []error
		var first, afterDelete, late []store.Task
		var ackAfterDelete int64
		rt.Go("slow", func(ctx context.Context) {
			errs = append(errs, createTestRun(ctx, s, 0, "slow")) // allocates ID 1, commits at +10s
		})
		rt.Go("fast", func(ctx context.Context) {
			rt.Sleep(time.Second)
			errs = append(errs, createTestRun(ctx, s, 0, "fast")) // allocates ID 2, commits at +1s
		})
		rt.Go("processor", func(ctx context.Context) {
			rt.Sleep(2 * time.Second)
			s.Settle(ctx)
			first = drainQueue(s, matching.WorkflowTasks, "q")
			// Start and delete the fast run's task, as a poll handler would.
			f, _ := s.fence(0)
			var keys []store.TaskKey
			for _, tk := range first {
				keys = append(keys, tk.Key())
			}
			errs = append(errs, st.DeleteTasks(ctx, f, keys))
			s.Settle(ctx)
			afterDelete = drainQueue(s, matching.WorkflowTasks, "q")
			ackAfterDelete = s.transferShard(0).ack
			rt.Sleep(10 * time.Second)
			s.Settle(ctx)
			late = drainQueue(s, matching.WorkflowTasks, "q")
		})
		if res := rt.Run(time.Time{}, nil); res != simrt.Idle {
			t.Fatalf("seed %d: run ended with %v, tasks left: %s", seed, res, rt.Tasks())
		}
		rt.Close()

		for _, err := range errs {
			if err != nil {
				t.Fatalf("seed %d: %v", seed, err)
			}
		}
		if got := taskIDs(first); !slices.Equal(got, []int64{2}) {
			t.Fatalf("seed %d: first settle pushed %v, want [2]", seed, got)
		}
		if len(afterDelete) != 0 {
			t.Fatalf("seed %d: nothing is due after the delete, pushed %v", seed, taskIDs(afterDelete))
		}
		if ackAfterDelete != 1 {
			t.Fatalf("seed %d: ack level %d passed the uncommitted ID 1", seed, ackAfterDelete)
		}
		if got := taskIDs(late); !slices.Equal(got, []int64{1}) || late[0].Run.WorkflowID != "slow" {
			t.Fatalf("seed %d: late commit pushed %v, want the slow run's task 1", seed, late)
		}
	}
}

// The ack level moves past tasks once they are seen and deleted, and never past a present one.
func TestTransferAckLevelAdvancesOverDeletedTasks(t *testing.T) {
	ctx := context.Background()
	s, st, _ := newTransferServer(t, Config{NumShards: 1})
	for _, id := range []string{"a", "b", "c"} {
		if err := createTestRun(ctx, s, 0, id); err != nil {
			t.Fatal(err)
		}
	}
	s.Settle(ctx)
	pushed := drainQueue(s, matching.WorkflowTasks, "q")
	if got := taskIDs(pushed); !slices.Equal(got, []int64{1, 2, 3}) {
		t.Fatalf("pushed %v, want [1 2 3]", got)
	}
	f, _ := s.fence(0)
	del := func(ts ...store.Task) {
		t.Helper()
		var keys []store.TaskKey
		for _, tk := range ts {
			keys = append(keys, tk.Key())
		}
		if err := st.DeleteTasks(ctx, f, keys); err != nil {
			t.Fatal(err)
		}
	}
	if ack := s.transferShard(0).ack; ack != 1 {
		t.Fatalf("ack %d, want 1", ack)
	}
	del(pushed[0], pushed[2]) // 2 is still present: the ack level stops there
	s.Settle(ctx)
	if ack := s.transferShard(0).ack; ack != 2 {
		t.Fatalf("ack %d, want 2", ack)
	}
	del(pushed[1])
	s.Settle(ctx)
	if ack := s.transferShard(0).ack; ack != 4 {
		t.Fatalf("ack %d, want 4", ack)
	}
	if n := len(s.transferShard(0).pushed); n != 0 {
		t.Fatalf("%d deleted tasks still tracked", n)
	}
}

// A task lost from matching is pushed again once RedispatchInterval has passed on the server
// clock, and not a moment before (SPEC §8.2-8.3).
func TestTransferRedispatchAfterIntervalNotBefore(t *testing.T) {
	ctx := context.Background()
	const interval = 10 * time.Second
	s, _, clk := newTransferServer(t, Config{NumShards: 2, RedispatchInterval: interval})
	if _, err := s.StartWorkflow(ctx, startReq("wf", "r1")); err != nil {
		t.Fatal(err)
	}
	s.Settle(ctx)
	s.Settle(ctx) // already pushed: no duplicate at the same time
	if n := s.match.Len(matching.WorkflowTasks, "q"); n != 1 {
		t.Fatalf("queued %d tasks, want 1", n)
	}
	stored, err := s.st.ReadTransferTasks(ctx, s.ShardFor("wf"), 0, 10)
	if err != nil || len(stored) != 1 {
		t.Fatalf("stored transfer tasks %v, %v; want 1", stored, err)
	}
	original := stored[0]
	s.DropQueuedTasks() // lost from matching, as on a restart

	clk.Advance(interval - time.Nanosecond)
	s.Settle(ctx)
	if n := s.match.Len(matching.WorkflowTasks, "q"); n != 0 {
		t.Fatalf("re-pushed %d tasks before the redispatch interval", n)
	}

	clk.Advance(time.Nanosecond)
	s.Settle(ctx)
	again := drainQueue(s, matching.WorkflowTasks, "q")
	if len(again) != 1 || again[0].Key() != original.Key() || again[0].Run != original.Run {
		t.Fatalf("after the interval got %v, want the original task %v", again, original.Key())
	}
	s.Settle(ctx)
	if n := s.match.Len(matching.WorkflowTasks, "q"); n != 0 {
		t.Fatalf("re-pushed twice at one instant: %d queued", n)
	}

	// The next redispatch is one interval after the re-push.
	clk.Advance(interval)
	s.Settle(ctx)
	if n := s.match.Len(matching.WorkflowTasks, "q"); n != 1 {
		t.Fatalf("second redispatch queued %d, want 1", n)
	}
}

// A deleted (started) task is never redispatched.
func TestTransferDeletedTaskNotRedispatched(t *testing.T) {
	ctx := context.Background()
	s, st, clk := newTransferServer(t, Config{NumShards: 1})
	if _, err := s.StartWorkflow(ctx, startReq("wf", "r1")); err != nil {
		t.Fatal(err)
	}
	s.Settle(ctx)
	tk := drainQueue(s, matching.WorkflowTasks, "q")
	if len(tk) != 1 {
		t.Fatalf("pushed %d tasks, want 1", len(tk))
	}
	f, _ := s.fence(0)
	if err := st.DeleteTasks(ctx, f, []store.TaskKey{tk[0].Key()}); err != nil {
		t.Fatal(err)
	}
	clk.Advance(2 * s.Config().RedispatchInterval)
	s.Settle(ctx)
	if n := s.match.Len(matching.WorkflowTasks, "q"); n != 0 {
		t.Fatalf("deleted task redispatched: %d queued", n)
	}
}

// Settle returns once a pass pushes nothing: it routes activity tasks to the activity FIFO,
// does not spin on a task it cannot decode, and does no work beyond one scan when idle.
func TestSettleReturnsWhenNothingLeftToPush(t *testing.T) {
	ctx := context.Background()
	s, st, _ := newTransferServer(t, Config{NumShards: 1})

	s.Settle(ctx) // empty store
	if st.reads != 1 {
		t.Fatalf("idle settle read %d times, want 1", st.reads)
	}

	act, err := json.Marshal(TransferPayload{Dispatch: history.DispatchRef{Type: history.ActivityTaskType,
		TaskQueue: "acts", ScheduledEventID: 5, Attempt: 1}})
	if err != nil {
		t.Fatal(err)
	}
	key := store.RunKey{Shard: 0, WorkflowID: "raw", RunID: "raw-run"}
	f, _ := s.fence(0)
	_, err = st.CreateRun(ctx, f, store.NewRun{Key: key, Events: []store.EventRecord{{ID: 1, Data: []byte(`{}`)}},
		State: []byte(`{}`), Tasks: []store.Task{
			{Shard: 0, Kind: store.TransferTask, Run: key, Payload: act},
			{Shard: 0, Kind: store.TransferTask, Run: key, Payload: []byte(`not json`)},
			{Shard: 0, Kind: store.TransferTask, Run: key, Payload: []byte(`{"dispatch":{"type":"bogus"}}`)},
		}})
	if err != nil {
		t.Fatal(err)
	}

	st.reads = 0
	s.Settle(ctx)
	if st.reads != 2 {
		t.Fatalf("settle read %d times, want 2 (one pass that pushes, one that finds nothing)", st.reads)
	}
	got := drainQueue(s, matching.ActivityTasks, "acts")
	if len(got) != 1 || got[0].ID != 1 || got[0].Run != key {
		t.Fatalf("activity FIFO got %v, want task 1", got)
	}
	if n := s.match.Len(matching.WorkflowTasks, "acts"); n != 0 {
		t.Fatalf("activity task pushed to the workflow FIFO: %d", n)
	}

	st.reads = 0
	s.Settle(ctx)
	if st.reads != 1 || s.match.Len(matching.ActivityTasks, "acts") != 0 {
		t.Fatalf("settle with nothing due read %d times and queued %d", st.reads, s.match.Len(matching.ActivityTasks, "acts"))
	}
}
