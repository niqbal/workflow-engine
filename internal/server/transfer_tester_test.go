package server

import (
	"context"
	"fmt"
	"slices"
	"testing"
	"time"

	"github.com/niqbal/workflow-engine/internal/server/matching"
	"github.com/niqbal/workflow-engine/internal/simrt"
	"github.com/niqbal/workflow-engine/internal/store"
	"github.com/niqbal/workflow-engine/internal/store/memstore"
)

// Several writes commit in an order unrelated to their allocated task IDs while a processor
// keeps settling and deleting whatever it was given. Every task must be pushed exactly once
// (the redispatch interval is far beyond the test), whatever the commit order.
func TestTesterTransferManyOutOfOrderCommitsNoneSkipped(t *testing.T) {
	const n = 6
	for seed := uint64(1); seed <= 24; seed++ {
		ctx := context.Background()
		rt := simrt.New(seed, transferT0)
		calls := 0
		st := memstore.New(memstore.WithRuntime(rt), memstore.WithFaults(memstore.Faults{
			CommitDelay: func(op string) time.Duration {
				if op != "CreateRun" {
					return 0
				}
				i := calls
				calls++
				return time.Duration((int(seed)*7+i*3)%5) * time.Second
			},
		}))
		s := New(rt, st, rt.Rand(), Config{NumShards: 1, RedispatchInterval: time.Hour})
		if err := s.Start(ctx); err != nil {
			t.Fatal(err)
		}
		var errs []error
		for i := 0; i < n; i++ {
			id := fmt.Sprintf("w%d", i)
			delay := time.Duration(i) * 100 * time.Millisecond
			rt.Go(id, func(ctx context.Context) {
				rt.Sleep(delay)
				errs = append(errs, createTestRun(ctx, s, 0, id))
			})
		}
		var got []store.Task
		rt.Go("processor", func(ctx context.Context) {
			for step := 0; step < 40; step++ {
				s.Settle(ctx)
				batch := drainQueue(s, matching.WorkflowTasks, "q")
				got = append(got, batch...)
				if len(batch) > 0 {
					f, _ := s.fence(0)
					var keys []store.TaskKey
					for _, tk := range batch {
						keys = append(keys, tk.Key())
					}
					errs = append(errs, st.DeleteTasks(ctx, f, keys))
				}
				rt.Sleep(250 * time.Millisecond)
			}
		})
		if res := rt.Run(time.Time{}, nil); res != simrt.Idle {
			t.Fatalf("seed %d: run ended with %v", seed, res)
		}
		rt.Close()
		for _, err := range errs {
			if err != nil {
				t.Fatalf("seed %d: %v", seed, err)
			}
		}
		ids := taskIDs(got)
		slices.Sort(ids)
		want := []int64{1, 2, 3, 4, 5, 6}
		if !slices.Equal(ids, want) {
			t.Fatalf("seed %d: pushed IDs %v, want each of %v exactly once", seed, ids, want)
		}
		wfs := map[string]bool{}
		for _, tk := range got {
			wfs[tk.Run.WorkflowID] = true
		}
		if len(wfs) != n {
			t.Fatalf("seed %d: tasks for %d workflows, want %d", seed, len(wfs), n)
		}
	}
}

// More tasks than one read page are all pushed in a single Settle, in ID order, and the ack
// level stops at the lowest one still present.
func TestTesterTransferPagesBeyondBatch(t *testing.T) {
	ctx := context.Background()
	s, st, _ := newTransferServer(t, Config{NumShards: 1})
	const n = 2*transferBatch + 50
	for i := 0; i < n; i++ {
		if err := createTestRun(ctx, s, 0, fmt.Sprintf("p%d", i)); err != nil {
			t.Fatal(err)
		}
	}
	s.Settle(ctx)
	got := drainQueue(s, matching.WorkflowTasks, "q")
	if len(got) != n {
		t.Fatalf("pushed %d tasks, want %d", len(got), n)
	}
	for i, tk := range got {
		if tk.ID != int64(i+1) {
			t.Fatalf("task %d has ID %d, want %d", i, tk.ID, i+1)
		}
	}
	f, _ := s.fence(0)
	var keys []store.TaskKey
	for _, tk := range got[:150] {
		if tk.ID != 120 {
			keys = append(keys, tk.Key())
		}
	}
	if err := st.DeleteTasks(ctx, f, keys); err != nil {
		t.Fatal(err)
	}
	s.Settle(ctx)
	if ack := s.transferShard(0).ack; ack != 120 {
		t.Fatalf("ack %d, want 120", ack)
	}
	if n := s.match.Len(matching.WorkflowTasks, "q"); n != 0 {
		t.Fatalf("re-pushed %d tasks before the interval", n)
	}
}

// Each task's redispatch time counts from its own push, not from the latest scan.
func TestTesterTransferRedispatchPerTask(t *testing.T) {
	ctx := context.Background()
	const interval = 10 * time.Second
	s, _, clk := newTransferServer(t, Config{NumShards: 1, RedispatchInterval: interval})
	if err := createTestRun(ctx, s, 0, "a"); err != nil {
		t.Fatal(err)
	}
	s.Settle(ctx)
	clk.Advance(4 * time.Second)
	if err := createTestRun(ctx, s, 0, "b"); err != nil {
		t.Fatal(err)
	}
	s.Settle(ctx)
	if got := taskIDs(drainQueue(s, matching.WorkflowTasks, "q")); !slices.Equal(got, []int64{1, 2}) {
		t.Fatalf("pushed %v, want [1 2]", got)
	}
	clk.Advance(6 * time.Second) // a due, b not
	s.Settle(ctx)
	if got := taskIDs(drainQueue(s, matching.WorkflowTasks, "q")); !slices.Equal(got, []int64{1}) {
		t.Fatalf("at +10s re-pushed %v, want [1]", got)
	}
	clk.Advance(4*time.Second - time.Nanosecond)
	s.Settle(ctx)
	if got := taskIDs(drainQueue(s, matching.WorkflowTasks, "q")); len(got) != 0 {
		t.Fatalf("just before b's interval re-pushed %v", got)
	}
	clk.Advance(time.Nanosecond)
	s.Settle(ctx)
	if got := taskIDs(drainQueue(s, matching.WorkflowTasks, "q")); !slices.Equal(got, []int64{2}) {
		t.Fatalf("at +14s re-pushed %v, want [2]", got)
	}
}

// Settle covers every owned shard and returns once nothing is left.
func TestTesterSettleAllShards(t *testing.T) {
	ctx := context.Background()
	s, _, _ := newTransferServer(t, Config{NumShards: 4})
	for shard := 0; shard < 4; shard++ {
		if err := createTestRun(ctx, s, shard, fmt.Sprintf("s%d", shard)); err != nil {
			t.Fatal(err)
		}
	}
	s.Settle(ctx)
	got := drainQueue(s, matching.WorkflowTasks, "q")
	shards := map[int]bool{}
	for _, tk := range got {
		shards[tk.Shard] = true
	}
	if len(got) != 4 || len(shards) != 4 {
		t.Fatalf("pushed %d tasks over %d shards, want 4 over 4", len(got), len(shards))
	}
	s.Settle(ctx)
	if n := s.match.Len(matching.WorkflowTasks, "q"); n != 0 {
		t.Fatalf("second settle pushed %d", n)
	}
}
