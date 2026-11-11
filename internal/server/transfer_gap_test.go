package server

import (
	"context"
	"slices"
	"testing"
	"time"

	"github.com/niqbal/workflow-engine/internal/server/matching"
	"github.com/niqbal/workflow-engine/internal/simrt"
	"github.com/niqbal/workflow-engine/internal/store"
	"github.com/niqbal/workflow-engine/internal/store/memstore"
)

// A middle ID (2) commits after 1 and 3 have been pushed and deleted. The ack level holds at
// 2 while the gap is open, the late task is pushed once it commits, and the ack level then
// moves past all three.
func TestGapTransferMiddleIDCommitsLate(t *testing.T) {
	for seed := uint64(1); seed <= 8; seed++ {
		ctx := context.Background()
		rt := simrt.New(seed, transferT0)
		calls := 0
		st := memstore.New(memstore.WithRuntime(rt), memstore.WithFaults(memstore.Faults{
			CommitDelay: func(op string) time.Duration {
				if op != "CreateRun" {
					return 0
				}
				calls++
				if calls == 2 {
					return 10 * time.Second
				}
				return 0
			},
		}))
		s := New(rt, st, rt.Rand(), Config{NumShards: 1, RedispatchInterval: time.Hour})
		if err := s.Start(ctx); err != nil {
			t.Fatal(err)
		}
		var errs []error
		for i, id := range []string{"a", "b", "c"} {
			d := time.Duration(i) * 100 * time.Millisecond
			rt.Go(id, func(ctx context.Context) {
				rt.Sleep(d)
				errs = append(errs, createTestRun(ctx, s, 0, id))
			})
		}
		var first, late []store.Task
		var ackGap, ackEnd int64
		rt.Go("processor", func(ctx context.Context) {
			rt.Sleep(time.Second)
			s.Settle(ctx)
			first = drainQueue(s, matching.WorkflowTasks, "q")
			f, _ := s.fence(0)
			var keys []store.TaskKey
			for _, tk := range first {
				keys = append(keys, tk.Key())
			}
			errs = append(errs, st.DeleteTasks(ctx, f, keys))
			s.Settle(ctx)
			ackGap = s.transferShard(0).ack
			rt.Sleep(15 * time.Second)
			s.Settle(ctx)
			late = drainQueue(s, matching.WorkflowTasks, "q")
			keys = nil
			for _, tk := range late {
				keys = append(keys, tk.Key())
			}
			errs = append(errs, st.DeleteTasks(ctx, f, keys))
			s.Settle(ctx)
			ackEnd = s.transferShard(0).ack
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
		if got := taskIDs(first); !slices.Equal(got, []int64{1, 3}) {
			t.Fatalf("seed %d: first pushed %v, want [1 3]", seed, got)
		}
		if ackGap != 2 {
			t.Fatalf("seed %d: ack %d while ID 2 is uncommitted, want 2", seed, ackGap)
		}
		if got := taskIDs(late); !slices.Equal(got, []int64{2}) || late[0].Run.WorkflowID != "b" {
			t.Fatalf("seed %d: late pushed %v, want b's task 2", seed, got)
		}
		if ackEnd != 4 {
			t.Fatalf("seed %d: ack %d after all deleted, want 4", seed, ackEnd)
		}
	}
}

// A task that cannot be decoded is not re-attempted before the redispatch interval, and is
// re-attempted after it; Settle still returns each time.
func TestGapTransferUndecodableRetriedOnInterval(t *testing.T) {
	ctx := context.Background()
	const interval = 5 * time.Second
	s, st, clk := newTransferServer(t, Config{NumShards: 1, RedispatchInterval: interval})
	key := store.RunKey{Shard: 0, WorkflowID: "raw", RunID: "raw-run"}
	f, _ := s.fence(0)
	if _, err := st.CreateRun(ctx, f, store.NewRun{Key: key, Events: []store.EventRecord{{ID: 1, Data: []byte(`{}`)}},
		State: []byte(`{}`), Tasks: []store.Task{{Shard: 0, Kind: store.TransferTask, Run: key, Payload: []byte(`nope`)}}}); err != nil {
		t.Fatal(err)
	}
	s.Settle(ctx)
	if due := s.transferShard(0).pushed[1]; !due.Equal(transferT0.Add(interval)) {
		t.Fatalf("due %v, want %v", due, transferT0.Add(interval))
	}
	clk.Advance(interval - time.Nanosecond)
	s.Settle(ctx)
	if due := s.transferShard(0).pushed[1]; !due.Equal(transferT0.Add(interval)) {
		t.Fatalf("retried early: due %v", due)
	}
	clk.Advance(time.Nanosecond)
	s.Settle(ctx)
	if due := s.transferShard(0).pushed[1]; !due.Equal(transferT0.Add(2 * interval)) {
		t.Fatalf("not retried at interval: due %v", due)
	}
}
