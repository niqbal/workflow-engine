package server

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/niqbal/workflow-engine/internal/clock"
	"github.com/niqbal/workflow-engine/internal/history"
	"github.com/niqbal/workflow-engine/internal/rnd"
	"github.com/niqbal/workflow-engine/internal/server/api"
	"github.com/niqbal/workflow-engine/internal/server/matching"
	"github.com/niqbal/workflow-engine/internal/store"
	"github.com/niqbal/workflow-engine/internal/store/memstore"
	"github.com/niqbal/workflow-engine/internal/xsync"
)

// WI-032 edge cases (tester): timers due while the node is down, reload of more transfer
// tasks than one read batch, repeated restarts, and an ambiguous timer fire before a crash.

func countTimerFired(t *testing.T, s *Server, id string) int {
	t.Helper()
	evs, err := s.GetHistory(context.Background(), id)
	if err != nil {
		t.Fatal(err)
	}
	n := 0
	for _, e := range evs {
		if e.Type() == history.EventTimerFired {
			n++
		}
	}
	return n
}

// startTimerRun starts id on s and completes its first workflow task with one timer of d.
func startTimerRun(t *testing.T, s *Server, id string, d time.Duration) {
	t.Helper()
	ctx := context.Background()
	if _, err := s.StartWorkflow(ctx, startReq(id, "r-"+id)); err != nil {
		t.Fatal(err)
	}
	s.Settle(ctx)
	wt := takeTask(t, s, matching.WorkflowTasks, s.startWorkflowTask)
	if wt == nil {
		t.Fatal("no workflow task queued")
	}
	if err := s.CompleteWorkflowTask(ctx, api.CompleteWorkflowTaskRequest{TaskToken: wt.TaskToken,
		Commands: []history.Command{history.StartTimer{TimerID: "t", Duration: d}}}); err != nil {
		t.Fatal(err)
	}
}

// A timer that falls due while the node is down fires exactly once after restart, not before
// its time, and neither the fenced old node nor later restarts fire it again.
func TestRestartTimerDueDuringDowntimeFiresOnce(t *testing.T) {
	ctx := context.Background()
	clk := clock.NewManual(transferT0)
	rt := &xsync.Real{Ctx: context.Background(), C: clk}
	st := memstore.New(memstore.WithRuntime(rt))

	old := recoveryNode(t, rt, st, 1)
	startTimerRun(t, old, "wf", 30*time.Second)

	// Crash. Restart before the timer is due: nothing fires early.
	clk.Advance(10 * time.Second)
	node := recoveryNode(t, rt, st, 2)
	node.Settle(ctx)
	if n := countTimerFired(t, node, "wf"); n != 0 {
		t.Fatalf("timer fired early after restart: %d TimerFired", n)
	}

	// The timer falls due. The fenced old node must not fire it.
	clk.Advance(25 * time.Second)
	old.Settle(ctx)
	if n := countTimerFired(t, node, "wf"); n != 0 {
		t.Fatalf("fenced old node fired the timer: %d TimerFired", n)
	}
	node.Settle(ctx)
	if n := countTimerFired(t, node, "wf"); n != 1 {
		t.Fatalf("after restart %d TimerFired, want 1", n)
	}

	// Restart again, twice; settle every incarnation: still one.
	for i := uint64(3); i <= 4; i++ {
		next := recoveryNode(t, rt, st, i)
		clk.Advance(time.Minute)
		old.Settle(ctx)
		node.Settle(ctx)
		next.Settle(ctx)
		node = next
	}
	if n := countTimerFired(t, node, "wf"); n != 1 {
		t.Fatalf("after repeated restarts %d TimerFired, want 1: %v", n, historyTypes(t, node, "wf"))
	}
}

// The timer fire commits but the caller sees an error (ambiguous commit), then the node dies.
// The restarted node must not fire the timer a second time.
func TestRestartAfterAmbiguousTimerFireNoDuplicate(t *testing.T) {
	ctx := context.Background()
	clk := clock.NewManual(transferT0)
	rt := &xsync.Real{Ctx: context.Background(), C: clk}
	armed, hits := false, 0
	st := memstore.New(memstore.WithRuntime(rt), memstore.WithFaults(memstore.Faults{
		AfterCommit: func(op string) error {
			if armed && op == "UpdateRun" {
				hits++
				return errInjected
			}
			return nil
		},
	}))

	old := recoveryNode(t, rt, st, 1)
	startTimerRun(t, old, "wf", 30*time.Second)
	clk.Advance(31 * time.Second)
	armed = true
	old.Settle(ctx)
	armed = false
	if hits == 0 {
		t.Fatal("the timer fire was never attempted on the old node")
	}

	node := recoveryNode(t, rt, st, 2)
	node.Settle(ctx)
	clk.Advance(time.Minute)
	node.Settle(ctx)
	if n := countTimerFired(t, node, "wf"); n != 1 {
		t.Fatalf("%d TimerFired after ambiguous fire and restart, want 1: %v", n, historyTypes(t, node, "wf"))
	}
}

// A restarted node reloads every transfer task of a shard even when there are more than one
// read batch of them, and queues each exactly once.
func TestRestartReloadsMoreTransferTasksThanOneBatch(t *testing.T) {
	ctx := context.Background()
	clk := clock.NewManual(transferT0)
	rt := &xsync.Real{Ctx: context.Background(), C: clk}
	st := memstore.New(memstore.WithRuntime(rt))
	cfg := Config{NumShards: 1, RedispatchInterval: time.Hour}

	old := New(rt, st, rnd.New(1), cfg)
	if err := old.Start(ctx); err != nil {
		t.Fatal(err)
	}
	const runs = 2*transferBatch + 17
	for i := 0; i < runs; i++ {
		if _, err := old.StartWorkflow(ctx, startReq(fmt.Sprintf("wf-%d", i), fmt.Sprintf("r-%d", i))); err != nil {
			t.Fatal(err)
		}
	}
	old.Settle(ctx)

	for incarnation := uint64(2); incarnation <= 3; incarnation++ {
		node := New(rt, st, rnd.New(incarnation), cfg)
		if err := node.Start(ctx); err != nil {
			t.Fatal(err)
		}
		node.Settle(ctx)
		got := drainQueue(node, matching.WorkflowTasks, "q")
		seen := map[int64]bool{}
		for _, tk := range got {
			if seen[tk.ID] {
				t.Fatalf("incarnation %d: task %d queued twice", incarnation, tk.ID)
			}
			seen[tk.ID] = true
		}
		if len(seen) != runs {
			t.Fatalf("incarnation %d: reloaded %d workflow tasks, want %d", incarnation, len(seen), runs)
		}
		node.Settle(ctx)
		if n := node.match.Len(matching.WorkflowTasks, "q"); n != 0 {
			t.Fatalf("incarnation %d: pushed %d again in the same epoch", incarnation, n)
		}
	}
}

// The same node re-acquiring its shards several times re-pushes its queued tasks on each
// shard load, and a task started in between is not re-pushed.
func TestRestartSameNodeRepeatedShardLoads(t *testing.T) {
	ctx := context.Background()
	s, _, _ := newTransferServer(t, Config{NumShards: 2, RedispatchInterval: time.Hour})
	for _, id := range []string{"a", "b"} {
		if _, err := s.StartWorkflow(ctx, startReq(id, "r-"+id)); err != nil {
			t.Fatal(err)
		}
	}
	s.Settle(ctx)
	for round := 0; round < 3; round++ {
		s.DropQueuedTasks()
		if err := s.Start(ctx); err != nil {
			t.Fatal(err)
		}
		s.Settle(ctx)
		if n := s.match.Len(matching.WorkflowTasks, "q"); n != 2 {
			t.Fatalf("round %d: queued %d after shard load, want 2", round, n)
		}
	}
	// Start one task: its transfer task is deleted and must not come back on a later load.
	if wt := takeTask(t, s, matching.WorkflowTasks, s.startWorkflowTask); wt == nil {
		t.Fatal("no task to start")
	}
	s.DropQueuedTasks()
	if err := s.Start(ctx); err != nil {
		t.Fatal(err)
	}
	s.Settle(ctx)
	if got := drainQueue(s, matching.WorkflowTasks, "q"); len(got) != 1 {
		t.Fatalf("after starting one task and reloading, queued %v, want the other one", taskIDs(got))
	}
}

var _ store.Store = (*memstore.Store)(nil)
