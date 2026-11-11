package server

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/niqbal/workflow-engine/internal/clock"
	"github.com/niqbal/workflow-engine/internal/history"
	"github.com/niqbal/workflow-engine/internal/store/memstore"
	"github.com/niqbal/workflow-engine/internal/xsync"
)

// Tester's WI-032 checks: several restarts in a row, downtime that lets timers come due while
// no node is running, and timers that must not fire early after a restart.

func testerTimerCounts(t *testing.T, s *Server, id string) (started, fired int) {
	t.Helper()
	evs, err := s.GetHistory(context.Background(), id)
	if err != nil {
		t.Fatal(err)
	}
	for _, e := range evs {
		switch e.Type() {
		case history.EventTimerStarted:
			started++
		case history.EventTimerFired:
			fired++
		}
	}
	return started, fired
}

// The node is restarted several times. Between restarts the clock jumps (downtime), so timers
// come due while no node runs. Every old incarnation keeps settling as a zombie. All runs
// complete with exactly one TimerFired each.
func TestTesterRepeatedRestartsWithDowntime(t *testing.T) {
	ctx := context.Background()
	for _, gap := range []int{1, 2, 3, 5} {
		clk := clock.NewManual(transferT0)
		rt := &xsync.Real{Ctx: context.Background(), C: clk}
		st := memstore.New(memstore.WithRuntime(rt))

		nodes := []*Server{recoveryNode(t, rt, st, 1)}
		for i := 0; i < recoveryRuns; i++ {
			if _, err := nodes[0].StartWorkflow(ctx, startReq(fmt.Sprintf("wf-%d", i), "r")); err != nil {
				t.Fatal(err)
			}
		}
		cur := nodes[0]
		for step := 0; recoveryOpen(t, cur) > 0; step++ {
			if step == 600 {
				t.Fatalf("gap %d: %d runs still open after %d steps", gap, recoveryOpen(t, cur), step)
			}
			if step > 0 && step%gap == 0 && len(nodes) < 12 {
				// Crash: the worker dies holding a task, downtime passes, a new node starts.
				recoveryStep(t, cur, clk, false)
				clk.Advance(17 * time.Second)
				cur = recoveryNode(t, rt, st, uint64(len(nodes)+1))
				nodes = append(nodes, cur)
			}
			recoveryStep(t, cur, clk, true)
			for _, z := range nodes[:len(nodes)-1] {
				z.Settle(ctx) // zombies must not write
			}
		}
		clk.Advance(time.Hour)
		cur.Settle(ctx)
		for _, z := range nodes {
			z.Settle(ctx)
		}
		for i := 0; i < recoveryRuns; i++ {
			id := fmt.Sprintf("wf-%d", i)
			d, err := cur.DescribeWorkflow(ctx, id)
			if err != nil || d.Status != history.StatusCompleted {
				t.Fatalf("gap %d: %s: %+v, %v; want completed", gap, id, d, err)
			}
			if s, f := testerTimerCounts(t, cur, id); s != 1 || f != 1 {
				t.Fatalf("gap %d: %s has %d TimerStarted, %d TimerFired; want 1 and 1: %v",
					gap, id, s, f, historyTypes(t, cur, id))
			}
		}
	}
}

// A timer started before the crash is not fired early by the restarted node, fires exactly
// once when due, and settling again (on both nodes) adds nothing.
func TestTesterRestartPendingTimerFiresOnceOnTime(t *testing.T) {
	ctx := context.Background()
	clk := clock.NewManual(transferT0)
	rt := &xsync.Real{Ctx: context.Background(), C: clk}
	st := memstore.New(memstore.WithRuntime(rt))

	old := recoveryNode(t, rt, st, 1)
	if _, err := old.StartWorkflow(ctx, startReq("wf-0", "r")); err != nil {
		t.Fatal(err)
	}
	// One step: the first workflow task starts the 20s timer.
	recoveryStep(t, old, clk, true)
	if s, f := testerTimerCounts(t, old, "wf-0"); s != 1 || f != 0 {
		t.Fatalf("before crash: %d started, %d fired: %v", s, f, historyTypes(t, old, "wf-0"))
	}

	node := recoveryNode(t, rt, st, 2)
	clk.Advance(recoveryTimer(0) - time.Nanosecond)
	node.Settle(ctx)
	old.Settle(ctx)
	if _, f := testerTimerCounts(t, node, "wf-0"); f != 0 {
		t.Fatalf("timer fired early after restart: %v", historyTypes(t, node, "wf-0"))
	}
	clk.Advance(time.Nanosecond)
	old.Settle(ctx) // fenced zombie first: must not fire
	if _, f := testerTimerCounts(t, node, "wf-0"); f != 0 {
		t.Fatalf("fenced node fired the timer: %v", historyTypes(t, node, "wf-0"))
	}
	node.Settle(ctx)
	node.Settle(ctx)
	old.Settle(ctx)
	if _, f := testerTimerCounts(t, node, "wf-0"); f != 1 {
		t.Fatalf("want exactly one TimerFired: %v", historyTypes(t, node, "wf-0"))
	}

	// A third incarnation over the same store fires nothing more.
	third := recoveryNode(t, rt, st, 3)
	clk.Advance(time.Hour)
	third.Settle(ctx)
	if _, f := testerTimerCounts(t, third, "wf-0"); f != 1 {
		t.Fatalf("third incarnation re-fired: %v", historyTypes(t, third, "wf-0"))
	}
	// And it can finish the run.
	for step := 0; recoveryOpen1(t, third, "wf-0"); step++ {
		if step == 200 {
			t.Fatalf("run still open: %v", historyTypes(t, third, "wf-0"))
		}
		recoveryStep(t, third, clk, true)
	}
	if _, f := testerTimerCounts(t, third, "wf-0"); f != 1 {
		t.Fatalf("want exactly one TimerFired at the end: %v", historyTypes(t, third, "wf-0"))
	}
}

func recoveryOpen1(t *testing.T, s *Server, id string) bool {
	t.Helper()
	d, err := s.DescribeWorkflow(context.Background(), id)
	if err != nil {
		t.Fatal(err)
	}
	return d.Status.Open()
}
