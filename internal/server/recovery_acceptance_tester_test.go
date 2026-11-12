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

// Independent tester acceptance check for WI-032:
// restart a node mid-run multiple times and verify all runs complete with no duplicate TimerFired.
func TestTesterWI032RestartMidRunAllCompleteNoDuplicateTimerFired(t *testing.T) {
	ctx := context.Background()
	clk := clock.NewManual(transferT0)
	rt := &xsync.Real{Ctx: context.Background(), C: clk}
	st := memstore.New(memstore.WithRuntime(rt))

	node := recoveryNode(t, rt, st, 1)
	for i := 0; i < recoveryRuns; i++ {
		if _, err := node.StartWorkflow(ctx, startReq(fmt.Sprintf("wf-%d", i), "r")); err != nil {
			t.Fatal(err)
		}
	}

	restartAfter := map[int]bool{2: true, 6: true, 11: true, 17: true}
	nextSeed := uint64(2)
	for step := 0; recoveryOpen(t, node) > 0; step++ {
		if step == 600 {
			t.Fatalf("%d runs still open after %d steps", recoveryOpen(t, node), step)
		}
		if restartAfter[step] {
			// Crash while holding a task, then downtime before restart.
			recoveryStep(t, node, clk, false)
			clk.Advance(23 * time.Second)
			node = recoveryNode(t, rt, st, nextSeed)
			nextSeed++
			continue
		}
		recoveryStep(t, node, clk, true)
	}

	// Process any remaining due timers after closure.
	clk.Advance(time.Hour)
	node.Settle(ctx)

	for i := 0; i < recoveryRuns; i++ {
		id := fmt.Sprintf("wf-%d", i)
		d, err := node.DescribeWorkflow(ctx, id)
		if err != nil || d.Status != history.StatusCompleted {
			t.Fatalf("%s: %+v, %v; want completed", id, d, err)
		}
		started, fired := testerTimerCounts(t, node, id)
		if started != 1 || fired != 1 {
			t.Fatalf("%s has %d TimerStarted and %d TimerFired; want 1 and 1: %v",
				id, started, fired, historyTypes(t, node, id))
		}
	}
}
