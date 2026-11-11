package server

import (
	"context"
	"errors"
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

// WI-032 integration tests: a node killed at any point of a run and restarted over the same
// store finishes every run, and no timer fires twice.

const recoveryRuns = 4

func recoveryTimer(i int) time.Duration { return time.Duration(i+1) * 20 * time.Second }

// recoveryDecide is the workflow code: start a timer, then run one activity once it fires,
// then complete.
func recoveryDecide(evs []history.Event, timer time.Duration) []history.Command {
	has := map[history.EventType]bool{}
	for _, e := range evs {
		has[e.Type()] = true
	}
	switch {
	case has[history.EventActivityTaskCompleted]:
		return []history.Command{history.CompleteWorkflow{}}
	case has[history.EventTimerFired] && !has[history.EventActivityTaskScheduled]:
		return []history.Command{history.ScheduleActivity{ActivityID: "a", ActivityType: "A",
			StartToCloseTimeout: 30 * time.Second}}
	case !has[history.EventTimerStarted]:
		return []history.Command{history.StartTimer{TimerID: "t", Duration: timer}}
	}
	return nil
}

// recoveryNode is one incarnation of the node over the shared store.
func recoveryNode(t *testing.T, rt xsync.Runtime, st store.Store, seed uint64) *Server {
	t.Helper()
	s := New(rt, st, rnd.New(seed), Config{NumShards: 2})
	if err := s.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	return s
}

// takeTask starts the next startable task of kind from s's queue without waiting (the manual
// clock never reaches a long-poll deadline), skipping stale ones as the poll handlers do.
func takeTask[T any](t *testing.T, s *Server, kind matching.Kind,
	start func(context.Context, store.Task, string) (*T, error)) *T {
	t.Helper()
	for {
		tk, ok := s.match.Poll(kind, "q", time.Time{})
		if !ok {
			return nil
		}
		task, err := start(context.Background(), tk, "w")
		if errors.Is(err, api.ErrStaleTask) || errors.Is(err, api.ErrNotFound) {
			continue
		}
		if err != nil {
			t.Fatalf("start task %d: %v", tk.ID, err)
		}
		return task
	}
}

// recoveryStep settles s, then does one unit of worker work: start one workflow task or
// activity and, if deliver, report its result. With nothing to do it advances the clock.
func recoveryStep(t *testing.T, s *Server, clk *clock.Manual, deliver bool) {
	t.Helper()
	ctx := context.Background()
	s.Settle(ctx)
	if wt := takeTask(t, s, matching.WorkflowTasks, s.startWorkflowTask); wt != nil {
		if !deliver {
			return
		}
		var i int
		if _, err := fmt.Sscanf(wt.WorkflowID, "wf-%d", &i); err != nil {
			t.Fatal(err)
		}
		err := s.CompleteWorkflowTask(ctx, api.CompleteWorkflowTaskRequest{TaskToken: wt.TaskToken,
			Commands: recoveryDecide(wt.History, recoveryTimer(i))})
		if err != nil {
			t.Fatalf("complete workflow task of %s: %v", wt.WorkflowID, err)
		}
		return
	}
	if at := takeTask(t, s, matching.ActivityTasks, s.startActivityTask); at != nil {
		if !deliver {
			return
		}
		if err := s.CompleteActivityTask(ctx, api.CompleteActivityTaskRequest{TaskToken: at.TaskToken,
			Result: []byte(`"ok"`)}); err != nil {
			t.Fatalf("complete activity of %s: %v", at.WorkflowID, err)
		}
		return
	}
	clk.Advance(5 * time.Second)
}

func recoveryOpen(t *testing.T, s *Server) int {
	t.Helper()
	open := 0
	for i := 0; i < recoveryRuns; i++ {
		d, err := s.DescribeWorkflow(context.Background(), fmt.Sprintf("wf-%d", i))
		if err != nil {
			t.Fatal(err)
		}
		if d.Status.Open() {
			open++
		}
	}
	return open
}

// recoveryVersions returns every run's DBVersion.
func recoveryVersions(t *testing.T, s *Server, st store.Store) []int64 {
	t.Helper()
	ctx := context.Background()
	var out []int64
	for i := 0; i < recoveryRuns; i++ {
		id := fmt.Sprintf("wf-%d", i)
		key, err := st.GetLatestRun(ctx, s.ShardFor(id), id)
		if err != nil {
			t.Fatal(err)
		}
		snap, err := st.GetRun(ctx, key)
		if err != nil {
			t.Fatal(err)
		}
		out = append(out, snap.DBVersion)
	}
	return out
}

// recoveryHistories renders every run's event types.
func recoveryHistories(t *testing.T, s *Server) string {
	t.Helper()
	out := ""
	for i := 0; i < recoveryRuns; i++ {
		out += fmt.Sprintln(historyTypes(t, s, fmt.Sprintf("wf-%d", i)))
	}
	return out
}

// Kill the node after every possible number of worker steps, with the worker dying holding
// whatever task it took last, and restart it over the same store. The old incarnation keeps
// running its processors as a zombie. Every run completes on the new node, and every run has
// exactly one TimerFired for its one timer.
func TestRestartMidRunAllRunsCompleteNoDuplicateTimerFired(t *testing.T) {
	ctx := context.Background()
	midRun := 0
	const crashPoints = 45
	for crashAt := 0; crashAt < crashPoints; crashAt++ {
		clk := clock.NewManual(transferT0)
		rt := &xsync.Real{Ctx: context.Background(), C: clk}
		st := memstore.New(memstore.WithRuntime(rt))

		old := recoveryNode(t, rt, st, 1)
		for i := 0; i < recoveryRuns; i++ {
			if _, err := old.StartWorkflow(ctx, startReq(fmt.Sprintf("wf-%d", i), "r")); err != nil {
				t.Fatal(err)
			}
		}
		for step := 0; step < crashAt; step++ {
			recoveryStep(t, old, clk, true)
		}
		recoveryStep(t, old, clk, false) // the worker dies holding the task it just started
		if recoveryOpen(t, old) > 0 {
			midRun++
		}

		// Restart: a new incarnation of the node with nothing in memory.
		node := recoveryNode(t, rt, st, 2)
		for step := 0; recoveryOpen(t, node) > 0; step++ {
			if step == 500 {
				t.Fatalf("crash at %d: %d runs still open after %d steps on the restarted node",
					crashAt, recoveryOpen(t, node), step)
			}
			recoveryStep(t, node, clk, true)

			// The old incarnation is fenced: its processors write nothing.
			before := recoveryVersions(t, node, st)
			old.Settle(ctx)
			if after := recoveryVersions(t, node, st); fmt.Sprint(after) != fmt.Sprint(before) {
				t.Fatalf("crash at %d: fenced node wrote: DBVersions %v -> %v", crashAt, before, after)
			}
		}

		// Timeout timer tasks outlive the runs; firing them after close appends no event.
		closed := recoveryHistories(t, node)
		clk.Advance(time.Hour)
		node.Settle(ctx)
		if n := len(dueTimers(t, node, st, clk.Now().Add(24*time.Hour))); n != 0 {
			t.Fatalf("crash at %d: %d timer tasks left after every run completed", crashAt, n)
		}
		if after := recoveryHistories(t, node); after != closed {
			t.Fatalf("crash at %d: closed runs got events:\n%s\n->\n%s", crashAt, closed, after)
		}

		for i := 0; i < recoveryRuns; i++ {
			id := fmt.Sprintf("wf-%d", i)
			d, err := node.DescribeWorkflow(ctx, id)
			if err != nil || d.Status != history.StatusCompleted {
				t.Fatalf("crash at %d: %s: %+v, %v; want completed", crashAt, id, d, err)
			}
			evs, err := node.GetHistory(ctx, id)
			if err != nil {
				t.Fatal(err)
			}
			var started, fired []history.Event
			for _, e := range evs {
				switch e.Type() {
				case history.EventTimerStarted:
					started = append(started, e)
				case history.EventTimerFired:
					fired = append(fired, e)
				}
			}
			if len(started) != 1 || len(fired) != 1 {
				t.Fatalf("crash at %d: %s has %d TimerStarted and %d TimerFired, want 1 and 1: %v",
					crashAt, id, len(started), len(fired), eventTypes(evs))
			}
			if f := fired[0].Attrs.(history.TimerFired); f.StartedEventID != started[0].ID {
				t.Fatalf("crash at %d: %s fired timer of event %d, started %d", crashAt, id, f.StartedEventID, started[0].ID)
			}
			if got := fired[0].Time.Sub(transferT0); got < recoveryTimer(i) {
				t.Fatalf("crash at %d: %s timer fired early, %v after start", crashAt, id, got)
			}
		}
	}
	// Most crash points must actually land mid-run, or the test proves nothing.
	if midRun < crashPoints/2 {
		t.Fatalf("only %d of %d crash points left a run open", midRun, crashPoints)
	}
}

// A node that acquires its shards again after losing its matching queues reloads the transfer
// tasks from the store and pushes them at once, without waiting for the redispatch interval.
func TestRestartSameNodeReloadsTransferTasksOnShardLoad(t *testing.T) {
	ctx := context.Background()
	s, _, _ := newTransferServer(t, Config{NumShards: 2, RedispatchInterval: time.Hour})
	if _, err := s.StartWorkflow(ctx, startReq("wf", "r")); err != nil {
		t.Fatal(err)
	}
	s.Settle(ctx)
	if n := s.match.Len(matching.WorkflowTasks, "q"); n != 1 {
		t.Fatalf("queued %d, want 1", n)
	}

	s.DropQueuedTasks()
	s.Settle(ctx) // same epoch: already pushed, not due for redispatch
	if n := s.match.Len(matching.WorkflowTasks, "q"); n != 0 {
		t.Fatalf("re-pushed %d without a restart", n)
	}

	if err := s.Start(ctx); err != nil { // shard load
		t.Fatal(err)
	}
	s.Settle(ctx)
	got := drainQueue(s, matching.WorkflowTasks, "q")
	if len(got) != 1 || got[0].Run.WorkflowID != "wf" {
		t.Fatalf("after shard load queued %v, want the run's one workflow task", got)
	}
	s.Settle(ctx) // pushed once per epoch
	if n := s.match.Len(matching.WorkflowTasks, "q"); n != 0 {
		t.Fatalf("pushed again in the same epoch: %d", n)
	}
}
