package server

import (
	"context"
	"testing"
	"time"

	"github.com/niqbal/workflow-engine/internal/clock"
	"github.com/niqbal/workflow-engine/internal/history"
	"github.com/niqbal/workflow-engine/internal/server/api"
	"github.com/niqbal/workflow-engine/internal/store"
)

func manualClock(t *testing.T, s *Server) *clock.Manual {
	t.Helper()
	c, ok := s.clk.(*clock.Manual)
	if !ok {
		t.Fatalf("server clock is %T, want *clock.Manual", s.clk)
	}
	return c
}

func dueTimers(t *testing.T, s *Server, st store.Store, upTo time.Time) []store.Task {
	t.Helper()
	var out []store.Task
	for _, shard := range s.Shards() {
		ts, err := st.ReadTimerTasks(context.Background(), shard, upTo, 100)
		if err != nil {
			t.Fatal(err)
		}
		out = append(out, ts...)
	}
	return out
}

// A user timer is not fired early, fires once at its time with its task deleted in the same
// write, and a later Settle writes nothing more.
func TestSettleFiresUserTimerOnceAtItsTime(t *testing.T) {
	s, st := newTestServer(t)
	clk := manualClock(t, s)
	ctx := context.Background()
	if _, err := s.StartWorkflow(ctx, startReq("wf", "r")); err != nil {
		t.Fatal(err)
	}
	task := pollNow(t, s)
	if err := s.CompleteWorkflowTask(ctx, api.CompleteWorkflowTaskRequest{TaskToken: task.TaskToken,
		Commands: []history.Command{history.StartTimer{TimerID: "t", Duration: time.Minute}}}); err != nil {
		t.Fatal(err)
	}
	before := historyTypes(t, s, "wf")

	clk.Advance(time.Minute - time.Nanosecond)
	s.Settle(ctx)
	if got := historyTypes(t, s, "wf"); !sameTypes(got, before) {
		t.Fatalf("timer fired early: %v", got)
	}
	if n := len(dueTimers(t, s, st, clk.Now().Add(time.Hour))); n != 1 {
		t.Fatalf("%d timer tasks stored, want 1", n)
	}

	clk.Advance(time.Nanosecond)
	s.Settle(ctx)
	want := append(before, history.EventTimerFired, history.EventWorkflowTaskScheduled)
	if got := historyTypes(t, s, "wf"); !sameTypes(got, want) {
		t.Fatalf("history %v, want %v", got, want)
	}
	if n := len(dueTimers(t, s, st, clk.Now().Add(time.Hour))); n != 0 {
		t.Fatalf("%d timer tasks left after firing, want 0", n)
	}
	// The new workflow task was pushed by the same Settle.
	if got := pollNow(t, s); got.WorkflowID != "wf" {
		t.Fatalf("polled %+v", got)
	}
	s.Settle(ctx)
	want = append(want, history.EventWorkflowTaskStarted)
	if got := historyTypes(t, s, "wf"); !sameTypes(got, want) {
		t.Fatalf("history %v, want %v", got, want)
	}
}

// A timer task whose run has closed is consumed without writing an event.
func TestSettleConsumesTimerOfClosedRun(t *testing.T) {
	s, st := newTestServer(t)
	clk := manualClock(t, s)
	ctx := context.Background()
	req := startReq("wf", "r")
	req.ExecutionTimeout = time.Hour
	if _, err := s.StartWorkflow(ctx, req); err != nil {
		t.Fatal(err)
	}
	if err := s.TerminateWorkflow(ctx, api.TerminateRequest{WorkflowID: "wf", Reason: "stop"}); err != nil {
		t.Fatal(err)
	}
	before := historyTypes(t, s, "wf")
	clk.Advance(time.Hour)
	s.Settle(ctx)
	if got := historyTypes(t, s, "wf"); !sameTypes(got, before) {
		t.Fatalf("closed run got events: %v, want %v", got, before)
	}
	if n := len(dueTimers(t, s, st, clk.Now())); n != 0 {
		t.Fatalf("%d timer tasks left, want 0", n)
	}
}

// A timer task that cannot be fired does not stop Settle from returning or from firing the
// tasks due after it; it stays stored for a later attempt.
func TestSettleSkipsTimerThatCannotFire(t *testing.T) {
	s, st := newTestServer(t)
	clk := manualClock(t, s)
	ctx := context.Background()
	req := startReq("wf", "r")
	req.ExecutionTimeout = time.Minute
	if _, err := s.StartWorkflow(ctx, req); err != nil {
		t.Fatal(err)
	}
	shard := s.ShardFor("wf")
	f, _ := s.fence(shard)
	// A run whose only timer task is due first but has an undecodable payload.
	bad := store.RunKey{Shard: shard, WorkflowID: "broken", RunID: "x"}
	if _, err := st.CreateRun(ctx, f, store.NewRun{Key: bad, Events: []store.EventRecord{{ID: 1, Data: []byte("{}")}},
		State: []byte("{}"), Tasks: []store.Task{{Shard: shard, Kind: store.TimerTask, FireAt: clk.Now(), Run: bad,
			Payload: []byte("not json")}}}); err != nil {
		t.Fatal(err)
	}
	clk.Advance(time.Minute)
	s.Settle(ctx)
	if got := historyTypes(t, s, "wf"); len(got) == 0 || got[len(got)-1] != history.EventWorkflowExecutionTimedOut {
		t.Fatalf("history %v, want it to end in a timeout", got)
	}
	left := dueTimers(t, s, st, clk.Now())
	if len(left) != 1 || left[0].Run != bad {
		t.Fatalf("timer tasks left %+v, want only the broken one", left)
	}
	s.Settle(ctx) // still returns
}
