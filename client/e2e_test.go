package client_test

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/niqbal/workflow-engine/client"
	"github.com/niqbal/workflow-engine/internal/clock"
	"github.com/niqbal/workflow-engine/internal/history"
	"github.com/niqbal/workflow-engine/internal/httpapi"
	"github.com/niqbal/workflow-engine/internal/rnd"
	"github.com/niqbal/workflow-engine/internal/server"
	"github.com/niqbal/workflow-engine/internal/server/api"
	"github.com/niqbal/workflow-engine/internal/server/matching"
	"github.com/niqbal/workflow-engine/internal/store/memstore"
	"github.com/niqbal/workflow-engine/internal/xsync"
)

// stack is a single-node server behind the HTTP API, with a client pointed at it. The server's
// clock is manual and never advanced, so nothing times out; tests call Settle to dispatch.
type stack struct {
	s *server.Server
	c *client.Client
}

func newStack(t *testing.T) *stack {
	t.Helper()
	clk := clock.NewManual(time.Date(2026, 11, 11, 12, 0, 0, 0, time.UTC))
	rt := &xsync.Real{Ctx: context.Background(), C: clk}
	s := server.New(rt, memstore.New(memstore.WithRuntime(rt)), rnd.New(7), server.Config{NumShards: 2})
	if err := s.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	srv := httptest.NewServer(httpapi.New(s.Service(), httpapi.Options{}))
	t.Cleanup(srv.Close)
	c := client.New(srv.URL, client.Options{Sleep: func(context.Context, time.Duration) error {
		return errors.New("unexpected wait: every call here should succeed at once")
	}})
	return &stack{s: s, c: c}
}

// pollWT dispatches and polls a workflow task that must already be due, so the long poll never
// waits on the (frozen) server clock.
func (st *stack) pollWT(t *testing.T, queue string) *api.WorkflowTask {
	t.Helper()
	st.s.Settle(context.Background())
	if st.s.Matcher().Len(matching.WorkflowTasks, queue) == 0 {
		t.Fatal("no workflow task queued")
	}
	task, err := st.c.PollWorkflowTask(context.Background(), api.PollRequest{TaskQueue: queue, Identity: "w1"})
	if err != nil || task == nil {
		t.Fatalf("poll workflow task: %v, %v", task, err)
	}
	return task
}

func (st *stack) pollAT(t *testing.T, queue string) *api.ActivityTask {
	t.Helper()
	st.s.Settle(context.Background())
	if st.s.Matcher().Len(matching.ActivityTasks, queue) == 0 {
		t.Fatal("no activity task queued")
	}
	task, err := st.c.PollActivityTask(context.Background(), api.PollRequest{TaskQueue: queue, Identity: "a1"})
	if err != nil || task == nil {
		t.Fatalf("poll activity task: %v, %v", task, err)
	}
	return task
}

func wantCode(t *testing.T, err error, sentinel error, status int) {
	t.Helper()
	if !errors.Is(err, sentinel) || api.Code(err) != sentinel.Error() {
		t.Fatalf("error %v, want %v", err, sentinel)
	}
	var ce *client.Error
	if !errors.As(err, &ce) || ce.StatusCode != status {
		t.Fatalf("error %#v, want HTTP %d", err, status)
	}
}

func types(evs []history.Event) []history.EventType {
	out := make([]history.EventType, len(evs))
	for i, e := range evs {
		out[i] = e.Type()
	}
	return out
}

func count(evs []history.Event, t history.EventType) int {
	n := 0
	for _, e := range evs {
		if e.Type() == t {
			n++
		}
	}
	return n
}

// TestEndToEndOverHTTP drives a workflow from start to result through the HTTP API: start
// with dedup, workflow task, activity, signal with dedup, completion, and Run.Get.
func TestEndToEndOverHTTP(t *testing.T) {
	ctx := context.Background()
	st := newStack(t)
	c := st.c
	start := api.StartRequest{WorkflowID: "order/1", WorkflowType: "Refund", TaskQueue: "q",
		Input: json.RawMessage(`{"amount":5}`), RequestID: "req-1"}
	run, err := c.ExecuteWorkflow(ctx, start)
	if err != nil || run.RunID == "" || run.WorkflowID != "order/1" {
		t.Fatalf("execute: %+v, %v", run, err)
	}
	// Same requestID: same run (what makes a retried start safe). Different: already_started.
	again, err := c.StartWorkflow(ctx, start)
	if err != nil || again.RunID != run.RunID {
		t.Fatalf("start retry: %+v, %v; want run %s", again, err, run.RunID)
	}
	other := start
	other.RequestID = "req-2"
	_, err = c.StartWorkflow(ctx, other)
	wantCode(t, err, api.ErrAlreadyStarted, http.StatusConflict)

	wt := st.pollWT(t, "q")
	if wt.WorkflowID != "order/1" || wt.RunID != run.RunID || wt.WorkflowType != "Refund" || len(wt.History) == 0 {
		t.Fatalf("workflow task %+v", wt)
	}
	if a, ok := wt.History[0].Attrs.(history.WorkflowExecutionStarted); !ok || string(a.Input) != `{"amount":5}` {
		t.Fatalf("first event %+v", wt.History[0])
	}
	act := history.ScheduleActivity{ActivityID: "charge", ActivityType: "Charge", Input: json.RawMessage(`5`),
		StartToCloseTimeout: 10 * time.Second, RetryPolicy: history.RetryPolicy{MaximumAttempts: 3}}
	done := api.CompleteWorkflowTaskRequest{TaskToken: wt.TaskToken, Commands: []history.Command{act}, Identity: "w1"}
	if err := c.CompleteWorkflowTask(ctx, done); err != nil {
		t.Fatal(err)
	}
	wantCode(t, c.CompleteWorkflowTask(ctx, done), api.ErrStaleTask, http.StatusConflict)

	d, err := run.Describe(ctx)
	if err != nil || d.Status != history.StatusRunning || len(d.PendingActivities) != 1 ||
		d.PendingActivities[0].ActivityID != "charge" {
		t.Fatalf("describe: %+v, %v", d, err)
	}

	// A signal while no workflow task is running, then its retry with the same requestID.
	for range 2 {
		if err := run.Signal(ctx, "approve", map[string]bool{"ok": true}, "sig-1"); err != nil {
			t.Fatal(err)
		}
	}

	at := st.pollAT(t, "q")
	if at.ActivityType != "Charge" || string(at.Input) != `5` || at.Attempt != 1 ||
		at.IdempotencyKey != "order/1/"+run.RunID+"/"+jsonInt(at.ScheduledEventID) {
		t.Fatalf("activity task %+v", at)
	}
	if err := c.CompleteActivityTask(ctx, api.CompleteActivityTaskRequest{TaskToken: at.TaskToken,
		Result: json.RawMessage(`"charged"`), Identity: "a1"}); err != nil {
		t.Fatal(err)
	}

	wt = st.pollWT(t, "q")
	if count(wt.History, history.EventWorkflowExecutionSignaled) != 1 || count(wt.History, history.EventActivityTaskCompleted) != 1 {
		t.Fatalf("second task history %v", types(wt.History))
	}
	if err := c.CompleteWorkflowTask(ctx, api.CompleteWorkflowTaskRequest{TaskToken: wt.TaskToken,
		Commands: []history.Command{history.CompleteWorkflow{Result: json.RawMessage(`{"status":"refunded"}`)}}}); err != nil {
		t.Fatal(err)
	}

	var out struct{ Status string }
	if err := run.Get(ctx, &out); err != nil || out.Status != "refunded" {
		t.Fatalf("get: %+v, %v", out, err)
	}
	if err := c.GetRun("order/1", "").Get(ctx, nil); err != nil {
		t.Fatalf("get latest without a result: %v", err)
	}

	// The original signal still dedups after close; a new one is rejected.
	if err := run.Signal(ctx, "approve", nil, "sig-1"); err != nil {
		t.Fatalf("dedup after close: %v", err)
	}
	wantCode(t, run.Signal(ctx, "late", nil, "sig-2"), api.ErrWorkflowClosed, http.StatusConflict)
	// Terminating a closed run is a no-op.
	if err := run.Terminate(ctx, "late"); err != nil {
		t.Fatal(err)
	}

	evs, err := run.History(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if evs[len(evs)-1].Type() != history.EventWorkflowExecutionCompleted || count(evs, history.EventWorkflowExecutionSignaled) != 1 {
		t.Fatalf("final history %v", types(evs))
	}
	for i, e := range evs {
		if e.ID != int64(i+1) {
			t.Fatalf("event IDs not contiguous: %v", types(evs))
		}
	}
}

func jsonInt(n int64) string {
	b, _ := json.Marshal(n)
	return string(b)
}

func TestErrorCodesFromARealServer(t *testing.T) {
	ctx := context.Background()
	st := newStack(t)
	c := st.c

	_, err := c.DescribeWorkflow(ctx, "missing")
	wantCode(t, err, api.ErrNotFound, http.StatusNotFound)
	_, err = c.GetHistory(ctx, "missing")
	wantCode(t, err, api.ErrNotFound, http.StatusNotFound)
	wantCode(t, c.GetRun("missing", "").Get(ctx, nil), api.ErrNotFound, http.StatusNotFound)
	wantCode(t, c.TerminateWorkflow(ctx, api.TerminateRequest{WorkflowID: "missing"}), api.ErrNotFound, http.StatusNotFound)

	_, err = c.StartWorkflow(ctx, api.StartRequest{WorkflowID: "wf", TaskQueue: "q"})
	wantCode(t, err, api.ErrInvalidArgument, http.StatusBadRequest)
	_, err = c.PollWorkflowTask(ctx, api.PollRequest{})
	wantCode(t, err, api.ErrInvalidArgument, http.StatusBadRequest)
	wantCode(t, c.CompleteActivityTask(ctx, api.CompleteActivityTaskRequest{TaskToken: "garbage"}),
		api.ErrInvalidArgument, http.StatusBadRequest)
}

func TestFailedWorkflowOverHTTP(t *testing.T) {
	ctx := context.Background()
	st := newStack(t)
	c := st.c
	run, err := c.ExecuteWorkflow(ctx, api.StartRequest{WorkflowID: "wf", WorkflowType: "W", TaskQueue: "qf", RequestID: "r"})
	if err != nil {
		t.Fatal(err)
	}
	wt := st.pollWT(t, "qf")
	if err := c.CompleteWorkflowTask(ctx, api.CompleteWorkflowTaskRequest{TaskToken: wt.TaskToken, Commands: []history.Command{
		history.ScheduleActivity{ActivityID: "a", ActivityType: "A", StartToCloseTimeout: time.Second}}}); err != nil {
		t.Fatal(err)
	}
	at := st.pollAT(t, "qf")
	fail := api.FailActivityTaskRequest{TaskToken: at.TaskToken,
		Failure: history.Failure{Type: "CardDeclined", Message: "no", NonRetryable: true}, Identity: "a1"}
	if err := c.FailActivityTask(ctx, fail); err != nil {
		t.Fatal(err)
	}
	wantCode(t, c.FailActivityTask(ctx, fail), api.ErrStaleTask, http.StatusConflict)

	wt = st.pollWT(t, "qf")
	var failed *history.ActivityTaskFailed
	for _, e := range wt.History {
		if a, ok := e.Attrs.(history.ActivityTaskFailed); ok {
			failed = &a
		}
	}
	if failed == nil {
		t.Fatalf("no ActivityTaskFailed in %v", types(wt.History))
	}
	if err := c.CompleteWorkflowTask(ctx, api.CompleteWorkflowTaskRequest{TaskToken: wt.TaskToken, Commands: []history.Command{
		history.FailWorkflow{Failure: history.Failure{Type: "Declined", Message: "card declined"}}}}); err != nil {
		t.Fatal(err)
	}

	err = run.Get(ctx, nil)
	var we *client.WorkflowError
	if !errors.As(err, &we) || we.Status != history.StatusFailed || we.Failure == nil || we.Failure.Type != "Declined" ||
		we.RunID != run.RunID {
		t.Fatalf("get: %v", err)
	}
}

func TestFailWorkflowTaskOverHTTP(t *testing.T) {
	ctx := context.Background()
	st := newStack(t)
	if _, err := st.c.StartWorkflow(ctx, api.StartRequest{WorkflowID: "wf", WorkflowType: "W", TaskQueue: "qw"}); err != nil {
		t.Fatal(err)
	}
	wt := st.pollWT(t, "qw")
	fail := api.FailWorkflowTaskRequest{TaskToken: wt.TaskToken, Cause: history.Failure{Type: "Panic", Message: "nil map"}, Identity: "w1"}
	if err := st.c.FailWorkflowTask(ctx, fail); err != nil {
		t.Fatal(err)
	}
	wantCode(t, st.c.FailWorkflowTask(ctx, fail), api.ErrStaleTask, http.StatusConflict)
}

func TestTerminatedWorkflowOverHTTP(t *testing.T) {
	ctx := context.Background()
	st := newStack(t)
	run, err := st.c.ExecuteWorkflow(ctx, api.StartRequest{WorkflowID: "wf", WorkflowType: "W", TaskQueue: "qt"})
	if err != nil {
		t.Fatal(err)
	}
	if err := run.Terminate(ctx, "enough"); err != nil {
		t.Fatal(err)
	}
	err = run.Get(ctx, nil)
	var we *client.WorkflowError
	if !errors.As(err, &we) || we.Status != history.StatusTerminated || we.Reason != "enough" {
		t.Fatalf("get: %v", err)
	}
	// A handle on another run of the same workflow ID does not report this run.
	if err := st.c.GetRun("wf", "some-other-run").Get(ctx, nil); err == nil || errors.As(err, &we) {
		t.Fatalf("stale handle: %v", err)
	}
}
