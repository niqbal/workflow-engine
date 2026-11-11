// Package contracttest is the scenario suite for a single-node server (WI-005, SPEC §8).
// It is written before the server and owned by hand: server work items must pass it and may
// not change it. Every scenario talks only to api.Service plus the Harness controls.
package contracttest

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/niqbal/workflow-engine/internal/history"
	"github.com/niqbal/workflow-engine/internal/server/api"
)

// Harness wraps an in-process single-node server whose clock the test controls.
type Harness interface {
	Service() api.Service
	// Now returns the server clock.
	Now() time.Time
	// Settle runs transfer and timer processing at the current time until nothing is left to do.
	Settle()
	// Advance moves the server clock forward by d, firing every timer task that comes due
	// (in order, at its own time), then Settles.
	Advance(d time.Duration)
	// DropQueuedTasks discards everything in in-memory matching, as a node restart would.
	DropQueuedTasks()
}

// Factory builds a fresh harness (empty store) for one scenario.
type Factory func(t *testing.T) Harness

const q = "q"

// Run executes every scenario as a subtest.
func Run(t *testing.T, newHarness Factory) {
	for _, sc := range scenarios {
		t.Run(sc.name, func(t *testing.T) { sc.fn(t, newHarness(t)) })
	}
}

// Names lists the scenarios, in order.
func Names() []string {
	out := make([]string, len(scenarios))
	for i, sc := range scenarios {
		out[i] = sc.name
	}
	return out
}

// RunOne executes a single scenario. Servers wire each scenario to its own top-level test
// (see internal/server/contract_test.go) so gates compare results scenario by scenario.
func RunOne(t *testing.T, name string, newHarness Factory) {
	for _, sc := range scenarios {
		if sc.name == name {
			sc.fn(t, newHarness(t))
			return
		}
	}
	t.Fatalf("contracttest: no scenario %q", name)
}

var scenarios = []struct {
	name string
	fn   func(*testing.T, Harness)
}{
	{"StartRejectsMissingFields", startInvalid},
	{"StartIsIdempotentByRequestID", startIdempotent},
	{"StartDedupSurvivesClose", startDedupAfterClose},
	{"FirstWorkflowTaskCarriesHistory", firstWorkflowTask},
	{"ActivityRoundTrip", activityRoundTrip},
	{"StaleWorkflowTaskTokenRejected", staleWorkflowTaskToken},
	{"SignalAppendedWhileTaskScheduled", signalWhileScheduled},
	{"SignalSchedulesTaskWhenIdle", signalWhenIdle},
	{"SignalBufferedWhileTaskStarted", signalBuffered},
	{"CloseRejectedWhileEventsBuffered", closeRejectedWithBuffered},
	{"SignalDedupAndClosedRun", signalDedup},
	{"ActivityRetryWithBackoff", activityRetry},
	{"ActivityNonRetryableFailure", activityNonRetryable},
	{"ActivityAttemptsExhausted", activityExhausted},
	{"ActivityStartToCloseTimeoutAndStaleAttempt", activityTimeout},
	{"ActivityScheduleToCloseTimeout", activityScheduleToClose},
	{"UserTimerFiresOnceOnTime", userTimer},
	{"CanceledTimerNeverFires", canceledTimer},
	{"WorkflowTaskTimeoutRetries", workflowTaskTimeout},
	{"WorkflowTaskFailureSeriesWritesOneEvent", workflowTaskFailureSeries},
	{"LostWorkflowTaskRedispatched", redispatchWorkflowTask},
	{"LostActivityTaskRedispatched", redispatchActivityTask},
	{"ExecutionTimeoutClosesRun", executionTimeout},
	{"TerminateClosesRunAndDropsWork", terminate},
	{"DescribeShowsPendingWork", describe},
	{"InvalidCommandRejected", invalidCommand},
}

// ---- helpers ------------------------------------------------------------------------------

var bg = context.Background()

func fail(t *testing.T, format string, args ...any) {
	t.Helper()
	t.Fatalf(format, args...)
}

func start(t *testing.T, h Harness, id string, opts ...func(*api.StartRequest)) string {
	t.Helper()
	r := api.StartRequest{WorkflowID: id, WorkflowType: "W", TaskQueue: q, RequestID: "start-" + id, Input: json.RawMessage(`{"n":1}`)}
	for _, o := range opts {
		o(&r)
	}
	resp, err := h.Service().StartWorkflow(bg, r)
	if err != nil || resp.RunID == "" {
		fail(t, "start %s: %v (runID %q)", id, err, resp.RunID)
	}
	return resp.RunID
}

// poll helpers settle first, then wait briefly on a real-time context. Returning nil means
// the server has no task to hand out.
func pollWF(t *testing.T, h Harness) *api.WorkflowTask {
	t.Helper()
	h.Settle()
	ctx, cancel := context.WithTimeout(bg, 200*time.Millisecond)
	defer cancel()
	task, err := h.Service().PollWorkflowTask(ctx, api.PollRequest{TaskQueue: q, Identity: "w1"})
	if err != nil {
		fail(t, "poll workflow task: %v", err)
	}
	return task
}

func mustPollWF(t *testing.T, h Harness) *api.WorkflowTask {
	t.Helper()
	task := pollWF(t, h)
	if task == nil {
		fail(t, "expected a workflow task, got none")
	}
	return task
}

func pollAct(t *testing.T, h Harness) *api.ActivityTask {
	t.Helper()
	h.Settle()
	ctx, cancel := context.WithTimeout(bg, 200*time.Millisecond)
	defer cancel()
	task, err := h.Service().PollActivityTask(ctx, api.PollRequest{TaskQueue: q, Identity: "a1"})
	if err != nil {
		fail(t, "poll activity task: %v", err)
	}
	return task
}

func mustPollAct(t *testing.T, h Harness) *api.ActivityTask {
	t.Helper()
	task := pollAct(t, h)
	if task == nil {
		fail(t, "expected an activity task, got none")
	}
	return task
}

func complete(t *testing.T, h Harness, task *api.WorkflowTask, cmds ...history.Command) {
	t.Helper()
	if err := h.Service().CompleteWorkflowTask(bg, api.CompleteWorkflowTaskRequest{TaskToken: task.TaskToken, Commands: cmds, Identity: "w1"}); err != nil {
		fail(t, "complete workflow task: %v", err)
	}
}

func events(t *testing.T, h Harness, id string) []history.Event {
	t.Helper()
	evs, err := h.Service().GetHistory(bg, id)
	if err != nil {
		fail(t, "history %s: %v", id, err)
	}
	for i, e := range evs {
		if e.ID != int64(i+1) {
			fail(t, "history %s: event IDs not contiguous at index %d: %d", id, i, e.ID)
		}
	}
	return evs
}

func types(evs []history.Event) []history.EventType {
	out := make([]history.EventType, len(evs))
	for i, e := range evs {
		out[i] = e.Type()
	}
	return out
}

func show(ts []history.EventType) string {
	s := make([]string, len(ts))
	for i, x := range ts {
		s[i] = string(x)
	}
	return strings.Join(s, ", ")
}

// wantHistory asserts the full sequence of event types.
func wantHistory(t *testing.T, h Harness, id string, want ...history.EventType) []history.Event {
	t.Helper()
	evs := events(t, h, id)
	if got := types(evs); show(got) != show(want) {
		fail(t, "history of %s:\n got: %s\nwant: %s", id, show(got), show(want))
	}
	return evs
}

func wantErr(t *testing.T, err, target error) {
	t.Helper()
	if !errors.Is(err, target) {
		fail(t, "want error %v, got %v", target, err)
	}
}

func status(t *testing.T, h Harness, id string) history.RunStatus {
	t.Helper()
	d, err := h.Service().DescribeWorkflow(bg, id)
	if err != nil {
		fail(t, "describe %s: %v", id, err)
	}
	return d.Status
}

func sched(id string, mod ...func(*history.ScheduleActivity)) history.ScheduleActivity {
	c := history.ScheduleActivity{ActivityID: id, ActivityType: "A", Input: json.RawMessage(`"in"`),
		StartToCloseTimeout: 10 * time.Second,
		RetryPolicy:         history.RetryPolicy{InitialInterval: time.Second, BackoffCoefficient: 2, MaximumAttempts: 3, NonRetryableErrors: []string{"Fatal"}}}
	for _, m := range mod {
		m(&c)
	}
	return c
}

const (
	eStarted     = history.EventWorkflowExecutionStarted
	eWFTSched    = history.EventWorkflowTaskScheduled
	eWFTStarted  = history.EventWorkflowTaskStarted
	eWFTDone     = history.EventWorkflowTaskCompleted
	eWFTTimeout  = history.EventWorkflowTaskTimedOut
	eWFTFailed   = history.EventWorkflowTaskFailed
	eActSched    = history.EventActivityTaskScheduled
	eActStarted  = history.EventActivityTaskStarted
	eActDone     = history.EventActivityTaskCompleted
	eActFailed   = history.EventActivityTaskFailed
	eActTimeout  = history.EventActivityTaskTimedOut
	eTimerStart  = history.EventTimerStarted
	eTimerFired  = history.EventTimerFired
	eTimerCancel = history.EventTimerCanceled
	eSignaled    = history.EventWorkflowExecutionSignaled
	eCompleted   = history.EventWorkflowExecutionCompleted
	eTimedOut    = history.EventWorkflowExecutionTimedOut
	eTerminated  = history.EventWorkflowExecutionTerminated
)

// runToActivity starts a workflow and has its first task schedule one activity.
// History afterwards: Started, WFTScheduled, WFTStarted, WFTCompleted, ActivityTaskScheduled.
func runToActivity(t *testing.T, h Harness, id string, a history.ScheduleActivity) string {
	t.Helper()
	run := start(t, h, id)
	complete(t, h, mustPollWF(t, h), a)
	return run
}

// ---- scenarios ----------------------------------------------------------------------------

func startInvalid(t *testing.T, h Harness) {
	for _, r := range []api.StartRequest{
		{WorkflowType: "W", TaskQueue: q, RequestID: "r"},
		{WorkflowID: "x", TaskQueue: q, RequestID: "r"},
		{WorkflowID: "x", WorkflowType: "W", RequestID: "r"},
	} {
		_, err := h.Service().StartWorkflow(bg, r)
		wantErr(t, err, api.ErrInvalidArgument)
	}
	_, err := h.Service().GetHistory(bg, "missing")
	wantErr(t, err, api.ErrNotFound)
}

func startIdempotent(t *testing.T, h Harness) {
	run := start(t, h, "wf")
	again := start(t, h, "wf")
	if again != run {
		fail(t, "same requestID must return the same run: %s vs %s", run, again)
	}
	_, err := h.Service().StartWorkflow(bg, api.StartRequest{WorkflowID: "wf", WorkflowType: "W", TaskQueue: q, RequestID: "other"})
	wantErr(t, err, api.ErrAlreadyStarted)
	wantHistory(t, h, "wf", eStarted, eWFTSched)
}

func startDedupAfterClose(t *testing.T, h Harness) {
	run := start(t, h, "wf")
	complete(t, h, mustPollWF(t, h), history.CompleteWorkflow{Result: json.RawMessage(`1`)})
	if s := status(t, h, "wf"); s != history.StatusCompleted {
		fail(t, "want completed, got %s", s)
	}
	// The C4 case: the original response was lost and the run already finished.
	if again := start(t, h, "wf"); again != run {
		fail(t, "retry after close must return the original run %s, got %s", run, again)
	}
	_, err := h.Service().StartWorkflow(bg, api.StartRequest{WorkflowID: "wf", WorkflowType: "W", TaskQueue: q, RequestID: "new"})
	wantErr(t, err, api.ErrAlreadyStarted)
}

func firstWorkflowTask(t *testing.T, h Harness) {
	run := start(t, h, "wf")
	task := mustPollWF(t, h)
	if task.WorkflowID != "wf" || task.RunID != run || task.WorkflowType != "W" || task.Attempt != 1 {
		fail(t, "bad task %+v", task)
	}
	if got := show(types(task.History)); got != show([]history.EventType{eStarted, eWFTSched, eWFTStarted}) {
		fail(t, "task history: %s", got)
	}
	st := task.History[0].Attrs.(history.WorkflowExecutionStarted)
	if string(st.Input) != `{"n":1}` || st.TaskQueue != q {
		fail(t, "started attrs %+v", st)
	}
	if pollWF(t, h) != nil {
		fail(t, "only one workflow task may be outstanding")
	}
}

func activityRoundTrip(t *testing.T, h Harness) {
	run := runToActivity(t, h, "wf", sched("a1"))
	wantHistory(t, h, "wf", eStarted, eWFTSched, eWFTStarted, eWFTDone, eActSched)
	at := mustPollAct(t, h)
	if at.ActivityID != "a1" || at.ActivityType != "A" || at.Attempt != 1 || at.ScheduledEventID != 5 || string(at.Input) != `"in"` {
		fail(t, "bad activity task %+v", at)
	}
	if want := fmt.Sprintf("wf/%s/5", run); at.IdempotencyKey != want {
		fail(t, "idempotency key %q, want %q", at.IdempotencyKey, want)
	}
	if !at.Deadline.Equal(h.Now().Add(10 * time.Second)) {
		fail(t, "deadline %v, want start + 10s (%v)", at.Deadline, h.Now().Add(10*time.Second))
	}
	if err := h.Service().CompleteActivityTask(bg, api.CompleteActivityTaskRequest{TaskToken: at.TaskToken, Result: json.RawMessage(`"out"`)}); err != nil {
		fail(t, "complete activity: %v", err)
	}
	evs := wantHistory(t, h, "wf", eStarted, eWFTSched, eWFTStarted, eWFTDone, eActSched, eActStarted, eActDone, eWFTSched)
	if a := evs[5].Attrs.(history.ActivityTaskStarted); a.Attempt != 1 || a.ScheduledEventID != 5 {
		fail(t, "started attrs %+v", a)
	}
	if c := evs[6].Attrs.(history.ActivityTaskCompleted); string(c.Result) != `"out"` || c.StartedEventID != 6 {
		fail(t, "completed attrs %+v", c)
	}
	// Completing twice is stale.
	err := h.Service().CompleteActivityTask(bg, api.CompleteActivityTaskRequest{TaskToken: at.TaskToken})
	wantErr(t, err, api.ErrStaleTask)
	if got := mustPollWF(t, h); len(got.History) != 9 {
		fail(t, "second task should see 9 events, got %d", len(got.History))
	}
}

func staleWorkflowTaskToken(t *testing.T, h Harness) {
	start(t, h, "wf")
	task := mustPollWF(t, h)
	complete(t, h, task, history.StartTimer{TimerID: "t", Duration: time.Hour})
	err := h.Service().CompleteWorkflowTask(bg, api.CompleteWorkflowTaskRequest{TaskToken: task.TaskToken})
	wantErr(t, err, api.ErrStaleTask)
	err = h.Service().CompleteWorkflowTask(bg, api.CompleteWorkflowTaskRequest{TaskToken: "garbage"})
	if err == nil {
		fail(t, "garbage token accepted")
	}
}

func signal(t *testing.T, h Harness, id, req string) error {
	t.Helper()
	return h.Service().SignalWorkflow(bg, api.SignalRequest{WorkflowID: id, SignalName: "go", Payload: json.RawMessage(`"p"`), RequestID: req})
}

func signalWhileScheduled(t *testing.T, h Harness) {
	start(t, h, "wf")
	if err := signal(t, h, "wf", "s1"); err != nil {
		fail(t, "signal: %v", err)
	}
	// The task is only scheduled, so the worker will see the signal: append now, no extra task.
	wantHistory(t, h, "wf", eStarted, eWFTSched, eSignaled)
	task := mustPollWF(t, h)
	if got := show(types(task.History)); got != show([]history.EventType{eStarted, eWFTSched, eSignaled, eWFTStarted}) {
		fail(t, "task history: %s", got)
	}
}

func signalWhenIdle(t *testing.T, h Harness) {
	runToActivity(t, h, "wf", sched("a1"))
	if err := signal(t, h, "wf", "s1"); err != nil {
		fail(t, "signal: %v", err)
	}
	wantHistory(t, h, "wf", eStarted, eWFTSched, eWFTStarted, eWFTDone, eActSched, eSignaled, eWFTSched)
}

func signalBuffered(t *testing.T, h Harness) {
	start(t, h, "wf")
	task := mustPollWF(t, h)
	if err := signal(t, h, "wf", "s1"); err != nil {
		fail(t, "signal: %v", err)
	}
	// C1: the worker did not see this signal, so it must not land before the task's commands.
	wantHistory(t, h, "wf", eStarted, eWFTSched, eWFTStarted)
	complete(t, h, task, history.StartTimer{TimerID: "t", Duration: time.Hour})
	wantHistory(t, h, "wf", eStarted, eWFTSched, eWFTStarted, eWFTDone, eTimerStart, eSignaled, eWFTSched)
}

func closeRejectedWithBuffered(t *testing.T, h Harness) {
	start(t, h, "wf")
	task := mustPollWF(t, h)
	if err := signal(t, h, "wf", "s1"); err != nil {
		fail(t, "signal: %v", err)
	}
	complete(t, h, task, history.CompleteWorkflow{Result: json.RawMessage(`1`)})
	// An acknowledged signal is always seen before close: the close command is dropped.
	wantHistory(t, h, "wf", eStarted, eWFTSched, eWFTStarted, eWFTDone, eSignaled, eWFTSched)
	if s := status(t, h, "wf"); s != history.StatusRunning {
		fail(t, "run must stay open, got %s", s)
	}
	complete(t, h, mustPollWF(t, h), history.CompleteWorkflow{Result: json.RawMessage(`2`)})
	wantHistory(t, h, "wf", eStarted, eWFTSched, eWFTStarted, eWFTDone, eSignaled, eWFTSched, eWFTStarted, eWFTDone, eCompleted)
}

func signalDedup(t *testing.T, h Harness) {
	wantErr(t, signal(t, h, "missing", "s0"), api.ErrNotFound)
	start(t, h, "wf")
	for i := 0; i < 2; i++ {
		if err := signal(t, h, "wf", "s1"); err != nil {
			fail(t, "signal %d: %v", i, err)
		}
	}
	wantHistory(t, h, "wf", eStarted, eWFTSched, eSignaled)
	complete(t, h, mustPollWF(t, h), history.CompleteWorkflow{})
	// C5: a retry of an accepted signal after close returns the original outcome...
	if err := signal(t, h, "wf", "s1"); err != nil {
		fail(t, "retried signal after close must succeed, got %v", err)
	}
	// ...while a new signal to the closed run is rejected.
	wantErr(t, signal(t, h, "wf", "s2"), api.ErrWorkflowClosed)
	wantHistory(t, h, "wf", eStarted, eWFTSched, eSignaled, eWFTStarted, eWFTDone, eCompleted)
}

func failAct(t *testing.T, h Harness, at *api.ActivityTask, typ string) {
	t.Helper()
	if err := h.Service().FailActivityTask(bg, api.FailActivityTaskRequest{TaskToken: at.TaskToken, Failure: history.Failure{Type: typ, Message: "boom"}}); err != nil {
		fail(t, "fail activity: %v", err)
	}
}

func activityRetry(t *testing.T, h Harness) {
	runToActivity(t, h, "wf", sched("a1"))
	first := mustPollAct(t, h)
	failAct(t, h, first, "Transient")
	// Retries live in mutable state, not history (SPEC §4.2).
	wantHistory(t, h, "wf", eStarted, eWFTSched, eWFTStarted, eWFTDone, eActSched)
	if pollAct(t, h) != nil {
		fail(t, "retry dispatched before its backoff")
	}
	h.Advance(999 * time.Millisecond)
	if pollAct(t, h) != nil {
		fail(t, "retry dispatched before 1s backoff elapsed")
	}
	h.Advance(time.Millisecond)
	second := mustPollAct(t, h)
	if second.Attempt != 2 || second.IdempotencyKey != first.IdempotencyKey {
		fail(t, "attempt %d key %q, want 2 and %q", second.Attempt, second.IdempotencyKey, first.IdempotencyKey)
	}
	wantErr(t, h.Service().CompleteActivityTask(bg, api.CompleteActivityTaskRequest{TaskToken: first.TaskToken}), api.ErrStaleTask)
	if err := h.Service().CompleteActivityTask(bg, api.CompleteActivityTaskRequest{TaskToken: second.TaskToken, Result: json.RawMessage(`2`)}); err != nil {
		fail(t, "complete attempt 2: %v", err)
	}
	evs := wantHistory(t, h, "wf", eStarted, eWFTSched, eWFTStarted, eWFTDone, eActSched, eActStarted, eActDone, eWFTSched)
	if a := evs[5].Attrs.(history.ActivityTaskStarted); a.Attempt != 2 {
		fail(t, "ActivityTaskStarted must record the final attempt, got %d", a.Attempt)
	}
}

func activityNonRetryable(t *testing.T, h Harness) {
	runToActivity(t, h, "wf", sched("a1"))
	failAct(t, h, mustPollAct(t, h), "Fatal")
	evs := wantHistory(t, h, "wf", eStarted, eWFTSched, eWFTStarted, eWFTDone, eActSched, eActStarted, eActFailed, eWFTSched)
	if f := evs[6].Attrs.(history.ActivityTaskFailed); f.Failure.Type != "Fatal" || f.Attempt != 1 {
		fail(t, "failed attrs %+v", f)
	}
	// Finish wf so its pending workflow task does not sit ahead of wf2's on the shared queue.
	complete(t, h, mustPollWF(t, h), history.CompleteWorkflow{})
	// The activity itself can also refuse retries.
	runToActivity(t, h, "wf2", sched("a1"))
	at := mustPollAct(t, h)
	if err := h.Service().FailActivityTask(bg, api.FailActivityTaskRequest{TaskToken: at.TaskToken, Failure: history.Failure{Type: "Other", NonRetryable: true}}); err != nil {
		fail(t, "fail: %v", err)
	}
	wantHistory(t, h, "wf2", eStarted, eWFTSched, eWFTStarted, eWFTDone, eActSched, eActStarted, eActFailed, eWFTSched)
}

func activityExhausted(t *testing.T, h Harness) {
	runToActivity(t, h, "wf", sched("a1", func(a *history.ScheduleActivity) { a.RetryPolicy.MaximumAttempts = 2 }))
	failAct(t, h, mustPollAct(t, h), "Transient")
	h.Advance(time.Second)
	failAct(t, h, mustPollAct(t, h), "Transient")
	evs := wantHistory(t, h, "wf", eStarted, eWFTSched, eWFTStarted, eWFTDone, eActSched, eActStarted, eActFailed, eWFTSched)
	if f := evs[6].Attrs.(history.ActivityTaskFailed); f.Attempt != 2 {
		fail(t, "want final attempt 2, got %d", f.Attempt)
	}
}

func activityTimeout(t *testing.T, h Harness) {
	runToActivity(t, h, "wf", sched("a1"))
	first := mustPollAct(t, h)
	h.Advance(10 * time.Second) // start-to-close of attempt 1
	wantHistory(t, h, "wf", eStarted, eWFTSched, eWFTStarted, eWFTDone, eActSched)
	h.Advance(time.Second) // backoff before attempt 2
	second := mustPollAct(t, h)
	if second.Attempt != 2 {
		fail(t, "want attempt 2, got %d", second.Attempt)
	}
	wantErr(t, h.Service().CompleteActivityTask(bg, api.CompleteActivityTaskRequest{TaskToken: first.TaskToken}), api.ErrStaleTask)
	// C3: nothing armed for attempt 1 may time out attempt 2 early.
	h.Advance(9 * time.Second)
	if err := h.Service().CompleteActivityTask(bg, api.CompleteActivityTaskRequest{TaskToken: second.TaskToken}); err != nil {
		fail(t, "attempt 2 was timed out early: %v", err)
	}
}

func activityScheduleToClose(t *testing.T, h Harness) {
	runToActivity(t, h, "wf", sched("a1", func(a *history.ScheduleActivity) { a.ScheduleToCloseTimeout = 5 * time.Second }))
	mustPollAct(t, h)
	h.Advance(5 * time.Second)
	evs := wantHistory(t, h, "wf", eStarted, eWFTSched, eWFTStarted, eWFTDone, eActSched, eActStarted, eActTimeout, eWFTSched)
	if to := evs[6].Attrs.(history.ActivityTaskTimedOut); to.TimeoutType != history.TimeoutScheduleToClose {
		fail(t, "timeout type %s", to.TimeoutType)
	}
}

func userTimer(t *testing.T, h Harness) {
	start(t, h, "wf")
	complete(t, h, mustPollWF(t, h), history.StartTimer{TimerID: "t", Duration: time.Hour})
	evs := wantHistory(t, h, "wf", eStarted, eWFTSched, eWFTStarted, eWFTDone, eTimerStart)
	ts := evs[4].Attrs.(history.TimerStarted)
	if !ts.FireAt.Equal(h.Now().Add(time.Hour)) {
		fail(t, "fireAt %v, want now+1h", ts.FireAt)
	}
	h.Advance(59 * time.Minute)
	wantHistory(t, h, "wf", eStarted, eWFTSched, eWFTStarted, eWFTDone, eTimerStart)
	h.Advance(time.Minute)
	evs = wantHistory(t, h, "wf", eStarted, eWFTSched, eWFTStarted, eWFTDone, eTimerStart, eTimerFired, eWFTSched)
	if f := evs[5].Attrs.(history.TimerFired); f.TimerID != "t" || f.StartedEventID != 5 || evs[5].Time.Before(ts.FireAt) {
		fail(t, "fired attrs %+v at %v", f, evs[5].Time)
	}
	h.Advance(2 * time.Hour)
	wantHistory(t, h, "wf", eStarted, eWFTSched, eWFTStarted, eWFTDone, eTimerStart, eTimerFired, eWFTSched)
}

func canceledTimer(t *testing.T, h Harness) {
	start(t, h, "wf")
	complete(t, h, mustPollWF(t, h), history.StartTimer{TimerID: "t", Duration: time.Hour})
	if err := signal(t, h, "wf", "s1"); err != nil {
		fail(t, "signal: %v", err)
	}
	complete(t, h, mustPollWF(t, h), history.CancelTimer{TimerID: "t"})
	h.Advance(2 * time.Hour)
	wantHistory(t, h, "wf", eStarted, eWFTSched, eWFTStarted, eWFTDone, eTimerStart, eSignaled, eWFTSched, eWFTStarted, eWFTDone, eTimerCancel)
}

func workflowTaskTimeout(t *testing.T, h Harness) {
	start(t, h, "wf")
	mustPollWF(t, h)
	h.Advance(history.DefaultWorkflowTaskTimeout)
	wantHistory(t, h, "wf", eStarted, eWFTSched, eWFTStarted, eWFTTimeout)
	retry := mustPollWF(t, h)
	if retry.Attempt != 2 {
		fail(t, "want attempt 2, got %d", retry.Attempt)
	}
	// Attempts after the first failure are transient: shown to the worker, not yet persisted.
	if got := show(types(retry.History)); got != show([]history.EventType{eStarted, eWFTSched, eWFTStarted, eWFTTimeout, eWFTSched, eWFTStarted}) {
		fail(t, "retry history: %s", got)
	}
	complete(t, h, retry)
	wantHistory(t, h, "wf", eStarted, eWFTSched, eWFTStarted, eWFTTimeout, eWFTSched, eWFTStarted, eWFTDone)
}

func workflowTaskFailureSeries(t *testing.T, h Harness) {
	start(t, h, "wf")
	failWF := func(task *api.WorkflowTask) {
		t.Helper()
		if err := h.Service().FailWorkflowTask(bg, api.FailWorkflowTaskRequest{TaskToken: task.TaskToken, Cause: history.Failure{Type: "NonDeterministicError"}}); err != nil {
			fail(t, "fail workflow task: %v", err)
		}
	}
	failWF(mustPollWF(t, h))
	wantHistory(t, h, "wf", eStarted, eWFTSched, eWFTStarted, eWFTFailed)
	failWF(mustPollWF(t, h)) // attempt 2 is offered at once
	// A failure series writes one event, however long it runs (SPEC §4.2).
	wantHistory(t, h, "wf", eStarted, eWFTSched, eWFTStarted, eWFTFailed)
	if pollWF(t, h) != nil {
		fail(t, "attempt 3 offered before its backoff")
	}
	h.Advance(time.Second)
	third := mustPollWF(t, h)
	if third.Attempt != 3 {
		fail(t, "want attempt 3, got %d", third.Attempt)
	}
	if s := status(t, h, "wf"); s != history.StatusRunning {
		fail(t, "non-determinism blocks a run, it never fails it (D6): %s", s)
	}
	complete(t, h, third)
	evs := wantHistory(t, h, "wf", eStarted, eWFTSched, eWFTStarted, eWFTFailed, eWFTSched, eWFTStarted, eWFTDone)
	if a := evs[4].Attrs.(history.WorkflowTaskScheduled); a.Attempt != 3 {
		fail(t, "persisted attempt %d, want 3", a.Attempt)
	}
}

func redispatchWorkflowTask(t *testing.T, h Harness) {
	start(t, h, "wf")
	h.Settle()
	h.DropQueuedTasks()
	if pollWF(t, h) != nil {
		fail(t, "task should be lost from matching")
	}
	h.Advance(history.DefaultRedispatchInterval)
	mustPollWF(t, h)
}

func redispatchActivityTask(t *testing.T, h Harness) {
	runToActivity(t, h, "wf", sched("a1"))
	h.Settle()
	h.DropQueuedTasks()
	if pollAct(t, h) != nil {
		fail(t, "activity task should be lost from matching")
	}
	h.Advance(history.DefaultRedispatchInterval)
	if at := mustPollAct(t, h); at.Attempt != 1 {
		fail(t, "redispatch is not a retry: attempt %d", at.Attempt)
	}
}

func executionTimeout(t *testing.T, h Harness) {
	start(t, h, "wf", func(r *api.StartRequest) { r.ExecutionTimeout = time.Hour })
	h.Advance(time.Hour)
	wantHistory(t, h, "wf", eStarted, eWFTSched, eTimedOut)
	if s := status(t, h, "wf"); s != history.StatusTimedOut {
		fail(t, "status %s", s)
	}
	if pollWF(t, h) != nil {
		fail(t, "a closed run must not hand out tasks")
	}
}

func terminate(t *testing.T, h Harness) {
	runToActivity(t, h, "wf", sched("a1"))
	at := mustPollAct(t, h)
	if err := h.Service().TerminateWorkflow(bg, api.TerminateRequest{WorkflowID: "wf", Reason: "test"}); err != nil {
		fail(t, "terminate: %v", err)
	}
	wantHistory(t, h, "wf", eStarted, eWFTSched, eWFTStarted, eWFTDone, eActSched, eTerminated)
	if s := status(t, h, "wf"); s != history.StatusTerminated {
		fail(t, "status %s", s)
	}
	wantErr(t, h.Service().CompleteActivityTask(bg, api.CompleteActivityTaskRequest{TaskToken: at.TaskToken}), api.ErrStaleTask)
	h.Advance(time.Hour)
	if pollAct(t, h) != nil || pollWF(t, h) != nil {
		fail(t, "a terminated run must not hand out tasks")
	}
	wantHistory(t, h, "wf", eStarted, eWFTSched, eWFTStarted, eWFTDone, eActSched, eTerminated)
}

func describe(t *testing.T, h Harness) {
	start(t, h, "wf")
	complete(t, h, mustPollWF(t, h), sched("a1"), history.StartTimer{TimerID: "t", Duration: time.Minute})
	d, err := h.Service().DescribeWorkflow(bg, "wf")
	if err != nil {
		fail(t, "describe: %v", err)
	}
	if d.Status != history.StatusRunning || len(d.PendingActivities) != 1 || len(d.PendingTimers) != 1 {
		fail(t, "describe %+v", d)
	}
	if a := d.PendingActivities[0]; a.ActivityID != "a1" || a.Attempt != 1 || a.State != history.TaskScheduled {
		fail(t, "pending activity %+v", a)
	}
	if tm := d.PendingTimers[0]; tm.TimerID != "t" || !tm.FireAt.Equal(h.Now().Add(time.Minute)) {
		fail(t, "pending timer %+v", tm)
	}
}

func invalidCommand(t *testing.T, h Harness) {
	start(t, h, "wf")
	task := mustPollWF(t, h)
	bad := history.ScheduleActivity{ActivityID: "a", ActivityType: "A"} // no StartToCloseTimeout
	err := h.Service().CompleteWorkflowTask(bg, api.CompleteWorkflowTaskRequest{TaskToken: task.TaskToken, Commands: []history.Command{bad}})
	wantErr(t, err, api.ErrInvalidArgument)
	wantHistory(t, h, "wf", eStarted, eWFTSched, eWFTStarted)
	complete(t, h, task, sched("a"))
}
