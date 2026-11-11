package history

import (
	"encoding/json"
	"errors"
	"strings"
	"testing"
	"time"
)

var t0 = time.Date(2026, 11, 11, 12, 0, 0, 0, time.UTC)

// run is a test driver that keeps state and the accumulated history.
type run struct {
	t   *testing.T
	s   *MutableState
	h   []Event
	now time.Time
	out Effects // effects of the last step
}

func newRun(t *testing.T, timeout time.Duration) *run {
	s, eff := NewRun(StartInput{WorkflowID: "wf", RunID: "r", WorkflowType: "W", TaskQueue: "q", RequestID: "req", ExecutionTimeout: timeout}, t0)
	return &run{t: t, s: s, h: eff.Events, now: t0, out: eff}
}

func (r *run) step(f func(m *Machine) error) error {
	r.t.Helper()
	m := NewMachine(r.s, r.now)
	err := f(m)
	r.out = m.Out
	r.h = append(r.h, m.Out.Events...)
	for i, e := range r.h {
		if e.ID != int64(i+1) {
			r.t.Fatalf("history not contiguous at %d: %d", i, e.ID)
		}
	}
	return err
}

func (r *run) must(f func(m *Machine) error) {
	r.t.Helper()
	if err := r.step(f); err != nil {
		r.t.Fatalf("unexpected error: %v", err)
	}
}

func (r *run) types() string {
	var s []string
	for _, e := range r.h {
		s = append(s, string(e.Type()))
	}
	return strings.Join(s, ",")
}

func (r *run) want(ts ...EventType) {
	r.t.Helper()
	var s []string
	for _, x := range ts {
		s = append(s, string(x))
	}
	if got, want := r.types(), strings.Join(s, ","); got != want {
		r.t.Fatalf("history\n got: %s\nwant: %s", got, want)
	}
}

func (r *run) startWFT() int64 {
	r.t.Helper()
	seq := r.s.WorkflowTask.Seq
	r.must(func(m *Machine) error {
		_, err := m.WorkflowTaskStarted(DispatchRef{Type: WorkflowTaskType, Seq: seq}, "w")
		return err
	})
	return seq
}

func (r *run) complete(seq int64, cmds ...Command) {
	r.t.Helper()
	r.must(func(m *Machine) error { return m.CompleteWorkflowTask(seq, cmds) })
}

func act(id string) ScheduleActivity {
	return ScheduleActivity{ActivityID: id, ActivityType: "A", StartToCloseTimeout: 10 * time.Second,
		RetryPolicy: RetryPolicy{InitialInterval: time.Second, MaximumAttempts: 3, NonRetryableErrors: []string{"Fatal"}}}
}

func TestNewRun(t *testing.T) {
	r := newRun(t, time.Hour)
	r.want(EventWorkflowExecutionStarted, EventWorkflowTaskScheduled)
	if len(r.out.Dispatch) != 1 || r.out.Dispatch[0].Type != WorkflowTaskType || r.out.Dispatch[0].Seq != 1 {
		t.Fatalf("dispatch %+v", r.out.Dispatch)
	}
	if len(r.out.Timers) != 1 || r.out.Timers[0].Ref.Kind != TimerExecutionTimeout || !r.out.Timers[0].FireAt.Equal(t0.Add(time.Hour)) {
		t.Fatalf("timers %+v", r.out.Timers)
	}
}

func TestBufferedSignalLandsAfterCommands(t *testing.T) {
	r := newRun(t, 0)
	seq := r.startWFT()
	r.must(func(m *Machine) error { return m.Signal("go", nil, "s1") })
	r.want(EventWorkflowExecutionStarted, EventWorkflowTaskScheduled, EventWorkflowTaskStarted)
	r.complete(seq, StartTimer{TimerID: "t", Duration: time.Minute})
	r.want(EventWorkflowExecutionStarted, EventWorkflowTaskScheduled, EventWorkflowTaskStarted, EventWorkflowTaskCompleted,
		EventTimerStarted, EventWorkflowExecutionSignaled, EventWorkflowTaskScheduled)
}

func TestCloseRejectedWhileBuffered(t *testing.T) {
	r := newRun(t, 0)
	seq := r.startWFT()
	r.must(func(m *Machine) error { return m.Signal("go", nil, "s1") })
	r.complete(seq, RecordMarker{MarkerName: "m"}, CompleteWorkflow{})
	if r.s.Status != StatusRunning {
		t.Fatalf("closed with a buffered signal")
	}
	r.want(EventWorkflowExecutionStarted, EventWorkflowTaskScheduled, EventWorkflowTaskStarted, EventWorkflowTaskCompleted,
		EventMarkerRecorded, EventWorkflowExecutionSignaled, EventWorkflowTaskScheduled)
}

func TestBufferedActivityCompletionPatchesStartedID(t *testing.T) {
	r := newRun(t, 0)
	r.complete(r.startWFT(), act("a"))
	schedID := int64(5)
	r.must(func(m *Machine) error {
		_, _, err := m.ActivityStarted(DispatchRef{Type: ActivityTaskType, ScheduledEventID: schedID, Attempt: 1}, "aw")
		return err
	})
	r.must(func(m *Machine) error { return m.Signal("go", nil, "") }) // schedules WFT 2
	seq := r.startWFT()
	r.must(func(m *Machine) error { return m.CompleteActivity(schedID, 1, json.RawMessage(`1`)) })
	if len(r.s.Buffered) != 2 {
		t.Fatalf("want started+completed buffered, got %d", len(r.s.Buffered))
	}
	r.complete(seq)
	n := len(r.h)
	started, done := r.h[n-3], r.h[n-2]
	if done.Attrs.(ActivityTaskCompleted).StartedEventID != started.ID || started.Attrs.(ActivityTaskStarted).WorkerIdentity != "aw" {
		t.Fatalf("bad patch: started %+v done %+v", started, done)
	}
	if r.h[n-1].Type() != EventWorkflowTaskScheduled {
		t.Fatalf("buffered results must schedule a new task")
	}
}

func TestActivityRetryAndStaleRefs(t *testing.T) {
	r := newRun(t, 0)
	r.complete(r.startWFT(), act("a"))
	const id = 5
	start := func(attempt int) error {
		return r.step(func(m *Machine) error {
			_, _, err := m.ActivityStarted(DispatchRef{Type: ActivityTaskType, ScheduledEventID: id, Attempt: attempt}, "aw")
			return err
		})
	}
	if err := start(1); err != nil {
		t.Fatal(err)
	}
	r.must(func(m *Machine) error { return m.FailActivity(id, 1, Failure{Type: "Transient"}) })
	if a := r.s.Activities[id]; a.Attempt != 2 || a.State != TaskBackoff {
		t.Fatalf("activity %+v", a)
	}
	if len(r.out.Timers) != 1 || r.out.Timers[0].Ref.Kind != TimerActivityRetry || !r.out.Timers[0].FireAt.Equal(t0.Add(time.Second)) {
		t.Fatalf("retry timer %+v", r.out.Timers)
	}
	// Illegal: completing attempt 1, starting attempt 2 before backoff, a retry timer for attempt 3.
	if err := r.step(func(m *Machine) error { return m.CompleteActivity(id, 1, nil) }); !errors.Is(err, ErrStaleTask) {
		t.Fatalf("stale completion: %v", err)
	}
	if err := start(2); !errors.Is(err, ErrStaleTask) {
		t.Fatalf("start during backoff: %v", err)
	}
	r.must(func(m *Machine) error {
		m.FireTimer(TimerRef{Kind: TimerActivityRetry, EventID: id, Attempt: 3})
		return nil
	})
	if r.s.Activities[id].State != TaskBackoff {
		t.Fatal("stale retry timer acted")
	}
	r.must(func(m *Machine) error {
		m.FireTimer(TimerRef{Kind: TimerActivityRetry, EventID: id, Attempt: 2})
		return nil
	})
	if err := start(2); err != nil {
		t.Fatal(err)
	}
	// C3: attempt 1's start-to-close timer firing now must not touch attempt 2.
	r.must(func(m *Machine) error {
		m.FireTimer(TimerRef{Kind: TimerActivityStartToClose, EventID: id, Attempt: 1})
		return nil
	})
	if a := r.s.Activities[id]; a.Attempt != 2 || a.State != TaskStarted {
		t.Fatalf("old timeout affected attempt 2: %+v", a)
	}
	r.must(func(m *Machine) error { return m.FailActivity(id, 2, Failure{Type: "Fatal"}) })
	r.want(EventWorkflowExecutionStarted, EventWorkflowTaskScheduled, EventWorkflowTaskStarted, EventWorkflowTaskCompleted,
		EventActivityTaskScheduled, EventActivityTaskStarted, EventActivityTaskFailed, EventWorkflowTaskScheduled)
}

func TestScheduleToCloseStopsRetries(t *testing.T) {
	r := newRun(t, 0)
	a := act("a")
	a.ScheduleToCloseTimeout = 1500 * time.Millisecond
	a.RetryPolicy.InitialInterval = 2 * time.Second
	r.complete(r.startWFT(), a)
	r.must(func(m *Machine) error {
		_, _, err := m.ActivityStarted(DispatchRef{Type: ActivityTaskType, ScheduledEventID: 5, Attempt: 1}, "aw")
		return err
	})
	// A retry 2s out would pass the 1.5s schedule-to-close bound: fail for good instead.
	r.must(func(m *Machine) error { return m.FailActivity(5, 1, Failure{Type: "Transient"}) })
	if _, pending := r.s.Activities[5]; pending {
		t.Fatal("activity should have ended")
	}
}

func TestWorkflowTaskFailureSeriesIsTransient(t *testing.T) {
	r := newRun(t, 0)
	seq := r.startWFT()
	r.must(func(m *Machine) error { return m.FailWorkflowTask(seq, Failure{Type: "NDE"}) })
	r.want(EventWorkflowExecutionStarted, EventWorkflowTaskScheduled, EventWorkflowTaskStarted, EventWorkflowTaskFailed)
	// Attempt 2: offered at once, transient.
	if len(r.out.Dispatch) != 1 || r.out.Dispatch[0].Attempt != 2 {
		t.Fatalf("dispatch %+v", r.out.Dispatch)
	}
	var transient []Event
	seq2 := r.s.WorkflowTask.Seq
	r.must(func(m *Machine) error {
		var err error
		transient, err = m.WorkflowTaskStarted(DispatchRef{Type: WorkflowTaskType, Seq: seq2}, "w")
		return err
	})
	if len(transient) != 2 || transient[0].ID != 5 || transient[1].ID != 6 || transient[1].Attrs.(WorkflowTaskStarted).ScheduledEventID != 5 {
		t.Fatalf("transient events %+v", transient)
	}
	r.must(func(m *Machine) error { return m.FailWorkflowTask(seq2, Failure{Type: "NDE"}) })
	r.want(EventWorkflowExecutionStarted, EventWorkflowTaskScheduled, EventWorkflowTaskStarted, EventWorkflowTaskFailed)
	if r.s.WorkflowTask.State != TaskBackoff || len(r.out.Timers) != 1 || !r.out.Timers[0].FireAt.Equal(t0.Add(time.Second)) {
		t.Fatalf("attempt 3 must wait 1s: %+v %+v", r.s.WorkflowTask, r.out.Timers)
	}
	seq3 := r.s.WorkflowTask.Seq
	r.must(func(m *Machine) error { m.FireTimer(TimerRef{Kind: TimerWorkflowTaskBackoff, Seq: seq3}); return nil })
	r.startWFT()
	r.complete(seq3)
	r.want(EventWorkflowExecutionStarted, EventWorkflowTaskScheduled, EventWorkflowTaskStarted, EventWorkflowTaskFailed,
		EventWorkflowTaskScheduled, EventWorkflowTaskStarted, EventWorkflowTaskCompleted)
	if r.h[4].Attrs.(WorkflowTaskScheduled).Attempt != 3 || r.h[6].Attrs.(WorkflowTaskCompleted).StartedEventID != 6 {
		t.Fatalf("persisted attempt wrong: %+v %+v", r.h[4], r.h[6])
	}
	if WorkflowTaskRetryDelay(40) != MaxWorkflowTaskRetryInterval {
		t.Fatal("retry delay not capped")
	}
}

func TestWorkflowTaskTimeoutStaleRef(t *testing.T) {
	r := newRun(t, 0)
	seq := r.startWFT()
	r.complete(seq, StartTimer{TimerID: "t", Duration: time.Hour})
	// The completed task's timeout timer fires later: no-op.
	r.must(func(m *Machine) error { m.FireTimer(TimerRef{Kind: TimerWorkflowTaskTimeout, Seq: seq}); return nil })
	r.want(EventWorkflowExecutionStarted, EventWorkflowTaskScheduled, EventWorkflowTaskStarted, EventWorkflowTaskCompleted, EventTimerStarted)
}

func TestUserTimerFiresOnceAndCancel(t *testing.T) {
	r := newRun(t, 0)
	r.complete(r.startWFT(), StartTimer{TimerID: "t", Duration: time.Hour}, StartTimer{TimerID: "u", Duration: time.Hour})
	ref := r.out.Timers[0].Ref
	if ref.Kind != TimerUser || ref.TimerID != "t" {
		t.Fatalf("first timer request %+v", ref)
	}
	fire := func(ref TimerRef) { r.must(func(m *Machine) error { m.FireTimer(ref); return nil }) }
	fire(ref)
	fire(ref) // duplicate fire after a crash: no-op
	n := 0
	for _, e := range r.h {
		if e.Type() == EventTimerFired {
			n++
		}
	}
	if n != 1 {
		t.Fatalf("timer fired %d times", n)
	}
	// Illegal: cancel an unknown timer.
	seq := r.startWFT()
	if err := r.step(func(m *Machine) error { return m.CompleteWorkflowTask(seq, []Command{CancelTimer{TimerID: "nope"}}) }); !errors.Is(err, ErrInvalidCommand) {
		t.Fatalf("cancel unknown: %v", err)
	}
	r.complete(seq, CancelTimer{TimerID: "u"})
	fire(TimerRef{Kind: TimerUser, TimerID: "u", EventID: r.h[5].ID})
	if _, ok := r.s.Timers["u"]; ok {
		t.Fatal("canceled timer still pending")
	}
}

func TestSignalDedupAndClose(t *testing.T) {
	r := newRun(t, 0)
	r.must(func(m *Machine) error { return m.Signal("go", nil, "s1") })
	r.must(func(m *Machine) error { return m.Signal("go", nil, "s1") })
	r.complete(r.startWFT(), CompleteWorkflow{})
	if err := r.step(func(m *Machine) error { return m.Signal("go", nil, "s1") }); err != nil {
		t.Fatalf("retry after close: %v", err)
	}
	if err := r.step(func(m *Machine) error { return m.Signal("go", nil, "s2") }); !errors.Is(err, ErrWorkflowClosed) {
		t.Fatalf("new signal after close: %v", err)
	}
	r.want(EventWorkflowExecutionStarted, EventWorkflowTaskScheduled, EventWorkflowExecutionSignaled,
		EventWorkflowTaskStarted, EventWorkflowTaskCompleted, EventWorkflowExecutionCompleted)
	// Illegal transitions on a closed run.
	r.must(func(m *Machine) error { m.Terminate("x"); return nil })
	if r.s.Status != StatusCompleted {
		t.Fatal("terminate changed a closed run")
	}
	if err := r.step(func(m *Machine) error {
		_, err := m.WorkflowTaskStarted(DispatchRef{Seq: 99}, "w")
		return err
	}); !errors.Is(err, ErrStaleTask) {
		t.Fatalf("start on closed run: %v", err)
	}
}

func TestDedupBound(t *testing.T) {
	r := newRun(t, 0)
	for i := 0; i < SignalDedupLimit+5; i++ {
		id := "s" + string(rune('a'+i%26)) + json.Number(string(rune('0'+i%10))).String() + time.Duration(i).String()
		r.must(func(m *Machine) error { return m.Signal("go", nil, id) })
	}
	if len(r.s.Signals) != SignalDedupLimit {
		t.Fatalf("dedup set %d", len(r.s.Signals))
	}
}

func TestTimestampsNeverGoBackwards(t *testing.T) {
	r := newRun(t, 0)
	r.now = t0.Add(-time.Minute) // a new owner whose clock is behind
	r.must(func(m *Machine) error { return m.Signal("go", nil, "") })
	last := r.h[len(r.h)-1]
	if last.Time.Before(t0) {
		t.Fatalf("event time went backwards: %v", last.Time)
	}
}

func TestExecutionTimeoutAndTerminateClose(t *testing.T) {
	r := newRun(t, time.Hour)
	seq := r.startWFT()
	r.must(func(m *Machine) error { m.FireTimer(TimerRef{Kind: TimerExecutionTimeout}); return nil })
	if r.s.Status != StatusTimedOut {
		t.Fatalf("status %s", r.s.Status)
	}
	if err := r.step(func(m *Machine) error { return m.CompleteWorkflowTask(seq, nil) }); !errors.Is(err, ErrStaleTask) {
		t.Fatalf("complete after close: %v", err)
	}
	r2 := newRun(t, 0)
	r2.must(func(m *Machine) error { m.Terminate("bye"); return nil })
	r2.want(EventWorkflowExecutionStarted, EventWorkflowTaskScheduled, EventWorkflowExecutionTerminated)
}

func TestValidateCommands(t *testing.T) {
	r := newRun(t, 0)
	m := NewMachine(r.s, t0)
	bad := [][]Command{
		{ScheduleActivity{ActivityID: "a", ActivityType: "A"}},
		{act("a"), act("a")},
		{StartTimer{TimerID: "t"}},
		{StartTimer{TimerID: "t", Duration: 1}, StartTimer{TimerID: "t", Duration: 1}},
		{RecordMarker{}},
	}
	for i, cs := range bad {
		if err := m.ValidateCommands(cs); !errors.Is(err, ErrInvalidCommand) {
			t.Fatalf("case %d accepted: %v", i, err)
		}
	}
	if err := m.ValidateCommands([]Command{act("a"), StartTimer{TimerID: "t", Duration: 1}, CancelTimer{TimerID: "t"}}); err != nil {
		t.Fatalf("valid batch rejected: %v", err)
	}
}
