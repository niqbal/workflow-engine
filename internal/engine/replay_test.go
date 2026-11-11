package engine

import (
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/niqbal/workflow-engine/internal/history"
)

// loop drives history.Machine (the real server state machine) with Replay as the worker.
// Activities run inline; timers fire when the test advances time. Every history in these
// tests is therefore one the server can actually produce.
type loop struct {
	t       *testing.T
	s       *history.MutableState
	h       []history.Event
	now     time.Time
	fn      Workflow
	acts    map[string]func(json.RawMessage) (json.RawMessage, error)
	timers  []history.TimerRequest
	actRuns map[string]int
	// beforeComplete runs after a workflow task starts and before its commands are applied,
	// which is how tests inject events that get buffered.
	beforeComplete func(l *loop)
}

var start = time.Date(2026, 11, 11, 9, 0, 0, 0, time.UTC)

func newLoop(t *testing.T, fn Workflow, input string) *loop {
	s, eff := history.NewRun(history.StartInput{WorkflowID: "wf", RunID: "r1", WorkflowType: "W", TaskQueue: "q",
		Input: json.RawMessage(input)}, start)
	l := &loop{t: t, s: s, h: eff.Events, now: start, fn: fn, acts: map[string]func(json.RawMessage) (json.RawMessage, error){}, actRuns: map[string]int{}}
	l.timers = append(l.timers, eff.Timers...)
	return l
}

func (l *loop) apply(f func(m *history.Machine) error) error {
	m := history.NewMachine(l.s, l.now)
	err := f(m)
	l.h = append(l.h, m.Out.Events...)
	l.timers = append(l.timers, m.Out.Timers...)
	return err
}

// step runs every scheduled activity and workflow task until the run blocks on time.
func (l *loop) step() {
	l.t.Helper()
	for i := 0; i < 100; i++ {
		progressed := false
		for _, id := range sortedIDs(l.s.Activities) {
			a := l.s.Activities[id]
			if a.State != history.TaskScheduled {
				continue
			}
			progressed = true
			ref := history.DispatchRef{Type: history.ActivityTaskType, ScheduledEventID: id, Attempt: a.Attempt}
			if err := l.apply(func(m *history.Machine) error { _, _, err := m.ActivityStarted(ref, "aw"); return err }); err != nil {
				l.t.Fatal(err)
			}
			typ := a.Scheduled.ActivityType
			l.actRuns[typ]++
			res, err := l.acts[typ](a.Scheduled.Input)
			if err != nil {
				l.must(l.apply(func(m *history.Machine) error {
					return m.FailActivity(id, ref.Attempt, history.Failure{Type: "E", Message: err.Error(), NonRetryable: true})
				}))
			} else {
				l.must(l.apply(func(m *history.Machine) error { return m.CompleteActivity(id, ref.Attempt, res) }))
			}
		}
		if wt := l.s.WorkflowTask; wt != nil && wt.State == history.TaskScheduled {
			progressed = true
			l.runWorkflowTask()
		}
		if !progressed {
			return
		}
	}
	l.t.Fatal("loop did not settle")
}

func (l *loop) must(err error) {
	l.t.Helper()
	if err != nil {
		l.t.Fatal(err)
	}
}

func (l *loop) runWorkflowTask() {
	l.t.Helper()
	seq := l.s.WorkflowTask.Seq
	var transient []history.Event
	l.must(l.apply(func(m *history.Machine) error {
		var err error
		transient, err = m.WorkflowTaskStarted(history.DispatchRef{Type: history.WorkflowTaskType, Seq: seq}, "w")
		return err
	}))
	full := append(append([]history.Event(nil), l.h...), transient...)
	cmds, err := Replay(Info{WorkflowID: "wf", RunID: "r1"}, full, l.fn)
	if hook := l.beforeComplete; hook != nil {
		l.beforeComplete = nil
		hook(l)
	}
	if err != nil {
		l.must(l.apply(func(m *history.Machine) error {
			return m.FailWorkflowTask(seq, history.Failure{Type: "ReplayError", Message: err.Error()})
		}))
		l.t.Logf("workflow task failed: %v", err)
		return
	}
	l.must(l.apply(func(m *history.Machine) error { return m.CompleteWorkflowTask(seq, cmds) }))
}

// advance moves time and fires due timers in order.
func (l *loop) advance(d time.Duration) {
	l.t.Helper()
	until := l.now.Add(d)
	for {
		best := -1
		for i, tr := range l.timers {
			if !tr.FireAt.After(until) && (best < 0 || tr.FireAt.Before(l.timers[best].FireAt)) {
				best = i
			}
		}
		if best < 0 {
			break
		}
		tr := l.timers[best]
		l.timers = append(l.timers[:best], l.timers[best+1:]...)
		if tr.FireAt.After(l.now) {
			l.now = tr.FireAt
		}
		l.must(l.apply(func(m *history.Machine) error { m.FireTimer(tr.Ref); return nil }))
		l.step()
	}
	l.now = until
	l.step()
}

func (l *loop) signal(name, payload string) {
	l.must(l.apply(func(m *history.Machine) error { return m.Signal(name, json.RawMessage(payload), "") }))
}

func sortedIDs(m map[int64]*history.ActivityInfo) []int64 {
	var ids []int64
	for id := range m {
		ids = append(ids, id)
	}
	for i := 1; i < len(ids); i++ {
		for j := i; j > 0 && ids[j] < ids[j-1]; j-- {
			ids[j], ids[j-1] = ids[j-1], ids[j]
		}
	}
	return ids
}

func (l *loop) result() string {
	l.t.Helper()
	last := l.h[len(l.h)-1]
	switch a := last.Attrs.(type) {
	case history.WorkflowExecutionCompleted:
		return string(a.Result)
	case history.WorkflowExecutionFailed:
		return "failed: " + a.Failure.Message
	}
	l.t.Fatalf("run not closed; last event %s (status %s)", last.Type(), l.s.Status)
	return ""
}

// ---- workflows used by the tests --------------------------------------------------------

func act(typ string, input any) history.ScheduleActivity {
	b, _ := json.Marshal(input)
	return history.ScheduleActivity{ActivityType: typ, Input: b, StartToCloseTimeout: time.Minute}
}

func getInt(w *WorkflowContext, f *Future) int {
	v, err := f.Get(w.Ctx)
	if err != nil {
		panic(err)
	}
	var n int
	json.Unmarshal(v.(json.RawMessage), &n)
	return n
}

// sum runs Add three times in sequence, then in parallel, and returns the total.
func sumWorkflow(w *WorkflowContext, in json.RawMessage) (json.RawMessage, error) {
	total := 0
	for i := 1; i <= 3; i++ {
		total += getInt(w, w.ScheduleActivity(act("Add", i)))
	}
	fs := []*Future{w.ScheduleActivity(act("Add", 10)), w.ScheduleActivity(act("Add", 20))}
	for _, f := range fs {
		total += getInt(w, f)
	}
	return json.Marshal(total)
}

func addOne(in json.RawMessage) (json.RawMessage, error) {
	var n int
	json.Unmarshal(in, &n)
	return json.Marshal(n + 1)
}

func TestSequentialAndParallelActivities(t *testing.T) {
	l := newLoop(t, sumWorkflow, `null`)
	l.acts["Add"] = addOne
	l.step()
	if got := l.result(); got != "41" { // (2+3+4) + (11+21)
		t.Fatalf("result %s, history:\n%s", got, dump(l.h))
	}
	if l.actRuns["Add"] != 5 {
		t.Fatalf("activities ran %d times, want 5 (replay must not re-run them)", l.actRuns["Add"])
	}
}

func TestReplayOfEveryPrefixIsStable(t *testing.T) {
	l := newLoop(t, sumWorkflow, `null`)
	l.acts["Add"] = addOne
	l.step()
	// Replay each prefix that ends at a WorkflowTaskStarted: no errors, same commands as recorded.
	for i, e := range l.h {
		if e.Type() != history.EventWorkflowTaskStarted {
			continue
		}
		if _, err := Replay(Info{}, l.h[:i+1], sumWorkflow); err != nil {
			t.Fatalf("prefix ending at event %d: %v", e.ID, err)
		}
	}
}

func TestNonDeterminismDetected(t *testing.T) {
	l := newLoop(t, sumWorkflow, `null`)
	l.acts["Add"] = addOne
	l.step()
	changed := func(w *WorkflowContext, in json.RawMessage) (json.RawMessage, error) {
		getInt(w, w.ScheduleActivity(act("Add", 1)))
		getInt(w, w.ScheduleActivity(act("Multiply", 2))) // was Add
		return nil, nil
	}
	var cut int
	for i, e := range l.h {
		if e.Type() == history.EventWorkflowTaskStarted {
			cut = i // replay the last complete task
		}
	}
	_, err := Replay(Info{}, l.h[:cut+1], changed)
	var nde *NonDeterministicError
	if !errors.As(err, &nde) {
		t.Fatalf("want NonDeterministicError, got %v", err)
	}
	if !strings.Contains(nde.Expected, "Add") || !strings.Contains(nde.Got, "Multiply") || nde.EventID == 0 {
		t.Fatalf("unhelpful error: %v", nde)
	}
}

// signalOrTimeout waits for an "approve" signal or a 1h timer, whichever comes first.
func signalOrTimeout(w *WorkflowContext, in json.RawMessage) (json.RawMessage, error) {
	_, tf := w.StartTimer(time.Hour)
	ch := w.SignalChannel("approve")
	outcome := ""
	NewSelector().
		AddReceive(ch, func(c *Channel) { v, _ := c.ReceiveAsync(); outcome = "approved:" + string(v.(json.RawMessage)) }).
		AddFuture(tf, func(*Future) { outcome = "timeout" }).
		Select(w.Ctx)
	return json.Marshal(outcome)
}

func TestSignalWins(t *testing.T) {
	l := newLoop(t, signalOrTimeout, `null`)
	l.step()
	l.signal("approve", `"yes"`)
	l.step()
	if got := l.result(); got != `"approved:\"yes\""` {
		t.Fatalf("result %s", got)
	}
}

func TestTimerWins(t *testing.T) {
	l := newLoop(t, signalOrTimeout, `null`)
	l.step()
	l.advance(time.Hour)
	if got := l.result(); got != `"timeout"` {
		t.Fatalf("result %s", got)
	}
}

// C1: a signal that arrives while a task is running is buffered and lands after that task's
// commands, so every later replay sees exactly what the worker saw.
func TestBufferedSignalReplaysExactly(t *testing.T) {
	l := newLoop(t, signalOrTimeout, `null`)
	l.beforeComplete = func(l *loop) { l.signal("approve", `"early"`) }
	l.step()
	if got := l.result(); got != `"approved:\"early\""` {
		t.Fatalf("result %s\n%s", got, dump(l.h))
	}
	for i, e := range l.h {
		if e.Type() == history.EventWorkflowTaskStarted {
			if _, err := Replay(Info{}, l.h[:i+1], signalOrTimeout); err != nil {
				t.Fatalf("replay to %d: %v", e.ID, err)
			}
		}
	}
}

// D12: the code returns while a signal is buffered; the server drops the close, and the next
// task re-sends it.
func TestDroppedCloseIsResent(t *testing.T) {
	quick := func(w *WorkflowContext, in json.RawMessage) (json.RawMessage, error) { return json.Marshal("done") }
	l := newLoop(t, quick, `null`)
	l.beforeComplete = func(l *loop) { l.signal("late", `1`) }
	l.step()
	if got := l.result(); got != `"done"` {
		t.Fatalf("result %s", got)
	}
	if !strings.Contains(dump(l.h), "WorkflowExecutionSignaled") {
		t.Fatal("signal missing from history")
	}
}

// The race fixed in history: the timer fires during the task that cancels it.
func TestCancelRacingFire(t *testing.T) {
	wfn := func(w *WorkflowContext, in json.RawMessage) (json.RawMessage, error) {
		id, tf := w.StartTimer(time.Minute)
		w.SignalChannel("stop").Receive(w.Ctx)
		w.CancelTimer(id)
		_, err := tf.Get(w.Ctx)
		return json.Marshal(fmt.Sprint(err))
	}
	l := newLoop(t, wfn, `null`)
	l.step()
	l.signal("stop", `1`)
	l.beforeComplete = func(l *loop) {
		l.now = l.now.Add(time.Minute)
		var tr history.TimerRequest
		for i, x := range l.timers {
			if x.Ref.Kind == history.TimerUser {
				tr = x
				l.timers = append(l.timers[:i], l.timers[i+1:]...)
				break
			}
		}
		l.must(l.apply(func(m *history.Machine) error { m.FireTimer(tr.Ref); return nil }))
	}
	l.step()
	if got := l.result(); got != `"canceled"` {
		t.Fatalf("result %s\n%s", got, dump(l.h))
	}
}

func TestSideEffectRunsOnce(t *testing.T) {
	calls := 0
	wfn := func(w *WorkflowContext, in json.RawMessage) (json.RawMessage, error) {
		v, err := w.SideEffect(func() (json.RawMessage, error) { calls++; return json.Marshal(calls * 100) })
		if err != nil {
			return nil, err
		}
		getInt(w, w.ScheduleActivity(act("Add", 0)))
		return v, nil
	}
	l := newLoop(t, wfn, `null`)
	l.acts["Add"] = addOne
	l.step()
	if got := l.result(); got != "100" || calls != 1 {
		t.Fatalf("result %s after %d calls; the side effect must run once and replay its value", got, calls)
	}
}

func TestWorkflowErrorFailsRun(t *testing.T) {
	wfn := func(w *WorkflowContext, in json.RawMessage) (json.RawMessage, error) {
		_, err := w.ScheduleActivity(act("Boom", 0)).Get(w.Ctx)
		return nil, err
	}
	l := newLoop(t, wfn, `null`)
	l.acts["Boom"] = func(json.RawMessage) (json.RawMessage, error) { return nil, errors.New("kaput") }
	l.step()
	if got := l.result(); !strings.Contains(got, "failed: activity Boom failed") {
		t.Fatalf("result %s", got)
	}
}

func dump(evs []history.Event) string {
	var b strings.Builder
	for _, e := range evs {
		fmt.Fprintf(&b, "%3d %s\n", e.ID, e.Type())
	}
	return b.String()
}
