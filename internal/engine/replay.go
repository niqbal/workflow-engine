package engine

import (
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/niqbal/workflow-engine/internal/history"
)

// The replayer (WI-013, SPEC §7.2-7.3). It feeds a history through workflow code in batches
// that end at each WorkflowTaskStarted, checks that every command-generated event matches
// the command the code produced, and returns the commands of the final, new step.

// NonDeterministicError means the workflow code no longer produces the commands its history
// recorded. The worker fails the task; the server keeps the run blocked until a fixed deploy
// (D6).
type NonDeterministicError struct {
	EventID  int64
	Expected string // what history recorded
	Got      string // what the code produced
}

func (e *NonDeterministicError) Error() string {
	return fmt.Sprintf("non-deterministic workflow: event %d: history has %s, code produced %s", e.EventID, e.Expected, e.Got)
}

// ActivityError is the error a failed or timed-out activity's future returns.
type ActivityError struct {
	ActivityType string
	Failure      history.Failure
	TimedOut     history.TimeoutType // set for timeouts
}

func (e *ActivityError) Error() string {
	if e.TimedOut != "" {
		return fmt.Sprintf("activity %s timed out (%s)", e.ActivityType, e.TimedOut)
	}
	return fmt.Sprintf("activity %s failed: %s: %s", e.ActivityType, e.Failure.Type, e.Failure.Message)
}

// ErrCanceled is returned by a canceled timer's future.
var ErrCanceled = errors.New("canceled")

// Info describes the run being executed.
type Info struct {
	WorkflowID, RunID, WorkflowType, TaskQueue string
}

// Workflow is workflow code at the engine level: untyped JSON in and out.
type Workflow func(ctx *WorkflowContext, input json.RawMessage) (json.RawMessage, error)

type command struct {
	cmd    history.Command
	future *Future // activity result or timer
	timer  string  // timer ID for StartTimer
}

// WorkflowContext is what the SDK (package wf) builds on. One per execution; Ctx identifies
// the calling coroutine.
type WorkflowContext struct {
	*Ctx
	ex *execution
}

type execution struct {
	info        Info
	d           *Dispatcher
	now         time.Time
	pending     []*command // produced by the latest step, not yet matched
	activities  map[int64]*command
	timers      map[string]*command // by timer ID
	signals     map[string]*Channel
	actSeq      int
	timerSeq    int
	sideEffects []json.RawMessage
	sideIdx     int
	versions    map[string]int
	done        bool
	result      json.RawMessage
	err         error
	replaying   bool
}

// Replay runs fn over the full history of a workflow task (ending with its
// WorkflowTaskStarted, transient events included) and returns the new commands.
func Replay(info Info, events []history.Event, fn Workflow) ([]history.Command, error) {
	if len(events) == 0 {
		return nil, errors.New("engine: empty history")
	}
	started, ok := events[0].Attrs.(history.WorkflowExecutionStarted)
	if !ok {
		return nil, errors.New("engine: history must start with WorkflowExecutionStarted")
	}
	ex := &execution{info: info, activities: map[int64]*command{}, timers: map[string]*command{},
		signals: map[string]*Channel{}, versions: map[string]int{}}
	ex.prescan(events)
	ex.d = NewDispatcher(func(c *Ctx) {
		res, err := fn(&WorkflowContext{Ctx: c, ex: ex}, started.Input)
		ex.done, ex.result, ex.err = true, res, err
		if err != nil {
			ex.emit(&command{cmd: history.FailWorkflow{Failure: failureOf(err)}})
		} else {
			ex.emit(&command{cmd: history.CompleteWorkflow{Result: res}})
		}
	})
	defer ex.d.Close()

	ptr := 0 // next pending command to match
	for i, e := range events {
		switch a := e.Attrs.(type) {
		case history.ActivityTaskScheduled, history.TimerStarted, history.TimerCanceled,
			history.MarkerRecorded, history.WorkflowExecutionCompleted, history.WorkflowExecutionFailed:
			if err := ex.match(e, &ptr); err != nil {
				return nil, err
			}
		case history.WorkflowTaskStarted:
			if i+1 < len(events) {
				if t := events[i+1].Type(); t == history.EventWorkflowTaskFailed || t == history.EventWorkflowTaskTimedOut {
					continue // that attempt's commands were discarded
				}
			}
			if err := ex.checkAllMatched(e.ID, ptr); err != nil {
				return nil, err
			}
			ex.pending, ptr = nil, 0
			ex.now = e.Time
			ex.replaying = i+1 < len(events)
			if err := ex.d.ExecuteUntilAllBlocked(); err != nil {
				return nil, err
			}
		case history.ActivityTaskCompleted:
			if c := ex.activities[a.ScheduledEventID]; c != nil && !c.future.IsReady() {
				c.future.Set(a.Result, nil)
			}
		case history.ActivityTaskFailed:
			if c := ex.activities[a.ScheduledEventID]; c != nil && !c.future.IsReady() {
				c.future.Set(nil, &ActivityError{ActivityType: c.cmd.(history.ScheduleActivity).ActivityType, Failure: a.Failure})
			}
		case history.ActivityTaskTimedOut:
			if c := ex.activities[a.ScheduledEventID]; c != nil && !c.future.IsReady() {
				c.future.Set(nil, &ActivityError{ActivityType: c.cmd.(history.ScheduleActivity).ActivityType, TimedOut: a.TimeoutType})
			}
		case history.TimerFired:
			if c := ex.timers[a.TimerID]; c != nil && !c.future.IsReady() {
				c.future.Set(nil, nil)
			}
		case history.WorkflowExecutionSignaled:
			ex.signal(a.SignalName).Send(a.Payload)
		}
	}
	if last := events[len(events)-1]; last.Type() != history.EventWorkflowTaskStarted {
		return nil, fmt.Errorf("engine: history must end with WorkflowTaskStarted, ends with %s", last.Type())
	}
	out := make([]history.Command, 0, len(ex.pending))
	hasClose := false
	for _, c := range ex.pending {
		out = append(out, c.cmd)
		hasClose = hasClose || history.IsCloseCommand(c.cmd)
	}
	// A close command the server dropped (buffered events, D12) is re-sent once the code has
	// returned: its result does not change.
	if ex.done && !hasClose {
		if ex.err != nil {
			out = append(out, history.FailWorkflow{Failure: failureOf(ex.err)})
		} else {
			out = append(out, history.CompleteWorkflow{Result: ex.result})
		}
	}
	return out, nil
}

// prescan indexes side-effect and version markers, which code reads synchronously.
func (ex *execution) prescan(events []history.Event) {
	for _, e := range events {
		m, ok := e.Attrs.(history.MarkerRecorded)
		if !ok {
			continue
		}
		switch m.MarkerName {
		case history.MarkerSideEffect:
			ex.sideEffects = append(ex.sideEffects, m.Details)
		case history.MarkerVersion:
			var v versionMarker
			if json.Unmarshal(m.Details, &v) == nil {
				ex.versions[v.ChangeID] = v.Version
			}
		}
	}
}

type versionMarker struct {
	ChangeID string `json:"changeID"`
	Version  int    `json:"version"`
}

func describe(c history.Command) string {
	switch x := c.(type) {
	case history.ScheduleActivity:
		return fmt.Sprintf("ScheduleActivity(%s, %s)", x.ActivityID, x.ActivityType)
	case history.StartTimer:
		return fmt.Sprintf("StartTimer(%s)", x.TimerID)
	case history.CancelTimer:
		return fmt.Sprintf("CancelTimer(%s)", x.TimerID)
	case history.RecordMarker:
		return fmt.Sprintf("RecordMarker(%s)", x.MarkerName)
	}
	return string(c.CommandType())
}

func describeEvent(e history.Event) string {
	switch x := e.Attrs.(type) {
	case history.ActivityTaskScheduled:
		return fmt.Sprintf("ActivityTaskScheduled(%s, %s)", x.ActivityID, x.ActivityType)
	case history.TimerStarted:
		return fmt.Sprintf("TimerStarted(%s)", x.TimerID)
	case history.TimerCanceled:
		return fmt.Sprintf("TimerCanceled(%s)", x.TimerID)
	case history.MarkerRecorded:
		return fmt.Sprintf("MarkerRecorded(%s)", x.MarkerName)
	}
	return string(e.Type())
}

// match pairs a command-generated event with the next pending command.
func (ex *execution) match(e history.Event, ptr *int) error {
	nde := func(got string) error {
		return &NonDeterministicError{EventID: e.ID, Expected: describeEvent(e), Got: got}
	}
	if *ptr >= len(ex.pending) {
		return nde("nothing")
	}
	c := ex.pending[*ptr]
	*ptr++
	ok := false
	switch a := e.Attrs.(type) {
	case history.ActivityTaskScheduled:
		if x, is := c.cmd.(history.ScheduleActivity); is && x.ActivityID == a.ActivityID && x.ActivityType == a.ActivityType {
			ex.activities[e.ID], ok = c, true
		}
	case history.TimerStarted:
		if x, is := c.cmd.(history.StartTimer); is && x.TimerID == a.TimerID {
			ok = true
		}
	case history.TimerCanceled:
		x, is := c.cmd.(history.CancelTimer)
		ok = is && x.TimerID == a.TimerID
	case history.MarkerRecorded:
		x, is := c.cmd.(history.RecordMarker)
		ok = is && x.MarkerName == a.MarkerName
	case history.WorkflowExecutionCompleted:
		_, ok = c.cmd.(history.CompleteWorkflow)
	case history.WorkflowExecutionFailed:
		_, ok = c.cmd.(history.FailWorkflow)
	}
	if !ok {
		return nde(describe(c.cmd))
	}
	return nil
}

// checkAllMatched: every command of the previous step must have its event, except a close
// command the server dropped because events were buffered (D12).
func (ex *execution) checkAllMatched(eventID int64, ptr int) error {
	for _, c := range ex.pending[ptr:] {
		if history.IsCloseCommand(c.cmd) {
			return nil // the rest of the step was never applied
		}
		return &NonDeterministicError{EventID: eventID, Expected: "no event", Got: describe(c.cmd)}
	}
	return nil
}

func (ex *execution) emit(c *command) {
	ex.pending = append(ex.pending, c)
}

func (ex *execution) signal(name string) *Channel {
	ch := ex.signals[name]
	if ch == nil {
		ch = NewChannel()
		ex.signals[name] = ch
	}
	return ch
}

func failureOf(err error) history.Failure {
	var ae *ActivityError
	if errors.As(err, &ae) {
		return history.Failure{Type: "ActivityError", Message: err.Error()}
	}
	return history.Failure{Type: "WorkflowError", Message: err.Error()}
}

// ---- operations the SDK uses ------------------------------------------------------------

// Info returns the run's identity.
func (w *WorkflowContext) Info() Info { return w.ex.info }

// Now is the time of the current workflow task (its WorkflowTaskStarted event).
func (w *WorkflowContext) Now() time.Time { return w.ex.now }

// IsReplaying reports whether the current step is being replayed from history.
func (w *WorkflowContext) IsReplaying() bool { return w.ex.replaying }

// closed reports whether the root function has returned; later commands are dropped.
func (w *WorkflowContext) closed() bool { return w.ex.done }

// ScheduleActivity emits a ScheduleActivity command; its future gets the JSON result.
// An empty ActivityID is assigned deterministically ("1", "2", ...).
func (w *WorkflowContext) ScheduleActivity(cmd history.ScheduleActivity) *Future {
	f := NewFuture()
	if w.closed() {
		return f
	}
	w.ex.actSeq++
	if cmd.ActivityID == "" {
		cmd.ActivityID = fmt.Sprint(w.ex.actSeq)
	}
	w.ex.emit(&command{cmd: cmd, future: f})
	return f
}

// StartTimer emits a StartTimer command and returns its ID and future.
func (w *WorkflowContext) StartTimer(d time.Duration) (string, *Future) {
	f := NewFuture()
	if w.closed() {
		return "", f
	}
	w.ex.timerSeq++
	id := fmt.Sprintf("t%d", w.ex.timerSeq)
	c := &command{cmd: history.StartTimer{TimerID: id, Duration: d}, future: f, timer: id}
	w.ex.timers[id] = c
	w.ex.emit(c)
	return id, f
}

// CancelTimer cancels a pending timer: it emits CancelTimer and resolves the timer's future
// with ErrCanceled. Canceling a fired or unknown timer does nothing.
func (w *WorkflowContext) CancelTimer(id string) {
	c := w.ex.timers[id]
	if c == nil || c.future.IsReady() || w.closed() {
		return
	}
	c.future.Set(nil, ErrCanceled)
	w.ex.emit(&command{cmd: history.CancelTimer{TimerID: id}})
}

// SignalChannel returns the channel that receives signals with this name (JSON payloads).
func (w *WorkflowContext) SignalChannel(name string) *Channel { return w.ex.signal(name) }

// SideEffect returns the recorded value on replay; on first execution it runs fn once and
// records the value as a marker.
func (w *WorkflowContext) SideEffect(fn func() (json.RawMessage, error)) (json.RawMessage, error) {
	i := w.ex.sideIdx
	w.ex.sideIdx++
	var v json.RawMessage
	if i < len(w.ex.sideEffects) {
		v = w.ex.sideEffects[i]
	} else {
		var err error
		if v, err = fn(); err != nil {
			return nil, err
		}
		w.ex.sideEffects = append(w.ex.sideEffects, v)
	}
	if !w.closed() {
		w.ex.emit(&command{cmd: history.RecordMarker{MarkerName: history.MarkerSideEffect, Details: v}})
	}
	return v, nil
}

// Go starts a workflow coroutine.
func (w *WorkflowContext) Go(name string, fn func(*WorkflowContext)) {
	w.Ctx.Dispatcher().Go(name, func(c *Ctx) { fn(&WorkflowContext{Ctx: c, ex: w.ex}) })
}
