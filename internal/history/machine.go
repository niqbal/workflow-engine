package history

import (
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/niqbal/workflow-engine/internal/xsync"
)

// The state machine (WI-015). Every server-side state change is one call on a Machine:
// pure, no I/O. The history service loads MutableState, calls one method, and writes
// Effects (events, timer tasks, dispatches) and the new state in one store transaction.

var (
	ErrStaleTask      = errors.New("history: task is stale")
	ErrInvalidCommand = errors.New("history: invalid command")
	ErrWorkflowClosed = errors.New("history: workflow is closed")
)

// TimerKind names what a timer task does when it fires.
type TimerKind string

const (
	TimerUser                 TimerKind = "user"
	TimerActivityStartToClose TimerKind = "activity_start_to_close"
	TimerActivitySchedToClose TimerKind = "activity_schedule_to_close"
	TimerActivityRetry        TimerKind = "activity_retry"
	TimerWorkflowTaskTimeout  TimerKind = "workflow_task_timeout"
	TimerWorkflowTaskBackoff  TimerKind = "workflow_task_backoff"
	TimerExecutionTimeout     TimerKind = "execution_timeout"
)

// TimerRef identifies what armed a timer task. Firing is a no-op unless the referenced item
// is still pending at the same attempt (SPEC §8.3, review C3).
type TimerRef struct {
	Kind    TimerKind `json:"kind"`
	TimerID string    `json:"timerID,omitempty"` // user timers
	EventID int64     `json:"eventID,omitempty"` // TimerStarted or ActivityTaskScheduled ID
	Attempt int       `json:"attempt,omitempty"` // activity attempt
	Seq     int64     `json:"seq,omitempty"`     // workflow-task sequence number
}

// TimerRequest asks the service to write a timer task.
type TimerRequest struct {
	FireAt time.Time `json:"fireAt"`
	Ref    TimerRef  `json:"ref"`
}

// TaskType is the kind of task matching hands out.
type TaskType string

const (
	WorkflowTaskType TaskType = "workflow"
	ActivityTaskType TaskType = "activity"
)

// DispatchRef asks the service to write a transfer task that pushes a task to matching.
// It is also what the task token carries back on start.
type DispatchRef struct {
	Type             TaskType `json:"type"`
	TaskQueue        string   `json:"taskQueue"`
	ScheduledEventID int64    `json:"scheduledEventID,omitempty"` // activities
	Attempt          int      `json:"attempt"`
	Seq              int64    `json:"seq,omitempty"` // workflow tasks
}

// Effects is everything one transition produced.
type Effects struct {
	Events   []Event
	Timers   []TimerRequest
	Dispatch []DispatchRef
}

// Machine applies transitions to one run's state at one instant.
type Machine struct {
	S   *MutableState
	Now time.Time
	Out Effects
}

// NewMachine wraps state for one transition at time now.
func NewMachine(s *MutableState, now time.Time) *Machine {
	if s.Activities == nil {
		s.Activities = map[int64]*ActivityInfo{}
	}
	if s.Timers == nil {
		s.Timers = map[string]*TimerInfo{}
	}
	return &Machine{S: s, Now: now}
}

// ---- event plumbing -----------------------------------------------------------------------

// stamp returns the event time: never earlier than the previous event (SPEC §10).
func (m *Machine) stamp() time.Time {
	if m.Now.Before(m.S.LastEventTime) {
		return m.S.LastEventTime
	}
	return m.Now
}

// append adds an event to history now and returns its ID.
func (m *Machine) append(a Attributes) int64 {
	e := Event{ID: m.S.NextEventID, Time: m.stamp(), Attrs: a}
	m.S.NextEventID++
	m.S.LastEventTime = e.Time
	m.Out.Events = append(m.Out.Events, e)
	return e.ID
}

// deliver appends a result-like event, or buffers it while a workflow task is started (D12).
// It returns true if the event was appended.
func (m *Machine) deliver(attrs ...Attributes) bool {
	if wt := m.S.WorkflowTask; wt != nil && wt.State == TaskStarted {
		for _, a := range attrs {
			m.S.Buffered = append(m.S.Buffered, BufferedEvent{Event: Event{Attrs: a}})
		}
		return false
	}
	m.appendPatched(attrs)
	return true
}

// appendPatched appends events, filling StartedEventID on activity end events from the
// ActivityTaskStarted appended just before them.
func (m *Machine) appendPatched(attrs []Attributes) {
	started := map[int64]int64{}
	for _, a := range attrs {
		switch x := a.(type) {
		case ActivityTaskStarted:
			started[x.ScheduledEventID] = m.append(x)
			continue
		case ActivityTaskCompleted:
			x.StartedEventID = started[x.ScheduledEventID]
			a = x
		case ActivityTaskFailed:
			x.StartedEventID = started[x.ScheduledEventID]
			a = x
		case ActivityTaskTimedOut:
			x.StartedEventID = started[x.ScheduledEventID]
			a = x
		}
		m.append(a)
	}
}

func (m *Machine) flushBuffered() bool {
	for _, id := range xsync.SortedKeys(m.S.Timers) {
		if m.S.Timers[id].Fired {
			delete(m.S.Timers, id)
		}
	}
	if len(m.S.Buffered) == 0 {
		return false
	}
	attrs := make([]Attributes, len(m.S.Buffered))
	for i, b := range m.S.Buffered {
		attrs[i] = b.Event.Attrs
	}
	m.S.Buffered = nil
	m.appendPatched(attrs)
	return true
}

func (m *Machine) timer(at time.Time, ref TimerRef) {
	m.Out.Timers = append(m.Out.Timers, TimerRequest{FireAt: at, Ref: ref})
}

func (m *Machine) open() error {
	if !m.S.Status.Open() {
		return ErrWorkflowClosed
	}
	return nil
}

// ---- workflow tasks -----------------------------------------------------------------------

// scheduleWorkflowTask creates a persisted attempt-1 workflow task if none is outstanding.
func (m *Machine) scheduleWorkflowTask() {
	if m.S.WorkflowTask != nil || !m.S.Status.Open() {
		return
	}
	id := m.append(WorkflowTaskScheduled{TaskQueue: m.S.TaskQueue, Attempt: 1})
	m.S.WorkflowTaskSeq++
	m.S.WorkflowTask = &WorkflowTaskInfo{ScheduledEventID: id, Attempt: 1, State: TaskScheduled,
		TaskQueue: m.S.TaskQueue, Seq: m.S.WorkflowTaskSeq, ScheduledAt: m.stamp()}
	m.dispatchWorkflowTask()
}

func (m *Machine) dispatchWorkflowTask() {
	wt := m.S.WorkflowTask
	m.Out.Dispatch = append(m.Out.Dispatch, DispatchRef{Type: WorkflowTaskType, TaskQueue: wt.TaskQueue, Attempt: wt.Attempt, Seq: wt.Seq})
}

// Transient reports whether the current workflow task attempt is not persisted (SPEC §4.2).
func (wt *WorkflowTaskInfo) Transient() bool { return wt.ScheduledEventID == 0 }

// WorkflowTaskStarted records that a worker took the task identified by ref. It returns the
// transient events to append to the history sent to the worker (empty for a persisted attempt).
func (m *Machine) WorkflowTaskStarted(ref DispatchRef, identity string) ([]Event, error) {
	if err := m.open(); err != nil {
		return nil, ErrStaleTask
	}
	wt := m.S.WorkflowTask
	if wt == nil || wt.Seq != ref.Seq || wt.State != TaskScheduled {
		return nil, ErrStaleTask
	}
	wt.State, wt.StartedAt, wt.WorkerIdentity = TaskStarted, m.stamp(), identity
	m.timer(m.Now.Add(DefaultWorkflowTaskTimeout), TimerRef{Kind: TimerWorkflowTaskTimeout, Seq: wt.Seq})
	if !wt.Transient() {
		wt.StartedEventID = m.append(WorkflowTaskStarted{ScheduledEventID: wt.ScheduledEventID, WorkerIdentity: identity})
		return nil, nil
	}
	sched := m.S.NextEventID
	return []Event{
		{ID: sched, Time: wt.ScheduledAt, Attrs: WorkflowTaskScheduled{TaskQueue: wt.TaskQueue, Attempt: wt.Attempt}},
		{ID: sched + 1, Time: wt.StartedAt, Attrs: WorkflowTaskStarted{ScheduledEventID: sched, WorkerIdentity: identity}},
	}, nil
}

func (m *Machine) startedTask(seq int64) (*WorkflowTaskInfo, error) {
	if !m.S.Status.Open() {
		return nil, ErrStaleTask
	}
	wt := m.S.WorkflowTask
	if wt == nil || wt.Seq != seq || wt.State != TaskStarted {
		return nil, ErrStaleTask
	}
	return wt, nil
}

// ValidateCommands checks commands against the current state without changing it.
func (m *Machine) ValidateCommands(cmds []Command) error {
	acts, timers := map[string]bool{}, map[string]bool{}
	for _, id := range xsync.SortedKeys(m.S.Activities) {
		acts[m.S.Activities[id].Scheduled.ActivityID] = true
	}
	for _, id := range xsync.SortedKeys(m.S.Timers) {
		timers[id] = true
	}
	for i, c := range cmds {
		bad := func(why string) error {
			return fmt.Errorf("%w: command %d (%s): %s", ErrInvalidCommand, i, c.CommandType(), why)
		}
		switch x := c.(type) {
		case ScheduleActivity:
			switch {
			case x.ActivityID == "" || x.ActivityType == "":
				return bad("activityID and activityType are required")
			case x.StartToCloseTimeout <= 0:
				return bad("startToCloseTimeout is required")
			case acts[x.ActivityID]:
				return bad("activityID " + x.ActivityID + " is already pending")
			}
			acts[x.ActivityID] = true
		case StartTimer:
			switch {
			case x.TimerID == "" || x.Duration <= 0:
				return bad("timerID and a positive duration are required")
			case timers[x.TimerID]:
				return bad("timer " + x.TimerID + " is already pending")
			}
			timers[x.TimerID] = true
		case CancelTimer:
			if !timers[x.TimerID] {
				return bad("timer " + x.TimerID + " is not pending")
			}
			delete(timers, x.TimerID)
		case RecordMarker:
			if x.MarkerName == "" {
				return bad("markerName is required")
			}
		case CompleteWorkflow, FailWorkflow:
		default:
			return bad("unknown command")
		}
	}
	return nil
}

// CompleteWorkflowTask applies the worker's commands (SPEC §4.2, §8.2).
func (m *Machine) CompleteWorkflowTask(seq int64, cmds []Command) error {
	wt, err := m.startedTask(seq)
	if err != nil {
		return err
	}
	if err := m.ValidateCommands(cmds); err != nil {
		return err
	}
	if wt.Transient() {
		// Persist the transient attempt now that it succeeded.
		schedID := m.append(WorkflowTaskScheduled{TaskQueue: wt.TaskQueue, Attempt: wt.Attempt})
		wt.ScheduledEventID = schedID
		wt.StartedEventID = m.append(WorkflowTaskStarted{ScheduledEventID: schedID, WorkerIdentity: wt.WorkerIdentity})
	}
	m.append(WorkflowTaskCompleted{ScheduledEventID: wt.ScheduledEventID, StartedEventID: wt.StartedEventID})
	m.S.WorkflowTask = nil
	hasBuffered := len(m.S.Buffered) > 0
	for _, c := range cmds {
		if IsCloseCommand(c) && hasBuffered {
			break // unhandled events block close; a new task follows (D12)
		}
		if closed := m.applyCommand(c); closed {
			return nil
		}
	}
	if m.flushBuffered() {
		m.scheduleWorkflowTask()
	}
	return nil
}

func (m *Machine) applyCommand(c Command) (closed bool) {
	switch x := c.(type) {
	case ScheduleActivity:
		q := x.TaskQueue
		if q == "" {
			q = m.S.TaskQueue
		}
		ev := ActivityTaskScheduled{ActivityID: x.ActivityID, ActivityType: x.ActivityType, Input: x.Input, TaskQueue: q,
			StartToCloseTimeout: x.StartToCloseTimeout, ScheduleToCloseTimeout: x.ScheduleToCloseTimeout, RetryPolicy: x.RetryPolicy}
		id := m.append(ev)
		m.S.Activities[id] = &ActivityInfo{ScheduledEventID: id, Scheduled: ev, Attempt: 1, State: TaskScheduled, FirstScheduledAt: m.Now}
		m.Out.Dispatch = append(m.Out.Dispatch, DispatchRef{Type: ActivityTaskType, TaskQueue: q, ScheduledEventID: id, Attempt: 1})
		if x.ScheduleToCloseTimeout > 0 {
			m.timer(m.Now.Add(x.ScheduleToCloseTimeout), TimerRef{Kind: TimerActivitySchedToClose, EventID: id})
		}
	case StartTimer:
		at := m.Now.Add(x.Duration)
		id := m.append(TimerStarted{TimerID: x.TimerID, FireAt: at})
		m.S.Timers[x.TimerID] = &TimerInfo{TimerID: x.TimerID, StartedEventID: id, FireAt: at}
		m.timer(at, TimerRef{Kind: TimerUser, TimerID: x.TimerID, EventID: id})
	case CancelTimer:
		t := m.S.Timers[x.TimerID]
		delete(m.S.Timers, x.TimerID)
		m.append(TimerCanceled{TimerID: x.TimerID, StartedEventID: t.StartedEventID})
	case RecordMarker:
		m.append(MarkerRecorded{MarkerName: x.MarkerName, Details: x.Details})
	case CompleteWorkflow:
		m.close(WorkflowExecutionCompleted{Result: x.Result}, StatusCompleted)
		return true
	case FailWorkflow:
		m.close(WorkflowExecutionFailed{Failure: x.Failure}, StatusFailed)
		return true
	}
	return false
}

// close ends the run. Pending work is dropped; its timer and transfer tasks become no-ops.
func (m *Machine) close(a Attributes, st RunStatus) {
	m.append(a)
	m.S.Status = st
	m.S.WorkflowTask = nil
	m.S.Activities = map[int64]*ActivityInfo{}
	m.S.Timers = map[string]*TimerInfo{}
	m.S.Buffered = nil
}

// FailWorkflowTask records a worker-reported failure of the started attempt.
func (m *Machine) FailWorkflowTask(seq int64, cause Failure) error {
	wt, err := m.startedTask(seq)
	if err != nil {
		return err
	}
	m.failWorkflowTask(wt, cause, false)
	return nil
}

// failWorkflowTask ends a failed or timed-out attempt and schedules the next (SPEC §4.2).
func (m *Machine) failWorkflowTask(wt *WorkflowTaskInfo, cause Failure, timedOut bool) {
	if wt.FailuresInSeries == 0 && !wt.Transient() {
		if timedOut {
			m.append(WorkflowTaskTimedOut{ScheduledEventID: wt.ScheduledEventID, StartedEventID: wt.StartedEventID})
		} else {
			m.append(WorkflowTaskFailed{ScheduledEventID: wt.ScheduledEventID, StartedEventID: wt.StartedEventID, Cause: cause})
		}
	}
	// Events the failed attempt never processed go into history now; the next attempt sees them.
	m.flushBuffered()
	next := wt.Attempt + 1
	m.S.WorkflowTaskSeq++
	m.S.WorkflowTask = &WorkflowTaskInfo{Attempt: next, State: TaskScheduled, TaskQueue: wt.TaskQueue,
		Seq: m.S.WorkflowTaskSeq, ScheduledAt: m.stamp(), FailuresInSeries: wt.FailuresInSeries + 1}
	if d := WorkflowTaskRetryDelay(next); d > 0 {
		m.S.WorkflowTask.State = TaskBackoff
		m.timer(m.Now.Add(d), TimerRef{Kind: TimerWorkflowTaskBackoff, Seq: m.S.WorkflowTaskSeq})
		return
	}
	m.dispatchWorkflowTask()
}

// WorkflowTaskRetryDelay is the wait before workflow-task attempt n: none for attempt 2,
// then 1s doubling, capped at MaxWorkflowTaskRetryInterval.
func WorkflowTaskRetryDelay(n int) time.Duration {
	if n <= 2 {
		return 0
	}
	d := time.Second
	for i := 3; i < n; i++ {
		d *= 2
		if d >= MaxWorkflowTaskRetryInterval {
			return MaxWorkflowTaskRetryInterval
		}
	}
	return d
}

// ---- activities ---------------------------------------------------------------------------

// ActivityStarted records that a worker took attempt ref.Attempt. It returns the attempt's
// start-to-close deadline.
func (m *Machine) ActivityStarted(ref DispatchRef, identity string) (*ActivityInfo, time.Time, error) {
	if !m.S.Status.Open() {
		return nil, time.Time{}, ErrStaleTask
	}
	a := m.S.Activities[ref.ScheduledEventID]
	if a == nil || a.Attempt != ref.Attempt || a.State != TaskScheduled {
		return nil, time.Time{}, ErrStaleTask
	}
	a.State, a.StartedAt, a.WorkerIdentity = TaskStarted, m.Now, identity
	deadline := m.Now.Add(a.Scheduled.StartToCloseTimeout)
	m.timer(deadline, TimerRef{Kind: TimerActivityStartToClose, EventID: a.ScheduledEventID, Attempt: a.Attempt})
	return a, deadline, nil
}

func (m *Machine) startedActivity(schedID int64, attempt int) (*ActivityInfo, error) {
	if !m.S.Status.Open() {
		return nil, ErrStaleTask
	}
	a := m.S.Activities[schedID]
	if a == nil || a.Attempt != attempt || a.State != TaskStarted {
		return nil, ErrStaleTask
	}
	return a, nil
}

// endActivity removes the activity and delivers its started and end events.
func (m *Machine) endActivity(a *ActivityInfo, end Attributes) {
	delete(m.S.Activities, a.ScheduledEventID)
	var attrs []Attributes
	if a.State == TaskStarted {
		attrs = append(attrs, ActivityTaskStarted{ScheduledEventID: a.ScheduledEventID, Attempt: a.Attempt, WorkerIdentity: a.WorkerIdentity})
	}
	attrs = append(attrs, end)
	if m.deliver(attrs...) {
		m.scheduleWorkflowTask()
	}
}

// CompleteActivity records a successful attempt.
func (m *Machine) CompleteActivity(schedID int64, attempt int, result json.RawMessage) error {
	a, err := m.startedActivity(schedID, attempt)
	if err != nil {
		return err
	}
	m.endActivity(a, ActivityTaskCompleted{ScheduledEventID: schedID, Result: result})
	return nil
}

// FailActivity records a failed attempt: retry by policy or end the activity.
func (m *Machine) FailActivity(schedID int64, attempt int, f Failure) error {
	a, err := m.startedActivity(schedID, attempt)
	if err != nil {
		return err
	}
	if !f.NonRetryable && !contains(a.Scheduled.RetryPolicy.NonRetryableErrors, f.Type) && m.retry(a, f) {
		return nil
	}
	m.endActivity(a, ActivityTaskFailed{ScheduledEventID: schedID, Failure: f, Attempt: a.Attempt})
	return nil
}

// retry moves the activity to backoff for its next attempt, if the policy allows one.
func (m *Machine) retry(a *ActivityInfo, f Failure) bool {
	p := a.Scheduled.RetryPolicy
	if p.MaximumAttempts > 0 && a.Attempt >= p.MaximumAttempts {
		return false
	}
	delay := p.Backoff(a.Attempt + 1)
	if s2c := a.Scheduled.ScheduleToCloseTimeout; s2c > 0 && !m.Now.Add(delay).Before(a.FirstScheduledAt.Add(s2c)) {
		return false
	}
	a.Attempt++
	a.State, a.StartedAt, a.WorkerIdentity = TaskBackoff, time.Time{}, ""
	failure := f
	a.LastFailure = &failure
	m.timer(m.Now.Add(delay), TimerRef{Kind: TimerActivityRetry, EventID: a.ScheduledEventID, Attempt: a.Attempt})
	return true
}

// ---- external events ----------------------------------------------------------------------

// Signal applies SignalWorkflow with dedup before the closed check (SPEC §8.4, review C5).
func (m *Machine) Signal(name string, payload json.RawMessage, requestID string) error {
	if requestID != "" {
		for _, s := range m.S.Signals {
			if s.RequestID == requestID {
				if s.Accepted {
					return nil
				}
				return ErrWorkflowClosed
			}
		}
	}
	if err := m.open(); err != nil {
		return err
	}
	if m.deliver(WorkflowExecutionSignaled{SignalName: name, Payload: payload, RequestID: requestID}) {
		m.scheduleWorkflowTask()
	}
	if requestID != "" {
		m.S.Signals = append(m.S.Signals, SignalOutcome{RequestID: requestID, Accepted: true})
		if len(m.S.Signals) > SignalDedupLimit {
			m.S.Signals = m.S.Signals[len(m.S.Signals)-SignalDedupLimit:]
		}
	}
	return nil
}

// Terminate closes the run. Terminating a closed run is a no-op.
func (m *Machine) Terminate(reason string) {
	if m.S.Status.Open() {
		m.close(WorkflowExecutionTerminated{Reason: reason}, StatusTerminated)
	}
}

// FireTimer handles a due timer task. Stale refs are no-ops (SPEC §8.3, §10).
func (m *Machine) FireTimer(ref TimerRef) {
	if !m.S.Status.Open() {
		return
	}
	switch ref.Kind {
	case TimerUser:
		t := m.S.Timers[ref.TimerID]
		if t == nil || t.StartedEventID != ref.EventID || t.Fired {
			return
		}
		if m.deliver(TimerFired{TimerID: ref.TimerID, StartedEventID: t.StartedEventID}) {
			delete(m.S.Timers, ref.TimerID)
			m.scheduleWorkflowTask()
		} else {
			t.Fired = true // buffered: stays cancellable until its event lands
		}
	case TimerActivityStartToClose:
		a := m.S.Activities[ref.EventID]
		if a == nil || a.Attempt != ref.Attempt || a.State != TaskStarted {
			return
		}
		f := Failure{Type: "StartToCloseTimeout", Message: "activity attempt timed out"}
		if !m.retry(a, f) {
			m.endActivity(a, ActivityTaskTimedOut{ScheduledEventID: a.ScheduledEventID, TimeoutType: TimeoutStartToClose, Attempt: a.Attempt})
		}
	case TimerActivitySchedToClose:
		a := m.S.Activities[ref.EventID]
		if a == nil {
			return
		}
		m.endActivity(a, ActivityTaskTimedOut{ScheduledEventID: a.ScheduledEventID, TimeoutType: TimeoutScheduleToClose, Attempt: a.Attempt})
	case TimerActivityRetry:
		a := m.S.Activities[ref.EventID]
		if a == nil || a.Attempt != ref.Attempt || a.State != TaskBackoff {
			return
		}
		a.State = TaskScheduled
		m.Out.Dispatch = append(m.Out.Dispatch, DispatchRef{Type: ActivityTaskType, TaskQueue: a.Scheduled.TaskQueue, ScheduledEventID: a.ScheduledEventID, Attempt: a.Attempt})
	case TimerWorkflowTaskTimeout:
		wt := m.S.WorkflowTask
		if wt == nil || wt.Seq != ref.Seq || wt.State != TaskStarted {
			return
		}
		m.failWorkflowTask(wt, Failure{Type: "WorkflowTaskTimeout"}, true)
	case TimerWorkflowTaskBackoff:
		wt := m.S.WorkflowTask
		if wt == nil || wt.Seq != ref.Seq || wt.State != TaskBackoff {
			return
		}
		wt.State = TaskScheduled
		m.dispatchWorkflowTask()
	case TimerExecutionTimeout:
		m.close(WorkflowExecutionTimedOut{}, StatusTimedOut)
	}
}

// ---- start --------------------------------------------------------------------------------

// StartInput is a validated start request.
type StartInput struct {
	WorkflowID, RunID, WorkflowType, TaskQueue, RequestID string
	Input                                                 json.RawMessage
	ExecutionTimeout                                      time.Duration
}

// NewRun builds the state and effects of a new run: Started, WorkflowTaskScheduled, dispatch,
// and the execution timeout if any.
func NewRun(in StartInput, now time.Time) (*MutableState, Effects) {
	s := &MutableState{WorkflowID: in.WorkflowID, RunID: in.RunID, WorkflowType: in.WorkflowType, TaskQueue: in.TaskQueue,
		Status: StatusRunning, StartRequestID: in.RequestID, StartedAt: now, NextEventID: 1}
	m := NewMachine(s, now)
	m.append(WorkflowExecutionStarted{WorkflowType: in.WorkflowType, Input: in.Input, TaskQueue: in.TaskQueue,
		ExecutionTimeout: in.ExecutionTimeout, RequestID: in.RequestID})
	m.scheduleWorkflowTask()
	if in.ExecutionTimeout > 0 {
		m.timer(now.Add(in.ExecutionTimeout), TimerRef{Kind: TimerExecutionTimeout})
	}
	return s, m.Out
}

// ---- helpers ------------------------------------------------------------------------------

func contains(xs []string, s string) bool {
	for _, x := range xs {
		if x == s {
			return true
		}
	}
	return false
}
