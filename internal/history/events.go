// Package history defines the event history, workflow commands and per-run mutable state
// (SPEC §4, §8). Everything here is pure data and pure functions: no I/O, no clock.
package history

import (
	"encoding/json"
	"fmt"
	"time"
)

// EventType names an event in a run's history.
type EventType string

const (
	EventWorkflowExecutionStarted    EventType = "WorkflowExecutionStarted"
	EventWorkflowTaskScheduled       EventType = "WorkflowTaskScheduled"
	EventWorkflowTaskStarted         EventType = "WorkflowTaskStarted"
	EventWorkflowTaskCompleted       EventType = "WorkflowTaskCompleted"
	EventWorkflowTaskTimedOut        EventType = "WorkflowTaskTimedOut"
	EventWorkflowTaskFailed          EventType = "WorkflowTaskFailed"
	EventActivityTaskScheduled       EventType = "ActivityTaskScheduled"
	EventActivityTaskStarted         EventType = "ActivityTaskStarted"
	EventActivityTaskCompleted       EventType = "ActivityTaskCompleted"
	EventActivityTaskFailed          EventType = "ActivityTaskFailed"
	EventActivityTaskTimedOut        EventType = "ActivityTaskTimedOut"
	EventTimerStarted                EventType = "TimerStarted"
	EventTimerFired                  EventType = "TimerFired"
	EventTimerCanceled               EventType = "TimerCanceled"
	EventWorkflowExecutionSignaled   EventType = "WorkflowExecutionSignaled"
	EventMarkerRecorded              EventType = "MarkerRecorded"
	EventWorkflowExecutionCompleted  EventType = "WorkflowExecutionCompleted"
	EventWorkflowExecutionFailed     EventType = "WorkflowExecutionFailed"
	EventWorkflowExecutionTimedOut   EventType = "WorkflowExecutionTimedOut"
	EventWorkflowExecutionTerminated EventType = "WorkflowExecutionTerminated"
)

// Attributes is the type-specific payload of an event.
type Attributes interface{ EventType() EventType }

// Event is one entry in a run's history. IDs start at 1 and are contiguous (SPEC §4.2).
type Event struct {
	ID    int64
	Time  time.Time
	Attrs Attributes
}

// Type returns the event's type.
func (e Event) Type() EventType { return e.Attrs.EventType() }

// Failure describes an error crossing the worker/server boundary.
type Failure struct {
	Type         string `json:"type"` // error type name; matched against RetryPolicy.NonRetryableErrors
	Message      string `json:"message"`
	NonRetryable bool   `json:"nonRetryable,omitempty"` // the activity says: do not retry, whatever the policy
}

// RetryPolicy controls activity retries (SPEC §3.1). Zero fields take defaults (see Normalize).
type RetryPolicy struct {
	InitialInterval    time.Duration `json:"initialInterval,omitempty"`
	BackoffCoefficient float64       `json:"backoffCoefficient,omitempty"`
	MaximumInterval    time.Duration `json:"maximumInterval,omitempty"`
	MaximumAttempts    int           `json:"maximumAttempts,omitempty"` // 0 = unlimited
	NonRetryableErrors []string      `json:"nonRetryableErrors,omitempty"`
}

// ---- Event attributes -------------------------------------------------------------------

type WorkflowExecutionStarted struct {
	WorkflowType     string          `json:"workflowType"`
	Input            json.RawMessage `json:"input,omitempty"`
	TaskQueue        string          `json:"taskQueue"`
	ExecutionTimeout time.Duration   `json:"executionTimeout,omitempty"`
	RequestID        string          `json:"requestID,omitempty"`
}

type WorkflowTaskScheduled struct {
	TaskQueue string `json:"taskQueue"`
	Attempt   int    `json:"attempt"`
}

type WorkflowTaskStarted struct {
	ScheduledEventID int64  `json:"scheduledEventID"`
	WorkerIdentity   string `json:"workerIdentity,omitempty"`
}

type WorkflowTaskCompleted struct {
	ScheduledEventID int64 `json:"scheduledEventID"`
	StartedEventID   int64 `json:"startedEventID"`
}

type WorkflowTaskTimedOut struct {
	ScheduledEventID int64 `json:"scheduledEventID"`
	StartedEventID   int64 `json:"startedEventID,omitempty"`
}

type WorkflowTaskFailed struct {
	ScheduledEventID int64   `json:"scheduledEventID"`
	StartedEventID   int64   `json:"startedEventID"`
	Cause            Failure `json:"cause"`
}

type ActivityTaskScheduled struct {
	ActivityID             string          `json:"activityID"`
	ActivityType           string          `json:"activityType"`
	Input                  json.RawMessage `json:"input,omitempty"`
	TaskQueue              string          `json:"taskQueue"`
	StartToCloseTimeout    time.Duration   `json:"startToCloseTimeout"`
	ScheduleToCloseTimeout time.Duration   `json:"scheduleToCloseTimeout,omitempty"`
	RetryPolicy            RetryPolicy     `json:"retryPolicy"`
}

type ActivityTaskStarted struct {
	ScheduledEventID int64  `json:"scheduledEventID"`
	Attempt          int    `json:"attempt"`
	WorkerIdentity   string `json:"workerIdentity,omitempty"`
}

type ActivityTaskCompleted struct {
	ScheduledEventID int64           `json:"scheduledEventID"`
	StartedEventID   int64           `json:"startedEventID"`
	Result           json.RawMessage `json:"result,omitempty"`
}

type ActivityTaskFailed struct {
	ScheduledEventID int64   `json:"scheduledEventID"`
	StartedEventID   int64   `json:"startedEventID"`
	Failure          Failure `json:"failure"`
	Attempt          int     `json:"attempt"`
}

// TimeoutType names which activity timeout fired.
type TimeoutType string

const (
	TimeoutStartToClose    TimeoutType = "start_to_close"
	TimeoutScheduleToClose TimeoutType = "schedule_to_close"
)

type ActivityTaskTimedOut struct {
	ScheduledEventID int64       `json:"scheduledEventID"`
	StartedEventID   int64       `json:"startedEventID,omitempty"`
	TimeoutType      TimeoutType `json:"timeoutType"`
	Attempt          int         `json:"attempt"`
}

type TimerStarted struct {
	TimerID string    `json:"timerID"`
	FireAt  time.Time `json:"fireAt"`
}

type TimerFired struct {
	TimerID        string `json:"timerID"`
	StartedEventID int64  `json:"startedEventID"`
}

type TimerCanceled struct {
	TimerID        string `json:"timerID"`
	StartedEventID int64  `json:"startedEventID"`
}

type WorkflowExecutionSignaled struct {
	SignalName string          `json:"signalName"`
	Payload    json.RawMessage `json:"payload,omitempty"`
	RequestID  string          `json:"requestID,omitempty"`
}

// Marker names used by the SDK (SPEC §7.4, §3.1).
const (
	MarkerVersion    = "version"
	MarkerSideEffect = "side_effect"
)

type MarkerRecorded struct {
	MarkerName string          `json:"markerName"`
	Details    json.RawMessage `json:"details,omitempty"`
}

type WorkflowExecutionCompleted struct {
	Result json.RawMessage `json:"result,omitempty"`
}

type WorkflowExecutionFailed struct {
	Failure Failure `json:"failure"`
}

type WorkflowExecutionTimedOut struct{}

type WorkflowExecutionTerminated struct {
	Reason string `json:"reason,omitempty"`
}

func (WorkflowExecutionStarted) EventType() EventType    { return EventWorkflowExecutionStarted }
func (WorkflowTaskScheduled) EventType() EventType       { return EventWorkflowTaskScheduled }
func (WorkflowTaskStarted) EventType() EventType         { return EventWorkflowTaskStarted }
func (WorkflowTaskCompleted) EventType() EventType       { return EventWorkflowTaskCompleted }
func (WorkflowTaskTimedOut) EventType() EventType        { return EventWorkflowTaskTimedOut }
func (WorkflowTaskFailed) EventType() EventType          { return EventWorkflowTaskFailed }
func (ActivityTaskScheduled) EventType() EventType       { return EventActivityTaskScheduled }
func (ActivityTaskStarted) EventType() EventType         { return EventActivityTaskStarted }
func (ActivityTaskCompleted) EventType() EventType       { return EventActivityTaskCompleted }
func (ActivityTaskFailed) EventType() EventType          { return EventActivityTaskFailed }
func (ActivityTaskTimedOut) EventType() EventType        { return EventActivityTaskTimedOut }
func (TimerStarted) EventType() EventType                { return EventTimerStarted }
func (TimerFired) EventType() EventType                  { return EventTimerFired }
func (TimerCanceled) EventType() EventType               { return EventTimerCanceled }
func (WorkflowExecutionSignaled) EventType() EventType   { return EventWorkflowExecutionSignaled }
func (MarkerRecorded) EventType() EventType              { return EventMarkerRecorded }
func (WorkflowExecutionCompleted) EventType() EventType  { return EventWorkflowExecutionCompleted }
func (WorkflowExecutionFailed) EventType() EventType     { return EventWorkflowExecutionFailed }
func (WorkflowExecutionTimedOut) EventType() EventType   { return EventWorkflowExecutionTimedOut }
func (WorkflowExecutionTerminated) EventType() EventType { return EventWorkflowExecutionTerminated }

var eventFactories = map[EventType]func() Attributes{
	EventWorkflowExecutionStarted:    func() Attributes { return &WorkflowExecutionStarted{} },
	EventWorkflowTaskScheduled:       func() Attributes { return &WorkflowTaskScheduled{} },
	EventWorkflowTaskStarted:         func() Attributes { return &WorkflowTaskStarted{} },
	EventWorkflowTaskCompleted:       func() Attributes { return &WorkflowTaskCompleted{} },
	EventWorkflowTaskTimedOut:        func() Attributes { return &WorkflowTaskTimedOut{} },
	EventWorkflowTaskFailed:          func() Attributes { return &WorkflowTaskFailed{} },
	EventActivityTaskScheduled:       func() Attributes { return &ActivityTaskScheduled{} },
	EventActivityTaskStarted:         func() Attributes { return &ActivityTaskStarted{} },
	EventActivityTaskCompleted:       func() Attributes { return &ActivityTaskCompleted{} },
	EventActivityTaskFailed:          func() Attributes { return &ActivityTaskFailed{} },
	EventActivityTaskTimedOut:        func() Attributes { return &ActivityTaskTimedOut{} },
	EventTimerStarted:                func() Attributes { return &TimerStarted{} },
	EventTimerFired:                  func() Attributes { return &TimerFired{} },
	EventTimerCanceled:               func() Attributes { return &TimerCanceled{} },
	EventWorkflowExecutionSignaled:   func() Attributes { return &WorkflowExecutionSignaled{} },
	EventMarkerRecorded:              func() Attributes { return &MarkerRecorded{} },
	EventWorkflowExecutionCompleted:  func() Attributes { return &WorkflowExecutionCompleted{} },
	EventWorkflowExecutionFailed:     func() Attributes { return &WorkflowExecutionFailed{} },
	EventWorkflowExecutionTimedOut:   func() Attributes { return &WorkflowExecutionTimedOut{} },
	EventWorkflowExecutionTerminated: func() Attributes { return &WorkflowExecutionTerminated{} },
}

// AllEventTypes lists every event type, in declaration order.
func AllEventTypes() []EventType {
	return []EventType{
		EventWorkflowExecutionStarted, EventWorkflowTaskScheduled, EventWorkflowTaskStarted,
		EventWorkflowTaskCompleted, EventWorkflowTaskTimedOut, EventWorkflowTaskFailed,
		EventActivityTaskScheduled, EventActivityTaskStarted, EventActivityTaskCompleted,
		EventActivityTaskFailed, EventActivityTaskTimedOut, EventTimerStarted, EventTimerFired,
		EventTimerCanceled, EventWorkflowExecutionSignaled, EventMarkerRecorded,
		EventWorkflowExecutionCompleted, EventWorkflowExecutionFailed,
		EventWorkflowExecutionTimedOut, EventWorkflowExecutionTerminated,
	}
}

// IsClose reports whether an event closes the run.
func IsClose(t EventType) bool {
	switch t {
	case EventWorkflowExecutionCompleted, EventWorkflowExecutionFailed,
		EventWorkflowExecutionTimedOut, EventWorkflowExecutionTerminated:
		return true
	}
	return false
}

type eventJSON struct {
	ID    int64           `json:"id"`
	Time  time.Time       `json:"time"`
	Type  EventType       `json:"type"`
	Attrs json.RawMessage `json:"attrs"`
}

// MarshalJSON writes {"id","time","type","attrs"}.
func (e Event) MarshalJSON() ([]byte, error) {
	if e.Attrs == nil {
		return nil, fmt.Errorf("history: event %d has no attributes", e.ID)
	}
	a, err := json.Marshal(e.Attrs)
	if err != nil {
		return nil, err
	}
	return json.Marshal(eventJSON{ID: e.ID, Time: e.Time, Type: e.Attrs.EventType(), Attrs: a})
}

// UnmarshalJSON reads the format written by MarshalJSON. Attrs is set to a value (not pointer).
func (e *Event) UnmarshalJSON(b []byte) error {
	var j eventJSON
	if err := json.Unmarshal(b, &j); err != nil {
		return err
	}
	f, ok := eventFactories[j.Type]
	if !ok {
		return fmt.Errorf("history: unknown event type %q", j.Type)
	}
	p := f()
	if len(j.Attrs) > 0 {
		if err := json.Unmarshal(j.Attrs, p); err != nil {
			return fmt.Errorf("history: event %d (%s): %w", j.ID, j.Type, err)
		}
	}
	e.ID, e.Time, e.Attrs = j.ID, j.Time, deref(p)
	return nil
}
