package history

import (
	"encoding/json"
	"time"
)

// Defaults (SPEC §3.1, §8). Contract and scenario tests rely on these values.
const (
	DefaultWorkflowTaskTimeout   = 10 * time.Second
	DefaultRedispatchInterval    = 30 * time.Second
	DefaultInitialInterval       = time.Second
	DefaultBackoffCoefficient    = 2.0
	MaxWorkflowTaskRetryInterval = 10 * time.Minute
	SignalDedupLimit             = 1000 // SPEC G7
	MaxConflictRetries           = 5
)

// Normalize fills RetryPolicy defaults.
func (p RetryPolicy) Normalize() RetryPolicy {
	if p.InitialInterval <= 0 {
		p.InitialInterval = DefaultInitialInterval
	}
	if p.BackoffCoefficient < 1 {
		p.BackoffCoefficient = DefaultBackoffCoefficient
	}
	if p.MaximumInterval <= 0 {
		p.MaximumInterval = 100 * p.InitialInterval
	}
	return p
}

// Backoff returns the delay before attempt `next` (2, 3, ...), after attempt next-1 failed.
func (p RetryPolicy) Backoff(next int) time.Duration {
	p = p.Normalize()
	d := float64(p.InitialInterval)
	for i := 2; i < next; i++ {
		d *= p.BackoffCoefficient
		if d >= float64(p.MaximumInterval) {
			return p.MaximumInterval
		}
	}
	if time.Duration(d) > p.MaximumInterval {
		return p.MaximumInterval
	}
	return time.Duration(d)
}

// RunStatus is a run's lifecycle state.
type RunStatus string

const (
	StatusRunning    RunStatus = "running"
	StatusCompleted  RunStatus = "completed"
	StatusFailed     RunStatus = "failed"
	StatusTimedOut   RunStatus = "timed_out"
	StatusTerminated RunStatus = "terminated"
)

// Open reports whether the run still accepts events.
func (s RunStatus) Open() bool { return s == StatusRunning }

// TaskState is where a workflow task or activity attempt is.
type TaskState string

const (
	TaskScheduled TaskState = "scheduled"
	TaskStarted   TaskState = "started"
	TaskBackoff   TaskState = "backoff" // activity waiting for its retry timer
)

// WorkflowTaskInfo is the run's single outstanding workflow task, if any.
type WorkflowTaskInfo struct {
	ScheduledEventID int64     `json:"scheduledEventID"` // 0 while not yet in history (see Attempt > 1)
	StartedEventID   int64     `json:"startedEventID,omitempty"`
	Attempt          int       `json:"attempt"`
	State            TaskState `json:"state"`
	TaskQueue        string    `json:"taskQueue"`
	StartedAt        time.Time `json:"startedAt,omitempty"`
	FailuresInSeries int       `json:"failuresInSeries,omitempty"` // SPEC §4.2
}

// ActivityInfo is a scheduled activity that has not ended. Keyed by ScheduledEventID.
type ActivityInfo struct {
	ScheduledEventID int64                 `json:"scheduledEventID"`
	Scheduled        ActivityTaskScheduled `json:"scheduled"`
	Attempt          int                   `json:"attempt"`
	State            TaskState             `json:"state"`
	FirstScheduledAt time.Time             `json:"firstScheduledAt"`
	StartedAt        time.Time             `json:"startedAt,omitempty"`
	WorkerIdentity   string                `json:"workerIdentity,omitempty"`
	LastFailure      *Failure              `json:"lastFailure,omitempty"`
}

// TimerInfo is a started timer that has not fired or been canceled. Keyed by TimerID.
type TimerInfo struct {
	TimerID        string    `json:"timerID"`
	StartedEventID int64     `json:"startedEventID"`
	FireAt         time.Time `json:"fireAt"`
}

// BufferedEvent is an event held while a workflow task is started (SPEC §4.2, D12).
// It gets its ID and time when appended.
type BufferedEvent struct {
	Event Event `json:"event"`
}

// SignalOutcome is remembered per requestID so retries return the original result (SPEC G7).
type SignalOutcome struct {
	RequestID string `json:"requestID"`
	Accepted  bool   `json:"accepted"`
}

// MutableState is the server's summary of one run, written atomically with its events.
type MutableState struct {
	WorkflowID     string    `json:"workflowID"`
	RunID          string    `json:"runID"`
	WorkflowType   string    `json:"workflowType"`
	TaskQueue      string    `json:"taskQueue"`
	Status         RunStatus `json:"status"`
	StartRequestID string    `json:"startRequestID,omitempty"`
	StartedAt      time.Time `json:"startedAt"`
	NextEventID    int64     `json:"nextEventID"`
	LastEventTime  time.Time `json:"lastEventTime"` // timestamps are clamped non-decreasing (SPEC §10)

	WorkflowTask *WorkflowTaskInfo       `json:"workflowTask,omitempty"`
	Activities   map[int64]*ActivityInfo `json:"activities,omitempty"`
	Timers       map[string]*TimerInfo   `json:"timers,omitempty"`
	Buffered     []BufferedEvent         `json:"buffered,omitempty"`
	Signals      []SignalOutcome         `json:"signals,omitempty"` // last SignalDedupLimit, oldest first
}

// Encode serializes the state for the store, which treats it as opaque bytes.
func (s *MutableState) Encode() ([]byte, error) { return json.Marshal(s) }

// DecodeState reverses Encode.
func DecodeState(b []byte) (*MutableState, error) {
	s := &MutableState{}
	if err := json.Unmarshal(b, s); err != nil {
		return nil, err
	}
	return s, nil
}
