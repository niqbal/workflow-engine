package history

import (
	"encoding/json"
	"reflect"
	"testing"
	"time"
)

func sample(t EventType) Attributes {
	rp := RetryPolicy{InitialInterval: time.Second, MaximumAttempts: 3, NonRetryableErrors: []string{"Bad"}}
	f := Failure{Type: "Boom", Message: "m", NonRetryable: true}
	raw := json.RawMessage(`{"x":1}`)
	at := time.Date(2026, 11, 11, 12, 0, 0, 0, time.UTC)
	return map[EventType]Attributes{
		EventWorkflowExecutionStarted:    WorkflowExecutionStarted{WorkflowType: "W", Input: raw, TaskQueue: "q", ExecutionTimeout: time.Hour, RequestID: "r"},
		EventWorkflowTaskScheduled:       WorkflowTaskScheduled{TaskQueue: "q", Attempt: 1},
		EventWorkflowTaskStarted:         WorkflowTaskStarted{ScheduledEventID: 2, WorkerIdentity: "w1"},
		EventWorkflowTaskCompleted:       WorkflowTaskCompleted{ScheduledEventID: 2, StartedEventID: 3},
		EventWorkflowTaskTimedOut:        WorkflowTaskTimedOut{ScheduledEventID: 2, StartedEventID: 3},
		EventWorkflowTaskFailed:          WorkflowTaskFailed{ScheduledEventID: 2, StartedEventID: 3, Cause: f},
		EventActivityTaskScheduled:       ActivityTaskScheduled{ActivityID: "a", ActivityType: "A", Input: raw, TaskQueue: "q", StartToCloseTimeout: time.Minute, RetryPolicy: rp},
		EventActivityTaskStarted:         ActivityTaskStarted{ScheduledEventID: 5, Attempt: 2, WorkerIdentity: "w"},
		EventActivityTaskCompleted:       ActivityTaskCompleted{ScheduledEventID: 5, StartedEventID: 6, Result: raw},
		EventActivityTaskFailed:          ActivityTaskFailed{ScheduledEventID: 5, StartedEventID: 6, Failure: f, Attempt: 3},
		EventActivityTaskTimedOut:        ActivityTaskTimedOut{ScheduledEventID: 5, TimeoutType: TimeoutStartToClose, Attempt: 1},
		EventTimerStarted:                TimerStarted{TimerID: "t", FireAt: at},
		EventTimerFired:                  TimerFired{TimerID: "t", StartedEventID: 7},
		EventTimerCanceled:               TimerCanceled{TimerID: "t", StartedEventID: 7},
		EventWorkflowExecutionSignaled:   WorkflowExecutionSignaled{SignalName: "s", Payload: raw, RequestID: "r2"},
		EventMarkerRecorded:              MarkerRecorded{MarkerName: MarkerVersion, Details: raw},
		EventWorkflowExecutionCompleted:  WorkflowExecutionCompleted{Result: raw},
		EventWorkflowExecutionFailed:     WorkflowExecutionFailed{Failure: f},
		EventWorkflowExecutionTimedOut:   WorkflowExecutionTimedOut{},
		EventWorkflowExecutionTerminated: WorkflowExecutionTerminated{Reason: "why"},
	}[t]
}

func TestEventJSONRoundTripEveryType(t *testing.T) {
	if len(AllEventTypes()) != len(eventFactories) {
		t.Fatalf("AllEventTypes and factories disagree")
	}
	for i, typ := range AllEventTypes() {
		a := sample(typ)
		if a == nil {
			t.Fatalf("no sample for %s", typ)
		}
		in := Event{ID: int64(i + 1), Time: time.Date(2026, 1, 1, 0, 0, i, 0, time.UTC), Attrs: a}
		b, err := json.Marshal(in)
		if err != nil {
			t.Fatalf("%s: %v", typ, err)
		}
		var out Event
		if err := json.Unmarshal(b, &out); err != nil {
			t.Fatalf("%s: %v", typ, err)
		}
		if !reflect.DeepEqual(in, out) || out.Type() != typ {
			t.Fatalf("%s: round trip mismatch\n in=%#v\nout=%#v", typ, in, out)
		}
	}
}

func TestCommandsRoundTrip(t *testing.T) {
	cs := []Command{
		ScheduleActivity{ActivityID: "1", ActivityType: "A", StartToCloseTimeout: time.Second},
		StartTimer{TimerID: "t", Duration: time.Hour}, CancelTimer{TimerID: "t"},
		RecordMarker{MarkerName: MarkerSideEffect, Details: json.RawMessage(`1`)},
		CompleteWorkflow{Result: json.RawMessage(`"ok"`)}, FailWorkflow{Failure: Failure{Type: "X"}},
	}
	b, err := EncodeCommands(cs)
	if err != nil {
		t.Fatal(err)
	}
	out, err := DecodeCommands(b)
	if err != nil || !reflect.DeepEqual(cs, out) {
		t.Fatalf("mismatch: %v\n%#v", err, out)
	}
}

func TestBackoff(t *testing.T) {
	p := RetryPolicy{InitialInterval: time.Second, BackoffCoefficient: 2, MaximumInterval: 5 * time.Second}
	want := map[int]time.Duration{2: time.Second, 3: 2 * time.Second, 4: 4 * time.Second, 5: 5 * time.Second, 9: 5 * time.Second}
	for n, d := range want {
		if got := p.Backoff(n); got != d {
			t.Fatalf("attempt %d: got %v want %v", n, got, d)
		}
	}
}
