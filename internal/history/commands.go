package history

import (
	"encoding/json"
	"fmt"
	"reflect"
	"time"
)

// CommandType names a decision returned by a workflow task (SPEC §2).
type CommandType string

const (
	CommandScheduleActivity CommandType = "ScheduleActivity"
	CommandStartTimer       CommandType = "StartTimer"
	CommandCancelTimer      CommandType = "CancelTimer"
	CommandRecordMarker     CommandType = "RecordMarker"
	CommandCompleteWorkflow CommandType = "CompleteWorkflow"
	CommandFailWorkflow     CommandType = "FailWorkflow"
)

// Command is one decision from workflow code.
type Command interface{ CommandType() CommandType }

type ScheduleActivity struct {
	ActivityID             string          `json:"activityID"`
	ActivityType           string          `json:"activityType"`
	Input                  json.RawMessage `json:"input,omitempty"`
	TaskQueue              string          `json:"taskQueue,omitempty"` // default: the workflow's queue
	StartToCloseTimeout    time.Duration   `json:"startToCloseTimeout"`
	ScheduleToCloseTimeout time.Duration   `json:"scheduleToCloseTimeout,omitempty"`
	RetryPolicy            RetryPolicy     `json:"retryPolicy"`
}

type StartTimer struct {
	TimerID  string        `json:"timerID"`
	Duration time.Duration `json:"duration"`
}

type CancelTimer struct {
	TimerID string `json:"timerID"`
}

type RecordMarker struct {
	MarkerName string          `json:"markerName"`
	Details    json.RawMessage `json:"details,omitempty"`
}

type CompleteWorkflow struct {
	Result json.RawMessage `json:"result,omitempty"`
}

type FailWorkflow struct {
	Failure Failure `json:"failure"`
}

func (ScheduleActivity) CommandType() CommandType { return CommandScheduleActivity }
func (StartTimer) CommandType() CommandType       { return CommandStartTimer }
func (CancelTimer) CommandType() CommandType      { return CommandCancelTimer }
func (RecordMarker) CommandType() CommandType     { return CommandRecordMarker }
func (CompleteWorkflow) CommandType() CommandType { return CommandCompleteWorkflow }
func (FailWorkflow) CommandType() CommandType     { return CommandFailWorkflow }

var commandFactories = map[CommandType]func() Command{
	CommandScheduleActivity: func() Command { return &ScheduleActivity{} },
	CommandStartTimer:       func() Command { return &StartTimer{} },
	CommandCancelTimer:      func() Command { return &CancelTimer{} },
	CommandRecordMarker:     func() Command { return &RecordMarker{} },
	CommandCompleteWorkflow: func() Command { return &CompleteWorkflow{} },
	CommandFailWorkflow:     func() Command { return &FailWorkflow{} },
}

// IsCloseCommand reports whether a command would close the run.
func IsCloseCommand(c Command) bool {
	t := c.CommandType()
	return t == CommandCompleteWorkflow || t == CommandFailWorkflow
}

type commandJSON struct {
	Type  CommandType     `json:"type"`
	Attrs json.RawMessage `json:"attrs"`
}

// EncodeCommands serializes commands with a type tag.
func EncodeCommands(cs []Command) ([]byte, error) {
	out := make([]commandJSON, 0, len(cs))
	for _, c := range cs {
		a, err := json.Marshal(c)
		if err != nil {
			return nil, err
		}
		out = append(out, commandJSON{Type: c.CommandType(), Attrs: a})
	}
	return json.Marshal(out)
}

// DecodeCommands reverses EncodeCommands. Commands are returned as values.
func DecodeCommands(b []byte) ([]Command, error) {
	var in []commandJSON
	if err := json.Unmarshal(b, &in); err != nil {
		return nil, err
	}
	out := make([]Command, 0, len(in))
	for _, j := range in {
		f, ok := commandFactories[j.Type]
		if !ok {
			return nil, fmt.Errorf("history: unknown command type %q", j.Type)
		}
		p := f()
		if err := json.Unmarshal(j.Attrs, p); err != nil {
			return nil, err
		}
		out = append(out, deref(p).(Command))
	}
	return out, nil
}

// deref turns *T into T so values compare and switch by value type.
func deref[T any](p T) T {
	v := reflect.ValueOf(p)
	if v.Kind() == reflect.Pointer {
		return v.Elem().Interface().(T)
	}
	return p
}
