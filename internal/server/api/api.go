// Package api is the server's service contract: the operations behind the HTTP endpoints in
// SPEC §13.1. The HTTP layer, the Go client and the scenario suite all speak these types.
package api

import (
	"context"
	"encoding/json"
	"errors"
	"time"

	"github.com/niqbal/workflow-engine/internal/history"
)

// Error codes (SPEC §13.1). Service methods return these sentinels (possibly wrapped).
var (
	ErrNotFound        = errors.New("not_found")
	ErrAlreadyStarted  = errors.New("already_started")
	ErrStaleTask       = errors.New("stale_task")
	ErrWorkflowClosed  = errors.New("workflow_closed")
	ErrInvalidArgument = errors.New("invalid_argument")
	ErrNotOwner        = errors.New("not_owner")
	ErrUnavailable     = errors.New("unavailable")
)

// Code returns the wire code for an error, or "internal".
func Code(err error) string {
	for _, e := range []error{ErrNotFound, ErrAlreadyStarted, ErrStaleTask, ErrWorkflowClosed,
		ErrInvalidArgument, ErrNotOwner, ErrUnavailable} {
		if errors.Is(err, e) {
			return e.Error()
		}
	}
	return "internal"
}

type StartRequest struct {
	WorkflowID       string          `json:"workflowID"`
	WorkflowType     string          `json:"workflowType"`
	TaskQueue        string          `json:"taskQueue"`
	Input            json.RawMessage `json:"input,omitempty"`
	RequestID        string          `json:"requestID"`
	ExecutionTimeout time.Duration   `json:"executionTimeout,omitempty"`
}

type StartResponse struct {
	RunID string `json:"runID"`
}

type SignalRequest struct {
	WorkflowID string          `json:"workflowID"`
	SignalName string          `json:"signalName"`
	Payload    json.RawMessage `json:"payload,omitempty"`
	RequestID  string          `json:"requestID"`
}

type TerminateRequest struct {
	WorkflowID string `json:"workflowID"`
	Reason     string `json:"reason,omitempty"`
}

type PendingActivity struct {
	ScheduledEventID int64             `json:"scheduledEventID"`
	ActivityID       string            `json:"activityID"`
	ActivityType     string            `json:"activityType"`
	Attempt          int               `json:"attempt"`
	State            history.TaskState `json:"state"`
}

type PendingTimer struct {
	TimerID string    `json:"timerID"`
	FireAt  time.Time `json:"fireAt"`
}

type Description struct {
	WorkflowID        string            `json:"workflowID"`
	RunID             string            `json:"runID"`
	WorkflowType      string            `json:"workflowType"`
	Status            history.RunStatus `json:"status"`
	PendingActivities []PendingActivity `json:"pendingActivities,omitempty"`
	PendingTimers     []PendingTimer    `json:"pendingTimers,omitempty"`
}

type PollRequest struct {
	TaskQueue string `json:"taskQueue"`
	Identity  string `json:"identity"`
}

// WorkflowTask carries the full history up to and including its WorkflowTaskStarted (D5).
type WorkflowTask struct {
	TaskToken    string          `json:"taskToken"`
	WorkflowID   string          `json:"workflowID"`
	RunID        string          `json:"runID"`
	WorkflowType string          `json:"workflowType"`
	Attempt      int             `json:"attempt"`
	History      []history.Event `json:"history"`
}

type CompleteWorkflowTaskRequest struct {
	TaskToken string            `json:"taskToken"`
	Commands  []history.Command `json:"-"` // encoded with history.EncodeCommands on the wire
	Identity  string            `json:"identity"`
}

type FailWorkflowTaskRequest struct {
	TaskToken string          `json:"taskToken"`
	Cause     history.Failure `json:"cause"`
	Identity  string          `json:"identity"`
}

// ActivityTask is one attempt of an activity.
type ActivityTask struct {
	TaskToken        string          `json:"taskToken"`
	WorkflowID       string          `json:"workflowID"`
	RunID            string          `json:"runID"`
	ScheduledEventID int64           `json:"scheduledEventID"`
	ActivityID       string          `json:"activityID"`
	ActivityType     string          `json:"activityType"`
	Input            json.RawMessage `json:"input,omitempty"`
	Attempt          int             `json:"attempt"`
	// IdempotencyKey is workflowID/runID/scheduledEventID: stable across attempts (SPEC §3.2).
	IdempotencyKey string    `json:"idempotencyKey"`
	Deadline       time.Time `json:"deadline"` // start-to-close deadline of this attempt
}

type CompleteActivityTaskRequest struct {
	TaskToken string          `json:"taskToken"`
	Result    json.RawMessage `json:"result,omitempty"`
	Identity  string          `json:"identity"`
}

type FailActivityTaskRequest struct {
	TaskToken string          `json:"taskToken"`
	Failure   history.Failure `json:"failure"`
	Identity  string          `json:"identity"`
}

// Service is implemented by the frontend. Poll methods return (nil, nil) when no task
// arrives before ctx is done.
type Service interface {
	StartWorkflow(ctx context.Context, r StartRequest) (StartResponse, error)
	SignalWorkflow(ctx context.Context, r SignalRequest) error
	TerminateWorkflow(ctx context.Context, r TerminateRequest) error
	DescribeWorkflow(ctx context.Context, workflowID string) (Description, error)
	GetHistory(ctx context.Context, workflowID string) ([]history.Event, error)

	PollWorkflowTask(ctx context.Context, r PollRequest) (*WorkflowTask, error)
	CompleteWorkflowTask(ctx context.Context, r CompleteWorkflowTaskRequest) error
	FailWorkflowTask(ctx context.Context, r FailWorkflowTaskRequest) error

	PollActivityTask(ctx context.Context, r PollRequest) (*ActivityTask, error)
	CompleteActivityTask(ctx context.Context, r CompleteActivityTaskRequest) error
	FailActivityTask(ctx context.Context, r FailActivityTaskRequest) error
}
