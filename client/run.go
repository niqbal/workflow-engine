package client

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/niqbal/workflow-engine/internal/history"
	"github.com/niqbal/workflow-engine/internal/server/api"
)

// Run is a handle on one workflow run.
type Run struct {
	c          *Client
	WorkflowID string
	RunID      string // empty: whichever run is latest
}

// ExecuteWorkflow starts a run and returns its handle. Set r.RequestID so that a retry after a
// lost reply returns the same run instead of failing or being skipped.
func (c *Client) ExecuteWorkflow(ctx context.Context, r api.StartRequest) (*Run, error) {
	resp, err := c.StartWorkflow(ctx, r)
	if err != nil {
		return nil, err
	}
	return &Run{c: c, WorkflowID: r.WorkflowID, RunID: resp.RunID}, nil
}

// GetRun returns a handle on an existing run. An empty runID means the latest run.
func (c *Client) GetRun(workflowID, runID string) *Run {
	return &Run{c: c, WorkflowID: workflowID, RunID: runID}
}

// WorkflowError reports a run that closed without completing.
type WorkflowError struct {
	WorkflowID string
	RunID      string
	Status     history.RunStatus
	Failure    *history.Failure // set when Status is failed
	Reason     string           // set when Status is terminated
}

func (e *WorkflowError) Error() string {
	msg := fmt.Sprintf("workflow %s (run %s) %s", e.WorkflowID, e.RunID, e.Status)
	switch {
	case e.Failure != nil:
		msg += fmt.Sprintf(": %s: %s", e.Failure.Type, e.Failure.Message)
	case e.Reason != "":
		msg += ": " + e.Reason
	}
	return msg
}

// Describe describes the run.
func (r *Run) Describe(ctx context.Context) (api.Description, error) {
	d, err := r.c.DescribeWorkflow(ctx, r.WorkflowID)
	if err != nil {
		return d, err
	}
	return d, r.check(d.RunID)
}

// History returns the run's history.
func (r *Run) History(ctx context.Context) ([]history.Event, error) {
	return r.c.GetHistory(ctx, r.WorkflowID)
}

// Signal sends a signal. payload is JSON-encoded unless it is already a json.RawMessage. Give a
// requestID to make the call safe to retry; it is retried only then.
func (r *Run) Signal(ctx context.Context, name string, payload any, requestID string) error {
	p, err := encode(payload)
	if err != nil {
		return err
	}
	return r.c.SignalWorkflow(ctx, api.SignalRequest{WorkflowID: r.WorkflowID, SignalName: name, Payload: p, RequestID: requestID})
}

// Terminate terminates the run. Terminating a closed run is a no-op.
func (r *Run) Terminate(ctx context.Context, reason string) error {
	return r.c.TerminateWorkflow(ctx, api.TerminateRequest{WorkflowID: r.WorkflowID, Reason: reason})
}

// Get waits until the run closes. If it completed, its result is decoded into valuePtr (which
// may be nil); otherwise Get returns a *WorkflowError.
func (r *Run) Get(ctx context.Context, valuePtr any) error {
	for {
		d, err := r.Describe(ctx)
		if err != nil {
			return err
		}
		if !d.Status.Open() {
			return r.result(ctx, d, valuePtr)
		}
		if err := r.c.opts.Sleep(ctx, r.c.opts.PollInterval); err != nil {
			return err
		}
	}
}

func (r *Run) result(ctx context.Context, d api.Description, valuePtr any) error {
	evs, err := r.History(ctx)
	if err != nil {
		return err
	}
	for i := len(evs) - 1; i >= 0; i-- {
		if !history.IsClose(evs[i].Type()) {
			continue
		}
		we := &WorkflowError{WorkflowID: d.WorkflowID, RunID: d.RunID, Status: d.Status}
		switch a := evs[i].Attrs.(type) {
		case history.WorkflowExecutionCompleted:
			if valuePtr == nil || len(a.Result) == 0 {
				return nil
			}
			if err := json.Unmarshal(a.Result, valuePtr); err != nil {
				return fmt.Errorf("client: decode result of workflow %s: %w", r.WorkflowID, err)
			}
			return nil
		case history.WorkflowExecutionFailed:
			f := a.Failure
			we.Failure = &f
		case history.WorkflowExecutionTerminated:
			we.Reason = a.Reason
		}
		return we
	}
	return fmt.Errorf("client: workflow %s is %s but its history has no close event", r.WorkflowID, d.Status)
}

// check fails if the latest run is not the one this handle names.
func (r *Run) check(latest string) error {
	if r.RunID != "" && latest != r.RunID {
		return fmt.Errorf("client: run %s of workflow %s is no longer the latest (latest is %s)", r.RunID, r.WorkflowID, latest)
	}
	return nil
}

func encode(v any) (json.RawMessage, error) {
	switch p := v.(type) {
	case nil:
		return nil, nil
	case json.RawMessage:
		return p, nil
	}
	b, err := json.Marshal(v)
	if err != nil {
		return nil, fmt.Errorf("client: encode payload: %w", err)
	}
	return b, nil
}
