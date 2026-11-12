package client

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"time"

	"github.com/niqbal/workflow-engine/internal/history"
	"github.com/niqbal/workflow-engine/internal/httpapi"
	"github.com/niqbal/workflow-engine/internal/server/api"
)

// Options configures a Client. Zero fields take defaults.
type Options struct {
	// HTTPClient sends requests. Its Timeout must exceed the server's long poll (30 s), or be 0.
	HTTPClient *http.Client
	// MaxAttempts bounds attempts of a retried call, the first included. Default 5.
	MaxAttempts int
	// Backoff returns the wait before attempt n+1 after attempt n failed.
	// Default 100 ms doubling per attempt, at most 5 s.
	Backoff func(attempt int) time.Duration
	// Sleep waits d or until ctx is done. Default: a timer. Tests inject a fake.
	Sleep func(ctx context.Context, d time.Duration) error
	// PollInterval is how often Run.Get re-checks an open run. Default 1 s.
	PollInterval time.Duration
}

// Client talks to one server's HTTP API.
type Client struct {
	base string
	opts Options
}

var _ api.Service = (*Client)(nil)

// New returns a client for the server at baseURL, for example "http://localhost:7233".
func New(baseURL string, opts Options) *Client {
	if opts.HTTPClient == nil {
		opts.HTTPClient = &http.Client{}
	}
	if opts.MaxAttempts <= 0 {
		opts.MaxAttempts = 5
	}
	if opts.Backoff == nil {
		opts.Backoff = defaultBackoff
	}
	if opts.Sleep == nil {
		opts.Sleep = sleep
	}
	if opts.PollInterval <= 0 {
		opts.PollInterval = time.Second
	}
	return &Client{base: strings.TrimRight(baseURL, "/"), opts: opts}
}

func defaultBackoff(attempt int) time.Duration {
	d := 100 * time.Millisecond
	for i := 1; i < attempt && d < 5*time.Second; i++ {
		d *= 2
	}
	return min(d, 5*time.Second)
}

func sleep(ctx context.Context, d time.Duration) error {
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-t.C:
		return nil
	}
}

// Error is an error reply from the server. errors.Is matches it against the api sentinels
// (api.ErrNotFound and so on), and api.Code returns its code.
type Error struct {
	StatusCode int
	Code       string
	Message    string
}

func (e *Error) Error() string {
	return fmt.Sprintf("%s (HTTP %d): %s", e.Code, e.StatusCode, e.Message)
}

func (e *Error) Unwrap() error {
	for _, s := range []error{api.ErrNotFound, api.ErrAlreadyStarted, api.ErrStaleTask, api.ErrWorkflowClosed,
		api.ErrInvalidArgument, api.ErrNotOwner, api.ErrUnavailable} {
		if s.Error() == e.Code {
			return s
		}
	}
	return nil
}

// transportError means the request or its reply was lost: the server may or may not have acted.
type transportError struct{ err error }

func (e *transportError) Error() string { return e.err.Error() }
func (e *transportError) Unwrap() error { return e.err }

func retriable(err error) bool {
	var te *transportError
	if errors.As(err, &te) {
		return true
	}
	var se *Error
	if errors.As(err, &se) {
		return se.Code == api.ErrUnavailable.Error() || se.Code == api.ErrNotOwner.Error()
	}
	return false
}

// call sends one request, retrying it only if retry is set. out may be nil. It reports whether
// the server returned a body (false on 204 No Content).
func (c *Client) call(ctx context.Context, method, path string, in any, retry bool, out any) (bool, error) {
	var body []byte
	if in != nil {
		b, err := json.Marshal(in)
		if err != nil {
			return false, fmt.Errorf("client: encode %s %s: %w", method, path, err)
		}
		body = b
	}
	for attempt := 1; ; attempt++ {
		got, err := c.once(ctx, method, path, body, out)
		if err == nil || !retry || !retriable(err) || attempt >= c.opts.MaxAttempts || ctx.Err() != nil {
			return got, err
		}
		if serr := c.opts.Sleep(ctx, c.opts.Backoff(attempt)); serr != nil {
			return false, err
		}
	}
}

func (c *Client) once(ctx context.Context, method, path string, body []byte, out any) (bool, error) {
	var rd io.Reader = http.NoBody
	if body != nil {
		rd = bytes.NewReader(body)
	}
	req, err := http.NewRequestWithContext(ctx, method, c.base+path, rd)
	if err != nil {
		return false, fmt.Errorf("client: %s %s: %w", method, path, err)
	}
	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	resp, err := c.opts.HTTPClient.Do(req)
	if err != nil {
		return false, &transportError{fmt.Errorf("client: %s %s: %w", method, path, err)}
	}
	defer resp.Body.Close()
	data, err := io.ReadAll(resp.Body)
	if err != nil {
		return false, &transportError{fmt.Errorf("client: %s %s: read reply: %w", method, path, err)}
	}
	if resp.StatusCode >= 300 {
		return false, replyError(resp.StatusCode, data)
	}
	if resp.StatusCode == http.StatusNoContent {
		return false, nil
	}
	if out != nil {
		if err := json.Unmarshal(data, out); err != nil {
			return false, fmt.Errorf("client: %s %s: decode reply: %w", method, path, err)
		}
	}
	return true, nil
}

// replyError decodes an error reply. A body that is not ours (a proxy page) gets a code from
// the status alone.
func replyError(status int, data []byte) error {
	var b httpapi.ErrorBody
	if err := json.Unmarshal(data, &b); err != nil || b.Code == "" {
		b = httpapi.ErrorBody{Code: "internal", Message: strings.TrimSpace(string(data))}
		switch status {
		case http.StatusBadGateway, http.StatusServiceUnavailable, http.StatusGatewayTimeout:
			b.Code = api.ErrUnavailable.Error()
		}
	}
	return &Error{StatusCode: status, Code: b.Code, Message: b.Message}
}

func workflowPath(id, suffix string) string {
	return "/v1/workflows/" + url.PathEscape(id) + suffix
}

// StartWorkflow starts a run. It is retried only when r.RequestID is set.
func (c *Client) StartWorkflow(ctx context.Context, r api.StartRequest) (api.StartResponse, error) {
	var resp api.StartResponse
	_, err := c.call(ctx, http.MethodPost, "/v1/workflows", r, r.RequestID != "", &resp)
	return resp, err
}

// SignalWorkflow signals the latest run. It is retried only when r.RequestID is set.
func (c *Client) SignalWorkflow(ctx context.Context, r api.SignalRequest) error {
	_, err := c.call(ctx, http.MethodPost, workflowPath(r.WorkflowID, "/signal"), r, r.RequestID != "", nil)
	return err
}

// TerminateWorkflow terminates the latest run. It is idempotent, so it is retried.
func (c *Client) TerminateWorkflow(ctx context.Context, r api.TerminateRequest) error {
	_, err := c.call(ctx, http.MethodPost, workflowPath(r.WorkflowID, "/terminate"), r, true, nil)
	return err
}

// DescribeWorkflow describes the latest run. It is retried.
func (c *Client) DescribeWorkflow(ctx context.Context, workflowID string) (api.Description, error) {
	var d api.Description
	_, err := c.call(ctx, http.MethodGet, workflowPath(workflowID, ""), nil, true, &d)
	return d, err
}

// GetHistory returns the latest run's history. It is retried.
func (c *Client) GetHistory(ctx context.Context, workflowID string) ([]history.Event, error) {
	var h httpapi.HistoryResponse
	_, err := c.call(ctx, http.MethodGet, workflowPath(workflowID, "/history"), nil, true, &h)
	return h.Events, err
}

// pollEnded reports whether a poll failed only because ctx is done. api.Service says such a
// poll returns (nil, nil): no task arrived before ctx was done. An error reply the server sent
// is still reported.
func pollEnded(ctx context.Context, err error) bool {
	var te *transportError
	return err != nil && ctx.Err() != nil && errors.As(err, &te)
}

// PollWorkflowTask long-polls for a workflow task; (nil, nil) means none arrived before the
// server's long poll ended or ctx was done. Not retried.
func (c *Client) PollWorkflowTask(ctx context.Context, r api.PollRequest) (*api.WorkflowTask, error) {
	var t api.WorkflowTask
	got, err := c.call(ctx, http.MethodPost, "/v1/tasks/workflow/poll", r, false, &t)
	if pollEnded(ctx, err) {
		return nil, nil
	}
	if err != nil || !got {
		return nil, err
	}
	return &t, nil
}

// CompleteWorkflowTask returns the task's commands. Not retried.
func (c *Client) CompleteWorkflowTask(ctx context.Context, r api.CompleteWorkflowTaskRequest) error {
	cs, err := history.EncodeCommands(r.Commands)
	if err != nil {
		return fmt.Errorf("client: encode commands: %w", err)
	}
	body := httpapi.CompleteWorkflowTaskBody{TaskToken: r.TaskToken, Commands: cs, Identity: r.Identity}
	_, err = c.call(ctx, http.MethodPost, "/v1/tasks/workflow/complete", body, false, nil)
	return err
}

// FailWorkflowTask reports a failed workflow task. Not retried.
func (c *Client) FailWorkflowTask(ctx context.Context, r api.FailWorkflowTaskRequest) error {
	_, err := c.call(ctx, http.MethodPost, "/v1/tasks/workflow/fail", r, false, nil)
	return err
}

// PollActivityTask long-polls for an activity task; (nil, nil) means none arrived before the
// server's long poll ended or ctx was done. Not retried.
func (c *Client) PollActivityTask(ctx context.Context, r api.PollRequest) (*api.ActivityTask, error) {
	var t api.ActivityTask
	got, err := c.call(ctx, http.MethodPost, "/v1/tasks/activity/poll", r, false, &t)
	if pollEnded(ctx, err) {
		return nil, nil
	}
	if err != nil || !got {
		return nil, err
	}
	return &t, nil
}

// CompleteActivityTask reports an activity result. Not retried.
func (c *Client) CompleteActivityTask(ctx context.Context, r api.CompleteActivityTaskRequest) error {
	_, err := c.call(ctx, http.MethodPost, "/v1/tasks/activity/complete", r, false, nil)
	return err
}

// FailActivityTask reports an activity failure. Not retried.
func (c *Client) FailActivityTask(ctx context.Context, r api.FailActivityTaskRequest) error {
	_, err := c.call(ctx, http.MethodPost, "/v1/tasks/activity/fail", r, false, nil)
	return err
}
