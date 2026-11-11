package client_test

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"

	"github.com/niqbal/workflow-engine/client"
	"github.com/niqbal/workflow-engine/internal/history"
	"github.com/niqbal/workflow-engine/internal/httpapi"
	"github.com/niqbal/workflow-engine/internal/server/api"
)

// okService answers every call successfully. It keeps no state, so it is safe to share.
type okService struct{}

func (okService) StartWorkflow(context.Context, api.StartRequest) (api.StartResponse, error) {
	return api.StartResponse{RunID: "run-1"}, nil
}
func (okService) SignalWorkflow(context.Context, api.SignalRequest) error       { return nil }
func (okService) TerminateWorkflow(context.Context, api.TerminateRequest) error { return nil }
func (okService) DescribeWorkflow(_ context.Context, id string) (api.Description, error) {
	return api.Description{WorkflowID: id, RunID: "run-1", Status: history.StatusRunning}, nil
}
func (okService) GetHistory(context.Context, string) ([]history.Event, error) {
	return []history.Event{{ID: 1, Attrs: history.WorkflowExecutionStarted{WorkflowType: "W"}}}, nil
}
func (okService) PollWorkflowTask(context.Context, api.PollRequest) (*api.WorkflowTask, error) {
	return &api.WorkflowTask{TaskToken: "wt"}, nil
}
func (okService) CompleteWorkflowTask(context.Context, api.CompleteWorkflowTaskRequest) error {
	return nil
}
func (okService) FailWorkflowTask(context.Context, api.FailWorkflowTaskRequest) error {
	return nil
}
func (okService) PollActivityTask(context.Context, api.PollRequest) (*api.ActivityTask, error) {
	return &api.ActivityTask{TaskToken: "at"}, nil
}
func (okService) CompleteActivityTask(context.Context, api.CompleteActivityTaskRequest) error {
	return nil
}
func (okService) FailActivityTask(context.Context, api.FailActivityTaskRequest) error {
	return nil
}

// flaky fails the first `fail` requests in the given mode, then passes requests to next.
type flaky struct {
	next http.Handler
	fail int32
	mode string
	hits atomic.Int32
}

func (f *flaky) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if f.hits.Add(1) > f.fail {
		f.next.ServeHTTP(w, r)
		return
	}
	jsonErr := func(status int, code string) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(status)
		_ = json.NewEncoder(w).Encode(httpapi.ErrorBody{Code: code, Message: "injected"})
	}
	switch f.mode {
	case "unavailable":
		jsonErr(http.StatusServiceUnavailable, "unavailable")
	case "not_owner":
		jsonErr(http.StatusServiceUnavailable, "not_owner")
	case "not_found":
		jsonErr(http.StatusNotFound, "not_found")
	case "internal":
		jsonErr(http.StatusInternalServerError, "internal")
	case "proxy": // a load balancer's plain-text page
		http.Error(w, "bad gateway", http.StatusBadGateway)
	case "drop": // the request arrived; the reply is lost
		conn, _, err := w.(http.Hijacker).Hijack()
		if err != nil {
			panic(err)
		}
		_ = conn.Close()
	}
}

// sleeps records requested backoffs instead of waiting on the wall clock.
type sleeps struct {
	got []time.Duration
	err error
}

func (s *sleeps) sleep(_ context.Context, d time.Duration) error {
	s.got = append(s.got, d)
	return s.err
}

func newFlakyClient(t *testing.T, mode string, fail int32, maxAttempts int) (*client.Client, *flaky, *sleeps) {
	t.Helper()
	f := &flaky{next: httpapi.New(okService{}, httpapi.Options{}), fail: fail, mode: mode}
	srv := httptest.NewServer(f)
	t.Cleanup(srv.Close)
	s := &sleeps{}
	c := client.New(srv.URL+"/", client.Options{
		// No keep-alives: net/http would otherwise replay idempotent requests on a dead
		// reused connection by itself, hiding what the client decides.
		HTTPClient:  &http.Client{Transport: &http.Transport{DisableKeepAlives: true}},
		MaxAttempts: maxAttempts,
		Backoff:     func(attempt int) time.Duration { return time.Duration(attempt) * time.Second },
		Sleep:       s.sleep,
	})
	return c, f, s
}

type clientCall struct {
	name    string
	retried bool
	do      func(ctx context.Context, c *client.Client) error
}

var clientCalls = []clientCall{
	{"start with requestID", true, func(ctx context.Context, c *client.Client) error {
		r, err := c.StartWorkflow(ctx, api.StartRequest{WorkflowID: "wf", WorkflowType: "W", TaskQueue: "q", RequestID: "r1"})
		if err == nil && r.RunID != "run-1" {
			return errors.New("wrong runID " + r.RunID)
		}
		return err
	}},
	{"start without requestID", false, func(ctx context.Context, c *client.Client) error {
		_, err := c.StartWorkflow(ctx, api.StartRequest{WorkflowID: "wf", WorkflowType: "W", TaskQueue: "q"})
		return err
	}},
	{"execute with requestID", true, func(ctx context.Context, c *client.Client) error {
		_, err := c.ExecuteWorkflow(ctx, api.StartRequest{WorkflowID: "wf", WorkflowType: "W", TaskQueue: "q", RequestID: "r1"})
		return err
	}},
	{"signal with requestID", true, func(ctx context.Context, c *client.Client) error {
		return c.SignalWorkflow(ctx, api.SignalRequest{WorkflowID: "wf", SignalName: "s", RequestID: "s1"})
	}},
	{"signal without requestID", false, func(ctx context.Context, c *client.Client) error {
		return c.SignalWorkflow(ctx, api.SignalRequest{WorkflowID: "wf", SignalName: "s"})
	}},
	{"run signal with requestID", true, func(ctx context.Context, c *client.Client) error {
		return c.GetRun("wf", "run-1").Signal(ctx, "s", map[string]int{"n": 1}, "s1")
	}},
	{"run signal without requestID", false, func(ctx context.Context, c *client.Client) error {
		return c.GetRun("wf", "run-1").Signal(ctx, "s", nil, "")
	}},
	{"terminate", true, func(ctx context.Context, c *client.Client) error {
		return c.TerminateWorkflow(ctx, api.TerminateRequest{WorkflowID: "wf", Reason: "r"})
	}},
	{"describe", true, func(ctx context.Context, c *client.Client) error {
		d, err := c.DescribeWorkflow(ctx, "wf")
		if err == nil && d.WorkflowID != "wf" {
			return errors.New("wrong description")
		}
		return err
	}},
	{"history", true, func(ctx context.Context, c *client.Client) error {
		evs, err := c.GetHistory(ctx, "wf")
		if err == nil && len(evs) != 1 {
			return errors.New("wrong history")
		}
		return err
	}},
	{"poll workflow task", false, func(ctx context.Context, c *client.Client) error {
		_, err := c.PollWorkflowTask(ctx, api.PollRequest{TaskQueue: "q"})
		return err
	}},
	{"complete workflow task", false, func(ctx context.Context, c *client.Client) error {
		return c.CompleteWorkflowTask(ctx, api.CompleteWorkflowTaskRequest{TaskToken: "wt",
			Commands: []history.Command{history.CompleteWorkflow{}}})
	}},
	{"fail workflow task", false, func(ctx context.Context, c *client.Client) error {
		return c.FailWorkflowTask(ctx, api.FailWorkflowTaskRequest{TaskToken: "wt"})
	}},
	{"poll activity task", false, func(ctx context.Context, c *client.Client) error {
		_, err := c.PollActivityTask(ctx, api.PollRequest{TaskQueue: "q"})
		return err
	}},
	{"complete activity task", false, func(ctx context.Context, c *client.Client) error {
		return c.CompleteActivityTask(ctx, api.CompleteActivityTaskRequest{TaskToken: "at"})
	}},
	{"fail activity task", false, func(ctx context.Context, c *client.Client) error {
		return c.FailActivityTask(ctx, api.FailActivityTaskRequest{TaskToken: "at"})
	}},
}

// TestRetriesOnlyIdempotentOrRequestIDCalls: two transient failures, then success. Calls that
// are safe to repeat succeed on the third attempt after two backoffs; every other call reports
// the first failure and is sent exactly once.
func TestRetriesOnlyIdempotentOrRequestIDCalls(t *testing.T) {
	modes := map[string]error{"unavailable": api.ErrUnavailable, "not_owner": api.ErrNotOwner,
		"proxy": api.ErrUnavailable, "drop": nil}
	for _, mode := range []string{"unavailable", "not_owner", "proxy", "drop"} {
		for _, call := range clientCalls {
			t.Run(mode+"/"+call.name, func(t *testing.T) {
				c, f, s := newFlakyClient(t, mode, 2, 5)
				err := call.do(context.Background(), c)
				if call.retried {
					if err != nil {
						t.Fatalf("retried call failed: %v", err)
					}
					if h := f.hits.Load(); h != 3 {
						t.Fatalf("%d requests, want 3", h)
					}
					if len(s.got) != 2 || s.got[0] != time.Second || s.got[1] != 2*time.Second {
						t.Fatalf("backoffs %v, want [1s 2s]", s.got)
					}
					return
				}
				if err == nil {
					t.Fatal("non-retried call succeeded: it must have been retried")
				}
				if h := f.hits.Load(); h != 1 || len(s.got) != 0 {
					t.Fatalf("%d requests and backoffs %v, want 1 and none", h, s.got)
				}
				if want := modes[mode]; want != nil && !errors.Is(err, want) {
					t.Fatalf("error %v, want %v", err, want)
				}
			})
		}
	}
}

// Retried calls still stop at once on errors that a repeat cannot fix.
func TestPermanentErrorsAreNotRetried(t *testing.T) {
	for mode, want := range map[string]string{"not_found": "not_found", "internal": "internal"} {
		t.Run(mode, func(t *testing.T) {
			c, f, s := newFlakyClient(t, mode, 100, 5)
			_, err := c.DescribeWorkflow(context.Background(), "wf")
			if err == nil || api.Code(err) != want {
				t.Fatalf("error %v, want code %s", err, want)
			}
			var ce *client.Error
			if !errors.As(err, &ce) || ce.Code != want {
				t.Fatalf("error %#v, want *client.Error with code %s", err, want)
			}
			if h := f.hits.Load(); h != 1 || len(s.got) != 0 {
				t.Fatalf("%d requests and backoffs %v, want 1 and none", h, s.got)
			}
		})
	}
}

func TestRetriesStopAtMaxAttempts(t *testing.T) {
	c, f, s := newFlakyClient(t, "unavailable", 100, 3)
	err := c.TerminateWorkflow(context.Background(), api.TerminateRequest{WorkflowID: "wf"})
	if !errors.Is(err, api.ErrUnavailable) {
		t.Fatalf("error %v, want unavailable", err)
	}
	var ce *client.Error
	if !errors.As(err, &ce) || ce.StatusCode != http.StatusServiceUnavailable {
		t.Fatalf("error %#v", err)
	}
	if h := f.hits.Load(); h != 3 || len(s.got) != 2 {
		t.Fatalf("%d requests and backoffs %v, want 3 and 2", h, s.got)
	}
}

func TestRetriesStopWhenSleepIsCanceled(t *testing.T) {
	c, f, s := newFlakyClient(t, "unavailable", 100, 5)
	s.err = context.Canceled
	_, err := c.GetHistory(context.Background(), "wf")
	if !errors.Is(err, api.ErrUnavailable) {
		t.Fatalf("error %v, want the last call's error", err)
	}
	if h := f.hits.Load(); h != 1 || len(s.got) != 1 {
		t.Fatalf("%d requests and backoffs %v, want 1 and 1", h, s.got)
	}
}

func TestCanceledContextIsNotRetried(t *testing.T) {
	c, f, s := newFlakyClient(t, "unavailable", 0, 5)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := c.DescribeWorkflow(ctx, "wf"); !errors.Is(err, context.Canceled) {
		t.Fatalf("error %v, want context.Canceled", err)
	}
	if h := f.hits.Load(); h != 0 || len(s.got) != 0 {
		t.Fatalf("%d requests and backoffs %v, want none", h, s.got)
	}
}
