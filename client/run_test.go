package client_test

import (
	"context"
	"encoding/json"
	"errors"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"

	"github.com/niqbal/workflow-engine/client"
	"github.com/niqbal/workflow-engine/internal/history"
	"github.com/niqbal/workflow-engine/internal/httpapi"
	"github.com/niqbal/workflow-engine/internal/server/api"
)

// closingService reports the run open for the first `open` describes, then closed with close.
type closingService struct {
	okService
	open      int32
	describes atomic.Int32
	status    history.RunStatus
	close     history.Attributes
}

func (s *closingService) DescribeWorkflow(_ context.Context, id string) (api.Description, error) {
	d := api.Description{WorkflowID: id, RunID: "run-1", Status: history.StatusRunning}
	if s.describes.Add(1) > s.open {
		d.Status = s.status
	}
	return d, nil
}

func (s *closingService) GetHistory(context.Context, string) ([]history.Event, error) {
	return []history.Event{
		{ID: 1, Attrs: history.WorkflowExecutionStarted{WorkflowType: "W"}},
		{ID: 2, Attrs: s.close},
	}, nil
}

func newRunClient(t *testing.T, svc *closingService) (*client.Client, *sleeps) {
	t.Helper()
	srv := httptest.NewServer(httpapi.New(svc, httpapi.Options{}))
	t.Cleanup(srv.Close)
	s := &sleeps{}
	return client.New(srv.URL, client.Options{Sleep: s.sleep, PollInterval: 7 * time.Second}), s
}

func TestRunGetWaitsForCloseWithoutWallClock(t *testing.T) {
	svc := &closingService{open: 2, status: history.StatusCompleted,
		close: history.WorkflowExecutionCompleted{Result: json.RawMessage(`[1,2,3]`)}}
	c, s := newRunClient(t, svc)
	var out []int
	if err := c.GetRun("wf", "run-1").Get(context.Background(), &out); err != nil {
		t.Fatal(err)
	}
	if len(out) != 3 || out[2] != 3 {
		t.Fatalf("result %v", out)
	}
	if svc.describes.Load() != 3 || len(s.got) != 2 || s.got[0] != 7*time.Second || s.got[1] != 7*time.Second {
		t.Fatalf("%d describes, waits %v; want 3 and two 7s waits", svc.describes.Load(), s.got)
	}
}

func TestRunGetStopsWhenWaitIsCanceled(t *testing.T) {
	svc := &closingService{open: 100, status: history.StatusCompleted, close: history.WorkflowExecutionCompleted{}}
	c, s := newRunClient(t, svc)
	s.err = context.DeadlineExceeded
	if err := c.GetRun("wf", "").Get(context.Background(), nil); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("get: %v", err)
	}
}

func TestRunGetReportsTimeoutAndBadResult(t *testing.T) {
	svc := &closingService{status: history.StatusTimedOut, close: history.WorkflowExecutionTimedOut{}}
	c, _ := newRunClient(t, svc)
	err := c.GetRun("wf", "run-1").Get(context.Background(), nil)
	var we *client.WorkflowError
	if !errors.As(err, &we) || we.Status != history.StatusTimedOut || we.WorkflowID != "wf" || we.RunID != "run-1" {
		t.Fatalf("get: %v", err)
	}

	svc = &closingService{status: history.StatusCompleted, close: history.WorkflowExecutionCompleted{Result: json.RawMessage(`"text"`)}}
	c, _ = newRunClient(t, svc)
	var n int
	if err := c.GetRun("wf", "run-1").Get(context.Background(), &n); err == nil || errors.As(err, &we) {
		t.Fatalf("decoding a string into an int: %v", err)
	}
}
