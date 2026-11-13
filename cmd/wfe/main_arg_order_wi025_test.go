package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/niqbal/workflow-engine/internal/history"
	"github.com/niqbal/workflow-engine/internal/httpapi"
	"github.com/niqbal/workflow-engine/internal/server/api"
)

type wi025ArgOrderService struct {
	startReq      api.StartRequest
	signalReq     api.SignalRequest
	historyID     string
	historyEvents []history.Event
}

func (s *wi025ArgOrderService) StartWorkflow(context.Context, api.StartRequest) (api.StartResponse, error) {
	s.startReq = api.StartRequest{}
	return api.StartResponse{RunID: "run-1"}, nil
}

func (s *wi025ArgOrderService) SignalWorkflow(_ context.Context, req api.SignalRequest) error {
	s.signalReq = req
	return nil
}

func (s *wi025ArgOrderService) TerminateWorkflow(context.Context, api.TerminateRequest) error {
	return fmt.Errorf("not implemented")
}

func (s *wi025ArgOrderService) DescribeWorkflow(context.Context, string) (api.Description, error) {
	return api.Description{}, fmt.Errorf("not implemented")
}

func (s *wi025ArgOrderService) GetHistory(_ context.Context, workflowID string) ([]history.Event, error) {
	s.historyID = workflowID
	return s.historyEvents, nil
}

func (s *wi025ArgOrderService) PollWorkflowTask(context.Context, api.PollRequest) (*api.WorkflowTask, error) {
	return nil, fmt.Errorf("not implemented")
}

func (s *wi025ArgOrderService) CompleteWorkflowTask(context.Context, api.CompleteWorkflowTaskRequest) error {
	return fmt.Errorf("not implemented")
}

func (s *wi025ArgOrderService) FailWorkflowTask(context.Context, api.FailWorkflowTaskRequest) error {
	return fmt.Errorf("not implemented")
}

func (s *wi025ArgOrderService) PollActivityTask(context.Context, api.PollRequest) (*api.ActivityTask, error) {
	return nil, fmt.Errorf("not implemented")
}

func (s *wi025ArgOrderService) CompleteActivityTask(context.Context, api.CompleteActivityTaskRequest) error {
	return fmt.Errorf("not implemented")
}

func (s *wi025ArgOrderService) FailActivityTask(context.Context, api.FailActivityTaskRequest) error {
	return fmt.Errorf("not implemented")
}

func TestWI025DocumentedArgumentOrderStartSignalHistory(t *testing.T) {
	now := time.Date(2026, 11, 13, 1, 2, 3, 0, time.UTC)
	svc := &wi025ArgOrderService{historyEvents: []history.Event{{
		ID:   1,
		Time: now,
		Attrs: history.WorkflowExecutionStarted{
			WorkflowType: "X",
			TaskQueue:    "q",
			Input:        json.RawMessage(`{"k":1}`),
		},
	}}}
	srv := httptest.NewServer(httpapi.New(svc, httpapi.Options{}))
	defer srv.Close()

	t.Run("start id before flags", func(t *testing.T) {
		var out bytes.Buffer
		var errOut bytes.Buffer
		err := run([]string{"workflow", "start", "wf-1", "--workflow-type", "X", "--task-queue", "q", "--input", `{"k":1}`, "--request-id", "req-1", "--addr", srv.URL}, &out, &errOut)
		if err != nil {
			t.Fatalf("run workflow start: %v (stderr=%q)", err, errOut.String())
		}
		if strings.TrimSpace(out.String()) != "run-1" {
			t.Fatalf("expected run id output, got %q", out.String())
		}
	})

	t.Run("signal id and name before flags", func(t *testing.T) {
		var out bytes.Buffer
		var errOut bytes.Buffer
		err := run([]string{"workflow", "signal", "wf-1", "approval", "--payload", `{"approved":true}`, "--request-id", "r1", "--addr", srv.URL}, &out, &errOut)
		if err != nil {
			t.Fatalf("run workflow signal: %v (stderr=%q)", err, errOut.String())
		}
		if svc.signalReq.WorkflowID != "wf-1" || svc.signalReq.SignalName != "approval" {
			t.Fatalf("unexpected signal request: %+v", svc.signalReq)
		}
		if string(svc.signalReq.Payload) != `{"approved":true}` || svc.signalReq.RequestID != "r1" {
			t.Fatalf("unexpected signal payload/request-id: %+v", svc.signalReq)
		}
	})

	t.Run("history id before flags", func(t *testing.T) {
		var out bytes.Buffer
		var errOut bytes.Buffer
		err := run([]string{"workflow", "history", "wf-1", "--addr", srv.URL}, &out, &errOut)
		if err != nil {
			t.Fatalf("run workflow history: %v (stderr=%q)", err, errOut.String())
		}
		if svc.historyID != "wf-1" {
			t.Fatalf("expected workflow id wf-1, got %q", svc.historyID)
		}
		if !strings.Contains(out.String(), "WorkflowExecutionStarted") {
			t.Fatalf("expected history output to include event type, got:\n%s", out.String())
		}
	})
}
