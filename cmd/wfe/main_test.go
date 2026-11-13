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

type cliService struct {
	history []history.Event
}

func (s *cliService) StartWorkflow(context.Context, api.StartRequest) (api.StartResponse, error) {
	return api.StartResponse{}, fmt.Errorf("not implemented")
}

func (s *cliService) SignalWorkflow(context.Context, api.SignalRequest) error {
	return fmt.Errorf("not implemented")
}

func (s *cliService) TerminateWorkflow(context.Context, api.TerminateRequest) error {
	return fmt.Errorf("not implemented")
}

func (s *cliService) DescribeWorkflow(context.Context, string) (api.Description, error) {
	return api.Description{}, fmt.Errorf("not implemented")
}

func (s *cliService) GetHistory(context.Context, string) ([]history.Event, error) {
	return s.history, nil
}

func (s *cliService) PollWorkflowTask(context.Context, api.PollRequest) (*api.WorkflowTask, error) {
	return nil, fmt.Errorf("not implemented")
}

func (s *cliService) CompleteWorkflowTask(context.Context, api.CompleteWorkflowTaskRequest) error {
	return fmt.Errorf("not implemented")
}

func (s *cliService) FailWorkflowTask(context.Context, api.FailWorkflowTaskRequest) error {
	return fmt.Errorf("not implemented")
}

func (s *cliService) PollActivityTask(context.Context, api.PollRequest) (*api.ActivityTask, error) {
	return nil, fmt.Errorf("not implemented")
}

func (s *cliService) CompleteActivityTask(context.Context, api.CompleteActivityTaskRequest) error {
	return fmt.Errorf("not implemented")
}

func (s *cliService) FailActivityTask(context.Context, api.FailActivityTaskRequest) error {
	return fmt.Errorf("not implemented")
}

func TestWorkflowHistoryPrintsReadableEvents(t *testing.T) {
	now := time.Date(2026, 11, 13, 1, 2, 3, 456000000, time.UTC)
	svc := &cliService{history: []history.Event{
		{ID: 1, Time: now, Attrs: history.WorkflowExecutionStarted{
			WorkflowType: "Approval",
			TaskQueue:    "q1",
			Input:        json.RawMessage(`{"amount":5}`),
		}},
		{ID: 2, Time: now.Add(time.Second), Attrs: history.WorkflowExecutionSignaled{
			SignalName: "approval",
			Payload:    json.RawMessage(`{"approved":true}`),
		}},
	}}
	h := httpapi.New(svc, httpapi.Options{})
	srv := httptest.NewServer(h)
	defer srv.Close()

	var out bytes.Buffer
	var errOut bytes.Buffer
	err := run([]string{"workflow", "history", "--addr", srv.URL, "wf-1"}, &out, &errOut)
	if err != nil {
		t.Fatalf("run workflow history: %v (stderr=%q)", err, errOut.String())
	}
	got := out.String()
	for _, want := range []string{
		"ID", "Time", "Type", "Attrs",
		"WorkflowExecutionStarted",
		"WorkflowExecutionSignaled",
		now.Format(time.RFC3339Nano),
		`{"workflowType":"Approval","input":{"amount":5},"taskQueue":"q1"}`,
		`{"signalName":"approval","payload":{"approved":true}}`,
	} {
		if !strings.Contains(got, want) {
			t.Fatalf("history output missing %q:\n%s", want, got)
		}
	}
}

func TestHelpDocumentsFlags(t *testing.T) {
	tests := []struct {
		name   string
		args   []string
		wants  []string
		stderr bool
	}{
		{
			name:   "server help",
			args:   []string{"server", "--help"},
			wants:  []string{"wfe server [flags]", "-listen", "-num-shards", "-seed"},
			stderr: true,
		},
		{
			name: "workflow start help",
			args: []string{"workflow", "start", "--help"},
			wants: []string{
				"wfe workflow start <id> [flags]",
				"-workflow-type",
				"-task-queue",
				"-input",
				"-request-id",
				"-execution-timeout",
			},
			stderr: true,
		},
		{
			name:   "workflow signal help",
			args:   []string{"workflow", "signal", "--help"},
			wants:  []string{"wfe workflow signal <id> <name> [flags]", "-payload", "-request-id", "-addr"},
			stderr: true,
		},
		{
			name:   "workflow history help",
			args:   []string{"workflow", "history", "--help"},
			wants:  []string{"wfe workflow history <id> [flags]", "-addr"},
			stderr: true,
		},
		{
			name:   "root help",
			args:   []string{"--help"},
			wants:  []string{"wfe server [flags]", "wfe workflow <start|signal|history>"},
			stderr: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var out bytes.Buffer
			var errOut bytes.Buffer
			err := run(tt.args, &out, &errOut)
			if err != nil {
				t.Fatalf("run %v: %v", tt.args, err)
			}

			got := out.String()
			if tt.stderr {
				got = errOut.String()
			}
			for _, want := range tt.wants {
				if !strings.Contains(got, want) {
					t.Fatalf("help output missing %q:\n%s", want, got)
				}
			}
		})
	}
}
