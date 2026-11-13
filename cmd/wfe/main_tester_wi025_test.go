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

type wi025Service struct {
	history []history.Event
}

func (s *wi025Service) StartWorkflow(context.Context, api.StartRequest) (api.StartResponse, error) {
	return api.StartResponse{}, fmt.Errorf("not implemented")
}

func (s *wi025Service) SignalWorkflow(context.Context, api.SignalRequest) error {
	return fmt.Errorf("not implemented")
}

func (s *wi025Service) TerminateWorkflow(context.Context, api.TerminateRequest) error {
	return fmt.Errorf("not implemented")
}

func (s *wi025Service) DescribeWorkflow(context.Context, string) (api.Description, error) {
	return api.Description{}, fmt.Errorf("not implemented")
}

func (s *wi025Service) GetHistory(context.Context, string) ([]history.Event, error) {
	return s.history, nil
}

func (s *wi025Service) PollWorkflowTask(context.Context, api.PollRequest) (*api.WorkflowTask, error) {
	return nil, fmt.Errorf("not implemented")
}

func (s *wi025Service) CompleteWorkflowTask(context.Context, api.CompleteWorkflowTaskRequest) error {
	return fmt.Errorf("not implemented")
}

func (s *wi025Service) FailWorkflowTask(context.Context, api.FailWorkflowTaskRequest) error {
	return fmt.Errorf("not implemented")
}

func (s *wi025Service) PollActivityTask(context.Context, api.PollRequest) (*api.ActivityTask, error) {
	return nil, fmt.Errorf("not implemented")
}

func (s *wi025Service) CompleteActivityTask(context.Context, api.CompleteActivityTaskRequest) error {
	return fmt.Errorf("not implemented")
}

func (s *wi025Service) FailActivityTask(context.Context, api.FailActivityTaskRequest) error {
	return fmt.Errorf("not implemented")
}

func TestWI025WorkflowHistoryReadableOutput(t *testing.T) {
	t0 := time.Date(2026, 11, 13, 5, 4, 3, 21_000_000, time.FixedZone("offset", 2*3600))
	svc := &wi025Service{
		history: []history.Event{
			{
				ID:   10,
				Time: t0,
				Attrs: history.WorkflowExecutionStarted{
					WorkflowType: "Approval",
					TaskQueue:    "q-approvals",
					Input:        json.RawMessage(`{"amount":15}`),
				},
			},
			{
				ID:   11,
				Time: t0.Add(500 * time.Millisecond),
				Attrs: history.WorkflowExecutionSignaled{
					SignalName: "approval",
					Payload:    json.RawMessage(`{"approved":true}`),
				},
			},
		},
	}

	srv := httptest.NewServer(httpapi.New(svc, httpapi.Options{}))
	defer srv.Close()

	var out bytes.Buffer
	var errOut bytes.Buffer
	err := run(
		[]string{"workflow", "history", "--addr", srv.URL, "wf-42"},
		&out,
		&errOut,
	)
	if err != nil {
		t.Fatalf("run workflow history: %v (stderr=%q)", err, errOut.String())
	}

	got := out.String()
	lines := strings.Split(strings.TrimSpace(got), "\n")
	if len(lines) < 3 {
		t.Fatalf("expected header + 2 events, got %d lines:\n%s", len(lines), got)
	}

	wants := []string{
		"ID",
		"Time",
		"Type",
		"Attrs",
		"WorkflowExecutionStarted",
		"WorkflowExecutionSignaled",
		t0.UTC().Format(time.RFC3339Nano),
		`{"workflowType":"Approval","input":{"amount":15},"taskQueue":"q-approvals"}`,
		`{"signalName":"approval","payload":{"approved":true}}`,
	}
	for _, want := range wants {
		if !strings.Contains(got, want) {
			t.Fatalf("history output missing %q:\n%s", want, got)
		}
	}
}

func TestWI025HelpDocumentsFlags(t *testing.T) {
	tests := []struct {
		name    string
		args    []string
		wantOut []string
		wantErr []string
	}{
		{
			name: "root help lists commands",
			args: []string{"--help"},
			wantOut: []string{
				"wfe server [flags]",
				"wfe workflow <start|signal|history>",
				"Use 'wfe <command> --help'",
			},
		},
		{
			name: "workflow help lists subcommands",
			args: []string{"workflow", "--help"},
			wantOut: []string{
				"wfe workflow start <id> [flags]",
				"wfe workflow signal <id> <name> [flags]",
				"wfe workflow history <id> [flags]",
			},
		},
		{
			name: "server help flags",
			args: []string{"server", "--help"},
			wantErr: []string{
				"-listen",
				"-node",
				"-num-shards",
				"-long-poll",
				"-redispatch-interval",
				"-lease-duration",
				"-seed",
			},
		},
		{
			name: "workflow start help flags",
			args: []string{"workflow", "start", "--help"},
			wantErr: []string{
				"-addr",
				"-workflow-type",
				"-task-queue",
				"-input",
				"-request-id",
				"-execution-timeout",
			},
		},
		{
			name: "workflow signal help flags",
			args: []string{"workflow", "signal", "--help"},
			wantErr: []string{
				"-addr",
				"-payload",
				"-request-id",
			},
		},
		{
			name: "workflow history help flags",
			args: []string{"workflow", "history", "--help"},
			wantErr: []string{
				"-addr",
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var out bytes.Buffer
			var errOut bytes.Buffer

			err := run(tt.args, &out, &errOut)
			if err != nil {
				t.Fatalf("run(%v): %v", tt.args, err)
			}

			for _, w := range tt.wantOut {
				if !strings.Contains(out.String(), w) {
					t.Fatalf("stdout missing %q:\n%s", w, out.String())
				}
			}
			for _, w := range tt.wantErr {
				if !strings.Contains(errOut.String(), w) {
					t.Fatalf("stderr missing %q:\n%s", w, errOut.String())
				}
			}
		})
	}
}
