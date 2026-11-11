package server

import (
	"context"
	"errors"
	"testing"

	"github.com/niqbal/workflow-engine/internal/history"
	"github.com/niqbal/workflow-engine/internal/server/api"
	"github.com/niqbal/workflow-engine/internal/store"
)

// Tester edge cases for WI-017 (SPEC §8.4).

func TestEdge_StartWithoutRequestIDNeverDedups(t *testing.T) {
	ctx := context.Background()
	s, _ := verifyServer(t)
	first, err := s.StartWorkflow(ctx, verifyStart("wf", ""))
	if err != nil || first.RunID == "" {
		t.Fatalf("first start: %q, %v", first.RunID, err)
	}
	if _, err := s.StartWorkflow(ctx, verifyStart("wf", "")); !errors.Is(err, api.ErrAlreadyStarted) {
		t.Fatalf("second start without requestID on open run: want already_started, got %v", err)
	}
}

func TestEdge_SignalValidationAndOpenRunDescribe(t *testing.T) {
	ctx := context.Background()
	s, st := verifyServer(t)
	resp, err := s.StartWorkflow(ctx, verifyStart("wf", "r"))
	if err != nil {
		t.Fatal(err)
	}
	key := store.RunKey{Shard: s.ShardFor("wf"), WorkflowID: "wf", RunID: resp.RunID}
	before, err := st.GetRun(ctx, key)
	if err != nil {
		t.Fatal(err)
	}
	if err := s.SignalWorkflow(ctx, api.SignalRequest{WorkflowID: "wf", RequestID: "s"}); !errors.Is(err, api.ErrInvalidArgument) {
		t.Fatalf("signal without name: want invalid_argument, got %v", err)
	}
	after, err := st.GetRun(ctx, key)
	if err != nil || after.DBVersion != before.DBVersion {
		t.Fatalf("rejected signal wrote: %d -> %d, %v", before.DBVersion, after.DBVersion, err)
	}
	d, err := s.DescribeWorkflow(ctx, "wf")
	if err != nil || d.WorkflowID != "wf" || d.RunID != resp.RunID || d.WorkflowType != "T" || d.Status != history.StatusRunning {
		t.Fatalf("describe open run: %+v, %v", d, err)
	}
	if len(d.PendingActivities) != 0 || len(d.PendingTimers) != 0 {
		t.Fatalf("fresh run has no pending work: %+v", d)
	}
	if _, err := s.DescribeWorkflow(ctx, ""); !errors.Is(err, api.ErrInvalidArgument) {
		t.Fatalf("describe empty id: want invalid_argument, got %v", err)
	}
	evs, err := s.GetHistory(ctx, "wf")
	if err != nil || len(evs) != 2 || evs[0].Type() != history.EventWorkflowExecutionStarted {
		t.Fatalf("history of fresh run: %d events, %v", len(evs), err)
	}
}

func TestEdge_TerminateOpenRunThenSignalIsClosed(t *testing.T) {
	ctx := context.Background()
	s, _ := verifyServer(t)
	if _, err := s.StartWorkflow(ctx, verifyStart("wf", "r")); err != nil {
		t.Fatal(err)
	}
	if err := s.TerminateWorkflow(ctx, api.TerminateRequest{WorkflowID: "wf", Reason: "stop"}); err != nil {
		t.Fatal(err)
	}
	if err := s.SignalWorkflow(ctx, api.SignalRequest{WorkflowID: "wf", SignalName: "x", RequestID: "new"}); !errors.Is(err, api.ErrWorkflowClosed) {
		t.Fatalf("signal closed run: want workflow_closed, got %v", err)
	}
	if err := s.TerminateWorkflow(ctx, api.TerminateRequest{}); !errors.Is(err, api.ErrInvalidArgument) {
		t.Fatalf("terminate empty id: want invalid_argument, got %v", err)
	}
}
