package server

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/niqbal/workflow-engine/internal/clock"
	"github.com/niqbal/workflow-engine/internal/history"
	"github.com/niqbal/workflow-engine/internal/rnd"
	"github.com/niqbal/workflow-engine/internal/server/api"
	"github.com/niqbal/workflow-engine/internal/store"
	"github.com/niqbal/workflow-engine/internal/store/memstore"
	"github.com/niqbal/workflow-engine/internal/xsync"
)

func newTestServer(t *testing.T) (*Server, store.Store) {
	t.Helper()
	clk := clock.NewManual(time.Date(2026, 11, 11, 12, 0, 0, 0, time.UTC))
	rt := &xsync.Real{Ctx: context.Background(), C: clk}
	st := memstore.New(memstore.WithRuntime(rt))
	s := New(rt, st, rnd.New(7), Config{NumShards: 2})
	if err := s.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	return s, st
}

func startReq(id, req string) api.StartRequest {
	return api.StartRequest{WorkflowID: id, WorkflowType: "W", TaskQueue: "q", RequestID: req}
}

func historyTypes(t *testing.T, s *Server, id string) []history.EventType {
	t.Helper()
	evs, err := s.GetHistory(context.Background(), id)
	if err != nil {
		t.Fatalf("history: %v", err)
	}
	out := make([]history.EventType, len(evs))
	for i, e := range evs {
		out[i] = e.Type()
	}
	return out
}

func TestStartDedupAgainstClosedRun(t *testing.T) {
	ctx := context.Background()
	s, _ := newTestServer(t)
	resp, err := s.StartWorkflow(ctx, startReq("wf", "r1"))
	if err != nil {
		t.Fatal(err)
	}
	if err := s.TerminateWorkflow(ctx, api.TerminateRequest{WorkflowID: "wf", Reason: "done"}); err != nil {
		t.Fatal(err)
	}
	d, err := s.DescribeWorkflow(ctx, "wf")
	if err != nil || d.Status != history.StatusTerminated {
		t.Fatalf("describe: %+v, %v", d, err)
	}

	again, err := s.StartWorkflow(ctx, startReq("wf", "r1"))
	if err != nil || again.RunID != resp.RunID {
		t.Fatalf("retry after close: got %q, %v; want %q", again.RunID, err, resp.RunID)
	}
	if _, err := s.StartWorkflow(ctx, startReq("wf", "r2")); !errors.Is(err, api.ErrAlreadyStarted) {
		t.Fatalf("new requestID after close: want already_started, got %v", err)
	}
	// An empty requestID never matches a stored one.
	if _, err := s.StartWorkflow(ctx, startReq("wf", "")); !errors.Is(err, api.ErrAlreadyStarted) {
		t.Fatalf("empty requestID: want already_started, got %v", err)
	}
	d, err = s.DescribeWorkflow(ctx, "wf")
	if err != nil || d.RunID != resp.RunID || d.Status != history.StatusTerminated {
		t.Fatalf("no new run may be created: %+v, %v", d, err)
	}
}

func TestStartRejectsNegativeExecutionTimeout(t *testing.T) {
	s, _ := newTestServer(t)
	r := startReq("wf", "r1")
	r.ExecutionTimeout = -time.Second
	if _, err := s.StartWorkflow(context.Background(), r); !errors.Is(err, api.ErrInvalidArgument) {
		t.Fatalf("want invalid_argument, got %v", err)
	}
}

func TestSignalMissingWorkflowIsNotFound(t *testing.T) {
	ctx := context.Background()
	s, _ := newTestServer(t)
	err := s.SignalWorkflow(ctx, api.SignalRequest{WorkflowID: "missing", SignalName: "go", RequestID: "s1"})
	if !errors.Is(err, api.ErrNotFound) {
		t.Fatalf("want not_found, got %v", err)
	}
	if _, err := s.DescribeWorkflow(ctx, "missing"); !errors.Is(err, api.ErrNotFound) {
		t.Fatalf("describe missing: want not_found, got %v", err)
	}
	if err := s.TerminateWorkflow(ctx, api.TerminateRequest{WorkflowID: "missing"}); !errors.Is(err, api.ErrNotFound) {
		t.Fatalf("terminate missing: want not_found, got %v", err)
	}
}

func TestTerminateClosedRunIsNoOp(t *testing.T) {
	ctx := context.Background()
	s, st := newTestServer(t)
	resp, err := s.StartWorkflow(ctx, startReq("wf", "r1"))
	if err != nil {
		t.Fatal(err)
	}
	if err := s.TerminateWorkflow(ctx, api.TerminateRequest{WorkflowID: "wf", Reason: "first"}); err != nil {
		t.Fatal(err)
	}
	key := store.RunKey{Shard: s.ShardFor("wf"), WorkflowID: "wf", RunID: resp.RunID}
	before, err := st.GetRun(ctx, key)
	if err != nil {
		t.Fatal(err)
	}
	if before.Open {
		t.Fatal("terminated run must be stored closed")
	}

	if err := s.TerminateWorkflow(ctx, api.TerminateRequest{WorkflowID: "wf", Reason: "second"}); err != nil {
		t.Fatalf("terminate of a closed run: %v", err)
	}
	after, err := st.GetRun(ctx, key)
	if err != nil {
		t.Fatal(err)
	}
	if after.DBVersion != before.DBVersion {
		t.Fatalf("terminate of a closed run wrote: DBVersion %d -> %d", before.DBVersion, after.DBVersion)
	}
	got := historyTypes(t, s, "wf")
	want := []history.EventType{history.EventWorkflowExecutionStarted, history.EventWorkflowTaskScheduled,
		history.EventWorkflowExecutionTerminated}
	if len(got) != len(want) {
		t.Fatalf("history %v, want %v", got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("history %v, want %v", got, want)
		}
	}
}

func TestSignalAppendsAndDedups(t *testing.T) {
	ctx := context.Background()
	s, _ := newTestServer(t)
	if _, err := s.StartWorkflow(ctx, startReq("wf", "r1")); err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 2; i++ {
		if err := s.SignalWorkflow(ctx, api.SignalRequest{WorkflowID: "wf", SignalName: "go", RequestID: "s1"}); err != nil {
			t.Fatalf("signal %d: %v", i, err)
		}
	}
	if got := historyTypes(t, s, "wf"); len(got) != 3 || got[2] != history.EventWorkflowExecutionSignaled {
		t.Fatalf("history %v", got)
	}
	if err := s.TerminateWorkflow(ctx, api.TerminateRequest{WorkflowID: "wf"}); err != nil {
		t.Fatal(err)
	}
	if err := s.SignalWorkflow(ctx, api.SignalRequest{WorkflowID: "wf", SignalName: "go", RequestID: "s1"}); err != nil {
		t.Fatalf("retried accepted signal after close: %v", err)
	}
	if err := s.SignalWorkflow(ctx, api.SignalRequest{WorkflowID: "wf", SignalName: "go", RequestID: "s2"}); !errors.Is(err, api.ErrWorkflowClosed) {
		t.Fatalf("new signal to closed run: want workflow_closed, got %v", err)
	}
}
