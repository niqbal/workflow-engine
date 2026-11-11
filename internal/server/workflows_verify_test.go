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

// Independent verification of WI-017 (SPEC §8.4).

func verifyServer(t *testing.T) (*Server, store.Store) {
	t.Helper()
	clk := clock.NewManual(time.Date(2026, 11, 11, 12, 0, 0, 0, time.UTC))
	rt := &xsync.Real{Ctx: context.Background(), C: clk}
	st := memstore.New(memstore.WithRuntime(rt))
	s := New(rt, st, rnd.New(42), Config{NumShards: 4})
	if err := s.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	return s, st
}

func verifyStart(id, req string) api.StartRequest {
	return api.StartRequest{WorkflowID: id, WorkflowType: "T", TaskQueue: "tq", RequestID: req}
}

func TestVerify_StartRejectsEachMissingFieldAndCreatesNothing(t *testing.T) {
	ctx := context.Background()
	s, st := verifyServer(t)
	cases := map[string]api.StartRequest{
		"workflowID":   {WorkflowType: "T", TaskQueue: "tq", RequestID: "r"},
		"workflowType": {WorkflowID: "wf", TaskQueue: "tq", RequestID: "r"},
		"taskQueue":    {WorkflowID: "wf", WorkflowType: "T", RequestID: "r"},
	}
	for _, name := range xsync.SortedKeys(cases) {
		if _, err := s.StartWorkflow(ctx, cases[name]); !errors.Is(err, api.ErrInvalidArgument) {
			t.Errorf("missing %s: want invalid_argument, got %v", name, err)
		}
	}
	if _, err := st.GetLatestRun(ctx, s.ShardFor("wf"), "wf"); !errors.Is(err, store.ErrNotFound) {
		t.Fatalf("rejected start must not create a run, got %v", err)
	}
	// A later valid start still works.
	if _, err := s.StartWorkflow(ctx, verifyStart("wf", "r")); err != nil {
		t.Fatalf("valid start after rejections: %v", err)
	}
}

func TestVerify_StartStoresRunOnItsShard(t *testing.T) {
	ctx := context.Background()
	s, st := verifyServer(t)
	for _, id := range []string{"a", "b", "c", "order-17", "refund-9"} {
		resp, err := s.StartWorkflow(ctx, verifyStart(id, "req-"+id))
		if err != nil || resp.RunID == "" {
			t.Fatalf("start %s: %q, %v", id, resp.RunID, err)
		}
		key, err := st.GetLatestRun(ctx, s.ShardFor(id), id)
		if err != nil || key.RunID != resp.RunID || key.Shard != s.ShardFor(id) {
			t.Fatalf("%s: latest run %+v, %v; want run %s on shard %d", id, key, err, resp.RunID, s.ShardFor(id))
		}
	}
}

func TestVerify_StartDedupOpenAndClosedRun(t *testing.T) {
	ctx := context.Background()
	s, _ := verifyServer(t)
	first, err := s.StartWorkflow(ctx, verifyStart("wf", "req-1"))
	if err != nil {
		t.Fatal(err)
	}
	// Open run: same requestID returns the same run, different one is already_started.
	if r, err := s.StartWorkflow(ctx, verifyStart("wf", "req-1")); err != nil || r.RunID != first.RunID {
		t.Fatalf("open retry: %q, %v", r.RunID, err)
	}
	if _, err := s.StartWorkflow(ctx, verifyStart("wf", "req-2")); !errors.Is(err, api.ErrAlreadyStarted) {
		t.Fatalf("open, other requestID: want already_started, got %v", err)
	}

	if err := s.TerminateWorkflow(ctx, api.TerminateRequest{WorkflowID: "wf", Reason: "x"}); err != nil {
		t.Fatal(err)
	}
	evsBefore, err := s.GetHistory(ctx, "wf")
	if err != nil {
		t.Fatal(err)
	}
	// Closed run: retries still return the original run, repeatedly, without writing.
	for i := 0; i < 3; i++ {
		r, err := s.StartWorkflow(ctx, verifyStart("wf", "req-1"))
		if err != nil || r.RunID != first.RunID {
			t.Fatalf("closed retry %d: %q, %v; want %q", i, r.RunID, err, first.RunID)
		}
	}
	// Different requestID after close: already_started (workflowID used once).
	if _, err := s.StartWorkflow(ctx, verifyStart("wf", "req-3")); !errors.Is(err, api.ErrAlreadyStarted) {
		t.Fatalf("closed, other requestID: want already_started, got %v", err)
	}
	d, err := s.DescribeWorkflow(ctx, "wf")
	if err != nil || d.RunID != first.RunID || d.Status != history.StatusTerminated {
		t.Fatalf("describe after dedup: %+v, %v", d, err)
	}
	evsAfter, err := s.GetHistory(ctx, "wf")
	if err != nil || len(evsAfter) != len(evsBefore) {
		t.Fatalf("dedup wrote history: %d -> %d events, %v", len(evsBefore), len(evsAfter), err)
	}
}

func TestVerify_SignalMissingWorkflow(t *testing.T) {
	ctx := context.Background()
	s, _ := verifyServer(t)
	if err := s.SignalWorkflow(ctx, api.SignalRequest{WorkflowID: "nope", SignalName: "sig", RequestID: "s"}); !errors.Is(err, api.ErrNotFound) {
		t.Fatalf("signal missing: want not_found, got %v", err)
	}
	if err := s.SignalWorkflow(ctx, api.SignalRequest{WorkflowID: "nope", SignalName: "sig"}); !errors.Is(err, api.ErrNotFound) {
		t.Fatalf("signal missing without requestID: want not_found, got %v", err)
	}
	// Another workflow existing must not make an unrelated ID resolvable.
	if _, err := s.StartWorkflow(ctx, verifyStart("other", "r")); err != nil {
		t.Fatal(err)
	}
	if err := s.SignalWorkflow(ctx, api.SignalRequest{WorkflowID: "nope", SignalName: "sig", RequestID: "s"}); !errors.Is(err, api.ErrNotFound) {
		t.Fatalf("signal missing with other wf present: want not_found, got %v", err)
	}
	if _, err := s.GetHistory(ctx, "nope"); !errors.Is(err, api.ErrNotFound) {
		t.Fatalf("history missing: want not_found, got %v", err)
	}
	if err := s.SignalWorkflow(ctx, api.SignalRequest{SignalName: "sig"}); !errors.Is(err, api.ErrInvalidArgument) {
		t.Fatalf("signal empty workflowID: want invalid_argument, got %v", err)
	}
}

func TestVerify_TerminateClosedRunIsNoOp(t *testing.T) {
	ctx := context.Background()
	s, st := verifyServer(t)
	resp, err := s.StartWorkflow(ctx, verifyStart("wf", "r"))
	if err != nil {
		t.Fatal(err)
	}
	if err := s.TerminateWorkflow(ctx, api.TerminateRequest{WorkflowID: "wf", Reason: "one"}); err != nil {
		t.Fatal(err)
	}
	key := store.RunKey{Shard: s.ShardFor("wf"), WorkflowID: "wf", RunID: resp.RunID}
	snap1, err := st.GetRun(ctx, key)
	if err != nil {
		t.Fatal(err)
	}
	h1, err := s.GetHistory(ctx, "wf")
	if err != nil {
		t.Fatal(err)
	}
	terminated := 0
	for _, e := range h1 {
		if e.Type() == history.EventWorkflowExecutionTerminated {
			terminated++
		}
	}
	if terminated != 1 || h1[len(h1)-1].Type() != history.EventWorkflowExecutionTerminated {
		t.Fatalf("first terminate: history types end wrong, %d terminated events", terminated)
	}
	for i := 0; i < 2; i++ {
		if err := s.TerminateWorkflow(ctx, api.TerminateRequest{WorkflowID: "wf", Reason: "again"}); err != nil {
			t.Fatalf("terminate closed run %d: want nil, got %v", i, err)
		}
	}
	snap2, err := st.GetRun(ctx, key)
	if err != nil {
		t.Fatal(err)
	}
	if snap2.DBVersion != snap1.DBVersion || snap2.Open {
		t.Fatalf("no-op terminate wrote: version %d -> %d, open=%v", snap1.DBVersion, snap2.DBVersion, snap2.Open)
	}
	h2, err := s.GetHistory(ctx, "wf")
	if err != nil || len(h2) != len(h1) {
		t.Fatalf("no-op terminate changed history: %d -> %d, %v", len(h1), len(h2), err)
	}
	if err := s.TerminateWorkflow(ctx, api.TerminateRequest{WorkflowID: "ghost"}); !errors.Is(err, api.ErrNotFound) {
		t.Fatalf("terminate missing: want not_found, got %v", err)
	}
}
