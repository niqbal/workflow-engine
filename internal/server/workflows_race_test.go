package server

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/niqbal/workflow-engine/internal/server/api"
	"github.com/niqbal/workflow-engine/internal/simrt"
	"github.com/niqbal/workflow-engine/internal/store"
	"github.com/niqbal/workflow-engine/internal/store/memstore"
)

// countingStore records every successful CreateRun, so a test can count runs per workflow ID
// (the store only exposes the latest one).
type countingStore struct {
	store.Store
	created map[string][]string // workflowID -> run IDs
}

func (c *countingStore) CreateRun(ctx context.Context, f store.Fence, r store.NewRun) ([]store.Task, error) {
	ts, err := c.Store.CreateRun(ctx, f, r)
	if err == nil {
		c.created[r.Key.WorkflowID] = append(c.created[r.Key.WorkflowID], r.Key.RunID)
	}
	return ts, err
}

type startResult struct {
	runID string
	err   error
}

// raceStarts runs one StartWorkflow per requestID as concurrent simrt tasks and returns the
// results in request order together with the run IDs the store created for "wf".
func raceStarts(t *testing.T, seed uint64, requestIDs ...string) ([]startResult, []string) {
	t.Helper()
	ctx := context.Background()
	rt := simrt.New(seed, time.Date(2026, 11, 11, 12, 0, 0, 0, time.UTC))
	defer rt.Close()
	cs := &countingStore{Store: memstore.New(memstore.WithRuntime(rt)), created: map[string][]string{}}
	s := New(rt, cs, rt.Rand(), Config{NumShards: 2})
	if err := s.Start(ctx); err != nil {
		t.Fatal(err)
	}
	results := make([]startResult, len(requestIDs))
	for i, req := range requestIDs {
		rt.Go(fmt.Sprintf("start-%d", i), func(ctx context.Context) {
			resp, err := s.StartWorkflow(ctx, startReq("wf", req))
			results[i] = startResult{runID: resp.RunID, err: err}
		})
	}
	if res := rt.Run(time.Time{}, nil); res != simrt.Idle {
		t.Fatalf("seed %d: run ended with %v, tasks left: %s", seed, res, rt.Tasks())
	}
	return results, cs.created["wf"]
}

// A client retry racing its own in-flight start (same requestID) must not create a second
// run: every call gets the one run ID (SPEC §8.4).
func TestConcurrentStartsSameRequestIDCreateOneRun(t *testing.T) {
	for seed := uint64(1); seed <= 64; seed++ {
		results, created := raceStarts(t, seed, "r1", "r1", "r1")
		if len(created) != 1 {
			t.Fatalf("seed %d: created runs %v, want exactly one", seed, created)
		}
		for i, r := range results {
			if r.err != nil || r.runID != created[0] {
				t.Fatalf("seed %d: start %d got %q, %v; want %q", seed, i, r.runID, r.err, created[0])
			}
		}
	}
}

// Concurrent starts with different requestIDs: one wins, the others get already_started.
func TestConcurrentStartsDifferentRequestIDsCreateOneRun(t *testing.T) {
	for seed := uint64(1); seed <= 64; seed++ {
		results, created := raceStarts(t, seed, "r1", "r2", "r3")
		if len(created) != 1 {
			t.Fatalf("seed %d: created runs %v, want exactly one", seed, created)
		}
		won := 0
		for i, r := range results {
			switch {
			case r.err == nil && r.runID == created[0]:
				won++
			case errors.Is(r.err, api.ErrAlreadyStarted):
			default:
				t.Fatalf("seed %d: start %d got %q, %v; want %q or already_started", seed, i, r.runID, r.err, created[0])
			}
		}
		if won != 1 {
			t.Fatalf("seed %d: %d starts succeeded, want 1", seed, won)
		}
	}
}
