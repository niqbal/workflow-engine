package server

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/niqbal/workflow-engine/internal/history"
	"github.com/niqbal/workflow-engine/internal/server/api"
	"github.com/niqbal/workflow-engine/internal/server/matching"
	"github.com/niqbal/workflow-engine/internal/simrt"
	"github.com/niqbal/workflow-engine/internal/store/memstore"
)

func newSimServer(t *testing.T, seed uint64, longPoll time.Duration) (*simrt.Runtime, *Server) {
	t.Helper()
	rt := simrt.New(seed, wftT0)
	t.Cleanup(rt.Close)
	s := New(rt, memstore.New(memstore.WithRuntime(rt)), rt.Rand(), Config{NumShards: 2, LongPoll: longPoll})
	if err := s.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	return rt, s
}

// A task queued for a run that was terminated is stale: the poll drops it and keeps waiting
// until LongPoll, returning (nil, nil) rather than an error.
func TestTesterPollSkipsTaskOfTerminatedRun(t *testing.T) {
	rt, s := newSimServer(t, 11, 10*time.Second)
	var (
		task   *api.WorkflowTask
		perr   error
		queued int
		left   int
		done   bool
	)
	rt.Go("test", func(ctx context.Context) {
		if _, err := s.StartWorkflow(ctx, startReq("wf", "r")); err != nil {
			panic(err)
		}
		s.Settle(ctx)
		queued = s.match.Len(matching.WorkflowTasks, "q")
		if err := s.TerminateWorkflow(ctx, api.TerminateRequest{WorkflowID: "wf", Reason: "x"}); err != nil {
			panic(err)
		}
		task, perr = s.PollWorkflowTask(ctx, api.PollRequest{TaskQueue: "q", Identity: "w"})
		left = s.match.Len(matching.WorkflowTasks, "q")
		done = true
	})
	if res := rt.Run(time.Time{}, nil); res != simrt.Idle {
		t.Fatalf("run ended with %v: %s", res, rt.Tasks())
	}
	if !done {
		t.Fatal("test goroutine did not finish")
	}
	if queued != 1 {
		t.Fatalf("queued %d workflow tasks before terminate, want 1", queued)
	}
	if task != nil || perr != nil {
		t.Fatalf("poll after terminate: got %+v, %v; want nil, nil", task, perr)
	}
	if left != 0 {
		t.Fatalf("queue still has %d tasks", left)
	}
}

// A poll on another task queue does not take the task.
func TestTesterPollOtherQueueDoesNotTakeTask(t *testing.T) {
	rt, s := newSimServer(t, 12, 5*time.Second)
	var task *api.WorkflowTask
	var perr error
	var left int
	rt.Go("test", func(ctx context.Context) {
		if _, err := s.StartWorkflow(ctx, startReq("wf", "r")); err != nil {
			panic(err)
		}
		s.Settle(ctx)
		task, perr = s.PollWorkflowTask(ctx, api.PollRequest{TaskQueue: "other"})
		left = s.match.Len(matching.WorkflowTasks, "q")
	})
	if res := rt.Run(time.Time{}, nil); res != simrt.Idle {
		t.Fatalf("run ended with %v: %s", res, rt.Tasks())
	}
	if task != nil || perr != nil || left != 1 {
		t.Fatalf("got %+v, %v, left=%d; want nil, nil, 1", task, perr, left)
	}
}

// A token can be used once: a second complete, or a complete after a fail, is stale and
// writes nothing.
func TestTesterTokenSingleUse(t *testing.T) {
	s, _ := newTestServer(t)
	ctx := context.Background()
	if _, err := s.StartWorkflow(ctx, startReq("wf", "r")); err != nil {
		t.Fatal(err)
	}
	task := pollNow(t, s)
	if task.WorkflowID != "wf" || task.WorkflowType != "W" || task.Attempt != 1 || task.RunID == "" {
		t.Fatalf("task %+v", task)
	}
	if err := s.CompleteWorkflowTask(ctx, api.CompleteWorkflowTaskRequest{TaskToken: task.TaskToken}); err != nil {
		t.Fatal(err)
	}
	after := historyTypes(t, s, "wf")
	if err := s.CompleteWorkflowTask(ctx, api.CompleteWorkflowTaskRequest{TaskToken: task.TaskToken}); !errors.Is(err, api.ErrStaleTask) {
		t.Fatalf("second complete: %v, want ErrStaleTask", err)
	}
	if err := s.FailWorkflowTask(ctx, api.FailWorkflowTaskRequest{TaskToken: task.TaskToken}); !errors.Is(err, api.ErrStaleTask) {
		t.Fatalf("fail after complete: %v, want ErrStaleTask", err)
	}
	if got := historyTypes(t, s, "wf"); !sameTypes(got, after) {
		t.Fatalf("stale calls changed history: %v -> %v", after, got)
	}
}

func TestTesterFailThenCompleteIsStale(t *testing.T) {
	s, _ := newTestServer(t)
	ctx := context.Background()
	if _, err := s.StartWorkflow(ctx, startReq("wf", "r")); err != nil {
		t.Fatal(err)
	}
	task := pollNow(t, s)
	if err := s.FailWorkflowTask(ctx, api.FailWorkflowTaskRequest{TaskToken: task.TaskToken, Cause: history.Failure{Type: "X"}}); err != nil {
		t.Fatal(err)
	}
	if err := s.CompleteWorkflowTask(ctx, api.CompleteWorkflowTaskRequest{TaskToken: task.TaskToken}); !errors.Is(err, api.ErrStaleTask) {
		t.Fatalf("complete after fail: %v, want ErrStaleTask", err)
	}
}

func TestTesterTokenWithNonPositiveSeqRejected(t *testing.T) {
	s, _ := newTestServer(t)
	ctx := context.Background()
	if _, err := s.StartWorkflow(ctx, startReq("wf", "r")); err != nil {
		t.Fatal(err)
	}
	task := pollNow(t, s)
	tok, err := s.decodeWorkflowTaskToken(task.TaskToken)
	if err != nil {
		t.Fatal(err)
	}
	for _, seq := range []int64{0, -1} {
		bad, _ := encodeWorkflowTaskToken(s.cfg.TokenKey, workflowTaskToken{Shard: tok.Shard, WorkflowID: tok.WorkflowID, RunID: tok.RunID, Seq: seq})
		if err := s.CompleteWorkflowTask(ctx, api.CompleteWorkflowTaskRequest{TaskToken: bad}); !errors.Is(err, api.ErrInvalidArgument) {
			t.Fatalf("seq %d: %v", seq, err)
		}
	}
	// The genuine token still works afterwards.
	if err := s.CompleteWorkflowTask(ctx, api.CompleteWorkflowTaskRequest{TaskToken: task.TaskToken}); err != nil {
		t.Fatal(err)
	}
}
