package server

import (
	"context"
	"encoding/base64"
	"errors"
	"testing"
	"time"

	"github.com/niqbal/workflow-engine/internal/history"
	"github.com/niqbal/workflow-engine/internal/server/api"
	"github.com/niqbal/workflow-engine/internal/server/matching"
	"github.com/niqbal/workflow-engine/internal/simrt"
	"github.com/niqbal/workflow-engine/internal/store/memstore"
)

var wftT0 = time.Date(2026, 11, 11, 12, 0, 0, 0, time.UTC)

func eventTypes(evs []history.Event) []history.EventType {
	out := make([]history.EventType, len(evs))
	for i, e := range evs {
		out[i] = e.Type()
	}
	return out
}

func sameTypes(a, b []history.EventType) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

func flipTokenByte(t *testing.T, tok string) string {
	t.Helper()
	raw, err := base64.RawURLEncoding.DecodeString(tok)
	if err != nil {
		t.Fatalf("decode token %q: %v", tok, err)
	}
	if len(raw) == 0 {
		t.Fatal("token has no bytes")
	}
	raw[len(raw)-1] ^= 1
	return base64.RawURLEncoding.EncodeToString(raw)
}

// pollNow polls with a task already queued (the manual clock never reaches the long-poll
// deadline, so the caller must make sure one is there).
func pollNow(t *testing.T, s *Server) *api.WorkflowTask {
	t.Helper()
	s.Settle(context.Background())
	if s.match.Len(matching.WorkflowTasks, "q") == 0 {
		t.Fatal("no workflow task queued")
	}
	task, err := s.PollWorkflowTask(context.Background(), api.PollRequest{TaskQueue: "q", Identity: "w"})
	if err != nil || task == nil {
		t.Fatalf("poll: %v, %v", task, err)
	}
	return task
}

func TestPollWorkflowTaskValidatesAndHonoursDoneContext(t *testing.T) {
	s, _ := newTestServer(t)
	_, err := s.PollWorkflowTask(context.Background(), api.PollRequest{})
	if !errors.Is(err, api.ErrInvalidArgument) {
		t.Fatalf("empty task queue: got %v", err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := s.StartWorkflow(context.Background(), startReq("wf", "r")); err != nil {
		t.Fatal(err)
	}
	s.Settle(context.Background())
	task, err := s.PollWorkflowTask(ctx, api.PollRequest{TaskQueue: "q"})
	if task != nil || err != nil {
		t.Fatalf("done context: got %v, %v; want nil, nil", task, err)
	}
	if n := s.match.Len(matching.WorkflowTasks, "q"); n != 1 {
		t.Fatalf("a poll with a done context must not consume the task; queue has %d", n)
	}
}

func TestPollWorkflowTaskReturnsAtLongPollOnServerClock(t *testing.T) {
	rt := simrt.New(1, wftT0)
	defer rt.Close()
	s := New(rt, memstore.New(memstore.WithRuntime(rt)), rt.Rand(), Config{NumShards: 2, LongPoll: 20 * time.Second})
	if err := s.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	var (
		task   *api.WorkflowTask
		err    error
		waited time.Duration
		done   bool
	)
	rt.Go("poller", func(ctx context.Context) {
		begin := rt.Now()
		task, err = s.PollWorkflowTask(ctx, api.PollRequest{TaskQueue: "q"})
		waited, done = rt.Now().Sub(begin), true
	})
	if res := rt.Run(time.Time{}, nil); res != simrt.Idle {
		t.Fatalf("run ended with %v: %s", res, rt.Tasks())
	}
	if !done || task != nil || err != nil || waited != 20*time.Second {
		t.Fatalf("poll: done=%v task=%v err=%v waited=%v; want nil, nil after 20s", done, task, err, waited)
	}
}

func TestPollWorkflowTaskWakesWhenTaskArrives(t *testing.T) {
	rt := simrt.New(2, wftT0)
	defer rt.Close()
	s := New(rt, memstore.New(memstore.WithRuntime(rt)), rt.Rand(), Config{NumShards: 2})
	if err := s.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	var task *api.WorkflowTask
	var perr error
	var waited time.Duration
	rt.Go("poller", func(ctx context.Context) {
		begin := rt.Now()
		task, perr = s.PollWorkflowTask(ctx, api.PollRequest{TaskQueue: "q", Identity: "w"})
		waited = rt.Now().Sub(begin)
	})
	rt.Go("starter", func(ctx context.Context) {
		rt.Sleep(5 * time.Second)
		if _, err := s.StartWorkflow(ctx, startReq("wf", "r")); err != nil {
			panic(err)
		}
		s.Settle(ctx)
	})
	if res := rt.Run(time.Time{}, nil); res != simrt.Idle {
		t.Fatalf("run ended with %v: %s", res, rt.Tasks())
	}
	if perr != nil || task == nil {
		t.Fatalf("poll: %v, %v", task, perr)
	}
	if waited != 5*time.Second {
		t.Fatalf("poll waited %v, want 5s", waited)
	}
	want := []history.EventType{history.EventWorkflowExecutionStarted, history.EventWorkflowTaskScheduled, history.EventWorkflowTaskStarted}
	if got := eventTypes(task.History); !sameTypes(got, want) {
		t.Fatalf("history %v, want %v", got, want)
	}
}

// A duplicate delivery of an already-started task is skipped: the poll keeps waiting and the
// started task's transfer record is gone from the store.
func TestPollWorkflowTaskSkipsDuplicateDelivery(t *testing.T) {
	rt := simrt.New(3, wftT0)
	defer rt.Close()
	st := memstore.New(memstore.WithRuntime(rt))
	s := New(rt, st, rt.Rand(), Config{NumShards: 2, LongPoll: 10 * time.Second})
	if err := s.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	var first, second *api.WorkflowTask
	var err1, err2 error
	var left, stored int
	rt.Go("test", func(ctx context.Context) {
		if _, err := s.StartWorkflow(ctx, startReq("wf", "r")); err != nil {
			panic(err)
		}
		s.Settle(ctx)
		queued := drainQueue(s, matching.WorkflowTasks, "q")
		if len(queued) != 1 {
			panic("want one queued workflow task")
		}
		s.match.Add(matching.WorkflowTasks, "q", queued[0])
		s.match.Add(matching.WorkflowTasks, "q", queued[0])
		first, err1 = s.PollWorkflowTask(ctx, api.PollRequest{TaskQueue: "q"})
		second, err2 = s.PollWorkflowTask(ctx, api.PollRequest{TaskQueue: "q"})
		left = s.match.Len(matching.WorkflowTasks, "q")
		ts, err := st.ReadTransferTasks(ctx, queued[0].Shard, 1, 100)
		if err != nil {
			panic(err)
		}
		stored = len(ts)
	})
	if res := rt.Run(time.Time{}, nil); res != simrt.Idle {
		t.Fatalf("run ended with %v: %s", res, rt.Tasks())
	}
	if first == nil || err1 != nil {
		t.Fatalf("first poll: %v, %v", first, err1)
	}
	if second != nil || err2 != nil {
		t.Fatalf("duplicate must be skipped: got %v, %v", second, err2)
	}
	if left != 0 || stored != 0 {
		t.Fatalf("queue has %d, store has %d transfer tasks; want 0 and 0", left, stored)
	}
}

// A retried attempt is transient: the worker gets its scheduled and started events with the
// IDs they will have, and history stores them only when the attempt completes.
func TestPollWorkflowTaskCarriesTransientAttempt(t *testing.T) {
	s, _ := newTestServer(t)
	ctx := context.Background()
	if _, err := s.StartWorkflow(ctx, startReq("wf", "r")); err != nil {
		t.Fatal(err)
	}
	first := pollNow(t, s)
	if err := s.FailWorkflowTask(ctx, api.FailWorkflowTaskRequest{TaskToken: first.TaskToken, Cause: history.Failure{Type: "Boom"}}); err != nil {
		t.Fatal(err)
	}
	second := pollNow(t, s)
	if second.Attempt != 2 {
		t.Fatalf("attempt %d, want 2", second.Attempt)
	}
	want := []history.EventType{history.EventWorkflowExecutionStarted, history.EventWorkflowTaskScheduled,
		history.EventWorkflowTaskStarted, history.EventWorkflowTaskFailed, history.EventWorkflowTaskScheduled,
		history.EventWorkflowTaskStarted}
	if got := eventTypes(second.History); !sameTypes(got, want) {
		t.Fatalf("task history %v, want %v", got, want)
	}
	for i, e := range second.History {
		if e.ID != int64(i+1) {
			t.Fatalf("event %d has ID %d", i, e.ID)
		}
	}
	if got := historyTypes(t, s, "wf"); !sameTypes(got, want[:4]) {
		t.Fatalf("persisted %v, want %v", got, want[:4])
	}
	// The first token is stale for both complete and fail.
	if err := s.FailWorkflowTask(ctx, api.FailWorkflowTaskRequest{TaskToken: first.TaskToken}); !errors.Is(err, api.ErrStaleTask) {
		t.Fatalf("stale fail: %v", err)
	}
	if err := s.CompleteWorkflowTask(ctx, api.CompleteWorkflowTaskRequest{TaskToken: first.TaskToken}); !errors.Is(err, api.ErrStaleTask) {
		t.Fatalf("stale complete: %v", err)
	}
	if err := s.CompleteWorkflowTask(ctx, api.CompleteWorkflowTaskRequest{TaskToken: second.TaskToken}); err != nil {
		t.Fatal(err)
	}
	want = append(want, history.EventWorkflowTaskCompleted)
	if got := historyTypes(t, s, "wf"); !sameTypes(got, want) {
		t.Fatalf("persisted %v, want %v", got, want)
	}
}

func TestWorkflowTaskTokenRejectsForgedTokens(t *testing.T) {
	s, _ := newTestServer(t)
	ctx := context.Background()
	if _, err := s.StartWorkflow(ctx, startReq("wf", "r")); err != nil {
		t.Fatal(err)
	}
	task := pollNow(t, s)
	tok, err := s.decodeWorkflowTaskToken(task.TaskToken)
	if err != nil || tok.WorkflowID != "wf" || tok.RunID != task.RunID || tok.Shard != s.ShardFor("wf") || tok.Seq != 1 {
		t.Fatalf("token %+v, %v", tok, err)
	}
	wrongShard, _ := encodeWorkflowTaskToken(s.cfg.TokenKey, workflowTaskToken{Shard: 1 - tok.Shard, WorkflowID: "wf", RunID: tok.RunID, Seq: 1})
	wrongKey := append([]byte(nil), s.cfg.TokenKey...)
	wrongKey[0] ^= 1
	wrongKeySigned, _ := encodeWorkflowTaskToken(wrongKey, workflowTaskToken{Shard: tok.Shard, WorkflowID: tok.WorkflowID, RunID: tok.RunID, Seq: tok.Seq})
	flipped := flipTokenByte(t, task.TaskToken)
	for _, bad := range []string{"", "garbage!", base64.RawURLEncoding.EncodeToString([]byte("not json")), wrongShard, wrongKeySigned, flipped} {
		if err := s.CompleteWorkflowTask(ctx, api.CompleteWorkflowTaskRequest{TaskToken: bad}); !errors.Is(err, api.ErrInvalidArgument) {
			t.Fatalf("complete with %q: %v", bad, err)
		}
		if err := s.FailWorkflowTask(ctx, api.FailWorkflowTaskRequest{TaskToken: bad}); !errors.Is(err, api.ErrInvalidArgument) {
			t.Fatalf("fail with %q: %v", bad, err)
		}
	}
	unknown, _ := encodeWorkflowTaskToken(s.cfg.TokenKey, workflowTaskToken{Shard: tok.Shard, WorkflowID: "wf", RunID: "nope", Seq: 1})
	if err := s.CompleteWorkflowTask(ctx, api.CompleteWorkflowTaskRequest{TaskToken: unknown}); !errors.Is(err, api.ErrNotFound) {
		t.Fatalf("unknown run: %v", err)
	}
}
