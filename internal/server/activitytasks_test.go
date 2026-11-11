package server

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/niqbal/workflow-engine/internal/clock"
	"github.com/niqbal/workflow-engine/internal/history"
	"github.com/niqbal/workflow-engine/internal/server/api"
	"github.com/niqbal/workflow-engine/internal/server/matching"
	"github.com/niqbal/workflow-engine/internal/simrt"
	"github.com/niqbal/workflow-engine/internal/store/memstore"
)

func testActivity() history.ScheduleActivity {
	return history.ScheduleActivity{ActivityID: "a1", ActivityType: "A", Input: json.RawMessage(`"in"`),
		StartToCloseTimeout: 10 * time.Second,
		RetryPolicy:         history.RetryPolicy{InitialInterval: time.Second, BackoffCoefficient: 2, MaximumAttempts: 3}}
}

// scheduleTestActivity starts wf and completes its first workflow task with one activity.
// It returns the run ID.
func scheduleTestActivity(t *testing.T, ctx context.Context, s *Server) string {
	t.Helper()
	resp, err := s.StartWorkflow(ctx, startReq("wf", "r"))
	if err != nil {
		t.Fatal(err)
	}
	wt := pollNow(t, s)
	if err := s.CompleteWorkflowTask(ctx, api.CompleteWorkflowTaskRequest{TaskToken: wt.TaskToken,
		Commands: []history.Command{testActivity()}}); err != nil {
		t.Fatal(err)
	}
	return resp.RunID
}

// pollActNow polls with an activity task already queued.
func pollActNow(t *testing.T, s *Server) *api.ActivityTask {
	t.Helper()
	s.Settle(context.Background())
	if s.match.Len(matching.ActivityTasks, "q") == 0 {
		t.Fatal("no activity task queued")
	}
	task, err := s.PollActivityTask(context.Background(), api.PollRequest{TaskQueue: "q", Identity: "aw"})
	if err != nil || task == nil {
		t.Fatalf("poll activity: %v, %v", task, err)
	}
	return task
}

func TestActivityTaskRoundTripCarriesTokenAndAttemptInfo(t *testing.T) {
	s, _ := newTestServer(t)
	ctx := context.Background()
	run := scheduleTestActivity(t, ctx, s)
	at := pollActNow(t, s)
	if at.WorkflowID != "wf" || at.RunID != run || at.ScheduledEventID != 5 || at.ActivityID != "a1" ||
		at.ActivityType != "A" || at.Attempt != 1 || string(at.Input) != `"in"` {
		t.Fatalf("bad task %+v", at)
	}
	if want := fmt.Sprintf("wf/%s/5", run); at.IdempotencyKey != want {
		t.Fatalf("idempotency key %q, want %q", at.IdempotencyKey, want)
	}
	if want := s.clk.Now().Add(10 * time.Second); !at.Deadline.Equal(want) {
		t.Fatalf("deadline %v, want %v", at.Deadline, want)
	}
	tok, err := s.decodeActivityTaskToken(at.TaskToken)
	if err != nil || tok != (activityTaskToken{Shard: s.ShardFor("wf"), WorkflowID: "wf", RunID: run, ScheduledEventID: 5, Attempt: 1}) {
		t.Fatalf("token %+v, %v", tok, err)
	}
	if err := s.CompleteActivityTask(ctx, api.CompleteActivityTaskRequest{TaskToken: at.TaskToken, Result: json.RawMessage(`"out"`)}); err != nil {
		t.Fatal(err)
	}
	evs, err := s.GetHistory(ctx, "wf")
	if err != nil {
		t.Fatal(err)
	}
	want := []history.EventType{history.EventWorkflowExecutionStarted, history.EventWorkflowTaskScheduled,
		history.EventWorkflowTaskStarted, history.EventWorkflowTaskCompleted, history.EventActivityTaskScheduled,
		history.EventActivityTaskStarted, history.EventActivityTaskCompleted, history.EventWorkflowTaskScheduled}
	if got := eventTypes(evs); !sameTypes(got, want) {
		t.Fatalf("history %v, want %v", got, want)
	}
	if a := evs[5].Attrs.(history.ActivityTaskStarted); a.WorkerIdentity != "aw" || a.Attempt != 1 {
		t.Fatalf("started attrs %+v", a)
	}
	if err := s.CompleteActivityTask(ctx, api.CompleteActivityTaskRequest{TaskToken: at.TaskToken}); !errors.Is(err, api.ErrStaleTask) {
		t.Fatalf("second complete: %v, want ErrStaleTask", err)
	}
	if err := s.FailActivityTask(ctx, api.FailActivityTaskRequest{TaskToken: at.TaskToken}); !errors.Is(err, api.ErrStaleTask) {
		t.Fatalf("fail after complete: %v, want ErrStaleTask", err)
	}
}

// A retryable failure moves the activity to its next attempt with the same idempotency key;
// the old attempt's token is stale, and a non-retryable failure ends the activity.
func TestFailActivityTaskRetriesThenEnds(t *testing.T) {
	s, _ := newTestServer(t)
	ctx := context.Background()
	scheduleTestActivity(t, ctx, s)
	first := pollActNow(t, s)
	if err := s.FailActivityTask(ctx, api.FailActivityTaskRequest{TaskToken: first.TaskToken, Failure: history.Failure{Type: "Boom"}}); err != nil {
		t.Fatal(err)
	}
	clk := s.clk.(*clock.Manual)
	clk.AdvanceTo(clk.Now().Add(time.Minute))
	second := pollActNow(t, s)
	if second.Attempt != 2 || second.IdempotencyKey != first.IdempotencyKey || second.ScheduledEventID != first.ScheduledEventID {
		t.Fatalf("second attempt %+v, first %+v", second, first)
	}
	if err := s.CompleteActivityTask(ctx, api.CompleteActivityTaskRequest{TaskToken: first.TaskToken}); !errors.Is(err, api.ErrStaleTask) {
		t.Fatalf("stale attempt complete: %v", err)
	}
	if err := s.FailActivityTask(ctx, api.FailActivityTaskRequest{TaskToken: first.TaskToken}); !errors.Is(err, api.ErrStaleTask) {
		t.Fatalf("stale attempt fail: %v", err)
	}
	if err := s.FailActivityTask(ctx, api.FailActivityTaskRequest{TaskToken: second.TaskToken,
		Failure: history.Failure{Type: "Fatal", NonRetryable: true}}); err != nil {
		t.Fatal(err)
	}
	evs, err := s.GetHistory(ctx, "wf")
	if err != nil {
		t.Fatal(err)
	}
	want := []history.EventType{history.EventWorkflowExecutionStarted, history.EventWorkflowTaskScheduled,
		history.EventWorkflowTaskStarted, history.EventWorkflowTaskCompleted, history.EventActivityTaskScheduled,
		history.EventActivityTaskStarted, history.EventActivityTaskFailed, history.EventWorkflowTaskScheduled}
	if got := eventTypes(evs); !sameTypes(got, want) {
		t.Fatalf("history %v, want %v", got, want)
	}
	if f := evs[6].Attrs.(history.ActivityTaskFailed); f.Attempt != 2 || f.Failure.Type != "Fatal" {
		t.Fatalf("failed attrs %+v", f)
	}
}

func TestActivityTaskTokenRejectsForgedTokens(t *testing.T) {
	s, _ := newTestServer(t)
	ctx := context.Background()
	scheduleTestActivity(t, ctx, s)
	at := pollActNow(t, s)
	tok, err := s.decodeActivityTaskToken(at.TaskToken)
	if err != nil {
		t.Fatal(err)
	}
	forge := func(f func(*activityTaskToken)) string {
		c := tok
		f(&c)
		s, _ := encodeActivityTaskToken(c)
		return s
	}
	bads := []string{"", "garbage!", base64.RawURLEncoding.EncodeToString([]byte("not json")),
		forge(func(c *activityTaskToken) { c.Shard = 1 - c.Shard }),
		forge(func(c *activityTaskToken) { c.Attempt = 0 }),
		forge(func(c *activityTaskToken) { c.ScheduledEventID = 0 }),
		forge(func(c *activityTaskToken) { c.RunID = "" }),
	}
	for _, bad := range bads {
		if err := s.CompleteActivityTask(ctx, api.CompleteActivityTaskRequest{TaskToken: bad}); !errors.Is(err, api.ErrInvalidArgument) {
			t.Fatalf("complete with %q: %v", bad, err)
		}
		if err := s.FailActivityTask(ctx, api.FailActivityTaskRequest{TaskToken: bad}); !errors.Is(err, api.ErrInvalidArgument) {
			t.Fatalf("fail with %q: %v", bad, err)
		}
	}
	unknown := forge(func(c *activityTaskToken) { c.RunID = "nope" })
	if err := s.CompleteActivityTask(ctx, api.CompleteActivityTaskRequest{TaskToken: unknown}); !errors.Is(err, api.ErrNotFound) {
		t.Fatalf("unknown run: %v", err)
	}
	// The forged attempts left the real one untouched.
	if err := s.CompleteActivityTask(ctx, api.CompleteActivityTaskRequest{TaskToken: at.TaskToken}); err != nil {
		t.Fatalf("real token: %v", err)
	}
}

func TestPollActivityTaskValidatesAndHonoursDoneContext(t *testing.T) {
	s, _ := newTestServer(t)
	if _, err := s.PollActivityTask(context.Background(), api.PollRequest{}); !errors.Is(err, api.ErrInvalidArgument) {
		t.Fatalf("empty task queue: got %v", err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	task, err := s.PollActivityTask(ctx, api.PollRequest{TaskQueue: "q"})
	if task != nil || err != nil {
		t.Fatalf("done context: got %v, %v; want nil, nil", task, err)
	}
}

// A duplicate delivery of an already-started attempt is skipped: the poll keeps waiting until
// the long poll ends, and the started attempt's transfer record is gone from the store.
func TestPollActivityTaskSkipsDuplicateDelivery(t *testing.T) {
	rt := simrt.New(4, wftT0)
	defer rt.Close()
	st := memstore.New(memstore.WithRuntime(rt))
	s := New(rt, st, rt.Rand(), Config{NumShards: 2, LongPoll: 10 * time.Second})
	if err := s.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	var (
		first, second *api.ActivityTask
		err1, err2    error
		waited        time.Duration
		left, stored  int
	)
	rt.Go("test", func(ctx context.Context) {
		if _, err := s.StartWorkflow(ctx, startReq("wf", "r")); err != nil {
			panic(err)
		}
		s.Settle(ctx)
		wt, err := s.PollWorkflowTask(ctx, api.PollRequest{TaskQueue: "q"})
		if err != nil || wt == nil {
			panic(fmt.Sprintf("poll workflow task: %v, %v", wt, err))
		}
		if err := s.CompleteWorkflowTask(ctx, api.CompleteWorkflowTaskRequest{TaskToken: wt.TaskToken,
			Commands: []history.Command{testActivity()}}); err != nil {
			panic(err)
		}
		s.Settle(ctx)
		queued := drainQueue(s, matching.ActivityTasks, "q")
		if len(queued) != 1 {
			panic("want one queued activity task")
		}
		s.match.Add(matching.ActivityTasks, "q", queued[0])
		s.match.Add(matching.ActivityTasks, "q", queued[0])
		first, err1 = s.PollActivityTask(ctx, api.PollRequest{TaskQueue: "q"})
		begin := rt.Now()
		second, err2 = s.PollActivityTask(ctx, api.PollRequest{TaskQueue: "q"})
		waited = rt.Now().Sub(begin)
		left = s.match.Len(matching.ActivityTasks, "q")
		ts, err := st.ReadTransferTasks(ctx, queued[0].Shard, 1, 100)
		if err != nil {
			panic(err)
		}
		stored = len(ts)
	})
	if res := rt.Run(time.Time{}, nil); res != simrt.Idle {
		t.Fatalf("run ended with %v: %s", res, rt.Tasks())
	}
	if first == nil || err1 != nil || first.Attempt != 1 {
		t.Fatalf("first poll: %+v, %v", first, err1)
	}
	if second != nil || err2 != nil || waited != 10*time.Second {
		t.Fatalf("duplicate must be skipped: got %v, %v after %v", second, err2, waited)
	}
	if left != 0 || stored != 0 {
		t.Fatalf("queue has %d, store has %d transfer tasks; want 0 and 0", left, stored)
	}
}
