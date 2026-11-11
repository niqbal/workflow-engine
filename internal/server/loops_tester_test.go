package server

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/niqbal/workflow-engine/internal/history"
	"github.com/niqbal/workflow-engine/internal/server/api"
	"github.com/niqbal/workflow-engine/internal/simrt"
	"github.com/niqbal/workflow-engine/internal/store/memstore"
)

// Independent tests for WI-022. Every scenario is driven only through the service and the
// background loops: nothing here calls Settle.

type ltDrive func(ctx context.Context, rt *simrt.Runtime, s *Server) error

// ltScenario builds a server on rt, starts the loops, and runs drive in a task followed by
// Stop. The result is written to *res once the driver is done.
func ltScenario(cfg Config, drive ltDrive, res *error, done *bool) simrt.Scenario {
	return func(rt *simrt.Runtime) {
		*res, *done = nil, false
		s := New(rt, memstore.New(memstore.WithRuntime(rt)), rt.Rand(), cfg)
		ctx := context.Background()
		if err := s.Start(ctx); err != nil {
			panic(err)
		}
		if err := s.Run(ctx); err != nil {
			panic(err)
		}
		rt.Go("driver", func(ctx context.Context) {
			err := drive(ctx, rt, s)
			rt.Tracef("driver done: %v", err)
			if serr := s.Stop(ctx); serr != nil && err == nil {
				err = fmt.Errorf("stop: %w", serr)
			}
			*res, *done = err, true
		})
	}
}

// ltRun runs one scenario and checks that the driver finished and nothing is left running.
func ltRun(t *testing.T, seed uint64, cfg Config, drive ltDrive) {
	t.Helper()
	var res error
	var done bool
	rt := simrt.New(seed, simrt.Start)
	ltScenario(cfg, drive, &res, &done)(rt)
	r := rt.Run(simrt.Start.Add(72*time.Hour), nil)
	live := rt.Tasks()
	rt.Close()
	if !done {
		t.Fatalf("seed %d: driver did not finish (run ended %v); live tasks: %s", seed, r, live)
	}
	if res != nil {
		t.Fatalf("seed %d: %v", seed, res)
	}
	if r != simrt.Idle || live != "" {
		t.Fatalf("seed %d: after Stop run ended %v with live tasks %q", seed, r, live)
	}
}

func ltPollWF(ctx context.Context, s *Server, queue string) (*api.WorkflowTask, error) {
	task, err := s.PollWorkflowTask(ctx, api.PollRequest{TaskQueue: queue, Identity: "w"})
	if err == nil && task == nil {
		err = fmt.Errorf("poll %s: no workflow task", queue)
	}
	return task, err
}

func ltPollAct(ctx context.Context, s *Server, queue string) (*api.ActivityTask, error) {
	task, err := s.PollActivityTask(ctx, api.PollRequest{TaskQueue: queue, Identity: "aw"})
	if err == nil && task == nil {
		err = fmt.Errorf("poll %s: no activity task", queue)
	}
	return task, err
}

func ltHas(evs []history.Event, typ history.EventType) bool {
	for _, e := range evs {
		if e.Type() == typ {
			return true
		}
	}
	return false
}

// ltStartAndTimer starts id on queue and answers its first workflow task with a timer.
func ltStartAndTimer(ctx context.Context, s *Server, id, queue string, d time.Duration) error {
	if _, err := s.StartWorkflow(ctx, api.StartRequest{WorkflowID: id, WorkflowType: "W", TaskQueue: queue, RequestID: id}); err != nil {
		return err
	}
	wt, err := ltPollWF(ctx, s, queue)
	if err != nil {
		return err
	}
	return s.CompleteWorkflowTask(ctx, api.CompleteWorkflowTaskRequest{TaskToken: wt.TaskToken,
		Commands: []history.Command{history.StartTimer{TimerID: "t", Duration: d}}})
}

// A timer loop sleeping until a far FireAt must wake for a new, earlier timer on its shard.
func TestLoopsTesterEarlierTimerPreemptsSleepingTimerLoop(t *testing.T) {
	cfg := Config{NumShards: 2, LongPoll: 10 * time.Hour}
	for seed := uint64(1); seed <= 4; seed++ {
		ltRun(t, seed, cfg, func(ctx context.Context, rt *simrt.Runtime, s *Server) error {
			a := "far"
			b := ""
			for i := 0; i < 100 && b == ""; i++ {
				if c := fmt.Sprintf("near-%d", i); s.ShardFor(c) == s.ShardFor(a) {
					b = c
				}
			}
			if err := ltStartAndTimer(ctx, s, a, "qa", 5*time.Hour); err != nil {
				return err
			}
			rt.Sleep(time.Minute) // the timer loop is now asleep until the 5h timer
			if err := ltStartAndTimer(ctx, s, b, "qb", 10*time.Second); err != nil {
				return err
			}
			began := rt.Now()
			wt, err := ltPollWF(ctx, s, "qb")
			if err != nil {
				return err
			}
			if !ltHas(wt.History, history.EventTimerFired) {
				return fmt.Errorf("second task of %s has no TimerFired", b)
			}
			if took := rt.Now().Sub(began); took > 11*time.Second {
				return fmt.Errorf("10s timer fired after %v (timer loop not woken by the new timer)", took)
			}
			return s.TerminateWorkflow(ctx, api.TerminateRequest{WorkflowID: a})
		})
	}
}

// A pushed task lost from matching (restart) is pushed again at its redispatch time by the
// transfer loop, without a write to wake it.
func TestLoopsTesterRedispatchAfterDroppedQueue(t *testing.T) {
	cfg := Config{NumShards: 2, LongPoll: 10 * time.Hour, RedispatchInterval: 20 * time.Second}
	for seed := uint64(1); seed <= 4; seed++ {
		ltRun(t, seed, cfg, func(ctx context.Context, rt *simrt.Runtime, s *Server) error {
			if _, err := s.StartWorkflow(ctx, api.StartRequest{WorkflowID: "wf", WorkflowType: "W", TaskQueue: "q", RequestID: "r"}); err != nil {
				return err
			}
			rt.Sleep(time.Second)
			s.DropQueuedTasks()
			if _, err := ltPollWF(ctx, s, "q"); err != nil {
				return err
			}
			if at := rt.Now().Sub(simrt.Start); at < 20*time.Second || at > 21*time.Second {
				return fmt.Errorf("dropped task redelivered at +%v, want about +20s", at)
			}
			return s.TerminateWorkflow(ctx, api.TerminateRequest{WorkflowID: "wf"})
		})
	}
}

// A signal on an idle workflow is delivered at once: its write wakes the transfer loop.
func TestLoopsTesterSignalWakesTransferLoop(t *testing.T) {
	cfg := Config{NumShards: 2, LongPoll: 10 * time.Hour}
	ltRun(t, 7, cfg, func(ctx context.Context, rt *simrt.Runtime, s *Server) error {
		if _, err := s.StartWorkflow(ctx, api.StartRequest{WorkflowID: "wf", WorkflowType: "W", TaskQueue: "q", RequestID: "r"}); err != nil {
			return err
		}
		wt, err := ltPollWF(ctx, s, "q")
		if err != nil {
			return err
		}
		if err := s.CompleteWorkflowTask(ctx, api.CompleteWorkflowTaskRequest{TaskToken: wt.TaskToken}); err != nil {
			return err
		}
		rt.Sleep(time.Hour)
		if err := s.SignalWorkflow(ctx, api.SignalRequest{WorkflowID: "wf", SignalName: "go", RequestID: "s1"}); err != nil {
			return err
		}
		began := rt.Now()
		wt, err = ltPollWF(ctx, s, "q")
		if err != nil {
			return err
		}
		if !ltHas(wt.History, history.EventWorkflowExecutionSignaled) {
			return errors.New("task after signal has no WorkflowExecutionSignaled")
		}
		if took := rt.Now().Sub(began); took != 0 {
			return fmt.Errorf("signal's workflow task delivered after %v of virtual time", took)
		}
		return s.CompleteWorkflowTask(ctx, api.CompleteWorkflowTaskRequest{TaskToken: wt.TaskToken,
			Commands: []history.Command{history.CompleteWorkflow{}}})
	})
}

// activityRetryDrive: attempt 1 fails, attempt 2 times out (start-to-close), attempt 3
// completes. Retries and the timeout all need the timer loop.
func activityRetryDrive(ctx context.Context, rt *simrt.Runtime, s *Server) error {
	if _, err := s.StartWorkflow(ctx, api.StartRequest{WorkflowID: "wf", WorkflowType: "W", TaskQueue: "q", RequestID: "r"}); err != nil {
		return err
	}
	wt, err := ltPollWF(ctx, s, "q")
	if err != nil {
		return err
	}
	act := history.ScheduleActivity{ActivityID: "a", ActivityType: "A", StartToCloseTimeout: 10 * time.Second,
		RetryPolicy: history.RetryPolicy{InitialInterval: 5 * time.Second, BackoffCoefficient: 1, MaximumAttempts: 3}}
	if err := s.CompleteWorkflowTask(ctx, api.CompleteWorkflowTaskRequest{TaskToken: wt.TaskToken,
		Commands: []history.Command{act}}); err != nil {
		return err
	}
	at, err := ltPollAct(ctx, s, "q")
	if err != nil {
		return err
	}
	if at.Attempt != 1 {
		return fmt.Errorf("first attempt is %d", at.Attempt)
	}
	if err := s.FailActivityTask(ctx, api.FailActivityTaskRequest{TaskToken: at.TaskToken,
		Failure: history.Failure{Type: "Boom", Message: "x"}}); err != nil {
		return err
	}
	if at, err = ltPollAct(ctx, s, "q"); err != nil {
		return err
	}
	if at.Attempt != 2 {
		return fmt.Errorf("second attempt is %d", at.Attempt)
	}
	// Do not answer attempt 2: its start-to-close timer must time it out.
	if at, err = ltPollAct(ctx, s, "q"); err != nil {
		return err
	}
	if at.Attempt != 3 {
		return fmt.Errorf("third attempt is %d", at.Attempt)
	}
	if err := s.CompleteActivityTask(ctx, api.CompleteActivityTaskRequest{TaskToken: at.TaskToken, Result: json.RawMessage(`1`)}); err != nil {
		return err
	}
	if wt, err = ltPollWF(ctx, s, "q"); err != nil {
		return err
	}
	if !ltHas(wt.History, history.EventActivityTaskCompleted) {
		return errors.New("workflow task after activity has no ActivityTaskCompleted")
	}
	if err := s.CompleteWorkflowTask(ctx, api.CompleteWorkflowTaskRequest{TaskToken: wt.TaskToken,
		Commands: []history.Command{history.CompleteWorkflow{}}}); err != nil {
		return err
	}
	d, err := s.DescribeWorkflow(ctx, "wf")
	if err != nil {
		return err
	}
	if d.Status != history.StatusCompleted {
		return fmt.Errorf("status %s", d.Status)
	}
	return nil
}

func TestLoopsTesterActivityRetryAndTimeout(t *testing.T) {
	cfg := Config{NumShards: 2, LongPoll: 10 * time.Hour}
	for seed := uint64(1); seed <= 4; seed++ {
		ltRun(t, seed, cfg, activityRetryDrive)
	}
}

// An execution timeout fires through the timer loop with no worker at all.
func TestLoopsTesterExecutionTimeout(t *testing.T) {
	cfg := Config{NumShards: 2, LongPoll: 10 * time.Hour}
	ltRun(t, 5, cfg, func(ctx context.Context, rt *simrt.Runtime, s *Server) error {
		if _, err := s.StartWorkflow(ctx, api.StartRequest{WorkflowID: "wf", WorkflowType: "W", TaskQueue: "q",
			RequestID: "r", ExecutionTimeout: time.Minute}); err != nil {
			return err
		}
		rt.Sleep(2 * time.Minute)
		d, err := s.DescribeWorkflow(ctx, "wf")
		if err != nil {
			return err
		}
		if d.Status == history.StatusCompleted || d.Status.Open() {
			return fmt.Errorf("status %s after execution timeout", d.Status)
		}
		return nil
	})
}

// Run-twice check of a scenario that uses retries, timeouts and both loops.
func TestLoopsTesterActivityScenarioIsDeterministic(t *testing.T) {
	cfg := Config{NumShards: 3, LongPoll: 10 * time.Hour}
	var res error
	var done bool
	for seed := uint64(10); seed <= 13; seed++ {
		if err := simrt.CheckDeterministic(2, seed, 72*time.Hour, ltScenario(cfg, activityRetryDrive, &res, &done)); err != nil {
			t.Fatal(err)
		}
		if !done || res != nil {
			t.Fatalf("seed %d: done=%v err=%v", seed, done, res)
		}
	}
}

// On the real runtime, an activity retry (a timer) and a workflow task round trip complete
// with only the loops, and Stop leaves nothing running.
func TestLoopsTesterActivityRetryOnRealRuntime(t *testing.T) {
	s, rt := newRealServer(t)
	ctx := context.Background()
	if err := s.Run(ctx); err != nil {
		t.Fatal(err)
	}
	pctx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	err := func() error {
		if _, err := s.StartWorkflow(pctx, api.StartRequest{WorkflowID: "wf", WorkflowType: "W", TaskQueue: "q", RequestID: "r"}); err != nil {
			return err
		}
		wt, err := ltPollWF(pctx, s, "q")
		if err != nil {
			return err
		}
		act := history.ScheduleActivity{ActivityID: "a", ActivityType: "A", StartToCloseTimeout: time.Minute,
			RetryPolicy: history.RetryPolicy{InitialInterval: 20 * time.Millisecond, MaximumAttempts: 2}}
		if err := s.CompleteWorkflowTask(pctx, api.CompleteWorkflowTaskRequest{TaskToken: wt.TaskToken,
			Commands: []history.Command{act}}); err != nil {
			return err
		}
		at, err := ltPollAct(pctx, s, "q")
		if err != nil {
			return err
		}
		if err := s.FailActivityTask(pctx, api.FailActivityTaskRequest{TaskToken: at.TaskToken,
			Failure: history.Failure{Type: "Boom"}}); err != nil {
			return err
		}
		if at, err = ltPollAct(pctx, s, "q"); err != nil {
			return err
		}
		if at.Attempt != 2 {
			return fmt.Errorf("retry attempt %d", at.Attempt)
		}
		if err := s.CompleteActivityTask(pctx, api.CompleteActivityTaskRequest{TaskToken: at.TaskToken}); err != nil {
			return err
		}
		if wt, err = ltPollWF(pctx, s, "q"); err != nil {
			return err
		}
		return s.CompleteWorkflowTask(pctx, api.CompleteWorkflowTaskRequest{TaskToken: wt.TaskToken,
			Commands: []history.Command{history.CompleteWorkflow{}}})
	}()
	stopCtx, scancel := context.WithTimeout(ctx, 5*time.Second)
	defer scancel()
	began := time.Now()
	if serr := s.Stop(stopCtx); serr != nil {
		t.Fatalf("Stop: %v", serr)
	}
	if d := time.Since(began); d > time.Second {
		t.Fatalf("Stop took %v", d)
	}
	waitReal(t, rt)
	if err != nil {
		t.Fatal(err)
	}
}
