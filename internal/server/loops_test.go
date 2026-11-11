package server

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/niqbal/workflow-engine/internal/history"
	"github.com/niqbal/workflow-engine/internal/rnd"
	"github.com/niqbal/workflow-engine/internal/server/api"
	"github.com/niqbal/workflow-engine/internal/simrt"
	"github.com/niqbal/workflow-engine/internal/store/memstore"
	"github.com/niqbal/workflow-engine/internal/xsync"
)

// timerRoundTrip drives one workflow through the service only, never calling Settle: start,
// a workflow task that starts a timer, a second workflow task after the timer fires, and
// completion. The background loops must push the tasks and fire the timer.
func timerRoundTrip(ctx context.Context, s *Server, id, queue string, timer time.Duration) error {
	if _, err := s.StartWorkflow(ctx, api.StartRequest{WorkflowID: id, WorkflowType: "W", TaskQueue: queue, RequestID: id}); err != nil {
		return fmt.Errorf("%s: start: %w", id, err)
	}
	poll := func() (*api.WorkflowTask, error) {
		task, err := s.PollWorkflowTask(ctx, api.PollRequest{TaskQueue: queue, Identity: "w"})
		if err == nil && task == nil {
			err = errors.New("poll returned no task")
		}
		if err != nil {
			return nil, fmt.Errorf("%s: %w", id, err)
		}
		return task, nil
	}
	task, err := poll()
	if err != nil {
		return err
	}
	if err := s.CompleteWorkflowTask(ctx, api.CompleteWorkflowTaskRequest{TaskToken: task.TaskToken,
		Commands: []history.Command{history.StartTimer{TimerID: "t", Duration: timer}}}); err != nil {
		return fmt.Errorf("%s: complete 1: %w", id, err)
	}
	if task, err = poll(); err != nil {
		return err
	}
	fired := false
	for _, e := range task.History {
		fired = fired || e.Type() == history.EventTimerFired
	}
	if !fired {
		return fmt.Errorf("%s: second workflow task has no TimerFired: %v", id, eventTypes(task.History))
	}
	if err := s.CompleteWorkflowTask(ctx, api.CompleteWorkflowTaskRequest{TaskToken: task.TaskToken,
		Commands: []history.Command{history.CompleteWorkflow{}}}); err != nil {
		return fmt.Errorf("%s: complete 2: %w", id, err)
	}
	d, err := s.DescribeWorkflow(ctx, id)
	if err != nil {
		return fmt.Errorf("%s: describe: %w", id, err)
	}
	if d.Status != history.StatusCompleted {
		return fmt.Errorf("%s: status %s, want %s", id, d.Status, history.StatusCompleted)
	}
	return nil
}

// waitReal fails the test if a task started on rt is still running after a few seconds.
func waitReal(t *testing.T, rt *xsync.Real) {
	t.Helper()
	done := make(chan struct{})
	go func() {
		rt.Wait()
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("background tasks still running")
	}
}

func newRealServer(t *testing.T) (*Server, *xsync.Real) {
	t.Helper()
	rt := xsync.NewReal(context.Background())
	s := New(rt, memstore.New(memstore.WithRuntime(rt)), rnd.New(11), Config{NumShards: 2, LongPoll: 10 * time.Second})
	if err := s.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	return s, rt
}

// On the production runtime and wall clock, the loops alone carry a workflow through two
// workflow tasks and a timer; Stop then returns promptly and no task is left running.
func TestLoopsRoundTripOnRealRuntime(t *testing.T) {
	s, rt := newRealServer(t)
	ctx := context.Background()
	if err := s.Run(ctx); err != nil {
		t.Fatal(err)
	}
	if err := s.Run(ctx); !errors.Is(err, errLoopsRunning) {
		t.Fatalf("second Run: got %v, want errLoopsRunning", err)
	}
	stopCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()

	err := timerRoundTrip(ctx, s, "wf", "q", 50*time.Millisecond)
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
	// Stopped loops can be started again.
	if err := s.Run(ctx); err != nil {
		t.Fatalf("Run after Stop: %v", err)
	}
	if err := s.Stop(stopCtx); err != nil {
		t.Fatal(err)
	}
	waitReal(t, rt)
}

// Canceling Run's context ends the loops too.
func TestLoopsEndWhenRunContextIsCanceled(t *testing.T) {
	s, rt := newRealServer(t)
	ctx, cancel := context.WithCancel(context.Background())
	if err := s.Run(ctx); err != nil {
		t.Fatal(err)
	}
	cancel()
	waitReal(t, rt)
	if err := s.Stop(context.Background()); err != nil {
		t.Fatal(err)
	}
}

// Stop without Run is a no-op.
func TestStopWithoutRun(t *testing.T) {
	s, _ := newRealServer(t)
	if err := s.Stop(context.Background()); err != nil {
		t.Fatal(err)
	}
}

// loopOutcome is what one simulated run of loopScenario observed.
type loopOutcome struct {
	errs     []error
	stopAt   time.Time
	stopTook time.Duration
}

// loopScenario runs two workflows on different shards and task queues concurrently, driven
// only by the background loops, and stops the loops when both are done.
func loopScenario(out *loopOutcome) simrt.Scenario {
	return func(rt *simrt.Runtime) {
		*out = loopOutcome{}
		s := New(rt, memstore.New(memstore.WithRuntime(rt)), rt.Rand(), Config{NumShards: 2, LongPoll: time.Minute})
		ctx := context.Background()
		if err := s.Start(ctx); err != nil {
			panic(err)
		}
		if err := s.Run(ctx); err != nil {
			panic(err)
		}
		ids := []string{"a", "c"}
		if s.ShardFor(ids[0]) == s.ShardFor(ids[1]) {
			ids[1] = "b"
		}
		left := len(ids)
		for i, id := range ids {
			rt.Go("driver-"+id, func(ctx context.Context) {
				err := timerRoundTrip(ctx, s, id, "q-"+id, time.Duration(10+15*i)*time.Second)
				rt.Tracef("%s done: %v", id, err)
				if err != nil {
					out.errs = append(out.errs, err)
				}
				if left--; left > 0 {
					return
				}
				began := rt.Now()
				if err := s.Stop(ctx); err != nil {
					out.errs = append(out.errs, err)
				}
				out.stopAt, out.stopTook = rt.Now(), rt.Now().Sub(began)
				rt.Tracef("stopped")
			})
		}
	}
}

// Under simrt the loops' trace is identical across runs with the same seed.
func TestLoopsRunTwiceTraceIsIdentical(t *testing.T) {
	var out loopOutcome
	for seed := uint64(1); seed <= 5; seed++ {
		if err := simrt.CheckDeterministic(2, seed, 24*time.Hour, loopScenario(&out)); err != nil {
			t.Fatal(err)
		}
	}
}

// Under simrt the loops complete both workflows, Stop takes no virtual time, and afterwards no
// task is left and nothing is scheduled (the runtime is idle at the moment of Stop).
func TestLoopsUnderSimulationStopLeavesNothingRunning(t *testing.T) {
	for seed := uint64(1); seed <= 5; seed++ {
		rt := simrt.New(seed, simrt.Start)
		var out loopOutcome
		loopScenario(&out)(rt)
		res := rt.Run(simrt.Start.Add(24*time.Hour), nil)
		trace := rt.Trace()
		live := rt.Tasks()
		now := rt.Now()
		rt.Close()
		if res != simrt.Idle {
			t.Fatalf("seed %d: run ended with %v; live tasks: %s", seed, res, live)
		}
		if live != "" {
			t.Fatalf("seed %d: tasks left after Stop: %s", seed, live)
		}
		if len(out.errs) > 0 {
			t.Fatalf("seed %d: %v", seed, out.errs)
		}
		if out.stopAt.IsZero() || out.stopTook != 0 || !now.Equal(out.stopAt) {
			t.Fatalf("seed %d: stop at %v took %v, idle at %v", seed, out.stopAt, out.stopTook, now)
		}
		for _, name := range []string{"transfer-0", "transfer-1", "timers-0", "timers-1"} {
			if !traceHas(trace, " run "+name+"#") {
				t.Fatalf("seed %d: loop %s never ran", seed, name)
			}
		}
	}
}

// With nothing to do the loops wait without a deadline (no busy waiting: the simulation goes
// idle at once), and Stop still ends them.
func TestLoopsIdleWithoutWorkAndStop(t *testing.T) {
	rt := simrt.New(3, simrt.Start)
	defer rt.Close()
	s := New(rt, memstore.New(memstore.WithRuntime(rt)), rt.Rand(), Config{NumShards: 2})
	ctx := context.Background()
	if err := s.Start(ctx); err != nil {
		t.Fatal(err)
	}
	if err := s.Run(ctx); err != nil {
		t.Fatal(err)
	}
	if res := rt.Run(time.Time{}, nil); res != simrt.Idle || !rt.Now().Equal(simrt.Start) {
		t.Fatalf("run ended with %v at %v", res, rt.Now())
	}
	if rt.Tasks() == "" {
		t.Fatal("loops are not waiting")
	}
	steps := len(rt.Trace())
	if rt.Run(time.Time{}, nil); len(rt.Trace()) != steps {
		t.Fatalf("idle loops kept running: %v", rt.Trace()[steps:])
	}
	var stopErr error
	stopped := false
	rt.Go("stopper", func(ctx context.Context) {
		stopErr, stopped = s.Stop(ctx), true
	})
	if res := rt.Run(time.Time{}, nil); res != simrt.Idle {
		t.Fatalf("run ended with %v", res)
	}
	if !stopped || stopErr != nil || rt.Tasks() != "" || !rt.Now().Equal(simrt.Start) {
		t.Fatalf("stopped=%v err=%v live=%q at %v", stopped, stopErr, rt.Tasks(), rt.Now())
	}
}

func traceHas(trace []string, sub string) bool {
	for _, l := range trace {
		if strings.Contains(l, sub) {
			return true
		}
	}
	return false
}
