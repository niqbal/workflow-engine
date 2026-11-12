package server

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/niqbal/workflow-engine/internal/history"
	"github.com/niqbal/workflow-engine/internal/rnd"
	"github.com/niqbal/workflow-engine/internal/server/api"
	"github.com/niqbal/workflow-engine/internal/server/matching"
	"github.com/niqbal/workflow-engine/internal/simrt"
	"github.com/niqbal/workflow-engine/internal/store"
	"github.com/niqbal/workflow-engine/internal/store/memstore"
)

// WI-032 under the deterministic runtime. The tests in recovery_test.go run the old and the
// new incarnation one after the other, so the old one only ever writes with a fence that is
// already stale. Here both run at once, interleaved at every store yield point: the old
// node's processors keep scanning while the new node acquires the shards, so a timer fire or
// workflow task write of the old node can be in flight across the takeover. Optional store
// faults (aborted and ambiguous commits, commit delays) hit the first scans after restart.

// restartHorizon is how long the restarted node runs; every run completes well within it.
const restartHorizon = 10 * time.Minute

var errInjected = errors.New("injected store fault")

// zombieStore is the old incarnation's view of the shared store. It counts the writes the
// old node attempts once the new node owns every shard.
type zombieStore struct {
	store.Store
	dead      *bool
	rejected  int // writes that failed with ErrFenced
	committed int // writes started after the takeover that took effect: must stay 0
}

func (z *zombieStore) note(deadAtEntry bool, err error) {
	switch {
	case errors.Is(err, store.ErrFenced):
		z.rejected++
	case err == nil && deadAtEntry:
		z.committed++
	}
}

func (z *zombieStore) CreateRun(ctx context.Context, f store.Fence, r store.NewRun) ([]store.Task, error) {
	d := *z.dead
	ts, err := z.Store.CreateRun(ctx, f, r)
	z.note(d, err)
	return ts, err
}

func (z *zombieStore) UpdateRun(ctx context.Context, f store.Fence, key store.RunKey, u store.Update) (int64, []store.Task, error) {
	d := *z.dead
	v, ts, err := z.Store.UpdateRun(ctx, f, key, u)
	z.note(d, err)
	return v, ts, err
}

func (z *zombieStore) DeleteTasks(ctx context.Context, f store.Fence, keys []store.TaskKey) error {
	d := *z.dead
	err := z.Store.DeleteTasks(ctx, f, keys)
	z.note(d, err)
	return err
}

// restartSim is one crash-and-restart scenario over a shared store.
type restartSim struct {
	crashAt time.Duration // virtual time after start at which the new node takes over
	faults  bool
	// sameNode restarts the old Server in place (its queues dropped, shards acquired again)
	// instead of a new one, with redispatch so slow that only shard load can re-push tasks.
	sameNode bool

	t0     time.Time
	st     *memstore.Store
	zombie *zombieStore
	old    *Server
	node   *Server
	errs   []error

	openAtCrash  int // runs still open when the new node took over
	firedAtCrash int // TimerFired events already written when it took over
}

// simWorker polls s's queues without waiting and runs each task it starts: it decides the
// workflow task or "executes" the activity for 300ms, then reports. Once stop is true it
// exits, dying with whatever task it holds. Errors are expected under faults and ignored:
// the invariants are checked on the store afterwards.
func simWorker(rt *simrt.Runtime, s *Server, stop func() bool) {
	ctx := context.Background()
	for !stop() {
		if tk, ok := s.match.Poll(matching.WorkflowTasks, "q", time.Time{}); ok {
			wt, err := s.startWorkflowTask(ctx, tk, "w")
			if err != nil {
				continue
			}
			rt.Sleep(300 * time.Millisecond)
			if stop() {
				return
			}
			var i int
			if _, err := fmt.Sscanf(wt.WorkflowID, "wf-%d", &i); err != nil {
				panic(err)
			}
			_ = s.CompleteWorkflowTask(ctx, api.CompleteWorkflowTaskRequest{TaskToken: wt.TaskToken,
				Commands: recoveryDecide(wt.History, recoveryTimer(i))})
			continue
		}
		if tk, ok := s.match.Poll(matching.ActivityTasks, "q", time.Time{}); ok {
			at, err := s.startActivityTask(ctx, tk, "w")
			if err != nil {
				continue
			}
			rt.Sleep(300 * time.Millisecond)
			if stop() {
				return
			}
			_ = s.CompleteActivityTask(ctx, api.CompleteActivityTaskRequest{TaskToken: at.TaskToken,
				Result: []byte(`"ok"`)})
			continue
		}
		rt.Sleep(time.Second)
	}
}

// progress counts open runs and written TimerFired events, reading through s.
func progress(ctx context.Context, s *Server) (open, fired int, err error) {
	for i := 0; i < recoveryRuns; i++ {
		id := fmt.Sprintf("wf-%d", i)
		d, err := s.DescribeWorkflow(ctx, id)
		if err != nil {
			return 0, 0, err
		}
		if d.Status.Open() {
			open++
		}
		evs, err := s.GetHistory(ctx, id)
		if err != nil {
			return 0, 0, err
		}
		for _, e := range evs {
			if e.Type() == history.EventTimerFired {
				fired++
			}
		}
	}
	return open, fired, nil
}

// scenario creates the tasks on rt: the old node with its worker and processors, and the new
// node that takes over at crashAt while the old processors keep running for two more minutes.
func (c *restartSim) scenario(rt *simrt.Runtime) {
	c.t0 = rt.Now()
	opts := []memstore.Option{memstore.WithRuntime(rt)}
	if c.faults {
		var before, after int
		opts = append(opts, memstore.WithFaults(memstore.Faults{
			BeforeCommit: func(op string) error {
				if op != "UpdateRun" && op != "DeleteTasks" {
					return nil
				}
				before++
				if before%7 == 0 {
					return errInjected
				}
				return nil
			},
			AfterCommit: func(op string) error {
				if op != "UpdateRun" {
					return nil
				}
				after++
				if after%5 == 0 {
					return errInjected
				}
				return nil
			},
			CommitDelay: func(op string) time.Duration {
				if op == "UpdateRun" {
					return 150 * time.Millisecond
				}
				return 0
			},
		}))
	}
	c.st = memstore.New(opts...)
	dead, crashed := false, false
	c.zombie = &zombieStore{Store: c.st, dead: &dead}
	cfg := Config{NumShards: 2}
	if c.sameNode {
		cfg.RedispatchInterval = time.Hour
	}
	c.old = New(rt, c.zombie, rnd.New(1), cfg)
	c.node = New(rt, c.st, rnd.New(2), cfg)
	if c.sameNode {
		c.node = c.old
	}
	fail := func(err error) {
		if err != nil {
			c.errs = append(c.errs, err)
		}
	}
	end := c.t0.Add(restartHorizon)

	rt.Go("old-start", func(ctx context.Context) {
		if err := c.old.Start(ctx); err != nil {
			fail(err)
			return
		}
		for i := 0; i < recoveryRuns; i++ {
			_, err := c.old.StartWorkflow(ctx, startReq(fmt.Sprintf("wf-%d", i), "r"))
			fail(err)
		}
		rt.Go("old-worker", func(context.Context) {
			simWorker(rt, c.old, func() bool { return crashed })
		})
		rt.Go("old-processor", func(ctx context.Context) {
			// The old node's processors never learn they lost the shards: a zombie.
			for rt.Now().Before(c.t0.Add(c.crashAt + 2*time.Minute)) {
				c.old.Settle(ctx)
				rt.Sleep(200 * time.Millisecond)
			}
		})
	})
	rt.Go("restart", func(ctx context.Context) {
		rt.Sleep(c.crashAt)
		open, fired, err := progress(ctx, c.old)
		fail(err)
		c.openAtCrash, c.firedAtCrash = open, fired
		crashed = true
		if c.sameNode {
			c.node.DropQueuedTasks()
		}
		if err := c.node.Start(ctx); err != nil {
			fail(err)
			return
		}
		dead = !c.sameNode // in place, the old Server is the owner and its writes are legitimate
		rt.Go("new-worker", func(context.Context) {
			simWorker(rt, c.node, func() bool { return !rt.Now().Before(end) })
		})
		for rt.Now().Before(end) {
			c.node.Settle(ctx)
			rt.Sleep(time.Second)
		}
	})
}

// check asserts the WI-032 invariants after rt has run the scenario to idle.
func (c *restartSim) check(t *testing.T, label string) {
	t.Helper()
	for _, err := range c.errs {
		t.Fatalf("%s: %v", label, err)
	}
	if c.zombie.committed != 0 {
		t.Fatalf("%s: the fenced old node committed %d writes after the takeover", label, c.zombie.committed)
	}
	for i := 0; i < recoveryRuns; i++ {
		id := fmt.Sprintf("wf-%d", i)
		d, err := c.node.DescribeWorkflow(context.Background(), id)
		if err != nil || d.Status != history.StatusCompleted {
			t.Fatalf("%s: %s: %+v, %v; want completed: %v", label, id, d, err, historyTypes(t, c.node, id))
		}
		evs, err := c.node.GetHistory(context.Background(), id)
		if err != nil {
			t.Fatal(err)
		}
		var started, fired []history.Event
		for _, e := range evs {
			switch e.Type() {
			case history.EventTimerStarted:
				started = append(started, e)
			case history.EventTimerFired:
				fired = append(fired, e)
			}
		}
		if len(started) != 1 || len(fired) != 1 {
			t.Fatalf("%s: %s has %d TimerStarted and %d TimerFired, want 1 and 1: %v",
				label, id, len(started), len(fired), eventTypes(evs))
		}
		if f := fired[0].Attrs.(history.TimerFired); f.StartedEventID != started[0].ID {
			t.Fatalf("%s: %s fired timer of event %d, started %d", label, id, f.StartedEventID, started[0].ID)
		}
		if got := fired[0].Time.Sub(started[0].Time); got < recoveryTimer(i) {
			t.Fatalf("%s: %s timer fired early, %v after it started", label, id, got)
		}
	}
	// Every timer task due while the new node was running has been processed.
	if left := dueTimers(t, c.node, c.st, c.t0.Add(restartHorizon-time.Minute)); len(left) != 0 {
		t.Fatalf("%s: %d due timer tasks left unprocessed", label, len(left))
	}
}

// runRestartSim runs one seed of the scenario to idle and checks it.
func runRestartSim(t *testing.T, seed uint64, c *restartSim) {
	t.Helper()
	rt := simrt.New(seed, transferT0)
	defer rt.Close()
	c.scenario(rt)
	if res := rt.Run(time.Time{}, nil); res != simrt.Idle {
		t.Fatalf("seed %d: run ended with %v, tasks left: %s", seed, res, rt.Tasks())
	}
	c.check(t, fmt.Sprintf("seed %d, crash at %v", seed, c.crashAt))
}

// restartCrashAt spreads the crash points over the first 100s, where the four timers (20s to
// 80s) are started, fire, and their activities run.
func restartCrashAt(seed uint64) time.Duration {
	return time.Duration(seed*379%1000) * 100 * time.Millisecond
}

// The old node keeps running its processors concurrently with the restarted one, interleaved
// by the seeded scheduler. Every run completes on the new node with exactly one TimerFired,
// on time, and nothing the old node starts after the takeover commits.
func TestRestartSimZombieInterleavedNoDuplicateTimerFired(t *testing.T) {
	rejected, midRun, timersPending := 0, 0, 0
	for seed := uint64(1); seed <= 24; seed++ {
		c := &restartSim{crashAt: restartCrashAt(seed)}
		runRestartSim(t, seed, c)
		rejected += c.zombie.rejected
		if c.openAtCrash > 0 {
			midRun++
		}
		if c.firedAtCrash < recoveryRuns {
			timersPending++
		}
	}
	// The seeds must actually exercise what is claimed, or the test proves nothing.
	if midRun < 20 || timersPending < 12 {
		t.Fatalf("only %d crashes mid-run, %d with timers pending", midRun, timersPending)
	}
	if rejected == 0 {
		t.Fatal("the old node never attempted a write after the takeover: the race was not exercised")
	}
}

// The same with a faulty store: aborted writes, ambiguous commits (the write took effect but
// the caller sees an error) and commit delays that keep the old node's writes in flight across
// the takeover. Recovery still completes every run with exactly one TimerFired.
func TestRestartSimStoreFaultsNoDuplicateTimerFired(t *testing.T) {
	rejected := 0
	for seed := uint64(1); seed <= 16; seed++ {
		c := &restartSim{crashAt: restartCrashAt(seed), faults: true}
		runRestartSim(t, seed, c)
		rejected += c.zombie.rejected
	}
	if rejected == 0 {
		t.Fatal("the old node never attempted a write after the takeover: the race was not exercised")
	}
}

// The node restarts in place: its matching queues are lost and it acquires its shards again,
// with a redispatch interval far past the horizon. Only the transfer reload on shard load can
// re-push the tasks that were queued at the crash, so every run completing proves it ran.
func TestRestartSimSameNodeReloadsOnShardLoad(t *testing.T) {
	for seed := uint64(1); seed <= 16; seed++ {
		runRestartSim(t, seed, &restartSim{crashAt: restartCrashAt(seed), sameNode: true})
	}
}

// A restart scenario replays exactly from its seed, so any failure above can be reproduced.
func TestRestartSimDeterministic(t *testing.T) {
	for seed := uint64(1); seed <= 3; seed++ {
		sc := func(rt *simrt.Runtime) {
			(&restartSim{crashAt: restartCrashAt(seed), faults: true}).scenario(rt)
		}
		if err := simrt.CheckDeterministic(2, seed, 0, sc); err != nil {
			t.Fatal(err)
		}
	}
}
