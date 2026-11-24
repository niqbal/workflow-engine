package sim_test

// WI-041: the oracle. Two kinds of test here.
//
// The positive tests run a real cluster and assert the record is populated with what the
// invariants need (a ghost log with grants and fenced writes, history snapshots from served
// workflow tasks, a ledger whose keys were retried) and that every check passes.
//
// The negative tests are one per invariant. I1 and I3 are broken for real, by workflow code
// that never closes and by code whose replay diverges from what the history recorded. The
// rest are planted: the engine does not offer a way to rewrite history, fire a timer twice,
// commit under a stale fence or lose a signal from outside it — that is the point of the
// design — so those checks are proved against a record holding exactly the corruption such
// a bug would leave behind. Each test asserts the check names the invariant it caught.

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/niqbal/workflow-engine/activity"
	"github.com/niqbal/workflow-engine/internal/history"
	"github.com/niqbal/workflow-engine/internal/server/api"
	"github.com/niqbal/workflow-engine/internal/simrt"
	"github.com/niqbal/workflow-engine/internal/store"
	"github.com/niqbal/workflow-engine/internal/store/memstore"
	"github.com/niqbal/workflow-engine/sim"
	"github.com/niqbal/workflow-engine/wf"
)

const minute = time.Minute

// ---- positive: a correct run satisfies every invariant ---------------------------------

// retryWorkload runs, per workflow: a flaky activity that fails twice before applying its
// effect, a timer, a signal wait, and a result the reference model can derive from history.
// It exercises I1-I8 together on a three-node cluster.
func retryWorkload(led *sim.Ledger) sim.Workload {
	opts := wf.ActivityOptions{StartToCloseTimeout: minute}
	return sim.Workload{
		Nodes:      3,
		Shards:     4,
		Workers:    2,
		QuietLimit: 10 * 24 * time.Hour,
		Ledger:     led,
		Register: func(r sim.Registry) error {
			if err := r.RegisterWorkflow("refund", func(ctx wf.Context, amount int) (string, error) {
				var applied bool
				if err := wf.ExecuteActivity(ctx, "Pay", amount, opts).Get(ctx, &applied); err != nil {
					return "", err
				}
				if err := wf.Sleep(ctx, time.Hour); err != nil {
					return "", err
				}
				var decision string
				wf.GetSignalChannel(ctx, "approval").Receive(ctx, &decision)
				return decision, nil
			}); err != nil {
				return err
			}
			// Pay fails its first two attempts, then applies the effect once per key. The
			// key is stable across attempts (G4), so the ledger must end with one effect.
			return r.RegisterActivity("Pay", func(ctx context.Context, amount int) (bool, error) {
				info := activity.GetInfo(ctx)
				if info.Attempt < 3 {
					led.Attempt(info.IdempotencyKey)
					return false, fmt.Errorf("sink unavailable (attempt %d)", info.Attempt)
				}
				return led.Apply(info.IdempotencyKey, nil), nil
			})
		},
		Run: func(ctx context.Context, c *sim.Cluster) error {
			ids := []string{"r-1", "r-2", "r-3"}
			for i, id := range ids {
				if _, err := c.StartWorkflow(ctx, api.StartRequest{WorkflowID: id, WorkflowType: "refund",
					TaskQueue: "q", Input: json.RawMessage(fmt.Sprintf("%d", (i+1)*10)), RequestID: id}); err != nil {
					return err
				}
			}
			// Past the timer, then one signal each. Reading the history on the way is an
			// observation I2 will hold against the end state.
			c.Sleep(2 * time.Hour)
			for _, id := range ids {
				if _, err := c.GetHistory(ctx, id); err != nil {
					return err
				}
				if err := c.SignalWorkflow(ctx, api.SignalRequest{WorkflowID: id, SignalName: "approval",
					Payload: json.RawMessage(`"approved"`), RequestID: "sig-" + id}); err != nil {
					return err
				}
				// A retry of the same signal must be deduplicated, not appear twice (G7).
				if err := c.SignalWorkflow(ctx, api.SignalRequest{WorkflowID: id, SignalName: "approval",
					Payload: json.RawMessage(`"approved"`), RequestID: "sig-" + id}); err != nil {
					return err
				}
			}
			return nil
		},
		// I8: the result must be the payload of the first approval signal in the history.
		Reference: func(id string, evs []history.Event) (json.RawMessage, error) {
			for _, e := range evs {
				if a, ok := e.Attrs.(history.WorkflowExecutionSignaled); ok && a.SignalName == "approval" {
					return a.Payload, nil
				}
			}
			return nil, fmt.Errorf("no approval signal in history")
		},
	}
}

func TestWI041CorrectRunPassesEveryInvariant(t *testing.T) {
	led := sim.NewLedger()
	res := sim.Run(11, retryWorkload(led), nil)
	if res.Err != nil {
		t.Fatalf("run: %v", res.Err)
	}
	if res.Oracle != nil {
		t.Fatalf("oracle: %v", res.Oracle)
	}
	rec := res.Record
	if rec == nil {
		t.Fatal("no record")
	}
	if len(rec.Starts) != 3 || len(rec.Finals) != 3 {
		t.Fatalf("starts %d, finals %d, want 3 and 3", len(rec.Starts), len(rec.Finals))
	}
	// Six signals sent, three of them retries that must have been deduplicated.
	if len(rec.Signals) != 6 {
		t.Fatalf("signals recorded %d, want 6", len(rec.Signals))
	}
	for id, f := range rec.Finals {
		if f.Status != history.StatusCompleted {
			t.Fatalf("%s: status %s", id, f.Status)
		}
		if n := countType(f.Events, history.EventWorkflowExecutionSignaled); n != 1 {
			t.Fatalf("%s: %d signal events, want 1 after dedup", id, n)
		}
		if n := countType(f.Events, history.EventTimerFired); n != 1 {
			t.Fatalf("%s: %d TimerFired events, want 1", id, n)
		}
	}
	// The ledger proves the retries happened and that the effect still landed once.
	if led.Len() != 3 {
		t.Fatalf("ledger keys %d, want 3", led.Len())
	}
	for _, key := range led.Keys() {
		if got := led.Effects(key); got != 1 {
			t.Fatalf("%s: %d effects, want 1", key, got)
		}
		if got := led.Attempts(key); got != 3 {
			t.Fatalf("%s: %d attempts, want 3 (two failures then the effect)", key, got)
		}
	}
	// The ghost log holds the shard grants and the fenced writes that followed them.
	var grants, writes int
	for _, e := range rec.Commits {
		if e.Granted {
			grants++
		} else {
			writes++
		}
	}
	if grants < 4 {
		t.Fatalf("ghost log has %d shard grants, want at least one per shard (4)", grants)
	}
	if writes < 10 {
		t.Fatalf("ghost log has %d fenced writes, want many", writes)
	}
	// Workflow tasks served to workers are recorded as observations, not just reads.
	var fromTasks int
	for _, s := range rec.Snapshots {
		if strings.HasPrefix(s.Source, "task:") {
			fromTasks++
		}
	}
	if fromTasks < 9 {
		t.Fatalf("snapshots from served workflow tasks %d, want at least 3 per workflow", fromTasks)
	}
}

func TestWI041SameSeedSameOracleVerdict(t *testing.T) {
	for _, seed := range []uint64{1, 2, 3, 7, 11} {
		a := sim.Run(seed, retryWorkload(sim.NewLedger()), nil)
		b := sim.Run(seed, retryWorkload(sim.NewLedger()), nil)
		if a.Err != nil || b.Err != nil {
			t.Fatalf("seed %d: %v / %v", seed, a.Err, b.Err)
		}
		if len(a.Record.Commits) != len(b.Record.Commits) {
			t.Fatalf("seed %d: ghost log length %d vs %d", seed, len(a.Record.Commits), len(b.Record.Commits))
		}
		if len(a.Record.Snapshots) != len(b.Record.Snapshots) {
			t.Fatalf("seed %d: snapshots %d vs %d", seed, len(a.Record.Snapshots), len(b.Record.Snapshots))
		}
	}
}

// ---- I1: a run that never closes -------------------------------------------------------

func TestWI041I1CatchesRunThatNeverCloses(t *testing.T) {
	res := sim.Run(3, sim.Workload{
		QuietLimit: time.Hour,
		Register: func(r sim.Registry) error {
			return r.RegisterWorkflow("wait", func(ctx wf.Context) error {
				wf.GetSignalChannel(ctx, "never").Receive(ctx, nil)
				return nil
			})
		},
		Run: func(ctx context.Context, c *sim.Cluster) error {
			_, err := c.StartWorkflow(ctx, api.StartRequest{WorkflowID: "stuck",
				WorkflowType: "wait", TaskQueue: "q", RequestID: "stuck"})
			return err
		},
	}, nil)
	requireOracleCaught(t, res.Oracle, "I1", "quiet phase ran out of virtual time", "stuck")
}

// ---- I3: code whose replay diverges from the recorded history ---------------------------

// divergent returns a workflow that runs one activity for its first n replays and two
// afterwards. The live run only ever replays it n times, so the cluster records a correct
// history; the oracle's own replay is the one that diverges, which is exactly the shape of
// "workflow code changed while an execution was in flight" (SPEC §7.4) done wrong.
func divergent(n *int) func(ctx wf.Context) error {
	opts := wf.ActivityOptions{StartToCloseTimeout: minute}
	return func(ctx wf.Context) error {
		*n++
		if err := wf.ExecuteActivity(ctx, "A", 1, opts).Get(ctx, nil); err != nil {
			return err
		}
		if *n > 3 {
			if err := wf.ExecuteActivity(ctx, "B", 2, opts).Get(ctx, nil); err != nil {
				return err
			}
		}
		return nil
	}
}

func TestWI041I3CatchesReplayDivergence(t *testing.T) {
	replays := 0
	res := sim.Run(5, sim.Workload{
		QuietLimit: 24 * time.Hour,
		Register: func(r sim.Registry) error {
			if err := r.RegisterWorkflow("drift", divergent(&replays)); err != nil {
				return err
			}
			if err := r.RegisterActivity("A", func(int) error { return nil }); err != nil {
				return err
			}
			return r.RegisterActivity("B", func(int) error { return nil })
		},
		Run: func(ctx context.Context, c *sim.Cluster) error {
			_, err := c.StartWorkflow(ctx, api.StartRequest{WorkflowID: "d",
				WorkflowType: "drift", TaskQueue: "q", RequestID: "d"})
			return err
		},
	}, nil)
	if replays < 2 {
		t.Fatalf("the workflow replayed %d times; the test needs the live run to replay it", replays)
	}
	requireOracleCaught(t, res.Oracle, "I3", "d")
}

// ---- I2, I4, I5, I6, I7, I8: planted corruption ----------------------------------------

func TestWI041I2CatchesRewrittenHistory(t *testing.T) {
	rec := recordOf(t)
	id := "r-1"
	f := rec.Finals[id]
	// An observer saw an event that the final history no longer has in that position: a
	// rewrite, which G2 forbids.
	seen := cloneEvents(f.Events[:2])
	seen[1].Attrs = history.WorkflowTaskScheduled{TaskQueue: "other", Attempt: 7}
	rec.Snapshots = append(rec.Snapshots, sim.Snapshot{WorkflowID: id, RunID: f.RunID,
		Source: "test", At: simrt.Start, Events: seen})
	requireOracleCaught(t, (sim.Invariants{}).Check(rec), "I2", "not the final one")

	// And a snapshot longer than the end state: history that was truncated.
	rec2 := recordOf(t)
	f2 := rec2.Finals[id]
	long := append(cloneEvents(f2.Events), history.Event{ID: int64(len(f2.Events) + 1),
		Time: simrt.Start, Attrs: history.WorkflowExecutionTimedOut{}})
	rec2.Snapshots = append(rec2.Snapshots, sim.Snapshot{WorkflowID: id, RunID: f2.RunID,
		Source: "test", At: simrt.Start, Events: long})
	requireOracleCaught(t, (sim.Invariants{}).Check(rec2), "I2", "longer than the final history")
}

func TestWI041I4CatchesTimerFiredTwiceAndEarly(t *testing.T) {
	id := "r-1"

	// The same timer fires a second time.
	twice := recordOf(t)
	tf := twice.Finals[id]
	at, fired := timerEvents(t, tf.Events)
	dup := cloneEvents(tf.Events)
	last := dup[len(dup)-1]
	dup = append(dup, history.Event{ID: last.ID + 1, Time: last.Time,
		Attrs: history.TimerFired{TimerID: fired.TimerID, StartedEventID: at}})
	tf.Events = dup
	twice.Finals[id] = tf
	requireOracleCaught(t, (sim.Invariants{}).Check(twice), "I4", "fired twice")

	// The timer fires a second before its fireAt, with no skew allowed.
	early := recordOf(t)
	ef := early.Finals[id]
	evs := cloneEvents(ef.Events)
	for i, e := range evs {
		if a, ok := e.Attrs.(history.TimerStarted); ok {
			for j := i; j < len(evs); j++ {
				if fa, ok := evs[j].Attrs.(history.TimerFired); ok && fa.StartedEventID == e.ID {
					evs[j].Time = a.FireAt.Add(-time.Second)
				}
			}
		}
	}
	ef.Events = evs
	early.Finals[id] = ef
	early.Snapshots = nil // the rewritten times would trip I2 as well
	requireOracleCaught(t, (sim.Invariants{}).Check(early), "I4", "before its fireAt")

	// The same early fire is allowed once epsilon covers it (G5's lower bound).
	early.Epsilon = 2 * time.Second
	if err := (sim.Invariants{}).Check(early); err != nil {
		t.Fatalf("epsilon of 2s should permit a 1s early fire: %v", err)
	}
}

func TestWI041I5CatchesDoubleAppliedEffect(t *testing.T) {
	rec := recordOf(t)
	led := sim.NewLedger()
	// Two different keys for what the engine guaranteed would be one: a sink that keyed its
	// dedup on the attempt instead of the idempotency key applies the effect twice.
	led.Apply("wf/run/5", nil)
	led.Apply("wf/run/5#attempt-2", nil)
	rec.Ledger = led
	if err := (sim.Invariants{}).Check(rec); err != nil {
		t.Fatalf("two keys each applied once is not an I5 violation on its own: %v", err)
	}

	// The violation I5 exists for: one key, applied twice.
	double := sim.NewLedger()
	double.ApplyAlways("wf/run/5", nil)
	double.ApplyAlways("wf/run/5", nil)
	rec.Ledger = double
	requireOracleCaught(t, (sim.Invariants{}).Check(rec), "I5", "applied 2 times")
}

func TestWI041I6CatchesCommitUnderStaleFence(t *testing.T) {
	rec := recordOf(t)
	if len(rec.Commits) == 0 {
		t.Fatal("empty ghost log")
	}
	// A node that still believes it owns shard 0 writes after another node took the epoch.
	stale := append(cloneCommits(rec.Commits), memstore.CommitEntry{
		Op:    "UpdateRun",
		Fence: store.Fence{Shard: 0, Owner: "node-0", RangeID: 1},
		Run:   store.RunKey{Shard: 0, WorkflowID: "r-1", RunID: "x"},
	})
	rec.Commits = append(stale, memstore.CommitEntry{Op: "AcquireShard", Granted: true,
		Fence: store.Fence{Shard: 0, Owner: "node-1", RangeID: 9}})
	// Put the grant before the stale write so the write is provably behind the epoch.
	n := len(rec.Commits)
	rec.Commits[n-2], rec.Commits[n-1] = rec.Commits[n-1], rec.Commits[n-2]
	requireOracleCaught(t, (sim.Invariants{}).Check(rec), "I6", "while the current epoch was 9")

	// A write on a shard nobody ever acquired is also a violation.
	never := recordOf(t)
	never.Commits = []memstore.CommitEntry{{Op: "CreateRun",
		Fence: store.Fence{Shard: 3, Owner: "ghost", RangeID: 1}}}
	requireOracleCaught(t, (sim.Invariants{}).Check(never), "I6", "never acquired")
}

func TestWI041I7CatchesLostAndPhantomSignals(t *testing.T) {
	// An acknowledged signal that is nowhere in the history: the acknowledgement was a lie.
	lost := recordOf(t)
	lost.Signals = append(lost.Signals, sim.SignalRecord{WorkflowID: "r-1",
		Name: "approval", RequestID: "never-written"})
	requireOracleCaught(t, (sim.Invariants{}).Check(lost), "I7", "appears 0 times in history")

	// A signal rejected although the run was open: dropped, not rejected for cause.
	id := "r-1"
	rejected := recordOf(t)
	f := rejected.Finals[id]
	f.Status = history.StatusRunning
	rejected.Finals[id] = f
	rejected.Signals = append(rejected.Signals, sim.SignalRecord{WorkflowID: id,
		Name: "approval", RequestID: "r", Err: errors.New("not found")})
	rejected.Snapshots = nil
	requireOracleCaught(t, (sim.Invariants{}).Check(rejected), "I7", "although the run was open")

	// The same acknowledged request ID appearing twice: dedup failed.
	twice := recordOf(t)
	tf := twice.Finals[id]
	var dupID string
	for _, e := range tf.Events {
		if a, ok := e.Attrs.(history.WorkflowExecutionSignaled); ok {
			dupID = a.RequestID
		}
	}
	if dupID == "" {
		t.Fatal("no signal event to duplicate")
	}
	last := tf.Events[len(tf.Events)-1]
	tf.Events = append(cloneEvents(tf.Events), history.Event{ID: last.ID + 1, Time: last.Time,
		Attrs: history.WorkflowExecutionSignaled{SignalName: "approval", RequestID: dupID}})
	twice.Finals[id] = tf
	twice.Snapshots = nil
	requireOracleCaught(t, (sim.Invariants{}).Check(twice), "I7", "appears 2 times in history")
}

func TestWI041I8CatchesResultAgainstReference(t *testing.T) {
	rec := recordOf(t)
	rec.Reference = func(string, []history.Event) (json.RawMessage, error) {
		return json.RawMessage(`"rejected"`), nil
	}
	requireOracleCaught(t, (sim.Invariants{}).Check(rec), "I8", "reference model says")

	// A reference model that cannot derive a result reports that, rather than passing.
	rec2 := recordOf(t)
	rec2.Reference = func(string, []history.Event) (json.RawMessage, error) {
		return nil, errors.New("history has no decision")
	}
	requireOracleCaught(t, (sim.Invariants{}).Check(rec2), "I8", "history has no decision")
}

func TestWI041SkipLeavesTheOtherChecksOnAndIsReported(t *testing.T) {
	rec := recordOf(t)
	rec.Reference = func(string, []history.Event) (json.RawMessage, error) {
		return json.RawMessage(`"rejected"`), nil
	}
	if err := (sim.Invariants{Skip: []string{"I8"}}).Check(rec); err != nil {
		t.Fatalf("I8 was skipped, so the record is clean: %v", err)
	}
	// With another violation present, the report says what was not checked.
	rec.Signals = append(rec.Signals, sim.SignalRecord{WorkflowID: "r-1", Name: "approval", RequestID: "nope"})
	err := (sim.Invariants{Skip: []string{"I8"}}).Check(rec)
	requireOracleCaught(t, err, "I7")
	if !strings.Contains(err.Error(), "skipped I8") {
		t.Fatalf("error %q does not say which checks were skipped", err)
	}
}

func TestWI041NoOracleChecksNothing(t *testing.T) {
	rec := recordOf(t)
	rec.Signals = append(rec.Signals, sim.SignalRecord{WorkflowID: "r-1", Name: "x", RequestID: "ghost"})
	if err := sim.NoOracle.Check(rec); err != nil {
		t.Fatalf("NoOracle: %v", err)
	}
}

// ---- helpers ---------------------------------------------------------------------------

// recordOf returns the record of a correct run, for a test to then corrupt.
func recordOf(t *testing.T) *sim.Record {
	t.Helper()
	res := sim.Run(11, retryWorkload(sim.NewLedger()), nil)
	if res.Err != nil {
		t.Fatalf("baseline run: %v", res.Err)
	}
	if err := (sim.Invariants{}).Check(res.Record); err != nil {
		t.Fatalf("baseline record is not clean: %v", err)
	}
	return res.Record
}

// requireOracleCaught asserts err names the invariant and contains each fragment.
func requireOracleCaught(t *testing.T, err error, invariant string, fragments ...string) {
	t.Helper()
	if err == nil {
		t.Fatalf("%s: no violation reported", invariant)
	}
	msg := err.Error()
	if !strings.Contains(msg, invariant+":") {
		t.Fatalf("error %q does not name %s", msg, invariant)
	}
	for _, f := range fragments {
		if !strings.Contains(msg, f) {
			t.Fatalf("error %q does not contain %q", msg, f)
		}
	}
}

func countType(evs []history.Event, want history.EventType) int {
	n := 0
	for _, e := range evs {
		if e.Type() == want {
			n++
		}
	}
	return n
}

func cloneEvents(evs []history.Event) []history.Event {
	return append([]history.Event(nil), evs...)
}

func cloneCommits(cs []memstore.CommitEntry) []memstore.CommitEntry {
	return append([]memstore.CommitEntry(nil), cs...)
}

// timerEvents returns the ID of the first TimerStarted and the TimerFired that matched it.
func timerEvents(t *testing.T, evs []history.Event) (int64, history.TimerFired) {
	t.Helper()
	for _, e := range evs {
		if _, ok := e.Attrs.(history.TimerStarted); ok {
			for _, f := range evs {
				if a, ok := f.Attrs.(history.TimerFired); ok && a.StartedEventID == e.ID {
					return e.ID, a
				}
			}
		}
	}
	t.Fatal("no timer fired in this history")
	return 0, history.TimerFired{}
}
