package sim

// The oracle (WI-041, SPEC §11.3). Invariants I1-I8 are checked after the quiet phase on
// what one run recorded. Nothing here reads live cluster state: every check works from the
// Record, so a failure is reproducible from the seed alone.

import (
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"strings"
	"time"

	"github.com/niqbal/workflow-engine/internal/engine"
	"github.com/niqbal/workflow-engine/internal/history"
	"github.com/niqbal/workflow-engine/internal/store/memstore"
)

// Oracle checks a finished run. Check returns nil when every invariant holds.
type Oracle interface {
	Check(r *Record) error
}

// StartRecord is one start the workload acknowledged.
type StartRecord struct {
	WorkflowID string
	RunID      string
	Type       string
	TaskQueue  string
	RequestID  string
}

// SignalRecord is one signal the workload sent and what it was told.
type SignalRecord struct {
	WorkflowID string
	Name       string
	RequestID  string
	Err        error // nil means acknowledged
}

// Snapshot is a history as some component saw it at one moment: a workflow task served to a
// worker, or a read through the cluster. I2 holds each one against the final history.
type Snapshot struct {
	WorkflowID string
	RunID      string
	Source     string
	At         time.Time
	Events     []history.Event
}

// Final is a run's end state.
type Final struct {
	RunID  string
	Status history.RunStatus
	Events []history.Event
}

// Record is everything one run observed. The harness fills it; the oracle reads it.
type Record struct {
	Seed      uint64
	Starts    []StartRecord
	Signals   []SignalRecord
	Snapshots []Snapshot
	Finals    map[string]Final
	Commits   []memstore.CommitEntry
	Ledger    *Ledger
	Workflows map[string]engine.Workflow

	// Incarnations is how many times each node was started: one, plus a restart per crash.
	Incarnations []int

	// Epsilon is the maximum clock skew between nodes that faults may introduce. I4 allows a
	// timer to fire that much before its fireAt, and no more.
	Epsilon time.Duration
	// QuietDeadline is true when the quiet phase ran out of virtual time.
	QuietDeadline bool
	// Reference is the I8 model: given a final history it returns the result the workflow
	// must have produced. A nil Reference skips I8.
	Reference func(id string, evs []history.Event) (json.RawMessage, error)
}

// Invariants is the default oracle: every check of SPEC §11.3.
//
// Skip names checks to leave out ("I3"), for a workload that cannot support one — a
// workflow registered as a closure that replay cannot reach, say. Skipping is recorded in
// the failure message of any other check, so a quiet run cannot hide behind it.
type Invariants struct {
	Skip []string
}

var _ Oracle = Invariants{}

type check struct {
	id string
	fn func(*Record) error
}

// Check runs I1-I8 and returns every violation, not just the first: one bug often trips
// several invariants, and the combination is the most useful part of a failure report.
func (iv Invariants) Check(r *Record) error {
	if r == nil {
		return errors.New("oracle: no record")
	}
	skip := map[string]bool{}
	for _, s := range iv.Skip {
		skip[strings.ToUpper(strings.TrimSpace(s))] = true
	}
	checks := []check{
		{"I1", checkI1Closed},
		{"I2", checkI2Prefixes},
		{"I3", checkI3Replay},
		{"I4", checkI4Timers},
		{"I5", checkI5Effects},
		{"I6", checkI6Fences},
		{"I7", checkI7Signals},
		{"I8", checkI8Reference},
	}
	var failures []string
	for _, c := range checks {
		if skip[c.id] {
			continue
		}
		if err := c.fn(r); err != nil {
			failures = append(failures, fmt.Sprintf("%s: %v", c.id, err))
		}
	}
	if len(failures) == 0 {
		return nil
	}
	msg := strings.Join(failures, "; ")
	if len(iv.Skip) > 0 {
		msg += fmt.Sprintf(" (skipped %s)", strings.Join(iv.Skip, ","))
	}
	return fmt.Errorf("oracle: %s", msg)
}

// I1: every acknowledged start is closed once faults have stopped (G1, G8).
func checkI1Closed(r *Record) error {
	var open []string
	for _, s := range uniqueStarts(r) {
		f, ok := r.Finals[s.WorkflowID]
		if !ok {
			open = append(open, s.WorkflowID+" (no final state)")
			continue
		}
		if f.Status.Open() {
			open = append(open, fmt.Sprintf("%s (%s)", s.WorkflowID, f.Status))
		}
	}
	if len(open) == 0 {
		return nil
	}
	if r.QuietDeadline {
		return fmt.Errorf("quiet phase ran out of virtual time with %d run(s) open: %s",
			len(open), strings.Join(open, ", "))
	}
	return fmt.Errorf("%d started run(s) never closed: %s", len(open), strings.Join(open, ", "))
}

// I2: every history anyone saw is a prefix of the final history, with contiguous IDs (G2).
func checkI2Prefixes(r *Record) error {
	for _, id := range sortedKeys(r.Finals) {
		if err := contiguous(r.Finals[id].Events); err != nil {
			return fmt.Errorf("%s: final history: %w", id, err)
		}
	}
	for _, snap := range r.Snapshots {
		f, ok := r.Finals[snap.WorkflowID]
		if !ok {
			return fmt.Errorf("%s: a history was observed (%s) but the run has no final state",
				snap.WorkflowID, snap.Source)
		}
		if err := contiguous(snap.Events); err != nil {
			return fmt.Errorf("%s observed by %s: %w", snap.WorkflowID, snap.Source, err)
		}
		if snap.RunID != "" && f.RunID != "" && snap.RunID != f.RunID {
			continue // an earlier run of the same workflow ID; not this final history
		}
		if len(snap.Events) > len(f.Events) {
			return fmt.Errorf("%s observed by %s at %s: %d events, longer than the final history (%d)",
				snap.WorkflowID, snap.Source, snap.At.UTC().Format(time.RFC3339Nano), len(snap.Events), len(f.Events))
		}
		// A workflow task is served as the durable prefix plus the events the server has
		// just applied (internal/server/workflowtasks.go). Those trailing events are not
		// durable yet: a task rescheduled before the write lands changes its attempt, and
		// the copy a worker is handed can carry the time of an earlier attempt. So what I2
		// holds against the final history is:
		//
		//	ID and type		always, for every event observed: this is the prefix itself
		//	payload			for every event before the last workflow task the snapshot
		//					starts, which is the part that was already durable
		//	timestamp		never later than the final one: an observed event may carry a
		//					time from before it was written, but history never moves back
		//
		// A rewrite of durable history fails all three; a transient tail fails none.
		durable := durableLen(snap.Events)
		for i, e := range snap.Events {
			want := f.Events[i]
			if err := sameEventShape(e, want); err != nil {
				return fmt.Errorf("%s observed by %s at %s: event %d is not the final one: %w",
					snap.WorkflowID, snap.Source, snap.At.UTC().Format(time.RFC3339Nano), e.ID, err)
			}
			if e.Time.After(want.Time) {
				return fmt.Errorf("%s observed by %s at %s: event %d was seen at %s but the final history writes it earlier, at %s: history moved backwards",
					snap.WorkflowID, snap.Source, snap.At.UTC().Format(time.RFC3339Nano), e.ID,
					e.Time.UTC().Format(time.RFC3339Nano), want.Time.UTC().Format(time.RFC3339Nano))
			}
			if i < durable {
				if err := sameEvent(e, want); err != nil {
					return fmt.Errorf("%s observed by %s at %s: durable event %d is not the final one: %w",
						snap.WorkflowID, snap.Source, snap.At.UTC().Format(time.RFC3339Nano), e.ID, err)
				}
			}
		}
	}
	return nil
}

// I3: replaying a final history with the workflow code reproduces the recorded commands, at
// every workflow task in it (G3). engine.Replay reports the first divergence itself.
func checkI3Replay(r *Record) error {
	for _, s := range uniqueStarts(r) {
		f, ok := r.Finals[s.WorkflowID]
		if !ok {
			continue // I1 reports this
		}
		fn, ok := r.Workflows[s.Type]
		if !ok {
			return fmt.Errorf("%s: workflow type %q is not registered, so its history cannot be replayed",
				s.WorkflowID, s.Type)
		}
		info := engine.Info{WorkflowID: s.WorkflowID, RunID: f.RunID, WorkflowType: s.Type, TaskQueue: s.TaskQueue}
		if err := replayEveryTask(info, f.Events, fn); err != nil {
			return fmt.Errorf("%s: %w", s.WorkflowID, err)
		}
		if err := replayAgreesWithClose(info, f.Events, fn); err != nil {
			return fmt.Errorf("%s: %w", s.WorkflowID, err)
		}
	}
	return nil
}

// replayEveryTask replays every prefix that ends at a WorkflowTaskStarted, which is what a
// worker is handed. A task the server went on to fail or time out is skipped: its commands
// were never recorded, so there is nothing to reproduce.
func replayEveryTask(info engine.Info, evs []history.Event, fn engine.Workflow) error {
	for i, e := range evs {
		if e.Type() != history.EventWorkflowTaskStarted {
			continue
		}
		if i+1 < len(evs) {
			switch evs[i+1].Type() {
			case history.EventWorkflowTaskFailed, history.EventWorkflowTaskTimedOut:
				continue
			}
		}
		if _, err := engine.Replay(info, evs[:i+1], fn); err != nil {
			return fmt.Errorf("replay of events 1-%d: %w", e.ID, err)
		}
	}
	return nil
}

// replayAgreesWithClose checks the close the history records is the close the code asks for.
// A run closed by terminate or execution timeout is not the workflow's own decision, so it
// is exempt.
func replayAgreesWithClose(info engine.Info, evs []history.Event, fn engine.Workflow) error {
	if len(evs) == 0 {
		return nil
	}
	last := evs[len(evs)-1]
	var want history.CommandType
	switch last.Type() {
	case history.EventWorkflowExecutionCompleted:
		want = history.CommandCompleteWorkflow
	case history.EventWorkflowExecutionFailed:
		want = history.CommandFailWorkflow
	default:
		return nil
	}
	// Replay up to the last workflow task: its commands are the ones that closed the run.
	cut := -1
	for i, e := range evs {
		if e.Type() == history.EventWorkflowTaskStarted {
			cut = i
		}
	}
	if cut < 0 {
		return fmt.Errorf("history closes with %s but holds no workflow task", last.Type())
	}
	cmds, err := engine.Replay(info, evs[:cut+1], fn)
	if err != nil {
		return fmt.Errorf("replay of the closing task: %w", err)
	}
	for _, c := range cmds {
		if c.CommandType() == want {
			return nil
		}
	}
	return fmt.Errorf("history closes with %s, but replaying the closing workflow task asks for %s",
		last.Type(), commandTypes(cmds))
}

// I4: a timer fires at most once, never before fireAt - epsilon (G5).
func checkI4Timers(r *Record) error {
	for _, id := range sortedKeys(r.Finals) {
		f := r.Finals[id]
		started := map[int64]history.TimerStarted{}
		fired := map[int64]int64{} // started event ID -> firing event ID
		for _, e := range f.Events {
			switch a := e.Attrs.(type) {
			case history.TimerStarted:
				started[e.ID] = a
			case history.TimerFired:
				st, ok := started[a.StartedEventID]
				if !ok {
					return fmt.Errorf("%s: event %d fired timer %q, which no event started",
						id, e.ID, a.TimerID)
				}
				if prev, dup := fired[a.StartedEventID]; dup {
					return fmt.Errorf("%s: timer %q (started by event %d) fired twice, at events %d and %d",
						id, a.TimerID, a.StartedEventID, prev, e.ID)
				}
				fired[a.StartedEventID] = e.ID
				if earliest := st.FireAt.Add(-r.Epsilon); e.Time.Before(earliest) {
					return fmt.Errorf("%s: timer %q fired at %s, before its fireAt %s (epsilon %s)",
						id, a.TimerID, e.Time.UTC().Format(time.RFC3339Nano),
						st.FireAt.UTC().Format(time.RFC3339Nano), r.Epsilon)
				}
			case history.TimerCanceled:
				if _, ok := started[a.StartedEventID]; !ok {
					return fmt.Errorf("%s: event %d canceled timer %q, which no event started",
						id, e.ID, a.TimerID)
				}
				if prev, dup := fired[a.StartedEventID]; dup {
					return fmt.Errorf("%s: timer %q fired at event %d and was then canceled at event %d",
						id, a.TimerID, prev, e.ID)
				}
			}
		}
	}
	return nil
}

// I5: each idempotency key is applied exactly once, and no sink applies more effects than
// the attempts that reached it (G4).
func checkI5Effects(r *Record) error {
	if r.Ledger == nil {
		return nil
	}
	for _, key := range r.Ledger.Keys() {
		attempts, effects := r.Ledger.Attempts(key), r.Ledger.Effects(key)
		if effects != 1 {
			return fmt.Errorf("idempotency key %s was applied %d times, want exactly 1 (%d attempt(s))",
				key, effects, attempts)
		}
		if attempts < effects {
			return fmt.Errorf("idempotency key %s: %d effect(s) from %d attempt(s)", key, effects, attempts)
		}
	}
	return nil
}

// I6: no committed write carried a fence other than the shard's current epoch (G6).
//
// The ghost log is the store's commit log, kept here rather than read back from the store:
// the current epoch is derived from the AcquireShard grants in the log, so the check never
// asks the store whether the store was right.
func checkI6Fences(r *Record) error {
	type epoch struct {
		rangeID int64
		owner   string
	}
	current := map[int]epoch{}
	for i, e := range r.Commits {
		if e.Granted {
			current[e.Fence.Shard] = epoch{e.Fence.RangeID, e.Fence.Owner}
			continue
		}
		cur, ok := current[e.Fence.Shard]
		if !ok {
			return fmt.Errorf("commit %d: %s on shard %d with fence %d, but the shard was never acquired",
				i, e.Op, e.Fence.Shard, e.Fence.RangeID)
		}
		if e.Fence.RangeID != cur.rangeID {
			return fmt.Errorf("commit %d: %s committed on shard %d with fence %d while the current epoch was %d (owner %q)",
				i, e.Op, e.Fence.Shard, e.Fence.RangeID, cur.rangeID, cur.owner)
		}
		if e.Fence.Owner != cur.owner {
			return fmt.Errorf("commit %d: %s committed on shard %d as %q while %q owned epoch %d",
				i, e.Op, e.Fence.Shard, e.Fence.Owner, cur.owner, cur.rangeID)
		}
	}
	return nil
}

// I7: an acknowledged signal appears exactly once in history; a rejected one targeted a run
// that was closed or absent, and left no trace (G7).
func checkI7Signals(r *Record) error {
	type key struct{ id, requestID string }
	// A client retries a signal whose outcome is unknown with the same request ID, so a
	// rejected call is only a lost signal if no call with that ID was ever acknowledged.
	everAcked := map[key]bool{}
	for _, s := range r.Signals {
		if s.Err == nil && s.RequestID != "" {
			everAcked[key{s.WorkflowID, s.RequestID}] = true
		}
	}
	acked := map[key]bool{}
	for _, s := range r.Signals {
		f, ok := r.Finals[s.WorkflowID]
		if s.Err != nil {
			if !ok {
				continue // no run: a rejection is the only correct answer
			}
			if s.RequestID != "" && everAcked[key{s.WorkflowID, s.RequestID}] {
				continue // a retry of this call was acknowledged
			}
			if f.Status.Open() {
				return fmt.Errorf("%s: signal %q was rejected (%v) although the run was open",
					s.WorkflowID, s.Name, s.Err)
			}
			if s.RequestID != "" && countSignals(f.Events, s.RequestID) > 0 {
				return fmt.Errorf("%s: signal %q was rejected (%v) but request %s is in the history",
					s.WorkflowID, s.Name, s.Err, s.RequestID)
			}
			continue
		}
		if !ok {
			return fmt.Errorf("%s: signal %q was acknowledged but the run has no final state",
				s.WorkflowID, s.Name)
		}
		if s.RequestID == "" {
			continue // without a request ID an acknowledged signal cannot be told from a retry
		}
		k := key{s.WorkflowID, s.RequestID}
		if acked[k] {
			// A retry of an acknowledged signal: the dedup bound of G7 says it must not
			// appear a second time, which the count below enforces.
			continue
		}
		acked[k] = true
		if n := countSignals(f.Events, s.RequestID); n != 1 {
			return fmt.Errorf("%s: acknowledged signal %q (request %s) appears %d times in history, want 1",
				s.WorkflowID, s.Name, s.RequestID, n)
		}
	}
	return nil
}

func countSignals(evs []history.Event, requestID string) int {
	n := 0
	for _, e := range evs {
		if a, ok := e.Attrs.(history.WorkflowExecutionSignaled); ok && a.RequestID == requestID {
			n++
		}
	}
	return n
}

// I8: the result the run recorded is the result the reference model derives from the same
// final history, so the check does not depend on which race won.
func checkI8Reference(r *Record) error {
	if r.Reference == nil {
		return nil
	}
	for _, id := range sortedKeys(r.Finals) {
		f := r.Finals[id]
		if f.Status.Open() {
			continue // I1 reports this
		}
		want, err := r.Reference(id, f.Events)
		if err != nil {
			return fmt.Errorf("%s: reference model: %w", id, err)
		}
		if want == nil {
			continue
		}
		got, ok := closeResult(f.Events)
		if !ok {
			return fmt.Errorf("%s: closed as %s with no result to compare against the reference model",
				id, f.Status)
		}
		if !jsonEqual(got, want) {
			return fmt.Errorf("%s: result %s, reference model says %s", id, got, want)
		}
	}
	return nil
}

func closeResult(evs []history.Event) (json.RawMessage, bool) {
	for i := len(evs) - 1; i >= 0; i-- {
		if a, ok := evs[i].Attrs.(history.WorkflowExecutionCompleted); ok {
			if len(a.Result) == 0 {
				return json.RawMessage("null"), true
			}
			return a.Result, true
		}
	}
	return nil, false
}

func jsonEqual(a, b json.RawMessage) bool {
	var x, y any
	if err := json.Unmarshal(a, &x); err != nil {
		return false
	}
	if err := json.Unmarshal(b, &y); err != nil {
		return false
	}
	ab, err1 := json.Marshal(x)
	bb, err2 := json.Marshal(y)
	return err1 == nil && err2 == nil && string(ab) == string(bb)
}

// ---- helpers --------------------------------------------------------------------------

// uniqueStarts returns one record per workflow ID, in the order they were started.
func uniqueStarts(r *Record) []StartRecord {
	seen := map[string]bool{}
	out := make([]StartRecord, 0, len(r.Starts))
	for _, s := range r.Starts {
		if seen[s.WorkflowID] {
			continue
		}
		seen[s.WorkflowID] = true
		out = append(out, s)
	}
	return out
}

func contiguous(evs []history.Event) error {
	for i, e := range evs {
		if e.ID != int64(i+1) {
			return fmt.Errorf("event %d of %d has ID %d; IDs must start at 1 and be contiguous",
				i+1, len(evs), e.ID)
		}
		if e.Attrs == nil {
			return fmt.Errorf("event %d has no attributes", e.ID)
		}
	}
	return nil
}

// sameEvent compares two events by ID, type, timestamp and payload. An event is immutable
// once written (G2), so any difference is a rewrite.
func sameEvent(got, want history.Event) error {
	if got.ID != want.ID {
		return fmt.Errorf("ID %d, final history has %d", got.ID, want.ID)
	}
	if got.Type() != want.Type() {
		return fmt.Errorf("type %s, final history has %s", got.Type(), want.Type())
	}
	if !got.Time.Equal(want.Time) {
		return fmt.Errorf("time %s, final history has %s",
			got.Time.UTC().Format(time.RFC3339Nano), want.Time.UTC().Format(time.RFC3339Nano))
	}
	a, err1 := json.Marshal(got.Attrs)
	b, err2 := json.Marshal(want.Attrs)
	if err1 != nil || err2 != nil {
		return fmt.Errorf("attributes could not be encoded (%v, %v)", err1, err2)
	}
	if string(a) != string(b) {
		return fmt.Errorf("attributes %s, final history has %s", a, b)
	}
	return nil
}

// durableLen is how much of a served history was already durable: everything before the
// last workflow task in it, which is the one the server started as it served the snapshot.
func durableLen(evs []history.Event) int {
	for i := len(evs) - 1; i >= 0; i-- {
		if evs[i].Type() == history.EventWorkflowTaskStarted {
			// The task's own scheduled event may have been written in the same step.
			if i > 0 && evs[i-1].Type() == history.EventWorkflowTaskScheduled {
				return i - 1
			}
			return i
		}
	}
	return len(evs)
}

// sameEventShape compares only ID and type, for an event observed before it was durable.
func sameEventShape(got, want history.Event) error {
	if got.ID != want.ID {
		return fmt.Errorf("ID %d, final history has %d", got.ID, want.ID)
	}
	if got.Type() != want.Type() {
		return fmt.Errorf("type %s, final history has %s", got.Type(), want.Type())
	}
	return nil
}

func commandTypes(cmds []history.Command) string {
	if len(cmds) == 0 {
		return "nothing"
	}
	out := make([]string, 0, len(cmds))
	for _, c := range cmds {
		out = append(out, string(c.CommandType()))
	}
	return strings.Join(out, ",")
}

// sortedKeys is used by failure messages so a report from one seed never reorders.
func sortedKeys[V any](m map[string]V) []string {
	out := make([]string, 0, len(m))
	for k := range m {
		out = append(out, k)
	}
	sort.Strings(out)
	return out
}

// NoOracle checks nothing. Use it for a workload that is deliberately not correct, such as
// the harness's own tests for an open run or a workload error.
var NoOracle Oracle = noOracle{}

type noOracle struct{}

func (noOracle) Check(*Record) error { return nil }
