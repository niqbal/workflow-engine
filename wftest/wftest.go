// Package wftest runs workflows in-process for tests (WI-006, SPEC §7.4). Env drives the real
// server state machine (history.Machine) with the real replayer, so histories are exactly
// what the server would record, and ReplayFromFile checks that changed code still replays
// saved histories.
package wftest

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"sort"
	"time"

	"github.com/niqbal/workflow-engine/internal/engine"
	"github.com/niqbal/workflow-engine/internal/history"
	"github.com/niqbal/workflow-engine/wf"
)

// LoadHistory reads a history saved as a JSON array of events.
func LoadHistory(path string) ([]history.Event, error) {
	b, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	var evs []history.Event
	if err := json.Unmarshal(b, &evs); err != nil {
		return nil, fmt.Errorf("wftest: %s: %w", path, err)
	}
	return evs, nil
}

// ReplayFromFile replays a saved history through workflowFn up to every workflow task in it.
// It returns a *engine.NonDeterministicError when the code no longer matches.
func ReplayFromFile(path string, workflowFn any) error {
	evs, err := LoadHistory(path)
	if err != nil {
		return err
	}
	return Replay(evs, workflowFn)
}

// Replay checks every prefix of evs that ends at a WorkflowTaskStarted.
func Replay(evs []history.Event, workflowFn any) error {
	fn, err := wf.Adapt(workflowFn)
	if err != nil {
		return err
	}
	for i, e := range evs {
		if e.Type() != history.EventWorkflowTaskStarted {
			continue
		}
		if i+1 < len(evs) {
			if t := evs[i+1].Type(); t == history.EventWorkflowTaskFailed || t == history.EventWorkflowTaskTimedOut {
				continue
			}
		}
		if _, err := engine.Replay(engine.Info{}, evs[:i+1], fn); err != nil {
			return err
		}
	}
	return nil
}

// SaveHistory writes evs as JSON, for golden files.
func SaveHistory(path string, evs []history.Event) error {
	b, err := json.MarshalIndent(evs, "", " ")
	if err != nil {
		return err
	}
	return os.WriteFile(path, b, 0o644)
}

// ActivityFunc is an activity for Env: JSON in, JSON-able out.
type ActivityFunc func(input json.RawMessage) (any, error)

// Env is an in-process workflow environment with a manual clock.
type Env struct {
	S        *history.MutableState
	History  []history.Event
	Now      time.Time
	fn       engine.Workflow
	acts     map[string]ActivityFunc
	timers   []history.TimerRequest
	ActCalls map[string]int
}

// NewEnv starts workflowFn with input at a fixed start time.
func NewEnv(workflowFn any, input any) (*Env, error) {
	fn, err := wf.Adapt(workflowFn)
	if err != nil {
		return nil, err
	}
	in, err := json.Marshal(input)
	if err != nil {
		return nil, err
	}
	now := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	s, eff := history.NewRun(history.StartInput{WorkflowID: "wf", RunID: "run", WorkflowType: "W", TaskQueue: "q", Input: in}, now)
	return &Env{S: s, History: eff.Events, Now: now, fn: fn, acts: map[string]ActivityFunc{}, timers: eff.Timers, ActCalls: map[string]int{}}, nil
}

// RegisterActivity adds an activity implementation.
func (e *Env) RegisterActivity(name string, fn ActivityFunc) { e.acts[name] = fn }

func (e *Env) apply(f func(m *history.Machine) error) error {
	m := history.NewMachine(e.S, e.Now)
	err := f(m)
	e.History = append(e.History, m.Out.Events...)
	e.timers = append(e.timers, m.Out.Timers...)
	return err
}

// Run executes workflow tasks and activities until the workflow waits on time or a signal.
func (e *Env) Run() error {
	for i := 0; i < 10000; i++ {
		progressed := false
		ids := make([]int64, 0, len(e.S.Activities))
		for id := range e.S.Activities {
			ids = append(ids, id)
		}
		sort.Slice(ids, func(a, b int) bool { return ids[a] < ids[b] })
		for _, id := range ids {
			a := e.S.Activities[id]
			if a == nil || a.State != history.TaskScheduled {
				continue
			}
			progressed = true
			if err := e.runActivity(id, a); err != nil {
				return err
			}
		}
		if wt := e.S.WorkflowTask; wt != nil && wt.State == history.TaskScheduled {
			progressed = true
			if err := e.runWorkflowTask(); err != nil {
				return err
			}
		}
		if !progressed {
			return nil
		}
	}
	return errors.New("wftest: workflow did not settle")
}

func (e *Env) runActivity(id int64, a *history.ActivityInfo) error {
	ref := history.DispatchRef{Type: history.ActivityTaskType, ScheduledEventID: id, Attempt: a.Attempt}
	if err := e.apply(func(m *history.Machine) error { _, _, err := m.ActivityStarted(ref, "wftest"); return err }); err != nil {
		return err
	}
	typ := a.Scheduled.ActivityType
	impl := e.acts[typ]
	if impl == nil {
		return fmt.Errorf("wftest: activity %q not registered", typ)
	}
	e.ActCalls[typ]++
	out, err := impl(a.Scheduled.Input)
	if err != nil {
		return e.apply(func(m *history.Machine) error {
			return m.FailActivity(id, ref.Attempt, history.Failure{Type: "ActivityError", Message: err.Error()})
		})
	}
	b, err := json.Marshal(out)
	if err != nil {
		return err
	}
	return e.apply(func(m *history.Machine) error { return m.CompleteActivity(id, ref.Attempt, b) })
}

func (e *Env) runWorkflowTask() error {
	seq := e.S.WorkflowTask.Seq
	var transient []history.Event
	if err := e.apply(func(m *history.Machine) error {
		var err error
		transient, err = m.WorkflowTaskStarted(history.DispatchRef{Type: history.WorkflowTaskType, Seq: seq}, "wftest")
		return err
	}); err != nil {
		return err
	}
	full := append(append([]history.Event(nil), e.History...), transient...)
	cmds, err := engine.Replay(engine.Info{WorkflowID: "wf", RunID: "run", WorkflowType: "W", TaskQueue: "q"}, full, e.fn)
	if err != nil {
		return err // a test environment surfaces workflow bugs instead of retrying
	}
	return e.apply(func(m *history.Machine) error { return m.CompleteWorkflowTask(seq, cmds) })
}

// Signal sends a signal and runs the workflow forward.
func (e *Env) Signal(name string, payload any) error {
	b, err := json.Marshal(payload)
	if err != nil {
		return err
	}
	if err := e.apply(func(m *history.Machine) error { return m.Signal(name, b, "") }); err != nil {
		return err
	}
	return e.Run()
}

// Advance moves the clock forward, firing due timers in order, and runs the workflow.
func (e *Env) Advance(d time.Duration) error {
	until := e.Now.Add(d)
	for {
		sort.SliceStable(e.timers, func(a, b int) bool { return e.timers[a].FireAt.Before(e.timers[b].FireAt) })
		if len(e.timers) == 0 || e.timers[0].FireAt.After(until) {
			break
		}
		tr := e.timers[0]
		e.timers = e.timers[1:]
		if tr.FireAt.After(e.Now) {
			e.Now = tr.FireAt
		}
		if err := e.apply(func(m *history.Machine) error { m.FireTimer(tr.Ref); return nil }); err != nil {
			return err
		}
		if err := e.Run(); err != nil {
			return err
		}
	}
	e.Now = until
	return e.Run()
}

// Done reports whether the run has closed.
func (e *Env) Done() bool { return !e.S.Status.Open() }

// Result decodes a completed workflow's result into out, or returns its failure.
func (e *Env) Result(out any) error {
	if !e.Done() {
		return fmt.Errorf("wftest: workflow still %s", e.S.Status)
	}
	switch a := e.History[len(e.History)-1].Attrs.(type) {
	case history.WorkflowExecutionCompleted:
		if out != nil && len(a.Result) > 0 {
			return json.Unmarshal(a.Result, out)
		}
		return nil
	case history.WorkflowExecutionFailed:
		return errors.New(a.Failure.Message)
	}
	return fmt.Errorf("wftest: workflow ended %s", e.S.Status)
}

// SaveHistory writes the history as JSON, for golden files.
func (e *Env) SaveHistory(path string) error {
	return SaveHistory(path, e.History)
}
