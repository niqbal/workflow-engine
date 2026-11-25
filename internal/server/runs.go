package server

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"

	"github.com/niqbal/workflow-engine/internal/history"
	"github.com/niqbal/workflow-engine/internal/server/api"
	"github.com/niqbal/workflow-engine/internal/store"
)

// The single write path (SPEC §8.1). Handlers never write to the store directly: they call
// createRun or update with a function on a history.Machine.

// TransferPayload is the payload of a transfer task.
type TransferPayload struct {
	Dispatch history.DispatchRef `json:"dispatch"`
}

// TimerPayload is the payload of a timer task.
type TimerPayload struct {
	Ref history.TimerRef `json:"ref"`
}

// Written is the result of a successful write.
type Written struct {
	State *history.MutableState
	Out   history.Effects
	Tasks []store.Task // with IDs assigned
}

func encodeEvents(evs []history.Event) ([]store.EventRecord, error) {
	out := make([]store.EventRecord, len(evs))
	for i, e := range evs {
		b, err := json.Marshal(e)
		if err != nil {
			return nil, err
		}
		out[i] = store.EventRecord{ID: e.ID, Data: b}
	}
	return out, nil
}

// DecodeEvents turns stored records back into events.
func DecodeEvents(recs []store.EventRecord) ([]history.Event, error) {
	out := make([]history.Event, len(recs))
	for i, r := range recs {
		if err := json.Unmarshal(r.Data, &out[i]); err != nil {
			return nil, fmt.Errorf("server: decode event %d: %w", r.ID, err)
		}
	}
	return out, nil
}

func tasksFor(key store.RunKey, out history.Effects) ([]store.Task, error) {
	var ts []store.Task
	for _, d := range out.Dispatch {
		b, err := json.Marshal(TransferPayload{Dispatch: d})
		if err != nil {
			return nil, err
		}
		ts = append(ts, store.Task{Shard: key.Shard, Kind: store.TransferTask, Run: key, Payload: b})
	}
	for _, t := range out.Timers {
		b, err := json.Marshal(TimerPayload{Ref: t.Ref})
		if err != nil {
			return nil, err
		}
		ts = append(ts, store.Task{Shard: key.Shard, Kind: store.TimerTask, FireAt: t.FireAt, Run: key, Payload: b})
	}
	return ts, nil
}

// storeErr maps store errors to API errors.
func storeErr(err error) error {
	switch {
	case err == nil:
		return nil
	case errors.Is(err, store.ErrNotFound):
		return fmt.Errorf("%w: %v", api.ErrNotFound, err)
	case errors.Is(err, store.ErrFenced):
		return fmt.Errorf("%w: %v", api.ErrNotOwner, err)
	case errors.Is(err, store.ErrAlreadyExists):
		return fmt.Errorf("%w: %v", api.ErrAlreadyStarted, err)
	case errors.Is(err, store.ErrConflict):
		return fmt.Errorf("%w: %v", api.ErrUnavailable, err)
	}
	return err
}

// HistoryErr maps state-machine errors to API errors.
func HistoryErr(err error) error {
	switch {
	case err == nil:
		return nil
	case errors.Is(err, history.ErrStaleTask):
		return fmt.Errorf("%w: %v", api.ErrStaleTask, err)
	case errors.Is(err, history.ErrInvalidCommand):
		return fmt.Errorf("%w: %v", api.ErrInvalidArgument, err)
	case errors.Is(err, history.ErrWorkflowClosed):
		return fmt.Errorf("%w: %v", api.ErrWorkflowClosed, err)
	}
	return err
}

// createRun writes a new run built by history.NewRun.
func (s *Server) createRun(ctx context.Context, key store.RunKey, st *history.MutableState, out history.Effects) (Written, error) {
	f, ok := s.fence(key.Shard)
	if !ok {
		return Written{}, fmt.Errorf("%w: shard %d", api.ErrNotOwner, key.Shard)
	}
	recs, err := encodeEvents(out.Events)
	if err != nil {
		return Written{}, err
	}
	state, err := st.Encode()
	if err != nil {
		return Written{}, err
	}
	tasks, err := tasksFor(key, out)
	if err != nil {
		return Written{}, err
	}
	start := s.clk.Now()
	written, err := s.st.CreateRun(ctx, f, store.NewRun{Key: key, Events: recs, State: state, Tasks: tasks})
	if errors.Is(err, store.ErrFenced) {
		s.lostShard(key.Shard, err)
	}
	if err != nil {
		return Written{}, storeErr(err)
	}
	s.metrics().ObserveHistoryAppendLatency(s.clk.Now().Sub(start))
	return Written{State: st, Out: out, Tasks: written}, nil
}

// LoadRun reads a run's mutable state.
func (s *Server) LoadRun(ctx context.Context, key store.RunKey) (*history.MutableState, store.RunSnapshot, error) {
	snap, err := s.st.GetRun(ctx, key)
	if err != nil {
		return nil, store.RunSnapshot{}, storeErr(err)
	}
	st, err := history.DecodeState(snap.State)
	if err != nil {
		return nil, store.RunSnapshot{}, err
	}
	return st, snap, nil
}

// update applies fn to the run's state and persists the result in one store write, retrying
// on ErrConflict with freshly loaded state. If fn returns an error nothing is written, except
// that done tasks are still deleted when fn returns history.ErrStaleTask (the task they
// describe is gone). Every handler and processor changes state through here.
func (s *Server) update(ctx context.Context, key store.RunKey, done []store.TaskKey, fn func(m *history.Machine) error) (Written, error) {
	f, ok := s.fence(key.Shard)
	if !ok {
		return Written{}, fmt.Errorf("%w: shard %d", api.ErrNotOwner, key.Shard)
	}
	var lastErr error
	for attempt := 0; attempt <= history.MaxConflictRetries; attempt++ {
		st, snap, err := s.LoadRun(ctx, key)
		if err != nil {
			return Written{}, err
		}
		m := history.NewMachine(st, s.clk.Now())
		if ferr := fn(m); ferr != nil {
			if errors.Is(ferr, history.ErrStaleTask) && len(done) > 0 {
				err := s.st.DeleteTasks(ctx, f, done) // best effort: a stale task's record
				if errors.Is(err, store.ErrFenced) {
					s.metrics().IncErrFenced()
				}
			}
			return Written{}, HistoryErr(ferr)
		}
		recs, err := encodeEvents(m.Out.Events)
		if err != nil {
			return Written{}, err
		}
		state, err := st.Encode()
		if err != nil {
			return Written{}, err
		}
		tasks, err := tasksFor(key, m.Out)
		if err != nil {
			return Written{}, err
		}
		start := s.clk.Now()
		_, written, err := s.st.UpdateRun(ctx, f, key, store.Update{ExpectedDBVersion: snap.DBVersion, Events: recs,
			State: state, Open: st.Status.Open(), NewTasks: tasks, DoneTasks: done})
		if errors.Is(err, store.ErrConflict) {
			s.metrics().IncErrConflict()
			lastErr = err
			continue
		}
		if errors.Is(err, store.ErrFenced) {
			// The shard moved while this write was in flight. Stop serving it.
			s.lostShard(key.Shard, err)
		}
		if err != nil {
			return Written{}, storeErr(err)
		}
		s.metrics().ObserveHistoryAppendLatency(s.clk.Now().Sub(start))
		return Written{State: st, Out: m.Out, Tasks: written}, nil
	}
	return Written{}, storeErr(lastErr)
}

// ReadAllHistory returns a run's full persisted history.
func (s *Server) ReadAllHistory(ctx context.Context, key store.RunKey) ([]history.Event, error) {
	var out []history.Event
	from := int64(1)
	for {
		recs, err := s.st.ReadHistory(ctx, key, from, 500)
		if err != nil {
			return nil, storeErr(err)
		}
		evs, err := DecodeEvents(recs)
		if err != nil {
			return nil, err
		}
		out = append(out, evs...)
		if len(recs) < 500 {
			return out, nil
		}
		from = recs[len(recs)-1].ID + 1
	}
}
