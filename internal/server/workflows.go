package server

import (
	"context"
	"errors"
	"fmt"

	"github.com/niqbal/workflow-engine/internal/history"
	"github.com/niqbal/workflow-engine/internal/server/api"
	"github.com/niqbal/workflow-engine/internal/store"
	"github.com/niqbal/workflow-engine/internal/xsync"
)

// WI-017: start, signal, terminate, describe, history (SPEC §8.4, §13.1).

// errNoChange aborts an update whose transition would write nothing.
var errNoChange = errors.New("server: no change")

func invalid(format string, args ...any) error {
	return fmt.Errorf("%w: %s", api.ErrInvalidArgument, fmt.Sprintf(format, args...))
}

// latestRun returns the key of the latest run (open or closed) for a workflow ID.
func (s *Server) latestRun(ctx context.Context, workflowID string) (store.RunKey, error) {
	if workflowID == "" {
		return store.RunKey{}, invalid("workflowID is required")
	}
	key, err := s.st.GetLatestRun(ctx, s.ShardFor(workflowID), workflowID)
	if err != nil {
		return store.RunKey{}, storeErr(err)
	}
	return key, nil
}

// startLock returns the node-local lock that serializes starts on a shard. The store rejects
// duplicate run keys but not a second run for the same workflow ID, so the latest-run check
// and the create must not interleave with another start for that shard. Fences make sure only
// the shard owner can create runs, so a lock on this node is enough.
func (s *Server) startLock(shard int) xsync.Locker {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.sub.starts == nil {
		s.sub.starts = map[int]xsync.Locker{}
	}
	l, ok := s.sub.starts[shard]
	if !ok {
		l = s.rt.NewLocker()
		s.sub.starts[shard] = l
	}
	return l
}

// StartWorkflow starts a run, deduplicated by requestID against the latest run for the
// workflow ID, open or closed (SPEC §8.4). The check and the create run under the shard's
// start lock, so concurrent starts for one workflow ID create at most one run.
func (s *Server) StartWorkflow(ctx context.Context, r api.StartRequest) (api.StartResponse, error) {
	switch {
	case r.WorkflowID == "":
		return api.StartResponse{}, invalid("workflowID is required")
	case r.WorkflowType == "":
		return api.StartResponse{}, invalid("workflowType is required")
	case r.TaskQueue == "":
		return api.StartResponse{}, invalid("taskQueue is required")
	case r.ExecutionTimeout < 0:
		return api.StartResponse{}, invalid("executionTimeout must not be negative")
	}
	shard := s.ShardFor(r.WorkflowID)
	lock := s.startLock(shard)
	lock.Lock()
	defer lock.Unlock()
	latest, err := s.st.GetLatestRun(ctx, shard, r.WorkflowID)
	switch {
	case err == nil:
		st, _, err := s.LoadRun(ctx, latest)
		if err != nil {
			return api.StartResponse{}, err
		}
		if r.RequestID != "" && st.StartRequestID == r.RequestID {
			return api.StartResponse{RunID: latest.RunID}, nil
		}
		return api.StartResponse{}, fmt.Errorf("%w: workflow %s has run %s", api.ErrAlreadyStarted, r.WorkflowID, latest.RunID)
	case !errors.Is(err, store.ErrNotFound):
		return api.StartResponse{}, storeErr(err)
	}
	key := store.RunKey{Shard: shard, WorkflowID: r.WorkflowID, RunID: s.newRunID()}
	st, out := history.NewRun(history.StartInput{WorkflowID: r.WorkflowID, RunID: key.RunID, WorkflowType: r.WorkflowType,
		TaskQueue: r.TaskQueue, RequestID: r.RequestID, Input: r.Input, ExecutionTimeout: r.ExecutionTimeout}, s.clk.Now())
	if _, err := s.createRun(ctx, key, st, out); err != nil {
		return api.StartResponse{}, err
	}
	return api.StartResponse{RunID: key.RunID}, nil
}

// SignalWorkflow delivers a signal to the latest run (SPEC §8.4, G7).
func (s *Server) SignalWorkflow(ctx context.Context, r api.SignalRequest) error {
	if r.SignalName == "" {
		return invalid("signalName is required")
	}
	key, err := s.latestRun(ctx, r.WorkflowID)
	if err != nil {
		return err
	}
	_, err = s.update(ctx, key, nil, func(m *history.Machine) error {
		return m.Signal(r.SignalName, r.Payload, r.RequestID)
	})
	return err
}

// TerminateWorkflow closes the latest run. Terminating a closed run is a no-op and writes
// nothing.
func (s *Server) TerminateWorkflow(ctx context.Context, r api.TerminateRequest) error {
	key, err := s.latestRun(ctx, r.WorkflowID)
	if err != nil {
		return err
	}
	_, err = s.update(ctx, key, nil, func(m *history.Machine) error {
		if !m.S.Status.Open() {
			return errNoChange
		}
		m.Terminate(r.Reason)
		return nil
	})
	if errors.Is(err, errNoChange) {
		return nil
	}
	return err
}

// DescribeWorkflow summarizes the latest run and its pending work.
func (s *Server) DescribeWorkflow(ctx context.Context, id string) (api.Description, error) {
	key, err := s.latestRun(ctx, id)
	if err != nil {
		return api.Description{}, err
	}
	st, _, err := s.LoadRun(ctx, key)
	if err != nil {
		return api.Description{}, err
	}
	d := api.Description{WorkflowID: st.WorkflowID, RunID: st.RunID, WorkflowType: st.WorkflowType, Status: st.Status}
	for _, sid := range xsync.SortedKeys(st.Activities) {
		a := st.Activities[sid]
		d.PendingActivities = append(d.PendingActivities, api.PendingActivity{ScheduledEventID: a.ScheduledEventID,
			ActivityID: a.Scheduled.ActivityID, ActivityType: a.Scheduled.ActivityType, Attempt: a.Attempt, State: a.State})
	}
	for _, tid := range xsync.SortedKeys(st.Timers) {
		if t := st.Timers[tid]; !t.Fired {
			d.PendingTimers = append(d.PendingTimers, api.PendingTimer{TimerID: t.TimerID, FireAt: t.FireAt})
		}
	}
	return d, nil
}

// GetHistory returns the full history of the latest run.
func (s *Server) GetHistory(ctx context.Context, id string) ([]history.Event, error) {
	key, err := s.latestRun(ctx, id)
	if err != nil {
		return nil, err
	}
	return s.ReadAllHistory(ctx, key)
}
