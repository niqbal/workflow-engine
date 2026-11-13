package server

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/niqbal/workflow-engine/internal/history"
	"github.com/niqbal/workflow-engine/internal/server/api"
	"github.com/niqbal/workflow-engine/internal/server/matching"
	"github.com/niqbal/workflow-engine/internal/store"
)

// WI-018: workflow tasks (poll, complete, fail) (SPEC §4.2, §8.2, D5).

// workflowTaskToken is what a workflow task token carries. On the wire it is an opaque,
// signed token.
type workflowTaskToken struct {
	Shard      int    `json:"shard"`
	WorkflowID string `json:"workflowID"`
	RunID      string `json:"runID"`
	Seq        int64  `json:"seq"`
}

func (t workflowTaskToken) key() store.RunKey {
	return store.RunKey{Shard: t.Shard, WorkflowID: t.WorkflowID, RunID: t.RunID}
}

func encodeWorkflowTaskToken(key []byte, t workflowTaskToken) (string, error) {
	return encodeTaskToken(key, t)
}

func decodeWorkflowTaskToken(key []byte, tok string) (workflowTaskToken, error) {
	return decodeTaskToken[workflowTaskToken](key, tok)
}

// decodeWorkflowTaskToken parses a token, rejecting anything this node could not have issued.
func (s *Server) decodeWorkflowTaskToken(tok string) (workflowTaskToken, error) {
	t, err := decodeWorkflowTaskToken(s.cfg.TokenKey, tok)
	if err != nil {
		return t, err
	}
	if t.WorkflowID == "" || t.RunID == "" || t.Seq <= 0 || t.Shard != s.ShardFor(t.WorkflowID) {
		return t, invalid("malformed task token")
	}
	return t, nil
}

// PollWorkflowTask long-polls the task queue until a task can be started, ctx is done, or
// LongPoll passes on the server clock; then it returns (nil, nil). Starting a task deletes its
// transfer task in the same write. A stale task (its run moved on or closed) is dropped and
// polling continues.
func (s *Server) PollWorkflowTask(ctx context.Context, r api.PollRequest) (*api.WorkflowTask, error) {
	if r.TaskQueue == "" {
		return nil, invalid("taskQueue is required")
	}
	deadline := s.clk.Now().Add(s.cfg.LongPoll)
	for {
		t, ok := s.match.PollCtx(ctx, matching.WorkflowTasks, r.TaskQueue, deadline)
		if !ok {
			return nil, nil
		}
		task, err := s.startWorkflowTask(ctx, t, r.Identity)
		if errors.Is(err, api.ErrStaleTask) || errors.Is(err, api.ErrNotFound) {
			continue
		}
		if err != nil {
			return nil, err
		}
		return task, nil
	}
}

// startWorkflowTask records the start of the task t describes and builds what the worker gets:
// persisted history up to the start plus the transient events of a retried attempt.
func (s *Server) startWorkflowTask(ctx context.Context, t store.Task, identity string) (*api.WorkflowTask, error) {
	var p TransferPayload
	if err := json.Unmarshal(t.Payload, &p); err != nil {
		return nil, fmt.Errorf("server: decode transfer task %d on shard %d: %w", t.ID, t.Shard, err)
	}
	if p.Dispatch.Type != history.WorkflowTaskType {
		return nil, fmt.Errorf("server: transfer task %d on shard %d is not a workflow task", t.ID, t.Shard)
	}
	var transient []history.Event
	w, err := s.updateAndWake(ctx, t.Run, []store.TaskKey{t.Key()}, func(m *history.Machine) error {
		evs, err := m.WorkflowTaskStarted(p.Dispatch, identity)
		transient = evs
		return err
	})
	if err != nil {
		return nil, err
	}
	wt := w.State.WorkflowTask
	token, err := encodeWorkflowTaskToken(s.cfg.TokenKey, workflowTaskToken{Shard: t.Run.Shard, WorkflowID: t.Run.WorkflowID,
		RunID: t.Run.RunID, Seq: wt.Seq})
	if err != nil {
		return nil, err
	}
	evs, err := s.ReadAllHistory(ctx, t.Run)
	if err != nil {
		return nil, err
	}
	// Later writes may already have appended events; the worker sees history as of its start.
	n := 0
	for n < len(evs) && evs[n].ID < w.State.NextEventID {
		n++
	}
	hist := append(evs[:n:n], transient...)
	s.metrics().IncWorkflowTasks()
	if !wt.StartedAt.IsZero() && !wt.ScheduledAt.IsZero() {
		s.metrics().ObserveScheduleToStart("workflow", wt.StartedAt.Sub(wt.ScheduledAt))
	}
	return &api.WorkflowTask{TaskToken: token, WorkflowID: w.State.WorkflowID, RunID: w.State.RunID,
		WorkflowType: w.State.WorkflowType, Attempt: wt.Attempt, History: hist}, nil
}

// CompleteWorkflowTask applies the worker's commands to the started task the token names.
func (s *Server) CompleteWorkflowTask(ctx context.Context, r api.CompleteWorkflowTaskRequest) error {
	tok, err := s.decodeWorkflowTaskToken(r.TaskToken)
	if err != nil {
		return err
	}
	var stepLatency time.Duration
	_, err = s.updateAndWake(ctx, tok.key(), nil, func(m *history.Machine) error {
		if wt := m.S.WorkflowTask; wt != nil && wt.Seq == tok.Seq && wt.State == history.TaskStarted {
			stepLatency = m.Now.Sub(wt.StartedAt)
		} else {
			stepLatency = 0
		}
		return m.CompleteWorkflowTask(tok.Seq, r.Commands)
	})
	if err == nil && stepLatency > 0 {
		s.metrics().ObserveStepLatency("workflow", stepLatency)
	}
	return err
}

// FailWorkflowTask records a worker-reported failure of the started task the token names.
func (s *Server) FailWorkflowTask(ctx context.Context, r api.FailWorkflowTaskRequest) error {
	tok, err := s.decodeWorkflowTaskToken(r.TaskToken)
	if err != nil {
		return err
	}
	var stepLatency time.Duration
	_, err = s.updateAndWake(ctx, tok.key(), nil, func(m *history.Machine) error {
		if wt := m.S.WorkflowTask; wt != nil && wt.Seq == tok.Seq && wt.State == history.TaskStarted {
			stepLatency = m.Now.Sub(wt.StartedAt)
		} else {
			stepLatency = 0
		}
		return m.FailWorkflowTask(tok.Seq, r.Cause)
	})
	if err == nil && stepLatency > 0 {
		s.metrics().ObserveStepLatency("workflow", stepLatency)
	}
	return err
}
