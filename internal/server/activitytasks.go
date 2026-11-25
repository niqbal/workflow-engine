package server

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/niqbal/workflow-engine/internal/history"
	"github.com/niqbal/workflow-engine/internal/server/api"
	"github.com/niqbal/workflow-engine/internal/server/matching"
	"github.com/niqbal/workflow-engine/internal/store"
)

// WI-019: activity tasks (poll, complete, fail) (SPEC §3.2, §8.3).

// activityTaskToken is what an activity task token carries. On the wire it is an opaque,
// signed token.
type activityTaskToken struct {
	Shard            int    `json:"shard"`
	WorkflowID       string `json:"workflowID"`
	RunID            string `json:"runID"`
	ScheduledEventID int64  `json:"scheduledEventID"`
	Attempt          int    `json:"attempt"`
}

func (t activityTaskToken) key() store.RunKey {
	return store.RunKey{Shard: t.Shard, WorkflowID: t.WorkflowID, RunID: t.RunID}
}

func encodeActivityTaskToken(key []byte, t activityTaskToken) (string, error) {
	return encodeTaskToken(key, t)
}

func decodeActivityTaskToken(key []byte, tok string) (activityTaskToken, error) {
	return decodeTaskToken[activityTaskToken](key, tok)
}

// decodeActivityTaskToken parses a token, rejecting anything this node could not have issued.
func (s *Server) decodeActivityTaskToken(tok string) (activityTaskToken, error) {
	t, err := decodeActivityTaskToken(s.cfg.TokenKey, tok)
	if err != nil {
		return t, err
	}
	if t.WorkflowID == "" || t.RunID == "" || t.ScheduledEventID <= 0 || t.Attempt <= 0 ||
		t.Shard != s.ShardFor(t.WorkflowID) {
		return t, invalid("malformed task token")
	}
	return t, nil
}

// PollActivityTask long-polls the task queue until an activity attempt can be started, ctx is
// done, or LongPoll passes on the server clock; then it returns (nil, nil). Starting an
// attempt deletes its transfer task in the same write. A stale task (the attempt moved on, the
// activity ended, or the run closed) is dropped and polling continues.
func (s *Server) PollActivityTask(ctx context.Context, r api.PollRequest) (*api.ActivityTask, error) {
	if r.TaskQueue == "" {
		return nil, invalid("taskQueue is required")
	}
	deadline := s.clk.Now().Add(s.cfg.LongPoll)
	for {
		t, ok := s.match.PollCtx(ctx, matching.ActivityTasks, r.TaskQueue, deadline)
		if !ok {
			return nil, nil
		}
		task, err := s.startActivityTaskAnywhere(ctx, t, r.Identity)
		if droppable(err) {
			continue
		}
		if err != nil {
			return nil, err
		}
		if task == nil {
			continue
		}
		return task, nil
	}
}

// startActivityTask records the start of the attempt t describes and builds what the worker
// gets.
func (s *Server) startActivityTask(ctx context.Context, t store.Task, identity string) (*api.ActivityTask, error) {
	var p TransferPayload
	if err := json.Unmarshal(t.Payload, &p); err != nil {
		return nil, fmt.Errorf("server: decode transfer task %d on shard %d: %w", t.ID, t.Shard, err)
	}
	if p.Dispatch.Type != history.ActivityTaskType {
		return nil, fmt.Errorf("server: transfer task %d on shard %d is not an activity task", t.ID, t.Shard)
	}
	var (
		info     history.ActivityInfo
		deadline time.Time
	)
	_, err := s.updateAndWake(ctx, t.Run, []store.TaskKey{t.Key()}, func(m *history.Machine) error {
		a, d, err := m.ActivityStarted(p.Dispatch, identity)
		if err != nil {
			return err
		}
		info, deadline = *a, d
		return nil
	})
	if err != nil {
		return nil, err
	}
	token, err := encodeActivityTaskToken(s.cfg.TokenKey, activityTaskToken{Shard: t.Run.Shard, WorkflowID: t.Run.WorkflowID,
		RunID: t.Run.RunID, ScheduledEventID: info.ScheduledEventID, Attempt: info.Attempt})
	if err != nil {
		return nil, err
	}
	s.metrics().IncActivityTasks()
	if !info.StartedAt.IsZero() && !info.FirstScheduledAt.IsZero() {
		s.metrics().ObserveScheduleToStart("activity", info.StartedAt.Sub(info.FirstScheduledAt))
	}
	return &api.ActivityTask{
		TaskToken:        token,
		WorkflowID:       t.Run.WorkflowID,
		RunID:            t.Run.RunID,
		ScheduledEventID: info.ScheduledEventID,
		ActivityID:       info.Scheduled.ActivityID,
		ActivityType:     info.Scheduled.ActivityType,
		Input:            info.Scheduled.Input,
		Attempt:          info.Attempt,
		IdempotencyKey:   fmt.Sprintf("%s/%s/%d", t.Run.WorkflowID, t.Run.RunID, info.ScheduledEventID),
		Deadline:         deadline,
	}, nil
}

// CompleteActivityTask records a successful attempt for the started attempt the token names.
func (s *Server) CompleteActivityTask(ctx context.Context, r api.CompleteActivityTaskRequest) error {
	tok, err := s.decodeActivityTaskToken(r.TaskToken)
	if err != nil {
		return err
	}
	var stepLatency time.Duration
	_, err = s.updateAndWake(ctx, tok.key(), nil, func(m *history.Machine) error {
		if a := m.S.Activities[tok.ScheduledEventID]; a != nil && a.Attempt == tok.Attempt && a.State == history.TaskStarted {
			stepLatency = m.Now.Sub(a.StartedAt)
		} else {
			stepLatency = 0
		}
		return m.CompleteActivity(tok.ScheduledEventID, tok.Attempt, r.Result)
	})
	if err == nil && stepLatency > 0 {
		s.metrics().ObserveStepLatency("activity", stepLatency)
	}
	return err
}

// FailActivityTask records a failed attempt for the started attempt the token names; the
// machine retries it by policy or ends the activity.
func (s *Server) FailActivityTask(ctx context.Context, r api.FailActivityTaskRequest) error {
	tok, err := s.decodeActivityTaskToken(r.TaskToken)
	if err != nil {
		return err
	}
	var stepLatency time.Duration
	_, err = s.updateAndWake(ctx, tok.key(), nil, func(m *history.Machine) error {
		if a := m.S.Activities[tok.ScheduledEventID]; a != nil && a.Attempt == tok.Attempt && a.State == history.TaskStarted {
			stepLatency = m.Now.Sub(a.StartedAt)
		} else {
			stepLatency = 0
		}
		return m.FailActivity(tok.ScheduledEventID, tok.Attempt, r.Failure)
	})
	if err == nil && stepLatency > 0 {
		s.metrics().ObserveStepLatency("activity", stepLatency)
	}
	return err
}
