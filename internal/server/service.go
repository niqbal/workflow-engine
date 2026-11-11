package server

import (
	"context"
	"errors"

	"github.com/niqbal/workflow-engine/internal/server/api"
)

// Service handlers. Each is a factory work item; until it lands, it returns errNotBuilt.
// Handlers change state only through s.createRun and s.update (runs.go).

var errNotBuilt = errors.New("server: not built yet")

var _ api.Service = (*Server)(nil)

// Service returns the node's api.Service.
func (s *Server) Service() api.Service { return s }

// WI-017 (start, signal, terminate, describe, history) lives in workflows.go.

// WI-018: workflow tasks (poll, complete, fail).
func (s *Server) PollWorkflowTask(ctx context.Context, r api.PollRequest) (*api.WorkflowTask, error) {
	return nil, errNotBuilt
}
func (s *Server) CompleteWorkflowTask(ctx context.Context, r api.CompleteWorkflowTaskRequest) error {
	return errNotBuilt
}
func (s *Server) FailWorkflowTask(ctx context.Context, r api.FailWorkflowTaskRequest) error {
	return errNotBuilt
}

// WI-019: activity tasks (poll, complete, fail).
func (s *Server) PollActivityTask(ctx context.Context, r api.PollRequest) (*api.ActivityTask, error) {
	return nil, errNotBuilt
}
func (s *Server) CompleteActivityTask(ctx context.Context, r api.CompleteActivityTaskRequest) error {
	return errNotBuilt
}
func (s *Server) FailActivityTask(ctx context.Context, r api.FailActivityTaskRequest) error {
	return errNotBuilt
}

// Settle runs transfer and timer processing for every owned shard at the current time until
// there is nothing left to do (WI-020 transfer, WI-021 timers). Production runs the same
// processing in background loops; tests and the simulator call Settle directly.
func (s *Server) Settle(ctx context.Context) {
	s.settleTransfer(ctx)
}
