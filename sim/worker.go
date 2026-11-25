package sim

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"time"

	"github.com/niqbal/workflow-engine/internal/store"

	"github.com/niqbal/workflow-engine/activity"
	"github.com/niqbal/workflow-engine/internal/engine"
	"github.com/niqbal/workflow-engine/internal/history"
	"github.com/niqbal/workflow-engine/internal/server/api"
	"github.com/niqbal/workflow-engine/wf"
)

// The simulated worker polls one node's queues from simulator tasks. It does not use the
// production worker: that one starts real goroutines and waits on the wall clock, which
// the deterministic scheduler cannot gate.

type activityFunc func(ctx context.Context, input json.RawMessage) (json.RawMessage, error)

type registry struct {
	workflows  map[string]engine.Workflow
	activities map[string]activityFunc
}

func newRegistry() *registry {
	return &registry{workflows: map[string]engine.Workflow{}, activities: map[string]activityFunc{}}
}

func (r *registry) RegisterWorkflow(workflowType string, fn any) error {
	if workflowType == "" {
		return errors.New("sim: workflow type is required")
	}
	adapted, err := wf.Adapt(fn)
	if err != nil {
		return err
	}
	if _, ok := r.workflows[workflowType]; ok {
		return fmt.Errorf("sim: workflow type %q already registered", workflowType)
	}
	r.workflows[workflowType] = adapted
	return nil
}

func (r *registry) RegisterActivity(activityType string, fn any) error {
	if activityType == "" {
		return errors.New("sim: activity type is required")
	}
	adapted, err := adaptActivity(fn)
	if err != nil {
		return err
	}
	if _, ok := r.activities[activityType]; ok {
		return fmt.Errorf("sim: activity type %q already registered", activityType)
	}
	r.activities[activityType] = adapted
	return nil
}

// workflowLoop polls node index for workflow tasks. It resolves the node's service on every
// pass, so a worker follows the node through a crash and restart instead of holding a dead
// incarnation; errors from an incarnation that is going away are retried, not fatal.
func (c *Cluster) workflowLoop(ctx context.Context, index int, reg *registry, queue, id string) {
	poll := api.PollRequest{TaskQueue: queue, Identity: id}
	for {
		svc := c.waitForService(index)
		if svc == nil {
			return
		}
		task, err := svc.PollWorkflowTask(ctx, poll)
		if err != nil {
			if c.transient(err, index) {
				continue
			}
			c.fail(fmt.Errorf("sim: poll workflow %s: %w", id, err))
			return
		}
		if task == nil {
			continue
		}
		// The history the server served is an observation of it (I2).
		c.observe(task.WorkflowID, task.RunID, "task:"+id, task.History)
		if err := handleWorkflowTask(ctx, svc, reg, queue, id, task); err != nil {
			if c.transient(err, index) {
				continue
			}
			c.fail(err)
			return
		}
	}
}

func (c *Cluster) activityLoop(ctx context.Context, index int, reg *registry, queue, id string) {
	poll := api.PollRequest{TaskQueue: queue, Identity: id}
	for {
		svc := c.waitForService(index)
		if svc == nil {
			return
		}
		task, err := svc.PollActivityTask(ctx, poll)
		if err != nil {
			if c.transient(err, index) {
				continue
			}
			c.fail(fmt.Errorf("sim: poll activity %s: %w", id, err))
			return
		}
		if task == nil {
			continue
		}
		if err := c.handleActivityTask(ctx, svc, reg, id, task); err != nil {
			if c.transient(err, index) {
				continue
			}
			c.fail(err)
			return
		}
	}
}

// waitForService returns the node's current frontend, waiting out a crash. It returns nil
// only when the run is halting.
func (c *Cluster) waitForService(index int) api.Service {
	for {
		if c.halt {
			return nil
		}
		if svc := c.serviceFor(index); svc != nil {
			return svc
		}
		c.rt.Sleep(10 * time.Millisecond)
	}
}

// transient reports whether err is what a worker sees when the incarnation it was talking to
// went away, or when an injected store fault hit the call. The worker retries those against
// whatever incarnation is current; anything else is a bug and fails the run.
func (c *Cluster) transient(err error, index int) bool {
	if err == nil || c.halt {
		return false
	}
	switch {
	case errors.Is(err, errInjected), errors.Is(err, errAmbiguous), errors.Is(err, errDown):
		return true
	case errors.Is(err, store.ErrFenced), errors.Is(err, store.ErrConflict):
		// The node lost its shard, or another writer got there first.
		return true
	case errors.Is(err, api.ErrStaleTask), errors.Is(err, api.ErrNotFound):
		return true
	case errors.Is(err, api.ErrNotOwner):
		// The node lost the shard between serving this task and hearing about it. A real
		// cluster's frontend forwards the call to the new owner (WI-051); here the worker
		// polls again, which reaches whoever owns it now.
		return true
	case errors.Is(err, context.Canceled):
		// The incarnation's loops were stopped under this call.
		return true
	}
	// A call that raced a crash on this node is transient too, however it failed.
	return c.slots[index].server() == nil
}

func handleWorkflowTask(ctx context.Context, svc api.Service, reg *registry, queue, id string, task *api.WorkflowTask) error {
	fn, ok := reg.workflows[task.WorkflowType]
	if !ok {
		return failWorkflow(ctx, svc, id, task.TaskToken, history.Failure{
			Type:    "ReplayError",
			Message: fmt.Sprintf("workflow type %q not registered", task.WorkflowType),
		})
	}
	cmds, err := engine.Replay(engine.Info{
		WorkflowID:   task.WorkflowID,
		RunID:        task.RunID,
		WorkflowType: task.WorkflowType,
		TaskQueue:    queue,
	}, task.History, fn)
	if err != nil {
		return failWorkflow(ctx, svc, id, task.TaskToken, workflowFailure(err))
	}
	err = svc.CompleteWorkflowTask(ctx, api.CompleteWorkflowTaskRequest{
		TaskToken: task.TaskToken,
		Commands:  cmds,
		Identity:  id,
	})
	if errors.Is(err, api.ErrStaleTask) || errors.Is(err, api.ErrNotFound) {
		return nil
	}
	return err
}

func failWorkflow(ctx context.Context, svc api.Service, id, token string, cause history.Failure) error {
	err := svc.FailWorkflowTask(ctx, api.FailWorkflowTaskRequest{TaskToken: token, Cause: cause, Identity: id})
	if errors.Is(err, api.ErrStaleTask) || errors.Is(err, api.ErrNotFound) {
		return nil
	}
	return err
}

func workflowFailure(err error) history.Failure {
	typ := "ReplayError"
	var nde *engine.NonDeterministicError
	if errors.As(err, &nde) {
		typ = "NonDeterministicError"
	}
	return history.Failure{Type: typ, Message: err.Error()}
}

func (c *Cluster) handleActivityTask(ctx context.Context, svc api.Service, reg *registry, id string, task *api.ActivityTask) error {
	fn, ok := reg.activities[task.ActivityType]
	if !ok {
		return failActivity(ctx, svc, id, task.TaskToken, history.Failure{
			Type:         "ActivityError",
			Message:      fmt.Sprintf("activity %q not registered", task.ActivityType),
			NonRetryable: true,
		})
	}
	// No wall-clock deadline: activity code runs inline on the simulator task.
	actCtx := activity.WithInfo(ctx, activity.Info{
		WorkflowID:     task.WorkflowID,
		RunID:          task.RunID,
		ActivityID:     task.ActivityID,
		ActivityType:   task.ActivityType,
		Attempt:        task.Attempt,
		IdempotencyKey: task.IdempotencyKey,
		Deadline:       task.Deadline,
	})
	result, err := runActivity(fn, actCtx, task.Input)
	// The worker dies here, after the activity's side effect and before the server hears
	// about it (SPEC §11.2). The attempt is lost, the activity times out and runs again,
	// and the idempotency key is what keeps the effect at one (G4, I5).
	if c.faults.workerCrashes(c.rt.Rand()) {
		c.rt.Tracef("fault worker-crash %s %s attempt %d", id, task.ActivityType, task.Attempt)
		return nil
	}
	if err != nil {
		return failActivity(ctx, svc, id, task.TaskToken, history.Failure{
			Type:    "ActivityError",
			Message: err.Error(),
		})
	}
	err = svc.CompleteActivityTask(ctx, api.CompleteActivityTaskRequest{
		TaskToken: task.TaskToken,
		Result:    result,
		Identity:  id,
	})
	if errors.Is(err, api.ErrStaleTask) || errors.Is(err, api.ErrNotFound) {
		return nil
	}
	return err
}

func failActivity(ctx context.Context, svc api.Service, id, token string, failure history.Failure) error {
	err := svc.FailActivityTask(ctx, api.FailActivityTaskRequest{TaskToken: token, Failure: failure, Identity: id})
	if errors.Is(err, api.ErrStaleTask) || errors.Is(err, api.ErrNotFound) {
		return nil
	}
	return err
}

func runActivity(fn activityFunc, ctx context.Context, input json.RawMessage) (out json.RawMessage, err error) {
	defer func() {
		if r := recover(); r != nil {
			err = fmt.Errorf("panic: %v", r)
		}
	}()
	return fn(ctx, input)
}

var (
	contextType = reflect.TypeOf((*context.Context)(nil)).Elem()
	errorType   = reflect.TypeOf((*error)(nil)).Elem()
)

func adaptActivity(fn any) (activityFunc, error) {
	if fn == nil {
		return nil, errors.New("sim: activity is nil")
	}
	v := reflect.ValueOf(fn)
	t := v.Type()
	if t.Kind() != reflect.Func {
		return nil, errors.New("sim: activity must be a function")
	}
	hasCtx := false
	var inputType reflect.Type
	switch t.NumIn() {
	case 0:
	case 1:
		if t.In(0) == contextType {
			hasCtx = true
		} else {
			inputType = t.In(0)
		}
	case 2:
		if t.In(0) != contextType {
			return nil, errors.New("sim: 2-arg activity must be func(context.Context, In)")
		}
		hasCtx = true
		inputType = t.In(1)
	default:
		return nil, errors.New("sim: activity must take 0, 1 or 2 arguments")
	}
	if t.NumOut() < 1 || t.NumOut() > 2 || t.Out(t.NumOut()-1) != errorType {
		return nil, errors.New("sim: activity must return error or (Out, error)")
	}
	return func(ctx context.Context, input json.RawMessage) (json.RawMessage, error) {
		args := make([]reflect.Value, 0, t.NumIn())
		if hasCtx {
			args = append(args, reflect.ValueOf(ctx))
		}
		if inputType != nil {
			p := reflect.New(inputType)
			if len(input) > 0 && string(input) != "null" {
				if err := json.Unmarshal(input, p.Interface()); err != nil {
					return nil, fmt.Errorf("sim: decode activity input: %w", err)
				}
			}
			args = append(args, p.Elem())
		}
		out := v.Call(args)
		if e := out[len(out)-1]; !e.IsNil() {
			return nil, e.Interface().(error)
		}
		if len(out) == 2 {
			b, err := json.Marshal(out[0].Interface())
			if err != nil {
				return nil, fmt.Errorf("sim: encode activity result: %w", err)
			}
			return b, nil
		}
		return nil, nil
	}, nil
}
