package worker

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"sync"
	"time"

	"github.com/niqbal/workflow-engine/activity"
	"github.com/niqbal/workflow-engine/internal/engine"
	"github.com/niqbal/workflow-engine/internal/history"
	"github.com/niqbal/workflow-engine/internal/server/api"
	"github.com/niqbal/workflow-engine/wf"
)

// Options configures one worker.
type Options struct {
	TaskQueue string
	Identity  string

	// RetryDelay is how long the worker waits before polling again after a failed call.
	// Zero means one second.
	//
	// A worker does not stop because a call failed. In a cluster, polling is forwarded to
	// whichever node holds the task queue, and that node can be restarting, have just lost
	// the shard, or drop the connection of a long poll it was serving: all of it ordinary,
	// none of it a reason to stop processing work. Only a cancelled context ends Run.
	RetryDelay time.Duration
	// OnError, when set, is called with every error the worker retries. Without it a worker
	// that cannot reach its server is silent, which is worse than noisy.
	OnError func(error)
}

// Worker polls workflow and activity tasks, executes them, and reports results.
type Worker struct {
	svc  api.Service
	opts Options

	mu         sync.RWMutex
	workflows  map[string]engine.Workflow
	activities map[string]activityFunc
}

// New returns a worker over svc.
func New(svc api.Service, opts Options) *Worker {
	if opts.Identity == "" {
		opts.Identity = "worker"
	}
	return &Worker{
		svc:        svc,
		opts:       opts,
		workflows:  map[string]engine.Workflow{},
		activities: map[string]activityFunc{},
	}
}

// RegisterWorkflow registers workflow code under workflowType.
func (w *Worker) RegisterWorkflow(workflowType string, fn any) error {
	if workflowType == "" {
		return errors.New("worker: workflow type is required")
	}
	adapted, err := wf.Adapt(fn)
	if err != nil {
		return err
	}
	w.mu.Lock()
	defer w.mu.Unlock()
	if _, ok := w.workflows[workflowType]; ok {
		return fmt.Errorf("worker: workflow type %q already registered", workflowType)
	}
	w.workflows[workflowType] = adapted
	return nil
}

// RegisterActivity registers activity code under activityType.
func (w *Worker) RegisterActivity(activityType string, fn any) error {
	if activityType == "" {
		return errors.New("worker: activity type is required")
	}
	adapted, err := adaptActivity(fn)
	if err != nil {
		return err
	}
	w.mu.Lock()
	defer w.mu.Unlock()
	if _, ok := w.activities[activityType]; ok {
		return fmt.Errorf("worker: activity type %q already registered", activityType)
	}
	w.activities[activityType] = adapted
	return nil
}

// Run starts workflow and activity poll loops and blocks until ctx is done or an
// unrecoverable call error happens.
func (w *Worker) Run(ctx context.Context) error {
	if w.svc == nil {
		return errors.New("worker: nil service")
	}
	if w.opts.TaskQueue == "" {
		return errors.New("worker: task queue is required")
	}

	runCtx, cancel := context.WithCancel(ctx)
	defer cancel()

	errCh := make(chan error, 2)
	var wg sync.WaitGroup
	wg.Add(2)
	go func() {
		defer wg.Done()
		if err := w.workflowLoop(runCtx); err != nil {
			errCh <- err
			cancel()
		}
	}()
	go func() {
		defer wg.Done()
		if err := w.activityLoop(runCtx); err != nil {
			errCh <- err
			cancel()
		}
	}()
	wg.Wait()
	close(errCh)

	for err := range errCh {
		if err != nil {
			return err
		}
	}
	return nil
}

func (w *Worker) workflowLoop(ctx context.Context) error {
	poll := api.PollRequest{TaskQueue: w.opts.TaskQueue, Identity: w.opts.Identity}
	for {
		if ctx.Err() != nil {
			return nil
		}
		task, err := w.svc.PollWorkflowTask(ctx, poll)
		if err != nil {
			if !w.backoff(ctx, err) {
				return nil
			}
			continue
		}
		if task == nil {
			continue
		}
		if err := w.handleWorkflowTask(ctx, task); err != nil {
			// The task is not lost: the server redispatches one whose outcome it never
			// heard, so retrying the poll is the whole of the recovery.
			if !w.backoff(ctx, err) {
				return nil
			}
		}
	}
}

// backoff reports the error and waits before the next attempt. It returns false when the
// context is done, which is the only thing that ends a worker.
func (w *Worker) backoff(ctx context.Context, err error) bool {
	if ctx.Err() != nil {
		return false
	}
	if w.opts.OnError != nil {
		w.opts.OnError(err)
	}
	delay := w.opts.RetryDelay
	if delay <= 0 {
		delay = time.Second
	}
	t := time.NewTimer(delay)
	defer t.Stop()
	select {
	case <-ctx.Done():
		return false
	case <-t.C:
		return true
	}
}

func (w *Worker) activityLoop(ctx context.Context) error {
	poll := api.PollRequest{TaskQueue: w.opts.TaskQueue, Identity: w.opts.Identity}
	for {
		if ctx.Err() != nil {
			return nil
		}
		task, err := w.svc.PollActivityTask(ctx, poll)
		if err != nil {
			if !w.backoff(ctx, err) {
				return nil
			}
			continue
		}
		if task == nil {
			continue
		}
		if err := w.handleActivityTask(ctx, task); err != nil {
			if !w.backoff(ctx, err) {
				return nil
			}
		}
	}
}

func (w *Worker) handleWorkflowTask(ctx context.Context, task *api.WorkflowTask) error {
	fn, ok := w.workflow(task.WorkflowType)
	if !ok {
		return w.failWorkflowTask(ctx, task.TaskToken, history.Failure{
			Type:    "ReplayError",
			Message: fmt.Sprintf("workflow type %q not registered", task.WorkflowType),
		})
	}
	cmds, err := engine.Replay(engine.Info{
		WorkflowID:   task.WorkflowID,
		RunID:        task.RunID,
		WorkflowType: task.WorkflowType,
		TaskQueue:    w.opts.TaskQueue,
	}, task.History, fn)
	if err != nil {
		return w.failWorkflowTask(ctx, task.TaskToken, workflowFailure(err))
	}
	err = w.svc.CompleteWorkflowTask(ctx, api.CompleteWorkflowTaskRequest{
		TaskToken: task.TaskToken,
		Commands:  cmds,
		Identity:  w.opts.Identity,
	})
	if errors.Is(err, api.ErrStaleTask) || errors.Is(err, api.ErrNotFound) {
		return nil
	}
	return err
}

func (w *Worker) failWorkflowTask(ctx context.Context, taskToken string, cause history.Failure) error {
	err := w.svc.FailWorkflowTask(ctx, api.FailWorkflowTaskRequest{
		TaskToken: taskToken,
		Cause:     cause,
		Identity:  w.opts.Identity,
	})
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

func (w *Worker) handleActivityTask(ctx context.Context, task *api.ActivityTask) error {
	fn, ok := w.activity(task.ActivityType)
	if !ok {
		return w.failActivityTask(ctx, task.TaskToken, history.Failure{
			Type:         "ActivityError",
			Message:      fmt.Sprintf("activity %q not registered", task.ActivityType),
			NonRetryable: true,
		})
	}

	actCtx := ctx
	cancel := func() {}
	if !task.Deadline.IsZero() {
		actCtx, cancel = context.WithDeadline(ctx, task.Deadline)
	}
	defer cancel()

	actCtx = activity.WithInfo(actCtx, activity.Info{
		WorkflowID:     task.WorkflowID,
		RunID:          task.RunID,
		ActivityID:     task.ActivityID,
		ActivityType:   task.ActivityType,
		Attempt:        task.Attempt,
		IdempotencyKey: task.IdempotencyKey,
		Deadline:       task.Deadline,
	})

	result, err := runActivitySafely(fn, actCtx, task.Input)
	if err != nil {
		return w.failActivityTask(ctx, task.TaskToken, history.Failure{
			Type:    "ActivityError",
			Message: err.Error(),
		})
	}

	err = w.svc.CompleteActivityTask(ctx, api.CompleteActivityTaskRequest{
		TaskToken: task.TaskToken,
		Result:    result,
		Identity:  w.opts.Identity,
	})
	if errors.Is(err, api.ErrStaleTask) || errors.Is(err, api.ErrNotFound) {
		return nil
	}
	return err
}

func (w *Worker) failActivityTask(ctx context.Context, taskToken string, failure history.Failure) error {
	err := w.svc.FailActivityTask(ctx, api.FailActivityTaskRequest{
		TaskToken: taskToken,
		Failure:   failure,
		Identity:  w.opts.Identity,
	})
	if errors.Is(err, api.ErrStaleTask) || errors.Is(err, api.ErrNotFound) {
		return nil
	}
	return err
}

type activityFunc func(ctx context.Context, input json.RawMessage) (json.RawMessage, error)

func runActivitySafely(fn activityFunc, ctx context.Context, input json.RawMessage) (out json.RawMessage, err error) {
	defer func() {
		if r := recover(); r != nil {
			err = fmt.Errorf("panic: %v", r)
		}
	}()
	return fn(ctx, input)
}

var contextType = reflect.TypeOf((*context.Context)(nil)).Elem()
var errorType = reflect.TypeOf((*error)(nil)).Elem()

func adaptActivity(fn any) (activityFunc, error) {
	if fn == nil {
		return nil, errors.New("worker: activity is nil")
	}
	v := reflect.ValueOf(fn)
	t := v.Type()
	if t.Kind() != reflect.Func {
		return nil, errors.New("worker: activity must be a function")
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
			return nil, errors.New("worker: 2-arg activity must be func(context.Context, In)")
		}
		hasCtx = true
		inputType = t.In(1)
	default:
		return nil, errors.New("worker: activity must take 0, 1 or 2 arguments")
	}

	if t.NumOut() < 1 || t.NumOut() > 2 || t.Out(t.NumOut()-1) != errorType {
		return nil, errors.New("worker: activity must return error or (Out, error)")
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
					return nil, fmt.Errorf("worker: decode activity input: %w", err)
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
				return nil, fmt.Errorf("worker: encode activity result: %w", err)
			}
			return b, nil
		}
		return nil, nil
	}, nil
}

func (w *Worker) workflow(workflowType string) (engine.Workflow, bool) {
	w.mu.RLock()
	defer w.mu.RUnlock()
	fn, ok := w.workflows[workflowType]
	return fn, ok
}

func (w *Worker) activity(activityType string) (activityFunc, bool) {
	w.mu.RLock()
	defer w.mu.RUnlock()
	fn, ok := w.activities[activityType]
	return fn, ok
}
