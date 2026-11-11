// Package wf is the workflow SDK (SPEC §3.1). Workflow functions have one of these shapes:
//
//	func(ctx wf.Context, in In) (Out, error)
//	func(ctx wf.Context, in In) error
//	func(ctx wf.Context) (Out, error)
//	func(ctx wf.Context) error
//
// Workflow code must be deterministic (SPEC §3.3): use this package for time, timers,
// concurrency and side effects, never the standard library equivalents.
package wf

import (
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"time"

	"github.com/niqbal/workflow-engine/internal/engine"
	"github.com/niqbal/workflow-engine/internal/history"
)

// Context is passed to workflow code and to every SDK call.
type Context struct{ w *engine.WorkflowContext }

// RetryPolicy controls activity retries. Zero fields take defaults (1s initial, x2, 100x cap).
type RetryPolicy = history.RetryPolicy

// ActivityOptions configure one activity call. StartToCloseTimeout is required.
type ActivityOptions struct {
	TaskQueue              string
	ScheduleToCloseTimeout time.Duration
	StartToCloseTimeout    time.Duration
	RetryPolicy            RetryPolicy
}

// ErrCanceled is returned by a canceled timer.
var ErrCanceled = engine.ErrCanceled

// ActivityError is returned when an activity fails or times out for good.
type ActivityError = engine.ActivityError

// Info identifies the running workflow.
type Info = engine.Info

// Future is a result that becomes ready once.
type Future interface {
	// Get blocks until ready and decodes the JSON result into out (out may be nil).
	Get(ctx Context, out any) error
	IsReady() bool
	inner() *engine.Future
}

type future struct{ f *engine.Future }

func (f future) inner() *engine.Future { return f.f }
func (f future) IsReady() bool         { return f.f.IsReady() }
func (f future) Get(ctx Context, out any) error {
	v, err := f.f.Get(ctx.w.Ctx)
	if err != nil || out == nil || v == nil {
		return err
	}
	raw, ok := v.(json.RawMessage)
	if !ok || len(raw) == 0 {
		return nil
	}
	return json.Unmarshal(raw, out)
}

// ExecuteActivity schedules an activity and returns its future.
func ExecuteActivity(ctx Context, activity string, input any, opts ActivityOptions) Future {
	b, err := json.Marshal(input)
	if err != nil {
		panic(fmt.Sprintf("wf: activity %s input: %v", activity, err))
	}
	return future{ctx.w.ScheduleActivity(history.ScheduleActivity{ActivityType: activity, Input: b, TaskQueue: opts.TaskQueue,
		StartToCloseTimeout: opts.StartToCloseTimeout, ScheduleToCloseTimeout: opts.ScheduleToCloseTimeout, RetryPolicy: opts.RetryPolicy})}
}

// Timer is a durable timer's future that can also be canceled.
type Timer interface {
	Future
	// Cancel stops a pending timer; its Get then returns ErrCanceled.
	Cancel()
}

type timer struct {
	future
	id string
	w  *engine.WorkflowContext
}

func (t timer) Cancel() { t.w.CancelTimer(t.id) }

// NewTimer starts a durable timer.
func NewTimer(ctx Context, d time.Duration) Timer {
	id, f := ctx.w.StartTimer(d)
	return timer{future: future{f}, id: id, w: ctx.w}
}

// Sleep blocks the workflow for d (durably: it survives crashes and restarts).
func Sleep(ctx Context, d time.Duration) error { return NewTimer(ctx, d).Get(ctx, nil) }

// Now returns the current workflow time: the start time of the current workflow task.
func Now(ctx Context) time.Time { return ctx.w.Now() }

// GetInfo returns the workflow's identity.
func GetInfo(ctx Context) Info { return ctx.w.Info() }

// IsReplaying reports whether code is being replayed from history (useful to silence logs).
func IsReplaying(ctx Context) bool { return ctx.w.IsReplaying() }

// Go starts a workflow coroutine (never a goroutine).
func Go(ctx Context, fn func(Context)) {
	ctx.w.Go("wf.Go", func(w *engine.WorkflowContext) { fn(Context{w}) })
}

// ReceiveChannel delivers signals.
type ReceiveChannel interface {
	// Receive blocks for the next value and decodes it into out.
	Receive(ctx Context, out any) bool
	// ReceiveAsync takes a value if one is waiting.
	ReceiveAsync(out any) bool
	Len() int
	inner() *engine.Channel
}

type channel struct{ ch *engine.Channel }

func (c channel) inner() *engine.Channel { return c.ch }
func (c channel) Len() int               { return c.ch.Len() }

func decode(v any, out any) {
	if raw, ok := v.(json.RawMessage); ok && out != nil && len(raw) > 0 {
		if err := json.Unmarshal(raw, out); err != nil {
			panic(fmt.Sprintf("wf: decode signal: %v", err))
		}
	}
}

func (c channel) Receive(ctx Context, out any) bool {
	v, ok := c.ch.Receive(ctx.w.Ctx)
	decode(v, out)
	return ok
}

func (c channel) ReceiveAsync(out any) bool {
	v, ok := c.ch.ReceiveAsync()
	decode(v, out)
	return ok
}

// GetSignalChannel returns the channel for signals named name.
func GetSignalChannel(ctx Context, name string) ReceiveChannel {
	return channel{ctx.w.SignalChannel(name)}
}

// Selector waits for the first of several futures or channels. Cases are checked in the
// order they were added; a future case fires once.
type Selector interface {
	AddFuture(f Future, fn func(Future)) Selector
	AddReceive(ch ReceiveChannel, fn func(ReceiveChannel)) Selector
	Select(ctx Context)
}

type selector struct{ s *engine.Selector }

// NewSelector returns an empty selector.
func NewSelector(ctx Context) Selector { return selector{engine.NewSelector()} }

func (s selector) AddFuture(f Future, fn func(Future)) Selector {
	s.s.AddFuture(f.inner(), func(*engine.Future) { fn(f) })
	return s
}

func (s selector) AddReceive(ch ReceiveChannel, fn func(ReceiveChannel)) Selector {
	s.s.AddReceive(ch.inner(), func(*engine.Channel) { fn(ch) })
	return s
}

func (s selector) Select(ctx Context) { s.s.Select(ctx.w.Ctx) }

// Value is a recorded side-effect result.
type Value struct{ raw json.RawMessage }

// Get decodes the value into out.
func (v Value) Get(out any) error { return json.Unmarshal(v.raw, out) }

// SideEffect runs fn once, records its result in history, and returns the recorded result on
// every replay. Use it for random IDs and other non-deterministic reads.
func SideEffect(ctx Context, fn func() any) Value {
	raw, err := ctx.w.SideEffect(func() (json.RawMessage, error) { return json.Marshal(fn()) })
	if err != nil {
		panic(fmt.Sprintf("wf: side effect: %v", err))
	}
	return Value{raw}
}

// ---- registration -------------------------------------------------------------------------

var ctxType = reflect.TypeOf(Context{})
var errType = reflect.TypeOf((*error)(nil)).Elem()

// Adapt turns a workflow function of a supported shape into an engine.Workflow.
func Adapt(fn any) (engine.Workflow, error) {
	v := reflect.ValueOf(fn)
	t := v.Type()
	if t.Kind() != reflect.Func || t.NumIn() < 1 || t.NumIn() > 2 || t.In(0) != ctxType {
		return nil, errors.New("wf: workflow must be func(wf.Context[, In]) ([Out, ]error)")
	}
	if t.NumOut() < 1 || t.NumOut() > 2 || t.Out(t.NumOut()-1) != errType {
		return nil, errors.New("wf: workflow must return error or (Out, error)")
	}
	return func(w *engine.WorkflowContext, input json.RawMessage) (json.RawMessage, error) {
		args := []reflect.Value{reflect.ValueOf(Context{w})}
		if t.NumIn() == 2 {
			p := reflect.New(t.In(1))
			if len(input) > 0 && string(input) != "null" {
				if err := json.Unmarshal(input, p.Interface()); err != nil {
					return nil, fmt.Errorf("wf: decode workflow input: %w", err)
				}
			}
			args = append(args, p.Elem())
		}
		out := v.Call(args)
		if e := out[len(out)-1]; !e.IsNil() {
			return nil, e.Interface().(error)
		}
		if len(out) == 2 {
			return json.Marshal(out[0].Interface())
		}
		return nil, nil
	}, nil
}

// MustAdapt is Adapt that panics on a bad signature.
func MustAdapt(fn any) engine.Workflow {
	w, err := Adapt(fn)
	if err != nil {
		panic(err)
	}
	return w
}
