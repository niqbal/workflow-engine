// Package engine runs workflow code deterministically inside the worker (SPEC §7).
//
// The Dispatcher (WI-012) runs coroutines one at a time in creation order. A coroutine
// yields only at blocking SDK calls (Future.Get, Channel.Receive, Selector.Select). One
// ExecuteUntilAllBlocked call is one deterministic "step": it resumes coroutines in order,
// round after round, until every one of them is blocked or done.
//
// This package is outside the determinism lint's scope: it is the gate itself.
package engine

import (
	"errors"
	"fmt"
	"runtime"
	"strings"
)

// ErrCanceled is returned by blocking calls when the dispatcher is closed.
var errKilled = errors.New("engine: dispatcher closed")

type coroutine struct {
	name    string
	resume  chan bool // true = run, false = killed
	yielded chan struct{}
	done    bool
	blocked func() bool // condition the coroutine is waiting for; nil = runnable
}

// Dispatcher owns the coroutines of one workflow execution.
type Dispatcher struct {
	cs       []*coroutine
	current  *coroutine
	running  bool
	closed   bool
	panicErr error
	trace    []string
	seq      int
}

// Ctx is passed to coroutine functions; blocking calls need it to know who is blocking.
type Ctx struct {
	d *Dispatcher
	c *coroutine
}

// Dispatcher returns the dispatcher this context belongs to.
func (x *Ctx) Dispatcher() *Dispatcher { return x.d }

// NewDispatcher creates a dispatcher whose first coroutine is root.
func NewDispatcher(root func(ctx *Ctx)) *Dispatcher {
	d := &Dispatcher{}
	d.spawn("root", root)
	return d
}

// Go starts a coroutine. It runs after the existing coroutines in the current round.
func (d *Dispatcher) Go(name string, fn func(ctx *Ctx)) { d.spawn(name, fn) }

func (d *Dispatcher) spawn(name string, fn func(ctx *Ctx)) {
	d.seq++
	c := &coroutine{name: fmt.Sprintf("%s#%d", name, d.seq), resume: make(chan bool), yielded: make(chan struct{})}
	d.cs = append(d.cs, c)
	go func() {
		defer func() {
			if p := recover(); p != nil && !d.closed {
				buf := make([]byte, 4096)
				n := runtime.Stack(buf, false)
				d.panicErr = fmt.Errorf("workflow coroutine %s panicked: %v\n%s", c.name, p, buf[:n])
			}
			c.done = true
			c.yielded <- struct{}{}
		}()
		if !<-c.resume {
			runtime.Goexit()
		}
		fn(&Ctx{d: d, c: c})
	}()
}

// Tracef records an application-level event in the trace.
func (d *Dispatcher) Tracef(format string, args ...any) {
	d.trace = append(d.trace, fmt.Sprintf(format, args...))
}

// Trace returns the resume order and Tracef lines.
func (d *Dispatcher) Trace() []string { return append([]string(nil), d.trace...) }

// block parks the calling coroutine until ready() is true. ready is re-checked each round.
func (x *Ctx) block(ready func() bool) {
	if ready() {
		return
	}
	if x.d.current != x.c {
		panic("engine: blocking call from outside the workflow's coroutines")
	}
	x.c.blocked = ready
	x.c.yielded <- struct{}{}
	if !<-x.c.resume {
		runtime.Goexit()
	}
}

// ExecuteUntilAllBlocked runs coroutines until none can make progress. It returns the first
// coroutine panic as an error.
func (d *Dispatcher) ExecuteUntilAllBlocked() error {
	if d.closed {
		return errKilled
	}
	if d.running {
		return errors.New("engine: ExecuteUntilAllBlocked is not reentrant")
	}
	d.running = true
	defer func() { d.running = false }()
	for {
		progressed := false
		for i := 0; i < len(d.cs); i++ { // len re-read: coroutines spawned this round run too
			c := d.cs[i]
			if c.done || (c.blocked != nil && !c.blocked()) {
				continue
			}
			c.blocked = nil
			progressed = true
			d.trace = append(d.trace, "resume "+c.name)
			d.current = c
			c.resume <- true
			<-c.yielded
			d.current = nil
			if d.panicErr != nil {
				return d.panicErr
			}
		}
		if !progressed {
			return nil
		}
	}
}

// AllDone reports whether every coroutine has returned.
func (d *Dispatcher) AllDone() bool {
	for _, c := range d.cs {
		if !c.done {
			return false
		}
	}
	return true
}

// Blocked lists coroutines that are waiting, for diagnostics.
func (d *Dispatcher) Blocked() string {
	var out []string
	for _, c := range d.cs {
		if !c.done {
			out = append(out, c.name)
		}
	}
	return strings.Join(out, ", ")
}

// Close kills every unfinished coroutine; their goroutines exit.
func (d *Dispatcher) Close() {
	if d.closed {
		return
	}
	d.closed = true
	for _, c := range d.cs {
		if !c.done {
			c.resume <- false
			<-c.yielded
		}
	}
}

// ---- Future -------------------------------------------------------------------------------

// Future is a value that becomes ready once.
type Future struct {
	ready bool
	value any
	err   error
}

// NewFuture returns an unready future.
func NewFuture() *Future { return &Future{} }

// Set makes the future ready. Setting twice panics: it means a history bug.
func (f *Future) Set(v any, err error) {
	if f.ready {
		panic("engine: future set twice")
	}
	f.ready, f.value, f.err = true, v, err
}

// IsReady reports whether Set has been called.
func (f *Future) IsReady() bool { return f.ready }

// Get blocks the coroutine until the future is ready.
func (f *Future) Get(ctx *Ctx) (any, error) {
	ctx.block(f.IsReady)
	return f.value, f.err
}

// ---- Channel ------------------------------------------------------------------------------

// Channel is an unbounded FIFO between coroutines, and from history into workflow code
// (signals). Send never blocks.
type Channel struct {
	items  []any
	closed bool
}

// NewChannel returns an empty channel.
func NewChannel() *Channel { return &Channel{} }

// Send appends v.
func (ch *Channel) Send(v any) { ch.items = append(ch.items, v) }

// Close marks the channel closed; Receive returns ok=false once it is empty.
func (ch *Channel) Close() { ch.closed = true }

// Len returns the number of buffered items.
func (ch *Channel) Len() int { return len(ch.items) }

func (ch *Channel) readyToReceive() bool { return len(ch.items) > 0 || ch.closed }

// Receive blocks until an item is available or the channel is closed.
func (ch *Channel) Receive(ctx *Ctx) (any, bool) {
	ctx.block(ch.readyToReceive)
	return ch.ReceiveAsync()
}

// ReceiveAsync returns the head item without blocking.
func (ch *Channel) ReceiveAsync() (any, bool) {
	if len(ch.items) == 0 {
		return nil, false
	}
	v := ch.items[0]
	ch.items = ch.items[1:]
	return v, true
}

// ---- Selector -----------------------------------------------------------------------------

type selectCase struct {
	future *Future
	ch     *Channel
	onF    func(*Future)
	onCh   func(*Channel)
}

// Selector waits on several futures and channels. Select picks the first ready case in the
// order cases were added, which is what makes it deterministic.
type Selector struct {
	cases []selectCase
}

// NewSelector returns an empty selector.
func NewSelector() *Selector { return &Selector{} }

// AddFuture adds a case that fires once f is ready. A future case fires at most once.
func (s *Selector) AddFuture(f *Future, fn func(*Future)) *Selector {
	s.cases = append(s.cases, selectCase{future: f, onF: fn})
	return s
}

// AddReceive adds a case that fires when ch has an item. The callback must receive it.
func (s *Selector) AddReceive(ch *Channel, fn func(*Channel)) *Selector {
	s.cases = append(s.cases, selectCase{ch: ch, onCh: fn})
	return s
}

func (s *Selector) ready() int {
	for i, c := range s.cases {
		if c.future != nil && c.onF != nil && c.future.IsReady() {
			return i
		}
		if c.ch != nil && c.ch.readyToReceive() {
			return i
		}
	}
	return -1
}

// Select blocks until a case is ready, then runs its callback.
func (s *Selector) Select(ctx *Ctx) {
	ctx.block(func() bool { return s.ready() >= 0 })
	i := s.ready()
	c := s.cases[i]
	if c.future != nil {
		fn := c.onF
		s.cases[i].onF = nil // a future fires once
		fn(c.future)
		return
	}
	c.onCh(c.ch)
}
