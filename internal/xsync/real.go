package xsync

import (
	"context"
	"sync"
	"time"

	"github.com/niqbal/workflow-engine/internal/clock"
)

// Real is the production Runtime over goroutines and package sync.
type Real struct {
	Ctx context.Context // passed to every task; cancel it to ask tasks to stop
	C   clock.Clock
	wg  sync.WaitGroup
}

// NewReal returns a production Runtime on the wall clock.
func NewReal(ctx context.Context) *Real { return &Real{Ctx: ctx, C: clock.Real{}} }

func (r *Real) Go(name string, fn func(ctx context.Context)) {
	r.wg.Add(1)
	go func() {
		defer r.wg.Done()
		fn(r.Ctx)
	}()
}

// Wait blocks until every task started with Go has returned.
func (r *Real) Wait() { r.wg.Wait() }

func (r *Real) NewLocker() Locker     { return &sync.Mutex{} }
func (r *Real) Clock() clock.Clock    { return r.C }
func (r *Real) NewCond(l Locker) Cond { return &realCond{l: l, clk: r.C, ch: make(chan struct{})} }

// realCond is a broadcast-by-channel condition variable that supports deadlines.
type realCond struct {
	l   Locker
	clk clock.Clock
	mu  sync.Mutex
	ch  chan struct{}
}

func (c *realCond) waiter() chan struct{} {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.ch
}

func (c *realCond) Wait() {
	ch := c.waiter()
	c.l.Unlock()
	<-ch
	c.l.Lock()
}

func (c *realCond) WaitUntil(deadline time.Time) bool {
	ch := c.waiter()
	c.l.Unlock()
	defer c.l.Lock()
	d := deadline.Sub(c.clk.Now())
	if d <= 0 {
		return false
	}
	timeout := make(chan struct{})
	cancel := c.clk.AfterFunc(d, func() { close(timeout) })
	defer cancel()
	select {
	case <-ch:
		return true
	case <-timeout:
		return false
	}
}

// Signal wakes all waiters (spurious wakeups are allowed by the Cond contract; callers loop).
func (c *realCond) Signal() { c.Broadcast() }

func (c *realCond) Broadcast() {
	c.mu.Lock()
	defer c.mu.Unlock()
	close(c.ch)
	c.ch = make(chan struct{})
}
