// Package xsync is the concurrency toolkit for server code (SPEC §6.1, D9).
//
// Server packages never use goroutines, channels, select or package sync directly. They use a
// Runtime: in production a thin layer over the standard library; under simulation (WI-010)
// a gate that lets exactly one task run at a time and switches only at yield points
// (Clock waits, Transport calls, Store calls and the blocking calls in this package).
package xsync

import (
	"cmp"
	"context"
	"slices"
	"time"

	"github.com/niqbal/workflow-engine/internal/clock"
)

// Locker is a mutex. Under simulation, Lock is a yield point.
type Locker interface {
	Lock()
	Unlock()
}

// Cond is a condition variable bound to a Locker.
type Cond interface {
	// Wait atomically unlocks, waits for Signal/Broadcast, and relocks.
	Wait()
	// WaitUntil is Wait with a deadline on the Runtime's clock. It reports false on timeout.
	WaitUntil(deadline time.Time) bool
	Signal()
	Broadcast()
}

// Runtime creates tasks and synchronization primitives.
type Runtime interface {
	// Go starts a tracked task. name appears in simulation traces.
	Go(name string, fn func(ctx context.Context))
	NewLocker() Locker
	NewCond(l Locker) Cond
	Clock() clock.Clock
	// Yield lets other tasks run. Store and Transport implementations call it on entry, which
	// is what makes every Store call a yield point under simulation. No-op in production.
	Yield()
}

// SortedKeys returns a map's keys in ascending order. Server code iterates maps only through it.
func SortedKeys[K cmp.Ordered, V any](m map[K]V) []K {
	keys := make([]K, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	slices.Sort(keys)
	return keys
}

// Chan is an unbounded FIFO queue built on a Locker and Cond, so it behaves the same under
// the real runtime and the simulator.
type Chan[T any] struct {
	l      Locker
	c      Cond
	items  []T
	closed bool
}

// NewChan returns an empty queue.
func NewChan[T any](rt Runtime) *Chan[T] {
	l := rt.NewLocker()
	return &Chan[T]{l: l, c: rt.NewCond(l)}
}

// Send appends v. It never blocks. Sending on a closed Chan is a no-op.
func (q *Chan[T]) Send(v T) {
	q.l.Lock()
	defer q.l.Unlock()
	if q.closed {
		return
	}
	q.items = append(q.items, v)
	q.c.Signal()
}

// Receive waits until an item is available, the deadline passes, or the queue closes.
// A zero deadline waits forever. ok is false on timeout or close.
func (q *Chan[T]) Receive(deadline time.Time) (v T, ok bool) {
	q.l.Lock()
	defer q.l.Unlock()
	for len(q.items) == 0 && !q.closed {
		if deadline.IsZero() {
			q.c.Wait()
		} else if !q.c.WaitUntil(deadline) {
			break
		}
	}
	if len(q.items) == 0 {
		return v, false
	}
	v, q.items = q.items[0], q.items[1:]
	return v, true
}

// ReceiveCtx is Receive that also returns (ok=false) when ctx is done. It uses
// context.AfterFunc to wake waiters, so callers need no channels. Under simulation, pass a
// context that is never canceled asynchronously.
func (q *Chan[T]) ReceiveCtx(ctx context.Context, deadline time.Time) (v T, ok bool) {
	stop := context.AfterFunc(ctx, func() {
		q.l.Lock()
		q.c.Broadcast()
		q.l.Unlock()
	})
	defer stop()
	q.l.Lock()
	defer q.l.Unlock()
	for len(q.items) == 0 && !q.closed && ctx.Err() == nil {
		if deadline.IsZero() {
			q.c.Wait()
		} else if !q.c.WaitUntil(deadline) {
			break
		}
	}
	if len(q.items) == 0 || ctx.Err() != nil {
		return v, false
	}
	v, q.items = q.items[0], q.items[1:]
	return v, true
}

// TryReceive returns the head item without waiting.
func (q *Chan[T]) TryReceive() (v T, ok bool) {
	q.l.Lock()
	defer q.l.Unlock()
	if len(q.items) == 0 {
		return v, false
	}
	v, q.items = q.items[0], q.items[1:]
	return v, true
}

// Len returns the number of queued items.
func (q *Chan[T]) Len() int {
	q.l.Lock()
	defer q.l.Unlock()
	return len(q.items)
}

// Drain removes and returns every queued item.
func (q *Chan[T]) Drain() []T {
	q.l.Lock()
	defer q.l.Unlock()
	out := q.items
	q.items = nil
	return out
}

// Close wakes all waiters; later Receives return ok=false once the queue is empty.
func (q *Chan[T]) Close() {
	q.l.Lock()
	defer q.l.Unlock()
	q.closed = true
	q.c.Broadcast()
}
