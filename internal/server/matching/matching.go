// Package matching holds the in-memory task queues of one node (SPEC §6, D4).
//
// Each task queue name has two FIFOs, one for workflow tasks and one for activity tasks. The
// transfer processor pushes outbox tasks with Add; frontends long-poll with Poll, which waits
// on the Runtime's clock until a task arrives or the poll's deadline passes.
//
// Matching is deliberately not durable: Drop discards everything, as a node restart would, and
// redispatch (SPEC §8.2-8.3) re-pushes what was lost. Duplicate delivery is harmless because
// starting a task is a conditional transition in history.
package matching

import (
	"fmt"
	"time"

	"github.com/niqbal/workflow-engine/internal/store"
	"github.com/niqbal/workflow-engine/internal/xsync"
)

// Kind selects which of a task queue's two FIFOs an operation uses.
type Kind int

const (
	WorkflowTasks Kind = iota
	ActivityTasks
)

func (k Kind) String() string {
	switch k {
	case WorkflowTasks:
		return "workflow"
	case ActivityTasks:
		return "activity"
	}
	return fmt.Sprintf("Kind(%d)", int(k))
}

// taskQueue is the pair of FIFOs behind one task queue name.
type taskQueue struct {
	workflow *xsync.Chan[store.Task]
	activity *xsync.Chan[store.Task]
}

func (q *taskQueue) of(k Kind) *xsync.Chan[store.Task] {
	switch k {
	case WorkflowTasks:
		return q.workflow
	case ActivityTasks:
		return q.activity
	}
	panic("matching: unknown task kind " + k.String())
}

// Matcher is the set of task queues on one node. It is safe for concurrent use.
type Matcher struct {
	rt     xsync.Runtime
	mu     xsync.Locker
	queues map[string]*taskQueue
}

// New returns an empty Matcher on rt.
func New(rt xsync.Runtime) *Matcher {
	return &Matcher{rt: rt, mu: rt.NewLocker(), queues: map[string]*taskQueue{}}
}

// queue returns the named task queue, creating it on first use.
func (m *Matcher) queue(name string) *taskQueue {
	m.mu.Lock()
	defer m.mu.Unlock()
	q, ok := m.queues[name]
	if !ok {
		q = &taskQueue{workflow: xsync.NewChan[store.Task](m.rt), activity: xsync.NewChan[store.Task](m.rt)}
		m.queues[name] = q
	}
	return q
}

// Add appends t to the kind FIFO of task queue name and wakes one waiting poller. It never
// blocks on queue capacity.
func (m *Matcher) Add(kind Kind, name string, t store.Task) {
	m.queue(name).of(kind).Send(t)
}

// Poll takes the oldest task from the kind FIFO of task queue name. If none is queued it waits
// until one is added or the deadline passes on the Runtime's clock, and reports ok=false at
// the deadline. A deadline that is not after the current time makes Poll non-blocking, so a
// poll is always bounded by the clock.
func (m *Matcher) Poll(kind Kind, name string, deadline time.Time) (t store.Task, ok bool) {
	c := m.queue(name).of(kind)
	if !deadline.After(m.rt.Clock().Now()) {
		return c.TryReceive()
	}
	return c.Receive(deadline)
}

// Len returns the number of tasks waiting in the kind FIFO of task queue name.
func (m *Matcher) Len(kind Kind, name string) int {
	return m.queue(name).of(kind).Len()
}

// Drop discards every queued task in every task queue, as a node restart would. Pollers that
// are waiting keep waiting and receive tasks added later.
func (m *Matcher) Drop() {
	m.mu.Lock()
	qs := make([]*taskQueue, 0, len(m.queues))
	for _, name := range xsync.SortedKeys(m.queues) {
		qs = append(qs, m.queues[name])
	}
	m.mu.Unlock()
	for _, q := range qs {
		q.workflow.Drain()
		q.activity.Drain()
	}
}
