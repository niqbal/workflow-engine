package matching

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/niqbal/workflow-engine/internal/simrt"
	"github.com/niqbal/workflow-engine/internal/store"
)

func task(id int64) store.Task {
	return store.Task{Shard: 1, Kind: store.TransferTask, ID: id, Run: store.RunKey{Shard: 1, WorkflowID: fmt.Sprintf("wf%d", id)}}
}

type pollResult struct {
	task    store.Task
	ok      bool
	waited  time.Duration
	didPoll bool
}

// pollIn starts a task that polls once with a deadline wait from its start time.
func pollIn(rt *simrt.Runtime, m *Matcher, kind Kind, queue string, wait time.Duration, out *pollResult) {
	rt.Go("poller", func(context.Context) {
		begin := rt.Now()
		out.task, out.ok = m.Poll(kind, queue, begin.Add(wait))
		out.waited = rt.Now().Sub(begin)
		out.didPoll = true
	})
}

func TestPollReturnsAtOnceWhenTaskQueued(t *testing.T) {
	rt := simrt.New(1, simrt.Start)
	defer rt.Close()
	m := New(rt)
	m.Add(WorkflowTasks, "q", task(7))
	var r pollResult
	pollIn(rt, m, WorkflowTasks, "q", time.Minute, &r)
	if res := rt.Run(time.Time{}, nil); res != simrt.Idle {
		t.Fatalf("run ended with %v", res)
	}
	if !r.ok || r.task.ID != 7 {
		t.Fatalf("got %+v ok=%v, want task 7", r.task, r.ok)
	}
	if r.waited != 0 || !rt.Now().Equal(simrt.Start) {
		t.Fatalf("poll waited %v (clock at %v); want an immediate return", r.waited, rt.Now())
	}
}

func TestPollWaitsUntilTaskAdded(t *testing.T) {
	rt := simrt.New(2, simrt.Start)
	defer rt.Close()
	m := New(rt)
	var r pollResult
	pollIn(rt, m, ActivityTasks, "q", time.Minute, &r)
	rt.Go("pusher", func(context.Context) {
		rt.Sleep(10 * time.Second)
		m.Add(ActivityTasks, "q", task(3))
	})
	if res := rt.Run(time.Time{}, nil); res != simrt.Idle {
		t.Fatalf("run ended with %v", res)
	}
	if !r.ok || r.task.ID != 3 {
		t.Fatalf("got %+v ok=%v, want task 3", r.task, r.ok)
	}
	if r.waited != 10*time.Second {
		t.Fatalf("poll returned after %v, want 10s (when the task was added)", r.waited)
	}
	if got := rt.Now().Sub(simrt.Start); got != 10*time.Second {
		t.Fatalf("clock advanced %v; the cancelled poll deadline must not advance it", got)
	}
}

func TestPollReturnsEmptyAtDeadline(t *testing.T) {
	rt := simrt.New(3, simrt.Start)
	defer rt.Close()
	m := New(rt)
	m.Add(ActivityTasks, "q", task(1))     // other kind
	m.Add(WorkflowTasks, "other", task(2)) // other queue
	var r pollResult
	pollIn(rt, m, WorkflowTasks, "q", 30*time.Second, &r)
	if res := rt.Run(time.Time{}, nil); res != simrt.Idle {
		t.Fatalf("run ended with %v", res)
	}
	if !r.didPoll || r.ok {
		t.Fatalf("didPoll=%v ok=%v task=%+v; want an empty poll", r.didPoll, r.ok, r.task)
	}
	if r.waited != 30*time.Second {
		t.Fatalf("empty poll returned after %v, want exactly the 30s deadline", r.waited)
	}
	if m.Len(ActivityTasks, "q") != 1 || m.Len(WorkflowTasks, "other") != 1 {
		t.Fatal("poll consumed a task from another kind or queue")
	}
}

func TestPollWithPastDeadlineDoesNotBlock(t *testing.T) {
	rt := simrt.New(4, simrt.Start)
	defer rt.Close()
	m := New(rt)
	// Outside any task: a blocking call would panic, so this proves Poll does not wait.
	if _, ok := m.Poll(WorkflowTasks, "q", time.Time{}); ok {
		t.Fatal("empty queue returned a task")
	}
	if _, ok := m.Poll(WorkflowTasks, "q", rt.Now()); ok {
		t.Fatal("empty queue returned a task")
	}
	m.Add(WorkflowTasks, "q", task(5))
	if got, ok := m.Poll(WorkflowTasks, "q", rt.Now().Add(-time.Second)); !ok || got.ID != 5 {
		t.Fatalf("got %+v ok=%v, want task 5 without waiting", got, ok)
	}
}

func TestFIFOPerKindAndQueue(t *testing.T) {
	rt := simrt.New(5, simrt.Start)
	defer rt.Close()
	m := New(rt)
	for id := int64(1); id <= 3; id++ {
		m.Add(WorkflowTasks, "q", task(id))
		m.Add(ActivityTasks, "q", task(10+id))
		m.Add(WorkflowTasks, "other", task(20+id))
	}
	check := func(kind Kind, queue string, want ...int64) {
		t.Helper()
		for _, w := range want {
			got, ok := m.Poll(kind, queue, time.Time{})
			if !ok || got.ID != w {
				t.Fatalf("%s/%s: got %d ok=%v, want %d", kind, queue, got.ID, ok, w)
			}
		}
		if _, ok := m.Poll(kind, queue, time.Time{}); ok {
			t.Fatalf("%s/%s: extra task", kind, queue)
		}
	}
	check(ActivityTasks, "q", 11, 12, 13)
	check(WorkflowTasks, "other", 21, 22, 23)
	check(WorkflowTasks, "q", 1, 2, 3)
	check(ActivityTasks, "other")
}

func TestDropDiscardsQueuedTasksButKeepsPollersWaiting(t *testing.T) {
	rt := simrt.New(6, simrt.Start)
	defer rt.Close()
	m := New(rt)
	m.Add(WorkflowTasks, "q", task(1))
	m.Add(ActivityTasks, "q", task(2))
	m.Add(ActivityTasks, "z", task(3))
	m.Drop()
	for _, k := range []Kind{WorkflowTasks, ActivityTasks} {
		for _, q := range []string{"q", "z"} {
			if n := m.Len(k, q); n != 0 {
				t.Fatalf("%s/%s has %d tasks after Drop", k, q, n)
			}
		}
	}
	var r pollResult
	pollIn(rt, m, WorkflowTasks, "q", time.Minute, &r)
	rt.Go("dropper", func(context.Context) {
		rt.Sleep(time.Second)
		m.Drop()
		rt.Sleep(time.Second)
		m.Add(WorkflowTasks, "q", task(4)) // a redispatch after the restart
	})
	rt.Run(time.Time{}, nil)
	if !r.ok || r.task.ID != 4 || r.waited != 2*time.Second {
		t.Fatalf("got %+v ok=%v after %v, want task 4 after 2s", r.task, r.ok, r.waited)
	}
}

func TestUnknownKindPanics(t *testing.T) {
	rt := simrt.New(7, simrt.Start)
	defer rt.Close()
	m := New(rt)
	defer func() {
		if recover() == nil {
			t.Fatal("Add with an unknown kind did not panic")
		}
	}()
	m.Add(Kind(9), "q", task(1))
}
