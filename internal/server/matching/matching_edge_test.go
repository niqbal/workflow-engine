package matching

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/niqbal/workflow-engine/internal/simrt"
	"github.com/niqbal/workflow-engine/internal/xsync"
)

// Several pollers wait; a burst of tasks arrives in one step. Every waiter must get exactly one
// task at the time of the burst (no lost wakeups), across many seeds.
func TestBurstWakesEveryWaiter(t *testing.T) {
	for seed := uint64(1); seed <= 30; seed++ {
		rt := simrt.New(seed, simrt.Start)
		m := New(rt)
		const n = 5
		res := make([]pollResult, n)
		for i := range res {
			pollIn(rt, m, WorkflowTasks, "q", time.Minute, &res[i])
		}
		rt.Go("burst", func(context.Context) {
			rt.Sleep(5 * time.Second)
			for id := int64(1); id <= n; id++ {
				m.Add(WorkflowTasks, "q", task(id))
			}
		})
		rt.Run(time.Time{}, nil)
		seen := map[int64]bool{}
		for i, r := range res {
			if !r.ok {
				t.Fatalf("seed %d: poller %d got nothing after %v", seed, i, r.waited)
			}
			if r.waited != 5*time.Second {
				t.Fatalf("seed %d: poller %d returned after %v, want 5s", seed, i, r.waited)
			}
			if seen[r.task.ID] {
				t.Fatalf("seed %d: task %d delivered twice", seed, r.task.ID)
			}
			seen[r.task.ID] = true
		}
		if l := m.Len(WorkflowTasks, "q"); l != 0 {
			t.Fatalf("seed %d: %d tasks left queued", seed, l)
		}
		rt.Close()
	}
}

// A poller that timed out must not swallow a task added afterwards; the task stays queued
// for the next poll, which returns it at once.
func TestExpiredPollDoesNotConsumeLaterTask(t *testing.T) {
	rt := simrt.New(11, simrt.Start)
	defer rt.Close()
	m := New(rt)
	var first, second pollResult
	pollIn(rt, m, ActivityTasks, "q", time.Second, &first)
	rt.Go("later", func(context.Context) {
		rt.Sleep(3 * time.Second)
		m.Add(ActivityTasks, "q", task(9))
		rt.Sleep(time.Second)
		pollIn(rt, m, ActivityTasks, "q", time.Minute, &second)
	})
	rt.Run(time.Time{}, nil)
	if !first.didPoll || first.ok || first.waited != time.Second {
		t.Fatalf("first poll: ok=%v waited=%v; want empty after 1s", first.ok, first.waited)
	}
	if !second.ok || second.task.ID != 9 || second.waited != 0 {
		t.Fatalf("second poll: %+v ok=%v waited=%v; want task 9 at once", second.task, second.ok, second.waited)
	}
}

// Pollers with different deadlines on an empty queue each return at their own deadline.
func TestEachPollerHonoursItsOwnDeadline(t *testing.T) {
	rt := simrt.New(12, simrt.Start)
	defer rt.Close()
	m := New(rt)
	waits := []time.Duration{3 * time.Second, time.Second, 2 * time.Second}
	res := make([]pollResult, len(waits))
	for i, w := range waits {
		pollIn(rt, m, WorkflowTasks, "q", w, &res[i])
	}
	rt.Run(time.Time{}, nil)
	for i, r := range res {
		if r.ok || r.waited != waits[i] {
			t.Fatalf("poller %d: ok=%v waited=%v, want empty after %v", i, r.ok, r.waited, waits[i])
		}
	}
}

// A waiting poller on one queue/kind is not woken by tasks for another queue or kind.
func TestWaiterIgnoresOtherQueuesAndKinds(t *testing.T) {
	rt := simrt.New(13, simrt.Start)
	defer rt.Close()
	m := New(rt)
	var r pollResult
	pollIn(rt, m, WorkflowTasks, "q", 10*time.Second, &r)
	rt.Go("noise", func(context.Context) {
		for i := 0; i < 5; i++ {
			rt.Sleep(time.Second)
			m.Add(ActivityTasks, "q", task(int64(i)))
			m.Add(WorkflowTasks, fmt.Sprintf("q%d", i), task(int64(10+i)))
		}
		rt.Sleep(2 * time.Second)
		m.Add(WorkflowTasks, "q", task(99))
	})
	rt.Run(time.Time{}, nil)
	if !r.ok || r.task.ID != 99 || r.waited != 7*time.Second {
		t.Fatalf("got %+v ok=%v after %v, want task 99 after 7s", r.task, r.ok, r.waited)
	}
	if m.Len(ActivityTasks, "q") != 5 {
		t.Fatalf("activity queue has %d, want 5", m.Len(ActivityTasks, "q"))
	}
}

// Production runtime: queued tasks come back in FIFO order without waiting, and a past
// deadline does not block. No wall-clock waits are involved.
func TestRealRuntimeNonBlockingPaths(t *testing.T) {
	rt := xsync.NewReal(context.Background())
	m := New(rt)
	if _, ok := m.Poll(WorkflowTasks, "q", time.Time{}); ok {
		t.Fatal("empty queue returned a task")
	}
	m.Add(WorkflowTasks, "q", task(1))
	m.Add(WorkflowTasks, "q", task(2))
	far := rt.Clock().Now().Add(time.Hour)
	for _, want := range []int64{1, 2} {
		got, ok := m.Poll(WorkflowTasks, "q", far)
		if !ok || got.ID != want {
			t.Fatalf("got %d ok=%v, want %d", got.ID, ok, want)
		}
	}
	m.Add(ActivityTasks, "q", task(3))
	m.Drop()
	if _, ok := m.Poll(ActivityTasks, "q", rt.Clock().Now().Add(-time.Second)); ok {
		t.Fatal("task survived Drop")
	}
}

func TestKindString(t *testing.T) {
	if WorkflowTasks.String() != "workflow" || ActivityTasks.String() != "activity" || Kind(7).String() != "Kind(7)" {
		t.Fatal("unexpected Kind strings")
	}
}
