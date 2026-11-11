package simrt

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/niqbal/workflow-engine/internal/xsync"
)

// busy is a concurrent scenario: producers, consumers, a mutex-protected counter, sleeps,
// timers and receive deadlines all interleave.
func busy(rt *Runtime) {
	q := xsync.NewChan[int](rt)
	mu := rt.NewLocker()
	total := 0
	for p := 0; p < 3; p++ {
		p := p
		rt.Go(fmt.Sprintf("producer%d", p), func(context.Context) {
			for i := 0; i < 5; i++ {
				rt.Sleep(time.Duration(int(rt.Rand().Int63()%5)+1) * time.Millisecond)
				q.Send(p*100 + i)
				rt.Tracef("p%d sent %d", p, i)
			}
		})
	}
	for c := 0; c < 2; c++ {
		c := c
		rt.Go(fmt.Sprintf("consumer%d", c), func(context.Context) {
			for {
				v, ok := q.Receive(rt.Now().Add(50 * time.Millisecond))
				if !ok {
					rt.Tracef("c%d timed out", c)
					return
				}
				mu.Lock()
				total += v
				rt.Tracef("c%d got %d total %d", c, v, total)
				mu.Unlock()
			}
		})
	}
	rt.Clock().AfterFunc(7*time.Millisecond, func() { rt.Tracef("afterfunc fired") })
}

func TestSameSeedSameTrace(t *testing.T) {
	if err := CheckDeterministic(50, 7, time.Hour, busy); err != nil {
		t.Fatal(err)
	}
}

func TestDifferentSeedsExploreDifferentSchedules(t *testing.T) {
	a, _ := RunOnce(1, time.Hour, busy)
	distinct := 0
	for s := uint64(2); s < 12; s++ {
		b, _ := RunOnce(s, time.Hour, busy)
		if diff(a, b) != "" {
			distinct++
		}
	}
	if distinct == 0 {
		t.Fatal("ten seeds produced the same schedule; the scheduler is not exploring")
	}
}

// A planted nondeterminism: tasks are created in map iteration order, which Go randomizes.
func TestPlantedMapIterationIsCaught(t *testing.T) {
	planted := func(rt *Runtime) {
		m := map[string]int{"a": 1, "b": 2, "c": 3, "d": 4, "e": 5, "f": 6}
		for k := range m {
			k := k
			rt.Go("t-"+k, func(context.Context) { rt.Tracef("ran %s", k) })
		}
	}
	if err := CheckDeterministic(50, 3, time.Second, planted); err == nil {
		t.Fatal("map-order dependency was not detected across 50 runs")
	}
}

func TestVirtualTimeIsFast(t *testing.T) {
	start := time.Now()
	trace, err := RunOnce(1, 0, func(rt *Runtime) {
		rt.Go("sleeper", func(context.Context) {
			for d := 0; d < 30; d++ {
				rt.Sleep(24 * time.Hour)
			}
			rt.Tracef("done after 30 days")
		})
	})
	if err != nil {
		t.Fatal(err)
	}
	if last := trace[len(trace)-1]; !strings.Contains(last, "done after 30 days") {
		t.Fatalf("last trace line %q", last)
	}
	if time.Since(start) > 2*time.Second {
		t.Fatalf("30 virtual days took %v of wall time", time.Since(start))
	}
}

func TestCondDeadlineAndMutexExclusion(t *testing.T) {
	rt := New(9, Start)
	defer rt.Close()
	mu := rt.NewLocker()
	c := rt.NewCond(mu)
	var woke, timedOut bool
	inside := 0
	maxInside := 0
	for i := 0; i < 4; i++ {
		rt.Go("crit", func(context.Context) {
			mu.Lock()
			inside++
			if inside > maxInside {
				maxInside = inside
			}
			rt.Yield()
			inside--
			mu.Unlock()
		})
	}
	rt.Go("waiter", func(context.Context) {
		mu.Lock()
		timedOut = !c.WaitUntil(rt.Now().Add(time.Second))
		mu.Unlock()
	})
	rt.Go("waiter2", func(context.Context) {
		mu.Lock()
		woke = c.WaitUntil(rt.Now().Add(time.Hour))
		mu.Unlock()
	})
	rt.Go("signaler", func(context.Context) {
		rt.Sleep(2 * time.Second)
		mu.Lock()
		c.Signal()
		mu.Unlock()
	})
	if res := rt.Run(time.Time{}, nil); res != Idle {
		t.Fatalf("run ended with %v", res)
	}
	if maxInside != 1 {
		t.Fatalf("mutex admitted %d tasks at once", maxInside)
	}
	if !timedOut || !woke {
		t.Fatalf("timedOut=%v woke=%v", timedOut, woke)
	}
	if got := rt.Now().Sub(Start); got != 2*time.Second {
		t.Fatalf("virtual time %v, want 2s (the cancelled 1h deadline must not advance the clock)", got)
	}
}

func TestTaskPanicSurfaces(t *testing.T) {
	_, err := RunOnce(1, time.Second, func(rt *Runtime) {
		rt.Go("bad", func(context.Context) { panic("boom") })
	})
	if err == nil || !strings.Contains(err.Error(), "boom") {
		t.Fatalf("panic not surfaced: %v", err)
	}
}

func TestCloseReleasesParkedTasks(t *testing.T) {
	rt := New(1, Start)
	q := xsync.NewChan[int](rt)
	rt.Go("forever", func(context.Context) { q.Receive(time.Time{}) })
	rt.Run(time.Time{}, nil)
	rt.Close() // must not hang; goroutine exits via Goexit
}
