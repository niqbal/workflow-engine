// Package simrt is the deterministic runtime (WI-010, SPEC §6.1 and D9).
//
// Every task is a goroutine, but a baton lets exactly one run at a time. A task gives the
// baton back only at a yield point: Yield, Lock, Cond waits, or a clock wait. The scheduler
// picks the next runnable task with a seeded RNG, and when nothing is runnable it jumps the
// virtual clock to the next timer. Same seed and same code give the same schedule, which
// the trace records event by event.
//
// The driver (a test, or sim.Run) calls Go to create tasks and then Run to execute them. Code
// outside any task may call non-blocking operations (Now, Send, Unlock, uncontended Lock);
// a blocking call outside a task panics, because nothing could ever wake it.
package simrt

import (
	"context"
	"fmt"
	"runtime"
	"strings"
	"time"

	"github.com/niqbal/workflow-engine/internal/clock"
	"github.com/niqbal/workflow-engine/internal/rnd"
	"github.com/niqbal/workflow-engine/internal/xsync"
)

type task struct {
	id     int
	name   string
	baton  chan bool // true = run, false = killed
	done   bool
	waking bool // already queued to run
}

type timer struct {
	at     time.Time
	seq    int
	fire   func()
	active bool
}

// Runtime implements xsync.Runtime under a deterministic scheduler.
type Runtime struct {
	now      time.Time
	sched    rnd.Rand // scheduling decisions only
	user     rnd.Rand // handed to the code under test
	runnable []*task
	all      []*task
	timers   []*timer
	seq      int
	current  *task
	parked   chan struct{}
	trace    []string
	steps    int
	killed   bool
	panicVal any
	// MaxSteps bounds one Run call; exceeding it is reported as a livelock.
	MaxSteps int
}

// New returns a runtime at virtual time start.
func New(seed uint64, start time.Time) *Runtime {
	return &Runtime{now: start, sched: rnd.New(seed), user: rnd.New(seed ^ 0xa5a5a5a5),
		parked: make(chan struct{}), MaxSteps: 5_000_000}
}

var _ xsync.Runtime = (*Runtime)(nil)

// Rand is the deterministic RNG for code under test.
func (r *Runtime) Rand() rnd.Rand { return r.user }

// Now returns virtual time.
func (r *Runtime) Now() time.Time { return r.now }

// Clock returns the virtual clock.
func (r *Runtime) Clock() clock.Clock { return simClock{r} }

// Tracef appends a line to the trace. Use it for application-level events worth comparing.
func (r *Runtime) Tracef(format string, args ...any) {
	r.trace = append(r.trace, fmt.Sprintf("%s %s", r.now.Format("15:04:05.000000"), fmt.Sprintf(format, args...)))
}

// Trace returns the schedule and Tracef lines so far.
func (r *Runtime) Trace() []string { return append([]string(nil), r.trace...) }

// Go creates a runnable task. Tasks start in the order they are created.
func (r *Runtime) Go(name string, fn func(ctx context.Context)) {
	t := &task{id: len(r.all) + 1, name: name, baton: make(chan bool)}
	r.all = append(r.all, t)
	r.makeRunnable(t)
	go func() {
		if !<-t.baton {
			r.exit(t)
			return
		}
		defer func() {
			if p := recover(); p != nil && !r.killed {
				buf := make([]byte, 4096)
				n := runtime.Stack(buf, false)
				r.panicVal = fmt.Sprintf("task %s panicked: %v\n%s", t.name, p, buf[:n])
			}
			r.exit(t)
		}()
		fn(context.Background())
	}()
}

func (r *Runtime) exit(t *task) {
	t.done = true
	r.parked <- struct{}{}
}

func (r *Runtime) makeRunnable(t *task) {
	if t.done || t.waking {
		return
	}
	t.waking = true
	r.runnable = append(r.runnable, t)
}

// park hands the baton back and waits for it. The caller has already arranged to be woken.
func (r *Runtime) park() {
	t := r.current
	if t == nil {
		panic("simrt: blocking call outside a task")
	}
	r.parked <- struct{}{}
	if !<-t.baton {
		runtime.Goexit()
	}
}

// Yield lets the scheduler pick any runnable task, possibly this one.
func (r *Runtime) Yield() {
	if r.current == nil {
		return
	}
	r.makeRunnable(r.current)
	r.park()
}

// Result says why Run returned.
type Result int

const (
	Idle     Result = iota // no runnable task and no timer
	Deadline               // the next timer is past the deadline
	Stopped                // stop returned true
)

// Run executes tasks until stop() is true, nothing is left to do, or virtual time would pass
// until. A zero until means no time limit. It panics with the task's panic, or on livelock.
func (r *Runtime) Run(until time.Time, stop func() bool) Result {
	steps := 0
	for {
		if r.panicVal != nil {
			p := r.panicVal
			r.panicVal = nil
			panic(p)
		}
		if stop != nil && stop() {
			return Stopped
		}
		if len(r.runnable) == 0 {
			tm := r.nextTimer()
			if tm == nil {
				return Idle
			}
			if !until.IsZero() && tm.at.After(until) {
				r.now = until
				return Deadline
			}
			tm.active = false
			if tm.at.After(r.now) {
				r.now = tm.at
			}
			tm.fire()
			continue
		}
		steps++
		if steps > r.MaxSteps {
			panic(fmt.Sprintf("simrt: livelock: %d steps without becoming idle", steps))
		}
		i := int(uint64(r.sched.Int63()) % uint64(len(r.runnable)))
		t := r.runnable[i]
		r.runnable = append(r.runnable[:i], r.runnable[i+1:]...)
		t.waking = false
		r.steps++
		r.trace = append(r.trace, fmt.Sprintf("%s run %s#%d", r.now.Format("15:04:05.000000"), t.name, t.id))
		r.current = t
		t.baton <- true
		<-r.parked
		r.current = nil
	}
}

// RunFor runs until virtual time has advanced by d (or the system is idle earlier).
func (r *Runtime) RunFor(d time.Duration) Result { return r.Run(r.now.Add(d), nil) }

// Close kills every parked task so no goroutine outlives the test.
func (r *Runtime) Close() {
	r.killed = true
	for _, t := range r.all {
		if !t.done {
			t.baton <- false
			<-r.parked
		}
	}
}

func (r *Runtime) addTimer(at time.Time, fire func()) *timer {
	r.seq++
	tm := &timer{at: at, seq: r.seq, fire: fire, active: true}
	r.timers = append(r.timers, tm)
	return tm
}

// nextTimer removes and returns the earliest active timer (ties by creation order).
func (r *Runtime) nextTimer() *timer {
	best := -1
	for i, tm := range r.timers {
		if !tm.active {
			continue
		}
		if best < 0 || tm.at.Before(r.timers[best].at) || (tm.at.Equal(r.timers[best].at) && tm.seq < r.timers[best].seq) {
			best = i
		}
	}
	if best < 0 {
		r.timers = r.timers[:0]
		return nil
	}
	tm := r.timers[best]
	r.timers = append(r.timers[:best], r.timers[best+1:]...)
	return tm
}

// Sleep blocks the current task for d of virtual time.
func (r *Runtime) Sleep(d time.Duration) {
	t := r.current
	if t == nil {
		panic("simrt: Sleep outside a task")
	}
	r.addTimer(r.now.Add(d), func() { r.makeRunnable(t) })
	r.park()
}

// Tasks returns the names of tasks that have not finished, for debugging hangs.
func (r *Runtime) Tasks() string {
	var live []string
	for _, t := range r.all {
		if !t.done {
			live = append(live, fmt.Sprintf("%s#%d", t.name, t.id))
		}
	}
	return strings.Join(live, ", ")
}

// ---- clock --------------------------------------------------------------------------------

type simClock struct{ r *Runtime }

func (c simClock) Now() time.Time { return c.r.now }

func (c simClock) AfterFunc(d time.Duration, f func()) clock.Cancel {
	if d < 0 {
		d = 0
	}
	tm := c.r.addTimer(c.r.now.Add(d), func() { c.r.Go("afterfunc", func(context.Context) { f() }) })
	return func() bool {
		was := tm.active
		tm.active = false
		return was
	}
}

// ---- locks and conditions -----------------------------------------------------------------

// NewLocker returns a mutex whose Lock is a yield point.
func (r *Runtime) NewLocker() xsync.Locker { return &mutex{r: r} }

type mutex struct {
	r       *Runtime
	held    bool
	owner   *task
	waiters []*task
}

func (m *mutex) Lock() {
	if m.r.current == nil {
		if m.held {
			panic("simrt: Lock outside a task on a held mutex")
		}
		m.held = true
		return
	}
	m.r.Yield()
	for m.held {
		m.waiters = append(m.waiters, m.r.current)
		m.r.park()
	}
	m.held, m.owner = true, m.r.current
}

func (m *mutex) Unlock() {
	if !m.held {
		panic("simrt: unlock of unlocked mutex")
	}
	m.held, m.owner = false, nil
	if len(m.waiters) > 0 {
		w := m.waiters[0]
		m.waiters = m.waiters[1:]
		m.r.makeRunnable(w)
	}
}

// NewCond returns a condition variable on l.
func (r *Runtime) NewCond(l xsync.Locker) xsync.Cond { return &cond{r: r, l: l} }

type condWaiter struct {
	t        *task
	signaled bool
	timedOut bool
}

type cond struct {
	r       *Runtime
	l       xsync.Locker
	waiters []*condWaiter
}

func (c *cond) wait(deadline time.Time) bool {
	if c.r.current == nil {
		panic("simrt: Cond wait outside a task")
	}
	w := &condWaiter{t: c.r.current}
	c.waiters = append(c.waiters, w)
	var tm *timer
	if !deadline.IsZero() {
		tm = c.r.addTimer(deadline, func() {
			if !w.signaled {
				w.timedOut = true
				c.remove(w)
				c.r.makeRunnable(w.t)
			}
		})
	}
	c.l.Unlock()
	c.r.park()
	if tm != nil {
		tm.active = false
	}
	c.l.Lock()
	return w.signaled
}

func (c *cond) remove(w *condWaiter) {
	for i, x := range c.waiters {
		if x == w {
			c.waiters = append(c.waiters[:i], c.waiters[i+1:]...)
			return
		}
	}
}

func (c *cond) Wait() { c.wait(time.Time{}) }

func (c *cond) WaitUntil(deadline time.Time) bool {
	if !deadline.After(c.r.now) {
		return false
	}
	return c.wait(deadline)
}

func (c *cond) Signal() {
	if len(c.waiters) == 0 {
		return
	}
	w := c.waiters[0]
	c.waiters = c.waiters[1:]
	w.signaled = true
	c.r.makeRunnable(w.t)
}

func (c *cond) Broadcast() {
	for len(c.waiters) > 0 {
		c.Signal()
	}
}
