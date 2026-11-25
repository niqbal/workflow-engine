package server

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/niqbal/workflow-engine/internal/history"
	"github.com/niqbal/workflow-engine/internal/server/api"
	"github.com/niqbal/workflow-engine/internal/store"
	"github.com/niqbal/workflow-engine/internal/xsync"
)

// WI-022: background processing loops (production mode).
//
// Run starts, per owned shard, a transfer loop and a timer loop as Runtime tasks. Each does for
// its shard what Settle does, then waits on a Cond until the write path reports new tasks of
// its kind on its shard, or until the next time something becomes due: the earliest redispatch
// time (transfer) or the earliest FireAt (timers). Nothing polls. Stop ends the loops.

// loopRetryDelay is how long a loop waits before retrying work that failed (a store read
// error, or a timer that could not be fired), so a persistent failure cannot make it spin.
const loopRetryDelay = time.Second

// farFuture bounds the read for a shard's earliest timer task.
var farFuture = time.Date(9999, 12, 31, 0, 0, 0, 0, time.UTC)

var errLoopsRunning = errors.New("server: background loops already running")

// wakeup is one loop's doorbell. pending survives until the loop looks, so a write that lands
// while the loop is busy is not lost.
type wakeup struct {
	l       xsync.Locker
	c       xsync.Cond
	pending bool
	stopped bool
}

func newWakeup(rt xsync.Runtime) *wakeup {
	l := rt.NewLocker()
	return &wakeup{l: l, c: rt.NewCond(l)}
}

func (w *wakeup) ring() {
	w.l.Lock()
	defer w.l.Unlock()
	w.pending = true
	w.c.Broadcast()
}

func (w *wakeup) stop() {
	w.l.Lock()
	defer w.l.Unlock()
	w.stopped = true
	w.c.Broadcast()
}

// wait blocks until the bell rings, the loop is stopped, or until passes on the Runtime's
// clock (a zero until waits without a deadline). It reports false once stopped.
func (w *wakeup) wait(until time.Time) bool {
	w.l.Lock()
	defer w.l.Unlock()
	for !w.pending && !w.stopped {
		if until.IsZero() {
			w.c.Wait()
		} else if !w.c.WaitUntil(until) {
			break
		}
	}
	w.pending = false
	return !w.stopped
}

// loopSet is the set of loops one Run started. The maps are read-only once published.
type loopSet struct {
	cancel   context.CancelFunc
	unwatch  func() bool // stops watching Run's context
	transfer map[int]*wakeup
	timers   map[int]*wakeup

	l       xsync.Locker
	c       xsync.Cond
	running int // loops that have not returned
}

func (ls *loopSet) stopAll() {
	for _, shard := range xsync.SortedKeys(ls.transfer) {
		ls.transfer[shard].stop()
		ls.timers[shard].stop()
	}
}

func (ls *loopSet) exited() {
	ls.l.Lock()
	defer ls.l.Unlock()
	ls.running--
	ls.c.Broadcast()
}

// Run starts the transfer and timer loops of every owned shard and returns. The loops run
// until Stop, or until ctx is done. Under simulation, pass a context that is never canceled
// asynchronously and end the loops with Stop.
func (s *Server) Run(ctx context.Context) error {
	// In a cluster, loops run for every shard rather than only the ones owned right now:
	// ownership moves under them as leases change hands (WI-050), and a loop whose shard is
	// not owned waits instead of scanning. Starting them all once is simpler and cheaper
	// than adding and removing loops as shards come and go.
	//
	// A single node, or a layout pinned by StartOwned, keeps a loop per owned shard. There
	// ownership cannot move on its own, so a loop for someone else's shard would only poll
	// to find nothing — on a workload whose timers span a month, hundreds of thousands of
	// times.
	shards := s.Shards()
	if s.cfg.Nodes > 1 {
		shards = make([]int, 0, s.cfg.NumShards)
		for shard := 0; shard < s.cfg.NumShards; shard++ {
			shards = append(shards, shard)
		}
	}
	lctx, cancel := context.WithCancel(ctx)
	l := s.rt.NewLocker()
	ls := &loopSet{cancel: cancel, transfer: map[int]*wakeup{}, timers: map[int]*wakeup{},
		l: l, c: s.rt.NewCond(l), running: 2 * len(shards)}
	for _, shard := range shards {
		ls.transfer[shard] = newWakeup(s.rt)
		ls.timers[shard] = newWakeup(s.rt)
	}
	ls.unwatch = context.AfterFunc(ctx, ls.stopAll)
	if !s.sub.loops.CompareAndSwap(nil, ls) {
		ls.unwatch()
		cancel()
		return errLoopsRunning
	}
	for _, shard := range shards {
		s.rt.Go(fmt.Sprintf("transfer-%d", shard), func(context.Context) { s.transferLoop(lctx, ls, shard) })
		s.rt.Go(fmt.Sprintf("timers-%d", shard), func(context.Context) { s.timerLoop(lctx, ls, shard) })
	}
	return nil
}

// Stop ends the loops started by Run and waits until every one has returned. If ctx is done
// first it returns ctx.Err() (the loops still end on their own). Stop without Run is a no-op.
func (s *Server) Stop(ctx context.Context) error {
	ls := s.sub.loops.Load()
	if ls == nil {
		return nil
	}
	ls.unwatch()
	ls.stopAll()
	ls.cancel()
	wake := context.AfterFunc(ctx, func() {
		ls.l.Lock()
		defer ls.l.Unlock()
		ls.c.Broadcast()
	})
	defer wake()
	ls.l.Lock()
	for ls.running > 0 && ctx.Err() == nil {
		ls.c.Wait()
	}
	left := ls.running
	ls.l.Unlock()
	if left > 0 {
		return ctx.Err()
	}
	s.sub.loops.CompareAndSwap(ls, nil)
	return nil
}

// transferLoop pushes the shard's transfer tasks as Settle does, then sleeps until a write
// adds transfer tasks or the earliest redispatch time comes.
func (s *Server) transferLoop(ctx context.Context, ls *loopSet, shard int) {
	defer ls.exited()
	w := ls.transfer[shard]
	for {
		if _, owned := s.fence(shard); !owned {
			if !w.wait(s.unownedWait()) {
				return
			}
			continue
		}
		var next time.Time
		if _, err := s.processTransfer(ctx, shard); err != nil {
			next = s.clk.Now().Add(loopRetryDelay)
		}
		if due, ok := s.transferShard(shard).nextRedispatch(); ok && (next.IsZero() || due.Before(next)) {
			next = due
		}
		if !w.wait(next) {
			return
		}
	}
}

// nextRedispatch returns the earliest time a pushed task may be pushed again.
func (ts *transferShard) nextRedispatch() (time.Time, bool) {
	ts.mu.Lock()
	defer ts.mu.Unlock()
	var best time.Time
	found := false
	for _, id := range xsync.SortedKeys(ts.pushed) {
		if due := ts.pushed[id]; !found || due.Before(best) {
			best, found = due, true
		}
	}
	return best, found
}

// timerLoop fires the shard's due timer tasks as Settle does, then sleeps until a write adds
// timer tasks or the earliest stored FireAt comes.
func (s *Server) timerLoop(ctx context.Context, ls *loopSet, shard int) {
	defer ls.exited()
	w := ls.timers[shard]
	for {
		if _, owned := s.fence(shard); !owned {
			if !w.wait(s.unownedWait()) {
				return
			}
			continue
		}
		var next time.Time
		_, err := s.processTimers(ctx, shard)
		earliest, rerr := s.st.ReadTimerTasks(ctx, shard, farFuture, 1)
		switch {
		case err != nil || rerr != nil:
			// A timer that could not be fired is still stored and due: retry later, not now.
			next = s.clk.Now().Add(loopRetryDelay)
		case len(earliest) > 0:
			next = earliest[0].FireAt
		}
		if !w.wait(next) {
			return
		}
	}
}

// wake tells the shard's loops that tasks were written. It costs one atomic load when no
// loops are running.
func (s *Server) wake(shard int, tasks []store.Task) {
	ls := s.sub.loops.Load()
	if ls == nil {
		return
	}
	var transfer, timers bool
	for _, t := range tasks {
		switch t.Kind {
		case store.TransferTask:
			transfer = true
		case store.TimerTask:
			timers = true
		}
	}
	if w, ok := ls.transfer[shard]; ok && transfer {
		w.ring()
	}
	if w, ok := ls.timers[shard]; ok && timers {
		w.ring()
	}
}

// createRunAndWake is createRun followed by waking the loops for the tasks it wrote.
func (s *Server) createRunAndWake(ctx context.Context, key store.RunKey, st *history.MutableState, out history.Effects) (Written, error) {
	w, err := s.createRun(ctx, key, st, out)
	if err == nil {
		s.observeWriteMetrics(w.Tasks, nil)
		s.wake(key.Shard, w.Tasks)
		return w, nil
	}
	s.wakeUncertain(key.Shard, err)
	return w, err
}

// updateAndWake is update followed by waking the loops for the tasks it wrote.
func (s *Server) updateAndWake(ctx context.Context, key store.RunKey, done []store.TaskKey, fn func(m *history.Machine) error) (Written, error) {
	w, err := s.update(ctx, key, done, fn)
	if err == nil {
		s.observeWriteMetrics(w.Tasks, done)
		s.wake(key.Shard, w.Tasks)
		return w, nil
	}
	s.wakeUncertain(key.Shard, err)
	return w, err
}

// ownershipPoll is how long a loop waits before looking again at a shard this node does not
// own. A shard loop that acquires the shard rings the loop, so this is only a backstop.
func (s *Server) ownershipPoll() time.Duration {
	if !s.shards.Renewing() {
		return 0 // nothing can give this node the shard but an explicit acquire, which rings
	}
	d := s.shards.Config().Interval()
	if d <= 0 {
		d = s.cfg.LeaseDuration
	}
	return d
}

// unownedWait is when a loop should look at an unowned shard again: the next placement
// interval in a cluster, or never (a bare wake) when ownership cannot move on its own.
func (s *Server) unownedWait() time.Time {
	d := s.ownershipPoll()
	if d <= 0 {
		return time.Time{}
	}
	return s.clk.Now().Add(d)
}

// wakeUncertain rings a shard's loops after a write that may have committed anyway.
//
// A store can commit and then fail to report it (SPEC §8.1): the write is durable, its outbox
// tasks exist, and the caller is told it failed. Waking only on success would leave those
// tasks undispatched with nothing to wake the loops, and both loops wait without a deadline
// when they know of no due task, so the shard could sleep through an outbox that is not
// empty. A run whose first workflow task was written that way never starts at all, which the
// simulator found as a liveness failure on one seed in ten (docs/SIM-BUGS.md, 2026-11-25).
//
// Errors that prove nothing was written need no wake: a rejected fence means this node no
// longer owns the shard, and an invalid write never reached the store.
func (s *Server) wakeUncertain(shard int, err error) {
	switch {
	case errors.Is(err, store.ErrFenced), errors.Is(err, api.ErrNotOwner):
		return // not ours any more; the owner's own loops will scan
	case errors.Is(err, store.ErrInvalid), errors.Is(err, api.ErrInvalidArgument):
		return // never reached the store
	}
	ls := s.sub.loops.Load()
	if ls == nil {
		return
	}
	if w, ok := ls.transfer[shard]; ok {
		w.ring()
	}
	if w, ok := ls.timers[shard]; ok {
		w.ring()
	}
}
