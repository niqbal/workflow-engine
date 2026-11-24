package sim

// Fault injection (WI-042, SPEC §11.2). A zero Faults injects nothing and leaves the run
// byte-identical to one with no injector at all: every decision below is taken from the
// run's own seeded RNG, and nothing is drawn from it unless the matching field is set.
//
// What is injected:
//
//	node crash and restart   the node's in-memory state is dropped and a new incarnation
//	                         re-acquires its shards, which fences the old one (G6, I6)
//	process pause            the node stops making progress for longer than its lease
//	                         while its old fence stays in memory: the stale-owner case
//	store transient errors   a write fails before committing
//	ambiguous commits        a write commits and then reports an error to its caller
//	commit latency           a later task ID becomes visible before an earlier one, which
//	                         is what the outbox ack level has to survive (SPEC §8.1)
//	clock skew               per-node offsets within Epsilon, which is what I4 allows a
//	                         timer to fire early by
//	worker crash             a worker dies after its activity's side effect and before it
//	                         reports completion, so the attempt is retried (G4, I5)
//
// What is not injected, and why: message drop, delay and partition between nodes. The
// simulated cluster's nodes talk only through the shared store, so there are no inter-node
// messages to drop yet; internal/transport/httptransport is not on the simulator's path.
// A frozen node is the observable equivalent of one partitioned from the store. Routing
// between nodes arrives with M4 (WI-051, WI-052), and the injectors for it belong there.

import (
	"context"
	"time"

	"github.com/niqbal/workflow-engine/internal/rnd"
	"github.com/niqbal/workflow-engine/internal/store/memstore"
)

// Faults configures fault injection for one run. Every interval is virtual time and is
// treated as a mean: the actual wait is drawn uniformly from half to one and a half times
// it, so faults do not land on a regular beat.
type Faults struct {
	// CrashEvery is the mean interval between node crashes. Zero means no crashes.
	CrashEvery time.Duration
	// DownFor is how long a crashed node stays down before its new incarnation starts.
	// Zero means the server's lease duration, long enough for its lease to lapse.
	DownFor time.Duration

	// FreezeEvery is the mean interval between process pauses. Zero means none.
	FreezeEvery time.Duration
	// FrozenFor is how long a paused node makes no progress. Zero means twice the lease,
	// so the pause always outlasts it.
	FrozenFor time.Duration

	// StoreErrorRate is the probability in [0,1] that a write fails before committing.
	StoreErrorRate float64
	// AmbiguousRate is the probability that a write commits and then returns an error.
	AmbiguousRate float64
	// CommitDelayMax is the longest delay inserted between a write allocating its task IDs
	// and becoming visible. Zero means no delay.
	CommitDelayMax time.Duration

	// ClockSkewMax is the largest clock offset any node is given. Each node's offset is
	// drawn once, within plus or minus this, and holds for the run.
	ClockSkewMax time.Duration

	// WorkerCrashRate is the probability that a worker dies after an activity's side
	// effect and before reporting it.
	WorkerCrashRate float64
}

// NoFaults is a non-nil injector that injects nothing.
var NoFaults = &Faults{}

// any reports whether anything at all is enabled. A Faults with nothing set must leave the
// simulation exactly as it was without one.
func (f *Faults) any() bool {
	if f == nil {
		return false
	}
	return f.CrashEvery > 0 || f.FreezeEvery > 0 || f.StoreErrorRate > 0 ||
		f.AmbiguousRate > 0 || f.CommitDelayMax > 0 || f.ClockSkewMax > 0 ||
		f.WorkerCrashRate > 0
}

// storeFaults reports whether the store hooks are needed.
func (f *Faults) storeFaults() bool {
	return f != nil && (f.StoreErrorRate > 0 || f.AmbiguousRate > 0 || f.CommitDelayMax > 0)
}

// epsilon is the clock skew the oracle must allow for (G5, I4).
func (f *Faults) epsilon() time.Duration {
	if f == nil {
		return 0
	}
	return f.ClockSkewMax
}

// hooks builds the memstore fault hooks. r is the run's RNG, so the faults are part of the
// one deterministic sequence. Every hook stops injecting at the quiet phase: the liveness
// check (G8, I1) is a claim about a cluster whose faults have stopped.
func (f *Faults) hooks(r rnd.Rand, tr func(string, ...any), inTask, quiet func() bool) memstore.Faults {
	var h memstore.Faults
	if f.StoreErrorRate > 0 {
		h.BeforeCommit = func(op string) error {
			if quiet() {
				return nil
			}
			if !chance(r, f.StoreErrorRate) {
				return nil
			}
			tr("fault store-error %s", op)
			return errInjected
		}
	}
	if f.AmbiguousRate > 0 {
		h.AfterCommit = func(op string) error {
			if quiet() {
				return nil
			}
			if !chance(r, f.AmbiguousRate) {
				return nil
			}
			tr("fault ambiguous-commit %s", op)
			return errAmbiguous
		}
	}
	if f.CommitDelayMax > 0 {
		h.CommitDelay = func(op string) time.Duration {
			if quiet() {
				return 0
			}
			// A delay is a wait on the runtime, which only a simulator task may do: the
			// harness's own writes, outside any task, commit at once.
			if !inTask() {
				return 0
			}
			// Most writes commit at once; a minority are held back, which is what makes a
			// later task ID visible before an earlier one.
			if !chance(r, 0.25) {
				return 0
			}
			d := jitter(r, f.CommitDelayMax)
			tr("fault commit-delay %s %s", op, d)
			return d
		}
	}
	return h
}

// run is the injector task. It stops at the quiet phase and leaves every node up, which is
// what makes the liveness check (G8, I1) mean anything.
func (f *Faults) run(ctx context.Context, c *Cluster) {
	r := c.rt.Rand()
	for !c.Quiet() {
		next := f.nextAction(r)
		if next.wait <= 0 {
			return
		}
		c.rt.Sleep(next.wait)
		if c.Quiet() {
			break
		}
		node := int(uint64(r.Int63()) % uint64(c.NumNodes()))
		switch next.kind {
		case actionCrash:
			c.crashNode(ctx, node, f.downFor(c, r))
		case actionFreeze:
			c.freezeNode(node, f.frozenFor(c, r))
		}
	}
	c.restoreAll(ctx)
}

type action struct {
	kind int
	wait time.Duration
}

const (
	actionNone = iota
	actionCrash
	actionFreeze
)

// nextAction picks the next fault and how long to wait for it. With both kinds enabled it
// alternates by coin flip, so neither starves.
func (f *Faults) nextAction(r rnd.Rand) action {
	switch {
	case f.CrashEvery > 0 && f.FreezeEvery > 0:
		if chance(r, 0.5) {
			return action{actionCrash, jitter(r, f.CrashEvery)}
		}
		return action{actionFreeze, jitter(r, f.FreezeEvery)}
	case f.CrashEvery > 0:
		return action{actionCrash, jitter(r, f.CrashEvery)}
	case f.FreezeEvery > 0:
		return action{actionFreeze, jitter(r, f.FreezeEvery)}
	default:
		return action{actionNone, 0}
	}
}

func (f *Faults) downFor(c *Cluster, r rnd.Rand) time.Duration {
	if f.DownFor > 0 {
		return jitter(r, f.DownFor)
	}
	return jitter(r, c.leaseDuration())
}

func (f *Faults) frozenFor(c *Cluster, r rnd.Rand) time.Duration {
	if f.FrozenFor > 0 {
		return jitter(r, f.FrozenFor)
	}
	return jitter(r, 2*c.leaseDuration())
}

// workerCrashes reports whether a worker dies now.
func (f *Faults) workerCrashes(r rnd.Rand) bool {
	return f != nil && f.WorkerCrashRate > 0 && chance(r, f.WorkerCrashRate)
}

// chance returns true with probability p, drawing exactly one value from r.
func chance(r rnd.Rand, p float64) bool {
	if p <= 0 {
		return false
	}
	if p >= 1 {
		return true
	}
	const scale = 1 << 31
	return uint64(r.Int63())%scale < uint64(p*scale)
}

// jitter returns a duration drawn uniformly from half to one and a half times d.
func jitter(r rnd.Rand, d time.Duration) time.Duration {
	if d <= 0 {
		return 0
	}
	span := int64(d)
	off := uint64(r.Int63()) % uint64(span)
	return time.Duration(span/2 + int64(off))
}
