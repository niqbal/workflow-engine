package sim

// Node slots (WI-042). A node in the simulated cluster is a slot, not a server: a crash
// replaces the server inside it with a new incarnation that re-acquires the same shards,
// which bumps the shard's range ID and fences everything the old one still believes.
// Workers and the workload reach a node through its slot, so they follow the restart
// without knowing it happened.

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/niqbal/workflow-engine/internal/clock"
	"github.com/niqbal/workflow-engine/internal/rnd"
	"github.com/niqbal/workflow-engine/internal/server"
	"github.com/niqbal/workflow-engine/internal/server/api"
	"github.com/niqbal/workflow-engine/internal/store"
	"github.com/niqbal/workflow-engine/internal/xsync"
)

var (
	errInjected  = errors.New("sim: injected store error")
	errAmbiguous = errors.New("sim: injected ambiguous commit")
	errDown      = errors.New("sim: node is down")
)

// node is one slot in the cluster.
type node struct {
	c       *Cluster
	index   int
	shards  []int
	seed    uint64
	skew    time.Duration
	srv     *server.Server
	down    bool
	frozen  time.Time // virtual time the pause ends
	starts  int       // incarnations started, for traces
	cfgBase server.Config
}

// server returns the current incarnation, or nil while the node is down.
func (n *node) server() *server.Server {
	if n.down {
		return nil
	}
	return n.srv
}

// start builds an incarnation and takes its shards. Acquiring bumps the range ID, so the
// previous incarnation's fences are stale from here on.
//
// Acquiring is retried: an injected store fault must not end the run, because a node that
// cannot take its shards on the first try is a node that boots a moment later, not a
// cluster that has failed. Only a node that cannot start at all is a failure.
func (n *node) start(ctx context.Context) error {
	n.starts++
	cfg := n.cfgBase
	cfg.Node = fmt.Sprintf("node-%d", n.index)
	for attempt := 1; ; attempt++ {
		srv := server.New(n.c.runtimeFor(n), n.c.storeFor(n),
			rnd.New(split(n.seed, n.index*31+n.starts)), cfg)
		err := srv.StartOwned(ctx, n.shards)
		if err == nil {
			if err = srv.Run(ctx); err == nil {
				n.srv = srv
				n.down = false
				return nil
			}
		}
		if attempt >= startAttempts {
			return fmt.Errorf("sim: node-%d did not start in %d attempts: %w", n.index, attempt, err)
		}
		n.c.rt.Tracef("fault start-retry node-%d attempt %d: %v", n.index, attempt, err)
		if n.c.rt.InTask() {
			n.c.rt.Sleep(startRetryDelay)
		}
	}
}

// How long a node keeps trying to take its shards before the run is called failed.
const (
	startAttempts   = 20
	startRetryDelay = 250 * time.Millisecond
)

// stop ends the incarnation's loops and drops what it held in memory, as a crash would.
func (n *node) stop(ctx context.Context) {
	if n.srv == nil {
		return
	}
	_ = n.srv.Stop(ctx)
	n.srv.DropQueuedTasks()
	n.down = true
}

// wait blocks the calling task while the node is paused. Outside a simulator task it
// returns at once: the harness's own bookkeeping is not part of the paused process.
func (n *node) wait() {
	if !n.c.rt.InTask() {
		return
	}
	for {
		now := n.c.rt.Now()
		if !n.frozen.After(now) {
			return
		}
		n.c.rt.Sleep(n.frozen.Sub(now))
	}
}

// crashNode drops a node's in-memory state and starts a new incarnation after downFor.
func (c *Cluster) crashNode(ctx context.Context, i int, downFor time.Duration) {
	n := c.slots[i]
	if n.down {
		return
	}
	c.rt.Tracef("fault crash node-%d for %s", i, downFor)
	n.stop(ctx)
	c.rt.Sleep(downFor)
	if err := n.start(ctx); err != nil {
		c.fail(fmt.Errorf("sim: restart node-%d: %w", i, err))
		return
	}
	c.rt.Tracef("fault restart node-%d (incarnation %d)", i, n.starts)
}

// freezeNode stops a node making progress for d, with its fence still in memory.
func (c *Cluster) freezeNode(i int, d time.Duration) {
	n := c.slots[i]
	if n.down {
		return
	}
	until := c.rt.Now().Add(d)
	if until.After(n.frozen) {
		n.frozen = until
	}
	c.rt.Tracef("fault freeze node-%d for %s", i, d)
}

// restoreAll ends every pause and brings every node back up. The injector calls it when the
// quiet phase starts, so liveness is checked against a healthy cluster.
func (c *Cluster) restoreAll(ctx context.Context) {
	for _, n := range c.slots {
		n.frozen = time.Time{}
	}
	for i, n := range c.slots {
		if !n.down {
			continue
		}
		if err := n.start(ctx); err != nil {
			c.fail(fmt.Errorf("sim: restart node-%d at quiet: %w", i, err))
			return
		}
		c.rt.Tracef("fault restore node-%d (incarnation %d)", i, n.starts)
	}
}

// leaseDuration is the configured shard lease, the time scale a pause has to outlast.
func (c *Cluster) leaseDuration() time.Duration {
	if len(c.slots) == 0 {
		return 30 * time.Second
	}
	return c.slots[0].cfgBase.LeaseDuration
}

// serviceFor returns the node's frontend, or nil while it is down.
func (c *Cluster) serviceFor(i int) api.Service {
	srv := c.slots[i].server()
	if srv == nil {
		return nil
	}
	return srv.Service()
}

// storeFor returns the node's view of the store: the shared store, plus the pause.
func (c *Cluster) storeFor(n *node) store.Store {
	if !c.faults.any() {
		return c.st
	}
	return nodeStore{Store: c.st, n: n}
}

// runtimeFor returns the node's runtime: the simulator's, with its clock offset by the
// node's skew so its view of time differs from its peers' by up to Epsilon.
func (c *Cluster) runtimeFor(n *node) xsync.Runtime {
	if n.skew == 0 {
		return c.rt
	}
	return skewRuntime{Runtime: c.rt, off: n.skew}
}

// ---- the node's store view -------------------------------------------------------------

// nodeStore is the shared store as one node sees it. Every call waits out a pause first, so
// a paused node makes no progress at all: no reads, no writes, no timers serviced.
type nodeStore struct {
	store.Store
	n *node
}

func (s nodeStore) GetShard(ctx context.Context, shard int) (store.ShardInfo, error) {
	s.n.wait()
	return s.Store.GetShard(ctx, shard)
}

func (s nodeStore) AcquireShard(ctx context.Context, shard int, owner string, now time.Time, lease time.Duration) (store.Fence, error) {
	s.n.wait()
	return s.Store.AcquireShard(ctx, shard, owner, now, lease)
}

func (s nodeStore) RenewShard(ctx context.Context, f store.Fence, now time.Time, lease time.Duration) error {
	s.n.wait()
	return s.Store.RenewShard(ctx, f, now, lease)
}

func (s nodeStore) CreateRun(ctx context.Context, f store.Fence, r store.NewRun) ([]store.Task, error) {
	s.n.wait()
	return s.Store.CreateRun(ctx, f, r)
}

func (s nodeStore) UpdateRun(ctx context.Context, f store.Fence, key store.RunKey, u store.Update) (int64, []store.Task, error) {
	s.n.wait()
	return s.Store.UpdateRun(ctx, f, key, u)
}

func (s nodeStore) GetRun(ctx context.Context, key store.RunKey) (store.RunSnapshot, error) {
	s.n.wait()
	return s.Store.GetRun(ctx, key)
}

func (s nodeStore) GetLatestRun(ctx context.Context, shard int, workflowID string) (store.RunKey, error) {
	s.n.wait()
	return s.Store.GetLatestRun(ctx, shard, workflowID)
}

func (s nodeStore) ReadHistory(ctx context.Context, key store.RunKey, fromID int64, limit int) ([]store.EventRecord, error) {
	s.n.wait()
	return s.Store.ReadHistory(ctx, key, fromID, limit)
}

func (s nodeStore) ReadTransferTasks(ctx context.Context, shard int, fromID int64, limit int) ([]store.Task, error) {
	s.n.wait()
	return s.Store.ReadTransferTasks(ctx, shard, fromID, limit)
}

func (s nodeStore) ReadTimerTasks(ctx context.Context, shard int, upTo time.Time, limit int) ([]store.Task, error) {
	s.n.wait()
	return s.Store.ReadTimerTasks(ctx, shard, upTo, limit)
}

func (s nodeStore) DeleteTasks(ctx context.Context, f store.Fence, keys []store.TaskKey) error {
	s.n.wait()
	return s.Store.DeleteTasks(ctx, f, keys)
}

// ---- the node's clock ------------------------------------------------------------------

// skewRuntime is the simulator's runtime with one node's clock offset. Waits keep their
// duration and deadlines are interpreted on the same offset clock, so the node is internally
// consistent and only disagrees with its peers, which is what clock skew is.
type skewRuntime struct {
	xsync.Runtime
	off time.Duration
}

func (r skewRuntime) Clock() clock.Clock { return skewClock{c: r.Runtime.Clock(), off: r.off} }

func (r skewRuntime) NewCond(l xsync.Locker) xsync.Cond {
	return skewCond{Cond: r.Runtime.NewCond(l), off: r.off}
}

type skewClock struct {
	c   clock.Clock
	off time.Duration
}

func (c skewClock) Now() time.Time { return c.c.Now().Add(c.off) }

func (c skewClock) AfterFunc(d time.Duration, f func()) clock.Cancel { return c.c.AfterFunc(d, f) }

type skewCond struct {
	xsync.Cond
	off time.Duration
}

func (c skewCond) WaitUntil(deadline time.Time) bool {
	return c.Cond.WaitUntil(deadline.Add(-c.off))
}
