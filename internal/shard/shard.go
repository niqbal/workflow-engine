// Package shard owns a node's shards: which ones it holds, renewing their leases, and
// letting go of one before its lease can expire (WI-050, SPEC §9).
//
// The division of labour is the one the spec insists on: **leases are for liveness, fences
// are for safety**. A lease only decides when another node may try to take a shard over.
// Nothing here is trusted to keep two nodes from writing at once — that is the fence, checked
// inside every store transaction, and it holds however badly the clocks disagree.
//
// What this package adds is the other half: a node that cannot renew must stop writing
// *before* its lease expires, so that by the time another node can take the shard the old
// owner has already stopped. Without that margin the fence still keeps the data correct, but
// the old owner wastes work and fills the logs with rejected writes.
package shard

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/niqbal/workflow-engine/internal/clock"
	"github.com/niqbal/workflow-engine/internal/store"
	"github.com/niqbal/workflow-engine/internal/xsync"
)

// Store is the slice of store.Store the manager needs.
type Store interface {
	GetShard(ctx context.Context, shard int) (store.ShardInfo, error)
	AcquireShard(ctx context.Context, shard int, owner string, now time.Time, lease time.Duration) (store.Fence, error)
	RenewShard(ctx context.Context, f store.Fence, now time.Time, lease time.Duration) error
}

// Config configures a manager.
type Config struct {
	// Node is this node's ID, as it appears in the shard rows and the peer list.
	Node string
	// NumShards is the cluster's fixed shard count.
	NumShards int
	// Nodes is how many nodes the cluster is configured with, which sets the even share.
	// Zero or one means this node may hold every shard.
	Nodes int
	// Lease is how long an acquired lease lasts.
	Lease time.Duration
	// Margin is how long before expiry a shard is dropped when it has not been renewed.
	// Zero means a third of the lease.
	Margin time.Duration
	// FailoverAllowance is how many shards beyond the even share this node may take, so a
	// dead node's shards are picked up. Negative means a quarter of the even share, at least
	// one.
	FailoverAllowance int
	// Renewing says that something is renewing these leases — a cluster's shard loop. Only
	// then does the stop-serving margin apply: a shard that has not been renewed within it
	// stops being served, so this node is out before a peer may take over.
	//
	// It is off by default because ownership that nothing renews has nobody to lose a race
	// to. A single node that owns every shard, or a test that pins a layout, would otherwise
	// stop serving at the margin and never recover, protecting nothing: safety is the
	// fence's job, checked inside every store transaction, and that holds either way.
	Renewing bool
}

func (c Config) withDefaults() Config {
	if c.NumShards <= 0 {
		c.NumShards = 1
	}
	if c.Nodes <= 0 {
		c.Nodes = 1
	}
	if c.Lease <= 0 {
		c.Lease = 30 * time.Second
	}
	if c.Margin <= 0 {
		c.Margin = c.Lease / 3
	}
	if c.Margin >= c.Lease {
		c.Margin = c.Lease / 2
	}
	if c.FailoverAllowance < 0 {
		share := c.evenShare()
		c.FailoverAllowance = share / 4
		if c.FailoverAllowance < 1 {
			c.FailoverAllowance = 1
		}
	}
	return c
}

// evenShare is ceil(NumShards / Nodes).
func (c Config) evenShare() int {
	return (c.NumShards + c.Nodes - 1) / c.Nodes
}

// Cap is the most shards this node will hold: its even share plus the failover allowance,
// never more than the cluster has.
func (c Config) Cap() int {
	limit := c.evenShare() + c.FailoverAllowance
	if limit > c.NumShards {
		limit = c.NumShards
	}
	if limit < 1 {
		limit = 1
	}
	return limit
}

// holding is one shard this node believes it owns.
type holding struct {
	fence store.Fence
	// expires is when the lease lapses, on this node's clock. Writes stop at
	// expires - Margin unless a renewal moves it.
	expires time.Time
}

// Manager is a node's shard ownership. It is safe for concurrent use.
type Manager struct {
	cfg Config
	clk clock.Clock
	st  Store
	mu  xsync.Locker

	held map[int]holding
	// owners is the last owner seen per shard, for routing. It is a cache, never a
	// decision: a write is authorized by a fence, not by this map.
	owners map[int]string
	// dropped counts shards given up for lack of a renewal, for tests and metrics.
	dropped int
}

// New returns a manager that holds nothing yet.
func New(rt xsync.Runtime, st Store, cfg Config) *Manager {
	return &Manager{cfg: cfg.withDefaults(), clk: rt.Clock(), st: st, mu: rt.NewLocker(),
		held: map[int]holding{}, owners: map[int]string{}}
}

// Config returns the effective configuration.
func (m *Manager) Config() Config {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.cfg
}

// Renewing reports whether something is renewing these leases, which Tick turns on. When it
// is false the node's ownership is pinned: no margin, and a rejected write is a store
// anomaly rather than evidence that a peer has taken the shard.
func (m *Manager) Renewing() bool {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.cfg.Renewing
}

// Changes is what one pass did.
type Changes struct {
	Acquired []int
	Dropped  []int
}

// Any reports whether the pass changed anything.
func (c Changes) Any() bool { return len(c.Acquired) > 0 || len(c.Dropped) > 0 }

func (c Changes) String() string {
	if !c.Any() {
		return "no change"
	}
	return fmt.Sprintf("acquired %v, dropped %v", c.Acquired, c.Dropped)
}

// Fence returns the shard's fence if this node holds it and its lease has not reached the
// stop-serving margin. Every write goes through here, so a shard that is about to expire
// stops being written before another node can take it.
func (m *Manager) Fence(shard int) (store.Fence, bool) {
	m.mu.Lock()
	defer m.mu.Unlock()
	h, ok := m.held[shard]
	if !ok {
		return store.Fence{}, false
	}
	if !m.servable(h) {
		return store.Fence{}, false
	}
	return h.fence, true
}

// servable reports whether a holding may still be written through. Called with mu held.
func (m *Manager) servable(h holding) bool {
	if !m.cfg.Renewing || h.expires.IsZero() {
		return true // nothing renews this lease, so nothing is waiting to take it over
	}
	return m.clk.Now().Before(h.expires.Add(-m.cfg.Margin))
}

// Owned lists the shards this node holds and may still serve, in order.
func (m *Manager) Owned() []int {
	m.mu.Lock()
	defer m.mu.Unlock()
	out := make([]int, 0, len(m.held))
	for _, shard := range xsync.SortedKeys(m.held) {
		if m.servable(m.held[shard]) {
			out = append(out, shard)
		}
	}
	return out
}

// Holds reports whether the node holds the shard at all, servable or not.
func (m *Manager) Holds(shard int) bool {
	m.mu.Lock()
	defer m.mu.Unlock()
	_, ok := m.held[shard]
	return ok
}

// Dropped is how many shards have been given up for lack of a renewal.
func (m *Manager) Dropped() int {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.dropped
}

// Acquire takes one shard, whatever the placement cap says. The single-node server and the
// simulator use it to take a fixed set; Tick is what a cluster uses.
func (m *Manager) Acquire(ctx context.Context, shard int) (store.Fence, error) {
	if shard < 0 || shard >= m.cfg.NumShards {
		return store.Fence{}, fmt.Errorf("shard %d is not in 0..%d", shard, m.cfg.NumShards-1)
	}
	now := m.clk.Now()
	f, err := m.st.AcquireShard(ctx, shard, m.cfg.Node, now, m.cfg.Lease)
	if err != nil {
		return store.Fence{}, fmt.Errorf("acquire shard %d: %w", shard, err)
	}
	m.mu.Lock()
	m.held[shard] = holding{fence: f, expires: now.Add(m.cfg.Lease)}
	m.owners[shard] = m.cfg.Node
	m.mu.Unlock()
	return f, nil
}

// Release gives up a shard locally. It does not touch the store: the next node to want it
// waits for the lease to lapse, which is what makes a release safe without a round trip.
func (m *Manager) Release(shard int) {
	m.mu.Lock()
	defer m.mu.Unlock()
	delete(m.held, shard)
}

// Invalidate is Release for a shard whose write came back fenced: the node has evidently lost
// it, so it stops trying. It reports whether the shard was held.
func (m *Manager) Invalidate(shard int) bool {
	m.mu.Lock()
	defer m.mu.Unlock()
	_, ok := m.held[shard]
	if ok {
		delete(m.held, shard)
		m.dropped++
	}
	return ok
}

// Tick is one placement pass: renew what this node holds, drop what it could not renew in
// time, and take what it is entitled to. It is the only thing that needs to run on a timer.
//
// Tick marks the manager as renewing, which is what turns the stop-serving margin on.
//
// A renewal that fails for a reason other than a lost fence is not fatal on its own: the
// shard keeps being served until the stop-serving margin, which is what lets a node ride out
// a brief store outage without dropping everything.
func (m *Manager) Tick(ctx context.Context) (Changes, error) {
	m.mu.Lock()
	m.cfg.Renewing = true
	m.mu.Unlock()

	var ch Changes
	var firstErr error

	for _, shard := range m.renewable() {
		f, ok := m.fenceOf(shard)
		if !ok {
			continue
		}
		now := m.clk.Now()
		err := m.st.RenewShard(ctx, f, now, m.cfg.Lease)
		switch {
		case err == nil:
			m.mu.Lock()
			if h, still := m.held[shard]; still && h.fence == f {
				m.held[shard] = holding{fence: f, expires: now.Add(m.cfg.Lease)}
			}
			m.mu.Unlock()
		case errors.Is(err, store.ErrFenced), errors.Is(err, store.ErrNotFound):
			// Another node owns it now. Stop at once; the fence would reject us anyway.
			if m.Invalidate(shard) {
				ch.Dropped = append(ch.Dropped, shard)
			}
		default:
			if firstErr == nil {
				firstErr = fmt.Errorf("renew shard %d: %w", shard, err)
			}
			// Keep serving until the margin. expired() below gives it up if the outage
			// lasts, which is what stops us writing before anyone else may take over.
		}
	}

	for _, shard := range m.expired() {
		if m.Invalidate(shard) {
			ch.Dropped = append(ch.Dropped, shard)
		}
	}

	acquired, err := m.place(ctx)
	ch.Acquired = acquired
	if firstErr == nil {
		firstErr = err
	}
	return ch, firstErr
}

// place takes shards this node is entitled to. Shards are considered in order, so two nodes
// starting together contend for the same shard and one of them loses the compare-and-swap
// rather than both believing they won.
//
// Two limits, not one. Up to its **even share** a node takes anything free: a shard nobody
// owns, one it owned before a restart, or one whose lease has lapsed. Beyond the even share,
// up to the cap, it takes only shards that have been **orphaned** — owned by another node and
// now lapsed — which is the failover case the allowance exists for.
//
// The alternative, claiming up to the cap from the start, starves the peers: the first node up
// takes its share plus the allowance, keeps renewing, and a node that joins a minute later
// finds nothing to take. The cost of this rule is the opposite case: a cluster configured for
// two nodes while only one is running leaves the absent node's share unclaimed until it
// starts, because those shards are free rather than orphaned. The fence makes either choice
// safe; this one does not strand a running peer, and rebalancing when a node returns is a
// stretch item in the spec anyway.
func (m *Manager) place(ctx context.Context) ([]int, error) {
	var acquired []int
	var firstErr error
	share := m.cfg.evenShare()
	for shard := 0; shard < m.cfg.NumShards; shard++ {
		held := m.count()
		if held >= m.cfg.Cap() {
			return acquired, firstErr
		}
		if m.Holds(shard) {
			continue
		}
		info, err := m.st.GetShard(ctx, shard)
		if err != nil {
			if firstErr == nil {
				firstErr = fmt.Errorf("read shard %d: %w", shard, err)
			}
			continue
		}
		m.observe(info)
		free, orphaned := m.takeable(info)
		if !free {
			continue
		}
		if held >= share && !orphaned {
			continue // past the even share, only failover justifies more
		}
		if _, err := m.Acquire(ctx, shard); err != nil {
			if errors.Is(err, store.ErrFenced) {
				continue // another node got there first, which is the normal race
			}
			if firstErr == nil {
				firstErr = err
			}
			continue
		}
		acquired = append(acquired, shard)
	}
	return acquired, firstErr
}

// takeable reports whether a shard may be acquired now, and whether it is a failover: owned
// by another node whose lease has lapsed, rather than simply free.
func (m *Manager) takeable(info store.ShardInfo) (free, orphaned bool) {
	switch {
	case info.Owner == "":
		return true, false
	case info.Owner == m.cfg.Node:
		return true, false // ours before a restart
	case m.clk.Now().Before(info.LeaseExpiresAt):
		return false, false
	default:
		return true, true
	}
}

// Owner returns the node that owns a shard according to the store, and caches it for
// routing. An empty owner means nobody holds it yet.
func (m *Manager) Owner(ctx context.Context, shard int) (string, error) {
	if shard < 0 || shard >= m.cfg.NumShards {
		return "", fmt.Errorf("shard %d is not in 0..%d", shard, m.cfg.NumShards-1)
	}
	info, err := m.st.GetShard(ctx, shard)
	if err != nil {
		return "", fmt.Errorf("read shard %d: %w", shard, err)
	}
	m.observe(info)
	return info.Owner, nil
}

// CachedOwner returns the last owner seen for a shard without reading the store.
func (m *Manager) CachedOwner(shard int) (string, bool) {
	m.mu.Lock()
	defer m.mu.Unlock()
	owner, ok := m.owners[shard]
	return owner, ok && owner != ""
}

// Forget drops the cached owner of a shard, so the next lookup reads the store. Routing calls
// it when a node says it is not the owner.
func (m *Manager) Forget(shard int) {
	m.mu.Lock()
	defer m.mu.Unlock()
	delete(m.owners, shard)
}

func (m *Manager) observe(info store.ShardInfo) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if info.Owner == "" {
		delete(m.owners, info.Shard)
		return
	}
	m.owners[info.Shard] = info.Owner
}

// renewable lists held shards in order.
func (m *Manager) renewable() []int {
	m.mu.Lock()
	defer m.mu.Unlock()
	return xsync.SortedKeys(m.held)
}

// expired lists held shards that have reached the stop-serving margin.
func (m *Manager) expired() []int {
	m.mu.Lock()
	defer m.mu.Unlock()
	var out []int
	for _, shard := range xsync.SortedKeys(m.held) {
		if !m.servable(m.held[shard]) {
			out = append(out, shard)
		}
	}
	return out
}

func (m *Manager) fenceOf(shard int) (store.Fence, bool) {
	m.mu.Lock()
	defer m.mu.Unlock()
	h, ok := m.held[shard]
	return h.fence, ok
}

func (m *Manager) count() int {
	m.mu.Lock()
	defer m.mu.Unlock()
	return len(m.held)
}

// Interval is how often Tick should run: often enough that a renewal has several chances
// before the margin, which is what keeps a healthy node from dropping shards.
func (c Config) Interval() time.Duration {
	c = c.withDefaults()
	d := c.Margin / 2
	if d <= 0 {
		d = c.Lease / 6
	}
	if d <= 0 {
		d = time.Second
	}
	return d
}
