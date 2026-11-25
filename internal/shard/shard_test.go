package shard_test

// WI-050. These tests run two managers against one store under a manual clock, which is the
// whole of a cluster's ownership behaviour with none of its timing.

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/niqbal/workflow-engine/internal/clock"
	"github.com/niqbal/workflow-engine/internal/shard"
	"github.com/niqbal/workflow-engine/internal/store"
	"github.com/niqbal/workflow-engine/internal/store/memstore"
	"github.com/niqbal/workflow-engine/internal/xsync"
)

var ctx = context.Background()

const (
	shards = 8
	lease  = 30 * time.Second
)

type fixture struct {
	clk *clock.Manual
	rt  *xsync.Real
	st  *memstore.Store
	// down makes every write fail, as an unreachable store would.
	down bool
}

func newFixture(t *testing.T) *fixture {
	t.Helper()
	clk := clock.NewManual(time.Date(2026, 4, 1, 12, 0, 0, 0, time.UTC))
	rt := &xsync.Real{Ctx: ctx, C: clk}
	f := &fixture{clk: clk, rt: rt}
	f.st = memstore.New(memstore.WithRuntime(rt), memstore.WithFaults(memstore.Faults{
		BeforeCommit: func(string) error {
			if f.down {
				return errUnreachable
			}
			return nil
		},
	}))
	return f
}

func (f *fixture) manager(node string, nodes int) *shard.Manager {
	return shard.New(f.rt, f.st, shard.Config{
		Node: node, NumShards: shards, Nodes: nodes, Lease: lease,
	})
}

func TestPlacementSplitsTheClusterAndRespectsTheCap(t *testing.T) {
	f := newFixture(t)
	a := f.manager("node-a", 2)
	b := f.manager("node-b", 2)

	// Even share of 8 across 2 nodes is 4, plus a failover allowance of 0 by default.
	if got := a.Config().Cap(); got != 4 {
		t.Fatalf("cap %d, want 4", got)
	}

	if _, err := a.Tick(ctx); err != nil {
		t.Fatal(err)
	}
	if got := a.Owned(); len(got) != 4 {
		t.Fatalf("node-a holds %v, want 4 shards", got)
	}
	if _, err := b.Tick(ctx); err != nil {
		t.Fatal(err)
	}
	if got := b.Owned(); len(got) != 4 {
		t.Fatalf("node-b holds %v, want 4 shards", got)
	}
	// Between them they hold every shard, and no shard twice.
	owners := map[int]string{}
	for _, s := range a.Owned() {
		owners[s] = "node-a"
	}
	for _, s := range b.Owned() {
		if prev, dup := owners[s]; dup {
			t.Fatalf("shard %d held by both %s and node-b", s, prev)
		}
		owners[s] = "node-b"
	}
	if len(owners) != shards {
		t.Fatalf("%d shards owned, want %d", len(owners), shards)
	}

	// Another tick changes nothing: a node at its cap does not take more.
	ch, err := a.Tick(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if ch.Any() {
		t.Fatalf("a settled cluster still moved shards: %s", ch)
	}
}

func TestFailoverTakesADeadNodesShardsOnceTheLeaseLapses(t *testing.T) {
	f := newFixture(t)
	// One failover allowance per node, so a survivor can take more than its even share.
	cfg := shard.Config{Node: "node-a", NumShards: 4, Nodes: 2, Lease: lease, FailoverAllowance: 2}
	a := shard.New(f.rt, f.st, cfg)
	cfg.Node = "node-b"
	b := shard.New(f.rt, f.st, cfg)

	mustTick(t, a)
	mustTick(t, b)
	if len(a.Owned()) != 2 || len(b.Owned()) != 2 {
		t.Fatalf("a=%v b=%v, want two each", a.Owned(), b.Owned())
	}

	// node-b dies: it stops renewing. Before the lease lapses, node-a may take nothing.
	f.clk.Advance(lease / 2)
	mustTick(t, a)
	if got := len(a.Owned()); got != 2 {
		t.Fatalf("node-a holds %d shards before the lease lapsed, want 2", got)
	}

	// After it lapses, node-a takes them over, up to its cap of 4.
	f.clk.Advance(lease)
	mustTick(t, a)
	if got := a.Owned(); len(got) != 4 {
		t.Fatalf("node-a holds %v after failover, want all 4", got)
	}
}

func TestANodeStopsServingBeforeItsLeaseCanExpire(t *testing.T) {
	f := newFixture(t)
	m := shard.New(f.rt, f.st, shard.Config{Node: "node-a", NumShards: 1, Nodes: 1,
		Lease: lease, Margin: 10 * time.Second})
	mustTick(t, m)
	if _, ok := m.Fence(0); !ok {
		t.Fatal("the shard was not acquired")
	}

	// The store becomes unreachable: renewals fail, but the shard is still served while
	// there is time to spare.
	f.down = true
	f.clk.Advance(15 * time.Second) // 15s left on the lease, margin is 10s
	if _, err := m.Tick(ctx); err == nil {
		t.Fatal("a failing renewal was reported as success")
	}
	if _, ok := m.Fence(0); !ok {
		t.Fatal("the shard stopped being served while the lease had room")
	}

	// Past the margin, the shard is dropped: writes stop before anyone else may take over.
	f.clk.Advance(6 * time.Second) // 9s left, inside the 10s margin
	if _, ok := m.Fence(0); ok {
		t.Fatal("the shard was still served inside the stop-serving margin")
	}
	ch, _ := m.Tick(ctx)
	if len(ch.Dropped) != 1 || ch.Dropped[0] != 0 {
		t.Fatalf("tick dropped %v, want shard 0", ch.Dropped)
	}
	if m.Holds(0) {
		t.Fatal("the shard is still held after being dropped")
	}
	if got := m.Dropped(); got != 1 {
		t.Fatalf("dropped count %d, want 1", got)
	}
}

func TestARenewalRejectedByTheFenceDropsTheShardAtOnce(t *testing.T) {
	f := newFixture(t)
	a := shard.New(f.rt, f.st, shard.Config{Node: "node-a", NumShards: 1, Nodes: 1, Lease: lease})
	mustTick(t, a)

	// The lease lapses and another node takes the shard, which bumps the range ID.
	f.clk.Advance(2 * lease)
	b := shard.New(f.rt, f.st, shard.Config{Node: "node-b", NumShards: 1, Nodes: 1, Lease: lease})
	mustTick(t, b)

	// node-a's next renewal is rejected, so it gives the shard up without waiting for any
	// margin: the fence has already spoken.
	ch, err := a.Tick(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if len(ch.Dropped) != 1 {
		t.Fatalf("dropped %v, want shard 0", ch.Dropped)
	}
	if _, ok := a.Fence(0); ok {
		t.Fatal("node-a still serves a shard whose fence was rejected")
	}
	if !b.Holds(0) {
		t.Fatal("node-b does not hold the shard it took")
	}
}

func TestTheOldOwnersWritesAreRejectedAfterATakeover(t *testing.T) {
	f := newFixture(t)
	a := shard.New(f.rt, f.st, shard.Config{Node: "node-a", NumShards: 1, Nodes: 1, Lease: lease})
	fenceA, err := a.Acquire(ctx, 0)
	if err != nil {
		t.Fatal(err)
	}
	key := store.RunKey{Shard: 0, WorkflowID: "w", RunID: "r"}
	if _, err := f.st.CreateRun(ctx, fenceA, store.NewRun{Key: key,
		Events: []store.EventRecord{{ID: 1, Data: []byte(`{}`)}}, State: []byte("s")}); err != nil {
		t.Fatal(err)
	}

	f.clk.Advance(2 * lease)
	b := shard.New(f.rt, f.st, shard.Config{Node: "node-b", NumShards: 1, Nodes: 1, Lease: lease})
	if _, err := b.Acquire(ctx, 0); err != nil {
		t.Fatal(err)
	}

	// This is the safety property: whatever node-a still believes, the store rejects it.
	_, _, err = f.st.UpdateRun(ctx, fenceA, key, store.Update{ExpectedDBVersion: 1,
		Events: []store.EventRecord{{ID: 2, Data: []byte(`{}`)}}, State: []byte("s2"), Open: true})
	if !errors.Is(err, store.ErrFenced) {
		t.Fatalf("the old owner's write returned %v, want ErrFenced", err)
	}
	// And the manager notices, so it stops trying.
	if !a.Invalidate(0) {
		t.Fatal("Invalidate reported the shard was not held")
	}
	if _, ok := a.Fence(0); ok {
		t.Fatal("the shard is still served after a fenced write")
	}
}

func TestOwnerLookupAndItsCache(t *testing.T) {
	f := newFixture(t)
	a := f.manager("node-a", 2)
	b := f.manager("node-b", 2)
	mustTick(t, a)

	// b has taken nothing yet, so it reads the store to find the owner.
	owner, err := b.Owner(ctx, a.Owned()[0])
	if err != nil {
		t.Fatal(err)
	}
	if owner != "node-a" {
		t.Fatalf("owner %q, want node-a", owner)
	}
	cached, ok := b.CachedOwner(a.Owned()[0])
	if !ok || cached != "node-a" {
		t.Fatalf("cached owner %q (%v), want node-a", cached, ok)
	}
	// Forget makes the next lookup read the store again, which is what a not_owner reply
	// has to trigger.
	b.Forget(a.Owned()[0])
	if _, ok := b.CachedOwner(a.Owned()[0]); ok {
		t.Fatal("the owner is still cached after Forget")
	}
	// An unowned shard is reported as unowned, not as an error.
	free := freeShard(t, a, b)
	owner, err = b.Owner(ctx, free)
	if err != nil || owner != "" {
		t.Fatalf("unowned shard reported %q, %v", owner, err)
	}
	if _, err := b.Owner(ctx, shards); err == nil {
		t.Fatal("a shard outside the range was accepted")
	}
}

func TestConfigDefaults(t *testing.T) {
	f := newFixture(t)
	m := shard.New(f.rt, f.st, shard.Config{Node: "n", NumShards: 64, Nodes: 4,
		FailoverAllowance: -1})
	cfg := m.Config()
	if cfg.Lease != 30*time.Second {
		t.Errorf("lease %s", cfg.Lease)
	}
	if cfg.Margin != 10*time.Second {
		t.Errorf("margin %s, want a third of the lease", cfg.Margin)
	}
	// Even share 16, allowance a quarter of it.
	if cfg.FailoverAllowance != 4 || cfg.Cap() != 20 {
		t.Errorf("allowance %d, cap %d, want 4 and 20", cfg.FailoverAllowance, cfg.Cap())
	}
	if got := cfg.Interval(); got != 5*time.Second {
		t.Errorf("interval %s, want half the margin", got)
	}
	// A single node holds everything.
	one := shard.New(f.rt, f.st, shard.Config{Node: "n", NumShards: 64})
	if got := one.Config().Cap(); got != 64 {
		t.Errorf("single-node cap %d, want 64", got)
	}
}

func TestAcquireRejectsAShardOutsideTheCluster(t *testing.T) {
	f := newFixture(t)
	m := f.manager("node-a", 1)
	if _, err := m.Acquire(ctx, shards); err == nil {
		t.Fatal("acquiring shard 8 of 8 was accepted")
	}
	if _, err := m.Acquire(ctx, -1); err == nil {
		t.Fatal("acquiring shard -1 was accepted")
	}
}

func TestReleaseLetsAnotherNodeTakeOverOnceTheLeaseLapses(t *testing.T) {
	f := newFixture(t)
	a := f.manager("node-a", 1)
	mustTick(t, a)
	held := a.Owned()[0]
	a.Release(held)
	if a.Holds(held) {
		t.Fatal("the shard is still held after Release")
	}
	// Release does not touch the store, so the lease still has to lapse.
	b := f.manager("node-b", 1)
	mustTick(t, b)
	if b.Holds(held) {
		t.Fatalf("node-b took shard %d while its lease was live", held)
	}
	f.clk.Advance(2 * lease)
	mustTick(t, b)
	if !b.Holds(held) {
		t.Fatalf("node-b did not take shard %d after the lease lapsed", held)
	}
}

// ---- helpers ------------------------------------------------------------------------------

var errUnreachable = errors.New("store is unreachable")

func mustTick(t *testing.T, m *shard.Manager) shard.Changes {
	t.Helper()
	ch, err := m.Tick(ctx)
	if err != nil {
		t.Fatalf("tick: %v", err)
	}
	return ch
}

// freeShard returns a shard neither manager holds.
func freeShard(t *testing.T, ms ...*shard.Manager) int {
	t.Helper()
	for s := 0; s < shards; s++ {
		held := false
		for _, m := range ms {
			if m.Holds(s) {
				held = true
			}
		}
		if !held {
			return s
		}
	}
	t.Fatal("every shard is held")
	return 0
}
