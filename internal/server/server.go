// Package server is one node of the engine (SPEC §6). This file and runs.go are the
// hand-owned core: configuration, shard ownership for a single node, and the one write path
// every state change goes through (SPEC §8.1). The service handlers (service.go) and the
// transfer and timer processors are factory work items (WI-017..022) built on top.
package server

import (
	"context"
	"fmt"
	"hash/fnv"
	"time"

	"github.com/niqbal/workflow-engine/internal/clock"
	"github.com/niqbal/workflow-engine/internal/rnd"
	"github.com/niqbal/workflow-engine/internal/server/matching"
	"github.com/niqbal/workflow-engine/internal/shard"
	"github.com/niqbal/workflow-engine/internal/store"
	"github.com/niqbal/workflow-engine/internal/transport"
	"github.com/niqbal/workflow-engine/internal/xsync"
)

// Config for a node.
type Config struct {
	Node               string
	NumShards          int           // fixed for the cluster's life (SPEC §9)
	LongPoll           time.Duration // how long a poll waits on the server clock
	RedispatchInterval time.Duration // re-push unstarted tasks (SPEC §8.2-8.3)
	LeaseDuration      time.Duration
	// LeaseMargin is how long before a lease expires the node stops serving the shard.
	// Zero means a third of LeaseDuration (SPEC §9).
	LeaseMargin time.Duration
	// Nodes is how many nodes the cluster is configured with, which sets each node's even
	// share of the shards. Zero or one means this node may own every shard.
	Nodes int
	// FailoverAllowance is how many shards beyond its even share a node may take over from
	// a node whose lease has lapsed. Negative means a quarter of the even share.
	FailoverAllowance int
	TokenKey          []byte // HMAC key for task tokens; cluster-wide
}

func (c Config) withDefaults() Config {
	if c.Node == "" {
		c.Node = "node-1"
	}
	if c.NumShards <= 0 {
		c.NumShards = 4
	}
	if c.LongPoll <= 0 {
		c.LongPoll = 30 * time.Second
	}
	if c.RedispatchInterval <= 0 {
		c.RedispatchInterval = 30 * time.Second
	}
	if c.LeaseDuration <= 0 {
		c.LeaseDuration = 30 * time.Second
	}
	return c
}

// Server is one node. M1 runs a single node that owns every shard.
type Server struct {
	rt    xsync.Runtime
	clk   clock.Clock
	st    store.Store
	cfg   Config
	match *matching.Matcher

	mu   xsync.Locker
	rand rnd.Rand
	// shards is who owns what: the node's leases, their fences and the stop-serving
	// margin (internal/shard, WI-050). It replaced a plain fence map, so there is one
	// answer to "may I write to this shard" and the lease logic is testable on its own.
	shards *shard.Manager
	// peerTransport reaches the other nodes, for queue pushes (router.go). Nil on a node
	// that has no peers.
	peerTransport transport.Transport

	// sub holds per-subsystem state. Its type lives in subsystems.go, which work items may
	// extend; this file stays hand-owned.
	sub subsystems
}

// New builds a node. r provides run IDs; give each node its own seed.
func New(rt xsync.Runtime, st store.Store, r rnd.Rand, cfg Config) *Server {
	cfg = cfg.withDefaults()
	if len(cfg.TokenKey) == 0 {
		cfg.TokenKey = generatedTokenKey(r)
	} else {
		cfg.TokenKey = append([]byte(nil), cfg.TokenKey...)
	}
	s := &Server{rt: rt, clk: rt.Clock(), st: st, cfg: cfg, match: matching.New(rt),
		mu: rt.NewLocker(), rand: r}
	s.shards = shard.New(rt, st, shard.Config{
		Node:              cfg.Node,
		NumShards:         cfg.NumShards,
		Nodes:             cfg.Nodes,
		Lease:             cfg.LeaseDuration,
		Margin:            cfg.LeaseMargin,
		FailoverAllowance: cfg.FailoverAllowance,
	})
	return s
}

func generatedTokenKey(r rnd.Rand) []byte {
	key := make([]byte, 32)
	for i := 0; i < len(key); i += 8 {
		v := uint64(r.Int63())
		for j := 0; j < 8 && i+j < len(key); j++ {
			key[i+j] = byte(v >> (8 * j))
		}
	}
	return key
}

// Start acquires every shard, which is the single-node case. A cluster calls StartCluster
// instead and lets placement decide (WI-050).
func (s *Server) Start(ctx context.Context) error {
	for shard := 0; shard < s.cfg.NumShards; shard++ {
		if _, err := s.shards.Acquire(ctx, shard); err != nil {
			return fmt.Errorf("server: %w", err)
		}
	}
	s.refreshShardGaugeMetric()
	s.refreshPendingTimersMetric()
	return nil
}

// Config returns the effective configuration.
func (s *Server) Config() Config { return s.cfg }

// Matcher exposes the node's task queues.
func (s *Server) Matcher() *matching.Matcher { return s.match }

// DropQueuedTasks discards in-memory matching, as a restart would.
func (s *Server) DropQueuedTasks() { s.match.Drop() }

// ShardFor maps a workflow ID to its shard.
func (s *Server) ShardFor(workflowID string) int {
	h := fnv.New32a()
	h.Write([]byte(workflowID))
	return int(h.Sum32() % uint32(s.cfg.NumShards))
}

func (s *Server) fence(shard int) (store.Fence, bool) {
	return s.shards.Fence(shard)
}

// Shards lists the shards this node owns and may still serve, in order.
func (s *Server) Shards() []int { return s.shards.Owned() }

// ShardManager exposes ownership for routing and for the cluster loop.
func (s *Server) ShardManager() *shard.Manager { return s.shards }

func (s *Server) newRunID() string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return fmt.Sprintf("%016x", uint64(s.rand.Int63()))
}
