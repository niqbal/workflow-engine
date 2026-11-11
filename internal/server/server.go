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
	"github.com/niqbal/workflow-engine/internal/store"
	"github.com/niqbal/workflow-engine/internal/xsync"
)

// Config for a node.
type Config struct {
	Node               string
	NumShards          int           // fixed for the cluster's life (SPEC §9)
	LongPoll           time.Duration // how long a poll waits on the server clock
	RedispatchInterval time.Duration // re-push unstarted tasks (SPEC §8.2-8.3)
	LeaseDuration      time.Duration
	TokenKey           []byte // HMAC key for task tokens; cluster-wide
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

	mu     xsync.Locker
	rand   rnd.Rand
	fences map[int]store.Fence

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
	return &Server{rt: rt, clk: rt.Clock(), st: st, cfg: cfg, match: matching.New(rt),
		mu: rt.NewLocker(), rand: r, fences: map[int]store.Fence{}}
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

// Start acquires every shard (single-node M1; M4 replaces this with leases and placement).
func (s *Server) Start(ctx context.Context) error {
	for shard := 0; shard < s.cfg.NumShards; shard++ {
		f, err := s.st.AcquireShard(ctx, shard, s.cfg.Node, s.clk.Now(), s.cfg.LeaseDuration)
		if err != nil {
			return fmt.Errorf("server: acquire shard %d: %w", shard, err)
		}
		s.mu.Lock()
		s.fences[shard] = f
		s.mu.Unlock()
	}
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
	s.mu.Lock()
	defer s.mu.Unlock()
	f, ok := s.fences[shard]
	return f, ok
}

// Shards lists owned shards in order.
func (s *Server) Shards() []int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return xsync.SortedKeys(s.fences)
}

func (s *Server) newRunID() string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return fmt.Sprintf("%016x", uint64(s.rand.Int63()))
}
