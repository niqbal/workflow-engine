// Package store defines persistence for runs, history and the outbox (SPEC §8.1, §12).
//
// The store is generic: events and mutable state are opaque bytes. It enforces the three
// rules the guarantees rest on: every write is fenced by the shard's range ID (G6), every
// write compare-and-swaps the run's DBVersion (D11), and events, state and outbox tasks
// commit in one transaction (§8.1).
package store

import (
	"context"
	"errors"
	"time"
)

var (
	ErrFenced        = errors.New("store: shard fence is stale")
	ErrConflict      = errors.New("store: run was modified concurrently")
	ErrNotFound      = errors.New("store: not found")
	ErrAlreadyExists = errors.New("store: already exists")
	ErrInvalid       = errors.New("store: invalid write")
	// ErrClusterMismatch means the cluster was created with a different shard count.
	// NumShards is fixed for a cluster's life (SPEC §9), so a node configured with another
	// value would route by a different mapping and write to shards nobody owns.
	ErrClusterMismatch = errors.New("store: cluster shard count does not match")
)

// ClusterInfo is what a cluster fixes when it is created and can never change.
type ClusterInfo struct {
	NumShards int       `json:"numShards"`
	CreatedAt time.Time `json:"createdAt"`
}

// Fence is a shard ownership epoch. Writes carrying a fence whose RangeID is no longer the
// shard's current one fail with ErrFenced.
type Fence struct {
	Shard   int    `json:"shard"`
	Owner   string `json:"owner"`
	RangeID int64  `json:"rangeID"`
}

// ShardInfo is the shard row.
type ShardInfo struct {
	Shard          int       `json:"shard"`
	Owner          string    `json:"owner"`
	RangeID        int64     `json:"rangeID"`
	LeaseExpiresAt time.Time `json:"leaseExpiresAt"`
}

// RunKey identifies one run.
type RunKey struct {
	Shard      int    `json:"shard"`
	WorkflowID string `json:"workflowID"`
	RunID      string `json:"runID"`
}

// EventRecord is one encoded history event.
type EventRecord struct {
	ID   int64  `json:"id"`
	Data []byte `json:"data"`
}

// RunSnapshot is the stored state of a run.
type RunSnapshot struct {
	Key         RunKey `json:"key"`
	DBVersion   int64  `json:"dbVersion"` // starts at 1, +1 on every UpdateRun
	NextEventID int64  `json:"nextEventID"`
	Open        bool   `json:"open"`
	State       []byte `json:"state"`
}

// TaskKind separates the two outbox queues.
type TaskKind string

const (
	TransferTask TaskKind = "transfer"
	TimerTask    TaskKind = "timer"
)

// Task is an outbox record. The store assigns ID on write; IDs grow per shard and kind but
// may commit out of order (SPEC §8.1). FireAt is set for timer tasks only.
type Task struct {
	Shard   int       `json:"shard"`
	Kind    TaskKind  `json:"kind"`
	ID      int64     `json:"id"`
	FireAt  time.Time `json:"fireAt,omitempty"`
	Run     RunKey    `json:"run"`
	Payload []byte    `json:"payload"`
}

// Key returns the task's key.
func (t Task) Key() TaskKey { return TaskKey{Shard: t.Shard, Kind: t.Kind, ID: t.ID} }

// TaskKey identifies a task.
type TaskKey struct {
	Shard int      `json:"shard"`
	Kind  TaskKind `json:"kind"`
	ID    int64    `json:"id"`
}

// NewRun is the first write of a run.
type NewRun struct {
	Key    RunKey
	Events []EventRecord // must start at ID 1 and be contiguous
	State  []byte
	Tasks  []Task // Shard must equal Key.Shard; ID is assigned
}

// Update is one atomic state change of a run (SPEC §8.1).
type Update struct {
	ExpectedDBVersion int64
	Events            []EventRecord // appended; first ID must equal the stored NextEventID
	State             []byte
	Open              bool
	NewTasks          []Task
	DoneTasks         []TaskKey
}

// Store is implemented by memstore (tests, simulation) and pgstore (production).
// All methods are safe for concurrent use.
type Store interface {
	// GetShard returns the shard row, creating an unowned row (RangeID 0) if absent.
	GetShard(ctx context.Context, shard int) (ShardInfo, error)
	// AcquireShard takes ownership if the shard is unowned, owned by owner, or its lease has
	// expired at now. It increments RangeID and returns the new fence. Otherwise ErrFenced.
	AcquireShard(ctx context.Context, shard int, owner string, now time.Time, lease time.Duration) (Fence, error)
	// RenewShard extends the lease if f is still current, else ErrFenced.
	RenewShard(ctx context.Context, f Fence, now time.Time, lease time.Duration) error

	// CreateRun writes a new run. ErrAlreadyExists if the RunKey exists or the shard already
	// has a run with the same WorkflowID: v1 uses a workflow ID once (SPEC §8.4), and enforcing
	// it here makes concurrent starts safe even without a node-local lock.
	CreateRun(ctx context.Context, f Fence, r NewRun) ([]Task, error)
	// UpdateRun applies u if f is current (else ErrFenced) and the run's DBVersion equals
	// u.ExpectedDBVersion (else ErrConflict). It returns the new DBVersion and the written
	// tasks with IDs assigned. DoneTasks that no longer exist are ignored.
	UpdateRun(ctx context.Context, f Fence, key RunKey, u Update) (int64, []Task, error)
	// GetRun returns the run's snapshot or ErrNotFound.
	GetRun(ctx context.Context, key RunKey) (RunSnapshot, error)
	// GetLatestRun returns the most recently created run for a workflow ID, open or closed.
	GetLatestRun(ctx context.Context, shard int, workflowID string) (RunKey, error)
	// ReadHistory returns up to limit events with ID >= fromID, in order.
	ReadHistory(ctx context.Context, key RunKey, fromID int64, limit int) ([]EventRecord, error)

	// ReadTransferTasks returns undeleted transfer tasks with ID >= fromID, ascending.
	ReadTransferTasks(ctx context.Context, shard int, fromID int64, limit int) ([]Task, error)
	// ReadTimerTasks returns undeleted timer tasks with FireAt <= upTo, ordered by (FireAt, ID).
	ReadTimerTasks(ctx context.Context, shard int, upTo time.Time, limit int) ([]Task, error)
	// DeleteTasks removes tasks if f is current, else ErrFenced. Missing keys are ignored.
	DeleteTasks(ctx context.Context, f Fence, keys []TaskKey) error

	// EnsureCluster records numShards and now for a store that holds no cluster yet, and
	// returns what the store holds either way. It never changes a recorded value: a
	// different numShards is ErrClusterMismatch, with the recorded ClusterInfo returned
	// alongside so a node can say what it disagreed with.
	//
	// Every node calls this before it serves anything. It is the one piece of the cluster's
	// configuration that cannot be taken on trust from a config file, because getting it
	// wrong on one node routes that node's writes to shards by a mapping nobody else uses.
	EnsureCluster(ctx context.Context, numShards int, now time.Time) (ClusterInfo, error)
}
