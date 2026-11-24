package memstore

import (
	"bytes"
	"context"
	"fmt"
	"slices"
	"time"

	"github.com/niqbal/workflow-engine/internal/store"
	"github.com/niqbal/workflow-engine/internal/xsync"
)

// Faults are fault-injection hooks. op is the Store method name, e.g. "UpdateRun". Nil hooks
// do nothing. Only writes (AcquireShard, RenewShard, CreateRun, UpdateRun, DeleteTasks) call
// them.
type Faults struct {
	// BeforeCommit returning an error aborts the write with nothing committed.
	BeforeCommit func(op string) error
	// AfterCommit returning an error is returned to the caller although the write took
	// effect (an ambiguous commit).
	AfterCommit func(op string) error
	// CommitDelay is waited on the runtime's clock after task IDs are allocated and before
	// the write becomes visible, so a later task ID can commit first. Ignored without a
	// runtime.
	CommitDelay func(op string) time.Duration
}

// CommitEntry is one write that took effect, for the simulator's ghost log (SPEC §11.3, I6).
//
// Op is the Store method. Fence is the fence the write carried, except for AcquireShard,
// where Granted is true and Fence is the epoch the store just handed out. An ambiguous
// commit (a write that took effect and then returned an error through Faults.AfterCommit)
// is recorded like any other, which is the point: the ghost log says what the store did,
// not what the caller was told.
type CommitEntry struct {
	Op      string
	Fence   store.Fence
	Granted bool
	Run     store.RunKey
}

// CommitLog receives every committed write, in commit order, while the store's lock is
// held. It must not block and must not call back into the store.
type CommitLog func(CommitEntry)

// Option configures a Store.
type Option func(*Store)

// WithRuntime makes every method call rt.Yield() on entry and runs commit delays on rt's clock.
func WithRuntime(rt xsync.Runtime) Option { return func(s *Store) { s.rt = rt } }

// WithFaults installs fault hooks.
func WithFaults(f Faults) Option { return func(s *Store) { s.faults = f } }

// WithCommitLog installs a commit log. The simulator keeps the log outside the store, so
// invariant I6 does not have to trust the store's own view of who owns a shard.
func WithCommitLog(l CommitLog) Option { return func(s *Store) { s.clog = l } }

type queueKey struct {
	shard int
	kind  store.TaskKind
}

type workflowKey struct {
	shard      int
	workflowID string
}

type run struct {
	snap   store.RunSnapshot
	events []store.EventRecord // events[i].ID == i+1
}

// Store is an in-memory store.Store. All methods are safe for concurrent use.
type Store struct {
	rt     xsync.Runtime
	faults Faults
	clog   CommitLog
	mu     xsync.Locker
	delay  xsync.Cond // never signalled; commit delays wait on it with a deadline

	shards map[int]*store.ShardInfo
	runs   map[store.RunKey]*run
	latest map[workflowKey]store.RunKey
	tasks  map[queueKey]map[int64]store.Task
	nextID map[queueKey]int64
}

var _ store.Store = (*Store)(nil)

// New returns an empty store.
func New(opts ...Option) *Store {
	s := &Store{
		shards: map[int]*store.ShardInfo{},
		runs:   map[store.RunKey]*run{},
		latest: map[workflowKey]store.RunKey{},
		tasks:  map[queueKey]map[int64]store.Task{},
		nextID: map[queueKey]int64{},
	}
	for _, o := range opts {
		o(s)
	}
	if s.rt != nil {
		s.mu = s.rt.NewLocker()
		s.delay = s.rt.NewCond(s.mu)
	} else {
		s.mu = xsync.NewReal(context.Background()).NewLocker()
	}
	return s
}

// enter makes the call a yield point under a runtime.
func (s *Store) enter() {
	if s.rt != nil {
		s.rt.Yield()
	}
}

// write runs one write transaction. check validates against the current state; it runs
// again after a commit delay, because other writes may have committed meanwhile. alloc runs
// once after the first successful check, apply makes the write visible.
func (s *Store) write(op string, check func() error, alloc func(), apply func()) error {
	s.enter()
	if h := s.faults.BeforeCommit; h != nil {
		if err := h(op); err != nil {
			return err
		}
	}
	var delay time.Duration
	if s.rt != nil && s.faults.CommitDelay != nil {
		delay = s.faults.CommitDelay(op)
	}
	s.mu.Lock()
	err := check()
	if err == nil {
		if alloc != nil {
			alloc()
		}
		if delay > 0 {
			deadline := s.rt.Clock().Now().Add(delay)
			for s.delay.WaitUntil(deadline) {
			}
			err = check()
		}
	}
	if err == nil {
		apply()
	}
	s.mu.Unlock()
	if err != nil {
		return err
	}
	if h := s.faults.AfterCommit; h != nil {
		if err := h(op); err != nil {
			s.enter() // commit, yield, error: others may see the write before the caller fails
			return err
		}
	}
	return nil
}

// logCommit appends to the commit log. Called from an apply closure, with mu held, so the
// log order is the order writes became visible.
func (s *Store) logCommit(e CommitEntry) {
	if s.clog != nil {
		s.clog(e)
	}
}

// checkFence reports whether f authorizes writes to shard. Called with mu held.
func (s *Store) checkFence(f store.Fence, shard int) error {
	if f.Shard != shard {
		return fmt.Errorf("%w: fence for shard %d used on shard %d", store.ErrInvalid, f.Shard, shard)
	}
	sh := s.shards[shard]
	if sh == nil || f.RangeID == 0 || sh.RangeID != f.RangeID || sh.Owner != f.Owner {
		return store.ErrFenced
	}
	return nil
}

func checkEvents(evs []store.EventRecord, first int64) error {
	for i, e := range evs {
		if e.ID != first+int64(i) {
			return fmt.Errorf("%w: event ID %d, want %d", store.ErrInvalid, e.ID, first+int64(i))
		}
	}
	return nil
}

func checkTasks(tasks []store.Task, shard int) error {
	for _, t := range tasks {
		if t.Shard != shard {
			return fmt.Errorf("%w: task for shard %d in a write to shard %d", store.ErrInvalid, t.Shard, shard)
		}
		if t.Kind != store.TransferTask && t.Kind != store.TimerTask {
			return fmt.Errorf("%w: task kind %q", store.ErrInvalid, t.Kind)
		}
	}
	return nil
}

func checkKeys(keys []store.TaskKey, shard int) error {
	for _, k := range keys {
		if k.Shard != shard {
			return fmt.Errorf("%w: task key for shard %d in a write to shard %d", store.ErrInvalid, k.Shard, shard)
		}
	}
	return nil
}

// allocate assigns task IDs and returns private copies. Called with mu held.
func (s *Store) allocate(tasks []store.Task) []store.Task {
	out := cloneTasks(tasks)
	for i := range out {
		q := queueKey{out[i].Shard, out[i].Kind}
		s.nextID[q]++
		out[i].ID = s.nextID[q]
	}
	return out
}

// putTasks makes allocated tasks visible. Called with mu held.
func (s *Store) putTasks(tasks []store.Task) {
	for _, t := range tasks {
		q := queueKey{t.Shard, t.Kind}
		if s.tasks[q] == nil {
			s.tasks[q] = map[int64]store.Task{}
		}
		s.tasks[q][t.ID] = t
	}
}

func (s *Store) deleteTasks(keys []store.TaskKey) {
	for _, k := range keys {
		delete(s.tasks[queueKey{k.Shard, k.Kind}], k.ID)
	}
}

// GetShard returns the shard row, creating an unowned row if absent.
func (s *Store) GetShard(ctx context.Context, shard int) (store.ShardInfo, error) {
	s.enter()
	s.mu.Lock()
	defer s.mu.Unlock()
	sh := s.shards[shard]
	if sh == nil {
		sh = &store.ShardInfo{Shard: shard}
		s.shards[shard] = sh
	}
	return *sh, nil
}

// AcquireShard takes the shard if it is unowned, owned by owner, or its lease expired at now.
func (s *Store) AcquireShard(ctx context.Context, shard int, owner string, now time.Time, lease time.Duration) (store.Fence, error) {
	var f store.Fence
	err := s.write("AcquireShard",
		func() error {
			if owner == "" {
				return fmt.Errorf("%w: empty owner", store.ErrInvalid)
			}
			sh := s.shards[shard]
			if sh != nil && sh.Owner != "" && sh.Owner != owner && now.Before(sh.LeaseExpiresAt) {
				return store.ErrFenced
			}
			return nil
		},
		nil,
		func() {
			sh := s.shards[shard]
			if sh == nil {
				sh = &store.ShardInfo{Shard: shard}
				s.shards[shard] = sh
			}
			sh.RangeID++
			sh.Owner = owner
			sh.LeaseExpiresAt = now.Add(lease)
			f = store.Fence{Shard: shard, Owner: owner, RangeID: sh.RangeID}
			s.logCommit(CommitEntry{Op: "AcquireShard", Fence: f, Granted: true})
		})
	if err != nil {
		return store.Fence{}, err
	}
	return f, nil
}

// RenewShard extends the lease if f is still current.
func (s *Store) RenewShard(ctx context.Context, f store.Fence, now time.Time, lease time.Duration) error {
	return s.write("RenewShard",
		func() error { return s.checkFence(f, f.Shard) },
		nil,
		func() {
			s.shards[f.Shard].LeaseExpiresAt = now.Add(lease)
			s.logCommit(CommitEntry{Op: "RenewShard", Fence: f})
		})
}

// CreateRun writes a new run with its first events, state and tasks.
func (s *Store) CreateRun(ctx context.Context, f store.Fence, r store.NewRun) ([]store.Task, error) {
	var written []store.Task
	err := s.write("CreateRun",
		func() error {
			if err := s.checkFence(f, r.Key.Shard); err != nil {
				return err
			}
			if _, dup := s.latest[workflowKey{r.Key.Shard, r.Key.WorkflowID}]; dup {
				if _, same := s.runs[r.Key]; !same {
					return fmt.Errorf("%w: workflow %q already has a run on shard %d", store.ErrAlreadyExists, r.Key.WorkflowID, r.Key.Shard)
				}
			}
			if _, ok := s.runs[r.Key]; ok {
				return store.ErrAlreadyExists
			}
			if len(r.Events) == 0 {
				return fmt.Errorf("%w: a run starts with at least one event", store.ErrInvalid)
			}
			if err := checkEvents(r.Events, 1); err != nil {
				return err
			}
			return checkTasks(r.Tasks, r.Key.Shard)
		},
		func() { written = s.allocate(r.Tasks) },
		func() {
			s.runs[r.Key] = &run{
				snap: store.RunSnapshot{Key: r.Key, DBVersion: 1, NextEventID: int64(len(r.Events)) + 1,
					Open: true, State: bytes.Clone(r.State)},
				events: cloneEvents(r.Events),
			}
			s.latest[workflowKey{r.Key.Shard, r.Key.WorkflowID}] = r.Key
			s.putTasks(written)
			s.logCommit(CommitEntry{Op: "CreateRun", Fence: f, Run: r.Key})
		})
	if err != nil {
		return nil, err
	}
	return cloneTasks(written), nil
}

// UpdateRun applies u atomically if f is current and the run is at u.ExpectedDBVersion.
func (s *Store) UpdateRun(ctx context.Context, f store.Fence, key store.RunKey, u store.Update) (int64, []store.Task, error) {
	var written []store.Task
	var version int64
	err := s.write("UpdateRun",
		func() error {
			if err := s.checkFence(f, key.Shard); err != nil {
				return err
			}
			r, ok := s.runs[key]
			if !ok {
				return store.ErrNotFound
			}
			if r.snap.DBVersion != u.ExpectedDBVersion {
				return store.ErrConflict
			}
			if err := checkEvents(u.Events, r.snap.NextEventID); err != nil {
				return err
			}
			if err := checkTasks(u.NewTasks, key.Shard); err != nil {
				return err
			}
			return checkKeys(u.DoneTasks, key.Shard)
		},
		func() { written = s.allocate(u.NewTasks) },
		func() {
			r := s.runs[key]
			r.events = append(r.events, cloneEvents(u.Events)...)
			r.snap.DBVersion++
			r.snap.NextEventID += int64(len(u.Events))
			r.snap.Open = u.Open
			r.snap.State = bytes.Clone(u.State)
			s.deleteTasks(u.DoneTasks)
			s.putTasks(written)
			version = r.snap.DBVersion
			s.logCommit(CommitEntry{Op: "UpdateRun", Fence: f, Run: key})
		})
	if err != nil {
		return 0, nil, err
	}
	return version, cloneTasks(written), nil
}

// GetRun returns the run's snapshot or ErrNotFound.
func (s *Store) GetRun(ctx context.Context, key store.RunKey) (store.RunSnapshot, error) {
	s.enter()
	s.mu.Lock()
	defer s.mu.Unlock()
	r, ok := s.runs[key]
	if !ok {
		return store.RunSnapshot{}, store.ErrNotFound
	}
	snap := r.snap
	snap.State = bytes.Clone(snap.State)
	return snap, nil
}

// GetLatestRun returns the most recently created run for workflowID, open or closed.
func (s *Store) GetLatestRun(ctx context.Context, shard int, workflowID string) (store.RunKey, error) {
	s.enter()
	s.mu.Lock()
	defer s.mu.Unlock()
	k, ok := s.latest[workflowKey{shard, workflowID}]
	if !ok {
		return store.RunKey{}, store.ErrNotFound
	}
	return k, nil
}

// ReadHistory returns up to limit events with ID >= fromID, in order.
func (s *Store) ReadHistory(ctx context.Context, key store.RunKey, fromID int64, limit int) ([]store.EventRecord, error) {
	s.enter()
	s.mu.Lock()
	defer s.mu.Unlock()
	r, ok := s.runs[key]
	if !ok {
		return nil, store.ErrNotFound
	}
	fromID = max(fromID, 1)
	if limit <= 0 || fromID > int64(len(r.events)) {
		return nil, nil
	}
	end := min(fromID-1+int64(limit), int64(len(r.events)))
	return cloneEvents(r.events[fromID-1 : end]), nil
}

// ReadTransferTasks returns undeleted transfer tasks with ID >= fromID, ascending.
func (s *Store) ReadTransferTasks(ctx context.Context, shard int, fromID int64, limit int) ([]store.Task, error) {
	s.enter()
	s.mu.Lock()
	defer s.mu.Unlock()
	q := s.tasks[queueKey{shard, store.TransferTask}]
	var out []store.Task
	for _, id := range xsync.SortedKeys(q) {
		if len(out) >= limit {
			break
		}
		if id >= fromID {
			out = append(out, cloneTask(q[id]))
		}
	}
	return out, nil
}

// ReadTimerTasks returns undeleted timer tasks with FireAt <= upTo, ordered by (FireAt, ID).
func (s *Store) ReadTimerTasks(ctx context.Context, shard int, upTo time.Time, limit int) ([]store.Task, error) {
	s.enter()
	s.mu.Lock()
	defer s.mu.Unlock()
	q := s.tasks[queueKey{shard, store.TimerTask}]
	var due []store.Task
	for _, id := range xsync.SortedKeys(q) {
		if t := q[id]; !t.FireAt.After(upTo) {
			due = append(due, t)
		}
	}
	slices.SortStableFunc(due, func(a, b store.Task) int { return a.FireAt.Compare(b.FireAt) })
	due = due[:min(len(due), max(limit, 0))]
	return cloneTasks(due), nil
}

// DeleteTasks removes tasks if f is current. Missing keys are ignored.
func (s *Store) DeleteTasks(ctx context.Context, f store.Fence, keys []store.TaskKey) error {
	return s.write("DeleteTasks",
		func() error {
			if err := s.checkFence(f, f.Shard); err != nil {
				return err
			}
			return checkKeys(keys, f.Shard)
		},
		nil,
		func() {
			s.deleteTasks(keys)
			s.logCommit(CommitEntry{Op: "DeleteTasks", Fence: f})
		})
}

func cloneEvents(evs []store.EventRecord) []store.EventRecord {
	out := make([]store.EventRecord, len(evs))
	for i, e := range evs {
		out[i] = store.EventRecord{ID: e.ID, Data: bytes.Clone(e.Data)}
	}
	return out
}

func cloneTask(t store.Task) store.Task {
	t.Payload = bytes.Clone(t.Payload)
	return t
}

func cloneTasks(ts []store.Task) []store.Task {
	out := make([]store.Task, len(ts))
	for i, t := range ts {
		out[i] = cloneTask(t)
	}
	return out
}
