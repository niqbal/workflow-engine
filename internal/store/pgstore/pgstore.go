package pgstore

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"

	"github.com/niqbal/workflow-engine/internal/store"
	"github.com/niqbal/workflow-engine/internal/xsync"
)

// Option configures a Store.
type Option func(*Store)

// WithRuntime makes every method call rt.Yield() on entry and builds the connection pool on
// rt's primitives.
func WithRuntime(rt xsync.Runtime) Option { return func(s *Store) { s.rt = rt } }

// WithSchema sets search_path on every pooled connection, so tables are created and read in
// that schema. The name is sent as is; quote it yourself if it needs quoting.
func WithSchema(schema string) Option { return func(s *Store) { s.schema = schema } }

// WithMaxConns limits the number of open connections (default 8).
func WithMaxConns(n int) Option { return func(s *Store) { s.maxConns = n } }

const defaultMaxConns = 8

// taskKinds is the fixed order in which a write locks the task counter rows, so concurrent
// writes cannot deadlock on them.
var taskKinds = []store.TaskKind{store.TransferTask, store.TimerTask}

// Store is a Postgres store.Store. All methods are safe for concurrent use.
type Store struct {
	rt       xsync.Runtime
	schema   string
	maxConns int
	pool     *pool
}

var _ store.Store = (*Store)(nil)

// Open connects to the database at url. It does not apply migrations; call Migrate.
func Open(ctx context.Context, url string, opts ...Option) (*Store, error) {
	cfg, err := pgx.ParseConfig(url)
	if err != nil {
		return nil, fmt.Errorf("pgstore: %w", err)
	}
	s := &Store{maxConns: defaultMaxConns}
	for _, o := range opts {
		o(s)
	}
	if s.schema != "" {
		if cfg.RuntimeParams == nil {
			cfg.RuntimeParams = map[string]string{}
		}
		cfg.RuntimeParams["search_path"] = s.schema
	}
	rt := s.rt
	if rt == nil {
		rt = xsync.NewReal(context.Background())
	}
	s.pool = newPool(cfg, rt, max(s.maxConns, 1))
	if err := s.pool.with(ctx, func(c *pgx.Conn) error { return c.Ping(ctx) }); err != nil {
		s.Close()
		return nil, fmt.Errorf("pgstore: %w", err)
	}
	return s, nil
}

// Close closes the store's connections.
func (s *Store) Close() { s.pool.close() }

// enter makes the call a yield point under a runtime.
func (s *Store) enter() {
	if s.rt != nil {
		s.rt.Yield()
	}
}

// tx runs fn in one transaction, committed if fn returns nil and rolled back otherwise.
func (s *Store) tx(ctx context.Context, op string, fn func(pgx.Tx) error) error {
	err := s.pool.with(ctx, func(c *pgx.Conn) error { return pgx.BeginFunc(ctx, c, fn) })
	if err != nil {
		return fmt.Errorf("pgstore: %s: %w", op, err)
	}
	return nil
}

// read runs fn on a pooled connection outside an explicit transaction.
func (s *Store) read(ctx context.Context, op string, fn func(*pgx.Conn) error) error {
	if err := s.pool.with(ctx, fn); err != nil {
		return fmt.Errorf("pgstore: %s: %w", op, err)
	}
	return nil
}

// checkFence reports whether f authorizes writes to shard. The FOR SHARE lock holds the
// shard row until the transaction ends, so AcquireShard cannot bump the range meanwhile.
func checkFence(ctx context.Context, tx pgx.Tx, f store.Fence, shard int) error {
	if f.Shard != shard {
		return fmt.Errorf("%w: fence for shard %d used on shard %d", store.ErrInvalid, f.Shard, shard)
	}
	var owner string
	var rangeID int64
	err := tx.QueryRow(ctx, `SELECT owner, range_id FROM shards WHERE shard_id = $1 FOR SHARE`, shard).Scan(&owner, &rangeID)
	if errors.Is(err, pgx.ErrNoRows) {
		return store.ErrFenced
	}
	if err != nil {
		return err
	}
	if f.RangeID == 0 || rangeID != f.RangeID || owner != f.Owner {
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

func isUniqueViolation(err error) bool {
	var pgErr *pgconn.PgError
	return errors.As(err, &pgErr) && pgErr.Code == "23505"
}

func insertEvents(ctx context.Context, tx pgx.Tx, key store.RunKey, evs []store.EventRecord) error {
	if len(evs) == 0 {
		return nil
	}
	ids := make([]int64, len(evs))
	data := make([][]byte, len(evs))
	for i, e := range evs {
		ids[i], data[i] = e.ID, e.Data
	}
	_, err := tx.Exec(ctx, `INSERT INTO history (shard_id, workflow_id, run_id, event_id, data)
		SELECT $1, $2, $3, e.id, e.data FROM unnest($4::bigint[], $5::bytea[]) AS e(id, data)`,
		key.Shard, key.WorkflowID, key.RunID, ids, data)
	return err
}

// writeTasks assigns task IDs from the (shard, kind) counter rows and inserts the tasks. It
// returns private copies with IDs set. All tasks belong to shard (checked by checkTasks).
func writeTasks(ctx context.Context, tx pgx.Tx, shard int, tasks []store.Task) ([]store.Task, error) {
	out := cloneTasks(tasks)
	for _, kind := range taskKinds {
		n := 0
		for _, t := range out {
			if t.Kind == kind {
				n++
			}
		}
		if n == 0 {
			continue
		}
		var last int64
		err := tx.QueryRow(ctx, `INSERT INTO task_counters (shard_id, kind, last_id) VALUES ($1, $2, $3)
			ON CONFLICT (shard_id, kind) DO UPDATE SET last_id = task_counters.last_id + EXCLUDED.last_id
			RETURNING last_id`, shard, string(kind), int64(n)).Scan(&last)
		if err != nil {
			return nil, err
		}
		id := last - int64(n)
		for i := range out {
			if out[i].Kind == kind {
				id++
				out[i].ID = id
			}
		}
	}
	for _, t := range out {
		var err error
		switch t.Kind {
		case store.TransferTask:
			_, err = tx.Exec(ctx, `INSERT INTO transfer_tasks (shard_id, task_id, run_shard_id, workflow_id, run_id, payload)
				VALUES ($1, $2, $3, $4, $5, $6)`, t.Shard, t.ID, t.Run.Shard, t.Run.WorkflowID, t.Run.RunID, t.Payload)
		case store.TimerTask:
			_, err = tx.Exec(ctx, `INSERT INTO timer_tasks (shard_id, task_id, fire_at, run_shard_id, workflow_id, run_id, payload)
				VALUES ($1, $2, $3, $4, $5, $6, $7)`, t.Shard, t.ID, t.FireAt, t.Run.Shard, t.Run.WorkflowID, t.Run.RunID, t.Payload)
		}
		if err != nil {
			return nil, err
		}
	}
	return out, nil
}

// deleteTasks removes tasks by key; missing keys and unknown kinds are ignored. All keys
// belong to shard (checked by checkKeys).
func deleteTasks(ctx context.Context, tx pgx.Tx, shard int, keys []store.TaskKey) error {
	for _, kind := range taskKinds {
		var ids []int64
		for _, k := range keys {
			if k.Kind == kind {
				ids = append(ids, k.ID)
			}
		}
		if len(ids) == 0 {
			continue
		}
		q := `DELETE FROM transfer_tasks WHERE shard_id = $1 AND task_id = ANY($2)`
		if kind == store.TimerTask {
			q = `DELETE FROM timer_tasks WHERE shard_id = $1 AND task_id = ANY($2)`
		}
		if _, err := tx.Exec(ctx, q, shard, ids); err != nil {
			return err
		}
	}
	return nil
}

func scanShard(row pgx.Row) (store.ShardInfo, error) {
	var info store.ShardInfo
	var lease *time.Time
	if err := row.Scan(&info.Shard, &info.Owner, &info.RangeID, &lease); err != nil {
		return store.ShardInfo{}, err
	}
	if lease != nil {
		info.LeaseExpiresAt = lease.UTC()
	}
	return info, nil
}

const ensureShard = `INSERT INTO shards (shard_id) VALUES ($1) ON CONFLICT (shard_id) DO NOTHING`

// GetShard returns the shard row, creating an unowned row if absent.
func (s *Store) GetShard(ctx context.Context, shard int) (store.ShardInfo, error) {
	s.enter()
	var info store.ShardInfo
	err := s.read(ctx, "GetShard", func(c *pgx.Conn) error {
		if _, err := c.Exec(ctx, ensureShard, shard); err != nil {
			return err
		}
		var err error
		info, err = scanShard(c.QueryRow(ctx, `SELECT shard_id, owner, range_id, lease_expires_at FROM shards WHERE shard_id = $1`, shard))
		return err
	})
	return info, err
}

// AcquireShard takes the shard if it is unowned, owned by owner, or its lease expired at now.
func (s *Store) AcquireShard(ctx context.Context, shard int, owner string, now time.Time, lease time.Duration) (store.Fence, error) {
	s.enter()
	if owner == "" {
		return store.Fence{}, fmt.Errorf("%w: empty owner", store.ErrInvalid)
	}
	var rangeID int64
	err := s.tx(ctx, "AcquireShard", func(tx pgx.Tx) error {
		if _, err := tx.Exec(ctx, ensureShard, shard); err != nil {
			return err
		}
		sh, err := scanShard(tx.QueryRow(ctx, `SELECT shard_id, owner, range_id, lease_expires_at FROM shards WHERE shard_id = $1 FOR UPDATE`, shard))
		if err != nil {
			return err
		}
		if sh.Owner != "" && sh.Owner != owner && now.Before(sh.LeaseExpiresAt) {
			return store.ErrFenced
		}
		return tx.QueryRow(ctx, `UPDATE shards SET range_id = range_id + 1, owner = $2, lease_expires_at = $3
			WHERE shard_id = $1 RETURNING range_id`, shard, owner, now.Add(lease)).Scan(&rangeID)
	})
	if err != nil {
		return store.Fence{}, err
	}
	return store.Fence{Shard: shard, Owner: owner, RangeID: rangeID}, nil
}

// RenewShard extends the lease if f is still current.
func (s *Store) RenewShard(ctx context.Context, f store.Fence, now time.Time, lease time.Duration) error {
	s.enter()
	if f.RangeID == 0 {
		return store.ErrFenced
	}
	// One conditional UPDATE is the fence check: taking FOR SHARE first and then updating
	// the same row would let two concurrent renewals deadlock on the lock upgrade.
	return s.read(ctx, "RenewShard", func(c *pgx.Conn) error {
		tag, err := c.Exec(ctx, `UPDATE shards SET lease_expires_at = $4
			WHERE shard_id = $1 AND owner = $2 AND range_id = $3`, f.Shard, f.Owner, f.RangeID, now.Add(lease))
		if err != nil {
			return err
		}
		if tag.RowsAffected() == 0 {
			return store.ErrFenced
		}
		return nil
	})
}

// CreateRun writes a new run with its first events, state and tasks.
func (s *Store) CreateRun(ctx context.Context, f store.Fence, r store.NewRun) ([]store.Task, error) {
	s.enter()
	var written []store.Task
	err := s.tx(ctx, "CreateRun", func(tx pgx.Tx) error {
		k := r.Key
		if err := checkFence(ctx, tx, f, k.Shard); err != nil {
			return err
		}
		var exists bool
		if err := tx.QueryRow(ctx, `SELECT EXISTS (SELECT 1 FROM runs WHERE shard_id = $1 AND workflow_id = $2)`,
			k.Shard, k.WorkflowID).Scan(&exists); err != nil {
			return err
		}
		if exists {
			return fmt.Errorf("%w: workflow %q already has a run on shard %d", store.ErrAlreadyExists, k.WorkflowID, k.Shard)
		}
		if len(r.Events) == 0 {
			return fmt.Errorf("%w: a run starts with at least one event", store.ErrInvalid)
		}
		if err := checkEvents(r.Events, 1); err != nil {
			return err
		}
		if err := checkTasks(r.Tasks, k.Shard); err != nil {
			return err
		}
		// The unique constraints decide between concurrent creates that both passed the check.
		_, err := tx.Exec(ctx, `INSERT INTO runs (shard_id, workflow_id, run_id, db_version, next_event_id, is_open, state)
			VALUES ($1, $2, $3, 1, $4, true, $5)`, k.Shard, k.WorkflowID, k.RunID, int64(len(r.Events))+1, r.State)
		if isUniqueViolation(err) {
			return fmt.Errorf("%w: workflow %q already has a run on shard %d", store.ErrAlreadyExists, k.WorkflowID, k.Shard)
		}
		if err != nil {
			return err
		}
		if err := insertEvents(ctx, tx, k, r.Events); err != nil {
			return err
		}
		written, err = writeTasks(ctx, tx, k.Shard, r.Tasks)
		return err
	})
	if err != nil {
		return nil, err
	}
	return written, nil
}

// UpdateRun applies u atomically if f is current and the run is at u.ExpectedDBVersion.
func (s *Store) UpdateRun(ctx context.Context, f store.Fence, key store.RunKey, u store.Update) (int64, []store.Task, error) {
	s.enter()
	var version int64
	var written []store.Task
	err := s.tx(ctx, "UpdateRun", func(tx pgx.Tx) error {
		if err := checkFence(ctx, tx, f, key.Shard); err != nil {
			return err
		}
		var dbVersion, nextEventID int64
		err := tx.QueryRow(ctx, `SELECT db_version, next_event_id FROM runs WHERE shard_id = $1 AND workflow_id = $2 AND run_id = $3`,
			key.Shard, key.WorkflowID, key.RunID).Scan(&dbVersion, &nextEventID)
		if errors.Is(err, pgx.ErrNoRows) {
			return store.ErrNotFound
		}
		if err != nil {
			return err
		}
		if dbVersion != u.ExpectedDBVersion {
			return store.ErrConflict
		}
		if err := checkEvents(u.Events, nextEventID); err != nil {
			return err
		}
		if err := checkTasks(u.NewTasks, key.Shard); err != nil {
			return err
		}
		if err := checkKeys(u.DoneTasks, key.Shard); err != nil {
			return err
		}
		// The compare-and-swap: a writer that read the same version above but commits second
		// re-evaluates the WHERE clause after the first commits and matches no row.
		err = tx.QueryRow(ctx, `UPDATE runs SET db_version = db_version + 1, next_event_id = next_event_id + $5,
			is_open = $6, state = $7
			WHERE shard_id = $1 AND workflow_id = $2 AND run_id = $3 AND db_version = $4
			RETURNING db_version`,
			key.Shard, key.WorkflowID, key.RunID, u.ExpectedDBVersion, int64(len(u.Events)), u.Open, u.State).Scan(&version)
		if errors.Is(err, pgx.ErrNoRows) {
			return store.ErrConflict
		}
		if err != nil {
			return err
		}
		if err := insertEvents(ctx, tx, key, u.Events); err != nil {
			return err
		}
		if err := deleteTasks(ctx, tx, key.Shard, u.DoneTasks); err != nil {
			return err
		}
		written, err = writeTasks(ctx, tx, key.Shard, u.NewTasks)
		return err
	})
	if err != nil {
		return 0, nil, err
	}
	return version, written, nil
}

// GetRun returns the run's snapshot or ErrNotFound.
func (s *Store) GetRun(ctx context.Context, key store.RunKey) (store.RunSnapshot, error) {
	s.enter()
	snap := store.RunSnapshot{Key: key}
	err := s.read(ctx, "GetRun", func(c *pgx.Conn) error {
		err := c.QueryRow(ctx, `SELECT db_version, next_event_id, is_open, state FROM runs
			WHERE shard_id = $1 AND workflow_id = $2 AND run_id = $3`, key.Shard, key.WorkflowID, key.RunID).
			Scan(&snap.DBVersion, &snap.NextEventID, &snap.Open, &snap.State)
		if errors.Is(err, pgx.ErrNoRows) {
			return store.ErrNotFound
		}
		return err
	})
	if err != nil {
		return store.RunSnapshot{}, err
	}
	return snap, nil
}

// GetLatestRun returns the run for workflowID, open or closed. v1 allows one run per
// workflow ID (runs_one_per_workflow), so it is also the most recent.
func (s *Store) GetLatestRun(ctx context.Context, shard int, workflowID string) (store.RunKey, error) {
	s.enter()
	k := store.RunKey{Shard: shard, WorkflowID: workflowID}
	err := s.read(ctx, "GetLatestRun", func(c *pgx.Conn) error {
		err := c.QueryRow(ctx, `SELECT run_id FROM runs WHERE shard_id = $1 AND workflow_id = $2`, shard, workflowID).Scan(&k.RunID)
		if errors.Is(err, pgx.ErrNoRows) {
			return store.ErrNotFound
		}
		return err
	})
	if err != nil {
		return store.RunKey{}, err
	}
	return k, nil
}

// ReadHistory returns up to limit events with ID >= fromID, in order.
func (s *Store) ReadHistory(ctx context.Context, key store.RunKey, fromID int64, limit int) ([]store.EventRecord, error) {
	s.enter()
	var out []store.EventRecord
	err := s.read(ctx, "ReadHistory", func(c *pgx.Conn) error {
		var exists bool
		if err := c.QueryRow(ctx, `SELECT EXISTS (SELECT 1 FROM runs WHERE shard_id = $1 AND workflow_id = $2 AND run_id = $3)`,
			key.Shard, key.WorkflowID, key.RunID).Scan(&exists); err != nil {
			return err
		}
		if !exists {
			return store.ErrNotFound
		}
		if limit <= 0 {
			return nil
		}
		rows, err := c.Query(ctx, `SELECT event_id, data FROM history
			WHERE shard_id = $1 AND workflow_id = $2 AND run_id = $3 AND event_id >= $4
			ORDER BY event_id LIMIT $5`, key.Shard, key.WorkflowID, key.RunID, fromID, limit)
		if err != nil {
			return err
		}
		defer rows.Close()
		for rows.Next() {
			var e store.EventRecord
			if err := rows.Scan(&e.ID, &e.Data); err != nil {
				return err
			}
			out = append(out, e)
		}
		return rows.Err()
	})
	if err != nil {
		return nil, err
	}
	return out, nil
}

// ReadTransferTasks returns undeleted transfer tasks with ID >= fromID, ascending.
func (s *Store) ReadTransferTasks(ctx context.Context, shard int, fromID int64, limit int) ([]store.Task, error) {
	s.enter()
	var out []store.Task
	err := s.read(ctx, "ReadTransferTasks", func(c *pgx.Conn) error {
		rows, err := c.Query(ctx, `SELECT task_id, run_shard_id, workflow_id, run_id, payload FROM transfer_tasks
			WHERE shard_id = $1 AND task_id >= $2 ORDER BY task_id LIMIT $3`, shard, fromID, max(limit, 0))
		if err != nil {
			return err
		}
		defer rows.Close()
		for rows.Next() {
			t := store.Task{Shard: shard, Kind: store.TransferTask}
			if err := rows.Scan(&t.ID, &t.Run.Shard, &t.Run.WorkflowID, &t.Run.RunID, &t.Payload); err != nil {
				return err
			}
			out = append(out, t)
		}
		return rows.Err()
	})
	if err != nil {
		return nil, err
	}
	return out, nil
}

// ReadTimerTasks returns undeleted timer tasks with FireAt <= upTo, ordered by (FireAt, ID).
func (s *Store) ReadTimerTasks(ctx context.Context, shard int, upTo time.Time, limit int) ([]store.Task, error) {
	s.enter()
	var out []store.Task
	err := s.read(ctx, "ReadTimerTasks", func(c *pgx.Conn) error {
		rows, err := c.Query(ctx, `SELECT task_id, fire_at, run_shard_id, workflow_id, run_id, payload FROM timer_tasks
			WHERE shard_id = $1 AND fire_at <= $2 ORDER BY fire_at, task_id LIMIT $3`, shard, upTo, max(limit, 0))
		if err != nil {
			return err
		}
		defer rows.Close()
		for rows.Next() {
			t := store.Task{Shard: shard, Kind: store.TimerTask}
			if err := rows.Scan(&t.ID, &t.FireAt, &t.Run.Shard, &t.Run.WorkflowID, &t.Run.RunID, &t.Payload); err != nil {
				return err
			}
			t.FireAt = t.FireAt.UTC()
			out = append(out, t)
		}
		return rows.Err()
	})
	if err != nil {
		return nil, err
	}
	return out, nil
}

// DeleteTasks removes tasks if f is current. Missing keys are ignored.
func (s *Store) DeleteTasks(ctx context.Context, f store.Fence, keys []store.TaskKey) error {
	s.enter()
	return s.tx(ctx, "DeleteTasks", func(tx pgx.Tx) error {
		if err := checkFence(ctx, tx, f, f.Shard); err != nil {
			return err
		}
		if err := checkKeys(keys, f.Shard); err != nil {
			return err
		}
		return deleteTasks(ctx, tx, f.Shard, keys)
	})
}

func cloneTasks(ts []store.Task) []store.Task {
	out := make([]store.Task, len(ts))
	for i, t := range ts {
		t.Payload = bytes.Clone(t.Payload)
		out[i] = t
	}
	return out
}
