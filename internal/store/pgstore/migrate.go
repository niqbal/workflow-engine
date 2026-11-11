package pgstore

import (
	"context"
	"embed"
	"fmt"
	"io/fs"
	"strconv"
	"strings"

	"github.com/jackc/pgx/v5"
)

// Migrations are versioned SQL files named NNNN_description.sql, numbered from 1 without
// gaps. A file is never edited once merged; schema changes add a new file.
//
//go:embed migrations/*.sql
var migrationFS embed.FS

type migration struct {
	version int
	name    string
	sql     string
}

// migrationLockID is the advisory lock that serializes concurrent Migrate calls.
const migrationLockID int64 = 0x77666d6967726174 // "wfmigrat"

func migrations() ([]migration, error) {
	entries, err := fs.ReadDir(migrationFS, "migrations")
	if err != nil {
		return nil, err
	}
	var out []migration
	for _, e := range entries { // sorted by name
		name := e.Name()
		prefix, _, ok := strings.Cut(name, "_")
		v, err := strconv.Atoi(prefix)
		if !ok || err != nil || v != len(out)+1 {
			return nil, fmt.Errorf("pgstore: migration %q: want a name starting with %04d_", name, len(out)+1)
		}
		body, err := fs.ReadFile(migrationFS, "migrations/"+name)
		if err != nil {
			return nil, err
		}
		out = append(out, migration{version: v, name: name, sql: string(body)})
	}
	return out, nil
}

// Migrate applies every embedded migration that the database (the first schema on the
// search path) has not applied yet, in one transaction. It is idempotent and safe to call
// concurrently.
func (s *Store) Migrate(ctx context.Context) error {
	s.enter()
	ms, err := migrations()
	if err != nil {
		return err
	}
	return s.tx(ctx, "Migrate", func(tx pgx.Tx) error {
		if _, err := tx.Exec(ctx, `SELECT pg_advisory_xact_lock($1)`, migrationLockID); err != nil {
			return err
		}
		if _, err := tx.Exec(ctx, `CREATE TABLE IF NOT EXISTS schema_migrations (
			version    integer     PRIMARY KEY,
			name       text        NOT NULL,
			applied_at timestamptz NOT NULL DEFAULT now())`); err != nil {
			return err
		}
		var current int
		if err := tx.QueryRow(ctx, `SELECT coalesce(max(version), 0) FROM schema_migrations`).Scan(&current); err != nil {
			return err
		}
		if current > len(ms) {
			return fmt.Errorf("pgstore: database is at migration %d, this build knows %d", current, len(ms))
		}
		for _, m := range ms[current:] {
			if _, err := tx.Exec(ctx, m.sql); err != nil {
				return fmt.Errorf("%s: %w", m.name, err)
			}
			if _, err := tx.Exec(ctx, `INSERT INTO schema_migrations (version, name) VALUES ($1, $2)`, m.version, m.name); err != nil {
				return err
			}
		}
		return nil
	})
}
