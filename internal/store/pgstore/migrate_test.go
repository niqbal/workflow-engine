package pgstore_test

import (
	"context"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/niqbal/workflow-engine/internal/store/pgstore"
)

var tables = []string{"shards", "runs", "history", "task_counters", "transfer_tasks", "timer_tasks", "schema_migrations"}

// Migrations apply to an empty schema, and applying them again (from a second store too)
// changes nothing.
func TestMigrateEmptyDatabaseAndIdempotent(t *testing.T) {
	url := pgURL(t)
	schema := freshSchema(t, url)
	ctx := context.Background()

	conn, err := pgx.Connect(ctx, url)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close(ctx)
	countTables := func() int {
		t.Helper()
		var n int
		if err := conn.QueryRow(ctx, `SELECT count(*) FROM information_schema.tables WHERE table_schema = $1`, schema).Scan(&n); err != nil {
			t.Fatal(err)
		}
		return n
	}
	if n := countTables(); n != 0 {
		t.Fatalf("fresh schema has %d tables", n)
	}

	var stores []*pgstore.Store
	for i := 0; i < 2; i++ {
		s, err := pgstore.Open(ctx, url, pgstore.WithSchema(schema))
		if err != nil {
			t.Fatal(err)
		}
		defer s.Close()
		stores = append(stores, s)
	}
	for i, s := range []*pgstore.Store{stores[0], stores[0], stores[1]} {
		if err := s.Migrate(ctx); err != nil {
			t.Fatalf("migrate #%d: %v", i+1, err)
		}
		if n := countTables(); n != len(tables) {
			t.Fatalf("after migrate #%d: want %d tables, got %d", i+1, len(tables), n)
		}
		var applied, distinct int
		if err := conn.QueryRow(ctx, `SELECT count(*), count(DISTINCT version) FROM `+schema+`.schema_migrations`).Scan(&applied, &distinct); err != nil {
			t.Fatal(err)
		}
		if applied != 1 || distinct != 1 {
			t.Fatalf("after migrate #%d: want one applied migration, got %d (%d distinct)", i+1, applied, distinct)
		}
	}
	for _, name := range tables {
		var ok bool
		if err := conn.QueryRow(ctx, `SELECT EXISTS (SELECT 1 FROM information_schema.tables WHERE table_schema = $1 AND table_name = $2)`,
			schema, name).Scan(&ok); err != nil {
			t.Fatal(err)
		}
		if !ok {
			t.Errorf("table %s missing", name)
		}
	}
	// The store works after the repeated migrations.
	if _, err := stores[1].AcquireShard(ctx, 0, "a", time.Date(2026, 11, 11, 12, 0, 0, 0, time.UTC), time.Second); err != nil {
		t.Fatal(err)
	}
}

// Concurrent Migrate calls on one empty schema serialize on the advisory lock.
func TestMigrateConcurrent(t *testing.T) {
	url := pgURL(t)
	schema := freshSchema(t, url)
	ctx := context.Background()
	const n = 4
	errs := make(chan error, n)
	for i := 0; i < n; i++ {
		go func() {
			s, err := pgstore.Open(ctx, url, pgstore.WithSchema(schema))
			if err != nil {
				errs <- err
				return
			}
			defer s.Close()
			errs <- s.Migrate(ctx)
		}()
	}
	for i := 0; i < n; i++ {
		if err := <-errs; err != nil {
			t.Fatal(err)
		}
	}
}
