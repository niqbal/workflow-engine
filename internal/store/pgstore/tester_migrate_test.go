package pgstore_test

import (
	"context"
	"os"
	"testing"

	"github.com/jackc/pgx/v5"

	"github.com/niqbal/workflow-engine/internal/store/pgstore"
)

// A database already at a migration newer than this build is refused, not silently used,
// and the refusal changes nothing.
func TestTesterMigrateRefusesNewerDatabase(t *testing.T) {
	url := pgURL(t)
	schema := freshSchema(t, url)
	ctx := context.Background()
	s, err := pgstore.Open(ctx, url, pgstore.WithSchema(schema))
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	if err := s.Migrate(ctx); err != nil {
		t.Fatal(err)
	}
	conn, err := pgx.Connect(ctx, url)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close(ctx)
	if _, err := conn.Exec(ctx, `INSERT INTO `+schema+`.schema_migrations (version, name) VALUES (9999, '9999_future.sql')`); err != nil {
		t.Fatal(err)
	}
	if err := s.Migrate(ctx); err == nil {
		t.Fatal("Migrate accepted a database newer than the build")
	}
	var n int
	if err := conn.QueryRow(ctx, `SELECT count(*) FROM `+schema+`.schema_migrations`).Scan(&n); err != nil {
		t.Fatal(err)
	}
	// The refused Migrate must not have applied or removed anything: one row per embedded
	// migration, plus the planted future one.
	if want := migrationCount(t) + 1; n != want {
		t.Fatalf("refused Migrate changed schema_migrations: %d rows, want %d", n, want)
	}
}

// Migrating into a configured schema creates nothing outside it, and two schemas in one
// database are migrated independently.
func TestTesterMigrateStaysInSchema(t *testing.T) {
	url := pgURL(t)
	ctx := context.Background()
	conn, err := pgx.Connect(ctx, url)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close(ctx)
	countOutside := func(a, b string) int {
		t.Helper()
		var n int
		if err := conn.QueryRow(ctx, `SELECT count(*) FROM information_schema.tables
			WHERE table_name = 'schema_migrations' AND table_schema NOT IN ($1, $2) AND table_schema NOT LIKE 't\_%'`, a, b).Scan(&n); err != nil {
			t.Fatal(err)
		}
		return n
	}
	a, b := freshSchema(t, url), freshSchema(t, url)
	before := countOutside(a, b)
	for _, sc := range []string{a, b} {
		s, err := pgstore.Open(ctx, url, pgstore.WithSchema(sc))
		if err != nil {
			t.Fatal(err)
		}
		if err := s.Migrate(ctx); err != nil {
			s.Close()
			t.Fatalf("migrate %s: %v", sc, err)
		}
		s.Close()
		var got int
		if err := conn.QueryRow(ctx, `SELECT count(*) FROM information_schema.tables WHERE table_schema = $1`, sc).Scan(&got); err != nil {
			t.Fatal(err)
		}
		if got != len(tables) {
			t.Fatalf("schema %s: want %d tables, got %d", sc, len(tables), got)
		}
	}
	if after := countOutside(a, b); after != before {
		t.Fatalf("migration leaked outside its schema: %d -> %d", before, after)
	}
}

// migrationCount is how many migrations this build embeds, counted from the files so adding
// one does not need a number changed here.
func migrationCount(t *testing.T) int {
	t.Helper()
	entries, err := os.ReadDir("migrations")
	if err != nil {
		t.Fatal(err)
	}
	return len(entries)
}
