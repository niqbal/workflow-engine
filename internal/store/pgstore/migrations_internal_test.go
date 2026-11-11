package pgstore

import (
	"strings"
	"testing"
)

// The embedded migrations are numbered from 1 without gaps. Runs without a database.
func TestEmbeddedMigrationsAreVersioned(t *testing.T) {
	ms, err := migrations()
	if err != nil {
		t.Fatal(err)
	}
	if len(ms) == 0 {
		t.Fatal("no migrations embedded")
	}
	for i, m := range ms {
		if m.version != i+1 || !strings.HasSuffix(m.name, ".sql") || strings.TrimSpace(m.sql) == "" {
			t.Fatalf("migration %d: %+v", i, m.name)
		}
	}
	if !strings.Contains(ms[0].sql, "runs_one_per_workflow UNIQUE (shard_id, workflow_id)") {
		t.Fatal("runs must be unique per (shard, workflow ID)")
	}
}
