package pgstore_test

import (
	"context"
	"errors"
	"fmt"
	"math/rand/v2"
	"os"
	"sync/atomic"
	"testing"

	"github.com/jackc/pgx/v5"

	"github.com/niqbal/workflow-engine/internal/store"
	"github.com/niqbal/workflow-engine/internal/store/pgstore"
	"github.com/niqbal/workflow-engine/internal/store/storetest"
	"github.com/niqbal/workflow-engine/internal/xsync"
)

// pgURL returns PG_URL. The tests run whenever it is set (the factory sandbox sets it).
func pgURL(t *testing.T) string {
	t.Helper()
	u := os.Getenv("PG_URL")
	if u == "" {
		t.Skip("PG_URL is not set")
	}
	return u
}

// freshSchema creates an empty schema t_<random> and drops it when the test ends.
func freshSchema(t *testing.T, url string) string {
	t.Helper()
	ctx := context.Background()
	admin, err := pgx.Connect(ctx, url)
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	name := fmt.Sprintf("t_%016x", rand.Uint64())
	if _, err := admin.Exec(ctx, "CREATE SCHEMA "+name); err != nil {
		admin.Close(ctx)
		t.Fatalf("create schema: %v", err)
	}
	t.Cleanup(func() {
		if _, err := admin.Exec(ctx, "DROP SCHEMA "+name+" CASCADE"); err != nil {
			t.Errorf("drop schema %s: %v", name, err)
		}
		admin.Close(ctx)
	})
	return name
}

// openStore opens a migrated store in a fresh schema, closed before the schema is dropped.
func openStore(t *testing.T, opts ...pgstore.Option) *pgstore.Store {
	t.Helper()
	url := pgURL(t)
	schema := freshSchema(t, url)
	s, err := pgstore.Open(context.Background(), url, append(opts, pgstore.WithSchema(schema))...)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	t.Cleanup(s.Close)
	if err := s.Migrate(context.Background()); err != nil {
		t.Fatalf("migrate: %v", err)
	}
	return s
}

func TestPgstoreContract(t *testing.T) {
	pgURL(t)
	storetest.Run(t, func(t *testing.T) store.Store { return openStore(t) })
}

// The same contract with a production runtime, so the yielding path is covered too.
func TestPgstoreContractWithRealRuntime(t *testing.T) {
	pgURL(t)
	storetest.Run(t, func(t *testing.T) store.Store {
		return openStore(t, pgstore.WithRuntime(xsync.NewReal(context.Background())))
	})
}

// countingRuntime counts Yield calls.
type countingRuntime struct {
	*xsync.Real
	yields atomic.Int64
}

func (r *countingRuntime) Yield() { r.yields.Add(1) }

func TestPgstoreYieldsOnEveryCall(t *testing.T) {
	rt := &countingRuntime{Real: xsync.NewReal(context.Background())}
	s := openStore(t, pgstore.WithRuntime(rt))
	ctx := context.Background()
	calls := []func() error{
		func() error { _, err := s.GetShard(ctx, 0); return err },
		func() error { _, err := s.GetLatestRun(ctx, 0, "none"); return ignore(err, store.ErrNotFound) },
		func() error { _, err := s.ReadTransferTasks(ctx, 0, 0, 10); return err },
	}
	for i, call := range calls {
		before := rt.yields.Load()
		if err := call(); err != nil {
			t.Fatalf("call %d: %v", i, err)
		}
		if rt.yields.Load() != before+1 {
			t.Fatalf("call %d: want one Yield, got %d", i, rt.yields.Load()-before)
		}
	}
}

// ignore returns nil if err is target.
func ignore(err, target error) error {
	if errors.Is(err, target) {
		return nil
	}
	return err
}
