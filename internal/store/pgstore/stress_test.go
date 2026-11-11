package pgstore_test

// WI-031 (hand-owned): fencing and DBVersion under real concurrency on Postgres.

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/niqbal/workflow-engine/internal/store"
	"github.com/niqbal/workflow-engine/internal/store/pgstore"
)

func openStress(t *testing.T) *pgstore.Store {
	t.Helper()
	url := pgURL(t)
	s, err := pgstore.Open(context.Background(), url, pgstore.WithSchema(freshSchema(t, url)), pgstore.WithMaxConns(16))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(s.Close)
	if err := s.Migrate(context.Background()); err != nil {
		t.Fatal(err)
	}
	return s
}

// Two owners fight over one shard while both keep writing. A write that started after a newer
// fence was granted must never be accepted (G6). Every attempt is counted.
func TestStressNoStaleFenceWrites(t *testing.T) {
	s := openStress(t)
	ctx := context.Background()
	t0 := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	const attempts = 10000
	f0, err := s.AcquireShard(ctx, 0, "a", t0, time.Hour)
	if err != nil {
		t.Fatal(err)
	}
	// One run per writer, so DBVersion conflicts do not mask fence results.
	keys := map[string]store.RunKey{}
	for _, w := range []string{"a", "b"} {
		k := store.RunKey{Shard: 0, WorkflowID: "wf-" + w, RunID: "r"}
		if _, err := s.CreateRun(ctx, f0, store.NewRun{Key: k, Events: []store.EventRecord{{ID: 1, Data: []byte(`{}`)}}, State: []byte("s")}); err != nil {
			t.Fatal(err)
		}
		keys[w] = k
	}

	var maxRange atomic.Int64 // highest range ID granted so far
	maxRange.Store(f0.RangeID)
	var mu sync.Mutex
	fences := map[string]store.Fence{"a": f0}
	var tried, accepted, rejected, violations atomic.Int64
	var wg sync.WaitGroup
	clock := atomic.Int64{} // virtual seconds, so every acquire sees an expired lease

	write := func(owner string, key store.RunKey) {
		defer wg.Done()
		next, ver := int64(2), int64(1)
		for tried.Load() < attempts {
			mu.Lock()
			f, ok := fences[owner]
			mu.Unlock()
			if !ok {
				continue
			}
			before := maxRange.Load() // a newer fence granted before this write began
			tried.Add(1)
			v, _, err := s.UpdateRun(ctx, f, key, store.Update{ExpectedDBVersion: ver,
				Events: []store.EventRecord{{ID: next, Data: []byte(`{}`)}}, State: []byte("s"), Open: true})
			switch {
			case err == nil:
				accepted.Add(1)
				ver, next = v, next+1
				if f.RangeID < before {
					violations.Add(1)
					t.Errorf("accepted a write with fence %d after fence %d was granted", f.RangeID, before)
				}
			case errors.Is(err, store.ErrFenced):
				rejected.Add(1)
			default:
				t.Errorf("unexpected error: %v", err)
				return
			}
		}
	}
	steal := func() {
		defer wg.Done()
		owners := []string{"b", "a"}
		for i := 0; tried.Load() < attempts; i++ {
			o := owners[i%2]
			now := t0.Add(time.Duration(clock.Add(7200)) * time.Second)
			f, err := s.AcquireShard(ctx, 0, o, now, time.Hour)
			if err != nil {
				t.Errorf("acquire: %v", err)
				return
			}
			maxRange.Store(f.RangeID)
			mu.Lock()
			fences[o] = f
			mu.Unlock()
			time.Sleep(time.Millisecond)
		}
	}
	wg.Add(3)
	go write("a", keys["a"])
	go write("b", keys["b"])
	go steal()
	wg.Wait()
	t.Logf("%d attempts: %d accepted, %d fenced, final range %d", tried.Load(), accepted.Load(), rejected.Load(), maxRange.Load())
	if violations.Load() != 0 {
		t.Fatalf("%d stale-fence writes accepted", violations.Load())
	}
	if accepted.Load() == 0 || rejected.Load() == 0 {
		t.Fatalf("the test did not exercise both outcomes (accepted %d, fenced %d)", accepted.Load(), rejected.Load())
	}
}

// Many writers race on one run with the same expected version: exactly one wins each round,
// and the run's version and event IDs account for every winner (no lost or doubled write).
func TestStressOneWinnerPerVersion(t *testing.T) {
	s := openStress(t)
	ctx := context.Background()
	f, err := s.AcquireShard(ctx, 0, "a", time.Now(), time.Hour)
	if err != nil {
		t.Fatal(err)
	}
	k := store.RunKey{Shard: 0, WorkflowID: "wf", RunID: "r"}
	if _, err := s.CreateRun(ctx, f, store.NewRun{Key: k, Events: []store.EventRecord{{ID: 1, Data: []byte(`{}`)}}, State: []byte("s")}); err != nil {
		t.Fatal(err)
	}
	const rounds, writers = 625, 16 // 10,000 attempts
	stale := 0
	for r := 0; r < rounds; r++ {
		snap, err := s.GetRun(ctx, k)
		if err != nil {
			t.Fatal(err)
		}
		var wins atomic.Int64
		var wg sync.WaitGroup
		for w := 0; w < writers; w++ {
			wg.Add(1)
			go func(w int) {
				defer wg.Done()
				_, _, err := s.UpdateRun(ctx, f, k, store.Update{ExpectedDBVersion: snap.DBVersion,
					Events: []store.EventRecord{{ID: snap.NextEventID, Data: []byte(fmt.Sprintf(`{"w":%d}`, w))}}, State: []byte("s"), Open: true})
				if err == nil {
					wins.Add(1)
				} else if !errors.Is(err, store.ErrConflict) {
					t.Errorf("round %d writer %d: %v", r, w, err)
				}
			}(w)
		}
		wg.Wait()
		if wins.Load() != 1 {
			stale++
			t.Errorf("round %d: %d winners", r, wins.Load())
		}
	}
	snap, _ := s.GetRun(ctx, k)
	evs, _ := s.ReadHistory(ctx, k, 1, 100000)
	if snap.DBVersion != rounds+1 || snap.NextEventID != rounds+2 || len(evs) != rounds+1 || stale != 0 {
		t.Fatalf("version %d next %d events %d (want %d, %d, %d)", snap.DBVersion, snap.NextEventID, len(evs), rounds+1, rounds+2, rounds+1)
	}
}
