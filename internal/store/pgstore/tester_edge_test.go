package pgstore_test

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/niqbal/workflow-engine/internal/store"
	"github.com/niqbal/workflow-engine/internal/store/pgstore"
)

var edgeNow = time.Date(2026, 11, 11, 12, 0, 0, 0, time.UTC)

func edgeAcquire(t *testing.T, s store.Store, shard int, owner string) store.Fence {
	t.Helper()
	f, err := s.AcquireShard(context.Background(), shard, owner, edgeNow, time.Minute)
	if err != nil {
		t.Fatalf("acquire shard %d: %v", shard, err)
	}
	return f
}

func edgeRun(shard int, wf string, tasks ...store.Task) store.NewRun {
	return store.NewRun{
		Key:    store.RunKey{Shard: shard, WorkflowID: wf, RunID: "r-" + wf},
		Events: []store.EventRecord{{ID: 1, Data: []byte("e1")}},
		State:  []byte("s1"),
		Tasks:  tasks,
	}
}

func transfer(shard int, wf string) store.Task {
	return store.Task{Shard: shard, Kind: store.TransferTask, Run: store.RunKey{Shard: shard, WorkflowID: wf, RunID: "r-" + wf}, Payload: []byte("p")}
}

func timer(shard int, wf string, at time.Time) store.Task {
	return store.Task{Shard: shard, Kind: store.TimerTask, FireAt: at, Run: store.RunKey{Shard: shard, WorkflowID: wf, RunID: "r-" + wf}, Payload: []byte("t")}
}

// Re-running Migrate on a schema that already holds data keeps the data and adds no rows to
// schema_migrations; tables land in the configured schema, not public.
func TestEdgeMigrateAgainKeepsData(t *testing.T) {
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
	f := edgeAcquire(t, s, 3, "a")
	if _, err := s.CreateRun(ctx, f, edgeRun(3, "wf", transfer(3, "wf"))); err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 3; i++ {
		if err := s.Migrate(ctx); err != nil {
			t.Fatalf("re-migrate %d: %v", i, err)
		}
	}
	snap, err := s.GetRun(ctx, store.RunKey{Shard: 3, WorkflowID: "wf", RunID: "r-wf"})
	if err != nil || snap.DBVersion != 1 || string(snap.State) != "s1" {
		t.Fatalf("run after re-migrate: %+v, %v", snap, err)
	}
	if ts, err := s.ReadTransferTasks(ctx, 3, 0, 10); err != nil || len(ts) != 1 {
		t.Fatalf("tasks after re-migrate: %v, %v", ts, err)
	}
	if err := s.RenewShard(ctx, f, edgeNow, time.Minute); err != nil {
		t.Fatalf("fence must survive re-migrate: %v", err)
	}

	conn, err := pgx.Connect(ctx, url)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close(ctx)
	var n int
	if err := conn.QueryRow(ctx, `SELECT count(*) FROM `+schema+`.schema_migrations`).Scan(&n); err != nil {
		t.Fatal(err)
	}
	var runs int
	if err := conn.QueryRow(ctx, `SELECT count(*) FROM `+schema+`.runs`).Scan(&runs); err != nil {
		t.Fatal(err)
	}
	if runs != 1 {
		t.Fatalf("run not stored in schema %s: %d rows", schema, runs)
	}
	var applied int
	if err := conn.QueryRow(ctx, `SELECT max(version) FROM `+schema+`.schema_migrations`).Scan(&applied); err != nil {
		t.Fatal(err)
	}
	if n != applied {
		t.Fatalf("schema_migrations has %d rows for max version %d", n, applied)
	}
}

// Task IDs grow from 1 per (shard, kind), independently of other shards and kinds, and are
// returned to the caller in input order.
func TestEdgeTaskIDsPerShardAndKind(t *testing.T) {
	s := openStore(t)
	ctx := context.Background()
	f0 := edgeAcquire(t, s, 0, "a")
	f1 := edgeAcquire(t, s, 1, "a")
	at := edgeNow.Add(time.Hour)
	w, err := s.CreateRun(ctx, f0, edgeRun(0, "a", transfer(0, "a"), timer(0, "a", at), transfer(0, "a")))
	if err != nil {
		t.Fatal(err)
	}
	if len(w) != 3 || w[0].ID != 1 || w[1].ID != 1 || w[2].ID != 2 || w[1].Kind != store.TimerTask {
		t.Fatalf("first write IDs: %+v", w)
	}
	w, err = s.CreateRun(ctx, f1, edgeRun(1, "b", transfer(1, "b")))
	if err != nil || len(w) != 1 || w[0].ID != 1 {
		t.Fatalf("shard 1 must have its own counter: %+v, %v", w, err)
	}
	w, err = s.CreateRun(ctx, f0, edgeRun(0, "c", timer(0, "c", at), transfer(0, "c")))
	if err != nil || w[0].ID != 2 || w[1].ID != 3 {
		t.Fatalf("second write on shard 0: %+v, %v", w, err)
	}
}

// Concurrent writes on one shard get distinct task IDs and none fail.
func TestEdgeConcurrentTaskIDsUnique(t *testing.T) {
	s := openStore(t, pgstore.WithMaxConns(8))
	ctx := context.Background()
	f := edgeAcquire(t, s, 0, "a")
	const n = 16
	var mu sync.Mutex
	var ids []int64
	var wg sync.WaitGroup
	errs := make([]error, n)
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			wf := fmt.Sprintf("w%d", i)
			w, err := s.CreateRun(ctx, f, edgeRun(0, wf, transfer(0, wf), transfer(0, wf)))
			errs[i] = err
			mu.Lock()
			for _, tk := range w {
				ids = append(ids, tk.ID)
			}
			mu.Unlock()
		}(i)
	}
	wg.Wait()
	for i, err := range errs {
		if err != nil {
			t.Fatalf("writer %d: %v", i, err)
		}
	}
	sort.Slice(ids, func(i, j int) bool { return ids[i] < ids[j] })
	for i, id := range ids {
		if id != int64(i+1) {
			t.Fatalf("IDs not unique and dense: %v", ids)
		}
	}
	ts, err := s.ReadTransferTasks(ctx, 0, 0, 100)
	if err != nil || len(ts) != 2*n {
		t.Fatalf("read %d tasks, %v", len(ts), err)
	}
}

// Fence edge cases: never-acquired shard, wrong owner, wrong shard, stale after takeover.
func TestEdgeFenceCases(t *testing.T) {
	s := openStore(t)
	ctx := context.Background()
	if _, err := s.CreateRun(ctx, store.Fence{Shard: 5, Owner: "a", RangeID: 1}, edgeRun(5, "x")); !errors.Is(err, store.ErrFenced) {
		t.Fatalf("never-acquired shard: want ErrFenced, got %v", err)
	}
	if _, err := s.GetShard(ctx, 5); err != nil {
		t.Fatal(err)
	}
	if _, err := s.CreateRun(ctx, store.Fence{Shard: 5, Owner: "", RangeID: 0}, edgeRun(5, "x")); !errors.Is(err, store.ErrFenced) {
		t.Fatalf("zero fence on unowned shard: want ErrFenced, got %v", err)
	}
	f := edgeAcquire(t, s, 0, "a")
	bad := f
	bad.Owner = "b"
	if _, err := s.CreateRun(ctx, bad, edgeRun(0, "x")); !errors.Is(err, store.ErrFenced) {
		t.Fatalf("wrong owner: want ErrFenced, got %v", err)
	}
	if _, err := s.CreateRun(ctx, f, edgeRun(1, "x")); err == nil {
		t.Fatal("fence for shard 0 accepted on shard 1")
	}
	w, err := s.CreateRun(ctx, f, edgeRun(0, "x", transfer(0, "x")))
	if err != nil {
		t.Fatal(err)
	}
	// Takeover after lease expiry.
	f2, err := s.AcquireShard(ctx, 0, "b", edgeNow.Add(2*time.Minute), time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	if err := s.DeleteTasks(ctx, f, []store.TaskKey{w[0].Key()}); !errors.Is(err, store.ErrFenced) {
		t.Fatalf("stale DeleteTasks: want ErrFenced, got %v", err)
	}
	if _, _, err := s.UpdateRun(ctx, f, edgeRun(0, "x").Key, store.Update{ExpectedDBVersion: 1, Open: true}); !errors.Is(err, store.ErrFenced) {
		t.Fatalf("stale UpdateRun: want ErrFenced, got %v", err)
	}
	if err := s.RenewShard(ctx, f, edgeNow, time.Minute); !errors.Is(err, store.ErrFenced) {
		t.Fatalf("stale RenewShard: want ErrFenced, got %v", err)
	}
	if ts, _ := s.ReadTransferTasks(ctx, 0, 0, 10); len(ts) != 1 {
		t.Fatalf("stale delete removed tasks: %v", ts)
	}
	if err := s.DeleteTasks(ctx, f2, []store.TaskKey{w[0].Key(), {Shard: 0, Kind: store.TransferTask, ID: 999}}); err != nil {
		t.Fatalf("current DeleteTasks with a missing key: %v", err)
	}
	if ts, _ := s.ReadTransferTasks(ctx, 0, 0, 10); len(ts) != 0 {
		t.Fatalf("tasks not deleted: %v", ts)
	}
	// Same owner reacquiring bumps the range and fences its own old fence.
	f3 := edgeAcquire(t, s, 0, "b")
	if f3.RangeID != f2.RangeID+1 {
		t.Fatalf("range not bumped: %d -> %d", f2.RangeID, f3.RangeID)
	}
	if err := s.RenewShard(ctx, f2, edgeNow, time.Minute); !errors.Is(err, store.ErrFenced) {
		t.Fatalf("old fence of same owner: want ErrFenced, got %v", err)
	}
}

// A rejected UpdateRun leaves nothing behind: no events, no tasks, done tasks kept.
func TestEdgeRejectedUpdateIsAtomic(t *testing.T) {
	s := openStore(t)
	ctx := context.Background()
	f := edgeAcquire(t, s, 0, "a")
	r := edgeRun(0, "x", transfer(0, "x"))
	w, err := s.CreateRun(ctx, f, r)
	if err != nil {
		t.Fatal(err)
	}
	done := []store.TaskKey{w[0].Key()}
	gap := store.Update{ExpectedDBVersion: 1, Events: []store.EventRecord{{ID: 2}, {ID: 4}}, NewTasks: []store.Task{transfer(0, "x")}, DoneTasks: done, Open: true}
	other := store.Update{ExpectedDBVersion: 1, Events: []store.EventRecord{{ID: 2}}, NewTasks: []store.Task{transfer(1, "x")}, DoneTasks: done, Open: true}
	stale := store.Update{ExpectedDBVersion: 7, Events: []store.EventRecord{{ID: 2}}, NewTasks: []store.Task{transfer(0, "x")}, DoneTasks: done, Open: true}
	names := []string{"gap in event IDs", "task on other shard", "stale version"}
	for i, u := range []store.Update{gap, other, stale} {
		name := names[i]
		if _, _, err := s.UpdateRun(ctx, f, r.Key, u); err == nil {
			t.Fatalf("%s: accepted", name)
		}
		snap, err := s.GetRun(ctx, r.Key)
		if err != nil || snap.DBVersion != 1 || snap.NextEventID != 2 || string(snap.State) != "s1" {
			t.Fatalf("%s: run changed: %+v %v", name, snap, err)
		}
		if h, _ := s.ReadHistory(ctx, r.Key, 1, 10); len(h) != 1 {
			t.Fatalf("%s: history changed: %v", name, h)
		}
		if ts, _ := s.ReadTransferTasks(ctx, 0, 0, 10); len(ts) != 1 || ts[0].ID != w[0].ID {
			t.Fatalf("%s: tasks changed: %v", name, ts)
		}
	}
	// The counter row did not advance for the rejected writes.
	_, nw, err := s.UpdateRun(ctx, f, r.Key, store.Update{ExpectedDBVersion: 1, Events: []store.EventRecord{{ID: 2}}, NewTasks: []store.Task{transfer(0, "x")}, Open: true})
	if err != nil || len(nw) != 1 || nw[0].ID != 2 {
		t.Fatalf("valid update after rejects: %+v, %v", nw, err)
	}
}

// Many concurrent updates at one version: exactly one wins, the rest get ErrConflict (not a
// raw database error). Many concurrent creates of one workflow ID: one wins, the rest get
// ErrAlreadyExists.
func TestEdgeConcurrentCASAndCreate(t *testing.T) {
	s := openStore(t, pgstore.WithMaxConns(8))
	ctx := context.Background()
	f := edgeAcquire(t, s, 0, "a")
	const n = 12

	errs := make([]error, n)
	var wg sync.WaitGroup
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			r := edgeRun(0, "same", transfer(0, "same"))
			r.Key.RunID = fmt.Sprintf("run-%d", i)
			_, errs[i] = s.CreateRun(ctx, f, r)
		}(i)
	}
	wg.Wait()
	wins := 0
	for i, err := range errs {
		switch {
		case err == nil:
			wins++
		case !errors.Is(err, store.ErrAlreadyExists):
			t.Fatalf("create %d: want ErrAlreadyExists, got %v", i, err)
		}
	}
	if wins != 1 {
		t.Fatalf("want one create to win, got %d", wins)
	}
	key, err := s.GetLatestRun(ctx, 0, "same")
	if err != nil {
		t.Fatal(err)
	}
	if ts, _ := s.ReadTransferTasks(ctx, 0, 0, 100); len(ts) != 1 {
		t.Fatalf("losing creates left tasks: %d", len(ts))
	}

	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			_, _, errs[i] = s.UpdateRun(ctx, f, key, store.Update{
				ExpectedDBVersion: 1,
				Events:            []store.EventRecord{{ID: 2, Data: []byte(fmt.Sprint(i))}},
				State:             []byte(fmt.Sprint(i)),
				Open:              true,
			})
		}(i)
	}
	wg.Wait()
	wins = 0
	winner := -1
	for i, err := range errs {
		switch {
		case err == nil:
			wins++
			winner = i
		case !errors.Is(err, store.ErrConflict):
			t.Fatalf("update %d: want ErrConflict, got %v", i, err)
		}
	}
	if wins != 1 {
		t.Fatalf("want one update to win, got %d", wins)
	}
	snap, _ := s.GetRun(ctx, key)
	h, _ := s.ReadHistory(ctx, key, 1, 10)
	if snap.DBVersion != 2 || snap.NextEventID != 3 || string(snap.State) != fmt.Sprint(winner) || len(h) != 2 || string(h[1].Data) != fmt.Sprint(winner) {
		t.Fatalf("state after race: %+v history %v winner %d", snap, h, winner)
	}
}

// A takeover racing with writers: every write that succeeded committed before the takeover
// (its effects are visible), and writes with the old fence after it fail with ErrFenced.
func TestEdgeTakeoverRacesWrites(t *testing.T) {
	s := openStore(t, pgstore.WithMaxConns(8))
	ctx := context.Background()
	f := edgeAcquire(t, s, 0, "a")
	const n = 10
	errs := make([]error, n)
	var wg sync.WaitGroup
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			wf := fmt.Sprintf("w%d", i)
			_, errs[i] = s.CreateRun(ctx, f, edgeRun(0, wf, transfer(0, wf)))
		}(i)
	}
	f2, err := s.AcquireShard(ctx, 0, "b", edgeNow.Add(time.Hour), time.Minute)
	wg.Wait()
	if err != nil {
		t.Fatal(err)
	}
	ok := 0
	for i, e := range errs {
		switch {
		case e == nil:
			ok++
			if _, err := s.GetLatestRun(ctx, 0, fmt.Sprintf("w%d", i)); err != nil {
				t.Fatalf("successful write %d not visible: %v", i, err)
			}
		case errors.Is(e, store.ErrFenced):
			if _, err := s.GetLatestRun(ctx, 0, fmt.Sprintf("w%d", i)); !errors.Is(err, store.ErrNotFound) {
				t.Fatalf("fenced write %d left a run: %v", i, err)
			}
		default:
			t.Fatalf("write %d: %v", i, e)
		}
	}
	if ts, _ := s.ReadTransferTasks(ctx, 0, 0, 100); len(ts) != ok {
		t.Fatalf("%d tasks for %d successful writes", len(ts), ok)
	}
	if _, err := s.CreateRun(ctx, f, edgeRun(0, "late")); !errors.Is(err, store.ErrFenced) {
		t.Fatalf("write after takeover: want ErrFenced, got %v", err)
	}
	if _, err := s.CreateRun(ctx, f2, edgeRun(0, "late")); err != nil {
		t.Fatalf("new owner write: %v", err)
	}
}

// Timer FireAt in a non-UTC zone round-trips as the same instant and is compared against
// upTo as an instant; the lease time does too.
func TestEdgeTimesAcrossZones(t *testing.T) {
	s := openStore(t)
	ctx := context.Background()
	zone := time.FixedZone("X", 5*3600+30*60)
	f := edgeAcquire(t, s, 0, "a")
	at := time.Date(2026, 11, 11, 18, 0, 0, 123000, zone) // 12:30:00.000123 UTC
	if _, err := s.CreateRun(ctx, f, edgeRun(0, "x", timer(0, "x", at))); err != nil {
		t.Fatal(err)
	}
	if ts, err := s.ReadTimerTasks(ctx, 0, at.UTC().Add(-time.Microsecond), 10); err != nil || len(ts) != 0 {
		t.Fatalf("timer read before due: %v %v", ts, err)
	}
	ts, err := s.ReadTimerTasks(ctx, 0, at.UTC(), 10)
	if err != nil || len(ts) != 1 || !ts[0].FireAt.Equal(at) {
		t.Fatalf("timer at due time: %+v %v", ts, err)
	}
	info, err := s.GetShard(ctx, 0)
	if err != nil || !info.LeaseExpiresAt.Equal(edgeNow.Add(time.Minute)) {
		t.Fatalf("lease: %+v %v", info, err)
	}
	// Lease expiry compared as an instant: just before expiry in another zone is still owned.
	if _, err := s.AcquireShard(ctx, 0, "b", edgeNow.Add(time.Minute-time.Microsecond).In(zone), time.Minute); !errors.Is(err, store.ErrFenced) {
		t.Fatalf("takeover before expiry: want ErrFenced, got %v", err)
	}
	if _, err := s.AcquireShard(ctx, 0, "b", edgeNow.Add(time.Minute).In(zone), time.Minute); err != nil {
		t.Fatalf("takeover at expiry: %v", err)
	}
}

// Reads of unknown things and degenerate limits; a closed store errors instead of hanging.
func TestEdgeReadsAndClose(t *testing.T) {
	url := pgURL(t)
	schema := freshSchema(t, url)
	ctx := context.Background()
	s, err := pgstore.Open(ctx, url, pgstore.WithSchema(schema))
	if err != nil {
		t.Fatal(err)
	}
	if err := s.Migrate(ctx); err != nil {
		t.Fatal(err)
	}
	k := store.RunKey{Shard: 0, WorkflowID: "nope", RunID: "r"}
	if _, err := s.ReadHistory(ctx, k, 1, 10); !errors.Is(err, store.ErrNotFound) {
		t.Fatalf("ReadHistory unknown run: %v", err)
	}
	if _, err := s.GetRun(ctx, k); !errors.Is(err, store.ErrNotFound) {
		t.Fatalf("GetRun unknown run: %v", err)
	}
	f := edgeAcquire(t, s, 0, "a")
	r := edgeRun(0, "x")
	r.Events = append(r.Events, store.EventRecord{ID: 2, Data: nil}, store.EventRecord{ID: 3, Data: []byte{}})
	r.State = nil
	if _, err := s.CreateRun(ctx, f, r); err != nil {
		t.Fatal(err)
	}
	if h, err := s.ReadHistory(ctx, r.Key, 1, 0); err != nil || len(h) != 0 {
		t.Fatalf("limit 0: %v %v", h, err)
	}
	if h, err := s.ReadHistory(ctx, r.Key, 2, 1); err != nil || len(h) != 1 || h[0].ID != 2 {
		t.Fatalf("page: %v %v", h, err)
	}
	if h, err := s.ReadHistory(ctx, r.Key, 4, 10); err != nil || len(h) != 0 {
		t.Fatalf("past end: %v %v", h, err)
	}
	if snap, err := s.GetRun(ctx, r.Key); err != nil || snap.NextEventID != 4 || len(snap.State) != 0 {
		t.Fatalf("snapshot: %+v %v", snap, err)
	}
	if ts, err := s.ReadTransferTasks(ctx, 0, 0, -1); err != nil || len(ts) != 0 {
		t.Fatalf("negative limit: %v %v", ts, err)
	}
	s.Close()
	// A hang here is caught by the go test timeout.
	if _, err := s.GetShard(ctx, 0); err == nil {
		t.Fatal("call on closed store succeeded")
	}
}
