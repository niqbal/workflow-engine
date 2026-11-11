// Package storetest is the contract every Store implementation must pass (WI-004).
// It is owned by hand: implementations may not change it.
//
//	func TestMemstore(t *testing.T) { storetest.Run(t, func(t *testing.T) store.Store { return memstore.New() }) }
package storetest

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/niqbal/workflow-engine/internal/store"
)

// Factory returns a fresh, empty store for one subtest.
type Factory func(t *testing.T) store.Store

var t0 = time.Date(2026, 11, 11, 12, 0, 0, 0, time.UTC)

const lease = 10 * time.Second

// Run executes every contract case as a subtest.
func Run(t *testing.T, newStore Factory) {
	cases := []struct {
		name string
		fn   func(*testing.T, store.Store)
	}{
		{"ShardAcquireRenewAndFence", testShardAcquire},
		{"ShardLeaseExpiryAllowsTakeover", testShardTakeover},
		{"CreateAndGetRun", testCreateGet},
		{"CreateRunRejectsDuplicateAndBadEvents", testCreateInvalid},
		{"LatestRunIncludesClosed", testLatestRun},
		{"UpdateAppendsAndBumpsVersion", testUpdate},
		{"StateOnlyUpdateBumpsVersion", testStateOnly},
		{"StaleVersionConflicts", testConflict},
		{"EventIDsMustContinue", testEventContinuity},
		{"WritesWithStaleFenceRejected", testFencedWrites},
		{"ReadHistoryPaging", testReadHistory},
		{"TransferTasksFromAckLevel", testTransferTasks},
		{"TimerTasksOrdered", testTimerTasks},
		{"DoneTasksDeletedAtomically", testDoneTasks},
		{"ConcurrentWritersOneWinsPerVersion", testConcurrentWriters},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) { c.fn(t, newStore(t)) })
	}
}

var ctx = context.Background()

// must(call())(t) fails the test on error and returns the value.
func must[T any](v T, err error) func(t *testing.T) T {
	return func(t *testing.T) T {
		t.Helper()
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		return v
	}
}

func wantErr(t *testing.T, err, target error) {
	t.Helper()
	if !errors.Is(err, target) {
		t.Fatalf("want %v, got %v", target, err)
	}
}

func events(from, n int64) []store.EventRecord {
	out := make([]store.EventRecord, n)
	for i := range out {
		id := from + int64(i)
		out[i] = store.EventRecord{ID: id, Data: []byte(fmt.Sprintf(`{"e":%d}`, id))}
	}
	return out
}

func acquire(t *testing.T, s store.Store, shard int, owner string) store.Fence {
	t.Helper()
	return must(s.AcquireShard(ctx, shard, owner, t0, lease))(t)
}

func newRun(t *testing.T, s store.Store, f store.Fence, wf, run string, tasks ...store.Task) store.RunKey {
	t.Helper()
	k := store.RunKey{Shard: f.Shard, WorkflowID: wf, RunID: run}
	must(s.CreateRun(ctx, f, store.NewRun{Key: k, Events: events(1, 2), State: []byte("s1"), Tasks: tasks}))(t)
	return k
}

func testShardAcquire(t *testing.T, s store.Store) {
	info := must(s.GetShard(ctx, 3))(t)
	if info.RangeID != 0 || info.Owner != "" {
		t.Fatalf("fresh shard should be unowned, got %+v", info)
	}
	f1 := acquire(t, s, 3, "a")
	if f1.RangeID != 1 || f1.Owner != "a" || f1.Shard != 3 {
		t.Fatalf("bad fence %+v", f1)
	}
	// Re-acquire by the same owner bumps the range.
	f2 := acquire(t, s, 3, "a")
	if f2.RangeID != 2 {
		t.Fatalf("want range 2, got %d", f2.RangeID)
	}
	wantErr(t, s.RenewShard(ctx, f1, t0, lease), store.ErrFenced)
	if err := s.RenewShard(ctx, f2, t0.Add(5*time.Second), lease); err != nil {
		t.Fatal(err)
	}
	// Another owner cannot take a live lease.
	_, err := s.AcquireShard(ctx, 3, "b", t0.Add(14*time.Second), lease)
	wantErr(t, err, store.ErrFenced)
	info = must(s.GetShard(ctx, 3))(t)
	if !info.LeaseExpiresAt.Equal(t0.Add(15*time.Second)) || info.Owner != "a" {
		t.Fatalf("renew did not extend lease: %+v", info)
	}
}

func testShardTakeover(t *testing.T, s store.Store) {
	fa := acquire(t, s, 1, "a")
	fb := must(s.AcquireShard(ctx, 1, "b", t0.Add(lease+time.Millisecond), lease))(t)
	if fb.RangeID != fa.RangeID+1 || fb.Owner != "b" {
		t.Fatalf("takeover fence %+v after %+v", fb, fa)
	}
	wantErr(t, s.RenewShard(ctx, fa, t0.Add(lease+2*time.Millisecond), lease), store.ErrFenced)
}

func testCreateGet(t *testing.T, s store.Store) {
	f := acquire(t, s, 0, "a")
	k := newRun(t, s, f, "wf", "r1")
	snap := must(s.GetRun(ctx, k))(t)
	if snap.DBVersion != 1 || snap.NextEventID != 3 || !snap.Open || string(snap.State) != "s1" || snap.Key != k {
		t.Fatalf("bad snapshot %+v", snap)
	}
	_, err := s.GetRun(ctx, store.RunKey{Shard: 0, WorkflowID: "wf", RunID: "nope"})
	wantErr(t, err, store.ErrNotFound)
}

func testCreateInvalid(t *testing.T, s store.Store) {
	f := acquire(t, s, 0, "a")
	k := newRun(t, s, f, "wf", "r1")
	_, err := s.CreateRun(ctx, f, store.NewRun{Key: k, Events: events(1, 1), State: []byte("x")})
	wantErr(t, err, store.ErrAlreadyExists)
	k2 := store.RunKey{Shard: 0, WorkflowID: "wf2", RunID: "r"}
	_, err = s.CreateRun(ctx, f, store.NewRun{Key: k2, Events: events(2, 1), State: []byte("x")})
	wantErr(t, err, store.ErrInvalid)
	_, err = s.CreateRun(ctx, f, store.NewRun{Key: k2, Events: []store.EventRecord{{ID: 1}, {ID: 3}}, State: []byte("x")})
	wantErr(t, err, store.ErrInvalid)
}

func testLatestRun(t *testing.T, s store.Store) {
	f := acquire(t, s, 0, "a")
	_, err := s.GetLatestRun(ctx, 0, "wf")
	wantErr(t, err, store.ErrNotFound)
	k1 := newRun(t, s, f, "wf", "r1")
	if _, _, err := s.UpdateRun(ctx, f, k1, store.Update{ExpectedDBVersion: 1, Events: events(3, 1), State: []byte("closed"), Open: false}); err != nil {
		t.Fatal(err)
	}
	if got := must(s.GetLatestRun(ctx, 0, "wf"))(t); got != k1 {
		t.Fatalf("closed run must still be latest, got %+v", got)
	}
	k2 := newRun(t, s, f, "wf", "r2")
	if got := must(s.GetLatestRun(ctx, 0, "wf"))(t); got != k2 {
		t.Fatalf("want newest run %+v, got %+v", k2, got)
	}
}

func testUpdate(t *testing.T, s store.Store) {
	f := acquire(t, s, 0, "a")
	k := newRun(t, s, f, "wf", "r1")
	v, tasks, err := s.UpdateRun(ctx, f, k, store.Update{ExpectedDBVersion: 1, Events: events(3, 2), State: []byte("s2"), Open: true,
		NewTasks: []store.Task{{Shard: 0, Kind: store.TransferTask, Run: k, Payload: []byte("p")}}})
	if err != nil || v != 2 || len(tasks) != 1 || tasks[0].ID == 0 {
		t.Fatalf("update: v=%d tasks=%+v err=%v", v, tasks, err)
	}
	snap := must(s.GetRun(ctx, k))(t)
	if snap.DBVersion != 2 || snap.NextEventID != 5 || string(snap.State) != "s2" {
		t.Fatalf("bad snapshot %+v", snap)
	}
}

func testStateOnly(t *testing.T, s store.Store) {
	f := acquire(t, s, 0, "a")
	k := newRun(t, s, f, "wf", "r1")
	v := must(func() (int64, error) {
		v, _, err := s.UpdateRun(ctx, f, k, store.Update{ExpectedDBVersion: 1, State: []byte("retry-2"), Open: true})
		return v, err
	}())(t)
	if v != 2 {
		t.Fatalf("state-only write must bump DBVersion, got %d", v)
	}
	// A stale state-only write (the C2 race) must lose.
	_, _, err := s.UpdateRun(ctx, f, k, store.Update{ExpectedDBVersion: 1, State: []byte("stale"), Open: true})
	wantErr(t, err, store.ErrConflict)
	if snap := must(s.GetRun(ctx, k))(t); string(snap.State) != "retry-2" || snap.NextEventID != 3 {
		t.Fatalf("stale write leaked: %+v", snap)
	}
}

func testConflict(t *testing.T, s store.Store) {
	f := acquire(t, s, 0, "a")
	k := newRun(t, s, f, "wf", "r1")
	_, _, err := s.UpdateRun(ctx, f, k, store.Update{ExpectedDBVersion: 7, Events: events(3, 1), State: []byte("x"), Open: true})
	wantErr(t, err, store.ErrConflict)
	_, _, err = s.UpdateRun(ctx, f, store.RunKey{Shard: 0, WorkflowID: "none", RunID: "r"}, store.Update{ExpectedDBVersion: 1})
	wantErr(t, err, store.ErrNotFound)
}

func testEventContinuity(t *testing.T, s store.Store) {
	f := acquire(t, s, 0, "a")
	k := newRun(t, s, f, "wf", "r1")
	_, _, err := s.UpdateRun(ctx, f, k, store.Update{ExpectedDBVersion: 1, Events: events(4, 1), State: []byte("x"), Open: true})
	wantErr(t, err, store.ErrInvalid)
	_, _, err = s.UpdateRun(ctx, f, k, store.Update{ExpectedDBVersion: 1, Events: []store.EventRecord{{ID: 3}, {ID: 5}}, State: []byte("x"), Open: true})
	wantErr(t, err, store.ErrInvalid)
	if snap := must(s.GetRun(ctx, k))(t); snap.DBVersion != 1 {
		t.Fatalf("rejected write changed the run: %+v", snap)
	}
}

func testFencedWrites(t *testing.T, s store.Store) {
	old := acquire(t, s, 0, "a")
	k := newRun(t, s, old, "wf", "r1")
	cur := must(s.AcquireShard(ctx, 0, "b", t0.Add(lease+time.Second), lease))(t)
	_, _, err := s.UpdateRun(ctx, old, k, store.Update{ExpectedDBVersion: 1, Events: events(3, 1), State: []byte("x"), Open: true})
	wantErr(t, err, store.ErrFenced)
	_, err = s.CreateRun(ctx, old, store.NewRun{Key: store.RunKey{Shard: 0, WorkflowID: "w2", RunID: "r"}, Events: events(1, 1), State: []byte("x")})
	wantErr(t, err, store.ErrFenced)
	wantErr(t, s.DeleteTasks(ctx, old, nil), store.ErrFenced)
	// A fence for a different shard never authorizes writes here.
	other := acquire(t, s, 1, "b")
	_, _, err = s.UpdateRun(ctx, other, k, store.Update{ExpectedDBVersion: 1, Events: events(3, 1), State: []byte("x"), Open: true})
	if err == nil {
		t.Fatal("write with another shard's fence succeeded")
	}
	must(func() (int64, error) {
		v, _, err := s.UpdateRun(ctx, cur, k, store.Update{ExpectedDBVersion: 1, Events: events(3, 1), State: []byte("x"), Open: true})
		return v, err
	}())(t)
}

func testReadHistory(t *testing.T, s store.Store) {
	f := acquire(t, s, 0, "a")
	k := newRun(t, s, f, "wf", "r1")
	must(func() (int64, error) {
		v, _, err := s.UpdateRun(ctx, f, k, store.Update{ExpectedDBVersion: 1, Events: events(3, 8), State: []byte("x"), Open: true})
		return v, err
	}())(t)
	all := must(s.ReadHistory(ctx, k, 1, 100))(t)
	if len(all) != 10 || all[0].ID != 1 || all[9].ID != 10 || string(all[4].Data) != `{"e":5}` {
		t.Fatalf("bad history %+v", all)
	}
	page := must(s.ReadHistory(ctx, k, 4, 3))(t)
	if len(page) != 3 || page[0].ID != 4 || page[2].ID != 6 {
		t.Fatalf("bad page %+v", page)
	}
}

func testTransferTasks(t *testing.T, s store.Store) {
	f := acquire(t, s, 2, "a")
	k := newRun(t, s, f, "wf", "r1", store.Task{Shard: 2, Kind: store.TransferTask, Payload: []byte("t0")})
	var ids []int64
	ver := int64(1)
	next := int64(3)
	for i := 1; i <= 3; i++ {
		v, tasks, err := s.UpdateRun(ctx, f, k, store.Update{ExpectedDBVersion: ver, Events: events(next, 1), State: []byte("x"), Open: true,
			NewTasks: []store.Task{{Shard: 2, Kind: store.TransferTask, Run: k, Payload: []byte(fmt.Sprintf("t%d", i))}}})
		if err != nil {
			t.Fatal(err)
		}
		ver, next = v, next+1
		ids = append(ids, tasks[0].ID)
	}
	all := must(s.ReadTransferTasks(ctx, 2, 0, 100))(t)
	if len(all) != 4 {
		t.Fatalf("want 4 tasks, got %d", len(all))
	}
	for i := 1; i < len(all); i++ {
		if all[i].ID <= all[i-1].ID {
			t.Fatalf("tasks not ascending: %+v", all)
		}
	}
	// Delete a task in the middle: reading from the ack level (lowest undeleted) still
	// returns every remaining task, including those below a later cursor (SPEC §8.1).
	if err := s.DeleteTasks(ctx, f, []store.TaskKey{{Shard: 2, Kind: store.TransferTask, ID: ids[1]}}); err != nil {
		t.Fatal(err)
	}
	rest := must(s.ReadTransferTasks(ctx, 2, all[0].ID, 100))(t)
	if len(rest) != 3 || rest[1].ID != ids[0] || rest[2].ID != ids[2] {
		t.Fatalf("bad read after delete: %+v", rest)
	}
	if got := must(s.ReadTransferTasks(ctx, 2, ids[2], 100))(t); len(got) != 1 || got[0].ID != ids[2] {
		t.Fatalf("fromID must be inclusive: %+v", got)
	}
	if got := must(s.ReadTransferTasks(ctx, 2, 0, 2))(t); len(got) != 2 {
		t.Fatalf("limit ignored: %d", len(got))
	}
	if got := must(s.ReadTransferTasks(ctx, 3, 0, 100))(t); len(got) != 0 {
		t.Fatalf("tasks leaked across shards: %+v", got)
	}
	if got := must(s.ReadTimerTasks(ctx, 2, t0.Add(time.Hour), 100))(t); len(got) != 0 {
		t.Fatalf("transfer tasks leaked into timer queue: %+v", got)
	}
}

func testTimerTasks(t *testing.T, s store.Store) {
	f := acquire(t, s, 0, "a")
	at := func(sec int, p string) store.Task {
		return store.Task{Shard: 0, Kind: store.TimerTask, FireAt: t0.Add(time.Duration(sec) * time.Second), Payload: []byte(p)}
	}
	newRun(t, s, f, "wf", "r1", at(30, "c"), at(10, "a"), at(10, "b"), at(20, "x"))
	due := must(s.ReadTimerTasks(ctx, 0, t0.Add(20*time.Second), 100))(t)
	if len(due) != 3 || string(due[0].Payload) != "a" || string(due[1].Payload) != "b" || string(due[2].Payload) != "x" {
		t.Fatalf("want a,b,x by (FireAt, ID), got %+v", due)
	}
	if got := must(s.ReadTimerTasks(ctx, 0, t0, 100))(t); len(got) != 0 {
		t.Fatalf("nothing is due at t0, got %+v", got)
	}
}

func testDoneTasks(t *testing.T, s store.Store) {
	f := acquire(t, s, 0, "a")
	created := must(s.CreateRun(ctx, f, store.NewRun{
		Key: store.RunKey{Shard: 0, WorkflowID: "wf", RunID: "r"}, Events: events(1, 1), State: []byte("x"),
		Tasks: []store.Task{{Shard: 0, Kind: store.TimerTask, FireAt: t0, Payload: []byte("old-timeout")}}}))(t)
	if len(created) != 1 || created[0].ID == 0 {
		t.Fatalf("CreateRun must return its tasks with IDs assigned, got %+v", created)
	}
	k := store.RunKey{Shard: 0, WorkflowID: "wf", RunID: "r"}
	_, _, err := s.UpdateRun(ctx, f, k, store.Update{ExpectedDBVersion: 1, Events: events(2, 1), State: []byte("y"), Open: true,
		DoneTasks: []store.TaskKey{created[0].Key(), {Shard: 0, Kind: store.TimerTask, ID: 999999}},
		NewTasks:  []store.Task{{Shard: 0, Kind: store.TimerTask, FireAt: t0, Payload: []byte("new-timeout")}}})
	if err != nil {
		t.Fatal(err)
	}
	due := must(s.ReadTimerTasks(ctx, 0, t0, 100))(t)
	if len(due) != 1 || string(due[0].Payload) != "new-timeout" {
		t.Fatalf("done task not deleted in the same write: %+v", due)
	}
	// A failed write must not delete its DoneTasks.
	_, _, err = s.UpdateRun(ctx, f, k, store.Update{ExpectedDBVersion: 1, DoneTasks: []store.TaskKey{due[0].Key()}})
	wantErr(t, err, store.ErrConflict)
	if got := must(s.ReadTimerTasks(ctx, 0, t0, 100))(t); len(got) != 1 {
		t.Fatalf("rejected write deleted tasks: %+v", got)
	}
}

func testConcurrentWriters(t *testing.T, s store.Store) {
	f := acquire(t, s, 0, "a")
	k := newRun(t, s, f, "wf", "r1")
	const writers = 16
	var wg sync.WaitGroup
	wins := make(chan int, writers)
	for i := 0; i < writers; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			_, _, err := s.UpdateRun(ctx, f, k, store.Update{ExpectedDBVersion: 1, Events: events(3, 1),
				State: []byte(fmt.Sprintf("w%d", i)), Open: true})
			if err == nil {
				wins <- i
			} else if !errors.Is(err, store.ErrConflict) {
				t.Errorf("writer %d: %v", i, err)
			}
		}(i)
	}
	wg.Wait()
	close(wins)
	var winners []int
	for w := range wins {
		winners = append(winners, w)
	}
	if len(winners) != 1 {
		t.Fatalf("exactly one writer per version must win, got %v", winners)
	}
	snap := must(s.GetRun(ctx, k))(t)
	if snap.DBVersion != 2 || string(snap.State) != fmt.Sprintf("w%d", winners[0]) {
		t.Fatalf("state does not match the winner: %+v", snap)
	}
}
