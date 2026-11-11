package memstore_test

import (
	"context"
	"errors"
	"slices"
	"testing"
	"time"

	"github.com/niqbal/workflow-engine/internal/simrt"
	"github.com/niqbal/workflow-engine/internal/store"
	"github.com/niqbal/workflow-engine/internal/store/memstore"
	"github.com/niqbal/workflow-engine/internal/xsync"
)

var (
	ctx         = context.Background()
	errInjected = errors.New("injected")
	t0          = simrt.Start
)

func ev(id int64) []store.EventRecord {
	return []store.EventRecord{{ID: id, Data: []byte("e")}}
}

func transfer(k store.RunKey, p string) []store.Task {
	return []store.Task{{Shard: k.Shard, Kind: store.TransferTask, Run: k, Payload: []byte(p)}}
}

// setup acquires shard 0 and creates the given runs, each with events 1..2.
func setup(t *testing.T, s store.Store, runs ...string) (store.Fence, []store.RunKey) {
	t.Helper()
	f, err := s.AcquireShard(ctx, 0, "n1", t0, time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	var keys []store.RunKey
	for _, r := range runs {
		k := store.RunKey{Shard: 0, WorkflowID: "wf-" + r, RunID: r}
		if _, err := s.CreateRun(ctx, f, store.NewRun{Key: k, Events: append(ev(1), ev(2)...), State: []byte("s1")}); err != nil {
			t.Fatal(err)
		}
		keys = append(keys, k)
	}
	return f, keys
}

func payloads(t *testing.T, s store.Store) []string {
	t.Helper()
	tasks, err := s.ReadTransferTasks(ctx, 0, 0, 100)
	if err != nil {
		t.Fatal(err)
	}
	var out []string
	for _, tk := range tasks {
		out = append(out, string(tk.Payload))
	}
	return out
}

func TestBeforeCommitAbortsWithNothingCommitted(t *testing.T) {
	fail := ""
	s := memstore.New(memstore.WithFaults(memstore.Faults{BeforeCommit: func(op string) error {
		if op == fail {
			return errInjected
		}
		return nil
	}}))
	f, keys := setup(t, s, "r1")
	k := keys[0]

	fail = "UpdateRun"
	_, _, err := s.UpdateRun(ctx, f, k, store.Update{ExpectedDBVersion: 1, Events: ev(3), State: []byte("s2"), Open: true,
		NewTasks: transfer(k, "t")})
	if !errors.Is(err, errInjected) {
		t.Fatalf("want injected error, got %v", err)
	}
	snap, err := s.GetRun(ctx, k)
	if err != nil || snap.DBVersion != 1 || snap.NextEventID != 3 || string(snap.State) != "s1" {
		t.Fatalf("aborted update leaked: %+v %v", snap, err)
	}
	if p := payloads(t, s); len(p) != 0 {
		t.Fatalf("aborted update wrote tasks: %v", p)
	}

	fail = "CreateRun"
	k2 := store.RunKey{Shard: 0, WorkflowID: "wf2", RunID: "r"}
	if _, err := s.CreateRun(ctx, f, store.NewRun{Key: k2, Events: ev(1), Tasks: transfer(k2, "c")}); !errors.Is(err, errInjected) {
		t.Fatalf("want injected error, got %v", err)
	}
	if _, err := s.GetRun(ctx, k2); !errors.Is(err, store.ErrNotFound) {
		t.Fatalf("aborted create leaked: %v", err)
	}
	if _, err := s.GetLatestRun(ctx, 0, "wf2"); !errors.Is(err, store.ErrNotFound) {
		t.Fatalf("aborted create leaked into latest: %v", err)
	}

	fail = "AcquireShard"
	if _, err := s.AcquireShard(ctx, 0, "n1", t0, time.Minute); !errors.Is(err, errInjected) {
		t.Fatalf("want injected error, got %v", err)
	}
	if info, _ := s.GetShard(ctx, 0); info.RangeID != f.RangeID {
		t.Fatalf("aborted acquire bumped the range: %+v", info)
	}

	// The original write still succeeds once the fault clears.
	fail = ""
	if v, _, err := s.UpdateRun(ctx, f, k, store.Update{ExpectedDBVersion: 1, Events: ev(3), State: []byte("s2"), Open: true}); err != nil || v != 2 {
		t.Fatalf("retry: v=%d err=%v", v, err)
	}
}

func TestAfterCommitIsAnAmbiguousCommit(t *testing.T) {
	fail := ""
	s := memstore.New(memstore.WithFaults(memstore.Faults{AfterCommit: func(op string) error {
		if op == fail {
			return errInjected
		}
		return nil
	}}))
	f, keys := setup(t, s, "r1")
	k := keys[0]

	fail = "UpdateRun"
	_, _, err := s.UpdateRun(ctx, f, k, store.Update{ExpectedDBVersion: 1, Events: ev(3), State: []byte("s2"), Open: true,
		NewTasks: transfer(k, "t")})
	if !errors.Is(err, errInjected) {
		t.Fatalf("want injected error, got %v", err)
	}
	snap, err := s.GetRun(ctx, k)
	if err != nil || snap.DBVersion != 2 || snap.NextEventID != 4 || string(snap.State) != "s2" {
		t.Fatalf("ambiguous update did not take effect: %+v %v", snap, err)
	}
	if p := payloads(t, s); !slices.Equal(p, []string{"t"}) {
		t.Fatalf("ambiguous update tasks: %v", p)
	}
	// A blind retry of the same write now loses the compare-and-swap.
	if _, _, err := s.UpdateRun(ctx, f, k, store.Update{ExpectedDBVersion: 1, Events: ev(3), State: []byte("s2"), Open: true}); !errors.Is(err, store.ErrConflict) {
		t.Fatalf("retry after ambiguous commit: want ErrConflict, got %v", err)
	}

	fail = "CreateRun"
	k2 := store.RunKey{Shard: 0, WorkflowID: "wf2", RunID: "r"}
	if _, err := s.CreateRun(ctx, f, store.NewRun{Key: k2, Events: ev(1)}); !errors.Is(err, errInjected) {
		t.Fatalf("want injected error, got %v", err)
	}
	if _, err := s.CreateRun(ctx, f, store.NewRun{Key: k2, Events: ev(1)}); !errors.Is(err, store.ErrAlreadyExists) {
		t.Fatalf("retry after ambiguous create: want ErrAlreadyExists, got %v", err)
	}

	fail = "DeleteTasks"
	tasks, _ := s.ReadTransferTasks(ctx, 0, 0, 100)
	if err := s.DeleteTasks(ctx, f, []store.TaskKey{tasks[0].Key()}); !errors.Is(err, errInjected) {
		t.Fatalf("want injected error, got %v", err)
	}
	if p := payloads(t, s); len(p) != 0 {
		t.Fatalf("ambiguous delete did not take effect: %v", p)
	}
}

// Writes call every hook with their method name; reads call none.
func TestHooksSeeWriteOpsOnly(t *testing.T) {
	var before, after, delay []string
	s := memstore.New(memstore.WithRuntime(xsync.NewReal(ctx)), memstore.WithFaults(memstore.Faults{
		BeforeCommit: func(op string) error { before = append(before, op); return nil },
		AfterCommit:  func(op string) error { after = append(after, op); return nil },
		CommitDelay:  func(op string) time.Duration { delay = append(delay, op); return 0 },
	}))
	f, keys := setup(t, s, "r1")
	k := keys[0]
	if err := s.RenewShard(ctx, f, t0, time.Minute); err != nil {
		t.Fatal(err)
	}
	if _, _, err := s.UpdateRun(ctx, f, k, store.Update{ExpectedDBVersion: 1, State: []byte("x"), Open: true}); err != nil {
		t.Fatal(err)
	}
	if err := s.DeleteTasks(ctx, f, nil); err != nil {
		t.Fatal(err)
	}
	want := []string{"AcquireShard", "CreateRun", "RenewShard", "UpdateRun", "DeleteTasks"}
	for name, got := range map[string][]string{"BeforeCommit": before, "AfterCommit": after, "CommitDelay": delay} {
		if !slices.Equal(got, want) {
			t.Errorf("%s saw %v, want %v", name, got, want)
		}
	}

	before, after, delay = nil, nil, nil
	_, _ = s.GetShard(ctx, 0)
	_, _ = s.GetRun(ctx, k)
	_, _ = s.GetLatestRun(ctx, 0, k.WorkflowID)
	_, _ = s.ReadHistory(ctx, k, 1, 10)
	_, _ = s.ReadTransferTasks(ctx, 0, 0, 10)
	_, _ = s.ReadTimerTasks(ctx, 0, t0, 10)
	if len(before)+len(after)+len(delay) != 0 {
		t.Fatalf("reads called write hooks: %v %v %v", before, after, delay)
	}
}

// countingRuntime counts Yield calls on top of a production runtime.
type countingRuntime struct {
	xsync.Runtime
	yields int
}

func (c *countingRuntime) Yield() { c.yields++ }

func TestEveryMethodYieldsOnEntry(t *testing.T) {
	rt := &countingRuntime{Runtime: xsync.NewReal(ctx)}
	s := memstore.New(memstore.WithRuntime(rt))
	f, keys := setup(t, s, "r1")
	k := keys[0]
	if rt.yields != 2 {
		t.Fatalf("AcquireShard and CreateRun: want 2 yields, got %d", rt.yields)
	}
	calls := map[string]func(){
		"GetShard":          func() { _, _ = s.GetShard(ctx, 0) },
		"AcquireShard":      func() { _, _ = s.AcquireShard(ctx, 1, "n1", t0, time.Minute) },
		"RenewShard":        func() { _ = s.RenewShard(ctx, f, t0, time.Minute) },
		"CreateRun":         func() { _, _ = s.CreateRun(ctx, f, store.NewRun{Key: k, Events: ev(1)}) },
		"UpdateRun":         func() { _, _, _ = s.UpdateRun(ctx, f, k, store.Update{ExpectedDBVersion: 1}) },
		"GetRun":            func() { _, _ = s.GetRun(ctx, k) },
		"GetLatestRun":      func() { _, _ = s.GetLatestRun(ctx, 0, k.WorkflowID) },
		"ReadHistory":       func() { _, _ = s.ReadHistory(ctx, k, 1, 10) },
		"ReadTransferTasks": func() { _, _ = s.ReadTransferTasks(ctx, 0, 0, 10) },
		"ReadTimerTasks":    func() { _, _ = s.ReadTimerTasks(ctx, 0, t0, 10) },
		"DeleteTasks":       func() { _ = s.DeleteTasks(ctx, f, nil) },
	}
	for _, name := range xsync.SortedKeys(calls) {
		before := rt.yields
		calls[name]()
		if got := rt.yields - before; got != 1 {
			t.Errorf("%s yielded %d times, want 1", name, got)
		}
	}
}

func TestCommitDelayIgnoredWithoutRuntime(t *testing.T) {
	called := false
	s := memstore.New(memstore.WithFaults(memstore.Faults{CommitDelay: func(string) time.Duration {
		called = true
		return time.Hour
	}}))
	f, keys := setup(t, s, "r1")
	if _, _, err := s.UpdateRun(ctx, f, keys[0], store.Update{ExpectedDBVersion: 1, Open: true}); err != nil {
		t.Fatal(err)
	}
	if called {
		t.Fatal("CommitDelay consulted without a runtime")
	}
}

// delayFirstUpdate delays the first UpdateRun by d and no other write.
func delayFirstUpdate(d time.Duration) memstore.Faults {
	n := 0
	return memstore.Faults{CommitDelay: func(op string) time.Duration {
		if op != "UpdateRun" {
			return 0
		}
		n++
		if n == 1 {
			return d
		}
		return 0
	}}
}

func TestCommitDelayLetsALaterTaskIDCommitFirst(t *testing.T) {
	rt := simrt.New(1, t0)
	defer rt.Close()
	s := memstore.New(memstore.WithRuntime(rt), memstore.WithFaults(delayFirstUpdate(10*time.Second)))
	f, keys := setup(t, s, "r1", "r2")

	var slowID, fastID int64
	var slowDone time.Time
	var seenEarly []string
	rt.Go("slow", func(context.Context) {
		_, tasks, err := s.UpdateRun(ctx, f, keys[0], store.Update{ExpectedDBVersion: 1, Events: ev(3), Open: true,
			NewTasks: transfer(keys[0], "slow")})
		if err != nil {
			t.Errorf("slow: %v", err)
			return
		}
		slowID, slowDone = tasks[0].ID, rt.Now()
	})
	rt.Go("fast", func(context.Context) {
		rt.Sleep(time.Second)
		_, tasks, err := s.UpdateRun(ctx, f, keys[1], store.Update{ExpectedDBVersion: 1, Events: ev(3), Open: true,
			NewTasks: transfer(keys[1], "fast")})
		if err != nil {
			t.Errorf("fast: %v", err)
			return
		}
		fastID = tasks[0].ID
		seenEarly = payloads(t, s)
	})
	rt.Run(time.Time{}, nil)

	if slowID == 0 || fastID == 0 || slowID >= fastID {
		t.Fatalf("slow write must allocate the earlier ID: slow=%d fast=%d", slowID, fastID)
	}
	if !slices.Equal(seenEarly, []string{"fast"}) {
		t.Fatalf("before the slow commit only the later task is visible, got %v", seenEarly)
	}
	if !slowDone.Equal(t0.Add(10 * time.Second)) {
		t.Fatalf("slow write returned at %v, want after its 10s delay", slowDone)
	}
	if got := payloads(t, s); !slices.Equal(got, []string{"slow", "fast"}) {
		t.Fatalf("after both commits want [slow fast] by ID, got %v", got)
	}
}

// A write that was valid before its delay is checked again at commit time.
func TestCommitDelayRechecksVersionAtCommit(t *testing.T) {
	rt := simrt.New(1, t0)
	defer rt.Close()
	s := memstore.New(memstore.WithRuntime(rt), memstore.WithFaults(delayFirstUpdate(10*time.Second)))
	f, keys := setup(t, s, "r1")
	k := keys[0]

	var slowErr, fastErr error
	rt.Go("slow", func(context.Context) {
		_, _, slowErr = s.UpdateRun(ctx, f, k, store.Update{ExpectedDBVersion: 1, Events: ev(3), State: []byte("slow"), Open: true,
			NewTasks: transfer(k, "slow")})
	})
	rt.Go("fast", func(context.Context) {
		rt.Sleep(time.Second)
		_, _, fastErr = s.UpdateRun(ctx, f, k, store.Update{ExpectedDBVersion: 1, Events: ev(3), State: []byte("fast"), Open: true,
			NewTasks: transfer(k, "fast")})
	})
	rt.Run(time.Time{}, nil)

	if fastErr != nil || !errors.Is(slowErr, store.ErrConflict) {
		t.Fatalf("want fast to win and slow to conflict, got fast=%v slow=%v", fastErr, slowErr)
	}
	snap, err := s.GetRun(ctx, k)
	if err != nil || snap.DBVersion != 2 || string(snap.State) != "fast" {
		t.Fatalf("bad snapshot %+v %v", snap, err)
	}
	if got := payloads(t, s); !slices.Equal(got, []string{"fast"}) {
		t.Fatalf("conflicting write leaked tasks: %v", got)
	}
}
