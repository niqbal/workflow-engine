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
)

// delayFirst delays the first call of op by d and nothing else.
func delayFirst(op string, d time.Duration) func(string) time.Duration {
	n := 0
	return func(o string) time.Duration {
		if o != op {
			return 0
		}
		n++
		if n == 1 {
			return d
		}
		return 0
	}
}

// A write delayed past a shard takeover must be fenced at commit time.
func TestEdgeCommitDelayRechecksFence(t *testing.T) {
	rt := simrt.New(1, t0)
	defer rt.Close()
	s := memstore.New(memstore.WithRuntime(rt), memstore.WithFaults(memstore.Faults{CommitDelay: delayFirst("UpdateRun", 2*time.Minute)}))
	f, keys := setup(t, s, "r1")
	k := keys[0]

	var slowErr, takeErr error
	rt.Go("slow", func(context.Context) {
		_, _, slowErr = s.UpdateRun(ctx, f, k, store.Update{ExpectedDBVersion: 1, Events: ev(3), State: []byte("slow"), Open: true,
			NewTasks: transfer(k, "slow")})
	})
	rt.Go("takeover", func(context.Context) {
		rt.Sleep(90 * time.Second)
		_, takeErr = s.AcquireShard(ctx, 0, "n2", rt.Now(), time.Minute)
	})
	rt.Run(time.Time{}, nil)

	if takeErr != nil {
		t.Fatalf("takeover after lease expiry: %v", takeErr)
	}
	if !errors.Is(slowErr, store.ErrFenced) {
		t.Fatalf("delayed write after takeover: want ErrFenced, got %v", slowErr)
	}
	snap, err := s.GetRun(ctx, k)
	if err != nil || snap.DBVersion != 1 || string(snap.State) != "s1" {
		t.Fatalf("fenced write leaked: %+v %v", snap, err)
	}
	if p := payloads(t, s); len(p) != 0 {
		t.Fatalf("fenced write leaked tasks: %v", p)
	}
}

// A delayed CreateRun that loses to a concurrent create of the same key gets ErrAlreadyExists.
func TestEdgeCommitDelayRechecksCreateExistence(t *testing.T) {
	rt := simrt.New(1, t0)
	defer rt.Close()
	s := memstore.New(memstore.WithRuntime(rt), memstore.WithFaults(memstore.Faults{CommitDelay: delayFirst("CreateRun", 10*time.Second)}))
	f, err := s.AcquireShard(ctx, 0, "n1", t0, time.Hour)
	if err != nil {
		t.Fatal(err)
	}
	k := store.RunKey{Shard: 0, WorkflowID: "wf", RunID: "r"}
	var slowErr, fastErr error
	rt.Go("slow", func(context.Context) {
		_, slowErr = s.CreateRun(ctx, f, store.NewRun{Key: k, Events: ev(1), State: []byte("slow"), Tasks: transfer(k, "slow")})
	})
	rt.Go("fast", func(context.Context) {
		rt.Sleep(time.Second)
		_, fastErr = s.CreateRun(ctx, f, store.NewRun{Key: k, Events: ev(1), State: []byte("fast"), Tasks: transfer(k, "fast")})
	})
	rt.Run(time.Time{}, nil)
	if fastErr != nil || !errors.Is(slowErr, store.ErrAlreadyExists) {
		t.Fatalf("want fast ok, slow ErrAlreadyExists; got fast=%v slow=%v", fastErr, slowErr)
	}
	snap, err := s.GetRun(ctx, k)
	if err != nil || string(snap.State) != "fast" {
		t.Fatalf("bad snapshot %+v %v", snap, err)
	}
	if p := payloads(t, s); !slices.Equal(p, []string{"fast"}) {
		t.Fatalf("losing create leaked tasks: %v", p)
	}
}

// Delay and ambiguous commit together: the write is visible, the caller sees the error, and
// the write returns only after its delay.
func TestEdgeDelayedAmbiguousCommit(t *testing.T) {
	rt := simrt.New(1, t0)
	defer rt.Close()
	s := memstore.New(memstore.WithRuntime(rt), memstore.WithFaults(memstore.Faults{
		CommitDelay: delayFirst("UpdateRun", 5*time.Second),
		AfterCommit: func(op string) error {
			if op == "UpdateRun" {
				return errInjected
			}
			return nil
		},
	}))
	f, keys := setup(t, s, "r1")
	k := keys[0]
	var err error
	var done time.Time
	rt.Go("w", func(context.Context) {
		_, _, err = s.UpdateRun(ctx, f, k, store.Update{ExpectedDBVersion: 1, Events: ev(3), State: []byte("s2"), Open: true,
			NewTasks: transfer(k, "t")})
		done = rt.Now()
	})
	rt.Run(time.Time{}, nil)
	if !errors.Is(err, errInjected) {
		t.Fatalf("want injected, got %v", err)
	}
	if !done.Equal(t0.Add(5 * time.Second)) {
		t.Fatalf("returned at %v, want t0+5s", done)
	}
	snap, _ := s.GetRun(ctx, k)
	if snap.DBVersion != 2 || string(snap.State) != "s2" {
		t.Fatalf("ambiguous delayed write not applied: %+v", snap)
	}
	if p := payloads(t, s); !slices.Equal(p, []string{"t"}) {
		t.Fatalf("tasks: %v", p)
	}
}

// BeforeCommit on RenewShard and DeleteTasks leaves state untouched; AfterCommit on
// AcquireShard and RenewShard applies the write.
func TestEdgeFaultsOnShardAndDeleteOps(t *testing.T) {
	before, after := "", ""
	s := memstore.New(memstore.WithFaults(memstore.Faults{
		BeforeCommit: func(op string) error {
			if op == before {
				return errInjected
			}
			return nil
		},
		AfterCommit: func(op string) error {
			if op == after {
				return errInjected
			}
			return nil
		},
	}))
	f, keys := setup(t, s, "r1")
	k := keys[0]
	if _, _, err := s.UpdateRun(ctx, f, k, store.Update{ExpectedDBVersion: 1, Open: true, NewTasks: transfer(k, "t")}); err != nil {
		t.Fatal(err)
	}

	before = "RenewShard"
	if err := s.RenewShard(ctx, f, t0.Add(time.Hour), time.Minute); !errors.Is(err, errInjected) {
		t.Fatalf("want injected, got %v", err)
	}
	if info, _ := s.GetShard(ctx, 0); !info.LeaseExpiresAt.Equal(t0.Add(time.Minute)) {
		t.Fatalf("aborted renew changed lease: %+v", info)
	}

	before = "DeleteTasks"
	tasks, _ := s.ReadTransferTasks(ctx, 0, 0, 10)
	if err := s.DeleteTasks(ctx, f, []store.TaskKey{tasks[0].Key()}); !errors.Is(err, errInjected) {
		t.Fatalf("want injected, got %v", err)
	}
	if p := payloads(t, s); !slices.Equal(p, []string{"t"}) {
		t.Fatalf("aborted delete removed tasks: %v", p)
	}
	before = ""

	after = "RenewShard"
	if err := s.RenewShard(ctx, f, t0.Add(time.Hour), time.Minute); !errors.Is(err, errInjected) {
		t.Fatalf("want injected, got %v", err)
	}
	if info, _ := s.GetShard(ctx, 0); !info.LeaseExpiresAt.Equal(t0.Add(time.Hour + time.Minute)) {
		t.Fatalf("ambiguous renew not applied: %+v", info)
	}

	after = "AcquireShard"
	if _, err := s.AcquireShard(ctx, 0, "n1", t0, time.Minute); !errors.Is(err, errInjected) {
		t.Fatalf("want injected, got %v", err)
	}
	if info, _ := s.GetShard(ctx, 0); info.RangeID != f.RangeID+1 {
		t.Fatalf("ambiguous acquire did not bump range: %+v", info)
	}
	// The old fence is now stale.
	after = ""
	if err := s.RenewShard(ctx, f, t0, time.Minute); !errors.Is(err, store.ErrFenced) {
		t.Fatalf("old fence after ambiguous acquire: want ErrFenced, got %v", err)
	}
}

// Callers cannot mutate stored data through inputs or outputs.
func TestEdgeNoAliasing(t *testing.T) {
	s := memstore.New()
	f, err := s.AcquireShard(ctx, 0, "n1", t0, time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	k := store.RunKey{Shard: 0, WorkflowID: "wf", RunID: "r"}
	evs := []store.EventRecord{{ID: 1, Data: []byte("e1")}}
	state := []byte("st")
	in := transfer(k, "p")
	out, err := s.CreateRun(ctx, f, store.NewRun{Key: k, Events: evs, State: state, Tasks: in})
	if err != nil {
		t.Fatal(err)
	}
	if in[0].ID != 0 {
		t.Errorf("input task mutated: %+v", in[0])
	}
	evs[0].Data[0] = 'X'
	state[0] = 'X'
	in[0].Payload[0] = 'X'
	out[0].Payload[0] = 'Y'

	snap, _ := s.GetRun(ctx, k)
	snap.State[0] = 'Z'
	hist, _ := s.ReadHistory(ctx, k, 1, 10)
	hist[0].Data[0] = 'Z'
	tasks, _ := s.ReadTransferTasks(ctx, 0, 0, 10)
	tasks[0].Payload[0] = 'Z'

	snap, _ = s.GetRun(ctx, k)
	hist, _ = s.ReadHistory(ctx, k, 1, 10)
	if string(snap.State) != "st" || string(hist[0].Data) != "e1" {
		t.Fatalf("stored data aliased: state=%q event=%q", snap.State, hist[0].Data)
	}
	if p := payloads(t, s); !slices.Equal(p, []string{"p"}) {
		t.Fatalf("stored task aliased: %v", p)
	}
}

// Timer tasks come back by (FireAt, ID), filtered by upTo and cut by limit.
func TestEdgeTimerOrdering(t *testing.T) {
	s := memstore.New()
	f, keys := setup(t, s, "r1")
	k := keys[0]
	timer := func(at time.Duration, p string) store.Task {
		return store.Task{Shard: 0, Kind: store.TimerTask, Run: k, FireAt: t0.Add(at), Payload: []byte(p)}
	}
	if _, _, err := s.UpdateRun(ctx, f, k, store.Update{ExpectedDBVersion: 1, Open: true,
		NewTasks: []store.Task{timer(3*time.Second, "c"), timer(time.Second, "a"), timer(time.Second, "b"), timer(time.Hour, "late")}}); err != nil {
		t.Fatal(err)
	}
	get := func(upTo time.Time, limit int) []string {
		ts, err := s.ReadTimerTasks(ctx, 0, upTo, limit)
		if err != nil {
			t.Fatal(err)
		}
		var out []string
		for _, x := range ts {
			out = append(out, string(x.Payload))
		}
		return out
	}
	if got := get(t0.Add(time.Minute), 10); !slices.Equal(got, []string{"a", "b", "c"}) {
		t.Fatalf("got %v", got)
	}
	if got := get(t0.Add(time.Second), 10); !slices.Equal(got, []string{"a", "b"}) {
		t.Fatalf("upTo inclusive: got %v", got)
	}
	if got := get(t0.Add(time.Minute), 1); !slices.Equal(got, []string{"a"}) {
		t.Fatalf("limit: got %v", got)
	}
	// Timer tasks do not show up in the transfer queue.
	if p := payloads(t, s); len(p) != 0 {
		t.Fatalf("timer tasks in transfer queue: %v", p)
	}
}
