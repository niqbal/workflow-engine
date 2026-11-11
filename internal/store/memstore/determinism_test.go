package memstore_test

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/niqbal/workflow-engine/internal/simrt"
	"github.com/niqbal/workflow-engine/internal/store"
	"github.com/niqbal/workflow-engine/internal/store/memstore"
)

// faulty has writers racing on one run and a reader draining the outbox, with random
// errors before commit, ambiguous commits and commit latency drawn from the seed.
func faulty(rt *simrt.Runtime) {
	r := rt.Rand()
	chance := func(pct int64) bool { return r.Int63()%100 < pct }
	s := memstore.New(memstore.WithRuntime(rt), memstore.WithFaults(memstore.Faults{
		BeforeCommit: func(op string) error {
			if chance(10) {
				return errInjected
			}
			return nil
		},
		AfterCommit: func(op string) error {
			if chance(10) {
				return errors.New("ambiguous")
			}
			return nil
		},
		CommitDelay: func(op string) time.Duration { return time.Duration(r.Int63()%50) * time.Millisecond },
	}))
	k := store.RunKey{Shard: 0, WorkflowID: "wf", RunID: "r"}
	rt.Go("setup", func(context.Context) {
		var f store.Fence
		for {
			var err error
			if f, err = s.AcquireShard(ctx, 0, "n1", rt.Now(), time.Minute); err == nil {
				break
			}
			rt.Tracef("acquire: %v", err)
		}
		for {
			_, err := s.CreateRun(ctx, f, store.NewRun{Key: k, Events: ev(1), State: []byte("s")})
			rt.Tracef("create: %v", err)
			if err == nil || errors.Is(err, store.ErrAlreadyExists) {
				break
			}
		}
		for w := 0; w < 3; w++ {
			rt.Go(fmt.Sprintf("writer%d", w), func(context.Context) {
				for i := 0; i < 5; i++ {
					snap, err := s.GetRun(ctx, k)
					if err != nil {
						rt.Tracef("w%d get: %v", w, err)
						continue
					}
					v, tasks, err := s.UpdateRun(ctx, f, k, store.Update{ExpectedDBVersion: snap.DBVersion,
						Events: ev(snap.NextEventID), State: []byte(fmt.Sprintf("w%d-%d", w, i)), Open: true,
						NewTasks: transfer(k, fmt.Sprintf("w%d-%d", w, i))})
					var ids []int64
					for _, t := range tasks {
						ids = append(ids, t.ID)
					}
					rt.Tracef("w%d update %d: v=%d ids=%v err=%v", w, i, v, ids, err)
				}
			})
		}
		rt.Go("reader", func(context.Context) {
			for i := 0; i < 8; i++ {
				rt.Sleep(20 * time.Millisecond)
				tasks, err := s.ReadTransferTasks(ctx, 0, 0, 100)
				var keys []store.TaskKey
				for _, t := range tasks {
					keys = append(keys, t.Key())
				}
				rt.Tracef("read %v err=%v", keys, err)
				rt.Tracef("delete err=%v", s.DeleteTasks(ctx, f, keys))
			}
			hist, err := s.ReadHistory(ctx, k, 1, 1000)
			rt.Tracef("history len=%d err=%v", len(hist), err)
		})
	})
}

func TestRunTwiceTraceIsIdentical(t *testing.T) {
	for seed := uint64(1); seed <= 5; seed++ {
		if err := simrt.CheckDeterministic(20, seed, time.Hour, faulty); err != nil {
			t.Fatal(err)
		}
	}
}
