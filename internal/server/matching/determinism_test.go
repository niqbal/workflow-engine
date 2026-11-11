package matching

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/niqbal/workflow-engine/internal/simrt"
)

// busy has several pollers per kind racing several pushers on two queues, with deadlines that
// sometimes expire before work arrives, and a Drop in the middle.
func busy(delivered map[int64]int) simrt.Scenario {
	return func(rt *simrt.Runtime) {
		m := New(rt)
		queues := []string{"a", "b"}
		kinds := []Kind{WorkflowTasks, ActivityTasks}
		for p := 0; p < 3; p++ {
			p := p
			rt.Go(fmt.Sprintf("pusher%d", p), func(context.Context) {
				for i := 0; i < 6; i++ {
					rt.Sleep(time.Duration(rt.Rand().Int63()%2000) * time.Millisecond)
					id := int64(p*100 + i)
					k, q := kinds[i%2], queues[(p+i)%2]
					m.Add(k, q, task(id))
					rt.Tracef("push %d %s/%s", id, k, q)
				}
			})
		}
		for c := 0; c < 4; c++ {
			c := c
			k, q := kinds[c%2], queues[c/2]
			rt.Go(fmt.Sprintf("poller%d", c), func(context.Context) {
				for misses := 0; misses < 3; {
					got, ok := m.Poll(k, q, rt.Now().Add(1500*time.Millisecond))
					if !ok {
						misses++
						rt.Tracef("c%d %s/%s empty", c, k, q)
						continue
					}
					if delivered != nil {
						delivered[got.ID]++
					}
					rt.Tracef("c%d %s/%s got %d", c, k, q, got.ID)
				}
			})
		}
		rt.Clock().AfterFunc(5*time.Second, func() {
			m.Drop()
			rt.Tracef("drop")
		})
	}
}

func TestRunTwiceTraceIsIdentical(t *testing.T) {
	for seed := uint64(1); seed <= 5; seed++ {
		if err := simrt.CheckDeterministic(20, seed, time.Hour, busy(nil)); err != nil {
			t.Fatal(err)
		}
	}
}

func TestEachTaskDeliveredAtMostOnce(t *testing.T) {
	for seed := uint64(1); seed <= 20; seed++ {
		delivered := map[int64]int{}
		if _, err := simrt.RunOnce(seed, time.Hour, busy(delivered)); err != nil {
			t.Fatalf("seed %d: %v", seed, err)
		}
		if len(delivered) == 0 {
			t.Fatalf("seed %d: no task was delivered", seed)
		}
		for id, n := range delivered {
			if n != 1 {
				t.Fatalf("seed %d: task %d delivered %d times", seed, id, n)
			}
		}
	}
}
