package sim_test

// WI-050: shard leases under the simulator. The acceptance criterion is the one that matters
// for safety — a node paused for longer than its lease must not get a single write in — and
// the oracle's I6 is what checks it, from the ghost log of committed writes rather than from
// anything the engine says about itself.

import (
	"strings"
	"testing"
	"time"

	"github.com/niqbal/workflow-engine/sim"
)

// leaseWorkload is the standard mix, run as a cluster that places shards by lease.
func leaseWorkload() sim.Workload {
	wl := sim.Basic()
	wl.Leases = true
	wl.LeaseDuration = 10 * time.Second
	return wl
}

func TestWI050ClusterPlacesEveryShardByLease(t *testing.T) {
	res := sim.Run(7, leaseWorkload(), nil)
	if res.Err != nil {
		t.Fatalf("run: %v", res.Err)
	}
	// Every shard was acquired by someone, and the ghost log says who.
	granted := map[int]int{}
	for _, e := range res.Record.Commits {
		if e.Granted {
			granted[e.Fence.Shard]++
		}
	}
	if len(granted) != 4 {
		t.Fatalf("%d of 4 shards were ever acquired: %v", len(granted), granted)
	}
}

// TestWI050APausedNodeGetsNoWriteIn is the acceptance test. A node is frozen for several
// lease durations while the rest of the cluster carries on; by the time it wakes, its shards
// have moved. Nothing it believed is trusted: I6 holds every committed write against the
// epoch that was current at the moment it committed.
func TestWI050APausedNodeGetsNoWriteIn(t *testing.T) {
	wl := leaseWorkload()
	// Freeze one node at a time for three lease durations, which is long enough that its
	// lease lapses and a peer takes the shards over.
	faults := &sim.Faults{FreezeEvery: 20 * time.Second, FrozenFor: 30 * time.Second}

	for seed := uint64(1); seed <= 15; seed++ {
		res := sim.Run(seed, wl, faults)
		if res.Err != nil {
			t.Fatalf("seed %d: %v", seed, res.Err)
		}
		// I6 is in that verdict: no committed write carried a stale fence. Assert the test
		// actually exercised it, so a run where nothing froze cannot pass quietly.
		if !strings.Contains(strings.Join(res.Trace, "\n"), "fault freeze") {
			t.Fatalf("seed %d: no node was paused", seed)
		}
		if seed == 1 && !hasTakeover(res) {
			t.Fatalf("seed %d: no shard changed hands while a node was paused", seed)
		}
	}
}

// TestWI050ShardsFailOverWhenANodeDies is the liveness half: a node that is killed for longer
// than its lease has its shards taken over, and the runs it was serving still finish.
func TestWI050ShardsFailOverWhenANodeDies(t *testing.T) {
	wl := leaseWorkload()
	res := sim.Run(3, wl, &sim.Faults{CrashEvery: 25 * time.Second, DownFor: 40 * time.Second})
	if res.Err != nil {
		t.Fatalf("run: %v", res.Err)
	}
	if !hasTakeover(res) {
		t.Fatal("no shard was taken over while a node was down")
	}
	for id, f := range res.Record.Finals {
		if f.Status.Open() {
			t.Fatalf("%s is still open after a failover", id)
		}
	}
}

// TestWI050LeasesSurviveEveryFault runs the lease machinery under the whole fault set. The
// invariants are the point: ownership moving around must not cost a run, a timer or an effect.
func TestWI050LeasesSurviveEveryFault(t *testing.T) {
	all := sim.Standard()
	all.CrashEvery = 25 * time.Second
	all.FreezeEvery = 30 * time.Second
	for seed := uint64(1); seed <= 10; seed++ {
		res := sim.Run(seed, leaseWorkload(), all)
		if res.Err != nil {
			t.Fatalf("seed %d: %v\n  reproduce: wfe sim --workload leases --seeds %d", seed, res.Err, seed)
		}
	}
}

func TestWI050LeaseModeIsDeterministic(t *testing.T) {
	faults := &sim.Faults{CrashEvery: 25 * time.Second, FreezeEvery: 30 * time.Second}
	const seed = 5
	first := strings.Join(sim.Run(seed, leaseWorkload(), faults).Trace, "\n")
	for n := 2; n <= 3; n++ {
		got := strings.Join(sim.Run(seed, leaseWorkload(), faults).Trace, "\n")
		if got != first {
			t.Fatalf("run %d diverged: %s", n, firstDiff([]byte(first), []byte(got)))
		}
	}
}

// hasTakeover reports whether any shard was acquired by a second owner during the run, which
// is what a failover looks like in the ghost log.
func hasTakeover(res sim.Result) bool {
	owners := map[int]string{}
	for _, e := range res.Record.Commits {
		if !e.Granted {
			continue
		}
		if prev, seen := owners[e.Fence.Shard]; seen && prev != e.Fence.Owner {
			return true
		}
		owners[e.Fence.Shard] = e.Fence.Owner
	}
	return false
}
