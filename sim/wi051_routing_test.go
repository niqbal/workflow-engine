package sim_test

// WI-051 and WI-052. The claim is that a client or a worker may talk to any node: the
// frontend forwards to whichever node owns the shard, or the queue, at that moment. These
// tests make the simulator route every call that way, including while shards fail over and
// while calls between nodes are being lost.

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/niqbal/workflow-engine/internal/history"
	"github.com/niqbal/workflow-engine/internal/server/api"
	"github.com/niqbal/workflow-engine/sim"
)

// routedWorkload is the standard workload with a routing cluster: leases decide ownership,
// and every client and worker call goes through a node's router.
func routedWorkload() sim.Workload {
	wl := sim.Basic()
	wl.Leases = true
	wl.Routing = true
	wl.LeaseDuration = 10 * time.Second
	return wl
}

func TestWI051EveryCallIsRoutedAndTheRunsComplete(t *testing.T) {
	res := sim.Run(11, routedWorkload(), nil)
	if res.Err != nil {
		t.Fatalf("run: %v", res.Err)
	}
	if len(res.Record.Finals) != 3 {
		t.Fatalf("%d runs, want 3", len(res.Record.Finals))
	}
	for id, f := range res.Record.Finals {
		if f.Status.Open() {
			t.Fatalf("%s is still open", id)
		}
	}
}

// TestWI051RequestsSucceedWhileShardsFailOver is WI-051's acceptance criterion.
func TestWI051RequestsSucceedWhileShardsFailOver(t *testing.T) {
	faults := &sim.Faults{CrashEvery: 20 * time.Second, DownFor: 25 * time.Second}
	for seed := uint64(1); seed <= 15; seed++ {
		res := sim.Run(seed, routedWorkload(), faults)
		if res.Err != nil {
			t.Fatalf("seed %d: %v", seed, res.Err)
		}
		if seed == 1 {
			if !hasTakeover(res) {
				t.Fatal("seed 1: no shard changed hands, so nothing was routed around")
			}
			if !strings.Contains(strings.Join(res.Trace, "\n"), "fault crash") {
				t.Fatal("seed 1: no node crashed")
			}
		}
	}
}

// TestWI052WorkersPollingAnyNodeReceiveEveryTask is WI-052's acceptance criterion: three
// nodes, workers on all of them, and a single task queue that lives on one node. Every task
// has to reach a worker, which means the queue owner's matching is reachable from any
// frontend, and that a task written by another shard's owner was pushed to it.
func TestWI052WorkersPollingAnyNodeReceiveEveryTask(t *testing.T) {
	wl := routedWorkload()
	// More runs than nodes, so the runs' shards and the queue's shard are owned by
	// different nodes and the pushes have to cross the network.
	wl.Run = func(ctx context.Context, c *sim.Cluster) error {
		ids := make([]string, 0, 8)
		for i := 0; i < 8; i++ {
			id := basicID(i)
			ids = append(ids, id)
			if _, err := c.StartRetrying(ctx, api.StartRequest{WorkflowID: id,
				WorkflowType: "basic", TaskQueue: "q", RequestID: id}); err != nil {
				return err
			}
		}
		c.Sleep(30 * time.Minute)
		for _, id := range ids {
			if err := c.SignalRetrying(ctx, api.SignalRequest{WorkflowID: id, SignalName: "go",
				Payload: []byte(`"done"`), RequestID: "sig-" + id}); err != nil {
				return err
			}
		}
		return nil
	}

	for seed := uint64(1); seed <= 10; seed++ {
		res := sim.Run(seed, wl, nil)
		if res.Err != nil {
			t.Fatalf("seed %d: %v", seed, res.Err)
		}
		if len(res.Record.Finals) != 8 {
			t.Fatalf("seed %d: %d runs finished, want 8", seed, len(res.Record.Finals))
		}
		// Workers on every node served tasks, which is what "poll any node" means.
		if n := len(workerIdentities(res)); n < 2 {
			t.Fatalf("seed %d: tasks were served to workers on %d node(s), want several", seed, n)
		}
	}
}

// TestWI052TasksCrossTheNetworkToTheirQueueOwner shows the push itself: with one queue and
// four shards, most runs live on a node that does not hold the queue, so their tasks have to
// be handed over.
func TestWI052TasksCrossTheNetworkToTheirQueueOwner(t *testing.T) {
	wl := routedWorkload()
	res := sim.Run(4, wl, nil)
	if res.Err != nil {
		t.Fatalf("run: %v", res.Err)
	}
	// Every run completed, and each one's tasks were served by some worker: without the
	// cross-node push the runs whose shard is not the queue's owner would never start.
	if len(res.Record.Finals) != 3 {
		t.Fatalf("%d runs, want 3", len(res.Record.Finals))
	}
	if len(workerIdentities(res)) == 0 {
		t.Fatal("no worker ever served a task")
	}
}

// TestWI051RoutingSurvivesLostCallsAndReplies is the ambiguity the transport contract exists
// for: a reply lost after the peer has acted. Routing retries, so a start or a signal may be
// delivered twice, and the request ID is what keeps that from showing up in history (G1, G7).
func TestWI051RoutingSurvivesLostCallsAndReplies(t *testing.T) {
	faults := &sim.Faults{MessageDropRate: 0.1, MessageDelayMax: 250 * time.Millisecond}
	for seed := uint64(1); seed <= 10; seed++ {
		res := sim.Run(seed, routedWorkload(), faults)
		if res.Err != nil {
			t.Fatalf("seed %d: %v", seed, res.Err)
		}
		if seed == 1 {
			trace := strings.Join(res.Trace, "\n")
			if !strings.Contains(trace, "fault drop-call") && !strings.Contains(trace, "fault drop-reply") {
				t.Fatal("seed 1: no call or reply was lost")
			}
		}
	}
}

// TestWI051EverythingAtOnce is the one that matters: leases moving, nodes crashing and
// pausing, the store failing, calls between nodes lost, and every client and worker call
// routed. The oracle checks the rest.
func TestWI051EverythingAtOnce(t *testing.T) {
	all := sim.Standard()
	all.CrashEvery = 25 * time.Second
	all.FreezeEvery = 30 * time.Second
	all.MessageDropRate = 0.05
	all.MessageDelayMax = 200 * time.Millisecond
	for seed := uint64(1); seed <= 10; seed++ {
		res := sim.Run(seed, routedWorkload(), all)
		if res.Err != nil {
			t.Fatalf("seed %d: %v\n  reproduce: wfe sim --workload routed --seeds %d", seed, res.Err, seed)
		}
	}
}

func TestWI051RoutingIsDeterministic(t *testing.T) {
	faults := &sim.Faults{CrashEvery: 25 * time.Second, MessageDropRate: 0.05}
	const seed = 6
	first := strings.Join(sim.Run(seed, routedWorkload(), faults).Trace, "\n")
	for n := 2; n <= 3; n++ {
		got := strings.Join(sim.Run(seed, routedWorkload(), faults).Trace, "\n")
		if got != first {
			t.Fatalf("run %d diverged: %s", n, firstDiff([]byte(first), []byte(got)))
		}
	}
}

// ---- helpers ------------------------------------------------------------------------------

func basicID(i int) string { return fmt.Sprintf("basic-%d", i) }

// workerIdentities collects the workers that were handed a task, from the histories.
func workerIdentities(res sim.Result) map[string]bool {
	out := map[string]bool{}
	for _, f := range res.Record.Finals {
		for _, e := range f.Events {
			if a, ok := e.Attrs.(history.WorkflowTaskStarted); ok && a.WorkerIdentity != "" {
				out[a.WorkerIdentity] = true
			}
		}
	}
	return out
}
