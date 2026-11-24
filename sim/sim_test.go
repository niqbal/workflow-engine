package sim_test

import (
	"bytes"
	"context"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/niqbal/workflow-engine/internal/history"
	"github.com/niqbal/workflow-engine/internal/server/api"
	"github.com/niqbal/workflow-engine/internal/simrt"
	"github.com/niqbal/workflow-engine/sim"
	"github.com/niqbal/workflow-engine/wf"
)

// longTimer starts four workflows, each an activity and then a 30-day timer, on 3 nodes.
func longTimer() sim.Workload {
	return sim.Workload{
		Nodes:      3,
		Shards:     4,
		QuietLimit: 40 * 24 * time.Hour,
		Register: func(r sim.Registry) error {
			if err := r.RegisterWorkflow("sleep", func(ctx wf.Context) error {
				opts := wf.ActivityOptions{StartToCloseTimeout: time.Minute}
				if err := wf.ExecuteActivity(ctx, "Nop", nil, opts).Get(ctx, nil); err != nil {
					return err
				}
				return wf.Sleep(ctx, 30*24*time.Hour)
			}); err != nil {
				return err
			}
			return r.RegisterActivity("Nop", func() error { return nil })
		},
		Run: func(ctx context.Context, c *sim.Cluster) error {
			for i := 0; i < 4; i++ {
				id := fmt.Sprintf("wf-%d", i)
				if _, err := c.StartWorkflow(ctx, api.StartRequest{
					WorkflowID: id, WorkflowType: "sleep", TaskQueue: "q", RequestID: id,
				}); err != nil {
					return err
				}
			}
			return nil
		},
	}
}

func traceBytes(lines []string) []byte { return []byte(strings.Join(lines, "\n")) }

func diffAt(a, b []byte) string {
	n := len(a)
	if len(b) < n {
		n = len(b)
	}
	for i := 0; i < n; i++ {
		if a[i] != b[i] {
			return fmt.Sprintf("byte %d: %#x vs %#x", i, a[i], b[i])
		}
	}
	if len(a) != len(b) {
		return fmt.Sprintf("length %d vs %d", len(a), len(b))
	}
	return ""
}

func requireClosed(t *testing.T, res sim.Result) {
	t.Helper()
	if res.Err != nil {
		t.Fatal(res.Err)
	}
	if !res.Closed {
		t.Fatal("workflows still open")
	}
	text := strings.Join(res.Trace, "\n")
	for i := 0; i < 4; i++ {
		id := fmt.Sprintf("wf-%d", i)
		if !strings.Contains(text, "status "+id+" completed") {
			t.Fatalf("missing completed status for %s", id)
		}
		if !strings.Contains(text, "history "+id+" ") || !strings.Contains(text, string(history.EventTimerFired)) {
			t.Fatalf("missing timer fire in trace for %s", id)
		}
	}
	got := res.Now.Sub(simrt.Start)
	if got < 30*24*time.Hour || got >= 31*24*time.Hour {
		t.Fatalf("virtual time %s, want 30 days and less than 31", got)
	}
}

func TestSameSeedIdenticalTrace(t *testing.T) {
	wl := longTimer()
	const seed = 42
	start := time.Now()
	first := sim.Run(seed, wl, nil)
	if time.Since(start) >= 5*time.Second {
		t.Fatalf("30-day workload took %s, want under 5s", time.Since(start))
	}
	requireClosed(t, first)
	want := traceBytes(first.Trace)
	for n := 2; n <= 20; n++ {
		got := sim.Run(seed, wl, nil)
		requireClosed(t, got)
		if d := diffAt(want, traceBytes(got.Trace)); d != "" {
			t.Fatalf("run %d diverged from run 1 (seed %d): %s", n, seed, d)
		}
	}
	// A non-nil injector with nothing enabled must not change the trace.
	withFaults := sim.Run(seed, wl, sim.NoFaults)
	requireClosed(t, withFaults)
	if d := diffAt(want, traceBytes(withFaults.Trace)); d != "" {
		t.Fatalf("faults value changed the trace: %s", d)
	}
	other := sim.Run(seed+1, wl, nil)
	requireClosed(t, other)
	if bytes.Equal(want, traceBytes(other.Trace)) {
		t.Fatal("different seeds produced the same trace")
	}
}
