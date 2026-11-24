package sim_test

// WI-043 and WI-044: the workload mix, the seed sweep and its failure reports.

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/niqbal/workflow-engine/internal/server/api"
	"github.com/niqbal/workflow-engine/sim"
	"github.com/niqbal/workflow-engine/wf"
)

// TestWI043EveryWorkloadHoldsUnderEveryFault runs the mix under the standard fault set. The
// ranges are small enough for `go test` to stay quick; `wfe sim --seeds 1-200` is the sweep.
func TestWI043EveryWorkloadHoldsUnderEveryFault(t *testing.T) {
	for _, name := range sim.WorkloadNames() {
		name := name
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			seeds := []uint64{1, 2, 3, 4, 5}
			if name == "timers" {
				seeds = []uint64{1, 2} // a month of virtual time per run
			}
			res, err := sim.Sweep(sim.SweepOptions{Workload: name, Seeds: seeds})
			if err != nil {
				t.Fatal(err)
			}
			for _, f := range res.Failures {
				t.Errorf("seed %d: %v\n  reproduce: wfe sim --workload %s --seeds %d",
					f.Seed, f.Err, name, f.Seed)
			}
		})
	}
}

// TestWI043WorkloadsAreCleanWithoutFaults is the control: a failure here is the workload's
// own bug, not something the injectors did, and that distinction is worth one test.
func TestWI043WorkloadsAreCleanWithoutFaults(t *testing.T) {
	for _, name := range sim.WorkloadNames() {
		res, err := sim.Sweep(sim.SweepOptions{Workload: name, Seeds: []uint64{1, 2}, Faults: sim.NoFaults})
		if err != nil {
			t.Fatal(err)
		}
		for _, f := range res.Failures {
			t.Errorf("%s seed %d has no faults to blame: %v", name, f.Seed, f.Err)
		}
	}
}

// plantedBug is a workload whose workflow code replays differently from the history the live
// run recorded, which is the bug class I3 exists for.
func plantedBug() sim.Workload {
	replays := 0
	return sim.Workload{
		Nodes:      2,
		Shards:     2,
		QuietLimit: 24 * time.Hour,
		LongPoll:   30 * time.Second,
		Register: func(r sim.Registry) error {
			if err := r.RegisterWorkflow("planted", func(ctx wf.Context) error {
				replays++
				opts := wf.ActivityOptions{StartToCloseTimeout: time.Minute}
				if err := wf.ExecuteActivity(ctx, "A", 1, opts).Get(ctx, nil); err != nil {
					return err
				}
				if replays > 3 {
					return wf.ExecuteActivity(ctx, "B", 2, opts).Get(ctx, nil)
				}
				return nil
			}); err != nil {
				return err
			}
			if err := r.RegisterActivity("A", func(int) error { return nil }); err != nil {
				return err
			}
			return r.RegisterActivity("B", func(int) error { return nil })
		},
		Run: func(ctx context.Context, c *sim.Cluster) error {
			_, err := c.StartRetrying(ctx, api.StartRequest{WorkflowID: "p",
				WorkflowType: "planted", TaskQueue: "q", RequestID: "p"})
			return err
		},
	}
}

func TestWI044SweepFindsAPlantedBugAndWritesAReport(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "failures")
	res, err := sim.Sweep(sim.SweepOptions{
		Workload: "planted",
		Build:    plantedBug,
		Seeds:    []uint64{5},
		Faults:   sim.NoFaults,
		Dir:      dir,
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(res.Failures) != 1 {
		t.Fatalf("the planted bug was not found: %d failures in %d seeds", len(res.Failures), len(res.Results))
	}
	f := res.Failures[0]
	if f.FilePath == "" {
		t.Fatal("no report was written")
	}
	b, err := os.ReadFile(f.FilePath)
	if err != nil {
		t.Fatal(err)
	}
	report := string(b)
	// The report has to stand on its own: the verdict, the fault set it ran under, the
	// reproduction, and the trace.
	for _, want := range []string{"seed:     5", "reproduce:", "wfe sim --workload planted --seeds 5",
		"faults:", "verdict:", "I3", "trace ("} {
		if !strings.Contains(report, want) {
			t.Errorf("report does not contain %q:\n%s", want, firstLines(report, 25))
		}
	}
	// And the reproduction has to reproduce: the same seed fails the same way.
	again, err := sim.Sweep(sim.SweepOptions{Workload: "planted", Build: plantedBug,
		Seeds: []uint64{5}, Faults: sim.NoFaults})
	if err != nil {
		t.Fatal(err)
	}
	if len(again.Failures) != 1 {
		t.Fatal("the planted bug did not reproduce")
	}
	if again.Failures[0].Err.Error() != f.Err.Error() {
		t.Errorf("the reproduction failed differently:\n%v\n%v", again.Failures[0].Err, f.Err)
	}
}

func TestWI044SweepReportsEverySeedAndKeepsGoing(t *testing.T) {
	var seen []uint64
	res, err := sim.Sweep(sim.SweepOptions{
		Workload: "planted",
		Build:    plantedBug,
		Seeds:    []uint64{1, 2, 3},
		Faults:   sim.NoFaults,
		Report:   func(r sim.SeedResult) { seen = append(seen, r.Seed) },
	})
	if err != nil {
		t.Fatal(err)
	}
	// Every seed runs even though every seed fails: a sweep that stops at the first failure
	// hides the rest.
	if len(res.Results) != 3 || len(res.Failures) != 3 {
		t.Fatalf("%d results, %d failures, want 3 and 3", len(res.Results), len(res.Failures))
	}
	if len(seen) != 3 {
		t.Fatalf("the progress callback saw %d of 3 seeds", len(seen))
	}
	for i, f := range res.Failures {
		if i > 0 && f.Seed <= res.Failures[i-1].Seed {
			t.Fatalf("failures are not in seed order: %v", res.Failures)
		}
	}
}

func TestWI044ParseSeeds(t *testing.T) {
	cases := []struct {
		in   string
		want []uint64
	}{
		{"7", []uint64{7}},
		{"1-5", []uint64{1, 2, 3, 4, 5}},
		{"1-3,9,12-13", []uint64{1, 2, 3, 9, 12, 13}},
		{"4,4,4", []uint64{4}},
		{" 2 - 4 ", []uint64{2, 3, 4}},
	}
	for _, c := range cases {
		got, err := sim.ParseSeeds(c.in)
		if err != nil {
			t.Errorf("%q: %v", c.in, err)
			continue
		}
		if fmt.Sprint(got) != fmt.Sprint(c.want) {
			t.Errorf("%q: %v, want %v", c.in, got, c.want)
		}
	}
	for _, bad := range []string{"", "x", "5-1", "1-", "-3", "1,,x"} {
		if got, err := sim.ParseSeeds(bad); err == nil {
			t.Errorf("%q parsed as %v", bad, got)
		}
	}
}

func TestWI044SweepRejectsAnUnknownWorkload(t *testing.T) {
	_, err := sim.Sweep(sim.SweepOptions{Workload: "nope", Seeds: []uint64{1}})
	if err == nil {
		t.Fatal("an unknown workload is not an error")
	}
	for _, name := range sim.WorkloadNames() {
		if !strings.Contains(err.Error(), name) {
			t.Errorf("the error does not list %q: %v", name, err)
		}
	}
}

// TestWI043LedgerAndReferenceAreWiredForEveryWorkload checks the mix actually exercises the
// invariants it claims to: a workload with an effect has a ledger with keys in it, and every
// workload has a reference model that resolves.
func TestWI043LedgerAndReferenceAreWiredForEveryWorkload(t *testing.T) {
	for _, name := range sim.WorkloadNames() {
		build := sim.Workloads()[name]
		wl := build()
		if wl.Reference == nil {
			t.Errorf("%s has no reference model, so I8 checks nothing", name)
		}
		res := sim.Run(1, wl, sim.NoFaults)
		if res.Err != nil {
			t.Errorf("%s: %v", name, res.Err)
			continue
		}
		if wl.Ledger != nil && wl.Ledger.Len() == 0 {
			t.Errorf("%s has a ledger but applied no effects", name)
		}
		for _, key := range keysOf(wl.Ledger) {
			if got := wl.Ledger.Effects(key); got != 1 {
				t.Errorf("%s: %s applied %d times", name, key, got)
			}
		}
		if len(res.Record.Finals) == 0 {
			t.Errorf("%s started no workflows", name)
		}
		for id, f := range res.Record.Finals {
			want, err := wl.Reference(id, f.Events)
			if err != nil {
				t.Errorf("%s: reference model on %s: %v", name, id, err)
			}
			if len(want) == 0 {
				t.Errorf("%s: reference model has no result for %s", name, id)
			}
		}
	}
}

func keysOf(l *sim.Ledger) []string {
	if l == nil {
		return nil
	}
	return l.Keys()
}

func firstLines(s string, n int) string {
	lines := strings.Split(s, "\n")
	if len(lines) > n {
		lines = lines[:n]
	}
	return strings.Join(lines, "\n")
}
