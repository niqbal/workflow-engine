package main

// wfe sim: the seed sweep (WI-044, SPEC §11.5).
//
// One workload, a range of seeds, every fault the simulator can inject, and the full oracle.
// A failing seed is written to sim/failures/<seed>.txt with the command that reproduces it,
// which is the whole value of a deterministic simulator: a bug found once is a bug anyone can
// step through afterwards.

import (
	"flag"
	"fmt"
	"io"
	"strings"
	"time"

	"github.com/niqbal/workflow-engine/sim"
)

func runSim(args []string, stdout, stderr io.Writer) error {
	fs := flag.NewFlagSet("sim", flag.ContinueOnError)
	fs.SetOutput(stderr)
	seeds := fs.String("seeds", "1-50", "seeds to run: 7, 1-200, or 1-10,42,99-101")
	workload := fs.String("workload", "basic", "workload: "+strings.Join(sim.WorkloadNames(), ", "))
	parallel := fs.Int("parallel", 0, "runs at a time (default: one per CPU)")
	dir := fs.String("failures", "sim/failures", "directory for failure reports; empty to write none")
	quiet := fs.Bool("quiet", false, "print only the summary and the failures")
	noFaults := fs.Bool("no-faults", false, "run without fault injection")
	fs.Usage = func() {
		fmt.Fprintln(stderr, "Usage:")
		fmt.Fprintln(stderr, "  wfe sim [flags]")
		fmt.Fprintln(stderr)
		fmt.Fprintln(stderr, "Runs the deterministic cluster simulation over a range of seeds and checks")
		fmt.Fprintln(stderr, "invariants I1-I8 on every run (SPEC §11). Same seed, same run, every time.")
		fmt.Fprintln(stderr)
		fmt.Fprintln(stderr, "Flags:")
		fs.PrintDefaults()
	}
	if err := fs.Parse(args); err != nil {
		return err
	}
	if fs.NArg() > 0 {
		fs.Usage()
		return fmt.Errorf("unexpected argument %q", fs.Arg(0))
	}

	list, err := sim.ParseSeeds(*seeds)
	if err != nil {
		return err
	}
	faults := sim.Standard()
	if *noFaults {
		faults = sim.NoFaults
	}

	opts := sim.SweepOptions{
		Workload: *workload,
		Seeds:    list,
		Faults:   faults,
		Parallel: *parallel,
		Dir:      *dir,
	}
	if !*quiet {
		opts.Report = func(r sim.SeedResult) {
			if r.Failed() {
				return // printed in the summary, with its report path
			}
			fmt.Fprintf(stdout, "seed %d ok (%s virtual in %s)\n", r.Seed,
				r.Virtual.Round(time.Second), r.Elapsed.Round(time.Millisecond))
		}
	}

	res, err := sim.Sweep(opts)
	if err != nil {
		return err
	}
	for _, f := range res.Failures {
		fmt.Fprintf(stdout, "\nseed %d FAILED\n  %s\n", f.Seed, indentLines(f.Err.Error()))
		if f.FilePath != "" {
			fmt.Fprintf(stdout, "  report: %s\n", f.FilePath)
		}
		fmt.Fprintf(stdout, "  reproduce: wfe sim --workload %s --seeds %d\n", *workload, f.Seed)
	}
	fmt.Fprintf(stdout, "\n%d seeds, %d failed, %s\n", len(res.Results), len(res.Failures),
		res.Elapsed.Round(time.Millisecond))
	if len(res.Failures) > 0 {
		return fmt.Errorf("%d of %d seeds failed", len(res.Failures), len(res.Results))
	}
	return nil
}

func indentLines(s string) string {
	return strings.ReplaceAll(strings.TrimRight(s, "\n"), "\n", "\n  ")
}
