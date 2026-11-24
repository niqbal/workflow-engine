package sim

// Seed sweeps and failure reports (WI-044, SPEC §11.5).
//
// A sweep runs one workload over a range of seeds and writes a report for every seed that
// fails. The report is the whole point: it carries the oracle's verdict, the command that
// reproduces the run, and the trace, so a failure found once on a laptop is a failure anyone
// can step through later.

import (
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/niqbal/workflow-engine/internal/simrt"
)

// SweepOptions configures one sweep.
type SweepOptions struct {
	Workload string   // name from Workloads(); "" means "basic"
	Build    Builder  // overrides Workload when set
	Seeds    []uint64 // seeds to run, in order
	Faults   *Faults  // nil means Standard()
	Parallel int      // runs at a time; < 1 means GOMAXPROCS
	Dir      string   // where failure reports go; "" means no reports
	// Report is called as each seed finishes, in completion order, for progress output.
	Report func(SeedResult)
}

// SeedResult is one seed's outcome.
type SeedResult struct {
	Seed     uint64
	Err      error
	Oracle   error
	Elapsed  time.Duration
	Virtual  time.Duration // virtual time the run covered
	FilePath string        // the failure report, when one was written
}

// Failed reports whether this seed is a failure.
func (r SeedResult) Failed() bool { return r.Err != nil }

// SweepResult is the whole sweep.
type SweepResult struct {
	Results  []SeedResult // in seed order
	Failures []SeedResult // in seed order
	Elapsed  time.Duration
}

// Sweep runs the workload over every seed and returns what happened. It never stops early:
// a sweep is worth more when it reports every failing seed, not just the first.
func Sweep(opts SweepOptions) (SweepResult, error) {
	build := opts.Build
	if build == nil {
		name := opts.Workload
		if name == "" {
			name = "basic"
		}
		b, ok := Workloads()[name]
		if !ok {
			return SweepResult{}, fmt.Errorf("sim: unknown workload %q; have %s",
				name, strings.Join(WorkloadNames(), ", "))
		}
		build = b
	}
	if len(opts.Seeds) == 0 {
		return SweepResult{}, fmt.Errorf("sim: a sweep needs at least one seed")
	}
	parallel := opts.Parallel
	if parallel < 1 {
		parallel = runtime.GOMAXPROCS(0)
	}
	if parallel > len(opts.Seeds) {
		parallel = len(opts.Seeds)
	}
	if opts.Dir != "" {
		if err := os.MkdirAll(opts.Dir, 0o755); err != nil {
			return SweepResult{}, fmt.Errorf("sim: failure directory: %w", err)
		}
	}

	started := time.Now()
	results := make([]SeedResult, len(opts.Seeds))
	var mu sync.Mutex
	var wg sync.WaitGroup
	next := make(chan int)
	go func() {
		for i := range opts.Seeds {
			next <- i
		}
		close(next)
	}()
	for w := 0; w < parallel; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := range next {
				seed := opts.Seeds[i]
				r := runSeed(seed, build, opts)
				results[i] = r
				if opts.Report != nil {
					mu.Lock()
					opts.Report(r)
					mu.Unlock()
				}
			}
		}()
	}
	wg.Wait()

	out := SweepResult{Results: results, Elapsed: time.Since(started)}
	for _, r := range results {
		if r.Failed() {
			out.Failures = append(out.Failures, r)
		}
	}
	sort.Slice(out.Failures, func(i, j int) bool { return out.Failures[i].Seed < out.Failures[j].Seed })
	return out, nil
}

// runSeed runs one seed and writes its report when it fails.
func runSeed(seed uint64, build Builder, opts SweepOptions) SeedResult {
	faults := opts.Faults
	if faults == nil {
		faults = Standard()
	}
	started := time.Now()
	res := Run(seed, build(), faults)
	r := SeedResult{Seed: seed, Err: res.Err, Oracle: res.Oracle,
		Elapsed: time.Since(started), Virtual: res.Now.Sub(simStart())}
	if r.Failed() && opts.Dir != "" {
		name := opts.Workload
		if name == "" {
			name = "basic"
		}
		path, err := writeReport(opts.Dir, name, seed, faults, res)
		if err != nil {
			r.Err = fmt.Errorf("%w (and the report could not be written: %v)", r.Err, err)
		}
		r.FilePath = path
	}
	return r
}

// writeReport writes sim/failures/<seed>.txt: the verdict, how to reproduce it, and the trace.
func writeReport(dir, workload string, seed uint64, faults *Faults, res Result) (string, error) {
	path := filepath.Join(dir, strconv.FormatUint(seed, 10)+".txt")
	var b strings.Builder
	fmt.Fprintf(&b, "workflow-engine simulation failure\n")
	fmt.Fprintf(&b, "workload: %s\nseed:     %d\n", workload, seed)
	fmt.Fprintf(&b, "virtual:  %s\n\n", res.Now.Sub(simStart()))
	fmt.Fprintf(&b, "reproduce:\n  wfe sim --workload %s --seeds %d\n\n", workload, seed)
	fmt.Fprintf(&b, "faults:\n%s\n", indent(faults.describe()))
	fmt.Fprintf(&b, "verdict:\n%s\n", indent(res.Err.Error()))
	if res.Oracle != nil {
		fmt.Fprintf(&b, "\noracle:\n%s\n", indent(res.Oracle.Error()))
	}
	if res.Record != nil {
		fmt.Fprintf(&b, "\nruns:\n%s\n", indent(describeFinals(res.Record)))
	}
	fmt.Fprintf(&b, "\ntrace (%d lines):\n", len(res.Trace))
	for _, line := range res.Trace {
		b.WriteString("  " + line + "\n")
	}
	if err := os.WriteFile(path, []byte(b.String()), 0o644); err != nil {
		return "", err
	}
	return path, nil
}

// describe lists what the injector was set to, so a report stands on its own.
func (f *Faults) describe() string {
	if f == nil {
		return "none"
	}
	var out []string
	add := func(format string, args ...any) { out = append(out, fmt.Sprintf(format, args...)) }
	if f.CrashEvery > 0 {
		add("node crash every %s, down for %s", f.CrashEvery, f.DownFor)
	}
	if f.FreezeEvery > 0 {
		add("process pause every %s, frozen for %s", f.FreezeEvery, f.FrozenFor)
	}
	if f.StoreErrorRate > 0 {
		add("store errors %.0f%%", f.StoreErrorRate*100)
	}
	if f.AmbiguousRate > 0 {
		add("ambiguous commits %.0f%%", f.AmbiguousRate*100)
	}
	if f.CommitDelayMax > 0 {
		add("commit delays up to %s", f.CommitDelayMax)
	}
	if f.ClockSkewMax > 0 {
		add("clock skew up to %s", f.ClockSkewMax)
	}
	if f.WorkerCrashRate > 0 {
		add("worker crashes %.0f%%", f.WorkerCrashRate*100)
	}
	if len(out) == 0 {
		return "none"
	}
	return strings.Join(out, "\n")
}

func describeFinals(rec *Record) string {
	var out []string
	for _, id := range sortedKeys(rec.Finals) {
		f := rec.Finals[id]
		out = append(out, fmt.Sprintf("%s %s, %d events", id, f.Status, len(f.Events)))
	}
	if len(out) == 0 {
		return "none started"
	}
	return strings.Join(out, "\n")
}

func indent(s string) string {
	lines := strings.Split(strings.TrimRight(s, "\n"), "\n")
	for i, l := range lines {
		lines[i] = "  " + l
	}
	return strings.Join(lines, "\n")
}

// ParseSeeds reads "7", "1-200" or "1-10,42,99-101" into a list of seeds.
func ParseSeeds(spec string) ([]uint64, error) {
	var out []uint64
	seen := map[uint64]bool{}
	for _, part := range strings.Split(spec, ",") {
		part = strings.TrimSpace(part)
		if part == "" {
			continue
		}
		lo, hi, err := parseRange(part)
		if err != nil {
			return nil, err
		}
		for s := lo; s <= hi; s++ {
			if !seen[s] {
				seen[s] = true
				out = append(out, s)
			}
		}
	}
	if len(out) == 0 {
		return nil, fmt.Errorf("sim: no seeds in %q", spec)
	}
	return out, nil
}

func parseRange(part string) (uint64, uint64, error) {
	if i := strings.Index(part, "-"); i >= 0 {
		lo, err1 := strconv.ParseUint(strings.TrimSpace(part[:i]), 10, 64)
		hi, err2 := strconv.ParseUint(strings.TrimSpace(part[i+1:]), 10, 64)
		if err1 != nil || err2 != nil {
			return 0, 0, fmt.Errorf("sim: %q is not a seed range", part)
		}
		if hi < lo {
			return 0, 0, fmt.Errorf("sim: seed range %q ends before it starts", part)
		}
		return lo, hi, nil
	}
	s, err := strconv.ParseUint(part, 10, 64)
	if err != nil {
		return 0, 0, fmt.Errorf("sim: %q is not a seed", part)
	}
	return s, s, nil
}

// simStart is the virtual clock's origin, for reporting how far a run got.
func simStart() time.Time { return simrt.Start }
