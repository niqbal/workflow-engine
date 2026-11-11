package simrt

import (
	"fmt"
	"time"
)

// Start is the virtual time every check run begins at.
var Start = time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)

// Scenario builds tasks on rt; the checker then runs rt until idle or the limit.
type Scenario func(rt *Runtime)

// RunOnce runs a scenario with a seed and returns its trace.
func RunOnce(seed uint64, limit time.Duration, sc Scenario) (trace []string, err error) {
	rt := New(seed, Start)
	defer rt.Close()
	defer func() {
		if p := recover(); p != nil {
			err = fmt.Errorf("%v", p)
		}
	}()
	sc(rt)
	var until time.Time
	if limit > 0 {
		until = Start.Add(limit)
	}
	rt.Run(until, nil)
	return rt.Trace(), nil
}

// CheckDeterministic runs the scenario `runs` times with one seed and reports the first
// difference between traces. This is the run-twice gate for every change (SPEC §11.5).
func CheckDeterministic(runs int, seed uint64, limit time.Duration, sc Scenario) error {
	first, err := RunOnce(seed, limit, sc)
	if err != nil {
		return fmt.Errorf("run 1: %w", err)
	}
	for n := 2; n <= runs; n++ {
		got, err := RunOnce(seed, limit, sc)
		if err != nil {
			return fmt.Errorf("run %d: %w", n, err)
		}
		if d := diff(first, got); d != "" {
			return fmt.Errorf("run %d diverged from run 1 (seed %d): %s", n, seed, d)
		}
	}
	return nil
}

func diff(a, b []string) string {
	for i := 0; i < len(a) && i < len(b); i++ {
		if a[i] != b[i] {
			return fmt.Sprintf("line %d: %q vs %q", i+1, a[i], b[i])
		}
	}
	if len(a) != len(b) {
		return fmt.Sprintf("length %d vs %d", len(a), len(b))
	}
	return ""
}
