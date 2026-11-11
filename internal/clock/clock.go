// Package clock is the only way server code reads time or waits (SPEC §6.1).
// Production uses Real; the simulator supplies a virtual clock.
package clock

import "time"

// Cancel stops a pending AfterFunc. It reports whether the call stopped f from running.
type Cancel func() bool

// Clock is the source of time for all server code.
type Clock interface {
	Now() time.Time
	// AfterFunc runs f once, d from now, on its own goroutine (or simulated task).
	AfterFunc(d time.Duration, f func()) Cancel
}

// Real is the wall clock.
type Real struct{}

func (Real) Now() time.Time { return time.Now() }

func (Real) AfterFunc(d time.Duration, f func()) Cancel {
	t := time.AfterFunc(d, f)
	return t.Stop
}
