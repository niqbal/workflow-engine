package sim

// The side-effect ledger (WI-041, SPEC §11.3 I5). An activity that has an external effect
// takes its idempotency key from activity.Info and applies the effect through Apply, which
// is what an idempotent sink does: the second attempt with the same key is a no-op. The
// oracle then checks that every key ended up applied exactly once, however many attempts
// the cluster made.
//
// A Ledger is used from simulator tasks, which hold the scheduler's baton one at a time, so
// it needs no lock of its own. Do not share one across runs: the counts are the evidence
// for a single seed.

import "sort"

// Ledger counts attempts and applied effects per idempotency key.
type Ledger struct {
	order    []string
	attempts map[string]int
	effects  map[string]int
	values   map[string]any
}

// NewLedger returns an empty ledger.
func NewLedger() *Ledger {
	return &Ledger{attempts: map[string]int{}, effects: map[string]int{}, values: map[string]any{}}
}

// Attempt records that an attempt carrying key reached the sink. Call it on every attempt,
// including the ones that go on to fail or to find the effect already applied.
func (l *Ledger) Attempt(key string) {
	l.touch(key)
	l.attempts[key]++
}

// Apply records the attempt and applies the effect if this key has not been applied before.
// It returns true when fn ran. fn may be nil.
func (l *Ledger) Apply(key string, fn func()) bool {
	l.Attempt(key)
	if l.effects[key] > 0 {
		return false
	}
	l.effects[key]++
	if fn != nil {
		fn()
	}
	return true
}

// ApplyAlways records an attempt and applies the effect whether or not the key was applied
// before. It models a sink that forgot to deduplicate, and exists so a test can produce the
// violation I5 is for; correct activities use Apply.
func (l *Ledger) ApplyAlways(key string, fn func()) {
	l.Attempt(key)
	l.effects[key]++
	if fn != nil {
		fn()
	}
}

// Put applies a value once per key, like Apply for a sink that stores something.
func (l *Ledger) Put(key string, value any) bool {
	return l.Apply(key, func() { l.values[key] = value })
}

// Value returns what Put stored for key.
func (l *Ledger) Value(key string) (any, bool) {
	v, ok := l.values[key]
	return v, ok
}

// Attempts is how many attempts carried key.
func (l *Ledger) Attempts(key string) int { return l.attempts[key] }

// Effects is how many times key was applied. The oracle wants exactly 1.
func (l *Ledger) Effects(key string) int { return l.effects[key] }

// Keys lists every key the ledger saw, sorted, so reports from one seed never reorder.
func (l *Ledger) Keys() []string {
	out := append([]string(nil), l.order...)
	sort.Strings(out)
	return out
}

// Len is how many distinct keys the ledger saw.
func (l *Ledger) Len() int { return len(l.order) }

func (l *Ledger) touch(key string) {
	if _, seen := l.attempts[key]; !seen {
		l.order = append(l.order, key)
	}
}
