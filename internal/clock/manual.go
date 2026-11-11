package clock

import (
	"sort"
	"sync"
	"time"
)

// Manual is a clock that moves only when told to. AfterFunc callbacks run synchronously,
// in fire-time order, inside Advance. Used by the contract-test harness; the simulator has
// its own virtual clock (internal/simrt).
type Manual struct {
	mu     sync.Mutex
	now    time.Time
	seq    int
	timers []*manualTimer
}

type manualTimer struct {
	at     time.Time
	seq    int
	f      func()
	active bool
}

// NewManual returns a manual clock at t.
func NewManual(t time.Time) *Manual { return &Manual{now: t} }

func (m *Manual) Now() time.Time {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.now
}

func (m *Manual) AfterFunc(d time.Duration, f func()) Cancel {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.seq++
	t := &manualTimer{at: m.now.Add(d), seq: m.seq, f: f, active: true}
	m.timers = append(m.timers, t)
	return func() bool {
		m.mu.Lock()
		defer m.mu.Unlock()
		was := t.active
		t.active = false
		return was
	}
}

// Advance moves time forward by d, running each due callback at its own fire time.
func (m *Manual) Advance(d time.Duration) { m.AdvanceTo(m.Now().Add(d)) }

// AdvanceTo moves time forward to t (never backwards).
func (m *Manual) AdvanceTo(t time.Time) {
	for {
		m.mu.Lock()
		sort.SliceStable(m.timers, func(i, j int) bool {
			if !m.timers[i].at.Equal(m.timers[j].at) {
				return m.timers[i].at.Before(m.timers[j].at)
			}
			return m.timers[i].seq < m.timers[j].seq
		})
		var next *manualTimer
		for len(m.timers) > 0 && !m.timers[0].active {
			m.timers = m.timers[1:]
		}
		if len(m.timers) > 0 && !m.timers[0].at.After(t) {
			next = m.timers[0]
			m.timers = m.timers[1:]
			next.active = false
			if next.at.After(m.now) {
				m.now = next.at
			}
		} else if t.After(m.now) {
			m.now = t
		}
		m.mu.Unlock()
		if next == nil {
			return
		}
		next.f()
	}
}
