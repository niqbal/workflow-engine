package clock

import (
	"testing"
	"time"
)

func TestManualFiresInOrderAtTheirTime(t *testing.T) {
	t0 := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	m := NewManual(t0)
	var log []time.Duration
	m.AfterFunc(3*time.Second, func() { log = append(log, m.Now().Sub(t0)) })
	m.AfterFunc(time.Second, func() {
		log = append(log, m.Now().Sub(t0))
		m.AfterFunc(time.Second, func() { log = append(log, m.Now().Sub(t0)) }) // scheduled while advancing
	})
	cancel := m.AfterFunc(2500*time.Millisecond, func() { t.Fatal("canceled timer ran") })
	cancel()
	m.Advance(10 * time.Second)
	want := []time.Duration{time.Second, 2 * time.Second, 3 * time.Second}
	if len(log) != 3 || log[0] != want[0] || log[1] != want[1] || log[2] != want[2] {
		t.Fatalf("fired at %v, want %v", log, want)
	}
	if m.Now().Sub(t0) != 10*time.Second {
		t.Fatalf("now %v", m.Now())
	}
}
