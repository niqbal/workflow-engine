package client

import (
	"testing"
	"time"
)

func TestDefaultBackoffDoublesAndCaps(t *testing.T) {
	want := []time.Duration{100 * time.Millisecond, 200 * time.Millisecond, 400 * time.Millisecond,
		800 * time.Millisecond, 1600 * time.Millisecond, 3200 * time.Millisecond, 5 * time.Second, 5 * time.Second}
	for i, w := range want {
		if got := defaultBackoff(i + 1); got != w {
			t.Fatalf("defaultBackoff(%d) = %v, want %v", i+1, got, w)
		}
	}
	if got := defaultBackoff(1000); got != 5*time.Second {
		t.Fatalf("defaultBackoff(1000) = %v", got)
	}
}
