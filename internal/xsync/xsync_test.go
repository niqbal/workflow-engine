package xsync

import (
	"context"
	"testing"
	"time"
)

func TestChanFIFOAndDeadline(t *testing.T) {
	rt := NewReal(context.Background())
	q := NewChan[int](rt)
	q.Send(1)
	q.Send(2)
	if v, ok := q.Receive(time.Time{}); !ok || v != 1 {
		t.Fatalf("got %v %v", v, ok)
	}
	if v, ok := q.TryReceive(); !ok || v != 2 {
		t.Fatalf("got %v %v", v, ok)
	}
	start := time.Now()
	if _, ok := q.Receive(time.Now().Add(30 * time.Millisecond)); ok {
		t.Fatal("expected timeout")
	}
	if time.Since(start) < 25*time.Millisecond {
		t.Fatal("returned before deadline")
	}
}

func TestChanWakesWaiter(t *testing.T) {
	rt := NewReal(context.Background())
	q := NewChan[string](rt)
	got := make(chan string, 1)
	rt.Go("recv", func(context.Context) {
		v, _ := q.Receive(time.Now().Add(5 * time.Second))
		got <- v
	})
	time.Sleep(10 * time.Millisecond)
	q.Send("x")
	if v := <-got; v != "x" {
		t.Fatalf("got %q", v)
	}
	rt.Wait()
}

func TestSortedKeys(t *testing.T) {
	m := map[string]int{"b": 1, "a": 2, "c": 3}
	k := SortedKeys(m)
	if len(k) != 3 || k[0] != "a" || k[2] != "c" {
		t.Fatalf("got %v", k)
	}
}
