package engine

import (
	"fmt"
	"math/rand"
	"runtime"
	"strings"
	"testing"
	"time"
)

// program is a randomly generated workflow-shaped program: coroutines that spawn children,
// wait on futures, settle futures, send and receive on channels, and select. Between steps the
// "history" (the test) settles futures and delivers channel items, as replay would.
type program struct {
	seed     int64
	futures  []*Future
	channels []*Channel
	external []func() // settled between steps, in order
}

func build(seed int64) (*Dispatcher, *program) {
	r := rand.New(rand.NewSource(seed))
	p := &program{seed: seed}
	for i := 0; i < 4+r.Intn(4); i++ {
		p.futures = append(p.futures, NewFuture())
	}
	for i := 0; i < 2+r.Intn(3); i++ {
		p.channels = append(p.channels, NewChannel())
	}
	// Half the futures and some channel items come from "history" between steps.
	for i, f := range p.futures {
		if i%2 == 0 {
			f, v := f, i
			p.external = append(p.external, func() { f.Set(v, nil) })
		}
	}
	for i := 0; i < 3; i++ {
		ch, v := p.channels[r.Intn(len(p.channels))], 1000+i
		p.external = append(p.external, func() { ch.Send(v) })
	}
	var body func(ctx *Ctx, depth int, r *rand.Rand)
	body = func(ctx *Ctx, depth int, r *rand.Rand) {
		d := ctx.Dispatcher()
		for op := 0; op < 3+r.Intn(5); op++ {
			switch r.Intn(6) {
			case 0: // coroutines settle odd futures only; history settles the even ones
				f := p.futures[1+2*r.Intn(len(p.futures)/2)]
				if !f.IsReady() {
					f.Set(op, nil)
					d.Tracef("set future")
				}
			case 1:
				f := p.futures[r.Intn(len(p.futures))]
				v, _ := f.Get(ctx)
				d.Tracef("got %v", v)
			case 2:
				ch := p.channels[r.Intn(len(p.channels))]
				ch.Send(op)
				d.Tracef("sent %d", op)
			case 3:
				ch := p.channels[r.Intn(len(p.channels))]
				if v, ok := ch.ReceiveAsync(); ok {
					d.Tracef("recv async %v", v)
				}
			case 4:
				if depth < 3 {
					child := rand.New(rand.NewSource(r.Int63()))
					d.Go("child", func(c *Ctx) { body(c, depth+1, child) })
					d.Tracef("spawned")
				}
			case 5:
				s := NewSelector()
				s.AddFuture(p.futures[r.Intn(len(p.futures))], func(f *Future) { d.Tracef("select future") })
				s.AddReceive(p.channels[r.Intn(len(p.channels))], func(ch *Channel) {
					v, _ := ch.ReceiveAsync()
					d.Tracef("select recv %v", v)
				})
				s.Select(ctx)
			}
		}
	}
	d := NewDispatcher(func(ctx *Ctx) { body(ctx, 0, rand.New(rand.NewSource(seed))) })
	return d, p
}

// execute runs the program the way a worker replays history: step, deliver, step, ...
func execute(seed int64) ([]string, error) {
	d, p := build(seed)
	defer d.Close()
	if err := d.ExecuteUntilAllBlocked(); err != nil {
		return nil, err
	}
	for _, ext := range p.external {
		ext()
		if err := d.ExecuteUntilAllBlocked(); err != nil {
			return nil, err
		}
	}
	return d.Trace(), nil
}

func TestRandomProgramsAreDeterministic(t *testing.T) {
	programs, runs := 1000, 50
	if testing.Short() {
		programs, runs = 100, 10
	}
	for seed := int64(1); seed <= int64(programs); seed++ {
		first, err := execute(seed)
		if err != nil {
			t.Fatalf("program %d: %v", seed, err)
		}
		for n := 2; n <= runs; n++ {
			got, err := execute(seed)
			if err != nil {
				t.Fatalf("program %d run %d: %v", seed, n, err)
			}
			if strings.Join(got, "\n") != strings.Join(first, "\n") {
				t.Fatalf("program %d diverged on run %d", seed, n)
			}
		}
	}
}

func TestNoGoroutineLeaks(t *testing.T) {
	before := runtime.NumGoroutine()
	for seed := int64(1); seed <= 200; seed++ {
		if _, err := execute(seed); err != nil {
			t.Fatal(err)
		}
	}
	deadline := time.Now().Add(2 * time.Second)
	for runtime.NumGoroutine() > before && time.Now().Before(deadline) {
		time.Sleep(10 * time.Millisecond)
	}
	if n := runtime.NumGoroutine(); n > before {
		t.Fatalf("goroutines leaked: %d before, %d after", before, n)
	}
}

func TestOrderIsCreationOrderAndOneAtATime(t *testing.T) {
	var log []string
	running := 0
	f := NewFuture()
	enter := func() {
		running++
		if running != 1 {
			panic("two coroutines running at once")
		}
	}
	d := NewDispatcher(func(ctx *Ctx) {
		for i := 0; i < 3; i++ {
			i := i
			ctx.Dispatcher().Go("c", func(c *Ctx) {
				enter()
				log = append(log, fmt.Sprintf("c%d before", i))
				running--
				f.Get(c)
				enter()
				log = append(log, fmt.Sprintf("c%d after", i))
				running--
			})
		}
		log = append(log, "root waits")
		f.Get(ctx)
		log = append(log, "root done")
	})
	defer d.Close()
	if err := d.ExecuteUntilAllBlocked(); err != nil {
		t.Fatal(err)
	}
	if got := strings.Join(log, ","); got != "root waits,c0 before,c1 before,c2 before" {
		t.Fatalf("first step: %s", got)
	}
	log = nil
	f.Set(1, nil) // "history" settles the future between steps
	if err := d.ExecuteUntilAllBlocked(); err != nil {
		t.Fatal(err)
	}
	if got := strings.Join(log, ","); got != "root done,c0 after,c1 after,c2 after" {
		t.Fatalf("second step: %s", got)
	}
	if !d.AllDone() {
		t.Fatalf("still blocked: %s", d.Blocked())
	}
}

func TestSelectorPicksFirstReadyCaseInOrder(t *testing.T) {
	a, b := NewFuture(), NewFuture()
	ch := NewChannel()
	var picked []string
	d := NewDispatcher(func(ctx *Ctx) {
		s := NewSelector().
			AddFuture(a, func(*Future) { picked = append(picked, "a") }).
			AddReceive(ch, func(c *Channel) { v, _ := c.ReceiveAsync(); picked = append(picked, fmt.Sprint("ch", v)) }).
			AddFuture(b, func(*Future) { picked = append(picked, "b") })
		for i := 0; i < 3; i++ {
			s.Select(ctx)
		}
	})
	defer d.Close()
	d.ExecuteUntilAllBlocked()
	b.Set(nil, nil)
	ch.Send(7)
	a.Set(nil, nil)
	d.ExecuteUntilAllBlocked()
	if got := strings.Join(picked, ","); got != "a,ch7,b" {
		t.Fatalf("picked %s, want a,ch7,b (first ready case in order; a future fires once)", got)
	}
}

func TestPanicIsReported(t *testing.T) {
	d := NewDispatcher(func(ctx *Ctx) { panic("bad workflow") })
	defer d.Close()
	if err := d.ExecuteUntilAllBlocked(); err == nil || !strings.Contains(err.Error(), "bad workflow") {
		t.Fatalf("panic not reported: %v", err)
	}
}
