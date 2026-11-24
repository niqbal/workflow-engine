package sim_test

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/niqbal/workflow-engine/internal/history"
	"github.com/niqbal/workflow-engine/internal/server/api"
	"github.com/niqbal/workflow-engine/internal/simrt"
	"github.com/niqbal/workflow-engine/sim"
	"github.com/niqbal/workflow-engine/wf"
)

// thirtyDay is the acceptance workload: several workflows, each an activity then a
// 30-day timer, on more than one node.
func thirtyDay() sim.Workload {
	return sim.Workload{
		Nodes:      3,
		Shards:     4,
		Workers:    1,
		QuietLimit: 40 * 24 * time.Hour,
		Register: func(r sim.Registry) error {
			if err := r.RegisterWorkflow("sleep", func(ctx wf.Context) error {
				var n int
				err := wf.ExecuteActivity(ctx, "Inc", 1, wf.ActivityOptions{StartToCloseTimeout: time.Minute}).Get(ctx, &n)
				if err != nil {
					return err
				}
				if n != 2 {
					return fmt.Errorf("activity result %d", n)
				}
				return wf.Sleep(ctx, 30*24*time.Hour)
			}); err != nil {
				return err
			}
			return r.RegisterActivity("Inc", func(n int) (int, error) { return n + 1, nil })
		},
		Run: func(ctx context.Context, c *sim.Cluster) error {
			if c.NumNodes() != 3 {
				return fmt.Errorf("nodes %d", c.NumNodes())
			}
			for i := 0; i < 4; i++ {
				id := fmt.Sprintf("wf-%d", i)
				if _, err := c.StartWorkflow(ctx, api.StartRequest{
					WorkflowID: id, WorkflowType: "sleep", TaskQueue: "q", RequestID: id,
				}); err != nil {
					return err
				}
			}
			return nil
		},
	}
}

func traceBlob(lines []string) []byte { return []byte(strings.Join(lines, "\n")) }

func firstDiff(a, b []byte) string {
	n := len(a)
	if len(b) < n {
		n = len(b)
	}
	for i := 0; i < n; i++ {
		if a[i] != b[i] {
			return fmt.Sprintf("byte %d: %#x vs %#x", i, a[i], b[i])
		}
	}
	return fmt.Sprintf("length %d vs %d", len(a), len(b))
}

func requireThirtyDay(t *testing.T, res sim.Result) {
	t.Helper()
	if res.Err != nil {
		t.Fatal(res.Err)
	}
	if !res.Closed {
		t.Fatal("workflows still open")
	}
	got := res.Now.Sub(simrt.Start)
	if got < 30*24*time.Hour || got >= 31*24*time.Hour {
		t.Fatalf("virtual time %s, want at least 30 days and less than 31", got)
	}
	for i := 0; i < 4; i++ {
		id := fmt.Sprintf("wf-%d", i)
		status, fired := 0, 0
		for _, line := range res.Trace {
			if line == "status "+id+" completed" {
				status++
			}
			if strings.HasPrefix(line, "history "+id+" ") && strings.HasSuffix(line, " "+string(history.EventTimerFired)) {
				fired++
			}
		}
		if status != 1 || fired != 1 {
			t.Fatalf("%s: completed lines %d, TimerFired lines %d", id, status, fired)
		}
	}
}

func TestWI040SameSeedByteIdenticalAndThirtyDayUnderFiveSeconds(t *testing.T) {
	wl := thirtyDay()
	const seed = uint64(99)
	var want []byte
	var first sim.Result
	for n := 1; n <= 20; n++ {
		start := time.Now()
		got := sim.Run(seed, wl, nil)
		if elapsed := time.Since(start); elapsed >= 5*time.Second {
			t.Fatalf("run %d: 30-day workload took %s, want under 5s", n, elapsed)
		}
		requireThirtyDay(t, got)
		blob := traceBlob(got.Trace)
		if len(blob) == 0 {
			t.Fatal("empty trace")
		}
		if n == 1 {
			want = blob
			first = got
			continue
		}
		if !bytes.Equal(want, blob) {
			t.Fatalf("run %d diverged from run 1 (seed %d): %s", n, seed, firstDiff(want, blob))
		}
		if !got.Now.Equal(first.Now) {
			t.Fatalf("run %d virtual time %s, run 1 %s", n, got.Now, first.Now)
		}
	}

	// Judging a run must not change it: the oracle records and checks, and the trace of a
	// run with the full oracle must be the trace of the same seed without it.
	hookedWL := wl
	hookedWL.Oracle = sim.Invariants{}
	hooked := sim.Run(seed, hookedWL, sim.NoFaults)
	requireThirtyDay(t, hooked)
	if hooked.Oracle != nil {
		t.Fatalf("oracle: %v", hooked.Oracle)
	}
	if !bytes.Equal(want, traceBlob(hooked.Trace)) {
		t.Fatalf("fault and oracle values changed the trace: %s", firstDiff(want, traceBlob(hooked.Trace)))
	}

	other := sim.Run(seed+1, wl, nil)
	requireThirtyDay(t, other)
	if bytes.Equal(want, traceBlob(other.Trace)) {
		t.Fatal("different seeds produced byte-identical traces")
	}
}

func TestWI040QuietDeadlineLeavesWorkflowOpen(t *testing.T) {
	const limit = time.Hour
	res := sim.Run(3, sim.Workload{
		Nodes:      2,
		Shards:     2,
		QuietLimit: limit,
		Register: func(r sim.Registry) error {
			return r.RegisterWorkflow("wait", func(ctx wf.Context) error {
				wf.GetSignalChannel(ctx, "go").Receive(ctx, nil)
				return nil
			})
		},
		Run: func(ctx context.Context, c *sim.Cluster) error {
			_, err := c.StartWorkflow(ctx, api.StartRequest{
				WorkflowID: "stuck", WorkflowType: "wait", TaskQueue: "q", RequestID: "stuck",
			})
			return err
		},
	}, nil)
	if res.Closed {
		t.Fatal("workflow reported closed")
	}
	if res.Err == nil {
		t.Fatal("expected quiet-phase error")
	}
	msg := res.Err.Error()
	for _, part := range []string{"quiet phase", "deadline", "still open"} {
		if !strings.Contains(msg, part) {
			t.Fatalf("error %q does not contain %q", msg, part)
		}
	}
	if !res.Now.Equal(simrt.Start.Add(limit)) {
		t.Fatalf("virtual time %s, want exactly the quiet deadline", res.Now.Sub(simrt.Start))
	}
	if !strings.Contains(strings.Join(res.Trace, "\n"), "status stuck running") {
		t.Fatalf("trace lost the open workflow:\n%s", strings.Join(res.Trace, "\n"))
	}
	if strings.Contains(strings.Join(res.Trace, "\n"), "status stuck completed") {
		t.Fatal("open workflow recorded as completed")
	}
}

func TestWI040WorkloadErrorIsReturned(t *testing.T) {
	res := sim.Run(5, sim.Workload{
		QuietLimit: 10 * 24 * time.Hour,
		Register: func(r sim.Registry) error {
			return r.RegisterWorkflow("spin", func(ctx wf.Context) error {
				return wf.Sleep(ctx, 48*time.Hour)
			})
		},
		Run: func(ctx context.Context, c *sim.Cluster) error {
			if _, err := c.StartWorkflow(ctx, api.StartRequest{
				WorkflowID: "w", WorkflowType: "spin", TaskQueue: "q", RequestID: "w",
			}); err != nil {
				return err
			}
			return errors.New("workload failed")
		},
	}, nil)
	if res.Err == nil || !strings.Contains(res.Err.Error(), "workload failed") {
		t.Fatalf("err %v", res.Err)
	}
	if res.Closed {
		t.Fatal("closed after workload error")
	}
	if res.Now.Sub(simrt.Start) >= 10*24*time.Hour {
		t.Fatalf("workload error was replaced by the quiet deadline at %s", res.Now.Sub(simrt.Start))
	}
	if !strings.Contains(strings.Join(res.Trace, "\n"), "status w ") {
		t.Fatal("started workflow missing from the trace")
	}
}

func TestWI040RegisterErrorSkipsRun(t *testing.T) {
	res := sim.Run(1, sim.Workload{
		Register: func(r sim.Registry) error {
			if err := r.RegisterWorkflow("", func(wf.Context) error { return nil }); err == nil {
				return errors.New("empty workflow type accepted")
			}
			if err := r.RegisterActivity("A", nil); err == nil {
				return errors.New("nil activity accepted")
			}
			if err := r.RegisterActivity("A", func() {}); err == nil {
				return errors.New("activity without error accepted")
			}
			if err := r.RegisterWorkflow("W", func(wf.Context) error { return nil }); err != nil {
				return err
			}
			if err := r.RegisterWorkflow("W", func(wf.Context) error { return nil }); err == nil || !strings.Contains(err.Error(), "W") {
				return fmt.Errorf("duplicate workflow: %v", err)
			}
			if err := r.RegisterActivity("Act", func() error { return nil }); err != nil {
				return err
			}
			if err := r.RegisterActivity("Act", func() error { return nil }); err == nil || !strings.Contains(err.Error(), "Act") {
				return fmt.Errorf("duplicate activity: %v", err)
			}
			return errors.New("stop before boot")
		},
		Run: func(context.Context, *sim.Cluster) error {
			return errors.New("workload ran after register error")
		},
	}, nil)
	if res.Err == nil || res.Err.Error() != "stop before boot" {
		t.Fatalf("err %v", res.Err)
	}
	if res.Closed {
		t.Fatal("closed")
	}
}

func TestWI040ActivityFailuresClose(t *testing.T) {
	one := func(activity any, wfFn func(wf.Context) error) sim.Result {
		return sim.Run(11, sim.Workload{
			QuietLimit: time.Hour,
			Register: func(r sim.Registry) error {
				if err := r.RegisterWorkflow("W", wfFn); err != nil {
					return err
				}
				return r.RegisterActivity("A", activity)
			},
			Run: func(ctx context.Context, c *sim.Cluster) error {
				_, err := c.StartWorkflow(ctx, api.StartRequest{
					WorkflowID: "bad", WorkflowType: "W", TaskQueue: "q", RequestID: "bad",
				})
				return err
			},
		}, nil)
	}
	opts := wf.ActivityOptions{
		StartToCloseTimeout: time.Minute,
		RetryPolicy:         wf.RetryPolicy{InitialInterval: time.Second, BackoffCoefficient: 2, MaximumAttempts: 1},
	}
	cases := []struct {
		name string
		act  any
		wf   func(wf.Context) error
	}{
		{
			name: "returns error",
			act:  func() error { return errors.New("nope") },
			wf: func(ctx wf.Context) error {
				return wf.ExecuteActivity(ctx, "A", nil, opts).Get(ctx, nil)
			},
		},
		{
			name: "panics",
			act:  func() error { panic("boom") },
			wf: func(ctx wf.Context) error {
				return wf.ExecuteActivity(ctx, "A", nil, opts).Get(ctx, nil)
			},
		},
		{
			name: "bad input",
			act:  func(n int) (int, error) { return n, nil },
			wf: func(ctx wf.Context) error {
				var n int
				return wf.ExecuteActivity(ctx, "A", "x", opts).Get(ctx, &n)
			},
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			res := one(tc.act, tc.wf)
			if res.Err != nil {
				t.Fatal(res.Err)
			}
			if !res.Closed {
				t.Fatal("workflow still open")
			}
			text := strings.Join(res.Trace, "\n")
			if !strings.Contains(text, "status bad failed") {
				t.Fatalf("trace:\n%s", text)
			}
			if res.Now.Sub(simrt.Start) >= time.Hour {
				t.Fatalf("failure waited out the quiet deadline (%s)", res.Now.Sub(simrt.Start))
			}
		})
	}

	// An activity the worker does not know is a terminal failure, not an open run.
	missing := sim.Run(11, sim.Workload{
		QuietLimit: time.Hour,
		Register: func(r sim.Registry) error {
			return r.RegisterWorkflow("W", func(ctx wf.Context) error {
				return wf.ExecuteActivity(ctx, "Missing", nil, opts).Get(ctx, nil)
			})
		},
		Run: func(ctx context.Context, c *sim.Cluster) error {
			_, err := c.StartWorkflow(ctx, api.StartRequest{
				WorkflowID: "bad", WorkflowType: "W", TaskQueue: "q", RequestID: "bad",
			})
			return err
		},
	}, nil)
	if missing.Err != nil || !missing.Closed {
		t.Fatalf("unregistered activity: closed %v err %v", missing.Closed, missing.Err)
	}
	if !strings.Contains(strings.Join(missing.Trace, "\n"), "status bad failed") {
		t.Fatalf("trace:\n%s", strings.Join(missing.Trace, "\n"))
	}
}

func TestWI040SignalTerminateAndRouting(t *testing.T) {
	res := sim.Run(13, sim.Workload{
		Nodes:      3,
		Shards:     4,
		QuietLimit: time.Hour,
		Register: func(r sim.Registry) error {
			if err := r.RegisterWorkflow("wait", func(ctx wf.Context) error {
				var msg string
				wf.GetSignalChannel(ctx, "go").Receive(ctx, &msg)
				if msg != "ok" {
					return fmt.Errorf("signal %q", msg)
				}
				return nil
			}); err != nil {
				return err
			}
			return r.RegisterWorkflow("nap", func(ctx wf.Context) error {
				return wf.Sleep(ctx, 24*time.Hour)
			})
		},
		Run: func(ctx context.Context, c *sim.Cluster) error {
			if c.NumNodes() != 3 {
				return fmt.Errorf("nodes %d", c.NumNodes())
			}
			for i := 0; i < c.NumNodes(); i++ {
				for _, shard := range c.Node(i).Shards() {
					if shard%c.NumNodes() != i {
						return fmt.Errorf("node %d owns shard %d", i, shard)
					}
				}
			}
			shard := c.Node(0).ShardFor("sig")
			owner := shard % c.NumNodes()
			for i := 0; i < c.NumNodes(); i++ {
				if i == owner {
					continue
				}
				_, err := c.Node(i).StartWorkflow(ctx, api.StartRequest{
					WorkflowID: "sig", WorkflowType: "wait", TaskQueue: "q", RequestID: "nope",
				})
				if !errors.Is(err, api.ErrNotOwner) {
					return fmt.Errorf("node %d start: %v", i, err)
				}
			}
			if _, err := c.StartWorkflow(ctx, api.StartRequest{
				WorkflowID: "sig", WorkflowType: "wait", TaskQueue: "q", RequestID: "sig",
			}); err != nil {
				return err
			}
			if _, err := c.StartWorkflow(ctx, api.StartRequest{
				WorkflowID: "term", WorkflowType: "nap", TaskQueue: "q", RequestID: "term",
			}); err != nil {
				return err
			}
			nonOwner := (owner + 1) % c.NumNodes()
			if err := c.Node(nonOwner).SignalWorkflow(ctx, api.SignalRequest{
				WorkflowID: "sig", SignalName: "go", Payload: []byte(`"ok"`), RequestID: "bad",
			}); !errors.Is(err, api.ErrNotOwner) {
				return fmt.Errorf("non-owner signal: %v", err)
			}
			termNonOwner := (c.Node(0).ShardFor("term") + 1) % c.NumNodes()
			if err := c.Node(termNonOwner).TerminateWorkflow(ctx, api.TerminateRequest{WorkflowID: "term", Reason: "no"}); !errors.Is(err, api.ErrNotOwner) {
				return fmt.Errorf("non-owner terminate: %v", err)
			}
			if err := c.SignalWorkflow(ctx, api.SignalRequest{
				WorkflowID: "sig", SignalName: "go", Payload: []byte(`"ok"`), RequestID: "s1",
			}); err != nil {
				return err
			}
			return c.TerminateWorkflow(ctx, api.TerminateRequest{WorkflowID: "term", Reason: "stop"})
		},
	}, nil)
	if res.Err != nil {
		t.Fatal(res.Err)
	}
	if !res.Closed {
		t.Fatal("workflows still open")
	}
	text := strings.Join(res.Trace, "\n")
	if !strings.Contains(text, "status sig completed") {
		t.Fatalf("signal workflow:\n%s", text)
	}
	if !strings.Contains(text, "status term terminated") {
		t.Fatalf("terminated workflow:\n%s", text)
	}
	if res.Now.Sub(simrt.Start) >= 24*time.Hour {
		t.Fatalf("terminate waited for the timer (%s)", res.Now.Sub(simrt.Start))
	}
}

func TestWI040FailedStartDoesNotBlockQuietPhase(t *testing.T) {
	res := sim.Run(4, sim.Workload{
		QuietLimit: time.Minute,
		Register: func(r sim.Registry) error {
			return r.RegisterWorkflow("W", func(wf.Context) error { return nil })
		},
		Run: func(ctx context.Context, c *sim.Cluster) error {
			if _, err := c.StartWorkflow(ctx, api.StartRequest{}); err == nil {
				return errors.New("empty start succeeded")
			} else if !errors.Is(err, api.ErrInvalidArgument) {
				return fmt.Errorf("empty start: %v", err)
			}
			_, err := c.StartWorkflow(ctx, api.StartRequest{
				WorkflowID: "ok", WorkflowType: "W", TaskQueue: "q", RequestID: "ok",
			})
			return err
		},
	}, nil)
	if res.Err != nil {
		t.Fatal(res.Err)
	}
	if !res.Closed {
		t.Fatal("quiet phase waited on the rejected start")
	}
	text := strings.Join(res.Trace, "\n")
	if !strings.Contains(text, "status ok completed") {
		t.Fatalf("trace:\n%s", text)
	}
	if strings.Contains(text, "describe ") {
		t.Fatalf("rejected start left a describe failure:\n%s", text)
	}
}

func TestWI040EmptyClusterDefaults(t *testing.T) {
	res := sim.Run(9, sim.Workload{}, nil)
	if res.Err != nil {
		t.Fatal(res.Err)
	}
	if !res.Closed {
		t.Fatal("empty run not closed")
	}
	if res.Now.After(simrt.Start.Add(time.Second)) {
		t.Fatalf("empty workload ran to %s", res.Now.Sub(simrt.Start))
	}
	if !strings.Contains(strings.Join(res.Trace, "\n"), "quiet") {
		t.Fatalf("trace:\n%s", strings.Join(res.Trace, "\n"))
	}
}

func TestWI040NodesWithoutShardsStillComplete(t *testing.T) {
	res := sim.Run(8, sim.Workload{
		Nodes:      3,
		Shards:     1,
		QuietLimit: time.Hour,
		Register: func(r sim.Registry) error {
			return r.RegisterWorkflow("nap", func(ctx wf.Context) error {
				return wf.Sleep(ctx, time.Second)
			})
		},
		Run: func(ctx context.Context, c *sim.Cluster) error {
			owners := 0
			for i := 0; i < c.NumNodes(); i++ {
				owners += len(c.Node(i).Shards())
			}
			if owners != 1 {
				return fmt.Errorf("owned shards %d, want 1", owners)
			}
			_, err := c.StartWorkflow(ctx, api.StartRequest{
				WorkflowID: "one", WorkflowType: "nap", TaskQueue: "q", RequestID: "one",
			})
			return err
		},
	}, nil)
	if res.Err != nil {
		t.Fatal(res.Err)
	}
	if !res.Closed {
		t.Fatal("workflow still open")
	}
	got := res.Now.Sub(simrt.Start)
	if got < time.Second || got >= time.Hour {
		t.Fatalf("virtual time %s", got)
	}
	if !strings.Contains(strings.Join(res.Trace, "\n"), "status one completed") {
		t.Fatalf("trace:\n%s", strings.Join(res.Trace, "\n"))
	}
}

func TestWI040EmptyTaskQueueFails(t *testing.T) {
	res := sim.Run(2, sim.Workload{
		TaskQueues: []string{""},
		QuietLimit: 24 * time.Hour,
		Run:        func(context.Context, *sim.Cluster) error { return nil },
	}, nil)
	if res.Closed {
		t.Fatal("closed")
	}
	if res.Err == nil || !strings.Contains(res.Err.Error(), "poll") || !strings.Contains(res.Err.Error(), "taskQueue") {
		t.Fatalf("err %v", res.Err)
	}
	if res.Now.Sub(simrt.Start) >= 24*time.Hour {
		t.Fatalf("poll failure waited out the quiet deadline (%s)", res.Now.Sub(simrt.Start))
	}
}
