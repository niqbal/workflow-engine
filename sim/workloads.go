package sim

// The workload mix (WI-043, SPEC §11.4) and the fault set a sweep runs them under.
//
// Each workload is a named constructor so the CLI can run it by name and a test can run the
// whole set. They are written to be checkable rather than realistic: every one of them has a
// reference model derived from its own final history (I8), and the ones with external effects
// apply them through the ledger (I5).

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"time"

	"github.com/niqbal/workflow-engine/activity"
	"github.com/niqbal/workflow-engine/internal/history"
	"github.com/niqbal/workflow-engine/internal/server/api"
	"github.com/niqbal/workflow-engine/wf"
)

// Standard is the fault set a sweep injects: everything, at rates a cluster is expected to
// survive. The crash and pause intervals are short relative to the shard lease on purpose,
// so ownership changes hands often.
func Standard() *Faults {
	return &Faults{
		CrashEvery:      30 * time.Second,
		FreezeEvery:     40 * time.Second,
		StoreErrorRate:  0.05,
		AmbiguousRate:   0.02,
		CommitDelayMax:  time.Second,
		ClockSkewMax:    250 * time.Millisecond,
		WorkerCrashRate: 0.1,
	}
}

// A Builder makes one workload, with its own ledger.
type Builder func() Workload

// Workloads returns the mix by name.
func Workloads() map[string]Builder {
	return map[string]Builder{
		"basic":   Basic,
		"retries": Retries,
		"timers":  Timers,
		"fanout":  Fanout,
		"signals": Signals,
	}
}

// WorkloadNames lists the mix in a stable order.
func WorkloadNames() []string {
	names := make([]string, 0, len(Workloads()))
	for name := range Workloads() {
		names = append(names, name)
	}
	sort.Strings(names)
	return names
}

// base is the cluster shape every workload in the mix uses, with a long poll a real cluster
// would run rather than the harness default (docs/SIM-BUGS.md, 2026-11-25).
func base() Workload {
	return Workload{
		Nodes:      3,
		Shards:     4,
		Workers:    2,
		QuietLimit: 7 * 24 * time.Hour,
		LongPoll:   30 * time.Second,
	}
}

var shortActivity = wf.ActivityOptions{StartToCloseTimeout: 30 * time.Second}

// Basic is an activity, a timer and a signal, with the effect behind an idempotency key.
func Basic() Workload {
	led := NewLedger()
	wl := base()
	wl.Ledger = led
	wl.Register = func(r Registry) error {
		if err := r.RegisterWorkflow("basic", func(ctx wf.Context) (string, error) {
			if err := wf.ExecuteActivity(ctx, "Effect", 1, shortActivity).Get(ctx, nil); err != nil {
				return "", err
			}
			if err := wf.Sleep(ctx, 10*time.Minute); err != nil {
				return "", err
			}
			var decision string
			wf.GetSignalChannel(ctx, "go").Receive(ctx, &decision)
			return decision, nil
		}); err != nil {
			return err
		}
		return r.RegisterActivity("Effect", func(ctx context.Context, _ int) error {
			led.Apply(activity.GetInfo(ctx).IdempotencyKey, nil)
			return nil
		})
	}
	wl.Run = func(ctx context.Context, c *Cluster) error {
		ids, err := startN(ctx, c, "basic", 3)
		if err != nil {
			return err
		}
		c.Sleep(30 * time.Minute)
		for _, id := range ids {
			if err := c.SignalRetrying(ctx, api.SignalRequest{WorkflowID: id, SignalName: "go",
				Payload: json.RawMessage(`"done"`), RequestID: "sig-" + id}); err != nil {
				return err
			}
		}
		return nil
	}
	wl.Reference = signalPayloadReference("go")
	return wl
}

// Retries is an activity that fails twice before applying its effect, so every run exercises
// the retry policy and the stable idempotency key under it.
func Retries() Workload {
	led := NewLedger()
	wl := base()
	wl.Ledger = led
	wl.Register = func(r Registry) error {
		if err := r.RegisterWorkflow("retries", func(ctx wf.Context) (string, error) {
			var out string
			if err := wf.ExecuteActivity(ctx, "Flaky", 1, shortActivity).Get(ctx, &out); err != nil {
				return "", err
			}
			return out, nil
		}); err != nil {
			return err
		}
		return r.RegisterActivity("Flaky", func(ctx context.Context, _ int) (string, error) {
			info := activity.GetInfo(ctx)
			if info.Attempt < 3 {
				led.Attempt(info.IdempotencyKey)
				return "", fmt.Errorf("sink unavailable (attempt %d)", info.Attempt)
			}
			led.Apply(info.IdempotencyKey, nil)
			return "applied", nil
		})
	}
	wl.Run = func(ctx context.Context, c *Cluster) error {
		_, err := startN(ctx, c, "retries", 3)
		return err
	}
	wl.Reference = func(string, []history.Event) (json.RawMessage, error) {
		return json.RawMessage(`"applied"`), nil
	}
	return wl
}

// Timers is the long-timer case: a month of virtual time per run, which is what makes the
// durable-timer claim (G5) cost nothing to check.
func Timers() Workload {
	wl := base()
	wl.QuietLimit = 40 * 24 * time.Hour
	// A month of virtual time with a 30-second poll would wake every worker 86,000 times.
	// An hour-long poll keeps the run cheap without reaching the deadline that starves the
	// clock (docs/SIM-BUGS.md, 2026-11-25).
	wl.LongPoll = time.Hour
	wl.Register = func(r Registry) error {
		return r.RegisterWorkflow("timers", func(ctx wf.Context) (string, error) {
			if err := wf.Sleep(ctx, 30*24*time.Hour); err != nil {
				return "", err
			}
			if err := wf.Sleep(ctx, time.Hour); err != nil {
				return "", err
			}
			return "elapsed", nil
		})
	}
	wl.Run = func(ctx context.Context, c *Cluster) error {
		_, err := startN(ctx, c, "timers", 3)
		return err
	}
	wl.Reference = func(string, []history.Event) (json.RawMessage, error) {
		return json.RawMessage(`"elapsed"`), nil
	}
	return wl
}

// Fanout runs activities in parallel and joins them, so a workflow task carries several
// commands and the replay has to reproduce their order exactly.
func Fanout() Workload {
	led := NewLedger()
	wl := base()
	wl.Ledger = led
	wl.Register = func(r Registry) error {
		if err := r.RegisterWorkflow("fanout", func(ctx wf.Context) (int, error) {
			futures := make([]wf.Future, 0, 4)
			for i := 0; i < 4; i++ {
				futures = append(futures, wf.ExecuteActivity(ctx, "Add", i, shortActivity))
			}
			total := 0
			for _, f := range futures {
				var n int
				if err := f.Get(ctx, &n); err != nil {
					return 0, err
				}
				total += n
			}
			return total, nil
		}); err != nil {
			return err
		}
		return r.RegisterActivity("Add", func(ctx context.Context, n int) (int, error) {
			led.Apply(activity.GetInfo(ctx).IdempotencyKey, nil)
			return n + 1, nil
		})
	}
	wl.Run = func(ctx context.Context, c *Cluster) error {
		_, err := startN(ctx, c, "fanout", 2)
		return err
	}
	wl.Reference = func(string, []history.Event) (json.RawMessage, error) {
		return json.RawMessage(`10`), nil // (0+1) + (1+1) + (2+1) + (3+1)
	}
	return wl
}

// Signals covers the signal paths without racing itself: "await" always ends on a signal
// that arrives long before its fallback timer, and "lapse" has no signal at all and always
// ends on a short timer. Each run also gets a duplicate signal, which must be deduplicated,
// and a signal after it has closed, which must be rejected rather than dropped (G7).
//
// The signal-versus-timer race is deliberately not here: which side wins is readable from the
// history, but modelling it means modelling how Select consumes a replayed history, and that
// belongs with the selector's own work item rather than in a workload.
func Signals() Workload {
	wl := base()
	wl.Register = func(r Registry) error {
		if err := r.RegisterWorkflow("await", func(ctx wf.Context) (string, error) {
			sel := wf.NewSelector(ctx)
			var decision string
			sel.AddReceive(wf.GetSignalChannel(ctx, "go"), func(c wf.ReceiveChannel) {
				c.Receive(ctx, &decision)
			})
			sel.AddFuture(wf.NewTimer(ctx, 12*time.Hour), func(wf.Future) { decision = "timeout" })
			sel.Select(ctx)
			return decision, nil
		}); err != nil {
			return err
		}
		return r.RegisterWorkflow("lapse", func(ctx wf.Context) (string, error) {
			if err := wf.Sleep(ctx, 10*time.Minute); err != nil {
				return "", err
			}
			return "timeout", nil
		})
	}
	wl.Run = func(ctx context.Context, c *Cluster) error {
		await, err := startN(ctx, c, "await", 2)
		if err != nil {
			return err
		}
		if _, err := startN(ctx, c, "lapse", 2); err != nil {
			return err
		}
		c.Sleep(20 * time.Minute)
		for _, id := range await {
			req := api.SignalRequest{WorkflowID: id, SignalName: "go",
				Payload: json.RawMessage(`"signaled"`), RequestID: "sig-" + id}
			if err := c.SignalRetrying(ctx, req); err != nil {
				return err
			}
			// The same request ID again: G7 makes it one event in history, not two.
			if err := c.SignalRetrying(ctx, req); err != nil {
				return err
			}
		}
		// Long enough for the lapse runs to have closed, so these are rejected for cause.
		c.Sleep(2 * time.Hour)
		for i := 0; i < 2; i++ {
			err := c.SignalWorkflow(ctx, api.SignalRequest{WorkflowID: fmt.Sprintf("lapse-%d", i),
				SignalName: "go", Payload: json.RawMessage(`"late"`), RequestID: "late-lapse"})
			if err != nil && !errors.Is(err, api.ErrWorkflowClosed) {
				return err
			}
		}
		return nil
	}
	// Each workflow type has one possible result, so the model is the type itself.
	wl.Reference = func(id string, evs []history.Event) (json.RawMessage, error) {
		for _, e := range evs {
			if a, ok := e.Attrs.(history.WorkflowExecutionStarted); ok {
				switch a.WorkflowType {
				case "await":
					return json.RawMessage(`"signaled"`), nil
				case "lapse":
					return json.RawMessage(`"timeout"`), nil
				}
			}
		}
		return nil, fmt.Errorf("history has no start event")
	}
	return wl
}

// ---- helpers ---------------------------------------------------------------------------

// startN starts n workflows of a type, retrying transient failures, and returns their IDs.
func startN(ctx context.Context, c *Cluster, typ string, n int) ([]string, error) {
	ids := make([]string, 0, n)
	for i := 0; i < n; i++ {
		id := fmt.Sprintf("%s-%d", typ, i)
		if _, err := c.StartRetrying(ctx, api.StartRequest{WorkflowID: id, WorkflowType: typ,
			TaskQueue: "q", RequestID: id}); err != nil {
			return ids, err
		}
		ids = append(ids, id)
	}
	return ids, nil
}

// signalPayloadReference derives the result from the first signal of that name in history.
func signalPayloadReference(name string) func(string, []history.Event) (json.RawMessage, error) {
	return func(_ string, evs []history.Event) (json.RawMessage, error) {
		for _, e := range evs {
			if a, ok := e.Attrs.(history.WorkflowExecutionSignaled); ok && a.SignalName == name {
				return a.Payload, nil
			}
		}
		return nil, fmt.Errorf("no %q signal in history", name)
	}
}
