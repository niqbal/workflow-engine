package agent

// A workflow for the cluster smoke test (WI-055).
//
// The agent workflow needs a human to approve anything large, which is the wrong shape for an
// unattended script. This one needs nobody: an activity, a timer that really fires, and a
// result. That is enough to exercise what a cluster has to get right while a node is being
// killed — a task dispatched to a queue that may live elsewhere, a durable timer, and a
// completion — and it gives the script one TimerFired per run to count.

import (
	"context"
	"fmt"
	"time"

	"github.com/niqbal/workflow-engine/activity"
	"github.com/niqbal/workflow-engine/wf"
)

// Smoke workflow and activity names.
const (
	SmokeWorkflowType = "Smoke"
	SmokeActivity     = "Touch"
)

// SmokeInput is one run's input.
type SmokeInput struct {
	Ref   string        `json:"ref"`
	Sleep time.Duration `json:"sleep"`
}

// Smoke runs one activity, waits on a durable timer, and returns what it touched.
func Smoke(ctx wf.Context, in SmokeInput) (string, error) {
	var touched string
	opts := wf.ActivityOptions{
		StartToCloseTimeout: 30 * time.Second,
		RetryPolicy:         wf.RetryPolicy{InitialInterval: time.Second, MaximumInterval: 10 * time.Second},
	}
	if err := wf.ExecuteActivity(ctx, SmokeActivity, in, opts).Get(ctx, &touched); err != nil {
		return "", err
	}
	sleep := in.Sleep
	if sleep <= 0 {
		sleep = 2 * time.Second
	}
	if err := wf.Sleep(ctx, sleep); err != nil {
		return "", err
	}
	return touched, nil
}

// RegisterSmoke binds the smoke workflow and its activity. The activity records which node's
// worker ran it, which makes a run's history show the work moving around a cluster.
func RegisterSmoke(r Registry) error {
	if err := r.RegisterWorkflow(SmokeWorkflowType, Smoke); err != nil {
		return err
	}
	return r.RegisterActivity(SmokeActivity, func(ctx context.Context, in SmokeInput) (string, error) {
		info := activity.GetInfo(ctx)
		return fmt.Sprintf("%s@%s", in.Ref, info.IdempotencyKey), nil
	})
}
