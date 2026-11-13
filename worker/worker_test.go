package worker_test

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"path/filepath"
	"runtime"
	"sync/atomic"
	"testing"
	"time"

	"github.com/niqbal/workflow-engine/activity"
	"github.com/niqbal/workflow-engine/internal/clock"
	"github.com/niqbal/workflow-engine/internal/history"
	"github.com/niqbal/workflow-engine/internal/rnd"
	"github.com/niqbal/workflow-engine/internal/server"
	"github.com/niqbal/workflow-engine/internal/server/api"
	"github.com/niqbal/workflow-engine/internal/store/memstore"
	"github.com/niqbal/workflow-engine/internal/xsync"
	"github.com/niqbal/workflow-engine/wf"
	"github.com/niqbal/workflow-engine/wftest"
	"github.com/niqbal/workflow-engine/worker"
)

type Request struct{ Ticket string }
type Draft struct{ Text string }
type Decision struct {
	Approved bool
	Reason   string
}
type Result struct{ Status string }

var opts = wf.ActivityOptions{StartToCloseTimeout: time.Minute}

// -update rewrites wf/testdata/* from live in-process server runs in this package.
var update = flag.Bool("update", false, "rewrite wf/testdata approval histories from worker end-to-end runs")

// Approval is the SPEC §3.4 example.
func Approval(ctx wf.Context, req Request) (Result, error) {
	var draft Draft
	if err := wf.ExecuteActivity(ctx, "DraftReply", req, opts).Get(ctx, &draft); err != nil {
		return Result{}, err
	}
	sel := wf.NewSelector(ctx)
	var decision Decision
	sel.AddReceive(wf.GetSignalChannel(ctx, "approval"), func(c wf.ReceiveChannel) { c.Receive(ctx, &decision) })
	sel.AddFuture(wf.NewTimer(ctx, 72*time.Hour), func(wf.Future) { decision = Decision{Approved: false, Reason: "timeout"} })
	sel.Select(ctx)
	if !decision.Approved {
		return Result{Status: "rejected:" + decision.Reason}, nil
	}
	return Result{Status: "sent"}, wf.ExecuteActivity(ctx, "SendReply", draft, opts).Get(ctx, nil)
}

func TestApprovalExampleEndToEndAgainstInProcessServer(t *testing.T) {
	tests := []struct {
		name       string
		decision   *Decision
		advance    time.Duration
		wantStatus string
		wantSends  int64
		golden     string
	}{
		{name: "approved", decision: &Decision{Approved: true}, wantStatus: "sent", wantSends: 1, golden: "approval-approved.json"},
		{name: "rejected", decision: &Decision{Approved: false, Reason: "no"}, wantStatus: "rejected:no", wantSends: 0, golden: "approval-rejected.json"},
		{name: "timed_out", advance: 72 * time.Hour, wantStatus: "rejected:timeout", wantSends: 0, golden: "approval-timeout.json"},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			start := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
			clk := clock.NewManual(start)
			rt := &xsync.Real{Ctx: context.Background(), C: clk}
			s := server.New(rt, memstore.New(memstore.WithRuntime(rt)), rnd.New(7), server.Config{NumShards: 2})
			if err := s.Start(context.Background()); err != nil {
				t.Fatal(err)
			}
			if err := s.Run(context.Background()); err != nil {
				t.Fatal(err)
			}

			var sendCalls atomic.Int64
			w := worker.New(s.Service(), worker.Options{TaskQueue: "q", Identity: "wftest"})
			if err := w.RegisterWorkflow("W", Approval); err != nil {
				t.Fatal(err)
			}
			if err := w.RegisterActivity("DraftReply", func(in Request) (Draft, error) {
				return Draft{Text: "re: " + in.Ticket}, nil
			}); err != nil {
				t.Fatal(err)
			}
			if err := w.RegisterActivity("SendReply", func(context.Context, Draft) error {
				sendCalls.Add(1)
				return nil
			}); err != nil {
				t.Fatal(err)
			}

			wctx, cancel := context.WithCancel(context.Background())
			errCh := make(chan error, 1)
			go func() { errCh <- w.Run(wctx) }()
			t.Cleanup(func() {
				cancel()
				if err := <-errCh; err != nil {
					t.Errorf("worker run: %v", err)
				}
				if err := s.Stop(context.Background()); err != nil {
					t.Errorf("stop server loops: %v", err)
				}
				rt.Wait()
			})

			in, _ := json.Marshal(Request{Ticket: "T-1"})
			id := "wf-" + tc.name
			if _, err := s.StartWorkflow(context.Background(), api.StartRequest{
				WorkflowID:   id,
				WorkflowType: "W",
				TaskQueue:    "q",
				Input:        in,
			}); err != nil {
				t.Fatal(err)
			}

			waitForApprovalPoint(t, s, id)
			if tc.decision != nil {
				payload, _ := json.Marshal(tc.decision)
				if err := s.SignalWorkflow(context.Background(), api.SignalRequest{
					WorkflowID: id,
					SignalName: "approval",
					Payload:    payload,
				}); err != nil {
					t.Fatal(err)
				}
			} else {
				clk.Advance(tc.advance)
			}

			evs := waitClosedHistory(t, s, id)
			got, err := completedStatus(evs)
			if err != nil {
				t.Fatal(err)
			}
			if got != tc.wantStatus {
				t.Fatalf("status %q, want %q", got, tc.wantStatus)
			}
			if calls := sendCalls.Load(); calls != tc.wantSends {
				t.Fatalf("SendReply calls %d, want %d", calls, tc.wantSends)
			}

			goldenPath := filepath.Join("..", "wf", "testdata", tc.golden)
			if *update {
				if err := wftest.SaveHistory(goldenPath, evs); err != nil {
					t.Fatalf("save golden %s: %v", tc.golden, err)
				}
			}
			gold, err := wftest.LoadHistory(goldenPath)
			if err != nil {
				t.Fatal(err)
			}
			if err := wftest.ReplayFromFile(goldenPath, Approval); err != nil {
				t.Fatalf("golden replay %s: %v", tc.golden, err)
			}
			if err := wftest.Replay(evs, Approval); err != nil {
				t.Fatalf("live history replay: %v", err)
			}
			if got, want := canonicalHistoryJSON(t, evs), canonicalHistoryJSON(t, gold); got != want {
				t.Fatalf("live history does not match golden %s\nlive:   %s\nwant:   %s", tc.golden, got, want)
			}
		})
	}
}

func TestWorkerPassesActivityDeadlineToContext(t *testing.T) {
	start := time.Now().UTC().Add(24 * time.Hour).Truncate(time.Second)
	clk := clock.NewManual(start)
	rt := &xsync.Real{Ctx: context.Background(), C: clk}
	s := server.New(rt, memstore.New(memstore.WithRuntime(rt)), rnd.New(9), server.Config{NumShards: 2})
	if err := s.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	if err := s.Run(context.Background()); err != nil {
		t.Fatal(err)
	}

	deadlineWF := func(ctx wf.Context) error {
		opts := wf.ActivityOptions{StartToCloseTimeout: 15 * time.Second, RetryPolicy: wf.RetryPolicy{MaximumAttempts: 1}}
		return wf.ExecuteActivity(ctx, "ObserveDeadline", nil, opts).Get(ctx, nil)
	}

	seen := make(chan struct {
		ctxDeadline  time.Time
		infoDeadline time.Time
	}, 1)

	w := worker.New(s.Service(), worker.Options{TaskQueue: "q", Identity: "wftest"})
	if err := w.RegisterWorkflow("DeadlineWF", deadlineWF); err != nil {
		t.Fatal(err)
	}
	if err := w.RegisterActivity("ObserveDeadline", func(ctx context.Context) error {
		dl, ok := ctx.Deadline()
		if !ok {
			return errors.New("context deadline missing")
		}
		info := activity.GetInfo(ctx)
		if info.Deadline.IsZero() {
			return errors.New("activity info deadline missing")
		}
		if !info.Deadline.Equal(dl) {
			return fmt.Errorf("deadline mismatch: ctx=%v info=%v", dl, info.Deadline)
		}
		seen <- struct {
			ctxDeadline  time.Time
			infoDeadline time.Time
		}{ctxDeadline: dl, infoDeadline: info.Deadline}
		return nil
	}); err != nil {
		t.Fatal(err)
	}

	wctx, cancel := context.WithCancel(context.Background())
	errCh := make(chan error, 1)
	go func() { errCh <- w.Run(wctx) }()
	t.Cleanup(func() {
		cancel()
		if err := <-errCh; err != nil {
			t.Errorf("worker run: %v", err)
		}
		if err := s.Stop(context.Background()); err != nil {
			t.Errorf("stop server loops: %v", err)
		}
		rt.Wait()
	})

	if _, err := s.StartWorkflow(context.Background(), api.StartRequest{
		WorkflowID:   "wf-deadline",
		WorkflowType: "DeadlineWF",
		TaskQueue:    "q",
	}); err != nil {
		t.Fatal(err)
	}

	evs := waitClosedHistory(t, s, "wf-deadline")
	if evs[len(evs)-1].Type() != history.EventWorkflowExecutionCompleted {
		t.Fatalf("last event %s, want %s", evs[len(evs)-1].Type(), history.EventWorkflowExecutionCompleted)
	}
	select {
	case got := <-seen:
		if !got.ctxDeadline.Equal(got.infoDeadline) {
			t.Fatalf("deadline mismatch: ctx=%v info=%v", got.ctxDeadline, got.infoDeadline)
		}
	default:
		t.Fatal("activity did not report deadline")
	}
}

func waitForApprovalPoint(t *testing.T, svc api.Service, id string) {
	t.Helper()
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		d, err := svc.DescribeWorkflow(context.Background(), id)
		if err == nil && len(d.PendingTimers) == 1 && len(d.PendingActivities) == 0 && d.Status == history.StatusRunning {
			return
		}
		runtime.Gosched()
	}
	t.Fatalf("workflow %s did not reach approval wait", id)
}

func waitClosedHistory(t *testing.T, svc api.Service, id string) []history.Event {
	t.Helper()
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		evs, err := svc.GetHistory(context.Background(), id)
		if err == nil && len(evs) > 0 && history.IsClose(evs[len(evs)-1].Type()) {
			return evs
		}
		runtime.Gosched()
	}
	t.Fatalf("workflow %s did not close", id)
	return nil
}

func completedStatus(evs []history.Event) (string, error) {
	last := evs[len(evs)-1]
	a, ok := last.Attrs.(history.WorkflowExecutionCompleted)
	if !ok {
		return "", fmt.Errorf("last event %s, want WorkflowExecutionCompleted", last.Type())
	}
	var r Result
	if err := json.Unmarshal(a.Result, &r); err != nil {
		return "", err
	}
	return r.Status, nil
}

func canonicalHistoryJSON(t *testing.T, evs []history.Event) string {
	t.Helper()
	b, err := json.Marshal(evs)
	if err != nil {
		t.Fatal(err)
	}
	var v any
	if err := json.Unmarshal(b, &v); err != nil {
		t.Fatal(err)
	}
	b, err = json.Marshal(v)
	if err != nil {
		t.Fatal(err)
	}
	return string(b)
}
