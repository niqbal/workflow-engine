package worker_test

import (
	"context"
	"encoding/json"
	"path/filepath"
	"reflect"
	"runtime"
	"sync/atomic"
	"testing"
	"time"

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

type wi024Request struct{ Ticket string }
type wi024Draft struct{ Text string }
type wi024Decision struct {
	Approved bool
	Reason   string
}
type wi024Result struct{ Status string }

var wi024Opts = wf.ActivityOptions{StartToCloseTimeout: time.Minute}

func wi024Approval(ctx wf.Context, req wi024Request) (wi024Result, error) {
	var draft wi024Draft
	if err := wf.ExecuteActivity(ctx, "DraftReply", req, wi024Opts).Get(ctx, &draft); err != nil {
		return wi024Result{}, err
	}
	sel := wf.NewSelector(ctx)
	var decision wi024Decision
	sel.AddReceive(wf.GetSignalChannel(ctx, "approval"), func(c wf.ReceiveChannel) { c.Receive(ctx, &decision) })
	sel.AddFuture(wf.NewTimer(ctx, 72*time.Hour), func(wf.Future) {
		decision = wi024Decision{Approved: false, Reason: "timeout"}
	})
	sel.Select(ctx)
	if !decision.Approved {
		return wi024Result{Status: "rejected:" + decision.Reason}, nil
	}
	return wi024Result{Status: "sent"}, wf.ExecuteActivity(ctx, "SendReply", draft, wi024Opts).Get(ctx, nil)
}

func TestTesterWI024_ExampleAndGoldenHistories(t *testing.T) {
	tests := []struct {
		name       string
		decision   *wi024Decision
		advance    time.Duration
		wantStatus string
		wantSends  int64
		goldenFile string
	}{
		{name: "approved", decision: &wi024Decision{Approved: true}, wantStatus: "sent", wantSends: 1, goldenFile: "approval-approved.json"},
		{name: "rejected", decision: &wi024Decision{Approved: false, Reason: "no"}, wantStatus: "rejected:no", wantSends: 0, goldenFile: "approval-rejected.json"},
		{name: "timeout", advance: 72 * time.Hour, wantStatus: "rejected:timeout", wantSends: 0, goldenFile: "approval-timeout.json"},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			start := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
			clk := clock.NewManual(start)
			rt := &xsync.Real{Ctx: context.Background(), C: clk}
			s := server.New(rt, memstore.New(memstore.WithRuntime(rt)), rnd.New(2026), server.Config{NumShards: 2})
			if err := s.Start(context.Background()); err != nil {
				t.Fatal(err)
			}
			if err := s.Run(context.Background()); err != nil {
				t.Fatal(err)
			}

			var sendCalls atomic.Int64
			w := worker.New(s.Service(), worker.Options{TaskQueue: "q", Identity: "wftest"})
			if err := w.RegisterWorkflow("W", wi024Approval); err != nil {
				t.Fatal(err)
			}
			if err := w.RegisterActivity("DraftReply", func(in wi024Request) (wi024Draft, error) {
				return wi024Draft{Text: "re: " + in.Ticket}, nil
			}); err != nil {
				t.Fatal(err)
			}
			if err := w.RegisterActivity("SendReply", func(context.Context, wi024Draft) error {
				sendCalls.Add(1)
				return nil
			}); err != nil {
				t.Fatal(err)
			}

			wctx, cancelWorker := context.WithCancel(context.Background())
			workerErr := make(chan error, 1)
			go func() { workerErr <- w.Run(wctx) }()
			t.Cleanup(func() {
				cancelWorker()
				if err := <-workerErr; err != nil {
					t.Errorf("worker run: %v", err)
				}
				if err := s.Stop(context.Background()); err != nil {
					t.Errorf("stop server loops: %v", err)
				}
				rt.Wait()
			})

			in, err := json.Marshal(wi024Request{Ticket: "T-1"})
			if err != nil {
				t.Fatal(err)
			}
			workflowID := "wi024-" + tc.name
			if _, err := s.StartWorkflow(context.Background(), api.StartRequest{
				WorkflowID:   workflowID,
				WorkflowType: "W",
				TaskQueue:    "q",
				Input:        in,
			}); err != nil {
				t.Fatal(err)
			}

			wi024WaitForApprovalPoint(t, s.Service(), workflowID)
			if tc.decision != nil {
				payload, err := json.Marshal(tc.decision)
				if err != nil {
					t.Fatal(err)
				}
				if err := s.SignalWorkflow(context.Background(), api.SignalRequest{
					WorkflowID: workflowID,
					SignalName: "approval",
					Payload:    payload,
				}); err != nil {
					t.Fatal(err)
				}
			} else {
				clk.Advance(tc.advance)
			}

			live := wi024WaitClosedHistory(t, s.Service(), workflowID)
			gotStatus, err := wi024CompletedStatus(live)
			if err != nil {
				t.Fatal(err)
			}
			if gotStatus != tc.wantStatus {
				t.Fatalf("status = %q, want %q", gotStatus, tc.wantStatus)
			}
			if got := sendCalls.Load(); got != tc.wantSends {
				t.Fatalf("SendReply calls = %d, want %d", got, tc.wantSends)
			}

			goldenPath := filepath.Join("..", "wf", "testdata", tc.goldenFile)
			golden, err := wftest.LoadHistory(goldenPath)
			if err != nil {
				t.Fatal(err)
			}
			if err := wftest.ReplayFromFile(goldenPath, wi024Approval); err != nil {
				t.Fatalf("golden replay failed: %v", err)
			}
			if err := wftest.Replay(live, wi024Approval); err != nil {
				t.Fatalf("live history replay failed: %v", err)
			}
			if gotTypes, wantTypes := wi024EventTypes(live), wi024EventTypes(golden); !reflect.DeepEqual(gotTypes, wantTypes) {
				t.Fatalf("event type sequence mismatch for %s\n got: %v\nwant: %v", tc.goldenFile, gotTypes, wantTypes)
			}
		})
	}
}

func wi024WaitForApprovalPoint(t *testing.T, svc api.Service, workflowID string) {
	t.Helper()
	for i := 0; i < 100000; i++ {
		d, err := svc.DescribeWorkflow(context.Background(), workflowID)
		if err == nil && d.Status == history.StatusRunning && len(d.PendingTimers) == 1 && len(d.PendingActivities) == 0 {
			return
		}
		runtime.Gosched()
	}
	t.Fatalf("workflow %s did not reach approval wait point", workflowID)
}

func wi024WaitClosedHistory(t *testing.T, svc api.Service, workflowID string) []history.Event {
	t.Helper()
	for i := 0; i < 100000; i++ {
		evs, err := svc.GetHistory(context.Background(), workflowID)
		if err == nil && len(evs) > 0 && history.IsClose(evs[len(evs)-1].Type()) {
			return evs
		}
		runtime.Gosched()
	}
	t.Fatalf("workflow %s did not close", workflowID)
	return nil
}

func wi024CompletedStatus(evs []history.Event) (string, error) {
	last := evs[len(evs)-1]
	attrs, ok := last.Attrs.(history.WorkflowExecutionCompleted)
	if !ok {
		return "", &json.UnmarshalTypeError{Value: string(last.Type()), Type: reflect.TypeOf(history.WorkflowExecutionCompleted{})}
	}
	var out wi024Result
	if err := json.Unmarshal(attrs.Result, &out); err != nil {
		return "", err
	}
	return out.Status, nil
}

func wi024EventTypes(evs []history.Event) []history.EventType {
	out := make([]history.EventType, 0, len(evs))
	for _, ev := range evs {
		out = append(out, ev.Type())
	}
	return out
}
