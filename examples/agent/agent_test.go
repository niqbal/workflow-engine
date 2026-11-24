package agent_test

// WI-070's acceptance test and the rest of the agent's behaviour, all against a real
// in-process server with a manual clock: the same engine the demo runs, with time under the
// test's control so a 24-hour approval window costs nothing.

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"

	"github.com/niqbal/workflow-engine/examples/agent"
	"github.com/niqbal/workflow-engine/internal/clock"
	"github.com/niqbal/workflow-engine/internal/history"
	"github.com/niqbal/workflow-engine/internal/rnd"
	"github.com/niqbal/workflow-engine/internal/server"
	"github.com/niqbal/workflow-engine/internal/server/api"
	"github.com/niqbal/workflow-engine/internal/store/memstore"
	"github.com/niqbal/workflow-engine/internal/xsync"
	"github.com/niqbal/workflow-engine/wftest"
	"github.com/niqbal/workflow-engine/worker"
)

// stack is a server with a manual clock and a worker whose lifetime the test controls, so it
// can stop one worker mid-activity and start another.
type stack struct {
	t    *testing.T
	s    *server.Server
	clk  *clock.Manual
	acts agent.Activities
}

func newStack(t *testing.T, acts agent.Activities) *stack {
	t.Helper()
	clk := clock.NewManual(time.Date(2026, 3, 1, 9, 0, 0, 0, time.UTC))
	rt := &xsync.Real{Ctx: context.Background(), C: clk}
	s := server.New(rt, memstore.New(memstore.WithRuntime(rt)), rnd.New(4),
		server.Config{NumShards: 2, LongPoll: 50 * time.Millisecond})
	if err := s.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	if err := s.Run(context.Background()); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := s.Stop(context.Background()); err != nil {
			t.Errorf("stop server: %v", err)
		}
	})
	return &stack{t: t, s: s, clk: clk, acts: acts}
}

// startWorker runs a worker until the returned stop function is called. Calling stop is how
// the tests kill one: its in-flight activity never reports.
func (st *stack) startWorker(identity string) (stop func()) {
	st.t.Helper()
	w := worker.New(st.s.Service(), worker.Options{TaskQueue: "q", Identity: identity})
	if err := st.acts.Register(w); err != nil {
		st.t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- w.Run(ctx) }()
	stopped := false
	return func() {
		if stopped {
			return
		}
		stopped = true
		cancel()
		if err := <-done; err != nil && !errors.Is(err, context.Canceled) {
			st.t.Errorf("worker %s: %v", identity, err)
		}
	}
}

func (st *stack) start(id string, ticket agent.Ticket) {
	st.t.Helper()
	in, err := json.Marshal(ticket)
	if err != nil {
		st.t.Fatal(err)
	}
	if _, err := st.s.StartWorkflow(context.Background(), api.StartRequest{
		WorkflowID: id, WorkflowType: agent.WorkflowType, TaskQueue: "q",
		Input: in, RequestID: id,
	}); err != nil {
		st.t.Fatal(err)
	}
}

func (st *stack) signal(id string, a agent.Approval) {
	st.t.Helper()
	payload, err := json.Marshal(a)
	if err != nil {
		st.t.Fatal(err)
	}
	if err := st.s.SignalWorkflow(context.Background(), api.SignalRequest{
		WorkflowID: id, SignalName: agent.ApprovalSignal, Payload: payload,
		RequestID: "approval-" + id,
	}); err != nil {
		st.t.Fatal(err)
	}
}

// waitFor polls until cond holds, advancing nothing: it is for progress the cluster makes on
// its own. The manual clock means a hung test fails here rather than hanging forever.
func (st *stack) waitFor(what string, cond func() bool) {
	st.t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(time.Millisecond)
	}
	st.t.Fatalf("timed out waiting for %s", what)
}

func (st *stack) history(id string) []history.Event {
	st.t.Helper()
	evs, err := st.s.GetHistory(context.Background(), id)
	if err != nil {
		st.t.Fatal(err)
	}
	return evs
}

func (st *stack) closed(id string) bool {
	d, err := st.s.DescribeWorkflow(context.Background(), id)
	return err == nil && !d.Status.Open()
}

func (st *stack) result(id string) agent.Result {
	st.t.Helper()
	var out agent.Result
	for _, e := range st.history(id) {
		if a, ok := e.Attrs.(history.WorkflowExecutionCompleted); ok {
			if err := json.Unmarshal(a.Result, &out); err != nil {
				st.t.Fatalf("decode result: %v", err)
			}
			return out
		}
	}
	st.t.Fatalf("workflow %s did not complete: %s", id, types(st.history(id)))
	return out
}

func types(evs []history.Event) string {
	var out []string
	for _, e := range evs {
		out = append(out, string(e.Type()))
	}
	return fmt.Sprint(out)
}

func countType(evs []history.Event, want history.EventType) int {
	n := 0
	for _, e := range evs {
		if e.Type() == want {
			n++
		}
	}
	return n
}

// ---- WI-070's acceptance test -------------------------------------------------------------

// TestWorkerKilledAfterTheRefundLeavesExactlyOneRefund is the claim the example exists for.
//
// The first worker applies the refund to the ledger and is then killed before it can report
// it, which is the one window where an at-least-once activity could double-charge a customer.
// A second worker picks the attempt up after it times out, the stable idempotency key makes
// its refund a no-op, and the run finishes with one payment in the ledger.
func TestWorkerKilledAfterTheRefundLeavesExactlyOneRefund(t *testing.T) {
	payments := agent.NewMemSink()
	mail := agent.NewMemSink()
	var attempts atomic.Int64
	killed := make(chan string, 1)

	acts := agent.Activities{
		Model: agent.Scripted{}, Orders: agent.DemoOrders(),
		Payments: payments, Mail: mail,
		AfterRefund: func(ctx context.Context, key string) error {
			// First attempt only: the money has moved, and this worker is about to die.
			if attempts.Add(1) == 1 {
				killed <- key
				<-ctx.Done() // the attempt never reports
				return ctx.Err()
			}
			return nil
		},
	}
	st := newStack(t, acts)

	stopFirst := st.startWorker("worker-1")
	st.start("ticket-1", agent.Ticket{ID: "ticket-1", Customer: "Ada", OrderID: "A-1001",
		Text: "the mug arrived cracked"})

	// Wait until the refund has been applied and the worker is in the window.
	var key string
	select {
	case key = <-killed:
	case <-time.After(10 * time.Second):
		t.Fatal("the refund activity never reached the kill window")
	}
	if payments.Len() != 1 {
		t.Fatalf("ledger has %d payments at the kill point, want 1", payments.Len())
	}

	// Kill it. The attempt is gone without ever reporting.
	stopFirst()

	// The attempt times out on the server's clock, which the test owns.
	st.clk.Advance(2 * time.Minute)

	// A new worker takes over and finishes the run.
	stopSecond := st.startWorker("worker-2")
	defer stopSecond()
	st.waitFor("the run to finish after the restart", func() bool {
		st.clk.Advance(time.Second) // let retries and timeouts come due
		return st.closed("ticket-1")
	})

	res := st.result("ticket-1")
	if res.Status != "replied" {
		t.Fatalf("status %q, want replied", res.Status)
	}
	if res.RefundedCents != 2400 {
		t.Fatalf("refunded %d, want 2400", res.RefundedCents)
	}

	// The claim, in one assertion: the refund ran more than once and was applied once.
	if got := attempts.Load(); got < 2 {
		t.Fatalf("the refund activity ran %d times; the test needs the killed attempt to be retried", got)
	}
	if got := payments.Len(); got != 1 {
		t.Fatalf("the ledger holds %d payments, want exactly 1: %+v", got, payments.Payments())
	}
	if got := payments.Payments()[0]; got.Cents != 2400 || got.Key != key {
		t.Fatalf("ledger entry %+v, want %d cents under key %s", got, 2400, key)
	}
	if got := mail.Len(); got != 1 {
		t.Fatalf("the mail sink holds %d replies, want 1", got)
	}

	// And the history still replays: whatever the crash did, the code and the record agree.
	if err := wftest.Replay(st.history("ticket-1"), agent.Support); err != nil {
		t.Fatalf("replay: %v", err)
	}
}

// ---- the approval paths -------------------------------------------------------------------

func TestLargeRefundWaitsForApprovalAndIsPaidOnce(t *testing.T) {
	payments := agent.NewMemSink()
	acts := agent.Activities{Model: agent.Scripted{}, Orders: agent.DemoOrders(),
		Payments: payments, Mail: agent.NewMemSink()}
	st := newStack(t, acts)
	defer st.startWorker("worker-1")()

	st.start("ticket-big", agent.Ticket{ID: "ticket-big", Customer: "Grace", OrderID: "A-2002",
		Text: "the lamp was broken"})

	// The run stops at the approval point: a timer is pending and nothing has been paid.
	st.waitFor("the approval point", func() bool {
		return countType(st.history("ticket-big"), history.EventTimerStarted) == 1
	})
	if payments.Len() != 0 {
		t.Fatalf("a refund above the threshold was paid without approval: %+v", payments.Payments())
	}

	st.signal("ticket-big", agent.Approval{Approved: true, By: "reviewer"})
	st.waitFor("the run to finish", func() bool {
		st.clk.Advance(time.Second)
		return st.closed("ticket-big")
	})

	res := st.result("ticket-big")
	if res.Approval != "approved" || res.RefundedCents != 18900 || res.Status != "replied" {
		t.Fatalf("result %+v", res)
	}
	if payments.Len() != 1 {
		t.Fatalf("ledger holds %d payments, want 1", payments.Len())
	}
}

func TestRejectedApprovalPaysNothing(t *testing.T) {
	payments := agent.NewMemSink()
	mail := agent.NewMemSink()
	st := newStack(t, agent.Activities{Model: agent.Scripted{}, Orders: agent.DemoOrders(),
		Payments: payments, Mail: mail})
	defer st.startWorker("worker-1")()

	st.start("ticket-no", agent.Ticket{ID: "ticket-no", Customer: "Alan", OrderID: "A-2002"})
	st.waitFor("the approval point", func() bool {
		return countType(st.history("ticket-no"), history.EventTimerStarted) == 1
	})
	st.signal("ticket-no", agent.Approval{Approved: false, By: "reviewer", Reason: "outside policy"})
	st.waitFor("the run to finish", func() bool {
		st.clk.Advance(time.Second)
		return st.closed("ticket-no")
	})

	res := st.result("ticket-no")
	if res.Status != "rejected" || res.Approval != "rejected" || res.RefundedCents != 0 {
		t.Fatalf("result %+v", res)
	}
	if payments.Len() != 0 {
		t.Fatalf("a rejected refund was paid: %+v", payments.Payments())
	}
	// The customer still gets an answer.
	if mail.Len() != 1 {
		t.Fatalf("mail sink holds %d replies, want 1", mail.Len())
	}
}

func TestApprovalWindowExpiresOnTheDurableTimer(t *testing.T) {
	payments := agent.NewMemSink()
	st := newStack(t, agent.Activities{Model: agent.Scripted{}, Orders: agent.DemoOrders(),
		Payments: payments, Mail: agent.NewMemSink()})
	defer st.startWorker("worker-1")()

	st.start("ticket-quiet", agent.Ticket{ID: "ticket-quiet", Customer: "Edsger", OrderID: "A-2002"})
	st.waitFor("the approval point", func() bool {
		return countType(st.history("ticket-quiet"), history.EventTimerStarted) == 1
	})

	// Nobody answers. A day passes.
	st.clk.Advance(agent.ApprovalWindow + time.Minute)
	st.waitFor("the window to close the run", func() bool {
		st.clk.Advance(time.Second)
		return st.closed("ticket-quiet")
	})

	res := st.result("ticket-quiet")
	if res.Approval != "timed_out" || res.Status != "rejected" || res.RefundedCents != 0 {
		t.Fatalf("result %+v", res)
	}
	if payments.Len() != 0 {
		t.Fatalf("an unapproved refund was paid: %+v", payments.Payments())
	}
}

// ---- the sinks ----------------------------------------------------------------------------

func TestFileSinkAppliesOncePerKeyAndSurvivesReopening(t *testing.T) {
	path := filepath.Join(t.TempDir(), "ledger.json")
	sink := agent.NewFileSink(path)
	applied, err := sink.Apply("k1", agent.Payment{Key: "k1", OrderID: "A-1", Cents: 500})
	if err != nil || !applied {
		t.Fatalf("first apply: %v, %v", applied, err)
	}
	// The same key again, with a different value: still a no-op.
	applied, err = sink.Apply("k1", agent.Payment{Key: "k1", OrderID: "A-1", Cents: 999999})
	if err != nil || applied {
		t.Fatalf("second apply: %v, %v", applied, err)
	}
	// A new process reading the same file sees one entry and refuses the key again.
	reopened := agent.NewFileSink(path)
	entries, err := reopened.Entries()
	if err != nil {
		t.Fatal(err)
	}
	if len(entries) != 1 {
		t.Fatalf("%d entries after reopening, want 1", len(entries))
	}
	if applied, err := reopened.Apply("k1", agent.Payment{Key: "k1", Cents: 1}); err != nil || applied {
		t.Fatalf("reopened apply: %v, %v", applied, err)
	}
	total, err := reopened.Total()
	if err != nil || total != 500 {
		t.Fatalf("total %d (%v), want 500", total, err)
	}
	if _, err := os.Stat(path + ".tmp"); !os.IsNotExist(err) {
		t.Errorf("the temporary file was left behind")
	}
}

func TestScriptedModelPlansALookupAndARefund(t *testing.T) {
	order := agent.Order{ID: "A-1001", Cents: 2400, Status: "delivered", Delivered: true}
	plan, err := agent.Scripted{}.Plan(context.Background(),
		agent.Ticket{ID: "t", Customer: "Ada", OrderID: order.ID}, &order)
	if err != nil {
		t.Fatal(err)
	}
	if len(plan.Tools) != 2 || plan.Tools[0].Name != agent.ActivityLookupOrder ||
		plan.Tools[1].Name != agent.ActivityIssueRefund {
		t.Fatalf("plan %+v", plan)
	}
	var args agent.RefundArgs
	if err := json.Unmarshal(plan.Tools[1].Args, &args); err != nil {
		t.Fatal(err)
	}
	if args.Cents != order.Cents || args.OrderID != order.ID {
		t.Fatalf("refund args %+v", args)
	}
	if plan.Reply == "" {
		t.Fatal("the plan has no reply")
	}
	// A ticket whose order cannot be found gets an answer, not a refund.
	plan, err = agent.Scripted{}.Plan(context.Background(), agent.Ticket{ID: "t"}, nil)
	if err != nil || len(plan.Tools) != 0 || plan.Reply == "" {
		t.Fatalf("unknown order: %+v, %v", plan, err)
	}
}

func TestRefundWithoutAnIdempotencyKeyIsRefused(t *testing.T) {
	acts := agent.Activities{Payments: agent.NewMemSink()}
	err := acts.IssueRefund(context.Background(), agent.RefundArgs{OrderID: "A-1", Cents: 100})
	if err == nil {
		t.Fatal("a refund with no idempotency key was accepted")
	}
}
