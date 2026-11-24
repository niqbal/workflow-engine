// Package agent is the showcase (WI-070, SPEC §14): a support agent whose tool calls are
// activities and whose human approval is a signal.
//
// The point is not that an agent can call tools. It is that this one cannot lose or repeat a
// refund. The model's plan is recorded in history, so a replay after any crash makes the same
// tool calls in the same order; each tool call carries an idempotency key that is stable
// across attempts, so the sinks behind them apply an effect exactly once however many times
// the activity runs; and a refund over the threshold stops and waits for a human, for up to a
// day, across any number of restarts.
//
// What is not claimed: that the model is called exactly once. Plan has no idempotency key
// because a second call to a model is harmless — it costs money, not correctness — and after
// a worker crash it may well happen. The claim is exactly-once *effects at idempotent sinks*.
package agent

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/niqbal/workflow-engine/activity"
	"github.com/niqbal/workflow-engine/wf"
)

// Ticket is what the agent is asked to handle.
type Ticket struct {
	ID       string `json:"id"`
	Customer string `json:"customer"`
	OrderID  string `json:"orderID"`
	Text     string `json:"text"`
}

// ToolCall is one step the model asked for.
type ToolCall struct {
	Name string          `json:"name"` // LookupOrder or IssueRefund
	Args json.RawMessage `json:"args"`
}

// Plan is the model's answer: the tools to run, then the reply to send.
type Plan struct {
	Tools []ToolCall `json:"tools"`
	Reply string     `json:"reply"`
}

// RefundArgs is IssueRefund's input. Cents keeps money in integers.
type RefundArgs struct {
	OrderID string `json:"orderID"`
	Cents   int64  `json:"cents"`
	Reason  string `json:"reason"`
}

// LookupArgs is LookupOrder's input.
type LookupArgs struct {
	OrderID string `json:"orderID"`
}

// Order is what LookupOrder returns.
type Order struct {
	ID        string `json:"id"`
	Cents     int64  `json:"cents"`
	Status    string `json:"status"`
	Delivered bool   `json:"delivered"`
}

// Result is what the workflow returns.
type Result struct {
	Status        string `json:"status"` // replied, rejected or failed
	RefundedCents int64  `json:"refundedCents"`
	Approval      string `json:"approval,omitempty"` // approved, rejected or timed_out
	Reply         string `json:"reply,omitempty"`
}

// ApprovalThresholdCents is the refund size above which a human has to agree.
const ApprovalThresholdCents = 5000

// ApprovalWindow is how long the workflow waits for that human before giving up. It is a
// durable timer: restarting every process in the cluster does not shorten or lose it.
const ApprovalWindow = 24 * time.Hour

// ApprovalSignal is the signal name a reviewer sends.
const ApprovalSignal = "approval"

// Approval is the signal's payload.
type Approval struct {
	Approved bool   `json:"approved"`
	By       string `json:"by,omitempty"`
	Reason   string `json:"reason,omitempty"`
}

// Activity names, registered by the worker.
const (
	ActivityPlan        = "Plan"
	ActivityLookupOrder = "LookupOrder"
	ActivityIssueRefund = "IssueRefund"
	ActivitySendReply   = "SendReply"
)

// Workflow type name.
const WorkflowType = "SupportAgent"

// Activity options. Every one of them retries without a bound, with backoff: a ticket that
// gives up halfway leaves a customer unrefunded or unanswered, which is worse than a slow
// ticket. A bad request still ends the run at once, because the activity returns a
// non-retryable failure rather than exhausting a policy. The model call gets the longest
// attempt, since it is the slowest thing here.
var (
	planOpts = wf.ActivityOptions{
		StartToCloseTimeout: 2 * time.Minute,
		RetryPolicy:         wf.RetryPolicy{InitialInterval: 2 * time.Second, MaximumInterval: time.Minute},
	}
	toolOpts = wf.ActivityOptions{
		StartToCloseTimeout: 30 * time.Second,
		RetryPolicy:         wf.RetryPolicy{InitialInterval: time.Second, MaximumInterval: 30 * time.Second},
	}
	replyOpts = wf.ActivityOptions{
		StartToCloseTimeout: 30 * time.Second,
		RetryPolicy:         wf.RetryPolicy{InitialInterval: time.Second, MaximumInterval: 30 * time.Second},
	}
)

// Support is the agent workflow.
//
// It is an ordinary Go function. Every decision it makes is recorded as an event before it is
// acted on, so a crash anywhere in here resumes at the same line with the same values.
func Support(ctx wf.Context, t Ticket) (Result, error) {
	var plan Plan
	if err := wf.ExecuteActivity(ctx, ActivityPlan, t, planOpts).Get(ctx, &plan); err != nil {
		return Result{Status: "failed"}, err
	}

	var res Result
	for _, call := range plan.Tools {
		switch call.Name {
		case ActivityLookupOrder:
			var order Order
			if err := wf.ExecuteActivity(ctx, ActivityLookupOrder, call.Args, toolOpts).Get(ctx, &order); err != nil {
				return Result{Status: "failed"}, err
			}

		case ActivityIssueRefund:
			var args RefundArgs
			if err := json.Unmarshal(call.Args, &args); err != nil {
				return Result{Status: "failed"}, fmt.Errorf("refund arguments: %w", err)
			}
			// A large refund waits for a human. The wait is a signal with a durable timer as
			// its fallback, so an unanswered ticket resolves by itself after a day rather
			// than holding the run open forever.
			if args.Cents > ApprovalThresholdCents {
				decision := awaitApproval(ctx)
				res.Approval = decision
				if decision != "approved" {
					res.Status = "rejected"
					plan.Reply = rejectionReply(decision)
					break
				}
			}
			if err := wf.ExecuteActivity(ctx, ActivityIssueRefund, args, toolOpts).Get(ctx, nil); err != nil {
				return Result{Status: "failed"}, err
			}
			res.RefundedCents += args.Cents

		default:
			return Result{Status: "failed"}, fmt.Errorf("the model asked for an unknown tool %q", call.Name)
		}
	}

	if err := wf.ExecuteActivity(ctx, ActivitySendReply, Reply{
		Ticket: t.ID, Customer: t.Customer, Text: plan.Reply,
	}, replyOpts).Get(ctx, nil); err != nil {
		return Result{Status: "failed"}, err
	}
	res.Reply = plan.Reply
	if res.Status == "" {
		res.Status = "replied"
	}
	return res, nil
}

// awaitApproval waits for the approval signal, or gives up when the window closes. It
// returns "approved", "rejected" or "timed_out".
func awaitApproval(ctx wf.Context) string {
	decision := ""
	sel := wf.NewSelector(ctx)
	sel.AddReceive(wf.GetSignalChannel(ctx, ApprovalSignal), func(c wf.ReceiveChannel) {
		var a Approval
		c.Receive(ctx, &a)
		if a.Approved {
			decision = "approved"
		} else {
			decision = "rejected"
		}
	})
	sel.AddFuture(wf.NewTimer(ctx, ApprovalWindow), func(wf.Future) {
		decision = "timed_out"
	})
	sel.Select(ctx)
	return decision
}

func rejectionReply(decision string) string {
	if decision == "timed_out" {
		return "We could not approve this refund in time. A specialist will follow up."
	}
	return "We are unable to issue a refund for this order."
}

// Reply is SendReply's input.
type Reply struct {
	Ticket   string `json:"ticket"`
	Customer string `json:"customer"`
	Text     string `json:"text"`
}

// ---- activities --------------------------------------------------------------------------

// Model is what the Plan activity calls. Scripted implements it without a network; Claude
// implements it against the real API.
type Model interface {
	Plan(ctx context.Context, t Ticket, order *Order) (Plan, error)
}

// Orders is the order lookup a tool call reads.
type Orders interface {
	Order(ctx context.Context, id string) (Order, error)
}

// Sink applies an effect once per key. Both the payment ledger and the mail sink are one.
type Sink interface {
	// Apply records value under key if key is new, and reports whether it did. Applying the
	// same key again must be a no-op, whatever the value.
	Apply(key string, value any) (bool, error)
}

// Activities is the set an agent worker registers.
type Activities struct {
	Model  Model
	Orders Orders
	// Payments is the refund sink, keyed by the activity's idempotency key.
	Payments Sink
	// Mail is the reply sink, keyed the same way.
	Mail Sink
	// AfterRefund, when set, runs after the refund has been applied and before the activity
	// reports it. The crash test uses it to kill the worker in exactly that window.
	AfterRefund func(ctx context.Context, key string) error
}

// Plan asks the model what to do. It has no idempotency key: calling a model twice costs
// money, not correctness, and after a crash it can happen.
func (a Activities) Plan(ctx context.Context, t Ticket) (Plan, error) {
	var order *Order
	if t.OrderID != "" && a.Orders != nil {
		o, err := a.Orders.Order(ctx, t.OrderID)
		if err == nil {
			order = &o
		}
	}
	return a.Model.Plan(ctx, t, order)
}

// LookupOrder is a read, so it needs no key either.
func (a Activities) LookupOrder(ctx context.Context, args LookupArgs) (Order, error) {
	return a.Orders.Order(ctx, args.OrderID)
}

// IssueRefund moves money. The engine gives every attempt of this activity the same
// idempotency key, so the ledger behind it applies the refund once however many attempts run
// — including the attempt that applies it and then dies before saying so.
func (a Activities) IssueRefund(ctx context.Context, args RefundArgs) error {
	key := activity.GetInfo(ctx).IdempotencyKey
	if key == "" {
		return fmt.Errorf("refund without an idempotency key: refusing to move money")
	}
	applied, err := a.Payments.Apply(key, Payment{
		Key: key, OrderID: args.OrderID, Cents: args.Cents, Reason: args.Reason,
	})
	if err != nil {
		return err
	}
	if a.AfterRefund != nil {
		// The refund is durable at the sink; the report of it is not yet made. This is the
		// window the whole example exists to survive.
		if err := a.AfterRefund(ctx, key); err != nil {
			return err
		}
	}
	_ = applied // a repeat attempt applies nothing and still succeeds
	return nil
}

// SendReply sends the mail, once per key.
func (a Activities) SendReply(ctx context.Context, r Reply) error {
	key := activity.GetInfo(ctx).IdempotencyKey
	if key == "" {
		return fmt.Errorf("reply without an idempotency key")
	}
	_, err := a.Mail.Apply(key, r)
	return err
}

// Payment is a ledger entry.
type Payment struct {
	Key     string `json:"key"`
	OrderID string `json:"orderID"`
	Cents   int64  `json:"cents"`
	Reason  string `json:"reason,omitempty"`
}

// Registry is what the worker and the simulator both implement.
type Registry interface {
	RegisterWorkflow(workflowType string, fn any) error
	RegisterActivity(activityType string, fn any) error
}

// Register binds the workflow and every activity.
func (a Activities) Register(r Registry) error {
	if err := r.RegisterWorkflow(WorkflowType, Support); err != nil {
		return err
	}
	acts := []struct {
		name string
		fn   any
	}{
		{ActivityPlan, a.Plan},
		{ActivityLookupOrder, a.LookupOrder},
		{ActivityIssueRefund, a.IssueRefund},
		{ActivitySendReply, a.SendReply},
	}
	for _, act := range acts {
		if err := r.RegisterActivity(act.name, act.fn); err != nil {
			return err
		}
	}
	return nil
}
