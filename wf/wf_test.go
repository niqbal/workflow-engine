package wf_test

import (
	"encoding/json"
	"errors"
	"flag"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/niqbal/workflow-engine/internal/engine"
	"github.com/niqbal/workflow-engine/wf"
	"github.com/niqbal/workflow-engine/wftest"
)

var update = flag.Bool("update", false, "rewrite golden histories in testdata/")

type Request struct{ Ticket string }
type Draft struct{ Text string }
type Decision struct {
	Approved bool
	Reason   string
}
type Result struct{ Status string }

var opts = wf.ActivityOptions{StartToCloseTimeout: time.Minute}

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

func env(t *testing.T) *wftest.Env {
	t.Helper()
	e, err := wftest.NewEnv(Approval, Request{Ticket: "T-1"})
	if err != nil {
		t.Fatal(err)
	}
	e.RegisterActivity("DraftReply", func(in json.RawMessage) (any, error) {
		var r Request
		json.Unmarshal(in, &r)
		return Draft{Text: "re: " + r.Ticket}, nil
	})
	e.RegisterActivity("SendReply", func(json.RawMessage) (any, error) { return nil, nil })
	if err := e.Run(); err != nil {
		t.Fatal(err)
	}
	return e
}

func finish(t *testing.T, e *wftest.Env, golden string) Result {
	t.Helper()
	var r Result
	if err := e.Result(&r); err != nil {
		t.Fatal(err)
	}
	path := filepath.Join("testdata", golden)
	if *update {
		if err := e.SaveHistory(path); err != nil {
			t.Fatal(err)
		}
	}
	if err := wftest.ReplayFromFile(path, Approval); err != nil {
		t.Fatalf("golden %s does not replay: %v", golden, err)
	}
	return r
}

func TestApproved(t *testing.T) {
	e := env(t)
	if err := e.Signal("approval", Decision{Approved: true}); err != nil {
		t.Fatal(err)
	}
	if r := finish(t, e, "approval-approved.json"); r.Status != "sent" || e.ActCalls["SendReply"] != 1 {
		t.Fatalf("result %+v, SendReply calls %d", r, e.ActCalls["SendReply"])
	}
}

func TestRejected(t *testing.T) {
	e := env(t)
	if err := e.Signal("approval", Decision{Approved: false, Reason: "no"}); err != nil {
		t.Fatal(err)
	}
	if r := finish(t, e, "approval-rejected.json"); r.Status != "rejected:no" || e.ActCalls["SendReply"] != 0 {
		t.Fatalf("result %+v", r)
	}
}

func TestTimedOut(t *testing.T) {
	e := env(t)
	if err := e.Advance(71 * time.Hour); err != nil {
		t.Fatal(err)
	}
	if e.Done() {
		t.Fatal("finished before the 72h timer")
	}
	if err := e.Advance(time.Hour); err != nil {
		t.Fatal(err)
	}
	if r := finish(t, e, "approval-timeout.json"); r.Status != "rejected:timeout" {
		t.Fatalf("result %+v", r)
	}
}

// A code change that reorders activities is caught against the golden history.
func TestChangedCodeIsNonDeterministic(t *testing.T) {
	changed := func(ctx wf.Context, req Request) (Result, error) {
		wf.ExecuteActivity(ctx, "CheckFraud", req, opts).Get(ctx, nil) // inserted step
		return Approval(ctx, req)
	}
	err := wftest.ReplayFromFile(filepath.Join("testdata", "approval-approved.json"), changed)
	var nde *engine.NonDeterministicError
	if !errors.As(err, &nde) || !strings.Contains(nde.Got, "CheckFraud") {
		t.Fatalf("want NDE naming CheckFraud, got %v", err)
	}
}

func TestCoroutinesSideEffectsAndCancel(t *testing.T) {
	calls := 0
	w := func(ctx wf.Context) (string, error) {
		id := wf.SideEffect(ctx, func() any { calls++; return "id-42" })
		var s string
		if err := id.Get(&s); err != nil {
			return "", err
		}
		done := 0
		for i := 0; i < 3; i++ {
			wf.Go(ctx, func(c wf.Context) { wf.Sleep(c, time.Minute); done++ })
		}
		tm := wf.NewTimer(ctx, time.Hour)
		wf.Sleep(ctx, 2*time.Minute)
		tm.Cancel()
		err := tm.Get(ctx, nil)
		if !errors.Is(err, wf.ErrCanceled) || done != 3 {
			return "", errors.New("unexpected state")
		}
		return s + "@" + wf.Now(ctx).Format("15:04"), nil
	}
	e, err := wftest.NewEnv(w, nil)
	if err != nil {
		t.Fatal(err)
	}
	if err := e.Run(); err != nil {
		t.Fatal(err)
	}
	if err := e.Advance(3 * time.Minute); err != nil {
		t.Fatal(err)
	}
	var out string
	if err := e.Result(&out); err != nil {
		t.Fatal(err)
	}
	if out != "id-42@00:02" || calls != 1 {
		t.Fatalf("result %q after %d side-effect calls", out, calls)
	}
	// Replaying a history that already holds the side-effect marker must not run fn again.
	last := 0
	for i, ev := range e.History {
		if ev.Type() == "WorkflowTaskStarted" {
			last = i
		}
	}
	if _, err := engine.Replay(engine.Info{}, e.History[:last+1], wf.MustAdapt(w)); err != nil || calls != 1 {
		t.Fatalf("replay: %v (side effect ran %d times, want 1)", err, calls)
	}
}

func TestBadSignatures(t *testing.T) {
	for _, fn := range []any{func() error { return nil }, func(wf.Context) int { return 0 }, 42} {
		if _, err := wf.Adapt(fn); err == nil {
			t.Fatalf("%T accepted", fn)
		}
	}
}
