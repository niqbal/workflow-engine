package client_test

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"

	"github.com/niqbal/workflow-engine/client"
	"github.com/niqbal/workflow-engine/internal/clock"
	"github.com/niqbal/workflow-engine/internal/history"
	"github.com/niqbal/workflow-engine/internal/httpapi"
	"github.com/niqbal/workflow-engine/internal/rnd"
	"github.com/niqbal/workflow-engine/internal/server"
	"github.com/niqbal/workflow-engine/internal/server/api"
	"github.com/niqbal/workflow-engine/internal/store/memstore"
	"github.com/niqbal/workflow-engine/internal/xsync"
)

// actThenDrop lets the server act on the first `drop` requests, then throws the reply away by
// closing the connection, the case where a retry is only safe with a requestID.
type actThenDrop struct {
	next http.Handler
	drop atomic.Int32
	hits atomic.Int32
}

func (d *actThenDrop) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if d.hits.Add(1) > d.drop.Load() {
		d.next.ServeHTTP(w, r)
		return
	}
	d.next.ServeHTTP(httptest.NewRecorder(), r)
	conn, _, err := w.(http.Hijacker).Hijack()
	if err != nil {
		panic(err)
	}
	_ = conn.Close()
}

func newDropStack(t *testing.T, drop int32) (*client.Client, *actThenDrop, *sleeps) {
	t.Helper()
	clk := clock.NewManual(time.Date(2026, 11, 11, 12, 0, 0, 0, time.UTC))
	rt := &xsync.Real{Ctx: context.Background(), C: clk}
	s := server.New(rt, memstore.New(memstore.WithRuntime(rt)), rnd.New(11), server.Config{NumShards: 2})
	if err := s.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	d := &actThenDrop{next: httpapi.New(s.Service(), httpapi.Options{})}
	d.drop.Store(drop)
	srv := httptest.NewServer(d)
	t.Cleanup(srv.Close)
	sl := &sleeps{}
	c := client.New(srv.URL, client.Options{
		HTTPClient:  &http.Client{Transport: &http.Transport{DisableKeepAlives: true}},
		MaxAttempts: 5,
		Sleep:       sl.sleep,
	})
	return c, d, sl
}

// A start whose reply is lost after the server created the run is retried with the same
// requestID and returns that run; a later signal whose reply is lost is recorded once.
func TestLostRepliesWithRequestIDAreRetriedExactlyOnce(t *testing.T) {
	ctx := context.Background()
	c, d, sl := newDropStack(t, 1)
	run, err := c.ExecuteWorkflow(ctx, api.StartRequest{WorkflowID: "wf", WorkflowType: "W", TaskQueue: "q", RequestID: "req-1"})
	if err != nil {
		t.Fatalf("start after lost reply: %v", err)
	}
	if d.hits.Load() != 2 || len(sl.got) != 1 {
		t.Fatalf("%d requests, backoffs %v; want 2 and 1", d.hits.Load(), sl.got)
	}
	desc, err := run.Describe(ctx)
	if err != nil || desc.RunID != run.RunID {
		t.Fatalf("describe: %+v, %v; want run %s", desc, err, run.RunID)
	}

	d.hits.Store(0)
	if err := run.Signal(ctx, "go", json.RawMessage(`1`), "sig-1"); err != nil {
		t.Fatalf("signal after lost reply: %v", err)
	}
	evs, err := run.History(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if n := count(evs, history.EventWorkflowExecutionSignaled); n != 1 {
		t.Fatalf("%d signaled events, want 1: %v", n, types(evs))
	}
}

// Without a requestID a lost reply is reported, not retried: the server acted once.
func TestLostReplyWithoutRequestIDIsNotRetried(t *testing.T) {
	ctx := context.Background()
	c, d, sl := newDropStack(t, 1)
	if _, err := c.StartWorkflow(ctx, api.StartRequest{WorkflowID: "wf", WorkflowType: "W", TaskQueue: "q"}); err == nil {
		t.Fatal("start with lost reply reported success")
	}
	if d.hits.Load() != 1 || len(sl.got) != 0 {
		t.Fatalf("%d requests, backoffs %v; want 1 and none", d.hits.Load(), sl.got)
	}
	if _, err := c.DescribeWorkflow(ctx, "wf"); err != nil {
		t.Fatalf("describe: %v", err)
	}
	d.hits.Store(0)
	if err := c.SignalWorkflow(ctx, api.SignalRequest{WorkflowID: "wf", SignalName: "s"}); err == nil {
		t.Fatal("signal with lost reply reported success")
	}
	if d.hits.Load() != 1 {
		t.Fatalf("%d signal requests, want 1", d.hits.Load())
	}
}

// Workflow IDs with characters that are special in URLs reach the server intact.
func TestSpecialWorkflowIDsRoundTrip(t *testing.T) {
	ctx := context.Background()
	st := newStack(t)
	for i, id := range []string{"a/b/c", "with space", "q?x=1&y=2", "hash#frag", "pct%41%2F", "ünïcödé", "semi;colon", "plus+sign"} {
		q := "q" + string(rune('a'+i))
		run, err := st.c.ExecuteWorkflow(ctx, api.StartRequest{WorkflowID: id, WorkflowType: "W", TaskQueue: q, RequestID: "r"})
		if err != nil {
			t.Fatalf("start %q: %v", id, err)
		}
		d, err := run.Describe(ctx)
		if err != nil || d.WorkflowID != id || d.RunID != run.RunID {
			t.Fatalf("describe %q: %+v, %v", id, d, err)
		}
		if err := run.Signal(ctx, "s", nil, "sig"); err != nil {
			t.Fatalf("signal %q: %v", id, err)
		}
		if err := run.Terminate(ctx, "done"); err != nil {
			t.Fatalf("terminate %q: %v", id, err)
		}
		evs, err := run.History(ctx)
		if err != nil || len(evs) == 0 || evs[len(evs)-1].Type() != history.EventWorkflowExecutionTerminated {
			t.Fatalf("history %q: %v, %v", id, types(evs), err)
		}
		var we *client.WorkflowError
		if err := run.Get(ctx, nil); !errors.As(err, &we) || we.WorkflowID != id || we.Reason != "done" {
			t.Fatalf("get %q: %v", id, err)
		}
	}
}

// Proxy pages: gateway errors are retried as unavailable; other plain-text errors are not.
func TestPlainTextProxyErrors(t *testing.T) {
	for _, tc := range []struct {
		status  int
		retried bool
		code    string
	}{
		{http.StatusBadGateway, true, "unavailable"},
		{http.StatusServiceUnavailable, true, "unavailable"},
		{http.StatusGatewayTimeout, true, "unavailable"},
		{http.StatusInternalServerError, false, "internal"},
	} {
		var hits atomic.Int32
		srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
			hits.Add(1)
			http.Error(w, "<html>proxy</html>", tc.status)
		}))
		sl := &sleeps{}
		c := client.New(srv.URL, client.Options{MaxAttempts: 3, Sleep: sl.sleep})
		_, err := c.DescribeWorkflow(context.Background(), "wf")
		srv.Close()
		var ce *client.Error
		if !errors.As(err, &ce) || ce.StatusCode != tc.status || api.Code(err) != tc.code {
			t.Fatalf("HTTP %d: error %#v, want code %s", tc.status, err, tc.code)
		}
		want := int32(1)
		if tc.retried {
			want = 3
		}
		if hits.Load() != want {
			t.Fatalf("HTTP %d: %d requests, want %d", tc.status, hits.Load(), want)
		}
	}
}

// Every endpoint answers a wrong method with a JSON 405 over a real HTTP connection.
func TestWrongMethodOnEveryEndpointIsJSON405(t *testing.T) {
	srv := httptest.NewServer(httpapi.New(okService{}, httpapi.Options{}))
	t.Cleanup(srv.Close)
	for _, ep := range []struct{ method, path, allow string }{
		{"GET", "/v1/workflows", "POST"},
		{"GET", "/v1/workflows/wf/signal", "POST"},
		{"GET", "/v1/workflows/wf/terminate", "POST"},
		{"POST", "/v1/workflows/wf", "GET"},
		{"POST", "/v1/workflows/wf/history", "GET"},
		{"GET", "/v1/tasks/workflow/poll", "POST"},
		{"GET", "/v1/tasks/workflow/complete", "POST"},
		{"GET", "/v1/tasks/workflow/fail", "POST"},
		{"GET", "/v1/tasks/activity/poll", "POST"},
		{"GET", "/v1/tasks/activity/complete", "POST"},
		{"GET", "/v1/tasks/activity/fail", "POST"},
		{"POST", "/healthz", "GET"},
	} {
		req, _ := http.NewRequest(ep.method, srv.URL+ep.path, nil)
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatal(err)
		}
		var b httpapi.ErrorBody
		derr := json.NewDecoder(resp.Body).Decode(&b)
		resp.Body.Close()
		if resp.StatusCode != http.StatusMethodNotAllowed || derr != nil || b.Code != "invalid_argument" ||
			resp.Header.Get("Allow") != ep.allow {
			t.Fatalf("%s %s: %d %+v (%v) Allow %q", ep.method, ep.path, resp.StatusCode, b, derr, resp.Header.Get("Allow"))
		}
	}
}
