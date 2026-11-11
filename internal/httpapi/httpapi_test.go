package httpapi_test

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/niqbal/workflow-engine/internal/history"
	"github.com/niqbal/workflow-engine/internal/httpapi"
	"github.com/niqbal/workflow-engine/internal/server/api"
)

// fakeService records the last request of each kind and returns err from every method.
type fakeService struct {
	err error

	start        api.StartRequest
	signal       api.SignalRequest
	terminate    api.TerminateRequest
	describeID   string
	historyID    string
	pollWF       api.PollRequest
	completeWF   api.CompleteWorkflowTaskRequest
	failWF       api.FailWorkflowTaskRequest
	pollAct      api.PollRequest
	completeAct  api.CompleteActivityTaskRequest
	failAct      api.FailActivityTaskRequest
	wfTask       *api.WorkflowTask
	actTask      *api.ActivityTask
	description  api.Description
	historyReply []history.Event
}

func (f *fakeService) StartWorkflow(_ context.Context, r api.StartRequest) (api.StartResponse, error) {
	f.start = r
	if f.err != nil {
		return api.StartResponse{}, f.err
	}
	return api.StartResponse{RunID: "run-1"}, nil
}
func (f *fakeService) SignalWorkflow(_ context.Context, r api.SignalRequest) error {
	f.signal = r
	return f.err
}
func (f *fakeService) TerminateWorkflow(_ context.Context, r api.TerminateRequest) error {
	f.terminate = r
	return f.err
}
func (f *fakeService) DescribeWorkflow(_ context.Context, id string) (api.Description, error) {
	f.describeID = id
	return f.description, f.err
}
func (f *fakeService) GetHistory(_ context.Context, id string) ([]history.Event, error) {
	f.historyID = id
	return f.historyReply, f.err
}
func (f *fakeService) PollWorkflowTask(_ context.Context, r api.PollRequest) (*api.WorkflowTask, error) {
	f.pollWF = r
	if f.err != nil {
		return nil, f.err
	}
	return f.wfTask, nil
}
func (f *fakeService) CompleteWorkflowTask(_ context.Context, r api.CompleteWorkflowTaskRequest) error {
	f.completeWF = r
	return f.err
}
func (f *fakeService) FailWorkflowTask(_ context.Context, r api.FailWorkflowTaskRequest) error {
	f.failWF = r
	return f.err
}
func (f *fakeService) PollActivityTask(_ context.Context, r api.PollRequest) (*api.ActivityTask, error) {
	f.pollAct = r
	if f.err != nil {
		return nil, f.err
	}
	return f.actTask, nil
}
func (f *fakeService) CompleteActivityTask(_ context.Context, r api.CompleteActivityTaskRequest) error {
	f.completeAct = r
	return f.err
}
func (f *fakeService) FailActivityTask(_ context.Context, r api.FailActivityTaskRequest) error {
	f.failAct = r
	return f.err
}

func serve(h http.Handler, method, path, body string) *httptest.ResponseRecorder {
	req := httptest.NewRequest(method, path, strings.NewReader(body))
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	return rec
}

func decodeBody(t *testing.T, rec *httptest.ResponseRecorder, v any) {
	t.Helper()
	if ct := rec.Header().Get("Content-Type"); ct != "application/json" {
		t.Fatalf("content type %q", ct)
	}
	if err := json.Unmarshal(rec.Body.Bytes(), v); err != nil {
		t.Fatalf("decode %q: %v", rec.Body.String(), err)
	}
}

func wantOK(t *testing.T, rec *httptest.ResponseRecorder) {
	t.Helper()
	if rec.Code != http.StatusOK {
		t.Fatalf("status %d, body %s", rec.Code, rec.Body.String())
	}
}

func wantError(t *testing.T, rec *httptest.ResponseRecorder, status int, code string) httpapi.ErrorBody {
	t.Helper()
	if rec.Code != status {
		t.Fatalf("status %d, want %d; body %s", rec.Code, status, rec.Body.String())
	}
	var b httpapi.ErrorBody
	decodeBody(t, rec, &b)
	if b.Code != code || b.Message == "" {
		t.Fatalf("error body %+v, want code %q and a message", b, code)
	}
	return b
}

func TestStartEndpoint(t *testing.T) {
	f := &fakeService{}
	h := httpapi.New(f, httpapi.Options{})
	rec := serve(h, "POST", "/v1/workflows", `{"workflowID":"wf","workflowType":"W","taskQueue":"q","input":{"a":1},"requestID":"r1","executionTimeout":5000000000}`)
	wantOK(t, rec)
	var resp api.StartResponse
	decodeBody(t, rec, &resp)
	if resp.RunID != "run-1" {
		t.Fatalf("runID %q", resp.RunID)
	}
	want := api.StartRequest{WorkflowID: "wf", WorkflowType: "W", TaskQueue: "q", Input: json.RawMessage(`{"a":1}`),
		RequestID: "r1", ExecutionTimeout: 5 * time.Second}
	if !reflect.DeepEqual(f.start, want) {
		t.Fatalf("service got %+v, want %+v", f.start, want)
	}
}

func TestSignalEndpointTakesWorkflowIDFromPath(t *testing.T) {
	f := &fakeService{}
	h := httpapi.New(f, httpapi.Options{})
	// An escaped slash stays inside the {id} segment.
	wantOK(t, serve(h, "POST", "/v1/workflows/a%2Fb/signal", `{"signalName":"go","payload":[1],"requestID":"s1"}`))
	want := api.SignalRequest{WorkflowID: "a/b", SignalName: "go", Payload: json.RawMessage(`[1]`), RequestID: "s1"}
	if !reflect.DeepEqual(f.signal, want) {
		t.Fatalf("service got %+v, want %+v", f.signal, want)
	}
	// A body naming the same workflow is fine; a different one is rejected.
	wantOK(t, serve(h, "POST", "/v1/workflows/wf/signal", `{"workflowID":"wf","signalName":"go"}`))
	f.signal = api.SignalRequest{}
	wantError(t, serve(h, "POST", "/v1/workflows/wf/signal", `{"workflowID":"other","signalName":"go"}`),
		http.StatusBadRequest, "invalid_argument")
	if f.signal.WorkflowID != "" {
		t.Fatal("mismatched signal reached the service")
	}
}

func TestTerminateEndpoint(t *testing.T) {
	f := &fakeService{}
	h := httpapi.New(f, httpapi.Options{})
	wantOK(t, serve(h, "POST", "/v1/workflows/wf/terminate", `{"reason":"stop"}`))
	if f.terminate != (api.TerminateRequest{WorkflowID: "wf", Reason: "stop"}) {
		t.Fatalf("service got %+v", f.terminate)
	}
	// The body is optional.
	wantOK(t, serve(h, "POST", "/v1/workflows/wf2/terminate", ""))
	if f.terminate != (api.TerminateRequest{WorkflowID: "wf2"}) {
		t.Fatalf("service got %+v", f.terminate)
	}
}

func TestDescribeEndpoint(t *testing.T) {
	fire := time.Date(2026, 11, 11, 12, 0, 0, 0, time.UTC)
	f := &fakeService{description: api.Description{WorkflowID: "wf", RunID: "r", WorkflowType: "W",
		Status:            history.StatusRunning,
		PendingActivities: []api.PendingActivity{{ScheduledEventID: 5, ActivityID: "a1", ActivityType: "A", Attempt: 2}},
		PendingTimers:     []api.PendingTimer{{TimerID: "t1", FireAt: fire}}}}
	h := httpapi.New(f, httpapi.Options{})
	rec := serve(h, "GET", "/v1/workflows/wf", "")
	wantOK(t, rec)
	var d api.Description
	decodeBody(t, rec, &d)
	if f.describeID != "wf" || !reflect.DeepEqual(d, f.description) {
		t.Fatalf("id %q, got %+v, want %+v", f.describeID, d, f.description)
	}
}

func TestHistoryEndpoint(t *testing.T) {
	at := time.Date(2026, 11, 11, 12, 0, 0, 0, time.UTC)
	f := &fakeService{historyReply: []history.Event{
		{ID: 1, Time: at, Attrs: history.WorkflowExecutionStarted{WorkflowType: "W", Input: json.RawMessage(`1`)}},
		{ID: 2, Time: at, Attrs: history.WorkflowExecutionCompleted{Result: json.RawMessage(`"ok"`)}},
	}}
	h := httpapi.New(f, httpapi.Options{})
	rec := serve(h, "GET", "/v1/workflows/wf/history", "")
	wantOK(t, rec)
	var resp httpapi.HistoryResponse
	decodeBody(t, rec, &resp)
	if f.historyID != "wf" || len(resp.Events) != 2 || resp.Events[0].Type() != history.EventWorkflowExecutionStarted ||
		string(resp.Events[1].Attrs.(history.WorkflowExecutionCompleted).Result) != `"ok"` {
		t.Fatalf("id %q, events %+v", f.historyID, resp.Events)
	}
	// An empty history is an empty list, not null.
	f.historyReply = nil
	rec = serve(h, "GET", "/v1/workflows/wf/history", "")
	wantOK(t, rec)
	if got := strings.TrimSpace(rec.Body.String()); got != `{"events":[]}` {
		t.Fatalf("empty history body %s", got)
	}
}

func TestWorkflowTaskEndpoints(t *testing.T) {
	f := &fakeService{}
	h := httpapi.New(f, httpapi.Options{})

	// No task before the long poll ends: 204.
	rec := serve(h, "POST", "/v1/tasks/workflow/poll", `{"taskQueue":"q","identity":"w1"}`)
	if rec.Code != http.StatusNoContent || rec.Body.Len() != 0 {
		t.Fatalf("empty poll: %d %q", rec.Code, rec.Body.String())
	}
	if f.pollWF != (api.PollRequest{TaskQueue: "q", Identity: "w1"}) {
		t.Fatalf("poll request %+v", f.pollWF)
	}
	f.wfTask = &api.WorkflowTask{TaskToken: "tok", WorkflowID: "wf", RunID: "r", WorkflowType: "W", Attempt: 1,
		History: []history.Event{{ID: 1, Attrs: history.WorkflowExecutionStarted{WorkflowType: "W"}}}}
	rec = serve(h, "POST", "/v1/tasks/workflow/poll", `{"taskQueue":"q"}`)
	wantOK(t, rec)
	var task api.WorkflowTask
	decodeBody(t, rec, &task)
	if task.TaskToken != "tok" || task.Attempt != 1 || len(task.History) != 1 || task.History[0].Type() != history.EventWorkflowExecutionStarted {
		t.Fatalf("task %+v", task)
	}

	cmds := []history.Command{
		history.ScheduleActivity{ActivityID: "a1", ActivityType: "A", StartToCloseTimeout: time.Second},
		history.StartTimer{TimerID: "t", Duration: time.Minute},
		history.CompleteWorkflow{Result: json.RawMessage(`"done"`)},
	}
	enc, err := history.EncodeCommands(cmds)
	if err != nil {
		t.Fatal(err)
	}
	body, _ := json.Marshal(httpapi.CompleteWorkflowTaskBody{TaskToken: "tok", Commands: enc, Identity: "w1"})
	wantOK(t, serve(h, "POST", "/v1/tasks/workflow/complete", string(body)))
	want := api.CompleteWorkflowTaskRequest{TaskToken: "tok", Commands: cmds, Identity: "w1"}
	if !reflect.DeepEqual(f.completeWF, want) {
		t.Fatalf("complete got %+v, want %+v", f.completeWF, want)
	}
	// No commands at all is allowed through to the service.
	wantOK(t, serve(h, "POST", "/v1/tasks/workflow/complete", `{"taskToken":"tok2"}`))
	if f.completeWF.TaskToken != "tok2" || f.completeWF.Commands != nil {
		t.Fatalf("complete without commands: %+v", f.completeWF)
	}
	// Unknown command types are the caller's error.
	wantError(t, serve(h, "POST", "/v1/tasks/workflow/complete", `{"taskToken":"t","commands":[{"type":"Nope","attrs":{}}]}`),
		http.StatusBadRequest, "invalid_argument")

	wantOK(t, serve(h, "POST", "/v1/tasks/workflow/fail", `{"taskToken":"tok","cause":{"type":"Panic","message":"boom"},"identity":"w1"}`))
	if f.failWF != (api.FailWorkflowTaskRequest{TaskToken: "tok", Cause: history.Failure{Type: "Panic", Message: "boom"}, Identity: "w1"}) {
		t.Fatalf("fail got %+v", f.failWF)
	}
}

func TestActivityTaskEndpoints(t *testing.T) {
	f := &fakeService{}
	h := httpapi.New(f, httpapi.Options{})

	rec := serve(h, "POST", "/v1/tasks/activity/poll", `{"taskQueue":"q","identity":"a1"}`)
	if rec.Code != http.StatusNoContent || rec.Body.Len() != 0 {
		t.Fatalf("empty poll: %d %q", rec.Code, rec.Body.String())
	}
	if f.pollAct != (api.PollRequest{TaskQueue: "q", Identity: "a1"}) {
		t.Fatalf("poll request %+v", f.pollAct)
	}
	deadline := time.Date(2026, 11, 11, 12, 0, 10, 0, time.UTC)
	f.actTask = &api.ActivityTask{TaskToken: "atok", WorkflowID: "wf", RunID: "r", ScheduledEventID: 5, ActivityID: "a1",
		ActivityType: "A", Input: json.RawMessage(`"in"`), Attempt: 2, IdempotencyKey: "wf/r/5", Deadline: deadline}
	rec = serve(h, "POST", "/v1/tasks/activity/poll", `{"taskQueue":"q"}`)
	wantOK(t, rec)
	var task api.ActivityTask
	decodeBody(t, rec, &task)
	if !reflect.DeepEqual(&task, f.actTask) {
		t.Fatalf("task %+v, want %+v", task, *f.actTask)
	}

	wantOK(t, serve(h, "POST", "/v1/tasks/activity/complete", `{"taskToken":"atok","result":{"x":2},"identity":"a1"}`))
	if !reflect.DeepEqual(f.completeAct, api.CompleteActivityTaskRequest{TaskToken: "atok", Result: json.RawMessage(`{"x":2}`), Identity: "a1"}) {
		t.Fatalf("complete got %+v", f.completeAct)
	}
	wantOK(t, serve(h, "POST", "/v1/tasks/activity/fail", `{"taskToken":"atok","failure":{"type":"E","message":"m","nonRetryable":true}}`))
	if f.failAct != (api.FailActivityTaskRequest{TaskToken: "atok", Failure: history.Failure{Type: "E", Message: "m", NonRetryable: true}}) {
		t.Fatalf("fail got %+v", f.failAct)
	}
}

func TestHealthAndMetrics(t *testing.T) {
	h := httpapi.New(&fakeService{}, httpapi.Options{})
	rec := serve(h, "GET", "/healthz", "")
	wantOK(t, rec)
	var health map[string]string
	decodeBody(t, rec, &health)
	if health["status"] != "ok" {
		t.Fatalf("health %v", health)
	}
	rec = serve(h, "GET", "/metrics", "")
	if rec.Code != http.StatusOK || !strings.HasPrefix(rec.Header().Get("Content-Type"), "text/plain") {
		t.Fatalf("default metrics: %d %q", rec.Code, rec.Header().Get("Content-Type"))
	}

	custom := http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { fmt.Fprint(w, "wfe_up 1\n") })
	h = httpapi.New(&fakeService{}, httpapi.Options{Metrics: custom})
	if rec := serve(h, "GET", "/metrics", ""); rec.Code != http.StatusOK || rec.Body.String() != "wfe_up 1\n" {
		t.Fatalf("custom metrics: %d %q", rec.Code, rec.Body.String())
	}
}

// endpoint is one service-backed route with a valid body.
type endpoint struct{ method, path, body string }

var serviceEndpoints = []endpoint{
	{"POST", "/v1/workflows", `{"workflowID":"wf","workflowType":"W","taskQueue":"q"}`},
	{"POST", "/v1/workflows/wf/signal", `{"signalName":"s"}`},
	{"POST", "/v1/workflows/wf/terminate", `{}`},
	{"GET", "/v1/workflows/wf", ""},
	{"GET", "/v1/workflows/wf/history", ""},
	{"POST", "/v1/tasks/workflow/poll", `{"taskQueue":"q"}`},
	{"POST", "/v1/tasks/workflow/complete", `{"taskToken":"t","commands":[]}`},
	{"POST", "/v1/tasks/workflow/fail", `{"taskToken":"t"}`},
	{"POST", "/v1/tasks/activity/poll", `{"taskQueue":"q"}`},
	{"POST", "/v1/tasks/activity/complete", `{"taskToken":"t"}`},
	{"POST", "/v1/tasks/activity/fail", `{"taskToken":"t"}`},
}

func TestEveryEndpointMapsEveryErrorCode(t *testing.T) {
	cases := []struct {
		err    error
		status int
		code   string
	}{
		{api.ErrNotFound, http.StatusNotFound, "not_found"},
		{api.ErrAlreadyStarted, http.StatusConflict, "already_started"},
		{api.ErrStaleTask, http.StatusConflict, "stale_task"},
		{api.ErrWorkflowClosed, http.StatusConflict, "workflow_closed"},
		{api.ErrInvalidArgument, http.StatusBadRequest, "invalid_argument"},
		{api.ErrNotOwner, http.StatusServiceUnavailable, "not_owner"},
		{api.ErrUnavailable, http.StatusServiceUnavailable, "unavailable"},
		{errors.New("disk on fire"), http.StatusInternalServerError, "internal"},
	}
	for _, ep := range serviceEndpoints {
		for _, c := range cases {
			t.Run(ep.method+" "+ep.path+" "+c.code, func(t *testing.T) {
				// Services wrap the sentinels with detail; the code survives the wrapping.
				err := fmt.Errorf("%w: detail for %s", c.err, ep.path)
				h := httpapi.New(&fakeService{err: err}, httpapi.Options{})
				b := wantError(t, serve(h, ep.method, ep.path, ep.body), c.status, c.code)
				if b.Message != err.Error() {
					t.Fatalf("message %q, want %q", b.Message, err.Error())
				}
				if httpapi.StatusFor(c.code) != c.status {
					t.Fatalf("StatusFor(%q) = %d", c.code, httpapi.StatusFor(c.code))
				}
			})
		}
	}
}

func TestMalformedBodiesAreInvalidArgument(t *testing.T) {
	for _, ep := range serviceEndpoints {
		if ep.method != "POST" {
			continue
		}
		t.Run(ep.path, func(t *testing.T) {
			h := httpapi.New(&fakeService{}, httpapi.Options{})
			wantError(t, serve(h, "POST", ep.path, `{"taskQueue":`), http.StatusBadRequest, "invalid_argument")
			wantError(t, serve(h, "POST", ep.path, `[1,2]`), http.StatusBadRequest, "invalid_argument")
		})
	}
}

func TestOversizedBodyIsRejected(t *testing.T) {
	f := &fakeService{}
	h := httpapi.New(f, httpapi.Options{MaxBodyBytes: 64})
	body := `{"workflowID":"wf","workflowType":"W","taskQueue":"q","input":"` + strings.Repeat("x", 100) + `"}`
	wantError(t, serve(h, "POST", "/v1/workflows", body), http.StatusRequestEntityTooLarge, "invalid_argument")
	if f.start.WorkflowID != "" {
		t.Fatal("oversized request reached the service")
	}
}

func TestUnknownRoutesAndMethods(t *testing.T) {
	h := httpapi.New(&fakeService{}, httpapi.Options{})
	wantError(t, serve(h, "GET", "/v1/nope", ""), http.StatusNotFound, "not_found")
	wantError(t, serve(h, "POST", "/v1/tasks/activity/heartbeat", `{}`), http.StatusNotFound, "not_found")

	rec := serve(h, "GET", "/v1/tasks/workflow/poll", "")
	wantError(t, rec, http.StatusMethodNotAllowed, "invalid_argument")
	if rec.Header().Get("Allow") != "POST" {
		t.Fatalf("Allow %q", rec.Header().Get("Allow"))
	}
	rec = serve(h, "DELETE", "/v1/workflows/wf", "")
	wantError(t, rec, http.StatusMethodNotAllowed, "invalid_argument")
	if rec.Header().Get("Allow") != "GET" {
		t.Fatalf("Allow %q", rec.Header().Get("Allow"))
	}
}
