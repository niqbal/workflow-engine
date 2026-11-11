package httpapi

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"

	"github.com/niqbal/workflow-engine/internal/history"
	"github.com/niqbal/workflow-engine/internal/server/api"
)

// ErrorBody is the JSON body of every error response (SPEC §13.1).
type ErrorBody struct {
	Code    string `json:"code"`
	Message string `json:"message"`
}

// CompleteWorkflowTaskBody is the wire form of api.CompleteWorkflowTaskRequest: commands are
// encoded with history.EncodeCommands.
type CompleteWorkflowTaskBody struct {
	TaskToken string          `json:"taskToken"`
	Commands  json.RawMessage `json:"commands,omitempty"`
	Identity  string          `json:"identity"`
}

// HistoryResponse is the body of GET /v1/workflows/{id}/history.
type HistoryResponse struct {
	Events []history.Event `json:"events"`
}

// StatusFor maps an error code to its HTTP status.
func StatusFor(code string) int {
	switch code {
	case api.ErrNotFound.Error():
		return http.StatusNotFound
	case api.ErrAlreadyStarted.Error(), api.ErrStaleTask.Error(), api.ErrWorkflowClosed.Error():
		return http.StatusConflict
	case api.ErrInvalidArgument.Error():
		return http.StatusBadRequest
	case api.ErrNotOwner.Error(), api.ErrUnavailable.Error():
		return http.StatusServiceUnavailable
	}
	return http.StatusInternalServerError
}

// Options configures the handler.
type Options struct {
	// Metrics serves GET /metrics. Nil serves an empty Prometheus exposition (WI-061 fills it).
	Metrics http.Handler
	// MaxBodyBytes limits request bodies. Zero means 16 MiB.
	MaxBodyBytes int64
}

type handler struct {
	svc     api.Service
	mux     *http.ServeMux
	maxBody int64
}

// New returns the HTTP handler for svc.
func New(svc api.Service, opts Options) http.Handler {
	h := &handler{svc: svc, mux: http.NewServeMux(), maxBody: opts.MaxBodyBytes}
	if h.maxBody <= 0 {
		h.maxBody = 16 << 20
	}
	metrics := opts.Metrics
	if metrics == nil {
		metrics = http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
			w.Header().Set("Content-Type", "text/plain; version=0.0.4")
		})
	}
	m := h.mux
	m.HandleFunc("POST /v1/workflows", h.start)
	m.HandleFunc("POST /v1/workflows/{id}/signal", h.signal)
	m.HandleFunc("POST /v1/workflows/{id}/terminate", h.terminate)
	m.HandleFunc("GET /v1/workflows/{id}", h.describe)
	m.HandleFunc("GET /v1/workflows/{id}/history", h.history)
	m.HandleFunc("POST /v1/tasks/workflow/poll", h.pollWorkflow)
	m.HandleFunc("POST /v1/tasks/workflow/complete", h.completeWorkflow)
	m.HandleFunc("POST /v1/tasks/workflow/fail", h.failWorkflow)
	m.HandleFunc("POST /v1/tasks/activity/poll", h.pollActivity)
	m.HandleFunc("POST /v1/tasks/activity/complete", h.completeActivity)
	m.HandleFunc("POST /v1/tasks/activity/fail", h.failActivity)
	m.HandleFunc("GET /healthz", func(w http.ResponseWriter, _ *http.Request) {
		writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
	})
	m.Handle("GET /metrics", metrics)
	m.HandleFunc("/", h.noRoute)
	return m
}

func (h *handler) start(w http.ResponseWriter, r *http.Request) {
	var req api.StartRequest
	if !h.decode(w, r, &req) {
		return
	}
	resp, err := h.svc.StartWorkflow(r.Context(), req)
	reply(w, resp, err)
}

func (h *handler) signal(w http.ResponseWriter, r *http.Request) {
	var req api.SignalRequest
	if !h.decode(w, r, &req) || !pathID(w, r, &req.WorkflowID) {
		return
	}
	reply(w, struct{}{}, h.svc.SignalWorkflow(r.Context(), req))
}

func (h *handler) terminate(w http.ResponseWriter, r *http.Request) {
	var req api.TerminateRequest
	if !h.decode(w, r, &req) || !pathID(w, r, &req.WorkflowID) {
		return
	}
	reply(w, struct{}{}, h.svc.TerminateWorkflow(r.Context(), req))
}

func (h *handler) describe(w http.ResponseWriter, r *http.Request) {
	d, err := h.svc.DescribeWorkflow(r.Context(), r.PathValue("id"))
	reply(w, d, err)
}

func (h *handler) history(w http.ResponseWriter, r *http.Request) {
	evs, err := h.svc.GetHistory(r.Context(), r.PathValue("id"))
	if evs == nil {
		evs = []history.Event{}
	}
	reply(w, HistoryResponse{Events: evs}, err)
}

func (h *handler) pollWorkflow(w http.ResponseWriter, r *http.Request) {
	var req api.PollRequest
	if !h.decode(w, r, &req) {
		return
	}
	task, err := h.svc.PollWorkflowTask(r.Context(), req)
	if err == nil && task == nil {
		w.WriteHeader(http.StatusNoContent)
		return
	}
	reply(w, task, err)
}

func (h *handler) completeWorkflow(w http.ResponseWriter, r *http.Request) {
	var body CompleteWorkflowTaskBody
	if !h.decode(w, r, &body) {
		return
	}
	req := api.CompleteWorkflowTaskRequest{TaskToken: body.TaskToken, Identity: body.Identity}
	if len(body.Commands) > 0 {
		cs, err := history.DecodeCommands(body.Commands)
		if err != nil {
			writeError(w, fmt.Errorf("%w: commands: %v", api.ErrInvalidArgument, err))
			return
		}
		req.Commands = cs
	}
	reply(w, struct{}{}, h.svc.CompleteWorkflowTask(r.Context(), req))
}

func (h *handler) failWorkflow(w http.ResponseWriter, r *http.Request) {
	var req api.FailWorkflowTaskRequest
	if !h.decode(w, r, &req) {
		return
	}
	reply(w, struct{}{}, h.svc.FailWorkflowTask(r.Context(), req))
}

func (h *handler) pollActivity(w http.ResponseWriter, r *http.Request) {
	var req api.PollRequest
	if !h.decode(w, r, &req) {
		return
	}
	task, err := h.svc.PollActivityTask(r.Context(), req)
	if err == nil && task == nil {
		w.WriteHeader(http.StatusNoContent)
		return
	}
	reply(w, task, err)
}

func (h *handler) completeActivity(w http.ResponseWriter, r *http.Request) {
	var req api.CompleteActivityTaskRequest
	if !h.decode(w, r, &req) {
		return
	}
	reply(w, struct{}{}, h.svc.CompleteActivityTask(r.Context(), req))
}

func (h *handler) failActivity(w http.ResponseWriter, r *http.Request) {
	var req api.FailActivityTaskRequest
	if !h.decode(w, r, &req) {
		return
	}
	reply(w, struct{}{}, h.svc.FailActivityTask(r.Context(), req))
}

// noRoute answers unknown paths with not_found and known paths with the wrong method with 405,
// both as JSON.
func (h *handler) noRoute(w http.ResponseWriter, r *http.Request) {
	for _, m := range []string{http.MethodGet, http.MethodPost} {
		if m == r.Method {
			continue
		}
		probe := r.Clone(r.Context())
		probe.Method = m
		if _, p := h.mux.Handler(probe); p != "/" {
			w.Header().Set("Allow", m)
			writeJSON(w, http.StatusMethodNotAllowed, ErrorBody{Code: api.ErrInvalidArgument.Error(),
				Message: fmt.Sprintf("method %s not allowed on %s", r.Method, r.URL.Path)})
			return
		}
	}
	writeJSON(w, http.StatusNotFound, ErrorBody{Code: api.ErrNotFound.Error(),
		Message: fmt.Sprintf("no route for %s %s", r.Method, r.URL.Path)})
}

// decode reads a JSON body into v. An empty body leaves v zero; the service validates it.
func (h *handler) decode(w http.ResponseWriter, r *http.Request, v any) bool {
	err := json.NewDecoder(http.MaxBytesReader(w, r.Body, h.maxBody)).Decode(v)
	if err == nil || errors.Is(err, io.EOF) {
		return true
	}
	var tooBig *http.MaxBytesError
	if errors.As(err, &tooBig) {
		writeJSON(w, http.StatusRequestEntityTooLarge, ErrorBody{Code: api.ErrInvalidArgument.Error(),
			Message: fmt.Sprintf("request body exceeds %d bytes", tooBig.Limit)})
		return false
	}
	writeError(w, fmt.Errorf("%w: malformed JSON body: %v", api.ErrInvalidArgument, err))
	return false
}

// pathID sets *id from the {id} path segment. A body that names a different workflow is rejected.
func pathID(w http.ResponseWriter, r *http.Request, id *string) bool {
	p := r.PathValue("id")
	if *id != "" && *id != p {
		writeError(w, fmt.Errorf("%w: body workflowID %q does not match path %q", api.ErrInvalidArgument, *id, p))
		return false
	}
	*id = p
	return true
}

func reply(w http.ResponseWriter, v any, err error) {
	if err != nil {
		writeError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, v)
}

func writeError(w http.ResponseWriter, err error) {
	code := api.Code(err)
	writeJSON(w, StatusFor(code), ErrorBody{Code: code, Message: err.Error()})
}

func writeJSON(w http.ResponseWriter, status int, v any) {
	b, err := json.Marshal(v)
	if err != nil {
		status = http.StatusInternalServerError
		b, _ = json.Marshal(ErrorBody{Code: "internal", Message: "encode response: " + err.Error()})
	}
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_, _ = w.Write(append(b, '\n'))
}
