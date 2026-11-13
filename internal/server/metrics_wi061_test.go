package server

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/niqbal/workflow-engine/internal/history"
	"github.com/niqbal/workflow-engine/internal/server/api"
	"github.com/niqbal/workflow-engine/internal/simrt"
	"github.com/niqbal/workflow-engine/internal/store"
	"github.com/niqbal/workflow-engine/internal/store/memstore"
)

type metricsFaultStore struct {
	store.Store
	fenceCreateOnce bool
	conflictOnce    bool
}

func (m *metricsFaultStore) CreateRun(ctx context.Context, f store.Fence, r store.NewRun) ([]store.Task, error) {
	if m.fenceCreateOnce {
		m.fenceCreateOnce = false
		return nil, store.ErrFenced
	}
	return m.Store.CreateRun(ctx, f, r)
}

func (m *metricsFaultStore) UpdateRun(ctx context.Context, f store.Fence, key store.RunKey, u store.Update) (int64, []store.Task, error) {
	if m.conflictOnce {
		m.conflictOnce = false
		return 0, nil, store.ErrConflict
	}
	return m.Store.UpdateRun(ctx, f, key, u)
}

func scrapeMetric(t *testing.T, s *Server, sample string) float64 {
	t.Helper()
	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/metrics", nil)
	s.Metrics().Handler().ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("metrics status %d: %s", rec.Code, rec.Body.String())
	}
	for _, line := range strings.Split(rec.Body.String(), "\n") {
		if !strings.HasPrefix(line, sample+" ") {
			continue
		}
		v := strings.TrimSpace(strings.TrimPrefix(line, sample+" "))
		f, err := strconv.ParseFloat(v, 64)
		if err != nil {
			t.Fatalf("parse %q for %s: %v", v, sample, err)
		}
		return f
	}
	t.Fatalf("metric sample %q not found", sample)
	return 0
}

func pollWorkflowMetricTask(t *testing.T, s *Server, ctx context.Context, q string) *api.WorkflowTask {
	t.Helper()
	task, err := s.PollWorkflowTask(ctx, api.PollRequest{TaskQueue: q, Identity: "w"})
	if err != nil || task == nil {
		t.Fatalf("poll workflow: task=%v err=%v", task, err)
	}
	return task
}

func pollActivityMetricTask(t *testing.T, s *Server, ctx context.Context, q string) *api.ActivityTask {
	t.Helper()
	task, err := s.PollActivityTask(ctx, api.PollRequest{TaskQueue: q, Identity: "a"})
	if err != nil || task == nil {
		t.Fatalf("poll activity: task=%v err=%v", task, err)
	}
	return task
}

func hasEvent(evs []history.Event, typ history.EventType) bool {
	for _, e := range evs {
		if e.Type() == typ {
			return true
		}
	}
	return false
}

func countPendingTimersInStore(t *testing.T, s *Server, st store.Store) int64 {
	t.Helper()
	ctx := context.Background()
	var total int64
	for _, shard := range s.Shards() {
		limit := 1
		for {
			page, err := st.ReadTimerTasks(ctx, shard, farFuture, limit)
			if err != nil {
				t.Fatalf("read timers shard %d: %v", shard, err)
			}
			if len(page) < limit {
				total += int64(len(page))
				break
			}
			limit *= 2
		}
	}
	return total
}

func assertPendingTimersMetricMatchesStore(t *testing.T, s *Server, st store.Store) {
	t.Helper()
	got := scrapeMetric(t, s, "wfe_pending_timers")
	want := float64(countPendingTimersInStore(t, s, st))
	if got != want {
		t.Fatalf("wfe_pending_timers=%v, want authoritative store count %v", got, want)
	}
}

func TestServerMetricsTrackValuesThroughWorkflowAndTimer(t *testing.T) {
	rt := simrt.New(41, simrt.Start)
	defer rt.Close()
	base := memstore.New(memstore.WithRuntime(rt))
	st := &metricsFaultStore{Store: base, fenceCreateOnce: true, conflictOnce: true}
	s := New(rt, st, rt.Rand(), Config{NumShards: 2, LongPoll: time.Minute})
	ctx := context.Background()
	if err := s.Start(ctx); err != nil {
		t.Fatal(err)
	}

	if got := scrapeMetric(t, s, "wfe_shards_owned"); got != 2 {
		t.Fatalf("wfe_shards_owned=%v, want 2", got)
	}

	_, err := s.StartWorkflow(ctx, startReq("wf", "first"))
	if !errors.Is(err, api.ErrNotOwner) {
		t.Fatalf("first start: %v, want ErrNotOwner", err)
	}
	if got := scrapeMetric(t, s, "wfe_err_fenced_total"); got != 1 {
		t.Fatalf("wfe_err_fenced_total=%v, want 1", got)
	}

	if _, err := s.StartWorkflow(ctx, startReq("wf", "second")); err != nil {
		t.Fatal(err)
	}
	assertPendingTimersMetricMatchesStore(t, s, st)

	s.Settle(ctx)
	wt1 := pollWorkflowMetricTask(t, s, ctx, "q")
	if err := s.CompleteWorkflowTask(ctx, api.CompleteWorkflowTaskRequest{TaskToken: wt1.TaskToken,
		Commands: []history.Command{
			history.ScheduleActivity{ActivityID: "a1", ActivityType: "A", StartToCloseTimeout: time.Second},
			history.StartTimer{TimerID: "t1", Duration: 2 * time.Second},
		}}); err != nil {
		t.Fatal(err)
	}
	if got := scrapeMetric(t, s, "wfe_err_conflict_total"); got != 1 {
		t.Fatalf("wfe_err_conflict_total=%v, want 1", got)
	}
	assertPendingTimersMetricMatchesStore(t, s, st)

	s.Settle(ctx)
	at := pollActivityMetricTask(t, s, ctx, "q")
	if err := s.CompleteActivityTask(ctx, api.CompleteActivityTaskRequest{TaskToken: at.TaskToken}); err != nil {
		t.Fatal(err)
	}

	s.Settle(ctx)
	wt2 := pollWorkflowMetricTask(t, s, ctx, "q")
	if err := s.CompleteWorkflowTask(ctx, api.CompleteWorkflowTaskRequest{TaskToken: wt2.TaskToken}); err != nil {
		t.Fatal(err)
	}

	rt.Go("advance", func(ctx context.Context) { rt.Sleep(3 * time.Second) })
	if res := rt.Run(time.Time{}, nil); res != simrt.Idle {
		t.Fatalf("run ended with %v", res)
	}
	s.Settle(ctx)
	assertPendingTimersMetricMatchesStore(t, s, st)
	wt3 := pollWorkflowMetricTask(t, s, ctx, "q")
	if !hasEvent(wt3.History, history.EventTimerFired) {
		t.Fatalf("final workflow task has no TimerFired event")
	}
	if err := s.CompleteWorkflowTask(ctx, api.CompleteWorkflowTaskRequest{TaskToken: wt3.TaskToken,
		Commands: []history.Command{history.CompleteWorkflow{}}}); err != nil {
		t.Fatal(err)
	}

	if got := scrapeMetric(t, s, "wfe_workflow_tasks_total"); got != 3 {
		t.Fatalf("wfe_workflow_tasks_total=%v, want 3", got)
	}
	if got := scrapeMetric(t, s, "wfe_activity_tasks_total"); got != 1 {
		t.Fatalf("wfe_activity_tasks_total=%v, want 1", got)
	}
	if got := scrapeMetric(t, s, "wfe_timer_lag_seconds_count"); got != 1 {
		t.Fatalf("wfe_timer_lag_seconds_count=%v, want 1", got)
	}
	if got := scrapeMetric(t, s, "wfe_history_append_latency_seconds_count"); got <= 0 {
		t.Fatalf("wfe_history_append_latency_seconds_count=%v, want >0", got)
	}
}

func TestServerMetricsCollectorsAreIsolatedPerServer(t *testing.T) {
	rt := simrt.New(9, simrt.Start)
	defer rt.Close()
	st := memstore.New(memstore.WithRuntime(rt))
	s1 := New(rt, st, rt.Rand(), Config{NumShards: 2})
	s2 := New(rt, st, rt.Rand(), Config{NumShards: 2})

	s1.metrics().IncWorkflowTasks()
	s1.metrics().SetShardsOwned(2)

	if got := scrapeMetric(t, s1, "wfe_workflow_tasks_total"); got != 1 {
		t.Fatalf("s1 workflow tasks=%v, want 1", got)
	}
	if got := scrapeMetric(t, s2, "wfe_workflow_tasks_total"); got != 0 {
		t.Fatalf("s2 workflow tasks=%v, want 0", got)
	}
	if got := scrapeMetric(t, s2, "wfe_shards_owned"); got != 0 {
		t.Fatalf("s2 shards owned=%v, want 0", got)
	}
}
