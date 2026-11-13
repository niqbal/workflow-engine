package metrics

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

// WI-061 acceptance: /metrics exposes every metric listed in SPEC 13.3.
func TestCollectorHandlerExposesSpecMetricFamilies(t *testing.T) {
	c := &Collector{}
	req := httptest.NewRequest(http.MethodGet, "/metrics", nil)
	rec := httptest.NewRecorder()
	c.Handler().ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("status %d, body %q", rec.Code, rec.Body.String())
	}
	if ct := rec.Header().Get("Content-Type"); !strings.HasPrefix(ct, "text/plain") {
		t.Fatalf("content-type %q", ct)
	}

	body := rec.Body.String()
	want := []string{
		"# TYPE wfe_workflow_tasks_total counter",
		"# TYPE wfe_activity_tasks_total counter",
		"# TYPE wfe_schedule_to_start_latency_seconds summary",
		"# TYPE wfe_step_latency_seconds summary",
		"# TYPE wfe_history_append_latency_seconds summary",
		"# TYPE wfe_err_conflict_total counter",
		"# TYPE wfe_err_fenced_total counter",
		"# TYPE wfe_shards_owned gauge",
		"# TYPE wfe_pending_timers gauge",
		"# TYPE wfe_timer_lag_seconds summary",
		"# TYPE wfe_redispatch_total counter",
	}
	for _, line := range want {
		if !strings.Contains(body, line) {
			t.Fatalf("metrics output missing %q:\n%s", line, body)
		}
	}
}
