package httpapi_test

import (
	"net/http"
	"strings"
	"testing"

	"github.com/niqbal/workflow-engine/internal/httpapi"
)

// WI-061 acceptance: /metrics exposes every metric listed in SPEC 13.3.
func TestWI061MetricsScrapeIncludesSpecFamilies(t *testing.T) {
	h := httpapi.New(&fakeService{}, httpapi.Options{})
	rec := serve(h, http.MethodGet, "/metrics", "")
	if rec.Code != http.StatusOK {
		t.Fatalf("status %d, body %q", rec.Code, rec.Body.String())
	}

	body := rec.Body.String()
	wantFamilies := []string{
		"wfe_workflow_tasks_total",
		"wfe_activity_tasks_total",
		"wfe_schedule_to_start_latency_seconds",
		"wfe_step_latency_seconds",
		"wfe_history_append_latency_seconds",
		"wfe_err_conflict_total",
		"wfe_err_fenced_total",
		"wfe_shards_owned",
		"wfe_pending_timers",
		"wfe_timer_lag_seconds",
		"wfe_redispatch_total",
	}

	for _, family := range wantFamilies {
		if !strings.Contains(body, "# TYPE "+family+" ") {
			t.Fatalf("metrics output missing TYPE line for %q:\n%s", family, body)
		}
	}
}
