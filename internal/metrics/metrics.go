package metrics

import (
	"fmt"
	"net/http"
	"strings"
	"sync/atomic"
	"time"
)

// Collector holds process-wide Prometheus metric state.
type Collector struct {
	workflowTasksTotal atomic.Uint64
	activityTasksTotal atomic.Uint64

	scheduleToStartWorkflowNanos atomic.Int64
	scheduleToStartWorkflowCount atomic.Uint64
	scheduleToStartActivityNanos atomic.Int64
	scheduleToStartActivityCount atomic.Uint64

	stepLatencyWorkflowNanos atomic.Int64
	stepLatencyWorkflowCount atomic.Uint64
	stepLatencyActivityNanos atomic.Int64
	stepLatencyActivityCount atomic.Uint64

	historyAppendNanos atomic.Int64
	historyAppendCount atomic.Uint64

	errConflictTotal atomic.Uint64
	errFencedTotal   atomic.Uint64
	// shardPlacementErrors counts placement passes that could not renew or acquire
	// everything they tried. A node riding out a store fault increments it and keeps
	// serving until the stop-serving margin (cluster.go, SPEC §9).
	shardPlacementErrors atomic.Uint64

	shardsOwned   atomic.Int64
	pendingTimers atomic.Int64

	timerLagNanos atomic.Int64
	timerLagCount atomic.Uint64

	redispatchTotal atomic.Uint64
}

var defaultCollector = &Collector{}

// Default returns the process-wide collector used by server and HTTP wiring.
func Default() *Collector { return defaultCollector }

func clampNonNegativeNanos(d time.Duration) int64 {
	if d < 0 {
		return 0
	}
	return d.Nanoseconds()
}

func (c *Collector) IncWorkflowTasks() { c.workflowTasksTotal.Add(1) }

func (c *Collector) IncActivityTasks() { c.activityTasksTotal.Add(1) }

// ObserveScheduleToStart adds one schedule-to-start latency sample.
func (c *Collector) ObserveScheduleToStart(taskType string, d time.Duration) {
	ns := clampNonNegativeNanos(d)
	switch taskType {
	case "workflow":
		c.scheduleToStartWorkflowNanos.Add(ns)
		c.scheduleToStartWorkflowCount.Add(1)
	case "activity":
		c.scheduleToStartActivityNanos.Add(ns)
		c.scheduleToStartActivityCount.Add(1)
	}
}

// ObserveStepLatency adds one end-to-end step latency sample.
func (c *Collector) ObserveStepLatency(taskType string, d time.Duration) {
	ns := clampNonNegativeNanos(d)
	switch taskType {
	case "workflow":
		c.stepLatencyWorkflowNanos.Add(ns)
		c.stepLatencyWorkflowCount.Add(1)
	case "activity":
		c.stepLatencyActivityNanos.Add(ns)
		c.stepLatencyActivityCount.Add(1)
	}
}

// ObserveHistoryAppendLatency adds one history append latency sample.
func (c *Collector) ObserveHistoryAppendLatency(d time.Duration) {
	c.historyAppendNanos.Add(clampNonNegativeNanos(d))
	c.historyAppendCount.Add(1)
}

func (c *Collector) IncErrConflict() { c.errConflictTotal.Add(1) }

func (c *Collector) IncErrFenced() { c.errFencedTotal.Add(1) }

// IncShardPlacementError counts one placement pass that did not fully succeed.
func (c *Collector) IncShardPlacementError() { c.shardPlacementErrors.Add(1) }

func (c *Collector) SetShardsOwned(n int) {
	if n < 0 {
		n = 0
	}
	c.shardsOwned.Store(int64(n))
}

// SetPendingTimers sets the pending timer gauge from authoritative state.
func (c *Collector) SetPendingTimers(n int64) {
	if n < 0 {
		n = 0
	}
	c.pendingTimers.Store(n)
}

// ObserveTimerLag adds one timer lag sample (fire time minus fireAt).
func (c *Collector) ObserveTimerLag(d time.Duration) {
	c.timerLagNanos.Add(clampNonNegativeNanos(d))
	c.timerLagCount.Add(1)
}

func (c *Collector) IncRedispatch() { c.redispatchTotal.Add(1) }

func seconds(ns int64) float64 { return float64(ns) / float64(time.Second) }

func writeSummaryWithTaskType(b *strings.Builder, name, taskType string, nanos int64, count uint64) {
	fmt.Fprintf(b, "%s_sum{task_type=%q} %g\n", name, taskType, seconds(nanos))
	fmt.Fprintf(b, "%s_count{task_type=%q} %d\n", name, taskType, count)
}

func writeSummary(b *strings.Builder, name string, nanos int64, count uint64) {
	fmt.Fprintf(b, "%s_sum %g\n", name, seconds(nanos))
	fmt.Fprintf(b, "%s_count %d\n", name, count)
}

// Handler exposes Prometheus text format.
func (c *Collector) Handler() http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "text/plain; version=0.0.4")

		var b strings.Builder
		fmt.Fprintln(&b, "# HELP wfe_workflow_tasks_total Workflow tasks started")
		fmt.Fprintln(&b, "# TYPE wfe_workflow_tasks_total counter")
		fmt.Fprintf(&b, "wfe_workflow_tasks_total %d\n", c.workflowTasksTotal.Load())

		fmt.Fprintln(&b, "# HELP wfe_activity_tasks_total Activity tasks started")
		fmt.Fprintln(&b, "# TYPE wfe_activity_tasks_total counter")
		fmt.Fprintf(&b, "wfe_activity_tasks_total %d\n", c.activityTasksTotal.Load())

		fmt.Fprintln(&b, "# HELP wfe_schedule_to_start_latency_seconds Task schedule-to-start latency")
		fmt.Fprintln(&b, "# TYPE wfe_schedule_to_start_latency_seconds summary")
		writeSummaryWithTaskType(&b, "wfe_schedule_to_start_latency_seconds", "workflow",
			c.scheduleToStartWorkflowNanos.Load(), c.scheduleToStartWorkflowCount.Load())
		writeSummaryWithTaskType(&b, "wfe_schedule_to_start_latency_seconds", "activity",
			c.scheduleToStartActivityNanos.Load(), c.scheduleToStartActivityCount.Load())

		fmt.Fprintln(&b, "# HELP wfe_step_latency_seconds End-to-end step latency")
		fmt.Fprintln(&b, "# TYPE wfe_step_latency_seconds summary")
		writeSummaryWithTaskType(&b, "wfe_step_latency_seconds", "workflow",
			c.stepLatencyWorkflowNanos.Load(), c.stepLatencyWorkflowCount.Load())
		writeSummaryWithTaskType(&b, "wfe_step_latency_seconds", "activity",
			c.stepLatencyActivityNanos.Load(), c.stepLatencyActivityCount.Load())

		fmt.Fprintln(&b, "# HELP wfe_history_append_latency_seconds History append latency")
		fmt.Fprintln(&b, "# TYPE wfe_history_append_latency_seconds summary")
		writeSummary(&b, "wfe_history_append_latency_seconds", c.historyAppendNanos.Load(), c.historyAppendCount.Load())

		fmt.Fprintln(&b, "# HELP wfe_err_conflict_total Store ErrConflict occurrences")
		fmt.Fprintln(&b, "# TYPE wfe_err_conflict_total counter")
		fmt.Fprintf(&b, "wfe_err_conflict_total %d\n", c.errConflictTotal.Load())

		fmt.Fprintln(&b, "# HELP wfe_err_fenced_total Store ErrFenced occurrences")
		fmt.Fprintln(&b, "# TYPE wfe_err_fenced_total counter")
		fmt.Fprintf(&b, "wfe_err_fenced_total %d\n", c.errFencedTotal.Load())

		fmt.Fprintln(&b, "# HELP wfe_shard_placement_errors_total Placement passes that could not renew or acquire everything")
		fmt.Fprintln(&b, "# TYPE wfe_shard_placement_errors_total counter")
		fmt.Fprintf(&b, "wfe_shard_placement_errors_total %d\n", c.shardPlacementErrors.Load())

		fmt.Fprintln(&b, "# HELP wfe_shards_owned Number of shards currently owned by this node")
		fmt.Fprintln(&b, "# TYPE wfe_shards_owned gauge")
		fmt.Fprintf(&b, "wfe_shards_owned %d\n", c.shardsOwned.Load())

		fmt.Fprintln(&b, "# HELP wfe_pending_timers Pending timer tasks")
		fmt.Fprintln(&b, "# TYPE wfe_pending_timers gauge")
		fmt.Fprintf(&b, "wfe_pending_timers %d\n", c.pendingTimers.Load())

		fmt.Fprintln(&b, "# HELP wfe_timer_lag_seconds Timer lag (fire time minus fireAt)")
		fmt.Fprintln(&b, "# TYPE wfe_timer_lag_seconds summary")
		writeSummary(&b, "wfe_timer_lag_seconds", c.timerLagNanos.Load(), c.timerLagCount.Load())

		fmt.Fprintln(&b, "# HELP wfe_redispatch_total Task redispatch count")
		fmt.Fprintln(&b, "# TYPE wfe_redispatch_total counter")
		fmt.Fprintf(&b, "wfe_redispatch_total %d\n", c.redispatchTotal.Load())

		_, _ = w.Write([]byte(b.String()))
	})
}
