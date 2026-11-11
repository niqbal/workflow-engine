package server

// The contract-test harness (hand-owned, WI-005). One top-level test per scenario so the
// factory's gates compare scenario by scenario. Generated list: keep in sync with
// contracttest.Names (TestContractListIsComplete checks it).

import (
	"context"
	"testing"
	"time"

	"github.com/niqbal/workflow-engine/contracttest"
	"github.com/niqbal/workflow-engine/internal/clock"
	"github.com/niqbal/workflow-engine/internal/rnd"
	"github.com/niqbal/workflow-engine/internal/server/api"
	"github.com/niqbal/workflow-engine/internal/store"
	"github.com/niqbal/workflow-engine/internal/store/memstore"
	"github.com/niqbal/workflow-engine/internal/xsync"
)

type harness struct {
	s   *Server
	clk *clock.Manual
	st  store.Store
}

func newHarness(t *testing.T) contracttest.Harness {
	clk := clock.NewManual(time.Date(2026, 11, 11, 12, 0, 0, 0, time.UTC))
	rt := &xsync.Real{Ctx: context.Background(), C: clk}
	st := memstore.New(memstore.WithRuntime(rt))
	s := New(rt, st, rnd.New(1), Config{NumShards: 2})
	if err := s.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	return &harness{s: s, clk: clk, st: st}
}

func (h *harness) Service() api.Service { return h.s.Service() }
func (h *harness) Now() time.Time       { return h.clk.Now() }
func (h *harness) Settle()              { h.s.Settle(context.Background()) }
func (h *harness) DropQueuedTasks()     { h.s.DropQueuedTasks() }

// Advance fires each due timer task at its own time, settling after each, then moves to the
// target time.
func (h *harness) Advance(d time.Duration) {
	target := h.clk.Now().Add(d)
	for i := 0; i < 10000; i++ {
		next, ok := h.nextTimer(target)
		if !ok {
			break
		}
		if next.After(h.clk.Now()) {
			h.clk.AdvanceTo(next)
		}
		h.Settle()
		if n2, ok2 := h.nextTimer(target); ok2 && !n2.After(h.clk.Now()) {
			// Still due at this instant after settling: the processor did not consume it.
			// Move on to avoid looping forever; the scenario will report what is missing.
			break
		}
	}
	h.clk.AdvanceTo(target)
	h.Settle()
}

func (h *harness) nextTimer(upTo time.Time) (time.Time, bool) {
	var best time.Time
	found := false
	for _, shard := range h.s.Shards() {
		ts, err := h.st.ReadTimerTasks(context.Background(), shard, upTo, 1)
		if err != nil || len(ts) == 0 {
			continue
		}
		if !found || ts[0].FireAt.Before(best) {
			best, found = ts[0].FireAt, true
		}
	}
	return best, found
}

func TestContractListIsComplete(t *testing.T) {
	want := contracttest.Names()
	if len(want) != 26 {
		t.Fatalf("contracttest has %d scenarios; regenerate the TestContract_ list (have 26)", len(want))
	}
}

func TestContract_StartRejectsMissingFields(t *testing.T) {
	contracttest.RunOne(t, "StartRejectsMissingFields", newHarness)
}
func TestContract_StartIsIdempotentByRequestID(t *testing.T) {
	contracttest.RunOne(t, "StartIsIdempotentByRequestID", newHarness)
}
func TestContract_StartDedupSurvivesClose(t *testing.T) {
	contracttest.RunOne(t, "StartDedupSurvivesClose", newHarness)
}
func TestContract_FirstWorkflowTaskCarriesHistory(t *testing.T) {
	contracttest.RunOne(t, "FirstWorkflowTaskCarriesHistory", newHarness)
}
func TestContract_ActivityRoundTrip(t *testing.T) {
	contracttest.RunOne(t, "ActivityRoundTrip", newHarness)
}
func TestContract_StaleWorkflowTaskTokenRejected(t *testing.T) {
	contracttest.RunOne(t, "StaleWorkflowTaskTokenRejected", newHarness)
}
func TestContract_SignalAppendedWhileTaskScheduled(t *testing.T) {
	contracttest.RunOne(t, "SignalAppendedWhileTaskScheduled", newHarness)
}
func TestContract_SignalSchedulesTaskWhenIdle(t *testing.T) {
	contracttest.RunOne(t, "SignalSchedulesTaskWhenIdle", newHarness)
}
func TestContract_SignalBufferedWhileTaskStarted(t *testing.T) {
	contracttest.RunOne(t, "SignalBufferedWhileTaskStarted", newHarness)
}
func TestContract_CloseRejectedWhileEventsBuffered(t *testing.T) {
	contracttest.RunOne(t, "CloseRejectedWhileEventsBuffered", newHarness)
}
func TestContract_SignalDedupAndClosedRun(t *testing.T) {
	contracttest.RunOne(t, "SignalDedupAndClosedRun", newHarness)
}
func TestContract_ActivityRetryWithBackoff(t *testing.T) {
	contracttest.RunOne(t, "ActivityRetryWithBackoff", newHarness)
}
func TestContract_ActivityNonRetryableFailure(t *testing.T) {
	contracttest.RunOne(t, "ActivityNonRetryableFailure", newHarness)
}
func TestContract_ActivityAttemptsExhausted(t *testing.T) {
	contracttest.RunOne(t, "ActivityAttemptsExhausted", newHarness)
}
func TestContract_ActivityStartToCloseTimeoutAndStaleAttempt(t *testing.T) {
	contracttest.RunOne(t, "ActivityStartToCloseTimeoutAndStaleAttempt", newHarness)
}
func TestContract_ActivityScheduleToCloseTimeout(t *testing.T) {
	contracttest.RunOne(t, "ActivityScheduleToCloseTimeout", newHarness)
}
func TestContract_UserTimerFiresOnceOnTime(t *testing.T) {
	contracttest.RunOne(t, "UserTimerFiresOnceOnTime", newHarness)
}
func TestContract_CanceledTimerNeverFires(t *testing.T) {
	contracttest.RunOne(t, "CanceledTimerNeverFires", newHarness)
}
func TestContract_WorkflowTaskTimeoutRetries(t *testing.T) {
	contracttest.RunOne(t, "WorkflowTaskTimeoutRetries", newHarness)
}
func TestContract_WorkflowTaskFailureSeriesWritesOneEvent(t *testing.T) {
	contracttest.RunOne(t, "WorkflowTaskFailureSeriesWritesOneEvent", newHarness)
}
func TestContract_LostWorkflowTaskRedispatched(t *testing.T) {
	contracttest.RunOne(t, "LostWorkflowTaskRedispatched", newHarness)
}
func TestContract_LostActivityTaskRedispatched(t *testing.T) {
	contracttest.RunOne(t, "LostActivityTaskRedispatched", newHarness)
}
func TestContract_ExecutionTimeoutClosesRun(t *testing.T) {
	contracttest.RunOne(t, "ExecutionTimeoutClosesRun", newHarness)
}
func TestContract_TerminateClosesRunAndDropsWork(t *testing.T) {
	contracttest.RunOne(t, "TerminateClosesRunAndDropsWork", newHarness)
}
func TestContract_DescribeShowsPendingWork(t *testing.T) {
	contracttest.RunOne(t, "DescribeShowsPendingWork", newHarness)
}
func TestContract_InvalidCommandRejected(t *testing.T) {
	contracttest.RunOne(t, "InvalidCommandRejected", newHarness)
}
