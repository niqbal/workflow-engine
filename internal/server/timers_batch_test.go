package server

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/niqbal/workflow-engine/internal/history"
	"github.com/niqbal/workflow-engine/internal/server/api"
)

// More due timers than one ReadTimerTasks page all fire in a single Settle, each exactly once,
// and a second Settle writes nothing.
func TestSettleFiresMoreTimersThanOnePage(t *testing.T) {
	s, st := newTestServer(t)
	clk := manualClock(t, s)
	ctx := context.Background()
	if _, err := s.StartWorkflow(ctx, startReq("wf", "r")); err != nil {
		t.Fatal(err)
	}
	task := pollNow(t, s)
	const n = 2*timerBatch + 7
	var cmds []history.Command
	for i := 0; i < n; i++ {
		cmds = append(cmds, history.StartTimer{TimerID: fmt.Sprintf("t%d", i), Duration: time.Duration(i+1) * time.Second})
	}
	if err := s.CompleteWorkflowTask(ctx, api.CompleteWorkflowTaskRequest{TaskToken: task.TaskToken, Commands: cmds}); err != nil {
		t.Fatal(err)
	}
	clk.Advance(time.Duration(n) * time.Second)
	s.Settle(ctx)
	count := func() int {
		c := 0
		for _, e := range historyTypes(t, s, "wf") {
			if e == history.EventTimerFired {
				c++
			}
		}
		return c
	}
	if got := count(); got != n {
		t.Fatalf("%d TimerFired events, want %d", got, n)
	}
	if left := len(dueTimers(t, s, st, clk.Now().Add(time.Hour))); left != 0 {
		t.Fatalf("%d timer tasks left, want 0", left)
	}
	s.Settle(ctx)
	if got := count(); got != n {
		t.Fatalf("after second Settle %d TimerFired events, want %d", got, n)
	}
}
