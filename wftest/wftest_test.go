package wftest

import (
	"errors"
	"os"
	"path/filepath"
	"testing"
)

func TestReplayFromFileWaitsForReplayer(t *testing.T) {
	p := filepath.Join(t.TempDir(), "h.json")
	os.WriteFile(p, []byte(`[{"id":1,"time":"2026-11-11T00:00:00Z","type":"WorkflowExecutionStarted","attrs":{"workflowType":"W","taskQueue":"q"}}]`), 0o644)
	evs, err := LoadHistory(p)
	if err != nil || len(evs) != 1 {
		t.Fatalf("load: %v %d", err, len(evs))
	}
	if Replayer == nil {
		if err := ReplayFromFile(p, nil); !errors.Is(err, ErrReplayerMissing) {
			t.Fatalf("want ErrReplayerMissing, got %v", err)
		}
		t.Skip("replayer not built yet (WI-013)")
	}
}
