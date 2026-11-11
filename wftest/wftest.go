// Package wftest helps workflow authors check that changed code still replays old histories
// (SPEC §7.4, WI-006). Golden histories are generated from real runs (WI-024).
package wftest

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"

	"github.com/niqbal/workflow-engine/internal/history"
)

// ErrReplayerMissing is returned until the replayer exists (WI-013).
var ErrReplayerMissing = errors.New("wftest: replayer not built yet (WI-013)")

// Replayer replays a history through a workflow function. internal/engine sets it (WI-013).
var Replayer func(events []history.Event, workflowFn any) error

// LoadHistory reads a history saved as a JSON array of events.
func LoadHistory(path string) ([]history.Event, error) {
	b, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	var evs []history.Event
	if err := json.Unmarshal(b, &evs); err != nil {
		return nil, fmt.Errorf("wftest: %s: %w", path, err)
	}
	return evs, nil
}

// ReplayFromFile replays the saved history through workflowFn. It returns the replayer's
// error, which is a NonDeterministicError when the code no longer matches the history.
func ReplayFromFile(path string, workflowFn any) error {
	evs, err := LoadHistory(path)
	if err != nil {
		return err
	}
	if Replayer == nil {
		return ErrReplayerMissing
	}
	return Replayer(evs, workflowFn)
}
