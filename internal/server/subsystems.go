package server

import (
	"sync/atomic"

	"github.com/niqbal/workflow-engine/internal/xsync"
)

// subsystems is the extension point for state that handlers and processors keep on a Server.
// Add a field here (with the file that owns it) instead of changing the Server struct in
// server.go. Fields are guarded by Server.mu unless they say otherwise.
type subsystems struct {
	starts   map[int]xsync.Locker    // per-shard start serialization (workflows.go)
	transfer map[int]*transferShard  // per-shard transfer processor state (transfer.go)
	timers   map[int]xsync.Locker    // per-shard timer scan serialization (timers.go)
	loops    atomic.Pointer[loopSet] // running background loops or nil (loops.go); atomic, not under mu
}
