package server

import (
	"time"
)

// WI-032: crash recovery on shard load (SPEC §9 "Shard load", §11.2 node crash and restart).
//
// Everything a node needs to resume a shard is in the store: runs, history, and the transfer
// and timer tasks of the outbox. Timer processing keeps no state between scans; every scan
// reads the due timer tasks from the store, and firing is conditional on the ref still being
// pending, so a timer whose fire committed before a crash is never fired twice (G5).
//
// Transfer processing does keep state: the ack level, gaps, and which tasks it has pushed to
// matching. A restarted node is a new Server and starts that state empty. A node that
// acquires a shard again (Start after its matching queues were lost) must not trust what
// it remembers from the earlier ownership epoch either: the tasks it marked as pushed may no
// longer be queued. So the first scan under a new fence starts over: it reads every transfer
// task from the store and pushes it at once instead of waiting for the redispatch interval.

// loadTransferShard resets ts if the shard's fence has changed since ts was last used. Called
// with ts.mu held.
func (s *Server) loadTransferShard(shard int, ts *transferShard) {
	f, _ := s.fence(shard)
	if f == ts.fence {
		return
	}
	ts.fence = f
	ts.ack, ts.seen, ts.gaps = 1, 0, nil
	ts.pushed = map[int64]time.Time{}
}
