// Package client is the Go client for the HTTP API (SPEC §13.1). Client implements
// api.Service over HTTP, so workers and tools can use it in place of an in-process server, and
// Run is a handle on one started workflow.
//
// Retries: a call is retried only when repeating it cannot change the outcome. That holds for
// reads (describe, history), terminate (terminating a closed run is a no-op), and start and
// signal when they carry a requestID (the server deduplicates them, SPEC §8.4). Polls and task
// completions are never retried: a lost poll reply may already have started a task, and a
// repeated completion would report stale_task for work that succeeded, so the caller decides.
// Only transport failures and unavailable or not_owner replies are retried.
package client
