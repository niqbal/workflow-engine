// Package pgstore is the Postgres store.Store (SPEC §12, WI-030), built on pgx v5.
//
// Every write is one transaction that first checks the fence with
// SELECT ... FROM shards ... FOR SHARE, so a concurrent AcquireShard (which updates the shard
// row) either commits before the check or waits for the write to finish. Run updates
// compare-and-swap with UPDATE runs ... WHERE db_version = $expected. Task IDs come from a
// per-(shard, kind) counter row bumped in the same transaction. A unique constraint on
// (shard_id, workflow_id) enforces one run per workflow ID.
//
// The schema lives in versioned SQL files under migrations/, embedded in the binary and
// applied by Store.Migrate.
package pgstore
