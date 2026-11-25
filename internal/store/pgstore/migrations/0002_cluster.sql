-- 0002: the cluster's fixed configuration (SPEC §9, WI-054).
--
-- One row, ever. num_shards is chosen when the cluster is created and can never change: the
-- shard of a workflow ID is hash(id) mod num_shards, so a node that disagreed would own
-- shards that hold other runs' history. The single-row check is what makes that impossible
-- to get wrong by inserting a second row.

CREATE TABLE cluster (
    id         integer     PRIMARY KEY CHECK (id = 1),
    num_shards integer     NOT NULL CHECK (num_shards > 0),
    created_at timestamptz NOT NULL
);
