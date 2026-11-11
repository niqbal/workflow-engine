-- 0001: initial schema (SPEC §12).

CREATE TABLE shards (
    shard_id         integer     PRIMARY KEY,
    owner            text        NOT NULL DEFAULT '',
    range_id         bigint      NOT NULL DEFAULT 0,
    lease_expires_at timestamptz
);

CREATE TABLE runs (
    shard_id      integer NOT NULL,
    workflow_id   text    NOT NULL,
    run_id        text    NOT NULL,
    db_version    bigint  NOT NULL,
    next_event_id bigint  NOT NULL,
    is_open       boolean NOT NULL,
    state         bytea,
    PRIMARY KEY (shard_id, workflow_id, run_id),
    -- v1 uses a workflow ID once per shard (SPEC §8.4).
    CONSTRAINT runs_one_per_workflow UNIQUE (shard_id, workflow_id)
);

CREATE TABLE history (
    shard_id    integer NOT NULL,
    workflow_id text    NOT NULL,
    run_id      text    NOT NULL,
    event_id    bigint  NOT NULL,
    data        bytea,
    PRIMARY KEY (shard_id, workflow_id, run_id, event_id)
);

-- Last task ID allocated per (shard, kind); bumped inside the write transaction.
CREATE TABLE task_counters (
    shard_id integer NOT NULL,
    kind     text    NOT NULL,
    last_id  bigint  NOT NULL,
    PRIMARY KEY (shard_id, kind)
);

CREATE TABLE transfer_tasks (
    shard_id     integer NOT NULL,
    task_id      bigint  NOT NULL,
    run_shard_id integer NOT NULL,
    workflow_id  text    NOT NULL,
    run_id       text    NOT NULL,
    payload      bytea,
    PRIMARY KEY (shard_id, task_id)
);

CREATE TABLE timer_tasks (
    shard_id     integer     NOT NULL,
    task_id      bigint      NOT NULL,
    fire_at      timestamptz NOT NULL,
    run_shard_id integer     NOT NULL,
    workflow_id  text        NOT NULL,
    run_id       text        NOT NULL,
    payload      bytea,
    PRIMARY KEY (shard_id, task_id)
);

CREATE INDEX timer_tasks_due ON timer_tasks (shard_id, fire_at, task_id);
