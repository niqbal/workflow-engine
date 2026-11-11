# Workflow Engine — Specification

> Version 0.2 · 2026-11-11 · Status: revised after independent review
> (`docs/reviews/2026-11-11-spec-review.md`; changes listed in BUILD-LOG Step 7)
> Owner: Nawab Iqbal. Implementation: hand-written core plus the software factory (`asf`)
> for well-bounded work items (see §15).

---

## 1. Purpose

Build a durable workflow engine from scratch that a staff engineer can defend line by line in
an interview: what it guarantees, how it fails, and how we know.

A **workflow** is an ordinary Go function. The engine records every decision it makes as an
**event** in an append-only **history**. If a worker or server dies, a new worker replays
the history through the same function, which deterministically reaches the same point, and
execution continues. Side effects live in **activities**, which retry by policy. **Timers**
survive restarts. **Signals** deliver external events, which is how a workflow pauses for a
human approval and resumes on a webhook.

### 1.1 Goals
1. Precisely stated guarantees (§5), each backed by an automated check.
2. A deterministic simulation harness (§11) that runs the full cluster under injected faults
   from a seed, finds bugs, and reproduces any failure exactly.
3. Horizontal scale by sharding history with lease-based ownership and fencing (§9).
4. Durable timers that survive crashes and failover (§10); one million pending timers per
   node is a stretch benchmark.
5. Safe evolution of workflow code while executions are in flight (§7.4).
6. A showcase: an LLM agent whose tool calls are activities and whose human-approval step is a
   signal, killed mid-run and resumed without repeating any side effect (§14).

### 1.2 Non-goals (v1)
Multi-region replication, namespaces and auth, child workflows, continue-as-new, queries,
a web UI, gRPC, SDKs in other languages, archival, advanced visibility (search). Child
workflows and continue-as-new are the first stretch items (§16).

### 1.3 Why this is hard
Small code, subtle correctness. The bugs that matter are rare interleavings: a worker that
crashes between doing a side effect and reporting it, two nodes that both believe they own a
shard, a timer that fires twice across a failover, workflow code that changed while a run was
in flight. The project is judged on how well it finds and prevents these, not on line count.

---

## 2. Glossary

| Term | Meaning |
|---|---|
| Workflow type | A registered Go function `func(wf.Context, In) (Out, error)` |
| Workflow execution (run) | One execution, identified by `workflowID` (caller-chosen, unique among open runs) and `runID` (engine-generated) |
| Event / history | The append-only, gap-free sequence of events for one run. Event IDs start at 1 |
| Mutable state | The server's per-run summary derived from history: next event ID, pending activities and timers, workflow-task state. Written in the same transaction as the events |
| Command | What workflow code asks for during a workflow task: schedule activity, start timer, cancel timer, record marker, complete, fail |
| Workflow task | A unit of work for a worker: "run the workflow code forward from this history and return commands" |
| Activity | A function with side effects, run by a worker, at-least-once, with retries |
| Task queue | A named queue that workers poll for workflow or activity tasks |
| Shard | A partition of runs, `hash(workflowID) mod NumShards`. All of a run's state lives in one shard |
| Fence (range ID) | A shard's ownership epoch. Every write is conditional on it |
| Transfer task | An outbox record written with events, telling the shard to push a task to a queue |
| Timer task | An outbox record for something that must happen at a time: user timer, activity timeout, workflow-task timeout, retry backoff |

---

## 3. Programming model (Go SDK)

### 3.1 Workflow API (`package wf`)

```go
type Context interface{ /* opaque; carries the coroutine and the run's state */ }

func ExecuteActivity(ctx Context, activity string, input any, opts ActivityOptions) Future
func NewTimer(ctx Context, d time.Duration) Future   // cancellable via ctx cancel
func Sleep(ctx Context, d time.Duration) error
func GetSignalChannel(ctx Context, name string) ReceiveChannel
func NewSelector(ctx Context) Selector             // wait on several futures/channels
func Go(ctx Context, fn func(Context))             // deterministic coroutine, not a goroutine
func Now(ctx Context) time.Time                    // time of the current workflow task, from history
func SideEffect(ctx Context, fn func() any) Value  // runs once, result recorded as a marker
func GetVersion(ctx Context, changeID string, min, max int) int  // §7.4
func GetInfo(ctx Context) Info                     // workflowID, runID, attempt

type Future interface{ Get(ctx Context, out any) error; IsReady() bool }
type ActivityOptions struct {
    TaskQueue              string
    ScheduleToCloseTimeout time.Duration   // whole activity incl. retries; 0 = unbounded
    StartToCloseTimeout    time.Duration   // per attempt; required
    HeartbeatTimeout       time.Duration   // stretch (§16); 0 = no heartbeats expected
    RetryPolicy            RetryPolicy
}
type RetryPolicy struct {
    InitialInterval    time.Duration // default 1s
    BackoffCoefficient float64       // default 2.0
    MaximumInterval    time.Duration // default 100 × initial
    MaximumAttempts    int           // 0 = unlimited (bounded by ScheduleToCloseTimeout if set)
    NonRetryableErrors []string      // error type names
}
```

### 3.2 Activity API (`package activity`)

```go
func GetInfo(ctx context.Context) Info  // includes Attempt and IdempotencyKey
func RecordHeartbeat(ctx context.Context, details any)  // stretch
```

`IdempotencyKey` is `workflowID/runID/scheduledEventID`. It is **stable across retries** of
the same scheduled activity and unique otherwise. Activities that call external systems pass
it on (for example as a payment provider's idempotency key). This is how the showcase avoids
double charges (§5, guarantee G4).

### 3.3 Determinism rules for workflow code
Workflow code must not read the wall clock, use `math/rand`, start goroutines, use channels
or `select`, do I/O, or iterate maps when the order affects commands. The SDK gives a
deterministic replacement for each. Violations are caught at replay by the non-determinism
check (§7.3). A static checker (`wfe vet`) is a stretch item.

### 3.4 Example

```go
func Approval(ctx wf.Context, req Request) (Result, error) {
    var draft Draft
    if err := wf.ExecuteActivity(ctx, "DraftReply", req, opts).Get(ctx, &draft); err != nil {
        return Result{}, err
    }
    sel := wf.NewSelector(ctx)
    var decision Decision
    sel.AddReceive(wf.GetSignalChannel(ctx, "approval"), func(c wf.ReceiveChannel) { c.Receive(ctx, &decision) })
    sel.AddFuture(wf.NewTimer(ctx, 72*time.Hour), func(wf.Future) { decision = Decision{Approved: false, Reason: "timeout"} })
    sel.Select(ctx)
    if !decision.Approved {
        return Result{Status: "rejected"}, nil
    }
    return Result{Status: "sent"}, wf.ExecuteActivity(ctx, "SendReply", draft, opts).Get(ctx, nil)
}
```

---

## 4. Event history

### 4.1 Event types

| Event | Key fields |
|---|---|
| `WorkflowExecutionStarted` | workflowType, input, taskQueue, executionTimeout, requestID |
| `WorkflowTaskScheduled` | taskQueue, attempt |
| `WorkflowTaskStarted` | scheduledEventID, workerIdentity |
| `WorkflowTaskCompleted` | scheduledEventID, startedEventID |
| `WorkflowTaskTimedOut` / `WorkflowTaskFailed` | scheduledEventID, cause |
| `ActivityTaskScheduled` | activityID, activityType, input, taskQueue, timeouts, retryPolicy |
| `ActivityTaskStarted` | scheduledEventID, attempt, workerIdentity |
| `ActivityTaskCompleted` | scheduledEventID, startedEventID, result |
| `ActivityTaskFailed` | scheduledEventID, startedEventID, failure, attempt |
| `ActivityTaskTimedOut` | scheduledEventID, timeoutType, attempt |
| `TimerStarted` | timerID, fireAt |
| `TimerFired` / `TimerCanceled` | startedEventID |
| `WorkflowExecutionSignaled` | signalName, payload, requestID |
| `MarkerRecorded` | markerName (`version`, `side_effect`), details |
| `WorkflowExecutionCompleted` / `Failed` / `TimedOut` / `Terminated` | result or failure, reason |

Every event carries `eventID`, `timestamp` (server clock at append) and `type`. Payloads are
JSON-encoded bytes.

### 4.2 History rules
- Event IDs are contiguous from 1. An append succeeds only if the caller's
  `expectedNextEventID` equals the stored one (optimistic concurrency).
- Activity retries are tracked in mutable state, not history. `ActivityTaskStarted` is written
  only for the attempt that ends the activity, which keeps history bounded under long retry
  loops (Temporal makes the same choice).
- At most one workflow task is outstanding per run.
- **Buffered events.** Events that arrive while a workflow task is *started* (signals,
  activity results, timer fires) are **not** appended yet. They are kept in mutable state as
  buffered events and appended **after** that task's `WorkflowTaskCompleted`, followed by a new
  `WorkflowTaskScheduled`. This keeps replay exact: every event the worker did not see sits
  after the commands it produced. Events arriving while a task is only *scheduled* are
  appended at once (the worker will see them).
- **Unhandled events block close.** If a task returns a close command (complete or fail the
  workflow) while buffered events exist, the close command is rejected: the task completes
  with no close, the buffered events are appended, and a new task is scheduled. An acknowledged
  signal is therefore always seen by workflow code before the run can close.
- **Workflow-task failures and timeouts.** The first failure or timeout of a series writes
  `WorkflowTaskFailed` or `WorkflowTaskTimedOut`. Every later attempt in the series is
  **transient**: its `WorkflowTaskScheduled` and `WorkflowTaskStarted` are not persisted but
  are appended to the history sent to the worker (with the IDs they would get). They are
  persisted only when that attempt completes. Further failures write nothing. So a run blocked
  on non-determinism (§7.3) retries forever without growing history. Attempt 2 is offered at
  once; attempt n ≥ 3 waits `min(1s · 2^(n−3), 10 min)`.
- A closed run accepts no new events except via `terminate` on an already-closed run, which is
  a no-op.

---

## 5. Guarantees

These are the contract. Each one names the check that enforces it (§11.3).

| ID | Guarantee | Enforced by |
|---|---|---|
| G1 | **Durability.** An API call that returned success (start, signal, task completion) is never lost, even if any node crashes after returning | Oracle I1, I7 |
| G2 | **History integrity.** A run's history is append-only and gap-free, and every observer sees prefixes of one sequence. No event is ever rewritten | Oracle I2 |
| G3 | **Deterministic progress.** Replaying a run's history with the same workflow code reproduces exactly the recorded commands. Workflow code runs effectively once, however many times it is replayed | Oracle I3 |
| G4 | **Activities: at-least-once, with a stable key.** Each scheduled activity runs until it succeeds, fails non-retryably, or exhausts its policy. Attempts may repeat. `IdempotencyKey` is stable across them, so an idempotent sink applies the effect exactly once | Oracle I5 |
| G5 | **Timers.** A timer fires at most once. It never fires before `fireAt − ε`, where `fireAt` is computed on the owning node's clock and ε is the configured maximum clock skew between nodes (default 1 s). It fires within a bounded delay after `fireAt` once the cluster is healthy | Oracle I4 |
| G6 | **Single writer.** For each shard, writes from any node other than the current owner are rejected, whatever the clocks say | Oracle I6 |
| G7 | **Signals.** A signal acknowledged to the client appears exactly once in history and is delivered to workflow code before the run closes. Retrying with the same `requestID` returns the original outcome, even after the run has closed, for the last 1,000 request IDs per run (the stated dedup bound). A signal to a closed run that was never accepted is rejected, not dropped silently | Oracle I7 |
| G8 | **Liveness under quiet.** Once faults stop, every open run makes progress and every workflow whose code terminates reaches a closed state. Excluded: runs blocked on non-determinism (D6), which wait for a code fix by design | Oracle I1 |

Not guaranteed: exactly-once execution of activity *code*; ordering between signals sent
concurrently by different clients; progress while the store is unavailable; G5's lower bound
if real clock skew exceeds ε. Event timestamps are clamped to be non-decreasing per run, so
`wf.Now` never goes backwards even across failover.

---

## 6. Architecture

One binary, `wfe`, runs every server role. A deployment is one or more identical nodes
sharing one store.

```
 clients ──HTTP──▶ ┌──────────── node ─────────────────────────────────────┐
 workers ──HTTP──▶ │ Frontend (API, routing to shard owner) ──Transport──▶ │ other nodes
                   │   │                                                    │
                   │   ▼                                                    │
                   │ History service, per owned shard:                      │
                   │   mutable state cache · transfer processor ·           │
                   │   timer processor · lease renewer                      │
                   │   │                                                    │
                   │   ▼                                                    │
                   │ Matching: in-memory task queues, long-poll             │
                   └───┬────────────────────────────────────────────────────┘
                       ▼
                     Store (Postgres in production, in-memory in tests and simulation)
```

- **Frontend.** Validates requests, computes the shard, forwards to the owning node (cached
  ownership map, refreshed on `ErrNotOwner`). Long-polls for tasks.
- **History service.** The only writer of history. For each owned shard it holds the fence,
  caches mutable state, and runs two processors: **transfer** (outbox: push tasks to matching)
  and **timer** (fire due timer tasks).
- **Matching.** In-memory per-node task queues. It is deliberately **not durable**: a lost task
  is re-pushed by the transfer processor (§8.3), and duplicate dispatch is harmless because
  starting a task is a conditional transition in history.
- **Workers** (in the SDK, not the server) poll, replay workflow code, run activities and
  report results. The server never runs user code.

### 6.1 The rule that makes simulation possible
No server package may call `time.Now`, `time.Sleep`, `time.After`, `rand`, `net`, or start a
goroutine directly. They receive these interfaces:

```go
type Clock interface {
    Now() time.Time
    AfterFunc(d time.Duration, f func()) Cancel
}
type Rand interface{ Int63() int64 }
type Transport interface {          // node-to-node RPC
    Call(ctx context.Context, node NodeID, method string, req, resp any) error
}
type Spawner interface{ Go(name string, fn func(ctx context.Context)) }  // tracked goroutines
```

Production wiring passes real implementations. The simulator passes a virtual clock, a seeded
RNG, an in-process network that can drop, delay and partition, and a cooperative scheduler
(§11).

**Execution model (decided, D9).** Server code is written in ordinary blocking style, on
goroutines started only through `Spawner`. Under simulation every such goroutine is gated by
the same mechanism as the workflow dispatcher (§7.1): exactly one runs at a time, and it yields
only at **yield points**: `Clock` waits, `Transport.Call`, every `Store` call, and the
blocking operations of `internal/xsync` (`Mutex`, `Chan`, `Cond`). In production those are
thin wrappers over the standard library. Rules:
- No `sync.Mutex`, `sync.WaitGroup`, bare channels or `select` in server packages; use `xsync`.
- `xsync.Mutex` may be held across a yield point; `sync` primitives may not exist at all.
- Map iteration that can affect output must go through `xsync.SortedKeys`.

**Lint (WI-003).** A test parses every file in the server scope (`internal/server`,
`internal/shard`, `internal/history`, `internal/store/memstore`, `internal/store/pgstore`)
and fails on: `time.Now/Since/Until/Sleep/After/NewTimer/Tick`, `context.WithTimeout`,
`context.WithDeadline`, `math/rand`, `net`, `sync.` except `sync/atomic`, `go` statements,
`select` statements, channel types, and `range` over a map. HTTP lives in `internal/httpapi`,
outside the scope, and only translates requests into frontend calls. `internal/engine` is
outside the scope too: it is the gate itself and is reviewed by hand.

---

## 7. Workflow execution in the worker

### 7.1 Coroutine dispatcher
Workflow code runs on goroutines that are gated so that **exactly one runs at a time** and
they always yield in the same order. A coroutine yields only at blocking SDK calls
(`Future.Get`, `Receive`, `Select`, `Sleep`). The dispatcher runs coroutines in creation order
until all are blocked, which is one deterministic "step".

### 7.2 Workflow task processing
1. The worker receives a workflow task with the full history (no sticky cache in v1).
2. It creates a fresh workflow instance and feeds events in order, in batches. A batch ends at
   each `WorkflowTaskStarted`: the dispatcher runs once per batch, after resolving the
   futures and channels for every event in it. Because of buffering (§4.2), the events in a
   batch are exactly what the worker saw when it produced that task's commands.
3. Commands produced while replaying old batches are matched against the events those commands
   produced originally (§7.3). New commands after the last `WorkflowTaskStarted` are the
   task's output.
4. The worker returns the new commands, or a completion or failure if the function returned.

### 7.3 Non-determinism detection
During replay, each command must match the next command-generated event in history by type
and identity (activity ID and type, timer ID, marker name). A mismatch fails the workflow task
with `NonDeterministicError`. The run is left blocked, **not failed**, so that a fixed deploy
can resume it. That is a deliberate choice: failing a run because of a bad deploy turns a code
bug into data loss.

### 7.4 Versioning
`GetVersion(ctx, changeID, min, max)` on first execution records a `MarkerRecorded{version,
changeID, max}` and returns `max`. On replay it returns the recorded value. If there is no
marker and replay has passed the point where it would have been written, it returns
`DefaultVersion (-1)`. This lets old runs keep the old branch while new runs take the new one:

```go
v := wf.GetVersion(ctx, "add-fraud-check", wf.DefaultVersion, 1)
if v == 1 { /* new step */ }
```

A replay test helper (`wftest.ReplayFromFile(history.json, workflowFn)`) lets authors check
that new code still replays old histories. This is the CI gate for workflow changes.

---

## 8. Server state machines

### 8.1 Write path (one transaction, always)
Every state change does the following in **one store transaction**:
1. Check the shard fence (`range_id`) still matches. If it doesn't, return `ErrFenced`.
2. Compare-and-swap the run's **`dbVersion`**, which every write increments, including writes
   that change only mutable state (retry attempts, buffered events, dedup sets). A mismatch
   returns `ErrConflict`. `expectedNextEventID` alone is not enough: state-only writes would
   race (a stale write could reset an attempt counter).
3. Append events (IDs must continue from the stored next event ID).
4. Write the new mutable state.
5. Write new transfer and timer tasks, and delete completed ones.

On **any** error the cached mutable state for that run is evicted and reloaded before a retry.
`ErrConflict` is retried up to 5 times; `ErrFenced` drops the shard.

This is the transactional outbox pattern. No queue is ever told about something that is not
already durable in history.

**Outbox reading.** Task IDs are allocated before commit, so a later ID can commit before an
earlier one. The transfer processor therefore never advances a cursor past an undeleted task:
its ack level is the lowest task ID still present, and each scan reads from the ack level.
Tasks already pushed are tracked in memory and not re-pushed until their redispatch time.

### 8.2 Workflow task lifecycle
`scheduled → started → completed | failed | timed out`. The transition to started is
conditional on (scheduledEventID, attempt). Only one poller can win, so duplicate dispatch is
harmless. A start-to-close timer task (default 10 s) times out a stuck task and reschedules
it with attempt+1. Repeated failures back off (§4.2). **Redispatch** applies to workflow
tasks exactly as to activities (§8.3): a scheduled-but-not-started workflow task is re-pushed
every `redispatchInterval`, so one lost from in-memory matching cannot strand the run.

### 8.3 Activity task lifecycle
`scheduled → started(attempt n) → completed | failed(retry?) | timed out(retry?)`.
- Schedule-to-start timeouts are deferred (§16); redispatch covers lost tasks.
- **Start-to-close** timeout: a timer task armed at start.
- **Schedule-to-close** timeout: bounds all attempts together; when it fires the activity
  times out for good.
- **Every timeout task is bound to what armed it.** It carries `(scheduledEventID, attempt)`
  for activities and workflow tasks, and the `TimerStarted` event ID for user timers. Firing is
  a no-op unless the referenced item is still pending **and** at that attempt. An old attempt's
  start-to-close task can therefore never time out a newer attempt.
- Heartbeats and heartbeat timeouts are a stretch item (§16).
- **Retry**: on a retryable failure or timeout, mutable state records the attempt and a
  backoff timer task is written. When it fires, a new transfer task re-dispatches.
- **Redispatch**: a scheduled-but-not-started task has its transfer task re-pushed every
  `redispatchInterval` (default 30 s). This recovers tasks lost from in-memory matching.
- Completion carries a task token `{shard, workflowID, runID, scheduledEventID, attempt}`.
  A completion for a stale attempt is rejected with `ErrStaleTask`.

### 8.4 Start, signal, terminate
- **Start**: the `requestID` is checked against the **latest run for that workflowID,
  open or closed**. Same `requestID`: return success with that runID, even if it has since
  finished. Without this, a lost response plus a fast-finishing run would let a retry start a
  second run with new idempotency keys, and the showcase would refund twice. Different
  `requestID` and any run exists: return `AlreadyStarted` (v1 ID-reuse policy: a workflowID
  is used once).
- **Signal**: the dedup check runs **before** the closed-run check. A `requestID` already
  in the run's dedup set (kept after close, last 1,000 per run) returns the original outcome.
  Otherwise a closed run rejects the signal. An open run appends `WorkflowExecutionSignaled`
  (or buffers it, §4.2) and records the `requestID` in the same write.
- **Terminate**: append `WorkflowExecutionTerminated`, then drop pending timers and tasks.
- **Execution timeout**: `executionTimeout` on start writes a timer task of kind
  `workflow_timeout`. Firing closes the run with `WorkflowExecutionTimedOut` if still open.

---

## 9. Sharding and ownership

- `NumShards` is fixed when the cluster is created (default 64) and stored in the store.
  Changing it is out of scope.
- **Shard row**: `(shard_id, owner, range_id, lease_expires_at)`.
- **Acquire**: a node increments `range_id` with a compare-and-swap if the lease has expired
  (or it is the owner). The new `range_id` is its fence.
- **Every write** in §8.1 checks `range_id` inside the transaction. A former owner with a
  paused process, a slow clock or a network split can do nothing harmful: its writes fail with
  `ErrFenced`, and it drops the shard.
- **Leases are for liveness, fences are for safety.** Expiry only decides *when* another node
  may try to take over. Correctness never depends on clocks agreeing.
- **Placement (v1)**: static peer list in config. A node acquires shards whose lease has
  expired, up to `ceil(NumShards / configured nodes)` plus a failover allowance, so a dead
  node's shards are taken over. Voluntary rebalancing when a node returns is a stretch item.
- **Task queue placement.** Matching is per node, so a task queue must live on exactly one
  node: queue `q` lives on the owner of shard `hash(q) mod NumShards`. History services push
  tasks there over `Transport`, and frontends forward polls there. Ownership changes move the
  queue with the shard. Its in-memory contents are lost, which redispatch repairs (§8.2-8.3). A node that cannot renew within
  `leaseDuration` stops serving the shard before its lease can expire (it uses a margin).
- **Shard load**: on acquire, the node loads mutable state lazily per run, and loads transfer
  and timer tasks from the store.

---

## 10. Timers

- All time-based work is a timer task row: `(shard_id, fire_at, task_id, kind, run, ref)`.
- Each shard's timer processor keeps an in-memory min-heap of tasks due in the next
  `loadAheadWindow` (default 60 s). It reloads that window from the store periodically and
  whenever a new task falls inside it.
- Firing is a normal write (§8.1) that appends the event (for example `TimerFired`) only if the
  task's reference is still pending at the same attempt (§8.3), then deletes the timer task. A
  re-fire after a crash finds the timer no longer pending and does nothing. That gives G5's
  at-most-once.
- `fireAt` is computed on the owner's clock when the timer is armed. After failover, the new
  owner fires by its own clock, hence the ε in G5. Event timestamps are
  `max(previous event timestamp, now)` per run.
- **Scale target**: 1 million pending timers per node, with the heap holding only the load-ahead
  window. Benchmark it in M5.

---

## 11. Deterministic simulation testing

### 11.1 Harness
`sim.Run(seed, workload, faults) Result` runs N nodes, an in-memory store, and simulated
workers, all inside one OS thread under a cooperative scheduler driven by:
- a **virtual clock** that jumps to the next scheduled event (hours of virtual time run in
  seconds);
- a **seeded RNG** that decides every interleaving, delay and fault;
- the gated goroutines of §6.1 (D9), which yield only at Clock, Transport, Store and `xsync`
  calls;
- the **simulated network** from §6.1.

Same seed, same code: the same execution, event for event. A failing seed is a reproducible
bug report.

### 11.2 Faults
| Fault | How |
|---|---|
| Node crash and restart | Drop all in-memory state of a node (caches, heaps, matching queues). Restart it after a random delay |
| Network | Drop or delay messages; partition sets of nodes for a while. (Duplication and reordering are stretch items) |
| Process pause | Stop scheduling one node for longer than the lease, then resume it (the stale-owner case) |
| Clock skew | Per-node offsets and drift on the virtual clock |
| Store faults | Transient errors; ambiguous commits (commit, then report an error); **variable commit latency**, so a later task ID can commit before an earlier one (exercises the outbox ack level, §8.1) |
| Worker crash | Kill a worker after an activity's side effect but before it reports completion |

Every run ends with a **quiet phase**: faults stop, and the simulation continues until all
workflows close or a deadline passes. That is what makes the liveness check (G8) meaningful.

### 11.3 Oracle
Checked on every simulated run:

| ID | Check |
|---|---|
| I1 | Every successfully started workflow is closed after the quiet phase |
| I2 | Every history snapshot observed at any time is a prefix of the final history. IDs are contiguous |
| I3 | Replaying each final history with the workflow code reproduces exactly the recorded commands |
| I4 | Each `TimerStarted` has at most one `TimerFired`, never before `fireAt` |
| I5 | The side-effect ledger shows each idempotency key applied exactly once. Attempts recorded ≥ effects applied |
| I6 | From a **ghost log kept outside the store** (every committed write recorded as node, fence, operation): no committed write carried a fence other than the shard's current `range_id` at that moment. Checking the store's own view would be circular |
| I7 | Every acknowledged signal appears exactly once in history. Every rejected signal targeted a closed run |
| I8 | Each workflow's result equals the reference model applied **to its final history** (which signal, timer or result won a race is read from history, so the check does not depend on timing) |

### 11.4 Workloads
A mix of: sequential activities, parallel fan-out and fan-in with selectors, long timers,
signal-wait with timeout, failing activities with retry, non-retryable errors, version changes
mid-run (code swapped during the simulation), and the agent workflow from §14 with a scripted
model.

### 11.5 Running it
- **From M1:** a run-twice trace check (same seed, two runs, byte-identical traces) gates
  every change, including every factory job. Finding in M3 that factory code cannot run under
  the scheduler would mean a rewrite.
- `go test ./sim -seeds 1-200` runs in CI on every change from M3.
- `wfe sim --seeds A-B --parallel N` runs larger sweeps by hand. Failures are written as
  `sim/failures/<seed>.txt` with the reproduction command. (A nightly 100k sweep is a stretch item.)
- **Bug log.** Every bug the simulator finds is written up in `docs/SIM-BUGS.md`: seed,
  interleaving, root cause, fix. These are the project's best interview material.

---

## 12. Storage

```go
type Store interface {
    // Shards
    GetShard(ctx context.Context, shard int) (ShardInfo, error)
    AcquireShard(ctx context.Context, shard int, owner string, now time.Time, lease time.Duration) (Fence, error)
    RenewShard(ctx context.Context, f Fence, now time.Time, lease time.Duration) error

    // Runs and history: one atomic write per state change (§8.1). Events and state are
    // opaque bytes to the store.
    CreateRun(ctx context.Context, f Fence, r NewRun) ([]Task, error)
    UpdateRun(ctx context.Context, f Fence, key RunKey, u Update) (newDBVersion int64, written []Task, err error)
    GetRun(ctx context.Context, key RunKey) (RunSnapshot, error)  // DBVersion, NextEventID, Open, State
    GetLatestRun(ctx context.Context, shard int, workflowID string) (RunKey, error)  // open or closed
    ReadHistory(ctx context.Context, key RunKey, fromID int64, limit int) ([]EventRecord, error)

    // Outbox
    ReadTransferTasks(ctx context.Context, shard int, afterTaskID int64, limit int) ([]Task, error)
    ReadTimerTasks(ctx context.Context, shard int, upTo time.Time, limit int) ([]Task, error)
    DeleteTasks(ctx context.Context, f Fence, keys []TaskKey) error
}
// Errors: ErrFenced, ErrConflict, ErrNotFound, ErrAlreadyExists, ErrInvalid.
// The authoritative definition is internal/store/store.go.
```

- **storetest**: one contract test suite in `internal/store/storetest` that every
  implementation must pass, including concurrency, fencing and conflict cases.
- **memstore**: for unit tests and the simulator. Can inject faults, including the ambiguous
  commit.
- **pgstore**: Postgres with `pgx`. Tables `shards`, `runs`, `history`, `transfer_tasks`,
  `timer_tasks`. Fence checks use `SELECT range_id FROM shards WHERE shard_id=$1 FOR SHARE`
  inside the transaction, plus `UPDATE runs ... WHERE next_event_id = $expected`.
  Migrations are versioned SQL files.

---

## 13. API, CLI and observability

### 13.1 HTTP/JSON API (`/v1`)
| Method and path | Purpose |
|---|---|
| `POST /v1/workflows` | Start: `{workflowID, workflowType, taskQueue, input, requestID, executionTimeout}` returns `{runID}` |
| `POST /v1/workflows/{id}/signal` | `{signalName, payload, requestID}` |
| `POST /v1/workflows/{id}/terminate` | `{reason}` |
| `GET /v1/workflows/{id}` | Describe the latest run: status, pending activities and timers |
| `GET /v1/workflows/{id}/history` | Full history, JSON |
| `POST /v1/tasks/workflow/poll` | Long-poll (30 s) for a workflow task on a queue |
| `POST /v1/tasks/workflow/complete` · `/fail` | Return commands, or fail with a cause |
| `POST /v1/tasks/activity/poll` | Long-poll for an activity task |
| `POST /v1/tasks/activity/complete` · `/fail` | Report an activity outcome (`/heartbeat` is stretch) |
| `GET /healthz`, `GET /metrics` | Health and Prometheus |

Errors are JSON `{code, message}`, with codes `not_found`, `already_started`,
`stale_task`, `workflow_closed` (signal to a closed run), `not_owner` (retried internally),
`invalid_argument`, `unavailable`. The service contract is `internal/server/api`.
An activity failure is non-retryable if its type is in the policy's `NonRetryableErrors`
or the activity sets `NonRetryable`.

### 13.2 CLI
`wfe server --config wfe.yaml` · `wfe workflow start|signal|terminate|describe|history` ·
`wfe sim --seeds A-B` · `wfe bench <scenario>`.

### 13.3 Observability
Prometheus metrics: workflow tasks and activity tasks per second, schedule-to-start latency,
end-to-end step latency, history append latency, `ErrConflict` and `ErrFenced` rates, shards
owned, pending timers, timer lag (fire time minus `fireAt`), redispatch count. OpenTelemetry
traces from API call through history write to worker completion are a stretch item. Structured logs (`log/slog`)
with workflowID, runID and shard on every line.

---

## 14. Showcase: a crash-proof LLM agent

`examples/agent`: a support agent workflow.
1. Activity `Plan` calls the Claude API with the ticket and the available tools.
2. For each tool call the model requests, the workflow runs a tool activity (`LookupOrder`,
   `IssueRefund`). `IssueRefund` passes the `IdempotencyKey` to a fake payment ledger.
3. Refunds above a threshold wait for a `approval` signal, with a 24-hour timer as fallback.
4. The model's final reply is sent by `SendReply`.

The demo script (`examples/agent/demo.sh`) starts the workflow, kills the worker and a server
node in the middle of a refund, restarts them, approves via `curl`, and shows: the run
finished, the history, and the payment ledger with **exactly one** refund. The claim is
exactly-once **effects at idempotent sinks** (the ledger, and the fake mail sink for
`SendReply`, both keyed by `IdempotencyKey`). `Plan` may call the model more than once after
a crash; that is acceptable and stated in the demo. Tests run the same
flow with a scripted model and no network.

---

## 15. Building it with the software factory

The factory (`~/work/software-factory`, command `asf`) takes small work items with acceptance
criteria and delivers verified branches. It is good at bounded, well-specified work and is
held back by three rules that this plan uses deliberately:

1. **The factory may not edit existing tests above tier 0.** So the human writes the
   contract tests first (storetest, the oracle checks, the determinism lint, the replay
   tests). Factory implementations must pass them and cannot weaken them.
2. **Triage flags epics, and there is no planner.** So every work item is pre-cut to a
   size one implementer can finish (docs/WORK-ITEMS.md).
3. **Gates compare against the base commit.** So the repository needs a green skeleton
   (`go build`, `go vet`, `go test`) before the first job.

### 15.1 Who builds what
| Owner | Scope | Why |
|---|---|---|
| Human (with Claude Code) | M0 skeleton and interfaces, every contract and scenario test, the deterministic runtime, the coroutine dispatcher, the replayer, the `wf` SDK, mutable-state transitions, fencing in pgstore, the simulator core, oracle and fault injectors, shard leases, GetVersion | These are the parts the guarantees rest on, or (fault injectors) whose weakness would hide bugs silently. Writing them is also the learning |
| Factory | Work judged by tests that already exist: memstore and pgstore (storetest), matching, the history service write paths, transfer and timer processors (contracttest), HTTP API, client, worker, CLI, routing, metrics, workloads, the showcase | Bounded, testable, and never self-graded |

### 15.2 Factory configuration (applied after M0)
- `tiers.rules`: tier 3 (human approval): `internal/engine/**`, `internal/history/**`,
  `internal/shard/**`, `internal/xsync/**`, `internal/store/storetest/**`, `wf/**`,
  `sim/**`, `contracttest/**`. Tier 2: `internal/server/**`, `internal/store/**`,
  `internal/httpapi/**`, `worker/**`, `client/**`. Tier 1: `cmd/**`, `examples/**`,
  `bench/**`, `docs/**`. Every package path in §6 matches a rule.
- **No self-graded work.** Every factory item is judged by human-written contract or scenario
  tests that exist before the item is queued (WORK-ITEMS.md). The factory's tester may add
  tests; it cannot be the only source of them.
- Tier is set by these path rules only, never by a `tier:N` label on the item: in `asf` a
  label is a human override that beats the rules, so it would stop a diff that strays into
  `internal/engine/**` from being raised to tier 3.
- Intake is the local-file adapter pointed at `factory/ready/`. Items wait in
  `factory/backlog/` and are moved to `ready/` only when their dependencies are merged,
  because the factory does not order work.
- Shadow mode stays on: every job ends as a local branch that the human reviews and merges.
- Gates: `go build ./...`, `go vet ./...`, `go test ./... -race`, the run-twice trace check
  from M1, plus `go test ./sim -seeds 1-50` from M3 on.
- Budget: set `budgets.monthly_cap_usd` explicitly. Earlier factory jobs cost about $1–2 each.
  Expect more for server work. **The cap is the owner's call**.

---

## 16. Milestones

| Milestone | Outcome | Exit check |
|---|---|---|
| M0 Skeleton (human) | Repo, interfaces from §6.1 and §12, `xsync`, event types, empty packages, contract-test and scenario-test suites, lint, CI | `go build/vet/test` green; contract tests compile and fail against stubs |
| M1 Single node, in memory | Deterministic runtime and run-twice trace check, start, workflow tasks with buffering, activities with retry, timers, signals, worker SDK, HTTP API, minimal CLI | §3.4 example runs end to end; scenario tests pass; traces identical across runs |
| M2 Durable | pgstore passes storetest; server restart recovers timers and tasks | Kill -9 the server mid-run; every run still completes |
| M3 Simulation | Harness, faults, oracle I1–I8, seed sweep in CI | 10,000 seeds clean, after fixing whatever it finds |
| M4 Sharding (2–3 nodes) | Leases, fences, failover placement, request and task-queue routing | Simulation with node crashes, pauses and partitions clean over 10,000 seeds; I6 holds |
| M5 Versioning and metrics | GetVersion and the replay CI helper, Prometheus metrics | A versioned code change mid-simulation passes |
| M6 Showcase | Agent example, demo script, write-ups | Demo recorded; README results table |
| Stretch | Rebalancing, heartbeats, schedule-to-start timeouts, 1M-timer benchmark, OpenTelemetry, message duplication and reordering faults, nightly 100k sweep, full CLI, child workflows, continue-as-new, sticky cache, `wfe vet` | — |

**Scope rule.** About three months part-time covers M0–M6 as cut above. The stretch list is
what was cut to make that true (review §5).

Benchmark targets are set **after** the M2 baseline. Publishing guesses as targets would be
the kind of claim this project exists to avoid.

---

## 17. Decisions

| # | Decision | Alternatives | Reason |
|---|---|---|---|
| D1 | Go | Rust, Java | Fastest path to a working distributed system. Temporal and Weaviate are Go. The factory's setup agent supports Go |
| D2 | Postgres as the only production store | Cassandra, FoundationDB, own log | Transactions make the outbox and fencing simple and auditable. The interesting problems are above the store |
| D3 | HTTP/JSON API | gRPC | Debuggable with `curl`, no codegen. gRPC can come later behind the same handlers |
| D4 | Matching is in memory, recovered by redispatch | Durable matching (Temporal) | One fewer durable component. Correctness comes from conditional starts in history |
| D5 | Worker replays full history every task (no sticky cache) | Sticky cache | Simpler and always exercises replay, which surfaces non-determinism early. The cache is a stretch item |
| D6 | Non-determinism blocks a run instead of failing it | Fail the run | A bad deploy should never become lost work |
| D7 | Fences for safety, leases for liveness | Trust lease expiry | Clocks lie. Fencing makes stale owners harmless |
| D8 | Simulation-first architecture (§6.1) | Add tests later | Retrofitting determinism into a concurrent server is close to a rewrite |
| D9 | Server code in blocking style on gated goroutines that yield at Clock, Transport, Store and `xsync` calls | Event-loop actors; free goroutines | Same mechanism as the workflow dispatcher, ordinary code for factory items, and fully deterministic |
| D10 | Task queue lives on the owner of shard `hash(queue)` | Durable matching; fan-out polls | Reuses shard ownership; redispatch repairs what a move loses |
| D11 | Per-run `dbVersion` compare-and-swap on every write | Event-ID check only | State-only writes (retries, buffering, dedup) must be ordered too |
| D12 | Buffer events during a started workflow task | Append immediately | Keeps replay exact and makes "a signal is seen before close" provable |

---

## 18. Open questions
1. Upgrade Go to ≥ 1.22 on the build machine (currently 1.19.5) for `log/slog` and current
   tooling. Needed before M0.
2. pgstore under simulation: the simulator always uses memstore. pgstore is covered by
   storetest, the two-writer stress test and the kill -9 restart test. Is that enough, or do
   we also want a slower simulation mode against a real Postgres? Revisit after M3.
