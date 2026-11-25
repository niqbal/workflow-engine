# workflow-engine

[![CI](https://github.com/niqbal/workflow-engine/actions/workflows/ci.yml/badge.svg)](https://github.com/niqbal/workflow-engine/actions/workflows/ci.yml)

A durable workflow engine written from scratch in Go, in the style of Temporal. A workflow is
an ordinary Go function; the engine records every decision it makes as an event in an
append-only history, so a crash at any point resumes exactly where it stopped. Activities
retry, timers survive restarts, and signals let a workflow pause for a human and resume on a
webhook.

The project exists to make one thing checkable rather than claimed: **correctness under
failure**. Every component takes its clock, randomness, concurrency and network from
interfaces, and a lint running inside `go test` keeps the server packages free of
`time.Now`, `math/rand`, `net`, bare goroutines, channels and map iteration. That is what lets
the engine run under a deterministic scheduler where the same seed reproduces the same
execution event for event.

No external dependencies beyond a Postgres driver. Go 1.27.

## A workflow

This is the human-approval example from the spec, and it is live code —
[`worker/worker_test.go`](worker/worker_test.go) runs it end to end against an in-process
server for the approved, rejected and timed-out paths, and compares the resulting history
against goldens in [`wf/testdata`](wf/testdata).

```go
func Approval(ctx wf.Context, req Request) (Result, error) {
	var draft Draft
	if err := wf.ExecuteActivity(ctx, "DraftReply", req, opts).Get(ctx, &draft); err != nil {
		return Result{}, err
	}

	sel := wf.NewSelector(ctx)
	var decision Decision
	sel.AddReceive(wf.GetSignalChannel(ctx, "approval"), func(c wf.ReceiveChannel) {
		c.Receive(ctx, &decision)
	})
	sel.AddFuture(wf.NewTimer(ctx, 72*time.Hour), func(wf.Future) {
		decision = Decision{Approved: false, Reason: "timeout"}
	})
	sel.Select(ctx)

	if !decision.Approved {
		return Result{Status: "rejected:" + decision.Reason}, nil
	}
	return Result{Status: "sent"}, wf.ExecuteActivity(ctx, "SendReply", draft, opts).Get(ctx, nil)
}
```

Kill the worker while that timer is pending, restart it a day later, and the run picks up at
the `Select` with the timer still armed. Nothing about the function changes to make that work.

## Guarantees

The eight guarantees are stated precisely in [docs/SPEC.md §5](docs/SPEC.md), each one naming
the check that enforces it. In short:

| | Guarantee |
|---|---|
| G1 | **Durability.** A call that returned success is never lost, whatever crashes next |
| G2 | **History integrity.** Append-only, gap-free, never rewritten; every observer sees a prefix of one sequence |
| G3 | **Deterministic progress.** Replaying a history reproduces exactly the recorded commands; workflow code runs effectively once however often it replays |
| G4 | **Activities: at-least-once with a stable key.** `IdempotencyKey` is stable across attempts, so an idempotent sink applies the effect exactly once |
| G5 | **Timers.** Fire at most once, never before `fireAt − ε` for a configured maximum clock skew, within a bounded delay once healthy |
| G6 | **Single writer.** Per shard, writes from any node but the current owner are rejected, whatever the clocks say. Leases decide only who *tries*: a node that cannot renew stops serving before its lease expires, and the fence is what makes being wrong about it harmless |
| G7 | **Signals.** An acknowledged signal appears exactly once in history and is delivered before the run closes; retries with the same `requestID` return the original outcome |
| G8 | **Liveness under quiet.** Once faults stop, every open run makes progress and every terminating workflow closes |

The spec is equally explicit about what is *not* guaranteed: exactly-once execution of
activity *code*, ordering between concurrent signals from different clients, progress while
the store is down, and G5's lower bound if real skew exceeds ε.

## How it is checked

| Mechanism | What it covers |
|---|---|
| **Determinism lint** ([`internal/lint`](internal/lint)) | Runs as a normal test. Rejects `time.Now`, `time.Since`, `context.WithTimeout`, `math/rand`, `net`, `sync` (bar `sync/atomic`), `go` statements, channels, `select` and map `range` anywhere in the server, shard, history and store packages |
| **Store contract suite** ([`internal/store/storetest`](internal/store/storetest)) | One suite of 15 cases, run against both the in-memory store and Postgres: fencing, stale-version conflicts, event-ID continuity, atomic task completion, concurrent writers |
| **Scenario contracts** ([`contracttest`](contracttest), `internal/server/contract_test.go`) | 26 end-to-end scenarios over the server, fixed up front as the definition of correct behaviour. Agents working in this repo are forbidden to edit them |
| **Replay tests** ([`internal/engine/replay_test.go`](internal/engine/replay_test.go)) | Histories replay to identical command sequences; non-determinism is detected and blocks the run rather than corrupting it |
| **Crash-recovery simulation** ([`internal/server/recovery_sim_test.go`](internal/server/recovery_sim_test.go)) | Server crash and restart under virtual time, with the old incarnation's writes fenced out |
| **Store fault injection** ([`internal/store/memstore/faults_test.go`](internal/store/memstore/faults_test.go)) | Store errors, ambiguous commits and commit-latency reordering at the outbox |
| **Cluster harness** ([`sim`](sim)) | N nodes, simulated workers, virtual clock and quiet phase; 20 runs of a seed produce byte-identical traces, and 30 days of virtual timers finish in under 5 s |
| **The oracle** ([`sim/oracle.go`](sim/oracle.go)) | Invariants I1-I8 on every simulated run: runs close, history is append-only and never rewritten, replay reproduces the recorded commands, timers fire once and never early, each idempotency key is applied once, no write escapes its shard's fence, acknowledged signals appear exactly once, and results match a reference model derived from the same history |
| **Fault injection** ([`sim/faults.go`](sim/faults.go)) | Node crash and restart, process pauses that outlast a lease, failed writes, ambiguous commits, commit delays that reorder the outbox, per-node clock skew, and workers that die after a side effect — all from the seed, all stopping at the quiet phase |
| **Cluster routing** ([`sim/network.go`](sim/network.go)) | Nodes on a simulated network: leases move shards, every client and worker call is forwarded to the owner, and calls or replies are lost in either direction. `--workload routed --seeds 1-2000` is clean |
| **Seed sweeps** (`wfe sim --seeds 1-200`) | Seven workloads, including the agent showcase, under every fault at once. A failing seed is written to `sim/failures/<seed>.txt` with the verdict, the fault set and the trace |
| **The showcase's own test** ([`examples/agent/agent_test.go`](examples/agent/agent_test.go)) | A worker is killed after the refund reaches the ledger and before it reports; the next worker finishes the run and the ledger still holds exactly one payment |
| **Timestamp monotonicity, token signing, HTTP transport, metrics** | Covered by their own package tests |

Everything above runs on every push, with `-race`, against both stores. See
[Status](#status) for what is not built yet.

The simulator has already earned its keep. It found a liveness bug that left one run in ten
stuck forever under injected faults: a store that commits a write and then reports it as
failed left a shard's outbox undispatched, because the loops are woken by successful writes
and wait without a deadline when they know of nothing due. Isolating the faults one at a time
narrowed a seven-fault interaction to that single cause.
[docs/SIM-BUGS.md](docs/SIM-BUGS.md) has the seeds, the mechanism, the fix and the design
alternative that was rejected.

## Try it

```bash
make test
```

```bash
make contract
```

That prints `passing scenarios: 26`. For the race detector and vet as CI runs them:

```bash
make ci
```

Sweep seeds through the simulator, with every fault injected and every invariant checked:

```bash
go run ./cmd/wfe sim --seeds 1-200 --workload basic
```

Run a cluster of two nodes over one Postgres, kill one of them, and check what survived:

```bash
PG_URL='postgres://localhost:5432/postgres?sslmode=disable' scripts/cluster-smoke.sh
```

The Postgres suite is skipped unless you point it at a database:

```bash
PG_URL="postgres://localhost:5432/postgres?sslmode=disable" make test
```

A server and a workflow by hand:

```bash
go run ./cmd/wfe server --listen 127.0.0.1:7233
```

```bash
go run ./cmd/wfe workflow start order/1 --workflow-type Approval --task-queue q --input '{"Ticket":"T-1"}'
```

```bash
go run ./cmd/wfe workflow history order/1
```

```
ID  Time                         Type                      Attrs
1   2026-11-24T19:50:05.046395Z  WorkflowExecutionStarted  {"workflowType":"Approval","input":{"Ticket":"T-1"},"taskQueue":"q"}
2   2026-11-24T19:50:05.046395Z  WorkflowTaskScheduled     {"taskQueue":"q","attempt":1}
```

The run waits there until a worker registered for queue `q` polls for the task — build one
with [`worker.New`](worker/worker.go), as `worker/worker_test.go` does. Prometheus metrics are
at `/metrics` on the same listener.

## The showcase: an agent that cannot refund twice

[`examples/agent`](examples/agent) is a support agent whose tool calls are activities and
whose human approval is a signal. The interesting part is not that it calls tools; it is that
it cannot lose or repeat a refund.

```bash
PG_URL=postgres://localhost:5432/postgres?sslmode=disable examples/agent/demo.sh
```

The demo kills things at the two moments that matter. First, while the run is parked waiting
for a human, it kills **the server and the worker**, restarts both, and shows the run still
open with its 24-hour timer armed. Then a reviewer approves the refund with `curl`, the worker
applies the money to the payment ledger and stops before reporting it, and the demo kills the
worker **in exactly that window** — the one place where an at-least-once activity could charge
a customer twice.

The history tells the rest. Note the worker identity changing across the restarts, and
`"attempt":2` on the refund that the killed worker had already applied:

```
17  TimerStarted                {"timerID":"t1","fireAt":"2026-11-26T03:32:24Z"}      <- parked for a human
18  WorkflowExecutionSignaled   {"signalName":"approval","payload":{"approved":true}}  <- curl, after the restart
22  ActivityTaskScheduled       {"activityType":"IssueRefund","input":{"cents":18900}}
23  ActivityTaskStarted         {"attempt":2,"workerIdentity":"worker-8560"}           <- a third worker
24  ActivityTaskCompleted       {"scheduledEventID":22,"startedEventID":23}
34  WorkflowExecutionCompleted  {"result":{"status":"replied","refundedCents":18900}}
```

```
== The payment ledger
2026-11-24T22:32:24Z  A-2002   $189.00  ticket-1795559541/0707a01d6329783f/22

1 entry, $189.00 in total
```

One refund, after two kills and three workers. The key in that ledger line is the activity's
`IdempotencyKey` — `workflowID/runID/scheduledEventID` — which is the same for every attempt,
so the sink applies the refund once and every later attempt is a no-op.

What is *not* claimed: that the model was called once. A crash can repeat the `Plan` activity,
which costs money rather than correctness, and the demo says so. The claim is exactly-once
effects at idempotent sinks.

The same agent runs under the simulator as one of its workloads, so the claim is checked by
the oracle on every seed and not only by its own demo:

```bash
go run ./cmd/wfe sim --workload agent --seeds 1-100
```

## Architecture

```
examples/agent/ The showcase: the agent workflow, its sinks and the demo
wf/            Workflow SDK: Context, ExecuteActivity, NewTimer, Selector, signal channels
worker/        Polls task queues, replays workflow code, runs activities
client/        HTTP client for the public API, with retries
cmd/wfe/       CLI: server, workflow start/signal/history
internal/
  history/     Event types and the state machine that validates every transition
  engine/      Replay: drives workflow code forward from a history, collects commands
  server/      Task dispatch, timers, recovery, signed task tokens, the cluster router
  shard/       Shard ownership: leases, placement, the stop-serving margin
  cluster/     wfe.yaml: a node's name, peers, store and key, and every refusal
  store/       Store interface; memstore and pgstore implementations, one contract suite
  transport/   Node-to-node HTTP
  clock,rnd,xsync,simrt/  The seams that make the engine simulatable
  lint/        The determinism lint
sim/           Deterministic cluster harness
```

A run's state lives in one shard, `hash(workflowID) mod NumShards`. Each shard has an
ownership epoch (a fence); every write is conditional on it, so a paused node that wakes up
believing it still owns a shard cannot write. A task queue lives on the owner of
`hash(queue)`, which is usually a different node from the run's, so a task is handed to the
queue's owner, served to a worker from there, and started back on the run's owner — any hop
may fail, and the shard's outbox plus redispatch is what makes that safe. Events and the mutable state summary commit in
one transaction together with an outbox record, which is what makes a task and the history it
refers to impossible to disagree.

## Status

Built and green: the single-node engine end to end (M0–M2), durable Postgres storage, crash
recovery, HTTP API and Go client, node-to-node HTTP transport, CLI, Prometheus metrics,
signed task tokens, and the whole of M3 — the deterministic cluster harness, the oracle, the
fault injectors, the workload mix and the seed sweep — M6, the agent showcase with its demo,
and most of M4: shard leases with placement and a stop-serving margin, and a frontend that
routes every call to the node that owns the shard or holds the task queue. 38 of 41 work
items are merged; ~30k lines of Go, of which more than half are tests.

Not built yet, and deliberately named rather than implied:

- **Message-level network faults.** The simulated cluster's nodes talk only through the shared
  store, so there are no inter-node messages to drop, delay or partition yet; a paused node is
  the observable equivalent of one partitioned from the store. Node-to-node HTTP exists
  (WI-053) but is not on the simulator's path, and the injectors for it belong with the
  routing work below.
- **A run on two real machines.** The cluster config, the smoke test and the runbook are
  built and the smoke test passes with two nodes on one host; the laptop-and-mini run the
  design was written for has not happened yet.
- `GetVersion` (WI-060), shard leases and placement (WI-050), the one-million-timer
  benchmark, and everything in SPEC §1.2's non-goals.

[docs/BUILD-LOG.md](docs/BUILD-LOG.md) records every step and decision in order, including
the ones that were wrong.

## How this was built

The spec, the contracts, the determinism lint and the core — `internal/history`,
`internal/engine`, `internal/server/api`, the `Store` interface — are hand-written. Most
bounded work items were then implemented by an agent factory
([`asf`](https://github.com/niqbal/software-factory)) against the acceptance criteria in
[docs/WORK-ITEMS.md](docs/WORK-ITEMS.md), under the rules in [AGENTS.md](AGENTS.md): agents
may not touch the test contracts, must route any change to hand-owned code through an
`OWNER REVIEW:` summary that blocks the job for me, and must obey the determinism lint, which
fails their build before I see it. Each item arrives as a branch I review and merge; the
history in this repository is that review trail.

The gates were built first, on purpose. The interesting engineering is in deciding what
"correct" means tightly enough that generated code cannot quietly be wrong — and in the parts
where that was not possible, which is why the oracle and the fault injectors are owner-only
work items.

## Documents

| Document | What it holds |
|---|---|
| [docs/SPEC.md](docs/SPEC.md) | The specification: model, guarantees, architecture, storage, sharding, simulation, milestones |
| [docs/WORK-ITEMS.md](docs/WORK-ITEMS.md) | The spec cut into work items with acceptance criteria |
| [docs/BUILD-LOG.md](docs/BUILD-LOG.md) | Every step taken, in order, with decisions and their reasons |
| [docs/MULTI-MACHINE.md](docs/MULTI-MACHINE.md) | Design for a cluster across machines |
| [docs/RUNNING-A-CLUSTER.md](docs/RUNNING-A-CLUSTER.md) | Running one: config, ports, clocks, what a failover looks like |
| [docs/SIM-BUGS.md](docs/SIM-BUGS.md) | Every bug the simulator found: seed, mechanism, fix |
| [docs/REVIEWING.md](docs/REVIEWING.md) | How factory jobs are reviewed and steered |
| [AGENTS.md](AGENTS.md) | The rules agents work under |

## License

MIT. See [LICENSE](LICENSE).
