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
| G6 | **Single writer.** Per shard, writes from any node but the current owner are rejected, whatever the clocks say |
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
| **Fault injection** ([`internal/store/memstore/faults_test.go`](internal/store/memstore/faults_test.go)) | Store errors, ambiguous commits and commit-latency reordering at the outbox |
| **Cluster harness** ([`sim`](sim)) | N nodes, simulated workers, virtual clock and quiet phase; 20 runs of a seed produce byte-identical traces, and 30 days of virtual timers finish in under 5 s |
| **Timestamp monotonicity, token signing, HTTP transport, metrics** | Covered by their own package tests |

Everything above runs on every push, with `-race`, against both stores. See
[Status](#status) for the checks that are specified but not yet built.

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

## Architecture

```
wf/            Workflow SDK: Context, ExecuteActivity, NewTimer, Selector, signal channels
worker/        Polls task queues, replays workflow code, runs activities
client/        HTTP client for the public API, with retries
cmd/wfe/       CLI: server, workflow start/signal/history
internal/
  history/     Event types and the state machine that validates every transition
  engine/      Replay: drives workflow code forward from a history, collects commands
  server/      Shard ownership, task dispatch, timers, recovery, signed task tokens
  store/       Store interface; memstore and pgstore implementations, one contract suite
  transport/   Node-to-node HTTP
  clock,rnd,xsync,simrt/  The seams that make the engine simulatable
  lint/        The determinism lint
sim/           Deterministic cluster harness
```

A run's state lives in one shard, `hash(workflowID) mod NumShards`. Each shard has an
ownership epoch (a fence); every write is conditional on it, so a paused node that wakes up
believing it still owns a shard cannot write. Events and the mutable state summary commit in
one transaction together with an outbox record, which is what makes a task and the history it
refers to impossible to disagree.

## Status

Built and green: the single-node engine end to end (M0–M2), durable Postgres storage, crash
recovery, HTTP API and Go client, node-to-node HTTP transport, CLI, Prometheus metrics,
signed task tokens, and the deterministic cluster harness. 27 of 41 work items are merged;
~23k lines of Go, of which about half are tests.

Not built yet, and deliberately named rather than implied:

- **The simulation oracle and fault injectors** (WI-041, WI-042). The harness is
  deterministic and the seams are in place, but the invariant checks I1–I8 and the injected
  crashes, partitions, pauses and clock skew of [SPEC §11](docs/SPEC.md) are still ahead.
  Faults today are injected per component, not across the cluster.
- **Sharding across nodes** (M4: request routing to the shard owner, task-queue routing).
  Leases and fencing are implemented and tested; the routing layer above them is not.
- **The showcase** (M6): the crash-proof LLM agent of SPEC §14 and its demo script.
  `examples/agent` is a placeholder.
- Versioning beyond `GetVersion`, the one-million-timer benchmark, and everything in
  SPEC §1.2's non-goals.

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
| [docs/REVIEWING.md](docs/REVIEWING.md) | How factory jobs are reviewed and steered |
| [AGENTS.md](AGENTS.md) | The rules agents work under |

## License

MIT. See [LICENSE](LICENSE).
