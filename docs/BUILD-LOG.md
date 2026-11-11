# Build Log

Every step, in order: what was done, what was decided, why. Newest entries at the bottom.

---

## 2026-11-11 — Step 1: Choose the project

**Context.** Nawab wanted a hobby project hard enough to show deep back-end skill, on the
level of a friend who rebuilt RabbitMQ and made it faster. Two candidates were weighed:
a distributed hybrid search engine and a durable workflow engine like Temporal.

**Decision.** The workflow engine. It is smaller in code but harder in correctness: the bugs
are rare crash-and-race interleavings, not slow queries. It fits the Kafka and transactional
outbox work at Sendoso, and it answers the agent-runtime design question for the Vectara
interview (pause for human approval, resume on a webhook). It is also the more likely of the
two to be finished in about three months part-time.

**What makes it impressive.** Not the happy path, which is tutorial-sized, but four things:
deterministic simulation testing that finds real bugs from a seed, fencing that makes stale
shard owners harmless, safe code changes for in-flight runs, and a crash-proof LLM-agent demo.

## 2026-11-11 — Step 2: Learn how the software factory takes work

Read `~/work/software-factory/README.md` and the intake and policy code. Facts that shaped
the spec:

| Factory fact | Consequence for this project |
|---|---|
| Work items are title + body + acceptance list; the local-file adapter reads Markdown with YAML front matter | Every item is written in that format |
| No planner: triage only flags epics | Items are pre-cut small (35 items) |
| The implementer may not edit existing tests above tier 0 | The human writes the contract tests first; factory code must pass them and cannot weaken them. This is used as a feature |
| Gates compare against the base commit | The repo needs a green Go skeleton (M0) before the first factory job |
| A `tier:N` label is a human override that beats the path rules | Items carry no tier labels; tiers come only from path rules, so a diff that strays into engine code is still raised to tier 3 |
| Intake does not order work | Items wait in `factory/backlog/` and are moved to `factory/ready/` only when their dependencies are merged |
| The setup agent supports Go (`go test`, `go vet`, `go build`) | Go is viable with the factory |

## 2026-11-11 — Step 3: Create the repository

- `~/work/workflow-engine`, `git init -b main`. Nothing committed yet.
- Toolchain check: Go **1.19.5** is installed. Too old (no `log/slog`, which arrived in 1.21).
  **Upgrade to Go ≥ 1.22 before M0.** Postgres.app (`psql`) and Docker are present.

## 2026-11-11 — Step 4: Write the specification

`docs/SPEC.md` v0.1. Main choices, with reasons recorded in SPEC §17:
- Go; Postgres as the only production store; HTTP/JSON API.
- **Simulation-first:** no server code may touch the real clock, randomness, network or raw
  goroutines. They come in through interfaces, enforced by a lint test. Retrofitting this
  later would be close to a rewrite.
- **Fences for safety, leases for liveness.** Every write checks the shard's `range_id` in
  the same transaction.
- **One transaction per state change:** events, mutable state and outbox tasks together.
- Matching is in memory and recovered by redispatch; duplicate dispatch is harmless because
  starting a task is a conditional transition.
- Non-determinism blocks a run instead of failing it, so a bad deploy never loses work.
- Eight guarantees (G1–G8), each tied to a simulator oracle check (I1–I8).

## 2026-11-11 — Step 5: Cut the spec into work items

`factory/items.py` holds the single list and generates `docs/WORK-ITEMS.md` and one file per
factory item in `factory/backlog/`. 35 items: 11 human-owned (skeleton, interfaces, every
contract test, the coroutine dispatcher, the replayer, pgstore fencing, the simulator core and
oracle, shard leases) and 24 for the factory.

The split follows one rule: **the human owns whatever the guarantees rest on, and the tests
that check it; the factory builds what an existing test or a precise acceptance list can
verify.**

## 2026-11-11 — Step 6: Independent review of the spec

A fresh reviewer agent, given only the files, is reviewing the spec and work items for
correctness holes, underspecified points, simulation risks and scope. Output:
`docs/reviews/2026-11-11-spec-review.md`. Changes made in response are logged in Step 7.

## 2026-11-11 — Step 7: Apply the review (spec v0.2)

Verdict from the reviewer: **revise**. The architecture held, but four guarantees could be
broken by concrete races. Full review: `docs/reviews/2026-11-11-spec-review.md`. Every
must-fix item was accepted.

| Review finding | Change in SPEC v0.2 |
|---|---|
| C1. A signal appended mid-task breaks replay, and a close can skip an acknowledged signal | Events arriving while a workflow task is started are **buffered** and appended after `WorkflowTaskCompleted`; a close command is rejected while buffered events exist (§4.2, §7.2, D12) |
| C2. State-only writes (retries) race; a stale write can reset an attempt | Per-run **`dbVersion`** compare-and-swap on every write; cache evicted on any error (§8.1, D11) |
| C3. An old attempt's timeout can time out a newer attempt | Every timer task carries what armed it and its attempt; firing is a no-op on mismatch (§8.3, §10) |
| C4. A lost start response plus a fast run lets a retry start a second run (double refund) | Start dedup checks the latest run, open or closed; a workflowID is used once in v1 (§8.4) |
| C5. A committed signal can be reported "rejected" after the run closes; the dedup bound was hidden | Dedup check runs before the closed check and survives close; the 1,000-ID bound is stated in G7 (§5, §8.4) |
| C6. Lost workflow tasks are never re-pushed; the outbox cursor can skip a late commit | Workflow-task redispatch; transfer processor reads from the lowest undeleted task (§8.1-8.2) |
| C7. G5 silently depended on clocks | G5 restated with a skew bound ε; event timestamps clamped non-decreasing (§5, §10) |
| C8. The showcase overclaimed | Claim is exactly-once effects at idempotent sinks; `Plan` may repeat (§14) |
| Simulator execution model was undecided | Decided (D9): blocking-style code on gated goroutines that yield at Clock, Transport, Store and `xsync` calls. Lint scope and banned forms made exact; HTTP moved to `internal/httpapi` |
| In-memory store hid the outbox gap | memstore gets variable commit latency |
| I6 was circular; I8 depended on timing | I6 reads a ghost log outside the store; I8 derives its reference from the final history |
| Trace determinism checked too late (M3) | Run-twice trace check built in M1 (WI-010) and used as a gate for every factory job |
| Factory would grade its own tests for the server | New human scenario suite `contracttest` (WI-005) written before the server; tier rules now cover every package |
| Items too big or too critical for the factory | History service split into three items, timers into two; SDK, mutable state, fault injectors and GetVersion moved to human ownership; task-queue routing, workflow-task redispatch and execution timeout added |
| Scope too big for ~3 months | Cut to stretch: rebalancing, heartbeats, schedule-to-start, 1M-timer benchmark, OpenTelemetry, message duplication and reordering, nightly sweep, full CLI |

Work items after the re-cut: **37 (17 human, 20 factory)**. The human share rose from 11 to
17. That is the honest cost of the rule "the factory never grades its own work": the factory
accelerates the bulk code, while the parts the guarantees rest on are written and owned by
hand.

## Next steps

1. Upgrade Go to ≥ 1.22.
2. M0 by hand: WI-001 to WI-006 (skeleton, interfaces, lint, storetest, contracttest, replay helper).
3. M1 human items: WI-010 deterministic runtime, WI-012 to WI-015.
4. Onboard the factory: `asf --repo ~/work/workflow-engine init`, set the tier rules and
   gates from SPEC §15.2, point local-file intake at `factory/ready/`, set the budget cap.
5. Move WI-011 and WI-016 to `factory/ready/` and run the first two factory jobs.
