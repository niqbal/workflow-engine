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

## 2026-11-11 — Step 8: GitHub repository, issues and milestones

- Private repository: https://github.com/niqbal/workflow-engine (made private by default; it
  can be made public later).
- `factory/github_sync.py` creates, idempotently, from `factory/items.py`: labels
  (`owner:human`, `owner:factory`, `tier:1-3`, `tracking`), 7 milestones (M0-M6), one issue
  per work item (#1-#37, acceptance criteria as checklists) and one tracking issue per milestone
  (#38-#44). `factory/issues.json` maps item IDs to issue numbers, and every backlog file
  names its issue.

## 2026-11-11 — Step 9: Run asf on Claude Code headless instead of the API

The request: build with `asf`, but use Claude Code headless (`claude -p`) rather than direct API
calls.

- **Finding the CLI.** `claude` was not on PATH. The desktop app bundles Claude Code at
  `~/Library/Application Support/Claude/claude-code/<version>/<hash>/claude.app/Contents/MacOS/claude`
  (2.1.317 here; the `claude-code-vm` copy is a Linux build). It is signed in, so headless
  calls need no API key.
- **The design problem.** asf's security model is that agents reach the sandbox only through
  its broker, which enforces permissions, protected paths and test integrity. Letting Claude
  Code run its own tools would bypass all of that.
- **The design.** A new model provider, `claude_code` (`asf/adapters/models/claude_code.py`
  in the factory repo, branch `claude-code-provider`). Each agent turn is one `claude -p`
  call with Claude Code's tools switched off (`--tools ""`). The model returns the tool calls
  it wants as structured output (`--json-schema`). asf's unchanged agent loop runs them
  through the broker and sends results in the next call, which resumes the same session
  (`--resume`). User settings and MCP servers are excluded (`--setting-sources ""`,
  `--strict-mcp-config`), and API keys are scrubbed from the CLI's environment.
- **A prototype lesson.** The first prompt made the model try to call a real tool and give
  up ("the tool is unavailable"). The fix was an explicit protocol in the system prompt: the
  host runs tools after your reply; requesting one is how you use it.
- **Verification.** 4 new unit tests with a fake CLI (resume, tool-result threading, retry on
  bad output, key scrubbing, structured calls); the factory suite passes (51 tests); a live
  two-turn tool loop on the real CLI cost about $0.02 at list price.

## 2026-11-11 — Step 10: Go toolchain

Homebrew no longer builds Go for Intel Macs, and `brew upgrade go` was a no-op. Installed the
official go1.27.4 darwin-amd64 release from go.dev into `~/sdk/go1.27.4` after checking its
SHA-256 against go.dev's published list. The system Go (1.19.5) is untouched; the Makefile
picks the newest `~/sdk/go1.2*` automatically.

## 2026-11-11 — Step 11: M0 by hand (WI-001 to WI-006)

| Item | What was built |
|---|---|
| WI-001 | Module `github.com/niqbal/workflow-engine`, package layout, Makefile (`build`, `vet`, `test`, `race`, `ci`), `scripts/ci.sh` |
| WI-002 | `internal/clock`, `internal/rnd`, `internal/transport`, `internal/xsync` (Runtime, Locker, Cond with deadlines, generic `Chan`, `SortedKeys`, production runtime); `internal/history` (20 event types with tagged JSON, 6 command types, retry policy and backoff, MutableState with buffered events and signal dedup); `internal/store` (Store interface with fences, DBVersion and opaque events/state); `internal/server/api` (service contract and error codes) |
| WI-003 | `internal/lint`: AST lint over the server scope. Bans wall-clock time, `context.WithTimeout/WithDeadline`, `math/rand`, `net`, `sync`, `go`, `select`, channels and map ranges (type-checked). A test plants one violation of each kind |
| WI-004 | `storetest`: 15 contract cases, including state-only writes bumping DBVersion (review C2), reading tasks from the ack level after a middle delete (C6), atomic DoneTasks, stale fences, and 16 concurrent writers with exactly one winner |
| WI-005 | `contracttest`: 26 scenarios against `api.Service` plus a harness that controls the clock: start dedup across close (C4), signal dedup before the closed check (C5), buffering and rejected close (C1), retry backoff, stale attempts (C3), schedule-to-close, timers, transient workflow-task retries, redispatch of lost tasks, execution timeout, terminate, describe |
| WI-006 | `wftest`: history loading and `ReplayFromFile`, waiting on the replayer (WI-013) |

Checks: `make ci` (build, vet, `test -race`) is green. Both suites were run against
do-nothing stubs and fail as they should. The first stub run exposed a storetest case that
panicked instead of failing cleanly, which would have hidden the cases after it; it now
checks lengths first.

Decisions made while writing the contracts, folded back into SPEC §4.2, §12 and §13.1:
- Workflow-task retries after the first failure are **transient** (Temporal's approach):
  shown to the worker, persisted only on completion. One event per failure series.
- The store treats events and mutable state as opaque bytes; `GetLatestRun` replaces
  `GetOpenRunByWorkflowID` (start dedup needs closed runs too).
- New error code `workflow_closed`; activity failures carry an explicit `NonRetryable` flag.
