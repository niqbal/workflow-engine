# Spec review: SPEC.md v0.1 and WORK-ITEMS.md

## 1. Correctness holes (must fix)

**C1. Events arriving during a started workflow task break replay (G3, G7).** §4.2 says such events "set a flag" but not where they land. If appended immediately: `WFTStarted(5)`, `Signaled(6)`, `WFTCompleted(7)`, commands `(8..)` produced without seeing 6. On replay, batching "up to each WorkflowTaskStarted" (§7.2) delivers 6 before running the dispatcher, so a Selector may pick the signal instead of the timer: replay diverges from the recorded commands. Also: `CompleteWorkflow` returned after a mid-task signal closes the run with that signal unhandled. **Fix:** buffer events while a task is started and append them after `WFTCompleted`; reject a close command when buffered events exist and schedule a new task (Temporal's "unhandled command").

**C2. Mutable-state-only writes have no concurrency check (G4, G8).** Retries and heartbeats change mutable state without appending events (§4.2), so `expectedNextEventID` does not change. Heartbeat handler reads state (attempt 1, started); start-to-close timeout concurrently writes (attempt 2, backoff); heartbeat writes its stale copy with the same `expectedNextEventID` and wins. The attempt counter regresses and attempt 1's late completion is accepted. **Fix:** a `db_version` on `runs`, CAS-incremented on every `UpdateRun`; evict the cached run on any `UpdateRun` error.

**C3. Timeout and timer tasks are not bound to what armed them (G5, G4).** §10's "still pending" check is per timer or activity, not per attempt. A start-to-close task from attempt 1 that was not deleted fires during attempt 2 and times it out early; same for workflow-task timeouts. A reused timer ID lets an old task fire a new timer. **Fix:** every timer task carries `(scheduledEventID|startedEventID, attempt)` and is a no-op unless both match.

**C4. Start dedup ignores closed runs (G4 in the showcase).** Start commits, response is lost, run finishes fast, client retries with the same `requestID`: no open run, so a second run starts with a new `runID`, hence new idempotency keys and a second refund. **Fix:** dedup `requestID` against the latest run regardless of state; default ID-reuse policy "reject if any run exists for this workflowID".

**C5. Signal retry after ambiguous commit is reported as rejected (G7).** Signal commits, response lost, run closes, retry hits the closed-run check first. Client is told "rejected" for a signal that is in history. The 1,000-entry bound (§18.3) also voids "does not duplicate". **Fix:** check the dedup set before the closed check, keep it on closed runs, state the bound in G7.

**C6. Lost or skipped tasks leave runs stuck (G8).** (a) Redispatch (§8.3) is defined only for activities; a scheduled workflow task lost from in-memory matching is never re-pushed. (b) `ReadTransferTasks(afterTaskID)` has the classic outbox gap: IDs 9 and 10 allocated, 10 commits first, cursor advances past 9, which commits later and is never read. **Fix:** redispatch for workflow tasks; the processor's ack level is the lowest undeleted task, and it rescans from it.

**C7. G5 depends on clocks, contradicting D7.** `fireAt` is set from the owner's clock; after failover a node skewed ahead fires early in real time. Event timestamps can go backwards, so `wf.Now` is non-monotonic. **Fix:** define G5 against the firing node's clock with a stated skew bound ε (or use store time), and clamp event timestamps to be monotonic per run.

**C8. Showcase overclaims.** "Without repeating any side effect" is false for `Plan` and `SendReply`; claim exactly-once only at idempotent sinks.

## 2. Underspecified points

- **Task queue placement across nodes.** Matching is per node; a worker polling node X never sees tasks pushed to node Y's matching. M4 has no answer (route by `hash(taskQueue)`? fan-out polls?). This decides whether G8 holds with more than one node.
- Lease expiry on whose clock; whether `RenewShard` CASes `range_id`.
- `GetVersion` matching: is the version marker optional during NDE matching? What happens when a `GetVersion` call is later removed?
- Workflow execution timeout: there is an event but no timer kind for it. `ScheduleToCloseTimeout` is referenced but not in `ActivityOptions`.
- Whether every workflow-task retry writes `WFTFailed` (an NDE loop grows history without bound); what `wf.Now` reads.
- G8 versus D6: an NDE-blocked run violates I1 by design. Exclude it explicitly.

## 3. Simulation design risks

- **The execution model for server code is undecided.** A single-threaded cooperative scheduler cannot run code that blocks on `sync.Mutex`, channels or `select`. Choose either actor/event-loop handlers or gated goroutines that yield only at Clock/Transport/Store calls, and write it into §6.1. Every factory item depends on this choice.
- **The lint misses real nondeterminism:** `context.WithTimeout/Deadline`, `time.Since/Until/Tick/NewTicker`, `select` with several ready cases, `range` over maps. Conversely it bans `go` in `internal/engine` (the dispatcher needs goroutines) and `net.` in the frontend (`net/http`). WI-003 will fail on day one.
- **Memstore commits atomically and instantly**, so the C6(b) gap and transaction interleavings are invisible. Make every store call a yield point with a sampled latency, and split the ambiguous commit into "commit, yield, error".
- **Blind spots:** the HTTP frontend, client retries and pgstore isolation never run in the sim. Route sim workers through the real `client`/`worker` packages.
- **I6 is tautological** if memstore checks itself. Record `(node, fence, op)` in a ghost log outside the store and check that log.
- **I8:** signal-versus-timer races are timing-dependent; derive the reference from history.

## 4. Work-item problems

- **The factory grades its own homework.** §15 promises human contract tests first, but WI-014..019 have none; their tests come from the factory. Add human-written tests for transitions, the write path, matching and the timer processor.
- **Should be human-owned:** WI-013 (Selector and signal semantics are G3), WI-014 (the state machine is G4, G5, G7), WI-042 (weak injectors silently shrink coverage), WI-060 (it touches the replayer).
- **Too big:** WI-016 (six operations plus command application); split it into start/signal/terminate, workflow-task completion, and activity outcomes. WI-018: split into heap/window and timeouts/backoff.
- **Tier rules have gaps:** `wf/`, `activity/`, `worker/`, `client/`, `sim/` (except `sim/oracle`) and `internal/history` match no rule, so the stated tiers are not what `asf` will compute. WI-014 says `internal/history` while WI-016 says `internal/server/history`.
- **Dependencies:** WI-050 does not need WI-031; WI-060 needs WI-043; WI-062 needs WI-018.
- **Missing items:** task-queue routing, workflow-task redispatch, execution timeout, buffered events (C1).
- **Ordering:** a deterministic 2-run trace-diff harness should exist in M1 and gate every factory job. Finding in M3 that factory code cannot run under the scheduler means a rewrite.
- **WI-005:** hand-written golden histories will encode C1. Generate them from M1 runs.

## 5. Scope (about 3 months part-time)

The human-owned core alone is most of the budget. **Keep:** single node, sim, pgstore through storetest, 2–3 nodes with fencing and failover, the agent demo.

**Defer:** rebalancing (acquire-expired only), heartbeats, the 1M-timer benchmark, OpenTelemetry, message duplication/reordering, the nightly 100k sweep, most of the CLI, ScheduleToStart.

## 6. Verdict: **revise**, then build

Top 5 changes, in priority order:

1. **Pin history batching (C1):** buffer mid-task events after `WFTCompleted`; no close with buffered events.
2. **Concurrency-safe write path (C2, C3, C6):** per-run `db_version` CAS, attempt-bound timer tasks, workflow-task redispatch, gap-free transfer ack level.
3. **Dedup that survives close and ambiguity (C4, C5).**
4. **Decide the sim execution model now:** §6.1, fixed lint, store yield points, M1 trace-diff gate on every factory job.
5. **Re-cut the work items:** human contract tests for WI-014..019, four items to human, split WI-016/018, fix tier paths, add task-queue routing.
