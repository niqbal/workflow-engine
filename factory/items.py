"""Single source of the work items. Run `python3 factory/items.py` to regenerate
docs/WORK-ITEMS.md and factory/backlog/*.md (factory-owned items only).

Tier is NOT written as a label: in asf a tier:N label is a human override that beats the
path rules, which would stop a diff from raising the tier. Tiers come from factory.yaml path
rules (SPEC §15.2); the tier column here is the expected tier."""
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]

# (id, milestone, owner, tier, title, depends, body, acceptance)
ITEMS = [
 # ---------------- M0: human-owned skeleton and contracts ----------------
 ("WI-001","M0","human",3,"Repository skeleton and CI",[],
  "go.mod (Go >= 1.22), package layout from SPEC §6, §6.1 and §12, Makefile targets build/vet/test/sim, local CI script.",
  ["`go build ./...`, `go vet ./...`, `go test ./... -race` pass on an empty skeleton",
   "Every package path matches a tier rule in SPEC §15.2"]),
 ("WI-002","M0","human",3,"Core interfaces, xsync and event types",["WI-001"],
  "Clock, Rand, Transport, Spawner and internal/xsync (SPEC §6.1); Store, dbVersion and errors (§8.1, §12); every event type from §4.1; RunKey, Fence, Task (with its armed-by reference and attempt), MutableState including buffered events and dedup sets.",
  ["All types compile and are documented", "Event JSON round-trip test covers every event type"]),
 ("WI-003","M0","human",3,"Determinism lint",["WI-001"],
  "The lint test exactly as scoped in SPEC §6.1, including context.WithTimeout, time.Since, select, channel types and map range.",
  ["Fails on one planted violation of each banned form and passes on the skeleton", "internal/engine and internal/httpapi are outside the scope"]),
 ("WI-004","M0","human",3,"Store contract suite (storetest)",["WI-002"],
  "storetest.Run(t, factory): create, dbVersion compare-and-swap including state-only writes, event ID continuity, ErrConflict, ErrFenced after another node acquires, outbox read from ack level with out-of-order commits, timer task ordering, concurrent writers.",
  ["Fails against a stub store", "Includes a case where task 10 commits before task 9 and both are read"]),
 ("WI-005","M0","human",3,"Scenario test suite for the server (contracttest)",["WI-002"],
  "contracttest: scenario tests against an in-process single-node server, written before the server exists. Covers start dedup against closed runs, signal dedup before closed check, buffered events and rejected close, stale attempts, timeout tasks bound to attempts, workflow-task and activity redispatch, execution timeout, retry backoff math.",
  ["Every SPEC §8 rule has at least one named scenario", "Suite compiles against the server interfaces and fails against stubs"]),
 ("WI-006","M0","human",3,"Replay test helper",["WI-002"],
  "wftest.ReplayFromFile(path, fn). Golden histories are generated from real M1 runs (WI-024), not written by hand, so they cannot encode a misunderstanding of buffering.",
  ["Helper compiles; tests skip until the replayer exists"]),

 # ---------------- M1: single node in memory ----------------
 ("WI-010","M1","human",3,"Deterministic runtime and run-twice trace check",["WI-002","WI-003"],
  "Gated Spawner, simulated Clock and Rand, xsync over the gate (SPEC §6.1, D9). A harness that runs a scenario twice with one seed and diffs the traces. Becomes a gate for every factory job.",
  ["Identical traces across 50 runs of a concurrent scenario", "A planted map-iteration dependency is caught by the trace diff"]),
 ("WI-011","M1","factory",2,"In-memory store passing storetest",["WI-004","WI-010"],
  "internal/store/memstore satisfying Store (WI-004 storetest and WI-010 simrt are merged on main). API, exactly: `func New(opts ...Option) *Store`; `func WithRuntime(rt xsync.Runtime) Option` (when set, every Store method calls rt.Yield() on entry, and commit delays wait on rt's clock; without it there are no yields and no delays); `func WithFaults(f Faults) Option`; `type Faults struct { BeforeCommit func(op string) error; AfterCommit func(op string) error; CommitDelay func(op string) time.Duration }` where op is the method name (e.g. \"UpdateRun\"). BeforeCommit returning an error aborts the write with nothing committed. AfterCommit returning an error is returned to the caller although the write took effect (an ambiguous commit). CommitDelay is applied after task IDs are allocated and before the write becomes visible, so a later task ID can commit first. Nil hooks do nothing. Read-only methods call no write hooks.",
  ["`storetest.Run` passes with -race", "Fault hooks covered by new tests", "Run-twice trace check passes", "No changes to internal/store/storetest"]),
 ("WI-012","M1","human",3,"Coroutine dispatcher",["WI-002"],
  "internal/engine dispatcher per SPEC §7.1: one coroutine at a time, deterministic order, yields only at SDK blocking calls; Future, Channel, Selector.",
  ["Property test: 1,000 random programs produce identical traces across 50 runs each", "No goroutine leaks (goleak)"]),
 ("WI-013","M1","human",3,"Replayer and non-determinism detection",["WI-012"],
  "Batching per WorkflowTaskStarted (SPEC §7.2), command-to-event matching, NonDeterministicError (§7.3).",
  ["Hand-built histories with buffered events replay exactly", "A changed activity type yields NonDeterministicError naming the event ID"]),
 ("WI-014","M1","human",3,"Workflow SDK (package wf)",["WI-013"],
  "ExecuteActivity, NewTimer, Sleep, GetSignalChannel, NewSelector, Go, Now, SideEffect, GetInfo (SPEC §3.1). Human-owned: G3 rests on it.",
  ["Each API driven through the replayer with hand-built histories", "SideEffect runs once and returns the recorded value on replay"]),
 ("WI-015","M1","human",3,"Mutable state and transitions (internal/history)",["WI-002"],
  "Pure functions applying events and state-only changes to MutableState: buffering, rejected close, retry attempts, dedup sets, timer and timeout references (SPEC §4.2, §8). Human-owned: G4, G5 and G7 rest on it.",
  ["Table tests for every legal transition and one illegal transition per event type"]),
 ("WI-016","M1","factory",2,"Matching: in-memory task queues with long-poll",["WI-010"],
  "internal/server/matching: per task-queue FIFO of workflow and activity tasks; long-poll bounded by the Clock; xsync only.",
  ["Poll returns at once when a task is queued, waits otherwise, returns empty at deadline", "Tests use the simulated clock, no real sleeps", "Lint and run-twice trace check pass"]),
 ("WI-017","M1","factory",2,"History service: start, signal, terminate",["WI-011","WI-015","WI-005"],
  "Start with requestID dedup against the latest run open or closed; signal with dedup before the closed check; terminate. One store write each (SPEC §8.1, §8.4). All decisions come from history.Machine (internal/history/machine.go, WI-015): load MutableState, call one Machine method, then write Machine.Out (events, TimerRequests as timer tasks, DispatchRefs as transfer tasks) and the new state in one Store.UpdateRun. Do not re-implement its rules.",
  ["contracttest scenarios for start, signal and terminate pass", "Every state change is one UpdateRun (counting store wrapper)", "No changes to contracttest"]),
 ("WI-018","M1","factory",2,"History service: workflow task path",["WI-017"],
  "Record started, complete with commands, fail, buffered events while started, rejected close when buffered events exist (SPEC §4.2, §8.2). All decisions come from history.Machine (internal/history/machine.go, WI-015): load MutableState, call one Machine method, then write Machine.Out (events, TimerRequests as timer tasks, DispatchRefs as transfer tasks) and the new state in one Store.UpdateRun. Do not re-implement its rules.",
  ["contracttest workflow-task scenarios pass, including buffered events and rejected close", "No changes to contracttest"]),
 ("WI-019","M1","factory",2,"History service: activity path",["WI-017"],
  "Record started, complete, fail; stale attempt rejected; retry state and backoff timer task written (SPEC §8.3). All decisions come from history.Machine (internal/history/machine.go, WI-015): load MutableState, call one Machine method, then write Machine.Out (events, TimerRequests as timer tasks, DispatchRefs as transfer tasks) and the new state in one Store.UpdateRun. Do not re-implement its rules.",
  ["contracttest activity scenarios pass", "No changes to contracttest"]),
 ("WI-020","M1","factory",2,"Transfer processor with ack level and redispatch",["WI-016","WI-018","WI-019"],
  "Read from the lowest undeleted task, push to matching, delete on start, redispatch unstarted workflow and activity tasks every redispatchInterval (SPEC §8.1-8.3). All decisions come from history.Machine (internal/history/machine.go, WI-015): load MutableState, call one Machine method, then write Machine.Out (events, TimerRequests as timer tasks, DispatchRefs as transfer tasks) and the new state in one Store.UpdateRun. Do not re-implement its rules.",
  ["contracttest redispatch scenarios pass", "With variable commit latency in memstore, no task is ever skipped (new test)"]),
 ("WI-021","M1","factory",2,"Timer processor core",["WI-018"],
  "Per-shard min-heap with loadAheadWindow; conditional fire bound to the armed reference and attempt; delete after fire (SPEC §10). All decisions come from history.Machine (internal/history/machine.go, WI-015): load MutableState, call one Machine method, then write Machine.Out (events, TimerRequests as timer tasks, DispatchRefs as transfer tasks) and the new state in one Store.UpdateRun. Do not re-implement its rules.",
  ["Never fires before fireAt on the simulated clock", "Crash between fire and delete produces no second TimerFired", "contracttest timer scenarios pass"]),
 ("WI-022","M1","factory",2,"Timeouts and retry backoff",["WI-021","WI-019"],
  "Activity start-to-close and schedule-to-close, workflow-task timeout, workflow execution timeout, retry backoff per RetryPolicy. All decisions come from history.Machine (internal/history/machine.go, WI-015): load MutableState, call one Machine method, then write Machine.Out (events, TimerRequests as timer tasks, DispatchRefs as transfer tasks) and the new state in one Store.UpdateRun. Do not re-implement its rules.",
  ["contracttest timeout scenarios pass, including an old attempt's timeout being a no-op"]),
 ("WI-023","M1","factory",2,"HTTP API and Go client",["WI-017","WI-016"],
  "internal/httpapi endpoints from SPEC §13.1 (no heartbeat) with JSON error codes; client package with a Run handle.",
  ["httptest coverage for every endpoint and error code", "Client retries only idempotent calls or calls with a requestID"]),
 ("WI-024","M1","factory",2,"Worker and end-to-end example",["WI-014","WI-023","WI-022","WI-020"],
  "worker: register, poll, replay, run activities with deadlines, report. Run the SPEC §3.4 example for approved, rejected and timed-out paths and save the three histories as golden files for wftest.",
  ["Example passes end to end against an in-process server", "Golden histories generated and replayed by wftest"]),
 ("WI-025","M1","factory",1,"Minimal CLI",["WI-023"],
  "cmd/wfe: server (memstore), workflow start, signal, history.",
  ["`wfe workflow history <id>` prints readable events", "Flags documented in --help"]),

 # ---------------- M2: durable ----------------
 ("WI-030","M2","factory",2,"Postgres store",["WI-011"],
  "internal/store/pgstore with pgx and versioned SQL migrations (SPEC §12). Tests use PG_URL or docker; skipped when unavailable.",
  ["`storetest.Run` passes for pgstore", "Migrations apply to an empty database and are idempotent"]),
 ("WI-031","M2","human",3,"Postgres fencing and dbVersion stress test",["WI-030"],
  "Review pgstore's fence and dbVersion checks under concurrent transactions; two-writer stress test.",
  ["Zero accepted stale-fence or stale-version writes over 10,000 attempts"]),
 ("WI-032","M2","factory",2,"Crash recovery on restart",["WI-030","WI-020","WI-021"],
  "On shard load, read transfer and timer tasks from the store and resume. Integration test plus scripts/kill-restart.sh.",
  ["Restart mid-run: all runs complete, no duplicate TimerFired"]),

 # ---------------- M3: simulation ----------------
 ("WI-040","M3","human",3,"Cluster simulation harness",["WI-010","WI-024"],
  "sim.Run(seed, workload, faults): N nodes, memstore, simulated workers, quiet phase (SPEC §11.1).",
  ["Same seed yields byte-identical traces across 20 runs", "A 30-day virtual timer workload runs in under 5 s wall time"]),
 ("WI-041","M3","human",3,"Oracle I1-I8 with ghost log",["WI-040"],
  "Checks from SPEC §11.3; I6 reads a ghost log outside the store; I8 derives the reference from final history.",
  ["Each check catches a deliberately broken engine variant"]),
 ("WI-042","M3","human",3,"Fault injectors",["WI-040"],
  "Crash and restart, drop and delay, partitions, process pause, clock skew within epsilon, store transient errors, ambiguous commits, commit latency, worker crash after side effect. Human-owned: a weak injector silently shrinks what the simulator can find.",
  ["Each fault has a test proving it fires and reproduces by seed", "Faults stop at the quiet phase"]),
 ("WI-043","M3","factory",1,"Simulation workloads",["WI-041"],
  "The workload mix from SPEC §11.4.",
  ["Each workload passes the oracle with faults disabled"]),
 ("WI-044","M3","factory",1,"Seed sweep command and failure reports",["WI-041","WI-042"],
  "`wfe sim --seeds A-B --parallel N`, sim/failures/<seed>.txt, CI seeds 1-200, docs/SIM-BUGS.md template.",
  ["A planted bug is found and its report reproduces it"]),

 # ---------------- M4: sharding ----------------
 ("WI-050","M4","human",3,"Shard leases and fencing",["WI-031","WI-042"],
  "internal/shard: acquire, renew, stop-serving margin, failover placement (SPEC §9).",
  ["Simulated pause longer than the lease: all of the paused node's writes rejected (I6)"]),
 ("WI-051","M4","factory",2,"Request routing to shard owner",["WI-050"],
  "Frontend ownership cache, forward over Transport, refresh on not_owner, bounded retries.",
  ["Simulation: requests to any node succeed while shards fail over"]),
 ("WI-052","M4","factory",2,"Task-queue routing",["WI-050"],
  "Queue q lives on the owner of shard hash(q); history pushes and frontend polls go there (SPEC §9, D10).",
  ["Simulation with 3 nodes: workers polling any node receive every task; no oracle violations over 2,000 seeds"]),

 # ---------------- M5: versioning and metrics ----------------
 ("WI-060","M5","human",3,"GetVersion",["WI-014","WI-040"],
  "wf.GetVersion with MarkerRecorded and DefaultVersion (SPEC §7.4). Human-owned: G3 rests on it.",
  ["Golden histories replay with new code on the old branch", "Simulation swaps code mid-run without violations"]),
 ("WI-061","M5","factory",1,"Prometheus metrics",["WI-023"],
  "Metrics from SPEC §13.3 (no tracing); slog fields on every log line.",
  ["/metrics exposes every listed metric; a test scrapes and checks names"]),

 # ---------------- M6: showcase ----------------
 ("WI-070","M6","factory",1,"Agent example with scripted model",["WI-024","WI-060"],
  "examples/agent per SPEC §14 with idempotent ledger and mail sinks.",
  ["Test kills the worker after IssueRefund's side effect and before it reports: ledger shows exactly one refund"]),
 ("WI-071","M6","factory",1,"Demo script",["WI-070","WI-032"],
  "examples/agent/demo.sh: start, kill mid-refund, restart, approve with curl, print history and ledger.",
  ["Runs end to end with the scripted model; README shows the output"]),
]

import json as _json
_ISSUES = ROOT / "factory" / "issues.json"
ISSUES = _json.loads(_ISSUES.read_text()) if _ISSUES.exists() else {}


def front(it):
    i, m, owner, tier, title, deps, body, acc = it
    lines = ["---", f"title: \"{i} {title}\"", "kind: task", f"labels: [\"{m}\"]", "acceptance:"]
    lines += [f"  - \"{a.replace(chr(34), chr(39))}\"" for a in acc]
    lines += ["---", body, "", f"Depends on: {', '.join(deps) or 'none'}.",
              "Spec: docs/SPEC.md. Do not edit existing tests; contract tests are owned by the human."]
    if i in ISSUES:
        lines.append(f"GitHub issue: niqbal/workflow-engine#{ISSUES[i]}")
    return "\n".join(lines) + "\n"

def main():
    inbox = ROOT / "factory" / "backlog"
    moved = {p.stem for d in ("ready", "done") for p in (ROOT / "factory" / d).glob("*.md")}
    (ROOT / "factory" / "ready").mkdir(parents=True, exist_ok=True)
    (ROOT / "factory" / "ready" / ".gitkeep").touch()
    inbox.mkdir(parents=True, exist_ok=True)
    for old in inbox.glob("*.md"):
        old.unlink()
    doc = ["# Work Items", "",
           "Generated by `factory/items.py` from one list; edit that file, not this one.",
           "Owner `human` = written by hand (with Claude Code); `factory` = queued to `asf`, which",
           "reads items from `factory/ready/`. Items wait in `factory/backlog/`; move one to `ready/`",
           "only when every item it depends on is merged (the factory does not enforce order).",
           "Tier is the expected tier from the path rules in SPEC §15.2, not a label.", ""]
    by_m = {}
    for it in ITEMS:
        by_m.setdefault(it[1], []).append(it)
    names = {"M0":"Skeleton and contracts","M1":"Single node, in memory","M2":"Durable","M3":"Simulation",
             "M4":"Sharding","M5":"Versioning and metrics","M6":"Showcase"}
    for m, its in by_m.items():
        doc += [f"## {m} — {names[m]}", "", "| ID | Owner | Tier | Title | Depends on |", "|---|---|---|---|---|"]
        doc += [f"| {i} | {o} | {t} | {ti} | {', '.join(d) or '—'} |" for i,_,o,t,ti,d,_,_ in its]
        doc.append("")
        for i,_,o,t,ti,d,body,acc in its:
            ref = f" (#{ISSUES[i]})" if i in ISSUES else ""
            doc += [f"### {i} {ti}{ref}", "", body, "", "Acceptance:"] + [f"- {a}" for a in acc] + [""]
        for it in its:
            if it[2] == "factory" and it[0] not in moved:
                (inbox / f"{it[0]}.md").write_text(front(it))
    h = sum(1 for x in ITEMS if x[2]=="human"); f = len(ITEMS)-h
    doc.insert(6, f"**{len(ITEMS)} items: {h} human, {f} factory.**\n")
    (ROOT / "docs" / "WORK-ITEMS.md").write_text("\n".join(doc))
    print(f"{len(ITEMS)} items ({h} human, {f} factory) -> docs/WORK-ITEMS.md, {f} backlog files")

if __name__ == "__main__":
    main()
