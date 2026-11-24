# Simulation findings

Every bug the simulator finds is written up here: the seed, what happened, the mechanism, and
the fix (SPEC §11.5). A finding stays here after it is fixed — the record of what the design
got wrong is the point.

Each entry names the exact reproduction. Seeds are stable: the same seed and the same code
produce the same run, event for event.

---

## 2026-11-25 — An uncertain write left a shard's outbox undispatched

**Status:** fixed.

**Reproductions**, all of which failed before the fix and pass after it:

| Reproduction | What it showed |
|---|---|
| `wfe sim --workload retries --seeds 1` | the smallest case: 1 seed in 10 of every workload |
| `wfe sim --workload basic --seeds 48` | the same thing with a timer and a signal in the run |
| `TestWI042CompoundedStoreFaultsStillFinish`, seed 23 | 10% failed writes, 5% ambiguous commits, 2 s commit delays |
| `TestWI042EveryFaultAtOnceOverSeeds`, seed 13 | every fault at once |

**What happened.** Roughly one seed in ten left a workflow `running` for the rest of the
simulation — 7 virtual days after every fault had stopped. The oracle reported it as I1, the
liveness check behind G8. Every other invariant held: no history was rewritten, no timer fired
twice, no effect was applied twice, no write escaped its fence. The stuck run always had
exactly **two events**: `WorkflowExecutionStarted` and `WorkflowTaskScheduled`. It was created
and then never dispatched.

**Isolating it.** Each fault was run on its own over 30 seeds of the `retries` workload. Node
crashes, process pauses, failed writes, commit delays, clock skew and worker crashes were all
survived, 0 failures each. Ambiguous commits alone reproduced it, 3 seeds in 30. That narrowed
a seven-fault interaction to one fault in a few minutes, which is the whole argument for
injecting faults one at a time as well as together.

**Mechanism.** An ambiguous commit is the store committing a write and then failing to report
it (SPEC §8.1): the run and its first transfer task are durable, and the caller is told the
write failed. `createRunAndWake` and `updateAndWake` only woke the shard's loops when the
write succeeded, so nothing told the transfer processor that an outbox record existed. The
client retried with the same request ID, the server deduplicated it and returned the run that
was already there — again without waking anything. Meanwhile both loops wait **without a
deadline** when they know of no due task, because the design is wake-driven: a write rings the
loop it concerns. So the shard slept, with a transfer task in its outbox and a run that would
never start. Nothing recovers from this on its own: redispatch only runs inside a scan, and
there was no scan to run it.

**Fix.** `internal/server/loops.go`: `wakeUncertain` rings both of a shard's loops after a
write whose outcome is unknown, so the shard looks at its outbox instead of waiting for a
write that may never come. Errors that prove nothing was written are excluded — a rejected
fence means the node no longer owns the shard, and an invalid write never reached the store.

The alternative was to put a floor under the loops' wait, scanning every redispatch interval
whatever happens. That also fixes it, but it turns a wake-driven design into a polling one and
costs a scan per shard per interval forever; on the long-timer workload, where a month of
virtual time passes with nothing due, it would be the dominant cost. Waking on uncertainty
keeps the quiet case quiet.

**What it cost in the test suite.** Before the fix, the sim package took 413 s, almost all of
it stalled runs burning virtual days. After it: 7.8 s.

---

## 2026-11-25 — A long poll plus redispatch starves the virtual clock

**Status:** closed (simulation configuration), with an open question for production.

**Reproduction:** before the fix, `sim.Run(21, chaosWorkload(...), &sim.Faults{CrashEvery: 20s,
DownFor: 40s})` with `Workload.LongPoll` left at its default.

**What happened.** The run never finished. It burned the scheduler's five-million-step
livelock guard while the virtual clock crawled: 30 virtual days passed in 30-second steps and
no workflow closed.

**Mechanism.** `Workload.LongPoll` defaults to `QuietLimit`, which a long-timer workload wants
(a poller parks for the whole run instead of waking every 30 seconds, which is what makes "30
virtual days in under 5 seconds" possible). With fault injection that default becomes a 30-day
poll deadline. A node restart leaves a transfer task whose dispatch is stale; the owning node's
transfer processor re-pushes it every redispatch interval, and `Server.PollWorkflowTask`
consumes it, finds it stale and `continue`s inside the *same* poll call. With a 30-day deadline
that loop never ends: there is always a runnable task, so the scheduler never advances the
clock past the next redispatch, and the run grinds forward 30 seconds at a time.

**Fix.** The workloads set `LongPoll` to what a real cluster runs: 30 s, or an hour for the
long-timer workload, where a 30-second poll would wake every worker 86,000 times in a month of
virtual time. The harness default is unchanged: it is right for the long-timer workloads it was
written for, and the pathology needs a stale-task supply to sustain it.

**Open question for production.** The `continue` in `PollWorkflowTask`'s stale-task path has no
bound and no backoff. A steady supply of stale tasks — which a crash loop produces — is a hot
loop on a real server too, burning a core and hammering the store with reads, for as long as
the poll deadline allows. The simulator found it because its clock is virtual and the symptom
was total; on a wall clock it would show up as CPU, not as a stall. Worth bounding the retries
per poll. Not fixed here: it is engine behaviour rather than simulator behaviour, and it
deserves its own work item.
