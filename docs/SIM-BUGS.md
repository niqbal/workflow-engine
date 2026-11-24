# Simulation findings

Every bug the simulator finds is written up here: the seed, what happened, the mechanism, and
the fix (SPEC §11.5). A finding stays here after it is fixed — the record of what the design
got wrong is the point.

Each entry names the exact reproduction. Seeds are stable: the same seed and the same code
produce the same run, event for event.

---

## 2026-11-25 — A long poll plus redispatch starves the virtual clock

**Status:** closed (simulation configuration), with an open question for production.

**Reproduction:** before the fix, `sim.Run(21, chaosWorkload(...), &sim.Faults{CrashEvery: 20s,
DownFor: 40s})` with `Workload.LongPoll` left at its default.

**What happened.** The run never finished. It burned the scheduler's five-million-step
livelock guard while the virtual clock crawled: 30 virtual days passed in 30-second steps and
no workflow closed.

**Mechanism.** `Workload.LongPoll` defaults to `QuietLimit`, which a long-timer workload wants
(a poller parks for the whole run instead of waking every 30 seconds, which is what makes
"30 virtual days in under 5 seconds" possible). With fault injection that default becomes a
30-day poll deadline. A node restart leaves a transfer task whose dispatch is stale; the
owning node's transfer processor re-pushes it every redispatch interval, and
`Server.PollWorkflowTask` consumes it, finds it stale and `continue`s inside the *same* poll
call. With a 30-day deadline that loop never ends: there is always a runnable task, so the
scheduler never advances the clock past the next redispatch, and the run grinds forward 30
seconds at a time.

**Fix.** The fault-injection workloads set `LongPoll` explicitly to 30 s, which is what a real
cluster runs. The harness default is unchanged: it is right for the long-timer workloads it
was written for, and the pathology needs a stale-task supply to sustain it.

**Open question for production.** The `continue` in `PollWorkflowTask`'s stale-task path has no
bound and no backoff. A steady supply of stale tasks — which a crash loop produces — is a hot
loop on a real server too, burning a core and hammering the store with reads, for as long as
the poll deadline allows. The simulator found it because its clock is virtual and the symptom
was total; on a wall clock it would show up as CPU, not as a stall. Worth bounding the retries
per poll. Not fixed here: it is engine behaviour, not simulator behaviour, and it deserves its
own work item.

---

## 2026-11-25 — A run is left open for the rest of the simulation under compounded faults

**Status:** open.

**Reproductions**, both with the chaos workload (three nodes, four shards, two workers per
node, an activity through the side-effect ledger, a timer and a signal):

| Reproduction | Faults |
|---|---|
| `TestWI042StoreFaultsCompoundedStallARun` (skipped), seed 23 | 10% failed writes, 5% ambiguous commits, commit delays up to 2 s |
| `TestWI042EveryFaultAtOnceOverSeeds`, seed 13 | crashes every 30 s, pauses every 40 s, 5% failed writes, 2% ambiguous commits, 1 s commit delays, 250 ms clock skew, 10% worker crashes |

**What happened.** One workflow (`j-2` in both cases) is still `running` when the quiet phase
runs out of virtual time, 30 days after the faults stopped. The oracle reports it as I1, the
liveness check behind G8: once faults stop, every open run must make progress. Every other run
in the same simulation completes, and every other invariant holds — the history is intact, no
timer fired twice, no effect was applied twice, no write escaped its fence.

**What is known.** Each fault alone is survived at a higher rate than the combination: on seed
23 the same workload finishes with 10% failed writes alone, with 5% ambiguous commits alone,
and with 2 s commit delays alone. It is the compounding that stalls it. The faults stop at the
quiet phase (`TestWI042FaultsStopAtTheQuietPhase` proves that), so the cluster has 30 virtual
days of perfect health in which it fails to finish one run. That points at state that is lost
rather than at a fault that keeps arriving: something a healthy cluster never re-derives.

**Not yet known.** Which piece of state. The candidates are the transfer processor's ack level
and `pushed` map after an ambiguous commit (an outbox record the owner believes it has
dispatched and the poll handler believes it has deleted), and a workflow task that was started
and whose worker died while the write that recorded the start was reported as failed.

**Next step.** `wfe sim --seeds 13` reproduces it in under a second with the trace; the
interleaving around `j-2`'s last workflow task is where to look.
