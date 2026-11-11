# Rules for agents working on this repository

Read `docs/SPEC.md` for what the system must do. The work item tells you which sections matter.

1. **Toolchain.** Use the configured checks (`run_check`); they call Go 1.27 from `~/sdk`.
   The `go` on PATH is too old.
2. **Determinism (SPEC §6.1).** Code under `internal/server`, `internal/shard`,
   `internal/history`, `internal/store/memstore` and `internal/store/pgstore` must not use
   `time.Now/Since/Sleep/After`, `context.WithTimeout/WithDeadline`, `math/rand`, `net`,
   package `sync` (except `sync/atomic`), `go` statements, channels, `select`, or `range` over a
   map. Use `clock.Clock`, `rnd.Rand`, `xsync.Runtime` (`Go`, `NewLocker`, `NewCond`, `Yield`),
   `xsync.Chan` and `xsync.SortedKeys`. `internal/lint` enforces this in `go test`.
3. **Store calls are yield points.** A Store implementation calls `rt.Yield()` at the start of
   every method when it has a runtime.
4. **Contracts and hand-owned code.** Test contracts (`internal/store/storetest`,
   `contracttest`, `internal/server/contract_test.go`) are fixed: if one looks wrong, stop and
   report it as `blocked_reason`. Hand-owned code (`internal/server/api`, `internal/history`,
   `internal/engine`, `internal/store/store.go`, `internal/server/server.go`, `runs.go`,
   `go.mod`) may be changed **only when the work item cannot be done otherwise**. In that case,
   make the smallest change that works, touch nothing else in those files, and start your
   summary with `OWNER REVIEW:` followed by each hand-owned change and why it is needed. The job
   becomes tier 3 and waits for the owner, who reviews exactly those lines. Do not stop to ask.
5. **Tests.** New behavior gets new tests in new `_test.go` files. Edit an existing test only
   when the behavior it checks changes on purpose, and list it under `OWNER REVIEW:`. Use `simrt` (virtual time)
   rather than real sleeps; a test that sleeps on the wall clock is a defect.
6. **Style.** `gofmt` clean, `go vet` clean, no new dependencies unless the work item says so.
7. **Server state.** State a handler or processor keeps on the Server goes in a new field of
   `subsystems` (`internal/server/subsystems.go`), not in the `Server` struct. Configuration
   (`Config` in server.go) is hand-owned: add a field under rule 4 when an item needs one.
8. **Reviewers.** A minimal hand-owned change explained under `OWNER REVIEW:` is not a reason
   to block. Report anything wrong with it as you would any other finding; the owner approves
   the rest.
