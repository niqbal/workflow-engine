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
4. **Contracts are fixed.** `internal/store/storetest`, `contracttest`, `internal/server/api`,
   `internal/history` and `internal/store/store.go` are owned by hand. Do not edit them. If a
   contract looks wrong, stop and report it as `blocked_reason` instead of working around it.
5. **Tests.** New behavior gets new tests in new `_test.go` files. Use `simrt` (virtual time)
   rather than real sleeps; a test that sleeps on the wall clock is a defect.
6. **Style.** `gofmt` clean, `go vet` clean, no new dependencies unless the work item says so.
7. **Server state.** `internal/server/server.go` and `runs.go` are hand-owned. State a handler
   or processor keeps on the Server goes in a new field of `subsystems`
   (`internal/server/subsystems.go`), never in the `Server` struct.
