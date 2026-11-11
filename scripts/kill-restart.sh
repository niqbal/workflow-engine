#!/bin/sh
# Crash-recovery check (WI-032, SPEC §16 M2): kill the server mid-run, restart it over the
# same store, and require that every run completes with no duplicate TimerFired.
#
# There is no server binary to kill -9 yet (cmd/wfe arrives with WI-025), so this runs the
# in-process integration test, which kills the node at every step of a timer-and-activity
# workload and restarts a new incarnation with nothing in memory. Extra arguments are passed
# to go test, e.g. scripts/kill-restart.sh -count=20
set -eu
cd "$(dirname "$0")/.."
# Same toolchain choice as the Makefile: Go from ~/sdk when present (system Go is too old).
GO="${GO:-$(ls -d "$HOME"/sdk/go1.2*/bin/go 2>/dev/null | sort -V | tail -1)}"
GO="${GO:-go}"
exec "$GO" test -race -count=1 -run 'TestRestart' -v "$@" ./internal/server/
