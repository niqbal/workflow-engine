#!/usr/bin/env bash
# The showcase (WI-071, SPEC §14): a support agent that cannot lose or repeat a refund.
#
# The script kills things at the two moments where a workflow engine earns its keep:
#
#   1. while the run is parked on a human approval, it kills the server and the worker,
#      restarts both, and shows the run still parked with its 24-hour timer intact;
#   2. after the refund has reached the payment ledger and before the worker reports it, it
#      kills the worker, and shows the next one finishing the job with the ledger still at
#      exactly one payment.
#
# What is not claimed: that the model was called once. A crash can repeat the Plan activity,
# which costs money and not correctness. The claim is exactly-once effects at idempotent
# sinks, and the ledger is how you check it.
#
# Usage: PG_URL=postgres://localhost:5432/postgres?sslmode=disable examples/agent/demo.sh
#
# Postgres is required: the whole point is that state outlives the processes.

set -euo pipefail

if [[ -z "${PG_URL:-}" ]]; then
  echo "PG_URL is not set. This demo kills and restarts the server, so it needs a durable store:" >&2
  echo "  PG_URL=postgres://localhost:5432/postgres?sslmode=disable $0" >&2
  exit 2
fi

cd "$(dirname "$0")/../.."
GO="${GO:-$(ls -d "$HOME"/sdk/go1.2*/bin/go 2>/dev/null | sort -V | tail -1 || echo go)}"
WORK="$(mktemp -d)"
ADDR="127.0.0.1:${PORT:-7299}"
BASE="http://$ADDR"
LEDGER="$WORK/ledger.json"
MAIL="$WORK/mail.json"
TICKET="ticket-$(date +%s)"
TOKEN_KEY="demo-key-that-outlives-restarts"

server_pid=""
worker_pid=""
cleanup() {
  [[ -n "$worker_pid" ]] && kill "$worker_pid" 2>/dev/null || true
  [[ -n "$server_pid" ]] && kill "$server_pid" 2>/dev/null || true
  wait 2>/dev/null || true
}
trap cleanup EXIT

say() { printf '\n\033[1m== %s\033[0m\n' "$*"; }
note() { printf '   %s\n' "$*"; }

say "Building"
"$GO" build -o "$WORK/wfe" ./cmd/wfe
"$GO" build -o "$WORK/agent" ./examples/agent/cmd/agent
note "wfe and agent built into $WORK"

start_server() {
  "$WORK/wfe" server --listen "$ADDR" --store "$PG_URL" --token-key "$TOKEN_KEY" \
    --long-poll 2s --redispatch-interval 2s >>"$WORK/server.log" 2>&1 &
  server_pid=$!
  for _ in $(seq 100); do
    if curl -sf "$BASE/healthz" >/dev/null 2>&1; then return; fi
    sleep 0.1
  done
  echo "the server did not come up; $WORK/server.log has why" >&2
  exit 1
}

start_worker() {
  "$WORK/agent" worker --addr "$BASE" --ledger "$LEDGER" --mail "$MAIL" \
    --identity "worker-$RANDOM" "$@" >>"$WORK/worker.log" 2>&1 &
  worker_pid=$!
}

wait_for() { # wait_for <description> <command...>
  local what="$1"; shift
  for _ in $(seq 150); do
    if "$@" >/dev/null 2>&1; then return; fi
    sleep 0.2
  done
  echo "timed out waiting for $what" >&2
  echo "--- server.log ---" >&2; tail -20 "$WORK/server.log" >&2
  echo "--- worker.log ---" >&2; tail -20 "$WORK/worker.log" >&2
  exit 1
}

history_has() { curl -sf "$BASE/v1/workflows/$TICKET/history" | grep -q "$1"; }
status_is() { curl -sf "$BASE/v1/workflows/$TICKET" | grep -q "\"status\":\"$1\""; }
ledger_entries() { "$WORK/agent" ledger --ledger "$LEDGER" 2>/dev/null | tail -1; }

say "Starting the server on $ADDR, backed by Postgres"
start_server
note "pid $server_pid"

say "Starting an agent worker"
start_worker
note "pid $worker_pid"

say "A customer asks for a refund on order A-2002 (\$189.00)"
"$WORK/wfe" workflow start "$TICKET" --addr "$BASE" \
  --workflow-type SupportAgent --task-queue q \
  --input "{\"id\":\"$TICKET\",\"customer\":\"Grace\",\"orderID\":\"A-2002\",\"text\":\"the lamp arrived broken\"}" \
  >/dev/null
note "workflow $TICKET started"

say "The refund is over the threshold, so the agent stops and waits for a human"
wait_for "the approval point" history_has TimerStarted
"$WORK/wfe" workflow history "$TICKET" --addr "$BASE"
note "that TimerStarted is the 24-hour fallback. Nothing has been paid:"
note "$(ledger_entries)"

say "Killing the server and the worker while the run waits"
kill "$worker_pid"; wait "$worker_pid" 2>/dev/null || true; worker_pid=""
kill "$server_pid"; wait "$server_pid" 2>/dev/null || true; server_pid=""
note "both processes are gone. Nothing is running at all."

say "Starting them again"
start_server
start_worker --hang-after-refund
wait_for "the server to serve the run again" status_is running
note "the run is still open, its history intact, its timer still armed:"
"$WORK/wfe" workflow history "$TICKET" --addr "$BASE"

say "A reviewer approves the refund, by hand, with curl"
curl -sf -X POST "$BASE/v1/workflows/$TICKET/signal" \
  -H 'content-type: application/json' \
  --data '{"signalName":"approval","payload":{"approved":true,"by":"reviewer@example.com"},"requestID":"approval-1"}' \
  >/dev/null
note "signal accepted"

say "The worker applies the refund and then stops before reporting it"
wait_for "the refund to reach the ledger" test -s "$LEDGER"
note "$(ledger_entries)"
note "the money has moved. The server has not been told."

say "Killing the worker in exactly that window"
kill "$worker_pid"; wait "$worker_pid" 2>/dev/null || true; worker_pid=""
note "the attempt is gone, unreported. This is where a naive retry charges twice."

say "A new worker takes over"
start_worker
wait_for "the run to finish" status_is completed

say "The result"
"$WORK/wfe" workflow history "$TICKET" --addr "$BASE"

say "The payment ledger"
"$WORK/agent" ledger --ledger "$LEDGER"

entries=$("$WORK/agent" ledger --ledger "$LEDGER" | grep -c 'A-2002' || true)
if [[ "$entries" != "1" ]]; then
  echo "FAILED: the ledger holds $entries refunds for A-2002, want exactly 1" >&2
  exit 1
fi

say "Exactly one refund, after two kills and three workers"
note "ledger: $LEDGER"
note "logs:   $WORK/server.log, $WORK/worker.log"
