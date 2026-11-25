#!/usr/bin/env bash
# A cluster, under a kill -9 (WI-055).
#
#   scripts/cluster-smoke.sh                      two nodes on loopback
#   scripts/cluster-smoke.sh laptop.local mini.local   two machines
#
# It starts two nodes over one Postgres, runs 200 workflows through node A with workers
# polling node B, kills -9 whichever node owns more shards halfway through, and then checks
# the things a cluster has to get right:
#
#   1. every run completes, including the ones whose shards were on the dead node;
#   2. no timer fires twice, which a failover is the classic way to break (G5);
#   3. the survivor ends up owning every shard (/metrics);
#   4. the killed node serves again when it comes back.
#
# Each check prints its own line and a failure exits non-zero naming the one that failed.
#
# PG_URL must point at a Postgres both hosts can reach. docs/RUNNING-A-CLUSTER.md covers
# running it across two machines, where this script needs ssh to the second one.

set -euo pipefail

HOST_A="${1:-127.0.0.1}"
HOST_B="${2:-127.0.0.1}"
RUNS="${RUNS:-200}"
API_A="${API_A:-7233}"
API_B="${API_B:-7243}"
INT_A="${INT_A:-7234}"
INT_B="${INT_B:-7244}"
SHARDS="${SHARDS:-16}"
LEASE="${LEASE:-5s}"

if [[ -z "${PG_URL:-}" ]]; then
  echo "PG_URL is not set. A cluster needs one Postgres both nodes can reach:" >&2
  echo "  PG_URL=postgres://localhost:5432/postgres?sslmode=disable $0 $*" >&2
  exit 2
fi
if [[ "$HOST_A" != "127.0.0.1" || "$HOST_B" != "127.0.0.1" ]]; then
  echo "This script runs both nodes locally. For two machines, start node B there with its" >&2
  echo "own wfe.yaml as docs/RUNNING-A-CLUSTER.md describes, then run the checks by hand." >&2
  exit 2
fi

cd "$(dirname "$0")/.."
GO="${GO:-$(ls -d "$HOME"/sdk/go1.2*/bin/go 2>/dev/null | sort -V | tail -1 || echo go)}"
WORK="$(mktemp -d)"
SCHEMA="smoke_$(date +%s)"
export WFE_SMOKE_URL="${PG_URL}$(if [[ "$PG_URL" == *\?* ]]; then echo "&"; else echo "?"; fi)search_path=${SCHEMA}"
export WFE_SMOKE_KEY="$(head -c 32 /dev/urandom | base64)"

pids=()
cleanup() {
  for pid in ${pids[@]+"${pids[@]}"}; do kill -9 "$pid" 2>/dev/null || true; done
  wait 2>/dev/null || true
  psql_exec "DROP SCHEMA IF EXISTS ${SCHEMA} CASCADE" >/dev/null 2>&1 || true
}
trap cleanup EXIT

# curl with a deadline. Without one, a request the frontend is holding across an ownership
# change blocks the script for as long as the router is willing to wait.
CURL=(curl -sf --connect-timeout 2 --max-time 5)

say() { printf '\n\033[1m== %s\033[0m\n' "$*"; }
note() { printf '   %s\n' "$*"; }
pass() { printf '   \033[32mPASS\033[0m %s\n' "$*"; }
fail() { printf '   \033[31mFAIL\033[0m %s\n' "$*"; failed=$((failed + 1)); }
failed=0

psql_exec() { psql "$PG_URL" -v ON_ERROR_STOP=1 -q -c "$1"; }

say "Building"
"$GO" build -o "$WORK/wfe" ./cmd/wfe
"$GO" build -o "$WORK/agent" ./examples/agent/cmd/agent
note "into $WORK"

say "Preparing the store"
if ! command -v psql >/dev/null; then
  echo "psql is needed to create the test schema" >&2
  exit 2
fi
psql_exec "CREATE SCHEMA ${SCHEMA}"
note "schema ${SCHEMA}"

write_config() { # write_config <node> <api> <internal>
  cat >"$WORK/$1.yaml" <<YAML
node: $1
listen: {api: "127.0.0.1:$2", internal: "127.0.0.1:$3"}
peers:
  node-a: 127.0.0.1:${INT_A}
  node-b: 127.0.0.1:${INT_B}
store: {kind: postgres, url_env: WFE_SMOKE_URL}
cluster_key_env: WFE_SMOKE_KEY
num_shards: ${SHARDS}
lease_duration: ${LEASE}
YAML
}
write_config node-a "$API_A" "$INT_A"
write_config node-b "$API_B" "$INT_B"

# start_node <node> <api port>: starts a node and sets NODE_PID. It must not run in a
# command substitution: that would start the node in a subshell, leaving the parent with no
# pid to kill and a process that outlives the script.
NODE_PID=""
start_node() {
  "$WORK/wfe" server --config "$WORK/$1.yaml" >>"$WORK/$1.log" 2>&1 &
  NODE_PID=$!
  pids+=("$NODE_PID")
  for _ in $(seq 100); do
    if ! kill -0 "$NODE_PID" 2>/dev/null; then
      echo "node $1 exited at once; see $WORK/$1.log" >&2
      tail -20 "$WORK/$1.log" >&2
      exit 1
    fi
    if "${CURL[@]}" "http://127.0.0.1:$2/healthz" >/dev/null 2>&1; then return; fi
    sleep 0.1
  done
  echo "node $1 did not come up; see $WORK/$1.log" >&2
  tail -20 "$WORK/$1.log" >&2
  exit 1
}

shards_owned() { # shards_owned <api port>
  "${CURL[@]}" "http://127.0.0.1:$1/metrics" 2>/dev/null | awk '/^wfe_shards_owned /{print $2}' || echo 0
}

# Reads and starts go to whichever node is up. A client talks to any node, and after the kill
# there is only one left to talk to.
read_api="$API_B"
start_api="$API_A"
history_of() { "${CURL[@]}" "http://127.0.0.1:${read_api}/v1/workflows/$1/history" 2>/dev/null || true; }
status_of() { "${CURL[@]}" "http://127.0.0.1:${read_api}/v1/workflows/$1" 2>/dev/null | sed -n 's/.*"status":"\([a-z_]*\)".*/\1/p'; }

# start_run <n>: starts one workflow, retrying a few times, since a start that raced the kill
# may have reached a node that was already gone.
start_run() {
  local i="$1"
  # Long enough to outlast a lease: a run whose shard was on the dead node cannot be
  # served until the survivor takes that shard over, which needs the lease to lapse.
  for _ in $(seq 20); do
    if "$WORK/wfe" workflow start "smoke-$i" --addr "http://127.0.0.1:${start_api}" \
      --workflow-type Smoke --task-queue smoke \
      --input "{\"ref\":\"r$i\",\"sleep\":\"2s\"}" >/dev/null 2>>"$WORK/start.log"; then
      return 0
    fi
    sleep 1
  done
  return 1
}

say "Starting both nodes"
start_node node-a "$API_A"; pid_a=$NODE_PID
start_node node-b "$API_B"; pid_b=$NODE_PID
note "node-a pid $pid_a on :$API_A, node-b pid $pid_b on :$API_B"

say "Waiting for the shards to be placed"
for _ in $(seq 100); do
  a=$(shards_owned "$API_A"); b=$(shards_owned "$API_B")
  if [[ "${a:-0}" -gt 0 && "${b:-0}" -gt 0 && $((a + b)) -eq "$SHARDS" ]]; then break; fi
  sleep 0.2
done
note "node-a owns ${a:-0}, node-b owns ${b:-0} of $SHARDS"

say "Starting $RUNS workflows through node A, with a worker polling node B"
"$WORK/agent" worker --addr "http://127.0.0.1:${API_B}" --task-queue smoke \
  --ledger "$WORK/ledger.json" --mail "$WORK/mail.json" >>"$WORK/worker.log" 2>&1 &
pids+=("$!")

failed_starts=0
for i in $(seq 1 "$RUNS"); do
  start_run "$i" || failed_starts=$((failed_starts + 1))
  if [[ $i -eq $((RUNS / 2)) ]]; then
    say "Halfway: killing the node that owns more shards"
    a=$(shards_owned "$API_A"); b=$(shards_owned "$API_B")
    if [[ "${a:-0}" -ge "${b:-0}" ]]; then
      victim=node-a victim_pid=$pid_a victim_api=$API_A survivor_api=$API_B
    else
      victim=node-b victim_pid=$pid_b victim_api=$API_B survivor_api=$API_A
    fi
    note "$victim owned ${a:-0}/${b:-0}; kill -9 $victim_pid"
    kill -9 "$victim_pid" 2>/dev/null || true
    # The client now talks to the survivor, which is what a client with two addresses does.
    read_api="$survivor_api"
    start_api="$survivor_api"
    note "client switched to the survivor on :$survivor_api"
  fi
done
if [[ $failed_starts -eq 0 ]]; then
  note "all $RUNS started"
else
  note "$failed_starts of $RUNS starts never got through"
fi

say "Check 1: every run completes"
deadline=$((SECONDS + 180))
incomplete=0
while [[ $SECONDS -lt $deadline ]]; do
  incomplete=0
  for i in $(seq 1 "$RUNS"); do
    [[ "$(status_of "smoke-$i")" == "completed" ]] || incomplete=$((incomplete + 1))
  done
  [[ $incomplete -eq 0 ]] && break
  note "$incomplete of $RUNS still running"
  sleep 2
done
if [[ $incomplete -eq 0 && $failed_starts -eq 0 ]]; then
  pass "all $RUNS runs started and completed, with a node killed halfway"
elif [[ $failed_starts -gt 0 ]]; then
  fail "$failed_starts of $RUNS starts never got through"
else
  fail "$incomplete of $RUNS runs did not complete"
fi

say "Check 2: no timer fires twice"
dupes=0
for i in $(seq 1 "$RUNS"); do
  fires=$(history_of "smoke-$i" | grep -o 'TimerFired' | wc -l | tr -d ' ')
  if [[ "${fires:-0}" -gt 1 ]]; then
    note "smoke-$i fired $fires timers"
    dupes=$((dupes + 1))
  fi
done
if [[ $dupes -eq 0 ]]; then
  pass "no run fired a timer more than once"
else
  fail "$dupes run(s) fired a timer more than once"
fi

say "Check 3: the survivor owns every shard"
deadline=$((SECONDS + 60))
survivor_shards=0
while [[ $SECONDS -lt $deadline ]]; do
  survivor_shards=$(shards_owned "$survivor_api")
  [[ "${survivor_shards:-0}" -eq "$SHARDS" ]] && break
  sleep 1
done
if [[ "${survivor_shards:-0}" -eq "$SHARDS" ]]; then
  pass "the survivor owns all $SHARDS shards"
else
  fail "the survivor owns ${survivor_shards:-0} of $SHARDS shards"
fi

say "Check 4: the killed node serves again after a restart"
start_node "$victim" "$victim_api"
deadline=$((SECONDS + 60))
back=0
while [[ $SECONDS -lt $deadline ]]; do
  back=$(shards_owned "$victim_api")
  [[ "${back:-0}" -gt 0 ]] && break
  sleep 1
done
if [[ "${back:-0}" -gt 0 ]]; then
  pass "$victim is back and owns ${back} shard(s)"
else
  fail "$victim came back but owns no shards"
fi

say "Result"
if [[ $failed -eq 0 ]]; then
  note "4 of 4 checks passed. Logs: $WORK"
  exit 0
fi
note "$failed check(s) failed. Logs: $WORK"
exit 1
