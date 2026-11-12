# Running a cluster on more than one machine

Status: design, 2026-11-12. Nothing here runs yet.

## Where things stand

The spec has always described a multi-node cluster (SPEC §6, §9): identical `wfe` nodes, one
shared Postgres, shards owned by lease and protected by fences, frontends forwarding to the
shard owner over `Transport`. What exists today is narrower:

| Piece | State |
|---|---|
| Single node, in memory (M1) | Server, handlers, loops, HTTP API merged; worker example (WI-024) queued |
| `wfe` binary | Stub: prints "not built yet". `wfe server` arrives with WI-025, memstore only |
| Postgres store (WI-030/031) | Merged. Crash recovery (WI-032) merged |
| Simulation (M3) | Not started. M4 items depend on its fault injectors |
| Shard leases, routing (WI-050/051/052) | Backlog. Tested only inside the simulator |
| `Transport` | Interface only (`internal/transport`). No production implementation |
| Cluster config | None. No node ID, peer list, advertise address or shared token key |

So the engine does not run on two machines, and M4 as written would not get it there either:
WI-051/052 prove routing over the *simulated* network. Three pieces are missing between "M4
passes in simulation" and "two Macs share the work": a real transport, a cluster config, and a
smoke test on real hardware.

## Target: laptop + mini.local

```
 laptop (node-a)                         mini.local (node-b)
 ┌──────────────────────┐   HTTP :7234   ┌──────────────────────┐
 │ wfe server           │◀──────────────▶│ wfe server           │
 │  API :7233           │  /internal/v1  │  API :7233           │
 └─────────┬────────────┘                └─────────┬────────────┘
           │            Postgres :5432             │
           └───────────▶ (one host, LAN) ◀─────────┘
 workers poll either node's :7233; the frontend forwards to the owner.
```

- **One Postgres** for the cluster. It is the source of truth for fences, so it lives on one
  machine (mini, in Docker, since mini has Docker and no Postgres) and both nodes reach it on
  the LAN. Replicating Postgres is out of scope (SPEC §1.2).
- **Two listeners per node.** The public API (`/v1`, SPEC §13.1) and a node-to-node endpoint
  (`/internal/v1/<method>`) on a separate port, so the internal surface can be firewalled to
  the peer list.
- **Static peers** (SPEC §9, placement v1). No discovery, no gossip.

## The three new pieces

### 1. HTTP transport (WI-053, human, tier 3)

`internal/transport/httptransport`: `Call(ctx, node, method, req, resp)` as a JSON POST to the
peer's `/internal/v1/<method>`, and a `Handler` that dispatches registered methods.

- Errors map to the existing contract: connection refused, reset, timeout and 5xx with no body
  all become `ErrUnreachable`, which callers already treat as ambiguous. A typed `{code,
  message}` reply (e.g. `not_owner`) comes back as that error, not as unreachable.
- Deadlines come from `ctx`; no retries inside the transport (routing owns retries, WI-051).
- Peers authenticate with an HMAC header over the body using the cluster key (below). Not TLS
  in v1: the cluster runs on a trusted LAN, and the docs say so.
- It lives outside the determinism lint's scope like `internal/httpapi`, since it touches the
  real network. The simulator keeps its in-process network.
- Human-owned because `ErrUnreachable` vs. typed error is exactly the ambiguity G1 and I1 rest
  on; a transport that reports a lost reply as a clean failure would cause duplicate writes the
  simulator cannot see.

### 2. Cluster config and `wfe server --config` (WI-054, factory, tier 2 with OWNER REVIEW)

`wfe.yaml`, the format SPEC §13.2 already names:

```yaml
node: node-a
listen:   {api: ":7233", internal: ":7234"}
advertise: "laptop.local:7234"      # what peers dial
peers:
  node-a: laptop.local:7234
  node-b: mini.local:7234
store: {kind: postgres, url_env: WFE_PG_URL}
cluster_key_env: WFE_CLUSTER_KEY    # 32 bytes, base64; signs task tokens and peer calls
num_shards: 64                      # must match the value stored at cluster creation
max_clock_skew: 1s                  # SPEC G5 epsilon
```

- Secrets come from the environment, never the file.
- On start, the node checks `num_shards` against the store and refuses to start on a mismatch,
  refuses a `node` that is not in `peers`, and refuses memstore with more than one peer.
- The cluster key becomes `Config.TokenKey` (WI-062 deferred multi-node key distribution to
  M4: this is it). Every node must have the same key or tokens issued by one node are rejected
  by another.
- `wfe server` without `--config` keeps WI-025's single-node memstore behavior.

### 3. Two-machine smoke test and runbook (WI-055, factory, tier 1; then an owner run)

`scripts/cluster-smoke.sh` and `docs/RUNNING-A-CLUSTER.md`.

- The script takes hosts as arguments and works with two processes on one machine (loopback,
  two ports), which is what CI and the factory sandbox run. The owner runs the same script
  with `laptop.local mini.local`.
- Steps: start Postgres, start both nodes, start 200 workflows from the §3.4 example through
  node-a with workers polling node-b, `kill -9` the node owning most shards halfway, wait for
  lease expiry, check that every run completes, no `TimerFired` is duplicated and `/metrics`
  shows the survivor owning all 64 shards. Restart the killed node and check it serves again.
- The runbook covers: Go toolchain on mini (missing today), Postgres in Docker on mini, macOS
  firewall prompts for both ports, clock sync (`sntp`; skew above `max_clock_skew` breaks G5
  timing, not safety), and building with `GOOS=darwin GOARCH=arm64` on the laptop and copying
  the binary over `ssh mini` instead of installing Go there, if preferred.

## Order

```
WI-025 CLI ─────────────┐
WI-040..042 simulation ─┴▶ WI-050 leases ─▶ WI-051 routing ─┐
                                          └▶ WI-052 queues ─┤
WI-053 HTTP transport (can start now) ─────────────────────┤
WI-054 cluster config (after WI-025, WI-053) ───────────────┴▶ WI-055 smoke ▶ owner run on mini
```

WI-053 has no dependency on the simulator and can be written now. The real two-machine run
cannot happen before WI-050/051/052, because without leases a second node would either own
nothing or fight over every shard.

## A shortcut, and why not

Running two nodes before M4 with shards split statically by config (node-a owns 0–31, node-b
32–63, no failover) would give a demo on mini sooner. It is not worth it: it would need its own
routing path that M4 then deletes, and it would demonstrate the one property this project
exists to prove (correctness when a node dies) not holding.

## Out of scope here

TLS between nodes, rebalancing when a node returns (SPEC stretch), more than one Postgres,
dynamic membership, running the cluster outside a trusted LAN.
