# Running a cluster

Two nodes, one Postgres, on a LAN you trust. This is the operational half of SPEC §9 and
docs/MULTI-MACHINE.md: what to install, what to configure, which ports to open, and how to
tell whether it is working.

The worked example is a laptop and a Mac mini, because that is the cluster this was built
against. Any two hosts that can reach each other and one Postgres will do.

```
 laptop (node-a)                          mini.local (node-b)
 ┌──────────────────────┐   :7234 ↔ :7234  ┌──────────────────────┐
 │ wfe server --config  │◀────────────────▶│ wfe server --config  │
 │  api      :7233      │   /internal/v1   │  api      :7233      │
 └──────────┬───────────┘                  └──────────┬───────────┘
            │              Postgres :5432             │
            └──────────────▶ (on mini, Docker) ◀──────┘
 clients and workers talk to either node's :7233; the frontend forwards to the owner.
```

## What each node needs

- **The `wfe` binary.** Either install Go 1.27 on the second machine and `go build ./cmd/wfe`,
  or cross-build on the first and copy it over:

  ```bash
  GOOS=darwin GOARCH=arm64 go build -o wfe-arm64 ./cmd/wfe && scp wfe-arm64 mini.local:~/wfe
  ```

  Cross-building is usually the shorter path: the binary has no cgo and one dependency.
- **Reachability.** Each node dials the other's internal port. Nothing else needs to be open.
- **The same cluster key.** It signs task tokens and authenticates calls between nodes, so a
  token one node issues is only valid at the other if both read the same key.
- **The same shard count.** Fixed when the cluster is created and recorded in the store. A
  node configured for a different count is refused at start-up, with both numbers in the
  message.

## Postgres

One database for the cluster; it is the source of truth for fences, so it lives on one host
and both nodes reach it over the LAN. On a machine with Docker and no Postgres:

```bash
docker run -d --name wfe-pg -p 5432:5432 \
  -e POSTGRES_PASSWORD=wfe -e POSTGRES_DB=wfe postgres:17
```

Listening on `0.0.0.0:5432` is what makes it reachable from the other node; keep it on a
trusted network. Migrations run themselves: the first node to start applies them.

Replicating Postgres is out of scope (SPEC §1.2). If the database is down, the cluster stops
making progress and loses nothing.

## The config file

`wfe.yaml` per node. Everything in it is public; secrets come from the environment.

```yaml
node: node-a                              # this node, which must appear in peers
listen:
  api: ":7233"                            # clients and workers
  internal: ":7234"                       # node-to-node, firewalled to the peers
advertise: laptop.local:7234              # what peers dial; defaults to the peers entry
peers:
  node-a: laptop.local:7234
  node-b: mini.local:7234
store:
  kind: postgres
  url_env: WFE_PG_URL                     # the variable, never the URL
cluster_key_env: WFE_CLUSTER_KEY          # base64, at least 32 bytes
num_shards: 64                            # fixed for the cluster's life
lease_duration: 30s                       # how long a shard lease lasts
max_clock_skew: 1s                        # SPEC G5's epsilon
```

`node-b`'s file is the same with `node: node-b`. `wfe server --help` lists every field.

Generate the key once and give it to both:

```bash
head -c 32 /dev/urandom | base64
```

Then on each node:

```bash
export WFE_PG_URL='postgres://postgres:wfe@mini.local:5432/wfe?sslmode=disable'
export WFE_CLUSTER_KEY='<the base64 key>'
wfe server --config wfe.yaml
```

The start-up line says what it read, without the secrets:

```
wfe node=node-a api=:7233 internal=:7234 advertise=laptop.local:7234 peers=2 store=postgres shards=64 lease=30s skew=1s
```

### What start-up refuses, and why

Each of these would otherwise show up as missing work rather than as an error:

| Refused | Because |
|---|---|
| `node` not in `peers` | nothing can reach it, so it quietly serves nothing |
| `num_shards` ≠ the store's | the shard of a workflow is `hash(id) mod num_shards`; a node that disagrees writes to shards holding other runs |
| `store.kind: memory` with more than one peer | nodes sharing nothing are separate engines under one cluster's name |
| no `cluster_key_env` with peers | every task token one node issued would be rejected by the other |
| a key under 32 bytes, or not base64 | it signs tokens and peer calls: it is a secret, not a label |
| `listen.api` equal to `listen.internal` | the internal surface needs its own port to be firewallable |
| `lease_duration` ≤ `max_clock_skew` | a lease shorter than the clocks' disagreement can expire while its owner believes it holds it |

## macOS firewall

The first run of a copied binary prompts for incoming connections — once per binary, per
machine. Accept it on both, or pre-approve:

```bash
sudo /usr/libexec/ApplicationFirewall/socketfilterfw --add "$HOME/wfe"
sudo /usr/libexec/ApplicationFirewall/socketfilterfw --unblockapp "$HOME/wfe"
```

A copied binary may also be quarantined: `xattr -d com.apple.quarantine ~/wfe`.

Check reachability before blaming the cluster:

```bash
nc -z mini.local 7234 && echo internal port open
curl -sf http://mini.local:7233/healthz && echo api up
```

## Clocks

Keep them within `max_clock_skew`:

```bash
sudo sntp -sS time.apple.com   # on both
```

Skew costs timing, not correctness. A timer may fire up to the configured skew early
(SPEC G5), and the simulator's I4 allows exactly that much. Safety does not depend on clocks
agreeing: a write carries the fence it was acquired under and the store checks it inside the
transaction, so a node with a wrong clock has its writes rejected rather than accepted late.
What skew beyond the configured bound breaks is the promise about *when* a timer fires.

## Is it working?

Shard ownership, per node:

```bash
curl -s http://laptop.local:7233/metrics | grep wfe_shards_owned
curl -s http://mini.local:7233/metrics  | grep wfe_shards_owned
```

The two numbers should add up to `num_shards`. Start a workflow through one node and read it
back through the other — a cluster that routes gives the same answer from either:

```bash
wfe workflow start order/1 --addr http://laptop.local:7233 \
  --workflow-type Smoke --task-queue smoke --input '{"ref":"r1","sleep":"2s"}'
wfe workflow history order/1 --addr http://mini.local:7233
```

Other metrics worth watching:

| Metric | Means |
|---|---|
| `wfe_shards_owned` | shards this node is serving now |
| `wfe_err_fenced_total` | writes rejected by a fence: a few around a failover is normal, a steady stream is not |
| `wfe_shard_placement_errors_total` | placement passes that could not renew or acquire; rises while the store is unreachable |
| `wfe_queue_push_failures_total` | tasks that could not be handed to their queue's owner; redispatch retries them |
| `wfe_timer_lag_seconds` | how late timers are firing |

## The smoke test

`scripts/cluster-smoke.sh` runs the whole thing on one machine, two ports, and checks what a
cluster has to get right: every run completes, no timer fires twice, the survivor ends up
owning every shard, and the killed node serves again when it returns.

```bash
PG_URL='postgres://localhost:5432/postgres?sslmode=disable' scripts/cluster-smoke.sh
```

It starts two nodes, runs 200 workflows through one while a worker polls the other, and
`kill -9`s whichever node owns more shards halfway through. Each check prints its own line;
a failure exits non-zero and names the check that failed.

Across two machines the same checks are worth running by hand: start both nodes from their own
configs, start runs through one with a worker polling the other, `kill -9` one node, and watch
`wfe_shards_owned` on the survivor reach `num_shards` before every run completes. The script
does not drive a second host — it would need ssh and credentials it has no business holding.

## What a failover looks like

A node dies holding half the shards. For up to `lease_duration` nothing moves: its leases are
still live, and the survivor may not take what it may not be sure is free. Then the leases
lapse, the survivor's next placement pass takes those shards, rebuilds their task queues from
the outbox, and the runs continue. Clients see calls to the dead node fail and calls to the
survivor wait — the frontend holds a forwarded request across the handover rather than
returning an error a client cannot act on.

The window is the lease, which is why it is configurable. Shorter means faster failover and
less tolerance for a slow store; longer means the opposite. The default of 30 s is a
compromise, and the smoke test runs with 5 s to keep itself quick.

## Not in scope

TLS between nodes (the cluster assumes a trusted LAN, and this document is where that
assumption is written down), rebalancing when a node returns, more than one Postgres, dynamic
membership, and running across a network you do not control.
