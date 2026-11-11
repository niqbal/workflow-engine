# workflow-engine

A durable workflow engine built from scratch in Go, in the style of Temporal: workflows are
ordinary Go functions whose progress is recorded as an append-only event history, so a crash
at any point resumes exactly where it stopped. Activities retry, timers survive restarts,
and signals let a workflow pause for a human and resume on a webhook.

The point of the project is **correctness under failure, proven**: every component takes its
clock, randomness and network from interfaces, so the whole cluster can run inside a
deterministic simulator that crashes nodes, partitions the network and skews clocks from a
seed, then checks invariants and replays any failure exactly.

| Document | What it holds |
|---|---|
| [docs/SPEC.md](docs/SPEC.md) | The specification: model, guarantees, architecture, storage, sharding, simulation, milestones |
| [docs/WORK-ITEMS.md](docs/WORK-ITEMS.md) | The spec cut into factory-sized work items, with acceptance criteria |
| [docs/BUILD-LOG.md](docs/BUILD-LOG.md) | Every step taken, in order, with decisions and their reasons |
| [docs/REVIEWING.md](docs/REVIEWING.md) | **Owner's runbook**: how to review and steer factory jobs (`scripts/factory`) |
| `factory/items.py` | The single list of work items; generates WORK-ITEMS.md and the backlog |
| `factory/backlog/`, `factory/ready/` | Factory items as files `asf` can ingest; move to `ready/` when dependencies are merged |

Status: **specification v0.2 (revised after independent review), no code yet.** See the build log for what comes next.
