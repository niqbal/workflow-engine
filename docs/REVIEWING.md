# Reviewing factory work: the owner's runbook

The software factory (`asf`) builds work items in sandboxes and stops for you in a few
predictable ways. This page says what to do in each case. `scripts/factory` does the mechanical
parts; run everything from `~/work/workflow-engine`.

## Start here: the console

```bash
~/work/software-factory/.venv/bin/asf --repo ~/work/workflow-engine ui --open
```

It opens http://127.0.0.1:8787. Everything below is a button there:
- **Needs you:** jobs ready to review, plus anything that stopped, with the reason.
- **Working:** what agents are doing right now, with a live log.
- **Backlog:** each item is shown as unblocked or blocked by its `depends_on`, with a Queue
  button.
- **Job details:** owner-review lines, reviewer findings, diff, report and timeline.
- **Actions:** Merge, Send back with notes, Reject, Resume, Retry, Start over.

The header has **Run queue** (agents work through everything queued) and **Pause all**.
The cases below explain what each stop means; the console shows the same reasons.

**Delivered is not done.** A job counts as shipped only after it is merged *and* `make ci`
passes on main afterwards (`observe.post_merge`). The console notices merges you make with git
too.

## The loop from a terminal

```bash
scripts/factory pending          # 1. what is waiting on me, and why
scripts/factory show JOB-xxxxxx  # 2. the full story of one job
# 3. act on it: see "Cases" below
scripts/factory next             # 4. what is unblocked now
scripts/factory queue WI-0xx     # 5. hand it to the factory
scripts/factory run all          # 6. let it work (foreground; takes 10-40 minutes per job)
```

Jobs never merge on their own: shadow mode is on, so every change reaches `main` only through
you.

## Cases

`pending` prints one line per job. Match the reason to a case.

### 1. "delivered on factory/…: review and merge"

The job passed the implementer, the tester, the gates, the reviewer and the security reviewer.
You decide whether it goes to `main`.

```bash
scripts/factory show  JOB    # read any "Manual checks" it lists; do them
scripts/factory check JOB    # CI and the contract board on the branch, in a throwaway worktree
git diff main...<branch>     # read the code (focus: see the checklist below)
scripts/factory merge JOB    # merge, CI again, close the GitHub issue, mark done, push
```

If `check` shows **"Touches hand-owned files"**, read exactly those lines. Approving them is
your call (WI-017 and WI-020 each added one harmless field to `server.go`). If it is wrong,
don't merge. Use case 5.

### 2. "gates failed: diff_size …"

The change is bigger than `limits.max_changed_lines` in `factory.yaml`. Decide whether **the
work is bloated** or **the limit is too tight**. Tests count as changed lines, and items must
ship tests, so the limit is usually the problem.

- Limit too tight: raise it in `factory.yaml`, commit, then
  `scripts/factory retry JOB` and `scripts/factory run JOB`. The retry resumes at the gates
  with the same work; nothing is re-implemented.
- Work bloated: `scripts/factory rerun JOB` after tightening the item (case 3).

### 3. "clarification needed: …"

Triage found the work item ambiguous. It costs about nothing and is usually right: WI-011 asked
for an API the spec had left open.

1. Answer the question **in the work item**: edit its entry in `factory/items.py`, the
   single source of truth.
2. Regenerate: `python3 factory/items.py`. That rewrites `docs/WORK-ITEMS.md` and backlog
   files. For an item already in `factory/ready/`, copy the new text into that file too, or
   move it back to backlog and queue it again.
3. Update the GitHub issue: `python3 factory/github_sync.py --update WI-0xx`.
4. Commit, push, then `scripts/factory rerun JOB` and `scripts/factory run all`.

### 4. "implementer blocked: …"

The implementer refused to work around something it may not change. It is usually right, and
the fix is yours:

| It says | Do |
|---|---|
| Needs a dependency (`go.mod` is hand-owned) | `~/sdk/go1.27.4/bin/go get module@version`, run `make ci`, commit, then `scripts/factory rerun JOB` (a fresh job sees the new `go.mod`; `retry` would not) |
| A contract test looks wrong | Read the scenario. If the test is wrong (WI-019 was right about one), fix it on `main`. If the handlers were otherwise finished, use case 6 to keep the work |
| A hand-owned file needs to change | Make the change yourself on `main`, then `rerun` |

### 5. "reviewers still block after fix rounds: …"

The reviewer found problems the implementer could not fix in its rounds. Read the findings in
`show`.

- Finding is real and fixable by the factory: `scripts/factory rerun JOB` after putting the
  finding into the work item's text, so the next implementer knows.
- Finding is "touched a hand-owned file" and you approve the change: case 6.
- Finding is wrong: case 6, and say in the merge commit why you overruled it.

### 6. Finished work stuck in a stopped job

When the job stopped before delivery but the work in its sandbox is good:

```bash
scripts/factory adopt JOB     # commits the sandbox work to factory/<item>-adopted-<job>
scripts/factory check JOB
git diff main...<branch>
scripts/factory merge JOB
```

The factory's later stages (tester, reviewer, security) did **not** run on adopted work, so
review it as if nobody had. For anything non-trivial, ask Claude for an independent review
first (WI-019 was merged that way).

### 7. "paused: budget …" or "job ran N of M minutes"

`scripts/factory resume JOB` then `scripts/factory run JOB`. Time limits count wall-clock time
since the job started, including hours spent paused, so resume adds time.

### 8. "failed: … You've hit your … limit"

The Claude subscription's usage window ran out. Wait for the reset time it names, then
`scripts/factory retry JOB` and `scripts/factory run JOB`.

### 9. Something else, or you want to give up

`scripts/factory cancel JOB "reason"` marks it rejected; `scripts/factory rerun JOB` starts the
item fresh from current `main`. The audit trail of every tool call an agent made is
`asf events JOB` (path printed by `show`).

## What to look for in a diff

1. **Hand-owned files** (`check` flags them): `internal/history`, `internal/engine`,
   `contracttest`, `internal/store/storetest`, `internal/server/server.go`, `runs.go`,
   `contract_test.go`, `go.mod`, `factory.yaml`, `Makefile`.
2. **The write path:** server state changes only through `s.update` / `s.createRun`.
3. **Determinism:** `internal/lint` catches most of it; look anyway for wall-clock time,
   goroutines and map iteration in server code.
4. **Tests that test something:** a test that would still pass if the feature were deleted is
   not a test.
5. **The contract board never goes down** (`check` prints it; 26/26 is the full set for M1).

## Hand-owned items

`next` marks them `[human]`. They are the parts the guarantees rest on (simulator, oracle,
fault injectors, shard leases, GetVersion). Build them with Claude in this repo, not with the
factory. SPEC §15.1 says why.

## Where things live

| What | Where |
|---|---|
| Job state, reports, sandboxes | `~/.asf/workflow-engine-*/` |
| Work items (single source) | `factory/items.py` → `docs/WORK-ITEMS.md`, `factory/backlog/`, `factory/ready/`, `factory/done/` |
| Issue numbers | `factory/issues.json` (never type them from memory; Step 20 shows why) |
| Trust boundary | `factory.yaml` (tiers, gates, limits, budgets) and `AGENTS.md` |
| Every decision so far | `docs/BUILD-LOG.md` |
