# 05 — Concurrency & claim protocol (multi-session safety)

Multiple Claude sessions (and the automated driver) work this repo **in parallel,
all committing under the same git author** (`codecsrayo`). They are **not visible**
in `gt://agent/sessions`. Without a discipline, sessions race the same bead,
collide on worktree paths, and harvest each other's trees. This doc is the
stop-the-line protocol that prevents that. It is normative: if a step here
conflicts with how a tool behaves, file `meta.report_gap`, don't improvise.

Battle scar: on 2026-05-31 two sessions both "claimed" `hq-mod-core` /
`hq-mt-core.1`, one wrote files into another's worktree, and `main` advanced 6
commits in ~30 min under a third. Net wasted work + a bead left with a junk
`owner`. Every rule below traces to that incident.

## The collision surface

| Failure | Root cause | Defended by |
|---|---|---|
| Two sessions code the same bead | claim is advisory; MCP status lags `main` by minutes | §1 atomic claim |
| `git worktree add` fails `'…' already exists` | shared `wt-<bead>` path across actors | §2 per-actor worktree path |
| Your worktree hard-reset / harvested | driver assumes worktree exclusivity | §3 lease + don't-touch-others |
| Claimed a bead already in flight | only checked MCP, not git | §1 git ground-truth |
| Lost commit in merge race | parallel ff-merge to shared `main` | §4 rebase-before-merge |

## §1 — Atomic claim (the mutex)

The bead status transition **is** the lock. Treat it as compare-and-swap, not a
courtesy flag.

1. **Ground-truth via git first** (MCP sessions is unreliable, status lags):
   ```bash
   git fetch -q
   git log --oneline -8 main                  # is main advancing on its own?
   git worktree list                          # any wt for your target?
   git log --all --grep <bead-id>             # already committed?
   git branch -a | grep -E '<bead-id>|feat/<bead-id>'
   ```
   If any hit → the bead is taken. **Stand down. Pick another sub-epic.**

2. **Attempt the claim as a CAS:**
   ```
   issues.transition  id=<bead> target=working
   ```
   - `ok` → you hold it. Proceed.
   - `working -> working` (or any rejection) → **someone beat you**. Abort. Do
     NOT create a worktree, do NOT code. Re-pick.

   `target` takes `open|working|closed` (NOT `to`/`in_progress`).

3. **Never create the worktree before the claim succeeds.** Order is hard:
   claim → (only on `ok`) branch+worktree. A failed claim must leave zero
   filesystem trace.

> **Server-side hardening (tracked, not yet enforced):** `issues.transition` to
> `working` should be a single conditional write —
> `UPDATE issues SET status='working', owner=:actor WHERE id=:id AND status='open'`
> — and report **0 rows affected** as a hard "already claimed" gate. Until that
> lands, step 2's rejection check is the gate. Owner field today can't be cleared
> back to NULL (gap `hq-gap-issues-update-execute-1780255842`).

## §2 — Per-actor worktree paths

Shared `gt-core-wt-<bead>` is a collision magnet: two actors on the same bead, or
a stale dir from a crashed session, both land on one path. **Suffix the path with
an actor tag:**

```bash
git worktree add /home/nixos/gt-core-wt-<bead>-<actor> -b <bead> main
```

`<actor>` = a stable per-session tag (e.g. session PID, or `iv` for an
interactive operator session). The branch name stays canonical (`<bead>`); only
the *path* is namespaced. Result: `'… already exists'` means **you** already have
it, never someone else.

Never `Write` into a `gt-core-wt-*` directory you did not create. If your edit
target resolves inside another actor's worktree, stop — you are about to corrupt
their tree (this happened on 2026-05-31).

## §3 — Leases & not touching others' trees

A worktree on a branch you didn't create = another actor owns it. Hands off:

- Do not `git worktree remove`, `reset --hard`, or `git -C` into it.
- Do not assume a `wt-<bead>` dir you find is yours — `git worktree list` shows
  the owning branch; `lsof +D <dir>` shows live writers.
- The driver must respect the same rule: only reset/harvest a worktree whose
  `owner == self`.

If a bead's claim is stale (worktree gone, branch unmerged, no commits for a long
while), reclaim only after confirming via git that no live process holds it.

## §4 — Merge race discipline

`main` is shared; several actors ff-merge concurrently.

- ff-merge from **town root** (`/home/nixos/gt-core`), never from a worktree.
- `git fetch` + **rebase your branch onto `main` immediately before** the merge;
  re-run build + replay gate **after**, not just before.
- Frontier-style conflicts (e.g. workspace member lists, doc indexes) are
  additive unions — resolve by keeping both, never by clobbering.
- One bead per branch, one logical commit. Smaller surface = smaller race window.

## §5 — Partition the work (cheapest win)

Zero-code policy that removes most collisions outright: **actors don't share the
bead pool.**

- The automated driver drains `hq-mod-*` / `hq-mt-*` end-to-end. While it is
  active (check: `main` advancing under `codecsrayo` every few minutes), an
  interactive session should **not** hand-claim those sub-epics.
- Interactive work goes to a disjoint surface: docs, examples, a sub-epic the
  driver isn't draining, or coordination tasks — agreed out-of-band.
- When in doubt, `git log --oneline -5 main`: if it's moving on its own, you are
  not alone — partition, don't race.

## Quick checklist (before any bead)

```
[ ] git fetch; main not advancing on my target sub-epic
[ ] no worktree / branch / commit for <bead> exists
[ ] issues.transition <bead> target=working  → returned ok (not working->working)
[ ] worktree path is per-actor: wt-<bead>-<actor>
[ ] one bead, one branch, rebase-before-merge, ff from town root
[ ] replay gate green AFTER merge
```

See also: [AGENTS.md](../AGENTS.md) claim drill, [03-architecture-guardrails.md](03-architecture-guardrails.md).
