# Agent workflow — gt-core

Quick onboarding for Claude / human contributors working on the module system + tenancy primitives.

## STOP — read these first

1. **[docs/04-non-negotiables.md](docs/04-non-negotiables.md)** — 16 hard invariants (15 ported from the upstream app + NN-16 bead taxonomy). Stop-the-line if violated.
2. **[docs/03-architecture-guardrails.md](docs/03-architecture-guardrails.md)** — folder structure + kernel migration policy + module-system on-ramp.
3. **[docs/02-sse-pattern.md](docs/02-sse-pattern.md)** — streaming endpoint conventions.
4. **[docs/01-migration-plan.md](docs/01-migration-plan.md)** — what ships when.

If the bead you claimed conflicts with these docs, file a doc gap via `meta.report_gap` MCP — do NOT improvise.

## Where things are

- **Tracking:** Dolt `hq.issues` via the gt-core MCP server at `http://127.0.0.1:8765/mcp` (cutover done 2026-06-01 — the upstream legacy gt-mcp retired; this server exposes the `issues.*` tools only). Epics: `hq-mod` (66 beads) + `hq-mt` (95 beads).
- **Bead taxonomy (NN-16, mandatory):** `epic → sub-epic → bead`. Epic = `issue_type=epic`. Sub-epic = the `external_ref` (the canonical grouping key, e.g. `hq-mt-cli`). Bead = `issue_type=task`, id `<sub-epic>.<n>`. Every non-epic bead MUST set `external_ref` to its sub-epic and have id `<external_ref>.<n>`. Sub-epic → epic is by name prefix. Enforced at the MCP boundary — see docs/04 NN-16.
- **Code:** here, `/home/nixos/gt-core`.
- **Memory:** `~/.claude/projects/-home-nixos-gt-core/memory/` (upstream archive at `-home-nixos-gastown/memory/` for crate-port history).
- **Consumer:** the upstream app at `/home/nixos/gastown` pulls these crates via `[patch.crates-io]`.

## Claim a bead

```bash
# 1) See what's open
gt mcp call meta.help
# or browse epic
gt mcp resource 'gt://issues?external_ref=hq-mod-core'

# 2) Pre-claim reality check (ONE command — gate on exit 0 == READY).
#    Verifies dep deliverables exist, own crate is scaffolded, no branch/worktree
#    contention, and the bead isn't already shipped on origin/main (MCP status
#    lags main by minutes). Exit 1 == BLOCKED/CONTESTED — do NOT claim.
~/.claude/bin/gt-bead-check.sh hq-mod-core.1

# 3) Claim it (states are open/working/closed; auth comes from .gt-config)
gt mcp call issues.transition.execute '{"id":"hq-mod-core.1","target":"working"}'

# 4) Confirm before coding (avoid hijack — see memory)
git log --all --grep hq-mod-core.1
```

## Branch + worktree

Always work in a worktree on persistent storage (NEVER `/tmp` — it's tmpfs RAM and reboot wipes it):

Use a **per-actor path** (`-<actor>` suffix) so two sessions on the same bead, or
a stale dir from a crashed session, never collide on one path. The branch stays
canonical (`<bead-id>`); only the path is namespaced. See protocol §2.

```bash
cd /home/nixos/gt-core
git worktree add /home/nixos/gt-core-wt-<bead-id>-<actor> -b <bead-id> main
cd /home/nixos/gt-core-wt-<bead-id>-<actor>
```

`<actor>` = a stable per-session tag (session PID, or `iv` for an interactive
operator session). Never `Write` into a `gt-core-wt-*` you did not create.

## Commit + PR

- One bead per branch, prefer one logical commit.
- Subject ≤50 chars, conventional commits (`feat(gt-module): ...`, `chore(gt-core): ...`).
- ff-merge from `main` (not from worktree) via the serialized helper — never a raw `git merge`:

```bash
~/.claude/bin/gt-main-merge.sh <bead-id>
```

  It flock-serializes against parallel sessions, fails `--ff-only` if main advanced
  (rebase + re-run), auto-detects the town root from the branch, and runs a
  **post-merge gate**: `cargo build --workspace` plus the replay gate when the merge
  touched events/reducers. A failed gate rolls main back automatically (the helper
  never pushes). Escape hatch for a docs-only branch: `GT_SKIP_GATE=1`.

## Replay byte-for-byte gate (critical)

Any change touching events / reducers MUST pass the replay gate. `gt-main-merge.sh`
runs it automatically post-merge when the diff touches events/reducers AND the gate
package is present. The package `gt-module-events` has NOT migrated into gt-core yet
(P4-blocked — memory `project_hq_mod_events_blocked`); until then the gate runs from
the upstream repo. The command, when present:

```bash
cargo test -p gt-module-events --test replay_gate
```

## Consuming gt-core from the upstream app

Crates live at the repo root under `crates/{kernel,domain,modules}` (NO `apps/api/`
prefix — that path is stale; see memory `project_gt_core_layout`). In the upstream
`[workspace.dependencies]`, point at the real paths:

```toml
[workspace.dependencies]
gt-module           = { path = "../gt-core/crates/kernel/gt-module" }
gt-module-mcp       = { path = "../gt-core/crates/kernel/gt-module-mcp" }
gt-module-migrate   = { path = "../gt-core/crates/kernel/gt-module-migrate" }
gt-module-contracts = { path = "../gt-core/crates/kernel/gt-module-contracts" }
gt-feature-flags    = { path = "../gt-core/crates/kernel/gt-feature-flags" }
gt-hooks            = { path = "../gt-core/crates/kernel/gt-hooks" }
```

Run `ls crates/kernel crates/domain` for the current set — `gt-module-routes` and
`gt-module-events` do NOT exist yet (events is P4-blocked, see memory
`project_hq_mod_events_blocked`). **Phase-2 path-patch is currently blocked** on the
`gt-workspace` name collision (upstream town-root vs gt-core tenant) — decided rename
the upstream's to `gt-townroot`; until that lands, only the safe subset above patches
cleanly (memory `project_crate_name_collisions`). `cargo build` in the upstream then picks
up gt-core's path version transparently.

## Multi-agent coordination

- Run `~/.claude/bin/gt-bead-check.sh <bead-id>` before claiming — it scans for a
  branch/worktree/open-process on the bead and for a shipped-but-not-closed commit
  on origin/main. (The old `gt://agent/sessions` resource retired with the upstream
  gt-mcp; the new server exposes `issues.*` only.)
- A worktree on a branch you didn't create = another agent owns it. Don't hijack.
- Memory entry [feedback_worktree_hijack_parallel] applies here too.
- Main-merge race: `gt-main-merge.sh` flock-serializes + `--ff-only`-fails if main
  advanced; its post-merge gate verifies green AFTER the merge, not just before.

## Style

- Comments in English (matches upstream convention).
- Hexagonal: domain crate exports Port + InMemory adapter; heavy adapters (PG repo, axum extractor) live in the SAME domain crate behind off-by-default features (`pg`, `axum`), NOT separate adapter crates (docs/03 Rule 4). Generic migration-SQL plumbing is the kernel crate `crates/kernel/gt-store-pg` (already in gt-core).
- No `tokio::spawn` outside of explicitly-marked relay / actor crates.
- No `dyn Trait` in kernel except `gt-plugin` (cross-module subscribe needs erasure).
