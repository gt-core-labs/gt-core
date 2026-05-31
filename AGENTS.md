# Agent workflow — gt-core

Quick onboarding for Claude / human contributors working on the module system + tenancy primitives.

## STOP — read these first

1. **[docs/03-architecture-guardrails.md](docs/03-architecture-guardrails.md)** — invariants. Folder structure, kernel migration policy, module-system on-ramp, dep direction, event versioning, workspace boundary, replay gate.
2. **[docs/02-sse-pattern.md](docs/02-sse-pattern.md)** — streaming endpoint conventions.
3. **[docs/01-migration-plan.md](docs/01-migration-plan.md)** — what ships when.

If the bead you claimed conflicts with these docs, file a doc gap via `meta.report_gap` MCP — do NOT improvise.

## Where things are

- **Tracking:** Dolt `hq.issues` in gastown (via gt-mcp). Epics: `hq-mod` (66 beads) + `hq-mt` (95 beads).
- **Code:** here, `/home/nixos/gt-core`.
- **Memory:** `~/.claude/projects/-home-nixos-gastown/memory/`.
- **Consumer:** `/home/nixos/gastown` pulls these crates via `[patch.crates-io]`.

## Claim a bead

```bash
# 1) See what's open
gt-mcp-cli call meta.help
# or browse epic
gt-mcp-cli read gt://issues?external_ref=hq-mod-core

# 2) Transition to in_progress
gt-mcp-cli call issues.transition.execute \
  --id hq-mod-core.1 --to in_progress --actor "$GT_AGENT"

# 3) Confirm before coding (avoid hijack — see memory)
git log --all --grep hq-mod-core.1
```

## Branch + worktree

Always work in a worktree on persistent storage (NEVER `/tmp` — it's tmpfs RAM and reboot wipes it):

```bash
cd /home/nixos/gt-core
git worktree add /home/nixos/gt-core-wt-<bead-id> -b <bead-id> main
cd /home/nixos/gt-core-wt-<bead-id>
```

## Commit + PR

- One bead per branch, prefer one logical commit.
- Subject ≤50 chars, conventional commits (`feat(gt-module): ...`, `chore(gt-core): ...`).
- ff-merge from `main` (not from worktree) once review passes.

## Replay byte-for-byte gate (critical)

Any change touching events / reducers MUST pass the replay gate. Run before pushing:

```bash
cargo test -p gt-mod-events --test replay_gate
```

## Consuming gt-core from gastown

In gastown's workspace `[workspace.dependencies]`, point at the gt-core paths:

```toml
[workspace.dependencies]
gt-module        = { path = "../../gt-core/apps/api/crates/kernel/gt-module" }
gt-mod-routes    = { path = "../../gt-core/apps/api/crates/kernel/gt-mod-routes" }
gt-mod-mcp       = { path = "../../gt-core/apps/api/crates/kernel/gt-mod-mcp" }
gt-mod-events    = { path = "../../gt-core/apps/api/crates/kernel/gt-mod-events" }
gt-mod-migrate   = { path = "../../gt-core/apps/api/crates/kernel/gt-mod-migrate" }
gt-mod-contracts = { path = "../../gt-core/apps/api/crates/kernel/gt-mod-contracts" }
gt-feature-flags = { path = "../../gt-core/apps/api/crates/kernel/gt-feature-flags" }
gt-hooks         = { path = "../../gt-core/apps/api/crates/kernel/gt-hooks" }
gt-workspace     = { path = "../../gt-core/apps/api/crates/domain/platform/gt-workspace" }
gt-webhooks      = { path = "../../gt-core/apps/api/crates/domain/orchestration/gt-webhooks" }
gt-dog           = { path = "../../gt-core/apps/api/crates/domain/orchestration/gt-dog" }
```

`cargo build` in gastown picks up gt-core's path version transparently.

## Multi-agent coordination

- Check `gt-mcp-cli read gt://agent/sessions` before claiming a bead.
- A `/tmp/wt-*` worktree on a branch you didn't create = another agent owns it. Don't hijack.
- Memory entry [feedback_worktree_hijack_parallel] applies here too.
- Main-merge race: agents can ff-merge in parallel; verify `git fetch` + replay gate green AFTER the merge, not just before.

## Style

- Comments in English (matches gastown convention).
- Hexagonal: domain crate exports Port + InMemory adapter; PG adapter optional, in `gt-store-pg-*` (lives in gastown today, moves later).
- No `tokio::spawn` outside of explicitly-marked relay / actor crates.
- No `dyn Trait` in kernel except `gt-plugin` (cross-module subscribe needs erasure).
