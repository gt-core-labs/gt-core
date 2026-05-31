# Agent workflow — gt-core

Quick onboarding for Claude / human contributors working on the module system + tenancy primitives.

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

In `/home/nixos/gastown/apps/api/Cargo.toml`, add (or confirm) workspace `[patch.crates-io]`:

```toml
[patch.crates-io]
gt-module = { path = "../../gt-core/crates/gt-module" }
gt-workspace = { path = "../../gt-core/crates/gt-workspace" }
# ... etc
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
