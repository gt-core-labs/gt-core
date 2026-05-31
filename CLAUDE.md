# gt-core

Foundational module system + multi-tenant primitives for Gas Town and downstream apps.

**Primary working repo as of 2026-05-31.** Domain code migrates from `/home/nixos/gastown` into this repo crate-by-crate; tracking stays in gastown's Dolt `hq.issues` (via gt-mcp on :8765) until a dedicated gt-core tracking surface lands.

## Daily rules

- Tracking: gastown `hq.issues` via `gt-mcp` (`gt-mcp-cli` or http://127.0.0.1:8765/mcp). All work via a claimed bead in `hq-mod-*` / `hq-mt-*`.
- 23 gastown strategic beads were CLOSED with `[FROZEN 2026-05-31]` title prefix. Do NOT reopen them. They are: hq-fe-svelte (+children: hq-fe-build, hq-fe-view, hq-fe-api-r, hq-fe-cut.3, hq-fe-auth.0, hq-fe-view.9), hq-03aw (+.9, .10), hq-63az, hq-mc72 (+.12.23, .12.24), hq-oap5 (+.1, .2), hq-61v, hq-d412, hq-hamg, hq-68kn, hq-4dte, hq-jaeh. See [memory:gastown frozen](../.claude/projects/-home-nixos-gt-core/memory/project_gastown_frozen_2026_05_31.md).
- Code lives here. Branch off `main`. Worktrees under `/home/nixos/gt-core-wt-<bead-id>` (NEVER `/tmp` — tmpfs RAM, reboot wipes).
- Comments in English.
- Hexagonal: domain crate = Port + InMemory; PG adapter optional in `gt-store-pg-<X>`.
- Replay byte-for-byte gate required on every change touching events/reducers.
- One bead per branch, conventional commits (`feat(gt-module): ...`).
- Don't touch gastown unless porting a crate.

## MUST-READ before writing code

- **[docs/04-non-negotiables.md](docs/04-non-negotiables.md)** — 15 hard invariants ported from gastown. Stop-the-line if violated; battle scars behind every rule.
- **[docs/03-architecture-guardrails.md](docs/03-architecture-guardrails.md)** — folder structure, kernel migrates UP from gastown (no re-invention), module system is the only on-ramp, dep direction one-way, events versioned + replay-safe.
- **[docs/02-sse-pattern.md](docs/02-sse-pattern.md)** — auth via cookie, per-workspace channel keying, Last-Event-ID, KeepAlive. Read before adding any streaming endpoint.

## Layout

See [README.md](README.md) for crate table. [docs/00-overview.md](docs/00-overview.md) for layering. [docs/01-migration-plan.md](docs/01-migration-plan.md) for what comes from gastown and when. [AGENTS.md](AGENTS.md) for claim/branch/commit drill.

## Anti-patterns (see docs/03 for full list + rationale)

- Adding `tokio::spawn` in kernel crates. Allowed only in `gt-plugin` relay and explicit actor crates.
- Adding `dyn Trait` in kernel crates except observer plugins.
- Mutating domain state from observer subscriptions. Cross-module communication is event-driven only.
- Hand-wiring routes / MCP tools / migrations in app composition root. Use `RootBuilder::new(ws).module(...).build()`.
- Re-using a `/tmp/wt-*` worktree someone else created.
- **Re-inventing kernel primitives** that already exist in gastown (gt-events, gt-bus, gt-audit, gt-plugin, gt-telemetry). They migrate up in P4 — don't fork them.
- **Adding top-level folders** outside `crates/`, `examples/`, `docs/`. The taxonomy is fixed.
- **Cross-tier downward deps** (kernel depending on domain, domain depending on modules). One-way only.
- **Unversioned event kinds** (`bead.created` instead of `bead.created.v1`).
- **Workspace_id in MCP payload / URL / body.** Server-injected from auth ctx; spoofing rejected.
