# gt-core

Foundational module system + multi-tenant primitives for Gas Town and downstream apps.

**Role:** library workspace. Kernel-tier plumbing + cross-app domain primitives. App-specific domain logic (rigs/beads/sessions/merge/quota/convoy/crew/feed) stays in [gastown](../gastown) until ported per [docs/01-migration-plan.md](docs/01-migration-plan.md). Frontend lives in a separate repo.

**Status:** Primary working repo as of 2026-05-31. New `hq-mod-*` / `hq-mt-*` beads code-land here. Tracking remains in gastown's Dolt `hq.issues` (via gt-mcp) until a dedicated tracking surface ships.

## Layered layout

```
crates/
├── kernel/                       foundation, no domain logic
│   ├── gt-module                 GtModule trait + RootBuilder       hq-mod-core
│   ├── gt-mod-routes             axum router composition            hq-mod-routes
│   ├── gt-mod-mcp                MCP tool registry                  hq-mod-mcp
│   ├── gt-mod-events             event versioning .v1/.v2           hq-mod-events
│   ├── gt-mod-migrate            per-module SQL loader              hq-mod-migrate
│   ├── gt-mod-contracts          schema dump + TS codegen           hq-mod-contracts
│   ├── gt-feature-flags          FF repo + per-ws overrides         hq-mod-flags
│   └── gt-hooks                  HookPoint + Handler + Registry     hq-mod-hooks
├── domain/                       Gas Town semantic primitives
│   ├── platform/
│   │   └── gt-workspace          WorkspaceId + catalog + repo       hq-mt-core
│   ├── orchestration/
│   │   ├── gt-webhooks           inbound /api/v1/webhooks/<src>     hq-mod-hooks
│   │   └── gt-dog                worker + dispatcher + executor     hq-mod-dogs
│   ├── lifecycle/                (reserved — gt-agent, gt-polecat when ported P4)
│   └── roles/                    (reserved — sheriff/deacon/etc. when ported P4)
├── modules/                      (reserved — mod-kanban, mod-pages, etc., P6)
└── bins/                         (reserved — own binaries; rare)

examples/
└── mod-hello                     smallest viable module             hq-mod-docs.2

docs/
├── 00-overview.md                layering + boundaries
└── 01-migration-plan.md          6 phases gated

CLAUDE.md                         daily rules (project root)
.claude/CLAUDE.md                 project hints
AGENTS.md                         claim/branch/commit drill
Cargo.toml                        workspace + centralized path deps
```

## Layered model

```
modules    ← pluggable features (kanban, pages, cycles, intake)
   │
domain     ← Gas Town semantics: workspaces, dogs, polecats, roles
 ├ modules-platform-orchestration-lifecycle-roles
   │
kernel     ← pure plumbing: GtModule, registries, hooks, events
   │
gastown-kernel (still in gastown until P4): gt-events, gt-bus, gt-audit, gt-plugin, gt-telemetry
```

Dependency direction: down only. Kernel never depends on domain. Domain never depends on modules.

## Adding a new crate

1. Pick tier (`crates/<tier>/<group>/<name>`).
2. Add path to `Cargo.toml` `[workspace] members`.
3. Add path to `Cargo.toml` `[workspace.dependencies]` so siblings refer to it as `<name> = { workspace = true }`.
4. Member `Cargo.toml` uses `workspace = true` only.

## Workflow for agents

1. Read [AGENTS.md](AGENTS.md) for claim/branch/commit conventions.
2. Tracking: gastown `hq.issues` via gt-mcp.
3. Code: here. Branch off `main`, PR, ff-merge.
4. Gastown consumes gt-core via `[workspace.dependencies]` path patches (see Phase 2 in migration plan).

## What stays in gastown

- Domain crates listed above (until P3/P4 ports them).
- Bins: `gt`, `gt-web`, `gt-mcp`.
- `deploy/`, compose, observability dashboards.

## Frontend (separate repo)

The dashboard SvelteKit app — including module UI bundles (`ui/nav.ts`, `ui/routes.ts`, widgets) — ships in a separate repo. gt-core publishes contract JSON schemas under `crates/<tier>/<group>/<module>/contracts/v<N>.json`; the frontend codegens TS types from those.

## License

Dual MIT + Apache-2.0.
