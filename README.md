# gt-core

Foundational module system + multi-tenant primitives for Gas Town and downstream apps. Mirrors gastown's `apps/api/crates/{kernel,domain/*}` layout.

**Role:** kernel-tier + cross-app domain primitives only. App-specific domain logic (rigs/beads/sessions/merge/quota/convoy/crew/feed) stays in [gastown](../gastown) until ported per [docs/01-migration-plan.md](docs/01-migration-plan.md). Frontend lives in a separate repo.

**Status:** Primary working repo as of 2026-05-31. New `hq-mod-*` / `hq-mt-*` beads code-land here. Tracking remains in gastown's Dolt `hq.issues` (via gt-mcp) until a dedicated tracking surface ships.

## Layout

```
apps/api/
├── crates/
│   ├── kernel/                   foundation, no domain logic
│   │   ├── gt-module             GtModule trait + RootBuilder       hq-mod-core
│   │   ├── gt-mod-routes         axum router composition            hq-mod-routes
│   │   ├── gt-mod-mcp            MCP tool registry                  hq-mod-mcp
│   │   ├── gt-mod-events         event versioning .v1/.v2           hq-mod-events
│   │   ├── gt-mod-migrate        per-module SQL loader              hq-mod-migrate
│   │   ├── gt-mod-contracts      schema dump + TS codegen           hq-mod-contracts
│   │   ├── gt-feature-flags      FF repo + per-ws overrides         hq-mod-flags
│   │   └── gt-hooks              HookPoint + Handler + Registry     hq-mod-hooks
│   ├── domain/
│   │   ├── lifecycle/            (reserved — agent/polecat ports when ported)
│   │   ├── orchestration/
│   │   │   ├── gt-webhooks       /api/v1/webhooks/<source> router   hq-mod-hooks
│   │   │   └── gt-dog            worker + dispatcher + executor     hq-mod-dogs
│   │   ├── platform/
│   │   │   └── gt-workspace      WorkspaceId + catalog + repo       hq-mt-core
│   │   └── roles/                (reserved — sheriff/deacon/etc. when ported)
│   └── bins/                     (reserved — app-specific binaries stay in gastown)
└── examples/
    └── mod-hello                 smallest viable module             hq-mod-docs.2
```

## Workflow for agents

1. Read [AGENTS.md](AGENTS.md) for claim/branch/commit conventions.
2. Tracking lives in gastown (Dolt `hq.issues`). Beads `hq-mod-*` and `hq-mt-*` are claimed via gt-mcp.
3. Code work happens here. Branch off `main`, PR, ff-merge.
4. Gastown consumes gt-core via `[patch.crates-io]` path after the first kernel crate is ready (see Phase 2 in migration plan).

## What stays in gastown

- Domain crates: `gt-rig`, `gt-quota`, `gt-merge`, `gt-convoy`, `gt-polecat`, `gt-agent`, role crates, `gt-feed`, `gt-terminal`.
- Bins: `gt`, `gt-web`, `gt-mcp`.
- App-specific: `deploy/`, compose, observability dashboards.

## What lives in the frontend repo

The dashboard SvelteKit app — including module UI bundles (`ui/nav.ts`, `ui/routes.ts`, widgets) — ships in a separate repo. gt-core publishes contract JSON schemas under `apps/api/crates/<module>/contracts/v<N>.json`; the frontend repo codegens TS types from those.

## Migration policy

Foundational crates currently in gastown (gt-events, gt-bus, gt-audit, gt-plugin, gt-telemetry) move into `apps/api/crates/kernel/` only AFTER:

1. `hq-mod-refactor` ships (modules wrap existing domains).
2. Replay byte-for-byte gate green in both repos.
3. CI green across both.

## License

Dual MIT + Apache-2.0.
