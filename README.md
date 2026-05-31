# gt-core

Foundational module system + multi-tenant primitives for Gas Town and any downstream app.

**Role:** kernel-tier crates only. Domain logic (rigs, beads, sessions, merge, quota, convoy, crew, feed) stays in [gastown](../gastown) for now; portion migrates here per [docs/01-migration-plan.md](docs/01-migration-plan.md).

**Status:** Primary working repo as of 2026-05-31. New `hq-mod-*` / `hq-mt-*` beads code-land here. Tracking remains in gastown's Dolt `hq.issues` (via gt-mcp) until a dedicated tracking surface ships.

## Crates

| Crate | Purpose | Epic |
|-------|---------|------|
| `gt-module` | `GtModule` trait + `RootBuilder` + Capability + topo deps | `hq-mod-core` |
| `gt-mod-routes` | axum router composition per module + scope guards + OpenAPI | `hq-mod-routes` |
| `gt-mod-mcp` | MCP tool registry namespaced per module | `hq-mod-mcp` |
| `gt-mod-events` | Event versioning (`.v1`/`.v2`) + cross-module subscribe | `hq-mod-events` |
| `gt-mod-migrate` | Per-module SQL migration loader | `hq-mod-migrate` |
| `gt-feature-flags` | Feature-flag repo + per-workspace overrides | `hq-mod-flags` |
| `gt-mod-contracts` | TS DTO codegen + frozen contracts | `hq-mod-contracts` |
| `gt-hooks` | HookPoint + HookHandler + HookRegistry (lifecycle hooks) | `hq-mod-hooks` |
| `gt-webhooks` | Inbound webhook router + signature verify + GitHub/Linear sources | `hq-mod-hooks` |
| `gt-dog` | Dog worker + DogDispatcher + Gate evaluator + PluginExecutor | `hq-mod-dogs` |
| `gt-workspace` | WorkspaceId, catalog, repo (multi-tenant primitives) | `hq-mt-core` |
| `examples/mod-hello` | Smallest viable module — learn-by-example | `hq-mod-docs.2` |

## Workflow for agents

1. Read [AGENTS.md](AGENTS.md) for claim/branch/commit conventions.
2. Tracking lives in `gastown` (Dolt `hq.issues`). Beads `hq-mod-*` and `hq-mt-*` are claimed via gt-mcp.
3. Code work happens here in `gt-core`. Branch off `main`, PR, ff-merge.
4. `gastown` consumes gt-core via `[patch.crates-io]` path (see `apps/api/Cargo.toml` in gastown).

## What stays in gastown

- Domain crates: `gt-rig`, `gt-quota`, `gt-merge`, `gt-convoy`, `gt-polecat`, `gt-agent`, role crates.
- Bins: `gt`, `gt-web`, `gt-mcp`.
- App-specific: `apps/web` Svelte SPA, `deploy/`, compose.

## Migration policy

Foundational crates currently in `gastown` (gt-events, gt-bus, gt-audit, gt-plugin, gt-telemetry) move into `gt-core` only AFTER:

1. `hq-mod-refactor` ships (modules wrap existing domains).
2. Replay byte-for-byte gate green in both repos.
3. CI green across both.

Until then: gt-core re-exports / wraps gastown kernel crates via git dep; no destructive moves.

## License

Dual MIT + Apache-2.0.
