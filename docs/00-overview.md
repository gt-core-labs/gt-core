# gt-core overview

## Why

Three forces pushed the foundation out of `gastown`:

1. **Module system** ([epic hq-mod](../../gastown/apps/api/docs)): add/remove features without touching the rest. Kanban, pages, cycles, intake — each a separate crate, plugged via one line in `RootBuilder`.
2. **Multi-tenancy** ([epic hq-mt](../../gastown/apps/api/docs)): workspace = full tenant boundary. Per-ws data, compute, RBAC, deploy.
3. **Reuse across apps**: gas-town isn't the only consumer. Other product surfaces (downstream apps the user is planning) need the same primitives without dragging gas-town's domain in.

## Layering

```
                       ┌───────────────────────────────────────┐
                       │  apps:  gastown, future-app-X, …      │
                       │  - domain crates (rigs, beads, …)     │
                       │  - bins (gt-web, gt-mcp, gt CLI)      │
                       │  - frontend                            │
                       └─────────────────┬─────────────────────┘
                                         │ depends on
                       ┌─────────────────▼─────────────────────┐
                       │  gt-core (this repo)                  │
                       │  apps/api/crates/                     │
                       │   ├ kernel/                           │
                       │   │   gt-module, gt-mod-{routes,mcp,  │
                       │   │   events,migrate,contracts},      │
                       │   │   gt-feature-flags, gt-hooks      │
                       │   └ domain/                           │
                       │       platform/gt-workspace           │
                       │       orchestration/{gt-webhooks,     │
                       │                       gt-dog}         │
                       └─────────────────┬─────────────────────┘
                                         │ wraps / re-exports
                       ┌─────────────────▼─────────────────────┐
                       │  gastown kernel (today)               │
                       │  - gt-events, gt-bus, gt-audit,       │
                       │    gt-plugin, gt-telemetry            │
                       │  (migrates upward after refactor train│
                       │   ships and replay gate is green)     │
                       └───────────────────────────────────────┘
```

## Boundaries

- **gt-core MUST NOT** depend on domain crates (rigs, beads, merge, …).
- **gt-core MAY** depend on gastown kernel crates (gt-events, etc.) until they migrate up.
- **apps MUST** consume gt-core via Cargo path patch or git dep (no monkey-patching internals).
- **MCP / HTTP contracts** are gt-core's responsibility; apps register tools/routes via the builder, don't hand-wire.

## Status

- 2026-05-31: repo bootstrapped. Crate skeletons only. No working code yet.
- First bead: `hq-mod-core.1` — scaffold `gt-module` proper + `GtModule` trait.
- Foundation must land before any feature module (kanban, pages, cycles) is started.
