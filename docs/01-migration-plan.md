# Migration plan: upstream → gt-core

Stage-gated migration. Each phase ships independently; nothing destructive happens until the gate before it is green.

## Phase 0 — Bootstrap (DONE 2026-05-31)

- `/home/nixos/gt-core` created, pushed to `github.com/codecsrayo/gt-core` (commit `d8a5d7d`).
- 8 crate skeletons + `examples/mod-hello`.
- `cargo build` green.
- The upstream CLAUDE.md updated to point at gt-core for `hq-mod-*` / `hq-mt-*` work.

**Output:** repo exists, agents know where to work.

---

## Phase 1 — Module foundation (hq-mod-core/.routes/.mcp/.events/.migrate)

Real implementations of the skeleton crates. Built **in-place** in gt-core; no upstream change yet.

Order:
1. `hq-mod-core.1..8` → `gt-module` proper.
2. `hq-mod-events.1..7` → `gt-module-events` proper (event versioning + cross-module subscribe stubs).
3. `hq-mod-routes.1..6` → `gt-module-routes` proper.
4. `hq-mod-mcp.1..5` → `gt-module-mcp` proper.
5. `hq-mod-migrate.1..5` → `gt-module-migrate` proper.
6. `hq-mod-flags.1..6` → `gt-feature-flags` proper.
7. `hq-mod-contracts.1..5` → `gt-module-contracts` proper.
8. `hq-mod-hooks.1..7` → `gt-hooks` + `gt-webhooks` proper (lifecycle hooks + inbound webhook router + GitHub/Linear sources + sheriff hook refactor).
9. `hq-mod-dogs.1..9` → `gt-dog` proper (worker abstraction + DogDispatcher + Gate evaluator + PluginExecutor + digest receipts + notify).

**Gate:** `cargo test --workspace` green. `examples/mod-hello` registers + routes + emits one event + applies one migration end-to-end. Dog claims a sample plugin (cron gate, FakeAgent executor) and emits a digest receipt bead.

---

## Phase 2 — Upstream consumes gt-core (path patch only)

Add the **collision-free module-foundation** crates to `gastown/apps/api/Cargo.toml`
(done in `hq-mod-refactor.15`; paths reflect the real `crates/kernel/` layout):

```toml
[workspace.dependencies]
gt-module           = { path = "../../../gt-core/crates/kernel/gt-module" }
gt-module-mcp       = { path = "../../../gt-core/crates/kernel/gt-module-mcp" }
gt-module-migrate   = { path = "../../../gt-core/crates/kernel/gt-module-migrate" }
gt-module-contracts = { path = "../../../gt-core/crates/kernel/gt-module-contracts" }
gt-feature-flags    = { path = "../../../gt-core/crates/kernel/gt-feature-flags" }
gt-hooks            = { path = "../../../gt-core/crates/kernel/gt-hooks" }
```

`cargo build` in the upstream picks them up. No domain change yet — just available.

**Not in this list (deliberately):**

- `gt-module-routes` / `gt-module-events` — **dropped**: folded into `gt-module`
  (`hq-mod-core.9`, commit `5b6ec66`). They never become separate crates.
- `gt-workspace` — **name collision**: the upstream app already had a `gt-workspace` (town
  root / `FindFromCwd`), a different concept from gt-core's tenant `gt-workspace`.
  The upstream's was renamed to `gt-townroot` (`hq-mod-refactor.15`); gt-core's
  `gt-workspace` (+ `gt-runtime`, which pulls it) can be path-patched in once the
  rename lands — needed by Phase 5 (`hq-mt-*`).
- `gt-store-pg` — the upstream app already has its own (`QuotaRepository` / audit / outbox
  adapter); gt-core's (migration host + `schema_for`/`WorkspacePool`) **merges with
  it in Phase 4**, not a path-patch here.

**Gate:** upstream `cargo build` + `cargo test` still green. Nothing imports gt-core yet, but the deps resolve.

---

## Phase 3 — Wrap existing domains as Modules (hq-mod-refactor)

In **the upstream app**, each domain crate (`gt-rig`, `gt-quota`, `gt-merge`, `gt-convoy`, `gt-polecat`, `gt-agent`, role crates, `gt-feed`, `gt-terminal`) grows a thin `impl GtModule for FooModule { ... }`. No domain logic change.

Composition root (`gt-web`, `gt-mcp`, `gt`) drops hand-wired routes + replaces with `RootBuilder::new(ws).module(BeadsModule).module(RigsModule)...build()`.

Beads: `hq-mod-refactor.1..12`.

**Gate:** every existing route still answers identically. `replay_gt` byte-for-byte equal pre/post refactor.

---

## Phase 4 — Move foundational kernel into gt-core

Now the kernel crates that gt-core was depending on through the upstream app migrate up:

- `gt-events` → `gt-core/crates/gt-events`
- `gt-bus` → `gt-core/crates/gt-bus`
- `gt-audit` → `gt-core/crates/gt-audit`
- `gt-plugin` → `gt-core/crates/gt-plugin`
- `gt-telemetry` → `gt-core/crates/gt-telemetry`

For each: `git mv` source, update Cargo.toml, update upstream `[workspace.dependencies]` to point at gt-core path. Single PR per crate. Replay gate after each.

**Gate:** upstream still builds + replay byte-for-byte equal + integration tests green.

---

## Phase 5 — Multi-tenant (hq-mt-*)

By now gt-core hosts the module + workspace primitives and the kernel. hq-mt-* beads layer the tenancy boundary:

1. `hq-mt-core.1..8` → `gt-workspace` proper (in gt-core).
2. `hq-mt-data.1..12` → PG/Dolt partitioning (migrations in gt-core + the upstream app).
3. `hq-mt-auth.1..7` → JWT claim + WorkspaceGuard (split: trait in gt-core, upstream integration).
4. `hq-mt-routing.1..8` → RootRegistry in gt-core.
5. `hq-mt-rigs.1..6` → upstream gt-rig refactor (depends on Phase 3 module wrap).
6. `hq-mt-runtime.1..9` → polecat/sessions/quota in the upstream app, scoped.
7. `hq-mt-cli.1..6` → upstream `gt` CLI.
8. `hq-mt-bootstrap.1..7` → workspace lifecycle (the upstream app).
9. `hq-mt-migrate.1..5` → one-shot data migration (the upstream app).
10. `hq-mt-deploy.1..8` → compose + traefik wildcard.
11. `hq-mt-ui.1..6` + `hq-mt-views.1..8` → frontend (upstream apps/web).
12. `hq-mt-ops.1..5` → dashboards + leak detector.

**Gate:** two workspaces (`default` + `acme`) live on the same compose stack; cross-ws leak test green.

---

## Phase 6 — Features as modules

Now the substrate carries its weight. Features ship as standalone crates in `gt-core/crates/mod-<name>/`:

- `mod-kanban` (Plane-style board over beads)
- `mod-pages` (rich text docs)
- `mod-cycles` (sprints)
- `mod-intake` (triage)

Each = one crate, one composition-root line, one PR. No refactor anywhere else.

---

## What stays upstream forever

- `apps/web` (Svelte SPA) — frontend is the app, not the kernel. Apps still embed the kernel's UI conventions but ship in their own repo.
- `deploy/` (compose, traefik config, observability dashboards) — operator artifact, app-specific.
- Domain crates that ARE specific to gas-town's product (sheriff/deacon/refinery/witness/mayor patrol logic).

## What stays upstream until further notice

- `gt-store-pg-*` adapters (PG schema is the kernel's contract; adapters are app-specific until we factor out generic shapes).
- The Dolt `hq` DB (tracking lives there).

## Triggers for revisiting

- If a second app picks up gt-core, that's the signal to extract the polecat/sling primitives upward.
- If gas-town gets a co-tenant on a different schema, that's the signal to extract `gt-store-pg-shared` into gt-core.
