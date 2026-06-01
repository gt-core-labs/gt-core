# 06 — Epic Roadmap (semantic ordering)

Canonical, **semantically reordered** view of the two live epic trees and the
dependency edges between their sub-epics. Source of truth for *status* and
*surface* stays in gastown `hq.issues` (via gt-mcp); this file is the
human-readable map of **how the sub-epics layer and in what order they unlock**.

It exists because the epic-level `depends_on` stored in `hq.issues` was an
imprecise hand-set approximation: several sub-epics declared coarse "feels
right" edges while their child beads implied a *different* dependency set. This
document records the **derived-correct** ordering — the edges you get when you
collapse the real child-bead `depends_on` up to the sub-epic granularity — plus
the two structural anomalies that collapse surfaces (one cross-tier leak, one
epic-level cycle) and how each is resolved.

> Convention (NN-16, doc 04): `epic → sub-epic (external_ref) → bead`. The two
> top epics are **`hq-mod`** (foundation) and **`hq-mt`** (built on top). Every
> sub-epic below carries `external_ref = hq-mod | hq-mt`; ordering between
> sub-epics is `depends_on`. Closed legacy gastown epics (the `Paso N` /
> `v1 operational` / `mcp-*` families) are all `closed` and intentionally
> omitted — they are superseded by the gt-core migration, not part of this map.

## Tier law

```
hq-mod   Module Architecture — pluggable feature system
  │       (kernel + module trait + the on-ramp for every domain)
  ▼  depends_on
hq-mt    Multi-tenant — workspace as full isolation boundary
          (data + compute + RBAC + deploy, layered ON the module system)
```

**One-way only.** `hq-mt` depends on `hq-mod`; `hq-mod` must **never** depend on
`hq-mt`. Multi-tenant is a consumer of the module system, not the reverse
(mirrors the one-way dep rule in doc 03). The single edge that violates this is
called out in §Anomalies below.

---

## Tree 1 — `hq-mod` (foundation), build order

`hq-mod-core` is the root (closed — `GtModule` trait + `RootBuilder` +
`Capability` + version registry). Everything else hangs off it. Order is the
topological sort of the **derived-correct** edges; `+add` marks an edge the
stored epic data was missing.

| # | sub-epic | depends_on (corrected) | title |
|---|----------|------------------------|-------|
| 0 | `hq-mod-core` ✅ | — | GtModule trait + RootBuilder + Capability + version registry |
| 1 | `hq-mod-events` | core | Event versioning (.v1/.v2 coexist) + cross-module subscribe |
| 1 | `hq-mod-routes` | core | axum router composition per module + OpenAPI namespacing |
| 1 | `hq-mod-mcp` | core | MCP tool namespacing per module (tool prefix = module-id) |
| 1 | `hq-mod-migrate` | core | Per-module SQL migration namespacing (sqlx multi-source) |
| 2 | `hq-mod-contracts` | core, events | TS DTO generation per module + frozen versions + CI drift |
| 2 | `hq-mod-flags` | core, migrate | FeatureFlags repo + per-ws overrides + `gt feature` |
| 3 | `hq-mod-frontend` | core, flags | Vite module federation + sidebar/route auto-registry |
| 3 | `hq-mod-refactor` | core, events, mcp, migrate, routes | Wrap 8 existing domains as Modules |
| 4 | `hq-mod-hooks` | core, **refactor** `+add` | Lifecycle hooks framework + webhook router |
| 5 | `hq-mod-dogs` | core, **events** `+add`, hooks | Dog worker + DogDispatcher + Gate evaluator + PluginExecutor |
| 6 | `hq-mod-docs` | (all of the above) | Author guide (docs/16) + mod-hello sample module — **last** |

Notes on the corrections:

- **`hq-mod-hooks` += `hq-mod-refactor`.** A hooks child (Sheriff pre-merge
  watchdog refactored as a registrable hook) targets a domain that only exists
  once `hq-mod-refactor` has wrapped it as a Module. The epic missed this edge.
- **`hq-mod-dogs` += `hq-mod-events`.** The Gate evaluator subscribes to events,
  so Dogs cannot land before the cross-module subscribe API in `hq-mod-events`.
- **`hq-mod-docs`** legitimately depends on every sibling — it is the "document
  it last" capstone (author guide + sample module). This wide fan-in is kept
  on purpose, not pruned.

---

## Tree 2 — `hq-mt` (multi-tenant), build order

`hq-mt-core` is the root (`gt-workspace` crate: `WorkspaceId`, catalog,
commands, events, repo port). The corrected edges below add several edges the
stored data was missing (every `+add` is a child bead that already depends
across the sub-epic boundary).

| # | sub-epic | depends_on (corrected) | title |
|---|----------|------------------------|-------|
| 0 | `hq-mt-core` | — | gt-workspace crate: WorkspaceId, catalog, commands, events, repo port |
| 1 | `hq-mt-data` | core | PG schema-per-ws + Dolt DB-per-ws + event log split |
| 2 | `hq-mt-auth` | core, **data** `+add`, routing† | JWT workspace claim + WorkspaceGuard + scope matrix |
| 2 | `hq-mt-routing` | core, data, auth† | RootRegistry per workspace + lazy hydrate + idle teardown |
| 3 | `hq-mt-rigs` | core, data, **auth** `+add` | Rigs scoped per workspace (absorbs worktree_root) |
| 3 | `hq-mt-bootstrap` | core, data, routing, **`hq-mod-migrate`** `+add` | workspace.create/suspend/archive/restore |
| 4 | `hq-mt-runtime` | core, data, auth, rigs, routing `+add ×3` | Polecat/tmux/sling/sessions/quota per workspace |
| 4 | `hq-mt-cli` | core, rigs, bootstrap, routing `+add ×3` | gt-cli workspace context (GT_WORKSPACE, gt prime, config) |
| 4 | `hq-mt-migrate` | data, bootstrap | One-shot single-tenant → workspace=default |
| 5 | `hq-mt-ui` | core, auth, routing, bootstrap `+add ×2` | apps/web workspace switcher + /api/workspaces + URL routing |
| 5 | `hq-mt-deploy` | core, routing, runtime, bootstrap, migrate `+add ×3` | Compose / traefik wildcard / per-ws backup / observability |
| 6 | `hq-mt-views` | auth, rigs, routing, runtime, ui | Refactor 8 sidebar views per workspace |
| 7 | `hq-mt-ops` | auth `+add`, deploy, runtime, views | Per-ws dashboards + audit query + cross-ws leak detector |

† `hq-mt-auth` and `hq-mt-routing` form an **epic-level cycle** — see Anomaly 2.

---

## Anomalies and their resolution

### Anomaly 1 — cross-tier leak: `hq-mod-dogs.8 → hq-mt-runtime.2`

Bead `hq-mod-dogs.8` ("Per-workspace pool") declares a dependency on
`hq-mt-runtime.2` ("Polecat pool size config per workspace + host capacity
allocator"). Collapsed to the epic level this makes **`hq-mod` depend on
`hq-mt`**, inverting the tier law (multi-tenant builds on the module system,
never the reverse).

**Resolution — the edge is dropped at the module tier.** The Dogs *module*
(`hq-mod-dogs`) must ship a **workspace-agnostic** worker pool: a fixed/config
pool size with no notion of per-workspace budget. The *per-workspace* pool
sizing and host-capacity allocation is a multi-tenant concern that belongs to
`hq-mt-runtime`, which **consumes** the Dogs module and layers the per-ws budget
on top. Concretely:

- `hq-mod-dogs.8` is rescoped to "pool size from module config" (no `hq-mt-*`
  dependency) — or folded into `hq-mt-runtime` as the workspace-aware variant.
- The per-workspace budget wiring lives in `hq-mt-runtime` (which already
  depends on the whole `hq-mod` tree transitively via `hq-mt → hq-mod`).

Net: `hq-mod-dogs` depends only on `core, events, hooks`. No `hq-mod → hq-mt`
edge survives.

### Anomaly 2 — epic-level cycle: `hq-mt-auth ↔ hq-mt-routing`

Two child beads cross the boundary in opposite directions:

- `hq-mt-routing.6` ("gt-web AppState swaps single root for RootRegistry")
  depends on `hq-mt-auth.3` ("WorkspaceGuard middleware: validate claim, fetch
  catalog, insert WorkspaceContext").
- `hq-mt-auth.5` ("gt-mcp tool dispatch resolves RootHandle by workspace before
  command") depends on `hq-mt-routing.5` ("gt-mcp service holds RootRegistry").

Each individual bead edge is **acyclic** and correct; the cycle only appears
when you collapse to the sub-epic. So this is not a real deadlock — it is a sign
that `auth` and `routing` are **co-developed**, not strictly sequenced.

**Resolution — split `auth` into a foundational half and a dispatch half.**

1. **Foundational auth** (`hq-mt-auth.1..4`: JWT workspace claim, scope matrix,
   `WorkspaceGuard`) comes **before** routing — routing's registry swap needs the
   guard to insert `WorkspaceContext`.
2. **Dispatch auth** (`hq-mt-auth.5`: gt-mcp resolves `RootHandle` per workspace)
   comes **after** `hq-mt-routing.5` — it consumes the `RootRegistry`.

So the true order is `core → data → auth(guard+claims) → routing(registry) →
auth.5(dispatch)`. Treat `auth` and `routing` as one **co-built cluster** in
scheduling: open them together, land the guard, then the registry, then the
dispatch bead. The stored epic edges (`routing` deps `auth`) stay; the back-edge
(`auth` deps `routing`) is documented as the intra-cluster forward reference of
`auth.5` only — not an epic-wide dependency.

---

## How to keep this honest

- The derived edges here come from collapsing child-bead `depends_on` up to the
  sub-epic. Re-derive after large bead churn — if a sub-epic's children gain a
  cross-boundary `depends_on`, this map is stale.
- Epic-level `depends_on` in `hq.issues` is now patchable via
  `issues.update` (it gained `depends_on`/`surface`/`domain` overwrite — see
  [[project_nn16_bead_taxonomy]]). This doc is the *intended* shape; reconcile
  the stored edges to it deliberately, not blindly (the two anomalies above must
  **not** be written back as literal epic edges).
- New cross-tier edge from `hq-mod` to `hq-mt`? Stop — it violates the tier law.
  Re-tier the offending bead instead (Anomaly 1 is the template).

See [docs/03-architecture-guardrails.md](03-architecture-guardrails.md) for the
one-way dependency rule and [docs/04-non-negotiables.md](04-non-negotiables.md)
for NN-16 (the bead taxonomy this map obeys).
