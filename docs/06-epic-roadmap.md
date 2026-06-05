# 06 — Epic Roadmap (semantic ordering)

Canonical, **semantically reordered** view of the two live epic trees and the
dependency edges between their sub-epics. Source of truth for *status* and
*surface* stays in the upstream `hq.issues` (via gt-mcp); this file is the
human-readable map of **how the sub-epics layer and in what order they unlock**.

It exists because the epic-level `depends_on` stored in `hq.issues` was an
imprecise hand-set approximation: several sub-epics declared coarse "feels
right" edges while their child beads implied a *different* dependency set. This
document records the **derived-correct** ordering — the edges you get when you
collapse the real child-bead `depends_on` up to the sub-epic granularity — plus
two structural anomalies the collapse surfaces (one cross-tier leak, one
epic-level cycle). The anomalies are **observations, not decisions**: this doc
flags them and sketches options, but resolving them is the epic owner's call —
the stored edges keep the real dependencies so neither anomaly is silently
papered over.

> Convention (NN-16, doc 04): `epic → sub-epic (external_ref) → bead`. The two
> top epics are **`hq-mod`** (foundation) and **`hq-mt`** (built on top). Every
> sub-epic below carries `external_ref = hq-mod | hq-mt`; ordering between
> sub-epics is `depends_on`. Closed legacy upstream epics (the `Paso N` /
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
| 5 | `hq-mod-dogs` | core, **events** `+add`, hooks, **hq-mt-runtime** ⚠️ | Dog worker + DogDispatcher + Gate evaluator + PluginExecutor |
| 6 | `hq-mod-docs` | (all of the above) | Author guide (docs/16) + mod-hello sample module — **last** |

Notes on the corrections:

- **`hq-mod-hooks` += `hq-mod-refactor`.** A hooks child (Sheriff pre-merge
  watchdog refactored as a registrable hook) targets a domain that only exists
  once `hq-mod-refactor` has wrapped it as a Module. The epic missed this edge.
- **`hq-mod-dogs` += `hq-mod-events`.** The Gate evaluator subscribes to events,
  so Dogs cannot land before the cross-module subscribe API in `hq-mod-events`.
- **`hq-mod-dogs` → `hq-mt-runtime` ⚠️ tier violation (kept on purpose).** Bead
  `hq-mod-dogs.8` really depends on `hq-mt-runtime.2`, so the epic edge is stored
  as-is — see Anomaly 1. It is left visible (not dropped) so the owner decides
  how to re-tier it rather than the map hiding it.
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
| 2 | `hq-mt-auth` | core, **data** `+add` | JWT workspace claim + WorkspaceGuard + scope matrix (auth.5→routing.5 lives at bead level only† — would cycle) |
| 2 | `hq-mt-routing` | core, data, auth† | RootRegistry per workspace + lazy hydrate + idle teardown |
| 3 | `hq-mt-authprov` | core, **auth** `+add` | Login providers: `gt-auth::IdentityProvider` port, default email+password (argon2, `password-hash` feature) + OAuth/OIDC shape-reserved stubs |
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

## Anomalies (observed — the epic owner resolves)

These are **observations from the data**, not decisions made by this map. The
stored epic edges keep the real dependencies; the options below are sketches for
whoever owns the epic, not a chosen design.

### Anomaly 1 — cross-tier leak: `hq-mod-dogs.8 → hq-mt-runtime.2`

Bead `hq-mod-dogs.8` ("Per-workspace pool") declares a dependency on
`hq-mt-runtime.2` ("Polecat pool size config per workspace + host capacity
allocator"). Collapsed to the epic level this makes **`hq-mod` depend on
`hq-mt`**, inverting the tier law (multi-tenant builds on the module system,
never the reverse).

**Status: stored as-is.** `hq-mod-dogs`'s epic `depends_on` keeps the real
`hq-mt-runtime` edge, so the violation stays visible instead of being silently
dropped. **Not resolved here.** Options the owner might weigh:

- Rescope `hq-mod-dogs.8` to a workspace-agnostic pool (config pool size, no
  `hq-mt-*` dependency) and move per-workspace budget wiring into
  `hq-mt-runtime` (which consumes the Dogs module).
- Or re-tier `hq-mod-dogs.8` into `hq-mt` entirely if it is inherently
  multi-tenant.

Either way the fix is a **bead re-tiering decision**, not something this roadmap
should encode.

### Anomaly 2 — epic-level cycle: `hq-mt-auth ↔ hq-mt-routing`

Two child beads cross the boundary in opposite directions:

- `hq-mt-routing.6` ("gt-web AppState swaps single root for RootRegistry")
  depends on `hq-mt-auth.3` ("WorkspaceGuard middleware: validate claim, fetch
  catalog, insert WorkspaceContext").
- `hq-mt-auth.5` ("gt-mcp tool dispatch resolves RootHandle by workspace before
  command") depends on `hq-mt-routing.5` ("gt-mcp service holds RootRegistry").

Each individual bead edge is **acyclic**; the cycle only appears when you
collapse to the sub-epic — `auth` and `routing` are interleaved, not strictly
sequenced.

**Status: only the acyclic direction is stored.** `hq-mt-routing` keeps its
`depends_on hq-mt-auth` edge; the back-edge (`auth → routing`) is **not** written
to `hq-mt-auth`'s epic deps because it would store an epic-level cycle (the
`issues.update` cycle guard rejects it). This is a **storage constraint, not a
design decision** — the `auth.5 → routing.5` dependency still lives at the bead
level. Whether to formally split `auth` into a foundational half (`.1..4`) and a
dispatch half (`.5`) so the epic ordering is unambiguous is the owner's call, not
decided here.

---

## How to keep this honest

- The derived edges here come from collapsing child-bead `depends_on` up to the
  sub-epic. Re-derive after large bead churn — if a sub-epic's children gain a
  cross-boundary `depends_on`, this map is stale.
- Epic-level `depends_on` in `hq.issues` is now patchable via
  `issues.update` (it gained `depends_on`/`surface`/`domain` overwrite — see
  [[project_nn16_bead_taxonomy]]). The stored edges mirror the real child-bead
  dependencies, **including** the Anomaly-1 tier leak (`hq-mod-dogs →
  hq-mt-runtime`) — kept visible on purpose. The only edge NOT stored is the
  Anomaly-2 back-edge (`auth → routing`), and only because it would form a cycle
  the write path rejects. Don't "resolve" anomalies by editing epic edges to
  hide them; re-tier the offending **bead** instead.
- New cross-tier edge from `hq-mod` to `hq-mt`? Stop — it violates the tier law.
  Re-tier the offending bead instead (Anomaly 1 is the template).

See [docs/03-architecture-guardrails.md](03-architecture-guardrails.md) for the
one-way dependency rule and [docs/04-non-negotiables.md](04-non-negotiables.md)
for NN-16 (the bead taxonomy this map obeys).
