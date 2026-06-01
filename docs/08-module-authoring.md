# Module authoring guide

How to write a `GtModule` — the single on-ramp for a pluggable feature
([docs/03 Rule 3](03-architecture-guardrails.md#rule-3-module-system-is-the-only-on-ramp)).
One crate, one marker struct, registered with the builder in one line; the kernel
harvests everything it contributes. The composition root hand-wires nothing.

Read this alongside the worked example: [`examples/mod-hello`](../examples/mod-hello)
touches every contribution point in one file. This guide explains the shape; that
crate is the copyable reference.

## The shape

A module is a (usually zero-sized) struct implementing
[`GtModule`](../crates/kernel/gt-module/src/trait.rs):

```rust
use gt_module::{GtModule, ModuleId, ModuleMeta};
use semver::Version;

struct BeadsModule;

impl GtModule for BeadsModule {
    fn meta(&self) -> ModuleMeta {
        ModuleMeta::new(
            ModuleId::new("beads").unwrap(),
            "Beads",
            Version::new(1, 0, 0),
            "Issue tracking aggregate.",
        )
    }
}
```

Only `meta` is required. Every other trait method has a default (no-op / empty /
`None`), so a freshly scaffolded module compiles and you add contributions one at a
time without ever breaking the build. That additive-default rule is deliberate
(it is why not-yet-ported modules compile too) — keep it: new trait methods land
with defaults.

### Why the trait is sync

`GtModule` is a plain trait, dispatched statically by the builder's generic
`.module::<M>()` chain (non-negotiables #1, #2: `dyn` + `#[async_trait]` live only
in `gt-plugin`). A module that needs I/O at startup does **not** make trait methods
`async` — it registers an actor whose runtime handle the binary supplies
(non-negotiable #14). The trait takes `&self`, so an implementor that needs runtime
handles can hold them as fields and take them in its own constructor; it need not
be zero-sized.

## Contribution points

Each is one trait method. Implement only what your module offers.

### `meta` — identity (required)

`ModuleMeta::new(id, name, version, description)`. The `ModuleId` is a validated
slug (lowercase dotted-kebab); construct it with `ModuleId::new(...)`. The version
is semver. `meta` is pure data — the builder, diagnostics, and `meta.help` may call
it any number of times.

### `capability` — what the module owns

Returns a [`Capability`](../crates/kernel/gt-module/src/capability.rs), built
fluently from `Capability::empty()`:

```rust
fn capability(&self) -> Capability {
    Capability::empty()
        .claiming_all([
            Scope::new("beads.read").unwrap(),
            Scope::new("beads.write").unwrap(),
        ])
        .emitting(EventKind::new("beads.created.v1").unwrap())
}
```

- **Scopes** (`claiming` / `claiming_all`) — the RBAC authorization scopes this
  module owns. Claiming *any* scope opts every one of the module's routes into the
  per-method guard (GET → `<id>.read`, mutating → `<id>.write`). The builder
  rejects two modules claiming the same scope (`hq-mod-core.6`).
- **Events** (`emitting` / `emitting_all`) — the versioned event kinds the module
  emits. Declaration is validated at build time and is the contract other modules
  subscribe against. Only one module may own a given kind+version.
- **Hooks** (`subscribing` / `subscribing_all`) — the lifecycle `HookPoint`s the
  module reacts to.
- **Deprecation** (`deprecating` / `emitting_deprecated`) — mark a kind+version
  deprecated during a rollover (see [docs/07](07-version-rollover.md)).

### `migrations` — owned schema

```rust
fn migrations(&self) -> Vec<Migration> {
    vec![Migration::new(1, "create_beads",
        include_str!("../migrations/beads/0001_create_beads.sql"))]
}
```

A module owns its SQL. Supply each with `include_str!` from
`migrations/<module-id>/NNNN_*.sql` so the binary is self-contained. Versions must
be unique within the module; the builder sorts by version and applies modules in
dependency-first order. Disabling a module retains its applied migrations
(non-destructive) — see `gt-module-migrate`.

### `register_routes` — HTTP surface

```rust
fn register_routes(&self) -> Router {
    Router::new().route("/beads", get(list_beads))
}
```

Declare **plain relative paths**. The builder namespaces them under
`/api/v1/<module-id>` (so `/beads` answers at `/api/v1/beads/beads`) and wraps them
with the scope guard if the module claimed scopes — you never type the prefix or
the guard. The returned `Router` is **state-erased**: bake your repository ports in
with `Router::with_state(...)` *before* returning, so the kernel never learns a
shared application-state type and two modules cannot collide on one.

### `register_mcp_tools` — MCP tools

```rust
fn register_mcp_tools(&self, registry: &mut McpRegistry) {
    registry.tool("beads.create.show", "Show a bead.");
}
```

Tool names are `<module-id>.<action>.<verb>`. The builder checks the first segment
matches your module id, so you cannot squat another module's namespace. Disabled
modules' tools never appear in `meta.help`.

### `openapi` — API docs (optional)

Return `Some(MyApi::openapi())` from a `#[derive(utoipa::OpenApi)]` type with
*relative* paths; the builder rewrites them to the `/api/v1/<module-id>/` prefix and
merges every module's spec into one document. Defaults to `None`.

### `dependencies` — ordering

Return the `ModuleId`s that must initialize before this one. The builder orders
wiring dependencies-first and rejects cycles and dangling references
(`hq-mod-core.5`). Defaults to none — the common case.

## Wiring it in

The composition root registers modules and builds the root — and does nothing else:

```rust
let root = RootBuilder::new()
    .module(BeadsModule)
    .module(RigModule)
    .build()?;
```

`build()` returns `Result<Root, BuildError>`; the error enumerates exactly what a
module got wrong (duplicate scope, duplicate migration version, dependency cycle,
missing dependency). From `Root` you get the merged axum router, the migration
plan, the MCP registry, and the OpenAPI document. Never call `Router::route`,
register an MCP tool, or list a migration by hand in the composition root
([docs/03 Rule 3](03-architecture-guardrails.md#rule-3-module-system-is-the-only-on-ramp)).

> The per-workspace builder (`RootBuilder::new(workspace)…`) is a multi-tenant
> extension that lands with `hq-mt-routing`; today `new()` takes no argument.

## Conventions

- **Module id** — lowercase dotted-kebab slug; it prefixes routes, MCP tools, and
  scopes, so pick it once and keep it stable.
- **Event kinds** — always `<module-id>.<noun>.v<N>`; a breaking payload change is a
  new `vN`, never a mutation of the existing one ([docs/07](07-version-rollover.md),
  [docs/03 Rule 5](03-architecture-guardrails.md#rule-5-events-are-versioned-replay-safe-additive)).
- **Scopes** — `<module-id>.<action>` (`read` / `write` at minimum).
- **One crate per module**, ports + `InMemory` adapter in the crate; heavy adapters
  (PG, axum) behind off-by-default features in the *same* crate, never a separate
  adapter crate ([docs/03 Rule 4](03-architecture-guardrails.md#rule-4-dependency-direction)).

## Checklist

- [ ] One crate, one `GtModule` marker struct.
- [ ] `meta` returns a validated `ModuleId`, name, semver, description.
- [ ] Scopes claimed for every route that needs RBAC; named `<id>.<action>`.
- [ ] Event kinds declared in `capability`, formatted `<id>.<noun>.vN`.
- [ ] Migrations `include_str!`'d from `migrations/<id>/`, versions unique.
- [ ] Routes are relative + state-erased (`with_state` before return).
- [ ] MCP tools namespaced `<id>.<action>.<verb>`.
- [ ] Registered in the composition root with `.module(...)`; nothing hand-wired.
- [ ] `cargo build` + the replay gate green.

## See also

- [`examples/mod-hello`](../examples/mod-hello) — the smallest end-to-end module,
  proved by `tests/e2e.rs`.
- [`gt-module-contracts`](../crates/kernel/gt-module-contracts) — authoring a
  frozen DTO contract (rustdoc "Authoring a contract" walkthrough).
- [docs/07](07-version-rollover.md) — rolling a surface from v1 to v2.
