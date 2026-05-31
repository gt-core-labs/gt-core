# Architecture guardrails

**Read this before writing a single line of code in gt-core.** These rules are not preferences — they are invariants. Breaking them invalidates the migration plan, the replay gate, or the multi-tenant boundary.

## TL;DR

1. **Don't improvise the kernel.** Foundation crates (`gt-events`, `gt-bus`, `gt-audit`, `gt-plugin`, `gt-telemetry`) are already designed and tested in gastown. They migrate UP into `crates/kernel/` per Phase 4 — never re-invented here. Open gastown's source before writing any equivalent.
2. **Don't move folders.** The layered taxonomy (`kernel/` / `domain/{platform,orchestration,lifecycle,roles}/` / `modules/` / `bins/`) is fixed. New crates go INTO the existing tier; new tiers are not added without explicit user approval.
3. **Don't bypass the module system.** Every feature is a `GtModule`. No hand-wired routes, MCP tools, migrations, or actors in app composition roots.
4. **Don't cross tier boundaries downward.** Kernel never depends on domain. Domain never depends on modules.

The rest of this document is the long form.

---

## Rule 1: kernel migration, not re-invention

### What stays in gastown (until P4)

| Crate | Purpose | gastown path |
|-------|---------|--------------|
| `gt-events` | Event/Command/EventKind primitives, AppError | `apps/api/crates/kernel/gt-events` |
| `gt-bus` | Synchronous in-process bus + DeadLetter | `apps/api/crates/kernel/gt-bus` |
| `gt-audit` | EventRecord + event log writer + replay reader | `apps/api/crates/kernel/gt-audit` |
| `gt-plugin` | Observer plugin trait + relay + dead-letter | `apps/api/crates/kernel/gt-plugin` |
| `gt-telemetry` | OTel adapter + span helpers | `apps/api/crates/kernel/gt-telemetry` |

Until those crates migrate, gt-core crates that need them depend on the **published path** in gastown via `[workspace.dependencies]` in gastown's Cargo.toml during P2 wiring. Never copy-paste their code into gt-core.

### What to do when you need an existing kernel primitive

1. **Open** `apps/api/crates/kernel/<crate>/src/` in gastown.
2. **Use** the type/trait by importing from the crate (via path patch in P2; via gt-core re-export in P4).
3. **Do not** redefine `Event`, `Command`, `AppError`, `EventRecord`, etc. in gt-core. They exist.

### When you discover a kernel gap

If gastown's kernel lacks something gt-core needs (e.g., a `RootBuilder` hook):
1. **Don't add it to gt-core's kernel** as a parallel type.
2. **File a bead** under `hq-mod-core` or open a doc-gap ticket.
3. **Discuss** before extending. Kernel growth is deliberate.

---

## Rule 2: folder structure is fixed

### Allowed

```
crates/
├── kernel/<crate>/                       ← any new framework crate
├── domain/
│   ├── platform/<crate>/                 ← cross-cutting domain (workspace, etc.)
│   ├── orchestration/<crate>/            ← runtime coordination (webhooks, dog, scheduling)
│   ├── lifecycle/<crate>/                ← state-machine entities (agent, polecat)
│   └── roles/<crate>/                    ← behavioral actors (sheriff, deacon, ...)
├── modules/<mod-name>/                   ← user-facing features (kanban, pages, ...)
└── bins/<bin-name>/                      ← own binaries (rare; gastown owns these)

examples/<crate>/                         ← reference impls (mod-hello only today)
docs/<NN>-<topic>.md                      ← numbered design docs
```

### Forbidden

- ❌ New top-level dirs (`api/`, `services/`, `lib/`, `pkg/`, etc.). The tree is the tree.
- ❌ Crates outside `crates/` or `examples/`.
- ❌ "Helper" crates dumped at `crates/<name>` without a tier. Pick kernel or domain.
- ❌ Frontend code anywhere. FE has its own repo.
- ❌ Compose files / Dockerfiles / deploy scripts. Those live in app repos (gastown).

### Picking a tier — flowchart

```
Does the crate touch any Gas Town concept (workspace, rig, bead, polecat)?
├─ NO  → kernel/
└─ YES → domain/
    ├─ Owns a stateful entity with transitions? → lifecycle/
    ├─ Is a behavioral watcher / patrol?        → roles/
    ├─ Coordinates runtime work?                → orchestration/
    └─ Otherwise (cross-cutting primitive)      → platform/

Is it a user-facing feature (Kanban-like, board, page, view)? → modules/

Is it an executable? → bins/  (and you probably shouldn't add one)
```

### Naming

- Kernel: `gt-mod-<X>` or `gt-<X>` where `<X>` is single-purpose (events, hooks, routes).
- Domain: `gt-<noun>` (gt-workspace, gt-dog, gt-polecat).
- Modules: `mod-<noun>` (mod-kanban, mod-pages).
- Bins: `gt-<tool>` if ever (gt-contracts).
- All kebab-case, no underscores.

---

## Rule 3: module system is the only on-ramp

### Composition root contract

Apps compose their stack ONLY via `RootBuilder`:

```rust
let root = RootBuilder::new(workspace_id)
    .module(BeadsModule)
    .module(RigsModule)
    .module(KanbanModule)
    .with_flags(load_flags())
    .build()
    .await?;
```

### Forbidden patterns in app composition roots

- ❌ Direct `axum::Router::new().route("/api/...", ...)` for module routes. Use `GtModule::register_routes`.
- ❌ Direct `#[tool]` macro on a struct that isn't a module's `register_mcp_tools` target.
- ❌ Direct `sqlx::migrate!()` call. Use `gt-mod-migrate` multi-source loader.
- ❌ Spawning an actor outside `Module::register`. Lifecycle is owned by the builder.
- ❌ Observer plugin registered ad-hoc. Plugins are declared in `Capability::hooks` or `Capability::subscribes`.

### When you think "this can't be a module"

90% of the time it can. Examples that DO fit the Module mold:

- A background watchdog → register an actor in `Module::register`.
- A CLI subcommand → register an MCP tool; CLI calls MCP.
- A webhook endpoint → register a route under `gt-webhooks` infrastructure; provide a `WebhookSource` impl.
- A scheduled job → register a `Dog` with a Cron `Gate`.

The remaining 10% is genuinely kernel-level; file a doc gap.

---

## Rule 4: dependency direction

```
modules    → domain    → kernel    → (gastown kernel until P4)
```

### Allowed Cargo deps

| Crate location | May depend on |
|---------------|---------------|
| `crates/kernel/*` | std, external crates, other `crates/kernel/*` |
| `crates/domain/platform/*` | kernel, std, external |
| `crates/domain/orchestration/*` | kernel, `domain/platform/*`, std, external |
| `crates/domain/lifecycle/*` | kernel, `domain/platform/*`, std, external |
| `crates/domain/roles/*` | kernel, all `domain/*`, std, external |
| `crates/modules/*` | kernel, all `domain/*`, other modules' EVENT contracts (not direct calls), std, external |
| `examples/*` | anything (demo only) |

### Forbidden

- ❌ Kernel crate depending on a domain crate. The trait is in kernel; impls live in domain/modules.
- ❌ Module A calling Module B directly. Cross-module = event subscription + CommandBus dispatch only.
- ❌ Crate importing from a sibling via `../../<other>/src/lib.rs` path hack. Use `[workspace.dependencies]`.

### Centralized paths

All internal deps are declared in root `Cargo.toml`:

```toml
[workspace.dependencies]
gt-module = { path = "crates/kernel/gt-module" }
# ... etc
```

Members use:

```toml
[dependencies]
gt-module = { workspace = true }
```

**Never** a relative path inside a member Cargo.toml. If you need to add one, the workspace declaration is missing.

---

## Rule 5: events are versioned, replay-safe, additive

### Event kind format

```
<module-id>.<noun>.v<N>
```

Examples: `bead.created.v1`, `rig.prefix_changed.v1`, `kanban.card_moved.v2`.

### When v2 is needed

When you must add/remove/rename a field of a v1 event:

1. **Keep v1** emitted by old code path for a deprecation window.
2. **Emit v2** in new code with the new schema.
3. **Add to `Capability::emits`** both `<X>.v1` and `<X>.v2`.
4. **Frontend / downstream consumers** subscribe to whichever version they handle.
5. **Never** rewrite the event log to fold v1 → v2. Log is append-only forever.

### Forbidden

- ❌ Emitting an unversioned kind (`bead.created`).
- ❌ Changing the wire shape of an existing `.v1` event.
- ❌ Reusing an event kind name across modules.
- ❌ Mutating domain state from a cross-module event subscription (read-only projections only; mutations go through `RootCommand` + CommandBus).

---

## Rule 6: workspace boundary is sacred

### Every domain mutation carries workspace_id

- `RigCommand::Add` has `workspace_id` server-injected from `WorkspaceContext`.
- `BeadCommand::Create` has `workspace_id` server-injected.
- All domain repos take `&WorkspaceId` as first arg.
- All projection tables have a `workspace_id` column with FK to `workspaces.id`.

### Server injects, client never supplies

- ❌ MCP tool payload with `workspace_id` field. The auth ctx supplies it; payload spoofing rejected.
- ❌ Route param `/api/v1/<module>/:workspace_id/...`. URL is `/api/v1/<module>/...`; workspace comes from JWT.
- ❌ Reading `workspace_id` from request body.

### Cross-workspace queries

The ONLY consumer of cross-workspace state is the workspace lifecycle itself (`gt-workspace` repo). Every other crate is scope-bounded to one workspace.

---

## Rule 7: keep the replay byte-for-byte gate green

Touching `gt-events`, any reducer, or any event log writer means you must:

1. Snapshot state pre-change.
2. Replay full event log under the new code.
3. Diff state. **Must equal zero diff.**

This is the Step 3 gate mentioned in gastown's `docs/11-cutover-roadmap.md` and `docs/06-observability.md`. It is the only mechanism keeping the orchestrator deterministic across upgrades.

If your change WILL fail the gate (e.g., adding workspace_id to all events), do it in a single commit with the backfill migration alongside, and gate the commit behind a flag until the rest of the system is ready.

---

## Rule 8: when in doubt, file a doc gap

If a bead description tells you to do something that violates these rules:

1. **Don't comply silently.** Stop.
2. **File a doc gap** via `meta.report_gap` MCP tool with `hq-gap-arch-<slug>`.
3. **Wait** for an explicit decision before resuming.

These rules exist because past improvisations broke replay, leaked tenants, or fragmented the kernel. The cost of pausing is low. The cost of a wrong assumption is a multi-day rollback.

---

## Reference

- Layered model: [README.md](../README.md)
- Migration phases: [01-migration-plan.md](01-migration-plan.md)
- SSE pattern: [02-sse-pattern.md](02-sse-pattern.md)
- gastown kernel source: `/home/nixos/gastown/apps/api/crates/kernel/`
- Replay gate origin: `/home/nixos/gastown/apps/api/docs/11-cutover-roadmap.md` Paso 9.B / `/home/nixos/gastown/apps/api/docs/06-observability.md`
