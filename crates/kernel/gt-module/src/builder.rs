//! `RootBuilder` — the single composition seam that assembles a list of
//! [`GtModule`]s into a built [`Root`].
//!
//! Landed by [`hq-mod-core.4`]: the skeleton — register modules with a generic
//! `.module::<M>()` chain, then [`build`](RootBuilder::build). The app
//! composition root hand-wires nothing (see `docs/03-architecture-guardrails.md`
//! rule 3); it writes `RootBuilder::new().module(BeadsModule).module(RigModule).build()?`.
//!
//! ## What this bead does and does not do
//!
//! `.module::<M>()` consumes the module value, calls its pure-data accessors
//! ([`meta`](GtModule::meta), [`capability`](GtModule::capability)) eagerly, and
//! stores **only the extracted data** — never the module itself. There is no
//! `Box<dyn GtModule>` in the registry: non-negotiable #1 confines `dyn` to
//! `gt-plugin`, so the builder relies on static dispatch through the generic
//! call site. Behavioral contributions (routes, MCP tools, actors, migrations)
//! attach in later epics by growing [`Capability`] and the per-contribution
//! registration hooks; each is additive.
//!
//! [`build`](RootBuilder::build) returns a [`Result`] so the validation beads
//! slot in without churning the signature:
//!
//! - dependency-cycle detection + topological ordering — `hq-mod-core.5` (done)
//! - capability-conflict detection — `hq-mod-core.6` (done)
//! - feature-flag filtering of disabled modules — `hq-mod-core.7`
//!
//! Each grows a [`BuildError`] variant. As of `.5`, `build()` orders the
//! registered modules so each appears after every module it depends on (see
//! [`crate::deps`]); as of `.6` it also rejects two modules claiming the same
//! authorization scope.

use std::collections::{BTreeMap, BTreeSet};

use axum::Router;

use crate::capability::Capability;
use crate::event_kind::EventKind;
use crate::flags::{AllEnabled, FeatureFlags};
use crate::hook::HookPoint;
use crate::mcp::{McpRegistry, McpTool, McpToolNameError};
use crate::meta::{ModuleId, ModuleMeta};
use crate::migrate::Migration;
use crate::module_trait::GtModule;
use crate::scope::Scope;

/// Accumulates modules and assembles them into a [`Root`].
///
/// Constructed with [`RootBuilder::new`], extended one module at a time through
/// the generic [`module`](RootBuilder::module) method, and finalized with
/// [`build`](RootBuilder::build). Holds the extracted per-module data plus each
/// module's contributed [`Router`] (`hq-mod-routes.1`); it carries no runtime
/// handles of its own — any a module needs are baked into its router.
#[derive(Default)]
pub struct RootBuilder {
    /// One entry per registered module, in registration order.
    entries: Vec<ModuleEntry>,
    /// Each registered module's contributed router, parallel to `entries` by
    /// index. A module that contributes no HTTP surface stores an empty router.
    routers: Vec<Router>,
}

// `axum::Router` is not `Debug`; report the builder by the module ids it holds.
impl std::fmt::Debug for RootBuilder {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RootBuilder")
            .field("modules", &self.entries.iter().map(|e| &e.meta.id).collect::<Vec<_>>())
            .finish()
    }
}

/// Pure-data snapshot the builder keeps for one registered module.
///
/// The originating `M: GtModule` value is dropped after extraction; everything
/// the builder and the validation beads need is captured here. `pub(crate)` so
/// the sibling validation passes ([`crate::deps`]) can read it.
#[derive(Clone, Debug, PartialEq)]
pub(crate) struct ModuleEntry {
    pub(crate) meta: ModuleMeta,
    pub(crate) capability: Capability,
    pub(crate) depends_on: Vec<ModuleId>,
    /// MCP tools this module contributes, in declaration order (`hq-mod-mcp.1`).
    pub(crate) mcp_tools: Vec<McpTool>,
    /// SQL migrations this module owns, as returned (`hq-mod-migrate.1`); the
    /// builder validates and the [`Root`] exposes them version-sorted.
    pub(crate) migrations: Vec<Migration>,
    /// The module's OpenAPI spec with *relative* paths, if it documents its
    /// routes (`hq-mod-routes.4`); the [`Root`] prefixes and merges these.
    pub(crate) openapi: Option<utoipa::openapi::OpenApi>,
}

impl RootBuilder {
    /// Start an empty builder.
    pub fn new() -> Self {
        RootBuilder::default()
    }

    /// Register one module.
    ///
    /// Takes the module by value (implementors are zero-sized marker structs),
    /// reads its pure-data [`meta`](GtModule::meta) and
    /// [`capability`](GtModule::capability), harvests the MCP tools it pushes
    /// into a fresh [`McpRegistry`](crate::McpRegistry) via
    /// [`register_mcp_tools`](GtModule::register_mcp_tools) (`hq-mod-mcp.1`),
    /// stores the snapshot, and drops the value. Returns `self` so registrations
    /// chain. Dispatch is static — no trait object is retained (non-negotiable
    /// #1).
    pub fn module<M: GtModule>(mut self, module: M) -> Self {
        let mut mcp = McpRegistry::new();
        module.register_mcp_tools(&mut mcp);
        self.entries.push(ModuleEntry {
            meta: module.meta(),
            capability: module.capability(),
            depends_on: module.dependencies(),
            mcp_tools: mcp.into_tools(),
            migrations: module.migrations(),
            openapi: module.openapi(),
        });
        // Extract the module's HTTP contribution alongside its data, then drop
        // the value. The router is self-contained (`Router<()>`); merging is
        // deferred to `build()` so it happens in init order (`hq-mod-routes.1`).
        self.routers.push(module.register_routes());
        self
    }

    /// Finalize the registry with every module enabled.
    ///
    /// Shorthand for [`build_with_flags`](RootBuilder::build_with_flags) with
    /// [`AllEnabled`]. See that method for the validation passes.
    pub fn build(self) -> Result<Root, BuildError> {
        self.build_with_flags(&AllEnabled)
    }

    /// Finalize the registry, dropping modules `flags` reports as disabled.
    ///
    /// Runs, in order, and returns the first [`BuildError`]:
    ///
    /// 1. drop every module for which `flags.is_enabled` is false
    ///    (`hq-mod-core.7`);
    /// 2. reject an enabled module that depends on a disabled one
    ///    ([`BuildError::DisabledDependency`]) — a disabled dependency is a
    ///    configuration mistake, not a silent cascade;
    /// 3. reject two enabled modules claiming the same scope (`hq-mod-core.6`);
    /// 4. resolve the dependency graph into init order ([`crate::deps`],
    ///    `hq-mod-core.5`).
    ///
    /// Dispatch over `flags` is static (non-negotiable #1): no trait object is
    /// stored or boxed.
    pub fn build_with_flags<F: FeatureFlags>(self, flags: &F) -> Result<Root, BuildError> {
        // 1. Partition by flag. Keep enabled entries; remember disabled ids so a
        //    dangling enabled->disabled edge reports precisely. Owned ids so the
        //    set outlives the `into_iter` move below.
        let disabled: BTreeSet<ModuleId> = self
            .entries
            .iter()
            .map(|e| &e.meta.id)
            .filter(|id| !flags.is_enabled(id))
            .cloned()
            .collect();
        // 2. An enabled module may not depend on a disabled one.
        for entry in &self.entries {
            if disabled.contains(&entry.meta.id) {
                continue;
            }
            for dep in &entry.depends_on {
                if disabled.contains(dep) {
                    return Err(BuildError::DisabledDependency {
                        module: entry.meta.id.clone(),
                        dependency: dep.clone(),
                    });
                }
            }
        }
        // Filter entries and their parallel routers in lockstep so a router
        // stays aligned with its surviving module.
        let (enabled, mut enabled_routers): (Vec<ModuleEntry>, Vec<Router>) = self
            .entries
            .into_iter()
            .zip(self.routers)
            .filter(|(e, _)| !disabled.contains(&e.meta.id))
            .unzip();

        // 3 + 4. Validate and order the surviving set.
        Self::check_scope_conflicts(&enabled)?;
        Self::check_event_kind_conflicts(&enabled)?;
        Self::check_tool_namespacing(&enabled)?;
        Self::check_migration_versions(&enabled)?;
        let order = crate::deps::resolve_order(&enabled)?;
        // Reorder into init order. The module set is a handful of entries, so
        // cloning by index is cheaper than threading an in-place permutation.
        // Routers move out by index (each is taken exactly once) to avoid an
        // `axum::Router` clone per module.
        let entries = order.iter().map(|&i| enabled[i].clone()).collect();
        let routers = order
            .iter()
            .map(|&i| std::mem::replace(&mut enabled_routers[i], Router::new()))
            .collect();
        Ok(Root { entries, routers })
    }

    /// Reject a stack where two distinct modules claim the same authorization
    /// scope (`hq-mod-core.6`).
    ///
    /// A scope names a single source of truth for a `<resource>.<verb>`
    /// authority; two modules owning it is a wiring bug (overlapping route
    /// guards, ambiguous audit attribution). Detection is deterministic: scopes
    /// are visited in sorted order, so a given malformed stack always reports the
    /// same conflict. A module listing the same scope twice is not a conflict —
    /// only distinct claimants count.
    fn check_scope_conflicts(entries: &[ModuleEntry]) -> Result<(), BuildError> {
        let mut claimants: BTreeMap<&Scope, BTreeSet<&ModuleId>> = BTreeMap::new();
        for entry in entries {
            for scope in entry.capability.scopes() {
                claimants.entry(scope).or_default().insert(&entry.meta.id);
            }
        }
        for (scope, owners) in claimants {
            if owners.len() > 1 {
                return Err(BuildError::ScopeConflict {
                    scope: scope.clone(),
                    claimants: owners.into_iter().cloned().collect(),
                });
            }
        }
        Ok(())
    }

    /// Reject a stack where two distinct modules emit the same event kind+version
    /// (`hq-mod-events.5`).
    ///
    /// An event kind names a single source of truth; two modules emitting the
    /// same `<module>.<noun>.v<N>` is a wiring bug (ambiguous provenance, racing
    /// reducers). v1 and v2 of a noun are distinct kinds and never conflict — only
    /// the *same* kind+version emitted by two *different* modules does. Detection
    /// mirrors [`check_scope_conflicts`](Self::check_scope_conflicts): kinds are
    /// visited in sorted order, so a malformed stack always reports the same
    /// conflict, and a module listing the same kind twice is harmless.
    fn check_event_kind_conflicts(entries: &[ModuleEntry]) -> Result<(), BuildError> {
        let mut emitters: BTreeMap<&EventKind, BTreeSet<&ModuleId>> = BTreeMap::new();
        for entry in entries {
            for kind in entry.capability.emits() {
                emitters.entry(kind).or_default().insert(&entry.meta.id);
            }
        }
        for (kind, owners) in emitters {
            if owners.len() > 1 {
                return Err(BuildError::EventKindConflict {
                    kind: kind.clone(),
                    emitters: owners.into_iter().cloned().collect(),
                });
            }
        }
        Ok(())
    }

    /// Reject a contributed MCP tool whose name breaks the namespacing rule
    /// (`hq-mod-mcp.2`).
    ///
    /// Every tool name must be `<module-id>.<action>.<verb>` (three lowercase
    /// kebab-case segments), and its leading segment must be the id of the
    /// module that contributed it — a module may only register tools under its
    /// own namespace, so two modules can never collide on a tool name and audit
    /// attribution is unambiguous. Modules and their tools are visited in
    /// registration/declaration order, so a malformed stack always reports the
    /// same first offender.
    fn check_tool_namespacing(entries: &[ModuleEntry]) -> Result<(), BuildError> {
        for entry in entries {
            for tool in &entry.mcp_tools {
                let (prefix, _, _) = tool.parse_name().map_err(|reason| {
                    BuildError::MalformedToolName {
                        module: entry.meta.id.clone(),
                        tool: tool.name.clone(),
                        reason,
                    }
                })?;
                if prefix != entry.meta.id.as_str() {
                    return Err(BuildError::ToolNamespaceMismatch {
                        module: entry.meta.id.clone(),
                        tool: tool.name.clone(),
                        prefix: prefix.to_string(),
                    });
                }
            }
        }
        Ok(())
    }

    /// Reject a module whose migrations reuse a version number (`hq-mod-migrate.1`).
    ///
    /// Versions are the apply order within a module, so a duplicate is
    /// ambiguous — two distinct migrations claiming the same slot. Versions need
    /// not be contiguous and need not be declared in order (the [`Root`] sorts
    /// them); they only need to be unique per module. Modules are visited in
    /// registration order and versions in declaration order, so a malformed
    /// stack always reports the same first offender.
    fn check_migration_versions(entries: &[ModuleEntry]) -> Result<(), BuildError> {
        for entry in entries {
            let mut seen = BTreeSet::new();
            for migration in &entry.migrations {
                if !seen.insert(migration.version) {
                    return Err(BuildError::DuplicateMigrationVersion {
                        module: entry.meta.id.clone(),
                        version: migration.version,
                    });
                }
            }
        }
        Ok(())
    }
}

/// Why [`RootBuilder::build`] rejected a module set.
///
/// Marked `#[non_exhaustive]` so later epics can add variants — and so
/// downstream `match` arms keep a wildcard from day one.
#[derive(Debug, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub enum BuildError {
    /// A module declared a dependency on a module id that was never registered.
    UnknownDependency {
        /// The module carrying the dangling dependency.
        module: ModuleId,
        /// The unregistered id it named.
        missing: ModuleId,
    },
    /// An enabled module depends on a module the feature flags disabled
    /// (`hq-mod-core.7`). Enable the dependency or disable the dependent.
    DisabledDependency {
        /// The enabled module with the unsatisfiable dependency.
        module: ModuleId,
        /// The disabled module it depends on.
        dependency: ModuleId,
    },
    /// The module dependency graph contains a cycle, so no init order exists.
    /// Holds the ids on or downstream of the cycle.
    DependencyCycle(Vec<ModuleId>),
    /// Two or more distinct modules claim the same authorization scope
    /// (`hq-mod-core.6`). A scope must have exactly one owning module.
    ScopeConflict {
        /// The contested `<resource>.<verb>` scope.
        scope: Scope,
        /// The modules claiming it, sorted by id (always 2+).
        claimants: Vec<ModuleId>,
    },
    /// Two or more distinct modules emit the same event kind+version
    /// (`hq-mod-events.5`). Each `<module>.<noun>.v<N>` must have one emitter.
    EventKindConflict {
        /// The contested event kind, version included.
        kind: EventKind,
        /// The modules emitting it, sorted by id (always 2+).
        emitters: Vec<ModuleId>,
    },
    /// A module contributed an MCP tool whose name is not
    /// `<module-id>.<action>.<verb>` (`hq-mod-mcp.2`).
    MalformedToolName {
        /// The module that contributed the tool.
        module: ModuleId,
        /// The offending tool name, verbatim.
        tool: String,
        /// Why the name was rejected.
        reason: McpToolNameError,
    },
    /// A module contributed a well-formed tool name whose namespace prefix is
    /// not its own id (`hq-mod-mcp.2`) — a module may only register tools under
    /// its own namespace.
    ToolNamespaceMismatch {
        /// The module that contributed the tool.
        module: ModuleId,
        /// The offending tool name, verbatim.
        tool: String,
        /// The leading segment it used instead of `module`.
        prefix: String,
    },
    /// A module declared two migrations with the same version (`hq-mod-migrate.1`).
    /// Versions must be unique within a module.
    DuplicateMigrationVersion {
        /// The module with the colliding migrations.
        module: ModuleId,
        /// The version claimed more than once.
        version: u32,
    },
}

impl std::fmt::Display for BuildError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            BuildError::UnknownDependency { module, missing } => {
                write!(f, "module {module} depends on unregistered module {missing}")
            }
            BuildError::DisabledDependency { module, dependency } => {
                write!(f, "enabled module {module} depends on disabled module {dependency}")
            }
            BuildError::DependencyCycle(ids) => {
                write!(f, "module dependency cycle among: ")?;
                for (i, id) in ids.iter().enumerate() {
                    if i > 0 {
                        f.write_str(", ")?;
                    }
                    write!(f, "{id}")?;
                }
                Ok(())
            }
            BuildError::ScopeConflict { scope, claimants } => {
                let ids: Vec<&str> = claimants.iter().map(ModuleId::as_str).collect();
                write!(
                    f,
                    "scope `{scope}` is claimed by multiple modules: {}",
                    ids.join(", ")
                )
            }
            BuildError::EventKindConflict { kind, emitters } => {
                let ids: Vec<&str> = emitters.iter().map(ModuleId::as_str).collect();
                write!(
                    f,
                    "event kind `{kind}` is emitted by multiple modules: {}",
                    ids.join(", ")
                )
            }
            BuildError::MalformedToolName { module, tool, reason } => {
                write!(f, "module {module} contributes malformed MCP tool `{tool}`: {reason}")
            }
            BuildError::ToolNamespaceMismatch { module, tool, prefix } => {
                write!(
                    f,
                    "module {module} contributes MCP tool `{tool}` under foreign namespace `{prefix}`"
                )
            }
            BuildError::DuplicateMigrationVersion { module, version } => {
                write!(f, "module {module} declares migration version {version} more than once")
            }
        }
    }
}

impl std::error::Error for BuildError {}

/// The assembled module registry produced by [`RootBuilder::build`].
///
/// Read-only view over the registered modules' metadata. Later epics extend it
/// with the wired routers, MCP tool tables, and lifecycle handles; for now it
/// exposes the module list so the composition root and diagnostics can enumerate
/// what was loaded.
pub struct Root {
    entries: Vec<ModuleEntry>,
    /// Each module's contributed router, parallel to `entries`, in init order
    /// (`hq-mod-routes.1`). Consumed by [`into_router`](Root::into_router).
    routers: Vec<Router>,
}

// `axum::Router` is not `Debug`; report the root by its module metadata.
impl std::fmt::Debug for Root {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Root").field("modules", &self.entries).finish()
    }
}

impl Root {
    /// Number of registered modules.
    pub fn len(&self) -> usize {
        self.entries.len()
    }

    /// Whether no modules were registered.
    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }

    /// Metadata of every registered module, in init order — each module after
    /// every module it depends on (`hq-mod-core.5`). With no dependencies this
    /// is the registration order.
    pub fn modules(&self) -> impl Iterator<Item = &ModuleMeta> {
        self.entries.iter().map(|e| &e.meta)
    }

    /// Look up a module's metadata by id.
    pub fn module(&self, id: &ModuleId) -> Option<&ModuleMeta> {
        self.entries
            .iter()
            .map(|e| &e.meta)
            .find(|m| &m.id == id)
    }

    /// Every MCP tool contributed by the loaded modules, in module init order
    /// then per-module declaration order (`hq-mod-mcp.1`).
    ///
    /// Tools from feature-flag-disabled modules never appear: a disabled module
    /// is dropped before its [`ModuleEntry`] reaches the [`Root`], so its tools
    /// drop with it.
    pub fn mcp_tools(&self) -> impl Iterator<Item = &McpTool> {
        self.entries.iter().flat_map(|e| e.mcp_tools.iter())
    }

    /// Every active hook subscription as a `(module id, point)` pair, in module
    /// init order then per-module declaration order (`hq-mod-hooks.3`).
    ///
    /// A module declares the points it observes through
    /// [`Capability::subscribing`](crate::Capability::subscribing); this is the
    /// aggregated, flag-filtered view the `gt-hooks` dispatcher binds handlers
    /// against. Subscriptions from feature-flag-disabled modules never appear — a
    /// disabled module is dropped before its [`ModuleEntry`] reaches the [`Root`],
    /// so its subscriptions drop with it. Unlike scopes, the same [`HookPoint`]
    /// may be returned for several modules: observation is shared, not exclusive.
    pub fn hook_subscriptions(&self) -> impl Iterator<Item = (&ModuleId, HookPoint)> {
        self.entries.iter().flat_map(|e| {
            e.capability
                .subscribed_hooks()
                .iter()
                .map(move |&point| (&e.meta.id, point))
        })
    }

    /// The ordered migration apply plan (`hq-mod-migrate.1`).
    ///
    /// Each entry pairs the owning [`ModuleId`] (the migration's directory
    /// namespace) with the [`Migration`]. Ordered for application: modules in
    /// dependency-first init order — the same order [`modules`](Root::modules)
    /// reports — and, within a module, ascending [`Migration::version`].
    /// Disabled modules' migrations never appear (they dropped with their entry).
    pub fn migrations(&self) -> Vec<(&ModuleId, &Migration)> {
        let mut plan = Vec::new();
        for entry in &self.entries {
            let mut ms: Vec<&Migration> = entry.migrations.iter().collect();
            ms.sort_by_key(|m| m.version);
            plan.extend(ms.into_iter().map(|m| (&entry.meta.id, m)));
        }
        plan
    }

    /// The combined OpenAPI document for every loaded module that documents its
    /// routes (`hq-mod-routes.4`).
    ///
    /// Each contributing module's spec is mounted under its prefix
    /// [`module_prefix`](crate::module_prefix) — so a module's relative `/list`
    /// path appears as `/api/v1/<module>/list`, matching where
    /// [`into_router`](Root::into_router) actually serves it — and the specs are
    /// merged (paths plus component schemas) into one document in module init
    /// order. Modules that return no spec, and feature-flag-disabled modules
    /// (dropped before they reach the [`Root`]), contribute nothing. With no
    /// documenting module this is an empty-but-valid document carrying only the
    /// gt-core info block.
    pub fn openapi(&self) -> utoipa::openapi::OpenApi {
        use utoipa::openapi::{InfoBuilder, OpenApiBuilder};
        let mut combined = OpenApiBuilder::new()
            .info(
                InfoBuilder::new()
                    .title("gt-core")
                    .version(env!("CARGO_PKG_VERSION"))
                    .build(),
            )
            .build();
        for entry in &self.entries {
            if let Some(spec) = &entry.openapi {
                // `nest` rewrites the spec's paths under the prefix and merges its
                // components, so module annotations stay prefix-free.
                combined = combined.nest(crate::module_prefix(&entry.meta.id), spec.clone());
            }
        }
        combined
    }

    /// Mount every module's contributed routes into one application
    /// [`Router`] (`hq-mod-routes.1`, `.2`).
    ///
    /// Each module's router is nested under its per-module prefix
    /// [`module_prefix`](crate::module_prefix) — `/api/v1/<module-id>` — in init
    /// order (each module after its dependencies). The composition root obtains a
    /// fully wired router without naming a single route or prefix. With no
    /// modules — or only modules that contribute no HTTP surface — this is an
    /// empty router.
    ///
    /// Because each module owns a distinct path segment, two modules declaring
    /// the same relative path no longer collide (`hq-mod-routes.2`): a `/` route
    /// in `beads` answers at `/api/v1/beads/`, independent of a `/` route in
    /// `rigs`.
    ///
    /// A module that declares scopes in its
    /// [`capability`](crate::GtModule::capability) has every one of its routes
    /// guarded by [`guard_module_scopes`](crate::guard_module_scopes): the caller
    /// must hold the `<module>.<read|write>` scope the request method implies
    /// (`hq-mod-routes.3`). A module claiming no scope keeps public routes. Takes
    /// `self` because an `axum::Router` is consumed by nesting.
    pub fn into_router(self) -> Router {
        // Pair each module's id + claimed scopes with its router. Borrowing
        // `entries` while moving `routers` is fine — they are disjoint fields.
        self.entries
            .iter()
            .zip(self.routers)
            .fold(Router::new(), |app, (entry, module_router)| {
                let guarded = if entry.capability.scopes().is_empty() {
                    module_router
                } else {
                    crate::guard_module_scopes(module_router, &entry.meta.id)
                };
                app.nest(&crate::module_prefix(&entry.meta.id), guarded)
            })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use semver::Version;

    struct Beads;
    impl GtModule for Beads {
        fn meta(&self) -> ModuleMeta {
            ModuleMeta::new(
                ModuleId::new("beads").unwrap(),
                "Beads",
                Version::new(1, 0, 0),
                "Issue tracking aggregate.",
            )
        }
    }

    struct Rigs;
    impl GtModule for Rigs {
        fn meta(&self) -> ModuleMeta {
            ModuleMeta::new(
                ModuleId::new("rigs").unwrap(),
                "Rigs",
                Version::new(0, 2, 0),
                "Repo + worktree registry.",
            )
        }
    }

    /// Depends on `beads`; used to assert build orders dependencies first.
    struct Merge;
    impl GtModule for Merge {
        fn meta(&self) -> ModuleMeta {
            ModuleMeta::new(
                ModuleId::new("merge").unwrap(),
                "Merge",
                Version::new(1, 0, 0),
                "Merge queue over beads.",
            )
        }
        fn dependencies(&self) -> Vec<ModuleId> {
            vec![ModuleId::new("beads").unwrap()]
        }
    }

    #[test]
    fn empty_builder_builds_empty_root() {
        let root = RootBuilder::new().build().unwrap();
        assert!(root.is_empty());
        assert_eq!(root.len(), 0);
        assert_eq!(root.modules().count(), 0);
    }

    #[test]
    fn registers_modules_in_order() {
        let root = RootBuilder::new().module(Beads).module(Rigs).build().unwrap();
        assert_eq!(root.len(), 2);
        let ids: Vec<&str> = root.modules().map(|m| m.id.as_str()).collect();
        assert_eq!(ids, ["beads", "rigs"]);
    }

    #[test]
    fn looks_up_module_by_id() {
        let root = RootBuilder::new().module(Beads).module(Rigs).build().unwrap();
        let id = ModuleId::new("rigs").unwrap();
        assert_eq!(root.module(&id).unwrap().version, Version::new(0, 2, 0));
        assert!(root.module(&ModuleId::new("absent").unwrap()).is_none());
    }

    #[test]
    fn build_orders_dependencies_before_dependents() {
        // Merge registered before its dependency beads — build must reorder.
        let root = RootBuilder::new().module(Merge).module(Beads).build().unwrap();
        let ids: Vec<&str> = root.modules().map(|m| m.id.as_str()).collect();
        assert_eq!(ids, ["beads", "merge"]);
    }

    #[test]
    fn build_rejects_unknown_dependency() {
        // Merge depends on beads, which is never registered.
        let err = RootBuilder::new().module(Merge).build().unwrap_err();
        assert!(matches!(err, BuildError::UnknownDependency { .. }));
    }

    use crate::scope::Scope;

    /// Module with a configurable id + claimed scopes, for conflict tests.
    struct Claimer {
        id: &'static str,
        scopes: Vec<&'static str>,
    }

    impl GtModule for Claimer {
        fn meta(&self) -> ModuleMeta {
            ModuleMeta::new(
                ModuleId::new(self.id).unwrap(),
                self.id,
                Version::new(1, 0, 0),
                "scope-claiming test module",
            )
        }

        fn capability(&self) -> Capability {
            Capability::empty().claiming_all(self.scopes.iter().map(|s| Scope::new(*s).unwrap()))
        }
    }

    fn claimer(id: &'static str, scopes: &[&'static str]) -> Claimer {
        Claimer { id, scopes: scopes.to_vec() }
    }

    #[test]
    fn disjoint_scopes_build_cleanly() {
        let root = RootBuilder::new()
            .module(claimer("beads", &["beads.read", "beads.write"]))
            .module(claimer("rigs", &["rigs.read"]))
            .build()
            .unwrap();
        assert_eq!(root.len(), 2);
    }

    #[test]
    fn two_modules_same_scope_conflict() {
        let err = RootBuilder::new()
            .module(claimer("beads", &["beads.write"]))
            .module(claimer("beads-legacy", &["beads.write"]))
            .build()
            .unwrap_err();
        let BuildError::ScopeConflict { scope, claimants } = err else {
            panic!("expected ScopeConflict, got {err:?}");
        };
        assert_eq!(scope, Scope::new("beads.write").unwrap());
        // Sorted by id, both claimants present.
        let ids: Vec<String> = claimants.iter().map(|m| m.to_string()).collect();
        assert_eq!(ids, ["beads", "beads-legacy"]);
    }

    #[test]
    fn same_module_repeating_a_scope_is_not_a_conflict() {
        // One module listing the same scope twice is harmless.
        let root = RootBuilder::new()
            .module(claimer("beads", &["beads.write", "beads.write"]))
            .build()
            .unwrap();
        assert_eq!(root.len(), 1);
    }

    #[test]
    fn conflict_detection_is_deterministic() {
        // Two independent conflicts; the lexicographically-first scope wins.
        let make = || {
            RootBuilder::new()
                .module(claimer("a", &["alpha.read"]))
                .module(claimer("b", &["alpha.read", "zeta.read"]))
                .module(claimer("c", &["zeta.read"]))
                .build()
                .unwrap_err()
        };
        let BuildError::ScopeConflict { scope, .. } = make() else {
            panic!("expected ScopeConflict");
        };
        assert_eq!(scope, Scope::new("alpha.read").unwrap());
        // Repeated runs report the same scope.
        let BuildError::ScopeConflict { scope: again, .. } = make() else {
            panic!("expected ScopeConflict");
        };
        assert_eq!(again, Scope::new("alpha.read").unwrap());
    }

    #[test]
    fn conflict_message_names_scope_and_modules() {
        let err = RootBuilder::new()
            .module(claimer("beads", &["beads.write"]))
            .module(claimer("audit", &["beads.write"]))
            .build()
            .unwrap_err();
        let msg = err.to_string();
        assert!(msg.contains("beads.write"), "got: {msg}");
        assert!(msg.contains("audit") && msg.contains("beads"), "got: {msg}");
    }

    // --- Event-kind emission ownership (hq-mod-events.5) --------------------

    use crate::event_kind::EventKind;

    /// Module with a configurable id + emitted event kinds, for conflict tests.
    struct Emitter {
        id: &'static str,
        kinds: Vec<&'static str>,
    }
    impl GtModule for Emitter {
        fn meta(&self) -> ModuleMeta {
            ModuleMeta::new(
                ModuleId::new(self.id).unwrap(),
                self.id,
                Version::new(1, 0, 0),
                "event-emitting test module",
            )
        }
        fn capability(&self) -> Capability {
            Capability::empty().emitting_all(self.kinds.iter().map(|k| EventKind::new(*k).unwrap()))
        }
    }
    fn emitter(id: &'static str, kinds: &[&'static str]) -> Emitter {
        Emitter { id, kinds: kinds.to_vec() }
    }

    #[test]
    fn disjoint_event_kinds_build_cleanly() {
        let root = RootBuilder::new()
            .module(emitter("beads", &["beads.created.v1", "beads.closed.v1"]))
            .module(emitter("rigs", &["rigs.added.v1"]))
            .build()
            .unwrap();
        assert_eq!(root.len(), 2);
    }

    #[test]
    fn two_modules_emitting_same_kind_conflict() {
        let err = RootBuilder::new()
            .module(emitter("beads", &["beads.created.v1"]))
            .module(emitter("legacy", &["beads.created.v1"]))
            .build()
            .unwrap_err();
        let BuildError::EventKindConflict { kind, emitters } = err else {
            panic!("expected EventKindConflict, got {err:?}");
        };
        assert_eq!(kind, EventKind::new("beads.created.v1").unwrap());
        let ids: Vec<String> = emitters.iter().map(|m| m.to_string()).collect();
        assert_eq!(ids, ["beads", "legacy"]);
    }

    #[test]
    fn distinct_versions_of_a_noun_do_not_conflict() {
        // v1 and v2 of the same noun are separate kinds; one module emits v1, the
        // other v2 — no ownership clash.
        let root = RootBuilder::new()
            .module(emitter("beads", &["beads.created.v1"]))
            .module(emitter("beads-next", &["beads.created.v2"]))
            .build()
            .unwrap();
        assert_eq!(root.len(), 2);
    }

    #[test]
    fn same_module_repeating_a_kind_is_not_a_conflict() {
        let root = RootBuilder::new()
            .module(emitter("beads", &["beads.created.v1", "beads.created.v1"]))
            .build()
            .unwrap();
        assert_eq!(root.len(), 1);
    }

    #[test]
    fn event_kind_conflict_message_names_kind_and_modules() {
        let err = RootBuilder::new()
            .module(emitter("beads", &["beads.created.v1"]))
            .module(emitter("audit", &["beads.created.v1"]))
            .build()
            .unwrap_err();
        let msg = err.to_string();
        assert!(msg.contains("beads.created.v1"), "got: {msg}");
        assert!(msg.contains("audit") && msg.contains("beads"), "got: {msg}");
    }

    #[test]
    fn disabling_an_emitter_clears_the_event_kind_conflict() {
        let flags = crate::flags::DisabledModules::new([ModuleId::new("legacy").unwrap()]);
        let root = RootBuilder::new()
            .module(emitter("beads", &["beads.created.v1"]))
            .module(emitter("legacy", &["beads.created.v1"]))
            .build_with_flags(&flags)
            .unwrap();
        assert_eq!(root.len(), 1);
    }

    use crate::flags::DisabledModules;

    #[test]
    fn disabled_module_is_dropped_from_root() {
        let flags = DisabledModules::new([ModuleId::new("rigs").unwrap()]);
        let root = RootBuilder::new()
            .module(Beads)
            .module(Rigs)
            .build_with_flags(&flags)
            .unwrap();
        let ids: Vec<&str> = root.modules().map(|m| m.id.as_str()).collect();
        assert_eq!(ids, ["beads"]);
    }

    #[test]
    fn disabling_a_dependency_used_by_enabled_module_is_rejected() {
        // merge depends on beads; disabling beads leaves merge unsatisfiable.
        let flags = DisabledModules::new([ModuleId::new("beads").unwrap()]);
        let err = RootBuilder::new()
            .module(Beads)
            .module(Merge)
            .build_with_flags(&flags)
            .unwrap_err();
        let BuildError::DisabledDependency { module, dependency } = err else {
            panic!("expected DisabledDependency, got {err:?}");
        };
        assert_eq!(module.as_str(), "merge");
        assert_eq!(dependency.as_str(), "beads");
    }

    #[test]
    fn disabling_a_module_and_its_dependent_together_builds() {
        // Disable both merge and its dependency beads — no dangling edge.
        let flags =
            DisabledModules::new([ModuleId::new("beads").unwrap(), ModuleId::new("merge").unwrap()]);
        let root = RootBuilder::new()
            .module(Beads)
            .module(Merge)
            .module(Rigs)
            .build_with_flags(&flags)
            .unwrap();
        let ids: Vec<&str> = root.modules().map(|m| m.id.as_str()).collect();
        assert_eq!(ids, ["rigs"]);
    }

    #[test]
    fn build_defaults_to_all_enabled() {
        let root = RootBuilder::new().module(Beads).module(Rigs).build().unwrap();
        assert_eq!(root.len(), 2);
    }

    use crate::mcp::McpRegistry;

    /// Module that contributes two MCP tools, for collection tests.
    struct Toolful;
    impl GtModule for Toolful {
        fn meta(&self) -> ModuleMeta {
            ModuleMeta::new(
                ModuleId::new("toolful").unwrap(),
                "Toolful",
                Version::new(1, 0, 0),
                "Contributes MCP tools.",
            )
        }
        fn register_mcp_tools(&self, reg: &mut McpRegistry) {
            reg.tool("toolful.create.execute", "Create")
                .tool("toolful.close.execute", "Close");
        }
    }

    #[test]
    fn module_with_no_tools_contributes_nothing() {
        let root = RootBuilder::new().module(Beads).build().unwrap();
        assert_eq!(root.mcp_tools().count(), 0);
    }

    #[test]
    fn builder_collects_contributed_tools() {
        let root = RootBuilder::new().module(Toolful).build().unwrap();
        let names: Vec<&str> = root.mcp_tools().map(|t| t.name.as_str()).collect();
        assert_eq!(names, ["toolful.create.execute", "toolful.close.execute"]);
    }

    #[test]
    fn tools_follow_module_init_order() {
        // Merge depends on beads, so beads inits first; tools must follow.
        struct ToolfulBeads;
        impl GtModule for ToolfulBeads {
            fn meta(&self) -> ModuleMeta {
                ModuleMeta::new(
                    ModuleId::new("beads").unwrap(),
                    "Beads",
                    Version::new(1, 0, 0),
                    "Beads.",
                )
            }
            fn register_mcp_tools(&self, reg: &mut McpRegistry) {
                reg.tool("beads.read.execute", "Read");
            }
        }
        struct ToolfulMerge;
        impl GtModule for ToolfulMerge {
            fn meta(&self) -> ModuleMeta {
                ModuleMeta::new(
                    ModuleId::new("merge").unwrap(),
                    "Merge",
                    Version::new(1, 0, 0),
                    "Merge.",
                )
            }
            fn dependencies(&self) -> Vec<ModuleId> {
                vec![ModuleId::new("beads").unwrap()]
            }
            fn register_mcp_tools(&self, reg: &mut McpRegistry) {
                reg.tool("merge.submit.execute", "Submit");
            }
        }
        // Register merge first; build reorders so beads (dependency) inits first.
        let root = RootBuilder::new().module(ToolfulMerge).module(ToolfulBeads).build().unwrap();
        let names: Vec<&str> = root.mcp_tools().map(|t| t.name.as_str()).collect();
        assert_eq!(names, ["beads.read.execute", "merge.submit.execute"]);
    }

    #[test]
    fn disabled_module_tools_are_dropped() {
        let flags = DisabledModules::new([ModuleId::new("toolful").unwrap()]);
        let root = RootBuilder::new()
            .module(Beads)
            .module(Toolful)
            .build_with_flags(&flags)
            .unwrap();
        assert_eq!(root.mcp_tools().count(), 0);
    }

    // --- Hook subscriptions (hq-mod-hooks.3) --------------------------------

    use crate::hook::HookPoint;

    /// Module subscribing to a fixed set of hook points, for aggregation tests.
    struct Subscriber {
        id: &'static str,
        points: Vec<HookPoint>,
        deps: Vec<&'static str>,
    }
    impl GtModule for Subscriber {
        fn meta(&self) -> ModuleMeta {
            ModuleMeta::new(
                ModuleId::new(self.id).unwrap(),
                self.id,
                Version::new(1, 0, 0),
                "Subscriber.",
            )
        }
        fn capability(&self) -> Capability {
            Capability::empty().subscribing_all(self.points.clone())
        }
        fn dependencies(&self) -> Vec<ModuleId> {
            self.deps.iter().map(|d| ModuleId::new(*d).unwrap()).collect()
        }
    }

    #[test]
    fn module_with_no_subscriptions_contributes_none() {
        let root = RootBuilder::new().module(Beads).build().unwrap();
        assert_eq!(root.hook_subscriptions().count(), 0);
    }

    #[test]
    fn subscriptions_follow_module_init_then_declaration_order() {
        // `dependent` depends on `base`, so base inits first; within a module the
        // declaration order is preserved.
        let base = Subscriber {
            id: "base",
            points: vec![HookPoint::AfterCommand, HookPoint::OnClaim],
            deps: vec![],
        };
        let dependent = Subscriber {
            id: "dependent",
            points: vec![HookPoint::BeforeCommand],
            deps: vec!["base"],
        };
        // Register dependent first; build reorders so base (dependency) is first.
        let root = RootBuilder::new().module(dependent).module(base).build().unwrap();
        let subs: Vec<(&str, HookPoint)> = root
            .hook_subscriptions()
            .map(|(id, p)| (id.as_str(), p))
            .collect();
        assert_eq!(
            subs,
            [
                ("base", HookPoint::AfterCommand),
                ("base", HookPoint::OnClaim),
                ("dependent", HookPoint::BeforeCommand),
            ]
        );
    }

    #[test]
    fn the_same_point_may_be_observed_by_many_modules() {
        let a = Subscriber { id: "a", points: vec![HookPoint::OnTransition], deps: vec![] };
        let b = Subscriber { id: "b", points: vec![HookPoint::OnTransition], deps: vec![] };
        let root = RootBuilder::new().module(a).module(b).build().unwrap();
        let observers: Vec<&str> = root
            .hook_subscriptions()
            .filter(|(_, p)| *p == HookPoint::OnTransition)
            .map(|(id, _)| id.as_str())
            .collect();
        assert_eq!(observers, ["a", "b"]);
    }

    #[test]
    fn disabled_module_subscriptions_are_dropped() {
        let off = Subscriber { id: "off", points: vec![HookPoint::AfterEvent], deps: vec![] };
        let on = Subscriber { id: "on", points: vec![HookPoint::AfterCommand], deps: vec![] };
        let flags = DisabledModules::new([ModuleId::new("off").unwrap()]);
        let root = RootBuilder::new().module(off).module(on).build_with_flags(&flags).unwrap();
        let subs: Vec<(&str, HookPoint)> = root
            .hook_subscriptions()
            .map(|(id, p)| (id.as_str(), p))
            .collect();
        assert_eq!(subs, [("on", HookPoint::AfterCommand)]);
    }

    // --- Router contribution (hq-mod-routes.1) ------------------------------

    use axum::body::Body;
    use axum::http::{Request, StatusCode};
    use axum::routing::get;
    use axum::Router;
    use tower::ServiceExt; // `oneshot`

    /// Test module that contributes one GET route at a *relative* path,
    /// answering with its own id so the mounted router proves which module
    /// served. The builder namespaces it under `/api/v1/<id>` (`hq-mod-routes.2`).
    struct Routed {
        id: &'static str,
        rel_path: &'static str,
        deps: Vec<&'static str>,
        scopes: Vec<&'static str>,
    }

    impl Routed {
        /// All test modules declare the same relative path `/list`, so the only
        /// thing keeping them apart is the per-module prefix.
        fn new(id: &'static str) -> Self {
            Routed { id, rel_path: "/list", deps: Vec::new(), scopes: Vec::new() }
        }
        fn with_dep(mut self, dep: &'static str) -> Self {
            self.deps.push(dep);
            self
        }
        /// Claim a scope, opting this module's routes into RBAC (`hq-mod-routes.3`).
        fn claiming(mut self, scope: &'static str) -> Self {
            self.scopes.push(scope);
            self
        }
    }

    impl GtModule for Routed {
        fn meta(&self) -> ModuleMeta {
            ModuleMeta::new(
                ModuleId::new(self.id).unwrap(),
                self.id,
                Version::new(1, 0, 0),
                "route-contributing test module",
            )
        }
        fn dependencies(&self) -> Vec<ModuleId> {
            self.deps.iter().map(|d| ModuleId::new(*d).unwrap()).collect()
        }
        fn capability(&self) -> Capability {
            Capability::empty().claiming_all(self.scopes.iter().map(|s| Scope::new(*s).unwrap()))
        }
        fn register_routes(&self) -> Router {
            let id = self.id;
            // A read (GET) and a write (POST) route, so scope guards can be
            // method-distinguished.
            Router::new()
                .route(self.rel_path, get(move || async move { id }).post(move || async move { id }))
        }
    }

    /// GET `path` against `app`; returns the status (and body for 2xx).
    async fn hit(app: Router, path: &str) -> (StatusCode, String) {
        let res = app
            .oneshot(Request::builder().uri(path).body(Body::empty()).unwrap())
            .await
            .unwrap();
        let status = res.status();
        let bytes = axum::body::to_bytes(res.into_body(), usize::MAX).await.unwrap();
        (status, String::from_utf8(bytes.to_vec()).unwrap())
    }

    #[tokio::test]
    async fn module_route_is_reachable_under_its_prefix() {
        let root = RootBuilder::new().module(Routed::new("beads")).build().unwrap();
        let (status, body) = hit(root.into_router(), "/api/v1/beads/list").await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(body, "beads");
    }

    #[tokio::test]
    async fn relative_path_without_prefix_is_not_served() {
        // The module declares `/list`, but only the namespaced path resolves —
        // the prefix is mandatory (`hq-mod-routes.2`).
        let root = RootBuilder::new().module(Routed::new("beads")).build().unwrap();
        let app = root.into_router();
        assert_eq!(hit(app.clone(), "/list").await.0, StatusCode::NOT_FOUND);
        assert_eq!(hit(app, "/beads/list").await.0, StatusCode::NOT_FOUND);
    }

    #[tokio::test]
    async fn same_relative_path_in_two_modules_does_not_collide() {
        // Both modules declare `/list`; the per-module prefix keeps them apart.
        let root = RootBuilder::new()
            .module(Routed::new("beads"))
            .module(Routed::new("rigs"))
            .build()
            .unwrap();
        let app = root.into_router();
        assert_eq!(hit(app.clone(), "/api/v1/beads/list").await, (StatusCode::OK, "beads".into()));
        assert_eq!(hit(app, "/api/v1/rigs/list").await, (StatusCode::OK, "rigs".into()));
    }

    #[tokio::test]
    async fn module_with_no_routes_contributes_nothing() {
        // `Beads` does not override `register_routes`; the default empty router
        // means no path is served, even under its prefix.
        let root = RootBuilder::new().module(Beads).build().unwrap();
        let app = root.into_router();
        assert_eq!(hit(app.clone(), "/anything").await.0, StatusCode::NOT_FOUND);
        assert_eq!(hit(app, "/api/v1/beads/list").await.0, StatusCode::NOT_FOUND);
    }

    #[tokio::test]
    async fn disabled_module_route_is_dropped() {
        let flags = DisabledModules::new([ModuleId::new("rigs").unwrap()]);
        let root = RootBuilder::new()
            .module(Routed::new("beads"))
            .module(Routed::new("rigs"))
            .build_with_flags(&flags)
            .unwrap();
        let app = root.into_router();
        assert_eq!(hit(app.clone(), "/api/v1/beads/list").await.0, StatusCode::OK);
        // The disabled module's prefix serves nothing.
        assert_eq!(hit(app, "/api/v1/rigs/list").await.0, StatusCode::NOT_FOUND);
    }

    #[tokio::test]
    async fn routers_kept_aligned_with_modules_after_reorder() {
        // `merge` depends on `beads`, so build reorders it after `beads`. The
        // router ordering must follow the same permutation: both prefixes resolve.
        let root = RootBuilder::new()
            .module(Routed::new("merge").with_dep("beads"))
            .module(Routed::new("beads"))
            .build()
            .unwrap();
        // Sanity: build reordered beads before merge.
        let ids: Vec<&str> = root.modules().map(|m| m.id.as_str()).collect();
        assert_eq!(ids, ["beads", "merge"]);
        let app = root.into_router();
        assert_eq!(hit(app.clone(), "/api/v1/beads/list").await, (StatusCode::OK, "beads".into()));
        assert_eq!(hit(app, "/api/v1/merge/list").await, (StatusCode::OK, "merge".into()));
    }

    #[tokio::test]
    async fn claimed_scopes_guard_routes_through_the_builder() {
        use axum::http::Method;
        use crate::CallerScopes;

        // `beads` claims read+write → guarded; `rigs` claims nothing → public.
        let root = RootBuilder::new()
            .module(Routed::new("beads").claiming("beads.read").claiming("beads.write"))
            .module(Routed::new("rigs"))
            .build()
            .unwrap();
        let app = root.into_router();

        let send = |app: Router, method: Method, path: &'static str, cs: Option<CallerScopes>| async move {
            let mut req = Request::builder().method(method).uri(path).body(Body::empty()).unwrap();
            if let Some(cs) = cs {
                req.extensions_mut().insert(cs);
            }
            app.oneshot(req).await.unwrap().status()
        };

        // Guarded module: no caller scopes → 401.
        assert_eq!(
            send(app.clone(), Method::GET, "/api/v1/beads/list", None).await,
            StatusCode::UNAUTHORIZED
        );
        // With beads.read → GET ok, POST forbidden.
        let read = CallerScopes::new([Scope::new("beads.read").unwrap()]);
        assert_eq!(
            send(app.clone(), Method::GET, "/api/v1/beads/list", Some(read.clone())).await,
            StatusCode::OK
        );
        assert_eq!(
            send(app.clone(), Method::POST, "/api/v1/beads/list", Some(read)).await,
            StatusCode::FORBIDDEN
        );
        // Unguarded module stays public — no caller scopes needed.
        assert_eq!(
            send(app, Method::GET, "/api/v1/rigs/list", None).await,
            StatusCode::OK
        );
    }

    // --- OpenAPI aggregation (hq-mod-routes.4) ------------------------------

    /// Handlers + derived specs for documenting test modules. Each declares a
    /// *relative* `/list` path; the builder rewrites it under the module prefix.
    /// The handler fns are referenced only by the `utoipa::path` macro that feeds
    /// the derived spec, never called directly.
    #[allow(dead_code)]
    mod openapi_fixture {
        #[utoipa::path(get, path = "/list", responses((status = 200, description = "beads")))]
        pub async fn list_beads() -> &'static str {
            "[]"
        }
        #[derive(utoipa::OpenApi)]
        #[openapi(paths(list_beads))]
        pub struct BeadsApi;

        #[utoipa::path(get, path = "/list", responses((status = 200, description = "rigs")))]
        pub async fn list_rigs() -> &'static str {
            "[]"
        }
        #[derive(utoipa::OpenApi)]
        #[openapi(paths(list_rigs))]
        pub struct RigsApi;
    }

    /// Test module that documents its routes with the given derived spec.
    struct Documented {
        id: &'static str,
        spec: utoipa::openapi::OpenApi,
    }
    impl GtModule for Documented {
        fn meta(&self) -> ModuleMeta {
            ModuleMeta::new(
                ModuleId::new(self.id).unwrap(),
                self.id,
                Version::new(1, 0, 0),
                "documented test module",
            )
        }
        fn openapi(&self) -> Option<utoipa::openapi::OpenApi> {
            Some(self.spec.clone())
        }
    }

    fn beads_doc() -> Documented {
        Documented { id: "beads", spec: <openapi_fixture::BeadsApi as utoipa::OpenApi>::openapi() }
    }
    fn rigs_doc() -> Documented {
        Documented { id: "rigs", spec: <openapi_fixture::RigsApi as utoipa::OpenApi>::openapi() }
    }

    #[test]
    fn module_openapi_paths_are_prefixed() {
        let root = RootBuilder::new().module(beads_doc()).build().unwrap();
        let doc = root.openapi();
        assert!(doc.paths.paths.contains_key("/api/v1/beads/list"), "got: {:?}", doc.paths.paths.keys().collect::<Vec<_>>());
        // The bare relative path is rewritten away.
        assert!(!doc.paths.paths.contains_key("/list"));
    }

    #[test]
    fn multiple_module_specs_merge_under_their_prefixes() {
        let root = RootBuilder::new().module(beads_doc()).module(rigs_doc()).build().unwrap();
        let doc = root.openapi();
        assert!(doc.paths.paths.contains_key("/api/v1/beads/list"));
        assert!(doc.paths.paths.contains_key("/api/v1/rigs/list"));
    }

    #[test]
    fn module_without_openapi_contributes_no_paths() {
        let root = RootBuilder::new().module(Beads).build().unwrap();
        assert!(root.openapi().paths.paths.is_empty());
    }

    #[test]
    fn disabled_module_openapi_is_dropped() {
        let flags = DisabledModules::new([ModuleId::new("beads").unwrap()]);
        let root = RootBuilder::new().module(beads_doc()).build_with_flags(&flags).unwrap();
        assert!(root.openapi().paths.paths.is_empty());
    }

    // --- Tool name namespacing (hq-mod-mcp.2) -------------------------------

    /// Module whose id and contributed tool name are both configurable, for
    /// namespacing tests.
    struct NamedTool {
        id: &'static str,
        tool: &'static str,
    }
    impl GtModule for NamedTool {
        fn meta(&self) -> ModuleMeta {
            ModuleMeta::new(
                ModuleId::new(self.id).unwrap(),
                self.id,
                Version::new(1, 0, 0),
                "namespacing test module",
            )
        }
        fn register_mcp_tools(&self, reg: &mut McpRegistry) {
            reg.tool(self.tool, "t");
        }
    }

    #[test]
    fn well_namespaced_tool_builds() {
        let root = RootBuilder::new()
            .module(NamedTool { id: "gt-rig", tool: "gt-rig.set-prefix.execute" })
            .build()
            .unwrap();
        assert_eq!(root.mcp_tools().count(), 1);
    }

    #[test]
    fn malformed_tool_name_is_rejected() {
        let err = RootBuilder::new()
            .module(NamedTool { id: "beads", tool: "beads.create" })
            .build()
            .unwrap_err();
        let BuildError::MalformedToolName { module, tool, .. } = err else {
            panic!("expected MalformedToolName, got {err:?}");
        };
        assert_eq!(module.as_str(), "beads");
        assert_eq!(tool, "beads.create");
    }

    #[test]
    fn foreign_namespace_prefix_is_rejected() {
        // beads module trying to register a tool under the rigs namespace.
        let err = RootBuilder::new()
            .module(NamedTool { id: "beads", tool: "rigs.create.execute" })
            .build()
            .unwrap_err();
        let BuildError::ToolNamespaceMismatch { module, tool, prefix } = err else {
            panic!("expected ToolNamespaceMismatch, got {err:?}");
        };
        assert_eq!(module.as_str(), "beads");
        assert_eq!(tool, "rigs.create.execute");
        assert_eq!(prefix, "rigs");
    }

    #[test]
    fn namespace_mismatch_message_names_tool_and_module() {
        let err = RootBuilder::new()
            .module(NamedTool { id: "beads", tool: "rigs.create.execute" })
            .build()
            .unwrap_err();
        let msg = err.to_string();
        assert!(msg.contains("beads"), "got: {msg}");
        assert!(msg.contains("rigs.create.execute"), "got: {msg}");
    }

    #[test]
    fn disabled_module_with_bad_tool_does_not_fail_build() {
        // A disabled module's tools are never validated — it is dropped first.
        let flags = DisabledModules::new([ModuleId::new("beads").unwrap()]);
        let root = RootBuilder::new()
            .module(NamedTool { id: "beads", tool: "garbage" })
            .module(Rigs)
            .build_with_flags(&flags)
            .unwrap();
        let ids: Vec<&str> = root.modules().map(|m| m.id.as_str()).collect();
        assert_eq!(ids, ["rigs"]);
    }

    // --- Migrations (hq-mod-migrate.1) --------------------------------------

    use crate::migrate::Migration;

    /// Module contributing migrations declared out of version order, plus an
    /// optional dependency, for plan-ordering tests.
    struct Migrated {
        id: &'static str,
        versions: Vec<u32>,
        deps: Vec<&'static str>,
    }
    impl Migrated {
        fn new(id: &'static str, versions: &[u32]) -> Self {
            Migrated { id, versions: versions.to_vec(), deps: Vec::new() }
        }
        fn with_dep(mut self, dep: &'static str) -> Self {
            self.deps.push(dep);
            self
        }
    }
    impl GtModule for Migrated {
        fn meta(&self) -> ModuleMeta {
            ModuleMeta::new(
                ModuleId::new(self.id).unwrap(),
                self.id,
                Version::new(1, 0, 0),
                "migration test module",
            )
        }
        fn dependencies(&self) -> Vec<ModuleId> {
            self.deps.iter().map(|d| ModuleId::new(*d).unwrap()).collect()
        }
        fn migrations(&self) -> Vec<Migration> {
            self.versions.iter().map(|v| Migration::new(*v, format!("m{v}"), "")).collect()
        }
    }

    #[test]
    fn module_with_no_migrations_has_empty_plan() {
        let root = RootBuilder::new().module(Beads).build().unwrap();
        assert!(root.migrations().is_empty());
    }

    #[test]
    fn plan_sorts_by_version_within_module() {
        // Declared 3, 1, 2 — plan must apply 1, 2, 3.
        let root = RootBuilder::new().module(Migrated::new("beads", &[3, 1, 2])).build().unwrap();
        let versions: Vec<u32> = root.migrations().iter().map(|(_, m)| m.version).collect();
        assert_eq!(versions, [1, 2, 3]);
    }

    #[test]
    fn plan_orders_modules_dependency_first() {
        // merge depends on beads; even registered first, beads' migrations apply first.
        let root = RootBuilder::new()
            .module(Migrated::new("merge", &[1]).with_dep("beads"))
            .module(Migrated::new("beads", &[1]))
            .build()
            .unwrap();
        let owners: Vec<&str> = root.migrations().iter().map(|(id, _)| id.as_str()).collect();
        assert_eq!(owners, ["beads", "merge"]);
    }

    #[test]
    fn duplicate_version_within_module_is_rejected() {
        let err = RootBuilder::new().module(Migrated::new("beads", &[1, 2, 1])).build().unwrap_err();
        let BuildError::DuplicateMigrationVersion { module, version } = err else {
            panic!("expected DuplicateMigrationVersion, got {err:?}");
        };
        assert_eq!(module.as_str(), "beads");
        assert_eq!(version, 1);
    }

    #[test]
    fn same_version_across_modules_is_allowed() {
        // Versions are namespaced per module; beads v1 and rigs v1 do not collide.
        let root = RootBuilder::new()
            .module(Migrated::new("beads", &[1]))
            .module(Migrated::new("rigs", &[1]))
            .build()
            .unwrap();
        assert_eq!(root.migrations().len(), 2);
    }

    #[test]
    fn disabled_module_migrations_are_dropped() {
        let flags = DisabledModules::new([ModuleId::new("beads").unwrap()]);
        let root = RootBuilder::new()
            .module(Migrated::new("beads", &[1, 2]))
            .module(Migrated::new("rigs", &[1]))
            .build_with_flags(&flags)
            .unwrap();
        let owners: Vec<&str> = root.migrations().iter().map(|(id, _)| id.as_str()).collect();
        assert_eq!(owners, ["rigs"]);
    }
}
