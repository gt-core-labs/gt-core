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
use crate::flags::{AllEnabled, FeatureFlags};
use crate::mcp::{McpRegistry, McpTool, McpToolNameError};
use crate::meta::{ModuleId, ModuleMeta};
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
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct ModuleEntry {
    pub(crate) meta: ModuleMeta,
    pub(crate) capability: Capability,
    pub(crate) depends_on: Vec<ModuleId>,
    /// MCP tools this module contributes, in declaration order (`hq-mod-mcp.1`).
    pub(crate) mcp_tools: Vec<McpTool>,
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
        Self::check_tool_namespacing(&enabled)?;
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
            BuildError::MalformedToolName { module, tool, reason } => {
                write!(f, "module {module} contributes malformed MCP tool `{tool}`: {reason}")
            }
            BuildError::ToolNamespaceMismatch { module, tool, prefix } => {
                write!(
                    f,
                    "module {module} contributes MCP tool `{tool}` under foreign namespace `{prefix}`"
                )
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

    /// Merge every module's contributed routes into one application
    /// [`Router`] (`hq-mod-routes.1`).
    ///
    /// Routers are merged in init order (each module after its dependencies), so
    /// the composition root obtains a fully wired router without naming a single
    /// route. With no modules — or only modules that contribute no HTTP surface —
    /// this is an empty router.
    ///
    /// Path namespacing (`/api/v1/<module>`, `hq-mod-routes.2`) and per-route
    /// scope guards (`hq-mod-routes.3`) hook into this merge as those beads land;
    /// today it is a straight [`Router::merge`](axum::Router::merge), so two
    /// modules declaring the same path collide exactly as axum specifies. Takes
    /// `self` because an `axum::Router` is consumed by merge.
    pub fn into_router(self) -> Router {
        self.routers
            .into_iter()
            .fold(Router::new(), |app, module_router| app.merge(module_router))
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

    // --- Router contribution (hq-mod-routes.1) ------------------------------

    use axum::body::Body;
    use axum::http::{Request, StatusCode};
    use axum::routing::get;
    use axum::Router;
    use tower::ServiceExt; // `oneshot`

    /// Test module that contributes one GET route at a configurable path,
    /// answering with its own id so the merged router proves which module served.
    struct Routed {
        id: &'static str,
        path: &'static str,
        deps: Vec<&'static str>,
    }

    impl Routed {
        fn new(id: &'static str, path: &'static str) -> Self {
            Routed { id, path, deps: Vec::new() }
        }
        fn with_dep(mut self, dep: &'static str) -> Self {
            self.deps.push(dep);
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
        fn register_routes(&self) -> Router {
            let id = self.id;
            Router::new().route(self.path, get(move || async move { id }))
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
    async fn module_route_is_reachable_through_root() {
        let root = RootBuilder::new().module(Routed::new("beads", "/beads")).build().unwrap();
        let (status, body) = hit(root.into_router(), "/beads").await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(body, "beads");
    }

    #[tokio::test]
    async fn routes_from_multiple_modules_all_merge() {
        let root = RootBuilder::new()
            .module(Routed::new("beads", "/beads"))
            .module(Routed::new("rigs", "/rigs"))
            .build()
            .unwrap();
        let app = root.into_router();
        assert_eq!(hit(app.clone(), "/beads").await, (StatusCode::OK, "beads".into()));
        assert_eq!(hit(app, "/rigs").await, (StatusCode::OK, "rigs".into()));
    }

    #[tokio::test]
    async fn module_with_no_routes_contributes_nothing() {
        // `Beads` does not override `register_routes`; the default empty router
        // means no path is served.
        let root = RootBuilder::new().module(Beads).build().unwrap();
        let (status, _) = hit(root.into_router(), "/anything").await;
        assert_eq!(status, StatusCode::NOT_FOUND);
    }

    #[tokio::test]
    async fn disabled_module_route_is_dropped() {
        let flags = DisabledModules::new([ModuleId::new("rigs").unwrap()]);
        let root = RootBuilder::new()
            .module(Routed::new("beads", "/beads"))
            .module(Routed::new("rigs", "/rigs"))
            .build_with_flags(&flags)
            .unwrap();
        let app = root.into_router();
        assert_eq!(hit(app.clone(), "/beads").await.0, StatusCode::OK);
        // The disabled module's route is absent.
        assert_eq!(hit(app, "/rigs").await.0, StatusCode::NOT_FOUND);
    }

    #[tokio::test]
    async fn routers_kept_aligned_with_modules_after_reorder() {
        // `merge` depends on `beads`, so build reorders it after `beads`. The
        // router-ordering must follow the same permutation: both routes resolve.
        let root = RootBuilder::new()
            .module(Routed::new("merge", "/merge").with_dep("beads"))
            .module(Routed::new("beads", "/beads"))
            .build()
            .unwrap();
        // Sanity: build reordered beads before merge.
        let ids: Vec<&str> = root.modules().map(|m| m.id.as_str()).collect();
        assert_eq!(ids, ["beads", "merge"]);
        let app = root.into_router();
        assert_eq!(hit(app.clone(), "/beads").await, (StatusCode::OK, "beads".into()));
        assert_eq!(hit(app, "/merge").await, (StatusCode::OK, "merge".into()));
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
}
