//! `RigsModule` — the [`GtModule`] wrapper over the rig catalog (`hq-mod-refactor.1`).
//!
//! This is the **first** domain wrap of Phase 3 and the reference shape the rest of
//! `hq-mod-refactor.2..9` copy: it routes the rig catalog's *existing* contributions
//! through the kernel's module seam **without changing any domain logic**. Every
//! command, event, validation, and reducer still lives in [`crate::commands`],
//! [`crate::events`], and [`crate::state`]; this file only *declares* what gt-rig
//! already offers so the `RootBuilder` can harvest it instead of the composition root
//! hand-wiring routes/tools/migrations (`docs/03` rule 3).
//!
//! What it declares:
//!
//! - **Identity** ([`GtModule::meta`]) — id `rig`, semver, description.
//! - **Capability** ([`GtModule::capability`]) — the `rig.read` / `rig.write` scopes the
//!   module owns and the six versioned event kinds it emits.
//! - **MCP tools** ([`GtModule::register_mcp_tools`]) — the fourteen `rig.*` validate/execute
//!   tools, named and described verbatim from the current `gt-mcp` service.
//! - **Migrations** ([`GtModule::migrations`]) — the `rigs` table plus its `worktree_root`,
//!   `git_connection_ref`, and `semantic_tags` columns, owned by the module.
//!
//! - **HTTP routes + OpenAPI** (on the sibling [`RigsHttpModule`]) — under the off-by-default
//!   `axum` feature (`hq-fe-api-platform.2`), the platform sibling of the issues HTTP surface:
//!   the `rig.*` REST routes ([`crate::http`]) the builder mounts at `/api/v1/rig` behind the
//!   capability-derived scope guard, plus their utoipa spec. [`RigsModule::with_http`] builds the
//!   HTTP-enabled variant (carrying the per-workspace provider); the plain [`RigsModule`] keeps
//!   the empty-router / no-spec defaults (MCP-only). The catalog is per-tenant, so unlike the
//!   single-store issues surface the state is a [`WorkspaceRigs`](crate::WorkspaceRigs) provider
//!   that yields a workspace-scoped repo per request, resolving the tenant from the auth context,
//!   never the path. `RigsModule` stays a unit struct so existing `RigsModule.migrations()` call
//!   sites are untouched.
//!
//! ## Two faithful-wrap notes (no logic change)
//!
//! 1. **Module id is `rig`, singular** — even though the struct is `RigsModule`. Every
//!    contract that already exists is singular (`RigEvent::kind()` → `rig.*`, the
//!    `rig.*.{validate,execute}` MCP tools, [`crate::RigCommand::tool_name`]). Keeping the
//!    id singular makes `meta().id == event-kind module == MCP-tool namespace == scope
//!    resource`, so the wrap introduces zero rename/drift. The plural lives only in the
//!    Rust type name.
//! 2. **Event kinds are declared kebab + `.v1`** (`rig.prefix-changed.v1`), but
//!    [`crate::RigEvent::kind`] still returns the legacy bare, underscore form
//!    (`rig.prefix_changed`). The kernel [`EventKind`] type is versioned + kebab-only by
//!    construction, so the *declared contract* is the canonical forward shape; aligning the
//!    *emitted* string is the job of `hq-mod-events.2` (suffix `.v1`) and is intentionally
//!    out of scope here. The MCP tool names are declared verbatim with their current
//!    underscores; kebab-normalizing them is `hq-mod-mcp.4`'s concern when the builder's
//!    name rule actually runs.

use gt_module::{
    Capability, EventKind, GtModule, McpRegistry, Migration, ModuleId, ModuleMeta, Scope,
};
use semver::Version;

/// The [`GtModule`] facade over the rig catalog. Zero-sized: the module owns no runtime
/// state of its own (the live catalog lives in the actor spawned by [`crate::spawn`]), so the
/// unit struct is all the MCP harvest path registers.
///
/// To also serve the REST surface, the binary constructs it with [`with_http`](Self::with_http),
/// which returns a [`RigsHttpModule`] carrying the per-workspace
/// [`WorkspaceRigs`](crate::WorkspaceRigs) provider. That sibling delegates identity, capability,
/// MCP tools, and migrations back to this unit struct and *adds* the REST routes + OpenAPI spec —
/// so `RigsModule` stays a unit struct (existing `RigsModule.migrations()` call sites keep
/// working) while the HTTP-enabled variant carries the runtime handle the routes need.
#[derive(Clone, Copy, Debug, Default)]
pub struct RigsModule;

impl RigsModule {
    /// The module's stable id (`rig`). Singular, to match the existing event-kind,
    /// MCP-tool, and scope namespaces (see the module-level note). The literal is a
    /// known-valid slug.
    pub fn id() -> ModuleId {
        ModuleId::new("rig").expect("`rig` is a valid module id")
    }

    /// Build the HTTP-enabled rig module (`hq-fe-api-platform.2`), baking `state` (the
    /// per-workspace repository provider) into the router its
    /// [`register_routes`](GtModule::register_routes) returns. The binary calls this to opt the
    /// module into its REST surface; the MCP harvest path keeps the plain unit
    /// [`RigsModule`]. Returns a [`RigsHttpModule`] rather than mutating `RigsModule` so the
    /// unit struct (and its `RigsModule.migrations()` call sites) is left untouched.
    #[cfg(feature = "axum")]
    pub fn with_http(state: crate::http::RigApiState) -> RigsHttpModule {
        RigsHttpModule { http: state }
    }
}

/// The HTTP-enabled rig module (`hq-fe-api-platform.2`): the same `GtModule` contract as
/// [`RigsModule`] plus the `rig.*` REST routes + OpenAPI spec.
///
/// Built by [`RigsModule::with_http`]. Identity, capability, MCP tools, and migrations are
/// delegated verbatim to [`RigsModule`] (one source of truth for the catalog's contract); only
/// [`register_routes`](GtModule::register_routes) and [`openapi`](GtModule::openapi) are
/// overridden, carrying the per-workspace [`WorkspaceRigs`](crate::WorkspaceRigs) provider the
/// handlers dispatch through.
#[cfg(feature = "axum")]
#[derive(Clone)]
pub struct RigsHttpModule {
    /// The per-workspace REST state the routes dispatch through.
    http: crate::http::RigApiState,
}

#[cfg(feature = "axum")]
impl GtModule for RigsHttpModule {
    fn meta(&self) -> ModuleMeta {
        RigsModule.meta()
    }

    fn capability(&self) -> Capability {
        RigsModule.capability()
    }

    fn register_mcp_tools(&self, registry: &mut McpRegistry) {
        RigsModule.register_mcp_tools(registry);
    }

    fn migrations(&self) -> Vec<Migration> {
        RigsModule.migrations()
    }

    /// The rig REST routes (`hq-fe-api-platform.2`), relative — the builder nests them under
    /// `/api/v1/rig` and applies the `rig.read`/`rig.write` scope guard.
    fn register_routes(&self) -> axum::Router {
        crate::http::rig_router(self.http.clone())
    }

    /// The OpenAPI spec for the rig REST routes, so the combined document documents exactly the
    /// routes mounted under the HTTP-enabled module.
    fn openapi(&self) -> Option<utoipa::openapi::OpenApi> {
        use utoipa::OpenApi;
        Some(crate::http::ApiDoc::openapi())
    }
}

impl GtModule for RigsModule {
    fn meta(&self) -> ModuleMeta {
        ModuleMeta::new(
            Self::id(),
            "Rigs",
            Version::new(1, 0, 0),
            "Rig catalog as orchestrator state — register/adopt/remove managed repositories \
             and edit their beads prefix and default branch.",
        )
    }

    fn capability(&self) -> Capability {
        // The `rig.read` / `rig.write` scopes the module owns (the same `<resource>.<verb>`
        // convention `gt-merge` and `gt-quota` already follow). The six emitted kinds mirror
        // `RigEvent`'s variants, declared in the canonical versioned + kebab shape.
        Capability::empty()
            .claiming_all([
                Scope::new("rig.read").expect("valid scope"),
                Scope::new("rig.write").expect("valid scope"),
            ])
            .emitting_all([
                EventKind::new("rig.added.v1").expect("valid event kind"),
                EventKind::new("rig.adopted.v1").expect("valid event kind"),
                EventKind::new("rig.removed.v1").expect("valid event kind"),
                EventKind::new("rig.prefix-changed.v1").expect("valid event kind"),
                EventKind::new("rig.default-branch-changed.v1").expect("valid event kind"),
                EventKind::new("rig.worktree-root-changed.v1").expect("valid event kind"),
                EventKind::new("rig.semantic-tags-changed.v1").expect("valid event kind"),
            ])
    }

    fn register_mcp_tools(&self, registry: &mut McpRegistry) {
        // The ten rig tools the `gt-mcp` service serves today, names + descriptions verbatim.
        // A `validate` (no state change) and an `execute` per command, in declaration order.
        registry
            .tool(
                "rig.add.validate",
                "Check whether registering a new rig would be accepted (name/prefix grammar, \
                 no name/prefix collision). No state change.",
            )
            .tool(
                "rig.add.execute",
                "Register a new rig in the catalog (orchestrator state only; the on-disk clone \
                 is a deploy-edge step). Emits rig.added.",
            )
            .tool(
                "rig.adopt.validate",
                "Check whether adopting an existing on-disk rig directory would be accepted. \
                 Same validation as rig.add. No state change.",
            )
            .tool(
                "rig.adopt.execute",
                "Adopt an existing on-disk rig into the catalog without re-cloning. Emits \
                 rig.adopted.",
            )
            .tool(
                "rig.remove.validate",
                "Check whether removing a rig from the catalog would be accepted (must exist). \
                 No state change.",
            )
            .tool(
                "rig.remove.execute",
                "Drop a rig from the catalog (orchestrator loses routing authority; on-disk \
                 teardown is a deploy-edge step). Emits rig.removed.",
            )
            .tool(
                "rig.set-prefix.validate",
                "Check whether changing a rig's beads prefix would be accepted (grammar, no \
                 collision, not a no-op). No state change.",
            )
            .tool(
                "rig.set-prefix.execute",
                "Change a rig's beads prefix (the matching bd config set issue_prefix is a \
                 deploy-edge side-effect). Emits rig.prefix_changed.",
            )
            .tool(
                "rig.set-default-branch.validate",
                "Check whether changing a rig's default branch would be accepted (non-empty, \
                 not a no-op). No state change.",
            )
            .tool(
                "rig.set-default-branch.execute",
                "Change the default branch tracked for a rig. Emits rig.default_branch_changed.",
            )
            .tool(
                "rig.set-worktree-root.validate",
                "Check whether pinning a rig's worktree-root override would be accepted \
                 (absolute path, no `..`, length <= 256, not a no-op). No state change.",
            )
            .tool(
                "rig.set-worktree-root.execute",
                "Pin the absolute worktree root the orchestrator carves a rig's polecat \
                 checkouts under (the filesystem move is a deploy-edge side-effect). Emits \
                 rig.worktree_root_changed.",
            )
            .tool(
                "rig.set-semantic-tags.validate",
                "Check whether replacing a rig's semantic capability tags would be accepted \
                 (each tag alphanumeric + hyphens, <= 40 chars, <= 32 tags, not a no-op). No \
                 state change.",
            )
            .tool(
                "rig.set-semantic-tags.execute",
                "Replace the semantic capability tags a rig advertises (e.g. rust, backend, \
                 frontend) for capability-based peer selection in a2a.discover and the Agent \
                 Card skills. Input is normalized (trim/lowercase/dedupe). Emits \
                 rig.semantic_tags_changed.",
            );
    }

    fn migrations(&self) -> Vec<Migration> {
        // The `rigs` table backing `RigRepository`. The module owns its schema (the SQL lives
        // in `migrations/rig/`); `gt-store-pg` keeps a transitional applied copy until
        // `hq-mod-migrate` consolidates module-owned migrations as the single source.
        vec![
            Migration::new(
                1,
                "create_rigs",
                include_str!("../migrations/rig/0001__create_rigs.sql"),
            ),
            Migration::new(
                2,
                "add_worktree_root",
                include_str!("../migrations/rig/0002__add_worktree_root.sql"),
            ),
            Migration::new(
                3,
                "add_git_connection_ref",
                include_str!("../migrations/rig/0003__add_git_connection_ref.sql"),
            ),
            Migration::new(
                4,
                "add_semantic_tags",
                include_str!("../migrations/rig/0004__add_semantic_tags.sql"),
            ),
        ]
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn meta_identity_is_singular_rig() {
        let m = RigsModule.meta();
        assert_eq!(m.id.as_str(), "rig");
        assert_eq!(m.version, Version::new(1, 0, 0));
    }

    #[test]
    fn capability_owns_rig_scopes_and_seven_versioned_kinds() {
        let cap = RigsModule.capability();

        let scopes: Vec<&str> = cap.scopes().iter().map(Scope::as_str).collect();
        assert_eq!(scopes, ["rig.read", "rig.write"]);

        let kinds: Vec<&str> = cap.emits().iter().map(EventKind::as_str).collect();
        assert_eq!(
            kinds,
            [
                "rig.added.v1",
                "rig.adopted.v1",
                "rig.removed.v1",
                "rig.prefix-changed.v1",
                "rig.default-branch-changed.v1",
                "rig.worktree-root-changed.v1",
                "rig.semantic-tags-changed.v1",
            ]
        );
        // Every declared kind is owned by this module (prefix == meta id).
        for k in cap.emits() {
            assert_eq!(k.module(), RigsModule::id().as_str());
        }
    }

    #[test]
    fn registers_the_fourteen_existing_rig_tools() {
        let mut reg = McpRegistry::new();
        RigsModule.register_mcp_tools(&mut reg);
        let names: Vec<&str> = reg.tools().iter().map(|t| t.name.as_str()).collect();
        assert_eq!(
            names,
            [
                "rig.add.validate",
                "rig.add.execute",
                "rig.adopt.validate",
                "rig.adopt.execute",
                "rig.remove.validate",
                "rig.remove.execute",
                "rig.set-prefix.validate",
                "rig.set-prefix.execute",
                "rig.set-default-branch.validate",
                "rig.set-default-branch.execute",
                "rig.set-worktree-root.validate",
                "rig.set-worktree-root.execute",
                "rig.set-semantic-tags.validate",
                "rig.set-semantic-tags.execute",
            ]
        );
    }

    #[test]
    fn owns_the_rigs_table_migration() {
        let migs = RigsModule.migrations();
        assert_eq!(migs.len(), 4);
        assert_eq!(migs[0].version, 1);
        assert_eq!(migs[0].name, "create_rigs");
        // The worktree_root column override (hq-mt-rigs.5) is a follow-on migration, never
        // an edit of the applied 0001 (sqlx checksum-validates).
        assert_eq!(migs[1].version, 2);
        assert_eq!(migs[1].name, "add_worktree_root");
        assert!(migs[1]
            .sql
            .contains("ADD COLUMN IF NOT EXISTS worktree_root"));
        // The git_connection_ref column (hq-vcs-connections.3) links a rig to the VCS
        // connection it clones with — another follow-on migration on the same template.
        assert_eq!(migs[2].version, 3);
        assert_eq!(migs[2].name, "add_git_connection_ref");
        assert!(migs[2]
            .sql
            .contains("ADD COLUMN IF NOT EXISTS git_connection_ref"));
        // The semantic_tags column (B3, gtcore-dd3763) carries the rig's capability tags for
        // capability-based peer selection — another follow-on migration on the same template.
        assert_eq!(migs[3].version, 4);
        assert_eq!(migs[3].name, "add_semantic_tags");
        assert!(migs[3]
            .sql
            .contains("ADD COLUMN IF NOT EXISTS semantic_tags"));
        // Schema-per-ws (hq-mt-data.3, docs/04 §15): the table is created in the
        // `ws_default` template schema so `gt_create_workspace_schema` clones it per
        // tenant — not in `public` (which holds only cross-tenant catalogs).
        assert!(migs[0]
            .sql
            .contains("CREATE TABLE IF NOT EXISTS ws_default.rigs"));
        assert!(
            migs[0]
                .sql
                .contains("CREATE SCHEMA IF NOT EXISTS ws_default"),
            "must bootstrap the template schema it populates",
        );
    }

    #[test]
    fn contributes_no_openapi_surface() {
        // The plain unit module is MCP-only; the default (no OpenAPI, empty router) stands.
        assert!(RigsModule.openapi().is_none());
    }

    /// The HTTP-enabled variant (`hq-fe-api-platform.2`) documents its REST routes and returns a
    /// non-empty OpenAPI spec the builder mounts under `/api/v1/rig`, while delegating the
    /// catalog contract (id, scopes, MCP tools, migrations) verbatim to [`RigsModule`]. The
    /// router itself is exercised by the http module's own tests + the contract test; here we
    /// assert the trait wiring flips on and the delegation holds.
    #[cfg(feature = "axum")]
    #[test]
    fn with_http_contributes_routes_and_delegates_contract() {
        use crate::http::{DynRigRepository, RigApiState, WorkspaceRigs};
        use gt_events::AppError;
        use std::sync::Arc;

        // A never-used provider is fine: `openapi()`/`meta()` read no state, and
        // `register_routes` only clones the state into the router without dispatching.
        struct NoopRigs;
        #[async_trait::async_trait]
        impl WorkspaceRigs for NoopRigs {
            async fn repo(&self, _ws: &str) -> Result<Box<dyn DynRigRepository>, AppError> {
                Err(AppError::Other("unused".into()))
            }
        }

        let m = RigsModule::with_http(RigApiState::new(Arc::new(NoopRigs)));
        assert!(m.openapi().is_some(), "HTTP variant ships an OpenAPI spec");

        // Delegation: the HTTP variant carries the SAME contract as the unit module.
        assert_eq!(m.meta().id.as_str(), RigsModule.meta().id.as_str());
        assert_eq!(
            m.capability().scopes(),
            RigsModule.capability().scopes(),
            "scopes delegate to RigsModule"
        );
        assert_eq!(
            m.migrations().len(),
            RigsModule.migrations().len(),
            "migrations delegate to RigsModule"
        );
        let mut reg = McpRegistry::new();
        m.register_mcp_tools(&mut reg);
        assert_eq!(reg.tools().len(), 14, "the fourteen rig tools still register");
    }
}
