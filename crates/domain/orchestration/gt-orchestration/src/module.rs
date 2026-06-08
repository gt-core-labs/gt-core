//! `ConvoyModule` — the [`GtModule`] wrapper over the convoy domain (`hq-mod-refactor.6`).
//!
//! Like [`RigsModule`](../../../platform/gt-rig/src/module.rs) before it, this routes the
//! convoy aggregate's *existing* contributions through the kernel's module seam **without
//! changing any domain logic**. Every command, event, validation, and reducer still lives in
//! [`crate::commands`] / [`crate::events`] / [`crate::state`]; this file only *declares* what
//! `gt-orchestration` already offers so the `RootBuilder` can harvest it instead of the
//! composition root hand-wiring routes/tools (`docs/03` rule 3).
//!
//! What it declares:
//!
//! - **Identity** ([`GtModule::meta`]) — id `convoy`, semver, description.
//! - **Capability** ([`GtModule::capability`]) — the `convoy.read` / `convoy.write` scopes
//!   the module owns and the seven versioned event kinds it emits.
//! - **MCP tools** ([`GtModule::register_mcp_tools`]) — the six `orch.*` validate/execute
//!   tools, named verbatim from the current `gt-mcp` service.
//!
//! It does **not** override `register_routes`/`openapi`: the `convoys.*` HTTP handlers
//! (`GET /api/convoys`, create, fail-member) are still generic over `gt-web`'s application
//! state, so detangling them is `hq-mod-routes.5`'s job; the empty-router default stands.
//!
//! ## Faithful-wrap notes (no logic change)
//!
//! 1. **Module id is `convoy`** — matching the event-kind leaf namespace the convoy domain
//!    now uses (`hq-mod-events.8` aligned `OrchEvent::kind()` from the legacy `orch.*` family
//!    prefix to `convoy.*.v1`, the leaf shape rig/merge/quota already use). So
//!    `meta().id == EventKind::module()` for every declared kind — the invariant the rig
//!    reference set. The Rust type is still `OrchEvent`/`OrchCommand`; only the wire kinds are
//!    `convoy.*`.
//! 2. **Event kinds are declared kebab + `.v1`** (`convoy.member-dispatched.v1`); the emitted
//!    [`crate::OrchEvent::kind`] string is the underscore form (`convoy.member_dispatched.v1`),
//!    same split as the quota wrap — the kernel [`EventKind`] is kebab-only, so the declared
//!    contract is the canonical shape and the emitted underscore is aligned by a later bead.
//! 3. **MCP tools are in the `convoy.*` leaf namespace** (`convoy.launch.*`,
//!    `convoy.complete-member.*`, `convoy.fail-member.*`), so the tool namespace matches the
//!    module id `convoy` exactly as the event kinds do — `id == EventKind::module() ==
//!    MCP-tool namespace` all hold. `hq-mod-mcp.12` renamed them from the legacy `orch.*`
//!    form (`orch.launch_convoy` etc) across the `gt-mcp` service, `OrchCommand::tool_name`,
//!    and this declaration, completing the convoy domain's namespace coherence.
//! 4. **No migration is declared.** Unlike rig (which owns a `rigs` table SQL file), the
//!    convoy board has no module-owned migration in this crate — persistence is the
//!    `OrchRepository` Dolt adapter (`orch_repo.rs`), best-effort over the replayed
//!    [`crate::OrchState`]. Bringing that schema under the module (an owned migration) is left
//!    to the migration-consolidation work; declaring an empty set here changes no behaviour.

use gt_module::{
    Capability, EventKind, GtModule, McpRegistry, ModuleId, ModuleMeta, Scope,
};
use semver::Version;

/// The [`GtModule`] facade over the convoy domain. Zero-sized: the live board lives in the
/// actor spawned by [`crate::actor`], so the unit struct is all the composition root
/// registers.
#[derive(Clone, Copy, Debug, Default)]
pub struct ConvoyModule;

impl ConvoyModule {
    /// The module's stable id (`convoy`). Matches the event-kind leaf namespace
    /// (`convoy.*.v1`, see `hq-mod-events.8`) and the `convoys.*` scope resource. The literal
    /// is a known-valid slug.
    pub fn id() -> ModuleId {
        ModuleId::new("convoy").expect("`convoy` is a valid module id")
    }

    /// Build the HTTP-enabled convoy module (`hq-fe-api-orch.3`), baking `state` (the
    /// per-workspace event-log provider) into the router its
    /// [`register_routes`](GtModule::register_routes) returns. The binary calls this to opt the
    /// module into its REST surface; the MCP harvest path keeps the plain unit [`ConvoyModule`].
    /// Returns a [`ConvoyHttpModule`] rather than mutating [`ConvoyModule`] so the unit struct
    /// (and its `ConvoyModule.migrations()` call sites) is left untouched — the same shape
    /// `MergeModule::with_http` / `RigsModule::with_http` follow.
    #[cfg(feature = "axum")]
    pub fn with_http(state: crate::http::ConvoyApiState) -> ConvoyHttpModule {
        ConvoyHttpModule { http: state }
    }
}

/// The HTTP-enabled convoy module (`hq-fe-api-orch.3`): the same `GtModule` contract as
/// [`ConvoyModule`] plus the `convoy.*` REST routes + OpenAPI spec.
///
/// Built by [`ConvoyModule::with_http`]. Identity, capability, and MCP tools are delegated
/// verbatim to [`ConvoyModule`] (one source of truth for the board's contract); only
/// [`register_routes`](GtModule::register_routes) and [`openapi`](GtModule::openapi) are
/// overridden, carrying the per-workspace [`WorkspaceConvoy`](crate::WorkspaceConvoy) provider the
/// handlers dispatch through. Like the unit module it owns no migration (the board is
/// event-sourced), so the empty-`migrations` default stands.
#[cfg(feature = "axum")]
#[derive(Clone)]
pub struct ConvoyHttpModule {
    /// The per-workspace REST state the routes dispatch through.
    http: crate::http::ConvoyApiState,
}

#[cfg(feature = "axum")]
impl GtModule for ConvoyHttpModule {
    fn meta(&self) -> ModuleMeta {
        ConvoyModule.meta()
    }

    fn capability(&self) -> Capability {
        ConvoyModule.capability()
    }

    fn register_mcp_tools(&self, registry: &mut McpRegistry) {
        ConvoyModule.register_mcp_tools(registry);
    }

    /// The convoy REST routes (`hq-fe-api-orch.3`), relative — the builder nests them under
    /// `/api/v1/convoy` and applies the `convoy.read`/`convoy.write` scope guard.
    fn register_routes(&self) -> axum::Router {
        crate::http::convoy_router(self.http.clone())
    }

    /// The OpenAPI spec for the convoy REST routes, so the combined document documents exactly the
    /// routes mounted under the HTTP-enabled module.
    fn openapi(&self) -> Option<utoipa::openapi::OpenApi> {
        use utoipa::OpenApi;
        Some(crate::http::ApiDoc::openapi())
    }
}

impl GtModule for ConvoyModule {
    fn meta(&self) -> ModuleMeta {
        ModuleMeta::new(
            Self::id(),
            "Convoy",
            Version::new(1, 0, 0),
            "Convoy orchestration — drives an ordered set of member beads to completion: feeds \
             the next ready member on each handoff and closes when all members finish. The \
             aggregate the mayor/deacon drive and the crew executes.",
        )
    }

    fn capability(&self) -> Capability {
        // `convoy.read` / `convoy.write`: singular matches the MCP tool namespace (`convoy.*`)
        // so `from_workspace_claim` maps these REST scopes to the right allow pattern.
        Capability::empty()
            .claiming_all([
                Scope::new("convoy.read").expect("valid scope"),
                Scope::new("convoy.write").expect("valid scope"),
            ])
            .emitting_all([
                EventKind::new("convoy.created.v1").expect("valid event kind"),
                EventKind::new("convoy.launched.v1").expect("valid event kind"),
                EventKind::new("convoy.member-dispatched.v1").expect("valid event kind"),
                EventKind::new("convoy.member-completed.v1").expect("valid event kind"),
                EventKind::new("convoy.member-failed.v1").expect("valid event kind"),
                EventKind::new("convoy.closed.v1").expect("valid event kind"),
                EventKind::new("convoy.failed.v1").expect("valid event kind"),
            ])
    }

    fn register_mcp_tools(&self, registry: &mut McpRegistry) {
        // The six convoy tools the `gt-mcp` service serves, names + descriptions verbatim.
        // A `validate` (no state change) and an `execute` per command, in the `convoy.*` leaf
        // namespace matching the module id (note 3).
        registry
            .tool(
                "convoy.launch.validate",
                "Check whether launching a convoy with the given ordered members would be \
                 accepted (non-empty, member ids well-formed). No state change.",
            )
            .tool(
                "convoy.launch.execute",
                "Launch a convoy: register the ordered members and dispatch the first. Emits \
                 convoy.created + convoy.launched + the first convoy.member-dispatched.",
            )
            .tool(
                "convoy.complete-member.validate",
                "Check whether marking a convoy member complete would be accepted (convoy \
                 launched, member is the active one). No state change.",
            )
            .tool(
                "convoy.complete-member.execute",
                "Mark the active member complete and hand off to the next, or close the convoy \
                 when it was the last. Emits convoy.member-completed (+ the next \
                 convoy.member-dispatched, or convoy.closed).",
            )
            .tool(
                "convoy.fail-member.validate",
                "Check whether failing a convoy member would be accepted (convoy launched, \
                 member is the active one). No state change.",
            )
            .tool(
                "convoy.fail-member.execute",
                "Fail the active member and halt the convoy. Emits convoy.member-failed + \
                 convoy.failed.",
            );
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn meta_id_is_convoy() {
        let meta = ConvoyModule.meta();
        assert_eq!(meta.id.as_str(), "convoy");
        assert_eq!(meta.id, ConvoyModule::id());
        assert_eq!(meta.version, Version::new(1, 0, 0));
    }

    #[test]
    fn capability_claims_convoy_scopes_and_emits_seven_kinds() {
        let cap = ConvoyModule.capability();

        let scopes: Vec<&str> = cap.scopes().iter().map(Scope::as_str).collect();
        assert_eq!(scopes, ["convoy.read", "convoy.write"]);

        let kinds: Vec<&str> = cap.emits().iter().map(EventKind::as_str).collect();
        assert_eq!(
            kinds,
            [
                "convoy.created.v1",
                "convoy.launched.v1",
                "convoy.member-dispatched.v1",
                "convoy.member-completed.v1",
                "convoy.member-failed.v1",
                "convoy.closed.v1",
                "convoy.failed.v1",
            ]
        );
        // Every declared kind is owned by this module (kind prefix == meta id).
        for k in cap.emits() {
            assert_eq!(k.module(), ConvoyModule::id().as_str());
        }
    }

    #[test]
    fn registers_the_six_existing_convoy_tools() {
        let mut reg = McpRegistry::new();
        ConvoyModule.register_mcp_tools(&mut reg);
        let names: Vec<&str> = reg.tools().iter().map(|t| t.name.as_str()).collect();
        assert_eq!(
            names,
            [
                "convoy.launch.validate",
                "convoy.launch.execute",
                "convoy.complete-member.validate",
                "convoy.complete-member.execute",
                "convoy.fail-member.validate",
                "convoy.fail-member.execute",
            ]
        );
        // Coherence (hq-mod-mcp.12): every tool is in the module's own namespace —
        // `id == MCP-tool namespace`, the same invariant the event kinds already satisfy.
        for t in reg.tools() {
            let ns = t.name.split('.').next().unwrap();
            assert_eq!(ns, ConvoyModule::id().as_str(), "tool {} must be in the convoy namespace", t.name);
        }
    }

    #[test]
    fn owns_no_migration_and_no_route_surface() {
        // Persistence is the OrchRepository Dolt adapter, not a module-owned migration; the
        // convoys.* HTTP routes are still gt-web-coupled (harvested by hq-mod-routes.5).
        assert!(ConvoyModule.migrations().is_empty());
        assert!(ConvoyModule.openapi().is_none());
    }

    /// The HTTP-enabled variant (`hq-fe-api-orch.3`) documents its REST routes and returns a
    /// non-empty OpenAPI spec the builder mounts under `/api/v1/convoy`, while delegating the
    /// board contract (id, scopes, MCP tools, no migration) verbatim to [`ConvoyModule`]. The
    /// router itself is exercised by the http module's own tests + the contract test; here we
    /// assert the trait wiring flips on and the delegation holds.
    #[cfg(feature = "axum")]
    #[test]
    fn with_http_contributes_routes_and_delegates_contract() {
        use crate::events::OrchEvent;
        use crate::http::{ConvoyApiState, WorkspaceConvoy};
        use crate::state::ConvoyBoard;
        use gt_events::AppError;
        use std::sync::Arc;

        // A never-used provider is fine: `openapi()`/`meta()` read no state, and
        // `register_routes` only clones the state into the router without dispatching.
        struct NoopConvoy;
        #[async_trait::async_trait]
        impl WorkspaceConvoy for NoopConvoy {
            async fn board(&self, _ws: &str) -> Result<ConvoyBoard, AppError> {
                Err(AppError::Other("unused".into()))
            }
            async fn append(&self, _ws: &str, _events: Vec<OrchEvent>) -> Result<(), AppError> {
                Err(AppError::Other("unused".into()))
            }
        }

        let m = ConvoyModule::with_http(ConvoyApiState::new(Arc::new(NoopConvoy)));
        assert!(m.openapi().is_some(), "HTTP variant ships an OpenAPI spec");

        // Delegation: the HTTP variant carries the SAME contract as the unit module.
        assert_eq!(m.meta().id.as_str(), ConvoyModule.meta().id.as_str());
        assert_eq!(
            m.capability().scopes(),
            ConvoyModule.capability().scopes(),
            "scopes delegate to ConvoyModule"
        );
        let mut reg = McpRegistry::new();
        m.register_mcp_tools(&mut reg);
        assert_eq!(reg.tools().len(), 6, "the six convoy tools still register");
    }
}
