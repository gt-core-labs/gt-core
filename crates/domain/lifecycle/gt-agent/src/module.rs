//! `AgentModule` — the [`GtModule`] wrap over the agent/session lifecycle substrate
//! (`hq-mod-refactor.7`, re-scoped 2026-06-02).
//!
//! Originally `hq-mod-refactor.7` specified a `gt-crew` *CrewModule* aggregating this crate
//! with the five role crates, premised on "crew is a `SessionRole`". The ported domain
//! contradicts that: [`crate::SessionRole`] is `Mayor | Dog | Polecat` and crew is a
//! `Session` *attribute* ([`crate::Session::crew`]), deliberately **not** a role. A single
//! [`GtModule`] also cannot own six event-kind namespaces (the builder enforces
//! one-module-one-namespace). So the bead was re-scoped to this — the honest lifecycle wrap
//! of the session substrate alone — and roles-as-observers wiring moved to
//! `hq-mod-refactor.16`.
//!
//! This routes the agent domain's *existing* contributions through the kernel module seam
//! **without changing any domain logic** (the same faithful-wrap discipline as
//! `hq-mod-refactor.1`'s `RigsModule`). It lives in this `domain/lifecycle` crate: the
//! session substrate is consumed by the orchestration tier and the role observers, not a
//! user-facing board, so the wrap stays in the domain crate rather than the `modules/` tier.
//!
//! What it declares, and what is deferred (each additive, so the module keeps compiling):
//!
//! - **Identity** ([`GtModule::meta`]) — id `agent`, singular, matching the `agent.*`
//!   event-kind and `agent.*` MCP-tool namespaces (`meta().id == kind.module() ==
//!   tool-ns == scope-resource`, zero drift).
//! - **Capability** ([`GtModule::capability`]) — the `agent.read` / `agent.write` scopes
//!   (read = the sessions list the `/api/sessions` read-side serves; write = add/remove/
//!   transition), plus the four versioned event kinds mirroring [`crate::AgentEvent`].
//! - **MCP tools** ([`GtModule::register_mcp_tools`]) — the six `agent.*` validate/execute
//!   tools, one pair per [`crate::AgentCommand`] variant, named from
//!   [`crate::AgentCommand::tool_name`] verbatim.
//! - **No migration** — the sessions table is **Dolt-native** (`gt-store-dolt` implements
//!   [`crate::SessionWriter`]/[`crate::SessionQueries`]); there is no PG projection, exactly
//!   as `BeadsModule` (`hq-mod-refactor.2`) owns none. The empty-`migrations` default stands.
//! - **Routes deferred** — `/api/sessions` lives in the `gt-web` binary; lifting it into
//!   [`GtModule::register_routes`] is bins-decomposition work (`hq-mod-refactor.13`). The
//!   empty-router default stands until then.
//!
//! ## Event kinds are declared kebab + `.v1`
//!
//! [`crate::AgentEvent::kind`] emits the versioned + kebab-only form
//! (`agent.spawned.v1` / `agent.session-end.v1` / …), matching what this module's
//! capability declares. The emitted strings were the legacy bare/underscore shape until the
//! events-versioning follow-up to `hq-mod-events.2` (which had covered rig/quota/merge but
//! not agent) aligned them. Old log records keep their bare kind; the event-log NS filter is
//! the `agent.` prefix, so they still replay. The MCP tool names stay declared verbatim with
//! their current shape; kebab-normalization is `hq-mod-mcp.4`'s concern when the builder's
//! name rule runs.

use gt_module::{Capability, EventKind, GtModule, McpRegistry, ModuleId, ModuleMeta, Scope};
use semver::Version;

/// The [`GtModule`] facade over the agent/session lifecycle. Field-less on the MCP harvest path
/// (the live session registry is replayed from the event log per call, not held here), so the
/// default-constructed module is all the MCP harvest path registers.
///
/// To also serve the REST surface, the binary constructs it with [`with_http`](Self::with_http),
/// which bakes the event-log root into the router the trait's
/// [`register_routes`](GtModule::register_routes) returns (the trait takes `&self`, so an
/// implementor that needs runtime handles need not be zero-sized). Without the `axum` feature the
/// module is field-less and routes default to empty.
#[derive(Clone, Debug, Default)]
pub struct AgentModule {
    /// The REST state, present only when the binary opts the module into its HTTP surface via
    /// [`with_http`](Self::with_http). `None` (the default) keeps the empty-router default.
    #[cfg(feature = "axum")]
    http: Option<crate::http::AgentApiState>,
}

impl AgentModule {
    /// The module's stable id (`agent`). Singular, to match the existing `agent.*`
    /// event-kind, MCP-tool, and scope namespaces. The literal is a known-valid slug.
    pub fn id() -> ModuleId {
        ModuleId::new("agent").expect("`agent` is a valid module id")
    }

    /// Build a module that also serves the REST surface (`hq-fe-api-orch.1`), baking `state` (the
    /// event-log root) into the router [`register_routes`](GtModule::register_routes) returns. The
    /// binary calls this; the MCP harvest path uses [`default`](Self::default).
    #[cfg(feature = "axum")]
    pub fn with_http(state: crate::http::AgentApiState) -> Self {
        Self { http: Some(state) }
    }
}

impl GtModule for AgentModule {
    fn meta(&self) -> ModuleMeta {
        ModuleMeta::new(
            Self::id(),
            "Agent",
            Version::new(1, 0, 0),
            "Agent/session lifecycle substrate — spawn, heartbeat, transition, and tear down \
             the agent sessions (and the polecats they run in) the orchestrator supervises.",
        )
    }

    fn capability(&self) -> Capability {
        // The `agent.read` / `agent.write` scopes the module owns (the same `<resource>.<verb>`
        // convention `rig`/`merge`/`quota` follow). The four emitted kinds mirror
        // `AgentEvent`'s variants, declared in the canonical versioned + kebab shape.
        Capability::empty()
            .claiming_all([
                Scope::new("agent.read").expect("valid scope"),
                Scope::new("agent.write").expect("valid scope"),
            ])
            .emitting_all([
                EventKind::new("agent.spawned.v1").expect("valid event kind"),
                EventKind::new("agent.heartbeat.v1").expect("valid event kind"),
                EventKind::new("agent.session-end.v1").expect("valid event kind"),
                EventKind::new("agent.killed.v1").expect("valid event kind"),
                // Pause-in-place lifecycle (B2, gtcore-5731e9).
                EventKind::new("agent.paused.v1").expect("valid event kind"),
                EventKind::new("agent.resumed.v1").expect("valid event kind"),
            ])
    }

    fn register_mcp_tools(&self, registry: &mut McpRegistry) {
        // The six agent tools, one validate/execute pair per `AgentCommand` variant. Names
        // come from `AgentCommand::tool_name` (`agent.add`/`agent.remove`/`agent.transition`)
        // suffixed with the validate/execute convention, declared verbatim.
        registry
            .tool(
                "agent.add.validate",
                "Check whether spawning a new agent session would be accepted (id not already \
                 active). No state change.",
            )
            .tool(
                "agent.add.execute",
                "Spawn a new agent session on a rig. Emits agent.spawned.v1.",
            )
            .tool(
                "agent.remove.validate",
                "Check whether removing a session would be accepted (must exist). No state \
                 change.",
            )
            .tool(
                "agent.remove.execute",
                "Remove a session from the registry. Emits agent.session-end.v1.",
            )
            .tool(
                "agent.transition.validate",
                "Check whether a session lifecycle transition would be accepted by the state \
                 machine (legal target from the current state). No state change.",
            )
            .tool(
                "agent.transition.execute",
                "Transition a session to a new lifecycle state. Illegal transitions are \
                 rejected by the state machine.",
            );
    }

    /// The agent REST routes (`hq-fe-api-orch.1`), relative — the builder nests them under
    /// `/api/v1/agent` and applies the `agent.read`/`agent.write` scope guard. Present only when
    /// the module was built with [`with_http`](AgentModule::with_http); otherwise the empty
    /// default (MCP-only registration), so the MCP harvest path mounts no HTTP surface.
    #[cfg(feature = "axum")]
    fn register_routes(&self) -> axum::Router {
        match &self.http {
            Some(state) => crate::http::agent_router(state.clone()),
            None => axum::Router::new(),
        }
    }

    /// The OpenAPI spec for the agent REST routes, contributed only when the module carries the
    /// HTTP state (so the combined document documents exactly the routes actually mounted).
    #[cfg(feature = "axum")]
    fn openapi(&self) -> Option<utoipa::openapi::OpenApi> {
        use utoipa::OpenApi;
        self.http.as_ref().map(|_| crate::http::ApiDoc::openapi())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn meta_identity_is_singular_agent() {
        let m = AgentModule::default().meta();
        assert_eq!(m.id.as_str(), "agent");
        assert_eq!(m.version, Version::new(1, 0, 0));
    }

    #[test]
    fn capability_owns_agent_scopes_and_six_versioned_kinds() {
        let cap = AgentModule::default().capability();

        let scopes: Vec<&str> = cap.scopes().iter().map(Scope::as_str).collect();
        assert_eq!(scopes, ["agent.read", "agent.write"]);

        let kinds: Vec<&str> = cap.emits().iter().map(EventKind::as_str).collect();
        assert_eq!(
            kinds,
            [
                "agent.spawned.v1",
                "agent.heartbeat.v1",
                "agent.session-end.v1",
                "agent.killed.v1",
                "agent.paused.v1",
                "agent.resumed.v1",
            ]
        );
        // Every declared kind is owned by this module (kind prefix == meta id).
        for k in cap.emits() {
            assert_eq!(k.module(), AgentModule::id().as_str());
        }
    }

    #[test]
    fn registers_the_six_existing_agent_tools() {
        let mut reg = McpRegistry::new();
        AgentModule::default().register_mcp_tools(&mut reg);
        let names: Vec<&str> = reg.tools().iter().map(|t| t.name.as_str()).collect();
        assert_eq!(
            names,
            [
                "agent.add.validate",
                "agent.add.execute",
                "agent.remove.validate",
                "agent.remove.execute",
                "agent.transition.validate",
                "agent.transition.execute",
            ]
        );
    }

    #[test]
    fn owns_no_migration_sessions_are_dolt_native() {
        // The sessions table lives in Dolt (`gt-store-dolt` implements the write/query ports),
        // not a PG projection — so the wrap declares no migration, like `BeadsModule`.
        assert!(AgentModule::default().migrations().is_empty());
    }

    #[test]
    fn default_module_contributes_no_routes_or_openapi() {
        // The MCP harvest path uses the default module: no HTTP state ⇒ the empty-router /
        // no-OpenAPI defaults stand. The REST surface is opt-in via `with_http` (hq-fe-api-orch.1).
        assert!(AgentModule::default().openapi().is_none());
        assert!(AgentModule::default().migrations().is_empty());
    }

    #[cfg(feature = "axum")]
    #[test]
    fn with_http_contributes_routes_and_openapi() {
        // Built with HTTP state, the module documents its REST routes and returns a non-empty
        // OpenAPI spec the builder mounts under `/api/v1/agent`. (The router itself is exercised
        // by the http module's own tests + the contract test; here we assert the wiring flips on.)
        let m = AgentModule::with_http(crate::http::AgentApiState::new(
            std::sync::Arc::new(crate::http::FileAgentLog::new("/tmp/gt-agent-rest-test")),
        ));
        assert!(m.openapi().is_some());
    }
}
