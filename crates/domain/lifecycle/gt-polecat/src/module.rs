//! `SessionsModule` — the [`GtModule`] wrapper over the sessions surface (`hq-mod-refactor.3`).
//!
//! Like [`RigsModule`](../../../platform/gt-rig/src/module.rs) before it, this routes the
//! sessions surface's *existing* contribution through the kernel's module seam **without
//! changing any domain logic**. The lifecycle driver still lives in [`crate::lifecycle`] /
//! [`crate::supervisor`] / [`crate::tmux`] (launch, heartbeat, backoff-restart) and the
//! session projection in `gt-agent`; this file only *declares* what the sessions surface
//! already offers so the `RootBuilder` can harvest it instead of the composition root
//! hand-wiring scopes (`docs/03` rule 3).
//!
//! What it declares:
//!
//! - **Identity** ([`GtModule::meta`]) — id `sessions`, semver, description.
//! - **Capability** ([`GtModule::capability`]) — the `sessions.read` / `sessions.write`
//!   scopes the module owns (the read guard on `GET /api/sessions` and the write guard on
//!   the `:id` delete / interrupt / restart operator e-stops).
//!
//! ## Boundary with `CrewModule` (refactor.7) — whose are the `agent.*` events?
//!
//! `gt-polecat` *drives* a session but emits [`gt_agent::AgentEvent`]
//! (`agent.spawned`/`agent.heartbeat`/`agent.session_end`/`agent.killed`): those kinds, the
//! `agent.*` commands, and their MCP tools are **`gt-agent`'s contract**, wrapped by
//! `CrewModule` (`hq-mod-refactor.7`). So `SessionsModule` declares **no emitted kinds** —
//! it would otherwise double-own a namespace whose prefix (`agent`) isn't its id, breaking
//! the faithful-wrap rule (`meta.id == event-kind module == MCP-tool namespace == scope
//! resource`). The driver produces crew's events; the sessions module owns only the
//! `sessions.*` read/control surface over the resulting projection.
//!
//! ## Why this wrap is thin (no logic change)
//!
//! - **no emitted kinds** — see above (the lifecycle events are crew's).
//! - **no MCP tools** — the session controls (delete/interrupt/restart) are HTTP operator
//!   actions, not MCP commands; there are no `sessions.*` tools today.
//! - **no migrations** — the session list is an in-memory projection (`gt_agent`'s
//!   `InMemorySessions`), rebuilt by replay; the module owns no table.
//! - **no routes here** — the `sessions.read`/`sessions.write` handlers are still generic
//!   over `gt-web`'s application state (they reach the running root + the `PolecatControl`
//!   port). Detangling them into [`register_routes`](GtModule::register_routes) is
//!   `hq-mod-routes.5`'s job; moving them here would change logic, which this faithful wrap
//!   must not. The empty-router default stands until then — the `sessions.*` scopes this
//!   module declares are what those guards already name. (The `:id/term` route is
//!   `terminal.attach`, owned by `TerminalModule`/`hq-mod-refactor.9`, not here.)

use gt_module::{Capability, GtModule, ModuleId, ModuleMeta, Scope};
use semver::Version;

/// The [`GtModule`] facade over the sessions surface. Zero-sized: the live sessions are the
/// `gt-agent` projection folded by the bin's event loop and the polecats driven by the
/// supervisor actor, so the unit struct is all the composition root registers.
#[derive(Clone, Copy, Debug, Default)]
pub struct SessionsModule;

impl SessionsModule {
    /// The module's stable id (`sessions`). Matches the existing `sessions.read`/
    /// `sessions.write` scope resource and the `/api/sessions` route namespace, so the wrap
    /// introduces zero rename/drift. The literal is a known-valid slug.
    pub fn id() -> ModuleId {
        ModuleId::new("sessions").expect("`sessions` is a valid module id")
    }
}

impl GtModule for SessionsModule {
    fn meta(&self) -> ModuleMeta {
        ModuleMeta::new(
            Self::id(),
            "Sessions",
            Version::new(1, 0, 0),
            "Polecat session lifecycle + read surface — launch/heartbeat/backoff-restart the \
             driven processes and expose the running sessions (`sessions.read`) plus the \
             operator e-stops kill/interrupt/restart (`sessions.write`). The lifecycle events \
             are crew's (`agent.*`); this module owns the `sessions.*` control/read surface.",
        )
    }

    fn capability(&self) -> Capability {
        // The `sessions.read` / `sessions.write` scopes the module owns — the guards `gt-web`
        // already names on `GET /api/sessions` (read) and the `:id` delete/interrupt/restart
        // operator actions (write). No `emitting`: the lifecycle events are `agent.*`, owned
        // by `CrewModule` (see the module-level boundary note).
        Capability::empty().claiming_all([
            Scope::new("sessions.read").expect("valid scope"),
            Scope::new("sessions.write").expect("valid scope"),
        ])
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use gt_module::{EventKind, McpRegistry};

    #[test]
    fn meta_id_is_sessions() {
        let meta = SessionsModule.meta();
        assert_eq!(meta.id.as_str(), "sessions");
        assert_eq!(meta.id, SessionsModule::id());
        assert_eq!(meta.version, Version::new(1, 0, 0));
    }

    #[test]
    fn capability_claims_sessions_read_and_write() {
        let cap = SessionsModule.capability();
        let scopes: Vec<&str> = cap.scopes().iter().map(Scope::as_str).collect();
        assert_eq!(scopes, ["sessions.read", "sessions.write"]);
    }

    #[test]
    fn emits_nothing_agent_events_are_crews() {
        // The driver produces `agent.*` events, but those belong to CrewModule (refactor.7);
        // this module declares none of its own.
        let cap = SessionsModule.capability();
        let kinds: Vec<&EventKind> = cap.emits().iter().collect();
        assert!(kinds.is_empty());
    }

    #[test]
    fn registers_no_mcp_tools() {
        // Session controls are HTTP operator actions, not MCP commands.
        let mut reg = McpRegistry::new();
        SessionsModule.register_mcp_tools(&mut reg);
        assert!(reg.tools().is_empty());
    }

    #[test]
    fn owns_no_migration() {
        // In-memory projection (gt_agent::InMemorySessions), rebuilt by replay.
        assert!(SessionsModule.migrations().is_empty());
    }

    #[test]
    fn contributes_no_route_or_openapi_surface() {
        // The sessions surface is still gt-web-state-coupled; harvesting it is
        // hq-mod-routes.5. The default empty router + no OpenAPI stand here.
        assert!(SessionsModule.openapi().is_none());
    }
}
