//! `SkillsModule` — the [`GtModule`] wrap over the capability registry (`hq-mod-refactor.14`).
//!
//! A *skill* is a named capability whose enablement for a role **widens that role's gt-rbac
//! scope set** — so toggling a skill changes, with no redeploy, what every agent session
//! running that role may do. `gt-skills` is the event-sourced catalog + per-role bindings;
//! this file routes its *existing* contributions through the kernel module seam without
//! changing any domain logic (the same faithful-wrap discipline as `hq-mod-refactor.1`'s
//! `RigsModule`).
//!
//! The wrap lives in this `domain/platform` crate (not the `modules/` tier): skills are a
//! cross-cutting capability registry consumed by auth + roles, not a user-facing board.
//!
//! What it declares, and what is deferred (each additive, so the module keeps compiling):
//!
//! - **Identity** ([`GtModule::meta`]) — id `skills`, singular, matching the `skills.*`
//!   event-kind and `skills.{read,write}` scope namespaces (`meta().id == kind.module() ==
//!   scope-resource`, zero drift).
//! - **Capability** ([`GtModule::capability`]) — the `skills.read` / `skills.write` scopes the
//!   live `gt-web` routes already guard with (read hydrates the dashboard's role/skill lists;
//!   write is the per-role toggle), plus the four versioned event kinds mirroring
//!   [`crate::SkillEvent`].
//! - **No MCP tools** — skills are managed over REST (`/api/skills`, `/api/roles`, the toggle),
//!   not MCP, so [`GtModule::register_mcp_tools`] keeps its no-op default.
//! - **No migration** — the catalog is event-sourced (the [`crate::SkillEvent`] log rebuilds
//!   [`crate::SkillState`] via the actor's reducer); there is no projection table, exactly as
//!   `MergeModule` (`hq-mod-refactor.4`) owns none.
//! - **Routes deferred** — the `/api/skills` + `/api/roles` + toggle handlers still live in the
//!   `gt-web` binary; lifting them into [`GtModule::register_routes`] is bins-decomposition
//!   work (`hq-mod-refactor.13`), not this bead. The empty-router default stands until then.
//!
//! ## Event kinds are declared kebab + `.v1`
//!
//! [`crate::SkillEvent::kind`] still returns the legacy bare/underscore form
//! (`skills.enabled_for_role`); the kernel [`EventKind`] type is versioned + kebab-only by
//! construction, so the *declared* contract is the canonical forward shape
//! (`skills.enabled-for-role.v1`). Aligning the *emitted* strings to `.v1` is the cross-cutting
//! job of `hq-mod-events.2`, intentionally out of scope for the wrap.

#[cfg(feature = "axum")]
use gt_module::McpRegistry;
use gt_module::{Capability, EventKind, GtModule, ModuleId, ModuleMeta, Scope};
use semver::Version;

/// The [`GtModule`] facade over the capability registry. Zero-sized: the live catalog lives in
/// the actor spawned by [`crate::spawn`], so the unit struct is all the composition root
/// registers.
#[derive(Clone, Copy, Debug, Default)]
pub struct SkillsModule;

impl SkillsModule {
    /// The module's stable id (`skills`). Singular-namespace match with the `skills.*` event
    /// kinds and `skills.{read,write}` scopes. The literal is a known-valid slug.
    pub fn id() -> ModuleId {
        ModuleId::new("skills").expect("`skills` is a valid module id")
    }

    /// Build the HTTP-enabled skills module (`hq-web-extras.13`), baking `state` (the
    /// per-workspace catalog provider) into the router its
    /// [`register_routes`](GtModule::register_routes) returns. The binary calls this to opt the
    /// module into its read-only REST surface; the MCP harvest path keeps the plain unit
    /// [`SkillsModule`]. Returns a [`SkillsHttpModule`] rather than mutating `SkillsModule` so the
    /// unit struct (and its existing call sites) is left untouched.
    #[cfg(feature = "axum")]
    pub fn with_http(state: crate::http::SkillsApiState) -> SkillsHttpModule {
        SkillsHttpModule { http: state }
    }
}

/// The HTTP-enabled skills module (`hq-web-extras.13`): the same `GtModule` contract as
/// [`SkillsModule`] plus the read-only `skills.*` REST routes + OpenAPI spec.
///
/// Built by [`SkillsModule::with_http`]. Identity, capability, and MCP tools delegate verbatim to
/// [`SkillsModule`] (one source of truth for the contract); only
/// [`register_routes`](GtModule::register_routes) and [`openapi`](GtModule::openapi) are
/// overridden, carrying the per-workspace [`WorkspaceSkills`](crate::http::WorkspaceSkills)
/// provider the handlers read through.
#[cfg(feature = "axum")]
#[derive(Clone)]
pub struct SkillsHttpModule {
    /// The per-workspace REST state the routes read through.
    http: crate::http::SkillsApiState,
}

#[cfg(feature = "axum")]
impl GtModule for SkillsHttpModule {
    fn meta(&self) -> ModuleMeta {
        SkillsModule.meta()
    }

    fn capability(&self) -> Capability {
        SkillsModule.capability()
    }

    fn register_mcp_tools(&self, registry: &mut McpRegistry) {
        SkillsModule.register_mcp_tools(registry);
    }

    /// The read-only skills REST routes (`hq-web-extras.13`), relative — the builder nests them
    /// under `/api/v1/skills` and applies the `skills.read` scope guard (every route is a GET).
    fn register_routes(&self) -> axum::Router {
        crate::http::skills_router(self.http.clone())
    }

    /// The OpenAPI spec for the skills REST routes, so the combined document documents exactly the
    /// routes mounted under the HTTP-enabled module.
    fn openapi(&self) -> Option<utoipa::openapi::OpenApi> {
        use utoipa::OpenApi;
        Some(crate::http::ApiDoc::openapi())
    }
}

impl GtModule for SkillsModule {
    fn meta(&self) -> ModuleMeta {
        ModuleMeta::new(
            Self::id(),
            "Skills",
            Version::new(1, 0, 0),
            "Operator-tunable capability registry: a skill is a named capability whose \
             per-role enablement widens that role's RBAC scope set live, with no redeploy.",
        )
    }

    fn capability(&self) -> Capability {
        // The two scopes the live gt-web routes guard with (read = dashboard hydration,
        // write = per-role toggle). The four emitted kinds mirror `SkillEvent`'s variants,
        // declared in the canonical versioned + kebab shape.
        Capability::empty()
            .claiming_all([
                Scope::new("skills.read").expect("valid scope"),
                Scope::new("skills.write").expect("valid scope"),
            ])
            .emitting_all([
                EventKind::new("skills.registered.v1").expect("valid event kind"),
                EventKind::new("skills.retired.v1").expect("valid event kind"),
                EventKind::new("skills.enabled-for-role.v1").expect("valid event kind"),
                EventKind::new("skills.disabled-for-role.v1").expect("valid event kind"),
            ])
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn meta_identity_is_singular_skills() {
        let m = SkillsModule.meta();
        assert_eq!(m.id.as_str(), "skills");
        assert_eq!(m.version, Version::new(1, 0, 0));
    }

    #[test]
    fn capability_owns_skills_scopes_and_four_versioned_kinds() {
        let cap = SkillsModule.capability();

        let scopes: Vec<&str> = cap.scopes().iter().map(Scope::as_str).collect();
        assert_eq!(scopes, ["skills.read", "skills.write"]);

        let kinds: Vec<&str> = cap.emits().iter().map(EventKind::as_str).collect();
        assert_eq!(
            kinds,
            [
                "skills.registered.v1",
                "skills.retired.v1",
                "skills.enabled-for-role.v1",
                "skills.disabled-for-role.v1",
            ]
        );
        // Every declared kind is owned by this module (kind prefix == meta id).
        for k in cap.emits() {
            assert_eq!(k.module(), SkillsModule::id().as_str());
        }
    }

    #[test]
    fn registers_no_mcp_tools_rest_surface_not_mcp() {
        let mut reg = gt_module::McpRegistry::new();
        SkillsModule.register_mcp_tools(&mut reg);
        assert!(reg.tools().iter().next().is_none(), "the skills surface is REST, not MCP");
    }

    #[test]
    fn owns_no_migration_event_sourced_catalog() {
        // The catalog is the SkillEvent log replayed into SkillState; no projection table.
        assert!(SkillsModule.migrations().is_empty());
    }

    #[test]
    fn contributes_no_routes_until_gt_web_handlers_port() {
        // /api/skills + /api/roles + toggle live in gt-web today; porting them is
        // hq-mod-refactor.13. The default empty router + no OpenAPI stand until then.
        let _router = SkillsModule.register_routes();
        assert!(SkillsModule.openapi().is_none());
    }

    #[test]
    fn declared_kinds_cover_every_skill_event_variant() {
        // Anchor the declared contract to the domain: each SkillEvent variant's emitted kind
        // must appear verbatim in the module's declared set, so a new variant forces a
        // capability update. `kind()` now emits the canonical kebab `.v1` form directly, so
        // the comparison is exact — no normalization transform.
        use crate::SkillEvent;
        // Bring the domain's `EventKind::kind` method into scope (the gt-events trait),
        // distinct from gt_module's `EventKind` struct already imported above.
        use gt_events::EventKind as _;
        let cap = SkillsModule.capability();
        let declared: Vec<&str> = cap.emits().iter().map(EventKind::as_str).collect();
        for ev in [
            SkillEvent::Registered {
                skill: "s".into(),
                label: "l".into(),
                description: "d".into(),
                default_scopes: vec![],
                now_secs: 0,
            },
            SkillEvent::Retired { skill: "s".into(), now_secs: 0 },
            SkillEvent::EnabledForRole { role: "r".into(), skill: "s".into(), now_secs: 0 },
            SkillEvent::DisabledForRole { role: "r".into(), skill: "s".into(), now_secs: 0 },
        ] {
            let emitted = ev.kind();
            assert!(
                declared.contains(&emitted),
                "emitted kind not declared: {emitted}",
            );
        }
    }
}
