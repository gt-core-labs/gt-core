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
//! - **Capability** ([`GtModule::capability`]) — the `convoys.read` / `convoys.write` scopes
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
        // The `convoys.read` / `convoys.write` scopes the module owns (the guards `gt-web`
        // names on the convoy HTTP routes). The seven emitted kinds mirror `OrchEvent`'s
        // variants, declared in the canonical versioned + kebab shape (`convoy.*.v1`).
        Capability::empty()
            .claiming_all([
                Scope::new("convoys.read").expect("valid scope"),
                Scope::new("convoys.write").expect("valid scope"),
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
    fn capability_claims_convoys_scopes_and_emits_seven_kinds() {
        let cap = ConvoyModule.capability();

        let scopes: Vec<&str> = cap.scopes().iter().map(Scope::as_str).collect();
        assert_eq!(scopes, ["convoys.read", "convoys.write"]);

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
}
