//! `MergeModule` — the [`GtModule`] wrapper over the merge slot domain (`hq-mod-refactor.4`).
//!
//! Follows the reference shape `RigsModule` established in `hq-mod-refactor.1`: it routes
//! gt-merge's *existing* contributions through the kernel's module seam **without changing
//! any domain logic**. Every command, event, and reducer still lives in [`crate::commands`],
//! [`crate::events`], and [`crate::state`]; this file only *declares* what gt-merge already
//! offers so the `RootBuilder` can harvest it instead of the composition root hand-wiring
//! tools (`docs/03` rule 3).
//!
//! What it declares:
//!
//! - **Identity** ([`GtModule::meta`]) — id `merge`, semver, description.
//! - **Capability** ([`GtModule::capability`]) — the `merge.read` / `merge.write` scopes the
//!   module owns and the four versioned event kinds it emits.
//! - **MCP tools** ([`GtModule::register_mcp_tools`]) — the six `merge.*` validate/execute
//!   tools, named and described verbatim from the current `gt-mcp` service.
//!
//! It contributes **no migration**: the [`MergeBoard`](crate::MergeBoard) is event-sourced
//! and held in the actor's memory (no table backs it), so the empty-`migrations` default
//! stands. It also does **not** override `register_routes`/`openapi`: the merge slot is
//! managed over MCP only, so the empty-router default stands.
//!
//! ## Faithful-wrap notes (no logic change)
//!
//! 1. **Module id is `merge`, singular** — matching the existing `merge.*` event kinds, MCP
//!    tools, and scope resources, so `meta().id == event-kind module == MCP-tool namespace ==
//!    scope resource` with zero rename/drift.
//! 2. **Event kinds are declared kebab + `.v1`** (`merge.ready.v1`), the canonical forward
//!    shape — and here [`crate::MergeEvent::kind`] already emits that exact string (gt-merge
//!    was suffixed by `hq-mod-events.2`), so the declared and emitted contracts match. The
//!    MCP tool names are declared verbatim with their current segments; kebab-normalizing
//!    them is `hq-mod-mcp.4`'s concern when the builder's name rule runs.

use gt_module::{
    Capability, EventKind, GtModule, McpRegistry, Migration, ModuleId, ModuleMeta, Scope,
};
use semver::Version;

/// The [`GtModule`] facade over the merge slot domain. Zero-sized: the module owns no runtime
/// state of its own (the live board lives in the actor spawned by [`crate::actor`]), so the
/// unit struct is all the composition root registers.
#[derive(Clone, Copy, Debug, Default)]
pub struct MergeModule;

impl MergeModule {
    /// The module's stable id (`merge`). Singular, to match the existing event-kind, MCP-tool,
    /// and scope namespaces. The literal is a known-valid slug.
    pub fn id() -> ModuleId {
        ModuleId::new("merge").expect("`merge` is a valid module id")
    }
}

impl GtModule for MergeModule {
    fn meta(&self) -> ModuleMeta {
        ModuleMeta::new(
            Self::id(),
            "Merge",
            Version::new(1, 0, 0),
            "Merge slot board — submit a ready branch, then complete or fail the merge as the \
             composition-root edge runs the real git merge.",
        )
    }

    fn capability(&self) -> Capability {
        // The `merge.read` / `merge.write` scopes the module owns (the same `<resource>.<verb>`
        // convention gt-rig and gt-quota follow). The four emitted kinds mirror `MergeEvent`'s
        // variants in the canonical versioned + kebab shape.
        Capability::empty()
            .claiming_all([
                Scope::new("merge.read").expect("valid scope"),
                Scope::new("merge.write").expect("valid scope"),
            ])
            .emitting_all([
                EventKind::new("merge.ready.v1").expect("valid event kind"),
                EventKind::new("merge.started.v1").expect("valid event kind"),
                EventKind::new("merge.merged.v1").expect("valid event kind"),
                EventKind::new("merge.failed.v1").expect("valid event kind"),
            ])
    }

    fn register_mcp_tools(&self, registry: &mut McpRegistry) {
        // The six merge tools the `gt-mcp` service serves today, names + descriptions verbatim.
        // A `validate` (no state change) and an `execute` per command, in declaration order.
        registry
            .tool(
                "merge.submit.validate",
                "Check whether submitting a merge request would be accepted. No state change.",
            )
            .tool(
                "merge.submit.execute",
                "Register a new merge slot in Ready. Validates first inside the actor.",
            )
            .tool(
                "merge.complete.validate",
                "Check whether completing a merge (Merging -> Merged) would be accepted. No state change.",
            )
            .tool(
                "merge.complete.execute",
                "Mark a merge slot Merging -> Merged with the resulting sha.",
            )
            .tool(
                "merge.fail.validate",
                "Check whether failing a merge (Merging -> Failed) would be accepted. No state change.",
            )
            .tool(
                "merge.fail.execute",
                "Mark a merge slot Merging -> Failed with a reason.",
            );
    }

    fn migrations(&self) -> Vec<Migration> {
        // The merge board is event-sourced in the actor's memory — no table backs it — so the
        // module owns no schema. The empty default is the faithful declaration.
        Vec::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn meta_identity_is_singular_merge() {
        let m = MergeModule.meta();
        assert_eq!(m.id.as_str(), "merge");
        assert_eq!(m.version, Version::new(1, 0, 0));
    }

    #[test]
    fn capability_owns_merge_scopes_and_four_versioned_kinds() {
        let cap = MergeModule.capability();

        let scopes: Vec<&str> = cap.scopes().iter().map(Scope::as_str).collect();
        assert_eq!(scopes, ["merge.read", "merge.write"]);

        let kinds: Vec<&str> = cap.emits().iter().map(EventKind::as_str).collect();
        assert_eq!(
            kinds,
            [
                "merge.ready.v1",
                "merge.started.v1",
                "merge.merged.v1",
                "merge.failed.v1",
            ]
        );
        // Every declared kind is owned by this module (prefix == meta id).
        for k in cap.emits() {
            assert_eq!(k.module(), MergeModule::id().as_str());
        }
    }

    #[test]
    fn registers_the_six_existing_merge_tools() {
        let mut reg = McpRegistry::new();
        MergeModule.register_mcp_tools(&mut reg);
        let names: Vec<&str> = reg.tools().iter().map(|t| t.name.as_str()).collect();
        assert_eq!(
            names,
            [
                "merge.submit.validate",
                "merge.submit.execute",
                "merge.complete.validate",
                "merge.complete.execute",
                "merge.fail.validate",
                "merge.fail.execute",
            ]
        );
    }

    #[test]
    fn owns_no_migration_or_http_surface() {
        // The board is event-sourced (no table) and MCP-only: empty defaults stand.
        assert!(MergeModule.migrations().is_empty());
        assert!(MergeModule.openapi().is_none());
    }
}
