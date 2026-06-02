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
//!   module owns and the five versioned event kinds it emits.
//! - **MCP tools** ([`GtModule::register_mcp_tools`]) — the ten `rig.*` validate/execute
//!   tools, named and described verbatim from the current `gt-mcp` service.
//! - **Migrations** ([`GtModule::migrations`]) — the `rigs` table, owned by the module.
//!
//! It does **not** override `register_routes`/`openapi`: a rig is managed over MCP only
//! (the orchestrator-state facet has no HTTP surface of its own — `gt-web` exposes only a
//! `rig` *filter* field on session DTOs, not rig CRUD), so the empty-router default stands.
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
/// unit struct is all the composition root registers.
#[derive(Clone, Copy, Debug, Default)]
pub struct RigsModule;

impl RigsModule {
    /// The module's stable id (`rig`). Singular, to match the existing event-kind,
    /// MCP-tool, and scope namespaces (see the module-level note). The literal is a
    /// known-valid slug.
    pub fn id() -> ModuleId {
        ModuleId::new("rig").expect("`rig` is a valid module id")
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
        // convention `gt-merge` and `gt-quota` already follow). The five emitted kinds mirror
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
            );
    }

    fn migrations(&self) -> Vec<Migration> {
        // The `rigs` table backing `RigRepository`. The module owns its schema (the SQL lives
        // in `migrations/rig/`); `gt-store-pg` keeps a transitional applied copy until
        // `hq-mod-migrate` consolidates module-owned migrations as the single source.
        vec![Migration::new(
            1,
            "create_rigs",
            include_str!("../migrations/rig/0001__create_rigs.sql"),
        )]
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
    fn capability_owns_rig_scopes_and_five_versioned_kinds() {
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
            ]
        );
        // Every declared kind is owned by this module (prefix == meta id).
        for k in cap.emits() {
            assert_eq!(k.module(), RigsModule::id().as_str());
        }
    }

    #[test]
    fn registers_the_ten_existing_rig_tools() {
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
            ]
        );
    }

    #[test]
    fn owns_the_rigs_table_migration() {
        let migs = RigsModule.migrations();
        assert_eq!(migs.len(), 1);
        assert_eq!(migs[0].version, 1);
        assert_eq!(migs[0].name, "create_rigs");
        // Schema-per-ws (hq-mt-data.3, docs/04 §15): the table is created in the
        // `ws_default` template schema so `gt_create_workspace_schema` clones it per
        // tenant — not in `public` (which holds only cross-tenant catalogs).
        assert!(migs[0].sql.contains("CREATE TABLE IF NOT EXISTS ws_default.rigs"));
        assert!(
            migs[0].sql.contains("CREATE SCHEMA IF NOT EXISTS ws_default"),
            "must bootstrap the template schema it populates",
        );
    }

    #[test]
    fn contributes_no_openapi_surface() {
        // Rigs are MCP-only; the default (no OpenAPI, empty router) stands.
        assert!(RigsModule.openapi().is_none());
    }
}
