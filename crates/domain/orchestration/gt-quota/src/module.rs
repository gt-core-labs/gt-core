//! `QuotaModule` — the [`GtModule`] wrapper over the quota domain (`hq-mod-refactor.5`).
//!
//! Follows the reference shape `RigsModule` established in `hq-mod-refactor.1`: it routes
//! gt-quota's *existing* contributions through the kernel's module seam **without changing
//! any domain logic**. Every command, event, and reducer still lives in [`crate::commands`],
//! [`crate::events`], and [`crate::state`]; this file only *declares* what gt-quota already
//! offers so the `RootBuilder` can harvest it instead of the composition root hand-wiring
//! tools/migrations (`docs/03` rule 3).
//!
//! What it declares:
//!
//! - **Identity** ([`GtModule::meta`]) — id `quota`, semver, description.
//! - **Capability** ([`GtModule::capability`]) — the `quota.read` / `quota.write` scopes the
//!   module owns and the seven versioned event kinds it emits.
//! - **MCP tools** ([`GtModule::register_mcp_tools`]) — the ten `quota.*` validate/execute
//!   tools, named and described verbatim from the current `gt-mcp` service.
//! - **Migrations** ([`GtModule::migrations`]) — the `accounts` + `token_usage` tables backing
//!   [`QuotaRepository`](crate::QuotaRepository).
//!
//! It does **not** override `register_routes`/`openapi`: quota is managed over MCP only, so
//! the empty-router default stands.
//!
//! ## Faithful-wrap notes (no logic change)
//!
//! 1. **Module id is `quota`, singular** — matching the existing `quota.*` event kinds, MCP
//!    tools, and scope resources, so `meta().id == event-kind module == MCP-tool namespace ==
//!    scope resource` with zero rename/drift.
//! 2. **Event kinds are declared kebab + `.v1`** (`quota.tokens-sampled.v1`), the canonical
//!    forward shape. [`crate::QuotaEvent::kind`] still emits the legacy underscore form
//!    (`quota.tokens_sampled.v1`); aligning the emitted string to kebab is `hq-mod-events`'s
//!    concern — the declared contract here is the canonical shape (the kernel `EventKind` type
//!    is kebab-only by construction). MCP tool names are declared verbatim with their current
//!    segments; kebab-normalizing them is `hq-mod-mcp.4`'s job.
//! 3. **Migration is the module-owned copy** at `migrations/quota/0001__create_quota.sql`,
//!    verbatim from gt-store-pg's `init_quota`; gt-store-pg keeps the transitional applied copy
//!    until `hq-mod-migrate` consolidates. `register`/`retire` write this table (they are "not
//!    event-logged"), so the module must own its schema.

use gt_module::{
    Capability, EventKind, GtModule, McpRegistry, Migration, ModuleId, ModuleMeta, Scope,
};
use semver::Version;

/// The [`GtModule`] facade over the quota domain. Zero-sized: the module owns no runtime state
/// of its own (the live window lives in the actor spawned by [`crate::actor`]), so the unit
/// struct is all the composition root registers.
#[derive(Clone, Copy, Debug, Default)]
pub struct QuotaModule;

impl QuotaModule {
    /// The module's stable id (`quota`). Singular, to match the existing event-kind, MCP-tool,
    /// and scope namespaces. The literal is a known-valid slug.
    pub fn id() -> ModuleId {
        ModuleId::new("quota").expect("`quota` is a valid module id")
    }
}

impl GtModule for QuotaModule {
    fn meta(&self) -> ModuleMeta {
        ModuleMeta::new(
            Self::id(),
            "Quota",
            Version::new(1, 0, 0),
            "Per-account token quota — sample usage, probe provider rate limits, predict \
             blocks, and rotate off a limited account onto a healthy one.",
        )
    }

    fn capability(&self) -> Capability {
        // The `quota.read` / `quota.write` scopes the module owns (the same `<resource>.<verb>`
        // convention gt-rig and gt-merge follow). The seven emitted kinds mirror `QuotaEvent`'s
        // variants in the canonical versioned + kebab shape.
        Capability::empty()
            .claiming_all([
                Scope::new("quota.read").expect("valid scope"),
                Scope::new("quota.write").expect("valid scope"),
            ])
            .emitting_all([
                EventKind::new("quota.tokens-sampled.v1").expect("valid event kind"),
                EventKind::new("quota.usage-probed.v1").expect("valid event kind"),
                EventKind::new("quota.window-reset.v1").expect("valid event kind"),
                EventKind::new("quota.block-predicted.v1").expect("valid event kind"),
                EventKind::new("quota.account-limited.v1").expect("valid event kind"),
                EventKind::new("quota.rotated.v1").expect("valid event kind"),
                EventKind::new("quota.blocked.v1").expect("valid event kind"),
            ])
    }

    fn register_mcp_tools(&self, registry: &mut McpRegistry) {
        // The ten quota tools the `gt-mcp` service serves today, names + descriptions verbatim.
        // A `validate` (no state change) and an `execute` per command, in declaration order.
        registry
            .tool(
                "quota.sample.validate",
                "Check whether recording a token usage sample would be accepted. No state change.",
            )
            .tool(
                "quota.sample.execute",
                "Record a per-session token usage sample; feeds consumption + rate EWMA. Emits quota.tokens_sampled.",
            )
            .tool(
                "quota.probe.validate",
                "Check whether reconciling against provider rate-limit headers would be accepted. No state change.",
            )
            .tool(
                "quota.probe.execute",
                "Reconcile the live window against provider remaining/resets. Emits quota.usage_probed.",
            )
            .tool(
                "quota.rotate.validate",
                "Check whether rotating off an account would be accepted. No state change.",
            )
            .tool(
                "quota.rotate.execute",
                "Rotate off an account onto a healthy one; parks the source in cooldown. Emits quota.rotated.",
            )
            .tool(
                "quota.register.validate",
                "Check whether registering a quota account would be accepted. No state change.",
            )
            .tool(
                "quota.register.execute",
                "Register (or replace) a quota account with a live window so sample/probe/rotate can act on it. Not event-logged.",
            )
            .tool(
                "quota.retire.validate",
                "Check whether retiring a quota account would be accepted (non-empty id). No state change.",
            )
            .tool(
                "quota.retire.execute",
                "Drop an account from the quota registry. Returns `removed=true` when the id existed, `removed=false` otherwise (idempotent). Not event-logged.",
            );
    }

    fn migrations(&self) -> Vec<Migration> {
        // The `accounts` + `token_usage` tables backing `QuotaRepository`. The module owns its
        // schema (SQL at `migrations/quota/`); gt-store-pg keeps the transitional applied copy
        // until `hq-mod-migrate` consolidates module-owned migrations as the single source.
        vec![Migration::new(
            1,
            "create_quota",
            include_str!("../migrations/quota/0001__create_quota.sql"),
        )]
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn meta_identity_is_singular_quota() {
        let m = QuotaModule.meta();
        assert_eq!(m.id.as_str(), "quota");
        assert_eq!(m.version, Version::new(1, 0, 0));
    }

    #[test]
    fn capability_owns_quota_scopes_and_seven_versioned_kinds() {
        let cap = QuotaModule.capability();

        let scopes: Vec<&str> = cap.scopes().iter().map(Scope::as_str).collect();
        assert_eq!(scopes, ["quota.read", "quota.write"]);

        let kinds: Vec<&str> = cap.emits().iter().map(EventKind::as_str).collect();
        assert_eq!(
            kinds,
            [
                "quota.tokens-sampled.v1",
                "quota.usage-probed.v1",
                "quota.window-reset.v1",
                "quota.block-predicted.v1",
                "quota.account-limited.v1",
                "quota.rotated.v1",
                "quota.blocked.v1",
            ]
        );
        // Every declared kind is owned by this module (prefix == meta id).
        for k in cap.emits() {
            assert_eq!(k.module(), QuotaModule::id().as_str());
        }
    }

    #[test]
    fn registers_the_ten_existing_quota_tools() {
        let mut reg = McpRegistry::new();
        QuotaModule.register_mcp_tools(&mut reg);
        let names: Vec<&str> = reg.tools().iter().map(|t| t.name.as_str()).collect();
        assert_eq!(
            names,
            [
                "quota.sample.validate",
                "quota.sample.execute",
                "quota.probe.validate",
                "quota.probe.execute",
                "quota.rotate.validate",
                "quota.rotate.execute",
                "quota.register.validate",
                "quota.register.execute",
                "quota.retire.validate",
                "quota.retire.execute",
            ]
        );
    }

    #[test]
    fn owns_the_quota_tables_migration() {
        let migs = QuotaModule.migrations();
        assert_eq!(migs.len(), 1);
        assert_eq!(migs[0].version, 1);
        assert_eq!(migs[0].name, "create_quota");
        assert!(migs[0].sql.contains("CREATE TABLE IF NOT EXISTS accounts"));
        assert!(migs[0].sql.contains("CREATE TABLE IF NOT EXISTS token_usage"));
    }

    #[test]
    fn contributes_no_openapi_surface() {
        assert!(QuotaModule.openapi().is_none());
    }
}
