//! `FlagsModule` — the [`GtModule`] facade over the feature-flag override store
//! (hq-mod-flags.4).
//!
//! Declares what the flags feature offers so the `RootBuilder` harvests it
//! instead of the composition root hand-wiring tools (docs/03 rule 3):
//!
//! - **Identity** ([`GtModule::meta`]) — id `feature`, semver, description.
//! - **Capability** ([`GtModule::capability`]) — the `feature.read` /
//!   `feature.write` scopes the module owns. The flag tools persist overrides
//!   directly (no domain-event bus), so the module emits no event kinds.
//! - **MCP tools** ([`GtModule::register_mcp_tools`]) — the five
//!   `feature.{enable,disable}.{validate,execute}` + `feature.list.execute` tools.
//! - **Migrations** ([`GtModule::migrations`]) — the `flag_overrides` table,
//!   sourced from `gt-store-pg`.
//!
//! ## Dependency / ordering note
//!
//! `flag_overrides` FKs `workspaces.id`, so its migration must apply after the
//! workspaces table exists. The workspace catalog is not yet wrapped as a
//! `GtModule`, so this module declares no [`dependencies`](GtModule::dependencies)
//! (declaring an unregistered id would fail the build). When a `WorkspaceModule`
//! lands, add it here so the builder orders the migrations correctly. Wiring this
//! module into the live `gt-mcp-server` likewise waits on the multi-tenant PG
//! bootstrap (a pool + migration runner + per-workspace request context); until
//! then this module is a complete, tested unit that the server does not yet mount.

use gt_module::{Capability, GtModule, McpRegistry, Migration, ModuleId, ModuleMeta, Scope};
use gt_module_mcp::schema_for;
use semver::Version;

use crate::commands::{DisableFeature, EnableFeature, ListFeatures};

/// The [`GtModule`] facade over the feature-flag override store. Zero-sized: the
/// module owns no runtime state of its own (the live store is the
/// [`FeatureFlags`](crate::FeatureFlags) adapter the binary supplies to the
/// handlers), so the unit struct is all the composition root registers.
#[derive(Clone, Copy, Debug, Default)]
pub struct FlagsModule;

impl FlagsModule {
    /// The module's stable id (`feature`). Matches the `feature.*` MCP-tool and
    /// `feature.<verb>` scope namespaces. The literal is a known-valid slug.
    pub fn id() -> ModuleId {
        ModuleId::new("feature").expect("`feature` is a valid module id")
    }
}

impl GtModule for FlagsModule {
    fn meta(&self) -> ModuleMeta {
        ModuleMeta::new(
            Self::id(),
            "Feature Flags",
            Version::new(1, 0, 0),
            "Per-workspace feature toggles — enable/disable modules and capabilities \
             over MCP, persisted as override rows in flag_overrides.",
        )
    }

    fn capability(&self) -> Capability {
        // The `feature.read` / `feature.write` scopes the module owns (same
        // `<resource>.<verb>` convention `issues`/`rig`/`merge` follow). No event
        // kinds: the flag tools persist overrides directly, not through the bus.
        Capability::empty().claiming_all([
            Scope::new("feature.read").expect("valid scope"),
            Scope::new("feature.write").expect("valid scope"),
        ])
    }

    fn register_mcp_tools(&self, registry: &mut McpRegistry) {
        registry
            .tool_with_schema(
                "feature.enable.validate",
                "Check whether enabling a feature flag would be accepted (shape-only: the key \
                 parses as a dotted-kebab FlagKey). No state change.",
                schema_for::<EnableFeature>(),
            )
            .tool_with_schema(
                "feature.enable.execute",
                "Enable a feature flag for the calling workspace: write an `enabled = true` \
                 override to flag_overrides (upsert on (workspace, key)). The workspace is \
                 server-injected from the auth context, never a wire field.",
                schema_for::<EnableFeature>(),
            )
            .tool_with_schema(
                "feature.disable.validate",
                "Check whether disabling a feature flag would be accepted (shape-only: the key \
                 parses as a dotted-kebab FlagKey). No state change.",
                schema_for::<DisableFeature>(),
            )
            .tool_with_schema(
                "feature.disable.execute",
                "Disable a feature flag for the calling workspace: write an `enabled = false` \
                 override to flag_overrides. The workspace is server-injected.",
                schema_for::<DisableFeature>(),
            )
            .tool_with_schema(
                "feature.list.execute",
                "Snapshot the feature-flag overrides set in the calling workspace as a list of \
                 { key, enabled }. The workspace is server-injected.",
                schema_for::<ListFeatures>(),
            );
    }

    fn migrations(&self) -> Vec<Migration> {
        // The override table lives in the gt-store-pg migration host; the module
        // surfaces it so the kernel orders it alongside every other module's schema.
        gt_store_pg::feature_flags_migrations()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn meta_identity_is_feature() {
        let m = FlagsModule.meta();
        assert_eq!(m.id.as_str(), "feature");
        assert_eq!(m.version, Version::new(1, 0, 0));
    }

    #[test]
    fn capability_owns_feature_scopes_and_emits_nothing() {
        let cap = FlagsModule.capability();
        let scopes: Vec<&str> = cap.scopes().iter().map(Scope::as_str).collect();
        assert_eq!(scopes, ["feature.read", "feature.write"]);
        assert!(cap.emits().is_empty());
    }

    #[test]
    fn registers_the_five_feature_tools() {
        let mut reg = McpRegistry::new();
        FlagsModule.register_mcp_tools(&mut reg);
        let names: Vec<&str> = reg.tools().iter().map(|t| t.name.as_str()).collect();
        assert_eq!(
            names,
            [
                "feature.enable.validate",
                "feature.enable.execute",
                "feature.disable.validate",
                "feature.disable.execute",
                "feature.list.execute",
            ]
        );
    }

    #[test]
    fn every_tool_name_is_well_formed_and_namespaced() {
        let mut reg = McpRegistry::new();
        FlagsModule.register_mcp_tools(&mut reg);
        for tool in reg.tools() {
            let (module, _action, verb) = tool.parse_name().expect("well-formed tool name");
            assert_eq!(module, "feature", "tool {} not namespaced to feature", tool.name);
            assert!(verb == "validate" || verb == "execute", "unexpected verb in {}", tool.name);
        }
    }

    #[test]
    fn execute_tools_carry_an_object_input_schema() {
        let mut reg = McpRegistry::new();
        FlagsModule.register_mcp_tools(&mut reg);
        let enable = reg
            .tools()
            .iter()
            .find(|t| t.name == "feature.enable.execute")
            .expect("enable.execute present");
        assert_eq!(enable.input_schema["type"], "object");
        assert!(enable.input_schema["properties"]["key"].is_object());
    }

    #[test]
    fn surfaces_the_flag_overrides_migration() {
        let migrations = FlagsModule.migrations();
        assert_eq!(migrations.len(), 1);
        assert_eq!(migrations[0].name, "0001_flag_overrides");
    }
}
