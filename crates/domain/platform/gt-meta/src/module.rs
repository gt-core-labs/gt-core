//! [`MetaModule`] — the cross-cutting `meta.*` tools as a [`GtModule`].

use gt_module::{GtModule, McpRegistry, ModuleId, ModuleMeta};
use gt_module_mcp::schema_for;
use semver::Version;

use crate::commands::ReportGap;

/// The meta module. Stateless: it only declares the `meta.*` tool DESCRIPTORS —
/// the server bin owns their execution (meta.help over the built Root's tool
/// list, meta.report-gap over the issues store), keeping the descriptor-only
/// seam the kernel mandates.
pub struct MetaModule;

impl MetaModule {
    /// The module's stable id (`meta`). Matches the `meta.*` tool namespace.
    pub fn id() -> ModuleId {
        ModuleId::new("meta").expect("`meta` is a valid module id")
    }
}

impl GtModule for MetaModule {
    fn meta(&self) -> ModuleMeta {
        ModuleMeta::new(
            Self::id(),
            "Meta",
            Version::new(1, 0, 0),
            "Cross-cutting server tools: meta.help (tools/list) and meta.report-gap \
             (mint a hq-gap bead for a missing operation).",
        )
    }

    fn register_mcp_tools(&self, registry: &mut McpRegistry) {
        registry
            .tool(
                "meta.help.execute",
                "Server self-description: the full tools/list (names, descriptions, input \
                 schemas) the server serves. Single-call discovery. No state change.",
            )
            .tool_with_schema(
                "meta.report-gap.execute",
                "Surface a missing MCP operation: the server mints a hq-gap-<slug>-<ts> bead \
                 so the gap enters the routine catalog. Input: { operation (required), notes?, \
                 priority? (0=P0..2=P2, default 2), external_ref? (sub-epic; defaults to the \
                 hq-gaps catalog so the bead is NN-16-sound, never orphaned), surface? \
                 (crate/path strings), domain? (domain discriminators), depends_on? (blocker \
                 bead ids) }. The surface/domain/depends_on seeds make the gap visible to \
                 ?ready=true filtering + reconciler edge derivation without a manual update.",
                schema_for::<ReportGap>(),
            );
    }
}
