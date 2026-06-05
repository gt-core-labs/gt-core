//! Argument struct for `meta.report-gap` (hq-core-host.7).

use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

/// Input for `meta.report-gap.execute`: surface a missing MCP operation so the
/// server mints a `hq-gap-<slug>-<ts>` bead the routine catalog can pick up.
/// Mirrors the upstream `meta.report_gap` shape (renamed to kebab `report-gap`
/// for the kernel's three-segment tool-name rule).
///
/// The optional graph fields make a reported gap **taxonomy- and graph-sound on
/// mint** rather than a loose, contextless bead the catalog cannot route:
/// `external_ref` parents it under a sub-epic (NN-16), and
/// `surface`/`domain`/`depends_on` seed the columns `gt://issues?ready=true`
/// native-surface filtering and the `gt-reconciler` edge derivation key on (the
/// two gaps `hq-gap-meta-report-gap-execute-*` filed 2026-06-02).
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
pub struct ReportGap {
    /// Canonical name of the missing operation, e.g. `issues.archive.execute`.
    /// Required, non-empty.
    pub operation: String,
    /// Optional free-form context: what payload was expected, why blocked, links.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub notes: Option<String>,
    /// Optional priority override (0 = P0 .. 2 = P2). Defaults to P2 at mint time.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub priority: Option<u8>,
    /// Optional sub-epic this gap belongs under (NN-16 taxonomy). `None` lets the
    /// server default it to the gaps catalog sub-epic so the bead is never born
    /// orphaned.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub external_ref: Option<String>,
    /// Optional impact surfaces (crate names / repo paths) the fix will touch.
    /// Seeds `surface_json` so native-surface filtering + reconciler edge
    /// derivation see the bead without a manual `issues.update`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub surface: Option<Vec<String>>,
    /// Optional domain discriminators (e.g. `["orch.scheduling"]`). Seeds
    /// `domain_json`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub domain: Option<Vec<String>>,
    /// Optional ids of beads this gap is blocked on. Seeds `depends_on_json`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub depends_on: Option<Vec<String>>,
}

impl ReportGap {
    /// Shape check: the operation name must be present.
    pub fn validate(&self) -> Result<(), String> {
        if self.operation.trim().is_empty() {
            return Err("operation is empty".into());
        }
        Ok(())
    }
}
