//! Argument struct for `meta.report-gap` (hq-core-host.7).

use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

/// Input for `meta.report-gap.execute`: surface a missing MCP operation so the
/// server mints a `hq-gap-<slug>-<ts>` bead the routine catalog can pick up.
/// Mirrors the gastown `meta.report_gap` shape (renamed to kebab `report-gap`
/// for the kernel's three-segment tool-name rule).
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
