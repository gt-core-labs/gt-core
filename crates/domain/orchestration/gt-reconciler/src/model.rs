//! The reconciler's view of a bead (hq-core-mcp.12).
//!
//! A thin projection of [`gt_store_dolt::IssueRow`] with the JSON columns parsed
//! once, so the pure §A/§A'/§B logic operates on typed data and unit-tests without
//! a store.

use gt_issues::surface::parse_surface_json;
use gt_issues::SurfaceEntry;
use gt_store_dolt::IssueRow;

/// A bead as the reconciler reasons about it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Bead {
    pub id: String,
    pub issue_type: String,
    pub status: String,
    /// ISO-8601 close timestamp (lexicographically sortable); `None` when open.
    pub closed_at: Option<String>,
    pub surface: Vec<SurfaceEntry>,
    pub depends_on: Vec<String>,
    /// Whether the bead already carries a non-NULL `delivered_sha`.
    pub delivered: bool,
}

impl Bead {
    /// Project a store row, parsing the surface + depends_on JSON columns.
    pub fn from_row(r: &IssueRow) -> Self {
        Bead {
            id: r.id.clone(),
            issue_type: r.issue_type.clone(),
            status: r.status.clone(),
            closed_at: r.closed_at.clone(),
            surface: parse_surface_json(&r.surface_json),
            depends_on: serde_json::from_str(&r.depends_on_json).unwrap_or_default(),
            delivered: r
                .delivered_sha
                .as_ref()
                .map(|s| !s.trim().is_empty())
                .unwrap_or(false),
        }
    }

    /// Non-`planned` surface paths (the real, on-`main` paths). Used by §B to know
    /// which paths a delivering commit must touch.
    pub fn real_surface_paths(&self) -> Vec<String> {
        self.surface
            .iter()
            .filter(|e| !e.planned)
            .map(|e| e.path.clone())
            .collect()
    }

    /// `planned:true` surface paths (the crates this bead will create). Used by §A'
    /// to find the producer's target crate.
    pub fn planned_surface_paths(&self) -> Vec<String> {
        self.surface
            .iter()
            .filter(|e| e.planned)
            .map(|e| e.path.clone())
            .collect()
    }
}
