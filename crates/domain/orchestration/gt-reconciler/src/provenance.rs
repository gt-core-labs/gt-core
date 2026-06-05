//! Auto-stamp port provenance on producer beads (hq-core-mcp.12 §A').
//!
//! A `depends_on` edge is a bare id — it says *what* a bead consumes but not
//! *where the artifact comes from*. The provenance belongs on the producer NODE,
//! not the edge: a producer bead lands a crate (a `planned:true` surface), and for
//! a *ported* crate that crate already exists in a source repo. Stamping
//! `design = "port-origin: <repo>:<path>"` on the producer means a model can follow
//! an edge to the producer, read `surface` (the gt-core target) + `design` (the
//! upstream origin), and recover the full port path — structurally, no prose.
//!
//! Pure: resolution is over an in-memory name→path index per source repo, so it
//! unit-tests without a filesystem.

use std::collections::BTreeMap;

use crate::model::Bead;

/// A source repo's crate index: package name → repo-relative directory.
#[derive(Debug, Clone)]
pub struct SourceRepo {
    /// Short label used in the marker (the repo dir basename, e.g. `gastown`).
    pub label: String,
    /// package name → repo-relative path of its crate directory.
    pub index: BTreeMap<String, String>,
}

/// The marker a stamped producer carries. Kept as a free function so the parse
/// shape lives in one place (`port-origin: <label>:<path>`).
pub fn marker(label: &str, path: &str) -> String {
    format!("port-origin: {label}:{path}")
}

/// True when `design` already carries a port-origin stamp — so re-running is a
/// no-op (idempotent).
pub fn already_stamped(design: &str) -> bool {
    design.contains("port-origin:")
}

/// The last path segment of a surface path — the crate name a `planned` surface
/// will create (`crates/domain/platform/gt-rig` → `gt-rig`).
fn crate_name(path: &str) -> &str {
    path.trim().trim_end_matches('/').rsplit('/').next().unwrap_or("")
}

/// Resolve a producer bead's port origin (§A'): for the first of its `planned`
/// crates whose name matches a crate in any source repo, return that source's
/// [`marker`]. `None` when the bead has no `planned` surface or no source crate
/// matches (a gt-core-native crate, not a port) — caller stamps nothing, no crash.
pub fn resolve_origin(bead: &Bead, sources: &[SourceRepo]) -> Option<String> {
    for path in bead.planned_surface_paths() {
        let name = crate_name(&path);
        if name.is_empty() {
            continue;
        }
        for src in sources {
            if let Some(rel) = src.index.get(name) {
                return Some(marker(&src.label, rel));
            }
        }
    }
    None
}

#[cfg(test)]
mod tests {
    use super::*;
    use gt_issues::SurfaceEntry;

    fn producer(planned: &[&str]) -> Bead {
        Bead {
            id: "port.rig".into(),
            issue_type: "task".into(),
            status: "open".into(),
            closed_at: None,
            surface: planned
                .iter()
                .map(|p| SurfaceEntry { path: p.to_string(), planned: true })
                .collect(),
            depends_on: vec![],
            delivered: false,
        }
    }

    fn gastown() -> Vec<SourceRepo> {
        let mut index = BTreeMap::new();
        index.insert(
            "gt-rig".to_string(),
            "apps/api/crates/domain/platform/gt-rig".to_string(),
        );
        vec![SourceRepo { label: "gastown".into(), index }]
    }

    #[test]
    fn stamps_when_source_crate_exists() {
        let b = producer(&["crates/domain/platform/gt-rig"]);
        assert_eq!(
            resolve_origin(&b, &gastown()),
            Some("port-origin: gastown:apps/api/crates/domain/platform/gt-rig".into())
        );
    }

    #[test]
    fn no_stamp_when_no_source_match() {
        // gt-core-native crate: not present in any source repo → no stamp, no crash.
        let b = producer(&["crates/domain/orchestration/gt-reconciler"]);
        assert_eq!(resolve_origin(&b, &gastown()), None);
    }

    #[test]
    fn no_stamp_without_planned_surface() {
        let mut b = producer(&["crates/domain/platform/gt-rig"]);
        b.surface[0].planned = false; // real surface, not a producer target.
        assert_eq!(resolve_origin(&b, &gastown()), None);
    }

    #[test]
    fn already_stamped_is_idempotent() {
        assert!(already_stamped("port-origin: gastown:apps/api/crates/x\nmore prose"));
        assert!(!already_stamped("just some design prose"));
    }
}
