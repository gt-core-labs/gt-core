//! Intra-workspace crate graph from `cargo metadata` (hq-core-mcp.12 §A).
//!
//! The Cargo manifest is the *authoritative* source of which crate depends on
//! which — far more reliable than the hand-maintained `depends_on` edges. This
//! module turns `cargo metadata --no-deps` into:
//!
//! - `dirs`: crate name → repo-relative directory (so a bead's surface path can be
//!   mapped to the crate that owns it by longest-prefix), and
//! - `deps`: crate name → the set of *intra-workspace* crates it depends on
//!   (third-party deps are dropped — only workspace members can be owned by a
//!   bead).
//!
//! Pure: [`CrateGraph::from_metadata`] parses a JSON string + repo root and shells
//! out to nothing, so it unit-tests against a fixture.

use std::collections::{BTreeMap, BTreeSet};

use serde::Deserialize;

/// `cargo metadata --format-version 1` (the subset we read).
#[derive(Deserialize)]
struct Metadata {
    packages: Vec<Package>,
}

#[derive(Deserialize)]
struct Package {
    name: String,
    manifest_path: String,
    #[serde(default)]
    dependencies: Vec<Dependency>,
}

#[derive(Deserialize)]
struct Dependency {
    name: String,
}

/// The intra-workspace crate graph (§A).
#[derive(Debug, Default, Clone, PartialEq, Eq)]
pub struct CrateGraph {
    /// crate name → repo-relative directory, e.g. `crates/kernel/gt-module`.
    pub dirs: BTreeMap<String, String>,
    /// crate name → the set of workspace crates it depends on.
    pub deps: BTreeMap<String, BTreeSet<String>>,
}

impl CrateGraph {
    /// Parse `cargo metadata --no-deps --format-version 1` output (`json`) into the
    /// graph. `repo_root` is stripped from each `manifest_path` so directories are
    /// repo-relative (matching the form bead surfaces use). Third-party
    /// dependencies are filtered out by intersecting against the workspace member
    /// names, so `deps` carries only edges a bead can own.
    pub fn from_metadata(json: &str, repo_root: &str) -> anyhow::Result<Self> {
        let meta: Metadata = serde_json::from_str(json)?;
        let members: BTreeSet<&str> = meta.packages.iter().map(|p| p.name.as_str()).collect();
        let root = repo_root.trim_end_matches('/');

        let mut dirs = BTreeMap::new();
        let mut deps = BTreeMap::new();
        for p in &meta.packages {
            // manifest_path is `<dir>/Cargo.toml`; drop the file then make relative.
            let dir = p
                .manifest_path
                .strip_suffix("/Cargo.toml")
                .unwrap_or(&p.manifest_path);
            let rel = dir
                .strip_prefix(root)
                .map(|r| r.trim_start_matches('/'))
                .unwrap_or(dir)
                .to_string();
            dirs.insert(p.name.clone(), rel);

            let wdeps: BTreeSet<String> = p
                .dependencies
                .iter()
                .filter(|d| members.contains(d.name.as_str()) && d.name != p.name)
                .map(|d| d.name.clone())
                .collect();
            deps.insert(p.name.clone(), wdeps);
        }
        Ok(CrateGraph { dirs, deps })
    }

    /// The crate that owns `surface_path` (§A: longest-prefix over crate dirs). A
    /// surface entry is either the crate directory itself or a file under it, so
    /// the owner is the crate whose dir is the longest prefix of the (de-slashed)
    /// path. `None` when no crate dir is a prefix (a repo-root path like
    /// `README.md`, or a stale `apps/api/...`).
    pub fn owning_crate(&self, surface_path: &str) -> Option<&str> {
        let p = surface_path.trim().trim_end_matches('/');
        let mut best: Option<(&str, usize)> = None;
        for (name, dir) in &self.dirs {
            let d = dir.as_str();
            if p == d || p.starts_with(&format!("{d}/")) {
                if best.map(|(_, len)| d.len() > len).unwrap_or(true) {
                    best = Some((name.as_str(), d.len()));
                }
            }
        }
        best.map(|(name, _)| name)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const FIXTURE: &str = r#"{
      "packages": [
        {"name":"gt-module","manifest_path":"/repo/crates/kernel/gt-module/Cargo.toml","dependencies":[{"name":"serde"}]},
        {"name":"gt-issues","manifest_path":"/repo/crates/domain/platform/gt-issues/Cargo.toml","dependencies":[{"name":"gt-module"},{"name":"gt-store-dolt"},{"name":"serde"}]},
        {"name":"gt-store-dolt","manifest_path":"/repo/crates/kernel/gt-store-dolt/Cargo.toml","dependencies":[]}
      ]
    }"#;

    #[test]
    fn parses_dirs_and_filters_third_party_deps() {
        let g = CrateGraph::from_metadata(FIXTURE, "/repo").unwrap();
        assert_eq!(g.dirs["gt-module"], "crates/kernel/gt-module");
        // serde dropped (not a member); gt-module + gt-store-dolt kept.
        let d = &g.deps["gt-issues"];
        assert!(d.contains("gt-module") && d.contains("gt-store-dolt"));
        assert!(!d.contains("serde"));
        assert!(g.deps["gt-store-dolt"].is_empty());
    }

    #[test]
    fn owning_crate_is_longest_prefix() {
        let g = CrateGraph::from_metadata(FIXTURE, "/repo").unwrap();
        // file under a crate dir resolves to that crate.
        assert_eq!(
            g.owning_crate("crates/domain/platform/gt-issues/src/readiness.rs"),
            Some("gt-issues")
        );
        // the crate dir itself.
        assert_eq!(g.owning_crate("crates/kernel/gt-module"), Some("gt-module"));
        // repo-root / stale path → no owner.
        assert_eq!(g.owning_crate("README.md"), None);
        assert_eq!(g.owning_crate("apps/api/src/lib.rs"), None);
    }
}
