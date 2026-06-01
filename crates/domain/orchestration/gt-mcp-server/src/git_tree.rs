//! Git-tree adapter for surface referential integrity (hq-core-mcp.9, docs/10
//! §S3). The orchestration tier is where shelling out is allowed, so the
//! `git ls-tree` invocation lives here rather than in the `gt-issues` domain
//! crate, which depends only on the [`SurfaceTree`] port.
//!
//! The validator reads the in-process git tree of the gt-core repo the server is
//! configured to serve (`GT_REPO_DIR`) — no extra clone. When no repo is wired
//! (the live container has no checkout) the build falls back to the permissive
//! [`AllowAllTree`], so surface validation degrades to the pre-S3 behaviour
//! instead of rejecting every write.

use std::collections::HashSet;
use std::path::Path;
use std::process::Command;

use gt_issues::{AllowAllTree, SurfaceTree};

/// A snapshot of the `main` git tree: the flat set of every blob path returned by
/// `git ls-tree -r --name-only main`. [`SurfaceTree::contains`] treats a surface
/// entry as present when it equals a blob path or is a directory prefix of one.
pub struct GitSurfaceTree {
    paths: HashSet<String>,
}

impl GitSurfaceTree {
    /// Snapshot `git_ref`'s tree from the repo at `repo_dir`. Errors if `git` is
    /// missing, `repo_dir` is not a repo, or the ref does not resolve — the
    /// caller degrades to [`AllowAllTree`] on error.
    pub fn snapshot(repo_dir: &Path, git_ref: &str) -> std::io::Result<Self> {
        let out = Command::new("git")
            .arg("-C")
            .arg(repo_dir)
            .args(["ls-tree", "-r", "--name-only", git_ref])
            .output()?;
        if !out.status.success() {
            return Err(std::io::Error::other(format!(
                "git ls-tree {git_ref} failed: {}",
                String::from_utf8_lossy(&out.stderr).trim()
            )));
        }
        let paths = String::from_utf8_lossy(&out.stdout)
            .lines()
            .map(|l| l.trim_end().to_string())
            .filter(|l| !l.is_empty())
            .collect();
        Ok(Self { paths })
    }
}

impl SurfaceTree for GitSurfaceTree {
    fn contains(&self, path: &str) -> bool {
        let p = path.trim().trim_end_matches('/');
        if p.is_empty() {
            return false;
        }
        let prefix = format!("{p}/");
        self.paths.iter().any(|t| t == p || t.starts_with(&prefix))
    }
}

/// Resolve the [`SurfaceTree`] for a write: a fresh `main` snapshot when a repo
/// is configured, else the permissive fallback. Built per create/update call so a
/// freshly-merged path is visible without restarting the server. A git failure is
/// logged and degrades to [`AllowAllTree`] rather than blocking the write.
pub fn surface_tree(repo_dir: Option<&Path>) -> Box<dyn SurfaceTree + Send + Sync> {
    match repo_dir {
        Some(dir) => match GitSurfaceTree::snapshot(dir, "main") {
            Ok(tree) => Box::new(tree),
            Err(e) => {
                eprintln!(
                    "[gt-mcp-server] surface validation disabled — git tree unavailable: {e}"
                );
                Box::new(AllowAllTree)
            }
        },
        None => Box::new(AllowAllTree),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn contains_matches_exact_and_prefix() {
        let tree = GitSurfaceTree {
            paths: HashSet::from([
                "crates/domain/platform/gt-issues/src/lib.rs".to_string(),
                "README.md".to_string(),
            ]),
        };
        // Directory prefix matches the blobs beneath it.
        assert!(tree.contains("crates/domain/platform/gt-issues"));
        assert!(tree.contains("crates/domain/platform/gt-issues/"));
        // Exact blob path matches.
        assert!(tree.contains("README.md"));
        // A sibling that shares only a partial segment does not match.
        assert!(!tree.contains("crates/domain/platform/gt-iss"));
        assert!(!tree.contains("migrations/gt-rig"));
        assert!(!tree.contains(""));
    }

    #[test]
    fn no_repo_yields_permissive_tree() {
        assert!(surface_tree(None).contains("anything"));
    }
}
