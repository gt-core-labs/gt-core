//! Git adapters for the tracking-model refactor (docs/10). The orchestration tier
//! is where shelling out is allowed, so the `git` invocations live here rather
//! than in the `gt-issues` domain crate, which depends only on the
//! [`SurfaceTree`] (S3) and [`CommitInspector`] (S2) ports.
//!
//! - **[`GitSurfaceTree`]** (hq-core-mcp.9, §S3) reads `git ls-tree -r main` so
//!   `create`/`update` reject a `planned:false` surface path missing on `main`.
//! - **[`GitCommitInspector`]** (hq-core-mcp.10, §S2) resolves a closing
//!   `commit_sha` on `main` + the paths it changed so `close` can verify delivery.
//!
//! Both read the in-process git tree of the gt-core repo the server is configured
//! to serve (`GT_REPO_DIR`) — no extra clone. When no repo is wired (the live
//! container has no checkout) the surface check degrades to the permissive
//! [`AllowAllTree`] and delivery verification is skipped, so both refinements fall
//! back to pre-refactor behaviour instead of blocking writes.

use std::collections::HashSet;
use std::path::Path;
use std::process::Command;

use gt_issues::{AllowAllTree, CommitInfo, CommitInspector, SurfaceTree};

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

/// [`CommitInspector`] over a real git checkout (hq-core-mcp.10, §S2). Resolves a
/// closing `commit_sha` to its full form, verifies it is reachable from `main`,
/// and lists the paths it changed.
pub struct GitCommitInspector<'a> {
    repo_dir: &'a Path,
}

impl<'a> GitCommitInspector<'a> {
    fn run(&self, args: &[&str]) -> std::io::Result<std::process::Output> {
        Command::new("git").arg("-C").arg(self.repo_dir).args(args).output()
    }
}

impl CommitInspector for GitCommitInspector<'_> {
    fn inspect(&self, sha: &str) -> Option<CommitInfo> {
        // Resolve to a canonical commit sha; rejects a non-commit / unknown ref.
        let rev = self.run(&["rev-parse", "--verify", &format!("{sha}^{{commit}}")]).ok()?;
        if !rev.status.success() {
            return None;
        }
        let full_sha = String::from_utf8_lossy(&rev.stdout).trim().to_string();
        if full_sha.is_empty() {
            return None;
        }
        // The sha must be reachable from `main` — an off-main commit did not
        // deliver onto the line readiness evaluates against.
        let anc = self.run(&["merge-base", "--is-ancestor", &full_sha, "main"]).ok()?;
        if !anc.status.success() {
            return None;
        }
        // Paths the commit changed (empty for a treeless/root edge case — that
        // simply means it touches no surface, so the close is rejected upstream).
        let diff = self
            .run(&["diff-tree", "--no-commit-id", "--name-only", "-r", &full_sha])
            .ok()?;
        if !diff.status.success() {
            return None;
        }
        let changed_paths = String::from_utf8_lossy(&diff.stdout)
            .lines()
            .map(|l| l.trim_end().to_string())
            .filter(|l| !l.is_empty())
            .collect();
        Some(CommitInfo { full_sha, changed_paths })
    }
}

/// Resolve the [`CommitInspector`] for a close: `Some` when a repo is configured,
/// else `None` (delivery verification skipped, `delivered_sha` left NULL). Unlike
/// the surface tree, a missing repo yields `None` rather than a permissive
/// adapter — verification is opt-in, and absent it the close must NOT be rejected.
pub fn commit_inspector(repo_dir: Option<&Path>) -> Option<GitCommitInspector<'_>> {
    repo_dir.map(|repo_dir| GitCommitInspector { repo_dir })
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
