//! Delivery verification for `issues.close` phase-2 (hq-core-mcp.10, docs/10
//! §S2).
//!
//! `close.execute` already requires a `commit_sha` (phase 1). Phase 2 verifies
//! that the sha actually landed on `main` AND touches one of the bead's
//! non-`planned` surface paths before stamping `delivered_sha`. This makes
//! `delivered_sha IS NOT NULL` — not `status='closed'` — the trustworthy delivery
//! signal dependency readiness (S4) evaluates against; a wontfix/no-deliverable
//! close leaves it NULL.
//!
//! The git inspection (resolve a sha to its full form + the paths it changed) is
//! a process call, so it lives behind the [`CommitInspector`] port; the git
//! adapter is supplied by the server (orchestration tier). The domain crate
//! depends only on this trait, so it tests against a fake.

/// What a sha resolves to on `main`: its canonical full sha and the repo paths it
/// changed (`git diff-tree --no-commit-id --name-only -r <sha>`).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CommitInfo {
    /// Full 40-hex sha (the short input resolved + verified reachable from main).
    pub full_sha: String,
    /// Paths the commit changed, relative to the repo root.
    pub changed_paths: Vec<String>,
}

/// Read model of a delivering commit (S2). The port the server's git adapter
/// implements; the domain crate never shells out to `git` itself.
pub trait CommitInspector {
    /// Resolve `sha` against `main`. `Some(info)` when the sha names a commit
    /// reachable from `main`; `None` when it is unknown or not on `main` (so a
    /// close referencing an off-main / bogus sha is rejected).
    fn inspect(&self, sha: &str) -> Option<CommitInfo>;
}

/// True when changed-path `changed` falls at or under surface path `surface` — an
/// exact match or a file beneath a surface directory. Both are de-slashed so a
/// trailing `/` on a surface entry does not defeat the prefix test.
pub fn path_touches_surface(changed: &str, surface: &str) -> bool {
    let s = surface.trim().trim_end_matches('/');
    if s.is_empty() {
        return false;
    }
    changed == s || changed.starts_with(&format!("{s}/"))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn path_match_is_exact_or_under_directory() {
        assert!(path_touches_surface(
            "crates/domain/platform/gt-issues/src/lib.rs",
            "crates/domain/platform/gt-issues"
        ));
        assert!(path_touches_surface("README.md", "README.md"));
        // Trailing slash on the surface entry still matches.
        assert!(path_touches_surface("a/b.rs", "a/"));
        // Partial segment is not a match.
        assert!(!path_touches_surface("crates/gt-issues-x/y.rs", "crates/gt-issues"));
        assert!(!path_touches_surface("a/b.rs", ""));
    }
}
