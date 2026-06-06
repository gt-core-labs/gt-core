//! Where a claude account's credentials live on disk (`hq-quota-accounts.3`).
//!
//! Each onboarded account has a `CLAUDE_CONFIG_DIR` holding its logged-in creds. Those dirs sit
//! under one **accounts root** that the daemon (host) and the host-side onboarding flow (.4) both
//! see — by convention `<GT_EVENTLOG_ROOT>/accounts`, overridable via `GT_CLAUDE_ACCOUNTS_ROOT`.
//! (The containerized mcp-server only *displays* accounts from the event registry; it never touches
//! these dirs — claude is not in the container — so there is no host↔container path translation.)
//!
//! The account id flows in from the onboarding request, so [`account_config_dir`] **sanitizes** it
//! to a single safe path component: an id like `../../etc` or `a/b` must never escape the accounts
//! root (a path-traversal that would let an attacker point a polecat's `CLAUDE_CONFIG_DIR` at an
//! arbitrary location, or clobber one). Rejected ids return `None`; the caller refuses onboarding.

use std::path::{Path, PathBuf};

/// The accounts root: `GT_CLAUDE_ACCOUNTS_ROOT` when set, else `<eventlog_root>/accounts`. The
/// daemon and the onboarding flow resolve the same root so a dir written by one is read by the
/// other.
pub fn accounts_root(eventlog_root: &Path) -> PathBuf {
    match std::env::var("GT_CLAUDE_ACCOUNTS_ROOT") {
        Ok(v) if !v.trim().is_empty() => PathBuf::from(v.trim()),
        _ => eventlog_root.join("accounts"),
    }
}

/// Resolve the per-account credential dir under `root`, sanitizing `account` to a single safe path
/// component. Returns `None` when the id is unsafe — empty, `.`/`..`, or carrying a path separator
/// or NUL — so a traversal can never escape `root`.
pub fn account_config_dir(root: &Path, account: &str) -> Option<PathBuf> {
    if !is_safe_component(account) {
        return None;
    }
    Some(root.join(account))
}

/// A safe single path component: non-empty, not `.`/`..`, no `/`, `\\`, or NUL. (We keep dots
/// elsewhere — bead ids like `hq-x.1` are valid account labels — only the `.`/`..` whole-segment
/// and separators are rejected.)
fn is_safe_component(s: &str) -> bool {
    !s.is_empty()
        && s != "."
        && s != ".."
        && !s.contains('/')
        && !s.contains('\\')
        && !s.contains('\0')
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn config_dir_joins_a_safe_id() {
        let root = Path::new("/vol/accounts");
        assert_eq!(
            account_config_dir(root, "acctB"),
            Some(PathBuf::from("/vol/accounts/acctB"))
        );
        // Dots inside the id are fine (bead-style labels).
        assert_eq!(
            account_config_dir(root, "hq-x.1"),
            Some(PathBuf::from("/vol/accounts/hq-x.1"))
        );
    }

    #[test]
    fn config_dir_rejects_traversal_and_separators() {
        let root = Path::new("/vol/accounts");
        for bad in ["..", ".", "../../etc", "a/b", "a\\b", "", "x\0y", "/abs"] {
            assert_eq!(
                account_config_dir(root, bad),
                None,
                "unsafe id {bad:?} must be rejected (no escape from the root)"
            );
        }
    }

    #[test]
    fn accounts_root_defaults_under_eventlog() {
        // Note: depends on GT_CLAUDE_ACCOUNTS_ROOT being unset in the test env.
        if std::env::var("GT_CLAUDE_ACCOUNTS_ROOT").is_err() {
            assert_eq!(
                accounts_root(Path::new("/var/lib/gt-core")),
                PathBuf::from("/var/lib/gt-core/accounts")
            );
        }
    }
}
