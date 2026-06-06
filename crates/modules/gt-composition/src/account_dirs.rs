//! Where a claude account's credentials live on disk (`hq-quota-accounts.3`).
//!
//! Each onboarded account has a `CLAUDE_CONFIG_DIR` holding its logged-in creds. Those dirs sit
//! under one **accounts root** — by convention `<GT_EVENTLOG_ROOT>/accounts`, overridable via
//! `GT_CLAUDE_ACCOUNTS_ROOT`. Since `hq-quota-onboard-web.4` the backend container writes them there
//! (claude lives in the image); the daemon (host) reads the same shared volume, mounted at a
//! different absolute path — so the daemon translates container→host by basename when it hydrates.
//!
//! The account id flows in from the onboarding request, so [`account_config_dir`] **sanitizes** it
//! to a single safe path component: an id like `../../etc` or `a/b` must never escape the accounts
//! root (a path-traversal that would let an attacker point a polecat's `CLAUDE_CONFIG_DIR` at an
//! arbitrary location, or clobber one). Rejected ids return `None`; the caller refuses onboarding.
//!
//! [`gc_orphan_account_dirs`] reaps credential dirs no live account points at (a re-onboarded email
//! replaces its dir; a retire drops it; an abandoned `/start` leaves an empty one), so stale claude
//! credentials do not pile up on the volume.

use std::collections::HashSet;
use std::path::{Path, PathBuf};
use std::time::{Duration, SystemTime};

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

/// Reap orphan credential dirs directly under `root` (`hq-quota-onboard-web.6`). A dir is removed
/// only when **both** hold:
///
/// 1. its basename is **not** in `live` — the basenames (the `<id>` component) of the config_dirs of
///    currently-registered accounts, replayed from the quota log; and
/// 2. it was last modified more than `grace` ago — the grace window protects an onboarding **in
///    flight** (`/start` creates the dir before the `account_registered` event exists, so a brand-new
///    dir looks orphan until `/complete` lands).
///
/// Only direct children of `root` are considered, so host-native dirs (the `GT_CLAUDE_ACCOUNTS` env
/// bootstrap, which live elsewhere) are never touched. An unreadable `mtime` is treated as recent
/// (kept), failing safe. Returns the dirs actually removed.
pub fn gc_orphan_account_dirs(root: &Path, live: &HashSet<String>, grace: Duration) -> Vec<PathBuf> {
    let mut removed = Vec::new();
    let now = SystemTime::now();
    let Ok(entries) = std::fs::read_dir(root) else {
        return removed;
    };
    for entry in entries.flatten() {
        let path = entry.path();
        if !path.is_dir() {
            continue;
        }
        let Some(name) = path.file_name().and_then(|n| n.to_str()) else {
            continue;
        };
        if live.contains(name) {
            continue;
        }
        let recent = entry
            .metadata()
            .and_then(|m| m.modified())
            .ok()
            .and_then(|mt| now.duration_since(mt).ok())
            .map(|age| age < grace)
            .unwrap_or(true); // unknown mtime ⇒ treat as recent, keep
        if recent {
            continue;
        }
        if std::fs::remove_dir_all(&path).is_ok() {
            removed.push(path);
        }
    }
    removed
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
    fn gc_reaps_orphans_keeps_live_and_respects_grace() {
        let tmp = tempfile::tempdir().unwrap();
        let root = tmp.path();
        for id in ["live1", "orphanA", "orphanB"] {
            std::fs::create_dir(root.join(id)).unwrap();
        }
        std::fs::write(root.join("not-a-dir"), b"x").unwrap();
        let live: HashSet<String> = ["live1".to_string()].into_iter().collect();

        // Huge grace ⇒ everything is "recent" ⇒ nothing reaped.
        assert!(gc_orphan_account_dirs(root, &live, Duration::from_secs(86_400)).is_empty());
        assert!(root.join("orphanA").exists());

        // Zero grace ⇒ non-live dirs reaped, the live one and the plain file kept.
        let mut removed: Vec<String> = gc_orphan_account_dirs(root, &live, Duration::ZERO)
            .into_iter()
            .map(|p| p.file_name().unwrap().to_string_lossy().into_owned())
            .collect();
        removed.sort();
        assert_eq!(removed, vec!["orphanA".to_string(), "orphanB".to_string()]);
        assert!(root.join("live1").exists());
        assert!(!root.join("orphanA").exists());
        assert!(root.join("not-a-dir").exists());
    }

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
