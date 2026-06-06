//! Per-polecat git worktree provisioning (`hq-orchd-deploy.9`).
//!
//! `SpawnTemplate::spec_for` points every polecat at the same `GT_RIG_PATH` checkout. That is a
//! data race: CLAUDE.md is explicit that the working tree's HEAD/branch is global, so two agents
//! doing `git`-work in one checkout race — a commit lands on the wrong branch, or a branch tip
//! vanishes between two commands. The mandated fix is one worktree per actor:
//!
//! ```text
//! git worktree add <root>/<session> -b <bead-id> main
//! ```
//!
//! This module is the edge that runs that for a slung polecat: [`provision`] creates the
//! per-polecat worktree off the rig checkout (idempotent — an existing path is reused, so a
//! re-sling lands in the same tree), and [`remove`] tears it down when the bead merges. The
//! composition layer owns this (not `gt-polecat`) because it is git I/O against the daemon's host
//! checkout, and it is gated on `GT_POLECAT_WORKTREE_ROOT`: unset ⇒ the legacy shared-checkout
//! behaviour, unchanged.

use std::path::Path;
use std::process::Command;

/// The `git worktree add` argv (relative to a `-C <base>` invocation), as data so it can be
/// asserted without running git. Branches `branch` off the base checkout's current HEAD (the rig
/// checkout tracks `main`, mirroring CLAUDE.md's `-b <bead-id> main`). `--force` lets a re-create
/// after a crash reuse a registered-but-stale path rather than abort the sling.
fn add_argv(path: &Path, branch: &str) -> Vec<String> {
    vec![
        "worktree".to_string(),
        "add".to_string(),
        "--force".to_string(),
        path.display().to_string(),
        "-b".to_string(),
        branch.to_string(),
    ]
}

/// `git worktree add` argv that attaches an EXISTING branch (no `-b`) — the fallback when the
/// branch already exists (a re-sling after the branch was created but the worktree was pruned).
fn add_existing_branch_argv(path: &Path, branch: &str) -> Vec<String> {
    vec![
        "worktree".to_string(),
        "add".to_string(),
        "--force".to_string(),
        path.display().to_string(),
        branch.to_string(),
    ]
}

/// The `git worktree remove --force` argv for teardown.
fn remove_argv(path: &Path) -> Vec<String> {
    vec![
        "worktree".to_string(),
        "remove".to_string(),
        "--force".to_string(),
        path.display().to_string(),
    ]
}

/// Ensure a per-polecat worktree exists at `path`, branched `branch` off `base_repo`'s HEAD.
///
/// Idempotent: if `path` already exists (a re-sling, or a crash-restart) it is reused as-is and no
/// git runs. Otherwise `git -C base worktree add --force <path> -b <branch>` runs; if that fails
/// because the branch already exists, it retries attaching the existing branch. Returns the path on
/// success. Best-effort by contract: the caller logs and falls back to the shared checkout on
/// failure, so a git hiccup never strands a sling.
pub fn provision(base_repo: &Path, path: &Path, branch: &str) -> std::io::Result<()> {
    if path.exists() {
        return Ok(());
    }
    let run = |argv: &[String]| -> std::io::Result<bool> {
        let status = Command::new("git")
            .arg("-C")
            .arg(base_repo)
            .args(argv)
            .status()?;
        Ok(status.success())
    };
    if run(&add_argv(path, branch))? {
        return Ok(());
    }
    // The `-b` form fails if the branch already exists — attach it instead.
    if run(&add_existing_branch_argv(path, branch))? {
        return Ok(());
    }
    Err(std::io::Error::other(format!(
        "git worktree add failed for {} (branch {branch}) off {}",
        path.display(),
        base_repo.display()
    )))
}

/// Tear down a polecat's worktree (best-effort): `git -C base worktree remove --force <path>`. The
/// branch itself is reaped separately by the branch-GC reactor on `merge.merged.v1`. A failure
/// (already gone, dirty tree) is swallowed by the caller — leftover worktrees are housekeeping, not
/// correctness.
pub fn remove(base_repo: &Path, path: &Path) -> std::io::Result<()> {
    if !path.exists() {
        return Ok(());
    }
    let ok = Command::new("git")
        .arg("-C")
        .arg(base_repo)
        .args(remove_argv(path))
        .status()?
        .success();
    if ok {
        Ok(())
    } else {
        Err(std::io::Error::other(format!(
            "git worktree remove failed for {}",
            path.display()
        )))
    }
}

/// Seed the polecat's `.mcp.json` into a fresh worktree (`hq-orchd-deploy.10`). The file is
/// machine-local (untracked, so it does NOT come with the worktree's checkout) yet the polecat's
/// claude needs it to reach the `gt` MCP server. Copy it from the base rig checkout, which the
/// operator configures once. Best-effort: a missing source (operator didn't place one) or an
/// already-present target is a silent no-op — the caller treats MCP wiring as non-fatal.
pub fn seed_mcp_config(base_repo: &Path, worktree: &Path) {
    let src = base_repo.join(".mcp.json");
    let dst = worktree.join(".mcp.json");
    if dst.exists() || !src.exists() {
        return;
    }
    if let Err(e) = std::fs::copy(&src, &dst) {
        eprintln!(
            "[polecat] .mcp.json seed {} -> {} skipped: {e}",
            src.display(),
            dst.display()
        );
    }
}

/// `chown -R <user> <path>` argv, as data so it can be asserted without shelling.
fn chown_argv(path: &Path, user: &str) -> Vec<String> {
    vec!["-R".to_string(), user.to_string(), path.display().to_string()]
}

/// Hand a freshly-provisioned worktree to the non-root polecat user (`hq-quota-accounts.6`).
/// `git worktree add` runs as the root daemon, so the tree is root-owned; a polecat re-exec'd under
/// `GT_POLECAT_RUN_AS` could not read/write it. `chown -R <user>` fixes that. Best-effort: a failure
/// (no such user, not root) logs and the sling proceeds — the operator sees the cause. No-op for an
/// empty user.
pub fn chown_to(path: &Path, user: &str) -> std::io::Result<()> {
    if user.trim().is_empty() {
        return Ok(());
    }
    let ok = Command::new("chown")
        .args(chown_argv(path, user.trim()))
        .status()?
        .success();
    if ok {
        Ok(())
    } else {
        Err(std::io::Error::other(format!(
            "chown -R {user} {} failed",
            path.display()
        )))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::path::PathBuf;

    #[test]
    fn add_argv_branches_off_head_with_force() {
        let argv = add_argv(&PathBuf::from("/wt/hq-gg-1"), "gg-1");
        assert_eq!(
            argv,
            vec!["worktree", "add", "--force", "/wt/hq-gg-1", "-b", "gg-1"]
        );
    }

    #[test]
    fn add_existing_branch_argv_omits_dash_b() {
        let argv = add_existing_branch_argv(&PathBuf::from("/wt/hq-gg-1"), "gg-1");
        assert_eq!(
            argv,
            vec!["worktree", "add", "--force", "/wt/hq-gg-1", "gg-1"]
        );
        assert!(!argv.iter().any(|a| a == "-b"));
    }

    #[test]
    fn chown_argv_is_recursive() {
        assert_eq!(
            chown_argv(&PathBuf::from("/wt/hq-gg-1"), "gtpolecat"),
            vec!["-R", "gtpolecat", "/wt/hq-gg-1"]
        );
    }

    #[test]
    fn chown_to_empty_user_is_noop() {
        chown_to(&PathBuf::from("/anything"), "   ").expect("blank user is a no-op, no chown");
    }

    #[test]
    fn remove_argv_forces() {
        let argv = remove_argv(&PathBuf::from("/wt/hq-gg-1"));
        assert_eq!(argv, vec!["worktree", "remove", "--force", "/wt/hq-gg-1"]);
    }

    #[test]
    fn provision_is_idempotent_when_path_exists() {
        // An existing path is reused without shelling git (the re-sling / restart case).
        let dir = std::env::temp_dir();
        assert!(dir.exists());
        provision(&PathBuf::from("/nonexistent-base"), &dir, "any-branch")
            .expect("existing path short-circuits before any git call");
    }

    #[test]
    fn remove_is_noop_when_path_absent() {
        let missing = std::env::temp_dir().join("gt-wt-definitely-absent-xyz");
        remove(&PathBuf::from("/nonexistent-base"), &missing)
            .expect("absent path is a no-op, no git call");
    }

    #[test]
    fn seed_mcp_config_copies_from_base_when_absent_in_worktree() {
        let uniq = format!(
            "{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        );
        let base = std::env::temp_dir().join(format!("gt-mcpseed-base-{uniq}"));
        let wt = std::env::temp_dir().join(format!("gt-mcpseed-wt-{uniq}"));
        std::fs::create_dir_all(&base).unwrap();
        std::fs::create_dir_all(&wt).unwrap();
        std::fs::write(base.join(".mcp.json"), r#"{"mcpServers":{}}"#).unwrap();

        seed_mcp_config(&base, &wt);
        assert_eq!(
            std::fs::read_to_string(wt.join(".mcp.json")).unwrap(),
            r#"{"mcpServers":{}}"#,
            "the worktree got the base's .mcp.json"
        );

        // Idempotent + non-clobbering: a second call with a different source leaves it.
        std::fs::write(base.join(".mcp.json"), "CHANGED").unwrap();
        seed_mcp_config(&base, &wt);
        assert_eq!(
            std::fs::read_to_string(wt.join(".mcp.json")).unwrap(),
            r#"{"mcpServers":{}}"#,
            "existing target is not clobbered"
        );

        let _ = std::fs::remove_dir_all(&base);
        let _ = std::fs::remove_dir_all(&wt);
    }

    #[test]
    fn seed_mcp_config_is_silent_noop_without_a_source() {
        let wt = std::env::temp_dir().join(format!("gt-mcpseed-nosrc-{}", std::process::id()));
        std::fs::create_dir_all(&wt).unwrap();
        seed_mcp_config(&PathBuf::from("/nonexistent-base"), &wt);
        assert!(!wt.join(".mcp.json").exists());
        let _ = std::fs::remove_dir_all(&wt);
    }
}
