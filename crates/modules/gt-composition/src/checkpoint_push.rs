//! Periodic checkpoint-push of in-flight polecat branches to origin (`gtcore-4cea57`).
//!
//! INCIDENT (2026-06-15): three beads had their work COMMITTED in the polecat worktree on the
//! node-local PVC (`/rig-wt`) but NEVER on origin, because [`GitMergePlugin`](crate::git_merge)
//! only pushes a branch at merge-ready — the very end. The agents died before emitting merge-ready,
//! the merge slot went `failed`, and the committed work was invisible: it had to be recovered by
//! hand (`push` + PR). The PVC is `local-path` (node-local) → a node loss would have erased it for
//! real. The only durable store is origin.
//!
//! This module is the **complementary edge** (Option B of the bead): the daemon periodically pushes
//! every in-flight polecat branch to origin without waiting for merge-ready and without requiring
//! the agent's cooperation, so a polecat death never hides committed work. It is the orchd-driven
//! safety net under the per-agent checkpoint protocol (Option A, `gtcore-2467b4`).
//!
//! Mechanics mirror [`crate::git_merge`]: a polecat's branch is checked out in a per-bead worktree
//! ([`crate::worktree`]), but worktrees share the rig checkout's object DB and refs — so
//! `git -C <rig> push origin <branch>` from the rig checkout pushes exactly what the worktree
//! committed. We enumerate the live branches from `git worktree list --porcelain` (skipping the
//! main worktree — the rig checkout itself, on `main`), and push each:
//!
//! - **fast-forward first** (`git push origin <branch>:<branch>`) — the common case (the agent only
//!   adds commits) and idempotent: an unchanged branch is a silent "Everything up-to-date" (exit 0).
//! - **`--force-with-lease` fallback** — only when the ff push is refused (the branch was rebased,
//!   e.g. mid-merge). The lease protects the agent's own branch: it clobbers only when our
//!   remote-tracking ref still matches origin, never another writer's work.
//!
//! Blocking (shells `git`); the bin drives [`checkpoint_push_pass`] on a `spawn_blocking` timer, the
//! same way it drives the supervision tick. Gated by `GT_CHECKPOINT_PUSH_SECS` in the bin (`0` ⇒
//! disabled). Best-effort throughout: a failure is logged per branch and never aborts the pass —
//! the next merge-ready push (or the next tick) is the backstop.

use std::collections::BTreeSet;
use std::path::{Path, PathBuf};
use std::process::Command;

/// `git -C <rig> worktree list --porcelain` argv — to discover the live per-bead branches. Shared
/// shape with [`crate::git_merge`]; kept local so this module is self-contained (data, asserted in
/// tests without running git).
fn worktree_list_argv() -> Vec<String> {
    vec!["worktree".into(), "list".into(), "--porcelain".into()]
}

/// `git -C <rig> push origin <branch>:<branch>` argv — a plain fast-forward push of the branch to
/// its same-named remote ref. Idempotent: an up-to-date branch exits 0 ("Everything up-to-date").
fn push_argv(branch: &str) -> Vec<String> {
    vec!["push".into(), "origin".into(), format!("{branch}:{branch}")]
}

/// `git -C <rig> push --force-with-lease origin <branch>:<branch>` argv — the fallback when the ff
/// push is refused (a rebased branch). `--force-with-lease` (no value) protects against clobbering:
/// it only overwrites origin when our remote-tracking `origin/<branch>` still matches what's there,
/// so it advances the agent's OWN branch but refuses if someone else moved it.
fn force_push_argv(branch: &str) -> Vec<String> {
    vec![
        "push".into(),
        "--force-with-lease".into(),
        "origin".into(),
        format!("{branch}:{branch}"),
    ]
}

/// Run `git -C <dir> <argv...>`, returning (success, trimmed stdout|stderr) — the shape the push
/// sequence branches on. Mirrors [`crate::git_merge`]'s private `run`.
fn run(dir: &Path, argv: &[String]) -> std::io::Result<(bool, String)> {
    let out = Command::new("git").arg("-C").arg(dir).args(argv).output()?;
    let text = if out.status.success() {
        String::from_utf8_lossy(&out.stdout).trim().to_string()
    } else {
        String::from_utf8_lossy(&out.stderr).trim().to_string()
    };
    Ok((out.status.success(), text))
}

/// The in-flight branches to checkpoint from a `git worktree list --porcelain` dump: every LINKED
/// worktree's branch, skipping the main worktree (the rig checkout itself — `git` always lists it
/// first). Detached worktrees (no `branch` line) are naturally skipped. Pure, so it is unit-tested
/// against captured porcelain text.
///
/// Skipping the first worktree — rather than hard-coding `main` — means the rig checkout is excluded
/// whatever branch it sits on, and a deduped, deterministic order is returned (a branch checked out
/// in two trees, which `git` forbids anyway, is pushed once).
pub fn checkpoint_branches(porcelain: &str) -> Vec<String> {
    let mut seen = BTreeSet::new();
    let mut branches = Vec::new();
    // `worktree ` lines delimit blocks; the first block is the main worktree (index 0), skipped.
    let mut block: isize = -1;
    for line in porcelain.lines() {
        if line.starts_with("worktree ") {
            block += 1;
        } else if let Some(refname) = line.strip_prefix("branch ") {
            if block >= 1 {
                if let Some(name) = refname.strip_prefix("refs/heads/") {
                    if seen.insert(name.to_string()) {
                        branches.push(name.to_string());
                    }
                }
            }
        }
    }
    branches
}

/// Push one branch to origin: a fast-forward push, falling back to `--force-with-lease` only when
/// the ff is refused. Returns `Ok` on a landed (or already-current) branch, `Err(reason)` otherwise.
fn push_branch(rig: &Path, branch: &str) -> Result<(), String> {
    match run(rig, &push_argv(branch)) {
        Ok((true, _)) => Ok(()),
        // Refused (non-fast-forward: a rebased branch) → retry under the lease.
        Ok((false, _)) => match run(rig, &force_push_argv(branch)) {
            Ok((true, _)) => Ok(()),
            Ok((false, err)) => Err(format!("ff + force-with-lease push refused: {err}")),
            Err(e) => Err(format!("force-with-lease push error: {e}")),
        },
        Err(e) => Err(format!("push error: {e}")),
    }
}

/// Checkpoint-push every in-flight polecat branch in `rig`'s worktrees to origin (best-effort,
/// idempotent). Returns per-branch outcomes so the caller can log them. Blocking (shells `git`); run
/// it off the runtime workers. A failed `worktree list` yields a single error entry with an empty
/// branch name.
pub fn checkpoint_push_rig(rig: &Path) -> Vec<(String, Result<(), String>)> {
    let porcelain = match run(rig, &worktree_list_argv()) {
        Ok((true, out)) => out,
        Ok((false, err)) => {
            return vec![(String::new(), Err(format!("git worktree list failed: {err}")))]
        }
        Err(e) => return vec![(String::new(), Err(format!("git worktree list error: {e}")))],
    };
    checkpoint_branches(&porcelain)
        .into_iter()
        .map(|branch| {
            let res = push_branch(rig, &branch);
            (branch, res)
        })
        .collect()
}

/// Run one checkpoint-push pass over a set of rig checkouts (deduplicated), logging each branch
/// pushed and each failure. The bin calls this on a timer. Blocking — the caller wraps it in
/// `spawn_blocking`.
pub fn checkpoint_push_pass(rigs: &[PathBuf]) {
    let mut seen = BTreeSet::new();
    for rig in rigs {
        if !seen.insert(rig.clone()) {
            continue;
        }
        for (branch, res) in checkpoint_push_rig(rig) {
            match res {
                Ok(()) => {
                    if !branch.is_empty() {
                        eprintln!(
                            "[checkpoint-push] {branch} → origin (rig {})",
                            rig.display()
                        );
                    }
                }
                Err(reason) => eprintln!(
                    "[checkpoint-push] {} in rig {}: {reason}",
                    if branch.is_empty() { "<list>" } else { branch.as_str() },
                    rig.display()
                ),
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn argv_builders_push_branch_to_same_named_ref() {
        assert_eq!(worktree_list_argv(), vec!["worktree", "list", "--porcelain"]);
        assert_eq!(
            push_argv("gtcore-4cea57"),
            vec!["push", "origin", "gtcore-4cea57:gtcore-4cea57"],
            "ff push lands the branch on its same-named origin ref"
        );
        assert_eq!(
            force_push_argv("gtcore-4cea57"),
            vec![
                "push",
                "--force-with-lease",
                "origin",
                "gtcore-4cea57:gtcore-4cea57"
            ],
            "the fallback is lease-guarded, never a bare --force"
        );
    }

    #[test]
    fn checkpoint_branches_skips_the_main_worktree() {
        // The rig checkout (main worktree, listed first, on `main`) is skipped; the two per-bead
        // linked worktrees are the in-flight branches to checkpoint.
        let porcelain = "\
worktree /rig
HEAD 1111111111111111111111111111111111111111
branch refs/heads/main

worktree /rig-wt/gt-gtcore-4cea57
HEAD 2222222222222222222222222222222222222222
branch refs/heads/gtcore-4cea57

worktree /rig-wt/gt-gtweb-968172
HEAD 3333333333333333333333333333333333333333
branch refs/heads/gtweb-968172
";
        assert_eq!(
            checkpoint_branches(porcelain),
            vec!["gtcore-4cea57".to_string(), "gtweb-968172".to_string()],
        );
    }

    #[test]
    fn checkpoint_branches_skips_the_main_tree_whatever_branch_it_holds() {
        // Skipping the FIRST worktree (not the name "main") means the rig checkout is excluded even
        // if it sits on a non-main branch — and a release/* main tree is never checkpoint-pushed.
        let porcelain = "\
worktree /rig
HEAD 1111111111111111111111111111111111111111
branch refs/heads/release-2.0

worktree /rig-wt/gt-gtcore-x
HEAD 2222222222222222222222222222222222222222
branch refs/heads/gtcore-x
";
        assert_eq!(checkpoint_branches(porcelain), vec!["gtcore-x".to_string()]);
    }

    #[test]
    fn checkpoint_branches_ignores_detached_worktrees() {
        // A detached linked worktree has no `branch` line — there is nothing to push and it must not
        // be mis-attributed to a neighbour.
        let porcelain = "\
worktree /rig
HEAD 1111111111111111111111111111111111111111
branch refs/heads/main

worktree /rig-wt/detached
HEAD 4444444444444444444444444444444444444444
detached

worktree /rig-wt/gt-z
HEAD 5555555555555555555555555555555555555555
branch refs/heads/z-1
";
        assert_eq!(checkpoint_branches(porcelain), vec!["z-1".to_string()]);
    }

    #[test]
    fn checkpoint_branches_empty_when_only_the_main_tree() {
        // No polecats in flight → no linked worktrees → nothing to push.
        let porcelain = "\
worktree /rig
HEAD 1111111111111111111111111111111111111111
branch refs/heads/main
";
        assert!(checkpoint_branches(porcelain).is_empty());
    }

    /// THE BEAD's test (`gtcore-4cea57`): simulate a polecat that COMMITS work in its worktree and
    /// then dies — assert a checkpoint-push pass lands that commit on origin, recoverable via PR
    /// without re-running the work. No merge-ready is ever emitted.
    #[test]
    fn commit_then_death_leaves_the_branch_on_origin() {
        let uniq = format!(
            "{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        );
        let root = std::env::temp_dir().join(format!("gt-ckpt-{uniq}"));
        let origin = root.join("origin.git");
        let rig = root.join("rig");
        let wt_root = root.join("wt");
        std::fs::create_dir_all(&origin).unwrap();
        std::fs::create_dir_all(&wt_root).unwrap();
        let sh = |dir: &Path, args: &[&str]| {
            let out = Command::new("git")
                .arg("-C")
                .arg(dir)
                .args(args)
                .output()
                .unwrap();
            assert!(
                out.status.success(),
                "git {args:?} in {}: {}",
                dir.display(),
                String::from_utf8_lossy(&out.stderr)
            );
            String::from_utf8_lossy(&out.stdout).trim().to_string()
        };
        // A bare origin, a rig checkout cloned from it (on main), and a per-bead worktree the
        // "polecat" works in.
        sh(&origin, &["init", "-q", "--bare", "-b", "main"]);
        sh(&root, &["clone", "-q", origin.to_str().unwrap(), "rig"]);
        sh(&rig, &["config", "user.email", "t@t"]);
        sh(&rig, &["config", "user.name", "t"]);
        std::fs::write(rig.join("seed"), "0").unwrap();
        sh(&rig, &["add", "."]);
        sh(&rig, &["commit", "-qm", "seed"]);
        sh(&rig, &["push", "-q", "origin", "main"]);

        let bead = "gtcore-4cea57";
        let wt = wt_root.join(format!("gt-{bead}"));
        sh(&rig, &["worktree", "add", "-q", "-b", bead, wt.to_str().unwrap(), "main"]);
        // The polecat commits work — but NEVER emits merge-ready (it "dies" right after).
        std::fs::write(wt.join("work.txt"), "the recovered work").unwrap();
        sh(&wt, &["add", "."]);
        sh(&wt, &["commit", "-qm", "feat: in-flight work"]);
        let committed = sh(&wt, &["rev-parse", "HEAD"]);

        // Before the pass: origin has no such branch — the work is invisible (the incident).
        let pre = Command::new("git")
            .arg("-C")
            .arg(&origin)
            .args(["rev-parse", "--verify", "-q", &format!("refs/heads/{bead}")])
            .output()
            .unwrap();
        assert!(!pre.status.success(), "origin must not have the branch yet");

        // The orchd checkpoint-push pass (the agent is gone; this needs no cooperation).
        checkpoint_push_pass(&[rig.clone()]);

        // The committed work is now durable on origin at the exact sha — recoverable via PR.
        let landed = sh(&origin, &["rev-parse", &format!("refs/heads/{bead}")]);
        assert_eq!(landed, committed, "the in-flight commit is on origin/{bead}");

        // Idempotent: a second pass is a silent no-op (nothing new), and does not error.
        checkpoint_push_pass(&[rig.clone()]);
        let again = sh(&origin, &["rev-parse", &format!("refs/heads/{bead}")]);
        assert_eq!(again, committed, "a re-push of an unchanged branch is a no-op");

        let _ = std::fs::remove_dir_all(&root);
    }

    /// A rebased (diverged) branch still checkpoints: the ff push is refused and the
    /// `--force-with-lease` fallback advances the agent's own branch.
    #[test]
    fn rebased_branch_checkpoints_via_force_with_lease() {
        let uniq = format!(
            "{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        );
        let root = std::env::temp_dir().join(format!("gt-ckpt-rb-{uniq}"));
        let origin = root.join("origin.git");
        let rig = root.join("rig");
        let wt_root = root.join("wt");
        std::fs::create_dir_all(&origin).unwrap();
        std::fs::create_dir_all(&wt_root).unwrap();
        let sh = |dir: &Path, args: &[&str]| {
            let out = Command::new("git")
                .arg("-C")
                .arg(dir)
                .args(args)
                .output()
                .unwrap();
            assert!(
                out.status.success(),
                "git {args:?} in {}: {}",
                dir.display(),
                String::from_utf8_lossy(&out.stderr)
            );
            String::from_utf8_lossy(&out.stdout).trim().to_string()
        };
        sh(&origin, &["init", "-q", "--bare", "-b", "main"]);
        sh(&root, &["clone", "-q", origin.to_str().unwrap(), "rig"]);
        sh(&rig, &["config", "user.email", "t@t"]);
        sh(&rig, &["config", "user.name", "t"]);
        std::fs::write(rig.join("seed"), "0").unwrap();
        sh(&rig, &["add", "."]);
        sh(&rig, &["commit", "-qm", "seed"]);
        sh(&rig, &["push", "-q", "origin", "main"]);

        let bead = "gtcore-rb";
        let wt = wt_root.join(format!("gt-{bead}"));
        sh(&rig, &["worktree", "add", "-q", "-b", bead, wt.to_str().unwrap(), "main"]);
        std::fs::write(wt.join("w"), "v1").unwrap();
        sh(&wt, &["add", "."]);
        sh(&wt, &["commit", "-qm", "v1"]);
        // First checkpoint lands v1 on origin.
        checkpoint_push_pass(&[rig.clone()]);

        // The agent amends/rebases — the branch tip is REWRITTEN (diverges from origin).
        std::fs::write(wt.join("w"), "v2").unwrap();
        sh(&wt, &["add", "."]);
        sh(&wt, &["commit", "-q", "--amend", "-m", "v1+v2"]);
        let rewritten = sh(&wt, &["rev-parse", "HEAD"]);

        // The next pass must still land it: ff is refused, force-with-lease succeeds.
        checkpoint_push_pass(&[rig.clone()]);
        assert_eq!(
            sh(&origin, &["rev-parse", &format!("refs/heads/{bead}")]),
            rewritten,
            "the rewritten tip reached origin under the lease"
        );

        let _ = std::fs::remove_dir_all(&root);
    }

    #[test]
    fn push_pass_dedups_rig_paths() {
        // The same rig passed twice (the merge fallback often equals a routed path) is visited once;
        // a nonexistent rig only logs a list error and never panics.
        let missing = std::env::temp_dir().join("gt-ckpt-nonexistent-rig-xyz");
        // Just assert it does not panic over a missing checkout (worktree list fails → logged).
        checkpoint_push_pass(&[missing.clone(), missing]);
    }
}
