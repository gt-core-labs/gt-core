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
//!
//! ## Final drain on shutdown (`gtcore-0179f8`)
//!
//! The periodic pass above only pushes COMMITTED work. On a redeploy, k8s kills the orchd pod (and
//! every in-flight polecat) with a `Recreate` strategy: the agents never reach a commit, so their
//! UNCOMMITTED edits would be lost even though the early push saved what was already committed. The
//! [`drain_pass`] below is the SIGTERM/preStop counterpart: it stages + commits any pending changes
//! in every active worktree (`git add -A` then a `chore(checkpoint)` commit) and then pushes the
//! branch, so nothing committable dies with the pod. The bin runs it once, bounded by a timeout
//! under `terminationGracePeriodSeconds`, before draining the actor stack. Idempotent: a clean
//! worktree only re-pushes (a silent no-op), so a redeploy with no dirty worktrees stays fast.

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

/// Like [`run`] but, on failure, returns stdout AND stderr concatenated — `git commit` writes its
/// "nothing to commit" message to stdout, so the drain must inspect both streams to tell a benign
/// empty-commit race from a real commit failure.
fn run_combined(dir: &Path, argv: &[String]) -> std::io::Result<(bool, String)> {
    let out = Command::new("git").arg("-C").arg(dir).args(argv).output()?;
    let text = if out.status.success() {
        String::from_utf8_lossy(&out.stdout).trim().to_string()
    } else {
        format!(
            "{}{}",
            String::from_utf8_lossy(&out.stdout),
            String::from_utf8_lossy(&out.stderr)
        )
        .trim()
        .to_string()
    };
    Ok((out.status.success(), text))
}

/// The in-flight (worktree-path, branch) pairs from a `git worktree list --porcelain` dump: every
/// LINKED worktree that has a branch checked out, skipping the main worktree (the rig checkout
/// itself — `git` always lists it first). Detached worktrees (no `branch` line) are naturally
/// skipped. Pure, so it is unit-tested against captured porcelain text.
///
/// Skipping the first worktree — rather than hard-coding `main` — means the rig checkout is excluded
/// whatever branch it sits on, and a deduped, deterministic order is returned (a branch checked out
/// in two trees, which `git` forbids anyway, is yielded once). The path is needed to run `git` IN
/// the worktree (status/commit) during the final drain; the periodic push only needs the branch.
pub fn checkpoint_worktrees(porcelain: &str) -> Vec<(PathBuf, String)> {
    let mut seen = BTreeSet::new();
    let mut out = Vec::new();
    // `worktree ` lines delimit blocks; the first block is the main worktree (index 0), skipped.
    let mut block: isize = -1;
    let mut cur_path: Option<PathBuf> = None;
    for line in porcelain.lines() {
        if let Some(path) = line.strip_prefix("worktree ") {
            block += 1;
            cur_path = Some(PathBuf::from(path));
        } else if let Some(refname) = line.strip_prefix("branch ") {
            if block >= 1 {
                if let (Some(path), Some(name)) =
                    (cur_path.clone(), refname.strip_prefix("refs/heads/"))
                {
                    if seen.insert(name.to_string()) {
                        out.push((path, name.to_string()));
                    }
                }
            }
        }
    }
    out
}

/// The in-flight branches to checkpoint — just the branch names from [`checkpoint_worktrees`]. The
/// periodic push pushes from the rig checkout (shared object DB) and never enters the worktree, so
/// it only needs the branch.
pub fn checkpoint_branches(porcelain: &str) -> Vec<String> {
    checkpoint_worktrees(porcelain)
        .into_iter()
        .map(|(_, branch)| branch)
        .collect()
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

// --- Final drain (gtcore-0179f8): commit + push uncommitted worktree work on shutdown ---

/// `git -C <wt> status --porcelain` argv — non-empty stdout ⇒ the worktree has pending changes
/// (staged, unstaged, or untracked) worth committing before the pod dies.
fn status_argv() -> Vec<String> {
    vec!["status".into(), "--porcelain".into()]
}

/// `git -C <wt> add -A` argv — stage every change including new and deleted files, so the drain
/// commit captures the worktree exactly as it stands at shutdown.
fn add_all_argv() -> Vec<String> {
    vec!["add".into(), "-A".into()]
}

/// `git -C <wt> -c user.name=… -c user.email=… commit -m <msg>` argv — the drain checkpoint commit.
/// The identity is pinned on the command (not read from config) so the commit never fails for a
/// missing `user.name`/`user.email` — a pod with `HOME=/tmp` can lose global git config on redeploy,
/// the same way it loses `gh` auth. It marks the commit as a system drain, distinct from the agent's
/// own commits, and lands on the bead branch (never main) where a re-slung polecat continues from it.
fn commit_argv(msg: &str) -> Vec<String> {
    vec![
        "-c".into(),
        "user.name=gt-orchd".into(),
        "-c".into(),
        "user.email=orchd@gt-core.local".into(),
        "commit".into(),
        "-m".into(),
        msg.into(),
    ]
}

/// Drain one worktree: if it has uncommitted changes, stage + commit them; then push the branch to
/// origin (the drain commit plus any earlier commits). Returns `Ok(true)` when a drain commit was
/// made, `Ok(false)` when the worktree was already clean (only the idempotent push ran), `Err` on a
/// git failure. The push reuses [`push_branch`] from the rig checkout — worktrees share its refs, so
/// the freshly-committed branch tip is exactly what gets pushed.
fn drain_worktree(rig: &Path, wt: &Path, branch: &str) -> Result<bool, String> {
    let dirty = match run(wt, &status_argv()) {
        Ok((true, out)) => !out.is_empty(),
        Ok((false, err)) => return Err(format!("git status failed: {err}")),
        Err(e) => return Err(format!("git status error: {e}")),
    };
    let mut committed = false;
    if dirty {
        match run(wt, &add_all_argv()) {
            Ok((true, _)) => {}
            Ok((false, err)) => return Err(format!("git add -A failed: {err}")),
            Err(e) => return Err(format!("git add -A error: {e}")),
        }
        let msg = format!("chore(checkpoint): drain uncommitted work on orchd shutdown ({branch})");
        match run_combined(wt, &commit_argv(&msg)) {
            Ok((true, _)) => committed = true,
            // A race where the changes vanished between status and commit ("nothing to commit") is
            // not fatal — fall through to the push of whatever is already committed.
            Ok((false, err)) if err.contains("nothing to commit") => {}
            Ok((false, err)) => return Err(format!("git commit failed: {err}")),
            Err(e) => return Err(format!("git commit error: {e}")),
        }
    }
    push_branch(rig, branch).map(|()| committed)
}

/// Final-drain every in-flight worktree in `rig`: commit pending changes and push the branch to
/// origin. Returns per-branch outcomes so the caller can log them. Blocking (shells `git`). A failed
/// `worktree list` yields a single error entry with an empty branch name (mirrors
/// [`checkpoint_push_rig`]).
pub fn drain_rig(rig: &Path) -> Vec<(String, Result<bool, String>)> {
    let porcelain = match run(rig, &worktree_list_argv()) {
        Ok((true, out)) => out,
        Ok((false, err)) => {
            return vec![(String::new(), Err(format!("git worktree list failed: {err}")))]
        }
        Err(e) => return vec![(String::new(), Err(format!("git worktree list error: {e}")))],
    };
    checkpoint_worktrees(&porcelain)
        .into_iter()
        .map(|(wt, branch)| {
            let res = drain_worktree(rig, &wt, &branch);
            (branch, res)
        })
        .collect()
}

/// Run one final-drain pass over a set of rig checkouts (deduplicated), committing + pushing every
/// active worktree's pending work and logging each outcome. The bin calls this ONCE from the
/// SIGTERM/preStop path, before shutting down the actor stack — so a redeploy never loses
/// committable work. Blocking — the caller wraps it in `spawn_blocking` under a timeout. Best-effort
/// + idempotent: a clean worktree only re-pushes (a no-op), and a per-branch failure never aborts
/// the pass.
pub fn drain_pass(rigs: &[PathBuf]) {
    let mut seen = BTreeSet::new();
    for rig in rigs {
        if !seen.insert(rig.clone()) {
            continue;
        }
        for (branch, res) in drain_rig(rig) {
            match res {
                Ok(true) => eprintln!(
                    "[drain] {branch} → committed + pushed to origin (rig {})",
                    rig.display()
                ),
                Ok(false) => {
                    if !branch.is_empty() {
                        eprintln!(
                            "[drain] {branch} → clean, re-pushed (rig {})",
                            rig.display()
                        );
                    }
                }
                Err(reason) => eprintln!(
                    "[drain] {} in rig {}: {reason}",
                    if branch.is_empty() {
                        "<list>"
                    } else {
                        branch.as_str()
                    },
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
    fn drain_argv_builders_stage_all_and_pin_commit_identity() {
        assert_eq!(status_argv(), vec!["status", "--porcelain"]);
        assert_eq!(
            add_all_argv(),
            vec!["add", "-A"],
            "drain stages every change incl. untracked + deletions"
        );
        // Identity is pinned on the command so the commit can't fail for missing git config.
        assert_eq!(
            commit_argv("msg"),
            vec![
                "-c",
                "user.name=gt-orchd",
                "-c",
                "user.email=orchd@gt-core.local",
                "commit",
                "-m",
                "msg"
            ],
        );
    }

    #[test]
    fn checkpoint_worktrees_pairs_path_with_branch_and_skips_main() {
        // The drain needs the worktree PATH (to run git inside it), not just the branch.
        let porcelain = "\
worktree /rig
HEAD 1111111111111111111111111111111111111111
branch refs/heads/main

worktree /rig-wt/gt-gtcore-0179f8
HEAD 2222222222222222222222222222222222222222
branch refs/heads/gtcore-0179f8
";
        assert_eq!(
            checkpoint_worktrees(porcelain),
            vec![(
                PathBuf::from("/rig-wt/gt-gtcore-0179f8"),
                "gtcore-0179f8".to_string()
            )],
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

    /// THE BEAD's test (`gtcore-0179f8`): a polecat has UNCOMMITTED edits in its worktree when a
    /// redeploy is about to kill the pod. The final drain must COMMIT those edits and PUSH them to
    /// the bead branch on origin — so no committable work is lost (the early checkpoint-push, which
    /// only pushes already-committed work, would have lost them).
    #[test]
    fn drain_commits_and_pushes_uncommitted_work() {
        let uniq = format!(
            "{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        );
        let root = std::env::temp_dir().join(format!("gt-drain-{uniq}"));
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

        let bead = "gtcore-0179f8";
        let wt = wt_root.join(format!("gt-{bead}"));
        sh(
            &rig,
            &["worktree", "add", "-q", "-b", bead, wt.to_str().unwrap(), "main"],
        );
        // The polecat writes work but NEVER commits it (it is about to be killed). Include an
        // untracked file and an edit to the tracked seed, so `git add -A` is what saves them.
        std::fs::write(wt.join("work.txt"), "uncommitted in-flight work").unwrap();
        std::fs::write(wt.join("seed"), "edited").unwrap();

        // Before the drain: origin has no such branch — the work is invisible (the incident).
        let pre = Command::new("git")
            .arg("-C")
            .arg(&origin)
            .args(["rev-parse", "--verify", "-q", &format!("refs/heads/{bead}")])
            .output()
            .unwrap();
        assert!(!pre.status.success(), "origin must not have the branch yet");

        // The orchd final drain (the agent is being killed; this needs no cooperation).
        drain_pass(&[rig.clone()]);

        // The uncommitted work is now COMMITTED on the bead branch and durable on origin: the tree
        // at origin/<bead> contains both the new file and the edited one.
        assert_eq!(
            sh(&origin, &["show", &format!("refs/heads/{bead}:work.txt")]),
            "uncommitted in-flight work",
            "the untracked file was staged, committed, and pushed",
        );
        assert_eq!(
            sh(&origin, &["show", &format!("refs/heads/{bead}:seed")]),
            "edited",
            "the tracked edit was committed and pushed",
        );
        // The drain commit carries the pinned system identity.
        assert_eq!(
            sh(&origin, &["log", "-1", "--format=%an", &format!("refs/heads/{bead}")]),
            "gt-orchd",
        );
        // The worktree is now clean (everything committed).
        assert!(
            sh(&wt, &["status", "--porcelain"]).is_empty(),
            "drain left the worktree clean",
        );

        // Idempotent: a second drain makes no new commit and the branch is unchanged.
        let tip = sh(&origin, &["rev-parse", &format!("refs/heads/{bead}")]);
        drain_pass(&[rig.clone()]);
        assert_eq!(
            sh(&origin, &["rev-parse", &format!("refs/heads/{bead}")]),
            tip,
            "a clean re-drain is a no-op",
        );

        let _ = std::fs::remove_dir_all(&root);
    }

    /// Happy path: a clean worktree (all work already committed) drains as a silent no-op — the
    /// branch is re-pushed (idempotent) and no empty drain commit is created. Guards the AC that a
    /// shutdown with nothing pending stays fast and does not pollute history.
    #[test]
    fn drain_on_clean_worktree_makes_no_commit() {
        let uniq = format!(
            "{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        );
        let root = std::env::temp_dir().join(format!("gt-drain-clean-{uniq}"));
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

        let bead = "gtcore-clean";
        let wt = wt_root.join(format!("gt-{bead}"));
        sh(
            &rig,
            &["worktree", "add", "-q", "-b", bead, wt.to_str().unwrap(), "main"],
        );
        // Commit the work properly — the worktree is clean at drain time.
        std::fs::write(wt.join("done.txt"), "already committed").unwrap();
        sh(&wt, &["add", "."]);
        sh(&wt, &["commit", "-qm", "feat: committed work"]);
        let tip = sh(&wt, &["rev-parse", "HEAD"]);

        drain_pass(&[rig.clone()]);

        // No drain commit was layered on top — HEAD is still the agent's own commit, now on origin.
        assert_eq!(sh(&wt, &["rev-parse", "HEAD"]), tip, "no empty drain commit");
        assert_eq!(
            sh(&origin, &["rev-parse", &format!("refs/heads/{bead}")]),
            tip,
            "the committed work was pushed",
        );

        let _ = std::fs::remove_dir_all(&root);
    }
}
