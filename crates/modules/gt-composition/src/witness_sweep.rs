//! Witness completion safety-net (`hq-orchd-deploy.6`): the "dog" that catches a polecat which
//! finished its bead but crashed before self-signaling merge-ready.
//!
//! ## The minimal dog set (the decision this bead records)
//!
//! gt-core's autonomous loop needs far fewer supervisor agents than gastown's full menagerie
//! (witness / refinery / deacon / overseer / sheriff / mayor). What is already running in the
//! daemon covers the happy path:
//!
//! - **refinery** — [`gt_merge::refinery::run`], a channel consumer loop, already in the bin.
//! - **patrol** lease-expiry ticker — re-enqueues a dispatched-but-dead bead.
//! - **sheriff** — [`gt_sheriff::SheriffPlugin`] in `daemon_root`, a kind-only watchdog that
//!   observes EVERY hub kind (so it already reacts to `merge.ready.v1` / `merge.failed.v1`).
//! - **git-merge edge** ([`crate::git_merge`], `hq-orchd-deploy.12`) — lands the branch on main.
//! - **polecat supervisor** — re-slings a dead tmux polecat.
//!
//! The one genuine GAP for a safe rollout is the **witness**: gt-core polecats self-signal
//! merge-ready from their Stop hook (`hq-orchd-deploy.20`), so a polecat that finishes its work
//! but dies before the hook fires (OOM, kill, tmux crash) leaves a committed branch that NOTHING
//! ever merges. This module is the safety-net for exactly that — modeled on gastown's witness
//! `DiscoverCompletions` ("discover, don't track": observe bead/branch state each cycle, not a
//! mailbox). deacon (drain bookkeeping), mayor (convoy scan), and overseer are NOT needed for the
//! merge loop and are deferred.
//!
//! ## How it works
//!
//! Each tick [`WitnessSweep::sweep`] scans the per-polecat worktree root (`/rig-wt/<session>`,
//! [`crate::worktree`]). For each worktree it asks three questions and emits merge-ready only when
//! all three hold (see [`should_emit`]):
//!
//! 1. **Did it do work?** `git rev-list --count origin/main..HEAD > 0` — the branch is ahead of
//!    main, i.e. the polecat committed.
//! 2. **Is it done?** the tmux session is gone OR its heartbeat is stale — the polecat is no longer
//!    running (a live, healthy polecat is left alone; it will signal itself).
//! 3. **Was it missed?** no merge slot exists for the bead yet — the self-signal (or a prior sweep)
//!    did not already put it in the pipeline.
//!
//! It then emits a `{bead,branch}` message into the same merge-ready [`Channel`] the polecat Stop
//! hook writes to, so the refinery is the single submit path. Branch = bead = the worktree's
//! checked-out branch (CLAUDE.md `-b <bead-id>`). Idempotent across ticks: once emitted, the
//! refinery submits a slot, and the next sweep's question 3 short-circuits it; the merge board's
//! own duplicate-submit guard covers the brief window before that.

use std::path::{Path, PathBuf};
use std::process::Command;
use std::sync::Arc;
use std::time::Duration;

use gt_channel::Channel;
use gt_merge::actor::MergeHandle;
use gt_merge::MergeReadyPayload;
use gt_polecat::tmux::Tmux;

/// `git -C <rig> fetch origin` argv — refresh the remote-tracking `origin/main` so the per-worktree
/// ahead-count compares against the live tip (data, asserted in tests).
fn fetch_argv() -> Vec<String> {
    vec!["fetch".into(), "origin".into()]
}

/// `git -C <worktree> rev-parse --abbrev-ref HEAD` argv — the checked-out branch (= bead id).
fn branch_argv() -> Vec<String> {
    vec!["rev-parse".into(), "--abbrev-ref".into(), "HEAD".into()]
}

/// `git -C <worktree> rev-list --count origin/main..HEAD` argv — commits the branch is ahead of
/// main (0 ⇒ no work done by this polecat).
fn ahead_argv() -> Vec<String> {
    vec![
        "rev-list".into(),
        "--count".into(),
        "origin/main..HEAD".into(),
    ]
}

/// Run `git -C <dir> <argv...>`, returning trimmed stdout on success.
fn git(dir: &Path, argv: &[String]) -> Option<String> {
    let out = Command::new("git")
        .arg("-C")
        .arg(dir)
        .args(argv)
        .output()
        .ok()?;
    out.status
        .success()
        .then(|| String::from_utf8_lossy(&out.stdout).trim().to_string())
}

/// The witness decision (`hq-orchd-deploy.6`): emit merge-ready iff the polecat did work
/// (`ahead > 0`), is no longer running (`!alive`), and was not already put in the pipeline
/// (`!slotted`). Pure, so the safety-net logic is unit-tested without git/tmux.
fn should_emit(ahead: u32, alive: bool, slotted: bool) -> bool {
    ahead > 0 && !alive && !slotted
}

/// The witness completion safety-net. Holds the worktree root it scans, the rig checkout it
/// refreshes `origin/main` from, the liveness sources (tmux + heartbeat staleness window), the
/// merge handle (to dedup against the live board), and the merge-ready channel it emits into.
pub struct WitnessSweep {
    /// Per-polecat worktree root (`GT_POLECAT_WORKTREE_ROOT`, e.g. `/rig-wt`).
    worktree_root: PathBuf,
    /// Rig checkout (`GT_RIG_PATH`) — fetched once per sweep to refresh `origin/main`.
    rig: PathBuf,
    /// Heartbeat directory (`GT_HEARTBEAT_DIR`); a session's file is `<dir>/<session>.heartbeat`.
    heartbeat_dir: PathBuf,
    /// A heartbeat older than this counts the polecat as dead (alongside a missing tmux session).
    stale_after: Duration,
    /// tmux probe — `has_session(session)` ⇒ the polecat is still up.
    tmux: Arc<dyn Tmux>,
    /// Merge command handle — its board snapshot is the dedup source.
    merge: MergeHandle,
    /// The merge-ready channel (same one the polecat Stop hook + the refinery use).
    channel: Channel,
}

impl WitnessSweep {
    /// Wire the witness with every source it observes.
    pub fn new(
        worktree_root: PathBuf,
        rig: PathBuf,
        heartbeat_dir: PathBuf,
        stale_after: Duration,
        tmux: Arc<dyn Tmux>,
        merge: MergeHandle,
        channel: Channel,
    ) -> Self {
        Self {
            worktree_root,
            rig,
            heartbeat_dir,
            stale_after,
            tmux,
            merge,
            channel,
        }
    }

    /// Scan the worktrees once and emit merge-ready for every completed-but-unsignaled polecat.
    /// Returns the number of merge-ready messages emitted this sweep. Best-effort throughout: a
    /// worktree that fails any git probe is skipped, never aborting the sweep.
    pub async fn sweep(&self) -> usize {
        // Refresh origin/main once so the ahead-count is against the live tip. A fetch failure
        // (transient network) just means we compare against the last-known tip — still useful.
        let _ = git(&self.rig, &fetch_argv());

        let Ok(entries) = std::fs::read_dir(&self.worktree_root) else {
            return 0;
        };

        // The live board: a bead already here (Ready/Merging/…) was signaled — skip it.
        let slotted: std::collections::HashSet<String> = self
            .merge
            .snapshot()
            .await
            .into_iter()
            .map(|s| s.bead)
            .collect();

        let mut emitted = 0;
        for entry in entries.flatten() {
            let wt = entry.path();
            if !wt.is_dir() {
                continue;
            }
            let Some(session) = entry.file_name().to_str().map(|s| s.to_string()) else {
                continue;
            };

            // Branch = bead. A detached HEAD ("HEAD") or unreadable tree is not a polecat branch.
            let Some(branch) = git(&wt, &branch_argv()).filter(|b| b != "HEAD" && !b.is_empty())
            else {
                continue;
            };
            let ahead: u32 = git(&wt, &ahead_argv())
                .and_then(|s| s.parse().ok())
                .unwrap_or(0);

            let hb = self.heartbeat_dir.join(format!("{session}.heartbeat"));
            let alive = self.tmux.has_session(&session)
                && !gt_polecat::lifecycle::heartbeat_is_stale(&hb, self.stale_after);

            if !should_emit(ahead, alive, slotted.contains(&branch)) {
                continue;
            }

            let payload = MergeReadyPayload {
                bead: branch.clone(),
                branch: branch.clone(),
            };
            match serde_json::to_vec(&payload) {
                Ok(bytes) => match self.channel.emit(&bytes) {
                    Ok(_) => {
                        eprintln!(
                            "[witness] {branch}: completed without merge-ready (ahead {ahead}, polecat gone) — emitted merge-ready"
                        );
                        emitted += 1;
                    }
                    Err(e) => eprintln!("[witness] {branch}: merge-ready emit failed: {e}"),
                },
                Err(e) => eprintln!("[witness] {branch}: merge-ready encode failed: {e}"),
            }
        }
        emitted
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn argv_builders() {
        assert_eq!(fetch_argv(), vec!["fetch", "origin"]);
        assert_eq!(branch_argv(), vec!["rev-parse", "--abbrev-ref", "HEAD"]);
        assert_eq!(
            ahead_argv(),
            vec!["rev-list", "--count", "origin/main..HEAD"]
        );
    }

    #[test]
    fn emit_only_when_done_with_work_and_unsignaled() {
        // The safety-net fires for a polecat that committed (ahead) and is gone (!alive) and was
        // missed (!slotted).
        assert!(
            should_emit(1, false, false),
            "committed + dead + unsignaled ⇒ emit"
        );
        // Still running ⇒ leave it; it will self-signal.
        assert!(
            !should_emit(3, true, false),
            "live polecat is not the witness's job"
        );
        // No commits ⇒ nothing to merge.
        assert!(!should_emit(0, false, false), "no work ⇒ no merge-ready");
        // Already in the pipeline (self-signaled, or a prior sweep) ⇒ do not double-emit.
        assert!(!should_emit(2, false, true), "already slotted ⇒ skip");
    }
}
