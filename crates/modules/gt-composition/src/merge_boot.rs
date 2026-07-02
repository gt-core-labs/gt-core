//! Boot reconciliation of the merge board (gtcore-088db9).
//!
//! The refinery ([`gt_merge::refinery`]) only consumes the `MERGE_READY` channel: it turns a
//! fresh merge-ready signal into a `Ready` slot. Nothing re-drives a slot that was *already*
//! `Ready` or `Merging` when orchd restarted — boot hydration ([`gt_merge::actor::spawn_hydrated`])
//! seeds those slots into the actor board **silently** (no `merge.ready.v1`/`merge.started.v1`
//! re-emit), so the [`crate::MergePlugin`] reactor arm and the [`crate::git_merge::GitMergePlugin`]
//! edge never fire for them. The result, seen repeatedly in the 2026-06-25 incidents: a `Ready`
//! P0 slot stuck ~50 min, `Merging` orphans lingering >24h, and a phantom slot alive 5+ days.
//!
//! This module scans the hydrated board once at boot and heals it in two phases, split by *when*
//! the live edge + sheriff are wired onto the hub:
//!
//! - [`settle_delivered_slots`] — **before** the git-merge edge is subscribed. Any non-`Merged`
//!   slot whose bead is already `closed` with a `delivered_sha` is walked to `Merged` through the
//!   legal state machine with **no git attempt** (the boot-time equivalent of gtcore-71c575's
//!   `try_complete_slot` applied to the whole board). Running this before the edge is live is what
//!   keeps it from shelling a real merge for work that already landed (the gtcore-4ad682 hazard).
//! - [`reconcile_inflight_slots`] — **after** the edge + sheriff are live. A `Ready` slot is
//!   re-enqueued (driven to `Merging` so [`crate::git_merge::GitMergePlugin`] lands it); a
//!   `Merging` orphan is completed if its branch already reached `origin/main` with delivery
//!   evidence, else failed so the sheriff / polecat supervisor recover it.
//!
//! Both take their side-effecting lookups (bead close-state, git ancestry) as `async` closures, so
//! the reconciliation logic is unit-tested against an in-memory merge actor with no Dolt or git.

use std::future::Future;

use gt_merge::actor::MergeHandle;
use gt_merge::MergeSlotState;

/// Phase 1 — settle slots whose bead is already delivered.
///
/// For every non-`Merged` slot, `delivered(bead)` returns `Some(sha)` when that bead is `closed`
/// with a non-empty `delivered_sha`; such a slot is driven to `Merged` with `sha`, **without any
/// git merge**. A `Failed` slot is first re-submitted (reset to `Ready`) so it can walk the queue,
/// mirroring gtcore-71c575's `try_complete_slot`. Returns the number of slots healed.
///
/// Must run before the git-merge edge subscribes to the hub: the `start`/`complete` it emits then
/// have no live [`crate::git_merge::GitMergePlugin`] to trigger a real (and possibly duplicating)
/// merge for work that already landed.
pub async fn settle_delivered_slots<F, Fut>(merge: &MergeHandle, delivered: F) -> usize
where
    F: Fn(String) -> Fut,
    Fut: Future<Output = Option<String>>,
{
    let mut healed = 0usize;
    for slot in merge.snapshot().await {
        if slot.state == MergeSlotState::Merged {
            continue;
        }
        let Some(sha) = delivered(slot.bead.clone()).await else {
            continue;
        };
        match slot.state {
            MergeSlotState::Failed => {
                // Reset the terminal Failed slot back into the queue, then walk it forward.
                merge
                    .submit(
                        slot.bead.clone(),
                        slot.branch.clone(),
                        format!("boot-reconcile-{}", slot.bead),
                    )
                    .await;
                merge.start(slot.bead.clone()).await;
            }
            MergeSlotState::Ready => merge.start(slot.bead.clone()).await,
            MergeSlotState::Merging => {}
            MergeSlotState::Merged => continue,
        }
        merge.complete(slot.bead.clone(), sha).await;
        eprintln!(
            "[merge-boot] {}: bead closed+delivered → slot auto-completed at boot (no merge)",
            slot.bead
        );
        healed += 1;
    }
    healed
}

/// Phase 2 — reconcile the in-flight slots left after [`settle_delivered_slots`].
///
/// - A `Ready` slot is re-enqueued via `start` (→ `Merging`), so the now-live git-merge edge lands
///   it — this is what a restart-lost `merge.ready.v1` would otherwise have driven.
/// - A `Merging` orphan is resolved by `on_main(bead, branch)`: `Some(sha)` (the branch already
///   reached `origin/main` with delivery evidence) → `complete`; `None` → `fail`, so the sheriff /
///   supervisor recover the slot.
///
/// Must run after the edge + sheriff are subscribed, so a re-enqueued `Ready` actually merges and a
/// `fail` actually triggers recovery. Returns `(re-enqueued Ready count, reconciled Merging count)`.
pub async fn reconcile_inflight_slots<A, Fut>(merge: &MergeHandle, on_main: A) -> (usize, usize)
where
    A: Fn(String, String) -> Fut,
    Fut: Future<Output = Option<String>>,
{
    let mut requeued = 0usize;
    let mut orphans = 0usize;
    for slot in merge.snapshot().await {
        match slot.state {
            MergeSlotState::Ready => {
                merge.start(slot.bead.clone()).await;
                eprintln!("[merge-boot] {}: Ready slot re-enqueued at boot → merging", slot.bead);
                requeued += 1;
            }
            MergeSlotState::Merging => {
                orphans += 1;
                match on_main(slot.bead.clone(), slot.branch.clone()).await {
                    Some(sha) => {
                        eprintln!(
                            "[merge-boot] {}: orphaned merging slot — branch already on main ({sha}) → completing",
                            slot.bead
                        );
                        merge.complete(slot.bead.clone(), sha).await;
                    }
                    None => {
                        eprintln!(
                            "[merge-boot] {}: orphaned merging slot — branch not on main → failing (recovery)",
                            slot.bead
                        );
                        merge
                            .fail(
                                slot.bead.clone(),
                                "orchd restart — orphaned merging slot; branch not on main".to_string(),
                            )
                            .await;
                    }
                }
            }
            _ => {}
        }
    }
    (requeued, orphans)
}

#[cfg(test)]
mod tests {
    use super::*;
    use gt_merge::{InMemoryMergeRepo, MergeSlotState};

    /// Spawn a real merge actor over an in-memory repo, drop the event relay (we assert board
    /// state via snapshot, not the log). The board is seeded by driving the actor, mirroring how
    /// boot hydration seeds it.
    fn actor() -> MergeHandle {
        let (tx, _rx) = tokio::sync::mpsc::channel(64);
        gt_merge::actor::spawn(InMemoryMergeRepo::default(), tx)
    }

    async fn state_of(merge: &MergeHandle, bead: &str) -> Option<MergeSlotState> {
        merge
            .snapshot()
            .await
            .into_iter()
            .find(|s| s.bead == bead)
            .map(|s| s.state)
    }

    #[tokio::test]
    async fn settle_completes_ready_and_merging_delivered_beads() {
        let merge = actor();
        // A Ready slot and a Merging slot, both for beads that already closed with a sha.
        merge.submit("b-ready", "b-ready", "01").await;
        merge.submit("b-merging", "b-merging", "02").await;
        merge.start("b-merging").await;
        // An in-flight slot whose bead is NOT delivered — must be left untouched by phase 1.
        merge.submit("b-open", "b-open", "03").await;

        let healed = settle_delivered_slots(&merge, |bead| async move {
            match bead.as_str() {
                "b-ready" => Some("sha-ready".to_string()),
                "b-merging" => Some("sha-merging".to_string()),
                _ => None, // b-open: bead still open, no delivered_sha
            }
        })
        .await;

        assert_eq!(healed, 2);
        assert_eq!(state_of(&merge, "b-ready").await, Some(MergeSlotState::Merged));
        assert_eq!(state_of(&merge, "b-merging").await, Some(MergeSlotState::Merged));
        // The undelivered slot is still Ready — phase 1 never touched it.
        assert_eq!(state_of(&merge, "b-open").await, Some(MergeSlotState::Ready));
    }

    #[tokio::test]
    async fn settle_resets_failed_delivered_bead_to_merged() {
        let merge = actor();
        merge.submit("b1", "b1", "01").await;
        merge.start("b1").await;
        merge.fail("b1", "ci red").await;
        assert_eq!(state_of(&merge, "b1").await, Some(MergeSlotState::Failed));

        let healed =
            settle_delivered_slots(&merge, |_| async { Some("landed-sha".to_string()) }).await;

        assert_eq!(healed, 1);
        assert_eq!(state_of(&merge, "b1").await, Some(MergeSlotState::Merged));
    }

    #[tokio::test]
    async fn reconcile_reenqueues_ready_slot() {
        // Criterion 1: a Ready slot at boot is re-enqueued (→ Merging) with no new merge-ready event.
        let merge = actor();
        merge.submit("b1", "b1", "01").await;
        assert_eq!(state_of(&merge, "b1").await, Some(MergeSlotState::Ready));

        let (requeued, orphans) =
            reconcile_inflight_slots(&merge, |_, _| async { None }).await;

        assert_eq!((requeued, orphans), (1, 0));
        assert_eq!(state_of(&merge, "b1").await, Some(MergeSlotState::Merging));
    }

    #[tokio::test]
    async fn reconcile_completes_merging_orphan_already_on_main() {
        // Criterion 2a: a Merging orphan whose branch already landed → completed with main's sha.
        let merge = actor();
        merge.submit("b1", "b1", "01").await;
        merge.start("b1").await;

        let (requeued, orphans) = reconcile_inflight_slots(&merge, |bead, branch| async move {
            assert_eq!(bead, "b1");
            assert_eq!(branch, "b1");
            Some("main-sha".to_string())
        })
        .await;

        assert_eq!((requeued, orphans), (0, 1));
        assert_eq!(state_of(&merge, "b1").await, Some(MergeSlotState::Merged));
    }

    #[tokio::test]
    async fn reconcile_fails_merging_orphan_not_on_main() {
        // Criterion 2b: a Merging orphan whose branch is NOT on main → failed (recovery path).
        let merge = actor();
        merge.submit("b1", "b1", "01").await;
        merge.start("b1").await;

        let (requeued, orphans) =
            reconcile_inflight_slots(&merge, |_, _| async { None }).await;

        assert_eq!((requeued, orphans), (0, 1));
        assert_eq!(state_of(&merge, "b1").await, Some(MergeSlotState::Failed));
    }

    #[tokio::test]
    async fn full_boot_sequence_heals_a_mixed_board() {
        // An integration-style restart: hydrate a board with one slot of every in-flight shape,
        // then run phase 1 (settle delivered) followed by phase 2 (reconcile in-flight) and assert
        // the whole board reaches a healthy terminal/queued state.
        let merge = actor();
        merge.submit("delivered-ready", "delivered-ready", "01").await; // closed+delivered, Ready
        merge.submit("delivered-merging", "delivered-merging", "02").await;
        merge.start("delivered-merging").await; // closed+delivered, Merging
        merge.submit("pending-ready", "pending-ready", "03").await; // genuinely pending, Ready
        merge.submit("orphan-landed", "orphan-landed", "04").await;
        merge.start("orphan-landed").await; // Merging orphan, branch on main
        merge.submit("orphan-lost", "orphan-lost", "05").await;
        merge.start("orphan-lost").await; // Merging orphan, branch NOT on main

        let delivered: std::collections::HashSet<&str> =
            ["delivered-ready", "delivered-merging"].into_iter().collect();
        let healed = settle_delivered_slots(&merge, |bead| {
            let hit = delivered.contains(bead.as_str());
            async move { hit.then(|| format!("sha-{bead}")) }
        })
        .await;
        assert_eq!(healed, 2);

        let landed: std::collections::HashSet<&str> = ["orphan-landed"].into_iter().collect();
        let (requeued, orphans) = reconcile_inflight_slots(&merge, |bead, _| {
            let hit = landed.contains(bead.as_str());
            async move { hit.then(|| "main-sha".to_string()) }
        })
        .await;
        // Only pending-ready remains Ready for phase 2 to re-enqueue; two Merging orphans reconciled.
        assert_eq!((requeued, orphans), (1, 2));

        assert_eq!(state_of(&merge, "delivered-ready").await, Some(MergeSlotState::Merged));
        assert_eq!(state_of(&merge, "delivered-merging").await, Some(MergeSlotState::Merged));
        assert_eq!(state_of(&merge, "pending-ready").await, Some(MergeSlotState::Merging));
        assert_eq!(state_of(&merge, "orphan-landed").await, Some(MergeSlotState::Merged));
        assert_eq!(state_of(&merge, "orphan-lost").await, Some(MergeSlotState::Failed));
    }
}
