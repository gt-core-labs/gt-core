//! Branch-GC reactor proof (epic hq-branch-gc): [`BranchGcPlugin`] reaps a delivered branch when
//! it observes `merge.merged.v1`, resolving the branch from the live merge board (the event omits
//! it). Covers the happy path, idempotency on a replayed `Merged`, and that unrelated event kinds
//! are ignored.

use std::sync::Arc;

use gt_composition::BranchGcPlugin;
use gt_eventlog::EventRecord;
use gt_merge::{InMemoryBranchReaper, InMemoryMergeRepo, MergeEvent};
use gt_plugin::Plugin;
use tokio::sync::mpsc;

fn record(kind: &str, payload: serde_json::Value) -> EventRecord {
    EventRecord {
        event_id: "e".into(),
        correlation_id: "c".into(),
        causation_id: None,
        ts: "2026-06-05T12:00:00Z".into(),
        kind: kind.into(),
        payload,
    }
}

#[tokio::test]
async fn reaps_delivered_branch_on_merged() {
    // Spawn a real merge actor and drive a bead Ready → Merging → Merged so the board holds a
    // Merged slot carrying its branch. The relay rx is kept alive (capacity covers the 3 events).
    let (tx, _rx) = mpsc::channel(64);
    let merge = gt_merge::actor::spawn(InMemoryMergeRepo::default(), tx);
    merge.submit("gg-9", "wt/gg-9", "01ABC").await;
    merge.start("gg-9").await;
    merge.complete("gg-9", "deadbeef").await;

    let reaper = Arc::new(InMemoryBranchReaper::new());
    let plugin = BranchGcPlugin::new(merge.clone(), reaper.clone());

    let merged = record(
        "merge.merged.v1",
        serde_json::to_value(MergeEvent::Merged {
            bead: "gg-9".into(),
            sha: "deadbeef".into(),
        })
        .unwrap(),
    );

    // The plugin's internal `merge.snapshot()` is queued behind the three transitions above, so
    // it observes the Merged slot and resolves `wt/gg-9` without an explicit drain.
    plugin.on_event(&merged).await.unwrap();
    assert_eq!(
        reaper.reaped(),
        vec!["wt/gg-9".to_string()],
        "delivery reaps the slot's branch",
    );

    // Idempotent: a replayed Merged (boot hydration re-emitting) drops nothing the second time.
    plugin.on_event(&merged).await.unwrap();
    assert_eq!(
        reaper.reaped(),
        vec!["wt/gg-9".to_string()],
        "second Merged is a no-op",
    );

    // Unrelated kinds are ignored.
    let ready = record(
        "merge.ready.v1",
        serde_json::to_value(MergeEvent::Ready {
            bead: "gg-9".into(),
            branch: "wt/gg-9".into(),
            channel_msg_id: "01ABC".into(),
        })
        .unwrap(),
    );
    plugin.on_event(&ready).await.unwrap();
    assert_eq!(reaper.reaped().len(), 1, "ready event triggers no reap");
}

#[tokio::test]
async fn unknown_bead_reaps_nothing() {
    let (tx, _rx) = mpsc::channel(64);
    let merge = gt_merge::actor::spawn(InMemoryMergeRepo::default(), tx);
    let reaper = Arc::new(InMemoryBranchReaper::new());
    let plugin = BranchGcPlugin::new(merge.clone(), reaper.clone());

    // No slot was ever submitted for this bead → nothing to resolve, stay silent.
    let merged = record(
        "merge.merged.v1",
        serde_json::to_value(MergeEvent::Merged {
            bead: "ghost".into(),
            sha: "0".into(),
        })
        .unwrap(),
    );
    plugin.on_event(&merged).await.unwrap();
    assert!(reaper.reaped().is_empty(), "no board slot → no reap");
}
