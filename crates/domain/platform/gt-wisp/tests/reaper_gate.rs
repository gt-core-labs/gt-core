//! Gate (paso 9.C / hq-t9vt): seed a heartbeat wisp past TTL → reaper compacts it → a
//! repeat pass does not re-compact (idempotent, "no duplica"). Plus the preserve and purge
//! edges. Runs against `InMemoryWispRepo`; the same contract holds for `DoltWisp`.

use time::{Duration, OffsetDateTime};

use gt_wisp::{reaper, InMemoryWispRepo, PromotionReason, Wisp, WispKind, WispRepository, WispStatus};

const PURGE_AGE: Duration = Duration::days(7);

fn past_ttl_heartbeat(id: &str, now: OffsetDateTime) -> Wisp {
    // Heartbeat TTL is 24h; created 25h ago is comfortably past it.
    Wisp::open(id, WispKind::Heartbeat, now - Duration::hours(25))
}

#[tokio::test]
async fn reaper_compacts_then_repeat_is_noop() {
    let repo = InMemoryWispRepo::new();
    let now = OffsetDateTime::now_utc();
    repo.upsert(&past_ttl_heartbeat("w-hb", now)).await.unwrap();

    // First pass closes the stale heartbeat.
    let first = reaper::run(&repo, now, PURGE_AGE, false).await.unwrap();
    assert_eq!(first.scanned, 1);
    assert_eq!(first.reaped, 1);
    assert_eq!(first.purged, 0);
    assert!(first.promoted.is_empty());

    let w = repo.get("w-hb").await.unwrap().unwrap();
    assert_eq!(w.status, WispStatus::Closed);
    assert!(w.closed_at.is_some());

    // Second pass: nothing open past TTL → no duplicate work.
    let second = reaper::run(&repo, now, PURGE_AGE, false).await.unwrap();
    assert_eq!(second.scanned, 0, "closed wisp no longer scanned as open");
    assert_eq!(second.reaped, 0, "no duplica — reaping is idempotent");
    assert_eq!(second.purged, 0);
}

#[tokio::test]
async fn durable_value_wisp_is_preserved_not_reaped() {
    let repo = InMemoryWispRepo::new();
    let now = OffsetDateTime::now_utc();

    let mut kept = past_ttl_heartbeat("w-keep", now);
    kept.has_keep_label = true;
    repo.upsert(&kept).await.unwrap();

    let summary = reaper::run(&repo, now, PURGE_AGE, false).await.unwrap();
    assert_eq!(summary.reaped, 0, "kept wisp must not be reaped");
    assert_eq!(summary.promoted.len(), 1);
    assert_eq!(summary.promoted[0].id, "w-keep");
    assert_eq!(summary.promoted[0].reason, PromotionReason::KeepLabel);

    // Still open — preservation leaves the row for the composition root to promote.
    assert_eq!(
        repo.get("w-keep").await.unwrap().unwrap().status,
        WispStatus::Open
    );
}

#[tokio::test]
async fn within_ttl_wisp_is_left_alone() {
    let repo = InMemoryWispRepo::new();
    let now = OffsetDateTime::now_utc();
    // Heartbeat created 1h ago — well within its 24h TTL.
    repo.upsert(&Wisp::open("w-fresh", WispKind::Heartbeat, now - Duration::hours(1)))
        .await
        .unwrap();

    let summary = reaper::run(&repo, now, PURGE_AGE, false).await.unwrap();
    assert_eq!(summary.scanned, 1);
    assert_eq!(summary.reaped, 0);
}

#[tokio::test]
async fn purge_deletes_only_old_closed_wisps() {
    let repo = InMemoryWispRepo::new();
    let now = OffsetDateTime::now_utc();

    // Closed 8 days ago → past the 7d purge age.
    let mut old = Wisp::open("w-old", WispKind::GcReport, now - Duration::days(40));
    old.status = WispStatus::Closed;
    old.closed_at = Some(now - Duration::days(8));
    repo.upsert(&old).await.unwrap();

    // Closed 1 day ago → still within purge age.
    let mut recent = Wisp::open("w-recent", WispKind::GcReport, now - Duration::days(40));
    recent.status = WispStatus::Closed;
    recent.closed_at = Some(now - Duration::days(1));
    repo.upsert(&recent).await.unwrap();

    let summary = reaper::run(&repo, now, PURGE_AGE, false).await.unwrap();
    assert_eq!(summary.purged, 1);
    assert!(repo.get("w-old").await.unwrap().is_none(), "old closed purged");
    assert!(repo.get("w-recent").await.unwrap().is_some(), "recent kept");
}

#[tokio::test]
async fn dry_run_counts_but_does_not_mutate() {
    let repo = InMemoryWispRepo::new();
    let now = OffsetDateTime::now_utc();
    repo.upsert(&past_ttl_heartbeat("w-hb", now)).await.unwrap();

    let summary = reaper::run(&repo, now, PURGE_AGE, true).await.unwrap();
    assert_eq!(summary.reaped, 1, "counts the candidate");
    assert!(summary.dry_run);
    assert_eq!(
        repo.get("w-hb").await.unwrap().unwrap().status,
        WispStatus::Open,
        "dry run leaves the wisp open"
    );
}

#[tokio::test]
async fn per_kind_ttl_table_matches_roadmap() {
    // Forensic kinds kept ~14d; liveness kinds on the 24h baseline; patrol 48h.
    assert_eq!(WispKind::Heartbeat.default_ttl(), Duration::hours(24));
    assert_eq!(WispKind::Patrol.default_ttl(), Duration::hours(48));
    assert_eq!(WispKind::GcReport.default_ttl(), Duration::hours(24));
    assert_eq!(WispKind::Error.default_ttl(), Duration::hours(336));
    assert_eq!(WispKind::Escalation.default_ttl(), Duration::hours(336));

    // Wire forms round-trip and match the Go wisp_type column values.
    for kind in WispKind::ALL {
        assert_eq!(WispKind::parse(kind.as_str()).unwrap(), kind);
    }
    assert!(WispKind::parse("bogus").is_err());
}
