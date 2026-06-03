//! The reaper cycle — port of `internal/reaper` compaction, expressed over the
//! [`WispRepository`] port instead of raw SQL.
//!
//! One pass: **scan** open wisps, **reap** (close) those past their kind's TTL that carry no
//! durable-value signal, and **purge** (delete) closed wisps older than `purge_age`. Wisps
//! with durable value (discussed / referenced / `gt:keep`) are *preserved* and reported as
//! promote candidates rather than closed — the composition root turns those into beads.
//!
//! Idempotent by construction: reaping is the terminal `Open → Closed` transition, so a
//! second pass finds nothing open past TTL and reports `reaped == 0` ("no duplica"). The
//! cycle only touches the wisps compaction table — never the event log or domain reducers —
//! so it is replay-safe: replaying the audit log reconstructs identical state whether or not
//! a reaper pass ran.

use time::{Duration, OffsetDateTime};

use gt_events::AppError;

use crate::kind::WispKind;
use crate::promotion::{durable_value, PromotionReason};
use crate::repo::WispRepository;

/// A past-TTL wisp preserved for promotion to a permanent bead instead of being reaped.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize)]
pub struct PromoteCandidate {
    pub id: String,
    pub kind: WispKind,
    pub reason: PromotionReason,
}

/// Outcome of one reaper pass. Counts are *transitions this pass*, not totals, so a steady
/// state reports all zeros.
#[derive(Debug, Clone, Default, PartialEq, Eq, serde::Serialize)]
pub struct ReapSummary {
    /// Open wisps examined.
    pub scanned: usize,
    /// `Open → Closed` transitions performed.
    pub reaped: usize,
    /// Closed rows deleted.
    pub purged: usize,
    /// Past-TTL wisps preserved for promotion (not reaped).
    pub promoted: Vec<PromoteCandidate>,
    /// True when no mutations were applied (reported only).
    pub dry_run: bool,
}

/// Run one scan → reap → purge cycle against `repo`.
///
/// `now` is injected (not read from the clock) so the cycle is deterministic and testable.
/// `purge_age` is how long a wisp stays closed before deletion. With `dry_run`, candidates
/// are counted but no row is mutated.
pub async fn run<R: WispRepository>(
    repo: &R,
    now: OffsetDateTime,
    purge_age: Duration,
    dry_run: bool,
) -> Result<ReapSummary, AppError> {
    let mut summary = ReapSummary {
        dry_run,
        ..Default::default()
    };

    // Scan + reap: close open wisps past their kind's TTL, preserving durable-value ones.
    let open = repo.list_open().await?;
    summary.scanned = open.len();
    for wisp in open {
        if !wisp.is_past_ttl(now) {
            continue;
        }
        if let Some(reason) = durable_value(&wisp) {
            summary.promoted.push(PromoteCandidate {
                id: wisp.id,
                kind: wisp.kind,
                reason,
            });
            continue;
        }
        if dry_run {
            summary.reaped += 1;
        } else if repo.mark_reaped(&wisp.id, now).await? {
            summary.reaped += 1;
        }
    }

    // Purge: delete closed wisps that have aged past purge_age.
    let cutoff = now - purge_age;
    let stale_closed = repo.list_closed_before(cutoff).await?;
    for wisp in stale_closed {
        if dry_run {
            summary.purged += 1;
        } else if repo.purge(&wisp.id).await? {
            summary.purged += 1;
        }
    }

    Ok(summary)
}
