//! Promotion criteria — port of `internal/wisp/promotion.go`.
//!
//! A wisp that has proven value should become a permanent bead rather than be compacted
//! away. The reaper consults [`durable_value`] to decide preserve-vs-reap; the standalone
//! [`should_promote`] keeps the full Go criterion set (including past-TTL) for a compactor
//! that ranks promote candidates. The actual wisp→bead write is a composition-root concern:
//! this crate only classifies, so the domain stays free of a bead-store dependency.

use time::OffsetDateTime;

use crate::wisp::Wisp;

/// Why a wisp qualifies for promotion. First matching reason wins (stable for logging).
#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize)]
#[serde(rename_all = "snake_case")]
pub enum PromotionReason {
    /// Someone commented — the wisp was discussed.
    HasComments,
    /// A permanent bead links to it.
    Referenced,
    /// Explicitly flagged `gt:keep`.
    KeepLabel,
    /// Open past its TTL — something is stuck and needs human attention.
    PastTtl,
}

/// True if the wisp carries a durable-value signal independent of age: discussed, linked,
/// or explicitly kept. This is the reaper's preserve gate — such wisps are promoted instead
/// of reaped even once past TTL.
pub fn durable_value(wisp: &Wisp) -> Option<PromotionReason> {
    if wisp.comment_count > 0 {
        Some(PromotionReason::HasComments)
    } else if wisp.referenced {
        Some(PromotionReason::Referenced)
    } else if wisp.has_keep_label {
        Some(PromotionReason::KeepLabel)
    } else {
        None
    }
}

/// Full Go criterion set: any of comments / referenced / keep-label / open-past-TTL triggers
/// promotion. `durable_value` first so a stuck-but-discussed wisp reports the stronger reason.
pub fn should_promote(wisp: &Wisp, now: OffsetDateTime) -> Option<PromotionReason> {
    durable_value(wisp).or({
        if wisp.is_past_ttl(now) {
            Some(PromotionReason::PastTtl)
        } else {
            None
        }
    })
}
