//! The `Wisp` record and its lifecycle status.

use time::{Duration, OffsetDateTime};

use crate::kind::WispKind;

/// Lifecycle of a wisp. Open wisps are live; reaping is the single `Open → Closed`
/// transition; purge removes a `Closed` row entirely. There is no re-open — a closed wisp
/// is terminal, which is what makes the reaper idempotent.
#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize)]
#[serde(rename_all = "snake_case")]
pub enum WispStatus {
    Open,
    Closed,
}

impl WispStatus {
    pub fn as_str(self) -> &'static str {
        match self {
            WispStatus::Open => "open",
            WispStatus::Closed => "closed",
        }
    }
}

/// An ephemeral memory row. The promotion inputs (`comment_count`, `referenced`,
/// `has_keep_label`) are carried on the record so the reaper can decide preserve-vs-reap
/// without a cross-domain dependency on the bead store.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Wisp {
    pub id: String,
    pub kind: WispKind,
    pub status: WispStatus,
    pub created_at: OffsetDateTime,
    pub closed_at: Option<OffsetDateTime>,
    /// Comments prove a human discussed the wisp — a promotion signal.
    pub comment_count: u32,
    /// True when a non-wisp (permanent) bead links to this wisp — a promotion signal.
    pub referenced: bool,
    /// Explicit `gt:keep` flag — a promotion signal.
    pub has_keep_label: bool,
}

impl Wisp {
    /// Construct an open wisp created at `created_at` with no promotion signals. Test/seed
    /// helper; adapters build `Wisp` directly from rows.
    pub fn open(id: impl Into<String>, kind: WispKind, created_at: OffsetDateTime) -> Self {
        Self {
            id: id.into(),
            kind,
            status: WispStatus::Open,
            created_at,
            closed_at: None,
            comment_count: 0,
            referenced: false,
            has_keep_label: false,
        }
    }

    /// Age relative to `now`. Negative ages (clock skew / future timestamps) clamp to zero
    /// so a future-dated row is never treated as past-TTL.
    pub fn age(&self, now: OffsetDateTime) -> Duration {
        let age = now - self.created_at;
        if age.is_negative() {
            Duration::ZERO
        } else {
            age
        }
    }

    /// True if the wisp is open and older than its kind's TTL — i.e. a reaper candidate.
    pub fn is_past_ttl(&self, now: OffsetDateTime) -> bool {
        self.status == WispStatus::Open && self.age(now) > self.kind.default_ttl()
    }
}
