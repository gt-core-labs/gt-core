//! Last-activity color-coding (hq-mysw). Port of the Go `internal/activity` package onto the
//! type-erased feed read-side.
//!
//! The original Go computed `time.Since(lastActivity)` internally; that reads a wall clock,
//! which the feed must not do (the curator is pure + replay-deterministic, see `lib.rs`). So
//! the calculator here works on an already-computed `age_secs` and the *edge* supplies `now`.
//! [`activity_view`] is the only place that turns a stored RFC3339 `last_ts` into an age, and
//! it takes `now_secs` as an explicit parameter — never a clock.

use serde::{Deserialize, Serialize};

use crate::state::FeedState;

/// Active: under [`THRESHOLD_ACTIVE_SECS`].
pub const COLOR_GREEN: &str = "green";
/// Stale: between active and stale thresholds.
pub const COLOR_YELLOW: &str = "yellow";
/// Stuck: beyond [`THRESHOLD_STALE_SECS`].
pub const COLOR_RED: &str = "red";
/// No activity data (no parseable timestamp).
pub const COLOR_UNKNOWN: &str = "unknown";

/// Green threshold: activity newer than this is "active". Mirrors the Go default
/// (`5 * time.Minute`); operators override it via `worker_status` config at the edge.
pub const THRESHOLD_ACTIVE_SECS: u64 = 5 * 60;
/// Yellow threshold: beyond this an entry is "stuck" (red). Mirrors `10 * time.Minute`.
pub const THRESHOLD_STALE_SECS: u64 = 10 * 60;

/// Color-coded activity for one subject. `age_secs == None` means no activity data (unknown).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ActivityInfo {
    /// Seconds since last activity, or `None` when there is no timestamp to measure from.
    pub age_secs: Option<u64>,
    /// Short human-readable age (`<1m`, `5m`, `2h`, `3d`, or `unknown`).
    pub formatted_age: String,
    /// CSS-style class: `green` / `yellow` / `red` / `unknown`.
    pub color: String,
}

impl ActivityInfo {
    /// Color-code a known age. Uses the default thresholds; see
    /// [`ActivityInfo::from_age_with`] for operator overrides.
    pub fn from_age(age_secs: u64) -> Self {
        Self::from_age_with(age_secs, THRESHOLD_ACTIVE_SECS, THRESHOLD_STALE_SECS)
    }

    /// Color-code a known age against explicit thresholds (`active < stale`).
    pub fn from_age_with(age_secs: u64, active: u64, stale: u64) -> Self {
        Self {
            age_secs: Some(age_secs),
            formatted_age: format_age(age_secs),
            color: color_for_age(age_secs, active, stale).to_string(),
        }
    }

    /// No activity data — the zero-time case in the Go original.
    pub fn unknown() -> Self {
        Self {
            age_secs: None,
            formatted_age: "unknown".to_string(),
            color: COLOR_UNKNOWN.to_string(),
        }
    }

    /// Within the active threshold (green).
    pub fn is_active(&self) -> bool {
        self.color == COLOR_GREEN
    }

    /// In the stale range (yellow).
    pub fn is_stale(&self) -> bool {
        self.color == COLOR_YELLOW
    }

    /// Beyond the stale threshold (red) — a "stuck" signal the escalation path keys on.
    pub fn is_stuck(&self) -> bool {
        self.color == COLOR_RED
    }
}

fn color_for_age(age_secs: u64, active: u64, stale: u64) -> &'static str {
    if age_secs < active {
        COLOR_GREEN
    } else if age_secs < stale {
        COLOR_YELLOW
    } else {
        COLOR_RED
    }
}

/// Short age string: `<1m`, `<minutes>m`, `<hours>h`, `<days>d`. Mirrors `formatAge` in Go.
fn format_age(age_secs: u64) -> String {
    const MIN: u64 = 60;
    const HOUR: u64 = 60 * MIN;
    const DAY: u64 = 24 * HOUR;
    if age_secs < MIN {
        "<1m".to_string()
    } else if age_secs < HOUR {
        format!("{}m", age_secs / MIN)
    } else if age_secs < DAY {
        format!("{}h", age_secs / HOUR)
    } else {
        format!("{}d", age_secs / DAY)
    }
}

/// One activity row of the projection — a feed correlation lifeline with its color code.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ActivityRow {
    pub subject: String,
    pub last_ts: Option<String>,
    pub activity: ActivityInfo,
}

/// Project [`FeedState`] correlations into color-coded activity rows as of `now_secs`.
///
/// Each correlation is one workflow lifeline; its `last_ts` is the most recent event. The age
/// is `now_secs - last_ts` (clamped at 0 for clock skew). A correlation whose `last_ts` is
/// missing or unparseable is reported `unknown`, matching the Go zero-time path. `now_secs` is
/// supplied by the caller (the edge clock) so this stays pure.
pub fn activity_view(state: &FeedState, now_secs: u64) -> Vec<ActivityRow> {
    state
        .correlations
        .values()
        .map(|c| ActivityRow {
            subject: c.correlation_id.clone(),
            last_ts: c.last_ts.clone(),
            activity: match c.last_ts.as_deref().and_then(parse_epoch_secs) {
                Some(ts) => ActivityInfo::from_age(now_secs.saturating_sub(ts)),
                None => ActivityInfo::unknown(),
            },
        })
        .collect()
}

/// Parse an RFC3339 timestamp into UTC epoch seconds. Pure (no clock); `None` on a malformed
/// string so a bad row degrades to `unknown` instead of breaking the whole projection.
pub fn parse_epoch_secs(ts: &str) -> Option<u64> {
    use time::format_description::well_known::Rfc3339;
    use time::OffsetDateTime;
    OffsetDateTime::parse(ts, &Rfc3339)
        .ok()
        .map(|t| t.unix_timestamp())
        .filter(|s| *s >= 0)
        .map(|s| s as u64)
}
