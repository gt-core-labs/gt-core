//! `WispKind` and the per-kind default TTL table.
//!
//! Wire forms match the Go `wisp_type` column values (`internal/beads` role config:
//! `wisp_ttl_<type>`), so a Rust reaper and a Go-written wisps table agree on classification.
//! TTL defaults follow the Go role-config examples (`patrol: 48h`, `error: 336h`,
//! `gc_report: 24h`); the remaining kinds inherit the 24h reaper `max_age` baseline.

use time::Duration;

use gt_events::AppError;

/// Classification of an ephemeral wisp. Drives the compaction TTL and de-dup of the
/// `wisp_type` column. Liveness/observability signals only — never domain state.
#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize)]
#[serde(rename_all = "snake_case")]
pub enum WispKind {
    Heartbeat,
    Ping,
    Patrol,
    GcReport,
    Recovery,
    Error,
    Escalation,
}

impl WispKind {
    /// Every kind, for table-driven tests and config enumeration.
    pub const ALL: [WispKind; 7] = [
        WispKind::Heartbeat,
        WispKind::Ping,
        WispKind::Patrol,
        WispKind::GcReport,
        WispKind::Recovery,
        WispKind::Error,
        WispKind::Escalation,
    ];

    /// Wire form stored in `wisps.wisp_type` (matches Go `wisp_type`).
    pub fn as_str(self) -> &'static str {
        match self {
            WispKind::Heartbeat => "heartbeat",
            WispKind::Ping => "ping",
            WispKind::Patrol => "patrol",
            WispKind::GcReport => "gc_report",
            WispKind::Recovery => "recovery",
            WispKind::Error => "error",
            WispKind::Escalation => "escalation",
        }
    }

    /// Parse the wire form. Unknown values are a domain validation error rather than a
    /// silent fallback — a stray `wisp_type` should surface, not get reaped on the wrong TTL.
    pub fn parse(s: &str) -> Result<WispKind, AppError> {
        match s {
            "heartbeat" => Ok(WispKind::Heartbeat),
            "ping" => Ok(WispKind::Ping),
            "patrol" => Ok(WispKind::Patrol),
            "gc_report" => Ok(WispKind::GcReport),
            "recovery" => Ok(WispKind::Recovery),
            "error" => Ok(WispKind::Error),
            "escalation" => Ok(WispKind::Escalation),
            other => Err(AppError::Validation(format!("unknown wisp_type: {other}"))),
        }
    }

    /// Default TTL after which an open wisp of this kind is a reap candidate. Forensic kinds
    /// (`error`, `escalation`) are kept long; liveness kinds expire on the 24h baseline.
    pub fn default_ttl(self) -> Duration {
        match self {
            WispKind::Heartbeat => Duration::hours(24),
            WispKind::Ping => Duration::hours(24),
            WispKind::Patrol => Duration::hours(48),
            WispKind::GcReport => Duration::hours(24),
            WispKind::Recovery => Duration::hours(24),
            WispKind::Error => Duration::hours(336),
            WispKind::Escalation => Duration::hours(336),
        }
    }
}
