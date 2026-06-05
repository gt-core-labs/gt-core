use thiserror::Error;

/// Local error for the RBAC scope check. Lifted from the upstream `gt_events::AppError`
/// (hq-core-host.4) so the scope/auth pillar no longer depends on the unmigrated
/// `gt-events` crate — same behaviour-preserving move gt-store-dolt made in
/// hq-core-host.1. Only the variants this crate actually raises are kept;
/// `Scope::check` rejects with `Validation`.
#[derive(Debug, Clone, Error, PartialEq, Eq)]
pub enum AppError {
    /// A command failed validation — here, a tool the actor's scope forbids.
    #[error("validation failed: {0}")]
    Validation(String),

    /// Catch-all (config I/O, parse errors crossing the boundary).
    #[error("{0}")]
    Other(String),
}
