use thiserror::Error;

/// Core domain error for the gt-core store adapters. No I/O variants here: I/O
/// lives at the edges (`gt-store-*`) and maps its errors onto these variants when
/// crossing the boundary. Ported from the upstream `gt-events::AppError`
/// (hq-core-host.1) so the lifted issues store no longer depends on the
/// unmigrated `gt-events` crate; the variants are intentionally identical so the
/// port is behaviour-preserving.
#[derive(Debug, Clone, Error, PartialEq, Eq)]
pub enum AppError {
    /// Illegal state transition rejected by a state machine.
    #[error("invalid state transition: {0}")]
    InvalidTransition(String),

    /// Entity not found.
    #[error("not found: {0}")]
    NotFound(String),

    /// A command failed validation.
    #[error("validation failed: {0}")]
    Validation(String),

    /// A bus handler returned an error.
    #[error("handler error: {0}")]
    Handler(String),

    /// Catch-all for as-yet-unclassified errors.
    #[error("{0}")]
    Other(String),
}
