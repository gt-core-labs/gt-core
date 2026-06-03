//! Owned event surface for the login driver.
//!
//! Mirrors the kernel pattern (`docs/01-architecture.md`): exhaustive `Serialize` enum so
//! `gt-web` can map each variant to its SSE kind (`quota.login_*`, `hq-fe-auth.3`) without
//! coupling to driver internals. Variants carry only data — no `OffsetDateTime::now_utc()`,
//! no live process handles — so replaying a tape rebuilds the UI's view byte-for-byte.

use serde::{Deserialize, Serialize};

/// Driver-side events surfaced to subscribers (HTTP/SSE layers, tests).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum LoginEvent {
    /// The child process was spawned successfully and the driver is reading its output.
    Started,
    /// The authorization URL was extracted from the CLI's output. UI shows it to the user
    /// so they can paste it into a browser and copy the resulting token back.
    UrlReady { url: String },
    /// The user-supplied token was accepted by the CLI and the process exited cleanly.
    /// `account` is echoed back so callers don't need to keep external state.
    Complete { account: String },
    /// Terminal failure. The driver is no longer running; the handle is consumed.
    Failed { reason: LoginFailure },
}

/// Discriminated failure reasons. Each maps to a distinct SSE payload (`hq-fe-auth.3`) and
/// to a distinct rollback path in the HTTP layer (`hq-fe-auth.2`).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, thiserror::Error)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum LoginFailure {
    /// The CLI binary could not be spawned (not on PATH, permission denied, ...).
    #[error("spawn failed: {message}")]
    Spawn { message: String },
    /// The PTY closed before a URL was extracted.
    #[error("pty closed before url was extracted")]
    UrlMissing,
    /// The CLI exited with a non-zero status after we wrote the token.
    #[error("cli rejected token (exit status {status})")]
    TokenRejected { status: i32 },
    /// The caller cancelled the flow (UI close, /api/.../cancel).
    #[error("login cancelled by caller")]
    Cancelled,
    /// I/O failure on the PTY (read/write returned an OS-level error).
    #[error("pty io error: {message}")]
    Io { message: String },
    /// Wallclock deadline exceeded for `phase` (`"url"` or `"token"`). The watchdog
    /// killed the PTY child so the driver surfaces a typed terminal state instead
    /// of hanging — `hq-fe-auth.4`.
    #[error("login timed out during {phase} phase")]
    Timeout { phase: String },
}
