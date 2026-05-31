//! [`WebhookError`] — why an inbound webhook request was rejected, and its HTTP
//! mapping.
//!
//! The router turns each variant into a status code so a misbehaving sender gets
//! an actionable response and never sees a `500` for a client-side fault. Bodies
//! are intentionally terse: a webhook endpoint is unauthenticated by cookie, so
//! it must not leak why verification failed beyond the status line.

use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};

/// Why an inbound webhook was not accepted.
///
/// `#[non_exhaustive]` so later beads (GitHub/Linear sources, `hq-mod-hooks.5`/`.6`)
/// can add failure modes without breaking a `match`.
#[derive(Debug, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub enum WebhookError {
    /// No [`WebhookSource`](crate::WebhookSource) is registered under the
    /// `<source>` path segment. Maps to `404 Not Found`.
    UnknownSource(String),
    /// The request signature did not verify against the source's secret. Maps to
    /// `401 Unauthorized`. The reason is deliberately not disclosed in the body.
    InvalidSignature,
    /// The body verified but could not be parsed into a delivery (missing event
    /// header, malformed JSON). Maps to `400 Bad Request`.
    MalformedPayload(String),
}

impl std::fmt::Display for WebhookError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            WebhookError::UnknownSource(s) => write!(f, "unknown webhook source `{s}`"),
            WebhookError::InvalidSignature => write!(f, "webhook signature verification failed"),
            WebhookError::MalformedPayload(why) => write!(f, "malformed webhook payload: {why}"),
        }
    }
}

impl std::error::Error for WebhookError {}

impl WebhookError {
    /// The HTTP status this error maps to.
    pub fn status(&self) -> StatusCode {
        match self {
            WebhookError::UnknownSource(_) => StatusCode::NOT_FOUND,
            WebhookError::InvalidSignature => StatusCode::UNAUTHORIZED,
            WebhookError::MalformedPayload(_) => StatusCode::BAD_REQUEST,
        }
    }
}

impl IntoResponse for WebhookError {
    fn into_response(self) -> Response {
        // Status only; the body stays empty so an unauthenticated caller learns
        // nothing beyond the class of failure.
        self.status().into_response()
    }
}
