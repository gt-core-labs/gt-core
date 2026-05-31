//! [`WebhookSource`] — the per-provider port that authenticates and parses an
//! inbound webhook, plus the normalized [`WebhookDelivery`] it produces.
//!
//! A source is the integration seam for one external system (GitHub, Linear, …).
//! `hq-mod-hooks.4` defines the port and the router that drives it; the concrete
//! GitHub (`.5`) and Linear (`.6`) sources implement it. Both steps are **sync**:
//! signature verification is pure CPU (HMAC over the raw body) and parsing is
//! pure deserialization, so they belong in the sync core (non-negotiable #2) and
//! the async router simply calls them. Any I/O a delivery triggers happens later,
//! when the normalized delivery is dispatched to subscribed modules.

use axum::http::HeaderMap;
use serde_json::Value;

use crate::error::WebhookError;

/// A verified, normalized inbound webhook ready to dispatch to modules.
///
/// Providers disagree on envelope shape; a source flattens them into this common
/// form so downstream wiring (`hq-mod-hooks.5`/`.6`) is provider-agnostic.
/// `#[non_exhaustive]` so fields (delivery id, received-at) can be added later.
#[derive(Clone, Debug, PartialEq, Eq)]
#[non_exhaustive]
pub struct WebhookDelivery {
    /// The source id that produced this delivery (its path segment, e.g. `github`).
    pub source: String,
    /// The provider's event type for routing (e.g. `pull_request`, `issue`).
    pub event: String,
    /// The raw JSON payload, parsed but not yet interpreted.
    pub payload: Value,
}

impl WebhookDelivery {
    /// Construct a delivery from its parts. Sources call this from
    /// [`WebhookSource::parse`] after verification succeeds.
    pub fn new(source: impl Into<String>, event: impl Into<String>, payload: Value) -> Self {
        WebhookDelivery {
            source: source.into(),
            event: event.into(),
            payload,
        }
    }
}

/// The port one external provider plugs into to deliver webhooks.
///
/// Object-safe so the [`WebhookRegistry`](crate::WebhookRegistry) can hold
/// heterogeneous sources behind `dyn` — permitted here because `gt-webhooks` is a
/// domain/orchestration crate, not a kernel crate (non-negotiable #1 confines the
/// kernel `dyn` ban, not the edges). Implementations are stateless aside from the
/// secret they hold for verification.
pub trait WebhookSource: Send + Sync {
    /// The path segment this source answers on: `/api/v1/webhooks/<id>`. Must be a
    /// stable, lowercase provider name (`github`, `linear`).
    fn id(&self) -> &str;

    /// Authenticate the request from its headers and raw body, before any parsing.
    ///
    /// Implementations compute the provider's signature over `body` (the exact
    /// bytes received, never a re-serialization) and compare it in constant time
    /// to the header value. Returns [`WebhookError::InvalidSignature`] on mismatch
    /// or a missing signature header.
    fn verify(&self, headers: &HeaderMap, body: &[u8]) -> Result<(), WebhookError>;

    /// Parse an already-verified body into a [`WebhookDelivery`].
    ///
    /// Called only after [`verify`](Self::verify) succeeds. Returns
    /// [`WebhookError::MalformedPayload`] if the event-type header is absent or the
    /// body is not the JSON the provider promised.
    fn parse(&self, headers: &HeaderMap, body: &[u8]) -> Result<WebhookDelivery, WebhookError>;
}
