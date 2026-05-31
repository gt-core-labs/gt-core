//! Inbound webhook ingress: `POST /api/v1/webhooks/<source>`.
//!
//! One router fronts every external provider. Each provider plugs in a
//! [`WebhookSource`] that authenticates a request (signature over the raw body)
//! and parses it into a normalized [`WebhookDelivery`]; the [`WebhookRegistry`]
//! resolves the `<source>` path segment to its source and [`webhook_router`]
//! drives verify → parse → acknowledge.
//!
//! ## Landed so far
//!
//! - [`WebhookSource`] (port) + [`WebhookDelivery`] (normalized payload) — the
//!   signature-verify + parse seam, sync so it sits in the core (NN#2)
//!   (`hq-mod-hooks.4`).
//! - [`WebhookRegistry`] — sources keyed by path segment (`hq-mod-hooks.4`).
//! - [`webhook_router`] + [`WEBHOOK_BASE`] — the `POST /<source>` endpoint mapping
//!   [`WebhookError`] to `404`/`401`/`400` (`hq-mod-hooks.4`).
//!
//! The concrete GitHub (`hq-mod-hooks.5`) and Linear (`.6`) sources implement
//! [`WebhookSource`] and wire delivery dispatch to subscribed modules.

#![forbid(unsafe_code)]
#![warn(missing_docs)]

mod error;
mod registry;
mod router;
mod source;

pub use error::WebhookError;
pub use registry::WebhookRegistry;
pub use router::{webhook_router, WEBHOOK_BASE};
pub use source::{WebhookDelivery, WebhookSource};
