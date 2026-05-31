//! The inbound webhook router: `POST /api/v1/webhooks/<source>`.
//!
//! One endpoint fronts every provider. It resolves the `<source>` segment to a
//! registered [`WebhookSource`](crate::WebhookSource), verifies the request
//! signature over the **raw** body bytes, parses a normalized delivery, and
//! acknowledges receipt. Dispatching the delivery to subscribed modules is wired
//! alongside the concrete sources (`hq-mod-hooks.5`/`.6`); here the router's job
//! is authenticate → normalize → acknowledge.

use std::sync::Arc;

use axum::body::Bytes;
use axum::extract::{Path, State};
use axum::http::{HeaderMap, StatusCode};
use axum::routing::post;
use axum::Router;

use crate::error::WebhookError;
use crate::registry::WebhookRegistry;

/// Path the [`webhook_router`] is meant to be nested under by the composition
/// root, so a source answers at `/api/v1/webhooks/<id>`.
pub const WEBHOOK_BASE: &str = "/api/v1/webhooks";

/// Build the webhook router over a shared [`WebhookRegistry`].
///
/// Mounts a single `POST /<source>` route; nest it under [`WEBHOOK_BASE`] to get
/// the full `/api/v1/webhooks/<source>` path. The registry is shared (`Arc`)
/// rather than cloned because sources are registered once and only read.
pub fn webhook_router(registry: Arc<WebhookRegistry>) -> Router {
    Router::new()
        .route("/:source", post(receive))
        .with_state(registry)
}

/// Handle one inbound delivery: resolve source, verify, parse, acknowledge.
///
/// Reads the body as raw [`Bytes`] (never a re-serialized JSON value) so the
/// signature is computed over exactly what the provider signed. Returns
/// `202 Accepted` on success; a [`WebhookError`] otherwise, which carries its own
/// status (`404`/`401`/`400`).
async fn receive(
    State(registry): State<Arc<WebhookRegistry>>,
    Path(source): Path<String>,
    headers: HeaderMap,
    body: Bytes,
) -> Result<StatusCode, WebhookError> {
    let src = registry
        .get(&source)
        .ok_or(WebhookError::UnknownSource(source))?;
    src.verify(&headers, &body)?;
    // Parse to validate + normalize; dispatch to modules lands in .5/.6.
    src.parse(&headers, &body)?;
    Ok(StatusCode::ACCEPTED)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::source::{WebhookDelivery, WebhookSource};

    use axum::body::Body;
    use axum::http::{HeaderMap, Request, StatusCode};
    use serde_json::Value;
    use tower::ServiceExt; // `oneshot`

    /// Stub provider: accepts when `x-stub-sig: good`, parses when `x-stub-event`
    /// is present (its value becomes the delivery event).
    struct StubSource;
    impl WebhookSource for StubSource {
        fn id(&self) -> &str {
            "stub"
        }
        fn verify(&self, headers: &HeaderMap, _body: &[u8]) -> Result<(), WebhookError> {
            match headers.get("x-stub-sig").and_then(|v| v.to_str().ok()) {
                Some("good") => Ok(()),
                _ => Err(WebhookError::InvalidSignature),
            }
        }
        fn parse(
            &self,
            headers: &HeaderMap,
            body: &[u8],
        ) -> Result<WebhookDelivery, WebhookError> {
            let event = headers
                .get("x-stub-event")
                .and_then(|v| v.to_str().ok())
                .ok_or_else(|| WebhookError::MalformedPayload("missing x-stub-event".into()))?;
            let payload: Value = if body.is_empty() {
                Value::Null
            } else {
                serde_json::from_slice(body)
                    .map_err(|e| WebhookError::MalformedPayload(e.to_string()))?
            };
            Ok(WebhookDelivery::new("stub", event, payload))
        }
    }

    fn app() -> Router {
        let mut reg = WebhookRegistry::new();
        reg.register(Box::new(StubSource));
        webhook_router(Arc::new(reg))
    }

    async fn post(path: &str, headers: &[(&str, &str)], body: &str) -> StatusCode {
        let mut req = Request::builder().method("POST").uri(path);
        for (k, v) in headers {
            req = req.header(*k, *v);
        }
        let resp = app()
            .oneshot(req.body(Body::from(body.to_owned())).unwrap())
            .await
            .unwrap();
        resp.status()
    }

    #[tokio::test]
    async fn unknown_source_is_404() {
        assert_eq!(post("/nope", &[("x-stub-sig", "good")], "{}").await, StatusCode::NOT_FOUND);
    }

    #[tokio::test]
    async fn bad_signature_is_401() {
        assert_eq!(
            post("/stub", &[("x-stub-sig", "bad"), ("x-stub-event", "ping")], "{}").await,
            StatusCode::UNAUTHORIZED
        );
    }

    #[tokio::test]
    async fn verified_but_missing_event_header_is_400() {
        assert_eq!(
            post("/stub", &[("x-stub-sig", "good")], "{}").await,
            StatusCode::BAD_REQUEST
        );
    }

    #[tokio::test]
    async fn verified_malformed_json_is_400() {
        assert_eq!(
            post("/stub", &[("x-stub-sig", "good"), ("x-stub-event", "ping")], "not json").await,
            StatusCode::BAD_REQUEST
        );
    }

    #[tokio::test]
    async fn verified_well_formed_delivery_is_202() {
        assert_eq!(
            post(
                "/stub",
                &[("x-stub-sig", "good"), ("x-stub-event", "pull_request")],
                r#"{"action":"opened"}"#,
            )
            .await,
            StatusCode::ACCEPTED
        );
    }
}
