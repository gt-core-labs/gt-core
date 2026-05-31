//! [`GitHubSource`] — the GitHub [`WebhookSource`] (`hq-mod-hooks.5`).
//!
//! GitHub signs each delivery with HMAC-SHA256 over the **raw** request body,
//! keyed by the webhook's shared secret, and sends the hex digest in the
//! `X-Hub-Signature-256` header as `sha256=<digest>`. This source recomputes that
//! MAC and compares it in **constant time** (via [`Mac::verify_slice`], which is
//! backed by `subtle`) so a forged request cannot be told apart from a wrong-guess
//! by timing. The event type rides in `X-GitHub-Event`.
//!
//! Verification and parsing are pure (no I/O), so they live in the sync core
//! (non-negotiable #2). Mapping a parsed pull-request delivery into `mod-merge`
//! events is wired when the merge module lands (Phase 4); for now this source
//! authenticates, normalizes, and classifies the PR action via
//! [`pull_request_action`].

use axum::http::HeaderMap;
use hmac::{Hmac, Mac};
use serde_json::Value;
use sha2::Sha256;

use crate::error::WebhookError;
use crate::source::{WebhookDelivery, WebhookSource};

type HmacSha256 = Hmac<Sha256>;

/// Header carrying the hex HMAC-SHA256 signature, prefixed `sha256=`.
const SIGNATURE_HEADER: &str = "x-hub-signature-256";
/// Header carrying the GitHub event type (e.g. `pull_request`).
const EVENT_HEADER: &str = "x-github-event";
/// Mandatory prefix on the signature header value.
const SIGNATURE_PREFIX: &str = "sha256=";

/// The GitHub webhook source, holding the per-hook shared secret.
///
/// One instance per configured webhook secret; register it in the
/// [`WebhookRegistry`](crate::WebhookRegistry) so it answers at
/// `/api/v1/webhooks/github`.
pub struct GitHubSource {
    secret: Vec<u8>,
}

impl GitHubSource {
    /// Build a source from the webhook's shared secret (the value configured in
    /// GitHub's webhook settings). HMAC accepts a key of any length.
    pub fn new(secret: impl Into<Vec<u8>>) -> Self {
        GitHubSource {
            secret: secret.into(),
        }
    }
}

impl WebhookSource for GitHubSource {
    fn id(&self) -> &str {
        "github"
    }

    fn verify(&self, headers: &HeaderMap, body: &[u8]) -> Result<(), WebhookError> {
        let header = headers
            .get(SIGNATURE_HEADER)
            .and_then(|v| v.to_str().ok())
            .ok_or(WebhookError::InvalidSignature)?;
        // GitHub always sends `sha256=<hex>`; anything else is not a v2 signature.
        let hex_sig = header
            .strip_prefix(SIGNATURE_PREFIX)
            .ok_or(WebhookError::InvalidSignature)?;
        let provided = hex::decode(hex_sig).map_err(|_| WebhookError::InvalidSignature)?;

        // `new_from_slice` only errors for key lengths HMAC forbids — there are
        // none, so any secret is accepted.
        let mut mac = HmacSha256::new_from_slice(&self.secret)
            .expect("HMAC-SHA256 accepts a key of any length");
        mac.update(body);
        // Constant-time comparison; never compare the hex strings directly.
        mac.verify_slice(&provided)
            .map_err(|_| WebhookError::InvalidSignature)
    }

    fn parse(&self, headers: &HeaderMap, body: &[u8]) -> Result<WebhookDelivery, WebhookError> {
        let event = headers
            .get(EVENT_HEADER)
            .and_then(|v| v.to_str().ok())
            .ok_or_else(|| WebhookError::MalformedPayload(format!("missing {EVENT_HEADER}")))?;
        let payload: Value = serde_json::from_slice(body)
            .map_err(|e| WebhookError::MalformedPayload(e.to_string()))?;
        Ok(WebhookDelivery::new("github", event, payload))
    }
}

/// The lifecycle of a pull request, as GitHub reports it across `pull_request`
/// deliveries. `hq-mod-hooks.5` classifies these; the merge module consumes them
/// once it lands (Phase 4).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[non_exhaustive]
pub enum PullRequestAction {
    /// A pull request was opened.
    Opened,
    /// A pull request was closed without merging.
    Closed,
    /// A pull request was closed *and merged*.
    Merged,
}

/// Classify a `pull_request` delivery into the merge-relevant action, or `None`
/// if it is not a pull-request delivery or carries an action this maps doesn't
/// model.
///
/// GitHub reports a merge as `action: "closed"` with `pull_request.merged == true`,
/// so a plain `closed` and a `merged` are distinguished by that flag — not by a
/// distinct action string. Only `pull_request` deliveries are considered; any
/// other [`WebhookDelivery::event`] returns `None`.
pub fn pull_request_action(delivery: &WebhookDelivery) -> Option<PullRequestAction> {
    if delivery.event != "pull_request" {
        return None;
    }
    match delivery.payload.get("action").and_then(Value::as_str)? {
        "opened" => Some(PullRequestAction::Opened),
        "closed" => {
            let merged = delivery
                .payload
                .get("pull_request")
                .and_then(|pr| pr.get("merged"))
                .and_then(Value::as_bool)
                .unwrap_or(false);
            Some(if merged {
                PullRequestAction::Merged
            } else {
                PullRequestAction::Closed
            })
        }
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    const SECRET: &[u8] = b"it's a secret to everybody";

    /// Mint the header value GitHub would send for `body` under `SECRET`.
    fn sign(body: &[u8]) -> String {
        let mut mac = HmacSha256::new_from_slice(SECRET).unwrap();
        mac.update(body);
        format!("sha256={}", hex::encode(mac.finalize().into_bytes()))
    }

    fn headers(pairs: &[(&str, &str)]) -> HeaderMap {
        use axum::http::HeaderName;
        let mut h = HeaderMap::new();
        for (k, v) in pairs {
            h.insert(k.parse::<HeaderName>().unwrap(), v.parse().unwrap());
        }
        h
    }

    #[test]
    fn verifies_a_correct_signature() {
        let src = GitHubSource::new(SECRET);
        let body = br#"{"action":"opened"}"#;
        let h = headers(&[(SIGNATURE_HEADER, &sign(body))]);
        assert!(src.verify(&h, body).is_ok());
    }

    #[test]
    fn rejects_a_signature_for_different_body() {
        let src = GitHubSource::new(SECRET);
        let h = headers(&[(SIGNATURE_HEADER, &sign(b"other body"))]);
        assert_eq!(
            src.verify(&h, br#"{"action":"opened"}"#),
            Err(WebhookError::InvalidSignature)
        );
    }

    #[test]
    fn rejects_a_signature_under_a_different_secret() {
        let h = headers(&[(SIGNATURE_HEADER, &sign(b"{}"))]);
        let other = GitHubSource::new(b"wrong secret".to_vec());
        assert_eq!(other.verify(&h, b"{}"), Err(WebhookError::InvalidSignature));
    }

    #[test]
    fn rejects_missing_or_unprefixed_signature() {
        let src = GitHubSource::new(SECRET);
        assert_eq!(src.verify(&HeaderMap::new(), b"{}"), Err(WebhookError::InvalidSignature));
        // Valid hex but no `sha256=` prefix.
        let h = headers(&[(SIGNATURE_HEADER, "deadbeef")]);
        assert_eq!(src.verify(&h, b"{}"), Err(WebhookError::InvalidSignature));
    }

    #[test]
    fn rejects_non_hex_signature() {
        let src = GitHubSource::new(SECRET);
        let h = headers(&[(SIGNATURE_HEADER, "sha256=not-hex")]);
        assert_eq!(src.verify(&h, b"{}"), Err(WebhookError::InvalidSignature));
    }

    #[test]
    fn parse_requires_event_header() {
        let src = GitHubSource::new(SECRET);
        let err = src.parse(&HeaderMap::new(), b"{}").unwrap_err();
        assert!(matches!(err, WebhookError::MalformedPayload(_)));
    }

    #[test]
    fn parse_normalizes_event_and_payload() {
        let src = GitHubSource::new(SECRET);
        let h = headers(&[(EVENT_HEADER, "pull_request")]);
        let body = br#"{"action":"opened","number":7}"#;
        let d = src.parse(&h, body).unwrap();
        assert_eq!(d.source, "github");
        assert_eq!(d.event, "pull_request");
        assert_eq!(d.payload["number"], json!(7));
    }

    fn pr_delivery(payload: Value) -> WebhookDelivery {
        WebhookDelivery::new("github", "pull_request", payload)
    }

    #[test]
    fn classifies_opened_closed_and_merged() {
        assert_eq!(
            pull_request_action(&pr_delivery(json!({"action": "opened"}))),
            Some(PullRequestAction::Opened)
        );
        assert_eq!(
            pull_request_action(&pr_delivery(
                json!({"action": "closed", "pull_request": {"merged": false}})
            )),
            Some(PullRequestAction::Closed)
        );
        assert_eq!(
            pull_request_action(&pr_delivery(
                json!({"action": "closed", "pull_request": {"merged": true}})
            )),
            Some(PullRequestAction::Merged)
        );
    }

    #[test]
    fn ignores_non_pull_request_and_unmodeled_actions() {
        assert_eq!(
            pull_request_action(&WebhookDelivery::new("github", "push", json!({"action": "opened"}))),
            None
        );
        assert_eq!(
            pull_request_action(&pr_delivery(json!({"action": "synchronize"}))),
            None
        );
    }
}
