//! [`LinearSource`] — the Linear [`WebhookSource`] (`hq-mod-hooks.6`).
//!
//! Linear signs each delivery with HMAC-SHA256 over the **raw** request body,
//! keyed by the webhook's signing secret, and sends the hex digest in the
//! `Linear-Signature` header — as bare hex, with no `sha256=` prefix (unlike
//! GitHub). This source recomputes that MAC and compares it in **constant time**
//! (via [`Mac::verify_slice`], backed by `subtle`) so a forged request cannot be
//! told apart from a wrong guess by timing.
//!
//! Linear carries no event-type header: the entity type (`Issue`, `Comment`, …)
//! and the action (`create`/`update`/`remove`) live in the JSON body. So
//! [`parse`](LinearSource::parse) reads `type` from the payload as the delivery
//! event, and [`issue_action`] classifies an `Issue` delivery's `action` into the
//! transition `mod-beads` will sync on once it lands (Phase 4).
//!
//! Verification and parsing are pure (no I/O), so they live in the sync core
//! (non-negotiable #2); the async router merely calls them.

use axum::http::HeaderMap;
use hmac::{Hmac, Mac};
use serde_json::Value;
use sha2::Sha256;

use crate::error::WebhookError;
use crate::source::{WebhookDelivery, WebhookSource};

type HmacSha256 = Hmac<Sha256>;

/// Header carrying the hex HMAC-SHA256 signature. Bare hex, no prefix.
const SIGNATURE_HEADER: &str = "linear-signature";
/// Payload key holding the entity type Linear acted on (`Issue`, `Comment`, …).
const TYPE_KEY: &str = "type";
/// Payload key holding the action (`create`, `update`, `remove`).
const ACTION_KEY: &str = "action";

/// The Linear webhook source, holding the per-hook signing secret.
///
/// One instance per configured webhook secret; register it in the
/// [`WebhookRegistry`](crate::WebhookRegistry) so it answers at
/// `/api/v1/webhooks/linear`.
pub struct LinearSource {
    secret: Vec<u8>,
}

impl LinearSource {
    /// Build a source from the webhook's signing secret (the value shown in
    /// Linear's webhook settings). HMAC accepts a key of any length.
    pub fn new(secret: impl Into<Vec<u8>>) -> Self {
        LinearSource {
            secret: secret.into(),
        }
    }
}

impl WebhookSource for LinearSource {
    fn id(&self) -> &str {
        "linear"
    }

    fn verify(&self, headers: &HeaderMap, body: &[u8]) -> Result<(), WebhookError> {
        let hex_sig = headers
            .get(SIGNATURE_HEADER)
            .and_then(|v| v.to_str().ok())
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

    fn parse(&self, _headers: &HeaderMap, body: &[u8]) -> Result<WebhookDelivery, WebhookError> {
        let payload: Value = serde_json::from_slice(body)
            .map_err(|e| WebhookError::MalformedPayload(e.to_string()))?;
        // Linear has no event header; the entity type is the routing key.
        let event = payload
            .get(TYPE_KEY)
            .and_then(Value::as_str)
            .ok_or_else(|| WebhookError::MalformedPayload(format!("missing `{TYPE_KEY}`")))?
            .to_owned();
        Ok(WebhookDelivery::new("linear", event, payload))
    }
}

/// A Linear issue transition, as it maps onto a `mod-beads` sync intent.
/// `hq-mod-hooks.6` classifies these; the beads module consumes them once it
/// lands (Phase 4).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[non_exhaustive]
pub enum IssueAction {
    /// An issue was created → upsert the corresponding bead.
    Created,
    /// An issue was updated (status/title/…) → patch the bead.
    Updated,
    /// An issue was deleted → retire the bead.
    Removed,
}

/// Classify an `Issue` delivery into the bead-sync transition, or `None` if it
/// is not an issue delivery or carries an action this map doesn't model.
///
/// Only deliveries whose [`event`](WebhookDelivery::event) is `Issue` are
/// considered; the action rides in the payload's `action` field as one of
/// `create`/`update`/`remove`.
pub fn issue_action(delivery: &WebhookDelivery) -> Option<IssueAction> {
    if delivery.event != "Issue" {
        return None;
    }
    match delivery.payload.get(ACTION_KEY).and_then(Value::as_str)? {
        "create" => Some(IssueAction::Created),
        "update" => Some(IssueAction::Updated),
        "remove" => Some(IssueAction::Removed),
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    const SECRET: &[u8] = b"lin_wh_secret";

    /// Mint the header value Linear would send for `body` under `SECRET`.
    fn sign(body: &[u8]) -> String {
        let mut mac = HmacSha256::new_from_slice(SECRET).unwrap();
        mac.update(body);
        hex::encode(mac.finalize().into_bytes())
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
        let src = LinearSource::new(SECRET);
        let body = br#"{"type":"Issue","action":"create"}"#;
        let h = headers(&[(SIGNATURE_HEADER, &sign(body))]);
        assert!(src.verify(&h, body).is_ok());
    }

    #[test]
    fn rejects_a_signature_for_different_body() {
        let src = LinearSource::new(SECRET);
        let h = headers(&[(SIGNATURE_HEADER, &sign(b"other body"))]);
        assert_eq!(
            src.verify(&h, br#"{"type":"Issue"}"#),
            Err(WebhookError::InvalidSignature)
        );
    }

    #[test]
    fn rejects_a_signature_under_a_different_secret() {
        let h = headers(&[(SIGNATURE_HEADER, &sign(b"{}"))]);
        let other = LinearSource::new(b"wrong secret".to_vec());
        assert_eq!(other.verify(&h, b"{}"), Err(WebhookError::InvalidSignature));
    }

    #[test]
    fn rejects_missing_signature() {
        let src = LinearSource::new(SECRET);
        assert_eq!(
            src.verify(&HeaderMap::new(), b"{}"),
            Err(WebhookError::InvalidSignature)
        );
    }

    #[test]
    fn rejects_non_hex_signature() {
        let src = LinearSource::new(SECRET);
        let h = headers(&[(SIGNATURE_HEADER, "not-hex")]);
        assert_eq!(src.verify(&h, b"{}"), Err(WebhookError::InvalidSignature));
    }

    #[test]
    fn parse_reads_event_from_payload_type() {
        let src = LinearSource::new(SECRET);
        let body = br#"{"type":"Issue","action":"update","data":{"id":"abc"}}"#;
        let d = src.parse(&HeaderMap::new(), body).unwrap();
        assert_eq!(d.source, "linear");
        assert_eq!(d.event, "Issue");
        assert_eq!(d.payload["data"]["id"], json!("abc"));
    }

    #[test]
    fn parse_requires_a_type_field() {
        let src = LinearSource::new(SECRET);
        let err = src.parse(&HeaderMap::new(), br#"{"action":"create"}"#).unwrap_err();
        assert!(matches!(err, WebhookError::MalformedPayload(_)));
    }

    #[test]
    fn parse_rejects_non_json() {
        let src = LinearSource::new(SECRET);
        let err = src.parse(&HeaderMap::new(), b"not json").unwrap_err();
        assert!(matches!(err, WebhookError::MalformedPayload(_)));
    }

    fn issue_delivery(payload: Value) -> WebhookDelivery {
        WebhookDelivery::new("linear", "Issue", payload)
    }

    #[test]
    fn classifies_create_update_and_remove() {
        assert_eq!(
            issue_action(&issue_delivery(json!({"action": "create"}))),
            Some(IssueAction::Created)
        );
        assert_eq!(
            issue_action(&issue_delivery(json!({"action": "update"}))),
            Some(IssueAction::Updated)
        );
        assert_eq!(
            issue_action(&issue_delivery(json!({"action": "remove"}))),
            Some(IssueAction::Removed)
        );
    }

    #[test]
    fn ignores_non_issue_and_unmodeled_actions() {
        assert_eq!(
            issue_action(&WebhookDelivery::new("linear", "Comment", json!({"action": "create"}))),
            None
        );
        assert_eq!(
            issue_action(&issue_delivery(json!({"action": "restore"}))),
            None
        );
    }
}
