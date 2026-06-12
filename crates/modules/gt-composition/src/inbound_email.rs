//! Inbound-email webhook → `FileInbox` (hq-6c6d16, epic hq-9c199d).
//!
//! The mail provider (SES / Mailgun / Resend) receives a message for the
//! platform's address and POSTs it here over HTTPS (the 443 path Traefik
//! already exposes) — no inbound port 25 anywhere. This receiver does ONE
//! thing: authenticate the delivery, normalize the provider payload into the
//! [`gt_notify::InboundEmail`] shape, and drop it as a `*.json` file into the
//! command-mailbox directory (`GT_INBOUND_MAIL_DIR`). The existing mailbox
//! daemon ([`crate::mailbox`], hq-8a521a) consumes it unchanged: member
//! verification, role ladder, quarantine, and the email→comment bridge all
//! stay where they are.
//!
//! ## Auth
//!
//! A shared secret (`GT_INBOUND_WEBHOOK_SECRET`) carried in the
//! `x-gt-inbound-secret` header OR the `secret` query parameter (providers
//! like Resend let you set the endpoint URL but not custom headers — the
//! query form rides inside TLS), compared via SHA-256 digests so the check is
//! effectively constant-time. Providers that sign deliveries (Mailgun HMAC,
//! svix) can layer a verifying parser later — the shared secret is the
//! provider-agnostic baseline. No secret configured ⇒ `503` (never silently
//! open).
//!
//! ## Payload
//!
//! JSON only (v1). Two key vocabularies are normalized:
//! - generic/Resend-style: `from`, `subject`, `body` (or `text`),
//!   `message_id`, `in_reply_to`;
//! - Mailgun-style: `sender`, `subject`, `stripped-text` / `body-plain`,
//!   `Message-Id`, `In-Reply-To`.
//!
//! A missing `message_id` is minted (ulid) so the file name is always unique;
//! the write is tmp + rename so the polling daemon never sees a half-written
//! file.

use std::path::PathBuf;

use axum::body::Bytes;
use axum::extract::State;
use axum::http::{HeaderMap, StatusCode};
use axum::response::{IntoResponse, Response};
use axum::routing::post;
use axum::Router;
use serde_json::Value;
use sha2::{Digest, Sha256};

use gt_notify::InboundEmail;

/// Relative path the receiver mounts at; the binary nests it under `/api/v1/email`.
pub const INBOUND_EMAIL_PATH: &str = "/inbound";

/// Header carrying the shared webhook secret.
pub const SECRET_HEADER: &str = "x-gt-inbound-secret";

/// State for the inbound-email receiver: the shared secret and the mailbox
/// directory the daemon polls. Cheap to clone.
#[derive(Clone)]
pub struct InboundEmailState {
    /// `GT_INBOUND_WEBHOOK_SECRET`. `None` ⇒ receiver answers `503` (cannot
    /// authenticate) rather than accepting unauthenticated mail.
    secret: Option<String>,
    /// `GT_INBOUND_MAIL_DIR` — the same directory the mailbox daemon's
    /// `FileInbox` watches.
    dir: PathBuf,
}

impl InboundEmailState {
    /// Build the receiver state.
    pub fn new(secret: Option<String>, dir: impl Into<PathBuf>) -> Self {
        Self {
            secret,
            dir: dir.into(),
        }
    }
}

/// Build the inbound-email router (relative path — the binary nests it under
/// `/api/v1/email`, OUTSIDE the RBAC scope chain: like the GitHub webhook, a
/// delivery authenticates by its secret, not a workspace JWT).
pub fn inbound_email_router(state: InboundEmailState) -> Router {
    Router::new()
        .route(INBOUND_EMAIL_PATH, post(receive))
        .with_state(state)
}

/// SHA-256-digest equality: hashing both sides first makes the comparison
/// independent of where the strings differ, so the timing side-channel of a
/// byte-by-byte `==` on attacker-controlled input is gone.
fn secret_matches(provided: &str, expected: &str) -> bool {
    Sha256::digest(provided.as_bytes()) == Sha256::digest(expected.as_bytes())
}

/// First non-empty string under any of `keys`.
fn pick(v: &Value, keys: &[&str]) -> Option<String> {
    keys.iter()
        .filter_map(|k| v.get(k).and_then(Value::as_str))
        .map(str::trim)
        .find(|s| !s.is_empty())
        .map(str::to_string)
}

/// Normalize a provider JSON payload into the mailbox's [`InboundEmail`]
/// shape. `None` when no sender can be extracted — the one field the mailbox
/// cannot proceed without (it resolves the sender against the member mirror).
///
/// Event-wrapper payloads (Resend: `{"type":"email.received","data":{…}}`)
/// are unwrapped to their `data` object first; flat payloads (Mailgun routes,
/// generic forwarders) normalize as-is.
fn normalize(payload: &Value) -> Option<InboundEmail> {
    if let Some(inner) = payload.get("data").filter(|d| d.is_object()) {
        if let Some(msg) = normalize_flat(inner) {
            return Some(msg);
        }
    }
    normalize_flat(payload)
}

/// One vocabulary table over a flat (un-wrapped) payload.
fn normalize_flat(payload: &Value) -> Option<InboundEmail> {
    let from = pick(payload, &["from", "sender", "From"])?;
    Some(InboundEmail {
        from,
        subject: pick(payload, &["subject", "Subject"]).unwrap_or_default(),
        body: pick(
            payload,
            &["body", "text", "stripped-text", "body-plain", "body_plain"],
        )
        .unwrap_or_default(),
        message_id: pick(
            payload,
            &["message_id", "Message-Id", "Message-ID", "message-id", "email_id"],
        )
        .unwrap_or_else(|| ulid::Ulid::new().to_string()),
        in_reply_to: pick(payload, &["in_reply_to", "In-Reply-To", "in-reply-to"]),
    })
}

/// Mirror of `gt_notify::inbound`'s file-name sanitizer (alnum + `-`), so the
/// names this receiver writes are exactly the ones the inbox would mint.
fn sanitize(s: &str) -> String {
    s.chars()
        .map(|c| if c.is_ascii_alphanumeric() || c == '-' { c } else { '_' })
        .collect()
}

/// Write `msg` into `dir` as `<sanitized-id>.json` atomically (tmp + rename on
/// the same filesystem), so the polling `FileInbox` never reads a partial file.
fn write_message(dir: &PathBuf, msg: &InboundEmail) -> std::io::Result<()> {
    std::fs::create_dir_all(dir)?;
    let name = sanitize(&msg.message_id);
    let tmp = dir.join(format!("{name}.json.tmp"));
    let dst = dir.join(format!("{name}.json"));
    std::fs::write(&tmp, serde_json::to_vec(msg).expect("InboundEmail serializes"))?;
    std::fs::rename(&tmp, &dst)
}

/// `POST /inbound` — authenticate, normalize, persist. Returns:
/// - `503` when no secret is configured (receiver cannot authenticate),
/// - `401` on a bad/absent secret header (nothing written),
/// - `400` on non-JSON or a payload with no extractable sender (nothing written),
/// - `500` when the mailbox dir is unwritable (provider retries — the mail is real),
/// - `200` with the stored message id on success.
async fn receive(
    State(st): State<InboundEmailState>,
    axum::extract::RawQuery(query): axum::extract::RawQuery,
    headers: HeaderMap,
    body: Bytes,
) -> Response {
    let Some(expected) = st.secret.as_deref() else {
        return (
            StatusCode::SERVICE_UNAVAILABLE,
            "no inbound webhook secret configured",
        )
            .into_response();
    };
    // Header form, or `?secret=` for providers that cannot set custom headers
    // (Resend). The query value is taken verbatim — generate the secret
    // URL-safe (alnum) so no percent-encoding ambiguity exists.
    let provided = headers
        .get(SECRET_HEADER)
        .and_then(|v| v.to_str().ok())
        .map(str::to_string)
        .or_else(|| {
            query.as_deref().and_then(|q| {
                q.split('&')
                    .find_map(|kv| kv.strip_prefix("secret="))
                    .map(str::to_string)
            })
        });
    let authed = provided.is_some_and(|p| secret_matches(&p, expected));
    if !authed {
        return (StatusCode::UNAUTHORIZED, "invalid webhook secret").into_response();
    }

    let payload: Value = match serde_json::from_slice(&body) {
        Ok(v) => v,
        Err(_) => return (StatusCode::BAD_REQUEST, "malformed payload: not JSON").into_response(),
    };
    let Some(msg) = normalize(&payload) else {
        return (StatusCode::BAD_REQUEST, "malformed payload: no sender").into_response();
    };

    match write_message(&st.dir, &msg) {
        Ok(()) => (StatusCode::OK, format!("accepted {}", msg.message_id)).into_response(),
        // The provider really did receive mail for us; 500 so it redelivers.
        Err(e) => {
            eprintln!("[inbound-email] mailbox dir write failed: {e}");
            (StatusCode::INTERNAL_SERVER_ERROR, "mailbox write failed").into_response()
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use gt_notify::InboundMail;

    fn dir(tag: &str) -> PathBuf {
        let d = std::env::temp_dir().join(format!("gt-inbound-web-{tag}-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&d);
        d
    }

    async fn post_uri(
        state: InboundEmailState,
        uri: &str,
        secret: Option<&str>,
        body: &str,
    ) -> (StatusCode, String) {
        use tower::ServiceExt;
        let app = inbound_email_router(state);
        let mut req = axum::http::Request::builder()
            .method("POST")
            .uri(uri)
            .header("content-type", "application/json");
        if let Some(s) = secret {
            req = req.header(SECRET_HEADER, s);
        }
        let res = app
            .oneshot(req.body(axum::body::Body::from(body.to_string())).unwrap())
            .await
            .unwrap();
        let status = res.status();
        let bytes = axum::body::to_bytes(res.into_body(), 64 * 1024).await.unwrap();
        (status, String::from_utf8_lossy(&bytes).into_owned())
    }

    async fn post(
        state: InboundEmailState,
        secret: Option<&str>,
        body: &str,
    ) -> (StatusCode, String) {
        post_uri(state, INBOUND_EMAIL_PATH, secret, body).await
    }

    #[tokio::test]
    async fn secret_via_query_param_works_for_headerless_providers() {
        let d = dir("query");
        let st = InboundEmailState::new(Some("s3cret".into()), &d);
        let uri = format!("{INBOUND_EMAIL_PATH}?secret=s3cret");
        // Resend event-wrapper shape: fields nested under `data`.
        let payload = r#"{
            "type": "email.received",
            "data": {
                "email_id": "re-001",
                "from": "ana@x.com",
                "subject": "estado",
                "text": "ping"
            }
        }"#;
        let (status, body) = post_uri(st.clone(), &uri, None, payload).await;
        assert_eq!(status, StatusCode::OK, "{body}");
        let polled = gt_notify::FileInbox::new(&d).poll().expect("poll");
        assert_eq!(polled.len(), 1);
        assert_eq!(polled[0].from, "ana@x.com");
        assert_eq!(polled[0].body, "ping");
        assert_eq!(polled[0].message_id, "re-001");

        // Wrong query secret still 401.
        let bad = format!("{INBOUND_EMAIL_PATH}?secret=nope");
        let (status, _) = post_uri(st, &bad, None, payload).await;
        assert_eq!(status, StatusCode::UNAUTHORIZED);
        let _ = std::fs::remove_dir_all(&d);
    }

    #[tokio::test]
    async fn valid_delivery_roundtrips_through_the_file_inbox() {
        let d = dir("ok");
        let st = InboundEmailState::new(Some("s3cret".into()), &d);
        let payload = r#"{
            "sender": "ana@x.com",
            "subject": "move hq-1 to working",
            "stripped-text": "please",
            "Message-Id": "<abc-123@mail>",
            "In-Reply-To": "<outbox-42@gt>"
        }"#;
        let (status, body) = post(st, Some("s3cret"), payload).await;
        assert_eq!(status, StatusCode::OK, "{body}");

        // The mailbox daemon's own inbox consumes exactly what we wrote.
        let inbox = gt_notify::FileInbox::new(&d);
        let polled = inbox.poll().expect("poll");
        assert_eq!(polled.len(), 1);
        assert_eq!(polled[0].from, "ana@x.com");
        assert_eq!(polled[0].subject, "move hq-1 to working");
        assert_eq!(polled[0].body, "please");
        assert_eq!(polled[0].in_reply_to.as_deref(), Some("<outbox-42@gt>"));
        let _ = std::fs::remove_dir_all(&d);
    }

    #[tokio::test]
    async fn bad_or_absent_secret_is_401_and_writes_nothing() {
        let d = dir("401");
        let st = InboundEmailState::new(Some("s3cret".into()), &d);
        let payload = r#"{"from":"ana@x.com"}"#;
        let (status, _) = post(st.clone(), Some("wrong"), payload).await;
        assert_eq!(status, StatusCode::UNAUTHORIZED);
        let (status, _) = post(st, None, payload).await;
        assert_eq!(status, StatusCode::UNAUTHORIZED);
        assert!(gt_notify::FileInbox::new(&d).poll().expect("poll").is_empty());
        let _ = std::fs::remove_dir_all(&d);
    }

    #[tokio::test]
    async fn no_configured_secret_is_503_never_open() {
        let d = dir("503");
        let st = InboundEmailState::new(None, &d);
        let (status, _) = post(st, Some("anything"), r#"{"from":"a@x.com"}"#).await;
        assert_eq!(status, StatusCode::SERVICE_UNAVAILABLE);
        let _ = std::fs::remove_dir_all(&d);
    }

    #[tokio::test]
    async fn malformed_payloads_are_400_and_write_nothing() {
        let d = dir("400");
        let st = InboundEmailState::new(Some("s".into()), &d);
        let (status, body) = post(st.clone(), Some("s"), "{not json").await;
        assert_eq!(status, StatusCode::BAD_REQUEST, "{body}");
        // Valid JSON but no sender anywhere.
        let (status, body) = post(st, Some("s"), r#"{"subject":"hi"}"#).await;
        assert_eq!(status, StatusCode::BAD_REQUEST, "{body}");
        assert!(gt_notify::FileInbox::new(&d).poll().expect("poll").is_empty());
        let _ = std::fs::remove_dir_all(&d);
    }

    #[tokio::test]
    async fn missing_message_id_is_minted_and_generic_keys_normalize() {
        let d = dir("mint");
        let st = InboundEmailState::new(Some("s".into()), &d);
        let (status, _) =
            post(st, Some("s"), r#"{"from":"bob@x.com","text":"hola","subject":"q"}"#).await;
        assert_eq!(status, StatusCode::OK);
        let polled = gt_notify::FileInbox::new(&d).poll().expect("poll");
        assert_eq!(polled.len(), 1);
        assert_eq!(polled[0].body, "hola");
        assert!(!polled[0].message_id.is_empty(), "ulid minted");
        assert_eq!(polled[0].in_reply_to, None);
        let _ = std::fs::remove_dir_all(&d);
    }
}
