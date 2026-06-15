//! CI-gate receiver (gtcore-52c9ec): the orchd side of the
//! `GitHub → MCP server → orchd` CI-gate path that replaces the old 60s
//! `poll_pr_until_resolved` loop in [`crate::git_merge`].
//!
//! ## The flow
//!
//! The MCP server's GitHub webhook ([`crate::webhook`]) verifies the App's HMAC, sees a
//! `pull_request.closed` or `check_suite.completed` delivery for an agent PR, and forwards a small
//! JSON notification in-cluster to this receiver. This module mounts two thin routes — typically on
//! orchd's existing metrics port (`GT_METRICS_BIND`, default `127.0.0.1:9099`) — and drives the
//! merge slot to its terminal state through the [`MergeHandle`]:
//!
//! - `POST /ci-gate/merged`  `{"bead":…,"sha":…}`   → [`MergeHandle::complete`] (→ `merge.merged.v1`)
//! - `POST /ci-gate/failed`  `{"bead":…,"reason":…}` → [`MergeHandle::fail`] (→ `merge.failed.v1`,
//!   which the `SchedulerPlugin` re-enqueues — commit `bff7fc6`)
//!
//! `complete`/`fail` are no-ops for a bead not on the live board, so a delivery for a non-agent PR
//! (or a redelivery after the slot already resolved) is harmless.
//!
//! ## Auth
//!
//! The path is in-cluster, but a shared bearer token (`GT_CI_GATE_TOKEN`, mirrored on the MCP-server
//! forwarder) guards it when set: a request without the matching `Authorization: Bearer <token>` is
//! a clean `401` with no state change. With no token configured the receiver accepts any caller —
//! appropriate only when the port is not otherwise reachable.

use std::sync::Arc;

use axum::body::Bytes;
use axum::extract::State;
use axum::http::{HeaderMap, StatusCode};
use axum::response::{IntoResponse, Response};
use axum::routing::post;
use axum::Router;
use serde::Deserialize;

use gt_merge::actor::MergeHandle;

/// State for the CI-gate receiver: the merge command handle (the slot's terminal transition) and an
/// optional shared bearer token. Cheap to clone — both fields are handles/`Arc`.
#[derive(Clone)]
pub struct CiGateState {
    merge: MergeHandle,
    /// `Some` ⇒ every request must carry `Authorization: Bearer <token>`; `None` ⇒ no auth gate.
    /// Empty/whitespace tokens collapse to `None` so an unset env var never yields a token that
    /// rejects every caller.
    token: Option<Arc<String>>,
}

impl CiGateState {
    /// Wire the merge handle and the optional shared token (`GT_CI_GATE_TOKEN`). A blank token is
    /// treated as no token.
    pub fn new(merge: MergeHandle, token: Option<String>) -> Self {
        Self {
            merge,
            token: token
                .map(|t| t.trim().to_string())
                .filter(|t| !t.is_empty())
                .map(Arc::new),
        }
    }
}

/// A `POST /ci-gate/merged` body: the agent PR merged, with the sha now on `main`.
#[derive(Debug, Deserialize)]
struct MergedBody {
    bead: String,
    #[serde(default)]
    sha: String,
}

/// A `POST /ci-gate/failed` body: the agent PR's CI failed (or it closed unmerged); the reason is
/// surfaced on the failed event and the bead is re-enqueued.
#[derive(Debug, Deserialize)]
struct FailedBody {
    bead: String,
    #[serde(default)]
    reason: String,
}

/// Build the CI-gate router (`/ci-gate/merged`, `/ci-gate/failed`). The binary merges it into the
/// metrics router so both share one listener (`GT_METRICS_BIND`).
pub fn ci_gate_router(state: CiGateState) -> Router {
    Router::new()
        .route("/ci-gate/merged", post(merged))
        .route("/ci-gate/failed", post(failed))
        .with_state(state)
}

/// `true` when the request is authorized: either no token is configured, or the `Authorization`
/// header carries the exact `Bearer <token>`. Pure (header + token in, bool out) so the auth gate is
/// unit-testable without axum. The compare is byte-exact on a constant in-cluster secret.
fn authorized(token: Option<&str>, headers: &HeaderMap) -> bool {
    let Some(want) = token else {
        return true;
    };
    headers
        .get("authorization")
        .and_then(|v| v.to_str().ok())
        .and_then(|v| v.strip_prefix("Bearer "))
        .map(|got| got == want)
        .unwrap_or(false)
}

/// `POST /ci-gate/merged` — drive the slot to `Merged`. `401` (no token), `400` (bad body / empty
/// bead), else `202` once the `complete` is dispatched (a no-op for an unknown bead).
async fn merged(State(st): State<CiGateState>, headers: HeaderMap, body: Bytes) -> Response {
    if !authorized(st.token.as_deref().map(String::as_str), &headers) {
        return (StatusCode::UNAUTHORIZED, "invalid token").into_response();
    }
    let req: MergedBody = match serde_json::from_slice(&body) {
        Ok(r) => r,
        Err(_) => return (StatusCode::BAD_REQUEST, "malformed body").into_response(),
    };
    if req.bead.trim().is_empty() {
        return (StatusCode::BAD_REQUEST, "missing bead").into_response();
    }
    st.merge.complete(req.bead, req.sha).await;
    StatusCode::ACCEPTED.into_response()
}

/// `POST /ci-gate/failed` — drive the slot to `Failed` (the reactor re-enqueues). Same status shape
/// as [`merged`].
async fn failed(State(st): State<CiGateState>, headers: HeaderMap, body: Bytes) -> Response {
    if !authorized(st.token.as_deref().map(String::as_str), &headers) {
        return (StatusCode::UNAUTHORIZED, "invalid token").into_response();
    }
    let req: FailedBody = match serde_json::from_slice(&body) {
        Ok(r) => r,
        Err(_) => return (StatusCode::BAD_REQUEST, "malformed body").into_response(),
    };
    if req.bead.trim().is_empty() {
        return (StatusCode::BAD_REQUEST, "missing bead").into_response();
    }
    let reason = if req.reason.trim().is_empty() {
        "CI gate reported failure".to_string()
    } else {
        req.reason
    };
    st.merge.fail(req.bead, reason).await;
    StatusCode::ACCEPTED.into_response()
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::http::HeaderName;
    use gt_merge::actor::{spawn, MergeHandle};
    use gt_merge::{InMemoryMergeRepo, MergeEvent};
    use gt_events::Envelope;
    use serde_json::json;
    use tokio::sync::mpsc;

    fn auth_header(value: &str) -> HeaderMap {
        let mut h = HeaderMap::new();
        h.insert(
            "authorization".parse::<HeaderName>().unwrap(),
            value.parse().unwrap(),
        );
        h
    }

    #[test]
    fn no_token_accepts_any_caller() {
        // No configured token ⇒ the gate is open regardless of headers.
        assert!(authorized(None, &HeaderMap::new()));
        assert!(authorized(None, &auth_header("Bearer whatever")));
    }

    #[test]
    fn token_requires_exact_bearer() {
        let tok = Some("s3cret");
        assert!(authorized(tok, &auth_header("Bearer s3cret")));
        // Wrong token, wrong scheme, and a missing header are all rejected.
        assert!(!authorized(tok, &auth_header("Bearer nope")));
        assert!(!authorized(tok, &auth_header("s3cret")));
        assert!(!authorized(tok, &HeaderMap::new()));
    }

    #[test]
    fn blank_env_token_collapses_to_open() {
        // An empty/whitespace GT_CI_GATE_TOKEN must NOT produce a token that rejects every caller.
        let st = CiGateState::new(dummy_merge().0, Some("   ".into()));
        assert!(st.token.is_none());
    }

    /// A merge handle whose actor board we can seed + observe through the relay receiver.
    fn dummy_merge() -> (MergeHandle, mpsc::Receiver<Envelope<MergeEvent>>) {
        let (tx, rx) = mpsc::channel(16);
        (spawn(InMemoryMergeRepo::default(), tx), rx)
    }

    /// Drain the relay until an event whose `kind` matches is seen, or the channel empties.
    async fn next_kind(rx: &mut mpsc::Receiver<Envelope<MergeEvent>>) -> Vec<String> {
        let mut kinds = Vec::new();
        while let Ok(env) = rx.try_recv() {
            kinds.push(env.kind().to_string());
        }
        // Give the actor a tick to flush if nothing was buffered yet.
        if kinds.is_empty() {
            if let Some(env) = rx.recv().await {
                kinds.push(env.kind().to_string());
            }
        }
        kinds
    }

    #[tokio::test]
    async fn merged_completes_slot_on_live_board() {
        let (merge, mut rx) = dummy_merge();
        // Seed a slot in `Merging` (Ready → Started) so `complete` is a legal transition.
        merge.submit("b1", "b1", "01ABC").await;
        merge.start("b1").await;
        let _ = next_kind(&mut rx).await; // drain Ready/Started

        let st = CiGateState::new(merge.clone(), None);
        let resp = merged(
            State(st),
            HeaderMap::new(),
            Bytes::from(json!({"bead":"b1","sha":"deadbeef"}).to_string()),
        )
        .await;
        assert_eq!(resp.status(), StatusCode::ACCEPTED);

        let kinds = next_kind(&mut rx).await;
        assert!(
            kinds.iter().any(|k| k == "merge.merged.v1"),
            "expected merge.merged.v1, saw {kinds:?}"
        );
    }

    #[tokio::test]
    async fn failed_fails_slot_on_live_board() {
        let (merge, mut rx) = dummy_merge();
        merge.submit("b2", "b2", "01ABC").await;
        merge.start("b2").await;
        let _ = next_kind(&mut rx).await;

        let st = CiGateState::new(merge.clone(), None);
        let resp = failed(
            State(st),
            HeaderMap::new(),
            Bytes::from(json!({"bead":"b2","reason":"CI failed: build-test"}).to_string()),
        )
        .await;
        assert_eq!(resp.status(), StatusCode::ACCEPTED);

        let kinds = next_kind(&mut rx).await;
        assert!(
            kinds.iter().any(|k| k == "merge.failed.v1"),
            "expected merge.failed.v1, saw {kinds:?}"
        );
    }

    #[tokio::test]
    async fn unauthorized_when_token_missing() {
        let (merge, _rx) = dummy_merge();
        let st = CiGateState::new(merge, Some("tok".into()));
        let resp = merged(
            State(st),
            HeaderMap::new(),
            Bytes::from(json!({"bead":"b1"}).to_string()),
        )
        .await;
        assert_eq!(resp.status(), StatusCode::UNAUTHORIZED);
    }

    #[tokio::test]
    async fn bad_body_and_empty_bead_are_400() {
        let (merge, _rx) = dummy_merge();
        let st = CiGateState::new(merge, None);
        let resp = merged(State(st.clone()), HeaderMap::new(), Bytes::from("not json")).await;
        assert_eq!(resp.status(), StatusCode::BAD_REQUEST);
        let resp = failed(
            State(st),
            HeaderMap::new(),
            Bytes::from(json!({"bead":"  "}).to_string()),
        )
        .await;
        assert_eq!(resp.status(), StatusCode::BAD_REQUEST);
    }
}
