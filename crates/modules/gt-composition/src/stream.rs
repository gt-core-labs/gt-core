//! Per-workspace SSE event feed (`hq-mcp-dispatch.10`).
//!
//! The streaming side of the MCP-derived API: a `GET /stream` endpoint that fans a
//! workspace's event log out as Server-Sent Events, keyed per `(workspace, channel)`
//! so a subscription only ever sees its own tenant's events. The source is the
//! **path-partitioned** event log (`mt-data.8`): each workspace has its own log
//! directory, so cross-workspace isolation is structural — this handler reads the
//! caller's directory and no other.
//!
//! Per docs/02 (the SSE pattern):
//! - **Cookie auth, not header** (`hq-fe-api-stream.1`) — a browser `EventSource`
//!   cannot set `Authorization`/`X-Workspace` headers, so when a JWT verifier is
//!   configured the tenant comes from the **`gt_web_token` cookie**: the handler
//!   verifies the JWT and keys the feed by its `workspace` claim, never a URL/body
//!   field and never a client-supplied header. A missing/invalid/expired cookie is a
//!   `401` *before* the SSE upgrade. With no verifier configured the endpoint keeps
//!   its prior behaviour — tenant from the server-injected `X-Workspace` header — so
//!   enabling cookie auth is an opt-in env, not a behaviour change.
//! - **Per-workspace keying** — `?channel=<namespace>` narrows the feed to one
//!   domain (`merge`, `convoy`, …).
//! - **Last-Event-ID** — a reconnecting `EventSource` sends the last event's id (its
//!   record `ts`); the handler replays only newer records, then resumes live.
//! - **KeepAlive** — a comment line every 15s so proxies don't drop the idle stream.
//!
//! Live delivery is a **poll-tail** of the partitioned log (there is no broadcast
//! bus on the MCP server path yet): the stream re-reads records newer than the last
//! id every second. That is the docs/02 "polling is fine for cheap state" tier; a
//! `tokio::sync::broadcast` fan-out is the follow-up when one lands on this path.

use std::convert::Infallible;
use std::sync::Arc;
use std::time::Duration;

use axum::extract::{Query, State};
use axum::http::{HeaderMap, StatusCode};
use axum::response::sse::{Event, KeepAlive, Sse};
use axum::response::{IntoResponse, Response};
use axum::routing::get;
use axum::Router;
use serde::Deserialize;
use time::OffsetDateTime;

use gt_auth::JwtClaims;
use gt_eventlog::EventRecord;

use crate::auth::SharedAuthenticator;
use crate::denial_audit::{record_denial, SharedAudit, ANONYMOUS};
use crate::mcp::EventLog;

/// The server-injected tenant header the stream is keyed by when no cookie auth is
/// configured (mirrors the MCP dispatch `X-Workspace` convention). The workspace is
/// never read from the URL or body — only this header — so a caller cannot stream
/// another tenant's feed.
const WORKSPACE_HEADER: &str = "x-workspace";

/// The browser-nav JWT cookie the stream authenticates by (docs/02): an `EventSource`
/// cannot set an `Authorization` header, so it carries the same bearer token in this
/// cookie (`withCredentials: true`). The token is **never** read from the query string
/// — that would leak it into proxy/access logs.
const TOKEN_COOKIE: &str = "gt_web_token";

/// Most records a connect/reconnect replays — a reconnect can't drain an unbounded
/// backlog, and a fresh connect seeds with recent context only.
const REPLAY_CAP: usize = 256;

/// Poll cadence for new records on the partitioned log (the live tail).
const POLL_INTERVAL: Duration = Duration::from_secs(1);

/// Proxy keep-alive cadence — proxies drop idle SSE streams after ~60s (docs/02).
const KEEPALIVE_INTERVAL: Duration = Duration::from_secs(15);

/// The cookie-auth bundle: the verifier the `gt_web_token` JWT is checked with plus
/// the audit sink rejections are recorded into (mirrors [`AuthState`](crate::auth::AuthState)
/// for the REST chain). Present ⇒ the stream requires a valid cookie and keys the feed by
/// the token's verified `workspace` claim; absent ⇒ the legacy `X-Workspace` header path.
#[derive(Clone)]
struct CookieAuth {
    authenticator: SharedAuthenticator,
    audit: SharedAudit,
}

/// Shared state for the feed route: the per-workspace event log, plus the optional
/// cookie-auth bundle (`hq-fe-api-stream.1`). With auth `Some`, the tenant is the verified
/// JWT `workspace` claim; with `None`, it is the `X-Workspace` header (prior behaviour).
#[derive(Clone)]
pub struct FeedState {
    event_log: Arc<EventLog>,
    auth: Option<CookieAuth>,
}

impl FeedState {
    /// Wrap the per-workspace event log the feed streams from, with **no** cookie auth: the
    /// tenant is the server-injected `X-Workspace` header (the endpoint's prior behaviour,
    /// for a deploy that configures no RS256 verifier).
    pub fn new(event_log: Arc<EventLog>) -> Self {
        Self {
            event_log,
            auth: None,
        }
    }

    /// Wrap the event log **with** `gt_web_token` cookie auth (`hq-fe-api-stream.1`): every
    /// connect must present a JWT cookie the `authenticator` verifies, and the feed is keyed
    /// by that token's `workspace` claim. Rejections (`401`) are recorded into `audit`, the
    /// same sink the REST auth chain uses.
    pub fn with_cookie_auth(
        event_log: Arc<EventLog>,
        authenticator: SharedAuthenticator,
        audit: SharedAudit,
    ) -> Self {
        Self {
            event_log,
            auth: Some(CookieAuth {
                authenticator,
                audit,
            }),
        }
    }
}

/// `?channel=<namespace>` narrows the feed to one domain's events (`merge`,
/// `convoy`, `quota`, …). Absent ⇒ the whole workspace feed.
/// `?rig=<name>` further narrows to events whose payload carries `"rig"` equal to
/// `name` (hq-rig-isolation.3). Absent ⇒ all rigs (back-compat).
/// `?topic=board:{rig}:{ws}` / `?topic=doc:{id}` subscribes one Kanban scope
/// (hq-0c8fe1): a board topic delivers the rig's card events + card comments, a
/// doc topic one document's edits + comments. Overrides `channel`/`rig`.
#[derive(Debug, Default, Deserialize)]
pub struct FeedParams {
    #[serde(default)]
    channel: Option<String>,
    #[serde(default)]
    rig: Option<String>,
    #[serde(default)]
    topic: Option<String>,
}

/// One Kanban realtime scope (hq-0c8fe1, ADR hq-423a4b D7): the SSE topics the
/// board UI and the doc editor subscribe. Server-authoritative broadcast — the
/// stream only pushes change events; every write flows through REST/MCP. Doc
/// edits are last-write-wins per block (the ADR's ratified conflict policy);
/// CRDT is explicitly out of scope until conflict pain is demonstrated.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Topic {
    /// `board:{rig}:{workspace}` — the rig's card mutations
    /// (`issues.*`, payload `rig`) plus comments on the rig's cards
    /// (`comments.*`, `target_kind=card`, target id prefixed `{rig}-`).
    Board {
        /// The rig half of the board scope key.
        rig: String,
        /// The workspace half — must match the caller's resolved tenant.
        workspace: String,
    },
    /// `doc:{id}` — one document's edits (`documents.*`) plus its comments
    /// (`comments.*`, `target_kind=doc`).
    Doc {
        /// The document id.
        id: String,
    },
}

impl Topic {
    /// Parse a `?topic=` value. `None` for an unrecognized shape so the handler
    /// can 400 instead of silently streaming everything.
    pub fn parse(raw: &str) -> Option<Self> {
        if let Some(rest) = raw.strip_prefix("board:") {
            let (rig, ws) = rest.split_once(':')?;
            if rig.is_empty() || ws.is_empty() {
                return None;
            }
            return Some(Topic::Board { rig: rig.to_string(), workspace: ws.to_string() });
        }
        if let Some(id) = raw.strip_prefix("doc:") {
            if id.is_empty() {
                return None;
            }
            return Some(Topic::Doc { id: id.to_string() });
        }
        None
    }

    /// Whether one event record belongs to this topic. Pure — unit-testable
    /// without a stream.
    pub fn matches(&self, kind: &str, payload: &serde_json::Value) -> bool {
        let str_field = |k: &str| payload.get(k).and_then(|v| v.as_str());
        match self {
            Topic::Board { rig, .. } => {
                if kind.starts_with("issues.") {
                    return str_field("rig") == Some(rig.as_str());
                }
                if kind.starts_with("comments.") {
                    return str_field("target_kind") == Some("card")
                        && str_field("target_id")
                            .map(|id| id.starts_with(&format!("{rig}-")))
                            .unwrap_or(false);
                }
                false
            }
            Topic::Doc { id } => {
                if kind.starts_with("documents.") {
                    return str_field("id") == Some(id.as_str());
                }
                if kind.starts_with("comments.") {
                    return str_field("target_kind") == Some("doc")
                        && str_field("target_id") == Some(id.as_str());
                }
                false
            }
        }
    }
}

/// The SSE feed router (`GET /stream`), ready to `.merge()` into the server's app.
pub fn feed_router(state: FeedState) -> Router {
    Router::new()
        .route("/stream", get(feed_stream))
        .with_state(state)
}

/// Stream a workspace's event feed as SSE. The tenant is the verified `gt_web_token`
/// cookie's `workspace` claim when cookie auth is configured, else the `X-Workspace`
/// header; `?channel=` narrows to one domain; `Last-Event-ID` resumes from a prior record.
///
/// Authentication (when configured) happens **before** the SSE upgrade — a missing,
/// malformed, unverifiable, or expired cookie is a `401`, audited like the REST chain's
/// token rejections (`hq-auth-guard.3`).
async fn feed_stream(
    State(state): State<FeedState>,
    headers: HeaderMap,
    Query(params): Query<FeedParams>,
) -> Response {
    // Tenant keying. With cookie auth on, the workspace is the verified JWT claim (a
    // browser EventSource can't send X-Workspace); off, it is the server-injected header.
    // Never the query/body either way (docs/04 rule 15).
    let workspace = match resolve_tenant(&state, &headers) {
        Ok(ws) => ws,
        // `resolve_tenant` already audited the denial; the message is the 401 body.
        Err(msg) => return (StatusCode::UNAUTHORIZED, msg).into_response(),
    };
    // Kanban topic subscription (hq-0c8fe1): `?topic=` overrides channel/rig.
    // A board topic spans two domains (issues.* + comments.*), so it reads the
    // unnarrowed log and filters per record; its workspace half must agree with
    // the caller's resolved tenant — a topic is a scope, never an escape hatch.
    let topic = match params.topic.as_deref().filter(|t| !t.is_empty()) {
        Some(raw) => match Topic::parse(raw) {
            Some(t) => Some(t),
            None => {
                return (
                    StatusCode::BAD_REQUEST,
                    "unknown topic (expected board:{rig}:{workspace} or doc:{id})",
                )
                    .into_response()
            }
        },
        None => None,
    };
    if let Some(Topic::Board { workspace: topic_ws, .. }) = &topic {
        match &workspace {
            Some(ws) if ws != topic_ws => {
                return (
                    StatusCode::FORBIDDEN,
                    "topic workspace does not match the caller's tenant",
                )
                    .into_response();
            }
            _ => {}
        }
    }
    let channel = if topic.is_some() {
        None
    } else {
        params.channel.filter(|c| !c.is_empty())
    };
    let rig_filter = if topic.is_some() {
        None
    } else {
        params.rig.filter(|r| !r.is_empty())
    };
    // EventSource resends the last event's id on reconnect; our id is the record ts.
    let resume_from = header(&headers, "last-event-id");

    let log = state.event_log.clone();
    let stream = async_stream::stream! {
        // First pass with `resume_from` replays exactly what a reconnecting client
        // missed (or, on a fresh connect, seeds the recent tail); subsequent passes
        // are the live poll-tail, each reading only records newer than the last id.
        let mut last_ts = resume_from;
        loop {
            let batch = log
                .read_since(workspace.as_deref(), channel.as_deref(), last_ts.as_deref(), REPLAY_CAP)
                .unwrap_or_default();
            for record in batch {
                last_ts = Some(record.ts.clone());
                // hq-rig-isolation.3: skip records whose payload `rig` doesn't match.
                if let Some(r) = &rig_filter {
                    let payload_rig = record.payload.get("rig").and_then(|v| v.as_str());
                    if payload_rig != Some(r.as_str()) {
                        continue;
                    }
                }
                // hq-0c8fe1: a topic subscription only sees its own scope.
                if let Some(t) = &topic {
                    if !t.matches(&record.kind, &record.payload) {
                        continue;
                    }
                }
                // Annotate the error type: the handler now returns `Response`, so the stream's
                // `Infallible` error is no longer inferred from a `Sse<…>` return type.
                yield Ok::<Event, Infallible>(record_event(&record));
            }
            tokio::time::sleep(POLL_INTERVAL).await;
        }
    };

    Sse::new(stream)
        .keep_alive(
            KeepAlive::new()
                .interval(KEEPALIVE_INTERVAL)
                .text("keep-alive"),
        )
        .into_response()
}

/// Resolve the tenant the feed is keyed by, enforcing cookie auth when configured.
///
/// - **Cookie auth on** (`state.auth` is `Some`): read the `gt_web_token` cookie, verify it
///   through the authenticator, and gate its time/workspace invariants
///   ([`JwtClaims::validate`] — the verifier checks the signature only). The tenant is the
///   verified `workspace` claim. Any failure is a `401`, audited as an *unauthenticated*
///   denial (the actor is [`ANONYMOUS`]: a credential we couldn't trust names no one).
/// - **Cookie auth off** (`None`): the tenant is the `X-Workspace` header, or `None` (the
///   whole-server-feed legacy shape) when absent.
///
/// On failure returns the `401` body message; the denial is recorded into the audit sink as
/// a side effect (so the caller only has to turn the message into a response).
fn resolve_tenant(state: &FeedState, headers: &HeaderMap) -> Result<Option<String>, &'static str> {
    let Some(auth) = &state.auth else {
        // Legacy path: server-injected header, no authentication.
        return Ok(header(headers, WORKSPACE_HEADER));
    };

    // Audit an *unauthenticated* denial (`hq-auth-guard.3`) and return the body message. The
    // actor is ANONYMOUS: a credential we couldn't verify names no one.
    let reject = |msg: &'static str| -> &'static str {
        record_denial(
            auth.audit.as_ref(),
            ANONYMOUS,
            None,
            &axum::http::Method::GET,
            &"/stream".parse().expect("static uri"),
            None,
            StatusCode::UNAUTHORIZED,
        );
        msg
    };

    let token = match cookie(headers, TOKEN_COOKIE) {
        Some(t) => t,
        None => return Err(reject("missing gt_web_token cookie")),
    };
    let claims = match auth.authenticator.authenticate(&token) {
        Ok(claims) => claims,
        Err(_) => return Err(reject("invalid gt_web_token cookie")),
    };
    // Signature verified; now the clock + workspace-presence gate the verifier defers to us.
    let now = OffsetDateTime::now_utc().unix_timestamp().max(0) as u64;
    if claims
        .validate(now, JwtClaims::workspace_optional_from_env())
        .is_err()
    {
        return Err(reject("expired or incomplete gt_web_token cookie"));
    }
    Ok(Some(claims.workspace))
}

/// Value of a named cookie from the `Cookie` request header, trimmed and non-empty, or
/// `None`. Parses the `name=value; name2=value2` list without pulling in a cookie-jar
/// dependency (the token is the only cookie this endpoint reads).
fn cookie(headers: &HeaderMap, name: &str) -> Option<String> {
    headers
        .get(axum::http::header::COOKIE)
        .and_then(|v| v.to_str().ok())?
        .split(';')
        .filter_map(|pair| pair.split_once('='))
        .find(|(k, _)| k.trim() == name)
        .map(|(_, v)| v.trim().to_string())
        .filter(|v| !v.is_empty())
}

/// Trimmed, non-empty value of a request header, or `None`.
fn header(headers: &HeaderMap, name: &str) -> Option<String> {
    headers
        .get(name)
        .and_then(|v| v.to_str().ok())
        .map(str::trim)
        .filter(|s| !s.is_empty())
        .map(str::to_string)
}

/// Render one log record as an SSE event: the event name is the versioned kind
/// (`merge.merged.v1`, so clients subscribe by name), the id is the record `ts`
/// (the `Last-Event-ID` resume marker), and the data is the payload JSON.
fn record_event(record: &EventRecord) -> Event {
    Event::default()
        .event(record.kind.clone())
        .id(record.ts.clone())
        .data(record.payload.to_string())
}

#[cfg(test)]
mod tests {
    use super::*;
    use gt_events::EventKind;
    use serde::Serialize;
    use tempfile::TempDir;

    /// A minimal event for driving the log in tests.
    #[derive(Serialize)]
    struct TestEvent {
        kind: &'static str,
        n: u64,
    }
    impl EventKind for TestEvent {
        fn kind(&self) -> &'static str {
            self.kind
        }
    }

    fn log(dir: &TempDir) -> EventLog {
        EventLog::new(Some(dir.path().to_path_buf()))
    }

    /// `read_since` keys by workspace + channel + resume marker. This is the core of
    /// the SSE feed (the handler is a thin poll-tail over it).
    #[test]
    fn read_since_filters_by_channel_and_resume_marker() {
        let dir = TempDir::new().unwrap();
        let log = log(&dir);

        // Two channels in one workspace's log.
        log.append(
            Some("acme"),
            TestEvent {
                kind: "merge.submitted.v1",
                n: 1,
            },
        )
        .unwrap();
        log.append(
            Some("acme"),
            TestEvent {
                kind: "convoy.launched.v1",
                n: 2,
            },
        )
        .unwrap();
        log.append(
            Some("acme"),
            TestEvent {
                kind: "merge.merged.v1",
                n: 3,
            },
        )
        .unwrap();

        // Whole feed.
        let all = log.read_since(Some("acme"), None, None, 256).unwrap();
        assert_eq!(all.len(), 3);

        // Channel filter: only merge.* (not convoy, not a "merger" false-prefix).
        let merges = log
            .read_since(Some("acme"), Some("merge"), None, 256)
            .unwrap();
        assert_eq!(merges.len(), 2);
        assert!(merges.iter().all(|r| r.kind.starts_with("merge.")));

        // Resume marker: only records strictly newer than a given ts.
        let first_ts = all[0].ts.clone();
        let after = log
            .read_since(Some("acme"), None, Some(&first_ts), 256)
            .unwrap();
        assert!(after.iter().all(|r| r.ts.as_str() > first_ts.as_str()));
        assert!(
            after.len() < all.len(),
            "resume excludes the marker and older"
        );
    }

    /// The feed is path-partitioned per workspace: one tenant's stream never sees
    /// another's events (the cross-workspace isolation guarantee).
    #[test]
    fn read_since_is_isolated_per_workspace() {
        let dir = TempDir::new().unwrap();
        let log = log(&dir);

        log.append(
            Some("acme"),
            TestEvent {
                kind: "merge.merged.v1",
                n: 1,
            },
        )
        .unwrap();
        log.append(
            Some("beta"),
            TestEvent {
                kind: "merge.merged.v1",
                n: 2,
            },
        )
        .unwrap();

        let acme = log.read_since(Some("acme"), None, None, 256).unwrap();
        let beta = log.read_since(Some("beta"), None, None, 256).unwrap();
        assert_eq!(acme.len(), 1);
        assert_eq!(beta.len(), 1);
        // Distinct payloads — no bleed between tenants.
        assert_eq!(acme[0].payload["n"], 1);
        assert_eq!(beta[0].payload["n"], 2);
    }

    /// `limit` caps the batch from the newest end.
    #[test]
    fn read_since_caps_from_the_newest_end() {
        let dir = TempDir::new().unwrap();
        let log = log(&dir);
        for n in 0..10 {
            log.append(
                Some("acme"),
                TestEvent {
                    kind: "merge.merged.v1",
                    n,
                },
            )
            .unwrap();
        }
        let capped = log.read_since(Some("acme"), None, None, 3).unwrap();
        assert_eq!(capped.len(), 3, "only the newest 3 are returned");
        // Newest end: the last appended (n=9) is included.
        assert_eq!(capped.last().unwrap().payload["n"], 9);
    }
}

/// Cookie auth gate (`hq-fe-api-stream.1`): with a verifier wired, `/stream` requires the
/// `gt_web_token` cookie, keys the feed by its verified `workspace` claim (never a header),
/// and rejects a missing/invalid/expired cookie with a `401` audited like the REST chain.
///
/// Drives the real `feed_router` in-process (`tower` `oneshot`), reading the streaming
/// Server-Sent-Events body for the happy path. Pure (tempdir log + in-memory doubles).
#[cfg(test)]
mod cookie_auth_tests {
    use super::*;
    use axum::body::Body;
    use axum::http::{header, Request, StatusCode};
    use gt_audit::{InMemoryAudit, Outcome};
    use gt_auth::{InMemoryAuthenticator, JwtClaims};
    use gt_events::EventKind;
    use http_body_util::BodyExt;
    use serde::Serialize;
    use serde_json::Value;
    use std::time::Duration;
    use tempfile::TempDir;
    use tower::ServiceExt; // oneshot

    #[derive(Serialize)]
    struct TestEvent {
        #[serde(skip)]
        kind: &'static str,
        n: u64,
    }
    impl EventKind for TestEvent {
        fn kind(&self) -> &'static str {
            self.kind
        }
    }

    /// Claims for `sub`/`workspace`, far-future expiry (a live token) unless `exp` overridden.
    fn claims(workspace: &str, exp: u64) -> JwtClaims {
        JwtClaims {
            sub: "alice".into(),
            workspace: workspace.into(),
            scopes: vec![],
            exp,
            nbf: None,
            iat: 0,
        }
    }

    /// A `FeedState` with cookie auth over `token -> claims`, plus the audit sink so a test can
    /// inspect recorded denials. The log lives in `dir`.
    fn cookie_state(dir: &TempDir, token: &str, claims: JwtClaims) -> (FeedState, SharedAudit) {
        let log = Arc::new(EventLog::new(Some(dir.path().to_path_buf())));
        let audit: SharedAudit = Arc::new(InMemoryAudit::new());
        let verifier: SharedAuthenticator =
            Arc::new(InMemoryAuthenticator::new().with_token(token, claims));
        (
            FeedState::with_cookie_auth(log, verifier, audit.clone()),
            audit,
        )
    }

    /// Issue a `GET /stream` against the feed router, optionally with a `Cookie` and an
    /// `X-Workspace` header, returning the whole response.
    async fn get_stream(
        state: FeedState,
        cookie: Option<&str>,
        x_workspace: Option<&str>,
    ) -> axum::response::Response {
        let mut b = Request::builder().uri("/stream");
        if let Some(c) = cookie {
            b = b.header(header::COOKIE, c);
        }
        if let Some(ws) = x_workspace {
            b = b.header("x-workspace", ws);
        }
        feed_router(state)
            .oneshot(b.body(Body::empty()).unwrap())
            .await
            .unwrap()
    }

    /// Read the SSE body until one `data:` frame is parsed or a bounded wait elapses (the
    /// stream stays open for keep-alive, so we never read to EOF).
    async fn first_event(body: Body) -> Option<Value> {
        let mut buf = String::new();
        let mut body = body;
        let deadline = tokio::time::Instant::now() + Duration::from_secs(3);
        while tokio::time::Instant::now() < deadline {
            let frame = match tokio::time::timeout(Duration::from_millis(200), body.frame()).await {
                Ok(Some(Ok(f))) => f,
                Ok(Some(Err(_))) | Ok(None) => break,
                Err(_) => continue,
            };
            if let Some(chunk) = frame.data_ref() {
                buf.push_str(&String::from_utf8_lossy(chunk));
            }
            for line in buf.lines() {
                if let Some(v) = line.strip_prefix("data:") {
                    if let Ok(val) = serde_json::from_str::<Value>(v.trim()) {
                        return Some(val);
                    }
                }
            }
        }
        None
    }

    #[tokio::test]
    async fn a_valid_cookie_streams_the_claim_workspace() {
        let dir = TempDir::new().unwrap();
        // Two tenants in distinct partitions; the cookie's claim is `acme`.
        let log = EventLog::new(Some(dir.path().to_path_buf()));
        log.append(
            Some("acme"),
            TestEvent {
                kind: "merge.merged.v1",
                n: 1,
            },
        )
        .unwrap();
        log.append(
            Some("beta"),
            TestEvent {
                kind: "merge.merged.v1",
                n: 2,
            },
        )
        .unwrap();

        let (state, audit) = cookie_state(&dir, "good", claims("acme", 9_999_999_999));
        // An `X-Workspace: beta` header is present but must be ignored — the claim wins.
        let resp = get_stream(state, Some("gt_web_token=good"), Some("beta")).await;
        assert_eq!(resp.status(), StatusCode::OK);

        let event = first_event(resp.into_body())
            .await
            .expect("an SSE data frame");
        assert_eq!(
            event["n"], 1,
            "streamed acme's event (the claim), not beta's header"
        );
        assert!(
            audit.read_all().unwrap().is_empty(),
            "a valid cookie is not a denial"
        );
    }

    #[tokio::test]
    async fn a_missing_cookie_is_unauthorized_and_audited() {
        let dir = TempDir::new().unwrap();
        let (state, audit) = cookie_state(&dir, "good", claims("acme", 9_999_999_999));
        let resp = get_stream(state, None, Some("acme")).await;
        assert_eq!(resp.status(), StatusCode::UNAUTHORIZED);
        let rows = audit.read_all().unwrap();
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].outcome, Outcome::Unauthorized);
        assert_eq!(rows[0].actor, ANONYMOUS);
        assert_eq!(rows[0].args["verdict"], "unauthenticated");
    }

    #[tokio::test]
    async fn an_invalid_cookie_token_is_unauthorized_and_audited() {
        let dir = TempDir::new().unwrap();
        let (state, audit) = cookie_state(&dir, "good", claims("acme", 9_999_999_999));
        // A different token value than the verifier knows → rejected.
        let resp = get_stream(state, Some("gt_web_token=nope"), None).await;
        assert_eq!(resp.status(), StatusCode::UNAUTHORIZED);
        assert_eq!(audit.read_all().unwrap().len(), 1);
    }

    #[tokio::test]
    async fn an_expired_cookie_is_unauthorized() {
        let dir = TempDir::new().unwrap();
        // exp in the past → JwtClaims::validate rejects even though the signature "verifies".
        let (state, audit) = cookie_state(&dir, "good", claims("acme", 1));
        let resp = get_stream(state, Some("gt_web_token=good"), None).await;
        assert_eq!(resp.status(), StatusCode::UNAUTHORIZED);
        assert_eq!(audit.read_all().unwrap().len(), 1);
    }

    #[tokio::test]
    async fn the_cookie_is_picked_out_of_a_multi_cookie_header() {
        let dir = TempDir::new().unwrap();
        let log = EventLog::new(Some(dir.path().to_path_buf()));
        log.append(
            Some("acme"),
            TestEvent {
                kind: "merge.merged.v1",
                n: 7,
            },
        )
        .unwrap();
        let (state, _audit) = cookie_state(&dir, "good", claims("acme", 9_999_999_999));
        // Surrounded by other cookies, with spacing — must still be parsed.
        let resp = get_stream(
            state,
            Some("theme=dark; gt_web_token=good; sid=abc123"),
            None,
        )
        .await;
        assert_eq!(resp.status(), StatusCode::OK);
        let event = first_event(resp.into_body())
            .await
            .expect("an SSE data frame");
        assert_eq!(event["n"], 7);
    }

    /// Without cookie auth (`FeedState::new`) the endpoint keeps its prior behaviour: the
    /// `X-Workspace` header keys the feed and no cookie is required.
    #[tokio::test]
    async fn without_auth_the_x_workspace_header_still_keys_the_feed() {
        let dir = TempDir::new().unwrap();
        let log = EventLog::new(Some(dir.path().to_path_buf()));
        log.append(
            Some("acme"),
            TestEvent {
                kind: "merge.merged.v1",
                n: 3,
            },
        )
        .unwrap();
        let state = FeedState::new(Arc::new(EventLog::new(Some(dir.path().to_path_buf()))));
        let resp = get_stream(state, None, Some("acme")).await;
        assert_eq!(resp.status(), StatusCode::OK);
        let event = first_event(resp.into_body())
            .await
            .expect("an SSE data frame");
        assert_eq!(event["n"], 3);
    }
}

#[cfg(test)]
mod topic_tests {
    use super::Topic;
    use serde_json::json;

    #[test]
    fn parses_board_and_doc_topics_and_rejects_garbage() {
        assert_eq!(
            Topic::parse("board:hq:default"),
            Some(Topic::Board { rig: "hq".into(), workspace: "default".into() })
        );
        assert_eq!(Topic::parse("doc:01ABC"), Some(Topic::Doc { id: "01ABC".into() }));
        for bad in ["board:hq", "board::ws", "board:hq:", "doc:", "kanban:x", ""] {
            assert!(Topic::parse(bad).is_none(), "{bad} must not parse");
        }
    }

    #[test]
    fn board_topic_matches_rig_issue_events_and_card_comments() {
        let t = Topic::parse("board:hq:default").unwrap();
        // The rig's card mutations…
        assert!(t.matches("issues.transitioned.v1", &json!({ "rig": "hq", "id": "hq-1" })));
        // …but not another rig's.
        assert!(!t.matches("issues.transitioned.v1", &json!({ "rig": "gtweb" })));
        // Comments on the rig's cards ride the board topic.
        assert!(t.matches(
            "comments.created.v1",
            &json!({ "target_kind": "card", "target_id": "hq-62130a" })
        ));
        assert!(!t.matches(
            "comments.created.v1",
            &json!({ "target_kind": "card", "target_id": "gtweb-9" })
        ));
        // Doc comments and unrelated domains do not.
        assert!(!t.matches(
            "comments.created.v1",
            &json!({ "target_kind": "doc", "target_id": "01ABC" })
        ));
        assert!(!t.matches("merge.completed.v1", &json!({ "rig": "hq" })));
    }

    #[test]
    fn doc_topic_matches_one_documents_edits_and_comments() {
        let t = Topic::parse("doc:01ABC").unwrap();
        assert!(t.matches("documents.updated.v1", &json!({ "id": "01ABC", "version": 3 })));
        assert!(!t.matches("documents.updated.v1", &json!({ "id": "01XYZ" })));
        assert!(t.matches(
            "comments.created.v1",
            &json!({ "target_kind": "doc", "target_id": "01ABC" })
        ));
        assert!(!t.matches(
            "comments.created.v1",
            &json!({ "target_kind": "card", "target_id": "01ABC" })
        ));
        assert!(!t.matches("issues.updated.v1", &json!({ "id": "01ABC" })));
    }
}
