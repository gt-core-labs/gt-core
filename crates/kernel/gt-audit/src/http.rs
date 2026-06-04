//! The `axum` REST adapter for the read-only `audit.tail` surface (`hq-fe-api-kernel.2`).
//!
//! Exposes the MCP audit trail as a REST route that tails the **same** [`AuditSink`] the MCP
//! `audit.tail` tool reads — no domain logic is duplicated, only the wire shape differs. It folds
//! into `gt-audit` behind the off-by-default `axum` feature (docs/03 Rule 4), exactly as
//! gt-quota/gt-graphindex gate theirs, rather than living in a sibling crate.
//!
//! ## Per-tenant by construction
//!
//! Every returned record is filtered to the caller's resolved workspace, read straight off the
//! `X-Workspace` header — **kernel-pure**, so this crate takes no `gt-workspace` dependency (a
//! forbidden kernel→domain edge). An absent header resolves to [`DEFAULT_WORKSPACE`], matching the
//! MCP dispatch default. A record stamped for workspace `B` is invisible to a tail run in
//! workspace `A` — the cross-workspace leak invariant the `mcp_audit.workspace_id` column exists
//! to uphold (a SOC2 / GDPR per-tenant dump must never leak another tenant's calls).
//!
//! ## What it does *not* do
//!
//! - **It does not authenticate or authorize.** The builder mounts this router under
//!   `/api/v1/audit` and wraps it with the capability-derived scope guard; the composition root
//!   layers the auth middleware in front. The single route is a GET, so the guard requires
//!   `audit.read`.

use std::sync::Arc;

use axum::extract::{Query, State};
use axum::http::{HeaderMap, StatusCode};
use axum::response::{IntoResponse, Response};
use axum::routing::get;
use axum::{Json, Router};
use serde::Deserialize;
use serde_json::{json, Value};

use crate::{AppError, AuditRecord, AuditSink};

/// The header carrying the request's tenant — server-injected by the auth/workspace middleware,
/// never read from a URL or body field (the same `X-Workspace` convention the MCP dispatch keys on).
const WORKSPACE_HEADER: &str = "x-workspace";

/// The workspace a request resolves to when it carries no `X-Workspace` header — the single-tenant
/// default, matching the MCP `audit.tail` default and the `mcp_audit.workspace_id` column default.
const DEFAULT_WORKSPACE: &str = "default";

/// Default `limit` when the caller omits it — the same cap the MCP `audit.tail` uses.
fn default_limit() -> usize {
    20
}

/// Everything the audit REST handler needs, baked into the router with [`Router::with_state`]
/// before it leaves [`audit_router`] so the merged application router carries no outstanding state
/// type (the kernel's state-erased `Router<()>` contract).
#[derive(Clone)]
pub struct AuditApiState {
    /// The shared audit sink — pass the same `Arc` the server records into, so the tail reflects
    /// the live trail (the module owns no store of its own).
    sink: Arc<dyn AuditSink + Send + Sync>,
}

impl AuditApiState {
    /// Build the REST state over the server's shared audit sink.
    pub fn new(sink: Arc<dyn AuditSink + Send + Sync>) -> Self {
        Self { sink }
    }
}

/// Build the read-only audit REST router with `state` baked in (`hq-fe-api-kernel.2`).
///
/// The path is **relative**: the builder nests it under `/api/v1/audit` and applies the scope
/// guard. `register_routes` on the HTTP-enabled audit module returns exactly this router.
///
/// | Method + path | Maps to MCP tool |
/// |---------------|------------------|
/// | `GET /`       | `audit.tail`     |
pub fn audit_router(state: AuditApiState) -> Router {
    Router::new().route("/", get(tail)).with_state(state)
}

/// Querystring for `GET /` — the REST mirror of the `audit.tail` filters. Every filter is optional;
/// an empty query tails the most recent [`default_limit`] records for the caller's workspace.
#[derive(Debug, Default, Deserialize)]
struct AuditQuery {
    /// Keep only records whose `actor` equals this (exact match).
    actor: Option<String>,
    /// Keep only records whose `tool` equals this (exact match).
    tool: Option<String>,
    /// Keep only records with this outcome (`"invoked"` | `"unauthorized"`).
    outcome: Option<String>,
    /// RFC3339 inclusive lower bound on `ts`. Records with an empty `ts` are dropped when this is
    /// set (they cannot be compared).
    since: Option<String>,
    /// Max records returned, most recent first.
    #[serde(default = "default_limit")]
    limit: usize,
}

/// `GET /` — tail the audit trail for the caller's workspace, most recent first (`audit.tail`).
/// Reuses the shared sink's `read_all`, then applies the per-tenant gate + the supplied filters,
/// so the window matches the MCP read exactly.
#[cfg_attr(feature = "axum", utoipa::path(
    get, path = "/",
    params(
        ("actor" = Option<String>, Query, description = "Exact actor match"),
        ("tool" = Option<String>, Query, description = "Exact tool match"),
        ("outcome" = Option<String>, Query, description = "invoked | unauthorized"),
        ("since" = Option<String>, Query, description = "RFC3339 inclusive lower bound on ts"),
        ("limit" = Option<usize>, Query, description = "Max records, most recent first (default 20)"),
    ),
    responses((status = 200, description = "The most-recent-first window for the caller's workspace")),
))]
async fn tail(
    State(st): State<AuditApiState>,
    headers: HeaderMap,
    Query(q): Query<AuditQuery>,
) -> Result<Json<Value>, ApiError> {
    let ws = workspace_of(&headers);
    let ws = ws.as_deref().unwrap_or(DEFAULT_WORKSPACE);
    let records = tail_records(&*st.sink, ws, &q)?;
    Ok(Json(json!({ "count": records.len(), "records": records })))
}

/// Read the whole trail, apply the per-tenant gate + filters, then take the newest `limit` and
/// reverse so the response leads with the most recent call — the REST mirror of the MCP
/// `AuditHandler::tail`.
fn tail_records(
    sink: &(dyn AuditSink + Send + Sync),
    ws: &str,
    q: &AuditQuery,
) -> Result<Vec<AuditRecord>, ApiError> {
    let all = sink.read_all()?;
    let mut hits: Vec<AuditRecord> = all.into_iter().filter(|r| record_matches(r, ws, q)).collect();
    if hits.len() > q.limit {
        hits.drain(0..hits.len() - q.limit);
    }
    hits.reverse();
    Ok(hits)
}

/// Whether one record passes the tenant gate + every supplied filter (mirrors the MCP handler).
fn record_matches(r: &AuditRecord, ws: &str, q: &AuditQuery) -> bool {
    if r.workspace_id != ws {
        return false; // per-tenant gate — never leak another workspace's calls.
    }
    if q.actor.as_deref().is_some_and(|a| a != r.actor) {
        return false;
    }
    if q.tool.as_deref().is_some_and(|t| t != r.tool) {
        return false;
    }
    if let Some(want) = q.outcome.as_deref() {
        // Compare against the snake_case wire form (`invoked` / `unauthorized`).
        let got = serde_json::to_value(r.outcome).ok();
        if got.as_ref().and_then(Value::as_str) != Some(want) {
            return false;
        }
    }
    if let Some(since) = q.since.as_deref() {
        // RFC3339 sorts lexicographically, so a string compare is a time compare. A record with no
        // timestamp cannot satisfy a `since` bound.
        if r.ts.is_empty() || r.ts.as_str() < since {
            return false;
        }
    }
    true
}

/// The combined OpenAPI document for the read-only audit REST surface (`hq-fe-api-kernel.2`). The
/// builder mounts it under the module prefix and rewrites its relative path to `/api/v1/audit`, so
/// the `#[utoipa::path]` annotation stays prefix-free.
#[derive(utoipa::OpenApi)]
#[openapi(paths(tail))]
pub struct ApiDoc;

/// Read the request's tenant from the `X-Workspace` header, or `None` when it is absent/empty (the
/// caller then resolves to [`DEFAULT_WORKSPACE`]). Never read from a URL or body field (docs/03
/// Rule 6).
fn workspace_of(headers: &HeaderMap) -> Option<String> {
    headers
        .get(WORKSPACE_HEADER)
        .and_then(|v| v.to_str().ok())
        .map(str::to_string)
        .filter(|s| !s.is_empty())
}

/// HTTP wrapper over the audit [`AppError`] so the handler can `?`-propagate a sink failure. The
/// only failure shape is a sink I/O error, which is a server `500`.
#[derive(Debug)]
struct ApiError(AppError);

impl From<AppError> for ApiError {
    fn from(e: AppError) -> Self {
        ApiError(e)
    }
}

impl IntoResponse for ApiError {
    fn into_response(self) -> Response {
        // The only failure shape is a sink I/O error — a server fault.
        (StatusCode::INTERNAL_SERVER_ERROR, self.0.to_string()).into_response()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{InMemoryAudit, Outcome};
    use utoipa::OpenApi;

    fn seeded() -> Arc<dyn AuditSink + Send + Sync> {
        let sink = InMemoryAudit::new();
        for r in [
            AuditRecord::new("alice", "issues.close.execute", json!({}), Outcome::Invoked, "2026-06-01T10:00:00Z").in_workspace("acme"),
            AuditRecord::new("bob", "merge.submit.execute", json!({}), Outcome::Unauthorized, "2026-06-02T10:00:00Z").in_workspace("acme"),
            AuditRecord::new("alice", "rig.add.execute", json!({}), Outcome::Invoked, "2026-06-03T10:00:00Z").in_workspace("acme"),
            AuditRecord::new("mallory", "issues.close.execute", json!({}), Outcome::Invoked, "2026-06-03T11:00:00Z").in_workspace("other"),
        ] {
            sink.record(r).unwrap();
        }
        Arc::new(sink)
    }

    fn q() -> AuditQuery {
        AuditQuery { limit: default_limit(), ..Default::default() }
    }

    #[test]
    fn openapi_lists_the_relative_route_prefix_free() {
        let doc = ApiDoc::openapi();
        let paths: Vec<&str> = doc.paths.paths.keys().map(String::as_str).collect();
        assert!(paths.contains(&"/"), "{paths:?}");
        assert!(paths.iter().all(|p| !p.contains("/api/v1")), "{paths:?}");
    }

    #[test]
    fn tail_gate_is_per_tenant_and_most_recent_first() {
        let sink = seeded();
        let acme = tail_records(&*sink, "acme", &q()).unwrap();
        assert_eq!(acme.len(), 3, "the three acme records, not the `other` one");
        assert_eq!(acme[0].tool, "rig.add.execute", "newest first");
        assert!(acme.iter().all(|r| r.actor != "mallory"), "no cross-tenant leak");
        // The other tenant sees only its one record.
        let other = tail_records(&*sink, "other", &q()).unwrap();
        assert_eq!(other.len(), 1);
        assert_eq!(other[0].actor, "mallory");
    }

    #[test]
    fn filters_by_actor_outcome_since_and_limit() {
        let sink = seeded();
        let by_actor = tail_records(&*sink, "acme", &AuditQuery { actor: Some("alice".into()), ..q() }).unwrap();
        assert_eq!(by_actor.len(), 2);
        let by_outcome = tail_records(&*sink, "acme", &AuditQuery { outcome: Some("unauthorized".into()), ..q() }).unwrap();
        assert_eq!(by_outcome.len(), 1);
        assert_eq!(by_outcome[0].actor, "bob");
        let since = tail_records(&*sink, "acme", &AuditQuery { since: Some("2026-06-02T00:00:00Z".into()), ..q() }).unwrap();
        assert_eq!(since.len(), 2);
        let capped = tail_records(&*sink, "acme", &AuditQuery { limit: 1, ..q() }).unwrap();
        assert_eq!(capped.len(), 1);
        assert_eq!(capped[0].tool, "rig.add.execute", "the newest");
    }

    #[test]
    fn sink_error_maps_to_500() {
        assert_eq!(
            ApiError(AppError::Sink("boom".into())).into_response().status(),
            StatusCode::INTERNAL_SERVER_ERROR
        );
    }
}
