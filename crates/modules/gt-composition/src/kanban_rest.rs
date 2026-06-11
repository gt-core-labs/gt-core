//! Kanban bridge REST surface (hq-95c2bb, epic hq-56b5ee).
//!
//! The gt-web Kanban needs comments, report generation, and invites over
//! cookie-auth HTTP — capabilities that live as DOMAIN TOOLS (`comments.*`,
//! `report.*`, `invite.*`, MCP-first per ADR D6). This router is a thin BRIDGE:
//! each route authorizes the caller (same RS256 cookie/bearer + scope discipline
//! as the notifications REST), shapes the HTTP request into the tool's args, and
//! dispatches through the SAME [`DomainRouter`] the MCP path uses — zero logic
//! forks, one contract.
//!
//! | Method + path                    | Tool                       | Scope            |
//! |----------------------------------|----------------------------|------------------|
//! | `GET    /api/v1/comments`        | `comments.list.execute`    | `comments.read`  |
//! | `POST   /api/v1/comments`        | `comments.create.execute`  | `comments.write` |
//! | `PATCH  /api/v1/comments/:id`    | `comments.update.execute`  | `comments.write` |
//! | `DELETE /api/v1/comments/:id`    | `comments.delete.execute`  | `comments.write` |
//! | `POST   /api/v1/report/generate` | `report.generate`          | `documents.write`|
//! | `GET    /api/v1/invites`         | `invite.list`              | `invites.read`   |
//! | `POST   /api/v1/invites`         | `invite.create`            | `invites.write`  |
//! | `DELETE /api/v1/invites/:id`     | `invite.revoke`            | `invites.write`  |
//! | `POST   /api/v1/invites/accept`  | `invite.accept`            | (any valid session) |
//!
//! The seeded admin's `*` wildcard satisfies every scope. `invite.accept` only
//! needs an authenticated session: the token in the body is the capability, and
//! the accept binds THAT identity's email per the invite row.

use std::sync::Arc;

use axum::extract::{Path, Query, State};
use axum::http::{HeaderMap, Method, StatusCode};
use axum::response::{IntoResponse, Response};
use axum::routing::{delete, get, post};
use axum::{Json, Router};
use serde_json::{json, Value};
use time::OffsetDateTime;

use gt_auth::JwtClaims;
use gt_mcp_server::{DomainCtx, DomainRouter};
use gt_store_dolt::AppError;

use crate::auth::SharedAuthenticator;
use crate::denial_audit::{record_denial, SharedAudit, ANONYMOUS};

const TOKEN_COOKIE: &str = "gt_web_token";
const SCOPE_WILDCARD: &str = "*";

/// State for the bridge: the verifier + audit (the notifications-REST auth
/// discipline) and the live domain router the tools dispatch through.
#[derive(Clone)]
pub struct KanbanRestState {
    authenticator: SharedAuthenticator,
    audit: SharedAudit,
    domains: Arc<DomainRouter>,
}

impl KanbanRestState {
    /// Wire the verifier, the audit sink, and the domain router.
    pub fn new(
        authenticator: SharedAuthenticator,
        audit: SharedAudit,
        domains: Arc<DomainRouter>,
    ) -> Self {
        Self { authenticator, audit, domains }
    }
}

/// The bridge router, ready to `.merge()` into the server app.
pub fn kanban_rest_router(state: KanbanRestState) -> Router {
    Router::new()
        .route("/api/v1/comments", get(comments_list).post(comments_create))
        .route(
            "/api/v1/comments/:id",
            axum::routing::patch(comments_update).delete(comments_delete),
        )
        .route("/api/v1/report/generate", post(report_generate))
        .route("/api/v1/analytics/summary", get(analytics_summary))
        .route("/api/v1/invites", get(invites_list).post(invites_create))
        .route("/api/v1/invites/:id", delete(invites_revoke))
        .route("/api/v1/invites/accept", post(invites_accept))
        .with_state(state)
}

/// Authorize one bridge call: cookie/bearer → verified claims → scope check
/// (`""` = any authenticated session). Mirrors the notifications REST.
fn authorize(
    st: &KanbanRestState,
    headers: &HeaderMap,
    scope: &'static str,
    method: &Method,
    path: &'static str,
) -> Result<JwtClaims, Response> {
    let reject = |status: StatusCode, msg: &'static str| -> Response {
        record_denial(
            st.audit.as_ref(),
            ANONYMOUS,
            None,
            method,
            &path.parse().expect("static uri"),
            (!scope.is_empty()).then_some(scope),
            status,
        );
        (status, msg).into_response()
    };
    let token = bearer(headers)
        .or_else(|| cookie(headers, TOKEN_COOKIE))
        .ok_or_else(|| reject(StatusCode::UNAUTHORIZED, "missing gt_web_token cookie or bearer"))?;
    let claims = st
        .authenticator
        .authenticate(&token)
        .map_err(|_| reject(StatusCode::UNAUTHORIZED, "invalid token"))?;
    let now = OffsetDateTime::now_utc().unix_timestamp().max(0) as u64;
    if claims.validate(now, JwtClaims::workspace_optional_from_env()).is_err() {
        return Err(reject(StatusCode::UNAUTHORIZED, "expired or incomplete token"));
    }
    if !scope.is_empty()
        && !claims.scopes.iter().any(|s| s == SCOPE_WILDCARD || s == scope)
    {
        return Err(reject(StatusCode::FORBIDDEN, "missing scope"));
    }
    Ok(claims)
}

/// Dispatch one tool through the domain router with the caller's identity, and
/// shape the outcome as HTTP (the same status map the issues REST uses).
async fn dispatch(st: &KanbanRestState, claims: &JwtClaims, tool: &str, args: Value) -> Response {
    let ctx = DomainCtx {
        workspace: Some(claims.workspace.as_str()),
        actor: &claims.sub,
        args,
    };
    match st.domains.dispatch(tool, ctx).await {
        Ok(Some(value)) => Json(value).into_response(),
        Ok(None) => (StatusCode::NOT_FOUND, "tool namespace not wired on this deploy").into_response(),
        Err(AppError::NotFound(m)) => (StatusCode::NOT_FOUND, m).into_response(),
        Err(AppError::Validation(m)) => (StatusCode::UNPROCESSABLE_ENTITY, m).into_response(),
        Err(e) => (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()).into_response(),
    }
}

// ─── comments ────────────────────────────────────────────────────────────────

async fn comments_list(
    State(st): State<KanbanRestState>,
    headers: HeaderMap,
    Query(q): Query<Value>,
) -> Response {
    let claims = match authorize(&st, &headers, "comments.read", &Method::GET, "/api/v1/comments") {
        Ok(c) => c,
        Err(r) => return r,
    };
    dispatch(&st, &claims, "comments.list.execute", q).await
}

async fn comments_create(
    State(st): State<KanbanRestState>,
    headers: HeaderMap,
    Json(body): Json<Value>,
) -> Response {
    let claims = match authorize(&st, &headers, "comments.write", &Method::POST, "/api/v1/comments") {
        Ok(c) => c,
        Err(r) => return r,
    };
    dispatch(&st, &claims, "comments.create.execute", body).await
}

async fn comments_update(
    State(st): State<KanbanRestState>,
    headers: HeaderMap,
    Path(id): Path<String>,
    Json(mut body): Json<Value>,
) -> Response {
    let claims = match authorize(&st, &headers, "comments.write", &Method::PATCH, "/api/v1/comments") {
        Ok(c) => c,
        Err(r) => return r,
    };
    // Path id is authoritative (docs/03 Rule 6) — overwrite any body id.
    if let Value::Object(map) = &mut body {
        map.insert("id".into(), json!(id));
    }
    dispatch(&st, &claims, "comments.update.execute", body).await
}

async fn comments_delete(
    State(st): State<KanbanRestState>,
    headers: HeaderMap,
    Path(id): Path<String>,
) -> Response {
    let claims = match authorize(&st, &headers, "comments.write", &Method::DELETE, "/api/v1/comments") {
        Ok(c) => c,
        Err(r) => return r,
    };
    dispatch(&st, &claims, "comments.delete.execute", json!({ "id": id })).await
}

// ─── report ──────────────────────────────────────────────────────────────────

async fn report_generate(
    State(st): State<KanbanRestState>,
    headers: HeaderMap,
    Json(body): Json<Value>,
) -> Response {
    let claims = match authorize(
        &st,
        &headers,
        "documents.write",
        &Method::POST,
        "/api/v1/report/generate",
    ) {
        Ok(c) => c,
        Err(r) => return r,
    };
    dispatch(&st, &claims, "report.generate", body).await
}

/// `GET /api/v1/analytics/summary?rig&workspace[&…]` — the dashboard KPIs
/// (hq-1cd840). Read-only; `issues.read` suffices.
async fn analytics_summary(
    State(st): State<KanbanRestState>,
    headers: HeaderMap,
    Query(q): Query<Value>,
) -> Response {
    let claims = match authorize(&st, &headers, "issues.read", &Method::GET, "/api/v1/analytics/summary") {
        Ok(c) => c,
        Err(r) => return r,
    };
    dispatch(&st, &claims, "analytics.summary", q).await
}

// ─── invites ─────────────────────────────────────────────────────────────────

async fn invites_list(State(st): State<KanbanRestState>, headers: HeaderMap) -> Response {
    let claims = match authorize(&st, &headers, "invites.read", &Method::GET, "/api/v1/invites") {
        Ok(c) => c,
        Err(r) => return r,
    };
    dispatch(&st, &claims, "invite.list", json!({})).await
}

async fn invites_create(
    State(st): State<KanbanRestState>,
    headers: HeaderMap,
    Json(body): Json<Value>,
) -> Response {
    let claims = match authorize(&st, &headers, "invites.write", &Method::POST, "/api/v1/invites") {
        Ok(c) => c,
        Err(r) => return r,
    };
    dispatch(&st, &claims, "invite.create", body).await
}

async fn invites_revoke(
    State(st): State<KanbanRestState>,
    headers: HeaderMap,
    Path(id): Path<String>,
) -> Response {
    let claims = match authorize(&st, &headers, "invites.write", &Method::DELETE, "/api/v1/invites") {
        Ok(c) => c,
        Err(r) => return r,
    };
    dispatch(&st, &claims, "invite.revoke", json!({ "id": id })).await
}

/// Accept needs only a valid session: the body token is the capability, and the
/// bind targets the invite's email — the handler rolls back a consume whose
/// gt-login identity does not exist, so a stranger gains nothing here.
async fn invites_accept(
    State(st): State<KanbanRestState>,
    headers: HeaderMap,
    Json(body): Json<Value>,
) -> Response {
    let claims = match authorize(&st, &headers, "", &Method::POST, "/api/v1/invites/accept") {
        Ok(c) => c,
        Err(r) => return r,
    };
    dispatch(&st, &claims, "invite.accept", body).await
}

fn bearer(headers: &HeaderMap) -> Option<String> {
    let raw = headers
        .get(axum::http::header::AUTHORIZATION)
        .and_then(|v| v.to_str().ok())?;
    let token = raw
        .strip_prefix("Bearer ")
        .or_else(|| raw.strip_prefix("bearer "))?
        .trim();
    (!token.is_empty()).then(|| token.to_string())
}

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
