//! Notifications REST surface.
//!
//! The `mayor` role (or any caller with `notifications.write`) sends a notification to the
//! human operator via `POST /api/v1/notifications`. The web UI polls `GET
//! /api/v1/notifications?unread=true` or listens on the SSE feed for `notification.created.v1`
//! events and renders them in a bell-icon panel.
//!
//! ## Security
//!
//! Same verifier as hooks + terminal (RS256 cookie or bearer). GET = `notifications.read`, POST
//! / mark-read / delete = `notifications.write`. The seeded admin's `*` wildcard satisfies both.
//!
//! ## Schema
//!
//! `notifications` lives in the PUBLIC schema (not per-workspace), keyed by a `workspace` column
//! like `mcp_audit`. The migration is applied by `apply_pg_catalog` in the gt-mcp-server bin.

use std::sync::Arc;

use axum::{
    extract::{Path, Query, State},
    http::{HeaderMap, Method, StatusCode},
    response::{IntoResponse, Response},
    routing::{delete, get, post},
    Json, Router,
};
use serde::{Deserialize, Serialize};
use serde_json::json;
use sqlx::types::chrono::{DateTime, Utc};
use sqlx::PgPool;
use time::OffsetDateTime;

use gt_auth::JwtClaims;
use gt_events::EventKind;

use crate::auth::SharedAuthenticator;
use crate::denial_audit::{record_denial, SharedAudit, ANONYMOUS};
use crate::mcp::EventLog;

const TOKEN_COOKIE: &str = "gt_web_token";
const READ_SCOPE: &str = "notifications.read";
const WRITE_SCOPE: &str = "notifications.write";
const SCOPE_WILDCARD: &str = "*";
const NOTIFICATIONS_PATH: &str = "/api/v1/notifications";
const DEFAULT_WORKSPACE: &str = "default";

// ─── state ──────────────────────────────────────────────────────────────────

#[derive(Clone)]
pub struct NotificationsApiState {
    authenticator: SharedAuthenticator,
    audit: SharedAudit,
    pool: PgPool,
    event_log: Arc<EventLog>,
}

impl NotificationsApiState {
    pub fn new(
        authenticator: SharedAuthenticator,
        audit: SharedAudit,
        pool: PgPool,
        event_log: Arc<EventLog>,
    ) -> Self {
        Self { authenticator, audit, pool, event_log }
    }
}

// ─── router ─────────────────────────────────────────────────────────────────

pub fn notifications_router(state: NotificationsApiState) -> Router {
    Router::new()
        .route(NOTIFICATIONS_PATH, get(list_notifications).post(create_notification))
        .route(&format!("{NOTIFICATIONS_PATH}/:id/read"), post(mark_read))
        .route(&format!("{NOTIFICATIONS_PATH}/read-all"), post(mark_all_read))
        .route(&format!("{NOTIFICATIONS_PATH}/:id"), delete(delete_notification))
        .with_state(state)
}

// ─── models ─────────────────────────────────────────────────────────────────

#[derive(Debug, Serialize, sqlx::FromRow)]
pub struct Notification {
    pub id: String,
    pub workspace: String,
    pub from_role: String,
    pub title: String,
    pub body: String,
    pub kind: String,
    pub created_at: DateTime<Utc>,
    pub read_at: Option<DateTime<Utc>>,
}

#[derive(Debug, Deserialize)]
pub struct CreateNotificationBody {
    pub title: String,
    #[serde(default)]
    pub body: String,
    #[serde(default = "default_kind")]
    pub kind: String,
    #[serde(default = "default_from_role")]
    pub from_role: String,
}

fn default_kind() -> String {
    "decision".to_string()
}
fn default_from_role() -> String {
    "mayor".to_string()
}

#[derive(Debug, Deserialize, Default)]
pub struct ListParams {
    #[serde(default)]
    pub unread: Option<bool>,
}

// ─── SSE event ──────────────────────────────────────────────────────────────

/// Event emitted to the workspace event log when a notification is created.
/// Clients subscribed to `?channel=notifications` on `GET /stream` receive this.
#[derive(Serialize)]
struct NotificationCreatedEvent {
    id: String,
    workspace: String,
    from_role: String,
    title: String,
    body: String,
    kind: String,
}

impl EventKind for NotificationCreatedEvent {
    fn kind(&self) -> &'static str {
        "notification.created.v1"
    }
}

// ─── handlers ───────────────────────────────────────────────────────────────

/// `GET /api/v1/notifications[?unread=true]` — list notifications for the caller's workspace.
async fn list_notifications(
    State(st): State<NotificationsApiState>,
    headers: HeaderMap,
    Query(params): Query<ListParams>,
) -> Response {
    let claims = match authorize(&st, &headers, READ_SCOPE, &Method::GET) {
        Ok(c) => c,
        Err(r) => return r,
    };
    let workspace = claims.workspace;

    let rows: Result<Vec<Notification>, _> = if params.unread == Some(true) {
        sqlx::query_as(
            "SELECT id::text AS id, workspace, from_role, title, body, kind, created_at, read_at \
             FROM notifications WHERE workspace = $1 AND read_at IS NULL \
             ORDER BY created_at DESC",
        )
        .bind(&workspace)
        .fetch_all(&st.pool)
        .await
    } else {
        sqlx::query_as(
            "SELECT id::text AS id, workspace, from_role, title, body, kind, created_at, read_at \
             FROM notifications WHERE workspace = $1 \
             ORDER BY created_at DESC",
        )
        .bind(&workspace)
        .fetch_all(&st.pool)
        .await
    };

    match rows {
        Ok(items) => (StatusCode::OK, Json(json!({ "count": items.len(), "notifications": items }))).into_response(),
        Err(e) => (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()).into_response(),
    }
}

/// `POST /api/v1/notifications` — create a notification and emit an SSE event.
async fn create_notification(
    State(st): State<NotificationsApiState>,
    headers: HeaderMap,
    Json(body): Json<CreateNotificationBody>,
) -> Response {
    let claims = match authorize(&st, &headers, WRITE_SCOPE, &Method::POST) {
        Ok(c) => c,
        Err(r) => return r,
    };
    let workspace = claims.workspace;

    // Validate kind
    if !["decision", "info", "alert"].contains(&body.kind.as_str()) {
        return (StatusCode::UNPROCESSABLE_ENTITY, "kind must be one of: decision, info, alert").into_response();
    }
    if body.title.trim().is_empty() {
        return (StatusCode::UNPROCESSABLE_ENTITY, "title must not be empty").into_response();
    }

    let row: Result<(String,), _> = sqlx::query_as(
        "INSERT INTO notifications (workspace, from_role, title, body, kind) \
         VALUES ($1, $2, $3, $4, $5) RETURNING id::text",
    )
    .bind(&workspace)
    .bind(&body.from_role)
    .bind(&body.title)
    .bind(&body.body)
    .bind(&body.kind)
    .fetch_one(&st.pool)
    .await;

    match row {
        Ok((id,)) => {
            // Emit SSE event to the workspace log so `/stream?channel=notifications` picks it up.
            let ev = NotificationCreatedEvent {
                id: id.clone(),
                workspace: workspace.clone(),
                from_role: body.from_role.clone(),
                title: body.title.clone(),
                body: body.body.clone(),
                kind: body.kind.clone(),
            };
            let ws_opt = if workspace == DEFAULT_WORKSPACE { None } else { Some(workspace.as_str()) };
            if let Err(e) = st.event_log.append(ws_opt, ev) {
                eprintln!("[notifications] SSE emit failed: {e}");
            }
            (StatusCode::CREATED, Json(json!({ "id": id }))).into_response()
        }
        Err(e) => (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()).into_response(),
    }
}

/// `POST /api/v1/notifications/:id/read` — mark one notification as read.
async fn mark_read(
    State(st): State<NotificationsApiState>,
    headers: HeaderMap,
    Path(id): Path<String>,
) -> Response {
    let claims = match authorize(&st, &headers, WRITE_SCOPE, &Method::POST) {
        Ok(c) => c,
        Err(r) => return r,
    };
    let workspace = claims.workspace;
    let result = sqlx::query(
        "UPDATE notifications SET read_at = now() \
         WHERE id = $1::uuid AND workspace = $2 AND read_at IS NULL",
    )
    .bind(&id)
    .bind(&workspace)
    .execute(&st.pool)
    .await;

    match result {
        Ok(r) if r.rows_affected() == 0 => {
            (StatusCode::NOT_FOUND, "not found or already read").into_response()
        }
        Ok(_) => (StatusCode::OK, Json(json!({ "read": id }))).into_response(),
        Err(e) => (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()).into_response(),
    }
}

/// `POST /api/v1/notifications/read-all` — mark all unread notifications for the workspace as read.
async fn mark_all_read(
    State(st): State<NotificationsApiState>,
    headers: HeaderMap,
) -> Response {
    let claims = match authorize(&st, &headers, WRITE_SCOPE, &Method::POST) {
        Ok(c) => c,
        Err(r) => return r,
    };
    let workspace = claims.workspace;
    let result = sqlx::query(
        "UPDATE notifications SET read_at = now() WHERE workspace = $1 AND read_at IS NULL",
    )
    .bind(&workspace)
    .execute(&st.pool)
    .await;

    match result {
        Ok(r) => (StatusCode::OK, Json(json!({ "marked_read": r.rows_affected() }))).into_response(),
        Err(e) => (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()).into_response(),
    }
}

/// `DELETE /api/v1/notifications/:id` — delete a notification.
async fn delete_notification(
    State(st): State<NotificationsApiState>,
    headers: HeaderMap,
    Path(id): Path<String>,
) -> Response {
    let claims = match authorize(&st, &headers, WRITE_SCOPE, &Method::DELETE) {
        Ok(c) => c,
        Err(r) => return r,
    };
    let workspace = claims.workspace;
    let result = sqlx::query(
        "DELETE FROM notifications WHERE id = $1::uuid AND workspace = $2",
    )
    .bind(&id)
    .bind(&workspace)
    .execute(&st.pool)
    .await;

    match result {
        Ok(r) if r.rows_affected() == 0 => (StatusCode::NOT_FOUND, "not found").into_response(),
        Ok(_) => (StatusCode::OK, Json(json!({ "deleted": id }))).into_response(),
        Err(e) => (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()).into_response(),
    }
}

// ─── auth helpers ────────────────────────────────────────────────────────────

fn authorize(
    st: &NotificationsApiState,
    headers: &HeaderMap,
    scope: &'static str,
    method: &Method,
) -> Result<JwtClaims, Response> {
    let reject = |status: StatusCode, msg: &'static str| -> Response {
        record_denial(
            st.audit.as_ref(),
            ANONYMOUS,
            None,
            method,
            &NOTIFICATIONS_PATH.parse().expect("static uri"),
            Some(scope),
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
    if !has_scope(&claims.scopes, scope) {
        return Err(reject(StatusCode::FORBIDDEN, "missing notifications scope"));
    }
    Ok(claims)
}

fn has_scope(scopes: &[String], required: &str) -> bool {
    scopes.iter().any(|s| s == SCOPE_WILDCARD || s == required)
}

fn bearer(headers: &HeaderMap) -> Option<String> {
    let raw = headers
        .get(axum::http::header::AUTHORIZATION)
        .and_then(|v| v.to_str().ok())?;
    let token = raw
        .strip_prefix("Bearer ")
        .or_else(|| raw.strip_prefix("bearer "))?;
    let token = token.trim();
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
