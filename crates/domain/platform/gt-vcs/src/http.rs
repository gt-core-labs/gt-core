//! The `axum` REST adapter for the `connection.*` surface (hq-vcs-connections.1).
//!
//! Self-service CRUD over a workspace's VCS connections, the platform sibling of the rig REST
//! surface. It folds into `gt-vcs` behind the off-by-default `axum` feature (docs/03 Rule 4),
//! exactly as gt-rig / gt-auth gate theirs — never a sibling crate.
//!
//! ## Self-auth, per-workspace, never from the path
//!
//! A connection belongs to a tenant. The workspace comes from the
//! [`WorkspaceContext`](gt_workspace::WorkspaceContext) extractor (the JWT claim / sanctioned
//! header), NEVER from the URL or body (docs/03 Rule 6, docs/04 §15). Every read/write the
//! handlers issue is scoped to that workspace (its own rows plus the global ones), so a caller can
//! neither list nor address another tenant's connection. The store is a single GLOBAL
//! `public.vcs_connections` table carrying a `workspace_id` column, so one `Arc<dyn
//! VcsConnectionRepo>` serves every tenant (the per-request scoping is the `workspace` argument),
//! mirroring the OAuth provider store rather than the per-tenant rig-pool cache.
//!
//! ## What it does *not* do
//!
//! - **It does not authenticate or authorize.** The builder mounts this router under
//!   `/api/v1/connection` and wraps it with the capability-derived scope guard
//!   ([`guard_module_scopes`](gt_module::guard_module_scopes)); a handler runs only once the caller
//!   holds `connection.read` / `connection.write` for the request's verb-class.
//! - **It never returns a secret.** The sealed PAT is write-only: accepted on create/patch, sealed
//!   at rest, and OMITTED from every read projection ([`ConnectionView`]).

use std::sync::Arc;

use axum::extract::{Path, State};
use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use axum::routing::get;
use axum::{Json, Router};
use serde::{Deserialize, Serialize};

use gt_events::AppError;
use gt_workspace::WorkspaceContext;

use crate::repo::{
    ConnectionKind, ConnectionStatus, NewConnection, PatchConnection, VcsConnection,
    VcsConnectionRepo,
};

/// The REST state: a single connection store serving every tenant (per-request scoping is the
/// workspace argument). Cloning is cheap — it is an `Arc`.
#[derive(Clone)]
pub struct ConnectionApiState {
    connections: Arc<dyn VcsConnectionRepo>,
}

impl ConnectionApiState {
    /// Wrap a connection store.
    pub fn new(connections: Arc<dyn VcsConnectionRepo>) -> Self {
        ConnectionApiState { connections }
    }
}

/// Build the connection REST router (relative paths — the builder nests it under
/// `/api/v1/connection` and applies the `connection.read`/`connection.write` scope guard).
pub fn connection_router(state: ConnectionApiState) -> Router {
    Router::new()
        .route("/", get(list_connections).post(create_connection))
        .route(
            "/:id",
            get(get_connection)
                .patch(patch_connection)
                .delete(delete_connection),
        )
        .with_state(state)
}

/// A connection as returned by every read/echo — the projection that OMITS the sealed secret. The
/// secret is write-only; no read surface ever carries it, so a compromised token cannot exfiltrate
/// a stored PAT. `has_secret` reports WHETHER a PAT is stored (without revealing it), so the FE can
/// show "PAT configured" vs a GitHub App connection.
#[derive(Debug, Serialize)]
#[cfg_attr(feature = "axum", derive(utoipa::ToSchema))]
pub struct ConnectionView {
    /// The id / primary key.
    pub id: String,
    /// The owning workspace (`null` = global).
    pub workspace_id: Option<String>,
    /// The connection variant (`github_app` / `pat`).
    pub kind: String,
    /// The GitHub App installation id (`github_app` only).
    pub installation_id: Option<String>,
    /// The GitHub account/org login (`github_app` only).
    pub account_login: Option<String>,
    /// Whether a sealed PAT is stored (the secret itself is never returned).
    pub has_secret: bool,
    /// The lifecycle state (`active` / `disabled` / `revoked`).
    pub status: String,
    /// Creation time (epoch seconds).
    pub created_at: i64,
}

impl From<VcsConnection> for ConnectionView {
    fn from(c: VcsConnection) -> Self {
        // The sealed `secret_sealed` is intentionally DROPPED here — this projection is the only
        // shape any read/echo returns, so the PAT can never leak through the HTTP surface.
        ConnectionView {
            id: c.id,
            workspace_id: c.workspace_id,
            kind: c.kind.as_str().to_owned(),
            installation_id: c.installation_id,
            account_login: c.account_login,
            has_secret: c.secret_sealed.is_some(),
            status: c.status.as_str().to_owned(),
            created_at: c.created_at,
        }
    }
}

/// `POST /` body — register a connection for the caller's workspace.
///
/// `kind = github_app` requires an `installation_id` and stores NO secret; `kind = pat` requires a
/// `secret` (the Personal Access Token, sealed at rest) and no installation id. The `workspace_id`
/// is NOT taken from the body — it is the caller's resolved workspace (self-auth).
#[derive(Debug, Deserialize)]
#[cfg_attr(feature = "axum", derive(utoipa::ToSchema))]
pub struct CreateConnectionRequest {
    /// The stable id / primary key.
    pub id: String,
    /// The connection variant: `github_app` or `pat`.
    pub kind: String,
    /// The GitHub App installation id (`github_app`).
    #[serde(default)]
    pub installation_id: Option<String>,
    /// The GitHub account/org login (`github_app`).
    #[serde(default)]
    pub account_login: Option<String>,
    /// The Personal Access Token, cleartext on the wire; sealed at rest, never returned (`pat`).
    #[serde(default)]
    pub secret: Option<String>,
    /// The initial lifecycle state. Omitted ⇒ `active`.
    #[serde(default)]
    pub status: Option<String>,
}

/// `PATCH /:id` body — partial update. Every field is optional. `secret` is write-only: supply it
/// to ROTATE the PAT (sealed at rest), omit it to leave the stored one.
#[derive(Debug, Default, Deserialize)]
#[cfg_attr(feature = "axum", derive(utoipa::ToSchema))]
pub struct PatchConnectionRequest {
    /// New installation id, or absent to leave it.
    #[serde(default)]
    pub installation_id: Option<String>,
    /// New account login, or absent to leave it.
    #[serde(default)]
    pub account_login: Option<String>,
    /// New PAT to rotate to (sealed at rest, never returned), or absent to keep it.
    #[serde(default)]
    pub secret: Option<String>,
    /// New lifecycle state (`active` / `disabled` / `revoked`), or absent to leave it.
    #[serde(default)]
    pub status: Option<String>,
}

/// `GET /` — every connection visible to the caller's workspace (`connection.read`).
#[cfg_attr(feature = "axum", utoipa::path(
    get, path = "/",
    responses((status = 200, description = "Connections visible to the workspace (own + global)")),
))]
async fn list_connections(
    State(st): State<ConnectionApiState>,
    ctx: WorkspaceContext,
) -> Result<Json<Vec<ConnectionView>>, ApiError> {
    let conns = st
        .connections
        .list_for_workspace(ctx.workspace().as_str())
        .await?;
    Ok(Json(conns.into_iter().map(ConnectionView::from).collect()))
}

/// `GET /:id` — one connection visible to the workspace (`connection.read`); `404` otherwise.
#[cfg_attr(feature = "axum", utoipa::path(
    get, path = "/{id}",
    params(("id" = String, Path, description = "Connection id")),
    responses(
        (status = 200, description = "The connection"),
        (status = 404, description = "No such connection visible to the workspace"),
    ),
))]
async fn get_connection(
    State(st): State<ConnectionApiState>,
    ctx: WorkspaceContext,
    Path(id): Path<String>,
) -> Result<Json<ConnectionView>, ApiError> {
    match st
        .connections
        .get_for_workspace(ctx.workspace().as_str(), &id)
        .await?
    {
        Some(c) => Ok(Json(c.into())),
        None => Err(ApiError(AppError::NotFound(format!("connection {id}")))),
    }
}

/// `POST /` — register a connection for the caller's workspace (`connection.write`). `201` on
/// success; `422` on the kind/secret invariant (a `pat` without a secret, a `github_app` with one).
#[cfg_attr(feature = "axum", utoipa::path(
    post, path = "/",
    request_body = CreateConnectionRequest,
    responses(
        (status = 201, description = "Connection registered", body = ConnectionView),
        (status = 422, description = "Validation failed (kind/secret invariant or unknown enum)"),
    ),
))]
async fn create_connection(
    State(st): State<ConnectionApiState>,
    ctx: WorkspaceContext,
    Json(body): Json<CreateConnectionRequest>,
) -> Result<Response, ApiError> {
    let kind = ConnectionKind::parse(&body.kind)?;
    let status = match body.status.as_deref() {
        Some(s) => ConnectionStatus::parse(s)?,
        None => ConnectionStatus::Active,
    };
    // The connection is owned by the caller's workspace — taken from the auth context, never the
    // body (a request cannot register a connection for another tenant).
    let new = NewConnection {
        id: body.id,
        workspace_id: Some(ctx.workspace().as_str().to_owned()),
        kind,
        installation_id: body.installation_id,
        account_login: body.account_login,
        secret: body.secret,
        status,
    };
    let stored = st.connections.create(new).await?;
    Ok((StatusCode::CREATED, Json(ConnectionView::from(stored))).into_response())
}

/// `PATCH /:id` — partial update of a connection visible to the workspace (`connection.write`).
/// `404` when no such id is visible; `422` on the kind/secret invariant.
#[cfg_attr(feature = "axum", utoipa::path(
    patch, path = "/{id}",
    params(("id" = String, Path, description = "Connection id")),
    request_body = PatchConnectionRequest,
    responses(
        (status = 200, description = "The updated connection", body = ConnectionView),
        (status = 404, description = "No such connection visible to the workspace"),
        (status = 422, description = "Validation failed (kind/secret invariant or unknown enum)"),
    ),
))]
async fn patch_connection(
    State(st): State<ConnectionApiState>,
    ctx: WorkspaceContext,
    Path(id): Path<String>,
    Json(body): Json<PatchConnectionRequest>,
) -> Result<Json<ConnectionView>, ApiError> {
    let status = match body.status.as_deref() {
        Some(s) => Some(ConnectionStatus::parse(s)?),
        None => None,
    };
    let patch = PatchConnection {
        // workspace_id is NOT patchable from this self-auth surface — a member cannot re-home a
        // connection to another tenant.
        workspace_id: None,
        installation_id: body.installation_id.map(Some),
        account_login: body.account_login.map(Some),
        secret: body.secret.map(Some),
        status,
    };
    match st
        .connections
        .patch(ctx.workspace().as_str(), &id, patch)
        .await?
    {
        Some(c) => Ok(Json(c.into())),
        None => Err(ApiError(AppError::NotFound(format!("connection {id}")))),
    }
}

/// `DELETE /:id` — remove a connection visible to the workspace (`connection.write`). `204` whether
/// or not a row matched (idempotent delete), but never deletes another tenant's connection.
#[cfg_attr(feature = "axum", utoipa::path(
    delete, path = "/{id}",
    params(("id" = String, Path, description = "Connection id")),
    responses((status = 204, description = "Deleted (idempotent)")),
))]
async fn delete_connection(
    State(st): State<ConnectionApiState>,
    ctx: WorkspaceContext,
    Path(id): Path<String>,
) -> Result<Response, ApiError> {
    let _ = st
        .connections
        .delete(ctx.workspace().as_str(), &id)
        .await?;
    Ok(StatusCode::NO_CONTENT.into_response())
}

/// The OpenAPI document for the `connection.*` REST routes — relative paths, so nesting under
/// `/api/v1/connection` rewrites them. Merged into the fused `/openapi.json` by the composition root.
#[derive(utoipa::OpenApi)]
#[openapi(
    paths(
        list_connections,
        get_connection,
        create_connection,
        patch_connection,
        delete_connection,
    ),
    components(schemas(
        ConnectionView,
        CreateConnectionRequest,
        PatchConnectionRequest,
    ))
)]
pub struct ApiDoc;

/// Newtype so [`AppError`] maps to an HTTP status without an orphan-impl violation, mirroring the
/// rig REST adapter.
struct ApiError(AppError);

impl From<AppError> for ApiError {
    fn from(e: AppError) -> Self {
        ApiError(e)
    }
}

impl IntoResponse for ApiError {
    fn into_response(self) -> Response {
        let status = match &self.0 {
            AppError::NotFound(_) => StatusCode::NOT_FOUND,
            AppError::Validation(_) => StatusCode::UNPROCESSABLE_ENTITY,
            AppError::InvalidTransition(_) => StatusCode::CONFLICT,
            AppError::Handler(_) | AppError::Other(_) => StatusCode::INTERNAL_SERVER_ERROR,
        };
        (status, self.0.to_string()).into_response()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use async_trait::async_trait;
    use std::sync::Mutex;
    use tower::ServiceExt;
    use utoipa::OpenApi;

    #[test]
    fn openapi_lists_every_relative_route_prefix_free() {
        let doc = ApiDoc::openapi();
        let paths: Vec<&str> = doc.paths.paths.keys().map(String::as_str).collect();
        for expected in ["/", "/{id}"] {
            assert!(paths.contains(&expected), "missing route {expected}: {paths:?}");
        }
        // Paths are relative — nesting rewrites them under /api/v1/connection.
        assert!(paths.iter().all(|p| !p.starts_with("/api/v1")));
    }

    /// In-memory store for the router contract test — no Postgres needed.
    #[derive(Default)]
    struct MemConns {
        rows: Mutex<Vec<VcsConnection>>,
    }

    #[async_trait]
    impl VcsConnectionRepo for MemConns {
        async fn list_for_workspace(
            &self,
            workspace: &str,
        ) -> Result<Vec<VcsConnection>, AppError> {
            Ok(self
                .rows
                .lock()
                .unwrap()
                .iter()
                .filter(|c| c.workspace_id.is_none() || c.workspace_id.as_deref() == Some(workspace))
                .cloned()
                .collect())
        }
        async fn get_for_workspace(
            &self,
            workspace: &str,
            id: &str,
        ) -> Result<Option<VcsConnection>, AppError> {
            Ok(self
                .rows
                .lock()
                .unwrap()
                .iter()
                .find(|c| {
                    c.id == id
                        && (c.workspace_id.is_none()
                            || c.workspace_id.as_deref() == Some(workspace))
                })
                .cloned())
        }
        async fn create(&self, conn: NewConnection) -> Result<VcsConnection, AppError> {
            // Mirror the store's invariant so the 422 path is exercised without a DB.
            crate::repo::check_kind_secret_for_test(conn.kind, conn.secret.is_some())?;
            let rec = VcsConnection {
                id: conn.id,
                workspace_id: conn.workspace_id,
                kind: conn.kind,
                installation_id: conn.installation_id,
                account_login: conn.account_login,
                // Store a dummy non-empty blob to model "a secret is present" — the test asserts the
                // view never echoes the secret, only `has_secret`.
                secret_sealed: conn.secret.map(|s| s.into_bytes()),
                status: conn.status,
                created_at: 0,
            };
            self.rows.lock().unwrap().push(rec.clone());
            Ok(rec)
        }
        async fn patch(
            &self,
            _workspace: &str,
            _id: &str,
            _patch: PatchConnection,
        ) -> Result<Option<VcsConnection>, AppError> {
            Ok(None)
        }
        async fn delete(&self, _workspace: &str, _id: &str) -> Result<bool, AppError> {
            Ok(true)
        }
    }

    /// The router rejects a request with no resolvable workspace (the `WorkspaceContext` extractor
    /// fires before any handler), proving the tenant is sourced from the context, never the body.
    #[tokio::test]
    async fn missing_workspace_context_is_a_client_error() {
        let st = ConnectionApiState::new(Arc::new(MemConns::default()));
        let app = connection_router(st);
        let res = app
            .oneshot(
                axum::http::Request::builder()
                    .uri("/")
                    .body(axum::body::Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        // No X-GT-Workspace header / claim ⇒ the extractor rejects with 400 (Missing).
        assert_eq!(res.status(), StatusCode::BAD_REQUEST);
    }

    /// A create with a resolvable workspace returns 201 and a view that NEVER carries the secret.
    #[tokio::test]
    async fn create_returns_201_and_omits_the_secret() {
        let st = ConnectionApiState::new(Arc::new(MemConns::default()));
        let app = connection_router(st);
        let body = serde_json::json!({
            "id": "c1",
            "kind": "pat",
            "secret": "ghp_supersecret"
        });
        let res = app
            .oneshot(
                axum::http::Request::builder()
                    .method("POST")
                    .uri("/")
                    .header("X-GT-Workspace", "acme")
                    .header("content-type", "application/json")
                    .body(axum::body::Body::from(body.to_string()))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(res.status(), StatusCode::CREATED);
        let bytes = axum::body::to_bytes(res.into_body(), usize::MAX).await.unwrap();
        let text = String::from_utf8(bytes.to_vec()).unwrap();
        assert!(text.contains("\"has_secret\":true"), "view reports a secret is present: {text}");
        assert!(!text.contains("ghp_supersecret"), "view must never echo the secret: {text}");
    }

    /// A `pat` create with no secret is the 422 kind/secret invariant.
    #[tokio::test]
    async fn pat_without_secret_is_422() {
        let st = ConnectionApiState::new(Arc::new(MemConns::default()));
        let app = connection_router(st);
        let body = serde_json::json!({ "id": "c2", "kind": "pat" });
        let res = app
            .oneshot(
                axum::http::Request::builder()
                    .method("POST")
                    .uri("/")
                    .header("X-GT-Workspace", "acme")
                    .header("content-type", "application/json")
                    .body(axum::body::Body::from(body.to_string()))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(res.status(), StatusCode::UNPROCESSABLE_ENTITY);
    }
}
