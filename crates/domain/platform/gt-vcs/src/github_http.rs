//! The `axum` REST adapter for the GitHub App **install flow** + repo listing
//! (hq-vcs-connections.2). Behind the off-by-default `axum` feature, same gating as
//! [`crate::http`].
//!
//! Three routes, mounted by the composition root under `/api/v1/connection/github` behind the
//! `connection.read` / `connection.write` scope guard (the same module id `connection`, so they
//! share the existing scopes — no new scope to register):
//!
//! - `GET  /install`  (`connection.write`) — 302-redirect the admin to
//!   `https://github.com/apps/<slug>/installations/new` so they install the App + pick repos.
//! - `GET  /callback` (`connection.write`) — GitHub redirects back here with `installation_id` +
//!   `setup_action`; we capture `installation_id` + the org/account login (read AS THE APP) and
//!   PERSIST a `vcs_connections` row (`kind = github_app`). **No token is stored.**
//! - `GET  /repos`    (`connection.read`)  — given a `connection` id, mint an installation token
//!   JIT and return the repos the installation can reach. The token lives only in this request.
//!
//! The owning workspace is taken from the [`WorkspaceContext`](gt_workspace::WorkspaceContext)
//! extractor (self-auth), NEVER from the path or query.

use std::sync::Arc;

use axum::extract::{Query, State};
use axum::http::{HeaderMap, HeaderValue, StatusCode};
use axum::response::{IntoResponse, Response};
use axum::routing::get;
use axum::{Json, Router};
use serde::{Deserialize, Serialize};

use gt_events::AppError;
use gt_workspace::WorkspaceContext;

use crate::github::{GithubAppClient, InstallationRepo};
use crate::github_config::{GithubAppConfigInput, GithubAppConfigView, GithubAppSource};
use crate::repo::{ConnectionKind, ConnectionStatus, NewConnection, VcsConnectionRepo};

/// State for the GitHub install-flow routes: the DB-backed App config source (hq-61ea43) + the
/// connection store (to persist the `github_app` row on callback). Each request resolves the live
/// config from the source, so a UI change takes effect with no restart. Cheap to clone.
#[derive(Clone)]
pub struct GithubApiState {
    source: GithubAppSource,
    connections: Arc<dyn VcsConnectionRepo>,
}

impl GithubApiState {
    /// Wrap the App config `source` + the connection `store`.
    pub fn new(source: GithubAppSource, connections: Arc<dyn VcsConnectionRepo>) -> Self {
        GithubApiState {
            source,
            connections,
        }
    }

    /// Resolve the live App client from the config source, or `503` when no App is configured.
    async fn client(&self) -> Result<GithubAppClient, ApiError> {
        let cfg = self
            .source
            .load()
            .await
            .map_err(ApiError::App)?
            .ok_or(ApiError::NotConfigured)?;
        Ok(GithubAppClient::new(cfg))
    }
}

/// Build the GitHub install-flow router (relative paths — the builder nests it under
/// `/api/v1/connection/github` and applies the `connection.*` scope guard).
pub fn github_router(state: GithubApiState) -> Router {
    Router::new()
        .route("/install", get(install_redirect))
        .route("/callback", get(install_callback))
        .route("/repos", get(list_repos))
        // hq-61ea43: admin read/write of the DB-backed App config (secret-free read; write-only
        // secrets). Shares the `connection.*` scope guard the module mounts this router behind.
        .route("/config", get(get_config).put(put_config))
        .with_state(state)
}

/// `GET /install` — 302 to the App's `installations/new` URL so the admin installs + picks repos.
#[cfg_attr(feature = "axum", utoipa::path(
    get, path = "/install",
    responses((status = 302, description = "Redirect to the GitHub App install URL")),
))]
async fn install_redirect(
    State(st): State<GithubApiState>,
    _ctx: WorkspaceContext,
) -> Result<Response, ApiError> {
    let url = st.client().await?.config().install_url();
    let mut headers = HeaderMap::new();
    headers.insert(
        axum::http::header::LOCATION,
        HeaderValue::from_str(&url)
            .map_err(|e| ApiError::App(AppError::Other(format!("bad install url: {e}"))))?,
    );
    Ok((StatusCode::FOUND, headers).into_response())
}

/// Query params GitHub appends to the post-install redirect: the new `installation_id` and the
/// `setup_action` (`install` / `update`).
#[derive(Debug, Deserialize)]
pub struct CallbackParams {
    /// The installation id GitHub just created (or updated).
    pub installation_id: String,
    /// `install` for a fresh install, `update` for a repo-selection change. Optional.
    #[serde(default)]
    pub setup_action: Option<String>,
}

/// The callback response — the persisted `github_app` connection (no secret, by construction).
#[derive(Debug, Serialize)]
#[cfg_attr(feature = "axum", derive(utoipa::ToSchema))]
pub struct InstalledConnection {
    /// The connection id we minted (`gh-<installation_id>`).
    pub id: String,
    /// The installation id captured from GitHub.
    pub installation_id: String,
    /// The org/account login the installation lives under (read AS THE APP).
    pub account_login: Option<String>,
}

/// `GET /callback` — GitHub redirects here after the admin installs the App. We capture the
/// `installation_id`, resolve the org/account login by minting an App-level call, and PERSIST a
/// `vcs_connections` row (`kind = github_app`). **No token is stored** — only the `installation_id`.
#[cfg_attr(feature = "axum", utoipa::path(
    get, path = "/callback",
    params(
        ("installation_id" = String, Query, description = "The installation id GitHub created"),
        ("setup_action" = Option<String>, Query, description = "install | update"),
    ),
    responses(
        (status = 200, description = "Connection persisted (kind=github_app, no token stored)"),
    ),
))]
async fn install_callback(
    State(st): State<GithubApiState>,
    ctx: WorkspaceContext,
    Query(params): Query<CallbackParams>,
) -> Result<Json<InstalledConnection>, ApiError> {
    let workspace = ctx.workspace().as_str().to_owned();
    let client = st.client().await?;
    // Resolve the org/account login the installation lives under by minting an installation token
    // and reading the first repo's owner. The token is used ONLY here and dropped — never stored.
    let account_login = match client.installation_token(&params.installation_id).await {
        Ok(token) => client
            .list_installation_repos(&token)
            .await
            .ok()
            .and_then(|repos| repos.first().map(|r| owner_of(&r.full_name))),
        // A token mint failure (e.g. GitHub transiently unreachable) does not block recording the
        // installation: the login is best-effort metadata, not a credential. The connection is
        // still usable (we mint fresh tokens per clone).
        Err(_) => None,
    };

    let id = format!("gh-{}", params.installation_id);
    let stored = st
        .connections
        .create(NewConnection {
            id: id.clone(),
            workspace_id: Some(workspace),
            kind: ConnectionKind::GithubApp,
            installation_id: Some(params.installation_id.clone()),
            account_login: account_login.clone(),
            // github_app stores NO secret — tokens are minted JIT.
            secret: None,
            status: ConnectionStatus::Active,
        })
        .await?;

    Ok(Json(InstalledConnection {
        id: stored.id,
        installation_id: params.installation_id,
        account_login,
    }))
}

/// Query params for `GET /repos` — the connection whose installation to list.
#[derive(Debug, Deserialize)]
pub struct ReposParams {
    /// The `vcs_connections` id (a `github_app` connection visible to the workspace).
    pub connection: String,
}

/// `GET /repos?connection=<id>` — mint an installation token JIT for the named connection and
/// return the repos the installation can reach. The minted token lives only for this request
/// (never persisted).
#[cfg_attr(feature = "axum", utoipa::path(
    get, path = "/repos",
    params(("connection" = String, Query, description = "The github_app connection id")),
    responses(
        (status = 200, description = "Repos the installation can reach"),
        (status = 404, description = "No such connection visible to the workspace"),
        (status = 422, description = "The connection is not a github_app or has no installation_id"),
    ),
))]
async fn list_repos(
    State(st): State<GithubApiState>,
    ctx: WorkspaceContext,
    Query(params): Query<ReposParams>,
) -> Result<Json<Vec<InstallationRepo>>, ApiError> {
    let conn = st
        .connections
        .get_for_workspace(ctx.workspace().as_str(), &params.connection)
        .await?
        .ok_or_else(|| {
            ApiError::App(AppError::NotFound(format!(
                "connection {}",
                params.connection
            )))
        })?;
    if conn.kind != ConnectionKind::GithubApp {
        return Err(ApiError::App(AppError::Validation(
            "connection is not a github_app — repo listing needs an App installation".into(),
        )));
    }
    let installation_id = conn.installation_id.ok_or_else(|| {
        ApiError::App(AppError::Validation(
            "github_app connection has no installation_id".into(),
        ))
    })?;
    let client = st.client().await?;
    let token = client.installation_token(&installation_id).await?;
    let repos = client.list_installation_repos(&token).await?;
    // `token` drops here — never persisted, never logged.
    Ok(Json(repos))
}

/// `GET /config` — the secret-free view of the stored App config (hq-61ea43), or `404` when unset.
#[cfg_attr(feature = "axum", utoipa::path(
    get, path = "/config",
    responses(
        (status = 200, description = "The configured App (secret-free)", body = GithubAppConfigView),
        (status = 404, description = "No GitHub App configured"),
    ),
))]
async fn get_config(
    State(st): State<GithubApiState>,
    _ctx: WorkspaceContext,
) -> Result<Json<GithubAppConfigView>, ApiError> {
    st.source
        .view()
        .await
        .map_err(ApiError::App)?
        .map(Json)
        .ok_or(ApiError::App(AppError::NotFound(
            "no GitHub App configured".into(),
        )))
}

/// `PUT /config` — upsert the App config (hq-61ea43). Secrets are write-only (omit to keep). The
/// private key is required on first configure.
#[cfg_attr(feature = "axum", utoipa::path(
    put, path = "/config",
    request_body = GithubAppConfigInput,
    responses(
        (status = 204, description = "Config saved"),
        (status = 422, description = "Missing private key on first configure"),
    ),
))]
async fn put_config(
    State(st): State<GithubApiState>,
    _ctx: WorkspaceContext,
    Json(input): Json<GithubAppConfigInput>,
) -> Result<StatusCode, ApiError> {
    st.source.upsert(input).await.map_err(ApiError::App)?;
    Ok(StatusCode::NO_CONTENT)
}

/// `owner` from an `owner/name` full name (`acme/repo` -> `acme`).
fn owner_of(full_name: &str) -> String {
    full_name
        .split_once('/')
        .map(|(owner, _)| owner.to_owned())
        .unwrap_or_else(|| full_name.to_owned())
}

/// The OpenAPI document for the GitHub install-flow routes — relative paths, nested under
/// `/api/v1/connection/github`.
#[derive(utoipa::OpenApi)]
#[openapi(
    paths(install_redirect, install_callback, list_repos, get_config, put_config),
    components(schemas(
        InstalledConnection,
        InstallationRepo,
        GithubAppConfigView,
        GithubAppConfigInput
    ))
)]
pub struct GithubApiDoc;

/// Maps an [`AppError`] (or a not-configured App) to an HTTP status, mirroring [`crate::http::ApiError`].
enum ApiError {
    /// A wrapped [`AppError`] (the usual store/validation faults).
    App(AppError),
    /// No GitHub App is configured (hq-61ea43) — the install/callback/repos surface answers `503`
    /// so a caller (or the FE) can tell "set up the App" apart from a real failure.
    NotConfigured,
}

impl From<AppError> for ApiError {
    fn from(e: AppError) -> Self {
        ApiError::App(e)
    }
}

impl IntoResponse for ApiError {
    fn into_response(self) -> Response {
        match self {
            ApiError::NotConfigured => (
                StatusCode::SERVICE_UNAVAILABLE,
                "no GitHub App configured — set it under connection/github/config",
            )
                .into_response(),
            ApiError::App(e) => {
                let status = match &e {
                    AppError::NotFound(_) => StatusCode::NOT_FOUND,
                    AppError::Validation(_) => StatusCode::UNPROCESSABLE_ENTITY,
                    AppError::InvalidTransition(_) => StatusCode::CONFLICT,
                    AppError::Handler(_) | AppError::Other(_) => {
                        StatusCode::INTERNAL_SERVER_ERROR
                    }
                };
                (status, e.to_string()).into_response()
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::github::GithubAppConfig;
    use crate::github_config::GithubAppSource;
    use crate::repo::{PatchConnection, VcsConnection};
    use async_trait::async_trait;
    use std::sync::Mutex;
    use tower::ServiceExt;
    use utoipa::OpenApi;

    const TEST_PRIV_PEM: &[u8] = include_bytes!("../tests/data/test_app_key.pem");

    /// A fixed-config source standing in for the DB-backed one in the router tests.
    fn test_source() -> GithubAppSource {
        GithubAppSource::Fixed(Some(GithubAppConfig::new(
            "123456",
            TEST_PRIV_PEM.to_vec(),
            "gt-test-app",
            None,
        )))
    }

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
                .filter(|c| {
                    c.workspace_id.is_none() || c.workspace_id.as_deref() == Some(workspace)
                })
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
            let rec = VcsConnection {
                id: conn.id,
                workspace_id: conn.workspace_id,
                kind: conn.kind,
                installation_id: conn.installation_id,
                account_login: conn.account_login,
                secret_sealed: None,
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
        async fn find_by_installation(
            &self,
            _installation_id: &str,
        ) -> Result<Option<VcsConnection>, AppError> {
            Ok(None)
        }
    }

    #[test]
    fn owner_of_splits_full_name() {
        assert_eq!(owner_of("acme/inactivas-chain"), "acme");
        assert_eq!(owner_of("solo"), "solo");
    }

    #[test]
    fn openapi_lists_relative_routes() {
        let doc = GithubApiDoc::openapi();
        let paths: Vec<&str> = doc.paths.paths.keys().map(String::as_str).collect();
        for expected in ["/install", "/callback", "/repos"] {
            assert!(paths.contains(&expected), "missing {expected}: {paths:?}");
        }
        assert!(paths.iter().all(|p| !p.starts_with("/api/v1")));
    }

    /// `GET /install` 302s to the App's installations/new URL.
    #[tokio::test]
    async fn install_redirects_to_github() {
        let st = GithubApiState::new(test_source(), Arc::new(MemConns::default()));
        let app = github_router(st);
        let res = app
            .oneshot(
                axum::http::Request::builder()
                    .uri("/install")
                    .header("X-GT-Workspace", "acme")
                    .body(axum::body::Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(res.status(), StatusCode::FOUND);
        let loc = res.headers().get(axum::http::header::LOCATION).unwrap();
        assert_eq!(
            loc.to_str().unwrap(),
            "https://github.com/apps/gt-test-app/installations/new"
        );
    }

    /// The install routes require a resolvable workspace (self-auth, not the query).
    #[tokio::test]
    async fn install_requires_workspace_context() {
        let st = GithubApiState::new(test_source(), Arc::new(MemConns::default()));
        let app = github_router(st);
        let res = app
            .oneshot(
                axum::http::Request::builder()
                    .uri("/install")
                    .body(axum::body::Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(res.status(), StatusCode::BAD_REQUEST);
    }

    /// The callback persists a `github_app` connection with the installation id and NO secret. (The
    /// account-login lookup hits GitHub and fails offline, which is tolerated — the row is still
    /// recorded.)
    #[tokio::test]
    async fn callback_persists_github_app_connection_without_token() {
        let store = Arc::new(MemConns::default());
        let st = GithubApiState::new(test_source(), store.clone());
        let app = github_router(st);
        let res = app
            .oneshot(
                axum::http::Request::builder()
                    .uri("/callback?installation_id=987654&setup_action=install")
                    .header("X-GT-Workspace", "acme")
                    .body(axum::body::Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(res.status(), StatusCode::OK);
        let rows = store.rows.lock().unwrap();
        let row = rows
            .iter()
            .find(|c| c.id == "gh-987654")
            .expect("persisted");
        assert_eq!(row.kind, ConnectionKind::GithubApp);
        assert_eq!(row.installation_id.as_deref(), Some("987654"));
        assert!(row.secret_sealed.is_none(), "github_app stores no token");
        assert_eq!(row.workspace_id.as_deref(), Some("acme"));
    }

    /// `GET /repos` for an unknown connection is a 404.
    #[tokio::test]
    async fn repos_unknown_connection_is_404() {
        let st = GithubApiState::new(test_source(), Arc::new(MemConns::default()));
        let app = github_router(st);
        let res = app
            .oneshot(
                axum::http::Request::builder()
                    .uri("/repos?connection=nope")
                    .header("X-GT-Workspace", "acme")
                    .body(axum::body::Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(res.status(), StatusCode::NOT_FOUND);
    }
}
