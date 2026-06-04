//! The `axum` REST adapter for the `rig.*` surface (`hq-fe-api-platform.2`).
//!
//! The platform sibling of the issues REST surface (`hq-auth-routes.2`): it exposes the rig
//! catalog as REST routes that dispatch to the **same** [`RigCommand`](crate::RigCommand)
//! decide/apply layer the MCP `rig.*` tools use — no domain logic is duplicated, only the wire
//! shape differs. It folds into `gt-rig` behind the off-by-default `axum` feature
//! (docs/03 Rule 4), exactly as gt-issues/gt-workspace/gt-auth gate theirs, rather than living
//! in a sibling crate.
//!
//! ## Per-workspace, never from the path
//!
//! A rig catalog is **per-tenant**: each workspace owns its own `rigs` table in its
//! `ws_<slug>` schema (docs/04 §15). Unlike the single-tenant issues tracker — which bakes one
//! store into its REST state — a rig request must resolve the caller's tenant *first* and run
//! against that tenant's catalog. The tenant comes from the
//! [`WorkspaceContext`](gt_workspace::WorkspaceContext) extractor (the JWT claim / sanctioned
//! header), **never** from the URL or body (docs/03 Rule 6, docs/04 §15). The adapter holds a
//! [`WorkspaceRigs`] provider that hands back a workspace-scoped repository per request — the
//! REST mirror of the MCP `RigHandler`'s per-workspace pool cache.
//!
//! ## What it does *not* do
//!
//! - **It does not authenticate or authorize.** The builder mounts this router under
//!   `/api/v1/rig` and wraps it with the capability-derived scope guard
//!   ([`guard_module_scopes`](gt_module::guard_module_scopes)); the composition root layers the
//!   auth middleware in front. A handler here only runs once the caller already holds
//!   `rig.read` / `rig.write` for the request's verb-class.
//! - **It performs no filesystem I/O.** As with the MCP path, the on-disk clone / bd-config /
//!   worktree move are deploy-edge side-effects; this adapter only mutates orchestrator state.

use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use async_trait::async_trait;
use axum::extract::{Path, Query, State};
use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use axum::routing::{get, post};
use axum::{Json, Router};
use serde::de::DeserializeOwned;
use serde::Deserialize;
use serde_json::{json, Value};

use gt_events::{AppError, Command};
use gt_workspace::WorkspaceContext;

use crate::commands::{AddRig, AdoptRig, RemoveRig, SetRigDefaultBranch, SetRigPrefix, SetRigWorktreeRoot};
use crate::repo::RigRepository;
use crate::state::{RigCatalog, RigEntry};

/// A per-workspace rig repository provider for the REST adapter.
///
/// A rig catalog is tenant-local, so a request must run against the caller's own `rigs` table.
/// The binary supplies an implementation that, given the resolved workspace slug, hands back a
/// repository scoped to that tenant's schema (the PG path wraps `WorkspacePool` →
/// [`PgRigs`](crate::PgRigs)); a test supplies an in-memory one. This is the REST mirror of the
/// MCP `RigHandler`'s `WsPools` cache — the composition root owns the connection policy, the
/// adapter only asks for "the repo for *this* workspace".
#[async_trait]
pub trait WorkspaceRigs: Send + Sync {
    /// The rig repository scoped to `workspace` (the slug from the auth context, never the path).
    async fn repo(&self, workspace: &str) -> Result<Box<dyn DynRigRepository>, AppError>;
}

/// Object-safe mirror of the [`RigRepository`] port for the REST adapter.
///
/// [`RigRepository`] is an RPITIT trait (returns `impl Future`), so it cannot be made into a
/// `dyn` object — but [`WorkspaceRigs`] must return one (the concrete repo type is the binary's
/// choice, hidden behind the provider). This trait restates the same five operations with boxed
/// futures (`async_trait`) so a `Box<dyn DynRigRepository>` is returnable, and the blanket impl
/// below adapts every [`RigRepository`] to it for free.
#[async_trait]
pub trait DynRigRepository: Send + Sync {
    /// Upsert a rig entry (idempotent on name).
    async fn upsert(&self, entry: &RigEntry) -> Result<(), AppError>;
    /// Remove a rig by name; `Ok(false)` if it was absent.
    async fn remove(&self, name: &str) -> Result<bool, AppError>;
    /// Fetch one entry by name.
    async fn get(&self, name: &str) -> Result<Option<RigEntry>, AppError>;
    /// The rig owning `prefix`, if any.
    async fn prefix_owner(&self, prefix: &str) -> Result<Option<String>, AppError>;
    /// All entries, for catalog hydration.
    async fn list(&self) -> Result<Vec<RigEntry>, AppError>;
}

/// Every [`RigRepository`] is a [`DynRigRepository`] — the boxed-future adapter, so a provider
/// can return `Box::new(PgRigs::new(..))` (or any other adapter) without restating the methods.
#[async_trait]
impl<R: RigRepository> DynRigRepository for R {
    async fn upsert(&self, entry: &RigEntry) -> Result<(), AppError> {
        RigRepository::upsert(self, entry).await
    }
    async fn remove(&self, name: &str) -> Result<bool, AppError> {
        RigRepository::remove(self, name).await
    }
    async fn get(&self, name: &str) -> Result<Option<RigEntry>, AppError> {
        RigRepository::get(self, name).await
    }
    async fn prefix_owner(&self, prefix: &str) -> Result<Option<String>, AppError> {
        RigRepository::prefix_owner(self, prefix).await
    }
    async fn list(&self) -> Result<Vec<RigEntry>, AppError> {
        RigRepository::list(self).await
    }
}

/// Everything the rig REST handlers need, baked into the router with [`Router::with_state`]
/// before it leaves [`rig_router`] so the merged application router carries no outstanding
/// state type (the kernel's state-erased `Router<()>` contract).
#[derive(Clone)]
pub struct RigApiState {
    /// The per-workspace repository provider the binary supplies — the module owns no store of
    /// its own, exactly as the MCP `RigHandler` owns only its pool cache.
    rigs: Arc<dyn WorkspaceRigs>,
}

impl RigApiState {
    /// Build the REST state over a per-workspace repository provider.
    pub fn new(rigs: Arc<dyn WorkspaceRigs>) -> Self {
        Self { rigs }
    }
}

/// Build the rig REST router with `state` baked in (`hq-fe-api-platform.2`).
///
/// The paths are **relative**: the builder nests them under `/api/v1/rig` and applies the scope
/// guard. `register_routes` on [`RigsModule`](crate::RigsModule) returns exactly this router
/// when the module carries HTTP state.
///
/// | Method + path                  | Maps to MCP tool         |
/// |--------------------------------|--------------------------|
/// | `GET /`                        | `rig.list`               |
/// | `GET /lookup?prefix=`          | `rig.lookup-by-prefix`   |
/// | `GET /:name`                   | `rig.info`               |
/// | `POST /`                       | `rig.add`                |
/// | `POST /adopt`                  | `rig.adopt`              |
/// | `DELETE /:name`                | `rig.remove`             |
/// | `POST /:name/set-prefix`       | `rig.set-prefix`         |
/// | `POST /:name/set-default-branch` | `rig.set-default-branch` |
/// | `POST /:name/set-worktree-root`  | `rig.set-worktree-root`  |
pub fn rig_router(state: RigApiState) -> Router {
    Router::new()
        .route("/", get(list_rigs).post(add_rig))
        .route("/adopt", post(adopt_rig))
        .route("/lookup", get(lookup_by_prefix))
        .route("/:name", get(get_rig).delete(remove_rig))
        .route("/:name/set-prefix", post(set_prefix))
        .route("/:name/set-default-branch", post(set_default_branch))
        .route("/:name/set-worktree-root", post(set_worktree_root))
        .with_state(state)
}

/// `GET /` — every rig in the caller's catalog (`rig.list`).
#[cfg_attr(feature = "axum", utoipa::path(
    get, path = "/",
    responses((status = 200, description = "Every rig in the workspace catalog")),
))]
async fn list_rigs(
    State(st): State<RigApiState>,
    ctx: WorkspaceContext,
) -> Result<Json<Value>, ApiError> {
    let repo = st.rigs.repo(ctx.workspace().as_str()).await?;
    let rigs = repo.list().await?;
    Ok(Json(json!({ "rigs": rigs.iter().map(entry_json).collect::<Vec<_>>() })))
}

/// `GET /:name` — one rig's catalog entry (`rig.info`); `404` when no rig matches.
#[cfg_attr(feature = "axum", utoipa::path(
    get, path = "/{name}",
    params(("name" = String, Path, description = "Rig name")),
    responses(
        (status = 200, description = "The rig's catalog entry"),
        (status = 404, description = "No rig with that name"),
    ),
))]
async fn get_rig(
    State(st): State<RigApiState>,
    ctx: WorkspaceContext,
    Path(name): Path<String>,
) -> Result<Json<Value>, ApiError> {
    let repo = st.rigs.repo(ctx.workspace().as_str()).await?;
    match repo.get(&name).await? {
        Some(entry) => Ok(Json(entry_json(&entry))),
        None => Err(ApiError(AppError::NotFound(format!("rig {name}")))),
    }
}

/// `GET /lookup?prefix=` — resolve the rig owning a given bead-id prefix (`rig.lookup-by-prefix`).
///
/// A static segment, so it never shadows `GET /:name` (axum routes static before dynamic). `404`
/// when no rig in the workspace owns the prefix.
#[cfg_attr(feature = "axum", utoipa::path(
    get, path = "/lookup",
    params(("prefix" = String, Query, description = "Bead-id prefix to resolve")),
    responses(
        (status = 200, description = "The rig owning the prefix"),
        (status = 404, description = "No rig owns that prefix in this workspace"),
    ),
))]
async fn lookup_by_prefix(
    State(st): State<RigApiState>,
    ctx: WorkspaceContext,
    Query(q): Query<LookupQuery>,
) -> Result<Json<Value>, ApiError> {
    let repo = st.rigs.repo(ctx.workspace().as_str()).await?;
    match repo.prefix_owner(&q.prefix).await? {
        Some(name) => match repo.get(&name).await? {
            Some(entry) => Ok(Json(entry_json(&entry))),
            None => Err(ApiError(AppError::NotFound(format!("rig {name}")))),
        },
        None => Err(ApiError(AppError::NotFound(format!("rig for prefix {:?}", q.prefix)))),
    }
}

/// Querystring for `GET /lookup`.
#[derive(Debug, Deserialize)]
struct LookupQuery {
    /// The bead-id prefix to resolve to its owning rig.
    prefix: String,
}

/// `POST /` — register a new rig (`rig.add`). The name lives in the body (it names the new
/// resource, like the issues `create`), uniqueness enforced by the catalog. `201` on success.
#[cfg_attr(feature = "axum", utoipa::path(
    post, path = "/",
    responses(
        (status = 201, description = "Rig registered"),
        (status = 422, description = "Validation failed (name/prefix grammar or collision)"),
    ),
))]
async fn add_rig(
    State(st): State<RigApiState>,
    ctx: WorkspaceContext,
    Json(body): Json<Value>,
) -> Result<Response, ApiError> {
    let repo = st.rigs.repo(ctx.workspace().as_str()).await?;
    let cmd: AddRig = parse_cmd(body)?;
    // Decide + persist; the `201` envelope below replaces the helper's `200` body.
    let _ = apply_and_upsert(&*repo, cmd.name.clone(), &cmd).await?;
    Ok((StatusCode::CREATED, Json(json!({ "ok": true, "rig": cmd.name }))).into_response())
}

/// `POST /adopt` — adopt an existing on-disk rig into the catalog (`rig.adopt`). Same shape as
/// `POST /`; emits the distinct `rig.adopted` kind. `201` on success.
#[cfg_attr(feature = "axum", utoipa::path(
    post, path = "/adopt",
    responses(
        (status = 201, description = "Rig adopted"),
        (status = 422, description = "Validation failed (name/prefix grammar or collision)"),
    ),
))]
async fn adopt_rig(
    State(st): State<RigApiState>,
    ctx: WorkspaceContext,
    Json(body): Json<Value>,
) -> Result<Response, ApiError> {
    let repo = st.rigs.repo(ctx.workspace().as_str()).await?;
    let cmd: AdoptRig = parse_cmd(body)?;
    // Decide + persist; the `201` envelope below replaces the helper's `200` body.
    let _ = apply_and_upsert(&*repo, cmd.name.clone(), &cmd).await?;
    Ok((StatusCode::CREATED, Json(json!({ "ok": true, "rig": cmd.name }))).into_response())
}

/// `DELETE /:name` — drop a rig from the catalog (`rig.remove`). The name comes from the path;
/// the command decides against the live catalog (rejecting an absent rig as `404`) before the
/// row is deleted.
#[cfg_attr(feature = "axum", utoipa::path(
    delete, path = "/{name}",
    params(("name" = String, Path, description = "Rig name")),
    responses(
        (status = 200, description = "Rig removed"),
        (status = 404, description = "No rig with that name"),
    ),
))]
async fn remove_rig(
    State(st): State<RigApiState>,
    ctx: WorkspaceContext,
    Path(name): Path<String>,
) -> Result<Json<Value>, ApiError> {
    let repo = st.rigs.repo(ctx.workspace().as_str()).await?;
    let cmd: RemoveRig = with_path_name(json!({}), name.clone())?;
    // Decide against the live catalog (absent rig ⇒ NotFound), then delete the row.
    let mut catalog = hydrate(&*repo).await?;
    cmd.execute(&mut catalog)?;
    repo.remove(&name).await?;
    Ok(Json(json!({ "ok": true, "rig": name, "removed": true })))
}

/// `POST /:name/set-prefix` — change a rig's bead-id prefix (`rig.set-prefix`). The name always
/// comes from the path and overwrites any `name` in the body (docs/03 Rule 6).
#[cfg_attr(feature = "axum", utoipa::path(
    post, path = "/{name}/set-prefix",
    params(("name" = String, Path, description = "Rig name")),
    responses(
        (status = 200, description = "Prefix changed"),
        (status = 422, description = "Validation failed (grammar, collision, or no-op)"),
        (status = 404, description = "No rig with that name"),
    ),
))]
async fn set_prefix(
    State(st): State<RigApiState>,
    ctx: WorkspaceContext,
    Path(name): Path<String>,
    Json(body): Json<Value>,
) -> Result<Json<Value>, ApiError> {
    let repo = st.rigs.repo(ctx.workspace().as_str()).await?;
    let cmd: SetRigPrefix = with_path_name(body, name.clone())?;
    apply_and_upsert(&*repo, name, &cmd).await
}

/// `POST /:name/set-default-branch` — change a rig's default branch (`rig.set-default-branch`).
#[cfg_attr(feature = "axum", utoipa::path(
    post, path = "/{name}/set-default-branch",
    params(("name" = String, Path, description = "Rig name")),
    responses(
        (status = 200, description = "Default branch changed"),
        (status = 422, description = "Validation failed (empty or no-op)"),
        (status = 404, description = "No rig with that name"),
    ),
))]
async fn set_default_branch(
    State(st): State<RigApiState>,
    ctx: WorkspaceContext,
    Path(name): Path<String>,
    Json(body): Json<Value>,
) -> Result<Json<Value>, ApiError> {
    let repo = st.rigs.repo(ctx.workspace().as_str()).await?;
    let cmd: SetRigDefaultBranch = with_path_name(body, name.clone())?;
    apply_and_upsert(&*repo, name, &cmd).await
}

/// `POST /:name/set-worktree-root` — pin a rig's worktree-root override (`rig.set-worktree-root`).
#[cfg_attr(feature = "axum", utoipa::path(
    post, path = "/{name}/set-worktree-root",
    params(("name" = String, Path, description = "Rig name")),
    responses(
        (status = 200, description = "Worktree root pinned"),
        (status = 422, description = "Validation failed (relative path, `..`, too long, or no-op)"),
        (status = 404, description = "No rig with that name"),
    ),
))]
async fn set_worktree_root(
    State(st): State<RigApiState>,
    ctx: WorkspaceContext,
    Path(name): Path<String>,
    Json(body): Json<Value>,
) -> Result<Json<Value>, ApiError> {
    let repo = st.rigs.repo(ctx.workspace().as_str()).await?;
    let cmd: SetRigWorktreeRoot = with_path_name(body, name.clone())?;
    apply_and_upsert(&*repo, name, &cmd).await
}

/// The combined OpenAPI document for the rig REST surface (`hq-fe-api-platform.2`). The builder
/// mounts it under the module prefix and rewrites its relative paths to `/api/v1/rig/...`, so
/// the `#[utoipa::path]` annotations stay prefix-free.
#[derive(utoipa::OpenApi)]
#[openapi(paths(
    list_rigs,
    get_rig,
    lookup_by_prefix,
    add_rig,
    adopt_rig,
    remove_rig,
    set_prefix,
    set_default_branch,
    set_worktree_root,
))]
pub struct ApiDoc;

/// Hydrate a [`RigCatalog`] from the caller's `rigs` table (the same decide-against-live-state
/// path the MCP `RigHandler` takes).
async fn hydrate(repo: &dyn DynRigRepository) -> Result<RigCatalog, ApiError> {
    let mut catalog = RigCatalog::default();
    for entry in repo.list().await? {
        catalog.apply_add(entry);
    }
    Ok(catalog)
}

/// Decide a catalog-mutating command, then persist the touched entry.
///
/// `execute` validates against the hydrated catalog and mutates it in memory (the same path the
/// actor + MCP handler take); the entry the command touched is then upserted back so the change
/// is durable.
async fn apply_and_upsert<C>(
    repo: &dyn DynRigRepository,
    name: String,
    cmd: &C,
) -> Result<Json<Value>, ApiError>
where
    C: Command<State = RigCatalog>,
{
    let mut catalog = hydrate(repo).await?;
    cmd.execute(&mut catalog)?;
    let entry = catalog
        .get(&name)
        .cloned()
        .ok_or_else(|| ApiError(AppError::Other(format!("rig {name} missing after execute"))))?;
    repo.upsert(&entry).await?;
    Ok(Json(json!({ "ok": true, "rig": name })))
}

/// Deserialize a command struct from a request body, stamping `now_secs` with the server clock
/// when the caller omits it (the clock is the edge's to supply, not the model's). A malformed
/// payload is a `422`, matching the MCP `parse_cmd` path. `workspace_id` is `skip_deserializing`
/// on every rig command, so a body can never spoof the tenant — it is resolved from the auth
/// context and the per-workspace repo, never the payload.
fn parse_cmd<T: DeserializeOwned>(mut body: Value) -> Result<T, ApiError> {
    if let Value::Object(map) = &mut body {
        map.entry("now_secs").or_insert_with(|| json!(now_secs()));
    }
    serde_json::from_value(body)
        .map_err(|e| ApiError(AppError::Validation(format!("invalid arguments: {e}"))))
}

/// Deserialize a path-addressed command body, forcing `name` to the path segment so it is never
/// trusted from the payload (docs/03 Rule 6). The path name overwrites any `name` in the body
/// and supplies it when absent; `now_secs` is then stamped by [`parse_cmd`].
fn with_path_name<T: DeserializeOwned>(mut body: Value, name: String) -> Result<T, ApiError> {
    match &mut body {
        Value::Object(map) => {
            map.insert("name".to_string(), Value::String(name));
        }
        // A non-object body (e.g. `null` from an empty request) becomes just the name.
        other => *other = json!({ "name": name }),
    }
    parse_cmd(body)
}

/// Server-side epoch-seconds clock for command timestamps.
fn now_secs() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or(0)
}

/// Shape one rig entry as the REST payload — the same field set the MCP `RigHandler` emits.
fn entry_json(entry: &RigEntry) -> Value {
    json!({
        "name": entry.name,
        "prefix": entry.prefix,
        "git_url": entry.git_url,
        "push_url": entry.push_url,
        "upstream_url": entry.upstream_url,
        "default_branch": entry.default_branch,
        "registered_at_secs": entry.registered_at_secs,
        "worktree_root": entry.worktree_root,
    })
}

/// HTTP wrapper over the domain [`AppError`] so a handler can `?`-propagate it and have it
/// rendered with the right status. The body is the bare error message — the same text the MCP
/// path surfaces — so a client sees an identical reason across transports.
#[derive(Debug)]
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
    use utoipa::OpenApi;

    #[test]
    fn openapi_lists_every_relative_route_prefix_free() {
        // The spec the builder mounts: paths are relative (no `/api/v1/rig`), so nesting can
        // rewrite them. Every declared route must be present so the combined document is complete.
        let doc = ApiDoc::openapi();
        let paths: Vec<&str> = doc.paths.paths.keys().map(String::as_str).collect();
        for expected in [
            "/",
            "/{name}",
            "/lookup",
            "/adopt",
            "/{name}/set-prefix",
            "/{name}/set-default-branch",
            "/{name}/set-worktree-root",
        ] {
            assert!(paths.contains(&expected), "missing {expected} in {paths:?}");
        }
        // Prefix-free: the module builder, not the annotation, owns `/api/v1/rig`.
        assert!(paths.iter().all(|p| !p.contains("/api/v1")), "{paths:?}");
    }

    #[test]
    fn error_status_mapping_matches_app_error_kinds() {
        let cases = [
            (AppError::NotFound("x".into()), StatusCode::NOT_FOUND),
            (AppError::Validation("x".into()), StatusCode::UNPROCESSABLE_ENTITY),
            (AppError::InvalidTransition("x".into()), StatusCode::CONFLICT),
            (AppError::Handler("x".into()), StatusCode::INTERNAL_SERVER_ERROR),
            (AppError::Other("x".into()), StatusCode::INTERNAL_SERVER_ERROR),
        ];
        for (err, want) in cases {
            assert_eq!(ApiError(err).into_response().status(), want);
        }
    }

    #[test]
    fn parse_cmd_stamps_now_secs_when_absent() {
        let cmd: AddRig = parse_cmd(json!({
            "name": "plane", "prefix": "pl",
            "git_url": "git@x:y/plane.git", "default_branch": "main",
        }))
        .expect("valid add");
        assert!(cmd.now_secs > 0, "server stamped a clock");
        assert_eq!(cmd.name, "plane");
    }

    #[test]
    fn with_path_name_forces_name_over_body() {
        // The body names a DECOY rig; the path segment must win (docs/03 Rule 6).
        let cmd: SetRigPrefix = with_path_name(
            json!({ "name": "decoy", "new_prefix": "px" }),
            "keep".to_string(),
        )
        .expect("valid set-prefix");
        assert_eq!(cmd.name, "keep", "path name overrides the body");
        assert_eq!(cmd.new_prefix, "px");
    }

    #[test]
    fn parse_cmd_rejects_malformed() {
        let err = parse_cmd::<AddRig>(json!({ "name": "plane" })).unwrap_err();
        assert!(matches!(err.0, AppError::Validation(_)));
    }
}
