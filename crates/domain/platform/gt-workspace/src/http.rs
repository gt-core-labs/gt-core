//! The `axum` REST adapter for the `workspace.*` surface (`hq-fe-api-platform.1`).
//!
//! Exposes the workspace catalog as REST routes that dispatch to the **same**
//! [`WorkspaceCommand`] decide/apply layer the MCP `WorkspaceHandler` (in
//! `gt-composition`) wraps — no lifecycle logic is duplicated, only the wire shape
//! differs. It folds into `gt-workspace` behind the off-by-default `axum` feature
//! (docs/03 Rule 4), beside the [`WorkspaceContext`](crate::WorkspaceContext)
//! extractor, rather than living in a sibling crate.
//!
//! ## Admin-level, cross-workspace — *not* per-tenant
//!
//! Every other module's REST surface resolves a per-request tenant boundary through
//! [`WorkspaceContext`](crate::WorkspaceContext) (docs/04 §15: the workspace is read
//! from the verified auth context, never the path/body). **These routes deliberately
//! do not.** `workspace.*` is the single cross-workspace consumer (docs/03 Rule 6):
//! it administers the *catalog of workspaces itself*, the lifecycle of tenants, not
//! data inside one tenant. So:
//!
//! - The `:id` path segment names the **catalog resource** being administered (the
//!   workspace whose lifecycle the request changes), not a tenant context. This is
//!   the one legitimate place an id rides the path, because the catalog *is* the
//!   resource — there is no enclosing tenant to resolve.
//! - The guard scopes the module claims — `workspace.read` / `workspace.write` (see
//!   [`WorkspaceModule::capability`](crate::WorkspaceModule)) — are **operator/admin**
//!   authorities: they govern who may list and mutate the tenant catalog, and should
//!   be granted only to platform operators, never to an ordinary tenant user. The
//!   builder's capability-derived guard maps `GET` → `workspace.read` and the
//!   mutating verbs → `workspace.write`; `admin`-style enforcement is *who holds
//!   `workspace.write`*, not a separate verb the guard would leave ungated.
//!
//! ## What it does *not* do
//!
//! - **It does not authenticate or authorize.** The builder mounts this router under
//!   `/api/v1/workspace` and wraps it with the scope guard
//!   ([`guard_module_scopes`](gt_module::guard_module_scopes)); the composition root
//!   layers the auth middleware in front.
//! - **It does not provision the tenant's PG schema.** The MCP `workspace.create`
//!   path additionally calls `gt_create_workspace_schema` to clone `ws_default`; that
//!   is a composition-tier concern (raw SQL over the shared pool the domain tier does
//!   not own), layered by the binary, not this catalog-only adapter. A REST `create`
//!   records the catalog entry through the replayable [`WorkspaceActor`] cycle exactly
//!   as the MCP path's catalog step does.

use std::sync::Arc;

use axum::extract::{Path, State};
use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use axum::routing::{get, post};
use axum::{Extension, Json, Router};
use serde::Deserialize;
use serde_json::{json, Value};

use gt_auth::JwtClaims;

use crate::actor::{ActorError, WorkspaceActor};
use crate::commands::{WorkspaceCommand, WorkspaceError};
use crate::repo::{RepoError, WorkspaceRepository};
use crate::state::{WorkspaceEntry, WorkspaceStatus};
use crate::workspace_id::{WorkspaceId, WorkspaceIdError};

/// Provision a freshly-created workspace's per-tenant backends (PG schema + RBAC seed,
/// Dolt `hq_<slug>` + issues schema) so it is usable the moment it is created.
///
/// The composition root supplies the adapter (docs/03 Rule 4): only it knows both the PG
/// pool that owns `gt_create_workspace_schema` + the `ws_<slug>` RBAC tables and the
/// per-workspace Dolt pools. Keyed by the verified creator `sub` so membership is attributed
/// to the caller, never a path/body value. Idempotent — `create_workspace` calls it after the
/// catalog event so the REST path provisions exactly like the MCP `workspace.create` tool
/// (`hq-gap-workspace-rest-create-provision`).
#[async_trait::async_trait]
pub trait TenantProvisioner: Send + Sync {
    /// Provision the tenant `slug` on behalf of `actor` (the creator's verified `sub`, or `""`
    /// for a system-created workspace — the adapter then seeds the schema/role but no membership).
    ///
    /// `catalog` is the raw `domain_catalog` init directive (gtcore-22f57b H3): `None` /
    /// `"template"` seeds the editable generic base, `"empty"` seeds only the reserved domain,
    /// and `"clone_from:<workspace>"` copies another workspace's catalog. An unrecognised value
    /// is a caller-side error the adapter surfaces as a validation fault.
    async fn provision(
        &self,
        slug: &str,
        actor: &str,
        catalog: Option<&str>,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>>;

    /// Backfill an EXISTING workspace's domain catalog (gtcore-22f57b H3): apply the generic
    /// template (or the `catalog`-named mode) to a tenant `slug` provisioned before the catalog
    /// landed. Idempotent — a no-op once the workspace has a seeded catalog — and it refuses the
    /// technical workspaces (`default`/`gtcore`). Returns the number of rows seeded (0 ⇒ already
    /// seeded). A bad `catalog` directive or an ineligible target is a caller-side error.
    async fn backfill_catalog(
        &self,
        slug: &str,
        catalog: Option<&str>,
    ) -> Result<u64, Box<dyn std::error::Error + Send + Sync>>;
}

/// Everything the workspace REST handlers need, baked into the router with
/// [`Router::with_state`] before it leaves [`workspace_router`] so the merged
/// application router carries no outstanding state type (the kernel's state-erased
/// `Router<()>` contract).
///
/// Holds the [`WorkspaceRepository`] as a shared trait object so the adapter stays
/// independent of which backend the binary supplies (the durable [`PgWorkspaces`]
/// under the `pg` feature, or an in-memory store in a test). Each mutating handler
/// clones the `Arc` into a fresh [`WorkspaceActor`] per request — the same
/// hydrate-per-call shape the MCP `WorkspaceHandler` uses.
///
/// [`PgWorkspaces`]: crate::PgWorkspaces
#[derive(Clone)]
pub struct WorkspaceApiState {
    /// The live catalog repository the binary supplies — the module owns no store of
    /// its own.
    repo: Arc<dyn WorkspaceRepository>,
    /// Per-tenant backend provisioner, wired by the composition root when multi-tenant
    /// backends exist. `Some` ⇒ `POST /` also seeds the new tenant's PG schema/RBAC + Dolt
    /// (the REST mirror of the MCP `workspace.create` provisioning). `None` ⇒ catalog row only,
    /// exactly as before.
    provisioner: Option<Arc<dyn TenantProvisioner>>,
}

impl WorkspaceApiState {
    /// Build the REST state over a live catalog repository. Catalog-only by default; call
    /// [`with_provisioner`](Self::with_provisioner) to also provision per-tenant backends.
    pub fn new(repo: Arc<dyn WorkspaceRepository>) -> Self {
        Self { repo, provisioner: None }
    }

    /// Attach the per-tenant provisioner so `POST /` provisions a fully-usable workspace
    /// (PG schema/RBAC + Dolt), matching the MCP `workspace.create` tool.
    pub fn with_provisioner(mut self, provisioner: Arc<dyn TenantProvisioner>) -> Self {
        self.provisioner = Some(provisioner);
        self
    }

    /// Hydrate a fresh actor from the repository for one mutating command.
    async fn actor(&self) -> Result<WorkspaceActor<Arc<dyn WorkspaceRepository>>, ApiError> {
        Ok(WorkspaceActor::hydrate(self.repo.clone()).await?)
    }
}

/// Build the workspace REST router with `state` baked in (`hq-fe-api-platform.1`).
///
/// The paths are **relative**: the builder nests them under `/api/v1/workspace` and
/// applies the scope guard. [`register_routes`](crate::WorkspaceModule) on
/// [`WorkspaceModule`](crate::WorkspaceModule) returns exactly this router when the
/// module carries HTTP state.
///
/// | Method + path           | Maps to MCP tool      |
/// |-------------------------|-----------------------|
/// | `GET /`                 | `workspace.list`      |
/// | `POST /`                | `workspace.create`    |
/// | `GET /:id`              | `workspace.info`      |
/// | `POST /:id/suspend`     | `workspace.suspend`   |
/// | `POST /:id/resume`      | `workspace.resume`    |
/// | `POST /:id/archive`     | `workspace.archive`   |
/// | `POST /:id/backfill-catalog` | `workspace.backfill-catalog` |
pub fn workspace_router(state: WorkspaceApiState) -> Router {
    Router::new()
        .route("/", get(list_workspaces).post(create_workspace))
        .route("/:id", get(get_workspace))
        .route("/:id/suspend", post(suspend_workspace))
        .route("/:id/resume", post(resume_workspace))
        .route("/:id/archive", post(archive_workspace))
        .route("/:id/backfill-catalog", post(backfill_catalog))
        .with_state(state)
}

/// Request body for `POST /` — the catalog id + display name for the new workspace.
/// Mirrors the `workspace.create` MCP arguments (`{id, name}`).
#[derive(Debug, Deserialize)]
struct CreateBody {
    /// The workspace slug to create (validated as a [`WorkspaceId`]).
    id: String,
    /// Its display name.
    name: String,
    /// Optional `domain_catalog` initialisation (gtcore-22f57b H3): `"template"`
    /// (default), `"empty"`, or `"clone_from:<workspace>"`. Absent ⇒ the editable
    /// generic template, exactly the pre-H3 behaviour.
    #[serde(default)]
    catalog: Option<String>,
}

/// `GET /` — every workspace in the catalog (`workspace.list`). A pure read straight
/// off the repository; the envelope matches the MCP tool's `{ "workspaces": [...] }`.
#[cfg_attr(feature = "axum", utoipa::path(
    get, path = "/",
    responses((status = 200, description = "Every workspace in the catalog")),
))]
async fn list_workspaces(State(st): State<WorkspaceApiState>) -> Result<Json<Value>, ApiError> {
    let entries = st.repo.list().await?;
    Ok(Json(json!({
        "workspaces": entries.iter().map(entry_json).collect::<Vec<_>>(),
    })))
}

/// `GET /:id` — one workspace's id, name + status (`workspace.info`); `404` when the
/// catalog holds no such id.
#[cfg_attr(feature = "axum", utoipa::path(
    get, path = "/{id}",
    params(("id" = String, Path, description = "Workspace slug")),
    responses(
        (status = 200, description = "The workspace's id, name + status"),
        (status = 404, description = "No workspace with that id"),
    ),
))]
async fn get_workspace(
    State(st): State<WorkspaceApiState>,
    Path(id): Path<String>,
) -> Result<Json<Value>, ApiError> {
    let id = parse_id(&id)?;
    match st.repo.load(&id).await? {
        Some(entry) => Ok(Json(entry_json(&entry))),
        None => Err(ApiError::NotFound(format!("workspace {id}"))),
    }
}

/// `POST /` — provision a new active workspace in the catalog (`workspace.create`).
/// The id lives in the body (it names the new resource), uniqueness is enforced by
/// the decide layer (`409` on a duplicate). Runs the full [`WorkspaceActor`] cycle so
/// the change is a replayable event. `201` on success.
#[cfg_attr(feature = "axum", utoipa::path(
    post, path = "/",
    responses(
        (status = 201, description = "Workspace created"),
        (status = 409, description = "A workspace with that id already exists"),
        (status = 422, description = "Invalid id or blank name"),
    ),
))]
async fn create_workspace(
    State(st): State<WorkspaceApiState>,
    claims: Option<Extension<JwtClaims>>,
    Json(body): Json<CreateBody>,
) -> Result<Response, ApiError> {
    let id = parse_id(&body.id)?;
    let mut actor = st.actor().await?;
    actor
        .handle(WorkspaceCommand::Create { id: id.clone(), name: body.name.clone() })
        .await?;
    // Provision the tenant's backends so the workspace is usable on creation — the REST mirror
    // of the MCP `workspace.create` provisioning (`hq-gap-workspace-rest-create-provision`).
    // Attributed to the verified creator `sub` (membership lands for them); a request with no
    // verified identity provisions the schema/role but no membership (actor `""`). `None`
    // provisioner ⇒ catalog-only, exactly as before.
    if let Some(provisioner) = &st.provisioner {
        let creator = claims.as_ref().map(|Extension(c)| c.sub.as_str()).unwrap_or("");
        provisioner
            .provision(id.as_str(), creator, body.catalog.as_deref())
            .await
            .map_err(|e| provision_error(&id, e))?;
    }
    Ok((
        StatusCode::CREATED,
        Json(json!({
            "ok": true,
            "id": id.as_str(),
            "name": body.name,
            "status": status_str(WorkspaceStatus::Active),
        })),
    )
        .into_response())
}

/// `POST /:id/suspend` — reversibly disable an active workspace (`workspace.suspend`).
/// The decide layer rejects a non-active source as an illegal transition (`409`).
#[cfg_attr(feature = "axum", utoipa::path(
    post, path = "/{id}/suspend",
    params(("id" = String, Path, description = "Workspace slug")),
    responses(
        (status = 200, description = "Workspace suspended"),
        (status = 404, description = "No workspace with that id"),
        (status = 409, description = "Not active (illegal transition)"),
    ),
))]
async fn suspend_workspace(
    State(st): State<WorkspaceApiState>,
    Path(id): Path<String>,
) -> Result<Json<Value>, ApiError> {
    transition(st, &id, WorkspaceStatus::Suspended, |id| WorkspaceCommand::Suspend { id }).await
}

/// `POST /:id/resume` — restore a suspended workspace to active (`workspace.resume`).
/// The decide layer rejects a non-suspended source as an illegal transition (`409`).
#[cfg_attr(feature = "axum", utoipa::path(
    post, path = "/{id}/resume",
    params(("id" = String, Path, description = "Workspace slug")),
    responses(
        (status = 200, description = "Workspace resumed"),
        (status = 404, description = "No workspace with that id"),
        (status = 409, description = "Not suspended (illegal transition)"),
    ),
))]
async fn resume_workspace(
    State(st): State<WorkspaceApiState>,
    Path(id): Path<String>,
) -> Result<Json<Value>, ApiError> {
    transition(st, &id, WorkspaceStatus::Active, |id| WorkspaceCommand::Resume { id }).await
}

/// `POST /:id/archive` — archive a workspace (`workspace.archive`, terminal). The
/// decide layer rejects an already-archived workspace as an illegal transition (`409`).
#[cfg_attr(feature = "axum", utoipa::path(
    post, path = "/{id}/archive",
    params(("id" = String, Path, description = "Workspace slug")),
    responses(
        (status = 200, description = "Workspace archived"),
        (status = 404, description = "No workspace with that id"),
        (status = 409, description = "Already archived (illegal transition)"),
    ),
))]
async fn archive_workspace(
    State(st): State<WorkspaceApiState>,
    Path(id): Path<String>,
) -> Result<Json<Value>, ApiError> {
    transition(st, &id, WorkspaceStatus::Archived, |id| WorkspaceCommand::Archive { id }).await
}

/// Request body for `POST /:id/backfill-catalog` — the optional catalog init mode
/// (gtcore-22f57b H3). Absent ⇒ the generic template, exactly like `workspace.create`.
#[derive(Debug, Deserialize, Default)]
struct BackfillBody {
    /// `"template"` (default), `"empty"`, or `"clone_from:<workspace>"`.
    #[serde(default)]
    catalog: Option<String>,
}

/// `POST /:id/backfill-catalog` — apply the generic domain template to an existing
/// workspace that has no catalog yet (`workspace.backfill-catalog`, gtcore-22f57b H3).
/// Idempotent: a workspace that already has a seeded catalog returns `seeded: false`
/// with `200`. The technical workspaces (`default`/`gtcore`) are refused (`422`).
/// Requires the per-tenant provisioner to be wired; without it (single-tenant Dolt)
/// there is no per-workspace catalog to backfill (`500`).
#[cfg_attr(feature = "axum", utoipa::path(
    post, path = "/{id}/backfill-catalog",
    params(("id" = String, Path, description = "Workspace slug")),
    responses(
        (status = 200, description = "Catalog backfilled (or already seeded — echoes { seeded, rows })"),
        (status = 422, description = "Invalid id, bad catalog directive, or an ineligible technical workspace"),
        (status = 500, description = "No per-tenant provisioner wired, or a backend fault"),
    ),
))]
async fn backfill_catalog(
    State(st): State<WorkspaceApiState>,
    Path(id): Path<String>,
    body: Option<Json<BackfillBody>>,
) -> Result<Json<Value>, ApiError> {
    let id = parse_id(&id)?;
    let Json(body) = body.unwrap_or_default();
    let Some(provisioner) = &st.provisioner else {
        return Err(ApiError::Internal(
            "domain-catalog backfill needs the per-tenant provisioner (multi-tenant Dolt)".into(),
        ));
    };
    let rows = provisioner
        .backfill_catalog(id.as_str(), body.catalog.as_deref())
        .await
        .map_err(|e| provision_error(&id, e))?;
    Ok(Json(json!({
        "ok": true,
        "id": id.as_str(),
        "seeded": rows > 0,
        "rows": rows,
    })))
}

/// Run one lifecycle transition: parse the path id, hydrate an actor, decide+apply
/// the command, and echo the resulting status. Shared by suspend/resume/archive,
/// which differ only in the command they build and the status they land on.
async fn transition(
    st: WorkspaceApiState,
    id: &str,
    landed: WorkspaceStatus,
    build: impl FnOnce(WorkspaceId) -> WorkspaceCommand,
) -> Result<Json<Value>, ApiError> {
    let id = parse_id(id)?;
    let mut actor = st.actor().await?;
    actor.handle(build(id.clone())).await?;
    Ok(Json(json!({
        "ok": true,
        "id": id.as_str(),
        "status": status_str(landed),
    })))
}

/// The combined OpenAPI document for the workspace REST surface
/// (`hq-fe-api-platform.1`). The builder mounts it under the module prefix and
/// rewrites its relative paths to `/api/v1/workspace/...`, so the `#[utoipa::path]`
/// annotations stay prefix-free.
#[derive(utoipa::OpenApi)]
#[openapi(paths(
    list_workspaces,
    create_workspace,
    get_workspace,
    suspend_workspace,
    resume_workspace,
    archive_workspace,
    backfill_catalog,
))]
pub struct ApiDoc;

/// Parse a path/body slug into a [`WorkspaceId`], surfacing a malformed id as a `422`.
fn parse_id(raw: &str) -> Result<WorkspaceId, ApiError> {
    WorkspaceId::new(raw).map_err(ApiError::from)
}

/// Map a provisioner failure onto the HTTP error space (gtcore-22f57b H3). A bad
/// `catalog` init directive is a caller-side validation fault (`422`); any other
/// provisioning failure (PG/Dolt backend) is internal (`500`). The provisioner boxes
/// the underlying [`AppError`]'s text, so the validation case is recognised by the
/// `CatalogInit::parse` message shapes it forwards.
fn provision_error(id: &WorkspaceId, e: Box<dyn std::error::Error + Send + Sync>) -> ApiError {
    let msg = e.to_string();
    // Caller-side faults: a malformed `catalog` directive, a `clone_from` whose source has no
    // catalog, or an ineligible technical workspace. Everything else is a backend fault.
    let caller_fault = msg.contains("unknown catalog init")
        || msg.contains("clone_from")
        || msg.contains("not eligible for the generic-template backfill");
    if caller_fault {
        ApiError::Unprocessable(msg)
    } else {
        ApiError::Internal(format!("provision tenant {id}: {msg}"))
    }
}

/// The snake_case spelling of a status, matching the serde representation the MCP
/// path emits.
fn status_str(status: WorkspaceStatus) -> &'static str {
    match status {
        WorkspaceStatus::Active => "active",
        WorkspaceStatus::Suspended => "suspended",
        WorkspaceStatus::Archived => "archived",
    }
}

/// Shape one catalog entry as the REST payload — identical to the MCP tool's
/// `{ id, name, status }`.
fn entry_json(entry: &WorkspaceEntry) -> Value {
    json!({
        "id": entry.id.as_str(),
        "name": entry.name,
        "status": status_str(entry.status),
    })
}

/// HTTP error space for the workspace REST handlers, mapping the domain failures
/// onto statuses so a handler can `?`-propagate them. The body is the bare message,
/// matching the reason the MCP path surfaces so a client sees identical text across
/// transports.
enum ApiError {
    /// A targeted workspace is absent (`404`).
    NotFound(String),
    /// A request conflicts with the catalog's current state — a duplicate id or an
    /// illegal lifecycle transition (`409`).
    Conflict(String),
    /// The request was malformed — a bad slug or a blank name (`422`).
    Unprocessable(String),
    /// A persistence/apply fault behind the catalog (`500`).
    Internal(String),
}

impl From<WorkspaceIdError> for ApiError {
    fn from(e: WorkspaceIdError) -> Self {
        ApiError::Unprocessable(e.to_string())
    }
}

impl From<RepoError> for ApiError {
    fn from(e: RepoError) -> Self {
        // A backend or consistency fault is internal, never a caller fault.
        ApiError::Internal(e.to_string())
    }
}

impl From<ActorError> for ApiError {
    fn from(e: ActorError) -> Self {
        match e {
            // A missing target is a not-found; a duplicate or illegal transition is a
            // conflict with current state; a blank name is unprocessable.
            ActorError::Rejected(WorkspaceError::NotFound(id)) => {
                ApiError::NotFound(format!("workspace {id}"))
            }
            ActorError::Rejected(rejected @ WorkspaceError::AlreadyExists(_)) => {
                ApiError::Conflict(rejected.to_string())
            }
            ActorError::Rejected(rejected @ WorkspaceError::IllegalTransition { .. }) => {
                ApiError::Conflict(rejected.to_string())
            }
            ActorError::Rejected(rejected @ WorkspaceError::EmptyName(_)) => {
                ApiError::Unprocessable(rejected.to_string())
            }
            // Apply/persist faults are internal — they should not occur after a
            // successful decide.
            other => ApiError::Internal(other.to_string()),
        }
    }
}

impl IntoResponse for ApiError {
    fn into_response(self) -> Response {
        let (status, msg) = match self {
            ApiError::NotFound(m) => (StatusCode::NOT_FOUND, m),
            ApiError::Conflict(m) => (StatusCode::CONFLICT, m),
            ApiError::Unprocessable(m) => (StatusCode::UNPROCESSABLE_ENTITY, m),
            ApiError::Internal(m) => (StatusCode::INTERNAL_SERVER_ERROR, m),
        };
        (status, msg).into_response()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use utoipa::OpenApi;

    #[test]
    fn openapi_lists_every_relative_route_prefix_free() {
        // The spec the builder mounts: paths are relative (no `/api/v1/workspace`), so
        // nesting can rewrite them. Every declared route must be present.
        let doc = ApiDoc::openapi();
        let paths: Vec<&str> = doc.paths.paths.keys().map(String::as_str).collect();
        for expected in [
            "/",
            "/{id}",
            "/{id}/suspend",
            "/{id}/resume",
            "/{id}/archive",
            "/{id}/backfill-catalog",
        ] {
            assert!(paths.contains(&expected), "missing {expected} in {paths:?}");
        }
        // Prefix-free: the module builder, not the annotation, owns `/api/v1/workspace`.
        assert!(paths.iter().all(|p| !p.contains("/api/v1")), "{paths:?}");
    }

    /// A recording [`TenantProvisioner`] so the create test asserts the wiring without a DB.
    #[derive(Default)]
    struct RecordingProvisioner {
        calls: std::sync::Mutex<Vec<(String, String, Option<String>)>>,
    }

    #[async_trait::async_trait]
    impl TenantProvisioner for RecordingProvisioner {
        async fn provision(
            &self,
            slug: &str,
            actor: &str,
            catalog: Option<&str>,
        ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
            self.calls.lock().unwrap().push((
                slug.to_string(),
                actor.to_string(),
                catalog.map(str::to_string),
            ));
            Ok(())
        }

        async fn backfill_catalog(
            &self,
            _slug: &str,
            _catalog: Option<&str>,
        ) -> Result<u64, Box<dyn std::error::Error + Send + Sync>> {
            Ok(0)
        }
    }

    #[tokio::test]
    async fn rest_create_provisions_the_tenant_attributed_to_the_caller() {
        use axum::body::Body;
        use axum::http::Request;
        use tower::ServiceExt;

        let prov = Arc::new(RecordingProvisioner::default());
        let state = WorkspaceApiState::new(Arc::new(crate::InMemoryWorkspaces::new()))
            .with_provisioner(prov.clone());
        let app = workspace_router(state);

        // POST / carrying a verified JwtClaims (sub = alice): the catalog create succeeds AND the
        // provisioner runs for the new slug, attributed to the caller's sub — the REST mirror of
        // the MCP `workspace.create` provisioning.
        let mut req = Request::builder()
            .method("POST")
            .uri("/")
            .header("content-type", "application/json")
            .body(Body::from(json!({ "id": "acme", "name": "Acme" }).to_string()))
            .unwrap();
        req.extensions_mut().insert(JwtClaims {
            sub: "alice".to_string(),
            workspace: "acme".to_string(),
            scopes: vec![],
            exp: 0,
            nbf: None,
            iat: 0,
        });
        let resp = app.oneshot(req).await.unwrap();
        assert_eq!(resp.status(), StatusCode::CREATED);
        assert_eq!(
            prov.calls.lock().unwrap().as_slice(),
            // No `catalog` in the body ⇒ the provisioner is called with `None` (the
            // default template applies), attributed to the caller's sub.
            &[("acme".to_string(), "alice".to_string(), None)],
        );
    }

    #[tokio::test]
    async fn rest_create_forwards_the_catalog_init_directive() {
        use axum::body::Body;
        use axum::http::Request;
        use tower::ServiceExt;

        // A body that names `catalog: "empty"` reaches the provisioner verbatim (gtcore-22f57b H3),
        // so the REST path can pick the init mode exactly as the MCP `workspace.create` tool does.
        let prov = Arc::new(RecordingProvisioner::default());
        let state = WorkspaceApiState::new(Arc::new(crate::InMemoryWorkspaces::new()))
            .with_provisioner(prov.clone());
        let app = workspace_router(state);
        let req = Request::builder()
            .method("POST")
            .uri("/")
            .header("content-type", "application/json")
            .body(Body::from(
                json!({ "id": "acme", "name": "Acme", "catalog": "empty" }).to_string(),
            ))
            .unwrap();
        let resp = app.oneshot(req).await.unwrap();
        assert_eq!(resp.status(), StatusCode::CREATED);
        assert_eq!(
            prov.calls.lock().unwrap().as_slice(),
            &[("acme".to_string(), "".to_string(), Some("empty".to_string()))],
        );
    }

    #[tokio::test]
    async fn rest_create_without_provisioner_is_catalog_only() {
        use axum::body::Body;
        use axum::http::Request;
        use tower::ServiceExt;

        // No provisioner wired ⇒ create still succeeds (catalog row only), exactly as before.
        let app =
            workspace_router(WorkspaceApiState::new(Arc::new(crate::InMemoryWorkspaces::new())));
        let req = Request::builder()
            .method("POST")
            .uri("/")
            .header("content-type", "application/json")
            .body(Body::from(json!({ "id": "beta", "name": "Beta" }).to_string()))
            .unwrap();
        let resp = app.oneshot(req).await.unwrap();
        assert_eq!(resp.status(), StatusCode::CREATED);
    }

    #[test]
    fn error_status_mapping_matches_domain_failures() {
        let id = WorkspaceId::new("acme").unwrap();
        let cases = [
            (
                ApiError::from(ActorError::Rejected(WorkspaceError::NotFound(id.clone()))),
                StatusCode::NOT_FOUND,
            ),
            (
                ApiError::from(ActorError::Rejected(WorkspaceError::AlreadyExists(id.clone()))),
                StatusCode::CONFLICT,
            ),
            (
                ApiError::from(ActorError::Rejected(WorkspaceError::IllegalTransition {
                    id: id.clone(),
                    from: WorkspaceStatus::Archived,
                    action: "resume",
                })),
                StatusCode::CONFLICT,
            ),
            (
                ApiError::from(ActorError::Rejected(WorkspaceError::EmptyName(id))),
                StatusCode::UNPROCESSABLE_ENTITY,
            ),
            (
                ApiError::from(RepoError::Backend("boom".into())),
                StatusCode::INTERNAL_SERVER_ERROR,
            ),
            (
                ApiError::from(WorkspaceIdError::Empty),
                StatusCode::UNPROCESSABLE_ENTITY,
            ),
        ];
        for (err, want) in cases {
            assert_eq!(err.into_response().status(), want);
        }
    }

    #[test]
    fn entry_json_matches_the_mcp_shape() {
        let entry = WorkspaceEntry {
            id: WorkspaceId::new("acme").unwrap(),
            name: "Acme".into(),
            status: WorkspaceStatus::Suspended,
        };
        assert_eq!(
            entry_json(&entry),
            json!({ "id": "acme", "name": "Acme", "status": "suspended" })
        );
    }
}
