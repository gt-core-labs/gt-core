//! The `axum` REST adapter for the `domain.catalog.*` surface (gtcore-b37400 H4).
//!
//! Exposes the per-workspace domain catalog as admin-editable REST routes that
//! dispatch to the SAME [`DoltDomainCatalog`] CRUD the MCP `domain.catalog.*` tools
//! run — no logic is duplicated, only the wire shape differs. It folds into
//! `gt-meta` behind the off-by-default `axum` feature, exactly as the `meta.*`
//! adapter does.
//!
//! | Method + path             | Maps to MCP tool             | Verb-class |
//! |---------------------------|------------------------------|------------|
//! | `GET /catalog`            | `domain.catalog.list`        | read       |
//! | `POST /catalog`           | `domain.catalog.add`         | write      |
//! | `PATCH /catalog/{key}`    | `domain.catalog.rename` / `.set-enabled` | write |
//! | `DELETE /catalog/{key}`   | `domain.catalog.remove`      | write      |
//!
//! ## Auth
//!
//! The builder nests this router under `/api/v1/domain` and wraps it with the
//! module's scope guard: `domain.read` for `GET`, `domain.write` for the mutating
//! methods. Catalog editing is an ADMIN operation — only an admin/`*` token, or one
//! explicitly granted `domain.write`, holds it; the agent presets never do. The
//! reserved `meta.gap` is protected one layer deeper, in the store.
//!
//! ## Workspace resolution
//!
//! The active workspace is resolved through the verified [`WorkspaceContext`]
//! extractor — the same tenant boundary the issues surface and `GET /meta/domains`
//! use — never a path/body field. In multi-tenant mode the tenant's `hq_<ws>` pool
//! is selected; single-tenant falls back to the shared `store` pool. Identical to
//! the `MetaApiState::resolve_catalog` resolution, so the FE selector
//! (`GET /meta/domains`), the bead-create validation (H2), and these edits all read
//! and write one catalog.

use std::sync::Arc;

use axum::extract::{Path, State};
use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use axum::routing::get;
use axum::{Json, Router};
use serde::Deserialize;
use serde_json::{json, Value};

use gt_store_dolt::{
    validate_domain_key, AppError, DoltDomainCatalog, DoltIssues, DomainEntry, WorkspacePools,
};
use gt_workspace::WorkspaceContext;

/// Everything the domain-catalog REST handlers need, baked into the router with
/// [`Router::with_state`] before it leaves [`domain_router`].
#[derive(Clone)]
pub struct DomainApiState {
    /// The shared default-workspace store (`hq`), used single-tenant or when a call
    /// resolves to the default workspace.
    store: Arc<DoltIssues>,
    /// Per-workspace Dolt pools (`GT_DOLT_BASE_URL`). `Some` ⇒ a non-default active
    /// workspace edits its own `hq_<ws>` catalog; `None` ⇒ single-tenant.
    workspaces: Option<Arc<WorkspacePools>>,
}

impl DomainApiState {
    /// Build the REST state over the shared store. Single-tenant by default; call
    /// [`with_workspaces`](Self::with_workspaces) to enable per-request tenant routing.
    pub fn new(store: Arc<DoltIssues>) -> Self {
        Self { store, workspaces: None }
    }

    /// Enable multi-tenant routing: the request's verified workspace resolves its own
    /// `hq_<ws>` pool, so the catalog edits land in the active tenant.
    pub fn with_workspaces(mut self, pools: Arc<WorkspacePools>) -> Self {
        self.workspaces = Some(pools);
        self
    }

    /// Resolve the `domain_catalog` repo for one request's workspace, mirroring
    /// `MetaApiState::resolve_catalog` and the server's `resolve_store`.
    async fn resolve_catalog(&self, ctx: &WorkspaceContext) -> Result<DoltDomainCatalog, AppError> {
        match &self.workspaces {
            Some(pools) => {
                Ok(DoltDomainCatalog::new(pools.ensured_pool(ctx.workspace().as_str()).await?))
            }
            None => Ok(DoltDomainCatalog::new(self.store.pool().clone())),
        }
    }
}

/// Build the domain-catalog REST router with `state` baked in. The paths are
/// relative: the builder nests them under `/api/v1/domain` and applies the
/// `domain.read`/`domain.write` scope guard.
pub fn domain_router(state: DomainApiState) -> Router {
    Router::new()
        .route("/catalog", get(list).post(add))
        .route("/catalog/{key}", axum::routing::patch(patch).delete(remove))
        .with_state(state)
}

/// `GET /catalog` — the active workspace's domain catalog (`domain.catalog.list`):
/// every entry `{ key, label, tier, enabled, reserved }`. An un-seeded workspace
/// reads as an empty list rather than a `500`, the same degrade `GET /meta/domains`
/// uses. A pure read.
#[utoipa::path(
    get, path = "/catalog",
    responses(
        (status = 200, description = "The active workspace's domain catalog: [{ key, label, tier, enabled, reserved }]"),
        (status = 400, description = "No workspace selector resolved"),
    ),
)]
async fn list(
    State(st): State<DomainApiState>,
    ctx: WorkspaceContext,
) -> Result<Json<Value>, DomainApiError> {
    let catalog = st.resolve_catalog(&ctx).await?;
    let entries = catalog.list_opt().await?.unwrap_or_default();
    Ok(Json(json!({ "domains": entries.iter().map(entry_json).collect::<Vec<_>>() })))
}

/// `POST /catalog` — add a new domain (`domain.catalog.add`). Body `{ key, label?,
/// tier? }`; `label` defaults to the key, `tier` is optional. A malformed key, a
/// duplicate, or the reserved `meta.gap` is a `422`. `201` on success.
#[utoipa::path(
    post, path = "/catalog",
    request_body = CatalogAdd,
    responses(
        (status = 201, description = "Domain added; echoes the new entry"),
        (status = 422, description = "Invalid/duplicate key, or the reserved meta.gap"),
    ),
)]
async fn add(
    State(st): State<DomainApiState>,
    ctx: WorkspaceContext,
    Json(body): Json<CatalogAdd>,
) -> Result<Response, DomainApiError> {
    // Validate the key grammar before any I/O so a malformed value is a clean 422.
    let key = validate_domain_key(&body.key)?;
    let label = body
        .label
        .as_deref()
        .map(str::trim)
        .filter(|s| !s.is_empty())
        .unwrap_or(&key)
        .to_string();
    let tier = body.tier.as_deref().map(str::trim).filter(|s| !s.is_empty()).map(str::to_string);
    let entry = DomainEntry::new(key.clone(), label, tier);
    let catalog = st.resolve_catalog(&ctx).await?;
    catalog.add(&entry).await?;
    Ok((StatusCode::CREATED, Json(json!({ "ok": true, "key": key, "added": entry_json(&entry) })))
        .into_response())
}

/// `PATCH /catalog/{key}` — edit a domain (`domain.catalog.rename` /
/// `.set-enabled`). Body `{ label?, enabled? }`: a present `label` renames, a
/// present `enabled` toggles; at least one is required. A missing key is a `404`;
/// the reserved `meta.gap` is a `422`.
#[utoipa::path(
    patch, path = "/catalog/{key}",
    request_body = CatalogPatch,
    responses(
        (status = 200, description = "Domain updated"),
        (status = 404, description = "No such domain in this workspace"),
        (status = 422, description = "Empty patch, or the reserved meta.gap"),
    ),
)]
async fn patch(
    State(st): State<DomainApiState>,
    ctx: WorkspaceContext,
    Path(key): Path<String>,
    Json(body): Json<CatalogPatch>,
) -> Result<Json<Value>, DomainApiError> {
    if body.label.is_none() && body.enabled.is_none() {
        return Err(AppError::Validation(
            "patch must set at least one of `label` or `enabled`".to_string(),
        )
        .into());
    }
    let catalog = st.resolve_catalog(&ctx).await?;
    // Apply enabled first then label; each guards the reserved entry + missing key in
    // the store, so a reserved/absent key fails before the second mutation runs.
    if let Some(enabled) = body.enabled {
        catalog.set_enabled(&key, enabled).await?;
    }
    if let Some(label) = body.label.as_deref() {
        catalog.rename(&key, label).await?;
    }
    Ok(Json(json!({ "ok": true, "key": key })))
}

/// `DELETE /catalog/{key}` — remove a domain (`domain.catalog.remove`). A missing
/// key is a `404`; the reserved `meta.gap` is a `422`. `200` on success.
#[utoipa::path(
    delete, path = "/catalog/{key}",
    responses(
        (status = 200, description = "Domain removed"),
        (status = 404, description = "No such domain in this workspace"),
        (status = 422, description = "The reserved meta.gap may not be deleted"),
    ),
)]
async fn remove(
    State(st): State<DomainApiState>,
    ctx: WorkspaceContext,
    Path(key): Path<String>,
) -> Result<Json<Value>, DomainApiError> {
    let catalog = st.resolve_catalog(&ctx).await?;
    if !catalog.delete(&key).await? {
        return Err(AppError::NotFound(format!(
            "domain '{key}' not found in this workspace's catalog"
        ))
        .into());
    }
    Ok(Json(json!({ "ok": true, "key": key, "removed": true })))
}

/// `POST /catalog` body.
#[derive(Debug, Deserialize, utoipa::ToSchema)]
pub struct CatalogAdd {
    /// The stable domain key beads carry (e.g. `producto`, `orch.merge`).
    pub key: String,
    /// Optional display label; defaults to the key.
    #[serde(default)]
    pub label: Option<String>,
    /// Optional coarse tier (`None` for a flat business domain).
    #[serde(default)]
    pub tier: Option<String>,
}

/// `PATCH /catalog/{key}` body — a partial edit.
#[derive(Debug, Deserialize, utoipa::ToSchema)]
pub struct CatalogPatch {
    /// New display label (rename).
    #[serde(default)]
    pub label: Option<String>,
    /// New enabled state (set-enabled).
    #[serde(default)]
    pub enabled: Option<bool>,
}

/// Shape one catalog entry as the response payload (matches the MCP projection +
/// `GET /meta/domains` so every transport returns identical rows).
fn entry_json(e: &DomainEntry) -> Value {
    json!({
        "key": e.key,
        "label": e.label,
        "tier": e.tier,
        "enabled": e.enabled,
        "reserved": e.reserved,
    })
}

/// The combined OpenAPI document for the domain-catalog REST surface.
#[derive(utoipa::OpenApi)]
#[openapi(paths(list, add, patch, remove), components(schemas(CatalogAdd, CatalogPatch)))]
pub struct ApiDoc;

/// HTTP wrapper over the domain [`AppError`] so a handler can `?`-propagate it and
/// have it rendered with the right status — the same status map the `meta.*` adapter
/// uses, so a client sees identical reasons across transports.
struct DomainApiError(AppError);

impl From<AppError> for DomainApiError {
    fn from(e: AppError) -> Self {
        DomainApiError(e)
    }
}

impl IntoResponse for DomainApiError {
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
        let doc = ApiDoc::openapi();
        let paths: Vec<&str> = doc.paths.paths.keys().map(String::as_str).collect();
        for expected in ["/catalog", "/catalog/{key}"] {
            assert!(paths.contains(&expected), "missing {expected} in {paths:?}");
        }
        assert!(paths.iter().all(|p| !p.contains("/api/v1")), "{paths:?}");
    }

    #[test]
    fn error_status_mapping_matches_app_error_kinds() {
        let cases = [
            (AppError::NotFound("x".into()), StatusCode::NOT_FOUND),
            (AppError::Validation("x".into()), StatusCode::UNPROCESSABLE_ENTITY),
            (AppError::Other("x".into()), StatusCode::INTERNAL_SERVER_ERROR),
        ];
        for (err, want) in cases {
            assert_eq!(DomainApiError(err).into_response().status(), want);
        }
    }
}
