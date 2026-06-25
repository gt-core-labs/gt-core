//! The `axum` REST adapter for the `meta.*` surface (hq-fe-api-platform.4).
//!
//! Exposes the two cross-cutting meta operations as REST routes that dispatch to
//! the **same** logic the MCP tools run — no domain logic is duplicated, only the
//! wire shape differs. It folds into `gt-meta` behind the off-by-default `axum`
//! feature (docs/03 Rule 4), exactly as gt-issues/gt-workspace gate their
//! adapters, rather than living in a sibling crate.
//!
//! | Method + path        | Maps to MCP tool          | Verb-class |
//! |----------------------|---------------------------|------------|
//! | `GET /help`          | `meta.help.execute`       | read       |
//! | `POST /report-gap`   | `meta.report-gap.execute` | write      |
//!
//! ## What it does *not* do
//!
//! - **It does not authenticate or authorize.** The builder mounts this router
//!   under `/api/v1/meta` and wraps it with the capability-derived scope guard
//!   (`meta.read` for `GET`, `meta.write` for `POST`); the composition root layers
//!   the auth middleware in front.
//! - **It never reads a workspace from the path or body** (docs/04 §15, docs/03
//!   Rule 6). `report-gap` carries no resource id at all; the attribution actor is
//!   the server identity baked into [`MetaApiState`], never the request body.
//!
//! ## `meta.help` and the tool list
//!
//! `meta.help` returns the server's full `tools/list` payload — a snapshot of every
//! dispatchable tool. That set is only known once the builder has harvested every
//! module, so the binary captures it after the build and bakes it into
//! [`MetaApiState`]; the handler just serializes the carried descriptors. The
//! descriptors are MCP-canonical (`{ name, description, inputSchema }`), identical
//! to what the MCP `meta.help.execute` returns.

use std::sync::Arc;

use axum::extract::State;
use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use axum::routing::{get, post};
use axum::{Json, Router};
use serde_json::{json, Value};

use gt_module::McpTool;
use gt_store_dolt::{AppError, DoltDomainCatalog, DoltIssues, WorkspacePools};
use gt_workspace::WorkspaceContext;

use crate::commands::ReportGap;
use crate::handlers::run_report_gap;

/// Everything the meta REST handlers need, baked into the router with
/// [`Router::with_state`] before it leaves [`meta_router`] so the merged
/// application router carries no outstanding state type (the kernel's state-erased
/// `Router<()>` contract).
#[derive(Clone)]
pub struct MetaApiState {
    /// The live Dolt issues store `report-gap` mints the gap bead into — the same
    /// store the issues surface writes to. The module owns no store of its own.
    store: Arc<DoltIssues>,
    /// The attribution actor stamped as the gap's `created_by`, server-injected the
    /// same way the MCP path uses `GT_MCP_ACTOR`. It is the server identity, not the
    /// per-request caller, so a request body can never spoof it.
    actor: Arc<str>,
    /// The server's full tool-descriptor set, captured after the builder harvested
    /// every module. `meta.help` serializes it verbatim; it is shared (`Arc`) so the
    /// state stays cheap to clone per request.
    tools: Arc<[McpTool]>,
    /// Per-workspace Dolt pool cache, present only when the binary runs multi-tenant
    /// (gtcore-d81e77 H2). When `Some`, `GET /domains` resolves the request's tenant
    /// `hq_<ws>` pool from the verified [`WorkspaceContext`] so the FE domain selector
    /// is populated from the ACTIVE workspace's `domain_catalog`, not a global list.
    /// When `None`, the catalog is read from the single fallback `store` pool — the
    /// pre-routing single-tenant behaviour.
    workspaces: Option<Arc<WorkspacePools>>,
}

impl MetaApiState {
    /// Build the REST state over a live store, the server attribution actor, and the
    /// harvested tool-descriptor set (for `meta.help`). Single-tenant by default; call
    /// [`with_workspaces`](Self::with_workspaces) to enable per-request tenant routing
    /// for the `GET /domains` catalog.
    pub fn new(
        store: Arc<DoltIssues>,
        actor: impl Into<Arc<str>>,
        tools: impl Into<Arc<[McpTool]>>,
    ) -> Self {
        Self { store, actor: actor.into(), tools: tools.into(), workspaces: None }
    }

    /// Enable multi-tenant routing for `GET /domains` (gtcore-d81e77 H2): the request's
    /// verified workspace resolves its own `hq_<ws>` pool from the shared cache, so the
    /// domain catalog reflects the active tenant. Without this the catalog endpoint reads
    /// the single fallback `store` for every caller (single-tenant).
    pub fn with_workspaces(mut self, pools: Arc<WorkspacePools>) -> Self {
        self.workspaces = Some(pools);
        self
    }

    /// Resolve the `domain_catalog` repo for one request's workspace. In multi-tenant
    /// mode the tenant's `hq_<ws>` pool is selected (the slug is already validated by the
    /// [`WorkspaceContext`] extractor) and its schema ensured on first access; single-tenant
    /// falls back to the shared `store` pool, ignoring the workspace exactly as before.
    async fn resolve_catalog(
        &self,
        ctx: &WorkspaceContext,
    ) -> Result<DoltDomainCatalog, AppError> {
        match &self.workspaces {
            Some(pools) => {
                Ok(DoltDomainCatalog::new(pools.ensured_pool(ctx.workspace().as_str()).await?))
            }
            None => Ok(DoltDomainCatalog::new(self.store.pool().clone())),
        }
    }
}

/// Build the meta REST router with `state` baked in (hq-fe-api-platform.4).
///
/// The paths are **relative**: the builder nests them under `/api/v1/meta` and
/// applies the scope guard. `register_routes` on the HTTP-bearing module returns
/// exactly this router.
pub fn meta_router(state: MetaApiState) -> Router {
    Router::new()
        .route("/help", get(help))
        .route("/scopes", get(scopes))
        .route("/domains", get(domains))
        .route("/report-gap", post(report_gap))
        .with_state(state)
}

/// `GET /help` — the server's `tools/list` payload (`meta.help.execute`): every
/// dispatchable tool with its description + input schema. A pure read, no state
/// change. The shape matches the MCP `meta.help` result exactly.
#[cfg_attr(feature = "axum", utoipa::path(
    get, path = "/help",
    responses((status = 200, description = "The full tools/list (names, descriptions, input schemas)")),
))]
async fn help(State(st): State<MetaApiState>) -> Json<Value> {
    Json(json!({ "tools": st.tools.as_ref() }))
}

/// `GET /scopes` — the **grantable scope catalog** (`hq-scope-catalog`): every `<namespace>.<verb>`
/// scope a token may be granted, derived from the closed `gt_rbac::SCOPE_VERBS` vocabulary crossed
/// with the namespaces the server advertises. The namespaces come from the SAME carried tool
/// descriptors `meta.help` serves (`name.split('.').next()`), so a newly-registered MCP namespace
/// (e.g. `memory`) appears here with no code change — the token-permission UI reads `{ scope, label,
/// namespace }` instead of hardcoding a `SCOPE_LABELS` map. A pure read, no state change.
#[cfg_attr(feature = "axum", utoipa::path(
    get, path = "/scopes",
    responses((status = 200, description = "The grantable scope catalog: [{ scope, label, namespace }] over SCOPE_VERBS × registered namespaces")),
))]
async fn scopes(State(st): State<MetaApiState>) -> Json<Value> {
    // Namespace = the resource segment of each advertised tool name (the part before the first `.`).
    // A name with no `.` (e.g. the unauthenticated `ping`) carries no namespace and is skipped.
    let namespaces = st.tools.iter().filter_map(|t| {
        let ns = t.name.split('.').next().unwrap_or("");
        (t.name.contains('.') && !ns.is_empty()).then_some(ns)
    });
    // gt_rbac sorts + de-duplicates the namespaces and crosses them with the closed verb set.
    let catalog = gt_rbac::grantable_scopes(namespaces);
    Json(json!({ "scopes": catalog }))
}

/// `GET /domains` — the ACTIVE workspace's domain catalog (gtcore-d81e77 H2): every
/// `domain_catalog` entry for the request's tenant `{ key, label, tier, enabled,
/// reserved }`, so the FE domain selector (gtweb-186fbf) is populated per workspace
/// instead of from the global `Domain` enum. The workspace is resolved through the
/// verified [`WorkspaceContext`] — the same tenant boundary the issues surface uses —
/// never a query/body field. A pure read, no state change.
///
/// An un-seeded workspace (no `domain_catalog` table — a tenant provisioned before H1,
/// pending the H3 backfill) returns an empty list rather than a `500`, so the FE
/// degrades to its free-text fallback exactly as when the catalog is absent.
#[cfg_attr(feature = "axum", utoipa::path(
    get, path = "/domains",
    responses(
        (status = 200, description = "The active workspace's domain catalog: [{ key, label, tier, enabled, reserved }]"),
        (status = 400, description = "No workspace selector resolved (missing/invalid X-GT-Workspace + token claim)"),
    ),
))]
async fn domains(
    State(st): State<MetaApiState>,
    ctx: WorkspaceContext,
) -> Result<Json<Value>, ApiError> {
    let catalog = st.resolve_catalog(&ctx).await?;
    // `list_opt` is table-absent tolerant: `None` (un-seeded workspace) reads as an
    // empty catalog so the read never 500s on a pre-H3 tenant.
    let entries = catalog.list_opt().await?.unwrap_or_default();
    Ok(Json(json!({ "domains": entries })))
}

/// `POST /report-gap` — surface a missing operation (`meta.report-gap.execute`):
/// the server mints a `hq-gap-<slug>-<ts>` bead so the gap enters the routine
/// catalog. The attribution actor is the server identity in [`MetaApiState`], never
/// the body. `201` on success, echoing the minted bead summary.
#[cfg_attr(feature = "axum", utoipa::path(
    post, path = "/report-gap",
    responses(
        (status = 201, description = "Gap bead minted; echoes { bead, operation, priority, external_ref }"),
        (status = 422, description = "Validation failed (e.g. empty operation)"),
    ),
))]
async fn report_gap(
    State(st): State<MetaApiState>,
    Json(args): Json<ReportGap>,
) -> Result<Response, ApiError> {
    let body = run_report_gap(&st.store, &args, &st.actor).await?;
    Ok((StatusCode::CREATED, Json(body)).into_response())
}

/// The combined OpenAPI document for the meta REST surface (hq-fe-api-platform.4).
/// The builder mounts it under the module prefix and rewrites its relative paths to
/// `/api/v1/meta/...`, so the `#[utoipa::path]` annotations stay prefix-free.
#[derive(utoipa::OpenApi)]
#[openapi(paths(help, scopes, domains, report_gap))]
pub struct ApiDoc;

/// HTTP wrapper over the domain [`AppError`] so a handler can `?`-propagate it and
/// have it rendered with the right status. The body is the bare error message — the
/// same text the MCP path surfaces — so a client sees an identical reason across
/// transports.
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
        // The spec the builder mounts: paths are relative (no `/api/v1/meta`), so
        // nesting can rewrite them. Every declared route must be present.
        let doc = ApiDoc::openapi();
        let paths: Vec<&str> = doc.paths.paths.keys().map(String::as_str).collect();
        for expected in ["/help", "/scopes", "/domains", "/report-gap"] {
            assert!(paths.contains(&expected), "missing {expected} in {paths:?}");
        }
        // Prefix-free: the module builder, not the annotation, owns `/api/v1/meta`.
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
}
