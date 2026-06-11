//! The `axum` REST adapter for the cross-workspace `me.*` surface (`hq-web-extras.15`).
//!
//! `gt-web.5`'s statistics view models the user as the source of everything:
//! `Usuario -> Workspace[] -> Rig[]`. The per-workspace `GET /api/v1/issues/stats`
//! (`hq-web-extras.12`) only ever aggregates the *active* workspace; this surface complements it
//! by rolling up **every** workspace the caller is a member of at once, without switching tenant
//! by tenant. It depends on the global N:N identity (`hq-identity.3`): the caller's memberships
//! come from `public.user_workspaces`, and each workspace's beads from that tenant's own tracker.
//!
//! ## What it does *not* do
//!
//! - **It does not authenticate.** The builder mounts this router under `/api/v1/me` and wraps it
//!   with the `issues.read` scope guard; the composition auth middleware verifies the bearer and
//!   injects [`JwtClaims`](gt_auth::JwtClaims) before a handler here runs. The handler reads the
//!   caller's `sub` from those verified claims — never from the path or body.
//! - **It authorizes by membership.** The aggregation only ever covers workspaces the caller is a
//!   member of (the [`MeStatsSource`] resolves them by `sub`), so a caller can never see a tenant
//!   they do not belong to. There is no MCP tool sibling — it is a REST-only read surface.
//!
//! The aggregation math is the transport-free [`crate::stats::me_stats`]; this file only fetches
//! the per-workspace rows through the injected [`MeStatsSource`] port and folds them.

use std::sync::Arc;

use axum::extract::State;
use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use axum::routing::get;
use axum::{Extension, Json, Router};

use gt_auth::JwtClaims;
use gt_store_dolt::{AppError, IssueRow};

use crate::stats::{me_stats, MeStatsResponse};

/// One workspace the caller is a member of, as resolved by the [`MeStatsSource`]: the slug and the
/// role the caller holds there. The slug selects that tenant's tracker; the role is echoed into
/// the per-workspace stats bucket so the FE can render it next to the counts.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MeMembership {
    /// The workspace slug the caller belongs to.
    pub workspace: String,
    /// The role the caller holds in that workspace.
    pub role: String,
}

/// The composition-tier port behind `GET /api/v1/me/stats` (`hq-web-extras.15`): resolve the
/// caller's workspace memberships, and fetch one workspace's tracker rows.
///
/// The module owns no store: only the composition root knows both the global identity directory
/// (`public.user_workspaces`) and the per-workspace issue stores, so it supplies the adapter
/// (docs/03 Rule 4). [`memberships`](Self::memberships) is keyed by the verified `sub`, so a
/// caller only ever resolves their own tenants; [`issue_rows`](Self::issue_rows) reads one
/// tenant's beads, exactly the snapshot rows [`crate::stats`] folds.
#[async_trait::async_trait]
pub trait MeStatsSource: Send + Sync {
    /// The workspaces the caller (`sub`) is a member of. Empty ⇒ a global identity with no tenant
    /// yet (the response then carries no workspaces and a zero grand total, never an error).
    async fn memberships(&self, sub: &str) -> Result<Vec<MeMembership>, AppError>;

    /// The full tracker snapshot rows for one `workspace`, the same cheap rows the per-workspace
    /// stats endpoint folds. The slug is one the caller is a member of (resolved above), never an
    /// untrusted path segment.
    async fn issue_rows(&self, workspace: &str) -> Result<Vec<IssueRow>, AppError>;
}

/// Everything the `me.*` REST handler needs, baked into the router before it leaves
/// [`me_router`] so the merged application router carries no outstanding state type.
#[derive(Clone)]
pub struct MeApiState {
    /// The composition-supplied cross-workspace stats source.
    source: Arc<dyn MeStatsSource>,
}

impl MeApiState {
    /// Build the REST state over a cross-workspace stats source.
    pub fn new(source: Arc<dyn MeStatsSource>) -> Self {
        Self { source }
    }
}

/// Build the `me.*` REST router with `state` baked in (`hq-web-extras.15`).
///
/// The path is **relative**: the builder nests it under `/api/v1/me` and applies the `issues.read`
/// scope guard.
///
/// | Method + path | Maps to MCP tool |
/// |---------------|------------------|
/// | `GET /stats`  | none (REST-only cross-workspace aggregation) |
pub fn me_router(state: MeApiState) -> Router {
    Router::new()
        .route("/stats", get(me_stats_handler))
        .with_state(state)
}

/// `GET /stats` — the cross-workspace statistics tree for the authenticated caller
/// (`gt-web.5`, `hq-web-extras.15`).
///
/// Resolves the caller's workspace memberships (`public.user_workspaces`, keyed by the verified
/// `sub`) and, for each, folds that tenant's tracker into counts `{open,working,closed,other,total}`
/// + `progress_pct` + `lead_time` + a per-rig breakdown — the same bucket math as the per-workspace
/// `GET /issues/stats` — plus a `grand_total` summed across every membership. Scope: `issues.read`.
///
/// `401` when the bearer carries no verified claims (no `sub`); a caller with no memberships gets
/// `200` with an empty `workspaces` list and a zero `grand_total`.
#[cfg_attr(feature = "axum", utoipa::path(
    get, path = "/stats",
    responses(
        (status = 200, description = "Per-workspace counts + progress% + lead_time + per-rig breakdown, plus a grand total across every workspace the caller belongs to", body = MeStatsResponse),
        (status = 401, description = "No verified identity (missing bearer claims)"),
    ),
))]
async fn me_stats_handler(
    State(st): State<MeApiState>,
    claims: Option<Extension<JwtClaims>>,
) -> Result<Json<MeStatsResponse>, ApiError> {
    let Extension(claims) = claims.ok_or(ApiError::Unauthenticated)?;
    let memberships = st.source.memberships(&claims.sub).await?;
    let mut per_workspace = Vec::with_capacity(memberships.len());
    for m in memberships {
        let rows = st.source.issue_rows(&m.workspace).await?;
        per_workspace.push((m.workspace, m.role, rows));
    }
    Ok(Json(me_stats(per_workspace)))
}

/// The combined OpenAPI document for the `me.*` REST surface (`hq-web-extras.15`). The builder
/// mounts it under `/api/v1/me` and rewrites the relative paths, so the `#[utoipa::path]`
/// annotation stays prefix-free.
#[derive(utoipa::OpenApi)]
#[openapi(
    paths(me_stats_handler),
    components(schemas(
        MeStatsResponse,
        crate::stats::WorkspaceStats,
        crate::stats::StatBucket,
        crate::stats::LeadTime,
    ))
)]
pub struct ApiDoc;

/// HTTP wrapper over the domain [`AppError`] (plus the unauthenticated case) so a handler can
/// `?`-propagate and have it rendered with the right status.
enum ApiError {
    /// No verified bearer claims — the caller has no `sub` to resolve memberships for.
    Unauthenticated,
    /// A store/domain error from the cross-workspace source.
    Store(AppError),
}

impl From<AppError> for ApiError {
    fn from(e: AppError) -> Self {
        ApiError::Store(e)
    }
}

impl IntoResponse for ApiError {
    fn into_response(self) -> Response {
        match self {
            ApiError::Unauthenticated => {
                (StatusCode::UNAUTHORIZED, "missing or invalid bearer token").into_response()
            }
            ApiError::Store(e) => {
                let status = match &e {
                    AppError::NotFound(_) => StatusCode::NOT_FOUND,
                    AppError::Validation(_) => StatusCode::UNPROCESSABLE_ENTITY,
                    AppError::InvalidTransition(_) => StatusCode::CONFLICT,
                    AppError::Handler(_) | AppError::Other(_) => StatusCode::INTERNAL_SERVER_ERROR,
                };
                (status, e.to_string()).into_response()
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::body::Body;
    use axum::http::Request;
    use tower::ServiceExt;
    use utoipa::OpenApi;

    fn row(id: &str, status: &str) -> IssueRow {
        IssueRow {
            id: id.to_string(),
            title: "t".into(),
            status: status.into(),
            priority: 1,
            issue_type: "task".into(),
            assignee: None,
            owner: None,
            created_at: None,
            updated_at: None,
            closed_at: None,
            external_ref: None,
            spec_id: None,
            domain_json: "[]".into(),
            surface_json: "[]".into(),
            depends_on_json: "[]".into(),
            role_scope: None,
            version: 0,
            phase: "P1".into(),
            delivered_sha: None,
            rig: String::new(),
            workspace: "default".into(),
            board_rank: String::new(),
            estimated_hours: None,
            start_date: None,
            due_date: None,
            description: None,
            design: None,
            acceptance_criteria: None,
            notes: None,
        }
    }

    /// A stub source: a fixed membership list + per-workspace rows.
    struct StubSource {
        memberships: Vec<MeMembership>,
        rows: std::collections::HashMap<String, Vec<IssueRow>>,
    }

    #[async_trait::async_trait]
    impl MeStatsSource for StubSource {
        async fn memberships(&self, _sub: &str) -> Result<Vec<MeMembership>, AppError> {
            Ok(self.memberships.clone())
        }
        async fn issue_rows(&self, workspace: &str) -> Result<Vec<IssueRow>, AppError> {
            Ok(self.rows.get(workspace).cloned().unwrap_or_default())
        }
    }

    fn claims(sub: &str) -> JwtClaims {
        JwtClaims {
            sub: sub.into(),
            workspace: "acme".into(),
            scopes: vec!["issues.read".into()],
            exp: u64::MAX,
            nbf: None,
            iat: 0,
        }
    }

    fn app(source: StubSource) -> Router {
        me_router(MeApiState::new(Arc::new(source)))
    }

    #[tokio::test]
    async fn stats_rolls_up_every_membership_with_a_grand_total() {
        let mut rows = std::collections::HashMap::new();
        rows.insert(
            "acme".into(),
            vec![row("hq-a.1", "open"), row("hq-a.2", "closed")],
        );
        rows.insert("idb".into(), vec![row("tobx-b.1", "closed")]);
        let source = StubSource {
            memberships: vec![
                MeMembership {
                    workspace: "acme".into(),
                    role: "admin".into(),
                },
                MeMembership {
                    workspace: "idb".into(),
                    role: "member".into(),
                },
            ],
            rows,
        };
        let mut req = Request::builder()
            .method("GET")
            .uri("/stats")
            .body(Body::empty())
            .unwrap();
        req.extensions_mut().insert(claims("user-1"));
        let resp = app(source).oneshot(req).await.unwrap();
        assert_eq!(resp.status(), StatusCode::OK);
        let body = axum::body::to_bytes(resp.into_body(), usize::MAX)
            .await
            .unwrap();
        // The response types are serialize-only (shared with the per-workspace stats), so assert
        // over the JSON value rather than a typed deserialize.
        let v: serde_json::Value = serde_json::from_slice(&body).unwrap();
        let ws = v["workspaces"].as_array().unwrap();
        assert_eq!(ws.len(), 2);
        assert_eq!(ws[0]["workspace"], "acme");
        assert_eq!(ws[0]["role"], "admin");
        assert_eq!(ws[0]["totals"]["total"], 2);
        assert_eq!(ws[1]["workspace"], "idb");
        // Grand total folds all three beads, two closed.
        assert_eq!(v["grand_total"]["total"], 3);
        assert_eq!(v["grand_total"]["closed"], 2);
    }

    #[tokio::test]
    async fn stats_without_claims_is_unauthorized() {
        let source = StubSource {
            memberships: vec![],
            rows: Default::default(),
        };
        let req = Request::builder()
            .method("GET")
            .uri("/stats")
            .body(Body::empty())
            .unwrap();
        let resp = app(source).oneshot(req).await.unwrap();
        assert_eq!(resp.status(), StatusCode::UNAUTHORIZED);
    }

    #[tokio::test]
    async fn no_memberships_yields_empty_workspaces_and_zero_grand_total() {
        let source = StubSource {
            memberships: vec![],
            rows: Default::default(),
        };
        let mut req = Request::builder()
            .method("GET")
            .uri("/stats")
            .body(Body::empty())
            .unwrap();
        req.extensions_mut().insert(claims("lonely"));
        let resp = app(source).oneshot(req).await.unwrap();
        assert_eq!(resp.status(), StatusCode::OK);
        let body = axum::body::to_bytes(resp.into_body(), usize::MAX)
            .await
            .unwrap();
        let v: serde_json::Value = serde_json::from_slice(&body).unwrap();
        assert!(v["workspaces"].as_array().unwrap().is_empty());
        assert_eq!(v["grand_total"]["total"], 0);
    }

    #[test]
    fn openapi_lists_the_relative_stats_route_prefix_free() {
        let doc = ApiDoc::openapi();
        let paths: Vec<&str> = doc.paths.paths.keys().map(String::as_str).collect();
        assert!(paths.contains(&"/stats"), "missing /stats in {paths:?}");
        assert!(paths.iter().all(|p| !p.contains("/api/v1")), "{paths:?}");
    }
}
