//! The `axum` REST adapter for the read-only `skills.*` surface (`hq-web-extras.13`).
//!
//! Exposes the operator-tunable capability registry as REST routes that surface the **same**
//! [`SkillCatalog`](crate::SkillCatalog) the MCP/event-sourced catalog folds — no domain logic is
//! duplicated, only the wire shape differs. It folds into `gt-skills` behind the off-by-default
//! `axum` feature (docs/03 Rule 4), exactly as gt-rig/gt-workspace gate theirs, rather than living
//! in a sibling crate.
//!
//! ## Read-only, per-workspace
//!
//! Skills are the read side the dashboard hydrates from: `list` every registered skill, and `get`
//! one by id. Toggling a skill (the per-role `write` path) stays an MCP/`skills.write` concern, so
//! it is intentionally absent from this surface — every route is a GET ⇒ `skills.read`.
//!
//! The catalog is **per-tenant**: each workspace owns its own `skills.*` event stream. The tenant
//! comes from the [`WorkspaceContext`](gt_workspace::WorkspaceContext) extractor (the JWT claim /
//! sanctioned header), **never** from the URL or body (docs/03 Rule 6, docs/04 §15). The adapter
//! holds a [`WorkspaceSkills`] provider that hands back a workspace-scoped catalog snapshot per
//! request — the REST mirror of the MCP handler's per-workspace replay.
//!
//! ## What it does *not* do
//!
//! - **It does not authenticate or authorize.** The builder mounts this router under
//!   `/api/v1/skills` and wraps it with the capability-derived scope guard; the composition root
//!   layers the auth middleware in front. Every route is a GET, so the guard requires
//!   `skills.read`.

use std::sync::Arc;

use async_trait::async_trait;
use axum::extract::{Path, State};
use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use axum::routing::get;
use axum::{Json, Router};
use serde_json::{json, Value};

use gt_events::AppError;
use gt_workspace::WorkspaceContext;

use crate::state::{Skill, SkillCatalog};

/// A per-workspace skills-catalog provider for the REST adapter.
///
/// A skills catalog is tenant-local, so a request must run against the caller's own `skills.*`
/// stream. The binary supplies an implementation that, given the resolved workspace slug, hands
/// back the catalog snapshot for that tenant (the composition path replays the tenant's event
/// log); a test supplies an in-memory one. This is the read-only mirror of the MCP handler's
/// per-workspace replay — the composition root owns the store policy, the adapter only asks for
/// "the catalog for *this* workspace".
#[async_trait]
pub trait WorkspaceSkills: Send + Sync {
    /// The skills catalog snapshot for `workspace` (the slug from the auth context, never the path).
    async fn catalog(&self, workspace: &str) -> Result<SkillCatalog, AppError>;
}

/// Everything the skills REST handlers need, baked into the router with [`Router::with_state`]
/// before it leaves [`skills_router`] so the merged application router carries no outstanding
/// state type (the kernel's state-erased `Router<()>` contract).
#[derive(Clone)]
pub struct SkillsApiState {
    /// The per-workspace catalog provider the binary supplies — the module owns no store of its
    /// own, exactly as the MCP handler owns only its replay path.
    skills: Arc<dyn WorkspaceSkills>,
}

impl SkillsApiState {
    /// Build the REST state over a per-workspace catalog provider.
    pub fn new(skills: Arc<dyn WorkspaceSkills>) -> Self {
        Self { skills }
    }
}

/// Build the read-only skills REST router with `state` baked in (`hq-web-extras.13`).
///
/// The paths are **relative**: the builder nests them under `/api/v1/skills` and applies the scope
/// guard. `register_routes` on the HTTP-enabled skills module returns exactly this router.
///
/// | Method + path | Surfaces            |
/// |---------------|---------------------|
/// | `GET /`       | the whole catalog   |
/// | `GET /:id`    | one skill by id     |
pub fn skills_router(state: SkillsApiState) -> Router {
    Router::new()
        .route("/", get(list_skills))
        .route("/:id", get(get_skill))
        .with_state(state)
}

/// `GET /` — every registered skill in the caller's workspace, in sorted order, with the per-role
/// bindings the dashboard hydrates from.
#[cfg_attr(feature = "axum", utoipa::path(
    get, path = "/",
    responses((status = 200, description = "Every registered skill + per-role bindings for the caller's workspace")),
))]
async fn list_skills(
    State(st): State<SkillsApiState>,
    ctx: WorkspaceContext,
) -> Result<Json<Value>, ApiError> {
    let catalog = st.skills.catalog(ctx.workspace().as_str()).await?;
    let skills = catalog.skills();
    Ok(Json(json!({
        "count": skills.len(),
        "skills": skills,
        "bindings": catalog.bindings(),
    })))
}

/// `GET /:id` — one skill by id (`404` when the workspace has no skill registered under that id).
#[cfg_attr(feature = "axum", utoipa::path(
    get, path = "/{id}",
    params(("id" = String, Path, description = "Skill id")),
    responses(
        (status = 200, description = "The skill + the roles that have it enabled"),
        (status = 404, description = "No skill registered under that id"),
    ),
))]
async fn get_skill(
    State(st): State<SkillsApiState>,
    ctx: WorkspaceContext,
    Path(id): Path<String>,
) -> Result<Json<Value>, ApiError> {
    let catalog = st.skills.catalog(ctx.workspace().as_str()).await?;
    let skill: Skill = catalog
        .get(&id)
        .cloned()
        .ok_or_else(|| ApiError::NotFound(format!("no skill registered under id `{id}`")))?;
    // The roles that currently have this skill enabled — the inverse view the role editor reads.
    let roles: Vec<String> = catalog
        .bindings()
        .into_iter()
        .filter(|b| b.enabled_skills.contains(&id))
        .map(|b| b.role)
        .collect();
    Ok(Json(json!({ "skill": skill, "enabled_for_roles": roles })))
}

/// The combined OpenAPI document for the read-only skills REST surface (`hq-web-extras.13`). The
/// builder mounts it under the module prefix and rewrites its relative paths to
/// `/api/v1/skills/...`, so the `#[utoipa::path]` annotations stay prefix-free.
#[derive(utoipa::OpenApi)]
#[openapi(paths(list_skills, get_skill))]
pub struct ApiDoc;

/// HTTP wrapper over the skills errors so a handler can `?`-propagate them with the right status:
/// an unknown skill is a client-addressable `404`; a store/replay failure is a server `500`.
#[derive(Debug)]
enum ApiError {
    /// No skill exists under the requested id in the resolved workspace.
    NotFound(String),
    /// The catalog provider reported a failure.
    Catalog(AppError),
}

impl From<AppError> for ApiError {
    fn from(e: AppError) -> Self {
        ApiError::Catalog(e)
    }
}

impl IntoResponse for ApiError {
    fn into_response(self) -> Response {
        let (status, body) = match self {
            ApiError::NotFound(msg) => (StatusCode::NOT_FOUND, msg),
            // Every catalog failure is a server-side replay/store fault.
            ApiError::Catalog(e) => (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()),
        };
        (status, body).into_response()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::events::SkillEvent;
    use crate::state::SkillState;
    use utoipa::OpenApi;

    fn seeded() -> SkillCatalog {
        let mut s = SkillState::default();
        for e in [
            SkillEvent::Registered {
                skill: "merge_admin".into(),
                label: "Merge admin".into(),
                description: "Approve merges".into(),
                default_scopes: vec!["merge.read".into(), "merge.write".into()],
                now_secs: 1,
            },
            SkillEvent::Registered {
                skill: "feed_viewer".into(),
                label: "Feed viewer".into(),
                description: "Read the feed".into(),
                default_scopes: vec!["feed.read".into()],
                now_secs: 2,
            },
            SkillEvent::EnabledForRole { role: "deacon".into(), skill: "merge_admin".into(), now_secs: 3 },
        ] {
            s.apply(&e);
        }
        s.catalog
    }

    #[test]
    fn openapi_lists_every_relative_route_prefix_free() {
        let doc = ApiDoc::openapi();
        let paths: Vec<&str> = doc.paths.paths.keys().map(String::as_str).collect();
        for expected in ["/", "/{id}"] {
            assert!(paths.contains(&expected), "missing {expected} in {paths:?}");
        }
        assert!(paths.iter().all(|p| !p.contains("/api/v1")), "{paths:?}");
    }

    #[test]
    fn catalog_lists_sorted_skills() {
        let cat = seeded();
        let skills = cat.skills();
        assert_eq!(skills.len(), 2);
        // BTreeMap order: feed_viewer before merge_admin.
        assert_eq!(skills[0].id, "feed_viewer");
        assert_eq!(skills[1].id, "merge_admin");
    }

    #[test]
    fn get_resolves_a_skill_and_its_enabled_roles() {
        let cat = seeded();
        let skill = cat.get("merge_admin").cloned().unwrap();
        assert_eq!(skill.default_scopes, vec!["merge.read", "merge.write"]);
        let roles: Vec<String> = cat
            .bindings()
            .into_iter()
            .filter(|b| b.enabled_skills.contains("merge_admin"))
            .map(|b| b.role)
            .collect();
        assert_eq!(roles, vec!["deacon"]);
        // An unknown skill is absent (the handler maps this to a 404).
        assert!(cat.get("ghost").is_none());
    }

    #[test]
    fn error_status_mapping() {
        assert_eq!(
            ApiError::NotFound("x".into()).into_response().status(),
            StatusCode::NOT_FOUND
        );
        assert_eq!(
            ApiError::Catalog(AppError::Other("boom".into())).into_response().status(),
            StatusCode::INTERNAL_SERVER_ERROR
        );
    }
}
