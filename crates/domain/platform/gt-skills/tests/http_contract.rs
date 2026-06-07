//! Contract for the read-only skills REST adapter (`hq-web-extras.13`).
//!
//! Drives [`gt_skills::skills_router`] in-process with `tower`'s `oneshot` against an
//! **in-memory** per-workspace catalog provider, proving the REST surface surfaces the **same**
//! [`SkillCatalog`](gt_skills::SkillCatalog) the event-sourced catalog folds: `GET /` lists every
//! registered skill, `GET /:id` resolves one (and its enabled roles), a missing skill is a `404`,
//! and — critically — the catalog is per-tenant, so a skill in workspace `a` is invisible to
//! workspace `b`. The wire mapping (error → status) is unit-covered in `http.rs`; this is the
//! integration proof that the routes are wired to a workspace-scoped read.
//!
//! Compiled only under the `axum` feature (the adapter's gate). It needs no database: the provider
//! hands each workspace its own in-memory catalog, so the contract runs on host CI with no
//! Postgres sidecar.
#![cfg(feature = "axum")]

use std::collections::HashMap;
use std::sync::Arc;

use async_trait::async_trait;
use axum::body::Body;
use axum::http::{Request, StatusCode};
use serde_json::Value;
use tower::ServiceExt; // oneshot

use gt_events::AppError;
use gt_skills::{
    skills_router, SkillCatalog, SkillEvent, SkillState, SkillWriter, SkillsApiState,
    WorkspaceSkills,
};
use gt_workspace::WORKSPACE_HEADER;
use std::sync::Mutex;

/// An in-memory [`WorkspaceSkills`] provider: a fixed catalog per workspace slug, so two tenants
/// get genuinely separate catalogs (the REST mirror of stream-per-workspace).
struct MemProvider {
    by_ws: HashMap<String, SkillCatalog>,
}

impl MemProvider {
    fn new() -> Self {
        Self { by_ws: HashMap::new() }
    }

    /// Seed `workspace` with a catalog rebuilt from `events`.
    fn seed(mut self, workspace: &str, events: &[SkillEvent]) -> Self {
        let mut s = SkillState::default();
        for e in events {
            s.apply(e);
        }
        self.by_ws.insert(workspace.to_string(), s.catalog);
        self
    }
}

#[async_trait]
impl WorkspaceSkills for MemProvider {
    async fn catalog(&self, workspace: &str) -> Result<SkillCatalog, AppError> {
        // An unknown workspace yields an empty catalog (the REST list is then `count: 0`).
        Ok(self.by_ws.get(workspace).cloned().unwrap_or_default())
    }
}

fn skill(id: &str, scopes: &[&str]) -> SkillEvent {
    SkillEvent::Registered {
        skill: id.into(),
        label: format!("{id} label"),
        description: "test".into(),
        default_scopes: scopes.iter().map(|s| s.to_string()).collect(),
        body: String::new(),
        now_secs: 1,
    }
}

fn router(provider: MemProvider) -> axum::Router {
    skills_router(SkillsApiState::new(Arc::new(provider)))
}

/// A read+write in-memory store: one mutable catalog per workspace, replayed on each read so an
/// appended event (a PUT) is visible to the next GET. Backs the write-surface round-trip.
#[derive(Default)]
struct MemStore {
    by_ws: Mutex<HashMap<String, Vec<SkillEvent>>>,
}

#[async_trait]
impl WorkspaceSkills for MemStore {
    async fn catalog(&self, workspace: &str) -> Result<SkillCatalog, AppError> {
        let mut s = SkillState::default();
        if let Some(events) = self.by_ws.lock().unwrap().get(workspace) {
            for e in events {
                s.apply(e);
            }
        }
        Ok(s.catalog)
    }
}

#[async_trait]
impl SkillWriter for MemStore {
    async fn append(&self, workspace: &str, event: SkillEvent) -> Result<(), AppError> {
        self.by_ws.lock().unwrap().entry(workspace.to_string()).or_default().push(event);
        Ok(())
    }
}

fn put_json(ws: &str, uri: &str, body: Value) -> Request<Body> {
    Request::builder()
        .method("PUT")
        .uri(uri)
        .header(WORKSPACE_HEADER, ws)
        .header("content-type", "application/json")
        .body(Body::from(body.to_string()))
        .unwrap()
}

#[tokio::test]
async fn set_role_model_round_trips_into_the_bindings() {
    // hq-role-model.1: PUT /roles/{role}/model persists the config, and GET / surfaces it on the
    // role's binding for the dashboard to hydrate.
    let store = Arc::new(MemStore::default());
    let app = skills_router(SkillsApiState::new(store.clone()).with_writer(store.clone()));

    let (status, _) = send(
        &app,
        put_json(
            "acme",
            "/roles/polecat/model",
            serde_json::json!({ "model": "opus", "permission_mode": "acceptEdits", "effort": "xhigh" }),
        ),
    )
    .await;
    assert_eq!(status, StatusCode::OK);

    let (status, body) = send(&app, get_in("acme", "/")).await;
    assert_eq!(status, StatusCode::OK);
    let mc = &body["bindings"][0]["model_config"];
    assert_eq!(body["bindings"][0]["role"], "polecat");
    assert_eq!(mc["model"], "opus");
    assert_eq!(mc["permission_mode"], "acceptEdits");
    assert_eq!(mc["effort"], "xhigh");

    // A bad permission mode is a client error (422), no mutation.
    let (status, _) = send(
        &app,
        put_json("acme", "/roles/polecat/model", serde_json::json!({ "permission_mode": "yolo" })),
    )
    .await;
    assert_eq!(status, StatusCode::UNPROCESSABLE_ENTITY);
}

async fn send(app: &axum::Router, req: Request<Body>) -> (StatusCode, Value) {
    let resp = app.clone().oneshot(req).await.expect("router responds");
    let status = resp.status();
    let bytes = axum::body::to_bytes(resp.into_body(), usize::MAX).await.expect("body");
    let value = serde_json::from_slice(&bytes).unwrap_or(Value::Null);
    (status, value)
}

fn get_in(ws: &str, uri: &str) -> Request<Body> {
    Request::builder()
        .uri(uri)
        .header(WORKSPACE_HEADER, ws)
        .body(Body::empty())
        .unwrap()
}

#[tokio::test]
async fn list_surfaces_every_registered_skill() {
    let app = router(MemProvider::new().seed(
        "acme",
        &[
            skill("merge_admin", &["merge.read", "merge.write"]),
            skill("feed_viewer", &["feed.read"]),
            SkillEvent::EnabledForRole { role: "deacon".into(), skill: "merge_admin".into(), now_secs: 2 },
        ],
    ));
    let (status, body) = send(&app, get_in("acme", "/")).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body["count"], 2);
    let ids: Vec<&str> = body["skills"].as_array().unwrap().iter().map(|s| s["id"].as_str().unwrap()).collect();
    assert_eq!(ids, vec!["feed_viewer", "merge_admin"], "sorted");
    // The per-role bindings ride along for dashboard hydration.
    assert_eq!(body["bindings"][0]["role"], "deacon");
}

#[tokio::test]
async fn get_resolves_a_skill_and_its_enabled_roles() {
    let app = router(MemProvider::new().seed(
        "acme",
        &[
            skill("merge_admin", &["merge.read", "merge.write"]),
            SkillEvent::EnabledForRole { role: "deacon".into(), skill: "merge_admin".into(), now_secs: 2 },
        ],
    ));
    let (status, body) = send(&app, get_in("acme", "/merge_admin")).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body["skill"]["id"], "merge_admin");
    assert_eq!(body["skill"]["default_scopes"][0], "merge.read");
    assert_eq!(body["enabled_for_roles"], serde_json::json!(["deacon"]));
}

#[tokio::test]
async fn missing_skill_is_404() {
    let app = router(MemProvider::new().seed("acme", &[skill("alpha", &["alpha.read"])]));
    let (status, _) = send(&app, get_in("acme", "/ghost")).await;
    assert_eq!(status, StatusCode::NOT_FOUND);
}

#[tokio::test]
async fn catalog_is_per_tenant() {
    let app = router(MemProvider::new().seed("acme", &[skill("merge_admin", &["merge.read"])]));
    // Tenant `acme` sees its skill.
    let (_, acme) = send(&app, get_in("acme", "/")).await;
    assert_eq!(acme["count"], 1);
    // Tenant `other` shares no catalog — empty, no cross-tenant leak.
    let (status, other) = send(&app, get_in("other", "/")).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(other["count"], 0);
    let (s2, _) = send(&app, get_in("other", "/merge_admin")).await;
    assert_eq!(s2, StatusCode::NOT_FOUND, "acme's skill is invisible to other");
}
