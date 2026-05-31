//! Integration matrix for module route composition (`hq-mod-routes.6`).
//!
//! Black-box: drives only the crate's public API the way an app composition root
//! would — register modules, `build()`, then `into_router()` / `openapi()` — and
//! exercises the three guarantees the routes sub-epic makes:
//!
//! - **namespacing / collision** (`.2`): two modules may declare the same
//!   relative path; each answers under its own `/api/v1/<module>` prefix.
//! - **scope enforcement** (`.3`): a module that claims scopes guards every
//!   route by a method-derived `<module>.<read|write>` check; one that claims
//!   none stays public.
//! - **OpenAPI emission** (`.4`): each documenting module's spec is mounted under
//!   its prefix and merged into one document; disabled modules contribute none.
//!
//! If a later bead changes an internal detail but keeps the public contract,
//! these stay green.

use axum::body::Body;
use axum::http::{Method, Request, StatusCode};
use axum::routing::get;
use axum::Router;
use gt_module::{
    Capability, CallerScopes, DisabledModules, GtModule, ModuleId, ModuleMeta, RootBuilder, Scope,
};
use semver::Version;
use tower::ServiceExt; // `oneshot`

/// A module with a read (GET) + write (POST) route at the same relative `/list`
/// path, optionally claiming scopes (which opts its routes into RBAC).
struct ApiModule {
    id: &'static str,
    scopes: &'static [&'static str],
}

impl GtModule for ApiModule {
    fn meta(&self) -> ModuleMeta {
        ModuleMeta::new(
            ModuleId::new(self.id).unwrap(),
            self.id,
            Version::new(1, 0, 0),
            "routes integration module",
        )
    }
    fn capability(&self) -> Capability {
        Capability::empty().claiming_all(self.scopes.iter().map(|s| Scope::new(*s).unwrap()))
    }
    fn register_routes(&self) -> Router {
        let id = self.id;
        Router::new().route("/list", get(move || async move { id }).post(move || async move { id }))
    }
}

const fn api(id: &'static str, scopes: &'static [&'static str]) -> ApiModule {
    ApiModule { id, scopes }
}

/// Send `method path` with optional caller scopes; return (status, body).
async fn send(
    app: &Router,
    method: Method,
    path: &str,
    caller: Option<CallerScopes>,
) -> (StatusCode, String) {
    let mut req = Request::builder().method(method).uri(path).body(Body::empty()).unwrap();
    if let Some(cs) = caller {
        req.extensions_mut().insert(cs);
    }
    let res = app.clone().oneshot(req).await.unwrap();
    let status = res.status();
    let bytes = axum::body::to_bytes(res.into_body(), usize::MAX).await.unwrap();
    (status, String::from_utf8(bytes.to_vec()).unwrap())
}

fn scopes(list: &[&str]) -> CallerScopes {
    CallerScopes::new(list.iter().map(|s| Scope::new(*s).unwrap()))
}

#[tokio::test]
async fn same_relative_path_across_modules_does_not_collide() {
    // Two public modules each declare `/list`; the prefix keeps them apart.
    let app = RootBuilder::new()
        .module(api("beads", &[]))
        .module(api("rigs", &[]))
        .build()
        .unwrap()
        .into_router();

    assert_eq!(send(&app, Method::GET, "/api/v1/beads/list", None).await, (StatusCode::OK, "beads".into()));
    assert_eq!(send(&app, Method::GET, "/api/v1/rigs/list", None).await, (StatusCode::OK, "rigs".into()));
    // Unprefixed relative path is never served.
    assert_eq!(send(&app, Method::GET, "/list", None).await.0, StatusCode::NOT_FOUND);
}

#[tokio::test]
async fn scope_claiming_module_enforces_method_derived_scope() {
    // beads claims read+write → guarded; rigs claims nothing → public.
    let app = RootBuilder::new()
        .module(api("beads", &["beads.read", "beads.write"]))
        .module(api("rigs", &[]))
        .build()
        .unwrap()
        .into_router();

    // No caller scopes → unauthorized on the guarded module.
    assert_eq!(send(&app, Method::GET, "/api/v1/beads/list", None).await.0, StatusCode::UNAUTHORIZED);

    // read allows GET, forbids POST (which needs write).
    let read = scopes(&["beads.read"]);
    assert_eq!(send(&app, Method::GET, "/api/v1/beads/list", Some(read.clone())).await.0, StatusCode::OK);
    assert_eq!(send(&app, Method::POST, "/api/v1/beads/list", Some(read)).await.0, StatusCode::FORBIDDEN);

    // write allows POST.
    let write = scopes(&["beads.write"]);
    assert_eq!(send(&app, Method::POST, "/api/v1/beads/list", Some(write)).await.0, StatusCode::OK);

    // A scope for the wrong module never passes.
    assert_eq!(send(&app, Method::GET, "/api/v1/beads/list", Some(scopes(&["rigs.read"]))).await.0, StatusCode::FORBIDDEN);

    // The unguarded module stays public — no scopes needed.
    assert_eq!(send(&app, Method::GET, "/api/v1/rigs/list", None).await.0, StatusCode::OK);
}

#[tokio::test]
async fn feature_flag_disabled_module_serves_nothing() {
    let flags = DisabledModules::new([ModuleId::new("rigs").unwrap()]);
    let app = RootBuilder::new()
        .module(api("beads", &[]))
        .module(api("rigs", &[]))
        .build_with_flags(&flags)
        .unwrap()
        .into_router();

    assert_eq!(send(&app, Method::GET, "/api/v1/beads/list", None).await.0, StatusCode::OK);
    assert_eq!(send(&app, Method::GET, "/api/v1/rigs/list", None).await.0, StatusCode::NOT_FOUND);
}

// --- OpenAPI emission ------------------------------------------------------

/// Handlers + derived specs, referenced only by the `utoipa::path` macro.
#[allow(dead_code)]
mod docs {
    #[utoipa::path(get, path = "/list", responses((status = 200, description = "beads")))]
    pub async fn beads_list() -> &'static str {
        "[]"
    }
    #[derive(utoipa::OpenApi)]
    #[openapi(paths(beads_list))]
    pub struct BeadsApi;

    #[utoipa::path(get, path = "/list", responses((status = 200, description = "rigs")))]
    pub async fn rigs_list() -> &'static str {
        "[]"
    }
    #[derive(utoipa::OpenApi)]
    #[openapi(paths(rigs_list))]
    pub struct RigsApi;
}

/// A module that documents its routes with a derived spec.
struct DocModule {
    id: &'static str,
    spec: utoipa::openapi::OpenApi,
}
impl GtModule for DocModule {
    fn meta(&self) -> ModuleMeta {
        ModuleMeta::new(
            ModuleId::new(self.id).unwrap(),
            self.id,
            Version::new(1, 0, 0),
            "documented integration module",
        )
    }
    fn openapi(&self) -> Option<utoipa::openapi::OpenApi> {
        Some(self.spec.clone())
    }
}

#[test]
fn openapi_merges_module_specs_under_their_prefixes() {
    let root = RootBuilder::new()
        .module(DocModule { id: "beads", spec: <docs::BeadsApi as utoipa::OpenApi>::openapi() })
        .module(DocModule { id: "rigs", spec: <docs::RigsApi as utoipa::OpenApi>::openapi() })
        .build()
        .unwrap();
    let doc = root.openapi();
    let paths: Vec<&String> = doc.paths.paths.keys().collect();
    assert!(doc.paths.paths.contains_key("/api/v1/beads/list"), "got {paths:?}");
    assert!(doc.paths.paths.contains_key("/api/v1/rigs/list"), "got {paths:?}");
    // Relative path never leaks unprefixed.
    assert!(!doc.paths.paths.contains_key("/list"));
}

#[test]
fn openapi_omits_disabled_module() {
    let flags = DisabledModules::new([ModuleId::new("beads").unwrap()]);
    let root = RootBuilder::new()
        .module(DocModule { id: "beads", spec: <docs::BeadsApi as utoipa::OpenApi>::openapi() })
        .build_with_flags(&flags)
        .unwrap();
    assert!(root.openapi().paths.paths.is_empty());
}
