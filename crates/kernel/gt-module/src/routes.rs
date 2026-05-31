//! HTTP path namespacing and scope enforcement for module routers
//! (`hq-mod-routes.2`, `.3`).
//!
//! Every module's routes are mounted under a per-module prefix so two modules
//! can declare the same relative path (`/`, `/{id}`) without colliding, and so
//! a URL names its owning module. The builder applies this automatically in
//! [`Root::into_router`](crate::Root::into_router); a module author writes plain
//! relative routes and never types the prefix.
//!
//! The shape is fixed by `docs/03-architecture-guardrails.md` (rule: URL is
//! `/api/v1/<module>/...`, workspace comes from the JWT, never the path).
//!
//! ## Scope enforcement (`hq-mod-routes.3`)
//!
//! A module that declares authorization scopes in its
//! [`Capability`](crate::Capability) opts every one of its routes into RBAC: the
//! builder wraps the module's router with [`guard_module_scopes`], which rejects
//! a request whose caller does not hold the scope the route requires. A module
//! that claims no scope stays public.
//!
//! The required scope is derived per request from the HTTP method —
//! `GET`/`HEAD`/`OPTIONS` need `<module>.read`, every mutating method needs
//! `<module>.write`. Deriving it *inside* one middleware (rather than attaching a
//! static guard per method) is deliberate: a single `Router::layer` then guards
//! all methods correctly, sidestepping the trap that `route_layer` applies one
//! guard to every method on a router regardless of intent.
//!
//! The caller's granted scopes arrive in the request extensions as
//! [`CallerScopes`], inserted by the upstream authentication layer (JWT claim →
//! scope set; that extraction is `gt-mt-auth`/app territory, not the kernel's).
//! No scopes extension at all is treated as unauthenticated (`401`); present but
//! missing the required scope is forbidden (`403`).

use std::collections::BTreeSet;

use axum::extract::{Request, State};
use axum::http::{Method, StatusCode};
use axum::middleware::{from_fn_with_state, Next};
use axum::response::{IntoResponse, Response};
use axum::Router;

use crate::meta::ModuleId;
use crate::scope::Scope;

/// The version-pinned base every module's routes mount under.
///
/// Bumping the API version is a deliberate, cross-module event; keeping the base
/// in one place means a future `/api/v2` migration touches exactly this constant
/// and the beads that opt modules into it — not every call site.
pub const API_BASE: &str = "/api/v1";

/// The mount prefix for a module's routes: `/api/v1/<module-id>`.
///
/// `id` is a validated [`ModuleId`] (a lowercase slug), so the result is always a
/// well-formed, single-segment path suffix with no normalization needed. A
/// module that contributes routes for `beads` is mounted at `/api/v1/beads`, and
/// a relative `/` route it declares answers at `/api/v1/beads/`.
pub fn module_prefix(id: &ModuleId) -> String {
    format!("{API_BASE}/{}", id.as_str())
}

/// The authorization scopes the authenticated caller holds, carried in the
/// request extensions (`hq-mod-routes.3`).
///
/// The upstream authentication layer turns a verified JWT (or session) into this
/// set and inserts it; the per-module guard then checks the route's required
/// scope against it. The kernel defines the contract but never mints it — minting
/// is `gt-mt-auth`/app territory, so spoofing is impossible from a request body
/// (the set comes only from server-side auth, never the wire).
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct CallerScopes(BTreeSet<Scope>);

impl CallerScopes {
    /// Build a caller-scope set from the granted scopes.
    pub fn new(scopes: impl IntoIterator<Item = Scope>) -> Self {
        CallerScopes(scopes.into_iter().collect())
    }

    /// Whether the caller holds `scope`.
    pub fn holds(&self, scope: &Scope) -> bool {
        self.0.contains(scope)
    }
}

/// The scope verb a request method requires: read for safe/idempotent reads,
/// write for anything that mutates. `OPTIONS` (CORS preflight, idempotent) maps
/// to read so it is never gated behind write.
fn verb_for(method: &Method) -> &'static str {
    match *method {
        Method::GET | Method::HEAD | Method::OPTIONS => "read",
        _ => "write",
    }
}

/// Wrap a module's router so every route enforces the caller holds the scope the
/// request requires: `<module>.<read|write>` derived from the HTTP method
/// (`hq-mod-routes.3`).
///
/// The builder calls this only for modules that declare scopes in their
/// [`Capability`](crate::Capability); a module that claims none keeps public
/// routes. The returned router has the same `()` state, so it composes exactly
/// like an unguarded one.
pub fn guard_module_scopes(router: Router, module: &ModuleId) -> Router {
    router.layer(from_fn_with_state(module.clone(), enforce_module_scope))
}

/// Per-request scope check. `module` is the owning module's id (the scope
/// resource); the verb comes from the method, so one layer guards every method
/// with the right scope.
async fn enforce_module_scope(State(module): State<ModuleId>, req: Request, next: Next) -> Response {
    // `<module>.<verb>` is always a valid scope: `module` is a validated slug and
    // `verb_for` returns a fixed kebab word.
    let required = Scope::new(format!("{}.{}", module.as_str(), verb_for(req.method())))
        .expect("module id + fixed verb is a valid scope");
    match req.extensions().get::<CallerScopes>() {
        None => StatusCode::UNAUTHORIZED.into_response(),
        Some(scopes) if scopes.holds(&required) => next.run(req).await,
        Some(_) => StatusCode::FORBIDDEN.into_response(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn prefix_is_api_base_plus_id() {
        let id = ModuleId::new("beads").unwrap();
        assert_eq!(module_prefix(&id), "/api/v1/beads");
    }

    #[test]
    fn base_is_version_pinned() {
        assert_eq!(API_BASE, "/api/v1");
    }

    #[test]
    fn distinct_modules_get_distinct_prefixes() {
        let beads = module_prefix(&ModuleId::new("beads").unwrap());
        let rigs = module_prefix(&ModuleId::new("rigs").unwrap());
        assert_ne!(beads, rigs);
        assert!(beads.starts_with(API_BASE));
        assert!(rigs.starts_with(API_BASE));
    }

    // --- Scope enforcement (hq-mod-routes.3) --------------------------------

    use axum::body::Body;
    use axum::routing::get;
    use tower::ServiceExt; // `oneshot`

    fn scope(s: &str) -> Scope {
        Scope::new(s).unwrap()
    }

    #[test]
    fn safe_methods_map_to_read_mutating_to_write() {
        for m in [Method::GET, Method::HEAD, Method::OPTIONS] {
            assert_eq!(verb_for(&m), "read", "{m} should be read");
        }
        for m in [Method::POST, Method::PUT, Method::PATCH, Method::DELETE] {
            assert_eq!(verb_for(&m), "write", "{m} should be write");
        }
    }

    #[test]
    fn caller_scopes_membership() {
        let cs = CallerScopes::new([scope("beads.read")]);
        assert!(cs.holds(&scope("beads.read")));
        assert!(!cs.holds(&scope("beads.write")));
        assert!(CallerScopes::default().0.is_empty());
    }

    /// A `beads` router with a read (GET) and a write (POST) route, guarded.
    fn guarded_beads() -> Router {
        let inner = Router::new().route("/list", get(|| async { "ok" }).post(|| async { "ok" }));
        guard_module_scopes(inner, &ModuleId::new("beads").unwrap())
    }

    /// Send `method /list` with the optional caller scopes; return the status.
    async fn status(app: Router, method: Method, caller: Option<CallerScopes>) -> StatusCode {
        let mut req = Request::builder()
            .method(method)
            .uri("/list")
            .body(Body::empty())
            .unwrap();
        if let Some(cs) = caller {
            req.extensions_mut().insert(cs);
        }
        app.oneshot(req).await.unwrap().status()
    }

    #[tokio::test]
    async fn missing_caller_scopes_is_unauthorized() {
        let s = status(guarded_beads(), Method::GET, None).await;
        assert_eq!(s, StatusCode::UNAUTHORIZED);
    }

    #[tokio::test]
    async fn read_scope_allows_get_but_not_post() {
        let cs = CallerScopes::new([scope("beads.read")]);
        assert_eq!(status(guarded_beads(), Method::GET, Some(cs.clone())).await, StatusCode::OK);
        // GET passed with read; POST needs beads.write the caller lacks.
        assert_eq!(status(guarded_beads(), Method::POST, Some(cs)).await, StatusCode::FORBIDDEN);
    }

    #[tokio::test]
    async fn write_scope_allows_post_but_not_get() {
        let cs = CallerScopes::new([scope("beads.write")]);
        assert_eq!(status(guarded_beads(), Method::POST, Some(cs.clone())).await, StatusCode::OK);
        assert_eq!(status(guarded_beads(), Method::GET, Some(cs)).await, StatusCode::FORBIDDEN);
    }

    #[tokio::test]
    async fn holding_both_scopes_allows_both_methods() {
        let cs = CallerScopes::new([scope("beads.read"), scope("beads.write")]);
        assert_eq!(status(guarded_beads(), Method::GET, Some(cs.clone())).await, StatusCode::OK);
        assert_eq!(status(guarded_beads(), Method::POST, Some(cs)).await, StatusCode::OK);
    }

    #[tokio::test]
    async fn guard_resource_is_the_module_id_not_the_path() {
        // Caller holds `rigs.read`, but the guard for the `beads` module requires
        // `beads.read` — a scope for the wrong module never passes.
        let cs = CallerScopes::new([scope("rigs.read")]);
        assert_eq!(status(guarded_beads(), Method::GET, Some(cs)).await, StatusCode::FORBIDDEN);
    }
}
