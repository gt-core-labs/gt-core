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
//! ## The required scope comes from the Capability (`hq-auth-guard.2`)
//!
//! The HTTP method classifies a request as a read (`GET`/`HEAD`/`OPTIONS`,
//! safe/idempotent) or a write (every mutating method). But the *scope* that
//! class requires is not fabricated from the method — it is the module's own
//! claimed `<module>.read` / `<module>.write`, drawn from the
//! [`Capability`](crate::Capability) (the same declared scope vocabulary the
//! module's MCP tools name). Resolving the requirement from what the module
//! *declares* — instead of assuming a `read`/`write` scope exists — has two
//! consequences:
//!
//! - A verb-class the module claims **no** scope for stays **public** for that
//!   class. This is the additive generalization of the "no scopes → public"
//!   rule: a module that claims only `beads.read` gates reads and leaves writes
//!   open; a module whose only scope is domain-specific (e.g. `merge.submit`,
//!   never reached by a method class) keeps its whole HTTP surface public rather
//!   than being bricked behind a `read`/`write` scope it never declared.
//! - When the module *does* claim the matching scope, behavior is identical to
//!   deriving it from the method — so a module claiming both `read` and `write`
//!   is guarded exactly as before.
//!
//! Resolving the requirement *inside* one middleware (rather than attaching a
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

/// Stamped into a denied response's extensions by [`guard_module_scopes`] so an
/// outer audit layer can record the denial — the scope the route required and the
/// verdict — without re-deriving either (`hq-auth-guard.3`).
///
/// Additive and behaviour-preserving: the status the guard returns is unchanged; only
/// this marker rides along for observability. Its presence is the precise signal that a
/// `401`/`403` came from THIS scope guard, never from a route handler's own response.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ScopeDenied {
    /// The scope the route required (`<module>.<verb>`).
    pub required: Scope,
    /// The verdict: `401 UNAUTHORIZED` (no caller scopes ⇒ unauthenticated) or
    /// `403 FORBIDDEN` (authenticated but missing the required scope).
    pub status: StatusCode,
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

/// The scope each verb-class of request requires, resolved once from a module's
/// declared [`Capability`](crate::Capability) scopes (`hq-auth-guard.2`).
///
/// `read`/`write` is `Some` only when the module actually claims
/// `<module>.read` / `<module>.write`; a verb-class the module claims no scope
/// for is left `None` and stays public — the additive generalization of the
/// "no scopes → public" rule. Resolved at wiring time so the per-request guard
/// is a cheap lookup carrying no parsing.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
struct RequiredScopes {
    read: Option<Scope>,
    write: Option<Scope>,
}

impl RequiredScopes {
    /// Resolve from the owning module id and the scopes it claims: keep the
    /// claimed `<module>.read` / `<module>.write`, drop anything not declared.
    fn resolve(module: &ModuleId, claimed: &[Scope]) -> Self {
        // `<module>.<verb>` is always a valid scope (validated slug + fixed kebab
        // verb); pick it only when the module's Capability declares it.
        let pick = |verb: &str| {
            let candidate = Scope::new(format!("{}.{}", module.as_str(), verb))
                .expect("module id + fixed verb is a valid scope");
            claimed.contains(&candidate).then_some(candidate)
        };
        RequiredScopes { read: pick("read"), write: pick("write") }
    }

    /// The scope a request with `method` requires, or `None` when the module
    /// claims no scope for that verb-class (route is public for that class).
    fn for_method(&self, method: &Method) -> Option<&Scope> {
        match verb_for(method) {
            "read" => self.read.as_ref(),
            _ => self.write.as_ref(),
        }
    }
}

/// Wrap a module's router so every route enforces the caller holds the scope the
/// request requires — the module's claimed `<module>.read` / `<module>.write`
/// for the request's verb-class (`hq-auth-guard.2`).
///
/// The required scope is resolved from `claimed` (the module's
/// [`Capability`](crate::Capability) scopes), not fabricated from the method: a
/// verb-class the module declares no scope for stays public. The builder calls
/// this only for modules that declare at least one scope; a module that claims
/// none keeps public routes. The returned router has the same `()` state, so it
/// composes exactly like an unguarded one.
pub fn guard_module_scopes(router: Router, module: &ModuleId, claimed: &[Scope]) -> Router {
    let required = RequiredScopes::resolve(module, claimed);
    router.layer(from_fn_with_state(required, enforce_module_scope))
}

/// Per-request scope check. The request's verb-class selects the required scope
/// resolved from the module's Capability; a class with no claimed scope passes
/// through (public), otherwise the caller must hold that scope.
async fn enforce_module_scope(
    State(required): State<RequiredScopes>,
    req: Request,
    next: Next,
) -> Response {
    let Some(needed) = required.for_method(req.method()) else {
        // The module claims no scope for this verb-class → route is public.
        return next.run(req).await;
    };
    match req.extensions().get::<CallerScopes>() {
        None => deny(needed.clone(), StatusCode::UNAUTHORIZED),
        Some(scopes) if scopes.holds(needed) => next.run(req).await,
        Some(_) => deny(needed.clone(), StatusCode::FORBIDDEN),
    }
}

/// Build a guard denial response, tagging it with the [`ScopeDenied`] marker so an
/// outer audit layer can observe what was refused. The response body stays the bare
/// status — only the extension is added, so behaviour is unchanged.
fn deny(required: Scope, status: StatusCode) -> Response {
    let mut resp = status.into_response();
    resp.extensions_mut().insert(ScopeDenied { required, status });
    resp
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

    /// A `beads` router with a read (GET) and a write (POST) route, guarded by
    /// the given claimed scopes.
    fn guarded_beads_claiming(claimed: &[&str]) -> Router {
        let inner = Router::new().route("/list", get(|| async { "ok" }).post(|| async { "ok" }));
        let scopes: Vec<Scope> = claimed.iter().map(|s| scope(s)).collect();
        guard_module_scopes(inner, &ModuleId::new("beads").unwrap(), &scopes)
    }

    /// The common case: a `beads` module claiming both read and write.
    fn guarded_beads() -> Router {
        guarded_beads_claiming(&["beads.read", "beads.write"])
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

    /// Send `method /list` with the optional caller scopes; return the whole response
    /// so a test can inspect its [`ScopeDenied`] marker.
    async fn respond(app: Router, method: Method, caller: Option<CallerScopes>) -> Response {
        let mut req = Request::builder()
            .method(method)
            .uri("/list")
            .body(Body::empty())
            .unwrap();
        if let Some(cs) = caller {
            req.extensions_mut().insert(cs);
        }
        app.oneshot(req).await.unwrap()
    }

    #[tokio::test]
    async fn denials_carry_the_scope_denied_marker() {
        // 401 (no caller scopes) marks the required scope + status for an audit layer.
        let resp = respond(guarded_beads(), Method::GET, None).await;
        assert_eq!(resp.status(), StatusCode::UNAUTHORIZED);
        assert_eq!(
            resp.extensions().get::<ScopeDenied>(),
            Some(&ScopeDenied { required: scope("beads.read"), status: StatusCode::UNAUTHORIZED })
        );

        // 403 (present but lacking the scope) marks the required scope + status too.
        let cs = CallerScopes::new([scope("beads.read")]);
        let resp = respond(guarded_beads(), Method::POST, Some(cs)).await;
        assert_eq!(resp.status(), StatusCode::FORBIDDEN);
        assert_eq!(
            resp.extensions().get::<ScopeDenied>(),
            Some(&ScopeDenied { required: scope("beads.write"), status: StatusCode::FORBIDDEN })
        );
    }

    #[tokio::test]
    async fn allowed_and_public_responses_carry_no_marker() {
        // A passing request never stamps the marker, so an audit layer records nothing.
        let cs = CallerScopes::new([scope("beads.read")]);
        let resp = respond(guarded_beads(), Method::GET, Some(cs)).await;
        assert_eq!(resp.status(), StatusCode::OK);
        assert!(resp.extensions().get::<ScopeDenied>().is_none());
    }

    // --- Capability-derived requirement (hq-auth-guard.2) -------------------

    #[test]
    fn required_scopes_keep_only_declared_verbs() {
        let beads = ModuleId::new("beads").unwrap();
        // Both claimed → both required.
        let both = RequiredScopes::resolve(&beads, &[scope("beads.read"), scope("beads.write")]);
        assert_eq!(both.read, Some(scope("beads.read")));
        assert_eq!(both.write, Some(scope("beads.write")));
        // Only read claimed → write verb-class is unguarded.
        let read_only = RequiredScopes::resolve(&beads, &[scope("beads.read")]);
        assert_eq!(read_only.read, Some(scope("beads.read")));
        assert_eq!(read_only.write, None);
        // A domain-specific scope maps to neither read nor write.
        let domain = RequiredScopes::resolve(&beads, &[scope("beads.submit")]);
        assert_eq!(domain, RequiredScopes::default());
    }

    #[test]
    fn for_method_picks_the_verb_class_scope() {
        let beads = ModuleId::new("beads").unwrap();
        let req = RequiredScopes::resolve(&beads, &[scope("beads.read"), scope("beads.write")]);
        assert_eq!(req.for_method(&Method::GET), Some(&scope("beads.read")));
        assert_eq!(req.for_method(&Method::HEAD), Some(&scope("beads.read")));
        assert_eq!(req.for_method(&Method::POST), Some(&scope("beads.write")));
        assert_eq!(req.for_method(&Method::DELETE), Some(&scope("beads.write")));
    }

    #[tokio::test]
    async fn unclaimed_write_verb_leaves_writes_public() {
        // A module that claims only `beads.read` gates reads but leaves the write
        // verb-class public — the additive generalization of "no scopes → public".
        let app = || guarded_beads_claiming(&["beads.read"]);
        // Write needs no scope at all: even an unauthenticated POST passes.
        assert_eq!(status(app(), Method::POST, None).await, StatusCode::OK);
        // Reads are still gated.
        assert_eq!(status(app(), Method::GET, None).await, StatusCode::UNAUTHORIZED);
        let cs = CallerScopes::new([scope("beads.read")]);
        assert_eq!(status(app(), Method::GET, Some(cs)).await, StatusCode::OK);
    }

    #[tokio::test]
    async fn unclaimed_read_verb_leaves_reads_public() {
        let app = || guarded_beads_claiming(&["beads.write"]);
        assert_eq!(status(app(), Method::GET, None).await, StatusCode::OK);
        // Writes are still gated.
        assert_eq!(status(app(), Method::POST, None).await, StatusCode::UNAUTHORIZED);
        let cs = CallerScopes::new([scope("beads.write")]);
        assert_eq!(status(app(), Method::POST, Some(cs)).await, StatusCode::OK);
    }

    #[tokio::test]
    async fn domain_only_scope_leaves_whole_surface_public() {
        // The module's only claimed scope is neither read nor write, so no HTTP
        // verb-class is gated: the surface stays public rather than bricked.
        let app = || guarded_beads_claiming(&["beads.submit"]);
        assert_eq!(status(app(), Method::GET, None).await, StatusCode::OK);
        assert_eq!(status(app(), Method::POST, None).await, StatusCode::OK);
    }
}
