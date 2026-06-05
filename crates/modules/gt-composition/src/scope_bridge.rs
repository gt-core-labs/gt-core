//! The claims-to-scopes bridge middleware (`hq-auth-guard.1`).
//!
//! A thin tower layer that runs *after* the [`authenticate`](crate::auth::authenticate)
//! middleware (`hq-auth-context.3`) and *before* the kernel's per-route scope guard
//! ([`guard_module_scopes`](gt_module::guard_module_scopes)). It does one job: translate the
//! verified [`JwtClaims`] the auth layer injected into the [`CallerScopes`] the kernel guard
//! consumes. It performs **no** verification of its own — signature checking and identity are
//! the auth layer's concern, and the actual per-route authorization decision is the kernel's.
//!
//! ## Contract
//!
//! - **Claims present:** each string in [`JwtClaims::scopes`] is parsed through
//!   [`Scope::new`](gt_module::Scope::new). Strings that are not a valid `resource.verb`
//!   slug are **silently dropped**, not rejected — a token may legitimately carry scopes for
//!   namespaces this kernel build does not recognize, and one malformed entry must not sink an
//!   otherwise valid identity. The surviving scopes become a [`CallerScopes`] inserted into the
//!   extensions. If *every* scope is invalid the caller gets an empty `CallerScopes`, which the
//!   kernel guard treats as authenticated-but-unauthorized (`403`) — distinct from anonymous.
//! - **No claims (anonymous request):** nothing is inserted. The kernel guard reads a missing
//!   [`CallerScopes`] as unauthenticated and answers `401` on a guarded route; an unguarded
//!   (public) route is unaffected. This is the deliberate split: *absent* `CallerScopes` ⇒ 401,
//!   *empty* `CallerScopes` ⇒ 403.
//!
//! Wire it with [`axum::middleware::from_fn`] on the router, layered so it runs between the
//! authenticator and the kernel's module routers.

use axum::extract::Request;
use axum::middleware::Next;
use axum::response::Response;

use gt_auth::JwtClaims;
use gt_module::{CallerScopes, Scope};

/// Translate the verified [`JwtClaims`] (if any) into [`CallerScopes`] for the kernel guard.
///
/// See the module docs for the contract: invalid scope strings are dropped, and an anonymous
/// request (no claims) leaves `CallerScopes` absent so the guard answers `401`.
pub async fn bridge_scopes(mut req: Request, next: Next) -> Response {
    if let Some(scopes) = req
        .extensions()
        .get::<JwtClaims>()
        .map(|claims| caller_scopes(&claims.scopes))
    {
        req.extensions_mut().insert(scopes);
    }
    next.run(req).await
}

/// Parse the granted scope strings into a [`CallerScopes`]. A `"*"` grant is the superuser
/// wildcard (mirrors the MCP path's `gt_rbac` scope) and satisfies every route; otherwise each
/// string is parsed as a `resource.verb` slug, dropping any that don't parse. Pure, so it is
/// unit-testable without a router.
fn caller_scopes(granted: &[String]) -> CallerScopes {
    if granted.iter().any(|s| s == "*") {
        return CallerScopes::all();
    }
    CallerScopes::new(granted.iter().filter_map(|s| Scope::new(s.as_str()).ok()))
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::body::Body;
    use axum::http::StatusCode;
    use axum::routing::get;
    use axum::Router;
    use tower::ServiceExt; // oneshot

    fn claims_with(scopes: Vec<String>) -> JwtClaims {
        JwtClaims {
            sub: "alice".into(),
            workspace: "acme".into(),
            scopes,
            exp: 9_999_999_999,
            nbf: None,
            iat: 0,
        }
    }

    /// A probe handler reporting whether/what `CallerScopes` reached it.
    async fn probe(req: Request) -> String {
        match req.extensions().get::<CallerScopes>() {
            None => "absent".to_string(),
            Some(cs) => {
                let mut held: Vec<&str> = ["rig.read", "rig.write", "beads.read"]
                    .into_iter()
                    .filter(|s| cs.holds(&Scope::new(*s).unwrap()))
                    .collect();
                held.sort_unstable();
                format!("present:{}", held.join(","))
            }
        }
    }

    /// Run a request through the bridge, optionally pre-inserting claims (as the auth layer
    /// would have), and return the probe's report.
    async fn run(claims: Option<JwtClaims>) -> String {
        let app = Router::new()
            .route("/", get(probe))
            .layer(axum::middleware::from_fn(bridge_scopes));
        let mut req = Request::builder().uri("/").body(Body::empty()).unwrap();
        if let Some(c) = claims {
            req.extensions_mut().insert(c);
        }
        let resp = app.oneshot(req).await.unwrap();
        assert_eq!(resp.status(), StatusCode::OK);
        let bytes = axum::body::to_bytes(resp.into_body(), usize::MAX).await.unwrap();
        String::from_utf8(bytes.to_vec()).unwrap()
    }

    #[tokio::test]
    async fn claims_with_scopes_produce_matching_caller_scopes() {
        let out = run(Some(claims_with(vec!["rig.read".into(), "rig.write".into()]))).await;
        assert_eq!(out, "present:rig.read,rig.write");
    }

    #[tokio::test]
    async fn no_claims_leaves_caller_scopes_absent() {
        // Absent CallerScopes is the kernel's 401 signal — the bridge must not fabricate one.
        assert_eq!(run(None).await, "absent");
    }

    #[tokio::test]
    async fn invalid_scope_strings_are_dropped_not_rejected() {
        // "not a scope" (no verb) and "bad..seg" are invalid; "rig.read" survives.
        let out = run(Some(claims_with(vec![
            "not a scope".into(),
            "rig.read".into(),
            "bad..seg".into(),
        ])))
        .await;
        assert_eq!(out, "present:rig.read");
    }

    #[tokio::test]
    async fn all_invalid_scopes_yield_present_but_empty() {
        // Present-but-empty is authenticated-without-authority (kernel 403), not anonymous.
        let out = run(Some(claims_with(vec!["garbage".into()]))).await;
        assert_eq!(out, "present:");
    }

    #[tokio::test]
    async fn star_scope_is_the_superuser_wildcard() {
        // A "*" grant satisfies every probed scope (admin token ⇒ full REST access).
        let out = run(Some(claims_with(vec!["*".into()]))).await;
        assert_eq!(out, "present:beads.read,rig.read,rig.write");
    }

    #[test]
    fn caller_scopes_helper_filters_invalid() {
        let cs = caller_scopes(&["rig.read".into(), "nope".into()]);
        assert!(cs.holds(&Scope::new("rig.read").unwrap()));
        assert_eq!(caller_scopes(&[]), CallerScopes::default());
        // "*" ⇒ wildcard: holds an arbitrary scope it was never explicitly granted.
        assert!(caller_scopes(&["*".into()]).holds(&Scope::new("anything.write").unwrap()));
    }
}
