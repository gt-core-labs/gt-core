//! The HTTP authentication middleware (`hq-auth-context.3`).
//!
//! This is the tier-crossing seam the auth design hinges on. It sits in the `modules` tier —
//! the only tier that may depend on both `gt-auth` (the verifier) and `gt-workspace` (the
//! tenant types) — so it can do what neither platform crate may do alone: turn a request's
//! bearer token into a *verified* identity and hand the pieces downstream as request
//! extensions.
//!
//! On every request it:
//! 1. reads the `Authorization: Bearer <token>` header (absent ⇒ the request stays anonymous,
//!    so public routes like `/auth/login` need no token and the per-route guard decides);
//! 2. verifies the token through the injected [`Authenticator`] (a present-but-invalid token is
//!    a `401` — a broken credential is an explicit failure, not anonymity);
//! 3. inserts the verified [`JwtClaims`] into the request extensions for downstream guards, and
//!    a [`WorkspaceClaim`] so the [`WorkspaceContext`](gt_workspace::WorkspaceContext) extractor
//!    can resolve the tenant from the token when no `X-GT-Workspace` header is present
//!    (`hq-auth-context.1`).
//!
//! It does **not** authorize: mapping the claim's scopes to a route's required scope is the
//! per-route guard's job (`hq-auth-guard`). This layer only authenticates and injects, keeping
//! the verifier abstract (`Arc<dyn Authenticator>`) so the binary picks the concrete RS256
//! implementation and this crate needs no `jsonwebtoken` feature.

use std::sync::Arc;

use axum::extract::{Request, State};
use axum::http::{header::AUTHORIZATION, StatusCode};
use axum::middleware::Next;
use axum::response::{IntoResponse, Response};

use gt_auth::Authenticator;
use gt_workspace::WorkspaceClaim;

/// A shareable, object-safe authenticator handle the middleware verifies tokens with.
pub type SharedAuthenticator = Arc<dyn Authenticator + Send + Sync>;

/// Pull the bearer token out of an `Authorization` header value (`Bearer <token>`),
/// case-insensitively on the scheme. Returns `None` when the header is absent.
/// `Some(Err)` marks a present header whose scheme/shape is wrong — a client error.
fn bearer(req: &Request) -> Option<Result<&str, ()>> {
    let value = req.headers().get(AUTHORIZATION)?;
    let parsed = value.to_str().ok().and_then(|s| {
        let (scheme, token) = s.split_once(' ')?;
        scheme.eq_ignore_ascii_case("bearer")
            .then(|| token.trim())
            .filter(|t| !t.is_empty())
    });
    Some(parsed.ok_or(()))
}

/// Authenticate the request and inject the verified claim, or pass it through anonymously.
///
/// Wire it with [`axum::middleware::from_fn_with_state`] passing a [`SharedAuthenticator`] as
/// the state. See the module docs for the contract.
pub async fn authenticate(
    State(auth): State<SharedAuthenticator>,
    mut req: Request,
    next: Next,
) -> Response {
    match bearer(&req) {
        // No Authorization header — anonymous. The per-route guard rejects protected routes;
        // public ones (login) proceed.
        None => next.run(req).await,
        // Present but not a well-formed bearer token.
        Some(Err(())) => unauthorized("malformed Authorization header").into_response(),
        Some(Ok(token)) => match auth.authenticate(token) {
            Ok(claims) => {
                // WorkspaceClaim first (the slug the tenant extractor falls back to), then the
                // full claims for the scope guard.
                req.extensions_mut()
                    .insert(WorkspaceClaim(claims.workspace.clone()));
                req.extensions_mut().insert(claims);
                next.run(req).await
            }
            Err(_) => unauthorized("invalid bearer token").into_response(),
        },
    }
}

fn unauthorized(msg: &str) -> impl IntoResponse + '_ {
    (StatusCode::UNAUTHORIZED, msg.to_string())
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::body::Body;
    use axum::routing::get;
    use axum::Router;
    use gt_auth::{InMemoryAuthenticator, JwtClaims};
    use tower::ServiceExt; // oneshot

    fn claims() -> JwtClaims {
        JwtClaims {
            sub: "alice".into(),
            workspace: "acme".into(),
            scopes: vec!["rig.read".into()],
            exp: 9_999_999_999,
            nbf: None,
            iat: 0,
        }
    }

    /// A probe handler that reports which auth extensions reached it.
    async fn probe(req: Request) -> String {
        let claims = req.extensions().get::<JwtClaims>().map(|c| c.sub.clone());
        let ws = req.extensions().get::<WorkspaceClaim>().map(|w| w.slug().to_string());
        format!("claims={claims:?} ws={ws:?}")
    }

    fn app(auth: SharedAuthenticator) -> Router {
        Router::new()
            .route("/", get(probe))
            .layer(axum::middleware::from_fn_with_state(auth, authenticate))
    }

    async fn call(auth: SharedAuthenticator, header: Option<&str>) -> (StatusCode, String) {
        let mut builder = Request::builder().uri("/");
        if let Some(h) = header {
            builder = builder.header(AUTHORIZATION, h);
        }
        let resp = app(auth)
            .oneshot(builder.body(Body::empty()).unwrap())
            .await
            .unwrap();
        let status = resp.status();
        let bytes = axum::body::to_bytes(resp.into_body(), usize::MAX).await.unwrap();
        (status, String::from_utf8(bytes.to_vec()).unwrap())
    }

    fn verifier() -> SharedAuthenticator {
        Arc::new(InMemoryAuthenticator::new().with_token("good", claims()))
    }

    #[tokio::test]
    async fn a_valid_token_injects_claims_and_workspace() {
        let (status, body) = call(verifier(), Some("Bearer good")).await;
        assert_eq!(status, StatusCode::OK);
        assert!(body.contains(r#"claims=Some("alice")"#), "{body}");
        assert!(body.contains(r#"ws=Some("acme")"#), "{body}");
    }

    #[tokio::test]
    async fn the_bearer_scheme_is_case_insensitive() {
        let (status, body) = call(verifier(), Some("bearer good")).await;
        assert_eq!(status, StatusCode::OK);
        assert!(body.contains(r#"claims=Some("alice")"#), "{body}");
    }

    #[tokio::test]
    async fn no_header_passes_through_anonymously() {
        let (status, body) = call(verifier(), None).await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(body, "claims=None ws=None");
    }

    #[tokio::test]
    async fn an_invalid_token_is_unauthorized() {
        let (status, _) = call(verifier(), Some("Bearer nope")).await;
        assert_eq!(status, StatusCode::UNAUTHORIZED);
    }

    #[tokio::test]
    async fn a_non_bearer_authorization_is_unauthorized() {
        let (status, _) = call(verifier(), Some("Basic abc123")).await;
        assert_eq!(status, StatusCode::UNAUTHORIZED);
    }

    #[tokio::test]
    async fn an_empty_bearer_token_is_unauthorized() {
        let (status, _) = call(verifier(), Some("Bearer ")).await;
        assert_eq!(status, StatusCode::UNAUTHORIZED);
    }
}
