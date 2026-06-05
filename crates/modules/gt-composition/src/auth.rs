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
use axum::http::{
    header::{AUTHORIZATION, COOKIE},
    StatusCode,
};
use axum::middleware::Next;
use axum::response::{IntoResponse, Response};

use gt_auth::Authenticator;
use gt_workspace::WorkspaceClaim;

use crate::denial_audit::{record_denial, SharedAudit, ANONYMOUS};

/// A shareable, object-safe authenticator handle the middleware verifies tokens with.
pub type SharedAuthenticator = Arc<dyn Authenticator + Send + Sync>;

/// State for [`authenticate`]: the verifier it checks tokens with plus the audit sink it
/// records token rejections (`401`) into. Bundled because the middleware that both
/// authenticates and audits its own denials needs a single `State` (`hq-auth-guard.3`).
#[derive(Clone)]
pub struct AuthState {
    /// The object-safe authenticator handle.
    pub authenticator: SharedAuthenticator,
    /// The sink token-rejection denials are recorded into (the same `Arc` the
    /// [`audit_denials`](crate::denial_audit::audit_denials) layer holds).
    pub audit: SharedAudit,
}

impl AuthState {
    /// Bundle a verifier and an audit sink for the auth middleware.
    pub fn new(authenticator: SharedAuthenticator, audit: SharedAudit) -> Self {
        Self { authenticator, audit }
    }
}

/// The browser cookie carrying the access JWT (set by the login surface, `hq-web-extras.1`;
/// also what the SSE `/stream` reads). A same-origin SSR frontend cannot put an httpOnly cookie
/// into an `Authorization` header from JS, so this is the access-token fallback for `/api/v1/*`.
const ACCESS_COOKIE: &str = "gt_web_token";

/// Read the access token from the `gt_web_token` cookie (`hq-web-extras.8`). The `Cookie` header
/// is `name=value` pairs split on `;`; the token is a JWT (no cookie-special chars), so a split
/// is sufficient. `None` when the cookie is absent.
fn cookie_token(req: &Request) -> Option<&str> {
    req.headers()
        .get(COOKIE)?
        .to_str()
        .ok()?
        .split(';')
        .find_map(|kv| {
            let (k, v) = kv.split_once('=')?;
            (k.trim() == ACCESS_COOKIE)
                .then(|| v.trim())
                .filter(|t| !t.is_empty())
        })
}

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
/// Wire it with [`axum::middleware::from_fn_with_state`] passing an [`AuthState`] as the
/// state. See the module docs for the contract. A token rejection (`401`) is the one denial
/// this layer produces itself — it short-circuits before any downstream layer — so it audits
/// that denial inline (`hq-auth-guard.3`); the scope-guard denials are audited downstream by
/// [`audit_denials`](crate::denial_audit::audit_denials).
pub async fn authenticate(
    State(state): State<AuthState>,
    mut req: Request,
    next: Next,
) -> Response {
    match bearer(&req) {
        // No Authorization header: fall back to the `gt_web_token` cookie so a same-origin browser
        // authenticates `/api/v1/*` with its httpOnly access cookie (hq-web-extras.8) — the JS
        // cannot set the header itself. A present-but-invalid cookie falls through as anonymous
        // (NOT a 401): a stale cookie must never block a public route like `/auth/login`, and the
        // per-route scope guard still rejects protected routes. Only an explicit bad `Authorization`
        // header is a hard client error (below).
        None => {
            let claims =
                cookie_token(&req).and_then(|t| state.authenticator.authenticate(t).ok());
            if let Some(claims) = claims {
                req.extensions_mut()
                    .insert(WorkspaceClaim(claims.workspace.clone()));
                req.extensions_mut().insert(claims);
            }
            next.run(req).await
        }
        // Present but not a well-formed bearer token.
        Some(Err(())) => reject(&state, &req, "malformed Authorization header"),
        Some(Ok(token)) => match state.authenticator.authenticate(token) {
            Ok(claims) => {
                // WorkspaceClaim first (the slug the tenant extractor falls back to), then the
                // full claims for the scope guard.
                req.extensions_mut()
                    .insert(WorkspaceClaim(claims.workspace.clone()));
                req.extensions_mut().insert(claims);
                next.run(req).await
            }
            Err(_) => reject(&state, &req, "invalid bearer token"),
        },
    }
}

/// Audit a token-rejection denial (`401`, *unauthenticated* — no verified identity yet) and
/// return the `401` response. The actor is [`ANONYMOUS`]: a credential we could not verify
/// names no one, and no scope was ever resolved.
fn reject(state: &AuthState, req: &Request, msg: &str) -> Response {
    record_denial(
        state.audit.as_ref(),
        ANONYMOUS,
        None,
        req.method(),
        req.uri(),
        None,
        StatusCode::UNAUTHORIZED,
    );
    unauthorized(msg).into_response()
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
    use gt_audit::{InMemoryAudit, Outcome};
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

    fn app(state: AuthState) -> Router {
        Router::new()
            .route("/", get(probe))
            .layer(axum::middleware::from_fn_with_state(state, authenticate))
    }

    async fn call(state: AuthState, header: Option<&str>) -> (StatusCode, String) {
        let mut builder = Request::builder().uri("/");
        if let Some(h) = header {
            builder = builder.header(AUTHORIZATION, h);
        }
        let resp = app(state)
            .oneshot(builder.body(Body::empty()).unwrap())
            .await
            .unwrap();
        let status = resp.status();
        let bytes = axum::body::to_bytes(resp.into_body(), usize::MAX).await.unwrap();
        (status, String::from_utf8(bytes.to_vec()).unwrap())
    }

    /// An [`AuthState`] over the `good` token plus a fresh in-memory sink, returned so the
    /// test can inspect what denials the auth layer recorded.
    fn state() -> (AuthState, SharedAudit) {
        let audit: SharedAudit = Arc::new(InMemoryAudit::new());
        let verifier: SharedAuthenticator =
            Arc::new(InMemoryAuthenticator::new().with_token("good", claims()));
        (AuthState::new(verifier, audit.clone()), audit)
    }

    #[tokio::test]
    async fn a_valid_token_injects_claims_and_workspace() {
        let (st, audit) = state();
        let (status, body) = call(st, Some("Bearer good")).await;
        assert_eq!(status, StatusCode::OK);
        assert!(body.contains(r#"claims=Some("alice")"#), "{body}");
        assert!(body.contains(r#"ws=Some("acme")"#), "{body}");
        assert!(audit.read_all().unwrap().is_empty(), "a valid token is not a denial");
    }

    #[tokio::test]
    async fn the_bearer_scheme_is_case_insensitive() {
        let (st, _audit) = state();
        let (status, body) = call(st, Some("bearer good")).await;
        assert_eq!(status, StatusCode::OK);
        assert!(body.contains(r#"claims=Some("alice")"#), "{body}");
    }

    #[tokio::test]
    async fn no_header_passes_through_anonymously() {
        let (st, audit) = state();
        let (status, body) = call(st, None).await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(body, "claims=None ws=None");
        assert!(audit.read_all().unwrap().is_empty(), "anonymous pass-through is not a denial");
    }

    async fn call_cookie(state: AuthState, cookie: &str) -> (StatusCode, String) {
        let resp = app(state)
            .oneshot(
                Request::builder()
                    .uri("/")
                    .header(COOKIE, cookie)
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        let status = resp.status();
        let bytes = axum::body::to_bytes(resp.into_body(), usize::MAX).await.unwrap();
        (status, String::from_utf8(bytes.to_vec()).unwrap())
    }

    #[tokio::test]
    async fn a_valid_gt_web_token_cookie_injects_claims() {
        // hq-web-extras.8: the browser authenticates /api/v1/* with its httpOnly access cookie.
        let (st, audit) = state();
        let (status, body) = call_cookie(st, "gt_web_token=good").await;
        assert_eq!(status, StatusCode::OK);
        assert!(body.contains(r#"claims=Some("alice")"#), "{body}");
        assert!(body.contains(r#"ws=Some("acme")"#), "{body}");
        assert!(audit.read_all().unwrap().is_empty(), "cookie auth is not a denial");
    }

    #[tokio::test]
    async fn an_invalid_cookie_passes_through_anonymously() {
        // A stale/invalid cookie must NOT 401 — it falls through anonymous so a public route
        // (e.g. /auth/login) still works; the per-route scope guard rejects protected ones.
        let (st, audit) = state();
        let (status, body) = call_cookie(st, "gt_web_token=stale").await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(body, "claims=None ws=None");
        assert!(audit.read_all().unwrap().is_empty(), "an invalid cookie is not an audited denial");
    }

    #[tokio::test]
    async fn an_invalid_token_is_unauthorized_and_audited() {
        let (st, audit) = state();
        let (status, _) = call(st, Some("Bearer nope")).await;
        assert_eq!(status, StatusCode::UNAUTHORIZED);
        let rows = audit.read_all().unwrap();
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].outcome, Outcome::Unauthorized);
        assert_eq!(rows[0].actor, ANONYMOUS);
        assert_eq!(rows[0].args["verdict"], "unauthenticated");
    }

    #[tokio::test]
    async fn a_non_bearer_authorization_is_unauthorized_and_audited() {
        let (st, audit) = state();
        let (status, _) = call(st, Some("Basic abc123")).await;
        assert_eq!(status, StatusCode::UNAUTHORIZED);
        assert_eq!(audit.read_all().unwrap().len(), 1);
    }

    #[tokio::test]
    async fn an_empty_bearer_token_is_unauthorized_and_audited() {
        let (st, audit) = state();
        let (status, _) = call(st, Some("Bearer ")).await;
        assert_eq!(status, StatusCode::UNAUTHORIZED);
        assert_eq!(audit.read_all().unwrap().len(), 1);
    }
}
