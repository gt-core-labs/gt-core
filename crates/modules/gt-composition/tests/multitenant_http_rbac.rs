//! Multi-tenant RBAC at the HTTP route level (`hq-auth-guard.4`).
//!
//! The sibling of [`multitenant_rbac`](super) one tier down: that test exercises
//! the MCP-server seams (scope strings, tenant-from-header) as pure functions.
//! This one drives the **full HTTP request path** end to end, through every layer
//! a real request crosses, with nothing mocked but the token table:
//!
//! ```text
//!   authenticate          (src/auth.rs, hq-auth-context.3)
//!     Bearer token -> verified JwtClaims + WorkspaceClaim into extensions
//!        |
//!   bridge_scopes         (src/scope_bridge.rs, hq-auth-guard.1)
//!     JwtClaims.scopes -> CallerScopes into extensions
//!        |
//!   guard_module_scopes   (gt-module routes.rs, hq-auth-guard.2)
//!     no CallerScopes -> 401; present but missing the scope -> 403
//!        |
//!   WorkspaceContext      (gt-workspace web.rs, hq-auth-context.2)
//!     header X-GT-Workspace vs WorkspaceClaim -> Mismatch 403; else the tenant
//!        |
//!   handler: serves ONLY the resolved tenant's data
//! ```
//!
//! The four guardrails asserted, each as a distinct HTTP outcome:
//! - (a) a valid token sees **only its own** workspace's data (isolation);
//! - (b) a token for A that names B in the `X-GT-Workspace` header is a tenant
//!   spoof → `403` Mismatch (a token never reaches another tenant's resource);
//! - (c) an authenticated token lacking the route's scope → `403`;
//! - (d) no token at all → `401`.
//!
//! Offline-safe: `InMemoryAuthenticator` stands in for RS256 verification, the
//! handler is a string map, and the request is driven in-process with
//! `tower::oneshot` — no socket, no PG.

use std::sync::Arc;

use axum::body::Body;
use axum::extract::Request;
use axum::http::{header::AUTHORIZATION, Method, StatusCode};
use axum::routing::get;
use axum::Router;
use tower::ServiceExt; // oneshot

use gt_audit::InMemoryAudit;
use gt_auth::{InMemoryAuthenticator, JwtClaims};
use gt_composition::auth::{authenticate, AuthState, SharedAuthenticator};
use gt_composition::denial_audit::SharedAudit;
use gt_composition::scope_bridge::bridge_scopes;
use gt_module::{guard_module_scopes, module_prefix, ModuleId, Scope};
use gt_workspace::{WorkspaceContext, WORKSPACE_HEADER};

/// The dummy module guarded by this test: a `vault` whose single read route
/// requires `vault.read` and serves the calling tenant's secret.
const MODULE: &str = "vault";
const READ_SCOPE: &str = "vault.read";

/// Claims for `ws` carrying `scopes`, far-future expiry so validity never bites.
fn claims(ws: &str, scopes: &[&str]) -> JwtClaims {
    JwtClaims {
        sub: "user".into(),
        workspace: ws.into(),
        scopes: scopes.iter().map(|s| s.to_string()).collect(),
        exp: 9_999_999_999,
        nbf: None,
        iat: 0,
    }
}

/// The protected handler. It takes the workspace ONLY through [`WorkspaceContext`]
/// — the sanctioned, spoof-proof channel (docs/04 rule 15) — and returns that
/// tenant's secret. A handler written this way cannot read the tenant from the
/// path or body, so isolation is structural: the token decides what it sees.
async fn read_vault(ctx: WorkspaceContext) -> String {
    let ws = ctx.workspace().as_str();
    let secret = match ws {
        "acme" => "secret-acme",
        "beta" => "secret-beta",
        other => return format!("ws={other} data=none"),
    };
    format!("ws={ws} data={secret}")
}

/// The full router: the guarded `vault` module mounted at its real
/// `/api/v1/vault` prefix, wrapped by the scope bridge and the authenticator so a
/// request crosses every production layer in order.
fn app() -> Router {
    let id = ModuleId::new(MODULE).unwrap();
    let inner = Router::new().route("/data", get(read_vault));
    let guarded = guard_module_scopes(inner, &id, &[Scope::new(READ_SCOPE).unwrap()]);
    let routed = Router::new().nest(&module_prefix(&id), guarded);

    let audit: SharedAudit = Arc::new(InMemoryAudit::new());
    let verifier: SharedAuthenticator = Arc::new(
        InMemoryAuthenticator::new()
            // A and B: each holds vault.read, each scoped to its own tenant.
            .with_token("tok-a", claims("acme", &[READ_SCOPE]))
            .with_token("tok-b", claims("beta", &[READ_SCOPE]))
            // Authenticated for A but holding no usable scope → 403 at the guard.
            .with_token("tok-a-noscope", claims("acme", &[])),
    );
    let state = AuthState::new(verifier, audit);

    // Layers run outermost-first (last `.layer` wraps first): authenticate, then
    // bridge_scopes, then the per-module guard, then the handler.
    routed
        .layer(axum::middleware::from_fn(bridge_scopes))
        .layer(axum::middleware::from_fn_with_state(state, authenticate))
}

/// Drive `GET /api/v1/vault/data` with an optional bearer token and an optional
/// `X-GT-Workspace` header; return the status and body.
async fn call(token: Option<&str>, ws_header: Option<&str>) -> (StatusCode, String) {
    let mut builder = Request::builder()
        .method(Method::GET)
        .uri("/api/v1/vault/data");
    if let Some(t) = token {
        builder = builder.header(AUTHORIZATION, format!("Bearer {t}"));
    }
    if let Some(w) = ws_header {
        builder = builder.header(WORKSPACE_HEADER, w);
    }
    let resp = app()
        .oneshot(builder.body(Body::empty()).unwrap())
        .await
        .unwrap();
    let status = resp.status();
    let bytes = axum::body::to_bytes(resp.into_body(), usize::MAX).await.unwrap();
    (status, String::from_utf8(bytes.to_vec()).unwrap())
}

// (a) isolation — a valid token sees only its own tenant's data ----------------

#[tokio::test]
async fn token_for_a_sees_only_a_data() {
    let (status, body) = call(Some("tok-a"), None).await;
    assert_eq!(status, StatusCode::OK);
    assert!(body.contains("ws=acme"), "{body}");
    assert!(body.contains("secret-acme"), "{body}");
    // The crux of isolation: A's token never surfaces B's secret.
    assert!(!body.contains("secret-beta"), "A must not see B's data: {body}");
}

#[tokio::test]
async fn token_for_b_sees_only_b_data() {
    let (status, body) = call(Some("tok-b"), None).await;
    assert_eq!(status, StatusCode::OK);
    assert!(body.contains("ws=beta"), "{body}");
    assert!(body.contains("secret-beta"), "{body}");
    assert!(!body.contains("secret-acme"), "B must not see A's data: {body}");
}

#[tokio::test]
async fn header_agreeing_with_the_token_resolves_normally() {
    // A redundant-but-consistent X-GT-Workspace header is fine: same tenant.
    let (status, body) = call(Some("tok-a"), Some("acme")).await;
    assert_eq!(status, StatusCode::OK);
    assert!(body.contains("secret-acme"), "{body}");
}

// (b) cross-tenant spoof — A's token naming B in the header is a 403 -----------

#[tokio::test]
async fn token_for_a_naming_b_in_the_header_is_forbidden() {
    // The token authorizes A; the header asserts B. That disagreement is a
    // tenant-spoof attempt, rejected 403 Mismatch — A can never reach B's data
    // by switching the header.
    let (status, body) = call(Some("tok-a"), Some("beta")).await;
    assert_eq!(status, StatusCode::FORBIDDEN);
    assert!(body.contains("disagrees"), "expected a Mismatch body, got: {body}");
    // And of course B's secret never appears in the rejection.
    assert!(!body.contains("secret-beta"), "{body}");
}

// (c) insufficient scope — authenticated but unauthorized is a 403 -------------

#[tokio::test]
async fn authenticated_without_the_scope_is_forbidden() {
    // tok-a-noscope authenticates (so CallerScopes is present-but-empty) yet
    // holds no vault.read → the kernel guard answers 403, not 401.
    let (status, _) = call(Some("tok-a-noscope"), None).await;
    assert_eq!(status, StatusCode::FORBIDDEN);
}

// (d) no credential — anonymous on a guarded route is a 401 --------------------

#[tokio::test]
async fn no_token_is_unauthorized() {
    // No Authorization header → no CallerScopes inserted → the guard reads the
    // absence as unauthenticated and answers 401 (distinct from the 403 above).
    let (status, _) = call(None, None).await;
    assert_eq!(status, StatusCode::UNAUTHORIZED);
}

#[tokio::test]
async fn an_invalid_token_is_unauthorized() {
    // A present-but-unrecognized credential is an explicit auth failure (401),
    // short-circuited by the auth middleware before any guard runs.
    let (status, _) = call(Some("bogus"), None).await;
    assert_eq!(status, StatusCode::UNAUTHORIZED);
}
