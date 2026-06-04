//! The HTTP auth endpoints (`hq-auth-routes.1`): `POST /auth/login`, `/auth/refresh`,
//! `/auth/logout`, and `GET /auth/me`.
//!
//! This is the off-by-default `axum` driving adapter of the ports this crate owns — the HTTP
//! surface in front of [`LoginProvider`] (identity), [`JwtMinter`] (access tokens), and
//! [`RefreshStore`] (refresh tokens). It folds into `gt-auth` behind the `axum` feature rather
//! than a sibling crate (docs/03 Rule 4), exactly as the verify/mint/password adapters do.
//!
//! The composition root supplies the concrete pieces in an [`AuthState`] and mounts
//! [`auth_router`]; the clock is injected (`AuthState::now`) so the slice is deterministic under
//! test. The router authenticates and issues tokens; it never *authorizes* a downstream
//! route — that is the per-route scope guard's job.
//!
//! ## Flow
//!
//! - **login** — `{email, password}` → [`LoginProvider::login`] → a `VerifiedIdentity` →
//!   [`into_claims`](VerifiedIdentity::into_claims) → [`JwtMinter::mint`] for the short-lived
//!   access JWT, plus a long-lived refresh token from [`RefreshStore::issue`] carrying the
//!   granted scopes.
//! - **refresh** — `{refresh_token}` → [`RefreshStore::rotate`] (rotation + reuse detection) →
//!   re-mint an access token from the record's `sub`/`workspace`/`scopes`.
//! - **logout** — `{refresh_token}` → [`RefreshStore::revoke_by_token`] (idempotent).
//! - **me** — echo the verified [`JwtClaims`] the auth middleware injected; no claims ⇒ `401`.

use std::sync::Arc;

use async_trait::async_trait;
use axum::extract::{Extension, State};
use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use axum::routing::{get, post};
use axum::{Json, Router};
use serde::{Deserialize, Serialize};

use crate::{
    AuthError, Credentials, JwtClaims, JwtMinter, RefreshError, RefreshStore, RefreshToken,
    VerifiedIdentity,
};

/// The async login port — the I/O-bound sibling of the synchronous
/// [`IdentityProvider`](crate::IdentityProvider). A real store (e.g. `PgUsers`) authenticates
/// against a database, so the endpoint needs an `async` boundary the CPU-only sync port lacks.
#[async_trait]
pub trait LoginProvider: Send + Sync {
    /// Authenticate `creds` into a [`VerifiedIdentity`], or [`AuthError::InvalidCredentials`].
    async fn login(&self, creds: &Credentials) -> Result<VerifiedIdentity, AuthError>;
}

/// The Postgres login store is the production [`LoginProvider`]: it already exposes an inherent
/// async `authenticate`, so this just adapts it to the port (available when both `axum` and `pg`
/// are on).
#[cfg(feature = "pg")]
#[async_trait]
impl LoginProvider for crate::PgUsers {
    async fn login(&self, creds: &Credentials) -> Result<VerifiedIdentity, AuthError> {
        self.authenticate(creds).await
    }
}

/// An injected wall clock: seconds since the Unix epoch. Kept abstract so tests pin time.
pub type Clock = Arc<dyn Fn() -> u64 + Send + Sync>;

/// Everything the auth endpoints need, supplied by the composition root.
#[derive(Clone)]
pub struct AuthState {
    /// The login step (identity verification).
    pub login: Arc<dyn LoginProvider>,
    /// The RS256 access-token minter.
    pub minter: Arc<JwtMinter>,
    /// The refresh-token store (rotation, reuse detection, revocation).
    pub refresh: Arc<dyn RefreshStore + Send + Sync>,
    /// Access-token lifetime in seconds (short).
    pub access_ttl: u64,
    /// Refresh-token lifetime in seconds (long).
    pub refresh_ttl: u64,
    /// Injected clock.
    pub now: Clock,
}

/// Build the auth router. Mount it under the API base at the composition root.
pub fn auth_router(state: AuthState) -> Router {
    Router::new()
        .route("/auth/login", post(login))
        .route("/auth/refresh", post(refresh))
        .route("/auth/logout", post(logout))
        .route("/auth/me", get(me))
        .with_state(state)
}

// --- request/response DTOs --------------------------------------------------------------------

/// `POST /auth/login` body.
#[derive(Debug, Deserialize)]
pub struct LoginRequest {
    /// The principal's email address.
    pub email: String,
    /// The plaintext password presented this request.
    pub password: String,
}

/// A minted token pair, returned by login and refresh.
#[derive(Debug, Serialize)]
pub struct TokenResponse {
    /// The short-lived RS256 access JWT (sent as `Authorization: Bearer`).
    pub access_token: String,
    /// The long-lived opaque refresh token (exchanged at `/auth/refresh`).
    pub refresh_token: String,
    /// Always `"Bearer"`.
    pub token_type: &'static str,
    /// Access-token lifetime in seconds.
    pub expires_in: u64,
}

/// `POST /auth/refresh` / `POST /auth/logout` body.
#[derive(Debug, Deserialize)]
pub struct RefreshRequest {
    /// The opaque refresh token previously issued.
    pub refresh_token: String,
}

/// `GET /auth/me` body — the verified identity behind the bearer token.
#[derive(Debug, Serialize)]
pub struct MeResponse {
    /// Authenticated principal.
    pub sub: String,
    /// Tenant the token is scoped to.
    pub workspace: String,
    /// Granted authorization scopes.
    pub scopes: Vec<String>,
}

// --- handlers ---------------------------------------------------------------------------------

async fn login(
    State(state): State<AuthState>,
    Json(body): Json<LoginRequest>,
) -> Result<Json<TokenResponse>, AuthError> {
    let creds = Credentials::EmailPassword {
        email: body.email,
        password: body.password,
    };
    let identity = state.login.login(&creds).await?;
    Ok(Json(issue_tokens(&state, identity.sub, identity.workspace, identity.scopes)?))
}

async fn refresh(
    State(state): State<AuthState>,
    Json(body): Json<RefreshRequest>,
) -> Result<Json<TokenResponse>, ApiError> {
    let now = (state.now)();
    let token = RefreshToken::new(body.refresh_token);
    let (next, record) = state.refresh.rotate(&token, now)?;
    // Re-mint a faithful access token from the record's carried scopes.
    let claims = JwtClaims {
        sub: record.sub,
        workspace: record.workspace,
        scopes: record.scopes,
        exp: now + state.access_ttl,
        nbf: None,
        iat: now,
    };
    let access_token = state.minter.mint(&claims)?;
    Ok(Json(TokenResponse {
        access_token,
        refresh_token: next.as_str().to_owned(),
        token_type: "Bearer",
        expires_in: state.access_ttl,
    }))
}

async fn logout(State(state): State<AuthState>, Json(body): Json<RefreshRequest>) -> StatusCode {
    // Idempotent: revoking an unknown token is a no-op, so logout always succeeds and cannot
    // probe which tokens exist.
    state
        .refresh
        .revoke_by_token(&RefreshToken::new(body.refresh_token));
    StatusCode::NO_CONTENT
}

async fn me(claims: Option<Extension<JwtClaims>>) -> Result<Json<MeResponse>, ApiError> {
    let Extension(claims) = claims.ok_or(ApiError::Unauthenticated)?;
    Ok(Json(MeResponse {
        sub: claims.sub,
        workspace: claims.workspace,
        scopes: claims.scopes,
    }))
}

/// Mint an access + refresh token pair for a freshly verified identity.
fn issue_tokens(
    state: &AuthState,
    sub: String,
    workspace: String,
    scopes: Vec<String>,
) -> Result<TokenResponse, AuthError> {
    let now = (state.now)();
    let identity = VerifiedIdentity {
        sub: sub.clone(),
        workspace: workspace.clone(),
        scopes: scopes.clone(),
    };
    let claims = identity.into_claims(now + state.access_ttl, now);
    let access_token = state.minter.mint(&claims)?;
    let (refresh_token, _record) =
        state
            .refresh
            .issue(&sub, &workspace, &scopes, now, now + state.refresh_ttl);
    Ok(TokenResponse {
        access_token,
        refresh_token: refresh_token.as_str().to_owned(),
        token_type: "Bearer",
        expires_in: state.access_ttl,
    })
}

// --- error mapping ----------------------------------------------------------------------------

/// `401` for a rejected credential/token, `500` for a server-side signing/storage fault.
impl IntoResponse for AuthError {
    fn into_response(self) -> Response {
        let status = match self {
            AuthError::InvalidCredentials
            | AuthError::Expired
            | AuthError::NotYetValid
            | AuthError::InvalidSignature
            | AuthError::Malformed(_)
            | AuthError::UnknownToken
            | AuthError::UnknownKey(_)
            | AuthError::MissingWorkspace => StatusCode::UNAUTHORIZED,
            AuthError::UnsupportedProvider(_) => StatusCode::BAD_REQUEST,
            AuthError::HashFailure(_)
            | AuthError::SigningFailure(_)
            | AuthError::Backend(_) => StatusCode::INTERNAL_SERVER_ERROR,
        };
        (status, self.to_string()).into_response()
    }
}

/// The endpoints' error surface — a refresh rejection, an auth fault, or a missing identity.
#[derive(Debug)]
enum ApiError {
    /// A refresh-token rotation/validation failure (`401`).
    Refresh(RefreshError),
    /// An authentication/signing fault (delegates to [`AuthError`]'s mapping).
    Auth(AuthError),
    /// `GET /auth/me` with no verified claims in the request — `401`.
    Unauthenticated,
}

impl From<RefreshError> for ApiError {
    fn from(e: RefreshError) -> Self {
        ApiError::Refresh(e)
    }
}

impl From<AuthError> for ApiError {
    fn from(e: AuthError) -> Self {
        ApiError::Auth(e)
    }
}

impl IntoResponse for ApiError {
    fn into_response(self) -> Response {
        match self {
            // Every refresh failure (unknown / expired / reused / revoked) is a 401: the client
            // must re-authenticate. The reuse case has already burned the family server-side.
            ApiError::Refresh(e) => (StatusCode::UNAUTHORIZED, e.to_string()).into_response(),
            ApiError::Auth(e) => e.into_response(),
            ApiError::Unauthenticated => {
                (StatusCode::UNAUTHORIZED, "missing or invalid bearer token").into_response()
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::InMemoryRefreshStore;
    use axum::body::Body;
    use axum::http::Request;
    use tower::ServiceExt; // oneshot

    // A throwaway RS256 keypair for minting + (in the middleware tests upstream) verifying.
    const PRIV_PEM: &[u8] = include_bytes!("../tests/fixtures/rs256_priv.pem");

    /// In-memory login double: one known user, everything else is invalid credentials.
    struct OneUser;
    #[async_trait]
    impl LoginProvider for OneUser {
        async fn login(&self, creds: &Credentials) -> Result<VerifiedIdentity, AuthError> {
            match creds {
                Credentials::EmailPassword { email, password }
                    if email == "alice@acme.test" && password == "hunter2" =>
                {
                    Ok(VerifiedIdentity {
                        sub: "alice".into(),
                        workspace: "acme".into(),
                        scopes: vec!["rig.read".into()],
                    })
                }
                _ => Err(AuthError::InvalidCredentials),
            }
        }
    }

    fn state() -> AuthState {
        AuthState {
            login: Arc::new(OneUser),
            minter: Arc::new(JwtMinter::from_rsa_pem(PRIV_PEM).unwrap().with_kid("k1")),
            refresh: Arc::new(InMemoryRefreshStore::new()),
            access_ttl: 900,
            refresh_ttl: 1_209_600,
            now: Arc::new(|| 1_000_000_000),
        }
    }

    async fn post(app: &Router, path: &str, json: &str) -> (StatusCode, String) {
        let resp = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri(path)
                    .header("content-type", "application/json")
                    .body(Body::from(json.to_owned()))
                    .unwrap(),
            )
            .await
            .unwrap();
        let status = resp.status();
        let bytes = axum::body::to_bytes(resp.into_body(), usize::MAX).await.unwrap();
        (status, String::from_utf8(bytes.to_vec()).unwrap())
    }

    fn token_pair(body: &str) -> (String, String) {
        let v: serde_json::Value = serde_json::from_str(body).unwrap();
        (
            v["access_token"].as_str().unwrap().to_owned(),
            v["refresh_token"].as_str().unwrap().to_owned(),
        )
    }

    #[tokio::test]
    async fn login_issues_a_token_pair_for_valid_credentials() {
        let app = auth_router(state());
        let (status, body) = post(
            &app,
            "/auth/login",
            r#"{"email":"alice@acme.test","password":"hunter2"}"#,
        )
        .await;
        assert_eq!(status, StatusCode::OK);
        let (access, refresh) = token_pair(&body);
        assert!(!access.is_empty() && !refresh.is_empty());
        assert!(body.contains(r#""token_type":"Bearer""#));
    }

    #[tokio::test]
    async fn login_rejects_bad_credentials_with_401() {
        let app = auth_router(state());
        let (status, _) = post(
            &app,
            "/auth/login",
            r#"{"email":"alice@acme.test","password":"wrong"}"#,
        )
        .await;
        assert_eq!(status, StatusCode::UNAUTHORIZED);
    }

    #[tokio::test]
    async fn refresh_rotates_the_token_and_remints_an_access_token() {
        let app = auth_router(state());
        let (_, login_body) = post(
            &app,
            "/auth/login",
            r#"{"email":"alice@acme.test","password":"hunter2"}"#,
        )
        .await;
        let (_, refresh1) = token_pair(&login_body);

        let (status, body) = post(&app, "/auth/refresh", &format!(r#"{{"refresh_token":"{refresh1}"}}"#)).await;
        assert_eq!(status, StatusCode::OK);
        let (_, refresh2) = token_pair(&body);
        assert_ne!(refresh1, refresh2); // rotated

        // Reusing the now-rotated first token is rejected (and burns the family).
        let (status, _) = post(&app, "/auth/refresh", &format!(r#"{{"refresh_token":"{refresh1}"}}"#)).await;
        assert_eq!(status, StatusCode::UNAUTHORIZED);
    }

    #[tokio::test]
    async fn logout_revokes_the_refresh_family() {
        let app = auth_router(state());
        let (_, login_body) = post(
            &app,
            "/auth/login",
            r#"{"email":"alice@acme.test","password":"hunter2"}"#,
        )
        .await;
        let (_, refresh) = token_pair(&login_body);

        let (status, _) = post(&app, "/auth/logout", &format!(r#"{{"refresh_token":"{refresh}"}}"#)).await;
        assert_eq!(status, StatusCode::NO_CONTENT);

        // The revoked token can no longer be refreshed.
        let (status, _) = post(&app, "/auth/refresh", &format!(r#"{{"refresh_token":"{refresh}"}}"#)).await;
        assert_eq!(status, StatusCode::UNAUTHORIZED);
    }

    #[tokio::test]
    async fn me_echoes_injected_claims_and_401s_without_them() {
        let app = auth_router(state());

        // With claims in the extensions (as the auth middleware would inject).
        let claims = JwtClaims {
            sub: "alice".into(),
            workspace: "acme".into(),
            scopes: vec!["rig.read".into()],
            exp: 2_000_000_000,
            nbf: None,
            iat: 0,
        };
        let mut req = Request::builder().uri("/auth/me").body(Body::empty()).unwrap();
        req.extensions_mut().insert(claims);
        let resp = app.clone().oneshot(req).await.unwrap();
        assert_eq!(resp.status(), StatusCode::OK);
        let bytes = axum::body::to_bytes(resp.into_body(), usize::MAX).await.unwrap();
        let body = String::from_utf8(bytes.to_vec()).unwrap();
        assert!(body.contains(r#""sub":"alice""#) && body.contains(r#""workspace":"acme""#));

        // Without claims → 401.
        let resp = app
            .oneshot(Request::builder().uri("/auth/me").body(Body::empty()).unwrap())
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::UNAUTHORIZED);
    }
}
