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
//! - **jwks** — publish the verifier's public RS256 keys (RFC 7517 JWK Set) so a frontend or
//!   sibling service verifies access tokens offline; never exposes the signing secret.

use std::sync::Arc;

use async_trait::async_trait;
use axum::extract::{Extension, State};
use axum::http::{header, HeaderMap, HeaderValue, StatusCode};
use axum::response::{IntoResponse, Response};
use axum::routing::{get, post};
use axum::{Json, Router};
use serde::{Deserialize, Serialize};

use crate::{
    AuthError, Credentials, JwkSet, JwtClaims, JwtMinter, RefreshError, RefreshStore, RefreshToken,
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

/// The `SameSite` attribute stamped on the auth cookies (`hq-web-extras.1`). `Lax` suits a
/// same-origin deploy; `None` is for a cross-site SSR frontend and requires `Secure` — browsers
/// silently drop a `SameSite=None` cookie that is not also `Secure`.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum SameSite {
    Strict,
    Lax,
    None,
}

impl SameSite {
    /// The wire token for the `SameSite=` cookie attribute.
    fn as_str(self) -> &'static str {
        match self {
            SameSite::Strict => "Strict",
            SameSite::Lax => "Lax",
            SameSite::None => "None",
        }
    }
}

/// Browser cookie carrying the access JWT. The SSE `/stream` endpoint reads it by this exact
/// name (`hq-fe-api-stream.1`) and an SSR frontend forwards it; `HttpOnly` keeps page JS out.
const ACCESS_COOKIE: &str = "gt_web_token";
/// Browser cookie carrying the opaque refresh token, scoped to `/auth` so it rides only the
/// refresh/logout calls, never the rest of the app. `HttpOnly`.
const REFRESH_COOKIE: &str = "gt_refresh";

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
    /// `Secure` flag on the auth cookies — `true` in any HTTPS deploy, and REQUIRED when
    /// `cookie_same_site` is [`SameSite::None`]. Set `false` only for plain-http local dev.
    pub cookie_secure: bool,
    /// `SameSite` attribute stamped on the auth cookies.
    pub cookie_same_site: SameSite,
    /// The verifier's public JWKS, served at `GET /auth/jwks` so clients verify access tokens
    /// offline. Built from the verifier's public keys at the composition root
    /// ([`JwtAuthenticator::jwk_set`](crate::JwtAuthenticator::jwk_set)) — never the signing
    /// secret. `Arc` so cloning the state per request stays cheap.
    pub jwks: Arc<JwkSet>,
}

/// Build the auth router. Mount it under the API base at the composition root.
pub fn auth_router(state: AuthState) -> Router {
    Router::new()
        .route("/auth/login", post(login))
        .route("/auth/refresh", post(refresh))
        .route("/auth/logout", post(logout))
        .route("/auth/me", get(me))
        .route("/auth/jwks", get(jwks))
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

/// `POST /auth/refresh` / `POST /auth/logout` body. Optional: a browser supplies the refresh
/// token through the `gt_refresh` httpOnly cookie instead, so the JSON body may be absent.
#[derive(Debug, Default, Deserialize)]
pub struct RefreshRequest {
    /// The opaque refresh token previously issued. Absent ⇒ read from the cookie.
    pub refresh_token: Option<String>,
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
) -> Result<(HeaderMap, Json<TokenResponse>), AuthError> {
    let creds = Credentials::EmailPassword {
        email: body.email,
        password: body.password,
    };
    let identity = state.login.login(&creds).await?;
    let tokens = issue_tokens(&state, identity.sub, identity.workspace, identity.scopes)?;
    // Set httpOnly cookies for the browser (SSE + refresh) while still returning the JSON body
    // for non-browser clients (`hq-web-extras.1`).
    Ok((set_token_cookies(&state, &tokens), Json(tokens)))
}

async fn refresh(
    State(state): State<AuthState>,
    headers: HeaderMap,
    body: Option<Json<RefreshRequest>>,
) -> Result<(HeaderMap, Json<TokenResponse>), ApiError> {
    // Prefer the httpOnly cookie (browser); fall back to the JSON body (non-browser clients).
    let token = refresh_token_from(&headers, body).ok_or(ApiError::Unauthenticated)?;
    let now = (state.now)();
    let (next, record) = state.refresh.rotate(&RefreshToken::new(token), now)?;
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
    let tokens = TokenResponse {
        access_token,
        refresh_token: next.as_str().to_owned(),
        token_type: "Bearer",
        expires_in: state.access_ttl,
    };
    Ok((set_token_cookies(&state, &tokens), Json(tokens)))
}

async fn logout(
    State(state): State<AuthState>,
    headers: HeaderMap,
    body: Option<Json<RefreshRequest>>,
) -> (HeaderMap, StatusCode) {
    // Idempotent: revoking an unknown/absent token is a no-op, so logout always succeeds and
    // cannot probe which tokens exist. Either source (cookie or body) is honoured, and the
    // cookies are cleared regardless.
    if let Some(token) = refresh_token_from(&headers, body) {
        state.refresh.revoke_by_token(&RefreshToken::new(token));
    }
    (clear_token_cookies(&state), StatusCode::NO_CONTENT)
}

async fn me(claims: Option<Extension<JwtClaims>>) -> Result<Json<MeResponse>, ApiError> {
    let Extension(claims) = claims.ok_or(ApiError::Unauthenticated)?;
    Ok(Json(MeResponse {
        sub: claims.sub,
        workspace: claims.workspace,
        scopes: claims.scopes,
    }))
}

/// `GET /auth/jwks` — the verifier's public RS256 keys as an RFC 7517 JWK Set. A client matches a
/// token's header `kid` to a key here and verifies the signature offline. Returns `200` with
/// `{"keys":[]}` when no keys are configured (a valid, empty set — simpler for clients than a 404).
async fn jwks(State(state): State<AuthState>) -> Json<JwkSet> {
    Json(state.jwks.as_ref().clone())
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

// --- cookies (hq-web-extras.1 / .2) -----------------------------------------------------------

/// Resolve the refresh token from the request: the `gt_refresh` httpOnly cookie first (browser),
/// then the JSON body (non-browser clients). `None` ⇒ neither carried one.
fn refresh_token_from(headers: &HeaderMap, body: Option<Json<RefreshRequest>>) -> Option<String> {
    read_cookie(headers, REFRESH_COOKIE)
        .map(str::to_owned)
        .or_else(|| body.and_then(|Json(b)| b.refresh_token))
        .filter(|t| !t.is_empty())
}

/// Read a single cookie value out of the request's `Cookie` header. No external cookie crate:
/// the header is `name=value` pairs separated by `;`, and our tokens are JWT / opaque-base64
/// (no `;` or `,`), so a split is sufficient and unambiguous.
fn read_cookie<'a>(headers: &'a HeaderMap, name: &str) -> Option<&'a str> {
    headers
        .get(header::COOKIE)?
        .to_str()
        .ok()?
        .split(';')
        .find_map(|kv| {
            let (k, v) = kv.split_once('=')?;
            (k.trim() == name).then(|| v.trim())
        })
}

/// The `Set-Cookie` header pair stamped on login/refresh: the access JWT (path `/`, so the SSE
/// endpoint and the whole app see it) and the refresh token (path `/auth`, so it rides only the
/// refresh/logout calls). Both `HttpOnly`, with the configured `Secure`/`SameSite`.
fn set_token_cookies(state: &AuthState, tokens: &TokenResponse) -> HeaderMap {
    let mut headers = HeaderMap::new();
    headers.append(
        header::SET_COOKIE,
        build_cookie(state, ACCESS_COOKIE, &tokens.access_token, "/", state.access_ttl as i64),
    );
    headers.append(
        header::SET_COOKIE,
        build_cookie(state, REFRESH_COOKIE, &tokens.refresh_token, "/auth", state.refresh_ttl as i64),
    );
    headers
}

/// The `Set-Cookie` header pair that expires both auth cookies (`Max-Age=0`, empty value) on
/// logout — same name/path as [`set_token_cookies`], which is what makes the browser drop them.
fn clear_token_cookies(state: &AuthState) -> HeaderMap {
    let mut headers = HeaderMap::new();
    headers.append(header::SET_COOKIE, build_cookie(state, ACCESS_COOKIE, "", "/", 0));
    headers.append(header::SET_COOKIE, build_cookie(state, REFRESH_COOKIE, "", "/auth", 0));
    headers
}

/// Build one `Set-Cookie` value. `value` is a JWT or opaque token (ASCII, no cookie-special
/// chars), so it needs no escaping.
fn build_cookie(state: &AuthState, name: &str, value: &str, path: &str, max_age: i64) -> HeaderValue {
    let mut s = format!(
        "{name}={value}; Path={path}; HttpOnly; Max-Age={max_age}; SameSite={}",
        state.cookie_same_site.as_str()
    );
    if state.cookie_secure {
        s.push_str("; Secure");
    }
    HeaderValue::from_str(&s).expect("cookie attributes + token are ASCII")
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
    use crate::{Authenticator, InMemoryRefreshStore, JwtAuthenticator};
    use axum::body::Body;
    use axum::http::Request;
    use jsonwebtoken::DecodingKey;
    use tower::ServiceExt; // oneshot

    // A throwaway RS256 keypair for minting + (in the middleware tests upstream) verifying.
    const PRIV_PEM: &[u8] = include_bytes!("../tests/fixtures/rs256_priv.pem");
    // Its public half — the JWKS the verifier publishes.
    const PUB_PEM: &[u8] = include_bytes!("../tests/fixtures/rs256_pub.pem");

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
            cookie_secure: true,
            cookie_same_site: SameSite::Lax,
            // Publish the public half of the same "k1" key the minter signs with.
            jwks: Arc::new(
                JwtAuthenticator::from_kid_pems([("k1", PUB_PEM)]).unwrap().jwk_set(),
            ),
        }
    }

    /// Collect the `Set-Cookie` values off a response, lowest-level helper for the cookie tests.
    async fn login_response(app: &Router) -> axum::response::Response {
        app.clone()
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/auth/login")
                    .header("content-type", "application/json")
                    .body(Body::from(
                        r#"{"email":"alice@acme.test","password":"hunter2"}"#,
                    ))
                    .unwrap(),
            )
            .await
            .unwrap()
    }

    async fn get(app: &Router, path: &str) -> (StatusCode, String) {
        let resp = app
            .clone()
            .oneshot(Request::builder().uri(path).body(Body::empty()).unwrap())
            .await
            .unwrap();
        let status = resp.status();
        let bytes = axum::body::to_bytes(resp.into_body(), usize::MAX).await.unwrap();
        (status, String::from_utf8(bytes.to_vec()).unwrap())
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

    #[tokio::test]
    async fn jwks_publishes_the_public_key_under_its_kid() {
        let app = auth_router(state());
        let (status, body) = get(&app, "/auth/jwks").await;
        assert_eq!(status, StatusCode::OK);
        let v: serde_json::Value = serde_json::from_str(&body).unwrap();
        let keys = v["keys"].as_array().unwrap();
        assert_eq!(keys.len(), 1);
        let k = &keys[0];
        assert_eq!(k["kid"], "k1"); // matches the minter's signing kid
        assert_eq!(k["kty"], "RSA");
        assert_eq!(k["alg"], "RS256");
        assert_eq!(k["use"], "sig");
        assert_eq!(k["e"], "AQAB");
        assert!(!k["n"].as_str().unwrap().is_empty());
    }

    #[tokio::test]
    async fn a_token_verifies_against_the_published_jwks() {
        // End-to-end: log in for a real access token, fetch the JWKS, rebuild a verifier from the
        // advertised `n`/`e`, and confirm it accepts the token — no shared secret.
        let app = auth_router(state());
        let (_, login_body) = post(
            &app,
            "/auth/login",
            r#"{"email":"alice@acme.test","password":"hunter2"}"#,
        )
        .await;
        let (access, _) = token_pair(&login_body);

        let (_, jwks_body) = get(&app, "/auth/jwks").await;
        let v: serde_json::Value = serde_json::from_str(&jwks_body).unwrap();
        let k = &v["keys"][0];
        let key =
            DecodingKey::from_rsa_components(k["n"].as_str().unwrap(), k["e"].as_str().unwrap())
                .unwrap();
        let rebuilt = JwtAuthenticator::empty().with_key_kid("k1", key);
        assert_eq!(rebuilt.authenticate(&access).unwrap().sub, "alice");
    }

    fn set_cookies(resp: &axum::response::Response) -> Vec<String> {
        resp.headers()
            .get_all(header::SET_COOKIE)
            .iter()
            .map(|v| v.to_str().unwrap().to_owned())
            .collect()
    }

    #[tokio::test]
    async fn login_sets_httponly_access_and_refresh_cookies() {
        let resp = login_response(&auth_router(state())).await;
        let cookies = set_cookies(&resp);
        let access = cookies.iter().find(|c| c.starts_with("gt_web_token=")).unwrap();
        let refresh = cookies.iter().find(|c| c.starts_with("gt_refresh=")).unwrap();
        // access cookie: httpOnly, site-wide, the configured flags.
        assert!(access.contains("HttpOnly") && access.contains("Path=/"));
        assert!(access.contains("SameSite=Lax") && access.contains("Secure"));
        // refresh cookie: httpOnly, scoped to /auth.
        assert!(refresh.contains("HttpOnly") && refresh.contains("Path=/auth"));
    }

    #[tokio::test]
    async fn refresh_reads_the_cookie_when_the_body_is_absent() {
        let app = auth_router(state());
        let login = login_response(&app).await;
        let refresh_cookie = set_cookies(&login)
            .into_iter()
            .find(|c| c.starts_with("gt_refresh="))
            .map(|c| c.split(';').next().unwrap().to_owned())
            .unwrap();

        // No JSON body — the token rides the cookie alone.
        let resp = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/auth/refresh")
                    .header(header::COOKIE, refresh_cookie)
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::OK);
        // And it re-sets a fresh, rotated refresh cookie.
        assert!(set_cookies(&resp).iter().any(|c| c.starts_with("gt_refresh=")));
    }

    #[tokio::test]
    async fn logout_clears_both_cookies() {
        let resp = auth_router(state())
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/auth/logout")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::NO_CONTENT);
        let cookies = set_cookies(&resp);
        assert!(cookies.iter().any(|c| c.starts_with("gt_web_token=") && c.contains("Max-Age=0")));
        assert!(cookies.iter().any(|c| c.starts_with("gt_refresh=") && c.contains("Max-Age=0")));
    }
}
