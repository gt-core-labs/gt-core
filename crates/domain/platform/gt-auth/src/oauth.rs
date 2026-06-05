//! Real OAuth 2.0 / OpenID Connect login adapter — the network-I/O sibling of the
//! email+password [`IdentityProvider`](crate::IdentityProvider) (hq-platform-hardening.3).
//!
//! [`ProviderKind`](crate::ProviderKind) has long reserved `OAuth`/`Oidc` in the enum while
//! their adapters stayed [`AuthError::UnsupportedProvider`] stubs. This module lands the real
//! adapter: it runs the authorization-code handshake against a configured identity provider
//! (IdP) — exchange the `code` at the token endpoint, then read the userinfo endpoint — and
//! maps the resulting upstream identity onto a [`VerifiedIdentity`], exactly the value the
//! email+password path produces. The login tier then folds it into the SAME access/refresh
//! pair (docs/03 — adapter beside the port, never a sibling crate).
//!
//! Gated behind the off-by-default `oauth` feature so the core build stays HTTP-client-free
//! (`reqwest` is pulled only when a binary actually serves the OAuth/OIDC login path), the same
//! way the `pg`/`password-hash` adapters gate their stacks.
//!
//! ## Why async (mirrors [`AsyncRefreshStore`](crate::AsyncRefreshStore))
//!
//! The synchronous [`IdentityProvider`] port suits CPU-only password verification; an OAuth
//! handshake is two network round-trips, so this adapter implements the **async**
//! [`LoginProvider`](crate::LoginProvider) port that bead `.1` established for I/O-capable login
//! — never a `block_on` over a sync trait.
//!
//! ## Config (env contract)
//!
//! [`OidcConfig::from_env`] reads, all required unless noted:
//! - `GT_OIDC_ISSUER` — the issuer URL (`iss`), matched against `Credentials::Oidc.issuer`.
//! - `GT_OIDC_CLIENT_ID` / `GT_OIDC_CLIENT_SECRET` — the registered client credentials.
//! - `GT_OIDC_TOKEN_ENDPOINT` — where the authorization `code` is exchanged for an access token.
//! - `GT_OIDC_USERINFO_ENDPOINT` — the OIDC userinfo endpoint read with that access token.
//! - `GT_OIDC_REDIRECT_URI` — the redirect URI registered with the IdP (echoed in the exchange).
//! - `GT_OIDC_WORKSPACE` — the tenant the resolved identity is scoped to (the `workspace` claim).
//! - `GT_OIDC_SCOPES` (optional) — comma-separated scopes granted to a successful login.

use async_trait::async_trait;
use serde::Deserialize;

use crate::{AuthError, Credentials, LoginProvider, VerifiedIdentity};

/// Env var names for [`OidcConfig::from_env`] — public so the composition root can document the
/// contract without re-deriving the strings.
pub const ENV_ISSUER: &str = "GT_OIDC_ISSUER";
/// See [`ENV_ISSUER`].
pub const ENV_CLIENT_ID: &str = "GT_OIDC_CLIENT_ID";
/// See [`ENV_ISSUER`].
pub const ENV_CLIENT_SECRET: &str = "GT_OIDC_CLIENT_SECRET";
/// See [`ENV_ISSUER`].
pub const ENV_TOKEN_ENDPOINT: &str = "GT_OIDC_TOKEN_ENDPOINT";
/// See [`ENV_ISSUER`].
pub const ENV_USERINFO_ENDPOINT: &str = "GT_OIDC_USERINFO_ENDPOINT";
/// See [`ENV_ISSUER`].
pub const ENV_REDIRECT_URI: &str = "GT_OIDC_REDIRECT_URI";
/// See [`ENV_ISSUER`].
pub const ENV_WORKSPACE: &str = "GT_OIDC_WORKSPACE";
/// See [`ENV_ISSUER`] — optional, comma-separated.
pub const ENV_SCOPES: &str = "GT_OIDC_SCOPES";

/// Static configuration for one OAuth/OIDC identity provider.
///
/// Holds the registered client credentials and the IdP's token/userinfo endpoints, plus the
/// tenant + scopes a successful login is scoped to. Built from the environment with
/// [`from_env`](Self::from_env) at the composition root, or directly in tests against a mock IdP.
#[derive(Clone, Debug)]
pub struct OidcConfig {
    /// The issuer URL (`iss`); a [`Credentials::Oidc`] login whose `issuer` does not match this
    /// is rejected before any network call.
    pub issuer: String,
    /// The registered client id, sent on the token exchange.
    pub client_id: String,
    /// The registered client secret, sent on the token exchange.
    pub client_secret: String,
    /// The token endpoint the authorization `code` is POSTed to.
    pub token_endpoint: String,
    /// The userinfo endpoint read with the access token from the exchange.
    pub userinfo_endpoint: String,
    /// The redirect URI registered with the IdP, echoed back in the exchange.
    pub redirect_uri: String,
    /// The tenant the resolved identity is scoped to (the `workspace` claim).
    pub workspace: String,
    /// Scopes granted to a successful login. Empty ⇒ a login that authenticates but reaches
    /// nothing (the deploy can layer role expansion on top later).
    pub scopes: Vec<String>,
}

impl OidcConfig {
    /// Read the configuration from the environment (the [`ENV_ISSUER`] family). Any required var
    /// missing or blank is [`AuthError::Backend`] — a misconfiguration, not a rejected login.
    /// `GT_OIDC_SCOPES` is optional (comma-separated; blank ⇒ no scopes).
    pub fn from_env() -> Result<Self, AuthError> {
        Ok(Self {
            issuer: req_env(ENV_ISSUER)?,
            client_id: req_env(ENV_CLIENT_ID)?,
            client_secret: req_env(ENV_CLIENT_SECRET)?,
            token_endpoint: req_env(ENV_TOKEN_ENDPOINT)?,
            userinfo_endpoint: req_env(ENV_USERINFO_ENDPOINT)?,
            redirect_uri: req_env(ENV_REDIRECT_URI)?,
            workspace: req_env(ENV_WORKSPACE)?,
            scopes: std::env::var(ENV_SCOPES)
                .ok()
                .into_iter()
                .flat_map(|v| {
                    v.split(',')
                        .map(str::trim)
                        .filter(|s| !s.is_empty())
                        .map(str::to_owned)
                        .collect::<Vec<_>>()
                })
                .collect(),
        })
    }
}

/// A required env var, trimmed; blank or absent is [`AuthError::Backend`].
fn req_env(name: &str) -> Result<String, AuthError> {
    match std::env::var(name) {
        Ok(v) if !v.trim().is_empty() => Ok(v.trim().to_owned()),
        _ => Err(AuthError::Backend(format!("missing OIDC config: {name}"))),
    }
}

/// The production OAuth/OIDC [`LoginProvider`]: it runs the authorization-code handshake against
/// the configured [`OidcConfig`] and maps the upstream identity onto a [`VerifiedIdentity`].
///
/// `OAuth` credentials carry the authorization `code` to exchange; `Oidc` credentials carry the
/// `id_token` minted by the issuer (the `issuer` is matched against the configured one, then the
/// userinfo endpoint resolves the principal). `EmailPassword` credentials are not this adapter's
/// job — they are [`AuthError::UnsupportedProvider`] (the composition root routes them to the
/// password/PgUsers provider instead).
pub struct OidcProvider {
    config: OidcConfig,
    http: reqwest::Client,
}

/// The token endpoint's response — only the fields the userinfo read needs.
#[derive(Deserialize)]
struct TokenResponse {
    access_token: String,
}

/// The userinfo endpoint's response — the OIDC standard `sub` plus the optional `email`/`name`.
/// `sub` is the stable principal id we carry onto [`VerifiedIdentity::sub`].
#[derive(Deserialize)]
struct UserInfo {
    sub: String,
}

impl OidcProvider {
    /// Build a provider over `config`, with a fresh reqwest client.
    pub fn new(config: OidcConfig) -> Result<Self, AuthError> {
        let http = reqwest::Client::builder()
            .build()
            .map_err(|e| AuthError::Backend(format!("oidc http client: {e}")))?;
        Ok(Self { config, http })
    }

    /// Build a provider over `config` reusing an existing reqwest client (e.g. one the
    /// composition root already pools).
    pub fn with_client(config: OidcConfig, http: reqwest::Client) -> Self {
        Self { config, http }
    }

    /// The configured issuer URL.
    pub fn issuer(&self) -> &str {
        &self.config.issuer
    }

    /// Exchange `code` at the token endpoint for an access token (authorization-code grant).
    async fn exchange_code(&self, code: &str) -> Result<String, AuthError> {
        let form = [
            ("grant_type", "authorization_code"),
            ("code", code),
            ("redirect_uri", self.config.redirect_uri.as_str()),
            ("client_id", self.config.client_id.as_str()),
            ("client_secret", self.config.client_secret.as_str()),
        ];
        let resp = self
            .http
            .post(&self.config.token_endpoint)
            .form(&form)
            .send()
            .await
            .map_err(|e| AuthError::Backend(format!("oidc token request: {e}")))?;
        // A 4xx from the token endpoint is the IdP rejecting the code — a failed login, not an
        // outage; anything else (5xx, transport) is a backend fault we must not mask as a bad
        // credential.
        if resp.status().is_client_error() {
            return Err(AuthError::InvalidCredentials);
        }
        let resp = resp
            .error_for_status()
            .map_err(|e| AuthError::Backend(format!("oidc token status: {e}")))?;
        let token: TokenResponse = resp
            .json()
            .await
            .map_err(|e| AuthError::Backend(format!("oidc token decode: {e}")))?;
        Ok(token.access_token)
    }

    /// Read the userinfo endpoint with `access_token`, returning the principal's `sub`.
    async fn userinfo(&self, access_token: &str) -> Result<String, AuthError> {
        let resp = self
            .http
            .get(&self.config.userinfo_endpoint)
            .bearer_auth(access_token)
            .send()
            .await
            .map_err(|e| AuthError::Backend(format!("oidc userinfo request: {e}")))?;
        if resp.status().is_client_error() {
            return Err(AuthError::InvalidCredentials);
        }
        let resp = resp
            .error_for_status()
            .map_err(|e| AuthError::Backend(format!("oidc userinfo status: {e}")))?;
        let info: UserInfo = resp
            .json()
            .await
            .map_err(|e| AuthError::Backend(format!("oidc userinfo decode: {e}")))?;
        if info.sub.trim().is_empty() {
            return Err(AuthError::InvalidCredentials);
        }
        Ok(info.sub)
    }

    /// Map an upstream `sub` onto the [`VerifiedIdentity`] this login resolves to, scoped to the
    /// configured workspace + scopes — the SAME shape the password path yields.
    fn identity_for(&self, sub: String) -> VerifiedIdentity {
        VerifiedIdentity {
            sub,
            workspace: self.config.workspace.clone(),
            scopes: self.config.scopes.clone(),
        }
    }
}

#[async_trait]
impl LoginProvider for OidcProvider {
    async fn login(&self, creds: &Credentials) -> Result<VerifiedIdentity, AuthError> {
        let access_token = match creds {
            // OAuth authorization-code grant: exchange the code, then read userinfo.
            Credentials::OAuth { code, .. } => self.exchange_code(code).await?,
            // OIDC: the issuer must match this provider; the id_token is the access token the
            // userinfo endpoint accepts (the IdP minted both for this client).
            Credentials::Oidc { issuer, id_token } => {
                if issuer != &self.config.issuer {
                    return Err(AuthError::InvalidCredentials);
                }
                id_token.clone()
            }
            // Email+password is the other provider's job.
            Credentials::EmailPassword { .. } => {
                return Err(AuthError::UnsupportedProvider(creds.kind()))
            }
        };
        let sub = self.userinfo(&access_token).await?;
        Ok(self.identity_for(sub))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::extract::{Form, State};
    use axum::http::HeaderMap;
    use axum::routing::{get, post};
    use axum::{Json, Router};
    use std::collections::HashMap;
    use std::net::SocketAddr;
    use std::sync::Arc;

    /// A throwaway in-process IdP: a token endpoint that swaps a known code for an access token,
    /// and a userinfo endpoint that returns a `sub` for that bearer. Mirrors the two round-trips
    /// the adapter makes — no external network.
    #[derive(Clone, Default)]
    struct MockIdp {
        /// `code -> access_token` the token endpoint will honour.
        codes: Arc<HashMap<String, String>>,
        /// `access_token -> sub` the userinfo endpoint will resolve.
        tokens: Arc<HashMap<String, String>>,
    }

    async fn token_handler(
        State(idp): State<MockIdp>,
        Form(form): Form<HashMap<String, String>>,
    ) -> Result<Json<serde_json::Value>, axum::http::StatusCode> {
        let code = form.get("code").cloned().unwrap_or_default();
        match idp.codes.get(&code) {
            Some(access) => Ok(Json(serde_json::json!({
                "access_token": access,
                "token_type": "Bearer",
            }))),
            None => Err(axum::http::StatusCode::BAD_REQUEST),
        }
    }

    async fn userinfo_handler(
        State(idp): State<MockIdp>,
        headers: HeaderMap,
    ) -> Result<Json<serde_json::Value>, axum::http::StatusCode> {
        let bearer = headers
            .get("authorization")
            .and_then(|v| v.to_str().ok())
            .and_then(|v| v.strip_prefix("Bearer "))
            .unwrap_or_default()
            .to_owned();
        match idp.tokens.get(&bearer) {
            Some(sub) => Ok(Json(serde_json::json!({ "sub": sub, "email": "u@idp.test" }))),
            None => Err(axum::http::StatusCode::UNAUTHORIZED),
        }
    }

    /// Spin up the mock IdP on an ephemeral port; returns its base URL and a guard task.
    async fn spawn_idp(idp: MockIdp) -> String {
        let app = Router::new()
            .route("/token", post(token_handler))
            .route("/userinfo", get(userinfo_handler))
            .with_state(idp);
        let listener = tokio::net::TcpListener::bind(SocketAddr::from(([127, 0, 0, 1], 0)))
            .await
            .unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(async move {
            axum::serve(listener, app).await.unwrap();
        });
        format!("http://{addr}")
    }

    fn config_for(base: &str) -> OidcConfig {
        OidcConfig {
            issuer: format!("{base}/"),
            client_id: "gt-client".into(),
            client_secret: "s3cret".into(),
            token_endpoint: format!("{base}/token"),
            userinfo_endpoint: format!("{base}/userinfo"),
            redirect_uri: "https://gt.test/auth/callback".into(),
            workspace: "acme".into(),
            scopes: vec!["rig.read".into()],
        }
    }

    #[tokio::test]
    async fn oauth_code_exchange_resolves_a_verified_identity() {
        let idp = MockIdp {
            codes: Arc::new(HashMap::from([("good-code".into(), "tok-123".into())])),
            tokens: Arc::new(HashMap::from([("tok-123".into(), "alice-sub".into())])),
        };
        let base = spawn_idp(idp).await;
        let provider = OidcProvider::new(config_for(&base)).unwrap();

        let identity = provider
            .login(&Credentials::OAuth {
                provider: "mock".into(),
                code: "good-code".into(),
            })
            .await
            .unwrap();

        assert_eq!(
            identity,
            VerifiedIdentity {
                sub: "alice-sub".into(),
                workspace: "acme".into(),
                scopes: vec!["rig.read".into()],
            }
        );
    }

    #[tokio::test]
    async fn oidc_id_token_reads_userinfo_for_the_matching_issuer() {
        let idp = MockIdp {
            codes: Arc::new(HashMap::new()),
            tokens: Arc::new(HashMap::from([("id-tok".into(), "bob-sub".into())])),
        };
        let base = spawn_idp(idp).await;
        let config = config_for(&base);
        let issuer = config.issuer.clone();
        let provider = OidcProvider::new(config).unwrap();

        let identity = provider
            .login(&Credentials::Oidc {
                issuer,
                id_token: "id-tok".into(),
            })
            .await
            .unwrap();
        assert_eq!(identity.sub, "bob-sub");
        assert_eq!(identity.workspace, "acme");
    }

    #[tokio::test]
    async fn a_bad_code_is_invalid_credentials_not_a_backend_fault() {
        let idp = MockIdp::default();
        let base = spawn_idp(idp).await;
        let provider = OidcProvider::new(config_for(&base)).unwrap();
        let err = provider
            .login(&Credentials::OAuth {
                provider: "mock".into(),
                code: "nope".into(),
            })
            .await;
        assert_eq!(err, Err(AuthError::InvalidCredentials));
    }

    #[tokio::test]
    async fn an_unknown_issuer_is_rejected_before_any_network_call() {
        // token/userinfo would both fail, but the issuer guard rejects first.
        let provider = OidcProvider::new(config_for("http://127.0.0.1:1")).unwrap();
        let err = provider
            .login(&Credentials::Oidc {
                issuer: "https://evil.test/".into(),
                id_token: "whatever".into(),
            })
            .await;
        assert_eq!(err, Err(AuthError::InvalidCredentials));
    }

    #[tokio::test]
    async fn email_password_is_unsupported_by_the_oauth_adapter() {
        let provider = OidcProvider::new(config_for("http://127.0.0.1:1")).unwrap();
        let err = provider
            .login(&Credentials::EmailPassword {
                email: "a@b.test".into(),
                password: "x".into(),
            })
            .await;
        assert_eq!(
            err,
            Err(AuthError::UnsupportedProvider(crate::ProviderKind::EmailPassword))
        );
    }

    #[test]
    fn from_env_requires_the_core_vars() {
        // Missing config is a Backend error (misconfig), never a silent default.
        // (Run in a child scope so we don't leak vars into other tests in this process.)
        let err = OidcConfig::from_env();
        // In the test process these are unset, so it must error on the first required var.
        assert!(matches!(err, Err(AuthError::Backend(_))));
    }
}
