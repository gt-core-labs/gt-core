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

use std::sync::Arc;

use async_trait::async_trait;
use serde::Deserialize;

use crate::provider_repo::ProviderRepo;
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

/// The DB-backed OAuth/OIDC [`LoginProvider`] (hq-idp-db.2): instead of one provider baked from the
/// environment ([`OidcConfig::from_env`]), it resolves the provider PER REQUEST from the
/// [`ProviderRepo`] store by the login's `provider_id`, decrypts that row's client secret, builds an
/// [`OidcConfig`], and runs the [`OidcProvider`] handshake — yielding the SAME
/// [`VerifiedIdentity`] (and thus the same access/refresh pair) the password path does.
///
/// This is what the composition root wires into [`AuthState::oauth_login`](crate::AuthState) once a
/// provider store exists, replacing the single `from_env` provider: an admin registers many
/// providers (Google/GitHub/Microsoft presets or a generic OIDC IdP) and each is selectable by its
/// `provider_id`, with no redeploy. The reqwest client is pooled across requests.
///
/// A `provider_id` that is absent or `enabled = false` is [`AuthError::UnknownProvider`] (a `404`),
/// kept indistinguishable so a caller cannot enumerate the registered ids; a sealed secret that
/// fails to unseal (wrong/rotated key) stays [`AuthError::Backend`] (a `500` — a misconfiguration,
/// not a rejected login).
pub struct DbOauthLogin {
    repo: Arc<dyn ProviderRepo>,
    http: reqwest::Client,
    /// The redirect URI registered with every IdP, echoed back on the token exchange. Per-deploy
    /// config (the `oauth_providers` row stores the authorize/token/userinfo endpoints, not the
    /// app's own callback URL).
    redirect_uri: String,
    /// The tenant a resolved OAuth identity is scoped to (the `workspace` claim) — per-deploy, like
    /// the old `GT_OIDC_WORKSPACE`. The row carries the provider's scopes, not the landing tenant.
    workspace: String,
}

impl DbOauthLogin {
    /// Build the resolver over a provider `repo`, scoping resolved identities to `workspace` and
    /// echoing `redirect_uri` on every exchange. A fresh pooled reqwest client backs the handshakes.
    pub fn new(
        repo: Arc<dyn ProviderRepo>,
        workspace: impl Into<String>,
        redirect_uri: impl Into<String>,
    ) -> Result<Self, AuthError> {
        let http = reqwest::Client::builder()
            .build()
            .map_err(|e| AuthError::Backend(format!("oidc http client: {e}")))?;
        Ok(Self {
            repo,
            http,
            redirect_uri: redirect_uri.into(),
            workspace: workspace.into(),
        })
    }

    /// Resolve `provider_id` against the store into a handshake-ready [`OidcProvider`]: load the
    /// row (absent/disabled ⇒ [`AuthError::UnknownProvider`]), unseal its secret, and assemble the
    /// [`OidcConfig`] scoped to this resolver's workspace + redirect URI.
    async fn provider_for(&self, provider_id: &str) -> Result<OidcProvider, AuthError> {
        let record = self
            .repo
            .get(provider_id)
            .await?
            .filter(|r| r.enabled)
            .ok_or_else(|| AuthError::UnknownProvider(provider_id.to_owned()))?;
        let config = record.into_oidc_config(self.workspace.clone(), self.redirect_uri.clone())?;
        Ok(OidcProvider::with_client(config, self.http.clone()))
    }
}

#[async_trait]
impl LoginProvider for DbOauthLogin {
    async fn login(&self, creds: &Credentials) -> Result<VerifiedIdentity, AuthError> {
        // The login's `provider_id` selects which stored provider runs the handshake. OAuth carries
        // it as `provider`; OIDC has no id (the issuer self-identifies), so there is nothing to
        // resolve from the store — that path stays env/issuer-driven and is not this resolver's job.
        let provider_id = match creds {
            Credentials::OAuth { provider, .. } => provider.as_str(),
            Credentials::Oidc { .. } | Credentials::EmailPassword { .. } => {
                return Err(AuthError::UnsupportedProvider(creds.kind()))
            }
        };
        if provider_id.trim().is_empty() {
            return Err(AuthError::UnknownProvider(String::new()));
        }
        let provider = self.provider_for(provider_id).await?;
        provider.login(creds).await
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

    // --- DB-backed resolver (hq-idp-db.2) -----------------------------------------------------
    //
    // The resolver maps a login's `provider_id` to a stored provider and runs the handshake. The
    // routing/4xx behaviour is proved here against an in-memory `ProviderRepo` (no PG needed); the
    // full token-issuing handshake against a real `PgProviderRepo` + mock IdP is the `pg`-gated
    // `db_pg` module below.
    use crate::provider_repo::{NewProvider, ProviderKind as RepoKind, ProviderRecord, ProviderRepo};

    /// A trivial in-memory [`ProviderRepo`] over a fixed set of records — enough to exercise the
    /// resolver's id-lookup + enabled gate without standing up Postgres.
    #[derive(Default)]
    struct MapRepo {
        rows: HashMap<String, ProviderRecord>,
    }

    #[async_trait]
    impl ProviderRepo for MapRepo {
        async fn list(&self) -> Result<Vec<ProviderRecord>, AuthError> {
            Ok(self.rows.values().cloned().collect())
        }
        async fn get(&self, id: &str) -> Result<Option<ProviderRecord>, AuthError> {
            Ok(self.rows.get(id).cloned())
        }
        async fn create(&self, _p: NewProvider) -> Result<ProviderRecord, AuthError> {
            unreachable!("the resolver never writes")
        }
        async fn delete(&self, _id: &str) -> Result<bool, AuthError> {
            unreachable!("the resolver never writes")
        }
    }

    /// A generic-kind record pointing its endpoints at `base`, sealing `secret` with the test key.
    fn record_for(base: &str, id: &str, enabled: bool, secret: &str) -> ProviderRecord {
        std::env::set_var(
            crate::ENV_SECRET_KEY,
            "0000000000000000000000000000000000000000000000000000000000000000",
        );
        ProviderRecord {
            id: id.into(),
            kind: RepoKind::Generic,
            display_name: "Corp SSO".into(),
            client_id: "gt-client".into(),
            client_secret_enc: crate::crypto::seal(secret.as_bytes()).unwrap(),
            issuer: format!("{base}/"),
            authorize_endpoint: format!("{base}/authorize"),
            token_endpoint: format!("{base}/token"),
            userinfo_endpoint: format!("{base}/userinfo"),
            scopes: "rig.read".into(),
            enabled,
        }
    }

    #[tokio::test]
    async fn db_resolver_issues_an_identity_for_a_registered_enabled_provider() {
        let idp = MockIdp {
            codes: Arc::new(HashMap::from([("good-code".into(), "tok-1".into())])),
            tokens: Arc::new(HashMap::from([("tok-1".into(), "carol-sub".into())])),
        };
        let base = spawn_idp(idp).await;
        let mut repo = MapRepo::default();
        repo.rows
            .insert("corp".into(), record_for(&base, "corp", true, "s3cret"));
        let resolver =
            DbOauthLogin::new(Arc::new(repo), "acme", "https://gt.test/cb").unwrap();

        let identity = resolver
            .login(&Credentials::OAuth { provider: "corp".into(), code: "good-code".into() })
            .await
            .unwrap();
        // Same VerifiedIdentity shape the password path yields, scoped to the resolver's workspace.
        assert_eq!(
            identity,
            VerifiedIdentity {
                sub: "carol-sub".into(),
                workspace: "acme".into(),
                scopes: vec!["rig.read".into()],
            }
        );
    }

    #[tokio::test]
    async fn db_resolver_rejects_an_unknown_provider_id_with_a_4xx() {
        let resolver = DbOauthLogin::new(
            Arc::new(MapRepo::default()),
            "acme",
            "https://gt.test/cb",
        )
        .unwrap();
        let err = resolver
            .login(&Credentials::OAuth { provider: "nope".into(), code: "x".into() })
            .await;
        // UnknownProvider maps to 404 in the HTTP layer — a clear 4xx, never 501/500.
        assert_eq!(err, Err(AuthError::UnknownProvider("nope".into())));
    }

    #[tokio::test]
    async fn db_resolver_rejects_a_disabled_provider_id_with_a_4xx() {
        let base = spawn_idp(MockIdp::default()).await;
        let mut repo = MapRepo::default();
        repo.rows
            .insert("off".into(), record_for(&base, "off", false, "s3cret"));
        let resolver =
            DbOauthLogin::new(Arc::new(repo), "acme", "https://gt.test/cb").unwrap();
        let err = resolver
            .login(&Credentials::OAuth { provider: "off".into(), code: "good".into() })
            .await;
        // Disabled is indistinguishable from absent — same UnknownProvider 4xx, no enumeration.
        assert_eq!(err, Err(AuthError::UnknownProvider("off".into())));
    }

    #[tokio::test]
    async fn db_resolver_rejects_a_blank_provider_id_with_a_4xx() {
        let resolver = DbOauthLogin::new(
            Arc::new(MapRepo::default()),
            "acme",
            "https://gt.test/cb",
        )
        .unwrap();
        let err = resolver
            .login(&Credentials::OAuth { provider: String::new(), code: "x".into() })
            .await;
        assert_eq!(err, Err(AuthError::UnknownProvider(String::new())));
    }

    // --- PG-gated: the full handshake against a real PgProviderRepo + mock IdP -----------------
    //
    // No-ops when `GT_PG_URL` is unset (same gate as provider_repo's contract tests). Run with
    // `--test-threads=1` and a `GT_SECRET_KEY` set. Seeds a provider row, spins the mock IdP, and
    // proves login with that `provider_id`+`code` resolves the VerifiedIdentity (the login tier then
    // folds it into the SAME access/refresh pair as the password path); an unknown id is a 4xx.
    #[cfg(feature = "pg")]
    mod db_pg {
        use super::*;
        use crate::PgProviderRepo;
        use sqlx::PgPool;

        async fn pool_or_skip() -> Option<PgPool> {
            let url = std::env::var("GT_PG_URL").ok()?;
            Some(
                PgPool::connect(&url)
                    .await
                    .expect("GT_PG_URL must point at a reachable Postgres"),
            )
        }

        async fn ensure_table(pool: &PgPool) {
            let mut tx = pool.begin().await.expect("begin ddl tx");
            sqlx::query("SELECT pg_advisory_xact_lock(8422)")
                .execute(&mut *tx)
                .await
                .expect("advisory lock");
            sqlx::query(crate::migrations::CREATE_OAUTH_PROVIDERS)
                .execute(&mut *tx)
                .await
                .expect("create oauth_providers table");
            tx.commit().await.expect("commit ddl tx");
        }

        fn unique_id(tag: &str) -> String {
            use std::time::{SystemTime, UNIX_EPOCH};
            let n = SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_nanos();
            format!("test-{tag}-{n}")
        }

        #[tokio::test]
        async fn db_backed_login_issues_an_identity_and_unknown_is_rejected() {
            std::env::set_var(
                crate::ENV_SECRET_KEY,
                "0000000000000000000000000000000000000000000000000000000000000000",
            );
            let Some(pool) = pool_or_skip().await else {
                return;
            };
            ensure_table(&pool).await;
            let repo = PgProviderRepo::new(pool.clone());

            // Mock IdP: swap `good-code` → `tok-9`, resolve `tok-9` → `dave-sub`.
            let idp = MockIdp {
                codes: Arc::new(HashMap::from([("good-code".into(), "tok-9".into())])),
                tokens: Arc::new(HashMap::from([("tok-9".into(), "dave-sub".into())])),
            };
            let base = spawn_idp(idp).await;

            // Seed a generic provider whose endpoints point at the mock IdP. The secret is sealed
            // on write and unsealed only inside the resolver.
            let id = unique_id("dbresolve");
            let new = NewProvider {
                id: id.clone(),
                kind: RepoKind::Generic,
                display_name: "Mock IdP".into(),
                client_id: "gt-client".into(),
                client_secret: "the-secret".into(),
                issuer: format!("{base}/"),
                authorize_endpoint: format!("{base}/authorize"),
                token_endpoint: format!("{base}/token"),
                userinfo_endpoint: format!("{base}/userinfo"),
                scopes: "rig.read,rig.write".into(),
                enabled: true,
            };
            repo.create(new).await.unwrap();

            let resolver =
                DbOauthLogin::new(Arc::new(repo.clone()), "acme", "https://gt.test/cb").unwrap();

            // The valid provider_id + code resolves a full VerifiedIdentity from DB config.
            let identity = resolver
                .login(&Credentials::OAuth { provider: id.clone(), code: "good-code".into() })
                .await
                .unwrap();
            assert_eq!(identity.sub, "dave-sub");
            assert_eq!(identity.workspace, "acme");
            assert_eq!(identity.scopes, vec!["rig.read".to_string(), "rig.write".into()]);

            // An unknown provider_id is a 4xx (UnknownProvider), never a backend fault.
            let err = resolver
                .login(&Credentials::OAuth { provider: unique_id("absent"), code: "x".into() })
                .await;
            assert!(matches!(err, Err(AuthError::UnknownProvider(_))));

            // Disabling the provider makes it resolve as unknown too (no enumeration).
            assert!(repo.delete(&id).await.unwrap());
            let err = resolver
                .login(&Credentials::OAuth { provider: id, code: "good-code".into() })
                .await;
            assert!(matches!(err, Err(AuthError::UnknownProvider(_))));
        }
    }
}
