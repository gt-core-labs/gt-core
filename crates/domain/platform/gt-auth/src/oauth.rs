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
use std::time::Instant;

use async_trait::async_trait;
use jsonwebtoken::{decode, decode_header, Algorithm, DecodingKey, Validation};
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
    /// In-memory JWKS cache: the fetched remote keys + the instant they were fetched.
    /// `std::sync::Mutex` so the guard is never held across `.await` (the fetch happens outside
    /// the critical section).
    jwks_cache: std::sync::Mutex<Option<(Instant, Vec<RemoteJwk>)>>,
}

/// The token endpoint's response — only the fields the userinfo read needs.
#[derive(Deserialize)]
struct TokenResponse {
    access_token: String,
}

/// The userinfo endpoint's response — the OIDC standard `sub` plus the optional `email`/`name`.
/// `sub` is the stable principal id; `email`/`name` flow into [`VerifiedIdentity::email`] for JIT
/// user provisioning on first OAuth/OIDC login.
#[derive(Deserialize)]
struct UserInfo {
    sub: String,
    #[serde(default)]
    email: Option<String>,
}

/// OIDC discovery document (`{issuer}/.well-known/openid-configuration`). Only the field we need.
#[derive(Deserialize)]
struct OidcDiscovery {
    jwks_uri: String,
}

/// One JWK entry from the IdP's JWKS endpoint. Only the RSA `n`/`e` components and the `kid`
/// selector — enough to build a `DecodingKey` for id_token signature verification.
#[derive(Clone, Deserialize)]
struct RemoteJwk {
    #[serde(default)]
    kid: Option<String>,
    /// Base64url-encoded RSA modulus.
    #[serde(default)]
    n: Option<String>,
    /// Base64url-encoded RSA public exponent.
    #[serde(default)]
    e: Option<String>,
}

#[derive(Deserialize)]
struct RemoteJwkSet {
    keys: Vec<RemoteJwk>,
}

/// The minimal OIDC id_token claims we validate locally (`sub` + `iss`). The `aud`/`exp` checks
/// are handled by [`jsonwebtoken::Validation`] without needing to name them in this struct.
#[derive(Deserialize)]
struct IdTokenClaims {
    sub: String,
    iss: String,
}

/// How long (seconds) a fetched JWKS is held in memory before re-fetching.
const JWKS_TTL_SECS: u64 = 300; // 5 minutes

impl OidcProvider {
    /// Build a provider over `config`, with a fresh reqwest client.
    pub fn new(config: OidcConfig) -> Result<Self, AuthError> {
        let http = reqwest::Client::builder()
            .build()
            .map_err(|e| AuthError::Backend(format!("oidc http client: {e}")))?;
        Ok(Self { config, http, jwks_cache: std::sync::Mutex::new(None) })
    }

    /// Build a provider over `config` reusing an existing reqwest client (e.g. one the
    /// composition root already pools).
    pub fn with_client(config: OidcConfig, http: reqwest::Client) -> Self {
        Self { config, http, jwks_cache: std::sync::Mutex::new(None) }
    }

    /// The configured issuer URL.
    pub fn issuer(&self) -> &str {
        &self.config.issuer
    }

    /// Exchange `code` at the token endpoint for an access token (authorization-code grant).
    /// When the flow was started with PKCE (hq-idp-db.3), the matching `code_verifier` is replayed
    /// here so the IdP can verify it against the `code_challenge` it bound the `code` to; a
    /// `None`/blank verifier keeps the plain (non-PKCE) exchange working unchanged.
    async fn exchange_code(
        &self,
        code: &str,
        code_verifier: Option<&str>,
    ) -> Result<String, AuthError> {
        let mut form = vec![
            ("grant_type", "authorization_code"),
            ("code", code),
            ("redirect_uri", self.config.redirect_uri.as_str()),
            ("client_id", self.config.client_id.as_str()),
            ("client_secret", self.config.client_secret.as_str()),
        ];
        // Only append the PKCE proof when one was carried — a provider/flow without PKCE must not
        // see an empty `code_verifier` (some IdPs reject the parameter outright).
        if let Some(verifier) = code_verifier.filter(|v| !v.is_empty()) {
            form.push(("code_verifier", verifier));
        }
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

    /// Read the userinfo endpoint with `access_token`, returning the principal profile.
    async fn userinfo(&self, access_token: &str) -> Result<UserInfo, AuthError> {
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
        Ok(info)
    }

    /// Return the cached JWKS keys, re-fetching via OIDC discovery if the cache is cold or stale.
    /// The `std::sync::Mutex` guard is never held across an `.await` — the async fetch happens
    /// outside the critical section.
    async fn cached_jwks(&self) -> Result<Vec<RemoteJwk>, AuthError> {
        // Fast path: cache hit within TTL.
        {
            let guard = self.jwks_cache.lock().unwrap();
            if let Some((fetched_at, ref keys)) = *guard {
                if fetched_at.elapsed().as_secs() < JWKS_TTL_SECS {
                    return Ok(keys.clone());
                }
            }
        }
        // Slow path: fetch and populate cache.
        let keys = self.fetch_remote_jwks().await?;
        *self.jwks_cache.lock().unwrap() = Some((Instant::now(), keys.clone()));
        Ok(keys)
    }

    /// Fetch the IdP's JWKS via OIDC discovery (two round-trips: discovery doc → JWKS endpoint).
    async fn fetch_remote_jwks(&self) -> Result<Vec<RemoteJwk>, AuthError> {
        let disc_url = format!(
            "{}/.well-known/openid-configuration",
            self.config.issuer.trim_end_matches('/')
        );
        let disc: OidcDiscovery = self
            .http
            .get(&disc_url)
            .send()
            .await
            .map_err(|e| AuthError::Backend(format!("oidc discovery request: {e}")))?
            .error_for_status()
            .map_err(|e| AuthError::Backend(format!("oidc discovery status: {e}")))?
            .json()
            .await
            .map_err(|e| AuthError::Backend(format!("oidc discovery decode: {e}")))?;

        let jwks: RemoteJwkSet = self
            .http
            .get(&disc.jwks_uri)
            .send()
            .await
            .map_err(|e| AuthError::Backend(format!("oidc jwks request: {e}")))?
            .error_for_status()
            .map_err(|e| AuthError::Backend(format!("oidc jwks status: {e}")))?
            .json()
            .await
            .map_err(|e| AuthError::Backend(format!("oidc jwks decode: {e}")))?;
        Ok(jwks.keys)
    }

    /// Validate an OIDC `id_token` JWT against the IdP's JWKS, checking:
    /// - RS256 signature (using the matching key by `kid`, or the first RSA key as a fallback)
    /// - `aud` contains [`OidcConfig::client_id`] (via [`jsonwebtoken::Validation`])
    /// - `exp` is not in the past (via [`jsonwebtoken::Validation`])
    /// - `iss` matches [`OidcConfig::issuer`] (validated after decode)
    ///
    /// Returns the token's `sub` claim on success. An invalid/expired/wrong-issuer token is
    /// [`AuthError::InvalidCredentials`] (indistinguishable to callers — no enumeration).
    async fn validate_id_token(&self, id_token: &str) -> Result<String, AuthError> {
        let jwks = self.cached_jwks().await?;
        let header = decode_header(id_token).map_err(|_| AuthError::InvalidCredentials)?;

        // Select the JWK by kid. When the token carries a kid, pick the matching key; when it
        // does not, fall back to the first key in the set (single-key IdPs omit kid).
        let jwk = jwks
            .iter()
            .find(|k| match (&header.kid, &k.kid) {
                (Some(h), Some(k)) => h == k,
                (None, _) => true,
                (Some(_), None) => false,
            })
            .or_else(|| jwks.first())
            .ok_or(AuthError::InvalidCredentials)?;

        let (n, e) = match (jwk.n.as_deref(), jwk.e.as_deref()) {
            (Some(n), Some(e)) => (n, e),
            _ => {
                return Err(AuthError::Backend(
                    "oidc jwks: missing RSA n/e components".into(),
                ))
            }
        };
        let dec_key = DecodingKey::from_rsa_components(n, e)
            .map_err(|_| AuthError::InvalidCredentials)?;

        let mut validation = Validation::new(Algorithm::RS256);
        validation.set_audience(&[self.config.client_id.as_str()]);
        // validate_exp = true (default) — expired tokens are rejected.
        // validate_aud = true (set by set_audience) — aud must contain client_id.
        // Clear required_spec_claims so jsonwebtoken doesn't insist on fields we validate ourselves.
        validation.required_spec_claims.clear();

        let data = decode::<IdTokenClaims>(id_token, &dec_key, &validation).map_err(|e| {
            match e.kind() {
                jsonwebtoken::errors::ErrorKind::ExpiredSignature => AuthError::Expired,
                _ => AuthError::InvalidCredentials,
            }
        })?;

        // Validate the issuer manually (trim trailing slash for tolerance).
        let got_iss = data.claims.iss.trim_end_matches('/');
        let want_iss = self.config.issuer.trim_end_matches('/');
        if got_iss != want_iss {
            return Err(AuthError::InvalidCredentials);
        }

        Ok(data.claims.sub)
    }

    /// Map an upstream `sub` (+ optional `email`) onto the [`VerifiedIdentity`] this login
    /// resolves to, scoped to the configured workspace + scopes — the SAME shape the password
    /// path yields. `email` is forwarded for JIT user provisioning on first SSO login.
    fn identity_for(&self, sub: String, email: Option<String>) -> VerifiedIdentity {
        VerifiedIdentity {
            sub,
            workspace: self.config.workspace.clone(),
            scopes: self.config.scopes.clone(),
            email,
        }
    }

    /// Run the FULL authorization-code handshake for the PKCE redirect flow (hq-idp-db.3): exchange
    /// `code` (replaying the `code_verifier`), read userinfo, and resolve the
    /// [`VerifiedIdentity`] — the same value the `/login` path yields, so the callback folds it into
    /// the identical access/refresh pair. This is the verifier-carrying counterpart of the plain
    /// [`LoginProvider::login`] OAuth path.
    pub async fn exchange_for_identity(
        &self,
        code: &str,
        code_verifier: Option<&str>,
    ) -> Result<VerifiedIdentity, AuthError> {
        let access_token = self.exchange_code(code, code_verifier).await?;
        let info = self.userinfo(&access_token).await?;
        Ok(self.identity_for(info.sub, info.email))
    }

    /// The configured client id (sent on the authorize URL).
    pub fn client_id(&self) -> &str {
        &self.config.client_id
    }

    /// The configured redirect URI echoed on the exchange (and sent on the authorize URL).
    pub fn redirect_uri(&self) -> &str {
        &self.config.redirect_uri
    }

    /// The configured granted scopes (sent space-joined on the authorize URL).
    pub fn scopes(&self) -> &[String] {
        &self.config.scopes
    }
}

#[async_trait]
impl LoginProvider for OidcProvider {
    async fn login(&self, creds: &Credentials) -> Result<VerifiedIdentity, AuthError> {
        match creds {
            // OAuth authorization-code grant: exchange the code for an access token, then read
            // userinfo (carrying email for JIT provisioning). The plain login port carries no
            // PKCE verifier (the `/login` JSON path); the PKCE flow goes through
            // `exchange_for_identity` instead.
            Credentials::OAuth { code, .. } => {
                let access_token = self.exchange_code(code, None).await?;
                let info = self.userinfo(&access_token).await?;
                Ok(self.identity_for(info.sub, info.email))
            }
            // OIDC id_token: validate the JWT locally against the IdP's JWKS (discovery +
            // signature + aud + exp + iss). The `sub` is extracted from the validated token
            // directly — no second userinfo round-trip needed for the identity.
            Credentials::Oidc { issuer, id_token } => {
                if issuer != &self.config.issuer {
                    return Err(AuthError::InvalidCredentials);
                }
                let sub = self.validate_id_token(id_token).await?;
                Ok(self.identity_for(sub, None))
            }
            // Email+password is the other provider's job.
            Credentials::EmailPassword { .. } => {
                Err(AuthError::UnsupportedProvider(creds.kind()))
            }
        }
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
    /// [`OidcConfig`]. When the provider row carries a `workspace_id`, the resolved identity is
    /// scoped to that workspace; otherwise the deploy-level default (`self.workspace`) is used —
    /// so a global (NULL `workspace_id`) provider still works as before.
    async fn provider_for(&self, provider_id: &str) -> Result<OidcProvider, AuthError> {
        let record = self
            .repo
            .get(provider_id)
            .await?
            .filter(|r| r.enabled)
            .ok_or_else(|| AuthError::UnknownProvider(provider_id.to_owned()))?;
        let workspace = record
            .workspace_id
            .clone()
            .unwrap_or_else(|| self.workspace.clone());
        let config = record.into_oidc_config(workspace, self.redirect_uri.clone())?;
        Ok(OidcProvider::with_client(config, self.http.clone()))
    }

    /// The redirect URI every IdP echoes back (the app's own `/auth/callback`), per-deploy config.
    /// The `/authorize` leg records it on the pending row so the callback rebuilds the same value.
    pub fn redirect_uri(&self) -> &str {
        &self.redirect_uri
    }

    /// Build the IdP authorization URL for `provider_id` (hq-idp-db.3): load the row
    /// (absent/disabled ⇒ [`AuthError::UnknownProvider`], a `404`), then assemble the
    /// authorization-code request — `response_type=code`, the client id, this resolver's
    /// `redirect_uri`, the provider's scopes, the anti-CSRF `state`, and the PKCE `code_challenge`
    /// (`S256`). The browser is 302-redirected here to begin the flow.
    pub async fn authorize_url(
        &self,
        provider_id: &str,
        state: &str,
        code_challenge: &str,
    ) -> Result<String, AuthError> {
        let record = self
            .repo
            .get(provider_id)
            .await?
            .filter(|r| r.enabled)
            .ok_or_else(|| AuthError::UnknownProvider(provider_id.to_owned()))?;
        // OAuth scopes are space-delimited on the wire; the row stores them comma-separated.
        let scopes = record
            .scopes
            .split(',')
            .map(str::trim)
            .filter(|s| !s.is_empty())
            .collect::<Vec<_>>()
            .join(" ");
        let q = |s: &str| url_encode(s);
        let mut url = format!(
            "{}?response_type=code&client_id={}&redirect_uri={}&state={}&code_challenge={}&code_challenge_method=S256",
            record.authorize_endpoint,
            q(&record.client_id),
            q(&self.redirect_uri),
            q(state),
            q(code_challenge),
        );
        if !scopes.is_empty() {
            url.push_str("&scope=");
            url.push_str(&url_encode(&scopes));
        }
        Ok(url)
    }

    /// Finish the PKCE redirect flow (hq-idp-db.3): resolve `provider_id`, exchange `code` replaying
    /// `code_verifier`, and read userinfo into the [`VerifiedIdentity`] — the SAME value the
    /// `/login` path yields, so the callback issues the identical access/refresh pair. An
    /// absent/disabled provider is [`AuthError::UnknownProvider`].
    pub async fn exchange(
        &self,
        provider_id: &str,
        code: &str,
        code_verifier: &str,
    ) -> Result<VerifiedIdentity, AuthError> {
        let provider = self.provider_for(provider_id).await?;
        provider.exchange_for_identity(code, Some(code_verifier)).await
    }
}

/// Percent-encode a query-parameter value (RFC 3986 unreserved kept, everything else `%XX`). A
/// tiny dependency-free encoder — the values here are URLs / opaque tokens / scope lists, so a
/// correct minimal escape is all the authorize URL needs (no `url`/`percent-encoding` crate pulled).
fn url_encode(s: &str) -> String {
    let mut out = String::with_capacity(s.len());
    for &b in s.as_bytes() {
        match b {
            b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'-' | b'_' | b'.' | b'~' => {
                out.push(b as char)
            }
            _ => out.push_str(&format!("%{b:02X}")),
        }
    }
    out
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

    // RSA test fixtures (same pair used by jwt.rs tests).
    const TEST_PRIV_PEM: &[u8] = include_bytes!("../tests/fixtures/rs256_priv.pem");
    const TEST_PUB_PEM: &[u8] = include_bytes!("../tests/fixtures/rs256_pub.pem");
    const OTHER_PRIV_PEM: &[u8] = include_bytes!("../tests/fixtures/rs256_other_priv.pem");

    /// A throwaway in-process IdP: a token endpoint that swaps a known code for an access token,
    /// a userinfo endpoint that returns a `sub` for that bearer, and (optionally) OIDC discovery
    /// + JWKS endpoints for id_token validation tests.
    ///
    /// `base_url` is filled in by `spawn_idp` after the listener binds — callers leave it as
    /// `Default::default()` when constructing and let `spawn_idp` patch it in.
    #[derive(Clone, Default)]
    struct MockIdp {
        /// `code -> access_token` the token endpoint will honour.
        codes: Arc<HashMap<String, String>>,
        /// `access_token -> sub` the userinfo endpoint will resolve.
        tokens: Arc<HashMap<String, String>>,
        /// The server's own base URL; set by `spawn_idp`. Needed for the discovery document.
        base_url: Arc<String>,
        /// RSA public PEM bytes for the JWKS endpoint. `None` ⇒ `/jwks` returns empty keys.
        pub_pem: Arc<Option<Vec<u8>>>,
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

    /// `GET /.well-known/openid-configuration` — points the JWKS URL at `/jwks`.
    async fn discovery_handler(State(idp): State<MockIdp>) -> Json<serde_json::Value> {
        Json(serde_json::json!({
            "issuer": format!("{}/", idp.base_url.as_str()),
            "jwks_uri": format!("{}/jwks", idp.base_url.as_str()),
        }))
    }

    /// `GET /jwks` — returns the RSA public key as a JWK Set (or empty keys when none configured).
    async fn jwks_handler(State(idp): State<MockIdp>) -> Json<serde_json::Value> {
        match idp.pub_pem.as_ref().as_ref() {
            Some(pem) => {
                let jwk_set = crate::JwtAuthenticator::from_rsa_pem(pem)
                    .expect("test pub PEM must be valid")
                    .jwk_set();
                Json(serde_json::to_value(jwk_set).unwrap())
            }
            None => Json(serde_json::json!({"keys": []})),
        }
    }

    /// Spin up the mock IdP on an ephemeral port; returns its base URL.
    /// Fills in `idp.base_url` after binding so discovery/JWKS handlers know their own origin.
    async fn spawn_idp(mut idp: MockIdp) -> String {
        let listener = tokio::net::TcpListener::bind(SocketAddr::from(([127, 0, 0, 1], 0)))
            .await
            .unwrap();
        let addr = listener.local_addr().unwrap();
        let base = format!("http://{addr}");
        idp.base_url = Arc::new(base.clone());
        let app = Router::new()
            .route("/token", post(token_handler))
            .route("/userinfo", get(userinfo_handler))
            .route("/.well-known/openid-configuration", get(discovery_handler))
            .route("/jwks", get(jwks_handler))
            .with_state(idp);
        tokio::spawn(async move {
            axum::serve(listener, app).await.unwrap();
        });
        base
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
            ..Default::default()
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

        assert_eq!(identity.sub, "alice-sub");
        assert_eq!(identity.workspace, "acme");
        assert_eq!(identity.scopes, vec!["rig.read".to_string()]);
        // The mock userinfo endpoint returns `email: u@idp.test` — threaded through.
        assert_eq!(identity.email.as_deref(), Some("u@idp.test"));
    }

    /// Mint a short-lived RS256 id_token for the given `issuer` and `client_id`.
    fn mint_id_token(priv_pem: &[u8], issuer: &str, client_id: &str, sub: &str) -> String {
        use jsonwebtoken::{encode, Algorithm, EncodingKey, Header};
        #[derive(serde::Serialize)]
        struct Claims<'a> {
            sub: &'a str,
            iss: &'a str,
            aud: &'a str,
            exp: u64,
            iat: u64,
        }
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_secs();
        let claims = Claims { sub, iss: issuer, aud: client_id, exp: now + 3600, iat: now };
        let key = EncodingKey::from_rsa_pem(priv_pem).expect("test private key");
        encode(&Header::new(Algorithm::RS256), &claims, &key).expect("mint id_token")
    }

    #[tokio::test]
    async fn oidc_id_token_validates_signature_and_resolves_identity() {
        // Serve a JWKS endpoint with the test public key so the provider can validate id_tokens.
        let idp = MockIdp {
            pub_pem: Arc::new(Some(TEST_PUB_PEM.to_vec())),
            ..Default::default()
        };
        let base = spawn_idp(idp).await;
        let config = config_for(&base);
        let issuer = config.issuer.clone();
        let client_id = config.client_id.clone();
        let provider = OidcProvider::new(config).unwrap();

        let id_token = mint_id_token(TEST_PRIV_PEM, &issuer, &client_id, "bob-sub");
        let identity = provider
            .login(&Credentials::Oidc { issuer, id_token })
            .await
            .unwrap();

        assert_eq!(identity.sub, "bob-sub");
        assert_eq!(identity.workspace, "acme");
    }

    #[tokio::test]
    async fn oidc_id_token_rejects_a_wrong_key_signature() {
        let idp = MockIdp {
            pub_pem: Arc::new(Some(TEST_PUB_PEM.to_vec())),
            ..Default::default()
        };
        let base = spawn_idp(idp).await;
        let config = config_for(&base);
        let issuer = config.issuer.clone();
        let client_id = config.client_id.clone();
        let provider = OidcProvider::new(config).unwrap();

        // Sign with the OTHER private key — JWKS has the TEST public key → signature mismatch.
        let id_token = mint_id_token(OTHER_PRIV_PEM, &issuer, &client_id, "mallory");
        let err = provider
            .login(&Credentials::Oidc { issuer, id_token })
            .await;
        assert_eq!(err, Err(AuthError::InvalidCredentials));
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
        async fn list_for_workspace(&self, workspace: &str) -> Result<Vec<ProviderRecord>, AuthError> {
            Ok(self.rows.values()
                .filter(|r| r.workspace_id.is_none() || r.workspace_id.as_deref() == Some(workspace))
                .cloned()
                .collect())
        }
        async fn get(&self, id: &str) -> Result<Option<ProviderRecord>, AuthError> {
            Ok(self.rows.get(id).cloned())
        }
        async fn create(&self, _p: NewProvider) -> Result<ProviderRecord, AuthError> {
            unreachable!("the resolver never writes")
        }
        async fn patch(
            &self,
            _id: &str,
            _p: crate::provider_repo::PatchProvider,
        ) -> Result<Option<ProviderRecord>, AuthError> {
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
            workspace_id: None,
        }
    }

    #[tokio::test]
    async fn db_resolver_issues_an_identity_for_a_registered_enabled_provider() {
        let idp = MockIdp {
            codes: Arc::new(HashMap::from([("good-code".into(), "tok-1".into())])),
            tokens: Arc::new(HashMap::from([("tok-1".into(), "carol-sub".into())])),
            ..Default::default()
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
        assert_eq!(identity.sub, "carol-sub");
        assert_eq!(identity.workspace, "acme");
        assert_eq!(identity.scopes, vec!["rig.read".to_string()]);
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

    #[tokio::test]
    async fn db_resolver_uses_provider_workspace_id_over_default() {
        // A provider row with `workspace_id = Some("enterprise")` should produce an identity
        // scoped to "enterprise" even though the resolver was built with default workspace "acme".
        let idp = MockIdp {
            codes: Arc::new(HashMap::from([("corp-code".into(), "tok-e".into())])),
            tokens: Arc::new(HashMap::from([("tok-e".into(), "eve-sub".into())])),
            ..Default::default()
        };
        let base = spawn_idp(idp).await;
        let mut repo = MapRepo::default();
        let mut rec = record_for(&base, "enterprise-sso", true, "s3cret");
        rec.workspace_id = Some("enterprise".into());
        repo.rows.insert("enterprise-sso".into(), rec);
        let resolver =
            DbOauthLogin::new(Arc::new(repo), "acme", "https://gt.test/cb").unwrap();

        let identity = resolver
            .login(&Credentials::OAuth {
                provider: "enterprise-sso".into(),
                code: "corp-code".into(),
            })
            .await
            .unwrap();

        // Provider row says workspace = "enterprise"; resolver default "acme" is NOT used.
        assert_eq!(identity.workspace, "enterprise");
        assert_eq!(identity.sub, "eve-sub");
    }

    #[tokio::test]
    async fn db_resolver_falls_back_to_default_workspace_for_global_provider() {
        // A global provider (workspace_id = None) falls back to the resolver's deploy-level default.
        let idp = MockIdp {
            codes: Arc::new(HashMap::from([("g-code".into(), "tok-g".into())])),
            tokens: Arc::new(HashMap::from([("tok-g".into(), "grace-sub".into())])),
            ..Default::default()
        };
        let base = spawn_idp(idp).await;
        let mut repo = MapRepo::default();
        repo.rows.insert("google".into(), record_for(&base, "google", true, "s3cret"));
        let resolver =
            DbOauthLogin::new(Arc::new(repo), "acme", "https://gt.test/cb").unwrap();

        let identity = resolver
            .login(&Credentials::OAuth { provider: "google".into(), code: "g-code".into() })
            .await
            .unwrap();

        assert_eq!(identity.workspace, "acme");
        assert_eq!(identity.sub, "grace-sub");
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
                ..Default::default()
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
                workspace_id: None,
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
