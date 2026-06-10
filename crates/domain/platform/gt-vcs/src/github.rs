//! The platform GitHub App client (hq-vcs-connections.2) — the reusable engine behind the
//! "how we clone private repos without storing tokens" story.
//!
//! There is ONE GitHub App at the platform level (not per-workspace). A workspace *installs* that
//! App on its org/account; the server then mints **ephemeral installation tokens just-in-time** to
//! clone the workspace's private repos for the knowledge graph. Only the `installation_id` is ever
//! persisted ([`crate::repo`] / `vcs_connections`, `kind = github_app`) — never a long-lived
//! credential.
//!
//! ## Secrets come from MOUNTED FILES, never the DB and never plain env
//!
//! The App's identity (App ID, RS256 private key PEM, webhook secret) is read from files named by
//! env vars, MIRRORING `GT_JWT_RS256_PRIVATE_KEY_FILE` (the App ID itself is a non-secret public
//! identifier, so it may be given inline OR via a file):
//!
//! | env var | meaning |
//! |---------|---------|
//! | [`ENV_APP_ID`] / [`ENV_APP_ID_FILE`] | the numeric GitHub App ID (inline or file) |
//! | [`ENV_APP_PRIVATE_KEY_FILE`] | the file holding the App's RS256 private key PEM |
//! | [`ENV_APP_WEBHOOK_SECRET_FILE`] | the file holding the webhook HMAC secret (.7 consumes it) |
//! | [`ENV_APP_SLUG`] | the App's URL slug for the install redirect (`github.com/apps/<slug>`) |
//!
//! ## Token minting (JIT, in-memory only)
//!
//! 1. Build an **App JWT**: RS256-signed with the private key, `iss = App ID`, `iat`/`exp` a short
//!    (<=10 min) window — this authenticates AS THE APP, not an installation.
//! 2. `POST https://api.github.com/app/installations/{id}/access_tokens` with that JWT →
//!    a **1h installation token**.
//! 3. The token is returned as an [`InstallationToken`] and held only in memory by the caller —
//!    it is NEVER written to the database. `.4`/`.6`/`.7`/`.8` mint one, use it for a clone /
//!    repo-list, and drop it.

use std::time::{SystemTime, UNIX_EPOCH};

use gt_events::AppError;
use serde::{Deserialize, Serialize};

/// Inline numeric GitHub App ID (a public identifier, not a secret). Either this or
/// [`ENV_APP_ID_FILE`] must be set.
pub const ENV_APP_ID: &str = "GT_GITHUB_APP_ID";
/// File holding the numeric GitHub App ID (alternative to [`ENV_APP_ID`], for operators who keep
/// every App attribute as a mounted file).
pub const ENV_APP_ID_FILE: &str = "GT_GITHUB_APP_ID_FILE";
/// File holding the App's RS256 **private key** PEM — the signing material. Mounted, never in the
/// DB or a plain env var, mirroring `GT_JWT_RS256_PRIVATE_KEY_FILE`.
pub const ENV_APP_PRIVATE_KEY_FILE: &str = "GT_GITHUB_APP_PRIVATE_KEY_FILE";
/// File holding the webhook HMAC secret used to verify `X-Hub-Signature-256` on inbound pushes
/// (the freshness webhook lands in `.7`; this loads the secret so the App config is complete here).
pub const ENV_APP_WEBHOOK_SECRET_FILE: &str = "GT_GITHUB_APP_WEBHOOK_SECRET_FILE";
/// The App's URL **slug** — the `<app>` in `https://github.com/apps/<app>/installations/new`. A
/// public identifier, given inline.
pub const ENV_APP_SLUG: &str = "GT_GITHUB_APP_SLUG";

/// GitHub's REST API base. Overridable via [`GithubAppClient::with_api_base`] so a test can point at
/// a mock server.
pub const GITHUB_API_BASE: &str = "https://api.github.com";

/// The default App-JWT lifetime: 9 minutes. GitHub caps the App JWT `exp` at 10 minutes from `iat`;
/// 9 leaves headroom for modest clock skew without tripping the cap.
const APP_JWT_TTL_SECS: u64 = 9 * 60;
/// Backdate `iat` by 60s to absorb clock skew between us and GitHub (GitHub's own recommendation).
const APP_JWT_IAT_BACKDATE_SECS: u64 = 60;

/// The platform GitHub App's identity + secrets, loaded from mounted files (see the module docs).
///
/// Held by the composition root and shared (it is cheap to clone: a couple of `String`s plus the
/// PEM bytes). It carries the raw private-key PEM rather than a pre-parsed key so a config can be
/// constructed in a test without a real RSA key (parsing happens at mint time).
#[derive(Clone)]
pub struct GithubAppConfig {
    /// The numeric App ID (`iss` of the App JWT). A public identifier.
    app_id: String,
    /// The RS256 private key PEM bytes.
    private_key_pem: Vec<u8>,
    /// The App URL slug for the install redirect.
    app_slug: String,
    /// The webhook HMAC secret (loaded for `.7`; unused here beyond exposure via
    /// [`webhook_secret`](Self::webhook_secret)).
    webhook_secret: Option<String>,
}

impl std::fmt::Debug for GithubAppConfig {
    /// Never print the private key or webhook secret.
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("GithubAppConfig")
            .field("app_id", &self.app_id)
            .field("app_slug", &self.app_slug)
            .field("private_key_pem", &"<redacted>")
            .field(
                "webhook_secret",
                &self.webhook_secret.as_ref().map(|_| "<redacted>"),
            )
            .finish()
    }
}

impl GithubAppConfig {
    /// Build a config directly (the parts already in memory). Prefer [`from_env`](Self::from_env) in
    /// the binary; this is the test / programmatic seam.
    pub fn new(
        app_id: impl Into<String>,
        private_key_pem: impl Into<Vec<u8>>,
        app_slug: impl Into<String>,
        webhook_secret: Option<String>,
    ) -> Self {
        GithubAppConfig {
            app_id: app_id.into(),
            private_key_pem: private_key_pem.into(),
            app_slug: app_slug.into(),
            webhook_secret,
        }
    }

    /// Load the App config from the environment, reading every secret from a MOUNTED FILE (mirroring
    /// `GT_JWT_RS256_PRIVATE_KEY_FILE`).
    ///
    /// Returns `Ok(None)` when no App is configured at all (neither [`ENV_APP_ID`] nor
    /// [`ENV_APP_ID_FILE`] is set) — the GitHub-App surface is OPTIONAL, so a deploy without one
    /// boots fine (the connection CRUD + the PAT fallback still work). Returns `Err` when the App is
    /// *partially* configured (an App ID but a missing/unreadable private-key file, etc.) — a
    /// misconfiguration must fail loud, not silently disable cloning.
    pub fn from_env() -> Result<Option<Self>, AppError> {
        let app_id = match read_inline_or_file(ENV_APP_ID, ENV_APP_ID_FILE)? {
            Some(v) => v.trim().to_owned(),
            None => return Ok(None),
        };
        if app_id.is_empty() {
            return Err(AppError::Other(format!("{ENV_APP_ID} is empty")));
        }

        let key_path = std::env::var(ENV_APP_PRIVATE_KEY_FILE).map_err(|_| {
            AppError::Other(format!(
                "{ENV_APP_ID} is set but {ENV_APP_PRIVATE_KEY_FILE} is not — the GitHub App is \
                 half-configured"
            ))
        })?;
        let private_key_pem = std::fs::read(&key_path).map_err(|e| {
            AppError::Other(format!(
                "cannot read {ENV_APP_PRIVATE_KEY_FILE} ({key_path}): {e}"
            ))
        })?;

        let app_slug = std::env::var(ENV_APP_SLUG).map_err(|_| {
            AppError::Other(format!(
                "{ENV_APP_ID} is set but {ENV_APP_SLUG} is not — needed for the install redirect"
            ))
        })?;

        // The webhook secret is optional here: the webhook handler ships in `.7`. If the file path
        // is given it MUST be readable (a dangling mount is a misconfiguration).
        let webhook_secret = match std::env::var(ENV_APP_WEBHOOK_SECRET_FILE) {
            Ok(path) => Some(
                std::fs::read_to_string(&path)
                    .map_err(|e| {
                        AppError::Other(format!(
                            "cannot read {ENV_APP_WEBHOOK_SECRET_FILE} ({path}): {e}"
                        ))
                    })?
                    .trim()
                    .to_owned(),
            ),
            Err(_) => None,
        };

        Ok(Some(GithubAppConfig::new(
            app_id,
            private_key_pem,
            app_slug,
            webhook_secret,
        )))
    }

    /// The numeric App ID (a public identifier).
    pub fn app_id(&self) -> &str {
        &self.app_id
    }

    /// The App's URL slug.
    pub fn app_slug(&self) -> &str {
        &self.app_slug
    }

    /// The webhook HMAC secret, if configured (`.7` verifies `X-Hub-Signature-256` against it).
    pub fn webhook_secret(&self) -> Option<&str> {
        self.webhook_secret.as_deref()
    }

    /// The URL a workspace admin is redirected to in order to install the App:
    /// `https://github.com/apps/<slug>/installations/new`. The callback (after they pick repos)
    /// lands on our install-callback endpoint with `installation_id` + `setup_action`.
    pub fn install_url(&self) -> String {
        format!(
            "https://github.com/apps/{}/installations/new",
            self.app_slug
        )
    }
}

/// The current unix time in seconds.
fn now_secs() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or(0)
}

/// Read a value from an inline env var, falling back to a file named by a second env var. `Ok(None)`
/// when neither is set. A named-but-unreadable file is an error (a misconfiguration, not "absent").
fn read_inline_or_file(inline: &str, file: &str) -> Result<Option<String>, AppError> {
    if let Ok(v) = std::env::var(inline) {
        return Ok(Some(v));
    }
    match std::env::var(file) {
        Ok(path) => {
            let s = std::fs::read_to_string(&path)
                .map_err(|e| AppError::Other(format!("cannot read {file} ({path}): {e}")))?;
            Ok(Some(s))
        }
        Err(_) => Ok(None),
    }
}

/// The App-JWT claims: minimal per GitHub's spec — `iat`, `exp`, `iss` (the App ID). Signed RS256.
#[derive(Serialize)]
struct AppJwtClaims {
    /// Issued-at (unix seconds), backdated 60s for skew.
    iat: u64,
    /// Expiry (unix seconds), <= 10 min after `iat`.
    exp: u64,
    /// Issuer — the GitHub App ID.
    iss: String,
}

/// An installation access token minted JIT — **in-memory only, NEVER persisted**.
///
/// Use it to clone (`https://x-access-token:<token>@github.com/<owner>/<repo>.git`) or to call the
/// installation REST API, then drop it. GitHub issues these with a ~1h TTL; [`is_expired`] /
/// [`expires_in`] let a caller decide whether to re-mint.
#[derive(Clone)]
pub struct InstallationToken {
    /// The bearer token (`ghs_...`). Held in memory, never written to the DB.
    token: String,
    /// Expiry as unix seconds (parsed from GitHub's `expires_at`).
    expires_at: u64,
}

impl std::fmt::Debug for InstallationToken {
    /// Never print the token itself.
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("InstallationToken")
            .field("token", &"<redacted>")
            .field("expires_at", &self.expires_at)
            .finish()
    }
}

impl InstallationToken {
    /// The raw bearer token. Treat it as a secret: it grants repo access for the installation until
    /// it expires. Do not log or persist it.
    pub fn secret(&self) -> &str {
        &self.token
    }

    /// Expiry as unix seconds.
    pub fn expires_at(&self) -> u64 {
        self.expires_at
    }

    /// Whether the token is past (or at) its expiry.
    pub fn is_expired(&self) -> bool {
        now_secs() >= self.expires_at
    }

    /// Seconds remaining until expiry (`0` if already expired).
    pub fn expires_in(&self) -> u64 {
        self.expires_at.saturating_sub(now_secs())
    }

    /// A clone URL with the token embedded as `x-access-token`, the form `git clone` accepts for an
    /// App installation token: `https://x-access-token:<token>@github.com/<owner>/<repo>.git`.
    pub fn clone_url(&self, owner: &str, repo: &str) -> String {
        format!(
            "https://x-access-token:{}@github.com/{}/{}.git",
            self.token, owner, repo
        )
    }
}

/// GitHub's `POST /app/installations/{id}/access_tokens` response (the fields we use).
#[derive(Deserialize)]
struct AccessTokenResponse {
    token: String,
    /// RFC3339, e.g. `2026-06-10T17:00:00Z`.
    expires_at: String,
}

/// One repository as returned by `GET /installation/repositories` (the fields we surface).
#[derive(Clone, Debug, Deserialize, Serialize)]
#[cfg_attr(feature = "axum", derive(utoipa::ToSchema))]
pub struct InstallationRepo {
    /// Numeric repo id.
    pub id: i64,
    /// `owner/name`.
    pub full_name: String,
    /// Just the repo name.
    pub name: String,
    /// Whether the repo is private.
    pub private: bool,
    /// The default branch (the only branch the graph indexes — epic decision).
    pub default_branch: String,
    /// The HTTPS clone URL (token is injected at clone time, not stored here).
    pub clone_url: String,
}

/// GitHub's `GET /installation/repositories` envelope.
#[derive(Deserialize)]
struct RepositoriesResponse {
    repositories: Vec<InstallationRepo>,
}

/// The reusable platform GitHub App client. Wraps the App config + a pooled HTTP client. Mint a
/// token, list repos — `.4`/`.6`/`.7`/`.8` build on these.
#[derive(Clone)]
pub struct GithubAppClient {
    config: GithubAppConfig,
    http: reqwest::Client,
    api_base: String,
}

impl GithubAppClient {
    /// Build a client over `config` with a fresh pooled HTTP client.
    pub fn new(config: GithubAppConfig) -> Self {
        GithubAppClient {
            config,
            http: reqwest::Client::new(),
            api_base: GITHUB_API_BASE.to_owned(),
        }
    }

    /// Build a client reusing an existing pooled HTTP client.
    pub fn with_client(config: GithubAppConfig, http: reqwest::Client) -> Self {
        GithubAppClient {
            config,
            http,
            api_base: GITHUB_API_BASE.to_owned(),
        }
    }

    /// Override the GitHub API base (test seam, points at a mock server).
    pub fn with_api_base(mut self, base: impl Into<String>) -> Self {
        self.api_base = base.into();
        self
    }

    /// The App config (its install URL, slug, webhook secret, etc.).
    pub fn config(&self) -> &GithubAppConfig {
        &self.config
    }

    /// Mint a short-lived **App JWT** (RS256, `iss = App ID`, <=10 min) authenticating AS THE APP.
    /// Used internally to exchange for an installation token; exposed for `.7`'s App-level calls.
    pub fn app_jwt(&self) -> Result<String, AppError> {
        use jsonwebtoken::{encode, Algorithm, EncodingKey, Header};
        let key = EncodingKey::from_rsa_pem(&self.config.private_key_pem)
            .map_err(|e| AppError::Other(format!("invalid GitHub App RS256 private key: {e}")))?;
        let iat = now_secs().saturating_sub(APP_JWT_IAT_BACKDATE_SECS);
        let claims = AppJwtClaims {
            iat,
            exp: iat + APP_JWT_IAT_BACKDATE_SECS + APP_JWT_TTL_SECS,
            iss: self.config.app_id.clone(),
        };
        encode(&Header::new(Algorithm::RS256), &claims, &key)
            .map_err(|e| AppError::Other(format!("signing GitHub App JWT failed: {e}")))
    }

    /// Mint a **1h installation token** for `installation_id`: build an App JWT, then
    /// `POST /app/installations/{id}/access_tokens`. The returned [`InstallationToken`] is held only
    /// in memory by the caller and NEVER persisted.
    pub async fn installation_token(
        &self,
        installation_id: &str,
    ) -> Result<InstallationToken, AppError> {
        let jwt = self.app_jwt()?;
        let url = format!(
            "{}/app/installations/{}/access_tokens",
            self.api_base, installation_id
        );
        let resp = self
            .http
            .post(&url)
            .bearer_auth(&jwt)
            .header("Accept", "application/vnd.github+json")
            .header("X-GitHub-Api-Version", "2022-11-28")
            .header(reqwest::header::USER_AGENT, "gt-vcs")
            .send()
            .await
            .map_err(|e| AppError::Other(format!("github access_tokens request failed: {e}")))?;
        if !resp.status().is_success() {
            let status = resp.status();
            let body = resp.text().await.unwrap_or_default();
            return Err(AppError::Other(format!(
                "github access_tokens for installation {installation_id}: {status} {body}"
            )));
        }
        let parsed: AccessTokenResponse = resp
            .json()
            .await
            .map_err(|e| AppError::Other(format!("github access_tokens decode: {e}")))?;
        let expires_at = parse_rfc3339_to_unix(&parsed.expires_at)?;
        Ok(InstallationToken {
            token: parsed.token,
            expires_at,
        })
    }

    /// List the repositories an installation can reach: `GET /installation/repositories`, paginated
    /// (100/page), authenticated with `token`. The caller mints `token` via
    /// [`installation_token`](Self::installation_token) first.
    pub async fn list_installation_repos(
        &self,
        token: &InstallationToken,
    ) -> Result<Vec<InstallationRepo>, AppError> {
        let mut all = Vec::new();
        let mut page = 1u32;
        loop {
            let url = format!(
                "{}/installation/repositories?per_page=100&page={}",
                self.api_base, page
            );
            let resp = self
                .http
                .get(&url)
                .bearer_auth(token.secret())
                .header("Accept", "application/vnd.github+json")
                .header("X-GitHub-Api-Version", "2022-11-28")
                .header(reqwest::header::USER_AGENT, "gt-vcs")
                .send()
                .await
                .map_err(|e| {
                    AppError::Other(format!(
                        "github installation/repositories request failed: {e}"
                    ))
                })?;
            if !resp.status().is_success() {
                let status = resp.status();
                let body = resp.text().await.unwrap_or_default();
                return Err(AppError::Other(format!(
                    "github installation/repositories: {status} {body}"
                )));
            }
            let parsed: RepositoriesResponse = resp.json().await.map_err(|e| {
                AppError::Other(format!("github installation/repositories decode: {e}"))
            })?;
            let n = parsed.repositories.len();
            all.extend(parsed.repositories);
            // GitHub returns a full page (100) while more remain; a short page is the last one.
            if n < 100 {
                break;
            }
            page += 1;
        }
        Ok(all)
    }
}

/// Parse an RFC3339 timestamp (GitHub's `expires_at`, e.g. `2026-06-10T17:00:00Z`) to unix seconds,
/// using `chrono` (already a workspace dep). A malformed timestamp is [`AppError::Other`].
fn parse_rfc3339_to_unix(s: &str) -> Result<u64, AppError> {
    let dt = chrono::DateTime::parse_from_rfc3339(s)
        .map_err(|e| AppError::Other(format!("github expires_at parse ({s}): {e}")))?;
    Ok(dt.timestamp().max(0) as u64)
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Serializes the tests that mutate process-global `GT_GITHUB_APP_*` env vars, so a
    /// concurrent setter can't make a hermetic reader observe a half-configured App (CI race
    /// seen on `from_env_returns_none_when_unconfigured`). Poison-tolerant: a panicking test
    /// must not wedge the others.
    static ENV_LOCK: std::sync::Mutex<()> = std::sync::Mutex::new(());

    /// A throwaway 2048-bit RSA key (PKCS#8 PEM) for signing-path tests. Generated for tests only.
    const TEST_PRIV_PEM: &[u8] = include_bytes!("../tests/data/test_app_key.pem");

    fn test_config() -> GithubAppConfig {
        GithubAppConfig::new("123456", TEST_PRIV_PEM.to_vec(), "gt-test-app", None)
    }

    #[test]
    fn install_url_is_the_app_installations_new_url() {
        let cfg = test_config();
        assert_eq!(
            cfg.install_url(),
            "https://github.com/apps/gt-test-app/installations/new"
        );
    }

    #[test]
    fn app_jwt_signs_with_app_id_issuer_and_bounded_exp() {
        use jsonwebtoken::{decode, Algorithm, DecodingKey, Validation};
        let client = GithubAppClient::new(test_config());
        let jwt = client.app_jwt().expect("sign app jwt");

        // Verify with the matching public key derived from the private PEM.
        let pub_pem = include_bytes!("../tests/data/test_app_key.pub.pem");
        let mut validation = Validation::new(Algorithm::RS256);
        validation.set_issuer(&["123456"]);
        validation.validate_exp = true;
        #[derive(serde::Deserialize)]
        struct Claims {
            iss: String,
            iat: u64,
            exp: u64,
        }
        let data = decode::<Claims>(
            &jwt,
            &DecodingKey::from_rsa_pem(pub_pem).unwrap(),
            &validation,
        )
        .expect("verify app jwt");
        assert_eq!(data.claims.iss, "123456");
        // exp is within 10 minutes of iat (GitHub's cap).
        assert!(
            data.claims.exp - data.claims.iat <= 600,
            "exp must be <= 10min after iat"
        );
        assert!(data.claims.exp > data.claims.iat);
    }

    #[test]
    fn installation_token_redacts_and_computes_expiry() {
        let tok = InstallationToken {
            token: "ghs_supersecret".into(),
            expires_at: now_secs() + 3600,
        };
        // Debug never leaks the token.
        let dbg = format!("{tok:?}");
        assert!(
            !dbg.contains("ghs_supersecret"),
            "token must not appear in Debug: {dbg}"
        );
        assert!(!tok.is_expired());
        assert!(tok.expires_in() > 3500);
        // Clone URL embeds the token in the x-access-token form.
        let url = tok.clone_url("acme", "inactivas-chain");
        assert_eq!(
            url,
            "https://x-access-token:ghs_supersecret@github.com/acme/inactivas-chain.git"
        );
    }

    #[test]
    fn expired_token_reports_expired() {
        let tok = InstallationToken {
            token: "ghs_x".into(),
            expires_at: now_secs().saturating_sub(10),
        };
        assert!(tok.is_expired());
        assert_eq!(tok.expires_in(), 0);
    }

    #[test]
    fn rfc3339_parses_to_unix() {
        // 2021-01-01T00:00:00Z == 1609459200.
        assert_eq!(
            parse_rfc3339_to_unix("2021-01-01T00:00:00Z").unwrap(),
            1609459200
        );
        assert!(parse_rfc3339_to_unix("not-a-date").is_err());
    }

    #[test]
    fn config_debug_redacts_secrets() {
        let cfg = GithubAppConfig::new(
            "1",
            b"PRIVATEKEYBYTES".to_vec(),
            "slug",
            Some("whsec".into()),
        );
        let dbg = format!("{cfg:?}");
        assert!(
            !dbg.contains("PRIVATEKEYBYTES"),
            "private key must be redacted: {dbg}"
        );
        assert!(
            !dbg.contains("whsec"),
            "webhook secret must be redacted: {dbg}"
        );
        assert!(dbg.contains("app_id"));
    }

    #[test]
    fn from_env_returns_none_when_unconfigured() {
        let _env = ENV_LOCK.lock().unwrap_or_else(|e| e.into_inner());
        // Clear any App env so the test is hermetic.
        for k in [
            ENV_APP_ID,
            ENV_APP_ID_FILE,
            ENV_APP_PRIVATE_KEY_FILE,
            ENV_APP_SLUG,
            ENV_APP_WEBHOOK_SECRET_FILE,
        ] {
            std::env::remove_var(k);
        }
        assert!(GithubAppConfig::from_env().unwrap().is_none());
    }

    #[test]
    fn from_env_half_configured_is_an_error() {
        let _env = ENV_LOCK.lock().unwrap_or_else(|e| e.into_inner());
        for k in [
            ENV_APP_ID_FILE,
            ENV_APP_PRIVATE_KEY_FILE,
            ENV_APP_SLUG,
            ENV_APP_WEBHOOK_SECRET_FILE,
        ] {
            std::env::remove_var(k);
        }
        // An App ID but no private-key file is a half-configured App.
        std::env::set_var(ENV_APP_ID, "123");
        let err = GithubAppConfig::from_env();
        std::env::remove_var(ENV_APP_ID);
        assert!(err.is_err(), "half-configured App must fail loud");
    }
}
