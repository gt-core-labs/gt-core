//! The versioned, reproducible OAuth/IdP provider seed (`hq-greenfield-seeds.3`).
//!
//! The login providers (Google, …) were configured by hand in `/admin/providers` and lived ONLY in
//! prod's `public.oauth_providers` table — a clean cluster came up with an empty login page. This
//! module makes the NON-SECRET provider config reproducible: it is extracted from a live deploy and
//! versioned in [`SEED_JSON`] (`seeds/oauth-providers.json`), embedded into the binary so the seed
//! travels with it (orchestrator-agnostic — no external file under k8s), and replayed into an empty
//! `oauth_providers` table at boot (see `gt-mcp-server.rs::seed_oauth_providers`).
//!
//! ## Secrets are NOT vendored
//!
//! The one secret — the OAuth `client_secret` — is AES-256-GCM sealed at rest with `GT_SECRET_KEY`
//! ([`crypto`](crate::crypto)) and is NEVER committed to the repo. Each seed entry names the env var
//! ([`SeedProvider::secret_env`], e.g. `GT_OAUTH_SEED_SECRET_GOOGLE`) the cleartext secret is read
//! from at boot. A provider whose secret env is unset is SKIPPED cleanly (a log line, never fatal) —
//! exactly like `seed_admin` skips when `GT_ADMIN_*` is unset — so a deploy that has not yet provided
//! the secret still boots, just without that login button. The seed is gated on the table being EMPTY
//! by the caller, so it never clobbers a human-curated prod catalog.
//!
//! See `docs/ops/greenfield-seeds.md` §4.2 for the secrets matrix and how to regenerate the seed
//! (`scripts/extract-oauth-seed.py`) from a running deploy.

use serde::Deserialize;

use crate::provider_repo::{NewProvider, ProviderKind};
use crate::AuthError;

/// The versioned, non-secret OAuth/IdP provider extract, embedded as bytes so the seed travels with
/// the binary. Regenerate from a live deploy with `scripts/extract-oauth-seed.py` — do NOT invent
/// the content (the values are what prod's `/admin/providers` resolved).
pub const SEED_JSON: &str = include_str!("../seeds/oauth-providers.json");

/// One provider in the versioned seed: the full NON-SECRET row plus the name of the env var the
/// cleartext `client_secret` is read from. `secret_env` is required so the secret stays out of the
/// repo and is supplied per-deploy as a mounted secret / env.
#[derive(Clone, Debug, Deserialize)]
pub struct SeedProvider {
    /// Stable id / primary key (also the wire token a login button carries).
    pub id: String,
    /// Provider variant wire spelling (`google`/`github`/`microsoft`/`generic`).
    pub kind: String,
    /// Human label for the login button.
    #[serde(default)]
    pub display_name: String,
    /// Registered OAuth client id (a public identifier — safe to vendor).
    pub client_id: String,
    /// Issuer URL (`iss`).
    pub issuer: String,
    /// Authorization endpoint.
    pub authorize_endpoint: String,
    /// Token endpoint.
    pub token_endpoint: String,
    /// Userinfo endpoint.
    pub userinfo_endpoint: String,
    /// Comma-separated granted scopes.
    #[serde(default)]
    pub scopes: String,
    /// Whether the provider shows as a login button (mirrors the live `enabled` flag).
    #[serde(default)]
    pub enabled: bool,
    /// Optional workspace scope. `null` = global (every workspace's login page).
    #[serde(default)]
    pub workspace_id: Option<String>,
    /// The env var the cleartext `client_secret` is read from at boot. The secret is NEVER vendored
    /// in the seed — a provider whose env is unset is skipped.
    pub secret_env: String,
}

#[derive(Debug, Deserialize)]
struct SeedFile {
    providers: Vec<SeedProvider>,
}

impl SeedProvider {
    /// Resolve this seed entry into a [`NewProvider`] ready to insert, reading the cleartext
    /// `client_secret` from the env var named by [`secret_env`](Self::secret_env).
    ///
    /// Returns `Ok(None)` when the secret env is unset/empty — a clean skip (the caller logs it),
    /// matching `seed_admin`'s gate on `GT_ADMIN_*`. Returns `Err` when the `kind` is unknown (a
    /// corrupt/forward-incompatible seed, never a silent default).
    pub fn resolve(&self) -> Result<Option<NewProvider>, AuthError> {
        let kind = ProviderKind::parse(&self.kind)?;
        let secret = match std::env::var(&self.secret_env) {
            Ok(v) if !v.trim().is_empty() => v,
            _ => return Ok(None),
        };
        Ok(Some(NewProvider {
            id: self.id.clone(),
            kind,
            display_name: if self.display_name.is_empty() {
                self.id.clone()
            } else {
                self.display_name.clone()
            },
            client_id: self.client_id.clone(),
            client_secret: secret,
            issuer: self.issuer.clone(),
            authorize_endpoint: self.authorize_endpoint.clone(),
            token_endpoint: self.token_endpoint.clone(),
            userinfo_endpoint: self.userinfo_endpoint.clone(),
            scopes: self.scopes.clone(),
            enabled: self.enabled,
            workspace_id: self.workspace_id.clone(),
        }))
    }
}

/// Parse the embedded [`SEED_JSON`] into its provider entries. The seed is a build-time-checked
/// artifact, so a parse failure is a programmer error (a malformed commit), surfaced as
/// [`AuthError::Backend`] so the boot path can fail loudly rather than silently seed nothing.
pub fn seed_providers() -> Result<Vec<SeedProvider>, AuthError> {
    let file: SeedFile = serde_json::from_str(SEED_JSON)
        .map_err(|e| AuthError::Backend(format!("oauth-providers.json is malformed: {e}")))?;
    Ok(file.providers)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn embedded_seed_parses_and_carries_the_extracted_google_provider() {
        // The seed is the live-extracted non-secret config — assert the shape the prod row had so a
        // regen that drops/garbles it is caught at build/test time, not on a greenfield deploy.
        let providers = seed_providers().expect("embedded seed parses");
        let google = providers
            .iter()
            .find(|p| p.id == "google")
            .expect("seed carries the google provider");
        assert_eq!(google.kind, "google");
        assert_eq!(google.issuer, "https://accounts.google.com");
        assert_eq!(google.token_endpoint, "https://oauth2.googleapis.com/token");
        assert_eq!(google.scopes, "openid,email,profile");
        // Mirrors the live row: global (no workspace) and currently disabled.
        assert!(google.workspace_id.is_none());
        assert!(!google.enabled);
        // Every entry must name a secret env — secrets are never vendored.
        assert!(providers.iter().all(|p| !p.secret_env.is_empty()));
    }

    #[test]
    fn resolve_skips_when_the_secret_env_is_unset() {
        // A provider whose secret env is unset is a clean skip (like seed_admin), never a panic.
        let p = SeedProvider {
            id: "google".into(),
            kind: "google".into(),
            display_name: String::new(),
            client_id: "cid".into(),
            issuer: "https://accounts.google.com".into(),
            authorize_endpoint: "https://accounts.google.com/o/oauth2/v2/auth".into(),
            token_endpoint: "https://oauth2.googleapis.com/token".into(),
            userinfo_endpoint: "https://openidconnect.googleapis.com/v1/userinfo".into(),
            scopes: "openid,email".into(),
            enabled: true,
            workspace_id: None,
            secret_env: "GT_OAUTH_SEED_SECRET_TEST_UNSET_XYZ".into(),
        };
        std::env::remove_var("GT_OAUTH_SEED_SECRET_TEST_UNSET_XYZ");
        assert!(p.resolve().unwrap().is_none(), "unset secret env => skip");
    }

    #[test]
    fn resolve_builds_new_provider_when_secret_present_and_defaults_display_name() {
        let env = "GT_OAUTH_SEED_SECRET_TEST_PRESENT_ABC";
        std::env::set_var(env, "the-client-secret");
        let p = SeedProvider {
            id: "google".into(),
            kind: "google".into(),
            display_name: String::new(), // empty => defaults to id
            client_id: "cid".into(),
            issuer: "https://accounts.google.com".into(),
            authorize_endpoint: "https://accounts.google.com/o/oauth2/v2/auth".into(),
            token_endpoint: "https://oauth2.googleapis.com/token".into(),
            userinfo_endpoint: "https://openidconnect.googleapis.com/v1/userinfo".into(),
            scopes: "openid,email".into(),
            enabled: false,
            workspace_id: None,
            secret_env: env.into(),
        };
        let np = p.resolve().unwrap().expect("secret present => Some");
        assert_eq!(np.id, "google");
        assert_eq!(np.kind, ProviderKind::Google);
        assert_eq!(
            np.display_name, "google",
            "empty display_name defaults to id"
        );
        assert_eq!(np.client_secret, "the-client-secret");
        assert!(!np.enabled);
        std::env::remove_var(env);
    }

    #[test]
    fn resolve_rejects_an_unknown_kind() {
        let env = "GT_OAUTH_SEED_SECRET_TEST_KIND";
        std::env::set_var(env, "s");
        let p = SeedProvider {
            id: "x".into(),
            kind: "not-a-kind".into(),
            display_name: "X".into(),
            client_id: "c".into(),
            issuer: "i".into(),
            authorize_endpoint: "a".into(),
            token_endpoint: "t".into(),
            userinfo_endpoint: "u".into(),
            scopes: String::new(),
            enabled: true,
            workspace_id: None,
            secret_env: env.into(),
        };
        assert!(p.resolve().is_err(), "unknown kind is a hard error");
        std::env::remove_var(env);
    }
}
