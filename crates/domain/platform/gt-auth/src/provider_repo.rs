//! DB-backed OAuth/OIDC provider store (hq-idp-db.1) — the base of the admin-configurable login
//! providers epic.
//!
//! The OAuth/OIDC login adapter ([`oauth`](crate::oauth)) read a single provider from the
//! environment ([`OidcConfig::from_env`](crate::OidcConfig)). This module moves provider config into
//! Postgres so an admin can register MANY providers — Google/GitHub/Microsoft presets or a generic
//! OIDC IdP — and each appears as a login button, with NO redeploy. It owns:
//!
//! - [`ProviderKind`] — the variant of a stored provider (preset vs generic OIDC).
//! - [`ProviderPreset`] / [`presets`] — the baked endpoint catalog for the presets; the generic kind
//!   carries admin-supplied endpoints.
//! - [`ProviderRecord`] / [`NewProvider`] — a row read back / a row to create.
//! - [`ProviderRepo`] — the async CRUD port (list / get / create / delete).
//! - [`PgProviderRepo`] — the Postgres adapter, gated by `pg`. It SEALS the client secret on write
//!   ([`crypto::seal`](crate::crypto::seal)) and UNSEALS it only when building an
//!   [`OidcConfig`](crate::OidcConfig) for the handshake ([`ProviderRecord::into_oidc_config`]).
//!
//! Scope is GLOBAL — a login provider is deploy infrastructure, not tenant data — so the table is
//! `public.oauth_providers` (no `workspace_id`), mirroring `public.users`. Gated behind the `oauth`
//! feature with the rest of the provider store; the Postgres adapter additionally needs `pg`.

use async_trait::async_trait;

use crate::AuthError;

/// The variant of a stored login provider.
///
/// The three presets bake their authorize/token/userinfo endpoints (the admin only supplies the
/// client id/secret); [`Generic`](Self::Generic) is a raw OIDC provider whose endpoints the admin
/// supplies in full. The wire spelling (the TEXT stored in `oauth_providers.kind`) is the
/// lowercase name.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ProviderKind {
    /// Google (`accounts.google.com`).
    Google,
    /// GitHub (`github.com`).
    GitHub,
    /// Microsoft identity platform (`login.microsoftonline.com`, the `common` tenant).
    Microsoft,
    /// A generic OIDC provider — all endpoints supplied by the admin.
    Generic,
}

impl ProviderKind {
    /// The wire (column) spelling stored in `oauth_providers.kind`.
    pub fn as_str(&self) -> &'static str {
        match self {
            ProviderKind::Google => "google",
            ProviderKind::GitHub => "github",
            ProviderKind::Microsoft => "microsoft",
            ProviderKind::Generic => "generic",
        }
    }

    /// Parse the wire spelling back to a kind; an unknown value is [`AuthError::Backend`] (a
    /// corrupt/forward-incompatible row, never a silent default).
    pub fn parse(s: &str) -> Result<Self, AuthError> {
        match s {
            "google" => Ok(ProviderKind::Google),
            "github" => Ok(ProviderKind::GitHub),
            "microsoft" => Ok(ProviderKind::Microsoft),
            "generic" => Ok(ProviderKind::Generic),
            other => Err(AuthError::Backend(format!("unknown provider kind: {other}"))),
        }
    }
}

/// A preset's baked OAuth/OIDC endpoints + default scopes. Returned by [`presets`] so the admin CRUD
/// surface (hq-idp-db.4) can pre-fill a registration form: pick a preset, paste the client id/secret,
/// and the endpoints are already filled in. The generic kind has no preset — the admin supplies all
/// endpoints.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ProviderPreset {
    /// The preset this describes (never [`ProviderKind::Generic`]).
    pub kind: ProviderKind,
    /// A human label for the login button (e.g. "Google").
    pub display_name: &'static str,
    /// The issuer URL (`iss`).
    pub issuer: &'static str,
    /// The authorization endpoint the browser is redirected to.
    pub authorize_endpoint: &'static str,
    /// The token endpoint the authorization `code` is exchanged at.
    pub token_endpoint: &'static str,
    /// The userinfo endpoint read with the resulting access token.
    pub userinfo_endpoint: &'static str,
    /// The default comma-separated scopes a registration starts with.
    pub default_scopes: &'static str,
}

/// The baked endpoint catalog for the preset providers (Google / GitHub / Microsoft).
///
/// Each entry carries the public, well-known OAuth/OIDC endpoints + a sensible default scope set, so
/// registering a preset needs only a client id + secret. The generic kind is intentionally absent: a
/// generic OIDC provider has no well-known endpoints to bake, so the admin supplies them all.
pub fn presets() -> Vec<ProviderPreset> {
    vec![
        ProviderPreset {
            kind: ProviderKind::Google,
            display_name: "Google",
            issuer: "https://accounts.google.com",
            authorize_endpoint: "https://accounts.google.com/o/oauth2/v2/auth",
            token_endpoint: "https://oauth2.googleapis.com/token",
            userinfo_endpoint: "https://openidconnect.googleapis.com/v1/userinfo",
            default_scopes: "openid,email,profile",
        },
        ProviderPreset {
            kind: ProviderKind::GitHub,
            display_name: "GitHub",
            issuer: "https://github.com",
            authorize_endpoint: "https://github.com/login/oauth/authorize",
            token_endpoint: "https://github.com/login/oauth/access_token",
            userinfo_endpoint: "https://api.github.com/user",
            default_scopes: "read:user,user:email",
        },
        ProviderPreset {
            kind: ProviderKind::Microsoft,
            display_name: "Microsoft",
            issuer: "https://login.microsoftonline.com/common/v2.0",
            authorize_endpoint: "https://login.microsoftonline.com/common/oauth2/v2.0/authorize",
            token_endpoint: "https://login.microsoftonline.com/common/oauth2/v2.0/token",
            userinfo_endpoint: "https://graph.microsoft.com/oidc/userinfo",
            default_scopes: "openid,email,profile",
        },
    ]
}

/// Look up the preset for `kind`, or `None` for [`ProviderKind::Generic`] (no baked endpoints).
pub fn preset_for(kind: ProviderKind) -> Option<ProviderPreset> {
    presets().into_iter().find(|p| p.kind == kind)
}

/// A provider to create. The `client_secret` is cleartext here — the repo SEALS it
/// ([`crypto::seal`](crate::crypto::seal)) on write, so it never reaches the database in clear.
///
/// For a preset, build the endpoints from [`preset_for`] (the CRUD surface in hq-idp-db.4 does this);
/// for [`ProviderKind::Generic`] the caller supplies every endpoint.
#[derive(Clone, Debug)]
pub struct NewProvider {
    /// The stable id / primary key (also the wire token a login button carries).
    pub id: String,
    /// The provider variant.
    pub kind: ProviderKind,
    /// The human label for the login button.
    pub display_name: String,
    /// The registered client id.
    pub client_id: String,
    /// The registered client secret, in cleartext — sealed by the repo before it is stored.
    pub client_secret: String,
    /// The issuer URL (`iss`).
    pub issuer: String,
    /// The authorization endpoint.
    pub authorize_endpoint: String,
    /// The token endpoint.
    pub token_endpoint: String,
    /// The userinfo endpoint.
    pub userinfo_endpoint: String,
    /// Comma-separated granted scopes.
    pub scopes: String,
    /// Whether the provider shows as a login button.
    pub enabled: bool,
}

impl NewProvider {
    /// Build a [`NewProvider`] for a preset `kind` from its baked endpoints + default scopes,
    /// supplying only the per-deploy `id` / `display_name` / client credentials. Returns `None` for
    /// [`ProviderKind::Generic`] (it has no preset — use the struct literal with explicit endpoints).
    pub fn from_preset(
        id: impl Into<String>,
        kind: ProviderKind,
        client_id: impl Into<String>,
        client_secret: impl Into<String>,
    ) -> Option<Self> {
        let p = preset_for(kind)?;
        Some(NewProvider {
            id: id.into(),
            kind,
            display_name: p.display_name.to_owned(),
            client_id: client_id.into(),
            client_secret: client_secret.into(),
            issuer: p.issuer.to_owned(),
            authorize_endpoint: p.authorize_endpoint.to_owned(),
            token_endpoint: p.token_endpoint.to_owned(),
            userinfo_endpoint: p.userinfo_endpoint.to_owned(),
            scopes: p.default_scopes.to_owned(),
            enabled: true,
        })
    }
}

/// A partial update to an existing provider (hq-idp-db.4). Every field is optional: `None` leaves
/// the stored column untouched, `Some(_)` overwrites it. The `client_secret` is write-only — when
/// `Some`, the repo RE-SEALS the new cleartext ([`crypto::seal`](crate::crypto::seal)); when `None`,
/// the existing sealed blob is left in place (so a metadata edit, e.g. toggling `enabled`, does not
/// require re-supplying the secret).
#[derive(Clone, Debug, Default)]
pub struct PatchProvider {
    /// New human label, or `None` to leave it.
    pub display_name: Option<String>,
    /// New client id, or `None` to leave it.
    pub client_id: Option<String>,
    /// New client secret in cleartext (re-sealed by the repo), or `None` to keep the stored one.
    pub client_secret: Option<String>,
    /// New issuer URL, or `None` to leave it.
    pub issuer: Option<String>,
    /// New authorization endpoint, or `None` to leave it.
    pub authorize_endpoint: Option<String>,
    /// New token endpoint, or `None` to leave it.
    pub token_endpoint: Option<String>,
    /// New userinfo endpoint, or `None` to leave it.
    pub userinfo_endpoint: Option<String>,
    /// New comma-separated scopes, or `None` to leave them.
    pub scopes: Option<String>,
    /// Toggle whether the provider shows as a login button, or `None` to leave it.
    pub enabled: Option<bool>,
}

/// A provider read back from the store. The `client_secret_enc` is the SEALED blob — the cleartext
/// is recovered only in memory, only when building an [`OidcConfig`](crate::OidcConfig) for the
/// handshake ([`into_oidc_config`](Self::into_oidc_config)).
#[derive(Clone, Debug)]
pub struct ProviderRecord {
    /// The id / primary key.
    pub id: String,
    /// The provider variant.
    pub kind: ProviderKind,
    /// The human label for the login button.
    pub display_name: String,
    /// The registered client id.
    pub client_id: String,
    /// The SEALED client secret (`nonce || ciphertext+tag`, [`crypto::seal`](crate::crypto::seal)).
    /// Never the cleartext.
    pub client_secret_enc: Vec<u8>,
    /// The issuer URL (`iss`).
    pub issuer: String,
    /// The authorization endpoint.
    pub authorize_endpoint: String,
    /// The token endpoint.
    pub token_endpoint: String,
    /// The userinfo endpoint.
    pub userinfo_endpoint: String,
    /// Comma-separated granted scopes.
    pub scopes: String,
    /// Whether the provider shows as a login button.
    pub enabled: bool,
}

impl ProviderRecord {
    /// Unseal the client secret and assemble the [`OidcConfig`](crate::OidcConfig) the handshake
    /// adapter ([`OidcProvider`](crate::OidcProvider)) runs against, scoped to `workspace` and
    /// echoing `redirect_uri`. The secret is decrypted ONLY here, in memory — a sealed blob that
    /// fails to unseal (wrong/rotated key, corruption) is [`AuthError::Backend`].
    pub fn into_oidc_config(
        self,
        workspace: impl Into<String>,
        redirect_uri: impl Into<String>,
    ) -> Result<crate::OidcConfig, AuthError> {
        let secret = crate::crypto::unseal(&self.client_secret_enc)?;
        let client_secret = String::from_utf8(secret)
            .map_err(|_| AuthError::Backend("unsealed client secret is not utf-8".into()))?;
        Ok(crate::OidcConfig {
            issuer: self.issuer,
            client_id: self.client_id,
            client_secret,
            token_endpoint: self.token_endpoint,
            userinfo_endpoint: self.userinfo_endpoint,
            redirect_uri: redirect_uri.into(),
            workspace: workspace.into(),
            scopes: self
                .scopes
                .split(',')
                .map(str::trim)
                .filter(|s| !s.is_empty())
                .map(str::to_owned)
                .collect(),
        })
    }
}

/// The async CRUD port over the OAuth/OIDC provider store.
///
/// An abstract port (docs/03 Rule 4) so the admin surface (hq-idp-db.4) and the DB-resolution path
/// (hq-idp-db.2) depend on the contract, not the Postgres adapter. [`create`](Self::create) takes a
/// cleartext secret and is responsible for sealing it; reads return the sealed blob (cleartext is
/// recovered only via [`ProviderRecord::into_oidc_config`]).
#[async_trait]
pub trait ProviderRepo: Send + Sync {
    /// All registered providers, ordered by `created_at` (oldest first).
    async fn list(&self) -> Result<Vec<ProviderRecord>, AuthError>;
    /// One provider by id, or `None` if absent.
    async fn get(&self, id: &str) -> Result<Option<ProviderRecord>, AuthError>;
    /// Register `provider`, sealing its client secret before it is stored. Returns the stored
    /// record (with the sealed blob).
    async fn create(&self, provider: NewProvider) -> Result<ProviderRecord, AuthError>;
    /// Apply a partial update to the provider `id` (hq-idp-db.4): each `Some` field overwrites,
    /// each `None` leaves the column. A `Some` client secret is RE-SEALED; a `None` secret leaves
    /// the stored blob untouched. Returns the updated record, or `None` if no provider has that id.
    async fn patch(
        &self,
        id: &str,
        patch: PatchProvider,
    ) -> Result<Option<ProviderRecord>, AuthError>;
    /// Remove a provider by id; `true` if a row was deleted, `false` if none matched.
    async fn delete(&self, id: &str) -> Result<bool, AuthError>;
}

#[cfg(feature = "pg")]
pub use pg_impl::PgProviderRepo;

#[cfg(feature = "pg")]
mod pg_impl {
    use super::*;
    use sqlx::postgres::PgRow;
    use sqlx::{PgPool, Row};

    /// Postgres-backed [`ProviderRepo`] over the GLOBAL `public.oauth_providers` table.
    ///
    /// Statements are `public`-qualified (the table is cross-tenant, unlike the per-ws `users`/
    /// `refresh_tokens` which run unqualified against a `search_path`-scoped pool). The client
    /// secret is SEALED on [`create`](ProviderRepo::create) and only ever stored/returned as the
    /// sealed blob. Cloning is cheap: `PgPool` is an `Arc`.
    #[derive(Clone, Debug)]
    pub struct PgProviderRepo {
        pool: PgPool,
    }

    impl PgProviderRepo {
        /// Wrap a connection `pool`. The `public.oauth_providers` table is expected to already
        /// exist (provisioned via `migrations/auth/0006__create_oauth_providers.sql`).
        pub fn new(pool: PgPool) -> Self {
            PgProviderRepo { pool }
        }
    }

    /// Decode a `public.oauth_providers` row into a [`ProviderRecord`]. A column read fault is
    /// [`AuthError::Backend`] (an outage/corruption, never a denied request).
    fn row_to_record(row: &PgRow) -> Result<ProviderRecord, AuthError> {
        let kind: String = row
            .try_get("kind")
            .map_err(|e| AuthError::Backend(format!("oauth_providers postgres: {e}")))?;
        Ok(ProviderRecord {
            id: row
                .try_get("id")
                .map_err(|e| AuthError::Backend(format!("oauth_providers postgres: {e}")))?,
            kind: ProviderKind::parse(&kind)?,
            display_name: row
                .try_get("display_name")
                .map_err(|e| AuthError::Backend(format!("oauth_providers postgres: {e}")))?,
            client_id: row
                .try_get("client_id")
                .map_err(|e| AuthError::Backend(format!("oauth_providers postgres: {e}")))?,
            client_secret_enc: row
                .try_get("client_secret_enc")
                .map_err(|e| AuthError::Backend(format!("oauth_providers postgres: {e}")))?,
            issuer: row
                .try_get("issuer")
                .map_err(|e| AuthError::Backend(format!("oauth_providers postgres: {e}")))?,
            authorize_endpoint: row
                .try_get("authorize_endpoint")
                .map_err(|e| AuthError::Backend(format!("oauth_providers postgres: {e}")))?,
            token_endpoint: row
                .try_get("token_endpoint")
                .map_err(|e| AuthError::Backend(format!("oauth_providers postgres: {e}")))?,
            userinfo_endpoint: row
                .try_get("userinfo_endpoint")
                .map_err(|e| AuthError::Backend(format!("oauth_providers postgres: {e}")))?,
            scopes: row
                .try_get("scopes")
                .map_err(|e| AuthError::Backend(format!("oauth_providers postgres: {e}")))?,
            enabled: row
                .try_get("enabled")
                .map_err(|e| AuthError::Backend(format!("oauth_providers postgres: {e}")))?,
        })
    }

    #[async_trait]
    impl ProviderRepo for PgProviderRepo {
        async fn list(&self) -> Result<Vec<ProviderRecord>, AuthError> {
            let rows = sqlx::query(
                "SELECT id, kind, display_name, client_id, client_secret_enc, issuer, \
                 authorize_endpoint, token_endpoint, userinfo_endpoint, scopes, enabled \
                 FROM public.oauth_providers ORDER BY created_at",
            )
            .fetch_all(&self.pool)
            .await
            .map_err(|e| AuthError::Backend(format!("oauth_providers list: {e}")))?;
            rows.iter().map(row_to_record).collect()
        }

        async fn get(&self, id: &str) -> Result<Option<ProviderRecord>, AuthError> {
            let row = sqlx::query(
                "SELECT id, kind, display_name, client_id, client_secret_enc, issuer, \
                 authorize_endpoint, token_endpoint, userinfo_endpoint, scopes, enabled \
                 FROM public.oauth_providers WHERE id = $1",
            )
            .bind(id)
            .fetch_optional(&self.pool)
            .await
            .map_err(|e| AuthError::Backend(format!("oauth_providers get: {e}")))?;
            row.as_ref().map(row_to_record).transpose()
        }

        async fn create(&self, provider: NewProvider) -> Result<ProviderRecord, AuthError> {
            // Seal the cleartext secret BEFORE it touches the database — it is never stored in clear.
            let enc = crate::crypto::seal(provider.client_secret.as_bytes())?;
            sqlx::query(
                "INSERT INTO public.oauth_providers \
                 (id, kind, display_name, client_id, client_secret_enc, issuer, \
                  authorize_endpoint, token_endpoint, userinfo_endpoint, scopes, enabled) \
                 VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11)",
            )
            .bind(&provider.id)
            .bind(provider.kind.as_str())
            .bind(&provider.display_name)
            .bind(&provider.client_id)
            .bind(&enc)
            .bind(&provider.issuer)
            .bind(&provider.authorize_endpoint)
            .bind(&provider.token_endpoint)
            .bind(&provider.userinfo_endpoint)
            .bind(&provider.scopes)
            .bind(provider.enabled)
            .execute(&self.pool)
            .await
            .map_err(|e| AuthError::Backend(format!("oauth_providers create: {e}")))?;
            Ok(ProviderRecord {
                id: provider.id,
                kind: provider.kind,
                display_name: provider.display_name,
                client_id: provider.client_id,
                client_secret_enc: enc,
                issuer: provider.issuer,
                authorize_endpoint: provider.authorize_endpoint,
                token_endpoint: provider.token_endpoint,
                userinfo_endpoint: provider.userinfo_endpoint,
                scopes: provider.scopes,
                enabled: provider.enabled,
            })
        }

        async fn patch(
            &self,
            id: &str,
            patch: PatchProvider,
        ) -> Result<Option<ProviderRecord>, AuthError> {
            // Read-modify-write: load the current row, fold the `Some` fields over it, then write
            // it back. The secret is re-sealed only when the patch carries a new one; otherwise the
            // stored sealed blob rides through untouched (a metadata-only edit keeps the secret).
            let Some(current) = self.get(id).await? else {
                return Ok(None);
            };
            let enc = match patch.client_secret {
                Some(secret) => crate::crypto::seal(secret.as_bytes())?,
                None => current.client_secret_enc,
            };
            let next = ProviderRecord {
                id: current.id,
                kind: current.kind,
                display_name: patch.display_name.unwrap_or(current.display_name),
                client_id: patch.client_id.unwrap_or(current.client_id),
                client_secret_enc: enc,
                issuer: patch.issuer.unwrap_or(current.issuer),
                authorize_endpoint: patch.authorize_endpoint.unwrap_or(current.authorize_endpoint),
                token_endpoint: patch.token_endpoint.unwrap_or(current.token_endpoint),
                userinfo_endpoint: patch.userinfo_endpoint.unwrap_or(current.userinfo_endpoint),
                scopes: patch.scopes.unwrap_or(current.scopes),
                enabled: patch.enabled.unwrap_or(current.enabled),
            };
            sqlx::query(
                "UPDATE public.oauth_providers SET \
                 display_name = $2, client_id = $3, client_secret_enc = $4, issuer = $5, \
                 authorize_endpoint = $6, token_endpoint = $7, userinfo_endpoint = $8, \
                 scopes = $9, enabled = $10 WHERE id = $1",
            )
            .bind(&next.id)
            .bind(&next.display_name)
            .bind(&next.client_id)
            .bind(&next.client_secret_enc)
            .bind(&next.issuer)
            .bind(&next.authorize_endpoint)
            .bind(&next.token_endpoint)
            .bind(&next.userinfo_endpoint)
            .bind(&next.scopes)
            .bind(next.enabled)
            .execute(&self.pool)
            .await
            .map_err(|e| AuthError::Backend(format!("oauth_providers patch: {e}")))?;
            Ok(Some(next))
        }

        async fn delete(&self, id: &str) -> Result<bool, AuthError> {
            let res = sqlx::query("DELETE FROM public.oauth_providers WHERE id = $1")
                .bind(id)
                .execute(&self.pool)
                .await
                .map_err(|e| AuthError::Backend(format!("oauth_providers delete: {e}")))?;
            Ok(res.rows_affected() > 0)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn presets_carry_baked_endpoints_and_cover_the_three_kinds() {
        let all = presets();
        assert_eq!(all.len(), 3);
        let google = all.iter().find(|p| p.kind == ProviderKind::Google).unwrap();
        assert_eq!(google.issuer, "https://accounts.google.com");
        assert_eq!(google.token_endpoint, "https://oauth2.googleapis.com/token");
        assert!(!google.authorize_endpoint.is_empty());
        assert!(!google.userinfo_endpoint.is_empty());
        // No preset is the generic kind.
        assert!(all.iter().all(|p| p.kind != ProviderKind::Generic));
    }

    #[test]
    fn from_preset_prefills_endpoints_generic_has_none() {
        let np = NewProvider::from_preset("g1", ProviderKind::GitHub, "cid", "secret").unwrap();
        assert_eq!(np.kind, ProviderKind::GitHub);
        assert_eq!(np.token_endpoint, "https://github.com/login/oauth/access_token");
        assert_eq!(np.client_id, "cid");
        // Generic has no preset to build from.
        assert!(NewProvider::from_preset("x", ProviderKind::Generic, "c", "s").is_none());
        assert!(preset_for(ProviderKind::Generic).is_none());
    }

    #[test]
    fn provider_kind_wire_round_trips() {
        for k in [
            ProviderKind::Google,
            ProviderKind::GitHub,
            ProviderKind::Microsoft,
            ProviderKind::Generic,
        ] {
            assert_eq!(ProviderKind::parse(k.as_str()).unwrap(), k);
        }
        assert!(ProviderKind::parse("nope").is_err());
    }

    // --- PG-gated contract tests: round-trip against a real `public.oauth_providers` table. ---
    //
    // No-ops when `GT_PG_URL` is unset (same gate as the other `pg` adapters), so a developer box /
    // CI without Postgres still passes. Run with `--test-threads=1` and a `GT_SECRET_KEY` set.
    #[cfg(feature = "pg")]
    mod pg_contract {
        use super::*;
        use sqlx::PgPool;

        /// Connect to the contract-test Postgres, or `None` when `GT_PG_URL` is unset.
        async fn pool_or_skip() -> Option<PgPool> {
            let url = std::env::var("GT_PG_URL").ok()?;
            Some(
                PgPool::connect(&url)
                    .await
                    .expect("GT_PG_URL must point at a reachable Postgres"),
            )
        }

        /// Provision `public.oauth_providers` for the test. `CREATE TABLE IF NOT EXISTS` is not
        /// atomic against two sessions creating the same table (a duplicate `pg_type`, 23505), so
        /// the create runs under a transaction-scoped advisory lock that serializes first-runs —
        /// the same guard `refresh_pg`'s `ensure_table` uses.
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

        /// A unique id per test so rows never collide under the shared table (the suite is purely
        /// additive — never TRUNCATEs — so a parallel sibling is never clobbered).
        fn unique_id(tag: &str) -> String {
            use std::time::{SystemTime, UNIX_EPOCH};
            let n = SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_nanos();
            format!("test-{tag}-{n}")
        }

        #[tokio::test]
        async fn create_read_round_trips_and_decrypts_the_secret() {
            // The seal/unseal needs a master key; set one for the test process.
            std::env::set_var(
                crate::ENV_SECRET_KEY,
                "0000000000000000000000000000000000000000000000000000000000000000",
            );
            let Some(pool) = pool_or_skip().await else {
                return;
            };
            ensure_table(&pool).await;
            let repo = PgProviderRepo::new(pool.clone());

            let id = unique_id("google");
            let cleartext = "super-secret-client-secret";
            let new = NewProvider::from_preset(&id, ProviderKind::Google, "client-123", cleartext)
                .unwrap();
            let stored = repo.create(new).await.unwrap();

            // The stored ciphertext is NOT the cleartext (encrypted at rest).
            assert_ne!(stored.client_secret_enc, cleartext.as_bytes());
            assert!(!stored
                .client_secret_enc
                .windows(cleartext.len())
                .any(|w| w == cleartext.as_bytes()));

            // Read it back: the persisted BYTEA matches and decrypts to the original secret.
            let read = repo.get(&id).await.unwrap().expect("provider present");
            assert_eq!(read.id, id);
            assert_eq!(read.kind, ProviderKind::Google);
            assert_eq!(read.client_id, "client-123");
            assert_eq!(read.client_secret_enc, stored.client_secret_enc);
            let cfg = read
                .into_oidc_config("acme", "https://gt.test/auth/callback")
                .unwrap();
            assert_eq!(cfg.client_secret, cleartext);
            // The preset's baked endpoints survived the round-trip.
            assert_eq!(cfg.token_endpoint, "https://oauth2.googleapis.com/token");
            assert_eq!(cfg.workspace, "acme");
            assert_eq!(cfg.redirect_uri, "https://gt.test/auth/callback");

            // The raw BYTEA in PG itself (not via the adapter) is not the cleartext.
            let raw: Vec<u8> =
                sqlx::query_scalar("SELECT client_secret_enc FROM public.oauth_providers WHERE id = $1")
                    .bind(&id)
                    .fetch_one(&pool)
                    .await
                    .unwrap();
            assert!(!raw.windows(cleartext.len()).any(|w| w == cleartext.as_bytes()));

            // list() includes it; delete() removes it.
            assert!(repo.list().await.unwrap().iter().any(|p| p.id == id));
            assert!(repo.delete(&id).await.unwrap());
            assert!(repo.get(&id).await.unwrap().is_none());
            assert!(!repo.delete(&id).await.unwrap());
        }

        #[tokio::test]
        async fn patch_overwrites_fields_and_secret_is_optional() {
            std::env::set_var(
                crate::ENV_SECRET_KEY,
                "0000000000000000000000000000000000000000000000000000000000000000",
            );
            let Some(pool) = pool_or_skip().await else {
                return;
            };
            ensure_table(&pool).await;
            let repo = PgProviderRepo::new(pool.clone());

            let id = unique_id("patch");
            let new = NewProvider::from_preset(&id, ProviderKind::Google, "client-1", "secret-1")
                .unwrap();
            let original_enc = repo.create(new).await.unwrap().client_secret_enc;

            // A metadata-only patch (no secret) toggles `enabled` + edits the label, and LEAVES the
            // sealed secret untouched — a partial edit never requires re-supplying the credential.
            let patched = repo
                .patch(
                    &id,
                    PatchProvider {
                        enabled: Some(false),
                        display_name: Some("Renamed".into()),
                        ..Default::default()
                    },
                )
                .await
                .unwrap()
                .expect("provider present");
            assert!(!patched.enabled);
            assert_eq!(patched.display_name, "Renamed");
            assert_eq!(patched.client_secret_enc, original_enc, "secret left unchanged");
            assert_eq!(patched.client_id, "client-1", "untouched field preserved");

            // A patch WITH a secret re-seals it: the stored blob changes, and it decrypts to the new
            // cleartext via into_oidc_config.
            let rotated = repo
                .patch(
                    &id,
                    PatchProvider { client_secret: Some("secret-2".into()), ..Default::default() },
                )
                .await
                .unwrap()
                .unwrap();
            assert_ne!(rotated.client_secret_enc, original_enc, "secret re-sealed");
            let cfg = rotated.into_oidc_config("acme", "https://gt.test/cb").unwrap();
            assert_eq!(cfg.client_secret, "secret-2");

            // Patching an absent id is None (the handler maps it to 404).
            assert!(repo
                .patch(
                    "no-such-provider-xyz",
                    PatchProvider { enabled: Some(true), ..Default::default() },
                )
                .await
                .unwrap()
                .is_none());

            repo.delete(&id).await.unwrap();
        }

        #[tokio::test]
        async fn generic_provider_persists_manual_endpoints() {
            std::env::set_var(
                crate::ENV_SECRET_KEY,
                "0000000000000000000000000000000000000000000000000000000000000000",
            );
            let Some(pool) = pool_or_skip().await else {
                return;
            };
            ensure_table(&pool).await;
            let repo = PgProviderRepo::new(pool.clone());

            let id = unique_id("generic");
            let new = NewProvider {
                id: id.clone(),
                kind: ProviderKind::Generic,
                display_name: "Corp SSO".into(),
                client_id: "corp-client".into(),
                client_secret: "corp-secret".into(),
                issuer: "https://sso.corp.test/".into(),
                authorize_endpoint: "https://sso.corp.test/authorize".into(),
                token_endpoint: "https://sso.corp.test/token".into(),
                userinfo_endpoint: "https://sso.corp.test/userinfo".into(),
                scopes: "openid,email".into(),
                enabled: true,
            };
            repo.create(new).await.unwrap();
            let cfg = repo
                .get(&id)
                .await
                .unwrap()
                .unwrap()
                .into_oidc_config("acme", "https://gt.test/cb")
                .unwrap();
            assert_eq!(cfg.issuer, "https://sso.corp.test/");
            assert_eq!(cfg.token_endpoint, "https://sso.corp.test/token");
            assert_eq!(cfg.client_secret, "corp-secret");
            assert_eq!(cfg.scopes, vec!["openid".to_string(), "email".to_string()]);
            repo.delete(&id).await.unwrap();
        }
    }
}
