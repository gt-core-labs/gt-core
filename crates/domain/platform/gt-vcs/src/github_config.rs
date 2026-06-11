//! DB-backed platform GitHub App config (hq-61ea43).
//!
//! The App identity (App ID, URL slug, RS256 private key PEM, webhook secret) used to come ONLY from
//! mounted files / env vars ([`GithubAppConfig::from_env`](crate::github::GithubAppConfig::from_env)).
//! This module moves it into `public.github_app_config` (ONE global row) so an admin configures the
//! App from the UI — no redeploy, no `.env`. The two secrets (private key, webhook secret) are
//! AES-GCM sealed at rest with the SAME helper the PAT/OAuth secret use ([`gt_auth::seal`]).
//!
//! [`GithubAppSource`] is what the REST/webhook surfaces hold: a per-request
//! [`load`](GithubAppSource::load) that resolves the live [`GithubAppConfig`] from the DB (falling
//! back to env for back-compat), so the install flow + webhook pick up a UI change without a restart.

use gt_events::AppError;
use serde::{Deserialize, Serialize};

use crate::github::GithubAppConfig;

/// The admin-facing, SECRET-FREE view of the stored config (read surface). The private key and
/// webhook secret are NEVER returned — only whether each is set, so the UI can show "configured".
#[derive(Debug, Clone, Serialize)]
#[cfg_attr(feature = "axum", derive(utoipa::ToSchema))]
pub struct GithubAppConfigView {
    /// The numeric GitHub App ID (public).
    pub app_id: String,
    /// The App's URL slug (public).
    pub app_slug: String,
    /// Whether a private key PEM is stored (the key itself is never returned).
    pub has_private_key: bool,
    /// Whether a webhook secret is stored (the secret itself is never returned).
    pub has_webhook_secret: bool,
}

/// The admin-facing write body to upsert the config. `private_key_pem` / `webhook_secret` are
/// WRITE-ONLY: supply to set/rotate, omit (`None`) to keep the stored value on an update. On the
/// FIRST insert a `private_key_pem` is required (there is nothing to keep).
#[derive(Debug, Clone, Deserialize)]
#[cfg_attr(feature = "axum", derive(utoipa::ToSchema))]
pub struct GithubAppConfigInput {
    /// The numeric GitHub App ID.
    pub app_id: String,
    /// The App's URL slug (the `<app>` in `github.com/apps/<app>/installations/new`).
    pub app_slug: String,
    /// The RS256 private key PEM (cleartext on the wire; sealed at rest). `None` keeps the stored key.
    #[serde(default)]
    pub private_key_pem: Option<String>,
    /// The webhook HMAC secret (cleartext on the wire; sealed at rest). `None` keeps the stored one.
    #[serde(default)]
    pub webhook_secret: Option<String>,
}

/// How a surface resolves the live [`GithubAppConfig`]: from the DB row (env fallback), or a fixed
/// value (tests / programmatic). Cheap to clone (a pool handle or a small config).
#[derive(Clone)]
pub enum GithubAppSource {
    /// Resolve from `public.github_app_config`, falling back to env when no row exists.
    #[cfg(feature = "pg")]
    Db(PgGithubAppConfig),
    /// A fixed config (or none) — the test / programmatic seam.
    Fixed(Option<GithubAppConfig>),
}

impl GithubAppSource {
    /// Resolve the live config, or `None` when no App is configured (the surface answers `503`).
    pub async fn load(&self) -> Result<Option<GithubAppConfig>, AppError> {
        match self {
            #[cfg(feature = "pg")]
            GithubAppSource::Db(store) => store.load().await,
            GithubAppSource::Fixed(cfg) => Ok(cfg.clone()),
        }
    }

    /// The secret-free view of the stored config, or `None` when unset. Only the DB-backed source
    /// supports it (the admin read surface); a `Fixed` source is [`AppError::Validation`].
    pub async fn view(&self) -> Result<Option<GithubAppConfigView>, AppError> {
        match self {
            #[cfg(feature = "pg")]
            GithubAppSource::Db(store) => store.view().await,
            GithubAppSource::Fixed(_) => Err(AppError::Validation(
                "GitHub App config is not DB-backed on this deploy".into(),
            )),
        }
    }

    /// Upsert the config (sealing secrets). DB-backed only; a `Fixed` source is
    /// [`AppError::Validation`].
    pub async fn upsert(&self, input: GithubAppConfigInput) -> Result<(), AppError> {
        match self {
            #[cfg(feature = "pg")]
            GithubAppSource::Db(store) => store.upsert(input).await,
            GithubAppSource::Fixed(_) => {
                let _ = input;
                Err(AppError::Validation(
                    "GitHub App config is not DB-backed on this deploy".into(),
                ))
            }
        }
    }
}

#[cfg(feature = "pg")]
pub use pg_impl::PgGithubAppConfig;

#[cfg(feature = "pg")]
mod pg_impl {
    use super::*;
    use sqlx::{PgPool, Row};

    /// The fixed primary key of the single global config row.
    const ROW_ID: &str = "default";

    /// Postgres-backed store over the single-row `public.github_app_config` table. Seals the private
    /// key + webhook secret on write; recovers them only in memory on [`load`](Self::load). Cloning is
    /// cheap (`PgPool` is an `Arc`).
    #[derive(Clone, Debug)]
    pub struct PgGithubAppConfig {
        pool: PgPool,
    }

    impl PgGithubAppConfig {
        /// Wrap a connection `pool`. `public.github_app_config` is provisioned by the module migration.
        pub fn new(pool: PgPool) -> Self {
            PgGithubAppConfig { pool }
        }

        /// Resolve the live [`GithubAppConfig`]: the stored row (unsealing the secrets), or — when no
        /// row exists — [`GithubAppConfig::from_env`] (back-compat for env-configured deploys).
        pub async fn load(&self) -> Result<Option<GithubAppConfig>, AppError> {
            let row = sqlx::query(
                "SELECT app_id, app_slug, private_key_enc, webhook_secret_enc \
                 FROM public.github_app_config WHERE id = $1",
            )
            .bind(ROW_ID)
            .fetch_optional(&self.pool)
            .await
            .map_err(|e| AppError::Other(format!("github_app_config load: {e}")))?;

            let Some(row) = row else {
                // No DB row — fall back to the env/file config so an existing deploy keeps working.
                return GithubAppConfig::from_env();
            };

            let app_id: String = row
                .try_get("app_id")
                .map_err(|e| AppError::Other(format!("github_app_config: {e}")))?;
            let app_slug: String = row
                .try_get("app_slug")
                .map_err(|e| AppError::Other(format!("github_app_config: {e}")))?;
            let key_enc: Vec<u8> = row
                .try_get("private_key_enc")
                .map_err(|e| AppError::Other(format!("github_app_config: {e}")))?;
            let hook_enc: Option<Vec<u8>> = row
                .try_get("webhook_secret_enc")
                .map_err(|e| AppError::Other(format!("github_app_config: {e}")))?;

            let private_key_pem = gt_auth::unseal(&key_enc)
                .map_err(|e| AppError::Other(format!("github_app_config unseal key: {e}")))?;
            let webhook_secret = match hook_enc {
                Some(blob) => {
                    let bytes = gt_auth::unseal(&blob).map_err(|e| {
                        AppError::Other(format!("github_app_config unseal webhook: {e}"))
                    })?;
                    Some(
                        String::from_utf8(bytes).map_err(|_| {
                            AppError::Other("unsealed webhook secret is not utf-8".into())
                        })?,
                    )
                }
                None => None,
            };

            Ok(Some(GithubAppConfig::new(
                app_id,
                private_key_pem,
                app_slug,
                webhook_secret,
            )))
        }

        /// The secret-free view of the stored row, or `None` when no row exists.
        pub async fn view(&self) -> Result<Option<GithubAppConfigView>, AppError> {
            let row = sqlx::query(
                "SELECT app_id, app_slug, \
                 (webhook_secret_enc IS NOT NULL) AS has_hook \
                 FROM public.github_app_config WHERE id = $1",
            )
            .bind(ROW_ID)
            .fetch_optional(&self.pool)
            .await
            .map_err(|e| AppError::Other(format!("github_app_config view: {e}")))?;
            let Some(row) = row else {
                return Ok(None);
            };
            Ok(Some(GithubAppConfigView {
                app_id: row
                    .try_get("app_id")
                    .map_err(|e| AppError::Other(format!("github_app_config: {e}")))?,
                app_slug: row
                    .try_get("app_slug")
                    .map_err(|e| AppError::Other(format!("github_app_config: {e}")))?,
                // A row always carries a private key (NOT NULL), so it is always set when present.
                has_private_key: true,
                has_webhook_secret: row
                    .try_get("has_hook")
                    .map_err(|e| AppError::Other(format!("github_app_config: {e}")))?,
            }))
        }

        /// Upsert the config, sealing the secrets before they touch the database. A `None`
        /// `private_key_pem` / `webhook_secret` KEEPS the stored value (write-only rotate); a first
        /// insert with no `private_key_pem` is a [`AppError::Validation`] (there is nothing to keep).
        pub async fn upsert(&self, input: GithubAppConfigInput) -> Result<(), AppError> {
            let existing = self.view().await?;
            // Seal a supplied private key; otherwise reuse the stored blob (error if there is none yet).
            let key_enc: Vec<u8> = match input.private_key_pem.as_deref() {
                Some(pem) if !pem.trim().is_empty() => gt_auth::seal(pem.as_bytes())
                    .map_err(|e| AppError::Other(format!("github_app_config seal key: {e}")))?,
                _ => {
                    if existing.is_none() {
                        return Err(AppError::Validation(
                            "private_key_pem is required when first configuring the GitHub App".into(),
                        ));
                    }
                    // Keep the stored key: read it back as the sealed blob.
                    sqlx::query_scalar::<_, Vec<u8>>(
                        "SELECT private_key_enc FROM public.github_app_config WHERE id = $1",
                    )
                    .bind(ROW_ID)
                    .fetch_one(&self.pool)
                    .await
                    .map_err(|e| AppError::Other(format!("github_app_config keep key: {e}")))?
                }
            };
            // Seal a supplied webhook secret; otherwise keep the stored one (NULL stays NULL).
            let hook_enc: Option<Vec<u8>> = match input.webhook_secret.as_deref() {
                Some(s) if !s.trim().is_empty() => Some(
                    gt_auth::seal(s.as_bytes())
                        .map_err(|e| AppError::Other(format!("github_app_config seal hook: {e}")))?,
                ),
                _ => sqlx::query_scalar::<_, Option<Vec<u8>>>(
                    "SELECT webhook_secret_enc FROM public.github_app_config WHERE id = $1",
                )
                .bind(ROW_ID)
                .fetch_optional(&self.pool)
                .await
                .map_err(|e| AppError::Other(format!("github_app_config keep hook: {e}")))?
                .flatten(),
            };

            sqlx::query(
                "INSERT INTO public.github_app_config \
                 (id, app_id, app_slug, private_key_enc, webhook_secret_enc, updated_at) \
                 VALUES ($1, $2, $3, $4, $5, now()) \
                 ON CONFLICT (id) DO UPDATE SET \
                 app_id = EXCLUDED.app_id, app_slug = EXCLUDED.app_slug, \
                 private_key_enc = EXCLUDED.private_key_enc, \
                 webhook_secret_enc = EXCLUDED.webhook_secret_enc, updated_at = now()",
            )
            .bind(ROW_ID)
            .bind(&input.app_id)
            .bind(&input.app_slug)
            .bind(&key_enc)
            .bind(&hook_enc)
            .execute(&self.pool)
            .await
            .map_err(|e| AppError::Other(format!("github_app_config upsert: {e}")))?;
            Ok(())
        }
    }
}
