//! OAuth client registry — gt-core as an authorization SERVER (gtcore-95f950).
//!
//! The sibling [`provider_repo`](crate::provider_repo) is the INBOUND-SSO side: the external IdPs
//! gt-core logs its users INTO (Google/GitHub/generic OIDC). This module is the OUTBOUND side: the
//! OAuth **clients** (relying parties such as Claude.ai) that authenticate AGAINST gt-core's own
//! `/oauth/authorize` + `/oauth/token` endpoints. An admin registers a client with its
//! `client_id`/`client_secret`, the exact `redirect_uris` it may return to, and the `scopes` it may
//! request; the token endpoint then validates an incoming code exchange against that row.
//!
//! ## Secret at rest
//!
//! The `client_secret` is NEVER stored in cleartext — [`PgOauthClientRepo::register`] SEALS it with
//! [`crypto::seal`](crate::crypto::seal) (AES-256-GCM under `GT_SECRET_KEY`, the SAME infra the
//! provider store reuses) before it touches the database, and reads return only the sealed blob.
//! The cleartext is recovered in memory solely when the token endpoint verifies a presented secret.
//! Admin listings render the secret-free [`OauthClientView`], so a secret never leaves the process.
//!
//! ## Redirect URI validation (exact match, no wildcards)
//!
//! A registered `redirect_uri` is matched **byte-for-byte** ([`OauthClient::allows_redirect_uri`]).
//! Registration rejects a URI that is relative, carries a fragment, or contains a `*` wildcard
//! ([`NewOauthClient::validate`]): a wildcard could never match an exact comparison and only invites
//! the open-redirect mistake exact matching exists to prevent.
//!
//! Gated behind the `oauth` feature with the rest of the provider store; the Postgres adapter
//! ([`PgOauthClientRepo`]) additionally needs `pg`.

use async_trait::async_trait;

use crate::AuthError;

/// A registered OAuth client read back from the store.
///
/// The `client_secret_enc` is the SEALED blob ([`crypto::seal`](crate::crypto::seal)) — never the
/// cleartext. Use [`redacted`](Self::redacted) for anything an admin sees and
/// [`allows_redirect_uri`](Self::allows_redirect_uri) to validate a presented callback.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct OauthClient {
    /// The public client identifier the relying party presents (primary key).
    pub client_id: String,
    /// The SEALED client secret (`nonce || ciphertext+tag`). Never the cleartext.
    pub client_secret_enc: Vec<u8>,
    /// A human label for the client (e.g. "Claude.ai").
    pub display_name: String,
    /// The exact callback URLs the client may return to. Matched byte-for-byte, no wildcards.
    pub redirect_uris: Vec<String>,
    /// The scopes the client may request.
    pub scopes: Vec<String>,
    /// Whether the client may complete a flow. A disabled row is a soft revoke kept for audit;
    /// [`OauthClientRepo::revoke`] deletes it outright.
    pub enabled: bool,
}

impl OauthClient {
    /// Whether `uri` is one of the client's registered redirect URIs, compared EXACTLY (no
    /// prefix/wildcard match — an open-redirect hedge). This is the check the `/oauth/authorize`
    /// and `/oauth/token` legs run against a presented `redirect_uri`.
    pub fn allows_redirect_uri(&self, uri: &str) -> bool {
        self.redirect_uris.iter().any(|u| u == uri)
    }

    /// The secret-free projection an admin listing renders — every field except the sealed secret.
    pub fn redacted(&self) -> OauthClientView {
        OauthClientView {
            client_id: self.client_id.clone(),
            display_name: self.display_name.clone(),
            redirect_uris: self.redirect_uris.clone(),
            scopes: self.scopes.clone(),
            enabled: self.enabled,
        }
    }
}

/// A client to register. The `client_secret` is cleartext here — [`OauthClientRepo::register`]
/// SEALS it ([`crypto::seal`](crate::crypto::seal)) on write, so it never reaches the database in
/// clear. Call [`validate`](Self::validate) before persisting (the repo does).
#[derive(Clone, Debug)]
pub struct NewOauthClient {
    /// The public client identifier (primary key). Non-empty.
    pub client_id: String,
    /// The client secret in cleartext — sealed by the repo before it is stored. Non-empty.
    pub client_secret: String,
    /// A human label for the client.
    pub display_name: String,
    /// The exact callback URLs the client may return to. At least one; each absolute, fragment-free,
    /// and wildcard-free.
    pub redirect_uris: Vec<String>,
    /// The scopes the client may request. May be empty.
    pub scopes: Vec<String>,
}

impl NewOauthClient {
    /// Reject a malformed registration BEFORE it is sealed and stored: a blank `client_id`, a blank
    /// secret, no redirect URI, or any redirect URI that fails [`validate_redirect_uri`]. Returns
    /// [`AuthError::InvalidClient`] naming the first problem — a caller error, never a store fault.
    pub fn validate(&self) -> Result<(), AuthError> {
        if self.client_id.trim().is_empty() {
            return Err(AuthError::InvalidClient("client_id must not be blank".into()));
        }
        if self.client_secret.is_empty() {
            return Err(AuthError::InvalidClient("client_secret must not be blank".into()));
        }
        if self.redirect_uris.is_empty() {
            return Err(AuthError::InvalidClient(
                "at least one redirect_uri is required".into(),
            ));
        }
        for uri in &self.redirect_uris {
            validate_redirect_uri(uri)?;
        }
        Ok(())
    }
}

/// The secret-redacted projection of an [`OauthClient`] — every field except the sealed secret.
/// This is what [`OauthClientRepo::list`] callers render so a client secret never leaves the
/// process (acceptance: "List registered clients (secrets redacted)").
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct OauthClientView {
    /// The public client identifier.
    pub client_id: String,
    /// The human label.
    pub display_name: String,
    /// The exact registered callback URLs.
    pub redirect_uris: Vec<String>,
    /// The scopes the client may request.
    pub scopes: Vec<String>,
    /// Whether the client is enabled.
    pub enabled: bool,
}

/// Validate one redirect URI for registration: it must be absolute (carry a `scheme://`),
/// fragment-free (OAuth forbids a fragment in a redirect URI), and free of the `*` wildcard
/// (redirect URIs are matched EXACTLY — a wildcard could never match and only invites an
/// open-redirect mistake). A whitespace-bearing URI is rejected too. Returns
/// [`AuthError::InvalidClient`] naming the fault.
pub fn validate_redirect_uri(uri: &str) -> Result<(), AuthError> {
    if uri.trim().is_empty() {
        return Err(AuthError::InvalidClient("redirect_uri must not be blank".into()));
    }
    if uri.contains('*') {
        return Err(AuthError::InvalidClient(format!(
            "redirect_uri must not contain a wildcard: {uri}"
        )));
    }
    if uri.contains('#') {
        return Err(AuthError::InvalidClient(format!(
            "redirect_uri must not contain a fragment: {uri}"
        )));
    }
    if uri.chars().any(char::is_whitespace) {
        return Err(AuthError::InvalidClient(format!(
            "redirect_uri must not contain whitespace: {uri}"
        )));
    }
    // Absolute URI: a scheme followed by `://` and a non-empty remainder. This keeps a relative
    // path (`/callback`) — which an exact match against an attacker-controlled origin cannot make
    // safe — out of the registry.
    match uri.split_once("://") {
        Some((scheme, rest)) if !scheme.is_empty() && !rest.is_empty() => Ok(()),
        _ => Err(AuthError::InvalidClient(format!(
            "redirect_uri must be an absolute URL (scheme://host/...): {uri}"
        ))),
    }
}

/// The async CRUD port over the OAuth client registry.
///
/// An abstract port (docs/03 Rule 4) so the admin surface (the `gt-oauth-client` CLI) and the future
/// `/oauth/token` verification path depend on the contract, not the Postgres adapter.
/// [`register`](Self::register) takes a cleartext secret and is responsible for sealing it; reads
/// return the sealed blob (cleartext is recovered only when verifying a presented secret).
#[async_trait]
pub trait OauthClientRepo: Send + Sync {
    /// Register `client`, validating it ([`NewOauthClient::validate`]) and sealing its secret before
    /// it is stored. Returns the stored record (with the sealed blob). A duplicate `client_id` is
    /// [`AuthError::InvalidClient`].
    async fn register(&self, client: NewOauthClient) -> Result<OauthClient, AuthError>;
    /// All registered clients, ordered by `created_at` (oldest first). Callers render
    /// [`OauthClient::redacted`] so no secret leaves the process.
    async fn list(&self) -> Result<Vec<OauthClient>, AuthError>;
    /// One client by `client_id`, or `None` if absent.
    async fn get(&self, client_id: &str) -> Result<Option<OauthClient>, AuthError>;
    /// Revoke (delete) a client by `client_id`; `true` if a row was removed, `false` if none
    /// matched.
    async fn revoke(&self, client_id: &str) -> Result<bool, AuthError>;
}

#[cfg(feature = "pg")]
pub use pg_impl::PgOauthClientRepo;

#[cfg(feature = "pg")]
mod pg_impl {
    use super::*;
    use sqlx::postgres::PgRow;
    use sqlx::{PgPool, Row};

    /// Postgres-backed [`OauthClientRepo`] over the GLOBAL `public.oauth_clients` table.
    ///
    /// Statements are `public`-qualified (the registry is cross-tenant, like `oauth_providers`). The
    /// client secret is SEALED on [`register`](OauthClientRepo::register) and only ever stored/
    /// returned as the sealed blob. Cloning is cheap: `PgPool` is an `Arc`.
    #[derive(Clone, Debug)]
    pub struct PgOauthClientRepo {
        pool: PgPool,
    }

    impl PgOauthClientRepo {
        /// Wrap a connection `pool`. The `public.oauth_clients` table is expected to already exist
        /// (provisioned via `migrations/auth/0013__create_oauth_clients.sql`).
        pub fn new(pool: PgPool) -> Self {
            PgOauthClientRepo { pool }
        }
    }

    const COLS: &str =
        "client_id, client_secret_enc, display_name, redirect_uris, scopes, enabled";

    /// Decode a `public.oauth_clients` row into an [`OauthClient`]. A column read fault is
    /// [`AuthError::Backend`] (an outage/corruption, never a denied request).
    fn row_to_client(row: &PgRow) -> Result<OauthClient, AuthError> {
        Ok(OauthClient {
            client_id: row
                .try_get("client_id")
                .map_err(|e| AuthError::Backend(format!("oauth_clients postgres: {e}")))?,
            client_secret_enc: row
                .try_get("client_secret_enc")
                .map_err(|e| AuthError::Backend(format!("oauth_clients postgres: {e}")))?,
            display_name: row
                .try_get("display_name")
                .map_err(|e| AuthError::Backend(format!("oauth_clients postgres: {e}")))?,
            redirect_uris: row
                .try_get("redirect_uris")
                .map_err(|e| AuthError::Backend(format!("oauth_clients postgres: {e}")))?,
            scopes: row
                .try_get("scopes")
                .map_err(|e| AuthError::Backend(format!("oauth_clients postgres: {e}")))?,
            enabled: row
                .try_get("enabled")
                .map_err(|e| AuthError::Backend(format!("oauth_clients postgres: {e}")))?,
        })
    }

    #[async_trait]
    impl OauthClientRepo for PgOauthClientRepo {
        async fn register(&self, client: NewOauthClient) -> Result<OauthClient, AuthError> {
            // Reject a malformed registration before any DB work (blank id/secret, bad redirect).
            client.validate()?;
            // A duplicate id would surface as a unique-violation Backend error; pre-check so the
            // caller gets the clearer InvalidClient contract instead.
            if self.get(&client.client_id).await?.is_some() {
                return Err(AuthError::InvalidClient(format!(
                    "client_id already registered: {}",
                    client.client_id
                )));
            }
            // Seal the cleartext secret BEFORE it touches the database — it is never stored in clear.
            let enc = crate::crypto::seal(client.client_secret.as_bytes())?;
            sqlx::query(
                "INSERT INTO public.oauth_clients \
                 (client_id, client_secret_enc, display_name, redirect_uris, scopes, enabled) \
                 VALUES ($1, $2, $3, $4, $5, TRUE)",
            )
            .bind(&client.client_id)
            .bind(&enc)
            .bind(&client.display_name)
            .bind(&client.redirect_uris)
            .bind(&client.scopes)
            .execute(&self.pool)
            .await
            .map_err(|e| AuthError::Backend(format!("oauth_clients register: {e}")))?;
            Ok(OauthClient {
                client_id: client.client_id,
                client_secret_enc: enc,
                display_name: client.display_name,
                redirect_uris: client.redirect_uris,
                scopes: client.scopes,
                enabled: true,
            })
        }

        async fn list(&self) -> Result<Vec<OauthClient>, AuthError> {
            let rows = sqlx::query(&format!(
                "SELECT {COLS} FROM public.oauth_clients ORDER BY created_at"
            ))
            .fetch_all(&self.pool)
            .await
            .map_err(|e| AuthError::Backend(format!("oauth_clients list: {e}")))?;
            rows.iter().map(row_to_client).collect()
        }

        async fn get(&self, client_id: &str) -> Result<Option<OauthClient>, AuthError> {
            let row = sqlx::query(&format!(
                "SELECT {COLS} FROM public.oauth_clients WHERE client_id = $1"
            ))
            .bind(client_id)
            .fetch_optional(&self.pool)
            .await
            .map_err(|e| AuthError::Backend(format!("oauth_clients get: {e}")))?;
            row.as_ref().map(row_to_client).transpose()
        }

        async fn revoke(&self, client_id: &str) -> Result<bool, AuthError> {
            let res = sqlx::query("DELETE FROM public.oauth_clients WHERE client_id = $1")
                .bind(client_id)
                .execute(&self.pool)
                .await
                .map_err(|e| AuthError::Backend(format!("oauth_clients revoke: {e}")))?;
            Ok(res.rows_affected() > 0)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn new_client(redirect_uris: Vec<&str>) -> NewOauthClient {
        NewOauthClient {
            client_id: "claude-ai".into(),
            client_secret: "s3cret".into(),
            display_name: "Claude.ai".into(),
            redirect_uris: redirect_uris.into_iter().map(str::to_owned).collect(),
            scopes: vec!["rig.read".into()],
        }
    }

    #[test]
    fn a_well_formed_registration_validates() {
        let c = new_client(vec!["https://claude.ai/oauth/callback"]);
        assert!(c.validate().is_ok());
    }

    #[test]
    fn a_wildcard_redirect_uri_is_rejected() {
        let c = new_client(vec!["https://claude.ai/*"]);
        let err = c.validate().unwrap_err();
        assert!(matches!(err, AuthError::InvalidClient(_)));
    }

    #[test]
    fn a_relative_redirect_uri_is_rejected() {
        let c = new_client(vec!["/oauth/callback"]);
        assert!(matches!(c.validate(), Err(AuthError::InvalidClient(_))));
    }

    #[test]
    fn a_fragment_redirect_uri_is_rejected() {
        assert!(matches!(
            validate_redirect_uri("https://claude.ai/cb#frag"),
            Err(AuthError::InvalidClient(_))
        ));
    }

    #[test]
    fn a_whitespace_redirect_uri_is_rejected() {
        assert!(matches!(
            validate_redirect_uri("https://claude.ai/c b"),
            Err(AuthError::InvalidClient(_))
        ));
    }

    #[test]
    fn a_blank_client_id_or_secret_is_rejected() {
        let mut c = new_client(vec!["https://claude.ai/cb"]);
        c.client_id = "  ".into();
        assert!(matches!(c.validate(), Err(AuthError::InvalidClient(_))));
        let mut c = new_client(vec!["https://claude.ai/cb"]);
        c.client_secret = String::new();
        assert!(matches!(c.validate(), Err(AuthError::InvalidClient(_))));
    }

    #[test]
    fn no_redirect_uri_is_rejected() {
        let c = new_client(vec![]);
        assert!(matches!(c.validate(), Err(AuthError::InvalidClient(_))));
    }

    #[test]
    fn redirect_uri_match_is_exact_not_prefix() {
        let client = OauthClient {
            client_id: "claude-ai".into(),
            client_secret_enc: vec![],
            display_name: "Claude.ai".into(),
            redirect_uris: vec!["https://claude.ai/oauth/callback".into()],
            scopes: vec![],
            enabled: true,
        };
        assert!(client.allows_redirect_uri("https://claude.ai/oauth/callback"));
        // A prefix / suffix / different path is NOT allowed — exact match only.
        assert!(!client.allows_redirect_uri("https://claude.ai/oauth/callback/evil"));
        assert!(!client.allows_redirect_uri("https://claude.ai/oauth"));
        assert!(!client.allows_redirect_uri("https://evil.ai/oauth/callback"));
    }

    #[test]
    fn redacted_view_omits_the_secret() {
        let client = OauthClient {
            client_id: "claude-ai".into(),
            client_secret_enc: vec![1, 2, 3, 4],
            display_name: "Claude.ai".into(),
            redirect_uris: vec!["https://claude.ai/cb".into()],
            scopes: vec!["rig.read".into()],
            enabled: true,
        };
        let view = client.redacted();
        assert_eq!(view.client_id, "claude-ai");
        assert_eq!(view.redirect_uris, vec!["https://claude.ai/cb".to_string()]);
        assert_eq!(view.enabled, true);
        // The view struct has no secret field at all — a compile-time guarantee the secret can't
        // be rendered from it.
        assert_eq!(
            view,
            OauthClientView {
                client_id: "claude-ai".into(),
                display_name: "Claude.ai".into(),
                redirect_uris: vec!["https://claude.ai/cb".into()],
                scopes: vec!["rig.read".into()],
                enabled: true,
            }
        );
    }

    // --- PG-gated contract tests: round-trip against a real `public.oauth_clients` table. ---
    //
    // No-ops when `GT_PG_URL` is unset (same gate as the other `pg` adapters). Run with
    // `--test-threads=1` and a `GT_SECRET_KEY` set.
    #[cfg(feature = "pg")]
    mod pg_contract {
        use super::*;
        use sqlx::PgPool;

        async fn pool_or_skip() -> Option<PgPool> {
            let url = std::env::var("GT_PG_URL").ok()?;
            Some(
                PgPool::connect(&url)
                    .await
                    .expect("GT_PG_URL must point at a reachable Postgres"),
            )
        }

        /// Provision `public.oauth_clients` under a transaction-scoped advisory lock (the same guard
        /// the provider-store contract tests use), so two first-runs never race on the create.
        async fn ensure_table(pool: &PgPool) {
            let mut tx = pool.begin().await.expect("begin ddl tx");
            sqlx::query("SELECT pg_advisory_xact_lock(8422)")
                .execute(&mut *tx)
                .await
                .expect("advisory lock");
            sqlx::query(crate::migrations::CREATE_OAUTH_CLIENTS)
                .execute(&mut *tx)
                .await
                .expect("create oauth_clients table");
            tx.commit().await.expect("commit ddl tx");
        }

        fn unique_id(tag: &str) -> String {
            use std::time::{SystemTime, UNIX_EPOCH};
            let n = SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_nanos();
            format!("test-{tag}-{n}")
        }

        #[tokio::test]
        async fn register_read_round_trips_and_seals_the_secret() {
            std::env::set_var(
                crate::ENV_SECRET_KEY,
                "0000000000000000000000000000000000000000000000000000000000000000",
            );
            let Some(pool) = pool_or_skip().await else {
                return;
            };
            ensure_table(&pool).await;
            let repo = PgOauthClientRepo::new(pool.clone());

            let id = unique_id("claude");
            let cleartext = "super-secret-client-secret";
            let stored = repo
                .register(NewOauthClient {
                    client_id: id.clone(),
                    client_secret: cleartext.into(),
                    display_name: "Claude.ai".into(),
                    redirect_uris: vec![
                        "https://claude.ai/oauth/callback".into(),
                        "https://console.anthropic.com/oauth/callback".into(),
                    ],
                    scopes: vec!["rig.read".into(), "rig.write".into()],
                })
                .await
                .unwrap();

            // The stored ciphertext is NOT the cleartext (encrypted at rest).
            assert!(!stored
                .client_secret_enc
                .windows(cleartext.len())
                .any(|w| w == cleartext.as_bytes()));

            // Read it back: arrays and the sealed blob survive the round-trip and unseal.
            let read = repo.get(&id).await.unwrap().expect("client present");
            assert_eq!(read.client_id, id);
            assert_eq!(read.redirect_uris.len(), 2);
            assert!(read.allows_redirect_uri("https://claude.ai/oauth/callback"));
            assert!(!read.allows_redirect_uri("https://claude.ai/oauth/callback/evil"));
            assert_eq!(read.scopes, vec!["rig.read".to_string(), "rig.write".into()]);
            let secret = crate::unseal(&read.client_secret_enc).unwrap();
            assert_eq!(secret, cleartext.as_bytes());

            // The raw BYTEA in PG itself is not the cleartext.
            let raw: Vec<u8> = sqlx::query_scalar(
                "SELECT client_secret_enc FROM public.oauth_clients WHERE client_id = $1",
            )
            .bind(&id)
            .fetch_one(&pool)
            .await
            .unwrap();
            assert!(!raw.windows(cleartext.len()).any(|w| w == cleartext.as_bytes()));

            // list() includes it; revoke() removes it (idempotent second call is false).
            assert!(repo.list().await.unwrap().iter().any(|c| c.client_id == id));
            assert!(repo.revoke(&id).await.unwrap());
            assert!(repo.get(&id).await.unwrap().is_none());
            assert!(!repo.revoke(&id).await.unwrap());
        }

        #[tokio::test]
        async fn a_duplicate_client_id_is_rejected() {
            std::env::set_var(
                crate::ENV_SECRET_KEY,
                "0000000000000000000000000000000000000000000000000000000000000000",
            );
            let Some(pool) = pool_or_skip().await else {
                return;
            };
            ensure_table(&pool).await;
            let repo = PgOauthClientRepo::new(pool.clone());

            let id = unique_id("dup");
            let make = || NewOauthClient {
                client_id: id.clone(),
                client_secret: "s".into(),
                display_name: "Dup".into(),
                redirect_uris: vec!["https://dup.test/cb".into()],
                scopes: vec![],
            };
            repo.register(make()).await.unwrap();
            assert!(matches!(
                repo.register(make()).await,
                Err(AuthError::InvalidClient(_))
            ));
            repo.revoke(&id).await.unwrap();
        }

        #[tokio::test]
        async fn a_wildcard_redirect_is_rejected_before_any_write() {
            std::env::set_var(
                crate::ENV_SECRET_KEY,
                "0000000000000000000000000000000000000000000000000000000000000000",
            );
            let Some(pool) = pool_or_skip().await else {
                return;
            };
            ensure_table(&pool).await;
            let repo = PgOauthClientRepo::new(pool.clone());

            let id = unique_id("wild");
            let err = repo
                .register(NewOauthClient {
                    client_id: id.clone(),
                    client_secret: "s".into(),
                    display_name: "Wild".into(),
                    redirect_uris: vec!["https://wild.test/*".into()],
                    scopes: vec![],
                })
                .await;
            assert!(matches!(err, Err(AuthError::InvalidClient(_))));
            // Nothing was written — the validation runs before the INSERT.
            assert!(repo.get(&id).await.unwrap().is_none());
        }
    }
}
