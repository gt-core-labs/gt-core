//! Authentication identity port for gt-core.
//!
//! ## Why this crate exists
//!
//! `gt-workspace::WorkspaceContext` (the tenant-boundary extractor, `hq-mt-core.8`)
//! resolves a request's [workspace] from the `X-GT-Workspace` header today, but its module
//! docs spell out a deferred second source: *"the `X-GT-Workspace` header and a JWT claim.
//! Only the header path is implemented — the JWT/auth layer is not yet ported to gt-core (it
//! lives upstream)."* This crate ports that missing layer up as a **Port + InMemory**
//! adapter, the first concrete carve-out of the `hq-mod-refactor.13` bins decomposition
//! (upstream `bins/gt-web/{auth,jwt,login,scope}.rs`).
//!
//! ## Shape (hexagonal — docs/03)
//!
//! - [`JwtClaims`] — the verified token payload. The `workspace` claim is **required**
//!   (`hq-mt-auth.1`); [`JwtClaims::validate`] enforces it, with a grace toggle
//!   (`GT_JWT_WS_OPTIONAL`) for a rolling rollout.
//! - [`Authenticator`] — the inverted port: turn an opaque bearer token into [`JwtClaims`]
//!   (i.e. verify the signature and decode the payload). Synchronous: verification is CPU
//!   work, not I/O, so no `async` ceremony.
//! - [`InMemoryAuthenticator`] — the dependency-free test double (a `token -> claims` map,
//!   standing in for a real signature check), so the domain slice tests run without crypto.
//! - [`JwtAuthenticator`] — the production RS256 verifier (jsonwebtoken), behind the
//!   off-by-default `jsonwebtoken` feature.
//! - [`AuthError`] — the rejection vocabulary.
//!
//! ## Refresh tokens (the long-lived counterpart, `hq-auth-mint.3`)
//!
//! Access tokens are short-lived JWTs; [`RefreshToken`] is the long-lived **opaque** credential
//! a client trades in for a fresh access token without re-login. [`RefreshStore`] is the port
//! for issuing, **rotating** (single-use: each rotate mints a successor and spends the old
//! token), and revoking them, with **reuse detection** — presenting an already-rotated token is
//! the theft signal and revokes the whole [family](RefreshRecord::family). The dependency-free
//! [`InMemoryRefreshStore`] is the reference adapter (a Postgres-backed one is a follow-up). See
//! the `refresh` module docs for the security model and the `std`-only entropy rationale.
//!
//! ## Login providers (the step before token verification)
//!
//! [`Authenticator`] verifies an already-minted token. One step earlier, a principal logs in
//! by presenting [`Credentials`] to an [`IdentityProvider`], which authenticates them into a
//! [`VerifiedIdentity`] — the facts the token-minting tier folds into a fresh [`JwtClaims`]
//! ([`VerifiedIdentity::into_claims`]). [`ProviderKind`] enumerates the methods; **email +
//! password is the default** ([`ProviderKind::default`]), with `OAuth`/`Oidc` shape-reserved
//! (their adapters return [`AuthError::UnsupportedProvider`] until implemented). The real
//! email+password adapter ([`password::PasswordProvider`], argon2 PHC hashing) lives behind
//! the off-by-default `password-hash` feature; tests use the in-memory double. The
//! store-backed counterpart, [`PgUsers`] (`hq-auth-mint.2`, off-by-default `pg` feature),
//! looks the user up in the per-workspace `users` table and reuses that same argon2 check —
//! its async I/O makes it an inherent `authenticate` rather than the sync trait impl, and it
//! stamps the server-injected workspace onto the identity (docs/04 §15), never a payload one.
//!
//! The real signature-verifying adapter ([`JwtAuthenticator`], RS256 via jsonwebtoken) folds
//! into this same crate behind the off-by-default `jsonwebtoken` feature (`hq-auth-verify.1`) —
//! the way gt-rig/gt-quota gate their `pg`/`axum` adapters — never a sibling adapter crate
//! (Rule 4). It verifies the signature only; the `exp`/`workspace` gates stay
//! [`JwtClaims::validate`]. It holds one or more public keys indexed by the token's `kid` for
//! rotation (`hq-auth-verify.2`), loaded from the environment via
//! [`JwtAuthenticator::from_env`].
//!
//! Its production counterpart — the final `[sign]` arrow of the pipeline above — is
//! [`JwtMinter`] (RS256 via jsonwebtoken, `hq-auth-mint.1`), folded into this same crate behind
//! the *same* `jsonwebtoken` feature (never a sibling crate, Rule 4). Asymmetric on purpose:
//! the minter holds the **private** key and signs a [`JwtClaims`] into a bearer token, stamping
//! the signing `kid` into the header so [`JwtAuthenticator`]'s keyset can select the matching
//! public key; the verifier holds only the public side. It loads its key from the environment
//! via [`JwtMinter::from_env`].
//!
//! This crate intentionally does **not** depend on `gt-workspace`: platform → platform deps
//! are forbidden (Rule 4). The `workspace` claim is a plain `String`; the consumer (the
//! `WorkspaceContext` extractor, wired one tier up at the composition root) parses it into a
//! `WorkspaceId` and reconciles it against the header.
//!
//! [workspace]: JwtClaims::workspace

mod error;
mod pat;
mod provider;
mod refresh;
mod verify;

#[cfg(feature = "password-hash")]
pub mod password;

#[cfg(feature = "pg")]
mod pg;

#[cfg(feature = "pg")]
mod refresh_pg;

#[cfg(feature = "pg")]
mod pat_pg;

#[cfg(feature = "jsonwebtoken")]
mod jwt;

#[cfg(feature = "jsonwebtoken")]
mod mint;

#[cfg(feature = "axum")]
mod http;

#[cfg(feature = "oauth")]
mod oauth;

#[cfg(feature = "oauth")]
mod crypto;

#[cfg(feature = "oauth")]
mod provider_repo;

#[cfg(feature = "oauth")]
mod authz_state;

#[cfg(feature = "oauth")]
mod cli_code;

pub use error::AuthError;
pub use provider::{Credentials, IdentityProvider, ProviderKind, VerifiedIdentity};
pub use refresh::{
    InMemoryRefreshStore, RefreshError, RefreshId, RefreshRecord, RefreshStatus, RefreshStore,
    RefreshToken,
};
pub use verify::{Authenticator, InMemoryAuthenticator};

/// Personal Access Token domain types (`hq-security-pat.1`): the opaque [`PatToken`] (`gtpat_` +
/// 256 bits), its non-secret [`PatId`] handle, the [`PatRecord`] bookkeeping, the [`PatStatus`]
/// lifecycle, the [`PatError`] verify-rejection vocabulary, the [`PAT_PREFIX`]/[`has_pat_prefix`]
/// discriminator the middleware routes on, and the privilege-clamp [`clamp_scopes`]. The
/// Postgres-backed store ([`PgPatStore`]) is additionally gated by `pg`.
pub use pat::{
    clamp_scopes, has_pat_prefix, PatError, PatId, PatRecord, PatStatus, PatToken, PAT_PREFIX,
};

#[cfg(feature = "pg")]
pub use pg::{Membership, PgUsers};

#[cfg(feature = "pg")]
pub use refresh_pg::PgRefreshStore;

/// The Postgres-backed Personal Access Token store (`hq-security-pat.1`): mint (with scope clamp) /
/// list / revoke / verify against the per-workspace `personal_access_tokens` table.
#[cfg(feature = "pg")]
pub use pat_pg::PgPatStore;

/// The embedded auth migration SQL — the per-workspace `users` (login store) and
/// `refresh_tokens` tables, defined once in the `ws_default` template schema. The
/// composition root applies these against the workspace-scoped Postgres pool at boot
/// (the crate is not a `GtModule`, so the kernel migration runner does not carry them).
/// Each file is `CREATE ... IF NOT EXISTS`, so re-applying is idempotent. Exposed under
/// `pg` because only a Postgres-backed deploy needs them.
#[cfg(feature = "pg")]
pub mod migrations {
    /// `0001` — the email+password login store (`ws_default.users`).
    pub const CREATE_USERS: &str = include_str!("../migrations/auth/0001__create_users.sql");
    /// `0002` — durable refresh-token records (`ws_default.refresh_tokens`).
    pub const CREATE_REFRESH_TOKENS: &str =
        include_str!("../migrations/auth/0002__create_refresh_tokens.sql");
    /// `0003` — the role catalog backing role→scopes expansion (`ws_default.roles`, hq-rbac.3).
    pub const CREATE_ROLES: &str = include_str!("../migrations/auth/0003__create_roles.sql");
    /// `0004` — the `users.roles` assignment column (hq-rbac.3).
    pub const ADD_USER_ROLES: &str = include_str!("../migrations/auth/0004__add_user_roles.sql");
    /// `0005` — global identity (`public.users`) + N:N workspace membership
    /// (`public.user_workspaces`), the foundation of cross-workspace login (hq-identity.1).
    pub const CREATE_GLOBAL_IDENTITY: &str =
        include_str!("../migrations/auth/0005__global_identity.sql");
    /// `0006` — the GLOBAL DB-backed OAuth/OIDC provider store (`public.oauth_providers`,
    /// hq-idp-db.1); the client secret column holds the AES-GCM-sealed blob, never cleartext.
    pub const CREATE_OAUTH_PROVIDERS: &str =
        include_str!("../migrations/auth/0006__create_oauth_providers.sql");
    /// `0007` — the ephemeral OAuth authorize state + PKCE verifier store
    /// (`public.oauth_authz_state`, hq-idp-db.3): one short-lived, one-shot row per in-flight
    /// `/authorize`→`/callback` handshake, so the callback can validate the anti-CSRF `state` and
    /// recover the PKCE `code_verifier`. Durable so an in-flight login survives a redeploy.
    pub const CREATE_OAUTH_AUTHZ_STATE: &str =
        include_str!("../migrations/auth/0007__create_oauth_authz_state.sql");
    /// `0008` — the per-workspace Personal Access Token store (`ws_default.personal_access_tokens`,
    /// hq-security-pat.1): the long-lived opaque `gtpat_` bearer credential, stored only as its
    /// SHA-256 hash. Cloned into every tenant schema by `gt_create_workspace_schema`.
    pub const CREATE_PERSONAL_ACCESS_TOKENS: &str =
        include_str!("../migrations/auth/0008__create_personal_access_tokens.sql");
    /// `0009` — the nullable `cli_redirect` column on `public.oauth_authz_state`
    /// (hq-gt-login-oauth.1): the loopback URL a `gt login` browser handshake carries, so the
    /// callback can 302 a one-shot code back to the CLI instead of the web FE. Unset for the
    /// ordinary web login (its absence preserves the existing behaviour).
    pub const ADD_CLI_REDIRECT: &str =
        include_str!("../migrations/auth/0009__add_cli_redirect.sql");
    /// `0010` — the one-shot CLI hand-off code store (`public.oauth_cli_code`,
    /// hq-gt-login-oauth.2): the short-lived row the callback parks a `gt login` token pair on under
    /// an opaque `code`, redeemed once at `POST /auth/cli/exchange`.
    pub const CREATE_OAUTH_CLI_CODE: &str =
        include_str!("../migrations/auth/0010__create_oauth_cli_code.sql");
}

#[cfg(feature = "jsonwebtoken")]
pub use jwt::{Jwk, JwkSet, JwtAuthenticator};

#[cfg(feature = "jsonwebtoken")]
pub use mint::JwtMinter;

#[cfg(feature = "axum")]
pub use http::{
    auth_openapi, auth_router, AddMemberRequest, AssignRolesRequest, AsyncRefreshStore, AuthState,
    Clock, CreateRoleRequest, CreateTokenRequest, CreateUserRequest, CreatedTokenResponse,
    LoginProvider, LoginRequest, MeResponse, MembershipAdmin, MembershipDirectory, PatAdmin,
    RefreshRequest, RoleStore, RoleSummary, SameSite, SwitchRequest, TokenResponse, TokenSummary,
    UserStore, UserSummary, WorkspaceMembership,
};

/// The global-login port adapter (hq-identity.2) needs both the HTTP port (`axum`) and the
/// Postgres identity store (`pg`).
#[cfg(all(feature = "axum", feature = "pg"))]
pub use http::GlobalLogin;

/// The OAuth/OIDC login adapter (hq-platform-hardening.3): the authorization-code handshake
/// against a configured IdP, mapping the upstream identity onto the same [`VerifiedIdentity`] the
/// password path yields. Behind the off-by-default `oauth` feature.
#[cfg(feature = "oauth")]
pub use oauth::{
    DbOauthLogin, OidcConfig, OidcProvider, ENV_CLIENT_ID, ENV_CLIENT_SECRET, ENV_ISSUER,
    ENV_REDIRECT_URI, ENV_SCOPES, ENV_TOKEN_ENDPOINT, ENV_USERINFO_ENDPOINT, ENV_WORKSPACE,
};

/// The DB-backed OAuth/OIDC provider store (hq-idp-db.1): the `ProviderRepo` CRUD port over the
/// GLOBAL `public.oauth_providers` table, the `ProviderKind`/preset catalog, and the
/// `NewProvider`/`ProviderRecord` row shapes. The Postgres adapter `PgProviderRepo` is additionally
/// gated by `pg`. The AES-GCM master-key env var (`GT_SECRET_KEY`) is re-exported for the
/// composition root to document the contract.
#[cfg(feature = "oauth")]
pub use provider_repo::{
    preset_for, presets, NewProvider, PatchProvider, ProviderKind as OauthProviderKind,
    ProviderPreset, ProviderRecord, ProviderRepo,
};

/// The AES-GCM seal/unseal env contract for the provider-secret store (hq-idp-db.1).
#[cfg(feature = "oauth")]
pub use crypto::ENV_SECRET_KEY;

/// The system-admin OAuth/OIDC provider-administration surface (hq-idp-db.4): the abstract
/// [`ProviderStore`] port the `/auth/providers` CRUD handlers depend on, the request DTOs, and the
/// secret-omitting [`ProviderView`] projection. Behind the `oauth` feature (with the HTTP router it
/// rides). The composition root supplies the production adapter (`PgProviderRepo`) via
/// [`AuthState::providers`].
#[cfg(feature = "oauth")]
pub use http::{CreateProviderRequest, PatchProviderRequest, ProviderStore, ProviderView};

/// The PUBLIC OAuth discovery + redirect-login surface (hq-idp-db.3): the `AuthzStateStore` /
/// `OauthAuthzFlow` ports the `/authorize`→`/callback` handlers depend on, and the secret-free
/// [`PublicProvider`] projection the FE login page renders buttons from. Behind the `oauth` feature.
/// The composition root supplies the production adapters (`PgAuthzStateRepo` / `DbOauthLogin`) via
/// [`AuthState::authz_state`] / [`AuthState::authz_flow`].
#[cfg(feature = "oauth")]
pub use http::{AuthzStateStore, CliCodeStore, OauthAuthzFlow, PublicProvider};

#[cfg(all(feature = "oauth", feature = "pg"))]
pub use provider_repo::PgProviderRepo;

/// The ephemeral OAuth authorize-state + PKCE store (hq-idp-db.3): the `AuthzStateRepo` CRUD port
/// the public `/authorize`→`/callback` flow depends on, the `PendingAuthz`/`NewAuthz` row shapes,
/// and the PKCE helper ([`new_pkce`]) that mints a verifier + its S256 challenge. The Postgres
/// adapter `PgAuthzStateRepo` is additionally gated by `pg`.
#[cfg(feature = "oauth")]
pub use authz_state::{new_pkce, AuthzStateRepo, NewAuthz, PendingAuthz, Pkce};

/// The one-shot CLI hand-off code store (hq-gt-login-oauth.2): the `CliCodeRepo` port the
/// `/auth/callback`→`/auth/cli/exchange` `gt login` flow parks the minted token pair on, plus the
/// `NewCliCode`/`PendingCliCode` row shapes. The Postgres adapter `PgCliCodeRepo` is additionally
/// gated by `pg`.
#[cfg(feature = "oauth")]
pub use cli_code::{CliCodeRepo, NewCliCode, PendingCliCode};

#[cfg(all(feature = "oauth", feature = "pg"))]
pub use authz_state::PgAuthzStateRepo;

#[cfg(all(feature = "oauth", feature = "pg"))]
pub use cli_code::PgCliCodeRepo;

#[cfg(test)]
pub use provider::InMemoryIdentityProvider;

use serde::{Deserialize, Serialize};

/// The verified payload of a bearer token.
///
/// Field names match the wire (JWT) claim names so a real decoder can `serde`-deserialize
/// straight into this struct. Unknown claims are ignored on the way in.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct JwtClaims {
    /// Subject — the authenticated principal (user / agent id).
    pub sub: String,
    /// Tenant the token is scoped to. **Required** (`hq-mt-auth.1`): a token without it is
    /// rejected by [`JwtClaims::validate`] once the grace window closes. Kept a `String`
    /// here (not a `WorkspaceId`) so this crate stays free of a `gt-workspace` dep; the
    /// consumer validates it into a `WorkspaceId`.
    #[serde(default)]
    pub workspace: String,
    /// Authorization scopes granted to the principal (checked against `gt-rbac` scopes by
    /// the route guards, one tier up). Defaults to empty when the claim is absent.
    #[serde(default)]
    pub scopes: Vec<String>,
    /// Expiry, seconds since the Unix epoch (JWT `exp`).
    pub exp: u64,
    /// Not-before, seconds since the Unix epoch (JWT `nbf`). When set, the token is rejected
    /// before this instant. Absent (`None`) ⇒ valid from issue. Omitted from the wire when
    /// `None`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub nbf: Option<u64>,
    /// Issued-at, seconds since the Unix epoch (JWT `iat`).
    #[serde(default)]
    pub iat: u64,
}

/// The `GT_JWT_WS_OPTIONAL` env var: when truthy, a token may omit the `workspace` claim
/// during the rolling rollout (`hq-mt-auth.1`).
pub const ENV_WS_OPTIONAL: &str = "GT_JWT_WS_OPTIONAL";

impl JwtClaims {
    /// Validate the *semantic* invariants of an already-signature-verified token against the
    /// current wall clock `now_secs`, with **zero** clock-skew tolerance. See
    /// [`validate_with_leeway`](Self::validate_with_leeway) for the full contract.
    ///
    /// Signature verification is the [`Authenticator`]'s job and has already happened by the
    /// time you hold a `JwtClaims`; this is the cheap, crypto-free second gate.
    pub fn validate(&self, now_secs: u64, workspace_optional: bool) -> Result<(), AuthError> {
        self.validate_with_leeway(now_secs, 0, workspace_optional)
    }

    /// Validate the token's time bounds and workspace claim against `now_secs`, tolerating up to
    /// `leeway_secs` of clock skew between the issuer and this verifier.
    ///
    /// - **Expired** when `now_secs >= exp + leeway` ([`AuthError::Expired`]).
    /// - **Not yet valid** when a `nbf` is set and `now_secs + leeway < nbf`
    ///   ([`AuthError::NotYetValid`]); likewise when `iat` is stamped and lies more than
    ///   `leeway` in the future (a token minted "ahead" of this clock — skew or a forged
    ///   timestamp).
    /// - A blank `workspace` claim is [`AuthError::MissingWorkspace`] **unless**
    ///   `workspace_optional` is set — the [`GT_JWT_WS_OPTIONAL`](ENV_WS_OPTIONAL) grace flag
    ///   for the rolling rollout (`hq-mt-auth.1`). Drop the flag (and this branch) once every
    ///   issuer stamps the claim.
    ///
    /// The leeway widens the *exp* window and narrows the *nbf*/*iat* one symmetrically, the
    /// standard JWT skew allowance (RFC 7519 §4.1.4/§4.1.5).
    pub fn validate_with_leeway(
        &self,
        now_secs: u64,
        leeway_secs: u64,
        workspace_optional: bool,
    ) -> Result<(), AuthError> {
        if now_secs >= self.exp.saturating_add(leeway_secs) {
            return Err(AuthError::Expired);
        }
        if let Some(nbf) = self.nbf {
            if now_secs.saturating_add(leeway_secs) < nbf {
                return Err(AuthError::NotYetValid);
            }
        }
        // iat 0 ⇒ absent (the wire default); a non-zero iat in the future beyond leeway is a
        // token claiming to be minted later than now — reject it like an unmet nbf.
        if self.iat != 0 && self.iat > now_secs.saturating_add(leeway_secs) {
            return Err(AuthError::NotYetValid);
        }
        if self.workspace.trim().is_empty() && !workspace_optional {
            return Err(AuthError::MissingWorkspace);
        }
        Ok(())
    }

    /// Read the [`GT_JWT_WS_OPTIONAL`](ENV_WS_OPTIONAL) grace flag from the environment.
    /// Truthy values are `1`, `true`, `yes`, `on` (case-insensitive); anything else (or unset)
    /// is `false`, so the workspace claim is required by default.
    pub fn workspace_optional_from_env() -> bool {
        std::env::var(ENV_WS_OPTIONAL)
            .map(|v| {
                matches!(
                    v.trim().to_ascii_lowercase().as_str(),
                    "1" | "true" | "yes" | "on"
                )
            })
            .unwrap_or(false)
    }

    /// True when `scope` is among the granted [`scopes`](Self::scopes).
    pub fn has_scope(&self, scope: &str) -> bool {
        self.scopes.iter().any(|s| s == scope)
    }
}
