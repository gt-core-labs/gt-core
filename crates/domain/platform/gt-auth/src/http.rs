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
#[cfg(any(feature = "pg", feature = "oauth"))]
use axum::extract::Path;
use axum::extract::{Extension, State};
use axum::http::{header, HeaderMap, HeaderValue, StatusCode};
use axum::response::{IntoResponse, Response};
use axum::routing::{get, post};
use axum::{Json, Router};
use serde::{Deserialize, Serialize};

use crate::{
    AuthError, Credentials, JwkSet, JwtClaims, JwtMinter, PatError, PatRecord, PatToken,
    ProviderKind, RefreshError, RefreshRecord, RefreshStore, RefreshToken, VerifiedIdentity,
};
#[cfg(feature = "oauth")]
use crate::{
    NewAuthz, NewProvider, OauthProviderKind, PatchProvider, PendingAuthz, ProviderRecord,
};

/// The async refresh-store port behind [`AuthState::refresh`] — the I/O-capable boundary the HTTP
/// handlers actually call (`hq-platform-hardening.1`).
///
/// The core [`RefreshStore`](crate::RefreshStore) is **synchronous** (the in-memory adapter does
/// only map work). A durable adapter is I/O, so — like [`LoginProvider`] is to the sync
/// [`IdentityProvider`](crate::IdentityProvider) — the endpoints depend on this `async` sibling.
/// Both adapters back it without a `block_on`:
///
/// - any sync [`RefreshStore`] (the dependency-free [`InMemoryRefreshStore`](crate::InMemoryRefreshStore)
///   fallback) via the blanket impl below — its map work just returns immediately, wrapped `Ok`;
/// - [`PgRefreshStore`](crate::PgRefreshStore) (the durable, restart-surviving store) via its
///   inherent `async` methods directly, with no runtime parked on a DB round-trip.
///
/// Only the three methods the endpoints invoke are exposed (`issue` / `rotate` / `revoke_by_token`);
/// every verdict is a [`RefreshError`], so the sync store's infallible calls surface as `Ok`.
#[async_trait]
pub trait AsyncRefreshStore: Send + Sync {
    /// Mint a new token in a fresh family — the async counterpart of
    /// [`RefreshStore::issue`](crate::RefreshStore::issue).
    async fn issue(
        &self,
        sub: &str,
        workspace: &str,
        scopes: &[String],
        issued_at: u64,
        exp: u64,
    ) -> Result<(RefreshToken, RefreshRecord), RefreshError>;

    /// Exchange an active token for a successor — the async counterpart of
    /// [`RefreshStore::rotate`](crate::RefreshStore::rotate).
    async fn rotate(
        &self,
        token: &RefreshToken,
        now: u64,
    ) -> Result<(RefreshToken, RefreshRecord), RefreshError>;

    /// Revoke the presented token's family (logout) — the async counterpart of
    /// [`RefreshStore::revoke_by_token`](crate::RefreshStore::revoke_by_token).
    async fn revoke_by_token(&self, token: &RefreshToken) -> Result<(), RefreshError>;
}

/// Every synchronous [`RefreshStore`] is an [`AsyncRefreshStore`]: its calls are in-memory map
/// work, so the async wrapper just runs them inline and wraps the infallible ones in `Ok`. This is
/// what keeps the dependency-free [`InMemoryRefreshStore`](crate::InMemoryRefreshStore) usable as
/// the no-database fallback without changing the sync core.
#[async_trait]
impl<S: RefreshStore + Send + Sync> AsyncRefreshStore for S {
    async fn issue(
        &self,
        sub: &str,
        workspace: &str,
        scopes: &[String],
        issued_at: u64,
        exp: u64,
    ) -> Result<(RefreshToken, RefreshRecord), RefreshError> {
        Ok(RefreshStore::issue(
            self, sub, workspace, scopes, issued_at, exp,
        ))
    }

    async fn rotate(
        &self,
        token: &RefreshToken,
        now: u64,
    ) -> Result<(RefreshToken, RefreshRecord), RefreshError> {
        RefreshStore::rotate(self, token, now)
    }

    async fn revoke_by_token(&self, token: &RefreshToken) -> Result<(), RefreshError> {
        RefreshStore::revoke_by_token(self, token);
        Ok(())
    }
}

/// The durable Postgres store is the production [`AsyncRefreshStore`]: it already exposes inherent
/// `async` issue / rotate / revoke methods over the same [`RefreshError`] contract, so this just
/// adapts them to the port. Available with the `pg` adapter; wiring it in place of the in-memory
/// fallback is what makes a session survive a `gt-mcp-server` redeploy (`hq-platform-hardening.1`).
#[cfg(feature = "pg")]
#[async_trait]
impl AsyncRefreshStore for crate::PgRefreshStore {
    async fn issue(
        &self,
        sub: &str,
        workspace: &str,
        scopes: &[String],
        issued_at: u64,
        exp: u64,
    ) -> Result<(RefreshToken, RefreshRecord), RefreshError> {
        crate::PgRefreshStore::issue(self, sub, workspace, scopes, issued_at, exp).await
    }

    async fn rotate(
        &self,
        token: &RefreshToken,
        now: u64,
    ) -> Result<(RefreshToken, RefreshRecord), RefreshError> {
        crate::PgRefreshStore::rotate(self, token, now).await
    }

    async fn revoke_by_token(&self, token: &RefreshToken) -> Result<(), RefreshError> {
        crate::PgRefreshStore::revoke_by_token(self, token).await
    }
}

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

/// The GLOBAL login adapter (hq-identity.2): the production [`LoginProvider`] once identity is
/// shared across workspaces. It wraps a [`PgUsers`](crate::PgUsers) and delegates to
/// [`authenticate_global`](crate::PgUsers::authenticate_global) with NO preferred workspace, so a
/// plain `POST /auth/login` resolves the user's DEFAULT membership as the active tenant (the
/// per-workspace [`PgUsers`] login above stays in place for the transition). The composition root
/// swaps this in for the per-workspace login once the global admin seed exists (hq-identity.4),
/// keeping login working across the cutover; switching to another membership is hq-identity.3.
#[cfg(feature = "pg")]
pub struct GlobalLogin(pub std::sync::Arc<crate::PgUsers>);

#[cfg(feature = "pg")]
#[async_trait]
impl LoginProvider for GlobalLogin {
    async fn login(&self, creds: &Credentials) -> Result<VerifiedIdentity, AuthError> {
        self.0.authenticate_global(creds, None).await
    }
}

/// The workspace-membership port behind `GET /auth/workspaces` and `POST /auth/switch`
/// (hq-identity.3): list the workspaces an already-authenticated user can reach, and re-target a
/// session to one of them. Keyed by the verified token's `sub` — no password re-check, the bearer
/// already proved identity. Separate from [`LoginProvider`] (which authenticates from scratch) so
/// a deploy can mount login without the cross-workspace surface. The production adapter is
/// [`PgUsers`](crate::PgUsers).
#[async_trait]
pub trait MembershipDirectory: Send + Sync {
    /// The user's workspace memberships (slug + role), for the workspace picker. Empty ⇒ a global
    /// identity with no tenant yet (it could log in nowhere — see [`PgUsers::authenticate_global`]).
    async fn list(&self, sub: &str) -> Result<Vec<WorkspaceMembership>, AuthError>;

    /// The identity to re-mint when switching `sub` into `workspace`: that membership's role,
    /// expanded to scopes in that tenant. `None` ⇒ the user is NOT a member, which the switch
    /// endpoint maps to `403` — never a token for an unheld tenant.
    async fn resolve(
        &self,
        sub: &str,
        workspace: &str,
    ) -> Result<Option<VerifiedIdentity>, AuthError>;

    /// Every workspace in the deployment, for a SYSTEM ADMIN's picker: a `*`-scoped caller may
    /// switch into any tenant, not just their own memberships, so `GET /auth/workspaces` widens to
    /// this list for them and `/auth/switch` accepts any slug it contains. The role is reported as
    /// `admin` (the caller carries `*` regardless of any per-tenant role). Default `Ok(vec![])` so
    /// adapters that predate this — and the non-admin path — need no change.
    async fn list_all(&self) -> Result<Vec<WorkspaceMembership>, AuthError> {
        Ok(Vec::new())
    }
}

/// One membership row as returned by `GET /auth/workspaces`: the workspace slug and the role the
/// user holds there (the gt-web shell consumes this to render + drive the workspace selector).
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[cfg_attr(feature = "axum", derive(utoipa::ToSchema))]
pub struct WorkspaceMembership {
    /// The workspace slug the user can switch into.
    pub workspace: String,
    /// The role the user holds in that workspace.
    pub role: String,
}

/// The production [`MembershipDirectory`]: list + resolve over the global identity tables.
#[cfg(feature = "pg")]
#[async_trait]
impl MembershipDirectory for crate::PgUsers {
    async fn list(&self, sub: &str) -> Result<Vec<WorkspaceMembership>, AuthError> {
        Ok(self
            .memberships(sub)
            .await?
            .into_iter()
            .map(|m| WorkspaceMembership {
                workspace: m.workspace,
                role: m.role,
            })
            .collect())
    }

    async fn resolve(
        &self,
        sub: &str,
        workspace: &str,
    ) -> Result<Option<VerifiedIdentity>, AuthError> {
        self.resolve_membership(sub, workspace).await
    }

    async fn list_all(&self) -> Result<Vec<WorkspaceMembership>, AuthError> {
        Ok(self
            .all_workspaces()
            .await?
            .into_iter()
            .map(|workspace| WorkspaceMembership {
                workspace,
                role: "admin".to_string(),
            })
            .collect())
    }
}

/// The workspace-membership ADMINISTRATION port behind `POST`/`DELETE
/// `/auth/workspaces/{slug}/members` (hq-platform-hardening.2): a workspace admin attaches or
/// detaches ANOTHER user. Distinct from [`MembershipDirectory`] (which only lists/switches the
/// CALLER's own memberships): this one writes the N:N bridge for a third party, so it is gated on
/// the caller being an admin OF THE TARGET workspace, never the system admin. The production
/// adapter is [`PgUsers`](crate::PgUsers).
#[async_trait]
pub trait MembershipAdmin: Send + Sync {
    /// Grant `email` membership of `workspace` holding `role`, mirroring the user into the
    /// tenant's per-ws login store. `Ok(true)` ⇒ written; `Ok(false)` ⇒ no global user has that
    /// email (the handler maps it to `404`). Idempotent — re-adding updates the role in place.
    async fn add_member(
        &self,
        email: &str,
        workspace: &str,
        role: &str,
        now: u64,
    ) -> Result<bool, AuthError>;

    /// Revoke `email`'s membership of `workspace` (and the per-ws mirror). `Ok(true)` ⇒ a
    /// membership existed and was removed; `Ok(false)` ⇒ no such user/membership (`404`).
    async fn remove_member(&self, email: &str, workspace: &str) -> Result<bool, AuthError>;
}

/// The production [`MembershipAdmin`]: add/remove over the global identity + per-ws mirror tables.
#[cfg(feature = "pg")]
#[async_trait]
impl MembershipAdmin for crate::PgUsers {
    async fn add_member(
        &self,
        email: &str,
        workspace: &str,
        role: &str,
        now: u64,
    ) -> Result<bool, AuthError> {
        self.add_workspace_member(email, workspace, role, now).await
    }

    async fn remove_member(&self, email: &str, workspace: &str) -> Result<bool, AuthError> {
        self.remove_workspace_member(email, workspace).await
    }
}

/// The async user-administration port behind `POST`/`GET /auth/users` (`hq-web-extras.5`): the
/// onboarding surface that creates and lists users without hand-written SQL. Separate from
/// [`LoginProvider`] (which only authenticates) so a deploy can mount login without exposing
/// administration. The production adapter is [`PgUsers`](crate::PgUsers).
#[async_trait]
pub trait UserStore: Send + Sync {
    /// Insert a user with an already-hashed password. A duplicate email is
    /// [`AuthError::Backend`] (the unique index rejects it); a fault is [`AuthError::Backend`].
    async fn create_user(
        &self,
        id: &str,
        email: &str,
        password_hash: &str,
        scopes: &[String],
        now: u64,
    ) -> Result<(), AuthError>;

    /// List every user (no password material), oldest first.
    async fn list_users(&self) -> Result<Vec<UserSummary>, AuthError>;
}

/// The async role-administration port behind `/auth/roles` and `/auth/users/{email}/roles`
/// (hq-rbac.4): create/list/delete a role (a named scope bundle) and assign roles to a user.
/// Separate from [`UserStore`] so a deploy can expose user onboarding without role management.
/// The production adapter is [`PgUsers`](crate::PgUsers), over the same workspace pool.
#[async_trait]
pub trait RoleStore: Send + Sync {
    /// Upsert a role: create it, or replace an existing role's scope bundle. The caller has
    /// already validated `scopes` against the closed vocabulary. A fault is [`AuthError::Backend`].
    async fn upsert_role(&self, name: &str, scopes: &[String], now: u64) -> Result<(), AuthError>;

    /// List every role and its scope bundle, oldest first.
    async fn list_roles(&self) -> Result<Vec<RoleSummary>, AuthError>;

    /// Delete a role by name. `Ok(false)` when no such role existed (idempotent delete).
    async fn delete_role(&self, name: &str) -> Result<bool, AuthError>;

    /// Replace a user's assigned role set, keyed by login email. `Ok(false)` when no user has
    /// that email — the caller maps it to `404` rather than silently succeeding.
    async fn assign_user_roles(
        &self,
        email: &str,
        roles: &[String],
        now: u64,
    ) -> Result<bool, AuthError>;
}

/// The async Personal Access Token self-service port behind `/auth/tokens` (hq-security-pat.2): a
/// user mints, lists, and revokes their OWN tokens. Every method is keyed by the caller's verified
/// `sub`, so the surface is **self-only** by construction — a caller cannot see or revoke another
/// user's tokens even by guessing an id. The production adapter is [`PgPatStore`](crate::PgPatStore).
#[async_trait]
pub trait PatAdmin: Send + Sync {
    /// Mint a PAT for `sub`/`workspace` labelled `name`, granting the intersection of `requested`
    /// and the caller's own `granted` scopes (so it can never escalate), valid until `expires_at`
    /// (`None` ⇒ never), created at `now`. Returns the opaque secret (shown to the user **once**)
    /// plus its [`PatRecord`].
    #[allow(clippy::too_many_arguments)]
    async fn mint(
        &self,
        sub: &str,
        workspace: &str,
        name: &str,
        requested: &[String],
        granted: &[String],
        now: u64,
        expires_at: Option<u64>,
    ) -> Result<(PatToken, PatRecord), PatError>;

    /// List `sub`'s own tokens (newest first), without any secret material.
    async fn list(&self, sub: &str) -> Result<Vec<PatRecord>, PatError>;

    /// Revoke `sub`'s token addressed by `id` (self-only). `Ok(false)` when no active token of
    /// that `sub` has that id — the handler maps it to `404`.
    async fn revoke(&self, sub: &str, id: &str) -> Result<bool, PatError>;
}

/// The async OAuth/OIDC provider-administration port behind `POST`/`PATCH`/`DELETE
/// `/auth/providers` (hq-idp-db.4): a SYSTEM admin manages the GLOBAL login providers (presets +
/// generic OIDC). The `client_secret` is write-only — accepted on create/patch (sealed at rest by
/// the adapter), but the [`ProviderView`] this returns never carries it back. Separate from the
/// per-workspace admin ports because providers are deploy-global, not tenant data. The production
/// adapter is [`PgProviderRepo`](crate::PgProviderRepo).
#[cfg(feature = "oauth")]
#[async_trait]
pub trait ProviderStore: Send + Sync {
    /// List every registered provider (no secret material).
    async fn list_providers(&self) -> Result<Vec<ProviderRecord>, AuthError>;
    /// Register a provider, sealing its cleartext secret at rest.
    async fn create_provider(&self, provider: NewProvider) -> Result<ProviderRecord, AuthError>;
    /// Apply a partial update; `None` ⇒ no provider with that id. A `Some` secret is re-sealed.
    async fn patch_provider(
        &self,
        id: &str,
        patch: PatchProvider,
    ) -> Result<Option<ProviderRecord>, AuthError>;
    /// Remove a provider; `false` ⇒ none matched (idempotent delete).
    async fn delete_provider(&self, id: &str) -> Result<bool, AuthError>;
}

/// The production [`ProviderStore`]: CRUD over the GLOBAL `public.oauth_providers` table, sealing
/// the client secret on write. Available when both `oauth` and `pg` are on.
#[cfg(all(feature = "oauth", feature = "pg"))]
#[async_trait]
impl ProviderStore for crate::PgProviderRepo {
    async fn list_providers(&self) -> Result<Vec<ProviderRecord>, AuthError> {
        crate::ProviderRepo::list(self).await
    }
    async fn create_provider(&self, provider: NewProvider) -> Result<ProviderRecord, AuthError> {
        crate::ProviderRepo::create(self, provider).await
    }
    async fn patch_provider(
        &self,
        id: &str,
        patch: PatchProvider,
    ) -> Result<Option<ProviderRecord>, AuthError> {
        crate::ProviderRepo::patch(self, id, patch).await
    }
    async fn delete_provider(&self, id: &str) -> Result<bool, AuthError> {
        crate::ProviderRepo::delete(self, id).await
    }
}

/// The ephemeral authorize-state port behind the public `/authorize`→`/callback` flow
/// (hq-idp-db.3): persist a pending handshake on `/authorize`, then ONE-SHOT consume it on
/// `/callback` (so a replayed `state` is rejected). Distinct from [`ProviderStore`] (the admin CRUD)
/// — this holds the transient per-login state, not the provider catalog. The production adapter is
/// [`PgAuthzStateRepo`](crate::PgAuthzStateRepo).
#[cfg(feature = "oauth")]
#[async_trait]
pub trait AuthzStateStore: Send + Sync {
    /// Persist a pending authorize handshake (the `/authorize` leg).
    async fn insert(&self, authz: NewAuthz) -> Result<(), AuthError>;
    /// Atomically delete + return the row for `state`, or `None` if absent/already-consumed.
    async fn consume(&self, state: &str) -> Result<Option<PendingAuthz>, AuthError>;
}

/// The production [`AuthzStateStore`]: insert/consume over `public.oauth_authz_state`. Available
/// when both `oauth` and `pg` are on.
#[cfg(all(feature = "oauth", feature = "pg"))]
#[async_trait]
impl AuthzStateStore for crate::PgAuthzStateRepo {
    async fn insert(&self, authz: NewAuthz) -> Result<(), AuthError> {
        crate::AuthzStateRepo::insert(self, authz).await
    }
    async fn consume(&self, state: &str) -> Result<Option<PendingAuthz>, AuthError> {
        crate::AuthzStateRepo::consume(self, state).await
    }
}

/// The one-shot CLI hand-off code port behind the `gt login` `/callback`→`/auth/cli/exchange` flow
/// (hq-gt-login-oauth.2): park the minted token pair under an opaque `code` on the callback, then
/// ONE-SHOT consume it at the exchange (so a captured loopback URL is useless after first use). The
/// production adapter is [`PgCliCodeRepo`](crate::PgCliCodeRepo).
#[cfg(feature = "oauth")]
#[async_trait]
pub trait CliCodeStore: Send + Sync {
    /// Persist a minted hand-off code (the callback leg).
    async fn insert(&self, code: crate::NewCliCode) -> Result<(), AuthError>;
    /// Atomically delete + return the row for `code`, or `None` if absent/already-consumed.
    async fn consume(&self, code: &str) -> Result<Option<crate::PendingCliCode>, AuthError>;
}

/// The production [`CliCodeStore`]: insert/consume over `public.oauth_cli_code`. Available when both
/// `oauth` and `pg` are on.
#[cfg(all(feature = "oauth", feature = "pg"))]
#[async_trait]
impl CliCodeStore for crate::PgCliCodeRepo {
    async fn insert(&self, code: crate::NewCliCode) -> Result<(), AuthError> {
        crate::CliCodeRepo::insert(self, code).await
    }
    async fn consume(&self, code: &str) -> Result<Option<crate::PendingCliCode>, AuthError> {
        crate::CliCodeRepo::consume(self, code).await
    }
}

/// The OAuth redirect-flow port behind `/authorize` + `/callback` (hq-idp-db.3): build the IdP
/// authorization URL for a provider (with `state` + PKCE `code_challenge`), and finish the flow by
/// exchanging the returned `code` (replaying the `code_verifier`) into a [`VerifiedIdentity`] — the
/// SAME value the password path yields. The production adapter is [`DbOauthLogin`](crate::DbOauthLogin),
/// the per-request DB-backed resolver; it owns the app's own callback URL ([`redirect_uri`](Self::redirect_uri)).
#[cfg(feature = "oauth")]
#[async_trait]
pub trait OauthAuthzFlow: Send + Sync {
    /// The app's own `/auth/callback` URL echoed on every exchange (recorded on the pending row).
    fn redirect_uri(&self) -> &str;
    /// Build the IdP authorization URL for `provider_id` (`404` for an absent/disabled id).
    async fn authorize_url(
        &self,
        provider_id: &str,
        state: &str,
        code_challenge: &str,
    ) -> Result<String, AuthError>;
    /// Exchange `code` (replaying `code_verifier`) for `provider_id` into a [`VerifiedIdentity`].
    async fn exchange(
        &self,
        provider_id: &str,
        code: &str,
        code_verifier: &str,
    ) -> Result<VerifiedIdentity, AuthError>;
}

/// The production [`OauthAuthzFlow`]: the DB-backed resolver builds the authorize URL + runs the
/// PKCE exchange against the provider selected by `provider_id`. Available with the `oauth` feature.
#[cfg(feature = "oauth")]
#[async_trait]
impl OauthAuthzFlow for crate::DbOauthLogin {
    fn redirect_uri(&self) -> &str {
        crate::DbOauthLogin::redirect_uri(self)
    }
    async fn authorize_url(
        &self,
        provider_id: &str,
        state: &str,
        code_challenge: &str,
    ) -> Result<String, AuthError> {
        crate::DbOauthLogin::authorize_url(self, provider_id, state, code_challenge).await
    }
    async fn exchange(
        &self,
        provider_id: &str,
        code: &str,
        code_verifier: &str,
    ) -> Result<VerifiedIdentity, AuthError> {
        crate::DbOauthLogin::exchange(self, provider_id, code, code_verifier).await
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
    /// The login step (identity verification) for the default email+password path.
    pub login: Arc<dyn LoginProvider>,
    /// The OAuth/OIDC login provider (hq-platform-hardening.3): a `POST /auth/login` carrying a
    /// `code` (OAuth authorization-code grant) or an `id_token`+`issuer` (OIDC) is routed here
    /// instead of [`login`](Self::login). The production adapter is
    /// [`OidcProvider`](crate::OidcProvider) (behind the `oauth` feature). `None` ⇒ an OAuth/OIDC
    /// login responds `501`; the email+password path is unaffected.
    pub oauth_login: Option<Arc<dyn LoginProvider>>,
    /// The RS256 access-token minter.
    pub minter: Arc<JwtMinter>,
    /// The refresh-token store (rotation, reuse detection, revocation). The async port, so the
    /// durable [`PgRefreshStore`](crate::PgRefreshStore) (restart-surviving) backs it without a
    /// `block_on`; the in-memory fallback satisfies it via the blanket impl (`hq-platform-hardening.1`).
    pub refresh: Arc<dyn AsyncRefreshStore>,
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
    /// The user-administration store behind `/auth/users` (`hq-web-extras.5`). `None` ⇒ the
    /// admin endpoints are not configured and respond `501`; login still works without it.
    pub users: Option<Arc<dyn UserStore>>,
    /// The role-administration store behind `/auth/roles` + `/auth/users/{email}/roles`
    /// (hq-rbac.4). `None` ⇒ those endpoints respond `501`; login + user admin are unaffected.
    pub roles: Option<Arc<dyn RoleStore>>,
    /// The Personal Access Token store behind the SELF-SERVICE `/auth/tokens` surface
    /// (hq-security-pat.2): a user mints / lists / revokes their OWN tokens, gated by the
    /// `tokens.read`/`tokens.write` scopes. `None` ⇒ those endpoints respond `501`; everything
    /// else is unaffected. The production adapter is [`PgPatStore`](crate::PgPatStore).
    pub pat: Option<Arc<dyn PatAdmin>>,
    /// The workspace-membership directory behind `GET /auth/workspaces` + `POST /auth/switch`
    /// (hq-identity.3). `None` ⇒ those endpoints respond `501`; login + admin are unaffected.
    pub memberships: Option<Arc<dyn MembershipDirectory>>,
    /// The workspace-membership ADMIN surface behind `POST`/`DELETE
    /// `/auth/workspaces/{slug}/members` (hq-platform-hardening.2): a ws admin attaches/detaches
    /// another user. `None` ⇒ those endpoints respond `501`; everything else is unaffected.
    pub membership_admin: Option<Arc<dyn MembershipAdmin>>,
    /// The OAuth/OIDC provider-administration store behind `POST`/`PATCH`/`DELETE
    /// `/auth/providers` (hq-idp-db.4): a SYSTEM admin manages the GLOBAL login providers. `None`
    /// ⇒ those endpoints respond `501`; login + the per-ws admin surfaces are unaffected. Present
    /// only with the `oauth` feature (the provider store + secret crypto ride it).
    #[cfg(feature = "oauth")]
    pub providers: Option<Arc<dyn ProviderStore>>,
    /// The OAuth redirect-flow driver behind the PUBLIC `GET /auth/providers/{id}/authorize` +
    /// `GET /auth/callback` (hq-idp-db.3): builds the IdP authorization URL and runs the PKCE
    /// exchange. `None` ⇒ those endpoints respond `501`. The production adapter is
    /// [`DbOauthLogin`](crate::DbOauthLogin) (the same resolver behind [`oauth_login`](Self::oauth_login)).
    #[cfg(feature = "oauth")]
    pub authz_flow: Option<Arc<dyn OauthAuthzFlow>>,
    /// The ephemeral authorize-state store (hq-idp-db.3): holds the per-login `state`+PKCE
    /// `code_verifier` between `/authorize` and `/callback`. `None` ⇒ those endpoints respond `501`.
    /// The production adapter is [`PgAuthzStateRepo`](crate::PgAuthzStateRepo).
    #[cfg(feature = "oauth")]
    pub authz_state: Option<Arc<dyn AuthzStateStore>>,
    /// The one-shot CLI hand-off code store behind the `gt login` browser flow
    /// (hq-gt-login-oauth.2): the callback parks the minted token pair here under an opaque code and
    /// 302s it to the CLI loopback; `POST /auth/cli/exchange` redeems it. `None` ⇒ a `cli_redirect`
    /// handshake responds `501`. The production adapter is [`PgCliCodeRepo`](crate::PgCliCodeRepo).
    #[cfg(feature = "oauth")]
    pub cli_code: Option<Arc<dyn CliCodeStore>>,
    /// Where `GET /auth/callback` sends the browser after a successful login (hq-idp-db.3): the FE
    /// landing URL the freshly minted tokens are handed off to (via a short-lived URL fragment, plus
    /// the httpOnly auth cookies). `None` ⇒ the callback returns the token JSON directly (useful for
    /// a non-browser client / test). From `GT_OAUTH_FE_REDIRECT_URL`.
    #[cfg(feature = "oauth")]
    pub fe_redirect_url: Option<String>,
    /// The verifier's public JWKS, served at `GET /auth/jwks` so clients verify access tokens
    /// offline. Built from the verifier's public keys at the composition root
    /// ([`JwtAuthenticator::jwk_set`](crate::JwtAuthenticator::jwk_set)) — never the signing
    /// secret. `Arc` so cloning the state per request stays cheap.
    pub jwks: Arc<JwkSet>,
}

/// Build the auth router. Mount it under the API base at the composition root.
pub fn auth_router(state: AuthState) -> Router {
    let router = Router::new()
        .route("/auth/login", post(login))
        .route("/auth/refresh", post(refresh))
        .route("/auth/logout", post(logout))
        .route("/auth/me", get(me))
        .route("/auth/jwks", get(jwks));
    // User administration (`hq-web-extras.5`) hashes passwords, so it rides the `pg`/argon2
    // adapter — without it the surface is login-only.
    #[cfg(feature = "pg")]
    let router = router
        .route("/auth/users", post(create_user).get(list_users))
        // Role administration + assignment (hq-rbac.4), scope-gated like the user surface.
        .route("/auth/roles", post(create_role).get(list_roles))
        .route("/auth/roles/:name", axum::routing::delete(delete_role))
        .route("/auth/users/:email/roles", post(assign_roles))
        // Personal Access Tokens (hq-security-pat.2): the SELF-SERVICE surface — a user manages
        // their OWN tokens, gated by tokens.read / tokens.write. POST returns the secret ONCE.
        .route("/auth/tokens", get(list_tokens).post(create_token))
        .route("/auth/tokens/:id", axum::routing::delete(revoke_token))
        // Cross-workspace surface (hq-identity.3): list memberships + switch the active workspace.
        .route("/auth/workspaces", get(workspaces))
        .route("/auth/switch", post(switch))
        // Membership administration (hq-platform-hardening.2): a ws admin adds/removes another
        // user. Gated on the caller being an admin OF the path workspace, not the system admin.
        .route("/auth/workspaces/:slug/members", post(add_member))
        .route(
            "/auth/workspaces/:slug/members/:email",
            axum::routing::delete(remove_member),
        );
    // OAuth/OIDC provider administration (hq-idp-db.4): a SYSTEM admin manages the GLOBAL login
    // providers. Rides the `oauth` feature (the provider store + secret crypto), not `pg`, so a
    // login-only build carries no provider surface.
    #[cfg(feature = "oauth")]
    let router = router
        // PUBLIC discovery (no auth): the FE login page (hq-idp-db.5) fetches this to render a
        // button per enabled provider — secret-free projection. GET is public; the POST admin CRUD
        // (system-admin gated) shares the path.
        .route(
            "/auth/providers",
            get(list_public_providers).post(create_provider),
        )
        // ADMIN list (hq-idp-ui.1): the FULL provider list incl `enabled=false`, secret-free
        // ([`ProviderView`]), for the gt-web admin page. A STATIC segment, so matchit resolves it
        // before the `/:id` capture below — `all` is never read as a provider id.
        .route("/auth/providers/all", get(list_all_providers))
        // ADMIN single-by-id (hq-idp-ui.1): pre-fill the edit form. GET joins the existing
        // PATCH/DELETE on this path. `/:id/authorize` (public, below) is MORE specific, so matchit
        // keeps it resolving to the authorize handler — this GET only matches a bare `/{id}`.
        .route(
            "/auth/providers/:id",
            get(get_provider)
                .patch(patch_provider)
                .delete(delete_provider),
        )
        // PUBLIC authorize redirect (hq-idp-db.3): 302 to the IdP with state + PKCE challenge.
        .route("/auth/providers/:id/authorize", get(authorize))
        // PUBLIC callback (hq-idp-db.3): validate+consume state, redeem the code, issue tokens.
        .route("/auth/callback", get(callback))
        // PUBLIC CLI hand-off exchange (hq-gt-login-oauth.2): redeem the one-shot `gt login` code.
        .route("/auth/cli/exchange", post(cli_exchange));
    router.with_state(state)
}

/// The OpenAPI document for the public `/auth/*` surface (`hq-web-extras.10`). UNLIKE the
/// `/api/v1/<ns>` module ApiDocs, the auth router mounts at the server ROOT (not under a module
/// prefix), so these paths are ABSOLUTE (`/auth/login`, …) and the composition root merges this
/// doc verbatim into the fused `GET /openapi.json` — the FE codegen then reads the auth contract
/// from the spec instead of hand-maintaining `src/lib/api/auth.ts`.
#[derive(utoipa::OpenApi)]
#[openapi(
    paths(login, refresh, logout, me, jwks),
    components(schemas(
        LoginRequest,
        TokenResponse,
        RefreshRequest,
        MeResponse,
        crate::jwt::Jwk,
        crate::jwt::JwkSet,
    ))
)]
pub struct ApiDoc;

/// The admin / RBAC / cross-workspace half of `/auth/*`, present only with the `pg` adapter (the
/// user + role + membership stores these routes need). Folded into [`auth_openapi`] when compiled,
/// so a Postgres-backed deploy advertises the full surface and a login-only build advertises just
/// [`ApiDoc`].
#[cfg(feature = "pg")]
#[derive(utoipa::OpenApi)]
#[openapi(
    paths(
        create_user,
        list_users,
        create_role,
        list_roles,
        delete_role,
        assign_roles,
        list_tokens,
        create_token,
        revoke_token,
        workspaces,
        switch,
        add_member,
        remove_member,
    ),
    components(schemas(
        CreateUserRequest,
        UserSummary,
        CreateRoleRequest,
        RoleSummary,
        AssignRolesRequest,
        CreateTokenRequest,
        TokenSummary,
        CreatedTokenResponse,
        SwitchRequest,
        WorkspaceMembership,
        AddMemberRequest,
    ))
)]
struct AdminApiDoc;

/// The OAuth/OIDC provider-administration half of `/auth/*`, present only with the `oauth` feature
/// (hq-idp-db.4): the system-admin CRUD over the GLOBAL login providers. Folded into [`auth_openapi`]
/// when compiled, so an OAuth-enabled deploy advertises the provider surface.
#[cfg(feature = "oauth")]
#[derive(utoipa::OpenApi)]
#[openapi(
    paths(
        list_public_providers,
        list_all_providers,
        get_provider,
        authorize,
        callback,
        create_provider,
        patch_provider,
        delete_provider,
    ),
    components(schemas(
        CreateProviderRequest,
        PatchProviderRequest,
        ProviderView,
        PublicProvider,
    ))
)]
struct ProviderApiDoc;

/// The full `/auth/*` OpenAPI the composition root fuses into `GET /openapi.json`: the always-on
/// login surface ([`ApiDoc`]) plus, when the `pg` adapter is compiled, the admin / RBAC /
/// workspace routes ([`AdminApiDoc`]), plus the OAuth provider CRUD ([`ProviderApiDoc`]) with the
/// `oauth` feature.
pub fn auth_openapi() -> utoipa::openapi::OpenApi {
    use utoipa::OpenApi as _;
    #[allow(unused_mut)]
    let mut doc = ApiDoc::openapi();
    #[cfg(feature = "pg")]
    doc.merge(AdminApiDoc::openapi());
    #[cfg(feature = "oauth")]
    doc.merge(ProviderApiDoc::openapi());
    doc
}

// --- request/response DTOs --------------------------------------------------------------------

/// `POST /auth/login` body — the email+password path by default, or an OAuth/OIDC handshake when
/// the corresponding fields are present (hq-platform-hardening.3). The server picks the provider
/// from which fields are supplied: `code` ⇒ the OAuth authorization-code grant, `id_token` ⇒ the
/// OIDC path, otherwise `email`+`password`.
#[derive(Debug, Default, Deserialize)]
#[cfg_attr(feature = "axum", derive(utoipa::ToSchema))]
pub struct LoginRequest {
    /// The principal's email address (email+password path). Absent on an OAuth/OIDC login.
    #[serde(default)]
    pub email: Option<String>,
    /// The plaintext password presented this request (email+password path).
    #[serde(default)]
    pub password: Option<String>,
    /// The registered provider's id in the `oauth_providers` store (hq-idp-db.2) — the primary key
    /// the FE login button carries. Pairs with [`code`](Self::code); the server loads this row,
    /// decrypts its client secret, and runs the handshake against it. An unknown/disabled id is a
    /// `404`. Falls back to [`provider`](Self::provider) when absent (the pre-DB wire name).
    #[serde(default)]
    pub provider_id: Option<String>,
    /// Upstream provider id for the OAuth authorization-code grant (e.g. `"github"`); the pre-DB
    /// alias of [`provider_id`](Self::provider_id), kept for back-compat. Pairs with
    /// [`code`](Self::code).
    #[serde(default)]
    pub provider: Option<String>,
    /// The OAuth authorization code returned to the redirect URI — present ⇒ the OAuth path.
    #[serde(default)]
    pub code: Option<String>,
    /// The issuer (`iss`) an [`id_token`](Self::id_token) claims to come from (OIDC path).
    #[serde(default)]
    pub issuer: Option<String>,
    /// An OpenID Connect `id_token` minted by a trusted issuer — present ⇒ the OIDC path.
    #[serde(default)]
    pub id_token: Option<String>,
}

impl LoginRequest {
    /// Pick the login provider from the supplied fields and produce the matching [`Credentials`]
    /// (hq-platform-hardening.3). Precedence: a `code` ⇒ the OAuth authorization-code grant, an
    /// `id_token` ⇒ the OIDC path, otherwise `email`+`password`. A body that names none of these
    /// is [`AuthError::InvalidCredentials`] — an empty/garbage login, mapped to `401`, never a
    /// silent default.
    fn into_credentials(self) -> Result<Credentials, AuthError> {
        match self {
            LoginRequest {
                code: Some(code),
                provider_id,
                provider,
                ..
            } => {
                Ok(Credentials::OAuth {
                    // `provider_id` (the `oauth_providers` PK) wins; `provider` is the pre-DB alias.
                    provider: provider_id.or(provider).unwrap_or_default(),
                    code,
                })
            }
            LoginRequest {
                id_token: Some(id_token),
                issuer: Some(issuer),
                ..
            } => Ok(Credentials::Oidc { issuer, id_token }),
            LoginRequest {
                email: Some(email),
                password: Some(password),
                ..
            } => Ok(Credentials::EmailPassword { email, password }),
            _ => Err(AuthError::InvalidCredentials),
        }
    }
}

/// A minted token pair, returned by login and refresh.
#[derive(Debug, Serialize)]
#[cfg_attr(feature = "axum", derive(utoipa::ToSchema))]
pub struct TokenResponse {
    /// The short-lived RS256 access JWT (sent as `Authorization: Bearer`).
    pub access_token: String,
    /// The long-lived opaque refresh token (exchanged at `/auth/refresh`).
    pub refresh_token: String,
    /// Always `"Bearer"`.
    #[cfg_attr(feature = "axum", schema(value_type = String))]
    pub token_type: &'static str,
    /// Access-token lifetime in seconds.
    pub expires_in: u64,
}

/// `POST /auth/refresh` / `POST /auth/logout` body. Optional: a browser supplies the refresh
/// token through the `gt_refresh` httpOnly cookie instead, so the JSON body may be absent.
#[derive(Debug, Default, Deserialize)]
#[cfg_attr(feature = "axum", derive(utoipa::ToSchema))]
pub struct RefreshRequest {
    /// The opaque refresh token previously issued. Absent ⇒ read from the cookie.
    pub refresh_token: Option<String>,
}

/// `GET /auth/me` body — the verified identity behind the bearer token.
#[derive(Debug, Serialize)]
#[cfg_attr(feature = "axum", derive(utoipa::ToSchema))]
pub struct MeResponse {
    /// Authenticated principal.
    pub sub: String,
    /// Tenant the token is scoped to.
    pub workspace: String,
    /// Granted authorization scopes.
    pub scopes: Vec<String>,
}

/// `POST /auth/switch` body — the workspace to make active (hq-identity.3).
#[derive(Debug, Deserialize)]
#[cfg_attr(feature = "axum", derive(utoipa::ToSchema))]
pub struct SwitchRequest {
    /// The slug of a workspace the caller is a member of. Validated against their memberships
    /// server-side; an unheld workspace is a `403`, never an honoured switch.
    pub workspace: String,
}

/// `POST /auth/workspaces/{slug}/members` body — add another user to the workspace
/// (ws-admin only, hq-platform-hardening.2).
#[derive(Debug, Deserialize)]
#[cfg_attr(feature = "axum", derive(utoipa::ToSchema))]
pub struct AddMemberRequest {
    /// The login email of an existing global user to grant membership.
    pub email: String,
    /// The role the user holds in this workspace (expanded to scopes against `ws_<slug>.roles`
    /// at login/switch). Empty ⇒ a member with no role-granted scopes.
    #[serde(default)]
    pub role: String,
}

/// `POST /auth/users` body — create a user (admin only, `hq-web-extras.5`).
#[derive(Debug, Deserialize)]
#[cfg_attr(feature = "axum", derive(utoipa::ToSchema))]
pub struct CreateUserRequest {
    /// Login email (unique within the workspace).
    pub email: String,
    /// Plaintext password — argon2-hashed server-side, never stored or echoed.
    pub password: String,
    /// Granted authorization scopes. Empty ⇒ a user that can authenticate but reach nothing.
    #[serde(default)]
    pub scopes: Vec<String>,
}

/// A user as returned by `GET /auth/users` / `POST /auth/users` — never any password material.
#[derive(Debug, Serialize)]
#[cfg_attr(feature = "axum", derive(utoipa::ToSchema))]
pub struct UserSummary {
    /// The subject id (`sub`).
    pub sub: String,
    /// Login email.
    pub email: String,
    /// Granted authorization scopes.
    pub scopes: Vec<String>,
    /// Creation time (epoch seconds).
    pub created_at: i64,
}

/// `POST /auth/roles` body — create or replace a role (admin only, hq-rbac.4).
#[derive(Debug, Deserialize)]
#[cfg_attr(feature = "axum", derive(utoipa::ToSchema))]
pub struct CreateRoleRequest {
    /// The role name (referenced by a user's assigned roles).
    pub name: String,
    /// The scope bundle the role grants. Each entry is validated against the closed scope
    /// vocabulary ([`gt_rbac::validate_scope`]) before write — a typo is a `400`.
    #[serde(default)]
    pub scopes: Vec<String>,
}

/// A role as returned by `GET`/`POST /auth/roles`.
#[derive(Debug, Serialize)]
#[cfg_attr(feature = "axum", derive(utoipa::ToSchema))]
pub struct RoleSummary {
    /// The role name.
    pub name: String,
    /// The scope bundle the role grants.
    pub scopes: Vec<String>,
    /// Creation time (epoch seconds).
    pub created_at: i64,
}

/// `POST /auth/users/{email}/roles` body — set a user's assigned roles (admin only, hq-rbac.4).
#[derive(Debug, Deserialize)]
#[cfg_attr(feature = "axum", derive(utoipa::ToSchema))]
pub struct AssignRolesRequest {
    /// The role names to assign, replacing the user's current set.
    #[serde(default)]
    pub roles: Vec<String>,
}

/// `POST /auth/tokens` body — mint a Personal Access Token for the caller (hq-security-pat.2).
#[derive(Debug, Deserialize)]
#[cfg_attr(feature = "axum", derive(utoipa::ToSchema))]
pub struct CreateTokenRequest {
    /// A human label so a list of tokens is legible (e.g. "ci-deploy").
    pub name: String,
    /// The scopes to grant, **clamped** to the caller's own at mint (asking for a scope you do not
    /// hold drops it). Absent or empty ⇒ grant everything the caller currently holds.
    #[serde(default)]
    pub scopes: Option<Vec<String>>,
    /// Lifetime in seconds from now. Absent ⇒ the token never expires (revocation is then the only
    /// way to kill it).
    #[serde(default)]
    pub expires_in_secs: Option<u64>,
}

/// A Personal Access Token as returned by `GET /auth/tokens` and embedded in the create response —
/// the non-secret projection of a [`PatRecord`] (it carries the token's `id`, never the secret).
#[derive(Debug, Serialize)]
#[cfg_attr(feature = "axum", derive(utoipa::ToSchema))]
pub struct TokenSummary {
    /// The non-secret id — the handle `DELETE /auth/tokens/{id}` revokes by.
    pub id: String,
    /// The human label the owner gave the token.
    pub name: String,
    /// The clamped scopes the token grants.
    pub scopes: Vec<String>,
    /// Creation time (epoch seconds).
    pub created_at: i64,
    /// Expiry (epoch seconds), or `null` for a token that never expires.
    pub expires_at: Option<i64>,
    /// Last successful use (epoch seconds), or `null` if never used.
    pub last_used_at: Option<i64>,
    /// Lifecycle: `"active"` or `"revoked"`.
    pub status: String,
}

impl From<PatRecord> for TokenSummary {
    fn from(r: PatRecord) -> Self {
        TokenSummary {
            id: r.id.as_str().to_owned(),
            name: r.name,
            scopes: r.scopes,
            created_at: r.created_at as i64,
            expires_at: r.expires_at.map(|e| e as i64),
            last_used_at: r.last_used_at.map(|e| e as i64),
            status: r.status.as_str().to_owned(),
        }
    }
}

/// `POST /auth/tokens` 201 body — the freshly minted token. The `token` plaintext is returned
/// **exactly once**, here; it is never recoverable afterwards (the store keeps only its hash), so
/// the FE shows it once with a copy affordance.
#[derive(Debug, Serialize)]
#[cfg_attr(feature = "axum", derive(utoipa::ToSchema))]
pub struct CreatedTokenResponse {
    /// The opaque secret (`gtpat_…`) — shown once, never again.
    pub token: String,
    /// The token's non-secret record (id, name, scopes, …), the same shape `GET` lists.
    pub info: TokenSummary,
}

/// `POST /auth/providers` body — register a GLOBAL OAuth/OIDC login provider (system admin only,
/// hq-idp-db.4). The `client_secret` is WRITE-ONLY: accepted here, sealed at rest, and NEVER echoed
/// back (the response is a [`ProviderView`], which omits it). For a preset `kind`
/// (`google`/`github`/`microsoft`) the endpoints + default scopes are baked, so only the client
/// credentials are required; for the `generic` kind the admin supplies every endpoint.
#[cfg(feature = "oauth")]
#[derive(Debug, Deserialize)]
#[cfg_attr(feature = "axum", derive(utoipa::ToSchema))]
pub struct CreateProviderRequest {
    /// The stable id / primary key (also the login-button token).
    pub id: String,
    /// The provider variant: `google` / `github` / `microsoft` / `generic`.
    pub kind: String,
    /// Human label for the login button. Omitted ⇒ the preset's label (required for `generic`).
    #[serde(default)]
    pub display_name: Option<String>,
    /// The registered client id.
    pub client_id: String,
    /// The registered client secret (cleartext on the wire; sealed at rest, never returned).
    pub client_secret: String,
    /// Issuer URL. Omitted ⇒ the preset's (required for `generic`).
    #[serde(default)]
    pub issuer: Option<String>,
    /// Authorization endpoint. Omitted ⇒ the preset's (required for `generic`).
    #[serde(default)]
    pub authorize_endpoint: Option<String>,
    /// Token endpoint. Omitted ⇒ the preset's (required for `generic`).
    #[serde(default)]
    pub token_endpoint: Option<String>,
    /// Userinfo endpoint. Omitted ⇒ the preset's (required for `generic`).
    #[serde(default)]
    pub userinfo_endpoint: Option<String>,
    /// Comma-separated scopes. Omitted ⇒ the preset's defaults (required for `generic`).
    #[serde(default)]
    pub scopes: Option<String>,
    /// Whether the provider shows as a login button. Omitted ⇒ `true`.
    #[serde(default = "default_true")]
    pub enabled: bool,
    /// Optional workspace scope (hq-epic.auth-refactor.2). `null`/absent = global (shown on every
    /// workspace's login page). A non-null slug scopes the provider to that workspace only.
    #[serde(default)]
    pub workspace_id: Option<String>,
}

/// The default for [`CreateProviderRequest::enabled`] — a freshly registered provider is live.
#[cfg(feature = "oauth")]
fn default_true() -> bool {
    true
}

/// `PATCH /auth/providers/{id}` body — partial update of a provider (system admin only,
/// hq-idp-db.4). Every field is optional: `None`/absent leaves the column, `Some` overwrites.
/// `client_secret` is write-only — supply it to ROTATE the secret, omit it to leave the stored one.
#[cfg(feature = "oauth")]
#[derive(Debug, Default, Deserialize)]
#[cfg_attr(feature = "axum", derive(utoipa::ToSchema))]
pub struct PatchProviderRequest {
    /// New human label, or absent to leave it.
    #[serde(default)]
    pub display_name: Option<String>,
    /// New client id, or absent to leave it.
    #[serde(default)]
    pub client_id: Option<String>,
    /// New client secret to rotate to (sealed at rest, never returned), or absent to keep it.
    #[serde(default)]
    pub client_secret: Option<String>,
    /// New issuer URL, or absent to leave it.
    #[serde(default)]
    pub issuer: Option<String>,
    /// New authorization endpoint, or absent to leave it.
    #[serde(default)]
    pub authorize_endpoint: Option<String>,
    /// New token endpoint, or absent to leave it.
    #[serde(default)]
    pub token_endpoint: Option<String>,
    /// New userinfo endpoint, or absent to leave it.
    #[serde(default)]
    pub userinfo_endpoint: Option<String>,
    /// New comma-separated scopes, or absent to leave them.
    #[serde(default)]
    pub scopes: Option<String>,
    /// Toggle whether the provider shows as a login button, or absent to leave it.
    #[serde(default)]
    pub enabled: Option<bool>,
    /// New workspace scope (hq-epic.auth-refactor.2). Absent leaves the column untouched.
    /// `Some(None)` clears to global; `Some(Some(slug))` scopes to that workspace.
    #[serde(default)]
    pub workspace_id: Option<Option<String>>,
}

/// A provider as returned by every read/echo on `/auth/providers` (hq-idp-db.4) — the projection
/// that OMITS the `client_secret` (sealed or plain). The secret is write-only; no read surface ever
/// carries it, so a compromised token cannot exfiltrate a configured provider's credentials.
#[cfg(feature = "oauth")]
#[derive(Debug, Serialize)]
#[cfg_attr(feature = "axum", derive(utoipa::ToSchema))]
pub struct ProviderView {
    /// The id / primary key.
    pub id: String,
    /// The provider variant (`google` / `github` / `microsoft` / `generic`).
    pub kind: String,
    /// The human label for the login button.
    pub display_name: String,
    /// The registered client id (public; the secret is never included).
    pub client_id: String,
    /// The issuer URL.
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
    /// Optional workspace scope (hq-epic.auth-refactor.2). `null` = global.
    pub workspace_id: Option<String>,
}

/// A login provider as returned by the PUBLIC `GET /auth/providers` (hq-idp-db.3) — the projection
/// the FE login page renders a button from. Carries ONLY what a login button needs: the id, the
/// human label, the kind (so the FE can pick an icon), and the relative `authorize_url` to send the
/// browser to. It NEVER carries the client secret OR the client id / endpoints — strictly less than
/// even the admin [`ProviderView`], because this surface is unauthenticated.
#[cfg(feature = "oauth")]
#[derive(Debug, Serialize)]
#[cfg_attr(feature = "axum", derive(utoipa::ToSchema))]
pub struct PublicProvider {
    /// The id / primary key (also the login-button token).
    pub id: String,
    /// The provider variant (`google` / `github` / `microsoft` / `generic`) — for the button icon.
    pub kind: String,
    /// The human label for the login button.
    pub display_name: String,
    /// The relative URL the FE points the browser at to begin login (the server builds the full IdP
    /// authorize URL with state + PKCE there, so the FE never holds OAuth parameters).
    pub authorize_url: String,
}

#[cfg(feature = "oauth")]
impl From<ProviderRecord> for PublicProvider {
    fn from(r: ProviderRecord) -> Self {
        // The relative authorize link the FE follows; the full IdP URL (with state + PKCE) is built
        // server-side at `/auth/providers/{id}/authorize`, so no OAuth params leak to the page.
        let authorize_url = format!("/auth/providers/{}/authorize", r.id);
        PublicProvider {
            id: r.id,
            kind: r.kind.as_str().to_owned(),
            display_name: r.display_name,
            authorize_url,
        }
    }
}

#[cfg(feature = "oauth")]
impl From<ProviderRecord> for ProviderView {
    fn from(r: ProviderRecord) -> Self {
        // The sealed `client_secret_enc` is intentionally DROPPED here — this projection is the
        // only shape any read/echo returns, so the secret can never leak through the HTTP surface.
        ProviderView {
            id: r.id,
            kind: r.kind.as_str().to_owned(),
            display_name: r.display_name,
            client_id: r.client_id,
            issuer: r.issuer,
            authorize_endpoint: r.authorize_endpoint,
            token_endpoint: r.token_endpoint,
            userinfo_endpoint: r.userinfo_endpoint,
            scopes: r.scopes,
            enabled: r.enabled,
            workspace_id: r.workspace_id,
        }
    }
}

// --- handlers ---------------------------------------------------------------------------------

#[cfg_attr(feature = "axum", utoipa::path(
    post, path = "/auth/login", tag = "auth",
    request_body = LoginRequest,
    responses(
        (status = 200, description = "Authenticated — access + refresh pair (also set as httpOnly cookies)", body = TokenResponse),
        (status = 401, description = "Bad credentials"),
    ),
))]
async fn login(
    State(state): State<AuthState>,
    Json(body): Json<LoginRequest>,
) -> Result<(HeaderMap, Json<TokenResponse>), ApiError> {
    let creds = body.into_credentials()?;
    // The OAuth/OIDC handshake (hq-platform-hardening.3) is a different provider than
    // email+password: route by `kind()`. An OAuth/OIDC login with no provider configured is
    // `501 Not Implemented` (the deploy did not opt into the `oauth` adapter) rather than a
    // misleading `401` — login itself works, this method just is not wired.
    let provider: &Arc<dyn LoginProvider> = match creds.kind() {
        ProviderKind::EmailPassword => &state.login,
        ProviderKind::OAuth | ProviderKind::Oidc => state
            .oauth_login
            .as_ref()
            .ok_or(ApiError::OauthNotConfigured)?,
    };
    let identity = provider.login(&creds).await?;
    let tokens = issue_tokens(&state, identity.sub, identity.workspace, identity.scopes).await?;
    // Set httpOnly cookies for the browser (SSE + refresh) while still returning the JSON body
    // for non-browser clients (`hq-web-extras.1`).
    Ok((set_token_cookies(&state, &tokens), Json(tokens)))
}

#[cfg_attr(feature = "axum", utoipa::path(
    post, path = "/auth/refresh", tag = "auth",
    request_body(content = RefreshRequest, description = "Optional — the refresh token may instead ride the gt_refresh httpOnly cookie"),
    responses(
        (status = 200, description = "Rotated — a fresh access + refresh pair", body = TokenResponse),
        (status = 401, description = "Missing, unknown, or already-rotated refresh token"),
    ),
))]
async fn refresh(
    State(state): State<AuthState>,
    headers: HeaderMap,
    body: Option<Json<RefreshRequest>>,
) -> Result<(HeaderMap, Json<TokenResponse>), ApiError> {
    // Prefer the httpOnly cookie (browser); fall back to the JSON body (non-browser clients).
    let token = refresh_token_from(&headers, body).ok_or(ApiError::Unauthenticated)?;
    let now = (state.now)();
    let (next, record) = state.refresh.rotate(&RefreshToken::new(token), now).await?;
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

#[cfg_attr(feature = "axum", utoipa::path(
    post, path = "/auth/logout", tag = "auth",
    request_body(content = RefreshRequest, description = "Optional — token may instead ride the gt_refresh httpOnly cookie"),
    responses(
        (status = 204, description = "Logged out (idempotent — revoking an absent token is a no-op); cookies cleared"),
    ),
))]
async fn logout(
    State(state): State<AuthState>,
    headers: HeaderMap,
    body: Option<Json<RefreshRequest>>,
) -> (HeaderMap, StatusCode) {
    // Idempotent: revoking an unknown/absent token is a no-op, so logout always succeeds and
    // cannot probe which tokens exist. Either source (cookie or body) is honoured, and the
    // cookies are cleared regardless.
    if let Some(token) = refresh_token_from(&headers, body) {
        // Best-effort: a backend fault on revoke must not fail logout (the client is leaving and
        // the cookies are cleared regardless). The durable store's reuse/revoke verdicts are
        // already terminal; an outage here is logged-and-dropped, not surfaced.
        let _ = state
            .refresh
            .revoke_by_token(&RefreshToken::new(token))
            .await;
    }
    (clear_token_cookies(&state), StatusCode::NO_CONTENT)
}

#[cfg_attr(feature = "axum", utoipa::path(
    get, path = "/auth/me", tag = "auth",
    responses(
        (status = 200, description = "The verified identity behind the bearer token", body = MeResponse),
        (status = 401, description = "No verified claims (absent or invalid bearer)"),
    ),
))]
async fn me(claims: Option<Extension<JwtClaims>>) -> Result<Json<MeResponse>, ApiError> {
    let Extension(claims) = claims.ok_or(ApiError::Unauthenticated)?;
    Ok(Json(MeResponse {
        sub: claims.sub,
        workspace: claims.workspace,
        scopes: claims.scopes,
    }))
}

/// `GET /auth/workspaces` — the authenticated user's workspace memberships (slug + role), for the
/// gt-web workspace selector (hq-identity.3). `401` without verified claims; `501` when no
/// membership directory is configured. The list is keyed by the token's `sub`, so a normal caller
/// only ever sees their own memberships; a SYSTEM admin (`*`) sees every provisioned workspace, so
/// the selector lets them switch into any tenant.
#[cfg_attr(feature = "axum", utoipa::path(
    get, path = "/auth/workspaces", tag = "auth",
    responses(
        (status = 200, description = "The caller's workspace memberships (slug + role)", body = Vec<WorkspaceMembership>),
        (status = 401, description = "No verified claims"),
        (status = 501, description = "No membership directory configured"),
    ),
))]
#[cfg(feature = "pg")]
async fn workspaces(
    State(state): State<AuthState>,
    claims: Option<Extension<JwtClaims>>,
) -> Result<Json<Vec<WorkspaceMembership>>, ApiError> {
    let Extension(claims) = claims.ok_or(ApiError::Unauthenticated)?;
    let dir = state.memberships.as_ref().ok_or(ApiError::NotConfigured)?;
    // A system admin (`*`) reaches every tenant, so widen the picker to all workspaces; a normal
    // user sees only their own memberships.
    let rows = if is_system_admin(&claims) {
        dir.list_all().await?
    } else {
        dir.list(&claims.sub).await?
    };
    Ok(Json(rows))
}

/// `POST /auth/switch` `{workspace}` — re-target the session to another of the user's workspaces
/// (hq-identity.3). Re-mints the access + refresh pair (and cookies) with that workspace active and
/// its role-expanded scopes. `403` when the user is not a member of the requested workspace —
/// the active tenant is resolved server-side from membership, never granted on request. A SYSTEM
/// admin (`*`) is the exception: they may switch into ANY provisioned workspace, carrying their `*`
/// grant, and get `404` (not `403`) for a slug that names no workspace. `401` without claims; `501`
/// when no directory is configured.
#[cfg_attr(feature = "axum", utoipa::path(
    post, path = "/auth/switch", tag = "auth",
    request_body = SwitchRequest,
    responses(
        (status = 200, description = "Re-targeted — a fresh access + refresh pair scoped to the new workspace", body = TokenResponse),
        (status = 401, description = "No verified claims"),
        (status = 403, description = "Caller is not a member of the requested workspace (non-admin)"),
        (status = 404, description = "System admin requested a workspace that does not exist"),
        (status = 501, description = "No membership directory configured"),
    ),
))]
#[cfg(feature = "pg")]
async fn switch(
    State(state): State<AuthState>,
    claims: Option<Extension<JwtClaims>>,
    Json(body): Json<SwitchRequest>,
) -> Result<(HeaderMap, Json<TokenResponse>), ApiError> {
    let Extension(claims) = claims.ok_or(ApiError::Unauthenticated)?;
    let dir = state.memberships.as_ref().ok_or(ApiError::NotConfigured)?;
    let identity = match dir.resolve(&claims.sub, &body.workspace).await? {
        Some(identity) => identity,
        // A system admin may enter any PROVISIONED workspace even without a membership, carrying
        // their `*` grant into the new tenant. `404` (not `403`) when the slug names no workspace,
        // so a typo is distinguishable from a permission denial. A non-admin is still `403`.
        None if is_system_admin(&claims) => {
            let known = dir
                .list_all()
                .await?
                .iter()
                .any(|w| w.workspace == body.workspace);
            if !known {
                return Err(ApiError::NotFound);
            }
            VerifiedIdentity {
                sub: claims.sub.clone(),
                workspace: body.workspace.clone(),
                scopes: claims.scopes.clone(),
            }
        }
        None => return Err(ApiError::Forbidden),
    };
    let tokens = issue_tokens(&state, identity.sub, identity.workspace, identity.scopes).await?;
    Ok((set_token_cookies(&state, &tokens), Json(tokens)))
}

/// `POST /auth/workspaces/{slug}/members` `{email, role}` — add another user to `slug`
/// (hq-platform-hardening.2). Gated on the caller being an ADMIN OF `slug` (their active
/// workspace is `slug` and their scopes include `workspace.admin` or `*`) — a workspace's own
/// admin, never the system admin, and never an admin of a DIFFERENT workspace. The added user can
/// then `/auth/switch` into `slug`. `404` when no global user has that email.
#[cfg_attr(feature = "axum", utoipa::path(
    post, path = "/auth/workspaces/{slug}/members", tag = "auth",
    params(("slug" = String, Path, description = "The workspace to add the member to")),
    request_body = AddMemberRequest,
    responses(
        (status = 204, description = "Member added (idempotent — re-adding updates the role)"),
        (status = 401, description = "No verified claims"),
        (status = 403, description = "Caller is not an admin of this workspace"),
        (status = 404, description = "No global user with that email"),
        (status = 501, description = "No membership-admin surface configured"),
    ),
))]
#[cfg(feature = "pg")]
async fn add_member(
    State(state): State<AuthState>,
    claims: Option<Extension<JwtClaims>>,
    Path(slug): Path<String>,
    Json(body): Json<AddMemberRequest>,
) -> Result<StatusCode, ApiError> {
    require_workspace_admin(claims.as_deref(), &slug)?;
    let admin = state
        .membership_admin
        .as_ref()
        .ok_or(ApiError::NotConfigured)?;
    let now = (state.now)();
    match admin
        .add_member(&body.email, &slug, &body.role, now)
        .await?
    {
        true => Ok(StatusCode::NO_CONTENT),
        false => Err(ApiError::NotFound),
    }
}

/// `DELETE /auth/workspaces/{slug}/members/{email}` — remove a user from `slug`
/// (hq-platform-hardening.2). Same ws-admin gate as [`add_member`]. `404` when the user holds no
/// membership of `slug` (idempotent). After removal the user can no longer `/auth/switch` in.
#[cfg_attr(feature = "axum", utoipa::path(
    delete, path = "/auth/workspaces/{slug}/members/{email}", tag = "auth",
    params(
        ("slug" = String, Path, description = "The workspace to remove the member from"),
        ("email" = String, Path, description = "The member's login email"),
    ),
    responses(
        (status = 204, description = "Member removed"),
        (status = 401, description = "No verified claims"),
        (status = 403, description = "Caller is not an admin of this workspace"),
        (status = 404, description = "No such member of this workspace"),
        (status = 501, description = "No membership-admin surface configured"),
    ),
))]
#[cfg(feature = "pg")]
async fn remove_member(
    State(state): State<AuthState>,
    claims: Option<Extension<JwtClaims>>,
    Path((slug, email)): Path<(String, String)>,
) -> Result<StatusCode, ApiError> {
    require_workspace_admin(claims.as_deref(), &slug)?;
    let admin = state
        .membership_admin
        .as_ref()
        .ok_or(ApiError::NotConfigured)?;
    match admin.remove_member(&email, &slug).await? {
        true => Ok(StatusCode::NO_CONTENT),
        false => Err(ApiError::NotFound),
    }
}

/// `GET /auth/jwks` — the verifier's public RS256 keys as an RFC 7517 JWK Set. A client matches a
/// token's header `kid` to a key here and verifies the signature offline. Returns `200` with
/// `{"keys":[]}` when no keys are configured (a valid, empty set — simpler for clients than a 404).
#[cfg_attr(feature = "axum", utoipa::path(
    get, path = "/auth/jwks", tag = "auth",
    responses(
        (status = 200, description = "The verifier's public RS256 keys (RFC 7517 JWK Set; empty keys is valid)", body = JwkSet),
    ),
))]
async fn jwks(State(state): State<AuthState>) -> Json<JwkSet> {
    Json(state.jwks.as_ref().clone())
}

/// `POST /auth/users` — create a user (admin only). Requires a `users.write` (or `*`) scope in
/// the caller's verified claims; the password is argon2-hashed before storage.
#[cfg_attr(feature = "axum", utoipa::path(
    post, path = "/auth/users", tag = "auth",
    request_body = CreateUserRequest,
    responses(
        (status = 201, description = "Created — the new user (never any password material)", body = UserSummary),
        (status = 401, description = "No verified claims"),
        (status = 403, description = "Caller lacks the users.write scope"),
    ),
))]
#[cfg(feature = "pg")]
async fn create_user(
    State(state): State<AuthState>,
    claims: Option<Extension<JwtClaims>>,
    Json(body): Json<CreateUserRequest>,
) -> Result<(StatusCode, Json<UserSummary>), ApiError> {
    require_scope(claims.as_deref(), "users.write")?;
    let store = state.users.as_ref().ok_or(ApiError::NotConfigured)?;
    let hash = crate::password::hash_password(&body.password)?;
    let now = (state.now)();
    // The unique email doubles as the stable id source, so a re-create hits the unique index.
    let id = format!("user-{}", body.email);
    store
        .create_user(&id, &body.email, &hash, &body.scopes, now)
        .await?;
    Ok((
        StatusCode::CREATED,
        Json(UserSummary {
            sub: id,
            email: body.email,
            scopes: body.scopes,
            created_at: now as i64,
        }),
    ))
}

/// `GET /auth/users` — list users (admin only). Requires a `users.read` (or `*`) scope.
#[cfg_attr(feature = "axum", utoipa::path(
    get, path = "/auth/users", tag = "auth",
    responses(
        (status = 200, description = "Every user (no password material)", body = Vec<UserSummary>),
        (status = 401, description = "No verified claims"),
        (status = 403, description = "Caller lacks the users.read scope"),
    ),
))]
#[cfg(feature = "pg")]
async fn list_users(
    State(state): State<AuthState>,
    claims: Option<Extension<JwtClaims>>,
) -> Result<Json<Vec<UserSummary>>, ApiError> {
    require_scope(claims.as_deref(), "users.read")?;
    let store = state.users.as_ref().ok_or(ApiError::NotConfigured)?;
    Ok(Json(store.list_users().await?))
}

/// `POST /auth/roles` — create or replace a role (admin only). Requires `roles.write` (or `*`).
/// Every scope in the bundle is validated against the closed vocabulary first, so a typo is a
/// `400` rather than a silently dead grant.
#[cfg_attr(feature = "axum", utoipa::path(
    post, path = "/auth/roles", tag = "auth",
    request_body = CreateRoleRequest,
    responses(
        (status = 201, description = "Created or replaced — the role", body = RoleSummary),
        (status = 400, description = "A scope is outside the closed vocabulary"),
        (status = 401, description = "No verified claims"),
        (status = 403, description = "Caller lacks the roles.write scope"),
    ),
))]
#[cfg(feature = "pg")]
async fn create_role(
    State(state): State<AuthState>,
    claims: Option<Extension<JwtClaims>>,
    Json(body): Json<CreateRoleRequest>,
) -> Result<(StatusCode, Json<RoleSummary>), ApiError> {
    require_scope(claims.as_deref(), "roles.write")?;
    let store = state.roles.as_ref().ok_or(ApiError::NotConfigured)?;
    gt_rbac::validate_scopes(&body.scopes).map_err(|e| ApiError::BadScope(e.to_string()))?;
    let now = (state.now)();
    store.upsert_role(&body.name, &body.scopes, now).await?;
    Ok((
        StatusCode::CREATED,
        Json(RoleSummary {
            name: body.name,
            scopes: body.scopes,
            created_at: now as i64,
        }),
    ))
}

/// `GET /auth/roles` — list roles (admin only). Requires `roles.read` (or `*`).
#[cfg_attr(feature = "axum", utoipa::path(
    get, path = "/auth/roles", tag = "auth",
    responses(
        (status = 200, description = "Every role + its scope bundle", body = Vec<RoleSummary>),
        (status = 401, description = "No verified claims"),
        (status = 403, description = "Caller lacks the roles.read scope"),
    ),
))]
#[cfg(feature = "pg")]
async fn list_roles(
    State(state): State<AuthState>,
    claims: Option<Extension<JwtClaims>>,
) -> Result<Json<Vec<RoleSummary>>, ApiError> {
    require_scope(claims.as_deref(), "roles.read")?;
    let store = state.roles.as_ref().ok_or(ApiError::NotConfigured)?;
    Ok(Json(store.list_roles().await?))
}

/// `DELETE /auth/roles/{name}` — delete a role (admin only). Requires `roles.write` (or `*`).
/// Idempotent: deleting an absent role is `404`, deleting a present one is `204`.
#[cfg_attr(feature = "axum", utoipa::path(
    delete, path = "/auth/roles/{name}", tag = "auth",
    params(("name" = String, Path, description = "The role name to delete")),
    responses(
        (status = 204, description = "Deleted"),
        (status = 401, description = "No verified claims"),
        (status = 403, description = "Caller lacks the roles.write scope"),
        (status = 404, description = "No role with that name"),
    ),
))]
#[cfg(feature = "pg")]
async fn delete_role(
    State(state): State<AuthState>,
    claims: Option<Extension<JwtClaims>>,
    Path(name): Path<String>,
) -> Result<StatusCode, ApiError> {
    require_scope(claims.as_deref(), "roles.write")?;
    let store = state.roles.as_ref().ok_or(ApiError::NotConfigured)?;
    match store.delete_role(&name).await? {
        true => Ok(StatusCode::NO_CONTENT),
        false => Err(ApiError::NotFound),
    }
}

/// `POST /auth/users/{email}/roles` — set a user's assigned roles (admin only). Requires
/// `users.write` (or `*`): assignment is a write to the user record. `404` for an unknown email.
#[cfg_attr(feature = "axum", utoipa::path(
    post, path = "/auth/users/{email}/roles", tag = "auth",
    params(("email" = String, Path, description = "The user whose roles are being set")),
    request_body = AssignRolesRequest,
    responses(
        (status = 204, description = "Roles set (replaces the user's current set)"),
        (status = 401, description = "No verified claims"),
        (status = 403, description = "Caller lacks the users.write scope"),
        (status = 404, description = "No user with that email"),
    ),
))]
#[cfg(feature = "pg")]
async fn assign_roles(
    State(state): State<AuthState>,
    claims: Option<Extension<JwtClaims>>,
    Path(email): Path<String>,
    Json(body): Json<AssignRolesRequest>,
) -> Result<StatusCode, ApiError> {
    require_scope(claims.as_deref(), "users.write")?;
    let store = state.roles.as_ref().ok_or(ApiError::NotConfigured)?;
    let now = (state.now)();
    match store.assign_user_roles(&email, &body.roles, now).await? {
        true => Ok(StatusCode::NO_CONTENT),
        false => Err(ApiError::NotFound),
    }
}

/// `GET /auth/tokens` — list the CALLER's own Personal Access Tokens (hq-security-pat.2). Requires
/// `tokens.read` (or `*`). Self-only: keyed by the verified `sub`, so a caller never sees another
/// user's tokens, and never any secret material.
#[cfg_attr(feature = "axum", utoipa::path(
    get, path = "/auth/tokens", tag = "auth",
    responses(
        (status = 200, description = "The caller's own tokens (no secret material)", body = Vec<TokenSummary>),
        (status = 401, description = "No verified claims"),
        (status = 403, description = "Caller lacks the tokens.read scope"),
        (status = 501, description = "No PAT store configured"),
    ),
))]
#[cfg(feature = "pg")]
async fn list_tokens(
    State(state): State<AuthState>,
    claims: Option<Extension<JwtClaims>>,
) -> Result<Json<Vec<TokenSummary>>, ApiError> {
    let Extension(claims) = claims.ok_or(ApiError::Unauthenticated)?;
    require_scope(Some(&claims), "tokens.read")?;
    let store = state.pat.as_ref().ok_or(ApiError::NotConfigured)?;
    let tokens = store.list(&claims.sub).await?;
    Ok(Json(tokens.into_iter().map(TokenSummary::from).collect()))
}

/// `POST /auth/tokens` — mint a Personal Access Token for the caller (hq-security-pat.2). Requires
/// `tokens.write` (or `*`). The requested scopes are CLAMPED to the caller's own (no escalation),
/// and the plaintext token is returned **once** in the response — never recoverable afterwards.
#[cfg_attr(feature = "axum", utoipa::path(
    post, path = "/auth/tokens", tag = "auth",
    request_body = CreateTokenRequest,
    responses(
        (status = 201, description = "Created — the token plaintext (shown once) + its record", body = CreatedTokenResponse),
        (status = 401, description = "No verified claims"),
        (status = 403, description = "Caller lacks the tokens.write scope"),
        (status = 501, description = "No PAT store configured"),
    ),
))]
#[cfg(feature = "pg")]
async fn create_token(
    State(state): State<AuthState>,
    claims: Option<Extension<JwtClaims>>,
    Json(body): Json<CreateTokenRequest>,
) -> Result<(StatusCode, Json<CreatedTokenResponse>), ApiError> {
    let Extension(claims) = claims.ok_or(ApiError::Unauthenticated)?;
    require_scope(Some(&claims), "tokens.write")?;
    let store = state.pat.as_ref().ok_or(ApiError::NotConfigured)?;
    let now = (state.now)();
    let expires_at = body.expires_in_secs.map(|ttl| now.saturating_add(ttl));
    let requested = body.scopes.unwrap_or_default();
    // The minter's OWN scopes are the clamp ceiling — the store stores the intersection.
    let (token, record) = store
        .mint(
            &claims.sub,
            &claims.workspace,
            &body.name,
            &requested,
            &claims.scopes,
            now,
            expires_at,
        )
        .await?;
    Ok((
        StatusCode::CREATED,
        Json(CreatedTokenResponse {
            token: token.as_str().to_owned(),
            info: TokenSummary::from(record),
        }),
    ))
}

/// `DELETE /auth/tokens/{id}` — revoke one of the CALLER's own tokens (hq-security-pat.2). Requires
/// `tokens.write` (or `*`). Self-only: the `sub` predicate means a caller can only revoke their own
/// tokens. `204` on success, `404` for an unknown id or one that is not the caller's.
#[cfg_attr(feature = "axum", utoipa::path(
    delete, path = "/auth/tokens/{id}", tag = "auth",
    params(("id" = String, Path, description = "The token id to revoke")),
    responses(
        (status = 204, description = "Revoked"),
        (status = 401, description = "No verified claims"),
        (status = 403, description = "Caller lacks the tokens.write scope"),
        (status = 404, description = "No such token belonging to the caller"),
        (status = 501, description = "No PAT store configured"),
    ),
))]
#[cfg(feature = "pg")]
async fn revoke_token(
    State(state): State<AuthState>,
    claims: Option<Extension<JwtClaims>>,
    Path(id): Path<String>,
) -> Result<StatusCode, ApiError> {
    let Extension(claims) = claims.ok_or(ApiError::Unauthenticated)?;
    require_scope(Some(&claims), "tokens.write")?;
    let store = state.pat.as_ref().ok_or(ApiError::NotConfigured)?;
    match store.revoke(&claims.sub, &id).await? {
        true => Ok(StatusCode::NO_CONTENT),
        false => Err(ApiError::NotFound),
    }
}

/// Gate an admin endpoint: the caller must carry verified claims whose scopes include `needed`
/// or the `*` wildcard. No claims ⇒ `401`; claims without the scope ⇒ `403`.
#[cfg(feature = "pg")]
fn require_scope(claims: Option<&JwtClaims>, needed: &str) -> Result<(), ApiError> {
    let claims = claims.ok_or(ApiError::Unauthenticated)?;
    let ok = claims.scopes.iter().any(|s| s == "*" || s == needed);
    ok.then_some(()).ok_or(ApiError::Forbidden)
}

/// Gate a membership-admin endpoint on the caller being an ADMIN OF the target `workspace`
/// (hq-platform-hardening.2). Unlike [`require_scope`], a scope alone is not enough: the caller's
/// token must ALSO be active in `workspace`, so an admin of tenant A cannot manage tenant B. The
/// admin grant is `workspace.admin` or the `*` wildcard (the role the workspace seeds its creator
/// with). No claims ⇒ `401`; wrong workspace or missing grant ⇒ `403`.
#[cfg(feature = "pg")]
/// Whether the verified claims carry the deploy-wide super-admin grant (`*`). Unlike
/// [`require_system_admin`] this returns a bool (not a gate) and is feature-free, so the membership
/// handlers can WIDEN — not reject — for a system admin: a `*`-scoped caller lists and switches into
/// any workspace, while a normal user stays confined to their memberships.
fn is_system_admin(claims: &JwtClaims) -> bool {
    claims.scopes.iter().any(|s| s == "*")
}

fn require_workspace_admin(claims: Option<&JwtClaims>, workspace: &str) -> Result<(), ApiError> {
    let claims = claims.ok_or(ApiError::Unauthenticated)?;
    let is_admin = claims.workspace == workspace
        && claims
            .scopes
            .iter()
            .any(|s| s == "*" || s == "workspace.admin");
    is_admin.then_some(()).ok_or(ApiError::Forbidden)
}

/// Gate a SYSTEM-admin endpoint (hq-idp-db.4): the caller must carry verified claims whose scopes
/// include the `*` wildcard — the deploy-wide super-admin grant. UNLIKE [`require_workspace_admin`],
/// this is NOT tied to the token's active workspace: OAuth providers are GLOBAL infrastructure, so
/// only a system admin (never a mere workspace admin) may mutate them. No claims ⇒ `401`; a token
/// without the `*` grant ⇒ `403`.
#[cfg(feature = "oauth")]
fn require_system_admin(claims: Option<&JwtClaims>) -> Result<(), ApiError> {
    let claims = claims.ok_or(ApiError::Unauthenticated)?;
    claims
        .scopes
        .iter()
        .any(|s| s == "*")
        .then_some(())
        .ok_or(ApiError::Forbidden)
}

/// `POST /auth/providers` — register a GLOBAL OAuth/OIDC login provider (system admin only). The
/// `client_secret` is accepted in the body, sealed at rest, and NEVER returned: the `201` echoes a
/// [`ProviderView`], which omits it. A preset `kind` fills the endpoints/scopes from the baked
/// catalog when they are absent; the `generic` kind requires every endpoint. `400` for an unknown
/// kind or a `generic` provider missing an endpoint; `403` for a non-system-admin caller; `501`
/// when no provider store is configured.
#[cfg_attr(feature = "axum", utoipa::path(
    post, path = "/auth/providers", tag = "auth",
    request_body = CreateProviderRequest,
    responses(
        (status = 201, description = "Registered — the provider (never the client secret)", body = ProviderView),
        (status = 400, description = "Unknown kind, or a generic provider missing an endpoint"),
        (status = 401, description = "No verified claims"),
        (status = 403, description = "Caller is not a system admin"),
        (status = 501, description = "No provider store configured"),
    ),
))]
#[cfg(feature = "oauth")]
async fn create_provider(
    State(state): State<AuthState>,
    claims: Option<Extension<JwtClaims>>,
    Json(body): Json<CreateProviderRequest>,
) -> Result<(StatusCode, Json<ProviderView>), ApiError> {
    require_system_admin(claims.as_deref())?;
    let store = state.providers.as_ref().ok_or(ApiError::NotConfigured)?;
    let new = body.into_new_provider()?;
    let stored = store.create_provider(new).await?;
    Ok((StatusCode::CREATED, Json(stored.into())))
}

/// `PATCH /auth/providers/{id}` — partially update a provider (system admin only). Toggles
/// `enabled`, edits metadata/endpoints, and ROTATES the secret when `client_secret` is supplied
/// (left untouched when omitted). The `200` echoes the updated [`ProviderView`] — never the secret.
/// `403` for a non-system-admin caller; `404` for an unknown id; `501` with no store.
#[cfg_attr(feature = "axum", utoipa::path(
    patch, path = "/auth/providers/{id}", tag = "auth",
    params(("id" = String, Path, description = "The provider id to update")),
    request_body = PatchProviderRequest,
    responses(
        (status = 200, description = "Updated — the provider (never the client secret)", body = ProviderView),
        (status = 401, description = "No verified claims"),
        (status = 403, description = "Caller is not a system admin"),
        (status = 404, description = "No provider with that id"),
        (status = 501, description = "No provider store configured"),
    ),
))]
#[cfg(feature = "oauth")]
async fn patch_provider(
    State(state): State<AuthState>,
    claims: Option<Extension<JwtClaims>>,
    Path(id): Path<String>,
    Json(body): Json<PatchProviderRequest>,
) -> Result<Json<ProviderView>, ApiError> {
    require_system_admin(claims.as_deref())?;
    let store = state.providers.as_ref().ok_or(ApiError::NotConfigured)?;
    match store.patch_provider(&id, body.into_patch()).await? {
        Some(updated) => Ok(Json(updated.into())),
        None => Err(ApiError::NotFound),
    }
}

/// `DELETE /auth/providers/{id}` — remove a provider (system admin only). Idempotent: a present
/// provider is `204`, an absent one is `404`. `403` for a non-system-admin caller; `501` with no
/// store.
#[cfg_attr(feature = "axum", utoipa::path(
    delete, path = "/auth/providers/{id}", tag = "auth",
    params(("id" = String, Path, description = "The provider id to delete")),
    responses(
        (status = 204, description = "Deleted"),
        (status = 401, description = "No verified claims"),
        (status = 403, description = "Caller is not a system admin"),
        (status = 404, description = "No provider with that id"),
        (status = 501, description = "No provider store configured"),
    ),
))]
#[cfg(feature = "oauth")]
async fn delete_provider(
    State(state): State<AuthState>,
    claims: Option<Extension<JwtClaims>>,
    Path(id): Path<String>,
) -> Result<StatusCode, ApiError> {
    require_system_admin(claims.as_deref())?;
    let store = state.providers.as_ref().ok_or(ApiError::NotConfigured)?;
    match store.delete_provider(&id).await? {
        true => Ok(StatusCode::NO_CONTENT),
        false => Err(ApiError::NotFound),
    }
}

/// `GET /auth/providers` (PUBLIC, no auth) — the enabled login providers the FE renders buttons for
/// (hq-idp-db.3). Lists ONLY `enabled = true` providers and projects the secret-free
/// [`PublicProvider`] (id / kind / display_name / relative authorize_url) — never the client secret
/// OR the client id / endpoints. `501` when no provider store is configured.
#[cfg_attr(feature = "axum", utoipa::path(
    get, path = "/auth/providers", tag = "auth",
    responses(
        (status = 200, description = "The enabled login providers (secret-free; for the FE login buttons)", body = Vec<PublicProvider>),
        (status = 501, description = "No provider store configured"),
    ),
))]
#[cfg(feature = "oauth")]
async fn list_public_providers(
    State(state): State<AuthState>,
) -> Result<Json<Vec<PublicProvider>>, ApiError> {
    let store = state.providers.as_ref().ok_or(ApiError::NotConfigured)?;
    let providers = store
        .list_providers()
        .await?
        .into_iter()
        .filter(|p| p.enabled)
        .map(PublicProvider::from)
        .collect();
    Ok(Json(providers))
}

/// `GET /auth/providers/all` (system admin only, hq-idp-ui.1) — the FULL provider list, INCLUDING
/// `enabled = false`, projected secret-free as [`ProviderView`] (id / kind / display_name /
/// client_id / issuer / endpoints / scopes / enabled — never the client secret). UNLIKE the PUBLIC
/// `GET /auth/providers` (enabled-only, minimal [`PublicProvider`]), this is the surface the gt-web
/// admin page (hq-idp-ui.2) lists + edits from. `401` without claims; `403` for a non-system-admin;
/// `501` when no provider store is configured.
#[cfg_attr(feature = "axum", utoipa::path(
    get, path = "/auth/providers/all", tag = "auth",
    responses(
        (status = 200, description = "Every provider incl disabled (secret-free; for the admin page)", body = Vec<ProviderView>),
        (status = 401, description = "No verified claims"),
        (status = 403, description = "Caller is not a system admin"),
        (status = 501, description = "No provider store configured"),
    ),
))]
#[cfg(feature = "oauth")]
async fn list_all_providers(
    State(state): State<AuthState>,
    claims: Option<Extension<JwtClaims>>,
) -> Result<Json<Vec<ProviderView>>, ApiError> {
    require_system_admin(claims.as_deref())?;
    let store = state.providers.as_ref().ok_or(ApiError::NotConfigured)?;
    let providers = store
        .list_providers()
        .await?
        .into_iter()
        .map(ProviderView::from)
        .collect();
    Ok(Json(providers))
}

/// `GET /auth/providers/{id}` (system admin only, hq-idp-ui.1) — ONE provider by id, secret-free
/// ([`ProviderView`]), to pre-fill the edit form on the gt-web admin page (hq-idp-ui.2). Returns a
/// disabled provider too (unlike the public discovery list). `401` without claims; `403` for a
/// non-system-admin; `404` for an unknown id; `501` when no provider store is configured.
#[cfg_attr(feature = "axum", utoipa::path(
    get, path = "/auth/providers/{id}", tag = "auth",
    params(("id" = String, Path, description = "The provider id to read")),
    responses(
        (status = 200, description = "The provider (secret-free, incl disabled)", body = ProviderView),
        (status = 401, description = "No verified claims"),
        (status = 403, description = "Caller is not a system admin"),
        (status = 404, description = "No provider with that id"),
        (status = 501, description = "No provider store configured"),
    ),
))]
#[cfg(feature = "oauth")]
async fn get_provider(
    State(state): State<AuthState>,
    claims: Option<Extension<JwtClaims>>,
    Path(id): Path<String>,
) -> Result<Json<ProviderView>, ApiError> {
    require_system_admin(claims.as_deref())?;
    let store = state.providers.as_ref().ok_or(ApiError::NotConfigured)?;
    // No `get`-by-id on the [`ProviderStore`] port, so filter the full list (which already returns
    // disabled rows). The catalog is tiny deploy-global infrastructure — a linear scan is fine and
    // keeps the port (+ both test doubles) untouched.
    store
        .list_providers()
        .await?
        .into_iter()
        .find(|p| p.id == id)
        .map(|p| Json(ProviderView::from(p)))
        .ok_or(ApiError::NotFound)
}

/// Query params for `GET /auth/providers/{id}/authorize`. `cli_redirect` is present only for the
/// `gt login` browser flow (hq-gt-login-oauth.1): the local loopback URL the callback hands the
/// minted session back to (via a one-shot code). Absent ⇒ the ordinary web login, redirected to the
/// FE as before.
#[cfg(feature = "oauth")]
#[derive(Debug, Default, Deserialize)]
struct AuthorizeParams {
    #[serde(default)]
    cli_redirect: Option<String>,
}

/// The out-of-band `cli_redirect` sentinel (the classic OAuth OOB value): `gt login` sends this
/// instead of a loopback URL when it wants the callback to DISPLAY the one-shot code on a page for
/// the user to copy + paste, rather than 302 it to a local server (hq-gt-login-oauth.6).
#[cfg(feature = "oauth")]
const CLI_REDIRECT_OOB: &str = "urn:ietf:wg:oauth:2.0:oob";

/// True when `cli_redirect` is an accepted target: either the OOB paste sentinel, or a strict
/// loopback `http` URL (`http://127.0.0.1[:port]/…` / `http://localhost[:port]/…`). Anything else
/// is rejected so the authorize endpoint can never be turned into an open redirect.
#[cfg(feature = "oauth")]
fn is_allowed_cli_redirect(url: &str) -> bool {
    url == CLI_REDIRECT_OOB || is_loopback_redirect(url)
}

/// True when `url` is a strict loopback `http` URL — `http://127.0.0.1[:port]/…` or
/// `http://localhost[:port]/…`. Loopback is plain `http` (no TLS on 127.0.0.1), so any other scheme
/// is rejected too.
#[cfg(feature = "oauth")]
fn is_loopback_redirect(url: &str) -> bool {
    let Some(rest) = url.strip_prefix("http://") else {
        return false;
    };
    // The authority is everything up to the first '/'; strip an optional `:port` to get the host.
    let authority = rest.split('/').next().unwrap_or("");
    let host = authority.rsplit_once(':').map_or(authority, |(h, _)| h);
    host == "127.0.0.1" || host == "localhost"
}

/// `GET /auth/providers/{id}/authorize` (PUBLIC, no auth) — begin the authorization-code login
/// (hq-idp-db.3). Mints a fresh anti-CSRF `state` + PKCE pair, persists the pending handshake
/// (`state` → `code_verifier`, ~10 min TTL), and 302-redirects the browser to the IdP authorize URL
/// (`response_type=code`, client id, redirect_uri, scope, `state`, `code_challenge` S256). `404` for
/// an unknown/disabled id; `501` when the flow/state store is not configured.
#[cfg_attr(feature = "axum", utoipa::path(
    get, path = "/auth/providers/{id}/authorize", tag = "auth",
    params(
        ("id" = String, Path, description = "The enabled provider to begin login with"),
        ("cli_redirect" = Option<String>, Query, description = "gt login only: a http://127.0.0.1/localhost loopback URL to hand the session back to"),
    ),
    responses(
        (status = 302, description = "Redirect to the IdP authorize URL (carries state + PKCE challenge)"),
        (status = 400, description = "cli_redirect is not a loopback URL"),
        (status = 404, description = "No enabled provider with that id"),
        (status = 501, description = "OAuth authorize flow not configured"),
    ),
))]
#[cfg(feature = "oauth")]
async fn authorize(
    State(state): State<AuthState>,
    Path(id): Path<String>,
    axum::extract::Query(params): axum::extract::Query<AuthorizeParams>,
) -> Result<Response, ApiError> {
    let flow = state.authz_flow.as_ref().ok_or(ApiError::NotConfigured)?;
    let store = state.authz_state.as_ref().ok_or(ApiError::NotConfigured)?;
    // A CLI target, when present, must be the OOB paste sentinel or a 127.0.0.1/localhost loopback —
    // reject anything else BEFORE minting state, so authorize can never become an open redirect.
    if let Some(cli) = params.cli_redirect.as_deref() {
        if !is_allowed_cli_redirect(cli) {
            return Err(ApiError::BadCliRedirect);
        }
    }
    // Fresh anti-CSRF state + PKCE: the `state` is replayed by the IdP on the callback (binding the
    // round-trip to this browser); the `code_verifier` proves possession at the token exchange.
    let pkce = crate::new_pkce().map_err(ApiError::Auth)?;
    let csrf_state = csrf_token().map_err(ApiError::Auth)?;
    // Build the IdP URL BEFORE persisting, so an unknown/disabled id (404) leaves no orphan row.
    let url = flow
        .authorize_url(&id, &csrf_state, &pkce.challenge)
        .await?;
    let now = (state.now)();
    store
        .insert(NewAuthz {
            state: csrf_state,
            code_verifier: pkce.verifier,
            provider_id: id,
            redirect_uri: flow.redirect_uri().to_owned(),
            cli_redirect: params.cli_redirect,
            created_at: now,
            expires_at: now + AUTHZ_STATE_TTL_SECS,
        })
        .await?;
    // An explicit `302 Found` (not axum's `Redirect::to`, which is `303 See Other`) — the OAuth
    // authorize redirect is conventionally a 302, and the acceptance pins it.
    let mut headers = HeaderMap::new();
    headers.insert(
        header::LOCATION,
        HeaderValue::from_str(&url)
            .map_err(|_| ApiError::Auth(AuthError::Backend("invalid authorize URL".into())))?,
    );
    Ok((StatusCode::FOUND, headers).into_response())
}

/// `GET /auth/callback?code&state` (PUBLIC, no auth) — finish the authorization-code login
/// (hq-idp-db.3). Validates + ONE-SHOT consumes the `state` (a replayed/unknown/expired state is
/// `401`), resolves the provider, redeems the `code` through the PKCE exchange (replaying the stored
/// `code_verifier`), and issues the access/refresh pair. Hands the tokens to the FE: when a
/// `fe_redirect_url` is configured, 302 there with the access token in a URL fragment (plus the
/// httpOnly auth cookies); otherwise return the token JSON. `401` for a bad state or a rejected code.
#[cfg_attr(feature = "axum", utoipa::path(
    get, path = "/auth/callback", tag = "auth",
    params(
        ("code" = String, Query, description = "The authorization code returned by the IdP"),
        ("state" = String, Query, description = "The anti-CSRF state echoed back by the IdP"),
    ),
    responses(
        (status = 200, description = "Authenticated — token pair (when no FE redirect is configured)", body = TokenResponse),
        (status = 302, description = "Redirect to the FE with the tokens (fragment + httpOnly cookies)"),
        (status = 401, description = "Unknown/expired/replayed state, or a rejected code"),
        (status = 501, description = "OAuth callback flow not configured"),
    ),
))]
#[cfg(feature = "oauth")]
async fn callback(
    State(state): State<AuthState>,
    axum::extract::Query(params): axum::extract::Query<CallbackParams>,
) -> Result<Response, ApiError> {
    let flow = state.authz_flow.as_ref().ok_or(ApiError::NotConfigured)?;
    let store = state.authz_state.as_ref().ok_or(ApiError::NotConfigured)?;
    // One-shot consume: an unknown / already-used `state` is gone, so this is the anti-CSRF +
    // replay gate. A `401` (not `404`) — the caller must restart the flow.
    let pending = store
        .consume(&params.state)
        .await?
        .ok_or(ApiError::Unauthenticated)?;
    // Reject an expired pending row (the TTL elapsed between authorize and callback).
    if (state.now)() > pending.expires_at {
        return Err(ApiError::Unauthenticated);
    }
    // Redeem the code through the PKCE exchange (replaying the stored verifier) → VerifiedIdentity,
    // the same value the password path yields.
    let identity = flow
        .exchange(&pending.provider_id, &params.code, &pending.code_verifier)
        .await?;
    let tokens = issue_tokens(&state, identity.sub, identity.workspace, identity.scopes).await?;

    // CLI hand-off (hq-gt-login-oauth.2/.6): a `gt login` handshake carried a `cli_redirect`. The
    // token pair is too sensitive to expose, so park it under a fresh opaque one-shot code and hand
    // back ONLY that code — never the token. No auth cookies: they are for the same-origin web app.
    if let Some(cli_redirect) = pending.cli_redirect.as_deref() {
        let codes = state.cli_code.as_ref().ok_or(ApiError::NotConfigured)?;
        let code = csrf_token().map_err(ApiError::Auth)?;
        let now = (state.now)();
        codes
            .insert(crate::NewCliCode {
                code: code.clone(),
                access_token: tokens.access_token,
                refresh_token: tokens.refresh_token,
                token_type: tokens.token_type.to_owned(),
                expires_in: tokens.expires_in,
                created_at: now,
                expires_at: now + CLI_CODE_TTL_SECS,
            })
            .await?;

        if cli_redirect == CLI_REDIRECT_OOB {
            // OOB paste flow (hq-gt-login-oauth.6): render the code on a page for the user to copy
            // into the terminal. The CLI redeems it at `/auth/cli/exchange`.
            return Ok((
                StatusCode::OK,
                [(header::CONTENT_TYPE, "text/html; charset=utf-8")],
                cli_code_page(&code),
            )
                .into_response());
        }

        // Loopback flow (.2): append the code to the 127.0.0.1 URL and 302 there.
        let sep = if cli_redirect.contains('?') { '&' } else { '?' };
        let location = format!("{cli_redirect}{sep}code={}", url_encode(&code));
        let mut headers = HeaderMap::new();
        headers.insert(
            header::LOCATION,
            HeaderValue::from_str(&location).map_err(|_| {
                ApiError::Auth(AuthError::Backend("invalid CLI loopback URL".into()))
            })?,
        );
        return Ok((StatusCode::FOUND, headers).into_response());
    }

    let mut headers = set_token_cookies(&state, &tokens);
    match state.fe_redirect_url.as_deref() {
        // Browser handoff: 302 to the FE with the access token in a short-lived URL fragment (never
        // a query string — fragments are not sent to the server / logged), alongside the httpOnly
        // cookies the same-origin app reads. The FE strips the fragment after reading it.
        Some(fe) => {
            let location = format!(
                "{fe}#access_token={}&token_type=Bearer&expires_in={}",
                url_encode(&tokens.access_token),
                tokens.expires_in
            );
            headers.insert(
                header::LOCATION,
                HeaderValue::from_str(&location).map_err(|_| {
                    ApiError::Auth(AuthError::Backend("invalid FE redirect URL".into()))
                })?,
            );
            Ok((StatusCode::FOUND, headers).into_response())
        }
        // No FE configured: return the token JSON directly (non-browser client / test).
        None => Ok((headers, Json(tokens)).into_response()),
    }
}

/// `GET /auth/callback` query parameters: the IdP-returned authorization `code` and the `state`
/// echoed back from `/authorize`.
#[cfg(feature = "oauth")]
#[derive(Debug, Deserialize)]
pub struct CallbackParams {
    /// The authorization code the IdP returned to the redirect URI.
    pub code: String,
    /// The anti-CSRF state echoed back — validated + consumed against the pending store.
    pub state: String,
}

/// `POST /auth/cli/exchange` body — the one-shot hand-off `code` the CLI loopback received.
#[cfg(feature = "oauth")]
#[derive(Debug, Deserialize)]
#[cfg_attr(feature = "axum", derive(utoipa::ToSchema))]
pub struct CliExchangeRequest {
    /// The opaque one-shot code 302'd to the loopback by `/auth/callback`.
    pub code: String,
}

/// `POST /auth/cli/exchange` (PUBLIC, no auth) — redeem a one-shot `gt login` hand-off code
/// (hq-gt-login-oauth.2) for the token pair the callback parked. ONE-SHOT: an unknown / replayed /
/// expired code is `401`; `501` when the code store is not configured.
#[cfg_attr(feature = "axum", utoipa::path(
    post, path = "/auth/cli/exchange", tag = "auth",
    request_body = CliExchangeRequest,
    responses(
        (status = 200, description = "Redeemed — the access + refresh token pair", body = TokenResponse),
        (status = 401, description = "Unknown / already-redeemed / expired code"),
        (status = 501, description = "CLI hand-off code store not configured"),
    ),
))]
#[cfg(feature = "oauth")]
async fn cli_exchange(
    State(state): State<AuthState>,
    Json(body): Json<CliExchangeRequest>,
) -> Result<Json<TokenResponse>, ApiError> {
    let codes = state.cli_code.as_ref().ok_or(ApiError::NotConfigured)?;
    // One-shot consume: an unknown / already-redeemed code is gone, so a captured loopback URL can
    // never be replayed. A `401` — the caller must restart the login.
    let pending = codes
        .consume(&body.code)
        .await?
        .ok_or(ApiError::Unauthenticated)?;
    if (state.now)() > pending.expires_at {
        return Err(ApiError::Unauthenticated);
    }
    Ok(Json(TokenResponse {
        access_token: pending.access_token,
        refresh_token: pending.refresh_token,
        // The stored type is always "Bearer"; TokenResponse carries the static str.
        token_type: "Bearer",
        expires_in: pending.expires_in,
    }))
}

/// The TTL of a pending `/authorize` handshake — 10 minutes is ample for a human IdP round-trip
/// while keeping a leaked/abandoned `state` short-lived (hq-idp-db.3).
#[cfg(feature = "oauth")]
const AUTHZ_STATE_TTL_SECS: u64 = 600;

/// One-shot CLI hand-off code TTL (hq-gt-login-oauth.2/.6): the window between the callback parking
/// the token pair and the CLI redeeming it at `/auth/cli/exchange`. 5 minutes — long enough for the
/// OOB paste flow (a human copies the code from the page into the terminal), still short for a
/// one-shot credential.
#[cfg(feature = "oauth")]
const CLI_CODE_TTL_SECS: u64 = 300;

/// The HTML page the OOB callback renders (hq-gt-login-oauth.6): shows the one-shot `code` for the
/// user to copy into `gt login`. The code is a base64url token (URL-safe charset), so it needs no
/// HTML escaping.
#[cfg(feature = "oauth")]
fn cli_code_page(code: &str) -> String {
    format!(
        "<!doctype html><html><head><meta charset=\"utf-8\"><title>gt login</title></head>\
         <body style=\"font-family:system-ui,sans-serif;max-width:34rem;margin:4rem auto;padding:0 1rem\">\
         <h2>Almost there</h2>\
         <p>Copy this code and paste it back into your <code>gt login</code> terminal:</p>\
         <pre style=\"font-size:1.25rem;background:#f4f4f5;padding:1rem;border-radius:.5rem;\
         user-select:all;word-break:break-all\">{code}</pre>\
         <p style=\"color:#71717a\">This code expires in a few minutes and can be used once.</p>\
         </body></html>"
    )
}

/// A fresh anti-CSRF `state`: 32 CSPRNG bytes, base64url (no padding). High-entropy + opaque, so it
/// is both unguessable (CSRF defence) and a safe primary key for the pending-state row.
#[cfg(feature = "oauth")]
fn csrf_token() -> Result<String, AuthError> {
    use base64::Engine as _;
    let mut raw = [0u8; 32];
    getrandom::getrandom(&mut raw)
        .map_err(|e| AuthError::Backend(format!("OS CSPRNG for CSRF state: {e}")))?;
    Ok(base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(raw))
}

/// Percent-encode a value for a URL fragment/query (the auth callback's FE handoff). Same minimal
/// RFC 3986 unreserved-keep encoder the `oauth` adapter uses for the authorize URL.
#[cfg(feature = "oauth")]
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

#[cfg(feature = "oauth")]
impl CreateProviderRequest {
    /// Turn the request into a [`NewProvider`], filling a preset kind's baked endpoints/scopes when
    /// they are absent and requiring every endpoint for the `generic` kind. An unknown kind or a
    /// `generic` provider missing an endpoint is [`ApiError::BadProvider`] (`400`).
    fn into_new_provider(self) -> Result<NewProvider, ApiError> {
        let kind = OauthProviderKind::parse(&self.kind)
            .map_err(|e| ApiError::BadProvider(e.to_string()))?;
        let preset = crate::preset_for(kind);
        // Fill from the preset when the field is absent; the `generic` kind has no preset, so an
        // absent endpoint there is a caller error rather than a silent empty string.
        let need = |field: Option<String>, from_preset: Option<&str>, name: &str| {
            field
                .or_else(|| from_preset.map(str::to_owned))
                .ok_or_else(|| {
                    ApiError::BadProvider(format!("missing `{name}` for a generic provider"))
                })
        };
        let display_name = self
            .display_name
            .or_else(|| preset.as_ref().map(|p| p.display_name.to_owned()))
            .unwrap_or_else(|| self.id.clone());
        Ok(NewProvider {
            id: self.id,
            kind,
            display_name,
            client_id: self.client_id,
            client_secret: self.client_secret,
            issuer: need(self.issuer, preset.as_ref().map(|p| p.issuer), "issuer")?,
            authorize_endpoint: need(
                self.authorize_endpoint,
                preset.as_ref().map(|p| p.authorize_endpoint),
                "authorize_endpoint",
            )?,
            token_endpoint: need(
                self.token_endpoint,
                preset.as_ref().map(|p| p.token_endpoint),
                "token_endpoint",
            )?,
            userinfo_endpoint: need(
                self.userinfo_endpoint,
                preset.as_ref().map(|p| p.userinfo_endpoint),
                "userinfo_endpoint",
            )?,
            scopes: need(
                self.scopes,
                preset.as_ref().map(|p| p.default_scopes),
                "scopes",
            )?,
            enabled: self.enabled,
            workspace_id: self.workspace_id,
        })
    }
}

#[cfg(feature = "oauth")]
impl PatchProviderRequest {
    /// Project the request onto the repo's [`PatchProvider`] — a field-for-field move, the secret
    /// carried through write-only.
    fn into_patch(self) -> PatchProvider {
        PatchProvider {
            display_name: self.display_name,
            client_id: self.client_id,
            client_secret: self.client_secret,
            issuer: self.issuer,
            authorize_endpoint: self.authorize_endpoint,
            token_endpoint: self.token_endpoint,
            userinfo_endpoint: self.userinfo_endpoint,
            scopes: self.scopes,
            enabled: self.enabled,
            workspace_id: self.workspace_id,
        }
    }
}

/// Mint an access + refresh token pair for a freshly verified identity. Async: the refresh store
/// is the I/O-capable [`AsyncRefreshStore`] port, so issuing the durable token is awaited.
async fn issue_tokens(
    state: &AuthState,
    sub: String,
    workspace: String,
    scopes: Vec<String>,
) -> Result<TokenResponse, ApiError> {
    let now = (state.now)();
    let identity = VerifiedIdentity {
        sub: sub.clone(),
        workspace: workspace.clone(),
        scopes: scopes.clone(),
    };
    let claims = identity.into_claims(now + state.access_ttl, now);
    let access_token = state.minter.mint(&claims)?;
    let (refresh_token, _record) = state
        .refresh
        .issue(&sub, &workspace, &scopes, now, now + state.refresh_ttl)
        .await?;
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
        build_cookie(
            state,
            ACCESS_COOKIE,
            &tokens.access_token,
            "/",
            state.access_ttl as i64,
        ),
    );
    headers.append(
        header::SET_COOKIE,
        build_cookie(
            state,
            REFRESH_COOKIE,
            &tokens.refresh_token,
            "/auth",
            state.refresh_ttl as i64,
        ),
    );
    headers
}

/// The `Set-Cookie` header pair that expires both auth cookies (`Max-Age=0`, empty value) on
/// logout — same name/path as [`set_token_cookies`], which is what makes the browser drop them.
fn clear_token_cookies(state: &AuthState) -> HeaderMap {
    let mut headers = HeaderMap::new();
    headers.append(
        header::SET_COOKIE,
        build_cookie(state, ACCESS_COOKIE, "", "/", 0),
    );
    headers.append(
        header::SET_COOKIE,
        build_cookie(state, REFRESH_COOKIE, "", "/auth", 0),
    );
    headers
}

/// Build one `Set-Cookie` value. `value` is a JWT or opaque token (ASCII, no cookie-special
/// chars), so it needs no escaping.
fn build_cookie(
    state: &AuthState,
    name: &str,
    value: &str,
    path: &str,
    max_age: i64,
) -> HeaderValue {
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
            // An unknown/disabled `provider_id` is a client error (wrong/retired login button),
            // not a server fault: a clear `404`, distinct from the `501` "oauth not wired at all".
            AuthError::UnknownProvider(_) => StatusCode::NOT_FOUND,
            AuthError::HashFailure(_) | AuthError::SigningFailure(_) | AuthError::Backend(_) => {
                StatusCode::INTERNAL_SERVER_ERROR
            }
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
    /// A `POST /auth/login` selected the OAuth/OIDC path but the deploy configured no
    /// [`oauth_login`](AuthState::oauth_login) provider — `501` (hq-platform-hardening.3).
    OauthNotConfigured,
    /// Verified caller, but the claims lack the required scope — `403` (`hq-web-extras.5`).
    #[cfg(any(feature = "pg", feature = "oauth"))]
    Forbidden,
    /// The endpoint needs a backing store the deploy did not configure — `501` (`hq-web-extras.5`).
    #[cfg(any(feature = "pg", feature = "oauth"))]
    NotConfigured,
    /// A role's scope failed the closed-vocabulary check — `400` (hq-rbac.4).
    #[cfg(feature = "pg")]
    BadScope(String),
    /// A Personal Access Token store fault on the self-service surface — `500` for a backend
    /// outage, `401` for a lifecycle verdict (hq-security-pat.2).
    #[cfg(feature = "pg")]
    Pat(PatError),
    /// The addressed role/user/provider does not exist — `404` (hq-rbac.4 / hq-idp-db.4).
    #[cfg(any(feature = "pg", feature = "oauth"))]
    NotFound,
    /// A provider registration was malformed — unknown kind, or a generic provider missing an
    /// endpoint — `400` (hq-idp-db.4).
    #[cfg(feature = "oauth")]
    BadProvider(String),
    /// A `gt login` `cli_redirect` that is not a `127.0.0.1`/`localhost` loopback URL — `400`
    /// (hq-gt-login-oauth.1). Refusing a non-loopback target is what stops the authorize endpoint
    /// from becoming an open redirect.
    #[cfg(feature = "oauth")]
    BadCliRedirect,
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

#[cfg(feature = "pg")]
impl From<PatError> for ApiError {
    fn from(e: PatError) -> Self {
        ApiError::Pat(e)
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
            ApiError::OauthNotConfigured => (
                StatusCode::NOT_IMPLEMENTED,
                "oauth/oidc login is not configured",
            )
                .into_response(),
            #[cfg(any(feature = "pg", feature = "oauth"))]
            ApiError::Forbidden => (StatusCode::FORBIDDEN, "insufficient scope").into_response(),
            #[cfg(any(feature = "pg", feature = "oauth"))]
            ApiError::NotConfigured => (
                StatusCode::NOT_IMPLEMENTED,
                "user administration is not configured",
            )
                .into_response(),
            #[cfg(feature = "pg")]
            ApiError::BadScope(msg) => (StatusCode::BAD_REQUEST, msg).into_response(),
            #[cfg(feature = "pg")]
            ApiError::Pat(e) => match e {
                // A backend outage is a 500 (retryable), not a token verdict; every lifecycle
                // verdict on this surface (it should only ever see Backend) collapses to a 401.
                PatError::Backend(msg) => (StatusCode::INTERNAL_SERVER_ERROR, msg).into_response(),
                other => (StatusCode::UNAUTHORIZED, other.to_string()).into_response(),
            },
            #[cfg(any(feature = "pg", feature = "oauth"))]
            ApiError::NotFound => (StatusCode::NOT_FOUND, "not found").into_response(),
            #[cfg(feature = "oauth")]
            ApiError::BadProvider(msg) => (StatusCode::BAD_REQUEST, msg).into_response(),
            #[cfg(feature = "oauth")]
            ApiError::BadCliRedirect => (
                StatusCode::BAD_REQUEST,
                "cli_redirect must be a http://127.0.0.1 or http://localhost loopback URL",
            )
                .into_response(),
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

    /// The fused `/auth/*` OpenAPI must carry the absolute auth paths so the FE codegen
    /// (`hq-web-extras.10`) sees them — login surface always, admin/RBAC routes with `pg`.
    #[test]
    fn auth_openapi_lists_every_absolute_route() {
        let doc = auth_openapi();
        let paths: Vec<&str> = doc.paths.paths.keys().map(String::as_str).collect();
        for expected in [
            "/auth/login",
            "/auth/refresh",
            "/auth/logout",
            "/auth/me",
            "/auth/jwks",
        ] {
            assert!(paths.contains(&expected), "missing {expected} in {paths:?}");
        }
        #[cfg(feature = "pg")]
        for expected in [
            "/auth/users",
            "/auth/roles",
            "/auth/roles/{name}",
            "/auth/users/{email}/roles",
            "/auth/workspaces",
            "/auth/switch",
            "/auth/workspaces/{slug}/members",
            "/auth/workspaces/{slug}/members/{email}",
        ] {
            assert!(paths.contains(&expected), "missing {expected} in {paths:?}");
        }
    }

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

    /// In-memory [`RoleStore`] double recording upserts/deletes/assignments for the router tests.
    #[derive(Default)]
    struct MemRoles {
        roles: std::sync::Mutex<Vec<RoleSummary>>,
        assigned: std::sync::Mutex<Vec<(String, Vec<String>)>>,
        /// Emails the store "knows"; an assignment to anything else returns `false` (→ 404).
        known_users: Vec<String>,
    }

    #[async_trait]
    impl RoleStore for MemRoles {
        async fn upsert_role(
            &self,
            name: &str,
            scopes: &[String],
            now: u64,
        ) -> Result<(), AuthError> {
            let mut roles = self.roles.lock().unwrap();
            roles.retain(|r| r.name != name);
            roles.push(RoleSummary {
                name: name.into(),
                scopes: scopes.to_vec(),
                created_at: now as i64,
            });
            Ok(())
        }
        async fn list_roles(&self) -> Result<Vec<RoleSummary>, AuthError> {
            Ok(self
                .roles
                .lock()
                .unwrap()
                .iter()
                .map(|r| RoleSummary {
                    name: r.name.clone(),
                    scopes: r.scopes.clone(),
                    created_at: r.created_at,
                })
                .collect())
        }
        async fn delete_role(&self, name: &str) -> Result<bool, AuthError> {
            let mut roles = self.roles.lock().unwrap();
            let before = roles.len();
            roles.retain(|r| r.name != name);
            Ok(roles.len() != before)
        }
        async fn assign_user_roles(
            &self,
            email: &str,
            roles: &[String],
            _now: u64,
        ) -> Result<bool, AuthError> {
            if !self.known_users.iter().any(|e| e == email) {
                return Ok(false);
            }
            self.assigned
                .lock()
                .unwrap()
                .push((email.into(), roles.to_vec()));
            Ok(true)
        }
    }

    /// Build state whose role store is the given [`MemRoles`] double; everything else as [`state`].
    fn state_with_roles(roles: Arc<MemRoles>) -> AuthState {
        AuthState {
            roles: Some(roles),
            ..state()
        }
    }

    /// A request to `path` with `method`/`json` body and the caller's scopes injected as verified
    /// claims (as the auth middleware would). `None` scopes ⇒ no claims (unauthenticated).
    async fn admin_request(
        app: &Router,
        method: &str,
        path: &str,
        scopes: Option<&[&str]>,
        json: Option<&str>,
    ) -> StatusCode {
        let mut builder = Request::builder().method(method).uri(path);
        if json.is_some() {
            builder = builder.header("content-type", "application/json");
        }
        let mut req = builder
            .body(
                json.map(|j| Body::from(j.to_owned()))
                    .unwrap_or_else(Body::empty),
            )
            .unwrap();
        if let Some(scopes) = scopes {
            req.extensions_mut().insert(JwtClaims {
                sub: "admin".into(),
                workspace: "acme".into(),
                scopes: scopes.iter().map(|s| s.to_string()).collect(),
                exp: 2_000_000_000,
                nbf: None,
                iat: 0,
            });
        }
        app.clone().oneshot(req).await.unwrap().status()
    }

    #[tokio::test]
    async fn role_create_is_scope_gated_and_validates_the_vocabulary() {
        let roles = Arc::new(MemRoles::default());
        let app = auth_router(state_with_roles(roles.clone()));
        let body = r#"{"name":"reviewer","scopes":["merge.read","merge.submit"]}"#;

        // No claims → 401; wrong scope → 403; right scope → 201.
        assert_eq!(
            admin_request(&app, "POST", "/auth/roles", None, Some(body)).await,
            StatusCode::UNAUTHORIZED
        );
        assert_eq!(
            admin_request(
                &app,
                "POST",
                "/auth/roles",
                Some(&["roles.read"]),
                Some(body)
            )
            .await,
            StatusCode::FORBIDDEN
        );
        assert_eq!(
            admin_request(
                &app,
                "POST",
                "/auth/roles",
                Some(&["roles.write"]),
                Some(body)
            )
            .await,
            StatusCode::CREATED
        );
        assert_eq!(roles.roles.lock().unwrap().len(), 1);

        // A scope outside the closed vocabulary is a 400, never persisted.
        let bad = r#"{"name":"oops","scopes":["merge.frobnicate"]}"#;
        assert_eq!(
            admin_request(
                &app,
                "POST",
                "/auth/roles",
                Some(&["roles.write"]),
                Some(bad)
            )
            .await,
            StatusCode::BAD_REQUEST
        );
        assert_eq!(
            roles.roles.lock().unwrap().len(),
            1,
            "bad scope did not persist"
        );

        // The `*` wildcard caller passes the gate too.
        let body2 = r#"{"name":"reader","scopes":["issues.read"]}"#;
        assert_eq!(
            admin_request(&app, "POST", "/auth/roles", Some(&["*"]), Some(body2)).await,
            StatusCode::CREATED
        );
    }

    #[tokio::test]
    async fn role_delete_is_404_when_absent_204_when_present() {
        let roles = Arc::new(MemRoles::default());
        roles.upsert_role("ghost-check", &[], 0).await.unwrap();
        let app = auth_router(state_with_roles(roles.clone()));
        assert_eq!(
            admin_request(
                &app,
                "DELETE",
                "/auth/roles/ghost-check",
                Some(&["roles.write"]),
                None
            )
            .await,
            StatusCode::NO_CONTENT
        );
        assert_eq!(
            admin_request(
                &app,
                "DELETE",
                "/auth/roles/ghost-check",
                Some(&["roles.write"]),
                None
            )
            .await,
            StatusCode::NOT_FOUND
        );
    }

    #[tokio::test]
    async fn assigning_roles_needs_users_write_and_404s_unknown_user() {
        let roles = Arc::new(MemRoles {
            known_users: vec!["alice@acme.test".into()],
            ..Default::default()
        });
        let app = auth_router(state_with_roles(roles.clone()));
        let body = r#"{"roles":["reviewer"]}"#;

        // Gated on users.write (assignment is a write to the user record).
        assert_eq!(
            admin_request(
                &app,
                "POST",
                "/auth/users/alice@acme.test/roles",
                Some(&["roles.write"]),
                Some(body)
            )
            .await,
            StatusCode::FORBIDDEN
        );
        assert_eq!(
            admin_request(
                &app,
                "POST",
                "/auth/users/alice@acme.test/roles",
                Some(&["users.write"]),
                Some(body)
            )
            .await,
            StatusCode::NO_CONTENT
        );
        // Unknown user → 404.
        assert_eq!(
            admin_request(
                &app,
                "POST",
                "/auth/users/ghost@acme.test/roles",
                Some(&["users.write"]),
                Some(body)
            )
            .await,
            StatusCode::NOT_FOUND
        );
        assert_eq!(
            roles.assigned.lock().unwrap().as_slice(),
            &[("alice@acme.test".to_string(), vec!["reviewer".to_string()])]
        );
    }

    #[tokio::test]
    async fn role_endpoints_501_when_no_store_configured() {
        // Default state() has roles: None → the surface reports "not configured".
        let app = auth_router(state());
        assert_eq!(
            admin_request(&app, "GET", "/auth/roles", Some(&["roles.read"]), None).await,
            StatusCode::NOT_IMPLEMENTED
        );
    }

    // --- Personal Access Tokens self-service surface (hq-security-pat.2) -------------------------

    /// An in-memory [`PatAdmin`] double: a `Mutex<Vec<PatRecord>>` keyed by the secret it minted, so
    /// the handler tests run without Postgres. Mint clamps via the production [`clamp_scopes`].
    #[derive(Default)]
    struct MemPat {
        rows: std::sync::Mutex<Vec<(crate::PatToken, PatRecord)>>,
    }

    #[async_trait]
    impl PatAdmin for MemPat {
        async fn mint(
            &self,
            sub: &str,
            workspace: &str,
            name: &str,
            requested: &[String],
            granted: &[String],
            now: u64,
            expires_at: Option<u64>,
        ) -> Result<(crate::PatToken, PatRecord), PatError> {
            let rec = PatRecord {
                id: crate::PatId::generate(),
                sub: sub.into(),
                workspace: workspace.into(),
                name: name.into(),
                scopes: crate::clamp_scopes(requested, granted),
                created_at: now,
                expires_at,
                last_used_at: None,
                status: crate::PatStatus::Active,
            };
            let token = crate::PatToken::generate();
            self.rows.lock().unwrap().push((token.clone(), rec.clone()));
            Ok((token, rec))
        }

        async fn list(&self, sub: &str) -> Result<Vec<PatRecord>, PatError> {
            Ok(self
                .rows
                .lock()
                .unwrap()
                .iter()
                .filter(|(_, r)| r.sub == sub)
                .map(|(_, r)| r.clone())
                .collect())
        }

        async fn revoke(&self, sub: &str, id: &str) -> Result<bool, PatError> {
            let mut rows = self.rows.lock().unwrap();
            for (_, r) in rows.iter_mut() {
                if r.sub == sub && r.id.as_str() == id && r.status == crate::PatStatus::Active {
                    r.status = crate::PatStatus::Revoked;
                    return Ok(true);
                }
            }
            Ok(false)
        }
    }

    fn state_with_pat(pat: Arc<MemPat>) -> AuthState {
        AuthState {
            pat: Some(pat),
            ..state()
        }
    }

    /// Like [`admin_request`] but with a caller-chosen `sub`, so the self-only behaviour (one user
    /// cannot see/revoke another's tokens) can be exercised. Returns status + body.
    async fn tokens_request(
        app: &Router,
        method: &str,
        path: &str,
        sub: &str,
        scopes: Option<&[&str]>,
        json: Option<&str>,
    ) -> (StatusCode, String) {
        let mut builder = Request::builder().method(method).uri(path);
        if json.is_some() {
            builder = builder.header("content-type", "application/json");
        }
        let mut req = builder
            .body(
                json.map(|j| Body::from(j.to_owned()))
                    .unwrap_or_else(Body::empty),
            )
            .unwrap();
        if let Some(scopes) = scopes {
            req.extensions_mut().insert(JwtClaims {
                sub: sub.into(),
                workspace: "acme".into(),
                scopes: scopes.iter().map(|s| s.to_string()).collect(),
                exp: 2_000_000_000,
                nbf: None,
                iat: 0,
            });
        }
        let resp = app.clone().oneshot(req).await.unwrap();
        let status = resp.status();
        let bytes = axum::body::to_bytes(resp.into_body(), usize::MAX)
            .await
            .unwrap();
        (status, String::from_utf8(bytes.to_vec()).unwrap())
    }

    #[tokio::test]
    async fn tokens_surface_is_scope_gated_and_501_without_a_store() {
        // No store configured → 501 even with the right scope.
        let app = auth_router(state());
        let (st, _) = tokens_request(
            &app,
            "GET",
            "/auth/tokens",
            "alice",
            Some(&["tokens.read"]),
            None,
        )
        .await;
        assert_eq!(st, StatusCode::NOT_IMPLEMENTED);

        let app = auth_router(state_with_pat(Arc::new(MemPat::default())));
        // No claims → 401.
        let (st, _) = tokens_request(&app, "GET", "/auth/tokens", "alice", None, None).await;
        assert_eq!(st, StatusCode::UNAUTHORIZED);
        // Wrong scope → 403 (read needs tokens.read, write needs tokens.write).
        let (st, _) = tokens_request(
            &app,
            "GET",
            "/auth/tokens",
            "alice",
            Some(&["issues.read"]),
            None,
        )
        .await;
        assert_eq!(st, StatusCode::FORBIDDEN);
        let body = r#"{"name":"ci"}"#;
        let (st, _) = tokens_request(
            &app,
            "POST",
            "/auth/tokens",
            "alice",
            Some(&["tokens.read"]),
            Some(body),
        )
        .await;
        assert_eq!(
            st,
            StatusCode::FORBIDDEN,
            "minting needs tokens.write, not tokens.read"
        );
    }

    #[tokio::test]
    async fn mint_clamps_returns_plaintext_once_then_list_and_revoke_are_self_only() {
        let pat = Arc::new(MemPat::default());
        let app = auth_router(state_with_pat(pat.clone()));

        // Alice mints asking for a scope she does NOT hold → it is clamped away; the plaintext
        // token comes back ONCE in the body.
        let body = r#"{"name":"ci-deploy","scopes":["tokens.read","issues.write"]}"#;
        let (st, created) = tokens_request(
            &app,
            "POST",
            "/auth/tokens",
            "alice",
            Some(&["tokens.write", "tokens.read"]),
            Some(body),
        )
        .await;
        assert_eq!(st, StatusCode::CREATED);
        assert!(
            created.contains("gtpat_"),
            "the plaintext token is returned once: {created}"
        );
        let v: serde_json::Value = serde_json::from_str(&created).unwrap();
        let id = v["info"]["id"].as_str().unwrap().to_string();
        // issues.write was clamped out (alice doesn't hold it); tokens.read survives.
        let scopes = v["info"]["scopes"].as_array().unwrap();
        assert_eq!(scopes.len(), 1);
        assert_eq!(scopes[0], "tokens.read");

        // Alice lists → sees her token.
        let (st, list) = tokens_request(
            &app,
            "GET",
            "/auth/tokens",
            "alice",
            Some(&["tokens.read"]),
            None,
        )
        .await;
        assert_eq!(st, StatusCode::OK);
        assert!(list.contains(&id), "alice sees her own token");

        // Bob lists → sees NOTHING of alice's (self-only).
        let (st, bob_list) = tokens_request(
            &app,
            "GET",
            "/auth/tokens",
            "bob",
            Some(&["tokens.read"]),
            None,
        )
        .await;
        assert_eq!(st, StatusCode::OK);
        assert!(!bob_list.contains(&id), "bob cannot see alice's tokens");

        // Bob cannot revoke alice's token by id (self-only → 404).
        let path = format!("/auth/tokens/{id}");
        let (st, _) =
            tokens_request(&app, "DELETE", &path, "bob", Some(&["tokens.write"]), None).await;
        assert_eq!(st, StatusCode::NOT_FOUND);

        // Alice revokes it → 204; a second revoke is an idempotent 404.
        let (st, _) = tokens_request(
            &app,
            "DELETE",
            &path,
            "alice",
            Some(&["tokens.write"]),
            None,
        )
        .await;
        assert_eq!(st, StatusCode::NO_CONTENT);
        let (st, _) = tokens_request(
            &app,
            "DELETE",
            &path,
            "alice",
            Some(&["tokens.write"]),
            None,
        )
        .await;
        assert_eq!(st, StatusCode::NOT_FOUND);
    }

    fn state() -> AuthState {
        AuthState {
            login: Arc::new(OneUser),
            oauth_login: None,
            minter: Arc::new(JwtMinter::from_rsa_pem(PRIV_PEM).unwrap().with_kid("k1")),
            refresh: Arc::new(InMemoryRefreshStore::new()),
            access_ttl: 900,
            refresh_ttl: 1_209_600,
            now: Arc::new(|| 1_000_000_000),
            cookie_secure: true,
            cookie_same_site: SameSite::Lax,
            users: None,
            roles: None,
            pat: None,
            memberships: None,
            membership_admin: None,
            #[cfg(feature = "oauth")]
            providers: None,
            #[cfg(feature = "oauth")]
            authz_flow: None,
            #[cfg(feature = "oauth")]
            authz_state: None,
            #[cfg(feature = "oauth")]
            cli_code: None,
            #[cfg(feature = "oauth")]
            fe_redirect_url: None,
            // Publish the public half of the same "k1" key the minter signs with.
            jwks: Arc::new(
                JwtAuthenticator::from_kid_pems([("k1", PUB_PEM)])
                    .unwrap()
                    .jwk_set(),
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
        let bytes = axum::body::to_bytes(resp.into_body(), usize::MAX)
            .await
            .unwrap();
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
        let bytes = axum::body::to_bytes(resp.into_body(), usize::MAX)
            .await
            .unwrap();
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
    async fn login_with_no_recognizable_provider_fields_is_401() {
        // A body that names neither a password nor a code/id_token is a rejected (empty) login,
        // never a silent default (hq-platform-hardening.3).
        let app = auth_router(state());
        let (status, _) = post(&app, "/auth/login", r#"{}"#).await;
        assert_eq!(status, StatusCode::UNAUTHORIZED);
    }

    #[tokio::test]
    async fn oauth_login_is_501_when_no_provider_is_configured() {
        // The default state() has `oauth_login: None`, so an OAuth body reports "not configured"
        // (501) — distinct from a bad credential (401) — while email+password keeps working.
        let app = auth_router(state());
        let (status, _) = post(&app, "/auth/login", r#"{"provider":"github","code":"abc"}"#).await;
        assert_eq!(status, StatusCode::NOT_IMPLEMENTED);
    }

    /// Spin up a throwaway in-process IdP (token + userinfo endpoints) and wire a real
    /// [`OidcProvider`](crate::OidcProvider) into `oauth_login`, proving the bead acceptance: a
    /// `POST /auth/login` carrying an OAuth `code` runs the handshake and issues the SAME
    /// access/refresh pair the password path yields. No external network (hq-platform-hardening.3).
    #[cfg(feature = "oauth")]
    #[tokio::test]
    async fn login_via_oauth_issues_the_same_token_pair_as_the_password_path() {
        use axum::routing::{get as axget, post as axpost};
        use axum::Json as AxJson;

        async fn token_handler() -> AxJson<serde_json::Value> {
            AxJson(serde_json::json!({ "access_token": "tok-xyz", "token_type": "Bearer" }))
        }
        async fn userinfo_handler(
            headers: HeaderMap,
        ) -> Result<AxJson<serde_json::Value>, StatusCode> {
            let ok = headers
                .get("authorization")
                .and_then(|v| v.to_str().ok())
                .map(|v| v == "Bearer tok-xyz")
                .unwrap_or(false);
            if ok {
                Ok(AxJson(serde_json::json!({ "sub": "oauth-alice" })))
            } else {
                Err(StatusCode::UNAUTHORIZED)
            }
        }
        let idp = Router::new()
            .route("/token", axpost(token_handler))
            .route("/userinfo", axget(userinfo_handler));
        let listener = tokio::net::TcpListener::bind(("127.0.0.1", 0))
            .await
            .unwrap();
        let base = format!("http://{}", listener.local_addr().unwrap());
        tokio::spawn(async move { axum::serve(listener, idp).await.unwrap() });

        let config = crate::OidcConfig {
            issuer: format!("{base}/"),
            client_id: "gt".into(),
            client_secret: "s".into(),
            token_endpoint: format!("{base}/token"),
            userinfo_endpoint: format!("{base}/userinfo"),
            redirect_uri: "https://gt.test/cb".into(),
            workspace: "acme".into(),
            scopes: vec!["rig.read".into()],
        };
        let oauth = Arc::new(crate::OidcProvider::new(config).unwrap()) as Arc<dyn LoginProvider>;
        let app = auth_router(AuthState {
            oauth_login: Some(oauth),
            ..state()
        });

        let (status, body) = post(
            &app,
            "/auth/login",
            r#"{"provider":"mock","code":"good-code"}"#,
        )
        .await;
        assert_eq!(status, StatusCode::OK, "oauth login body: {body}");
        let (access, refresh) = token_pair(&body);
        assert!(!access.is_empty() && !refresh.is_empty());
        assert!(body.contains(r#""token_type":"Bearer""#));
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

        let (status, body) = post(
            &app,
            "/auth/refresh",
            &format!(r#"{{"refresh_token":"{refresh1}"}}"#),
        )
        .await;
        assert_eq!(status, StatusCode::OK);
        let (_, refresh2) = token_pair(&body);
        assert_ne!(refresh1, refresh2); // rotated

        // Reusing the now-rotated first token is rejected (and burns the family).
        let (status, _) = post(
            &app,
            "/auth/refresh",
            &format!(r#"{{"refresh_token":"{refresh1}"}}"#),
        )
        .await;
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

        let (status, _) = post(
            &app,
            "/auth/logout",
            &format!(r#"{{"refresh_token":"{refresh}"}}"#),
        )
        .await;
        assert_eq!(status, StatusCode::NO_CONTENT);

        // The revoked token can no longer be refreshed.
        let (status, _) = post(
            &app,
            "/auth/refresh",
            &format!(r#"{{"refresh_token":"{refresh}"}}"#),
        )
        .await;
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
        let mut req = Request::builder()
            .uri("/auth/me")
            .body(Body::empty())
            .unwrap();
        req.extensions_mut().insert(claims);
        let resp = app.clone().oneshot(req).await.unwrap();
        assert_eq!(resp.status(), StatusCode::OK);
        let bytes = axum::body::to_bytes(resp.into_body(), usize::MAX)
            .await
            .unwrap();
        let body = String::from_utf8(bytes.to_vec()).unwrap();
        assert!(body.contains(r#""sub":"alice""#) && body.contains(r#""workspace":"acme""#));

        // Without claims → 401.
        let resp = app
            .oneshot(
                Request::builder()
                    .uri("/auth/me")
                    .body(Body::empty())
                    .unwrap(),
            )
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
        let access = cookies
            .iter()
            .find(|c| c.starts_with("gt_web_token="))
            .unwrap();
        let refresh = cookies
            .iter()
            .find(|c| c.starts_with("gt_refresh="))
            .unwrap();
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
        assert!(set_cookies(&resp)
            .iter()
            .any(|c| c.starts_with("gt_refresh=")));
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
        assert!(cookies
            .iter()
            .any(|c| c.starts_with("gt_web_token=") && c.contains("Max-Age=0")));
        assert!(cookies
            .iter()
            .any(|c| c.starts_with("gt_refresh=") && c.contains("Max-Age=0")));
    }

    // --- hq-identity.3: GET /auth/workspaces + POST /auth/switch ---------------------------------

    /// In-memory membership directory double: `sub -> [(workspace, role, scopes)]`. `list` projects
    /// the slug+role; `resolve` finds the held workspace and yields the identity to re-mint (the
    /// scopes it carries stand in for that tenant's role expansion).
    #[derive(Default)]
    struct MemDirectory {
        by_sub: std::collections::HashMap<String, Vec<(String, String, Vec<String>)>>,
        /// Every provisioned workspace, for the system-admin (`list_all`) path.
        all: Vec<String>,
    }

    #[async_trait]
    impl MembershipDirectory for MemDirectory {
        async fn list_all(&self) -> Result<Vec<WorkspaceMembership>, AuthError> {
            Ok(self
                .all
                .iter()
                .map(|w| WorkspaceMembership {
                    workspace: w.clone(),
                    role: "admin".to_string(),
                })
                .collect())
        }
        async fn list(&self, sub: &str) -> Result<Vec<WorkspaceMembership>, AuthError> {
            Ok(self
                .by_sub
                .get(sub)
                .into_iter()
                .flatten()
                .map(|(w, r, _)| WorkspaceMembership {
                    workspace: w.clone(),
                    role: r.clone(),
                })
                .collect())
        }
        async fn resolve(
            &self,
            sub: &str,
            workspace: &str,
        ) -> Result<Option<VerifiedIdentity>, AuthError> {
            Ok(self
                .by_sub
                .get(sub)
                .into_iter()
                .flatten()
                .find(|(w, _, _)| w == workspace)
                .map(|(w, _, scopes)| VerifiedIdentity {
                    sub: sub.to_string(),
                    workspace: w.clone(),
                    scopes: scopes.clone(),
                }))
        }
    }

    fn state_with_memberships(dir: Arc<MemDirectory>) -> AuthState {
        AuthState {
            memberships: Some(dir),
            ..state()
        }
    }

    /// In-memory [`MembershipAdmin`] double: records add/remove calls, and "knows" a fixed set of
    /// global emails so an add to anything else returns `false` (→ 404), mirroring the real adapter.
    #[derive(Default)]
    struct MemAdmin {
        known_emails: Vec<String>,
        added: std::sync::Mutex<Vec<(String, String, String)>>,
        removed: std::sync::Mutex<Vec<(String, String)>>,
    }

    #[async_trait]
    impl MembershipAdmin for MemAdmin {
        async fn add_member(
            &self,
            email: &str,
            workspace: &str,
            role: &str,
            _now: u64,
        ) -> Result<bool, AuthError> {
            if !self.known_emails.iter().any(|e| e == email) {
                return Ok(false);
            }
            self.added
                .lock()
                .unwrap()
                .push((email.into(), workspace.into(), role.into()));
            Ok(true)
        }
        async fn remove_member(&self, email: &str, workspace: &str) -> Result<bool, AuthError> {
            self.removed
                .lock()
                .unwrap()
                .push((email.into(), workspace.into()));
            Ok(self.known_emails.iter().any(|e| e == email))
        }
    }

    fn state_with_membership_admin(admin: Arc<MemAdmin>) -> AuthState {
        AuthState {
            membership_admin: Some(admin),
            ..state()
        }
    }

    /// A request carrying verified claims whose active `workspace` + `scopes` are the caller's —
    /// the membership-admin gate keys on BOTH, so the tests vary them.
    fn claim_req(
        method: &str,
        path: &str,
        workspace: Option<(&str, &[&str])>,
        json: Option<&str>,
    ) -> Request<Body> {
        let mut builder = Request::builder().method(method).uri(path);
        if json.is_some() {
            builder = builder.header("content-type", "application/json");
        }
        let mut req = builder
            .body(
                json.map(|j| Body::from(j.to_owned()))
                    .unwrap_or_else(Body::empty),
            )
            .unwrap();
        if let Some((workspace, scopes)) = workspace {
            req.extensions_mut().insert(JwtClaims {
                sub: "admin".into(),
                workspace: workspace.into(),
                scopes: scopes.iter().map(|s| s.to_string()).collect(),
                exp: 2_000_000_000,
                nbf: None,
                iat: 0,
            });
        }
        req
    }

    #[tokio::test]
    async fn ws_admin_can_add_a_member_but_a_non_admin_is_forbidden() {
        let admin = Arc::new(MemAdmin {
            known_emails: vec!["bob@acme.test".into()],
            ..Default::default()
        });
        let app = auth_router(state_with_membership_admin(admin.clone()));
        let path = "/auth/workspaces/acme/members";
        let body = r#"{"email":"bob@acme.test","role":"member"}"#;

        // No claims → 401.
        let resp = app
            .clone()
            .oneshot(claim_req("POST", path, None, Some(body)))
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::UNAUTHORIZED);

        // A member of acme WITHOUT the admin grant → 403 (a normal user cannot add).
        let resp = app
            .clone()
            .oneshot(claim_req(
                "POST",
                path,
                Some(("acme", &["beads.read"])),
                Some(body),
            ))
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::FORBIDDEN);

        // An admin of a DIFFERENT workspace → 403 (the gate keys on the active workspace too).
        let resp = app
            .clone()
            .oneshot(claim_req("POST", path, Some(("other", &["*"])), Some(body)))
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::FORBIDDEN);

        // The workspace's own admin (`workspace.admin` while active in acme) → 204, recorded.
        let resp = app
            .clone()
            .oneshot(claim_req(
                "POST",
                path,
                Some(("acme", &["workspace.admin"])),
                Some(body),
            ))
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::NO_CONTENT);
        // The `*` wildcard admin active in acme also passes.
        let resp = app
            .clone()
            .oneshot(claim_req("POST", path, Some(("acme", &["*"])), Some(body)))
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::NO_CONTENT);
        assert_eq!(admin.added.lock().unwrap().len(), 2);

        // An unknown email → 404.
        let resp = app
            .clone()
            .oneshot(claim_req(
                "POST",
                path,
                Some(("acme", &["*"])),
                Some(r#"{"email":"ghost@acme.test","role":"member"}"#),
            ))
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::NOT_FOUND);
    }

    #[tokio::test]
    async fn ws_admin_can_remove_a_member_and_non_member_is_404() {
        let admin = Arc::new(MemAdmin {
            known_emails: vec!["bob@acme.test".into()],
            ..Default::default()
        });
        let app = auth_router(state_with_membership_admin(admin.clone()));

        // Non-admin remove → 403.
        let resp = app
            .clone()
            .oneshot(claim_req(
                "DELETE",
                "/auth/workspaces/acme/members/bob@acme.test",
                Some(("acme", &["beads.read"])),
                None,
            ))
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::FORBIDDEN);

        // Admin removes a known member → 204.
        let resp = app
            .clone()
            .oneshot(claim_req(
                "DELETE",
                "/auth/workspaces/acme/members/bob@acme.test",
                Some(("acme", &["workspace.admin"])),
                None,
            ))
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::NO_CONTENT);

        // Removing an unknown member → 404.
        let resp = app
            .clone()
            .oneshot(claim_req(
                "DELETE",
                "/auth/workspaces/acme/members/ghost@acme.test",
                Some(("acme", &["workspace.admin"])),
                None,
            ))
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::NOT_FOUND);
    }

    #[tokio::test]
    async fn membership_admin_501_when_no_surface_configured() {
        // Default state() has membership_admin: None → "not configured", but only AFTER the gate
        // passes (an authorized admin call reaches the missing surface).
        let app = auth_router(state());
        let resp = app
            .oneshot(claim_req(
                "POST",
                "/auth/workspaces/acme/members",
                Some(("acme", &["*"])),
                Some(r#"{"email":"x@y.z","role":"member"}"#),
            ))
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::NOT_IMPLEMENTED);
    }

    /// Two-membership directory for "gwen": lead@ida (beads.read) and lead@idb (rig.read).
    fn gwen_directory() -> Arc<MemDirectory> {
        let mut by_sub = std::collections::HashMap::new();
        by_sub.insert(
            "gwen".to_string(),
            vec![
                (
                    "ida".to_string(),
                    "lead".to_string(),
                    vec!["beads.read".to_string()],
                ),
                (
                    "idb".to_string(),
                    "lead".to_string(),
                    vec!["rig.read".to_string()],
                ),
            ],
        );
        // The deployment also has `idc`, which gwen is NOT a member of — only a system admin
        // reaches it (exercised by the system-admin tests).
        Arc::new(MemDirectory {
            by_sub,
            all: vec!["ida".to_string(), "idb".to_string(), "idc".to_string()],
        })
    }

    /// Build a request to `path` with optional verified claims (`sub`/`workspace`) and JSON body.
    fn authed_req(
        method: &str,
        path: &str,
        sub: Option<&str>,
        json: Option<&str>,
    ) -> Request<Body> {
        let mut builder = Request::builder().method(method).uri(path);
        if json.is_some() {
            builder = builder.header("content-type", "application/json");
        }
        let mut req = builder
            .body(
                json.map(|j| Body::from(j.to_owned()))
                    .unwrap_or_else(Body::empty),
            )
            .unwrap();
        if let Some(sub) = sub {
            req.extensions_mut().insert(JwtClaims {
                sub: sub.into(),
                workspace: "ida".into(),
                scopes: vec!["beads.read".into()],
                exp: 2_000_000_000,
                nbf: None,
                iat: 0,
            });
        }
        req
    }

    #[tokio::test]
    async fn workspaces_lists_the_callers_memberships() {
        let app = auth_router(state_with_memberships(gwen_directory()));
        let resp = app
            .oneshot(authed_req("GET", "/auth/workspaces", Some("gwen"), None))
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::OK);
        let bytes = axum::body::to_bytes(resp.into_body(), usize::MAX)
            .await
            .unwrap();
        let got: Vec<WorkspaceMembership> = serde_json::from_slice(&bytes).unwrap();
        assert_eq!(
            got,
            vec![
                WorkspaceMembership {
                    workspace: "ida".into(),
                    role: "lead".into()
                },
                WorkspaceMembership {
                    workspace: "idb".into(),
                    role: "lead".into()
                },
            ]
        );
    }

    #[tokio::test]
    async fn workspaces_requires_authentication_and_a_directory() {
        // No claims → 401.
        let app = auth_router(state_with_memberships(gwen_directory()));
        let resp = app
            .oneshot(authed_req("GET", "/auth/workspaces", None, None))
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::UNAUTHORIZED);
        // No directory configured → 501.
        let resp = auth_router(state())
            .oneshot(authed_req("GET", "/auth/workspaces", Some("gwen"), None))
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::NOT_IMPLEMENTED);
    }

    #[tokio::test]
    async fn switch_remints_for_a_held_workspace_with_that_tenants_scopes() {
        let app = auth_router(state_with_memberships(gwen_directory()));
        let resp = app
            .oneshot(authed_req(
                "POST",
                "/auth/switch",
                Some("gwen"),
                Some(r#"{"workspace":"idb"}"#),
            ))
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::OK);
        // The cookies are re-stamped on switch.
        let cookies = set_cookies(&resp);
        assert!(cookies.iter().any(|c| c.starts_with("gt_web_token=")));
        // The re-minted access token names idb + that tenant's scopes — proving the server
        // resolved the active workspace from membership, not from the prior claim (ida).
        let bytes = axum::body::to_bytes(resp.into_body(), usize::MAX)
            .await
            .unwrap();
        let body: serde_json::Value = serde_json::from_slice(&bytes).unwrap();
        let access = body["access_token"].as_str().unwrap();
        let verifier = JwtAuthenticator::from_kid_pems([("k1", PUB_PEM)]).unwrap();
        let claims = verifier.authenticate(access).unwrap();
        assert_eq!(claims.sub, "gwen");
        assert_eq!(claims.workspace, "idb");
        assert_eq!(claims.scopes, vec!["rig.read".to_string()]);
    }

    #[tokio::test]
    async fn switch_to_an_unheld_workspace_is_forbidden() {
        let app = auth_router(state_with_memberships(gwen_directory()));
        let resp = app
            .oneshot(authed_req(
                "POST",
                "/auth/switch",
                Some("gwen"),
                Some(r#"{"workspace":"ghost"}"#),
            ))
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::FORBIDDEN);
    }

    #[tokio::test]
    async fn switch_without_claims_is_unauthorized() {
        let app = auth_router(state_with_memberships(gwen_directory()));
        let resp = app
            .oneshot(authed_req(
                "POST",
                "/auth/switch",
                None,
                Some(r#"{"workspace":"idb"}"#),
            ))
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::UNAUTHORIZED);
    }

    /// A request carrying SYSTEM-admin claims (`*`) — used to prove the cross-tenant widening of
    /// the membership endpoints. `sub` is a user with NO membership at all, so any reach beyond
    /// memberships is purely the admin grant.
    fn admin_req(method: &str, path: &str, json: Option<&str>) -> Request<Body> {
        let mut builder = Request::builder().method(method).uri(path);
        if json.is_some() {
            builder = builder.header("content-type", "application/json");
        }
        let mut req = builder
            .body(json.map(|j| Body::from(j.to_owned())).unwrap_or_else(Body::empty))
            .unwrap();
        req.extensions_mut().insert(JwtClaims {
            sub: "root".into(),
            workspace: "default".into(),
            scopes: vec!["*".into()],
            exp: 2_000_000_000,
            nbf: None,
            iat: 0,
        });
        req
    }

    #[tokio::test]
    async fn system_admin_workspaces_lists_every_workspace() {
        // `root` holds no membership, yet the `*` grant widens the picker to all provisioned ws.
        let app = auth_router(state_with_memberships(gwen_directory()));
        let resp = app
            .oneshot(admin_req("GET", "/auth/workspaces", None))
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::OK);
        let bytes = axum::body::to_bytes(resp.into_body(), usize::MAX)
            .await
            .unwrap();
        let got: Vec<WorkspaceMembership> = serde_json::from_slice(&bytes).unwrap();
        let slugs: Vec<&str> = got.iter().map(|w| w.workspace.as_str()).collect();
        assert_eq!(slugs, vec!["ida", "idb", "idc"]);
        assert!(got.iter().all(|w| w.role == "admin"));
    }

    #[tokio::test]
    async fn system_admin_switches_into_an_unheld_workspace() {
        // `root` is not a member of `idc`, but a system admin may enter it, carrying `*`.
        let app = auth_router(state_with_memberships(gwen_directory()));
        let resp = app
            .oneshot(admin_req("POST", "/auth/switch", Some(r#"{"workspace":"idc"}"#)))
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::OK);
        let bytes = axum::body::to_bytes(resp.into_body(), usize::MAX)
            .await
            .unwrap();
        let body: serde_json::Value = serde_json::from_slice(&bytes).unwrap();
        let access = body["access_token"].as_str().unwrap();
        let verifier = JwtAuthenticator::from_kid_pems([("k1", PUB_PEM)]).unwrap();
        let claims = verifier.authenticate(access).unwrap();
        assert_eq!(claims.workspace, "idc");
        assert_eq!(claims.scopes, vec!["*".to_string()]);
    }

    #[tokio::test]
    async fn system_admin_switch_to_a_nonexistent_workspace_is_not_found() {
        let app = auth_router(state_with_memberships(gwen_directory()));
        let resp = app
            .oneshot(admin_req("POST", "/auth/switch", Some(r#"{"workspace":"ghost"}"#)))
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::NOT_FOUND);
    }

    // --- OAuth/OIDC provider CRUD (hq-idp-db.4): system-admin gate + secret write-only ----------
    #[cfg(feature = "oauth")]
    mod providers {
        use super::*;
        use std::sync::Mutex;

        /// In-memory [`ProviderStore`] double recording the records as written — including the
        /// SEALED secret, so the test can prove the HTTP projection drops it (the store keeps it,
        /// the wire never shows it).
        #[derive(Default)]
        struct MemProviders {
            rows: Mutex<Vec<ProviderRecord>>,
        }

        #[async_trait]
        impl ProviderStore for MemProviders {
            async fn list_providers(&self) -> Result<Vec<ProviderRecord>, AuthError> {
                Ok(self.rows.lock().unwrap().clone())
            }
            async fn create_provider(
                &self,
                provider: NewProvider,
            ) -> Result<ProviderRecord, AuthError> {
                // Seal the secret exactly as the real adapter would, so the stored record carries a
                // ciphertext blob the projection must omit.
                let enc = crate::crypto::seal(provider.client_secret.as_bytes())?;
                let rec = ProviderRecord {
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
                    workspace_id: provider.workspace_id,
                };
                self.rows.lock().unwrap().push(rec.clone());
                Ok(rec)
            }
            async fn patch_provider(
                &self,
                id: &str,
                patch: PatchProvider,
            ) -> Result<Option<ProviderRecord>, AuthError> {
                let mut rows = self.rows.lock().unwrap();
                let Some(rec) = rows.iter_mut().find(|r| r.id == id) else {
                    return Ok(None);
                };
                if let Some(v) = patch.enabled {
                    rec.enabled = v;
                }
                if let Some(v) = patch.client_secret {
                    rec.client_secret_enc = crate::crypto::seal(v.as_bytes())?;
                }
                if let Some(v) = patch.display_name {
                    rec.display_name = v;
                }
                Ok(Some(rec.clone()))
            }
            async fn delete_provider(&self, id: &str) -> Result<bool, AuthError> {
                let mut rows = self.rows.lock().unwrap();
                let before = rows.len();
                rows.retain(|r| r.id != id);
                Ok(rows.len() != before)
            }
        }

        fn state_with_providers(p: Arc<MemProviders>) -> AuthState {
            AuthState {
                providers: Some(p),
                ..state()
            }
        }

        /// A request with the caller's scopes injected as verified claims; `None` ⇒ no claims.
        fn req(
            method: &str,
            path: &str,
            scopes: Option<&[&str]>,
            json: Option<&str>,
        ) -> Request<Body> {
            let mut builder = Request::builder().method(method).uri(path);
            if json.is_some() {
                builder = builder.header("content-type", "application/json");
            }
            let mut r = builder
                .body(
                    json.map(|j| Body::from(j.to_owned()))
                        .unwrap_or_else(Body::empty),
                )
                .unwrap();
            if let Some(scopes) = scopes {
                r.extensions_mut().insert(JwtClaims {
                    sub: "caller".into(),
                    workspace: "acme".into(),
                    scopes: scopes.iter().map(|s| s.to_string()).collect(),
                    exp: 2_000_000_000,
                    nbf: None,
                    iat: 0,
                });
            }
            r
        }

        async fn body_str(resp: axum::response::Response) -> String {
            let bytes = axum::body::to_bytes(resp.into_body(), usize::MAX)
                .await
                .unwrap();
            String::from_utf8(bytes.to_vec()).unwrap()
        }

        /// The full admin lifecycle: a system admin (`*`) creates → the create echo carries NO
        /// secret; patches `enabled`; lists → still no secret; deletes. A non-admin caller is 403
        /// on every mutation, and an unauthenticated one is 401.
        #[tokio::test]
        async fn system_admin_crud_and_secret_is_write_only() {
            // The seal step needs a master key for the test process.
            std::env::set_var(
                crate::ENV_SECRET_KEY,
                "0000000000000000000000000000000000000000000000000000000000000000",
            );
            let store = Arc::new(MemProviders::default());
            let app = auth_router(state_with_providers(store.clone()));
            let create_body = r#"{"id":"goog","kind":"google","client_id":"cid","client_secret":"top-secret-xyz"}"#;

            // Non-admin (a mere workspace.admin, not the system `*`) → 403; unauthenticated → 401.
            let forbidden = app
                .clone()
                .oneshot(req(
                    "POST",
                    "/auth/providers",
                    Some(&["workspace.admin"]),
                    Some(create_body),
                ))
                .await
                .unwrap();
            assert_eq!(forbidden.status(), StatusCode::FORBIDDEN);
            let unauth = app
                .clone()
                .oneshot(req("POST", "/auth/providers", None, Some(create_body)))
                .await
                .unwrap();
            assert_eq!(unauth.status(), StatusCode::UNAUTHORIZED);

            // System admin (`*`) → 201, and the echo omits the secret (sealed or plain).
            let created = app
                .clone()
                .oneshot(req(
                    "POST",
                    "/auth/providers",
                    Some(&["*"]),
                    Some(create_body),
                ))
                .await
                .unwrap();
            assert_eq!(created.status(), StatusCode::CREATED);
            let echo = body_str(created).await;
            assert!(echo.contains("\"id\":\"goog\""), "echo: {echo}");
            assert!(echo.contains("\"client_id\":\"cid\""), "echo: {echo}");
            assert!(
                echo.contains("oauth2.googleapis.com"),
                "preset endpoints filled: {echo}"
            );
            assert!(
                !echo.contains("client_secret"),
                "echo must not name the secret: {echo}"
            );
            assert!(
                !echo.contains("top-secret-xyz"),
                "echo must not carry the secret: {echo}"
            );

            // Patch `enabled` (no secret re-supplied) → 200, still no secret on the wire.
            let patched = app
                .clone()
                .oneshot(req(
                    "PATCH",
                    "/auth/providers/goog",
                    Some(&["*"]),
                    Some(r#"{"enabled":false}"#),
                ))
                .await
                .unwrap();
            assert_eq!(patched.status(), StatusCode::OK);
            let pbody = body_str(patched).await;
            assert!(
                pbody.contains("\"enabled\":false"),
                "patch toggled enabled: {pbody}"
            );
            assert!(
                !pbody.contains("top-secret-xyz"),
                "patch echo carries no secret: {pbody}"
            );

            // The store still holds the SEALED secret (write-only is about the wire, not at rest).
            assert!(!store.rows.lock().unwrap()[0].client_secret_enc.is_empty());

            // A delete by a non-admin is 403; by the system admin, 204; a second delete is 404.
            let del_forbidden = app
                .clone()
                .oneshot(req(
                    "DELETE",
                    "/auth/providers/goog",
                    Some(&["issues.write"]),
                    None,
                ))
                .await
                .unwrap();
            assert_eq!(del_forbidden.status(), StatusCode::FORBIDDEN);
            let deleted = app
                .clone()
                .oneshot(req("DELETE", "/auth/providers/goog", Some(&["*"]), None))
                .await
                .unwrap();
            assert_eq!(deleted.status(), StatusCode::NO_CONTENT);
            let gone = app
                .clone()
                .oneshot(req("DELETE", "/auth/providers/goog", Some(&["*"]), None))
                .await
                .unwrap();
            assert_eq!(gone.status(), StatusCode::NOT_FOUND);
        }

        /// A `generic` provider missing an endpoint is a 400 (no preset to fill it), and an unknown
        /// kind is a 400 — never a silent half-registered row.
        #[tokio::test]
        async fn generic_requires_endpoints_and_unknown_kind_is_400() {
            std::env::set_var(
                crate::ENV_SECRET_KEY,
                "0000000000000000000000000000000000000000000000000000000000000000",
            );
            let app = auth_router(state_with_providers(Arc::new(MemProviders::default())));

            let missing = app
                .clone()
                .oneshot(req(
                    "POST",
                    "/auth/providers",
                    Some(&["*"]),
                    Some(r#"{"id":"x","kind":"generic","client_id":"c","client_secret":"s"}"#),
                ))
                .await
                .unwrap();
            assert_eq!(missing.status(), StatusCode::BAD_REQUEST);

            let unknown = app
                .oneshot(req(
                    "POST",
                    "/auth/providers",
                    Some(&["*"]),
                    Some(r#"{"id":"x","kind":"nope","client_id":"c","client_secret":"s"}"#),
                ))
                .await
                .unwrap();
            assert_eq!(unknown.status(), StatusCode::BAD_REQUEST);
        }

        /// With no provider store configured the surface is 501 (login + admin unaffected).
        #[tokio::test]
        async fn not_configured_is_501() {
            let app = auth_router(state()); // providers: None
            let resp = app
                .oneshot(req(
                    "POST",
                    "/auth/providers",
                    Some(&["*"]),
                    Some(r#"{"id":"x","kind":"google","client_id":"c","client_secret":"s"}"#),
                ))
                .await
                .unwrap();
            assert_eq!(resp.status(), StatusCode::NOT_IMPLEMENTED);
        }

        /// The fused OpenAPI advertises the provider routes when `oauth` is built.
        #[test]
        fn openapi_lists_provider_routes() {
            let doc = auth_openapi();
            let paths: Vec<&str> = doc.paths.paths.keys().map(String::as_str).collect();
            assert!(paths.contains(&"/auth/providers"), "{paths:?}");
            assert!(paths.contains(&"/auth/providers/{id}"), "{paths:?}");
            // The admin read endpoints (hq-idp-ui.1) are advertised too.
            assert!(paths.contains(&"/auth/providers/all"), "{paths:?}");
        }

        /// The admin READ surface (hq-idp-ui.1): a system admin lists ALL providers (incl
        /// `enabled=false`) with the full secret-free shape, and reads a single disabled one by id;
        /// a non-admin is 403, an unauthenticated caller 401, and an unknown id 404. The PUBLIC
        /// `GET /auth/providers` stays enabled-only + minimal.
        #[tokio::test]
        async fn admin_read_lists_all_incl_disabled_without_secret() {
            std::env::set_var(
                crate::ENV_SECRET_KEY,
                "0000000000000000000000000000000000000000000000000000000000000000",
            );
            let store = Arc::new(MemProviders::default());
            let app = auth_router(state_with_providers(store.clone()));

            // Seed one ENABLED + one DISABLED provider via the system-admin CRUD.
            for body in [
                r#"{"id":"goog","kind":"google","client_id":"cid-on","client_secret":"sec-on","enabled":true}"#,
                r#"{"id":"gh","kind":"github","client_id":"cid-off","client_secret":"sec-off","enabled":false}"#,
            ] {
                let created = app
                    .clone()
                    .oneshot(req("POST", "/auth/providers", Some(&["*"]), Some(body)))
                    .await
                    .unwrap();
                assert_eq!(created.status(), StatusCode::CREATED);
            }

            // Admin LIST → BOTH providers, with client_id/endpoints/enabled and NO secret.
            let listed = app
                .clone()
                .oneshot(req("GET", "/auth/providers/all", Some(&["*"]), None))
                .await
                .unwrap();
            assert_eq!(listed.status(), StatusCode::OK);
            let all = body_str(listed).await;
            assert!(all.contains("\"id\":\"goog\""), "enabled present: {all}");
            assert!(all.contains("\"id\":\"gh\""), "disabled present: {all}");
            assert!(
                all.contains("\"client_id\":\"cid-off\""),
                "client_id present: {all}"
            );
            assert!(
                all.contains("github.com/login/oauth"),
                "endpoints present: {all}"
            );
            assert!(
                all.contains("\"enabled\":false"),
                "disabled flag present: {all}"
            );
            assert!(
                !all.contains("client_secret"),
                "list names no secret: {all}"
            );
            assert!(
                !all.contains("sec-on") && !all.contains("sec-off"),
                "no secret value: {all}"
            );

            // Admin SINGLE by id → the DISABLED provider, full secret-free shape.
            let single = app
                .clone()
                .oneshot(req("GET", "/auth/providers/gh", Some(&["*"]), None))
                .await
                .unwrap();
            assert_eq!(single.status(), StatusCode::OK);
            let one = body_str(single).await;
            assert!(one.contains("\"id\":\"gh\""), "single by id: {one}");
            assert!(
                one.contains("\"enabled\":false"),
                "single is the disabled one: {one}"
            );
            assert!(
                one.contains("\"client_id\":\"cid-off\""),
                "single carries client_id: {one}"
            );
            assert!(
                !one.contains("client_secret"),
                "single names no secret: {one}"
            );

            // A non-admin is 403 on both admin reads; unauthenticated is 401; an unknown id 404.
            let list_forbidden = app
                .clone()
                .oneshot(req(
                    "GET",
                    "/auth/providers/all",
                    Some(&["workspace.admin"]),
                    None,
                ))
                .await
                .unwrap();
            assert_eq!(list_forbidden.status(), StatusCode::FORBIDDEN);
            let single_forbidden = app
                .clone()
                .oneshot(req(
                    "GET",
                    "/auth/providers/gh",
                    Some(&["workspace.admin"]),
                    None,
                ))
                .await
                .unwrap();
            assert_eq!(single_forbidden.status(), StatusCode::FORBIDDEN);
            let unauth = app
                .clone()
                .oneshot(req("GET", "/auth/providers/all", None, None))
                .await
                .unwrap();
            assert_eq!(unauth.status(), StatusCode::UNAUTHORIZED);
            let unknown = app
                .clone()
                .oneshot(req("GET", "/auth/providers/nope", Some(&["*"]), None))
                .await
                .unwrap();
            assert_eq!(unknown.status(), StatusCode::NOT_FOUND);

            // The PUBLIC GET /auth/providers stays enabled-only + minimal (no client_id/endpoints).
            let public = app
                .clone()
                .oneshot(req("GET", "/auth/providers", None, None))
                .await
                .unwrap();
            assert_eq!(public.status(), StatusCode::OK);
            let pub_body = body_str(public).await;
            assert!(
                pub_body.contains("\"id\":\"goog\""),
                "public lists enabled: {pub_body}"
            );
            assert!(
                !pub_body.contains("\"id\":\"gh\""),
                "public hides disabled: {pub_body}"
            );
            assert!(
                !pub_body.contains("client_id"),
                "public is minimal (no client_id): {pub_body}"
            );
        }
    }

    // --- Public discovery + authorize/callback redirect flow (hq-idp-db.3) ----------------------
    #[cfg(feature = "oauth")]
    mod authz_flow {
        use super::*;
        use crate::provider_repo::{
            NewProvider, ProviderKind as RepoKind, ProviderRecord, ProviderRepo,
        };
        use crate::{NewAuthz, PendingAuthz};
        use std::collections::HashMap;
        use std::sync::Mutex;

        #[test]
        fn loopback_allowlist_accepts_only_127_and_localhost() {
            // Accept: 127.0.0.1 / localhost, with or without a port + path.
            assert!(is_loopback_redirect("http://127.0.0.1:8976/callback"));
            assert!(is_loopback_redirect("http://localhost:54321/cb"));
            assert!(is_loopback_redirect("http://127.0.0.1/"));
            // Reject: external hosts, https, look-alikes, and a missing scheme.
            assert!(!is_loopback_redirect("http://evil.com/callback"));
            assert!(!is_loopback_redirect("https://127.0.0.1:8976/callback"));
            assert!(!is_loopback_redirect("http://127.0.0.1.evil.com/cb"));
            assert!(!is_loopback_redirect("http://localhost.evil.com/cb"));
            assert!(!is_loopback_redirect("ftp://127.0.0.1/cb"));
            assert!(!is_loopback_redirect("127.0.0.1:8976/cb"));
        }

        #[test]
        fn allowed_cli_redirect_covers_oob_and_loopback() {
            // The broader gate authorize uses: OOB sentinel + loopback are allowed, nothing else.
            assert!(is_allowed_cli_redirect(CLI_REDIRECT_OOB));
            assert!(is_allowed_cli_redirect("http://127.0.0.1:8976/callback"));
            assert!(!is_allowed_cli_redirect("http://evil.com/cb"));
            assert!(!is_allowed_cli_redirect("urn:ietf:wg:oauth:2.0:oob-ish"));
        }

        /// An in-memory [`ProviderStore`] + [`ProviderRepo`] over a fixed record set — enough to
        /// drive both the public discovery projection and the `DbOauthLogin` resolver.
        #[derive(Default)]
        struct MapProviders {
            rows: Mutex<Vec<ProviderRecord>>,
        }
        #[async_trait]
        impl ProviderStore for MapProviders {
            async fn list_providers(&self) -> Result<Vec<ProviderRecord>, AuthError> {
                Ok(self.rows.lock().unwrap().clone())
            }
            async fn create_provider(&self, _p: NewProvider) -> Result<ProviderRecord, AuthError> {
                unreachable!()
            }
            async fn patch_provider(
                &self,
                _id: &str,
                _p: PatchProvider,
            ) -> Result<Option<ProviderRecord>, AuthError> {
                unreachable!()
            }
            async fn delete_provider(&self, _id: &str) -> Result<bool, AuthError> {
                unreachable!()
            }
        }
        #[async_trait]
        impl ProviderRepo for MapProviders {
            async fn list(&self) -> Result<Vec<ProviderRecord>, AuthError> {
                Ok(self.rows.lock().unwrap().clone())
            }
            async fn list_for_workspace(&self, workspace: &str) -> Result<Vec<ProviderRecord>, AuthError> {
                Ok(self.rows.lock().unwrap().iter()
                    .filter(|r| r.workspace_id.is_none() || r.workspace_id.as_deref() == Some(workspace))
                    .cloned()
                    .collect())
            }
            async fn get(&self, id: &str) -> Result<Option<ProviderRecord>, AuthError> {
                Ok(self
                    .rows
                    .lock()
                    .unwrap()
                    .iter()
                    .find(|r| r.id == id)
                    .cloned())
            }
            async fn create(&self, _p: NewProvider) -> Result<ProviderRecord, AuthError> {
                unreachable!()
            }
            async fn patch(
                &self,
                _id: &str,
                _p: PatchProvider,
            ) -> Result<Option<ProviderRecord>, AuthError> {
                unreachable!()
            }
            async fn delete(&self, _id: &str) -> Result<bool, AuthError> {
                unreachable!()
            }
        }

        /// An in-memory one-shot [`AuthzStateStore`]: insert + delete-on-read, exactly the durable
        /// adapter's contract (so the replay rejection is exercised without Postgres).
        #[derive(Default)]
        struct MemAuthzState {
            rows: Mutex<HashMap<String, PendingAuthz>>,
        }
        #[async_trait]
        impl AuthzStateStore for MemAuthzState {
            async fn insert(&self, authz: NewAuthz) -> Result<(), AuthError> {
                self.rows.lock().unwrap().insert(
                    authz.state.clone(),
                    PendingAuthz {
                        state: authz.state,
                        code_verifier: authz.code_verifier,
                        provider_id: authz.provider_id,
                        redirect_uri: authz.redirect_uri,
                        cli_redirect: authz.cli_redirect,
                        expires_at: authz.expires_at,
                    },
                );
                Ok(())
            }
            async fn consume(&self, state: &str) -> Result<Option<PendingAuthz>, AuthError> {
                Ok(self.rows.lock().unwrap().remove(state))
            }
        }

        /// An in-memory one-shot [`CliCodeStore`]: insert + delete-on-read, the durable adapter's
        /// contract without Postgres (exercises the CLI hand-off replay rejection).
        #[derive(Default)]
        struct MemCliCode {
            rows: Mutex<HashMap<String, crate::PendingCliCode>>,
        }
        #[async_trait]
        impl CliCodeStore for MemCliCode {
            async fn insert(&self, code: crate::NewCliCode) -> Result<(), AuthError> {
                self.rows.lock().unwrap().insert(
                    code.code,
                    crate::PendingCliCode {
                        access_token: code.access_token,
                        refresh_token: code.refresh_token,
                        token_type: code.token_type,
                        expires_in: code.expires_in,
                        expires_at: code.expires_at,
                    },
                );
                Ok(())
            }
            async fn consume(
                &self,
                code: &str,
            ) -> Result<Option<crate::PendingCliCode>, AuthError> {
                Ok(self.rows.lock().unwrap().remove(code))
            }
        }

        /// A generic-kind record whose endpoints point at `base`, sealing `secret` with the test key.
        fn record(base: &str, id: &str, enabled: bool) -> ProviderRecord {
            std::env::set_var(
                crate::ENV_SECRET_KEY,
                "0000000000000000000000000000000000000000000000000000000000000000",
            );
            ProviderRecord {
                id: id.into(),
                kind: RepoKind::Generic,
                display_name: "Corp SSO".into(),
                client_id: "gt-client".into(),
                client_secret_enc: crate::crypto::seal(b"s3cret").unwrap(),
                issuer: format!("{base}/"),
                authorize_endpoint: format!("{base}/authorize"),
                token_endpoint: format!("{base}/token"),
                userinfo_endpoint: format!("{base}/userinfo"),
                scopes: "rig.read".into(),
                enabled,
                workspace_id: None,
            }
        }

        /// Spin up a throwaway in-process IdP: `/token` swaps `good-code` (with the right
        /// `code_verifier` present) for an access token; `/userinfo` resolves it to a `sub`.
        async fn spawn_idp() -> String {
            use axum::extract::Form;
            use axum::routing::{get as axget, post as axpost};
            async fn token(
                Form(form): Form<HashMap<String, String>>,
            ) -> Result<Json<serde_json::Value>, StatusCode> {
                // The PKCE verifier MUST have reached the token endpoint (proves the exchange
                // threaded it through).
                if form.get("code").map(String::as_str) == Some("good-code")
                    && form
                        .get("code_verifier")
                        .map(|v| !v.is_empty())
                        .unwrap_or(false)
                {
                    Ok(Json(
                        serde_json::json!({ "access_token": "tok-1", "token_type": "Bearer" }),
                    ))
                } else {
                    Err(StatusCode::BAD_REQUEST)
                }
            }
            async fn userinfo(headers: HeaderMap) -> Result<Json<serde_json::Value>, StatusCode> {
                let ok = headers
                    .get("authorization")
                    .and_then(|v| v.to_str().ok())
                    .map(|v| v == "Bearer tok-1")
                    .unwrap_or(false);
                if ok {
                    Ok(Json(serde_json::json!({ "sub": "carol-sub" })))
                } else {
                    Err(StatusCode::UNAUTHORIZED)
                }
            }
            let app = Router::new()
                .route("/token", axpost(token))
                .route("/userinfo", axget(userinfo));
            let listener = tokio::net::TcpListener::bind(("127.0.0.1", 0))
                .await
                .unwrap();
            let base = format!("http://{}", listener.local_addr().unwrap());
            tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
            base
        }

        /// Build an [`AuthState`] wired with the discovery store + the DB resolver as both
        /// `authz_flow` and `oauth_login`, plus the in-memory state store. `fe` is the optional FE
        /// redirect URL.
        fn flow_state(
            base: &str,
            fe: Option<&str>,
        ) -> (AuthState, Arc<MemAuthzState>, Arc<MemCliCode>) {
            let providers = Arc::new(MapProviders::default());
            providers
                .rows
                .lock()
                .unwrap()
                .push(record(base, "corp", true));
            // A disabled provider must NOT appear in discovery.
            providers
                .rows
                .lock()
                .unwrap()
                .push(record(base, "off", false));
            let resolver = Arc::new(
                crate::DbOauthLogin::new(
                    providers.clone() as Arc<dyn ProviderRepo>,
                    "acme",
                    "https://gt.test/auth/callback",
                )
                .unwrap(),
            );
            let authz_state = Arc::new(MemAuthzState::default());
            let cli_code = Arc::new(MemCliCode::default());
            let st = AuthState {
                providers: Some(providers as Arc<dyn ProviderStore>),
                authz_flow: Some(resolver as Arc<dyn OauthAuthzFlow>),
                authz_state: Some(authz_state.clone() as Arc<dyn AuthzStateStore>),
                cli_code: Some(cli_code.clone() as Arc<dyn CliCodeStore>),
                fe_redirect_url: fe.map(str::to_owned),
                ..state()
            };
            (st, authz_state, cli_code)
        }

        /// Pull the single `Location` header off a response.
        fn location(resp: &axum::response::Response) -> String {
            resp.headers()
                .get(header::LOCATION)
                .unwrap()
                .to_str()
                .unwrap()
                .to_owned()
        }

        /// `GET /auth/providers` lists ONLY the enabled provider and leaks NO secret (or client id).
        #[tokio::test]
        async fn public_discovery_lists_enabled_without_secret() {
            let base = spawn_idp().await;
            let (st, _, _) = flow_state(&base, None);
            let app = auth_router(st);
            let resp = app
                .oneshot(
                    Request::builder()
                        .uri("/auth/providers")
                        .body(Body::empty())
                        .unwrap(),
                )
                .await
                .unwrap();
            assert_eq!(resp.status(), StatusCode::OK);
            let bytes = axum::body::to_bytes(resp.into_body(), usize::MAX)
                .await
                .unwrap();
            let body = String::from_utf8(bytes.to_vec()).unwrap();
            let v: serde_json::Value = serde_json::from_str(&body).unwrap();
            let arr = v.as_array().unwrap();
            assert_eq!(arr.len(), 1, "only the enabled provider: {body}");
            assert_eq!(arr[0]["id"], "corp");
            assert_eq!(arr[0]["authorize_url"], "/auth/providers/corp/authorize");
            // No secret material — and not even the client id / endpoints — on the public surface.
            assert!(!body.contains("s3cret") && !body.contains("client_secret"));
            assert!(!body.contains("gt-client") && !body.contains("token"));
        }

        /// `/authorize` 302s to the IdP with `state` + the S256 `code_challenge` in the Location.
        #[tokio::test]
        async fn authorize_redirects_with_state_and_pkce_challenge() {
            let base = spawn_idp().await;
            let (st, store, _) = flow_state(&base, None);
            let app = auth_router(st);
            let resp = app
                .oneshot(
                    Request::builder()
                        .uri("/auth/providers/corp/authorize")
                        .body(Body::empty())
                        .unwrap(),
                )
                .await
                .unwrap();
            assert_eq!(resp.status(), StatusCode::FOUND);
            let loc = location(&resp);
            assert!(loc.starts_with(&format!("{base}/authorize?")), "loc: {loc}");
            assert!(loc.contains("response_type=code"));
            assert!(loc.contains("code_challenge="));
            assert!(loc.contains("code_challenge_method=S256"));
            assert!(loc.contains("state="));
            // The pending row was persisted under exactly the `state` sent to the IdP.
            let state_param = loc
                .split("state=")
                .nth(1)
                .unwrap()
                .split('&')
                .next()
                .unwrap()
                .to_owned();
            assert!(store.rows.lock().unwrap().contains_key(&state_param));

            // An unknown / disabled id is a 404, with no orphan state row left behind.
            for id in ["nope", "off"] {
                let r = auth_router(flow_state(&base, None).0)
                    .oneshot(
                        Request::builder()
                            .uri(format!("/auth/providers/{id}/authorize"))
                            .body(Body::empty())
                            .unwrap(),
                    )
                    .await
                    .unwrap();
                assert_eq!(r.status(), StatusCode::NOT_FOUND, "id {id}");
            }
        }

        /// Full roundtrip: authorize → carry the state → callback with the mock code issues tokens;
        /// the state is consumed once (a replay is rejected `401`).
        #[tokio::test]
        async fn authorize_then_callback_issues_tokens_and_state_is_one_shot() {
            let base = spawn_idp().await;
            let (st, _store, _) = flow_state(&base, None);
            let app = auth_router(st);

            // Authorize, capture the state the server minted.
            let authz = app
                .clone()
                .oneshot(
                    Request::builder()
                        .uri("/auth/providers/corp/authorize")
                        .body(Body::empty())
                        .unwrap(),
                )
                .await
                .unwrap();
            let loc = location(&authz);
            let state_param = loc
                .split("state=")
                .nth(1)
                .unwrap()
                .split('&')
                .next()
                .unwrap()
                .to_owned();

            // Callback with the mock `code` + that `state`: no FE configured ⇒ token JSON back.
            let cb = app
                .clone()
                .oneshot(
                    Request::builder()
                        .uri(format!("/auth/callback?code=good-code&state={state_param}"))
                        .body(Body::empty())
                        .unwrap(),
                )
                .await
                .unwrap();
            assert_eq!(cb.status(), StatusCode::OK);
            let bytes = axum::body::to_bytes(cb.into_body(), usize::MAX)
                .await
                .unwrap();
            let body = String::from_utf8(bytes.to_vec()).unwrap();
            let (access, refresh) = token_pair(&body);
            assert!(!access.is_empty() && !refresh.is_empty());

            // Replaying the same state is rejected (the row was consumed one-shot) → 401.
            let replay = app
                .oneshot(
                    Request::builder()
                        .uri(format!("/auth/callback?code=good-code&state={state_param}"))
                        .body(Body::empty())
                        .unwrap(),
                )
                .await
                .unwrap();
            assert_eq!(replay.status(), StatusCode::UNAUTHORIZED);
        }

        /// CLI hand-off (hq-gt-login-oauth.2): authorize with a loopback `cli_redirect` → the
        /// callback 302s a one-shot `code` to that loopback (no token in the URL), and
        /// `/auth/cli/exchange` redeems the code ONCE for the token pair (a replay is `401`).
        #[tokio::test]
        async fn cli_redirect_hands_off_a_one_shot_code() {
            let base = spawn_idp().await;
            let (st, _store, _cli) = flow_state(&base, Some("https://app.gt.test/landed"));
            let app = auth_router(st);

            // Authorize carrying a loopback cli_redirect; capture the minted state.
            let authz = app
                .clone()
                .oneshot(
                    Request::builder()
                        .uri("/auth/providers/corp/authorize?cli_redirect=http://127.0.0.1:8976/callback")
                        .body(Body::empty())
                        .unwrap(),
                )
                .await
                .unwrap();
            assert_eq!(authz.status(), StatusCode::FOUND);
            let state_param = location(&authz)
                .split("state=")
                .nth(1)
                .unwrap()
                .split('&')
                .next()
                .unwrap()
                .to_owned();

            // Callback: despite a configured FE, the CLI handshake 302s to the LOOPBACK with a
            // `code` query param — and NO token anywhere in the URL.
            let cb = app
                .clone()
                .oneshot(
                    Request::builder()
                        .uri(format!("/auth/callback?code=good-code&state={state_param}"))
                        .body(Body::empty())
                        .unwrap(),
                )
                .await
                .unwrap();
            assert_eq!(cb.status(), StatusCode::FOUND);
            let loc = location(&cb);
            assert!(
                loc.starts_with("http://127.0.0.1:8976/callback?code="),
                "loc: {loc}"
            );
            assert!(
                !loc.contains("access_token"),
                "token must not be in the URL: {loc}"
            );
            let code = loc.split("code=").nth(1).unwrap().to_owned();

            // Exchange the code → the token pair.
            let ex = app
                .clone()
                .oneshot(
                    Request::builder()
                        .method("POST")
                        .uri("/auth/cli/exchange")
                        .header("content-type", "application/json")
                        .body(Body::from(format!(r#"{{"code":"{code}"}}"#)))
                        .unwrap(),
                )
                .await
                .unwrap();
            assert_eq!(ex.status(), StatusCode::OK);
            let bytes = axum::body::to_bytes(ex.into_body(), usize::MAX)
                .await
                .unwrap();
            let (access, refresh) = token_pair(&String::from_utf8(bytes.to_vec()).unwrap());
            assert!(!access.is_empty() && !refresh.is_empty());

            // Replaying the same code is rejected — it was consumed one-shot → 401.
            let replay = app
                .oneshot(
                    Request::builder()
                        .method("POST")
                        .uri("/auth/cli/exchange")
                        .header("content-type", "application/json")
                        .body(Body::from(format!(r#"{{"code":"{code}"}}"#)))
                        .unwrap(),
                )
                .await
                .unwrap();
            assert_eq!(replay.status(), StatusCode::UNAUTHORIZED);
        }

        /// OOB paste flow (hq-gt-login-oauth.6): authorize with the OOB sentinel → the callback
        /// RENDERS the one-shot code on an HTML page (no 302, no token on the page), and
        /// `/auth/cli/exchange` redeems that code for the token pair.
        #[tokio::test]
        async fn oob_cli_redirect_renders_a_code_page() {
            let base = spawn_idp().await;
            let (st, _store, _cli) = flow_state(&base, Some("https://app.gt.test/landed"));
            let app = auth_router(st);

            let authz = app
                .clone()
                .oneshot(
                    Request::builder()
                        .uri(
                            "/auth/providers/corp/authorize?cli_redirect=urn:ietf:wg:oauth:2.0:oob",
                        )
                        .body(Body::empty())
                        .unwrap(),
                )
                .await
                .unwrap();
            assert_eq!(authz.status(), StatusCode::FOUND);
            let state_param = location(&authz)
                .split("state=")
                .nth(1)
                .unwrap()
                .split('&')
                .next()
                .unwrap()
                .to_owned();

            // Callback renders a 200 HTML page; the code is in the body, the token is NOT.
            let cb = app
                .clone()
                .oneshot(
                    Request::builder()
                        .uri(format!("/auth/callback?code=good-code&state={state_param}"))
                        .body(Body::empty())
                        .unwrap(),
                )
                .await
                .unwrap();
            assert_eq!(cb.status(), StatusCode::OK);
            let body = String::from_utf8(
                axum::body::to_bytes(cb.into_body(), usize::MAX)
                    .await
                    .unwrap()
                    .to_vec(),
            )
            .unwrap();
            assert!(body.contains("<pre"), "should render a code page: {body}");
            assert!(!body.contains("access_token"), "no token on the page");
            // Pull the code out of the <pre>…</pre> and redeem it.
            let code = body
                .split("user-select:all;word-break:break-all\">")
                .nth(1)
                .unwrap()
                .split("</pre>")
                .next()
                .unwrap()
                .to_owned();
            let ex = app
                .oneshot(
                    Request::builder()
                        .method("POST")
                        .uri("/auth/cli/exchange")
                        .header("content-type", "application/json")
                        .body(Body::from(format!(r#"{{"code":"{code}"}}"#)))
                        .unwrap(),
                )
                .await
                .unwrap();
            assert_eq!(ex.status(), StatusCode::OK);
        }

        /// An unknown `state` at the callback is a 401 (anti-CSRF), never a token.
        #[tokio::test]
        async fn callback_with_unknown_state_is_401() {
            let base = spawn_idp().await;
            let (st, _, _) = flow_state(&base, None);
            let app = auth_router(st);
            let resp = app
                .oneshot(
                    Request::builder()
                        .uri("/auth/callback?code=good-code&state=never-issued")
                        .body(Body::empty())
                        .unwrap(),
                )
                .await
                .unwrap();
            assert_eq!(resp.status(), StatusCode::UNAUTHORIZED);
        }

        /// With a FE redirect configured, the callback 302s there with the access token in a
        /// fragment (plus the auth cookies).
        #[tokio::test]
        async fn callback_hands_tokens_to_the_fe_via_fragment() {
            let base = spawn_idp().await;
            let (st, _, _) = flow_state(&base, Some("https://app.gt.test/auth/landed"));
            let app = auth_router(st);
            let authz = app
                .clone()
                .oneshot(
                    Request::builder()
                        .uri("/auth/providers/corp/authorize")
                        .body(Body::empty())
                        .unwrap(),
                )
                .await
                .unwrap();
            let loc = location(&authz);
            let state_param = loc
                .split("state=")
                .nth(1)
                .unwrap()
                .split('&')
                .next()
                .unwrap()
                .to_owned();
            let cb = app
                .oneshot(
                    Request::builder()
                        .uri(format!("/auth/callback?code=good-code&state={state_param}"))
                        .body(Body::empty())
                        .unwrap(),
                )
                .await
                .unwrap();
            assert_eq!(cb.status(), StatusCode::FOUND);
            let dest = location(&cb);
            assert!(
                dest.starts_with("https://app.gt.test/auth/landed#access_token="),
                "dest: {dest}"
            );
            assert!(dest.contains("token_type=Bearer"));
            // The httpOnly auth cookies rode along.
            assert!(cb
                .headers()
                .get_all(header::SET_COOKIE)
                .iter()
                .any(|c| c.to_str().unwrap().starts_with("gt_web_token=")));
        }

        /// Without a flow/state store the public redirect endpoints are 501 (login unaffected).
        #[tokio::test]
        async fn flow_endpoints_501_without_configuration() {
            // Default state(): authz_flow + authz_state are None.
            let app = auth_router(state());
            let a = app
                .clone()
                .oneshot(
                    Request::builder()
                        .uri("/auth/providers/corp/authorize")
                        .body(Body::empty())
                        .unwrap(),
                )
                .await
                .unwrap();
            assert_eq!(a.status(), StatusCode::NOT_IMPLEMENTED);
        }
    }
}
