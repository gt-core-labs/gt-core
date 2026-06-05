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
use axum::extract::{Extension, State};
#[cfg(feature = "pg")]
use axum::extract::Path;
use axum::http::{header, HeaderMap, HeaderValue, StatusCode};
use axum::response::{IntoResponse, Response};
use axum::routing::{get, post};
use axum::{Json, Router};
use serde::{Deserialize, Serialize};

use crate::{
    AuthError, Credentials, JwkSet, JwtClaims, JwtMinter, ProviderKind, RefreshError, RefreshRecord,
    RefreshStore, RefreshToken, VerifiedIdentity,
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
        Ok(RefreshStore::issue(self, sub, workspace, scopes, issued_at, exp))
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
    /// The workspace-membership directory behind `GET /auth/workspaces` + `POST /auth/switch`
    /// (hq-identity.3). `None` ⇒ those endpoints respond `501`; login + admin are unaffected.
    pub memberships: Option<Arc<dyn MembershipDirectory>>,
    /// The workspace-membership ADMIN surface behind `POST`/`DELETE
    /// `/auth/workspaces/{slug}/members` (hq-platform-hardening.2): a ws admin attaches/detaches
    /// another user. `None` ⇒ those endpoints respond `501`; everything else is unaffected.
    pub membership_admin: Option<Arc<dyn MembershipAdmin>>,
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
    )),
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
        SwitchRequest,
        WorkspaceMembership,
        AddMemberRequest,
    )),
)]
struct AdminApiDoc;

/// The full `/auth/*` OpenAPI the composition root fuses into `GET /openapi.json`: the always-on
/// login surface ([`ApiDoc`]) plus, when the `pg` adapter is compiled, the admin / RBAC /
/// workspace routes ([`AdminApiDoc`]).
pub fn auth_openapi() -> utoipa::openapi::OpenApi {
    use utoipa::OpenApi as _;
    #[allow(unused_mut)]
    let mut doc = ApiDoc::openapi();
    #[cfg(feature = "pg")]
    doc.merge(AdminApiDoc::openapi());
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
            LoginRequest { code: Some(code), provider_id, provider, .. } => {
                Ok(Credentials::OAuth {
                    // `provider_id` (the `oauth_providers` PK) wins; `provider` is the pre-DB alias.
                    provider: provider_id.or(provider).unwrap_or_default(),
                    code,
                })
            }
            LoginRequest { id_token: Some(id_token), issuer: Some(issuer), .. } => {
                Ok(Credentials::Oidc { issuer, id_token })
            }
            LoginRequest { email: Some(email), password: Some(password), .. } => {
                Ok(Credentials::EmailPassword { email, password })
            }
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
        ProviderKind::OAuth | ProviderKind::Oidc => {
            state.oauth_login.as_ref().ok_or(ApiError::OauthNotConfigured)?
        }
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
        let _ = state.refresh.revoke_by_token(&RefreshToken::new(token)).await;
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
/// membership directory is configured. The list is keyed by the token's `sub`, so a caller only
/// ever sees their own memberships.
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
    Ok(Json(dir.list(&claims.sub).await?))
}

/// `POST /auth/switch` `{workspace}` — re-target the session to another of the user's workspaces
/// (hq-identity.3). Re-mints the access + refresh pair (and cookies) with that workspace active and
/// its role-expanded scopes. `403` when the user is not a member of the requested workspace —
/// the active tenant is resolved server-side from membership, never granted on request. `401`
/// without claims; `501` when no directory is configured.
#[cfg_attr(feature = "axum", utoipa::path(
    post, path = "/auth/switch", tag = "auth",
    request_body = SwitchRequest,
    responses(
        (status = 200, description = "Re-targeted — a fresh access + refresh pair scoped to the new workspace", body = TokenResponse),
        (status = 401, description = "No verified claims"),
        (status = 403, description = "Caller is not a member of the requested workspace"),
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
    let identity = dir
        .resolve(&claims.sub, &body.workspace)
        .await?
        .ok_or(ApiError::Forbidden)?;
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
    let admin = state.membership_admin.as_ref().ok_or(ApiError::NotConfigured)?;
    let now = (state.now)();
    match admin.add_member(&body.email, &slug, &body.role, now).await? {
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
    let admin = state.membership_admin.as_ref().ok_or(ApiError::NotConfigured)?;
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
        Json(RoleSummary { name: body.name, scopes: body.scopes, created_at: now as i64 }),
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

/// Gate an admin endpoint: the caller must carry verified claims whose scopes include `needed`
/// or the `*` wildcard. No claims ⇒ `401`; claims without the scope ⇒ `403`.
#[cfg(feature = "pg")]
fn require_scope(claims: Option<&JwtClaims>, needed: &str) -> Result<(), ApiError> {
    let claims = claims.ok_or(ApiError::Unauthenticated)?;
    let ok = claims
        .scopes
        .iter()
        .any(|s| s == "*" || s == needed);
    ok.then_some(()).ok_or(ApiError::Forbidden)
}

/// Gate a membership-admin endpoint on the caller being an ADMIN OF the target `workspace`
/// (hq-platform-hardening.2). Unlike [`require_scope`], a scope alone is not enough: the caller's
/// token must ALSO be active in `workspace`, so an admin of tenant A cannot manage tenant B. The
/// admin grant is `workspace.admin` or the `*` wildcard (the role the workspace seeds its creator
/// with). No claims ⇒ `401`; wrong workspace or missing grant ⇒ `403`.
#[cfg(feature = "pg")]
fn require_workspace_admin(claims: Option<&JwtClaims>, workspace: &str) -> Result<(), ApiError> {
    let claims = claims.ok_or(ApiError::Unauthenticated)?;
    let is_admin = claims.workspace == workspace
        && claims
            .scopes
            .iter()
            .any(|s| s == "*" || s == "workspace.admin");
    is_admin.then_some(()).ok_or(ApiError::Forbidden)
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
        build_cookie(state, ACCESS_COOKIE, &tokens.access_token, "/", state.access_ttl as i64),
    );
    headers.append(
        header::SET_COOKIE,
        build_cookie(state, REFRESH_COOKIE, &tokens.refresh_token, "/auth", state.refresh_ttl as i64),
    );
    headers
}

/// The `Set-Cookie` header pair that expires both auth cookies (`Max-Age=0`, empty value) on
/// logout — same name/path as [`set_token_cookies`], which is what makes the browser drop them.
fn clear_token_cookies(state: &AuthState) -> HeaderMap {
    let mut headers = HeaderMap::new();
    headers.append(header::SET_COOKIE, build_cookie(state, ACCESS_COOKIE, "", "/", 0));
    headers.append(header::SET_COOKIE, build_cookie(state, REFRESH_COOKIE, "", "/auth", 0));
    headers
}

/// Build one `Set-Cookie` value. `value` is a JWT or opaque token (ASCII, no cookie-special
/// chars), so it needs no escaping.
fn build_cookie(state: &AuthState, name: &str, value: &str, path: &str, max_age: i64) -> HeaderValue {
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
            AuthError::HashFailure(_)
            | AuthError::SigningFailure(_)
            | AuthError::Backend(_) => StatusCode::INTERNAL_SERVER_ERROR,
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
    #[cfg(feature = "pg")]
    Forbidden,
    /// The endpoint needs a backing store the deploy did not configure — `501` (`hq-web-extras.5`).
    #[cfg(feature = "pg")]
    NotConfigured,
    /// A role's scope failed the closed-vocabulary check — `400` (hq-rbac.4).
    #[cfg(feature = "pg")]
    BadScope(String),
    /// The addressed role/user does not exist — `404` (hq-rbac.4).
    #[cfg(feature = "pg")]
    NotFound,
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
            ApiError::OauthNotConfigured => {
                (StatusCode::NOT_IMPLEMENTED, "oauth/oidc login is not configured").into_response()
            }
            #[cfg(feature = "pg")]
            ApiError::Forbidden => {
                (StatusCode::FORBIDDEN, "insufficient scope").into_response()
            }
            #[cfg(feature = "pg")]
            ApiError::NotConfigured => {
                (StatusCode::NOT_IMPLEMENTED, "user administration is not configured").into_response()
            }
            #[cfg(feature = "pg")]
            ApiError::BadScope(msg) => (StatusCode::BAD_REQUEST, msg).into_response(),
            #[cfg(feature = "pg")]
            ApiError::NotFound => (StatusCode::NOT_FOUND, "not found").into_response(),
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
        for expected in ["/auth/login", "/auth/refresh", "/auth/logout", "/auth/me", "/auth/jwks"] {
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
        async fn upsert_role(&self, name: &str, scopes: &[String], now: u64) -> Result<(), AuthError> {
            let mut roles = self.roles.lock().unwrap();
            roles.retain(|r| r.name != name);
            roles.push(RoleSummary { name: name.into(), scopes: scopes.to_vec(), created_at: now as i64 });
            Ok(())
        }
        async fn list_roles(&self) -> Result<Vec<RoleSummary>, AuthError> {
            Ok(self
                .roles
                .lock()
                .unwrap()
                .iter()
                .map(|r| RoleSummary { name: r.name.clone(), scopes: r.scopes.clone(), created_at: r.created_at })
                .collect())
        }
        async fn delete_role(&self, name: &str) -> Result<bool, AuthError> {
            let mut roles = self.roles.lock().unwrap();
            let before = roles.len();
            roles.retain(|r| r.name != name);
            Ok(roles.len() != before)
        }
        async fn assign_user_roles(&self, email: &str, roles: &[String], _now: u64) -> Result<bool, AuthError> {
            if !self.known_users.iter().any(|e| e == email) {
                return Ok(false);
            }
            self.assigned.lock().unwrap().push((email.into(), roles.to_vec()));
            Ok(true)
        }
    }

    /// Build state whose role store is the given [`MemRoles`] double; everything else as [`state`].
    fn state_with_roles(roles: Arc<MemRoles>) -> AuthState {
        AuthState { roles: Some(roles), ..state() }
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
            .body(json.map(|j| Body::from(j.to_owned())).unwrap_or_else(Body::empty))
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
        assert_eq!(admin_request(&app, "POST", "/auth/roles", None, Some(body)).await, StatusCode::UNAUTHORIZED);
        assert_eq!(
            admin_request(&app, "POST", "/auth/roles", Some(&["roles.read"]), Some(body)).await,
            StatusCode::FORBIDDEN
        );
        assert_eq!(
            admin_request(&app, "POST", "/auth/roles", Some(&["roles.write"]), Some(body)).await,
            StatusCode::CREATED
        );
        assert_eq!(roles.roles.lock().unwrap().len(), 1);

        // A scope outside the closed vocabulary is a 400, never persisted.
        let bad = r#"{"name":"oops","scopes":["merge.frobnicate"]}"#;
        assert_eq!(
            admin_request(&app, "POST", "/auth/roles", Some(&["roles.write"]), Some(bad)).await,
            StatusCode::BAD_REQUEST
        );
        assert_eq!(roles.roles.lock().unwrap().len(), 1, "bad scope did not persist");

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
            admin_request(&app, "DELETE", "/auth/roles/ghost-check", Some(&["roles.write"]), None).await,
            StatusCode::NO_CONTENT
        );
        assert_eq!(
            admin_request(&app, "DELETE", "/auth/roles/ghost-check", Some(&["roles.write"]), None).await,
            StatusCode::NOT_FOUND
        );
    }

    #[tokio::test]
    async fn assigning_roles_needs_users_write_and_404s_unknown_user() {
        let roles = Arc::new(MemRoles { known_users: vec!["alice@acme.test".into()], ..Default::default() });
        let app = auth_router(state_with_roles(roles.clone()));
        let body = r#"{"roles":["reviewer"]}"#;

        // Gated on users.write (assignment is a write to the user record).
        assert_eq!(
            admin_request(&app, "POST", "/auth/users/alice@acme.test/roles", Some(&["roles.write"]), Some(body)).await,
            StatusCode::FORBIDDEN
        );
        assert_eq!(
            admin_request(&app, "POST", "/auth/users/alice@acme.test/roles", Some(&["users.write"]), Some(body)).await,
            StatusCode::NO_CONTENT
        );
        // Unknown user → 404.
        assert_eq!(
            admin_request(&app, "POST", "/auth/users/ghost@acme.test/roles", Some(&["users.write"]), Some(body)).await,
            StatusCode::NOT_FOUND
        );
        assert_eq!(roles.assigned.lock().unwrap().as_slice(), &[("alice@acme.test".to_string(), vec!["reviewer".to_string()])]);
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
            memberships: None,
            membership_admin: None,
            // Publish the public half of the same "k1" key the minter signs with.
            jwks: Arc::new(
                JwtAuthenticator::from_kid_pems([("k1", PUB_PEM)]).unwrap().jwk_set(),
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
        let bytes = axum::body::to_bytes(resp.into_body(), usize::MAX).await.unwrap();
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
        let bytes = axum::body::to_bytes(resp.into_body(), usize::MAX).await.unwrap();
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
        let (status, _) = post(
            &app,
            "/auth/login",
            r#"{"provider":"github","code":"abc"}"#,
        )
        .await;
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
        async fn userinfo_handler(headers: HeaderMap) -> Result<AxJson<serde_json::Value>, StatusCode> {
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
        let listener = tokio::net::TcpListener::bind(("127.0.0.1", 0)).await.unwrap();
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
        let app = auth_router(AuthState { oauth_login: Some(oauth), ..state() });

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

        let (status, body) = post(&app, "/auth/refresh", &format!(r#"{{"refresh_token":"{refresh1}"}}"#)).await;
        assert_eq!(status, StatusCode::OK);
        let (_, refresh2) = token_pair(&body);
        assert_ne!(refresh1, refresh2); // rotated

        // Reusing the now-rotated first token is rejected (and burns the family).
        let (status, _) = post(&app, "/auth/refresh", &format!(r#"{{"refresh_token":"{refresh1}"}}"#)).await;
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

        let (status, _) = post(&app, "/auth/logout", &format!(r#"{{"refresh_token":"{refresh}"}}"#)).await;
        assert_eq!(status, StatusCode::NO_CONTENT);

        // The revoked token can no longer be refreshed.
        let (status, _) = post(&app, "/auth/refresh", &format!(r#"{{"refresh_token":"{refresh}"}}"#)).await;
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
        let mut req = Request::builder().uri("/auth/me").body(Body::empty()).unwrap();
        req.extensions_mut().insert(claims);
        let resp = app.clone().oneshot(req).await.unwrap();
        assert_eq!(resp.status(), StatusCode::OK);
        let bytes = axum::body::to_bytes(resp.into_body(), usize::MAX).await.unwrap();
        let body = String::from_utf8(bytes.to_vec()).unwrap();
        assert!(body.contains(r#""sub":"alice""#) && body.contains(r#""workspace":"acme""#));

        // Without claims → 401.
        let resp = app
            .oneshot(Request::builder().uri("/auth/me").body(Body::empty()).unwrap())
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
        let access = cookies.iter().find(|c| c.starts_with("gt_web_token=")).unwrap();
        let refresh = cookies.iter().find(|c| c.starts_with("gt_refresh=")).unwrap();
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
        assert!(set_cookies(&resp).iter().any(|c| c.starts_with("gt_refresh=")));
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
        assert!(cookies.iter().any(|c| c.starts_with("gt_web_token=") && c.contains("Max-Age=0")));
        assert!(cookies.iter().any(|c| c.starts_with("gt_refresh=") && c.contains("Max-Age=0")));
    }

    // --- hq-identity.3: GET /auth/workspaces + POST /auth/switch ---------------------------------

    /// In-memory membership directory double: `sub -> [(workspace, role, scopes)]`. `list` projects
    /// the slug+role; `resolve` finds the held workspace and yields the identity to re-mint (the
    /// scopes it carries stand in for that tenant's role expansion).
    #[derive(Default)]
    struct MemDirectory {
        by_sub: std::collections::HashMap<String, Vec<(String, String, Vec<String>)>>,
    }

    #[async_trait]
    impl MembershipDirectory for MemDirectory {
        async fn list(&self, sub: &str) -> Result<Vec<WorkspaceMembership>, AuthError> {
            Ok(self
                .by_sub
                .get(sub)
                .into_iter()
                .flatten()
                .map(|(w, r, _)| WorkspaceMembership { workspace: w.clone(), role: r.clone() })
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
        AuthState { memberships: Some(dir), ..state() }
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
            self.removed.lock().unwrap().push((email.into(), workspace.into()));
            Ok(self.known_emails.iter().any(|e| e == email))
        }
    }

    fn state_with_membership_admin(admin: Arc<MemAdmin>) -> AuthState {
        AuthState { membership_admin: Some(admin), ..state() }
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
            .body(json.map(|j| Body::from(j.to_owned())).unwrap_or_else(Body::empty))
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
        let admin = Arc::new(MemAdmin { known_emails: vec!["bob@acme.test".into()], ..Default::default() });
        let app = auth_router(state_with_membership_admin(admin.clone()));
        let path = "/auth/workspaces/acme/members";
        let body = r#"{"email":"bob@acme.test","role":"member"}"#;

        // No claims → 401.
        let resp = app.clone().oneshot(claim_req("POST", path, None, Some(body))).await.unwrap();
        assert_eq!(resp.status(), StatusCode::UNAUTHORIZED);

        // A member of acme WITHOUT the admin grant → 403 (a normal user cannot add).
        let resp = app
            .clone()
            .oneshot(claim_req("POST", path, Some(("acme", &["beads.read"])), Some(body)))
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
            .oneshot(claim_req("POST", path, Some(("acme", &["workspace.admin"])), Some(body)))
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
        let admin = Arc::new(MemAdmin { known_emails: vec!["bob@acme.test".into()], ..Default::default() });
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
                ("ida".to_string(), "lead".to_string(), vec!["beads.read".to_string()]),
                ("idb".to_string(), "lead".to_string(), vec!["rig.read".to_string()]),
            ],
        );
        Arc::new(MemDirectory { by_sub })
    }

    /// Build a request to `path` with optional verified claims (`sub`/`workspace`) and JSON body.
    fn authed_req(method: &str, path: &str, sub: Option<&str>, json: Option<&str>) -> Request<Body> {
        let mut builder = Request::builder().method(method).uri(path);
        if json.is_some() {
            builder = builder.header("content-type", "application/json");
        }
        let mut req = builder
            .body(json.map(|j| Body::from(j.to_owned())).unwrap_or_else(Body::empty))
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
        let bytes = axum::body::to_bytes(resp.into_body(), usize::MAX).await.unwrap();
        let got: Vec<WorkspaceMembership> = serde_json::from_slice(&bytes).unwrap();
        assert_eq!(
            got,
            vec![
                WorkspaceMembership { workspace: "ida".into(), role: "lead".into() },
                WorkspaceMembership { workspace: "idb".into(), role: "lead".into() },
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
            .oneshot(authed_req("POST", "/auth/switch", Some("gwen"), Some(r#"{"workspace":"idb"}"#)))
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::OK);
        // The cookies are re-stamped on switch.
        let cookies = set_cookies(&resp);
        assert!(cookies.iter().any(|c| c.starts_with("gt_web_token=")));
        // The re-minted access token names idb + that tenant's scopes — proving the server
        // resolved the active workspace from membership, not from the prior claim (ida).
        let bytes = axum::body::to_bytes(resp.into_body(), usize::MAX).await.unwrap();
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
            .oneshot(authed_req("POST", "/auth/switch", Some("gwen"), Some(r#"{"workspace":"ghost"}"#)))
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::FORBIDDEN);
    }

    #[tokio::test]
    async fn switch_without_claims_is_unauthorized() {
        let app = auth_router(state_with_memberships(gwen_directory()));
        let resp = app
            .oneshot(authed_req("POST", "/auth/switch", None, Some(r#"{"workspace":"idb"}"#)))
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::UNAUTHORIZED);
    }
}
