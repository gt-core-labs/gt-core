//! Authentication identity port for gt-core.
//!
//! ## Why this crate exists
//!
//! `gt-workspace::WorkspaceContext` (the tenant-boundary extractor, `hq-mt-core.8`)
//! resolves a request's [workspace] from the `X-GT-Workspace` header today, but its module
//! docs spell out a deferred second source: *"the `X-GT-Workspace` header and a JWT claim.
//! Only the header path is implemented — the JWT/auth layer is not yet ported to gt-core (it
//! lives in gastown)."* This crate ports that missing layer up as a **Port + InMemory**
//! adapter, the first concrete carve-out of the `hq-mod-refactor.13` bins decomposition
//! (gastown `bins/gt-web/{auth,jwt,login,scope}.rs`).
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
//! - [`AuthError`] — the rejection vocabulary.
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
//! the off-by-default `password-hash` feature; tests use the in-memory double.
//!
//! A real signature-verifying adapter (jsonwebtoken HS256/RS256) folds into this same crate
//! behind an off-by-default `jsonwebtoken` feature in a follow-up — the way gt-rig/gt-quota
//! gate their `pg`/`axum` adapters — never a sibling adapter crate (Rule 4).
//!
//! This crate intentionally does **not** depend on `gt-workspace`: platform → platform deps
//! are forbidden (Rule 4). The `workspace` claim is a plain `String`; the consumer (the
//! `WorkspaceContext` extractor, wired one tier up at the composition root) parses it into a
//! `WorkspaceId` and reconciles it against the header.
//!
//! [workspace]: JwtClaims::workspace

mod error;
mod provider;
mod verify;

#[cfg(feature = "password-hash")]
pub mod password;

pub use error::AuthError;
pub use provider::{Credentials, IdentityProvider, ProviderKind, VerifiedIdentity};
pub use verify::{Authenticator, InMemoryAuthenticator};

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
    /// Issued-at, seconds since the Unix epoch (JWT `iat`).
    #[serde(default)]
    pub iat: u64,
}

impl JwtClaims {
    /// Validate the *semantic* invariants of an already-signature-verified token against the
    /// current wall clock `now_secs`.
    ///
    /// - Expired when `now_secs >= exp` ([`AuthError::Expired`]).
    /// - A blank `workspace` claim is [`AuthError::MissingWorkspace`] **unless**
    ///   `workspace_optional` is set — the `GT_JWT_WS_OPTIONAL` grace flag for the rolling
    ///   rollout described in `hq-mt-auth.1`. Drop the flag (and this branch) once every
    ///   issuer stamps the claim.
    ///
    /// Signature verification is the [`Authenticator`]'s job and has already happened by the
    /// time you hold a `JwtClaims`; this is the cheap, crypto-free second gate.
    pub fn validate(&self, now_secs: u64, workspace_optional: bool) -> Result<(), AuthError> {
        if now_secs >= self.exp {
            return Err(AuthError::Expired);
        }
        if self.workspace.trim().is_empty() && !workspace_optional {
            return Err(AuthError::MissingWorkspace);
        }
        Ok(())
    }

    /// True when `scope` is among the granted [`scopes`](Self::scopes).
    pub fn has_scope(&self, scope: &str) -> bool {
        self.scopes.iter().any(|s| s == scope)
    }
}
