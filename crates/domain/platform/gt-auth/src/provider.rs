//! The login-side [`IdentityProvider`] port: turn caller-supplied [`Credentials`] into a
//! [`VerifiedIdentity`].
//!
//! ## Where this sits relative to [`Authenticator`]
//!
//! [`Authenticator`] is the *token* side — it verifies an already-minted bearer token into
//! [`JwtClaims`]. This module is the *login* side, one step earlier: a principal presents
//! credentials (email + password by default, an OAuth/OIDC handshake otherwise), a provider
//! authenticates them, and the result ([`VerifiedIdentity`]) is what the token-minting tier
//! folds into a fresh [`JwtClaims`] (via [`VerifiedIdentity::into_claims`]).
//!
//! ```text
//!   Credentials --[IdentityProvider::authenticate]--> VerifiedIdentity
//!               --[into_claims(exp, iat)]-----------> JwtClaims --[sign]--> bearer token
//!                                                                          --[Authenticator]--> JwtClaims
//! ```
//!
//! ## Providers
//!
//! [`ProviderKind`] enumerates the supported methods; **email + password is the default**
//! ([`ProviderKind::default`]). `OAuth` and `Oidc` are carried in the enum to fix the shape
//! now — their adapters land in a follow-up, so a provider that does not implement them
//! returns [`AuthError::UnsupportedProvider`] rather than silently accepting.
//!
//! The port is **synchronous**, matching [`Authenticator`]: password verification is CPU
//! work. The OAuth/OIDC adapters involve network I/O and will introduce an async variant when
//! they are actually implemented; until then the stubs reject synchronously.

use crate::{AuthError, JwtClaims};

#[cfg(test)]
use std::collections::HashMap;

/// Which authentication method backs a login attempt.
///
/// `EmailPassword` is the [default](Self::default). `OAuth`/`Oidc` reserve the shape; their
/// adapters are not implemented yet (a provider rejects them with
/// [`AuthError::UnsupportedProvider`]).
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
#[non_exhaustive]
pub enum ProviderKind {
    /// Email + password — the default provider.
    #[default]
    EmailPassword,
    /// Delegated OAuth 2.0 authorization-code flow. Shape reserved; adapter pending.
    OAuth,
    /// OpenID Connect (an `id_token` from a trusted issuer). Shape reserved; adapter pending.
    Oidc,
}

/// Caller-supplied login credentials, tagged by the [`ProviderKind`] that consumes them.
///
/// The non-default variants carry the minimal handshake inputs so the enum shape is settled
/// even before the adapters exist.
#[derive(Clone, Debug, PartialEq, Eq)]
#[non_exhaustive]
pub enum Credentials {
    /// The default: an email address and a plaintext password (verified against a stored
    /// hash by the provider — never logged, never stored as-is).
    EmailPassword {
        /// The principal's email address (the username).
        email: String,
        /// The plaintext password as presented this request.
        password: String,
    },
    /// OAuth authorization-code grant: the upstream provider name plus the code to exchange.
    OAuth {
        /// Upstream provider id (e.g. `"github"`, `"google"`).
        provider: String,
        /// The authorization code returned to the redirect URI.
        code: String,
    },
    /// An OpenID Connect `id_token` minted by a trusted issuer.
    Oidc {
        /// The issuer (`iss`) the token claims to come from.
        issuer: String,
        /// The raw `id_token` JWT.
        id_token: String,
    },
}

impl Credentials {
    /// The [`ProviderKind`] these credentials select.
    pub fn kind(&self) -> ProviderKind {
        match self {
            Credentials::EmailPassword { .. } => ProviderKind::EmailPassword,
            Credentials::OAuth { .. } => ProviderKind::OAuth,
            Credentials::Oidc { .. } => ProviderKind::Oidc,
        }
    }
}

/// The principal facts a successful login establishes — everything a [`JwtClaims`] needs
/// except the wall-clock fields (`exp`/`iat`), which the token-minting tier stamps.
///
/// Mirrors [`JwtClaims`] minus the time fields so [`into_claims`](Self::into_claims) is a
/// total, lossless widening.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct VerifiedIdentity {
    /// Subject — the authenticated principal (user / agent id).
    pub sub: String,
    /// Tenant the principal is scoped to. Carried straight through to [`JwtClaims::workspace`].
    pub workspace: String,
    /// Authorization scopes granted to the principal.
    pub scopes: Vec<String>,
}

impl VerifiedIdentity {
    /// Fold this identity into a [`JwtClaims`], stamping the `exp`/`iat` the minting tier
    /// owns. The resulting claims still pass through [`JwtClaims::validate`] downstream.
    pub fn into_claims(self, exp: u64, iat: u64) -> JwtClaims {
        JwtClaims {
            sub: self.sub,
            workspace: self.workspace,
            scopes: self.scopes,
            exp,
            nbf: None,
            iat,
        }
    }
}

/// The login port: authenticate [`Credentials`] into a [`VerifiedIdentity`].
///
/// The inverted port (hexagonal): the domain depends on this trait, not on a concrete
/// credential store or OAuth client. The default-provider adapter (email + password, real
/// argon2 hashing) lives behind this crate's off-by-default `password-hash` feature; tests use
/// [`InMemoryIdentityProvider`].
///
/// Synchronous, like [`Authenticator`]: the default path (password hash verification) is CPU
/// work. A provider that does not implement the requested [`ProviderKind`] must return
/// [`AuthError::UnsupportedProvider`] — never a false success.
pub trait IdentityProvider {
    /// Authenticate `creds`, returning the established identity or the reason it was rejected.
    fn authenticate(&self, creds: &Credentials) -> Result<VerifiedIdentity, AuthError>;
}

/// A crypto-free [`IdentityProvider`] for domain tests: an `email -> (password, identity)`
/// table standing in for a credential store + hash check. Only [`Credentials::EmailPassword`]
/// is recognized; OAuth/OIDC credentials return [`AuthError::UnsupportedProvider`], matching
/// the still-stubbed real adapters. A wrong password or unknown email is the indistinguishable
/// [`AuthError::InvalidCredentials`].
#[cfg(test)]
#[derive(Clone, Debug, Default)]
pub struct InMemoryIdentityProvider {
    users: HashMap<String, (String, VerifiedIdentity)>,
}

#[cfg(test)]
impl InMemoryIdentityProvider {
    /// A provider that recognizes no users.
    pub fn new() -> Self {
        Self::default()
    }

    /// Register `email` with `password` (compared verbatim — this is the test double) and the
    /// `identity` it resolves to. Chainable.
    pub fn with_user(
        mut self,
        email: impl Into<String>,
        password: impl Into<String>,
        identity: VerifiedIdentity,
    ) -> Self {
        self.users
            .insert(email.into(), (password.into(), identity));
        self
    }
}

#[cfg(test)]
impl IdentityProvider for InMemoryIdentityProvider {
    fn authenticate(&self, creds: &Credentials) -> Result<VerifiedIdentity, AuthError> {
        let Credentials::EmailPassword { email, password } = creds else {
            return Err(AuthError::UnsupportedProvider(creds.kind()));
        };
        match self.users.get(email) {
            Some((stored, identity)) if stored == password => Ok(identity.clone()),
            _ => Err(AuthError::InvalidCredentials),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn identity() -> VerifiedIdentity {
        VerifiedIdentity {
            sub: "alice".into(),
            workspace: "acme".into(),
            scopes: vec!["rig.read".into()],
        }
    }

    fn provider() -> InMemoryIdentityProvider {
        InMemoryIdentityProvider::new().with_user("alice@acme.test", "hunter2", identity())
    }

    #[test]
    fn default_provider_is_email_password() {
        assert_eq!(ProviderKind::default(), ProviderKind::EmailPassword);
    }

    #[test]
    fn credentials_report_their_kind() {
        let pw = Credentials::EmailPassword {
            email: "a@b.test".into(),
            password: "x".into(),
        };
        assert_eq!(pw.kind(), ProviderKind::EmailPassword);
        let oauth = Credentials::OAuth {
            provider: "github".into(),
            code: "c".into(),
        };
        assert_eq!(oauth.kind(), ProviderKind::OAuth);
    }

    #[test]
    fn authenticate_returns_identity_for_correct_password() {
        let got = provider()
            .authenticate(&Credentials::EmailPassword {
                email: "alice@acme.test".into(),
                password: "hunter2".into(),
            })
            .unwrap();
        assert_eq!(got, identity());
    }

    #[test]
    fn authenticate_rejects_a_wrong_password() {
        let err = provider().authenticate(&Credentials::EmailPassword {
            email: "alice@acme.test".into(),
            password: "nope".into(),
        });
        assert_eq!(err, Err(AuthError::InvalidCredentials));
    }

    #[test]
    fn authenticate_hides_unknown_email_behind_the_same_error() {
        // Unknown email and wrong password are indistinguishable — no user enumeration.
        let err = provider().authenticate(&Credentials::EmailPassword {
            email: "ghost@acme.test".into(),
            password: "hunter2".into(),
        });
        assert_eq!(err, Err(AuthError::InvalidCredentials));
    }

    #[test]
    fn authenticate_rejects_oauth_as_unsupported_for_now() {
        let err = provider().authenticate(&Credentials::OAuth {
            provider: "github".into(),
            code: "c".into(),
        });
        assert_eq!(err, Err(AuthError::UnsupportedProvider(ProviderKind::OAuth)));
    }

    #[test]
    fn verified_identity_widens_into_claims() {
        let claims = identity().into_claims(100, 10);
        assert_eq!(claims.sub, "alice");
        assert_eq!(claims.workspace, "acme");
        assert!(claims.has_scope("rig.read"));
        assert_eq!(claims.exp, 100);
        assert_eq!(claims.iat, 10);
        // The widened claims still pass the semantic gate.
        assert_eq!(claims.validate(50, false), Ok(()));
    }
}
