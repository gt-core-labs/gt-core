//! The authentication rejection vocabulary.

use thiserror::Error;

/// Why a bearer token was rejected — by signature verification ([`Authenticator`]) or by the
/// semantic [`validate`] gate.
///
/// [`Authenticator`]: crate::Authenticator
/// [`validate`]: crate::JwtClaims::validate
#[derive(Debug, Clone, PartialEq, Eq, Error)]
#[non_exhaustive]
pub enum AuthError {
    /// The token is well-formed and correctly signed but past its `exp`.
    #[error("token expired")]
    Expired,
    /// The token carries no `workspace` claim and the grace window is closed
    /// (`GT_JWT_WS_OPTIONAL` unset). See [`JwtClaims::validate`](crate::JwtClaims::validate).
    #[error("token is missing the required workspace claim")]
    MissingWorkspace,
    /// The signature did not verify against the configured key.
    #[error("token signature is invalid")]
    InvalidSignature,
    /// The token names a signing key (`kid`) the verifier does not hold — or carries no `kid`
    /// and the verifier has no unambiguous key to fall back on. Distinct from
    /// [`InvalidSignature`](Self::InvalidSignature): the token may be perfectly valid, but this
    /// verifier cannot establish that without the matching public key. Carries the requested
    /// `kid` (empty when the token had none). Raised by the RS256 keyset adapter.
    #[error("no verification key for kid {0:?}")]
    UnknownKey(String),
    /// The token could not be decoded into [`JwtClaims`](crate::JwtClaims) — wrong shape,
    /// not three dot-separated segments, bad base64, etc. Carries a human-readable reason.
    #[error("malformed token: {0}")]
    Malformed(String),
    /// No identity backs this token (the in-memory double saw an unminted token; a real
    /// adapter would surface this as [`InvalidSignature`](Self::InvalidSignature)).
    #[error("unknown token")]
    UnknownToken,
    /// Login was rejected: the email is unknown **or** the password did not match. Kept
    /// deliberately indistinguishable so callers cannot enumerate users. Raised by an
    /// [`IdentityProvider`](crate::IdentityProvider).
    #[error("invalid email or password")]
    InvalidCredentials,
    /// The requested authentication provider is not implemented yet (OAuth/OIDC are
    /// shape-reserved; only [`ProviderKind::EmailPassword`](crate::ProviderKind::EmailPassword)
    /// is served today). Carries the kind that was asked for.
    #[error("authentication provider not supported: {0:?}")]
    UnsupportedProvider(crate::ProviderKind),
    /// Password hashing or PHC-hash parsing failed — a crypto/stored-data error, not a wrong
    /// password. Carries a human-readable reason. Only raised by the `password-hash` adapter.
    #[error("password hashing failure: {0}")]
    HashFailure(String),
}
