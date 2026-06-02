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
    /// The token could not be decoded into [`JwtClaims`](crate::JwtClaims) — wrong shape,
    /// not three dot-separated segments, bad base64, etc. Carries a human-readable reason.
    #[error("malformed token: {0}")]
    Malformed(String),
    /// No identity backs this token (the in-memory double saw an unminted token; a real
    /// adapter would surface this as [`InvalidSignature`](Self::InvalidSignature)).
    #[error("unknown token")]
    UnknownToken,
}
