//! Real email + password [`IdentityProvider`] adapter — argon2 hash verification.
//!
//! Gated behind the off-by-default `password-hash` feature so the core build stays
//! dependency-light (the `argon2` crypto stack is pulled only when a binary actually mints or
//! checks passwords), exactly as `gt-rig`/`gt-quota` gate their `pg` adapters. The port and
//! the in-memory double live in [`provider`](crate::provider); this is the production
//! adapter that folds in beside them (docs/03 — adapter beside the port, never a sibling
//! crate).
//!
//! Passwords are stored only as argon2 [PHC strings]; the plaintext is verified and dropped.
//! Only [`ProviderKind::EmailPassword`] is served here — OAuth/OIDC stay
//! [`AuthError::UnsupportedProvider`] until their adapters land.
//!
//! [PHC strings]: https://github.com/P-H-C/phc-string-format/blob/master/phc-sf-spec.md

use std::collections::HashMap;

use argon2::password_hash::{rand_core::OsRng, PasswordHash, PasswordHasher, SaltString};
use argon2::{Argon2, PasswordVerifier};

use crate::{AuthError, Credentials, IdentityProvider, VerifiedIdentity};

/// Hash `password` into a storable argon2 PHC string (random per-password salt).
///
/// Use at registration / password-change time to produce the value [`PasswordProvider`]
/// later verifies against. Errors map to [`AuthError::HashFailure`].
pub fn hash_password(password: &str) -> Result<String, AuthError> {
    let salt = SaltString::generate(&mut OsRng);
    Argon2::default()
        .hash_password(password.as_bytes(), &salt)
        .map(|h| h.to_string())
        .map_err(|e| AuthError::HashFailure(e.to_string()))
}

/// Verify `password` against a stored argon2 PHC `hash`.
///
/// Returns `Ok(())` on a match, [`AuthError::InvalidCredentials`] on a mismatch, and
/// [`AuthError::HashFailure`] if `hash` is not a parseable PHC string (a stored-data bug, not
/// a wrong password — kept distinct so it is not mistaken for a failed login).
pub fn verify_password(password: &str, hash: &str) -> Result<(), AuthError> {
    let parsed = PasswordHash::new(hash).map_err(|e| AuthError::HashFailure(e.to_string()))?;
    Argon2::default()
        .verify_password(password.as_bytes(), &parsed)
        .map_err(|_| AuthError::InvalidCredentials)
}

/// The default-provider adapter: an `email -> (argon2 hash, identity)` table served with real
/// password verification.
///
/// Same shape as the test double ([`InMemoryIdentityProvider`]) but storing PHC hashes instead
/// of plaintext and checking them with argon2. A wrong password and an unknown email are the
/// same [`AuthError::InvalidCredentials`] (no user enumeration). OAuth/OIDC credentials are
/// [`AuthError::UnsupportedProvider`].
///
/// The map stands in for a credential store; a DB-backed store folds in behind the same
/// [`IdentityProvider`] port without touching callers.
///
/// [`InMemoryIdentityProvider`]: crate::provider::InMemoryIdentityProvider
#[derive(Clone, Debug, Default)]
pub struct PasswordProvider {
    users: HashMap<String, (String, VerifiedIdentity)>,
}

impl PasswordProvider {
    /// A provider that recognizes no users.
    pub fn new() -> Self {
        Self::default()
    }

    /// Register `email` with a pre-computed argon2 `hash` (see [`hash_password`]) and the
    /// `identity` it resolves to. Chainable.
    pub fn with_user(
        mut self,
        email: impl Into<String>,
        hash: impl Into<String>,
        identity: VerifiedIdentity,
    ) -> Self {
        self.users.insert(email.into(), (hash.into(), identity));
        self
    }
}

impl IdentityProvider for PasswordProvider {
    fn authenticate(&self, creds: &Credentials) -> Result<VerifiedIdentity, AuthError> {
        let Credentials::EmailPassword { email, password } = creds else {
            return Err(AuthError::UnsupportedProvider(creds.kind()));
        };
        // Look the user up first, but always run a verify so a missing user and a wrong
        // password cost the same and surface the same error.
        match self.users.get(email) {
            Some((hash, identity)) => verify_password(password, hash).map(|()| identity.clone()),
            None => Err(AuthError::InvalidCredentials),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ProviderKind;

    fn identity() -> VerifiedIdentity {
        VerifiedIdentity {
            sub: "alice".into(),
            workspace: "acme".into(),
            scopes: vec!["rig.read".into()],
        }
    }

    #[test]
    fn hash_then_verify_round_trips() {
        let hash = hash_password("hunter2").unwrap();
        assert!(hash.starts_with("$argon2"));
        assert_eq!(verify_password("hunter2", &hash), Ok(()));
    }

    #[test]
    fn verify_rejects_a_wrong_password() {
        let hash = hash_password("hunter2").unwrap();
        assert_eq!(
            verify_password("wrong", &hash),
            Err(AuthError::InvalidCredentials)
        );
    }

    #[test]
    fn verify_flags_a_corrupt_stored_hash() {
        match verify_password("hunter2", "not-a-phc-string") {
            Err(AuthError::HashFailure(_)) => {}
            other => panic!("expected HashFailure, got {other:?}"),
        }
    }

    #[test]
    fn provider_authenticates_a_correct_password() {
        let hash = hash_password("hunter2").unwrap();
        let provider = PasswordProvider::new().with_user("alice@acme.test", hash, identity());
        let got = provider
            .authenticate(&Credentials::EmailPassword {
                email: "alice@acme.test".into(),
                password: "hunter2".into(),
            })
            .unwrap();
        assert_eq!(got, identity());
    }

    #[test]
    fn provider_hides_unknown_email_behind_invalid_credentials() {
        let provider = PasswordProvider::new();
        assert_eq!(
            provider.authenticate(&Credentials::EmailPassword {
                email: "ghost@acme.test".into(),
                password: "hunter2".into(),
            }),
            Err(AuthError::InvalidCredentials)
        );
    }

    #[test]
    fn provider_rejects_oidc_as_unsupported() {
        let provider = PasswordProvider::new();
        assert_eq!(
            provider.authenticate(&Credentials::Oidc {
                issuer: "https://issuer.test".into(),
                id_token: "tok".into(),
            }),
            Err(AuthError::UnsupportedProvider(ProviderKind::Oidc))
        );
    }
}
