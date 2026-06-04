//! The RS256 signature-verifying [`Authenticator`] — the `jsonwebtoken` adapter.
//!
//! This is the production counterpart of [`InMemoryAuthenticator`](crate::InMemoryAuthenticator):
//! it decodes a bearer token and verifies its **RS256** signature against a public key, yielding
//! [`JwtClaims`]. Per the [`Authenticator`] contract it verifies the *signature only* — the
//! wall-clock (`exp`) and `workspace`-presence gates stay [`JwtClaims::validate`]'s job, so the
//! clock is injected by the caller and the slice stays deterministic. We therefore switch
//! jsonwebtoken's own `exp` enforcement **off** and let `validate` own it.
//!
//! Asymmetric on purpose (hq-auth, RS256): the verifier holds only the **public** key, so a
//! frontend or sibling service can authenticate tokens without ever holding the minting secret.
//! Key loading by `kid` / rotation is `hq-auth-verify.2`; this module accepts a single key.
//!
//! Hexagonal (docs/03 Rule 4): this adapter lives inside `gt-auth` behind the off-by-default
//! `jsonwebtoken` feature — never a sibling crate — exactly as `password.rs` gates argon2.

use jsonwebtoken::{decode, Algorithm, DecodingKey, Validation};

use crate::{AuthError, Authenticator, JwtClaims};

/// An [`Authenticator`] that verifies an RS256-signed JWT against a public key.
///
/// Build it from a PEM/DER RSA public key ([`from_rsa_pem`](Self::from_rsa_pem) /
/// [`from_rsa_der`](Self::from_rsa_der)) or from a pre-built [`DecodingKey`]
/// ([`new`](Self::new)).
pub struct JwtAuthenticator {
    key: DecodingKey,
    validation: Validation,
}

impl JwtAuthenticator {
    /// Build the verifier from an already-constructed RS256 [`DecodingKey`].
    pub fn new(key: DecodingKey) -> Self {
        let mut validation = Validation::new(Algorithm::RS256);
        // Signature only. The `exp` clock check and the `workspace`-presence check are
        // `JwtClaims::validate`'s responsibility (the trait contract + injected clock), so we
        // disable jsonwebtoken's built-in expiry enforcement and its required-claims gate —
        // otherwise it would reject an expired token before `validate` ever sees it, and demand
        // an `exp`/`aud` we deliberately validate ourselves.
        validation.validate_exp = false;
        validation.validate_aud = false;
        validation.required_spec_claims.clear();
        Self { key, validation }
    }

    /// Build the verifier from an RSA public key in PEM form (`-----BEGIN PUBLIC KEY-----`).
    pub fn from_rsa_pem(pem: &[u8]) -> Result<Self, AuthError> {
        let key = DecodingKey::from_rsa_pem(pem)
            .map_err(|e| AuthError::Malformed(format!("invalid RS256 public key: {e}")))?;
        Ok(Self::new(key))
    }

    /// Build the verifier from an RSA public key in DER form.
    pub fn from_rsa_der(der: &[u8]) -> Self {
        Self::new(DecodingKey::from_rsa_der(der))
    }
}

impl Authenticator for JwtAuthenticator {
    fn authenticate(&self, token: &str) -> Result<JwtClaims, AuthError> {
        decode::<JwtClaims>(token, &self.key, &self.validation)
            .map(|data| data.claims)
            .map_err(map_err)
    }
}

/// Collapse a jsonwebtoken decode error into the crate's [`AuthError`] vocabulary: a failed
/// signature is [`AuthError::InvalidSignature`]; everything else (bad base64, wrong segment
/// count, JSON that is not a [`JwtClaims`], unexpected algorithm, …) is
/// [`AuthError::Malformed`] carrying the reason.
fn map_err(e: jsonwebtoken::errors::Error) -> AuthError {
    use jsonwebtoken::errors::ErrorKind;
    match e.kind() {
        ErrorKind::InvalidSignature => AuthError::InvalidSignature,
        _ => AuthError::Malformed(e.to_string()),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use jsonwebtoken::{encode, EncodingKey, Header};

    // A throwaway 2048-bit RSA keypair, generated once for the slice. Verifying against the
    // matching public key proves the signature path; signing with a *second*, unrelated key
    // proves rejection.
    const TEST_PRIV_PEM: &[u8] = include_bytes!("../tests/fixtures/rs256_priv.pem");
    const TEST_PUB_PEM: &[u8] = include_bytes!("../tests/fixtures/rs256_pub.pem");
    const OTHER_PRIV_PEM: &[u8] = include_bytes!("../tests/fixtures/rs256_other_priv.pem");

    fn claims() -> JwtClaims {
        JwtClaims {
            sub: "alice".into(),
            workspace: "acme".into(),
            scopes: vec!["rig.read".into()],
            exp: 100,
            iat: 0,
        }
    }

    fn sign(priv_pem: &[u8], claims: &JwtClaims) -> String {
        let key = EncodingKey::from_rsa_pem(priv_pem).expect("test private key");
        encode(&Header::new(Algorithm::RS256), claims, &key).expect("sign")
    }

    #[test]
    fn verifies_a_well_signed_token_into_its_claims() {
        let token = sign(TEST_PRIV_PEM, &claims());
        let auth = JwtAuthenticator::from_rsa_pem(TEST_PUB_PEM).unwrap();
        let got = auth.authenticate(&token).unwrap();
        assert_eq!(got.sub, "alice");
        assert_eq!(got.workspace, "acme");
        assert!(got.has_scope("rig.read"));
    }

    #[test]
    fn rejects_a_token_signed_by_a_different_key() {
        let token = sign(OTHER_PRIV_PEM, &claims());
        let auth = JwtAuthenticator::from_rsa_pem(TEST_PUB_PEM).unwrap();
        assert_eq!(auth.authenticate(&token), Err(AuthError::InvalidSignature));
    }

    #[test]
    fn rejects_a_garbage_token_as_malformed() {
        let auth = JwtAuthenticator::from_rsa_pem(TEST_PUB_PEM).unwrap();
        assert!(matches!(
            auth.authenticate("not.a.jwt"),
            Err(AuthError::Malformed(_))
        ));
    }

    #[test]
    fn verification_does_not_apply_the_clock_gate() {
        // exp is in the past, but `authenticate` only checks the signature — an expired token
        // still decodes here; the rejection is `JwtClaims::validate`'s job.
        let mut c = claims();
        c.exp = 1;
        let token = sign(TEST_PRIV_PEM, &c);
        let auth = JwtAuthenticator::from_rsa_pem(TEST_PUB_PEM).unwrap();
        let got = auth.authenticate(&token).expect("decodes despite being expired");
        assert_eq!(got.validate(50, false), Err(AuthError::Expired));
    }

    #[test]
    fn a_malformed_public_key_is_rejected_at_construction() {
        assert!(matches!(
            JwtAuthenticator::from_rsa_pem(b"-----BEGIN PUBLIC KEY-----\nnope\n-----END PUBLIC KEY-----"),
            Err(AuthError::Malformed(_))
        ));
    }
}
