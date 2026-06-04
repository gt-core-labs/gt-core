//! The RS256 signature-**minting** [`JwtMinter`] — the signing counterpart to
//! [`JwtAuthenticator`](crate::JwtAuthenticator).
//!
//! Where the verifier holds the **public** key and decodes a bearer token into [`JwtClaims`],
//! this adapter holds the **private** key and encodes a fresh [`JwtClaims`] into a signed
//! RS256 bearer token. Asymmetric on purpose (hq-auth, RS256): only the minting tier ever
//! holds the secret; verifiers downstream authenticate with the matching public key alone.
//!
//! ## Key rotation by `kid` (hq-auth-verify.2)
//!
//! Every minted token's header carries this minter's `kid` (key id), so a verifier holding a
//! `kid`-indexed keyset (see [`JwtAuthenticator::from_kid_pems`](crate::JwtAuthenticator)) can
//! select the matching public key. Rotating keys is then a no-flag-day affair: stand up a
//! minter under a new `kid`, publish the new public key, retire the old `kid` after the
//! deprecation window.
//!
//! Hexagonal (docs/03 Rule 4): this adapter lives inside `gt-auth` behind the off-by-default
//! `jsonwebtoken` feature — never a sibling crate — exactly as `jwt.rs` gates the verifier.

use jsonwebtoken::{encode, Algorithm, EncodingKey, Header};

use crate::{AuthError, JwtClaims, VerifiedIdentity};

/// Mints RS256-signed JWTs from a single RSA **private** key, stamping a `kid` into every
/// token header so the verifier can select the matching public key for rotation.
///
/// The signing counterpart of [`JwtAuthenticator`](crate::JwtAuthenticator): build it from a
/// PEM private key ([`from_rsa_pem`](Self::from_rsa_pem)), then [`mint`](Self::mint) claims —
/// or fold a [`VerifiedIdentity`] straight into a token with
/// [`mint_identity`](Self::mint_identity).
pub struct JwtMinter {
    /// The RS256 private signing key.
    key: EncodingKey,
    /// The key id stamped into every minted token's header (`kid`), so a rotating verifier can
    /// pick the matching public key.
    kid: String,
}

impl JwtMinter {
    /// Build the minter from an RSA private key in PEM form (`-----BEGIN PRIVATE KEY-----` /
    /// `-----BEGIN RSA PRIVATE KEY-----`), stamping `kid` into every token it mints.
    pub fn from_rsa_pem(kid: impl Into<String>, pem: &[u8]) -> Result<Self, AuthError> {
        let key = EncodingKey::from_rsa_pem(pem)
            .map_err(|e| AuthError::Malformed(format!("invalid RS256 private key: {e}")))?;
        Ok(Self {
            key,
            kid: kid.into(),
        })
    }

    /// Encode `claims` into an RS256-signed bearer token whose header carries this minter's
    /// `kid`. A jsonwebtoken encode failure surfaces as [`AuthError::Malformed`].
    pub fn mint(&self, claims: &JwtClaims) -> Result<String, AuthError> {
        let mut header = Header::new(Algorithm::RS256);
        header.kid = Some(self.kid.clone());
        encode(&header, claims, &self.key)
            .map_err(|e| AuthError::Malformed(format!("failed to mint RS256 token: {e}")))
    }

    /// Fold a [`VerifiedIdentity`] into [`JwtClaims`] (stamping `exp`/`iat`) and mint a token
    /// for it. The convenience path from the login tier ([`IdentityProvider`](crate::IdentityProvider))
    /// straight to a signed bearer token.
    pub fn mint_identity(
        &self,
        identity: VerifiedIdentity,
        exp: u64,
        iat: u64,
    ) -> Result<String, AuthError> {
        self.mint(&identity.into_claims(exp, iat))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{Authenticator, JwtAuthenticator};

    // The same throwaway 2048-bit RSA keypair the verifier's tests use: minting with the
    // private half and verifying with the public half proves the sign+verify interop end to
    // end. The second keypair stands in as an unrelated `kid` for the rotation tests.
    const TEST_PRIV_PEM: &[u8] = include_bytes!("../tests/fixtures/rs256_priv.pem");
    const TEST_PUB_PEM: &[u8] = include_bytes!("../tests/fixtures/rs256_pub.pem");
    const OTHER_PUB_PEM: &[u8] = include_bytes!("../tests/fixtures/rs256_other_pub.pem");

    fn claims() -> JwtClaims {
        JwtClaims {
            sub: "alice".into(),
            workspace: "acme".into(),
            scopes: vec!["rig.read".into(), "rig.write".into()],
            exp: 100,
            nbf: None,
            iat: 0,
        }
    }

    #[test]
    fn minted_token_round_trips_through_the_verifier() {
        let minter = JwtMinter::from_rsa_pem("k1", TEST_PRIV_PEM).unwrap();
        let token = minter.mint(&claims()).unwrap();

        let auth = JwtAuthenticator::from_kid_pems([("k1", TEST_PUB_PEM)]).unwrap();
        let got = auth.authenticate(&token).unwrap();

        assert_eq!(got.sub, "alice");
        assert_eq!(got.workspace, "acme");
        assert_eq!(got.scopes, vec!["rig.read".to_string(), "rig.write".to_string()]);
        assert!(got.has_scope("rig.read"));
        assert!(got.has_scope("rig.write"));
    }

    #[test]
    fn minted_token_header_carries_the_kid() {
        let minter = JwtMinter::from_rsa_pem("k1", TEST_PRIV_PEM).unwrap();
        let token = minter.mint(&claims()).unwrap();

        // A verifier holding only kid "k1" accepts it — the token's header selects that key.
        let k1 = JwtAuthenticator::from_kid_pems([("k1", TEST_PUB_PEM)]).unwrap();
        assert_eq!(k1.authenticate(&token).unwrap().sub, "alice");

        // A verifier holding only kid "k2" rejects it as an unknown key — the header's kid does
        // not resolve, so the signature is never even checked.
        let k2 = JwtAuthenticator::from_kid_pems([("k2", OTHER_PUB_PEM)]).unwrap();
        assert_eq!(
            k2.authenticate(&token),
            Err(AuthError::UnknownKey("k1".into()))
        );
    }

    #[test]
    fn from_rsa_pem_rejects_garbage_as_malformed() {
        assert!(matches!(
            JwtMinter::from_rsa_pem(
                "k1",
                b"-----BEGIN PRIVATE KEY-----\nnope\n-----END PRIVATE KEY-----"
            ),
            Err(AuthError::Malformed(_))
        ));
    }

    #[test]
    fn mint_identity_folds_the_identity_and_clock_into_the_token() {
        let minter = JwtMinter::from_rsa_pem("k1", TEST_PRIV_PEM).unwrap();
        let identity = VerifiedIdentity {
            sub: "bob".into(),
            workspace: "globex".into(),
            scopes: vec!["rig.read".into()],
        };
        let token = minter.mint_identity(identity, 200, 10).unwrap();

        let auth = JwtAuthenticator::from_kid_pems([("k1", TEST_PUB_PEM)]).unwrap();
        let got = auth.authenticate(&token).unwrap();

        assert_eq!(got.sub, "bob");
        assert_eq!(got.workspace, "globex");
        assert!(got.has_scope("rig.read"));
        assert_eq!(got.exp, 200);
        assert_eq!(got.iat, 10);
    }
}
