//! The RS256 token **minter** — the `jsonwebtoken` signing adapter.
//!
//! This is the production counterpart of [`JwtAuthenticator`](crate::JwtAuthenticator), and its
//! mirror image: where the verifier *decodes* a bearer token and checks its **RS256** signature
//! against a public key, the minter *encodes* a [`JwtClaims`] and **signs** it with a private
//! key, producing the bearer token. It is the final `[sign]` arrow of the login pipeline
//! ([`VerifiedIdentity::into_claims`](crate::VerifiedIdentity::into_claims) widens a verified
//! login into [`JwtClaims`]; this adapter signs those claims into the token the verifier later
//! authenticates).
//!
//! Asymmetric on purpose (hq-auth, RS256): the minter holds the **private** key — the signing
//! secret — while the verifier holds only the matching **public** key. A frontend or sibling
//! service can therefore authenticate tokens without ever being able to mint them; only the
//! issuing tier holds the secret.
//!
//! ## Key rotation by `kid` (mirror of hq-auth-verify.2)
//!
//! The minter can stamp a signing `kid` (key id) into the JWT header. The verifier's keyset
//! ([`JwtAuthenticator`](crate::JwtAuthenticator) loaded with `kid`-indexed public keys) selects
//! the matching public key by that header `kid`, so the issuing side can rotate keys without a
//! flag day: publish the new public key under a fresh `kid`, switch the minter to signing with
//! that `kid`, retire the old key after the deprecation window. A minter configured without a
//! `kid` emits a `kid`-less token (back-compat with single-key deploys, which the verifier still
//! accepts). [`from_env`](JwtMinter::from_env) loads the signing key — and its optional `kid` —
//! from the deployment's environment.
//!
//! Hexagonal (docs/03 Rule 4): this adapter lives inside `gt-auth` behind the off-by-default
//! `jsonwebtoken` feature — never a sibling crate — exactly as `jwt.rs` gates the verifier and
//! `password.rs` gates argon2.

use jsonwebtoken::{encode, Algorithm, EncodingKey, Header};

use crate::{AuthError, JwtClaims};

/// Environment variable naming the PEM file holding the RS256 **private** signing key.
pub const ENV_PRIVATE_KEY_FILE: &str = "GT_JWT_RS256_PRIVATE_KEY_FILE";
/// Environment variable naming the optional signing `kid` stamped into every minted token's
/// header, so the verifier's keyset can select the matching public key.
pub const ENV_SIGNING_KID: &str = "GT_JWT_RS256_SIGNING_KID";

/// A minter that signs a [`JwtClaims`] into an RS256-signed JWT bearer token.
///
/// Build it from a single signing key ([`new`](Self::new) / [`from_rsa_pem`](Self::from_rsa_pem)),
/// optionally tagging the signing `kid` for rotation ([`with_kid`](Self::with_kid)), or load the
/// key (and its `kid`) from the environment ([`from_env`](Self::from_env)).
///
/// The asymmetric counterpart of [`JwtAuthenticator`](crate::JwtAuthenticator): this holds the
/// **private** key and produces tokens; the verifier holds the **public** key and consumes them.
///
/// `Clone` (jsonwebtoken's `EncodingKey` is `Clone`) so a single configured minter can be shared by
/// more than one sling edge — e.g. the polecat supervisor and the trigger-driven role-agent launcher
/// (`gtcore-999795`) both mint per-agent tokens from the same daemon-held key.
#[derive(Clone)]
pub struct JwtMinter {
    /// The RS256 private key the token is signed with.
    key: EncodingKey,
    /// The signing `kid` stamped into the JWT header (`None` ⇒ a `kid`-less token).
    kid: Option<String>,
}

fn encoding_key_from_pem(pem: &[u8]) -> Result<EncodingKey, AuthError> {
    EncodingKey::from_rsa_pem(pem)
        .map_err(|e| AuthError::Malformed(format!("invalid RS256 private key: {e}")))
}

impl JwtMinter {
    /// Build the minter from an RS256 [`EncodingKey`], with no signing `kid`.
    pub fn new(key: EncodingKey) -> Self {
        Self { key, kid: None }
    }

    /// Build the minter from an RSA private key in PEM form
    /// (`-----BEGIN PRIVATE KEY-----` / `-----BEGIN RSA PRIVATE KEY-----`). A key that fails to
    /// parse is [`AuthError::Malformed`], symmetric to the verifier's
    /// [`from_rsa_pem`](crate::JwtAuthenticator::from_rsa_pem).
    pub fn from_rsa_pem(pem: &[u8]) -> Result<Self, AuthError> {
        Ok(Self::new(encoding_key_from_pem(pem)?))
    }

    /// Stamp `kid` into the header of every token this minter produces. Chainable. The verifier's
    /// keyset selects the matching public key by this `kid`.
    pub fn with_kid(mut self, kid: impl Into<String>) -> Self {
        self.kid = Some(kid.into());
        self
    }

    /// Load the minter from the deployment environment.
    ///
    /// - [`GT_JWT_RS256_PRIVATE_KEY_FILE`](ENV_PRIVATE_KEY_FILE) — the PEM file holding the RS256
    ///   private signing key. **Required**: missing or unreadable ⇒ [`AuthError::Malformed`] with
    ///   a reason naming the env var, symmetric to the verifier's
    ///   [`from_env`](crate::JwtAuthenticator::from_env).
    /// - [`GT_JWT_RS256_SIGNING_KID`](ENV_SIGNING_KID) — optional. When set, it is stamped into
    ///   the header of every minted token so the verifier's keyset can select the matching public
    ///   key (rotation). Unset ⇒ a `kid`-less token (single-key deploys).
    pub fn from_env() -> Result<Self, AuthError> {
        let path = std::env::var(ENV_PRIVATE_KEY_FILE).map_err(|_| {
            AuthError::Malformed(format!(
                "no RS256 private key configured (set {ENV_PRIVATE_KEY_FILE})"
            ))
        })?;
        let pem = std::fs::read(&path).map_err(|e| {
            AuthError::Malformed(format!(
                "cannot read {ENV_PRIVATE_KEY_FILE} key file {path}: {e}"
            ))
        })?;
        let mut minter = Self::from_rsa_pem(&pem)?;
        if let Ok(kid) = std::env::var(ENV_SIGNING_KID) {
            minter.kid = Some(kid);
        }
        Ok(minter)
    }

    /// Build the `Header::new(Algorithm::RS256)` for a minted token, stamping the signing `kid`.
    fn header(&self) -> Header {
        let mut header = Header::new(Algorithm::RS256);
        header.kid = self.kid.clone();
        header
    }

    /// Sign `claims` into an RS256-signed JWT bearer token. An encode failure (a serializer fault
    /// or a key that cannot produce a signature) is [`AuthError::SigningFailure`] — the minting
    /// mirror of the verifier collapsing a decode fault into its error vocabulary.
    pub fn mint(&self, claims: &JwtClaims) -> Result<String, AuthError> {
        encode(&self.header(), claims, &self.key)
            .map_err(|e| AuthError::SigningFailure(e.to_string()))
    }

    /// Sign an **arbitrary JSON document** into a compact RS256 JWS — the generic sibling of
    /// [`mint`](Self::mint) for payloads that are not [`JwtClaims`], e.g. the signed A2A Agent
    /// Card (gtcore-9039b5). Same private key, same `kid` stamping (rotation works identically),
    /// so a consumer verifies the document against the same public key/JWKS that verifies the
    /// platform's bearer tokens — see
    /// [`JwtAuthenticator::verify_json`](crate::JwtAuthenticator::verify_json). `payload` must be
    /// a JSON object (a JWS claims set is one); anything else is [`AuthError::SigningFailure`].
    pub fn sign_json(&self, payload: &serde_json::Value) -> Result<String, AuthError> {
        if !payload.is_object() {
            return Err(AuthError::SigningFailure(
                "JWS payload must be a JSON object".into(),
            ));
        }
        encode(&self.header(), payload, &self.key)
            .map_err(|e| AuthError::SigningFailure(e.to_string()))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{Authenticator, JwtAuthenticator};
    use jsonwebtoken::decode_header;

    // The same two throwaway 2048-bit RSA keypairs the verifier slice uses. Minting with the
    // primary private key and verifying against its public key proves the signing path;
    // minting with the unrelated key proves a real (rejectable) signature; the keys also stand
    // in as `kid`-named keys for rotation.
    const TEST_PRIV_PEM: &[u8] = include_bytes!("../tests/fixtures/rs256_priv.pem");
    const TEST_PUB_PEM: &[u8] = include_bytes!("../tests/fixtures/rs256_pub.pem");
    const OTHER_PRIV_PEM: &[u8] = include_bytes!("../tests/fixtures/rs256_other_priv.pem");

    fn claims() -> JwtClaims {
        JwtClaims {
            sub: "alice".into(),
            workspace: "acme".into(),
            scopes: vec!["rig.read".into()],
            exp: 100,
            nbf: None,
            iat: 0,
        }
    }

    #[test]
    fn mints_a_token_that_round_trips_through_the_verifier() {
        let minter = JwtMinter::from_rsa_pem(TEST_PRIV_PEM).unwrap();
        let token = minter.mint(&claims()).unwrap();
        let auth = JwtAuthenticator::from_rsa_pem(TEST_PUB_PEM).unwrap();
        let got = auth.authenticate(&token).unwrap();
        assert_eq!(got, claims());
        assert_eq!(got.sub, "alice");
        assert_eq!(got.workspace, "acme");
        assert!(got.has_scope("rig.read"));
        assert_eq!(got.exp, 100);
    }

    #[test]
    fn stamps_the_signing_kid_and_round_trips_through_a_keyset() {
        let minter = JwtMinter::from_rsa_pem(TEST_PRIV_PEM).unwrap().with_kid("k1");
        let token = minter.mint(&claims()).unwrap();

        // The header carries the configured kid, so the verifier's keyset can select the key.
        let header = decode_header(&token).unwrap();
        assert_eq!(header.kid.as_deref(), Some("k1"));

        let auth = JwtAuthenticator::from_kid_pems([("k1", TEST_PUB_PEM)]).unwrap();
        assert_eq!(auth.authenticate(&token).unwrap().sub, "alice");
    }

    #[test]
    fn a_token_minted_with_a_different_key_fails_signature_verification() {
        // Real, rejectable signature: the OTHER private key does not match the primary public key.
        let minter = JwtMinter::from_rsa_pem(OTHER_PRIV_PEM).unwrap();
        let token = minter.mint(&claims()).unwrap();
        let auth = JwtAuthenticator::from_rsa_pem(TEST_PUB_PEM).unwrap();
        assert_eq!(auth.authenticate(&token), Err(AuthError::InvalidSignature));
    }

    #[test]
    fn sign_json_round_trips_an_arbitrary_document_through_verify_json() {
        // gtcore-9039b5: the signed A2A Agent Card path — an arbitrary JSON object signed with
        // the platform key verifies (payload-identical) against the matching public key.
        let minter = JwtMinter::from_rsa_pem(TEST_PRIV_PEM).unwrap();
        let card = serde_json::json!({"name": "gt", "url": "https://gt/a2a", "skills": []});
        let jws = minter.sign_json(&card).unwrap();
        let auth = JwtAuthenticator::from_rsa_pem(TEST_PUB_PEM).unwrap();
        assert_eq!(auth.verify_json(&jws).unwrap(), card);
    }

    #[test]
    fn sign_json_with_a_different_key_fails_verification_and_non_objects_are_rejected() {
        let other = JwtMinter::from_rsa_pem(OTHER_PRIV_PEM).unwrap();
        let jws = other.sign_json(&serde_json::json!({"name": "gt"})).unwrap();
        let auth = JwtAuthenticator::from_rsa_pem(TEST_PUB_PEM).unwrap();
        assert_eq!(auth.verify_json(&jws), Err(AuthError::InvalidSignature));
        // A JWS claims set is a JSON object; anything else is a signing error, not a panic.
        let minter = JwtMinter::from_rsa_pem(TEST_PRIV_PEM).unwrap();
        assert!(matches!(
            minter.sign_json(&serde_json::json!("just a string")),
            Err(AuthError::SigningFailure(_))
        ));
    }

    #[test]
    fn a_malformed_private_key_is_rejected_at_construction() {
        assert!(matches!(
            JwtMinter::from_rsa_pem(b"-----BEGIN PRIVATE KEY-----\nnope\n-----END PRIVATE KEY-----"),
            Err(AuthError::Malformed(_))
        ));
    }

    // All env cases live in one test: process-global env vars are shared mutable state, so two
    // env tests would race under cargo's parallel runner. One test owns the vars start-to-end.
    #[test]
    fn from_env_loads_the_signing_key_and_errors_when_unconfigured() {
        let dir = std::env::temp_dir().join(format!("gt-auth-mint-env-{}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        let priv_path = dir.join("priv.pem");
        std::fs::write(&priv_path, TEST_PRIV_PEM).unwrap();

        // Nothing configured → error naming the env var.
        std::env::remove_var(ENV_PRIVATE_KEY_FILE);
        std::env::remove_var(ENV_SIGNING_KID);
        assert!(matches!(JwtMinter::from_env(), Err(AuthError::Malformed(_))));

        // Key file + signing kid configured → mint, then verify through the matching keyset.
        std::env::set_var(ENV_PRIVATE_KEY_FILE, &priv_path);
        std::env::set_var(ENV_SIGNING_KID, "k1");
        let minter = JwtMinter::from_env().unwrap();
        let token = minter.mint(&claims()).unwrap();
        assert_eq!(decode_header(&token).unwrap().kid.as_deref(), Some("k1"));
        let auth = JwtAuthenticator::from_kid_pems([("k1", TEST_PUB_PEM)]).unwrap();
        assert_eq!(auth.authenticate(&token).unwrap().sub, "alice");

        std::env::remove_var(ENV_PRIVATE_KEY_FILE);
        std::env::remove_var(ENV_SIGNING_KID);
    }
}
