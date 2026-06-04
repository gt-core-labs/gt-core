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
//!
//! ## Key rotation by `kid` (hq-auth-verify.2)
//!
//! A verifier can hold **several** public keys indexed by the token header's `kid` (key id), so
//! the minting side can rotate keys without a flag day: publish the new `kid`, start signing
//! with it, retire the old one after the deprecation window. Tokens select their key by `kid`;
//! a verifier configured with a single un-keyed key still accepts a `kid`-less token
//! (back-compat with the hq-auth-verify.1 single-key shape). [`from_env`](JwtAuthenticator::from_env)
//! loads the keyset from the deployment's environment.
//!
//! Hexagonal (docs/03 Rule 4): this adapter lives inside `gt-auth` behind the off-by-default
//! `jsonwebtoken` feature — never a sibling crate — exactly as `password.rs` gates argon2.

use std::collections::HashMap;

use jsonwebtoken::{decode, decode_header, Algorithm, DecodingKey, Validation};

use crate::{AuthError, Authenticator, JwtClaims};

/// Environment variable naming a `;`-separated rotation set of `kid=<pem-path>` entries.
pub const ENV_KEYS: &str = "GT_JWT_RS256_KEYS";
/// Environment variable naming a single PEM file, registered as the un-keyed default key.
pub const ENV_KEY_FILE: &str = "GT_JWT_RS256_PUBLIC_KEY_FILE";

/// An [`Authenticator`] that verifies RS256-signed JWTs against one or more public keys.
///
/// Build it from a single key ([`new`](Self::new) / [`from_rsa_pem`](Self::from_rsa_pem)),
/// add `kid`-indexed keys for rotation ([`with_rsa_pem_kid`](Self::with_rsa_pem_kid)), or load
/// the whole keyset from the environment ([`from_env`](Self::from_env)).
pub struct JwtAuthenticator {
    /// Keys selectable by the token header's `kid`.
    by_kid: HashMap<String, DecodingKey>,
    /// The key used for a token that carries no `kid` (single-key deploys).
    default_key: Option<DecodingKey>,
    validation: Validation,
}

fn rs256_validation() -> Validation {
    let mut validation = Validation::new(Algorithm::RS256);
    // Signature only. The `exp` clock check and the `workspace`-presence check are
    // `JwtClaims::validate`'s responsibility (the trait contract + injected clock), so we
    // disable jsonwebtoken's built-in expiry enforcement and its required-claims gate —
    // otherwise it would reject an expired token before `validate` ever sees it, and demand
    // an `exp`/`aud` we deliberately validate ourselves.
    validation.validate_exp = false;
    validation.validate_aud = false;
    validation.required_spec_claims.clear();
    validation
}

fn decoding_key_from_pem(pem: &[u8]) -> Result<DecodingKey, AuthError> {
    DecodingKey::from_rsa_pem(pem)
        .map_err(|e| AuthError::Malformed(format!("invalid RS256 public key: {e}")))
}

impl JwtAuthenticator {
    /// Build the verifier from a single, un-keyed RS256 [`DecodingKey`] (the default key).
    pub fn new(key: DecodingKey) -> Self {
        Self {
            by_kid: HashMap::new(),
            default_key: Some(key),
            validation: rs256_validation(),
        }
    }

    /// An empty keyset, to be populated with [`with_rsa_pem_kid`](Self::with_rsa_pem_kid).
    pub fn empty() -> Self {
        Self {
            by_kid: HashMap::new(),
            default_key: None,
            validation: rs256_validation(),
        }
    }

    /// Build the verifier from a single RSA public key in PEM form
    /// (`-----BEGIN PUBLIC KEY-----`), registered as the un-keyed default.
    pub fn from_rsa_pem(pem: &[u8]) -> Result<Self, AuthError> {
        Ok(Self::new(decoding_key_from_pem(pem)?))
    }

    /// Build the verifier from a single RSA public key in DER form.
    pub fn from_rsa_der(der: &[u8]) -> Self {
        Self::new(DecodingKey::from_rsa_der(der))
    }

    /// Register `key` under `kid`. Chainable. A token whose header `kid` matches verifies
    /// against this key.
    pub fn with_key_kid(mut self, kid: impl Into<String>, key: DecodingKey) -> Self {
        self.by_kid.insert(kid.into(), key);
        self
    }

    /// Register an RSA PEM public key under `kid`. Chainable.
    pub fn with_rsa_pem_kid(self, kid: impl Into<String>, pem: &[u8]) -> Result<Self, AuthError> {
        let key = decoding_key_from_pem(pem)?;
        Ok(self.with_key_kid(kid, key))
    }

    /// Build a `kid`-indexed keyset from `(kid, pem)` pairs.
    pub fn from_kid_pems<K, P>(entries: impl IntoIterator<Item = (K, P)>) -> Result<Self, AuthError>
    where
        K: Into<String>,
        P: AsRef<[u8]>,
    {
        let mut auth = Self::empty();
        for (kid, pem) in entries {
            auth = auth.with_rsa_pem_kid(kid, pem.as_ref())?;
        }
        Ok(auth)
    }

    /// Load the verifier from the deployment environment.
    ///
    /// Precedence:
    /// 1. [`GT_JWT_RS256_KEYS`](ENV_KEYS) — a `;`-separated rotation set of `kid=<pem-path>`
    ///    entries; each named file is read as an RSA public key registered under its `kid`.
    /// 2. [`GT_JWT_RS256_PUBLIC_KEY_FILE`](ENV_KEY_FILE) — a single PEM file, registered as the
    ///    un-keyed default (single-key deploys).
    ///
    /// Both may be set (a rotation set *and* a `kid`-less fallback). At least one key must
    /// result, else [`AuthError::Malformed`].
    pub fn from_env() -> Result<Self, AuthError> {
        let mut auth = Self::empty();
        if let Ok(spec) = std::env::var(ENV_KEYS) {
            for (kid, path) in parse_key_spec(&spec)? {
                let pem = std::fs::read(&path)
                    .map_err(|e| AuthError::Malformed(format!("cannot read key file {path}: {e}")))?;
                auth = auth.with_rsa_pem_kid(kid, &pem)?;
            }
        }
        if let Ok(path) = std::env::var(ENV_KEY_FILE) {
            let pem = std::fs::read(&path)
                .map_err(|e| AuthError::Malformed(format!("cannot read key file {path}: {e}")))?;
            auth.default_key = Some(decoding_key_from_pem(&pem)?);
        }
        if auth.by_kid.is_empty() && auth.default_key.is_none() {
            return Err(AuthError::Malformed(format!(
                "no RS256 public key configured (set {ENV_KEYS} or {ENV_KEY_FILE})"
            )));
        }
        Ok(auth)
    }

    /// Pick the verification key for a token's header `kid` (or the un-keyed default / sole key).
    fn key_for(&self, kid: Option<&str>) -> Result<&DecodingKey, AuthError> {
        match kid {
            Some(kid) => self
                .by_kid
                .get(kid)
                .ok_or_else(|| AuthError::UnknownKey(kid.to_string())),
            None => self
                .default_key
                .as_ref()
                // A `kid`-less token also resolves when exactly one keyed key exists — no
                // ambiguity about which to use.
                .or_else(|| match self.by_kid.len() {
                    1 => self.by_kid.values().next(),
                    _ => None,
                })
                .ok_or_else(|| AuthError::UnknownKey(String::new())),
        }
    }
}

impl Authenticator for JwtAuthenticator {
    fn authenticate(&self, token: &str) -> Result<JwtClaims, AuthError> {
        let header = decode_header(token).map_err(map_err)?;
        let key = self.key_for(header.kid.as_deref())?;
        decode::<JwtClaims>(token, key, &self.validation)
            .map(|data| data.claims)
            .map_err(map_err)
    }
}

/// Parse a [`GT_JWT_RS256_KEYS`](ENV_KEYS) spec into `(kid, path)` pairs. Entries are
/// `;`-separated; each is `kid=path`. Blank entries are skipped; a missing `=` or an empty
/// `kid`/`path` is [`AuthError::Malformed`].
fn parse_key_spec(spec: &str) -> Result<Vec<(String, String)>, AuthError> {
    let mut out = Vec::new();
    for entry in spec.split(';') {
        let entry = entry.trim();
        if entry.is_empty() {
            continue;
        }
        let (kid, path) = entry.split_once('=').ok_or_else(|| {
            AuthError::Malformed(format!("malformed key entry {entry:?} (expected kid=path)"))
        })?;
        let (kid, path) = (kid.trim(), path.trim());
        if kid.is_empty() || path.is_empty() {
            return Err(AuthError::Malformed(format!(
                "malformed key entry {entry:?} (empty kid or path)"
            )));
        }
        out.push((kid.to_string(), path.to_string()));
    }
    Ok(out)
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

    // Two throwaway 2048-bit RSA keypairs, generated once for the slice. Verifying against the
    // matching public key proves the signature path; signing with the unrelated key proves
    // rejection; the second keypair also stands in as a second `kid` for rotation.
    const TEST_PRIV_PEM: &[u8] = include_bytes!("../tests/fixtures/rs256_priv.pem");
    const TEST_PUB_PEM: &[u8] = include_bytes!("../tests/fixtures/rs256_pub.pem");
    const OTHER_PRIV_PEM: &[u8] = include_bytes!("../tests/fixtures/rs256_other_priv.pem");
    const OTHER_PUB_PEM: &[u8] = include_bytes!("../tests/fixtures/rs256_other_pub.pem");

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

    fn sign(priv_pem: &[u8], kid: Option<&str>, claims: &JwtClaims) -> String {
        let key = EncodingKey::from_rsa_pem(priv_pem).expect("test private key");
        let mut header = Header::new(Algorithm::RS256);
        header.kid = kid.map(|k| k.to_string());
        encode(&header, claims, &key).expect("sign")
    }

    // --- single-key (hq-auth-verify.1 back-compat) -----------------------------------------

    #[test]
    fn verifies_a_well_signed_token_into_its_claims() {
        let token = sign(TEST_PRIV_PEM, None, &claims());
        let auth = JwtAuthenticator::from_rsa_pem(TEST_PUB_PEM).unwrap();
        let got = auth.authenticate(&token).unwrap();
        assert_eq!(got.sub, "alice");
        assert_eq!(got.workspace, "acme");
        assert!(got.has_scope("rig.read"));
    }

    #[test]
    fn rejects_a_token_signed_by_a_different_key() {
        let token = sign(OTHER_PRIV_PEM, None, &claims());
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
        let token = sign(TEST_PRIV_PEM, None, &c);
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

    // --- kid rotation (hq-auth-verify.2) ---------------------------------------------------

    fn keyset() -> JwtAuthenticator {
        JwtAuthenticator::from_kid_pems([("k1", TEST_PUB_PEM), ("k2", OTHER_PUB_PEM)]).unwrap()
    }

    #[test]
    fn selects_the_key_named_by_the_token_kid() {
        let auth = keyset();
        let tok1 = sign(TEST_PRIV_PEM, Some("k1"), &claims());
        let tok2 = sign(OTHER_PRIV_PEM, Some("k2"), &claims());
        assert_eq!(auth.authenticate(&tok1).unwrap().sub, "alice");
        assert_eq!(auth.authenticate(&tok2).unwrap().sub, "alice");
    }

    #[test]
    fn rejects_a_token_whose_kid_matches_a_key_but_signature_does_not() {
        // signed by k1's private key but claiming kid=k2 → verified against k2's public key → fail
        let auth = keyset();
        let token = sign(TEST_PRIV_PEM, Some("k2"), &claims());
        assert_eq!(auth.authenticate(&token), Err(AuthError::InvalidSignature));
    }

    #[test]
    fn rejects_an_unknown_kid() {
        let auth = keyset();
        let token = sign(TEST_PRIV_PEM, Some("nope"), &claims());
        assert_eq!(
            auth.authenticate(&token),
            Err(AuthError::UnknownKey("nope".into()))
        );
    }

    #[test]
    fn a_kidless_token_resolves_against_a_sole_keyed_key() {
        let auth = JwtAuthenticator::from_kid_pems([("k1", TEST_PUB_PEM)]).unwrap();
        let token = sign(TEST_PRIV_PEM, None, &claims());
        assert_eq!(auth.authenticate(&token).unwrap().sub, "alice");
    }

    #[test]
    fn a_kidless_token_is_ambiguous_with_multiple_keyed_keys() {
        let auth = keyset(); // k1 + k2, no default
        let token = sign(TEST_PRIV_PEM, None, &claims());
        assert_eq!(
            auth.authenticate(&token),
            Err(AuthError::UnknownKey(String::new()))
        );
    }

    // --- env spec parsing ------------------------------------------------------------------

    #[test]
    fn parse_key_spec_reads_kid_path_entries() {
        let got = parse_key_spec("k1=/a/one.pem ; k2=/b/two.pem").unwrap();
        assert_eq!(
            got,
            vec![
                ("k1".to_string(), "/a/one.pem".to_string()),
                ("k2".to_string(), "/b/two.pem".to_string()),
            ]
        );
    }

    #[test]
    fn parse_key_spec_rejects_an_entry_without_equals() {
        assert!(matches!(parse_key_spec("k1:/a.pem"), Err(AuthError::Malformed(_))));
    }

    #[test]
    fn parse_key_spec_rejects_an_empty_kid_or_path() {
        assert!(matches!(parse_key_spec("=/a.pem"), Err(AuthError::Malformed(_))));
        assert!(matches!(parse_key_spec("k1="), Err(AuthError::Malformed(_))));
    }

    // Both env cases live in one test: process-global env vars are shared mutable state, so two
    // env tests would race under cargo's parallel runner. One test owns the vars start-to-end.
    #[test]
    fn from_env_loads_keys_and_errors_when_unconfigured() {
        let dir = std::env::temp_dir().join(format!("gt-auth-env-{}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        let single = dir.join("default.pem");
        std::fs::write(&single, TEST_PUB_PEM).unwrap();
        let rotated = dir.join("k1.pem");
        std::fs::write(&rotated, OTHER_PUB_PEM).unwrap();

        // Nothing configured → error.
        std::env::remove_var(ENV_KEYS);
        std::env::remove_var(ENV_KEY_FILE);
        assert!(matches!(JwtAuthenticator::from_env(), Err(AuthError::Malformed(_))));

        // Single un-keyed default key.
        std::env::set_var(ENV_KEY_FILE, &single);
        let auth = JwtAuthenticator::from_env().unwrap();
        let kidless = sign(TEST_PRIV_PEM, None, &claims());
        assert_eq!(auth.authenticate(&kidless).unwrap().sub, "alice");

        // Rotation set (kid=path) alongside the default.
        std::env::set_var(ENV_KEYS, format!("k1={}", rotated.display()));
        let auth = JwtAuthenticator::from_env().unwrap();
        let keyed = sign(OTHER_PRIV_PEM, Some("k1"), &claims());
        assert_eq!(auth.authenticate(&keyed).unwrap().sub, "alice");
        assert_eq!(auth.authenticate(&kidless).unwrap().sub, "alice"); // default still resolves

        std::env::remove_var(ENV_KEYS);
        std::env::remove_var(ENV_KEY_FILE);
    }
}
