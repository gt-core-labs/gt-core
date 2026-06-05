//! AES-256-GCM seal/unseal for secrets at rest (hq-idp-db.1).
//!
//! The DB-backed OAuth/OIDC provider store ([`provider_repo`](crate::provider_repo)) must hold each
//! provider's `client_secret`, but a secret in cleartext in a table is a leak waiting to happen.
//! This module is the small AEAD primitive that keeps it encrypted at rest: [`seal`] turns a
//! plaintext secret into an opaque blob stored in `client_secret_enc BYTEA`, and [`unseal`] recovers
//! it in memory only when a row is turned into an `OidcConfig` for the handshake.
//!
//! ## Cipher + framing
//!
//! AES-256-GCM (a NIST AEAD) with a 256-bit key and a fresh **random 96-bit nonce per call**. The
//! sealed blob is `nonce (12 bytes) || ciphertext+tag`: the nonce is public (it only needs to be
//! unique per key, not secret), so prepending it lets [`unseal`] split it back out without a second
//! column. GCM's authentication tag makes a tampered or truncated blob a decrypt **error**, never a
//! silently-wrong plaintext.
//!
//! Nonce reuse under a fixed key is catastrophic for GCM, so the nonce is drawn from the OS CSPRNG
//! (`getrandom`) on every [`seal`] — the same entropy source the refresh-token minter uses. At
//! 96 random bits the birthday bound is comfortably beyond any realistic number of provider rows.
//!
//! ## Key
//!
//! The master key comes from the `GT_SECRET_KEY` environment variable via [`master_key`], decoded
//! as base64 (standard) or hex into exactly 32 bytes. A missing var, an undecodable value, or a
//! wrong length is a clear [`AuthError::Backend`] (a deploy misconfiguration) — never a silent weak
//! key. Gated behind the `oauth` feature with the rest of the provider store.

use aes_gcm::aead::{Aead, KeyInit};
use aes_gcm::{Aes256Gcm, Key, Nonce};

use crate::AuthError;

/// The env var holding the AES-256-GCM master key — 32 bytes, base64 (standard) or hex.
pub const ENV_SECRET_KEY: &str = "GT_SECRET_KEY";

/// AES-256-GCM nonce length (96 bits — the GCM-recommended size), in bytes.
const NONCE_LEN: usize = 12;
/// AES-256 key length, in bytes.
const KEY_LEN: usize = 32;

/// Decode the 32-byte master key from `GT_SECRET_KEY`, accepting base64 (standard) or hex.
///
/// A missing/blank var, an undecodable value, or a length other than 32 bytes is
/// [`AuthError::Backend`] — a deploy misconfiguration, surfaced loudly rather than defaulted to a
/// predictable key.
pub fn master_key() -> Result<[u8; KEY_LEN], AuthError> {
    let raw = match std::env::var(ENV_SECRET_KEY) {
        Ok(v) if !v.trim().is_empty() => v.trim().to_owned(),
        _ => {
            return Err(AuthError::Backend(format!(
                "missing secret key: {ENV_SECRET_KEY} (32 bytes, base64 or hex)"
            )))
        }
    };
    let bytes = decode_key(&raw).ok_or_else(|| {
        AuthError::Backend(format!(
            "{ENV_SECRET_KEY} must be base64 or hex of 32 bytes"
        ))
    })?;
    if bytes.len() != KEY_LEN {
        return Err(AuthError::Backend(format!(
            "{ENV_SECRET_KEY} decodes to {} bytes, expected {KEY_LEN}",
            bytes.len()
        )));
    }
    let mut key = [0u8; KEY_LEN];
    key.copy_from_slice(&bytes);
    Ok(key)
}

/// Try base64 (standard) then hex; `None` if neither decodes. Hex is tried second because a 64-char
/// hex string is also valid base64, but a 32-byte key wants the hex reading — so the length check in
/// [`master_key`] rejects the base64 misread (44 bytes) and the hex branch wins.
fn decode_key(raw: &str) -> Option<Vec<u8>> {
    use base64::Engine as _;
    if let Ok(b) = base64::engine::general_purpose::STANDARD.decode(raw) {
        if b.len() == KEY_LEN {
            return Some(b);
        }
    }
    if let Ok(b) = hex::decode(raw) {
        if b.len() == KEY_LEN {
            return Some(b);
        }
    }
    // Fall back to whatever base64 produced (length-checked by the caller) so a wrong length still
    // yields a precise "decodes to N bytes" error rather than a generic decode failure.
    base64::engine::general_purpose::STANDARD.decode(raw).ok()
}

/// Seal `plaintext` under the `GT_SECRET_KEY` master key, returning `nonce || ciphertext+tag`.
///
/// A fresh random nonce is drawn per call (`getrandom`), so sealing the same secret twice yields
/// distinct blobs. The result is what the repo writes to `client_secret_enc`.
// Sealing only happens on a `PgProviderRepo::create` (the `pg` adapter); an `oauth`-without-`pg`
// build links only `unseal` (via `into_oidc_config`), so suppress the dead-code lint there.
#[cfg_attr(not(feature = "pg"), allow(dead_code))]
pub fn seal(plaintext: &[u8]) -> Result<Vec<u8>, AuthError> {
    seal_with(&master_key()?, plaintext)
}

/// Unseal a `nonce || ciphertext+tag` blob produced by [`seal`] back to the plaintext secret.
///
/// A blob shorter than the nonce, a wrong key, or a tampered ciphertext (GCM tag mismatch) is
/// [`AuthError::Backend`] — corruption/misconfiguration, never a usable secret.
pub fn unseal(blob: &[u8]) -> Result<Vec<u8>, AuthError> {
    unseal_with(&master_key()?, blob)
}

/// [`seal`] against an explicit key (the env-free core, exercised directly in tests).
#[cfg_attr(not(any(feature = "pg", test)), allow(dead_code))]
fn seal_with(key: &[u8; KEY_LEN], plaintext: &[u8]) -> Result<Vec<u8>, AuthError> {
    let cipher = Aes256Gcm::new(Key::<Aes256Gcm>::from_slice(key));
    let mut nonce_bytes = [0u8; NONCE_LEN];
    getrandom::getrandom(&mut nonce_bytes)
        .map_err(|e| AuthError::Backend(format!("OS CSPRNG for nonce: {e}")))?;
    let nonce = Nonce::from_slice(&nonce_bytes);
    let ciphertext = cipher
        .encrypt(nonce, plaintext)
        .map_err(|_| AuthError::Backend("secret seal (aes-gcm encrypt) failed".into()))?;
    let mut out = Vec::with_capacity(NONCE_LEN + ciphertext.len());
    out.extend_from_slice(&nonce_bytes);
    out.extend_from_slice(&ciphertext);
    Ok(out)
}

/// [`unseal`] against an explicit key (the env-free core, exercised directly in tests).
fn unseal_with(key: &[u8; KEY_LEN], blob: &[u8]) -> Result<Vec<u8>, AuthError> {
    if blob.len() < NONCE_LEN {
        return Err(AuthError::Backend("sealed secret too short".into()));
    }
    let (nonce_bytes, ciphertext) = blob.split_at(NONCE_LEN);
    let cipher = Aes256Gcm::new(Key::<Aes256Gcm>::from_slice(key));
    let nonce = Nonce::from_slice(nonce_bytes);
    cipher
        .decrypt(nonce, ciphertext)
        .map_err(|_| AuthError::Backend("secret unseal (aes-gcm decrypt) failed".into()))
}

#[cfg(test)]
mod tests {
    use super::*;

    const KEY: [u8; KEY_LEN] = [7u8; KEY_LEN];

    #[test]
    fn seal_then_unseal_round_trips_the_plaintext() {
        let secret = b"s3cret-client-secret";
        let blob = seal_with(&KEY, secret).unwrap();
        assert_eq!(unseal_with(&KEY, &blob).unwrap(), secret);
    }

    #[test]
    fn sealed_blob_is_not_the_cleartext_and_carries_a_nonce() {
        let secret = b"plaintext";
        let blob = seal_with(&KEY, secret).unwrap();
        // The blob never contains the cleartext as a contiguous run.
        assert!(!blob.windows(secret.len()).any(|w| w == secret));
        // nonce (12) + ciphertext (>= plaintext) + GCM tag (16).
        assert!(blob.len() >= NONCE_LEN + secret.len() + 16);
    }

    #[test]
    fn each_seal_uses_a_fresh_nonce_so_ciphertexts_differ() {
        let secret = b"same-input";
        let a = seal_with(&KEY, secret).unwrap();
        let b = seal_with(&KEY, secret).unwrap();
        assert_ne!(a, b);
        // Both still unseal to the same plaintext.
        assert_eq!(unseal_with(&KEY, &a).unwrap(), secret);
        assert_eq!(unseal_with(&KEY, &b).unwrap(), secret);
    }

    #[test]
    fn a_wrong_key_fails_to_unseal() {
        let blob = seal_with(&KEY, b"x").unwrap();
        let wrong = [9u8; KEY_LEN];
        assert!(unseal_with(&wrong, &blob).is_err());
    }

    #[test]
    fn a_tampered_ciphertext_is_rejected_by_the_gcm_tag() {
        let mut blob = seal_with(&KEY, b"important").unwrap();
        // Flip a byte in the ciphertext region (past the nonce).
        let last = blob.len() - 1;
        blob[last] ^= 0xff;
        assert!(unseal_with(&KEY, &blob).is_err());
    }

    #[test]
    fn a_blob_shorter_than_the_nonce_is_rejected() {
        assert!(unseal_with(&KEY, &[0u8; NONCE_LEN - 1]).is_err());
    }

    #[test]
    fn master_key_accepts_hex_and_base64_of_32_bytes() {
        use base64::Engine as _;
        let bytes = [3u8; KEY_LEN];
        // hex
        assert_eq!(decode_key(&hex::encode(bytes)).unwrap(), bytes);
        // base64 standard
        let b64 = base64::engine::general_purpose::STANDARD.encode(bytes);
        assert_eq!(decode_key(&b64).unwrap(), bytes.to_vec());
    }

    #[test]
    fn master_key_rejects_wrong_length() {
        use base64::Engine as _;
        // 16 bytes, not 32 — decodes but length-checks out.
        let short = base64::engine::general_purpose::STANDARD.encode([1u8; 16]);
        std::env::set_var(ENV_SECRET_KEY, short);
        assert!(master_key().is_err());
        std::env::remove_var(ENV_SECRET_KEY);
    }

    #[test]
    fn master_key_missing_is_a_clear_error() {
        std::env::remove_var(ENV_SECRET_KEY);
        assert!(matches!(master_key(), Err(AuthError::Backend(_))));
    }
}
