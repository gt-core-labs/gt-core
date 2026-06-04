//! Black-box contract gate for the RS256 verifier (`hq-auth-verify.4`).
//!
//! Exercises the full authenticate → validate pipeline through the crate's *public* API only,
//! against **golden tokens** — fixed, pre-signed JWTs checked in under `tests/fixtures/golden/`.
//! Because every token's `exp`/`nbf`/`iat` are fixed Unix seconds and the test injects a fixed
//! `NOW`, the matrix is fully deterministic (no wall clock). Regenerate the fixtures only when
//! the claim shape changes; the signing recipe is in the commit that added them.
//!
//! Compiled only under the `jsonwebtoken` feature (the verifier it tests lives behind it).
#![cfg(feature = "jsonwebtoken")]

use gt_auth::{AuthError, Authenticator, JwtAuthenticator};

/// The verifier's RSA public key (matches `rs256_priv.pem`, which signed every non-forged token).
const PUB_PEM: &[u8] = include_bytes!("fixtures/rs256_pub.pem");

// Golden tokens — see module docs.
const VALID: &str = include_str!("fixtures/golden/valid.jwt");
const EXPIRED: &str = include_str!("fixtures/golden/expired.jwt");
const FORGED: &str = include_str!("fixtures/golden/forged.jwt");
const UNKNOWN_KID: &str = include_str!("fixtures/golden/unknown_kid.jwt");
const MISSING_WS: &str = include_str!("fixtures/golden/missing_ws.jwt");
const NOT_YET: &str = include_str!("fixtures/golden/not_yet.jwt");

/// A wall-clock instant between every token's `iat` (~999_000_000) and `exp` (2_000_000_000),
/// and before the not-yet token's `nbf` (1_500_000_000).
const NOW: u64 = 1_000_000_000;

fn verifier() -> JwtAuthenticator {
    JwtAuthenticator::from_rsa_pem(PUB_PEM).expect("public key")
}

#[test]
fn a_valid_token_authenticates_and_validates() {
    let claims = verifier().authenticate(VALID.trim()).expect("signature verifies");
    assert_eq!(claims.sub, "alice");
    assert_eq!(claims.workspace, "acme");
    assert!(claims.has_scope("rig.read"));
    assert_eq!(claims.validate(NOW, false), Ok(()));
}

#[test]
fn an_expired_token_verifies_but_fails_the_clock_gate() {
    // Signature is sound, so authenticate succeeds; the expiry is validate's job.
    let claims = verifier().authenticate(EXPIRED.trim()).expect("signature verifies");
    assert_eq!(claims.validate(NOW, false), Err(AuthError::Expired));
}

#[test]
fn a_forged_signature_is_rejected() {
    // Signed by an unrelated private key; verified against PUB_PEM → invalid signature.
    assert_eq!(
        verifier().authenticate(FORGED.trim()),
        Err(AuthError::InvalidSignature)
    );
}

#[test]
fn a_token_naming_an_unknown_kid_is_rejected() {
    assert_eq!(
        verifier().authenticate(UNKNOWN_KID.trim()),
        Err(AuthError::UnknownKey("nope".into()))
    );
}

#[test]
fn a_token_without_a_workspace_is_rejected_unless_grace_is_on() {
    let claims = verifier().authenticate(MISSING_WS.trim()).expect("signature verifies");
    assert_eq!(claims.validate(NOW, false), Err(AuthError::MissingWorkspace));
    assert_eq!(claims.validate(NOW, true), Ok(())); // GT_JWT_WS_OPTIONAL grace window
}

#[test]
fn a_not_yet_valid_token_is_rejected_until_its_nbf() {
    let claims = verifier().authenticate(NOT_YET.trim()).expect("signature verifies");
    assert_eq!(claims.validate(NOW, false), Err(AuthError::NotYetValid));
    // After nbf (1_500_000_000) it validates.
    assert_eq!(claims.validate(1_600_000_000, false), Ok(()));
}
