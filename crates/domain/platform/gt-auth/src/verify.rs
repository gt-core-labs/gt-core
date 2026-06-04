//! The [`Authenticator`] port and its in-memory test double.

use std::collections::HashMap;

use crate::{AuthError, JwtClaims};

/// Turn an opaque bearer token into verified [`JwtClaims`].
///
/// The inverted port (hexagonal): the domain depends on this trait, not on a concrete JWT
/// library. A real adapter (jsonwebtoken HS256/RS256) implements it behind this crate's
/// off-by-default `jsonwebtoken` feature; tests use [`InMemoryAuthenticator`].
///
/// Synchronous on purpose — verifying a signature is CPU work, never I/O, so callers need no
/// async runtime to authenticate. Implementations decode + verify; they do **not** apply the
/// wall-clock / workspace-presence checks — that is [`JwtClaims::validate`]'s job, kept
/// separate so the clock is injected by the caller and the slice stays deterministic.
pub trait Authenticator {
    /// Verify `token` and return its decoded claims, or the reason it was rejected.
    fn authenticate(&self, token: &str) -> Result<JwtClaims, AuthError>;
}

/// A crypto-free [`Authenticator`] for domain tests and the no-key fallback: a fixed
/// `token -> claims` table standing in for signature verification. An unminted token is
/// [`AuthError::UnknownToken`].
#[derive(Clone, Debug, Default)]
pub struct InMemoryAuthenticator {
    tokens: HashMap<String, JwtClaims>,
}

impl InMemoryAuthenticator {
    /// An authenticator that recognizes no tokens.
    pub fn new() -> Self {
        Self::default()
    }

    /// Register `token` as resolving to `claims`. Chainable.
    pub fn with_token(mut self, token: impl Into<String>, claims: JwtClaims) -> Self {
        self.tokens.insert(token.into(), claims);
        self
    }
}

impl Authenticator for InMemoryAuthenticator {
    fn authenticate(&self, token: &str) -> Result<JwtClaims, AuthError> {
        self.tokens
            .get(token)
            .cloned()
            .ok_or(AuthError::UnknownToken)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn claims(workspace: &str, exp: u64) -> JwtClaims {
        JwtClaims {
            sub: "alice".into(),
            workspace: workspace.into(),
            scopes: vec!["rig.read".into()],
            exp,
            nbf: None,
            iat: 0,
        }
    }

    #[test]
    fn authenticate_returns_claims_for_a_minted_token() {
        let auth = InMemoryAuthenticator::new().with_token("tok-1", claims("acme", 100));
        let got = auth.authenticate("tok-1").unwrap();
        assert_eq!(got.sub, "alice");
        assert_eq!(got.workspace, "acme");
        assert!(got.has_scope("rig.read"));
    }

    #[test]
    fn authenticate_rejects_an_unminted_token() {
        let auth = InMemoryAuthenticator::new();
        assert_eq!(auth.authenticate("nope"), Err(AuthError::UnknownToken));
    }

    #[test]
    fn validate_passes_a_live_workspace_scoped_token() {
        assert_eq!(claims("acme", 100).validate(50, false), Ok(()));
    }

    #[test]
    fn validate_rejects_an_expired_token() {
        // now == exp is already expired (exp is exclusive).
        assert_eq!(claims("acme", 100).validate(100, false), Err(AuthError::Expired));
        assert_eq!(claims("acme", 100).validate(101, false), Err(AuthError::Expired));
    }

    #[test]
    fn validate_rejects_a_blank_workspace_without_grace() {
        assert_eq!(
            claims("   ", 100).validate(50, false),
            Err(AuthError::MissingWorkspace)
        );
    }

    #[test]
    fn validate_allows_a_blank_workspace_under_grace() {
        // GT_JWT_WS_OPTIONAL rollout window: a token without the claim still passes.
        assert_eq!(claims("", 100).validate(50, true), Ok(()));
    }

    #[test]
    fn claims_round_trip_through_serde() {
        let c = claims("acme", 100);
        let json = serde_json::to_string(&c).unwrap();
        let back: JwtClaims = serde_json::from_str(&json).unwrap();
        assert_eq!(c, back);
    }

    #[test]
    fn absent_optional_claims_deserialize_to_defaults() {
        // A minimal token (only sub + exp) decodes with empty workspace/scopes, iat 0, nbf
        // None — the grace path then decides whether the empty workspace is acceptable.
        let back: JwtClaims = serde_json::from_str(r#"{"sub":"bob","exp":100}"#).unwrap();
        assert_eq!(back.workspace, "");
        assert!(back.scopes.is_empty());
        assert_eq!(back.iat, 0);
        assert_eq!(back.nbf, None);
    }

    #[test]
    fn nbf_absent_when_serialized() {
        // A None nbf is omitted from the wire (skip_serializing_if), not emitted as null.
        let json = serde_json::to_string(&claims("acme", 100)).unwrap();
        assert!(!json.contains("nbf"), "nbf should be absent: {json}");
    }

    // --- clock skew leeway (hq-auth-verify.3) ---------------------------------------------

    #[test]
    fn leeway_widens_the_expiry_window() {
        let c = claims("acme", 100);
        // 5s past exp, but a 10s leeway still accepts it; 11s leeway boundary already covered.
        assert_eq!(c.validate_with_leeway(105, 10, false), Ok(()));
        // Beyond exp + leeway it is expired.
        assert_eq!(c.validate_with_leeway(111, 10, false), Err(AuthError::Expired));
    }

    fn timed(nbf: Option<u64>, iat: u64, exp: u64) -> JwtClaims {
        JwtClaims {
            sub: "alice".into(),
            workspace: "acme".into(),
            scopes: vec![],
            exp,
            nbf,
            iat,
        }
    }

    #[test]
    fn nbf_rejects_a_token_presented_too_early() {
        let c = timed(Some(50), 0, 100);
        assert_eq!(c.validate(49, false), Err(AuthError::NotYetValid));
        assert_eq!(c.validate(50, false), Ok(())); // valid from nbf
        // leeway lets a slightly-early presentation through.
        assert_eq!(c.validate_with_leeway(48, 5, false), Ok(()));
    }

    #[test]
    fn iat_in_the_future_is_rejected_as_not_yet_valid() {
        let c = timed(None, 80, 100); // minted at 80 (in the future relative to now=50)
        assert_eq!(c.validate(50, false), Err(AuthError::NotYetValid));
        assert_eq!(c.validate(80, false), Ok(()));
        assert_eq!(c.validate_with_leeway(78, 5, false), Ok(())); // skew tolerated
    }

    #[test]
    fn iat_zero_is_treated_as_absent() {
        // The wire default iat 0 must not be read as "minted at epoch, far in the past" nor
        // trip the future check.
        assert_eq!(timed(None, 0, 100).validate(50, false), Ok(()));
    }

    #[test]
    fn workspace_optional_from_env_reads_truthy_values() {
        // Single test owns the process-global var (parallel runner safety).
        for (val, want) in [("1", true), ("true", true), ("ON", true), ("0", false), ("no", false)] {
            std::env::set_var(crate::ENV_WS_OPTIONAL, val);
            assert_eq!(JwtClaims::workspace_optional_from_env(), want, "val={val}");
        }
        std::env::remove_var(crate::ENV_WS_OPTIONAL);
        assert!(!JwtClaims::workspace_optional_from_env()); // unset ⇒ required
    }
}
