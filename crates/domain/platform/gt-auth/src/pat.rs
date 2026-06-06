//! Personal Access Tokens: long-lived, self-service OPAQUE bearer credentials (`hq-security-pat.1`).
//!
//! ## Why a third credential
//!
//! Access tokens ([`JwtClaims`](crate::JwtClaims)) are short-lived JWTs a browser session carries;
//! [`RefreshToken`](crate::RefreshToken) is the long-lived opaque credential that re-mints them.
//! A **PAT** is the long-lived credential a user mints to drive the API *without* a browser — a CI
//! job, a script, a CLI. Like a refresh token it is **opaque** (a high-entropy random string, not a
//! signed JWT): it carries no claims and is validated only by a store lookup, so it can be revoked
//! the instant it leaks. Unlike a refresh token it authenticates API requests **directly** — its
//! row carries the [`scopes`](PatRecord::scopes) the verifier synthesizes a [`JwtClaims`] from, so
//! the same per-route guards gate a PAT call exactly as they gate a session call.
//!
//! ## Scopes are CLAMPED at mint — a PAT can never escalate
//!
//! A user mints a PAT against their *own* authority: the granted scopes are the **intersection** of
//! what they request and what their token already holds ([`clamp_scopes`]). Asking for a scope you
//! do not have silently drops it, so a PAT is always a *subset* of the minter's power, never a
//! privilege escalation. The `*` wildcard a system admin carries clamps like any other scope: an
//! admin can mint a `*` PAT, a non-admin cannot conjure one.
//!
//! ## Shape (hexagonal — docs/03)
//!
//! - [`PatToken`] — the opaque secret newtype (`gtpat_` + 256 bits).
//! - [`PatRecord`] — the server-side bookkeeping for one token (id, subject, scopes, clock bounds,
//!   status). The secret is **not** here — only its non-secret [`id`](PatRecord::id).
//! - [`PatStatus`] — the lifecycle (`Active` / `Revoked`).
//! - [`PatError`] — the verify-rejection vocabulary, separate from [`AuthError`](crate::AuthError)
//!   (a PAT verdict is a store lookup, not a signature check).
//!
//! The Postgres-backed store ([`PgPatStore`](crate::PgPatStore), mint/list/revoke/verify against
//! the per-workspace `personal_access_tokens` table) lives behind the `pg` feature in `pat_pg.rs`,
//! the same gating as [`PgRefreshStore`](crate::PgRefreshStore) — this module stays
//! `sqlx`/hash-free so the dependency-light core can name the types without pulling Postgres.
//!
//! ## Entropy source
//!
//! A PAT is a bearer credential, so its bytes come straight from the OS CSPRNG via `getrandom`:
//! [`PatToken::generate`] draws 256 bits, [`PatId::generate`] 128. Same primitive (and rationale)
//! as [`RefreshToken`](crate::RefreshToken); for deterministic tests there is
//! [`PatToken::from_bytes`].

use thiserror::Error;

/// The required prefix on every Personal Access Token. The auth middleware routes a presented
/// bearer token to the PAT verifier (instead of the JWT verifier) iff it starts with this — a
/// cheap, allocation-free discriminator that needs no store lookup.
pub const PAT_PREFIX: &str = "gtpat_";

/// True when `token` is shaped like a PAT (carries the [`PAT_PREFIX`]). The auth middleware uses
/// this to pick the PAT verify path over JWT verification — see the composition root's
/// `authenticate`. A JWT (three base64url segments) never starts with `gtpat_`, so the two token
/// families are unambiguous.
pub fn has_pat_prefix(token: &str) -> bool {
    token.starts_with(PAT_PREFIX)
}

/// A high-entropy **opaque** Personal Access Token — the [`PAT_PREFIX`] followed by a random hex
/// string with no internal structure.
///
/// It is a bearer secret: whoever holds it authenticates as the minting user (within the clamped
/// scopes). Compared only by value (a hash lookup in the store), never parsed. Mint one with
/// [`generate`](Self::generate) (OS entropy) or [`from_bytes`](Self::from_bytes) (caller-supplied
/// entropy, for deterministic tests).
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct PatToken(String);

impl PatToken {
    /// Wrap an already-formed opaque string as a token (e.g. one read off a request header). Most
    /// callers want [`generate`](Self::generate) instead, which produces fresh entropy.
    pub fn new(value: impl Into<String>) -> Self {
        Self(value.into())
    }

    /// Build a token from caller-supplied `bytes`: the [`PAT_PREFIX`] followed by the lowercase-hex
    /// encoding of the bytes. The entropy is exactly the bytes you pass, so this is the
    /// deterministic constructor used by tests.
    pub fn from_bytes(bytes: &[u8]) -> Self {
        let mut s = String::with_capacity(PAT_PREFIX.len() + bytes.len() * 2);
        s.push_str(PAT_PREFIX);
        push_hex(&mut s, bytes);
        Self(s)
    }

    /// Mint a fresh token with 256 bits of entropy drawn from the OS CSPRNG (`getrandom`) — wide
    /// enough that collisions are negligible and the value is unpredictable.
    pub fn generate() -> Self {
        Self::from_bytes(&random_bytes::<32>())
    }

    /// The opaque string, for transport (it is shown to the user exactly once, at mint).
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

/// A non-secret, random identifier for a PAT row (128 bits). Distinct from the secret [`PatToken`]
/// so it can be listed and revoked (`DELETE /auth/tokens/{id}`) without ever echoing the
/// credential back.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct PatId(String);

impl PatId {
    /// Mint a fresh random id (128 bits — ample for a non-secret handle), hex-encoded.
    pub fn generate() -> Self {
        Self(hex_string(&random_bytes::<16>()))
    }

    /// Rebuild an id from an already-formed string (e.g. the `id` column read back by the store).
    pub fn new(value: impl Into<String>) -> Self {
        Self(value.into())
    }

    /// The id as a string.
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

/// Lifecycle of a Personal Access Token.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum PatStatus {
    /// Live and usable — the verifier accepts it (until it expires).
    Active,
    /// Revoked by its owner (or an admin). The verifier rejects it; a row stays for audit.
    Revoked,
}

impl PatStatus {
    /// The wire (column) spelling, matching the migration's CHECK-free TEXT.
    pub fn as_str(self) -> &'static str {
        match self {
            PatStatus::Active => "active",
            PatStatus::Revoked => "revoked",
        }
    }

    /// Parse the column spelling back. An unknown value is `None` (the store maps that to a backend
    /// fault — a row it cannot classify).
    pub fn from_wire(s: &str) -> Option<Self> {
        match s {
            "active" => Some(PatStatus::Active),
            "revoked" => Some(PatStatus::Revoked),
            _ => None,
        }
    }
}

/// Server-side bookkeeping for one Personal Access Token. The secret string is **not** stored here
/// — only its non-secret [`id`](Self::id); the store keys the secret separately (by hash) so a
/// record can be listed without leaking the credential.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct PatRecord {
    /// This token's non-secret id — what the REST surface lists and revokes by.
    pub id: PatId,
    /// Subject — the user who minted (and authenticates as) this token.
    pub sub: String,
    /// Tenant the token is scoped to (mirrors the `workspace` access-token claim it synthesizes).
    pub workspace: String,
    /// A human label the owner gives the token, so a list of tokens is legible ("ci-deploy").
    pub name: String,
    /// The granted scopes — clamped to the minter's own at creation ([`clamp_scopes`]), carried so
    /// verify can synthesize a faithful [`JwtClaims`](crate::JwtClaims).
    pub scopes: Vec<String>,
    /// Created-at, seconds since the Unix epoch (injected, never read from the wall clock).
    pub created_at: u64,
    /// Expiry, seconds since the Unix epoch. `None` ⇒ the token never expires. Verifying at or
    /// past this instant is [`PatError::Expired`].
    pub expires_at: Option<u64>,
    /// Last successful verify, seconds since the Unix epoch (`None` until first use). Stamped on
    /// each accepted verify so the owner can spot a stale or compromised token.
    pub last_used_at: Option<u64>,
    /// Where this token sits in its lifecycle.
    pub status: PatStatus,
}

impl PatRecord {
    /// True when `now` is at or past [`expires_at`](Self::expires_at) (exclusive, matching
    /// [`JwtClaims::validate`](crate::JwtClaims::validate)). A token with no expiry never expires.
    pub fn is_expired(&self, now: u64) -> bool {
        matches!(self.expires_at, Some(exp) if now >= exp)
    }
}

/// Why a [`verify`](crate::PgPatStore::verify) was rejected. Separate from
/// [`AuthError`](crate::AuthError): these are PAT-store verdicts (lookup + lifecycle), not
/// signature/claims checks.
#[derive(Debug, Clone, PartialEq, Eq, Error)]
#[non_exhaustive]
pub enum PatError {
    /// No such token in the store — never minted, or pruned. (Indistinguishable from a wrong
    /// token: the lookup is by hash, so a forged token simply isn't found.)
    #[error("unknown personal access token")]
    Unknown,
    /// The token is known but past its `expires_at`.
    #[error("personal access token expired")]
    Expired,
    /// The token was revoked by its owner (or an admin).
    #[error("personal access token revoked")]
    Revoked,
    /// The durable store could not reach or query Postgres. Distinct from the lifecycle verdicts:
    /// an outage, not a denied token — the caller should fail the request, not treat the token as
    /// bad.
    #[error("personal access token store backend error: {0}")]
    Backend(String),
}

/// Clamp the `requested` scopes to the minter's `granted` authority: the result is exactly the
/// scopes that appear in BOTH, in the order they were requested. This is what makes a PAT
/// incapable of privilege escalation — you can only mint a token weaker than (or equal to) the
/// credential you mint it with.
///
/// `requested` empty ⇒ the caller wants the broadest token they may have, so the full `granted`
/// set is used (a convenience: "mint me a token with everything I can do"). De-duplicates while
/// preserving first-seen order. A `*` in `granted` is itself a scope, so a `*` request against a
/// `*` grant yields `["*"]` (a full-authority PAT — only a system admin holds `*` to begin with).
pub fn clamp_scopes(requested: &[String], granted: &[String]) -> Vec<String> {
    use std::collections::BTreeSet;
    let granted_set: BTreeSet<&str> = granted.iter().map(String::as_str).collect();
    // No explicit ask ⇒ grant everything the minter holds (already their own authority).
    let source: &[String] = if requested.is_empty() {
        granted
    } else {
        requested
    };
    let mut seen: BTreeSet<String> = BTreeSet::new();
    let mut out = Vec::new();
    for scope in source {
        if granted_set.contains(scope.as_str()) && seen.insert(scope.clone()) {
            out.push(scope.clone());
        }
    }
    out
}

/// Draw `N` bytes from the OS cryptographically-secure RNG. A PAT is a bearer credential, so its
/// bytes MUST be unpredictable — same primitive (and panic-on-failure rationale) as
/// [`RefreshToken`](crate::RefreshToken).
fn random_bytes<const N: usize>() -> [u8; N] {
    let mut out = [0u8; N];
    getrandom::getrandom(&mut out).expect("OS CSPRNG (getrandom) must be available to mint tokens");
    out
}

/// Append the lowercase-hex encoding of `bytes` to `s`.
fn push_hex(s: &mut String, bytes: &[u8]) {
    for b in bytes {
        s.push(char::from_digit((b >> 4) as u32, 16).unwrap());
        s.push(char::from_digit((b & 0x0f) as u32, 16).unwrap());
    }
}

/// Lowercase-hex encode `bytes` into a fresh string.
fn hex_string(bytes: &[u8]) -> String {
    let mut s = String::with_capacity(bytes.len() * 2);
    push_hex(&mut s, bytes);
    s
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn generated_tokens_carry_the_prefix_and_256_bits() {
        let tok = PatToken::generate();
        assert!(
            has_pat_prefix(tok.as_str()),
            "a PAT must carry the gtpat_ prefix"
        );
        // gtpat_ + 64 hex chars (256 bits).
        assert_eq!(tok.as_str().len(), PAT_PREFIX.len() + 64);
        let hex = &tok.as_str()[PAT_PREFIX.len()..];
        assert!(hex.chars().all(|c| c.is_ascii_hexdigit()));
        // Two draws differ.
        assert_ne!(PatToken::generate(), PatToken::generate());
    }

    #[test]
    fn from_bytes_is_deterministic_and_prefixed() {
        let tok = PatToken::from_bytes(&[0x00, 0x0f, 0xff, 0xa5]);
        assert_eq!(tok.as_str(), "gtpat_000fffa5");
    }

    #[test]
    fn has_pat_prefix_rejects_a_jwt_shape() {
        // A JWT is three base64url segments — never the gtpat_ prefix.
        assert!(!has_pat_prefix("eyJhbG.eyJzdWI.sig"));
        assert!(!has_pat_prefix(""));
        assert!(has_pat_prefix("gtpat_deadbeef"));
    }

    #[test]
    fn clamp_blocks_escalation_to_an_unheld_scope() {
        let granted = vec!["tokens.read".to_string(), "issues.read".to_string()];
        // Asking for a scope you do NOT hold drops it — no escalation.
        let got = clamp_scopes(
            &["tokens.read".into(), "tokens.write".into(), "*".into()],
            &granted,
        );
        assert_eq!(got, vec!["tokens.read".to_string()]);
    }

    #[test]
    fn clamp_empty_request_grants_the_full_held_authority() {
        let granted = vec!["tokens.read".to_string(), "tokens.write".to_string()];
        assert_eq!(clamp_scopes(&[], &granted), granted);
    }

    #[test]
    fn clamp_dedups_while_keeping_first_seen_order() {
        let granted = vec!["a".to_string(), "b".to_string()];
        let got = clamp_scopes(&["b".into(), "a".into(), "b".into()], &granted);
        assert_eq!(got, vec!["b".to_string(), "a".to_string()]);
    }

    #[test]
    fn clamp_of_star_request_against_star_grant_yields_star() {
        // Only a system admin holds `*`; they may mint a full-authority PAT.
        assert_eq!(
            clamp_scopes(&["*".into()], &["*".to_string()]),
            vec!["*".to_string()]
        );
        // A non-admin asking for `*` they do not hold gets nothing of it.
        assert!(clamp_scopes(&["*".into()], &["issues.read".to_string()]).is_empty());
    }

    #[test]
    fn is_expired_honours_a_null_expiry() {
        let mut rec = PatRecord {
            id: PatId::generate(),
            sub: "u".into(),
            workspace: "acme".into(),
            name: "ci".into(),
            scopes: vec![],
            created_at: 0,
            expires_at: None,
            last_used_at: None,
            status: PatStatus::Active,
        };
        // No expiry ⇒ never expires.
        assert!(!rec.is_expired(u64::MAX));
        // With an expiry, `exp` is exclusive: now == exp is already expired.
        rec.expires_at = Some(100);
        assert!(!rec.is_expired(99));
        assert!(rec.is_expired(100));
        assert!(rec.is_expired(101));
    }
}
