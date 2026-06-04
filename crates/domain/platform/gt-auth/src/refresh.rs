//! Refresh tokens: long-lived opaque credentials that mint fresh access tokens without a
//! re-login, with **rotation** and **reuse detection** (`hq-auth-mint.3`).
//!
//! ## Why a second token
//!
//! Access tokens ([`JwtClaims`](crate::JwtClaims), minted by [`JwtMinter`](crate::JwtMinter))
//! are deliberately short-lived so a leaked one expires fast. A **refresh token** is the
//! long-lived counterpart a client trades in for a fresh access token. It is *opaque* (a
//! high-entropy random string, not a signed JWT): it carries no claims and is only ever
//! validated by looking it up in a store, so it can be revoked the instant theft is suspected —
//! something a stateless JWT cannot do.
//!
//! ## Rotation + reuse detection (the security model)
//!
//! Every refresh belongs to a **family** (one login session). [`RefreshStore::rotate`] is
//! single-use: presenting an active token marks it `rotated` and returns a fresh successor in
//! the same family. Presenting an **already-rotated** token is the theft signal — a legitimate
//! client never reuses one, so seeing it means the token leaked and *both* the attacker and the
//! victim hold a copy. The response is to [`revoke_family`](RefreshStore::revoke_family) the
//! whole chain ([`RefreshError::Reused`]); every later rotate of any token in that family then
//! fails [`Revoked`](RefreshError::Revoked). This is the OWASP / RFC 6819 §5.2.2.3 "refresh
//! token rotation with automatic reuse detection" pattern.
//!
//! ## Shape (hexagonal — docs/03)
//!
//! - [`RefreshToken`] — the opaque secret newtype.
//! - [`RefreshRecord`] — the server-side bookkeeping for one token (family, subject, clock
//!   bounds, status).
//! - [`RefreshStore`] — the inverted **port**.
//! - [`InMemoryRefreshStore`] — the dependency-free [`Mutex`]-backed adapter, for tests and the
//!   no-DB path. A Postgres-backed adapter (single-statement compare-and-swap rotation, so the
//!   reuse race is settled by the DB) is a **deliberate follow-up** and lives behind a `pg`
//!   feature there — it is *not* added here; this crate stays free of `sqlx`/pg (Rule 4).
//! - [`RefreshError`] — the rejection vocabulary, separate from
//!   [`AuthError`](crate::AuthError) (refresh failures are store lookups, not signature/claims
//!   verdicts, so overloading the bearer-token enum would muddy both).
//!
//! ## Determinism (injected clock — no wall-clock reads)
//!
//! The store never reads the wall clock. Expiry (`exp`) is supplied at [`issue`]-time and every
//! [`rotate`] takes `now` as a parameter, exactly as [`JwtClaims::validate`](crate::JwtClaims::validate)
//! injects `now_secs` — so the slice is fully deterministic under test.
//!
//! ## Entropy source
//!
//! Refresh tokens are bearer credentials, so their bytes come straight from the OS
//! cryptographically-secure RNG via the `getrandom` crate: [`RefreshToken::generate`] draws 256
//! bits, [`RefreshId::generate`] 128. `getrandom` is a tiny, dependency-light primitive (the
//! same OS-entropy source `rand` sits on) and crossing no tier boundary — it is an external
//! utility, not a domain/platform crate, so Rule 4 is untouched. For fully deterministic tests
//! there is [`RefreshToken::from_bytes`], which hex-encodes caller-supplied entropy.
//!
//! [`issue`]: RefreshStore::issue
//! [`rotate`]: RefreshStore::rotate

use std::collections::HashMap;
use std::sync::Mutex;

use thiserror::Error;

/// A high-entropy **opaque** refresh token — a random hex string with no internal structure.
///
/// It is a bearer secret: whoever holds it can rotate it. Compared only by value (lookups in
/// the store), never parsed. Mint one with [`generate`](Self::generate) (OS entropy) or
/// [`from_bytes`](Self::from_bytes) (caller-supplied entropy, for deterministic tests).
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct RefreshToken(String);

impl RefreshToken {
    /// Wrap an already-formed opaque string as a token (e.g. one read back from a store). Most
    /// callers want [`generate`](Self::generate) instead, which produces fresh entropy.
    pub fn new(value: impl Into<String>) -> Self {
        Self(value.into())
    }

    /// Hex-encode caller-supplied `bytes` into a token. The entropy is exactly the bytes you
    /// pass, so this is the deterministic constructor used by tests (and by any caller that
    /// already holds a CSPRNG and wants to inject its own randomness).
    pub fn from_bytes(bytes: &[u8]) -> Self {
        let mut s = String::with_capacity(bytes.len() * 2);
        for b in bytes {
            // Lowercase hex, two nibbles per byte.
            s.push(char::from_digit((b >> 4) as u32, 16).unwrap());
            s.push(char::from_digit((b & 0x0f) as u32, 16).unwrap());
        }
        Self(s)
    }

    /// Mint a fresh token with 256 bits of entropy drawn from the OS CSPRNG (`getrandom`) —
    /// wide enough that collisions are negligible and the value is unpredictable.
    pub fn generate() -> Self {
        Self::from_bytes(&random_bytes::<32>())
    }

    /// The opaque string, for storage/transport.
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

/// An opaque identifier (token id or family id). Random like the token itself so it is
/// unguessable; kept distinct from the secret [`RefreshToken`] so an id can be logged/keyed
/// without exposing the credential.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct RefreshId(String);

impl RefreshId {
    /// Mint a fresh random id (128 bits — ample for a non-secret key).
    fn generate() -> Self {
        Self(RefreshToken::from_bytes(&random_bytes::<16>()).0)
    }

    /// The id as a string.
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

/// Draw `N` bytes from the OS cryptographically-secure RNG.
///
/// Refresh tokens are bearer credentials: their bytes MUST be unpredictable and uncorrelated,
/// so this goes straight to the kernel CSPRNG via `getrandom` (get/dev/urandom, `getrandom(2)`,
/// `RtlGenRandom`, …) — never `RandomState`/`HashMap` seeds, whose per-thread keys increment
/// deterministically between instances and would make issued tokens correlated and guessable.
/// A getrandom failure means the OS entropy source is unavailable; that is unrecoverable for a
/// security primitive, so we panic rather than mint a weak token.
fn random_bytes<const N: usize>() -> [u8; N] {
    let mut out = [0u8; N];
    getrandom::getrandom(&mut out).expect("OS CSPRNG (getrandom) must be available to mint tokens");
    out
}

/// Lifecycle of a single refresh token in its family.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum RefreshStatus {
    /// Live and unused — the one token in its family a client may rotate.
    Active,
    /// Already exchanged for a successor. Presenting it again is the reuse/theft signal.
    Rotated,
    /// The whole family was killed (logout or reuse-triggered revocation).
    Revoked,
}

/// Server-side bookkeeping for one refresh token. The secret string itself is **not** stored
/// here — only its [`id`](Self::id); the store keys the secret separately so a record can be
/// returned to a caller (e.g. for an audit log) without leaking the credential.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RefreshRecord {
    /// This token's own id.
    pub id: RefreshId,
    /// The family (session) this token belongs to. Stable across a rotation chain.
    pub family: RefreshId,
    /// Subject — the authenticated principal the access token will name (`sub`).
    pub sub: String,
    /// Tenant the session is scoped to (mirrors the `workspace` access-token claim).
    pub workspace: String,
    /// Authorization scopes granted to the session, carried so a rotation can re-mint a
    /// faithful access token without re-querying the user store. Inherited unchanged across a
    /// rotation chain.
    pub scopes: Vec<String>,
    /// Issued-at, seconds since the Unix epoch (injected, never read from the wall clock).
    pub issued_at: u64,
    /// Expiry, seconds since the Unix epoch. Rotation past this instant is
    /// [`RefreshError::Expired`].
    pub expires_at: u64,
    /// Where this token sits in its lifecycle.
    pub status: RefreshStatus,
}

impl RefreshRecord {
    /// True when `now` is at or past [`expires_at`](Self::expires_at) (`exp` is exclusive, matching
    /// [`JwtClaims::validate`](crate::JwtClaims::validate)).
    pub fn is_expired(&self, now: u64) -> bool {
        now >= self.expires_at
    }
}

/// Why a [`rotate`](RefreshStore::rotate) was rejected. Separate from
/// [`AuthError`](crate::AuthError): these are refresh-store verdicts (lookup + lifecycle), not
/// signature/claims checks.
#[derive(Debug, Clone, PartialEq, Eq, Error)]
#[non_exhaustive]
pub enum RefreshError {
    /// No such token in the store — never issued, or already pruned.
    #[error("unknown refresh token")]
    Unknown,
    /// The token is known but past its `expires_at`.
    #[error("refresh token expired")]
    Expired,
    /// The token was already rotated: a single-use token presented twice. The store has
    /// **revoked the whole family** in response — this is the theft signal.
    #[error("refresh token reuse detected; family revoked")]
    Reused,
    /// The token's family was revoked (an explicit logout, or a prior reuse on a sibling).
    #[error("refresh token revoked")]
    Revoked,
}

/// The inverted **port**: issue, rotate, and revoke refresh tokens.
///
/// Synchronous on purpose. The reference adapter ([`InMemoryRefreshStore`]) does only in-memory
/// map work — no I/O — so an `async` signature would force callers onto a runtime for nothing,
/// the same call this crate already makes for [`Authenticator`](crate::Authenticator). The
/// Postgres follow-up adapter does its rotation in a single compare-and-swap statement; it can
/// expose that behind a thin sync wrapper (block-on at its own edge) or grow an `async`
/// sibling trait there without disturbing this clean, runtime-free core.
pub trait RefreshStore {
    /// Mint a new token in a **new** family for `sub`/`workspace` carrying `scopes`, valid until
    /// `exp` and issued at `issued_at` (both injected — the store reads no clock). Returns the
    /// secret to hand to the client plus its server-side [`RefreshRecord`].
    fn issue(
        &self,
        sub: &str,
        workspace: &str,
        scopes: &[String],
        issued_at: u64,
        exp: u64,
    ) -> (RefreshToken, RefreshRecord);

    /// Exchange an **active** `token` for a fresh successor in the same family, against the
    /// injected clock `now`.
    ///
    /// - active & unexpired → mark it [`Rotated`](RefreshStatus::Rotated), mint a successor
    ///   (same family, inheriting `sub`/`workspace`, `issued_at = now`, same `expires_at`).
    /// - already rotated → [`RefreshError::Reused`] **and revoke the whole family** (theft).
    /// - family already revoked → [`RefreshError::Revoked`].
    /// - expired → [`RefreshError::Expired`].
    /// - unknown → [`RefreshError::Unknown`].
    fn rotate(
        &self,
        token: &RefreshToken,
        now: u64,
    ) -> Result<(RefreshToken, RefreshRecord), RefreshError>;

    /// Kill every token in `family` (logout). Idempotent — revoking an already-dead or unknown
    /// family is a no-op.
    fn revoke_family(&self, family: &RefreshId);

    /// Revoke the family the presented `token` belongs to (logout by refresh token). Resolves the
    /// token to its family and kills it. Idempotent: an unknown token is a no-op, so logout never
    /// errors and cannot be used to probe which tokens exist.
    fn revoke_by_token(&self, token: &RefreshToken);
}

/// A [`Mutex`]-guarded in-memory [`RefreshStore`] for tests and the no-database path.
///
/// Two maps under one lock: `tokens` resolves a presented secret to its [`RefreshId`], and
/// `records` holds the bookkeeping by id. The split mirrors a real store (a secret-hash index
/// plus a records table) and lets [`revoke_family`](RefreshStore::revoke_family) sweep a family
/// without re-hashing every secret.
///
/// The Postgres-backed adapter — where rotation is a single `UPDATE ... WHERE status='active'
/// RETURNING ...` so the reuse check and the status flip are one atomic step — is a deliberate
/// **follow-up**; it is *not* implemented here and this crate pulls no `sqlx`/pg dependency.
#[derive(Debug, Default)]
pub struct InMemoryRefreshStore {
    inner: Mutex<Inner>,
}

#[derive(Debug, Default)]
struct Inner {
    /// Presented secret → its record id.
    tokens: HashMap<RefreshToken, RefreshId>,
    /// Record id → bookkeeping.
    records: HashMap<RefreshId, RefreshRecord>,
}

impl InMemoryRefreshStore {
    /// An empty store.
    pub fn new() -> Self {
        Self::default()
    }
}

impl Inner {
    /// Insert a freshly-minted token+record pair. The token secret is generated here; the
    /// record's `id`/`family` are set by the caller.
    fn insert(&mut self, record: RefreshRecord) -> RefreshToken {
        // Mint a unique secret. RefreshToken::generate draws OS entropy; the loop is a
        // belt-and-suspenders guard against the (astronomically unlikely) duplicate.
        let token = loop {
            let candidate = RefreshToken::generate();
            if !self.tokens.contains_key(&candidate) {
                break candidate;
            }
        };
        self.tokens.insert(token.clone(), record.id.clone());
        self.records.insert(record.id.clone(), record);
        token
    }

    /// Mark every record sharing `family` as revoked.
    fn revoke_family(&mut self, family: &RefreshId) {
        for record in self.records.values_mut() {
            if &record.family == family {
                record.status = RefreshStatus::Revoked;
            }
        }
    }
}

impl RefreshStore for InMemoryRefreshStore {
    fn issue(
        &self,
        sub: &str,
        workspace: &str,
        scopes: &[String],
        issued_at: u64,
        exp: u64,
    ) -> (RefreshToken, RefreshRecord) {
        let mut inner = self.inner.lock().expect("refresh store mutex poisoned");
        let record = RefreshRecord {
            id: RefreshId::generate(),
            // A fresh login opens a fresh family.
            family: RefreshId::generate(),
            sub: sub.to_owned(),
            workspace: workspace.to_owned(),
            scopes: scopes.to_vec(),
            issued_at,
            expires_at: exp,
            status: RefreshStatus::Active,
        };
        let token = inner.insert(record.clone());
        (token, record)
    }

    fn rotate(
        &self,
        token: &RefreshToken,
        now: u64,
    ) -> Result<(RefreshToken, RefreshRecord), RefreshError> {
        let mut inner = self.inner.lock().expect("refresh store mutex poisoned");

        let id = inner.tokens.get(token).cloned().ok_or(RefreshError::Unknown)?;
        // The id was in `tokens`, so the record exists; clone the fields we need before we let
        // go of the immutable borrow.
        let (status, family, sub, workspace, scopes, expires_at) = {
            let record = inner.records.get(&id).expect("token id without a record");
            (
                record.status,
                record.family.clone(),
                record.sub.clone(),
                record.workspace.clone(),
                record.scopes.clone(),
                record.expires_at,
            )
        };

        match status {
            // Revoked beats every other verdict — a dead family stays dead even past exp.
            RefreshStatus::Revoked => Err(RefreshError::Revoked),
            // Reuse of a single-use token: the theft signal. Burn the whole family down.
            RefreshStatus::Rotated => {
                inner.revoke_family(&family);
                Err(RefreshError::Reused)
            }
            RefreshStatus::Active => {
                if now >= expires_at {
                    return Err(RefreshError::Expired);
                }
                // Single-use: this token is spent.
                inner
                    .records
                    .get_mut(&id)
                    .expect("token id without a record")
                    .status = RefreshStatus::Rotated;
                // Mint the successor in the same family.
                let successor = RefreshRecord {
                    id: RefreshId::generate(),
                    family,
                    sub,
                    workspace,
                    scopes,
                    issued_at: now,
                    expires_at,
                    status: RefreshStatus::Active,
                };
                let new_token = inner.insert(successor.clone());
                Ok((new_token, successor))
            }
        }
    }

    fn revoke_family(&self, family: &RefreshId) {
        let mut inner = self.inner.lock().expect("refresh store mutex poisoned");
        inner.revoke_family(family);
    }

    fn revoke_by_token(&self, token: &RefreshToken) {
        let mut inner = self.inner.lock().expect("refresh store mutex poisoned");
        // Resolve token -> id -> family, then sweep the family. Unknown token: nothing to do.
        if let Some(id) = inner.tokens.get(token).cloned() {
            if let Some(family) = inner.records.get(&id).map(|r| r.family.clone()) {
                inner.revoke_family(&family);
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const HOUR: u64 = 3600;

    #[test]
    fn scopes_are_carried_and_inherited_across_rotation() {
        let store = InMemoryRefreshStore::new();
        let scopes = vec!["rig.read".to_string(), "rig.write".to_string()];
        let (tok, rec) = store.issue("alice", "acme", &scopes, 0, HOUR);
        assert_eq!(rec.scopes, scopes);
        // The successor inherits the granted scopes so a refresh can re-mint a faithful access.
        let (_next, next_rec) = store.rotate(&tok, 10).unwrap();
        assert_eq!(next_rec.scopes, scopes);
    }

    #[test]
    fn revoke_by_token_kills_the_family_and_is_idempotent_for_unknown_tokens() {
        let store = InMemoryRefreshStore::new();
        let (tok, _) = store.issue("alice", "acme", &[], 0, HOUR);
        store.revoke_by_token(&tok);
        assert_eq!(store.rotate(&tok, 10), Err(RefreshError::Revoked));
        // Unknown token: no-op, no panic.
        store.revoke_by_token(&RefreshToken::new("never-issued"));
    }

    #[test]
    fn issue_then_rotate_swaps_the_token_and_invalidates_the_old_one() {
        let store = InMemoryRefreshStore::new();
        let (tok, rec) = store.issue("alice", "acme", &[],0, HOUR);
        assert_eq!(rec.sub, "alice");
        assert_eq!(rec.workspace, "acme");
        assert_eq!(rec.status, RefreshStatus::Active);

        let (next, next_rec) = store.rotate(&tok, 10).unwrap();
        // New token is a different secret in the same family, inheriting sub/workspace.
        assert_ne!(tok, next);
        assert_eq!(next_rec.family, rec.family);
        assert_eq!(next_rec.sub, "alice");
        assert_eq!(next_rec.workspace, "acme");
        assert_eq!(next_rec.issued_at, 10);
        assert_eq!(next_rec.expires_at, HOUR);

        // The successor rotates fine; the chain is healthy so far.
        assert!(store.rotate(&next, 20).is_ok());
    }

    #[test]
    fn rotating_the_same_token_twice_is_reuse_and_revokes_the_family() {
        let store = InMemoryRefreshStore::new();
        let (tok, _) = store.issue("alice", "acme", &[],0, HOUR);

        let (successor, _) = store.rotate(&tok, 10).unwrap();
        // Presenting the spent token again → reuse detected.
        assert_eq!(store.rotate(&tok, 20), Err(RefreshError::Reused));
        // The family is now revoked, so even the legitimate latest successor is dead.
        assert_eq!(store.rotate(&successor, 30), Err(RefreshError::Revoked));
    }

    #[test]
    fn an_expired_token_cannot_be_rotated() {
        let store = InMemoryRefreshStore::new();
        let (tok, _) = store.issue("alice", "acme", &[],0, 100);
        // exp is exclusive: now == exp is already expired.
        assert_eq!(store.rotate(&tok, 100), Err(RefreshError::Expired));
        assert_eq!(store.rotate(&tok, 101), Err(RefreshError::Expired));
        // The token was never spent (expiry short-circuits before the status flip), so a
        // presentation back inside the window still works.
        assert!(store.rotate(&tok, 99).is_ok());
    }

    #[test]
    fn an_unknown_token_is_rejected() {
        let store = InMemoryRefreshStore::new();
        let stranger = RefreshToken::from_bytes(&[0xde, 0xad, 0xbe, 0xef]);
        assert_eq!(store.rotate(&stranger, 0), Err(RefreshError::Unknown));
    }

    #[test]
    fn revoke_family_then_rotate_is_revoked() {
        let store = InMemoryRefreshStore::new();
        let (tok, rec) = store.issue("alice", "acme", &[],0, HOUR);
        store.revoke_family(&rec.family);
        assert_eq!(store.rotate(&tok, 10), Err(RefreshError::Revoked));
    }

    #[test]
    fn revoke_family_is_idempotent_and_safe_for_unknown_families() {
        let store = InMemoryRefreshStore::new();
        let (tok, rec) = store.issue("alice", "acme", &[],0, HOUR);
        store.revoke_family(&rec.family);
        // Second revoke is a no-op, not a panic.
        store.revoke_family(&rec.family);
        // Revoking an id no family uses is harmless.
        store.revoke_family(&RefreshId::generate());
        assert_eq!(store.rotate(&tok, 10), Err(RefreshError::Revoked));
    }

    #[test]
    fn generated_tokens_are_unique_across_many_issues() {
        use std::collections::HashSet;
        let store = InMemoryRefreshStore::new();
        let mut seen = HashSet::new();
        for _ in 0..10_000 {
            let (tok, _) = store.issue("alice", "acme", &[],0, HOUR);
            assert!(seen.insert(tok), "duplicate refresh token generated");
        }
    }

    #[test]
    fn from_bytes_hex_encodes_entropy_deterministically() {
        let tok = RefreshToken::from_bytes(&[0x00, 0x0f, 0xff, 0xa5]);
        assert_eq!(tok.as_str(), "000fffa5");
    }

    #[test]
    fn generate_yields_distinct_high_entropy_tokens() {
        // 256 bits → 64 hex chars; two draws must differ.
        let a = RefreshToken::generate();
        let b = RefreshToken::generate();
        assert_eq!(a.as_str().len(), 64);
        assert_ne!(a, b);
    }

    #[test]
    fn a_fresh_issue_opens_an_independent_family() {
        // Reuse on one session must not touch another.
        let store = InMemoryRefreshStore::new();
        let (tok_a, _) = store.issue("alice", "acme", &[],0, HOUR);
        let (tok_b, rec_b) = store.issue("alice", "acme", &[],0, HOUR);

        // Trip reuse detection on session A.
        let _ = store.rotate(&tok_a, 10).unwrap();
        assert_eq!(store.rotate(&tok_a, 20), Err(RefreshError::Reused));

        // Session B is untouched and rotates cleanly.
        let (next_b, next_rec_b) = store.rotate(&tok_b, 10).unwrap();
        assert_eq!(next_rec_b.family, rec_b.family);
        assert_ne!(tok_b, next_b);
    }
}
