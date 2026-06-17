//! Sling-time credential selection (gtcore-bf4acd).
//!
//! The incident this fixes: orchd pinned an account as **active** for slinging while its
//! `.credentials.json` had expired ~12h earlier. `quota_list` reported the account `Healthy`
//! because that status measures quota USAGE, not credential VALIDITY — so every polecat slung onto
//! it was born in `401 Invalid authentication credentials` and produced nothing.
//!
//! This is the PURE half of the fix: classify a credential file's health and pick a slingable
//! account from an ordered candidate list. No clock reads, no I/O — the edge
//! (`gt-composition::credential_guard`) reads the files and the wall clock and hands the results
//! here, exactly like [`crate::oauth_usage`] keeps parsing pure for the usage prober.
//!
//! Health vs. quota are independent axes: an account can be quota-`Healthy` (plenty of budget) yet
//! credential-dead (expired token, no refresh), and selection must reject the latter. The two
//! signals are deliberately NOT merged here — the caller orders candidates by quota preference and
//! this module filters that order down to the ones that can actually authenticate.

use crate::oauth_usage::parse_credentials_json;

/// How usable an account's stored `.credentials.json` is RIGHT NOW (at the `now_ms` the edge
/// stamped), independent of quota.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CredentialHealth {
    /// Access token valid past the refresh skew — usable as-is.
    Valid,
    /// Access token expired (or within the skew margin) but a refresh token is stored, so the
    /// `claude` CLI (and the periodic usage prober) can mint a fresh access token on use. Still
    /// slingable: the polecat refreshes at startup rather than being born in 401.
    Refreshable,
    /// Access token expired and NO refresh token stored — dead, cannot be revived. Slinging onto
    /// it means a guaranteed 401.
    Expired,
    /// File missing, unreadable, or unparseable — cannot authenticate, treat as unusable.
    Unreadable,
}

impl CredentialHealth {
    /// True when an account in this state can be slung without being born in 401: a valid token,
    /// or an expired one the CLI can refresh itself.
    pub fn is_slingable(self) -> bool {
        matches!(self, CredentialHealth::Valid | CredentialHealth::Refreshable)
    }
}

/// Classify one account's raw `.credentials.json` contents. `None` ⇒ the file was missing or could
/// not be read at the edge (Unreadable). `now_ms`/`skew_ms` come from the edge so this stays pure.
pub fn classify_credentials(raw: Option<&str>, now_ms: u64, skew_ms: u64) -> CredentialHealth {
    let Some(raw) = raw else {
        return CredentialHealth::Unreadable;
    };
    let creds = match parse_credentials_json(raw) {
        Ok(c) => c,
        Err(_) => return CredentialHealth::Unreadable,
    };
    if !creds.needs_refresh(now_ms, skew_ms) {
        CredentialHealth::Valid
    } else if creds.refresh_token.is_some() {
        CredentialHealth::Refreshable
    } else {
        CredentialHealth::Expired
    }
}

/// A candidate account the caller offers to selection, paired with its raw credential file. The
/// caller passes candidates in quota-preference order (typically Healthy-first); selection keeps
/// that order and filters on credential health.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Candidate {
    pub account: String,
    /// Raw `.credentials.json` contents, or `None` when the file is missing/unreadable on disk.
    pub creds: Option<String>,
}

/// An account that could NOT be slung because its credentials are expired/dead/unreadable. Carries
/// the health so the caller can alert the operator with a concrete reason.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DeadAccount {
    pub account: String,
    pub health: CredentialHealth,
}

impl DeadAccount {
    /// Operator-facing reason fragment explaining why the account was rejected. Reads as an
    /// adjective phrase so callers can embed it (e.g. "las credenciales están {reason}").
    pub fn reason(&self) -> &'static str {
        match self.health {
            CredentialHealth::Expired => "expiradas sin refresh token",
            CredentialHealth::Unreadable => "ausentes o ilegibles",
            // Slingable states never become a DeadAccount; keep the match total.
            CredentialHealth::Valid | CredentialHealth::Refreshable => "válidas",
        }
    }
}

/// The outcome of [`select_slingable`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Selection {
    /// Sling onto this account; `health` is `Valid` or `Refreshable`.
    Use {
        account: String,
        health: CredentialHealth,
    },
    /// No candidate has slingable credentials — the caller must block the sling and alert rather
    /// than birth a polecat into 401.
    NoValidAccount,
}

/// Pick a sling-ready account.
///
/// The `preferred` account (the keychain's live/active pointer) wins when its credentials are
/// slingable — rotation already chose it for quota reasons, so we don't move the pointer
/// gratuitously. Otherwise the FIRST candidate in `candidates` order whose credentials are
/// slingable is chosen (the caller passes them in rotation-preference order). Every candidate whose
/// credentials are Expired/Unreadable is collected into the returned `dead` list so the caller can
/// raise a credential alert — including `preferred` itself when it had to be skipped.
///
/// `candidates` should include the `preferred` account; a `preferred` not present among the
/// candidates is simply ignored (it has no creds to validate).
pub fn select_slingable(
    preferred: Option<&str>,
    candidates: &[Candidate],
    now_ms: u64,
    skew_ms: u64,
) -> (Selection, Vec<DeadAccount>) {
    let mut dead = Vec::new();
    let mut first_slingable: Option<usize> = None;
    let mut preferred_idx: Option<usize> = None;
    for (i, c) in candidates.iter().enumerate() {
        let health = classify_credentials(c.creds.as_deref(), now_ms, skew_ms);
        if health.is_slingable() {
            if first_slingable.is_none() {
                first_slingable = Some(i);
            }
            // Prefer the active pointer when it is slingable — no gratuitous rotation.
            if preferred == Some(c.account.as_str()) {
                preferred_idx = Some(i);
            }
        } else {
            dead.push(DeadAccount {
                account: c.account.clone(),
                health,
            });
        }
    }

    // The active pointer wins when slingable; otherwise the first slingable candidate in order.
    match preferred_idx.or(first_slingable) {
        Some(i) => {
            let c = &candidates[i];
            let health = classify_credentials(c.creds.as_deref(), now_ms, skew_ms);
            (
                Selection::Use {
                    account: c.account.clone(),
                    health,
                },
                dead,
            )
        }
        None => (Selection::NoValidAccount, dead),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// `.credentials.json` body with a controllable expiry and optional refresh token.
    fn creds(expires_at_ms: Option<u64>, refresh: bool) -> String {
        let rt = if refresh {
            r#","refreshToken":"rt-1""#
        } else {
            ""
        };
        let exp = expires_at_ms
            .map(|e| format!(r#","expiresAt":{e}"#))
            .unwrap_or_default();
        format!(r#"{{"claudeAiOauth":{{"accessToken":"at-1"{rt}{exp}}}}}"#)
    }

    const SKEW: u64 = 5 * 60 * 1000;

    #[test]
    fn classify_covers_every_health_axis() {
        let now = 1_000_000_000;
        // Valid: expiry well in the future.
        assert_eq!(
            classify_credentials(Some(&creds(Some(now + 3_600_000), true)), now, SKEW),
            CredentialHealth::Valid
        );
        // Refreshable: expired but a refresh token is present.
        assert_eq!(
            classify_credentials(Some(&creds(Some(now - 1), true)), now, SKEW),
            CredentialHealth::Refreshable
        );
        // Expired: past expiry, no refresh token.
        assert_eq!(
            classify_credentials(Some(&creds(Some(now - 1), false)), now, SKEW),
            CredentialHealth::Expired
        );
        // Unreadable: missing file, or garbage.
        assert_eq!(
            classify_credentials(None, now, SKEW),
            CredentialHealth::Unreadable
        );
        assert_eq!(
            classify_credentials(Some("not json"), now, SKEW),
            CredentialHealth::Unreadable
        );
        // Unknown expiry is Valid (the edge finds out via a 401 and refreshes then).
        assert_eq!(
            classify_credentials(Some(&creds(None, true)), now, SKEW),
            CredentialHealth::Valid
        );
    }

    #[test]
    fn active_account_with_dead_creds_is_skipped_to_a_healthy_one() {
        // The incident: active account `dead` is quota-Healthy but its token expired with no
        // refresh. Selection must skip it for a credential-valid alternative and report it dead.
        let now = 1_000_000_000;
        let candidates = vec![
            Candidate {
                account: "dead".into(),
                creds: Some(creds(Some(now - 1), false)),
            },
            Candidate {
                account: "fresh".into(),
                creds: Some(creds(Some(now + 3_600_000), true)),
            },
        ];
        let (sel, dead) = select_slingable(Some("dead"), &candidates, now, SKEW);
        assert_eq!(
            sel,
            Selection::Use {
                account: "fresh".into(),
                health: CredentialHealth::Valid
            }
        );
        assert_eq!(
            dead,
            vec![DeadAccount {
                account: "dead".into(),
                health: CredentialHealth::Expired
            }]
        );
    }

    #[test]
    fn preferred_active_wins_when_slingable() {
        // A valid active pointer is NOT moved just because earlier candidates are also valid.
        let now = 1_000_000_000;
        let candidates = vec![
            Candidate {
                account: "a".into(),
                creds: Some(creds(Some(now + 10_000_000), true)),
            },
            Candidate {
                account: "b".into(),
                creds: Some(creds(Some(now + 10_000_000), true)),
            },
        ];
        let (sel, dead) = select_slingable(Some("b"), &candidates, now, SKEW);
        assert_eq!(
            sel,
            Selection::Use {
                account: "b".into(),
                health: CredentialHealth::Valid
            }
        );
        assert!(dead.is_empty());
    }

    #[test]
    fn refreshable_active_is_used_directly() {
        // Expired-but-refreshable active stays selected: the CLI refreshes at startup.
        let now = 1_000_000_000;
        let candidates = vec![Candidate {
            account: "a".into(),
            creds: Some(creds(Some(now - 1), true)),
        }];
        let (sel, dead) = select_slingable(Some("a"), &candidates, now, SKEW);
        assert_eq!(
            sel,
            Selection::Use {
                account: "a".into(),
                health: CredentialHealth::Refreshable
            }
        );
        assert!(dead.is_empty());
    }

    #[test]
    fn no_valid_account_when_every_candidate_is_dead() {
        let now = 1_000_000_000;
        let candidates = vec![
            Candidate {
                account: "a".into(),
                creds: Some(creds(Some(now - 1), false)),
            },
            Candidate {
                account: "b".into(),
                creds: None,
            },
        ];
        let (sel, dead) = select_slingable(Some("a"), &candidates, now, SKEW);
        assert_eq!(sel, Selection::NoValidAccount);
        assert_eq!(dead.len(), 2);
        assert_eq!(dead[0].health, CredentialHealth::Expired);
        assert_eq!(dead[1].health, CredentialHealth::Unreadable);
    }

    #[test]
    fn empty_candidates_is_no_valid_account() {
        let (sel, dead) = select_slingable(None, &[], 0, SKEW);
        assert_eq!(sel, Selection::NoValidAccount);
        assert!(dead.is_empty());
    }
}
