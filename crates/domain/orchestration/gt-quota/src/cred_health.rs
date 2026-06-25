//! Per-account credential health report (gtcore-1fe9b4).
//!
//! The wedge incident (2026-06-18, memory `orchd-sling-provisioning-wedge-recovery`): an account
//! dir can be "alive" on one axis yet unslingable on another, and `quota.list` shows neither — it
//! reports quota USAGE, not credential VALIDITY nor onboarding state. A dir with a fresh
//! `.credentials.json` but NO `oauthAccount`/onboarding flags in `.claude.json` looks healthy to
//! rotation but births a polecat into the first-run onboarding TUI → OAuth prompt → wedge.
//!
//! This is the PURE half of the cred-health surface: given the two on-disk facts the edge reads
//! (the raw `.credentials.json` body and whether `.claude.json` carries the onboarding markers),
//! classify an account's slingability across BOTH axes and decide whether it needs a relogin. No
//! clock reads, no I/O — `gt-composition` reads the files + wall clock and feeds the results here,
//! exactly like [`crate::credential_select`] and [`crate::oauth_usage`] keep their parses pure.
//!
//! `needs_relogin` is the single signal the FE acts on: `true` when the account cannot be slung
//! as-is — either its credential is dead/unreadable, OR its dir is missing the onboarding markers
//! that make it consumable (the wedge cause). Either way `quota.relogin` is the fix: it refreshes
//! the credential AND seeds the onboarding-complete state in one flow.

use serde::{Deserialize, Serialize};

use crate::credential_select::{classify_credentials, CredentialHealth};

/// One account's credential health across the two slingability axes the wedge incident exposed.
///
/// Serializable so the REST/MCP cred-health endpoint emits it verbatim (the FE renders the badge
/// off `needs_relogin` + the reason fields).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CredHealthReport {
    /// The account id (its email — the id the quota registry keys by).
    pub account: String,
    /// Credential VALIDITY axis: how usable the stored `.credentials.json` is right now.
    pub credential: CredentialHealth,
    /// True when a refresh token is stored — the CLI (and the periodic prober) can mint a fresh
    /// access token on use, so an expired-but-refreshable account is still slingable.
    pub refresh: bool,
    /// The credential's `expiresAt` in UTC epoch SECONDS (the file stores millis; this is the
    /// edge-friendly seconds projection). `None` when the file carries no expiry or is unreadable.
    pub expires_at_secs: Option<u64>,
    /// IDENTITY/onboarding axis: the dir carries `oauthAccount` + `hasCompletedOnboarding` so a
    /// polecat slung into it skips the first-run TUI. `false` is the exact wedge cause.
    pub onboarding_complete: bool,
    /// The single FE signal: `true` when the account cannot be slung as-is and a `quota.relogin`
    /// is required — dead/unreadable credential OR missing onboarding markers.
    pub needs_relogin: bool,
}

/// Build the report for one account from the two on-disk facts the edge stamps:
///
/// - `creds_raw` — the raw `.credentials.json` body (`None` ⇒ the file is missing/unreadable).
/// - `onboarding_complete` — whether `<dir>/.claude.json` carries BOTH `oauthAccount.emailAddress`
///   and `hasCompletedOnboarding == true` (the edge reads them; the markers seeded by
///   `quota.relogin` complete).
///
/// `now_ms`/`skew_ms` come from the edge so this stays clock-free. The credential axis reuses
/// [`classify_credentials`] verbatim so cred-health and sling-time selection never disagree.
pub fn assess_cred_health(
    account: impl Into<String>,
    creds_raw: Option<&str>,
    onboarding_complete: bool,
    now_ms: u64,
    skew_ms: u64,
) -> CredHealthReport {
    let credential = classify_credentials(creds_raw, now_ms, skew_ms);
    // Parse once more for the surfaced fields (refresh-token presence + expiry). A parse failure
    // here means Unreadable above already, so the fields default to "none/false".
    let parsed = creds_raw.and_then(|raw| crate::oauth_usage::parse_credentials_json(raw).ok());
    let refresh = parsed.as_ref().is_some_and(|c| c.refresh_token.is_some());
    let expires_at_secs = parsed
        .as_ref()
        .and_then(|c| c.expires_at_ms)
        .map(|ms| ms / 1000);

    // Need a relogin when EITHER axis is unslingable: a credential that cannot authenticate, or a
    // dir missing the onboarding markers (which wedges the first-run TUI even with valid creds).
    let needs_relogin = !credential.is_slingable() || !onboarding_complete;

    CredHealthReport {
        account: account.into(),
        credential,
        refresh,
        expires_at_secs,
        onboarding_complete,
        needs_relogin,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const SKEW: u64 = 5 * 60 * 1000;

    /// `.credentials.json` body with a controllable expiry (epoch MILLIS) and optional refresh.
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

    #[test]
    fn fully_healthy_account_does_not_need_relogin() {
        let now = 1_000_000_000;
        let r = assess_cred_health(
            "a@x.com",
            Some(&creds(Some(now + 3_600_000), true)),
            true,
            now,
            SKEW,
        );
        assert_eq!(r.credential, CredentialHealth::Valid);
        assert!(r.refresh);
        assert_eq!(r.expires_at_secs, Some((now + 3_600_000) / 1000));
        assert!(r.onboarding_complete);
        assert!(!r.needs_relogin, "valid creds + onboarding ⇒ no relogin");
    }

    #[test]
    fn missing_onboarding_markers_need_relogin_even_with_valid_creds() {
        // The exact wedge cause: fresh credential, but the dir has no oauthAccount/onboarding flags.
        let now = 1_000_000_000;
        let r = assess_cred_health(
            "a@x.com",
            Some(&creds(Some(now + 3_600_000), true)),
            false,
            now,
            SKEW,
        );
        assert_eq!(r.credential, CredentialHealth::Valid);
        assert!(!r.onboarding_complete);
        assert!(r.needs_relogin, "valid creds but no onboarding ⇒ relogin");
    }

    #[test]
    fn expired_without_refresh_needs_relogin() {
        let now = 1_000_000_000;
        let r = assess_cred_health("a@x.com", Some(&creds(Some(now - 1), false)), true, now, SKEW);
        assert_eq!(r.credential, CredentialHealth::Expired);
        assert!(!r.refresh);
        assert!(r.needs_relogin);
    }

    #[test]
    fn refreshable_with_onboarding_does_not_need_relogin() {
        // Expired access token but a refresh token + onboarding markers: the CLI refreshes at
        // startup, so the account is still slingable as-is.
        let now = 1_000_000_000;
        let r = assess_cred_health("a@x.com", Some(&creds(Some(now - 1), true)), true, now, SKEW);
        assert_eq!(r.credential, CredentialHealth::Refreshable);
        assert!(r.refresh);
        assert!(!r.needs_relogin);
    }

    #[test]
    fn missing_credentials_file_needs_relogin() {
        let now = 1_000_000_000;
        let r = assess_cred_health("a@x.com", None, true, now, SKEW);
        assert_eq!(r.credential, CredentialHealth::Unreadable);
        assert!(!r.refresh);
        assert_eq!(r.expires_at_secs, None);
        assert!(r.needs_relogin);
    }
}
