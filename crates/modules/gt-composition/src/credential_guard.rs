//! Sling-time credential guard (gtcore-bf4acd): the EDGE half of
//! [`gt_quota::credential_select`].
//!
//! Before a polecat is slung, orchd stamps the keychain's ACTIVE account's `CLAUDE_CONFIG_DIR` so
//! the polecat's `claude` authenticates as that account. The incident this guards against: the
//! active account was quota-`Healthy` but its `.credentials.json` access token had expired ~12h
//! earlier with no refresh — so every polecat slung onto it was born in
//! `401 Invalid authentication credentials` and produced nothing for over an hour.
//!
//! This module reads the credential files off disk (the I/O the pure selector must not do) and
//! decides, per sling:
//!
//! - **active creds valid / refreshable** → use the active account (the common path; refreshable
//!   means `claude` mints a fresh token at startup from the stored refresh token).
//! - **active creds file MISSING** → use the active account anyway (legacy liveness: a freshly
//!   onboarded/seeded dir, or host-managed auth — the seeding step that follows populates it, and
//!   a missing file is NOT the expired-token failure mode the incident was about).
//! - **active creds EXPIRED (no refresh) or present-but-garbage** → the account is credential-dead:
//!   rotate to another keychain account whose `.credentials.json` is present and valid/refreshable,
//!   flip the live pointer so this and future slings land there, and report the dead account so the
//!   operator is alerted.
//! - **no account can authenticate** → [`CredOutcome::NoValidAccount`]: the caller blocks the sling
//!   and alerts rather than birthing a polecat into 401.
//!
//! Validity is checked at SELECTION time and decoupled from quota: an account can be quota-`Healthy`
//! yet credential-dead, and this guard rejects the latter — the gap `quota_list`'s `Healthy` hid.

use std::path::Path;
use std::sync::Arc;

use gt_quota::{
    classify_credentials, select_slingable, Candidate, CredentialHealth, DeadAccount, Keychain,
    Selection, REFRESH_SKEW_MS,
};

/// A resolved, slingable account and the `CLAUDE_CONFIG_DIR` to stamp.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ResolvedCredentials {
    pub account: String,
    pub config_dir: String,
}

/// What the guard decided for one sling.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CredOutcome {
    /// Stamp `resolved`'s `CLAUDE_CONFIG_DIR`. When `rotated_from` is set the active pointer was
    /// moved off a credential-dead account onto `resolved` (already persisted via `set_active`);
    /// `dead` then names the skipped account so the caller can raise a credential alert.
    Resolved {
        resolved: ResolvedCredentials,
        dead: Vec<DeadAccount>,
        rotated_from: Option<String>,
    },
    /// No keychain account can authenticate. The caller must block the sling and alert. `dead`
    /// names the credential-dead account(s) that drove the decision.
    NoValidAccount { dead: Vec<DeadAccount> },
    /// No keychain rotation is configured for this sling (no active pointer, or the active account
    /// has no stored credential record): fall back to the host default `~/.claude`, exactly as
    /// before the guard existed. Never alerts — this is the single-account / unconfigured path.
    HostDefault,
}

/// Read a config dir's `.credentials.json`, or `None` if it is missing/unreadable. Missing is
/// distinct from present-but-garbage at the call site: see [`resolve_for_sling_with`].
fn read_credentials(config_dir: &str) -> Option<String> {
    std::fs::read_to_string(Path::new(config_dir).join(".credentials.json")).ok()
}

/// Resolve the account a sling should authenticate as, validating the active account's credentials.
/// Reads files via [`read_credentials`]; see [`resolve_for_sling_with`] for the pure-ish core that
/// tests drive with an injected reader.
pub fn resolve_for_sling(keychain: &Arc<dyn Keychain>, now_ms: u64) -> CredOutcome {
    resolve_for_sling_with(keychain, now_ms, read_credentials)
}

/// Core of [`resolve_for_sling`] with the file reader injected so tests exercise every health path
/// without touching disk. `read(config_dir) -> Some(raw)` for a present file, `None` for
/// missing/unreadable.
pub fn resolve_for_sling_with(
    keychain: &Arc<dyn Keychain>,
    now_ms: u64,
    read: impl Fn(&str) -> Option<String>,
) -> CredOutcome {
    // No live pointer → nothing to validate; legacy host-default path.
    let active = match keychain.active() {
        Ok(Some(a)) => a,
        Ok(None) => return CredOutcome::HostDefault,
        Err(e) => {
            eprintln!("[cred-guard] keychain active() failed: {e} — host default ~/.claude");
            return CredOutcome::HostDefault;
        }
    };
    // Active set but no stored credential record → host default (unchanged from pre-guard).
    let active_dir = match keychain.get(&active) {
        Ok(Some(cred)) => cred.secret,
        Ok(None) => {
            eprintln!(
                "[cred-guard] active account {active} has no stored credential — host default ~/.claude"
            );
            return CredOutcome::HostDefault;
        }
        Err(e) => {
            eprintln!("[cred-guard] keychain get({active}) failed: {e} — host default ~/.claude");
            return CredOutcome::HostDefault;
        }
    };

    // Missing credential FILE is permissive: a fresh/seeded dir or host-managed auth. Only an
    // EXPIRED (or present-but-garbage) file is the incident's failure mode.
    let Some(active_raw) = read(&active_dir) else {
        return CredOutcome::Resolved {
            resolved: ResolvedCredentials {
                account: active,
                config_dir: active_dir,
            },
            dead: Vec::new(),
            rotated_from: None,
        };
    };

    let health = classify_credentials(Some(active_raw.as_str()), now_ms, REFRESH_SKEW_MS);
    if health.is_slingable() {
        return CredOutcome::Resolved {
            resolved: ResolvedCredentials {
                account: active,
                config_dir: active_dir,
            },
            dead: Vec::new(),
            rotated_from: None,
        };
    }

    // Active account is credential-dead. Try to rotate to another account whose `.credentials.json`
    // is present and slingable. Accounts with a MISSING file are skipped here (we only rotate ONTO
    // a credential we can positively validate — the incident recovery copied a *valid* file in).
    let active_dead = DeadAccount {
        account: active.clone(),
        health,
    };
    let accounts = keychain.accounts().unwrap_or_default();
    let mut dirs: Vec<(String, String)> = Vec::new(); // (account, config_dir)
    let mut candidates: Vec<Candidate> = Vec::new();
    for acc in &accounts {
        if *acc == active {
            continue;
        }
        let Ok(Some(cred)) = keychain.get(acc) else {
            continue;
        };
        let raw = read(&cred.secret);
        dirs.push((acc.clone(), cred.secret));
        candidates.push(Candidate {
            account: acc.clone(),
            creds: raw,
        });
    }

    match select_slingable(None, &candidates, now_ms, REFRESH_SKEW_MS).0 {
        Selection::Use { account, .. } => {
            let config_dir = dirs
                .iter()
                .find(|(a, _)| *a == account)
                .map(|(_, d)| d.clone())
                .unwrap_or_default();
            // Persist the rotation so the next sling (and the supervisor's re-sling, which reads
            // active()) also land on the healthy account — couple the pointer to the selection.
            if let Err(e) = keychain.set_active(&account) {
                eprintln!(
                    "[cred-guard] set_active({account}) failed after credential rotation: {e} — \
                     stamping its config dir for this sling anyway"
                );
            }
            eprintln!(
                "[cred-guard] active account {active} credential-dead ({}) — rotated to {account}",
                active_dead_reason(health)
            );
            CredOutcome::Resolved {
                resolved: ResolvedCredentials { account, config_dir },
                dead: vec![active_dead],
                rotated_from: Some(active),
            }
        }
        Selection::NoValidAccount => CredOutcome::NoValidAccount {
            dead: vec![active_dead],
        },
    }
}

fn active_dead_reason(health: CredentialHealth) -> &'static str {
    match health {
        CredentialHealth::Expired => "token expirado sin refresh",
        CredentialHealth::Unreadable => "credenciales ilegibles",
        CredentialHealth::Valid | CredentialHealth::Refreshable => "válida",
    }
}

/// Copy a source account's `.credentials.json` into another `CLAUDE_CONFIG_DIR` so the `claude` CLI
/// there picks up the source's tokens without restarting (the hot-swap rotation uses this to push
/// fresh credentials into in-flight polecats). Best-effort: returns the I/O error for the caller to
/// log. Tightens the destination to `0600` on unix, matching how the prober persists refreshes.
pub fn seed_credentials(src_config_dir: &str, dst_config_dir: &str) -> std::io::Result<()> {
    let src = Path::new(src_config_dir).join(".credentials.json");
    let dst = Path::new(dst_config_dir).join(".credentials.json");
    std::fs::copy(&src, &dst)?;
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        let _ = std::fs::set_permissions(&dst, std::fs::Permissions::from_mode(0o600));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use gt_quota::InMemoryKeychain;

    const NOW: u64 = 1_000_000_000;

    fn creds(expires_at_ms: Option<u64>, refresh: bool) -> String {
        let rt = if refresh { r#","refreshToken":"rt-1""# } else { "" };
        let exp = expires_at_ms
            .map(|e| format!(r#","expiresAt":{e}"#))
            .unwrap_or_default();
        format!(r#"{{"claudeAiOauth":{{"accessToken":"at-1"{rt}{exp}}}}}"#)
    }

    fn keychain(records: &[(&str, &str)]) -> Arc<dyn Keychain> {
        Arc::new(InMemoryKeychain::seeded(
            records.iter().map(|(a, s)| (a.to_string(), s.to_string())),
        ))
    }

    #[test]
    fn no_active_pointer_is_host_default() {
        let kc = keychain(&[("a", "/dir/a")]);
        let out = resolve_for_sling_with(&kc, NOW, |_| None);
        assert_eq!(out, CredOutcome::HostDefault);
    }

    #[test]
    fn missing_credential_file_uses_active_as_is() {
        // Legacy liveness: a config dir without a .credentials.json (fresh/seeded) is still used —
        // this is exactly the existing dispatch test's shape and must not regress.
        let kc = keychain(&[("a", "/dir/a")]);
        kc.set_active("a").unwrap();
        let out = resolve_for_sling_with(&kc, NOW, |_| None);
        assert_eq!(
            out,
            CredOutcome::Resolved {
                resolved: ResolvedCredentials {
                    account: "a".into(),
                    config_dir: "/dir/a".into()
                },
                dead: vec![],
                rotated_from: None
            }
        );
    }

    #[test]
    fn valid_active_credentials_use_the_active_account() {
        let kc = keychain(&[("a", "/dir/a")]);
        kc.set_active("a").unwrap();
        let valid = creds(Some(NOW + 3_600_000), true);
        let out = resolve_for_sling_with(&kc, NOW, |_| Some(valid.clone()));
        match out {
            CredOutcome::Resolved { resolved, rotated_from, .. } => {
                assert_eq!(resolved.account, "a");
                assert!(rotated_from.is_none());
            }
            other => panic!("expected Resolved, got {other:?}"),
        }
    }

    #[test]
    fn expired_active_rotates_to_a_healthy_account_and_flips_the_pointer() {
        // The incident: active `dead` is selected but its token expired with no refresh; `fresh`
        // has a valid file. The guard rotates, flips the live pointer, and reports `dead`.
        let kc = keychain(&[("dead", "/dir/dead"), ("fresh", "/dir/fresh")]);
        kc.set_active("dead").unwrap();
        let dead_raw = creds(Some(NOW - 1), false);
        let fresh_raw = creds(Some(NOW + 3_600_000), true);
        let out = resolve_for_sling_with(&kc, NOW, move |dir| match dir {
            "/dir/dead" => Some(dead_raw.clone()),
            "/dir/fresh" => Some(fresh_raw.clone()),
            _ => None,
        });
        match out {
            CredOutcome::Resolved { resolved, dead, rotated_from } => {
                assert_eq!(resolved.account, "fresh");
                assert_eq!(resolved.config_dir, "/dir/fresh");
                assert_eq!(rotated_from.as_deref(), Some("dead"));
                assert_eq!(dead.len(), 1);
                assert_eq!(dead[0].account, "dead");
                assert_eq!(dead[0].health, CredentialHealth::Expired);
            }
            other => panic!("expected Resolved, got {other:?}"),
        }
        // The live pointer was persisted so the NEXT sling also lands on the healthy account.
        assert_eq!(kc.active().unwrap().as_deref(), Some("fresh"));
    }

    #[test]
    fn all_dead_yields_no_valid_account() {
        let kc = keychain(&[("dead", "/dir/dead"), ("also", "/dir/also")]);
        kc.set_active("dead").unwrap();
        let dead_raw = creds(Some(NOW - 1), false);
        let also_raw = creds(Some(NOW - 1), false);
        let out = resolve_for_sling_with(&kc, NOW, move |dir| match dir {
            "/dir/dead" => Some(dead_raw.clone()),
            "/dir/also" => Some(also_raw.clone()),
            _ => None,
        });
        match out {
            CredOutcome::NoValidAccount { dead } => {
                assert_eq!(dead.len(), 1);
                assert_eq!(dead[0].account, "dead");
            }
            other => panic!("expected NoValidAccount, got {other:?}"),
        }
        // Pointer stays put — nothing valid to move to.
        assert_eq!(kc.active().unwrap().as_deref(), Some("dead"));
    }
}
