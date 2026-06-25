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
    classify_credentials, select_slingable, AccountQuotaStatus, Candidate, CredentialHealth,
    DeadAccount, Keychain, Selection, REFRESH_SKEW_MS,
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

/// Resolve the account a sling should authenticate as, validating the active account's credentials
/// AND its quota status. `status_of(account)` returns the account's quota status (`None` ⇒ unknown,
/// treated as slingable so an un-probed account is not blocked). `headroom_of(account)` returns
/// available quota headroom in \[0, 100\]: the sling lands on the slingable account with the MOST
/// headroom so concurrent slings distribute naturally (gtcore-98e14f gap #2). Pass `|_| 100.0`
/// when utilization data is unavailable — all slingable accounts are treated as equally available.
/// Reads files via [`read_credentials`]; see [`resolve_for_sling_with`] for the pure-ish core.
pub fn resolve_for_sling(
    keychain: &Arc<dyn Keychain>,
    now_ms: u64,
    status_of: impl Fn(&str) -> Option<AccountQuotaStatus>,
    headroom_of: impl Fn(&str) -> f64,
) -> CredOutcome {
    resolve_for_sling_with(keychain, now_ms, read_credentials, status_of, headroom_of)
}

/// Whether an account is usable for a NEW sling on BOTH axes: its quota status is slingable (Healthy,
/// or unknown/un-probed) AND its credentials authenticate. `status_of` `None` ⇒ unknown ⇒ permissive
/// on the quota axis (the first probe corrects it). This is the gtcore-2836bb gate: an account that
/// is quota-`Limited`/`Blocked` is rejected even when its credentials are perfectly valid, because a
/// polecat slung onto it is born straight into the rate-limit dialog.
fn quota_slingable(account: &str, status_of: &impl Fn(&str) -> Option<AccountQuotaStatus>) -> bool {
    status_of(account).map(|s| s.is_slingable()).unwrap_or(true)
}

/// Core of [`resolve_for_sling`] with the file reader + quota-status + headroom lookups injected
/// so tests exercise every path without touching disk or a live quota actor.
/// `read(config_dir) -> Some(raw)` for a present file, `None` for missing/unreadable.
/// `status_of(account)` reports the quota status. `headroom_of(account)` returns available quota
/// headroom in \[0, 100\] (higher = more room); used to distribute concurrent slings.
pub fn resolve_for_sling_with(
    keychain: &Arc<dyn Keychain>,
    now_ms: u64,
    read: impl Fn(&str) -> Option<String>,
    status_of: impl Fn(&str) -> Option<AccountQuotaStatus>,
    headroom_of: impl Fn(&str) -> f64,
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

    let active_quota_ok = quota_slingable(&active, &status_of);

    // Missing credential FILE is permissive on the credential axis (a fresh/seeded dir or
    // host-managed auth). But the QUOTA axis still applies: an active account that is Limited/Blocked
    // must NOT receive a new sling even with a missing/seeded creds file — that is exactly the
    // incident where polecats were born into the rate-limit dialog (gtcore-2836bb). Use the active
    // account as-is only when its quota is also slingable; otherwise fall through to rotation.
    let active_raw = read(&active_dir);
    let active_cred_ok = match &active_raw {
        // Missing file is credential-permissive.
        None => true,
        Some(raw) => classify_credentials(Some(raw.as_str()), now_ms, REFRESH_SKEW_MS).is_slingable(),
    };

    if active_cred_ok && active_quota_ok {
        // Distribute concurrent slings by headroom (gtcore-98e14f gap #2): if another slingable
        // account has STRICTLY more headroom, use it for this sling WITHOUT touching the active
        // pointer (quota_rotation owns pointer moves; here we only decide per-sling assignment).
        let active_headroom = headroom_of(&active);
        let best_alt = keychain
            .accounts()
            .unwrap_or_default()
            .into_iter()
            .filter(|acc| *acc != active && quota_slingable(acc, &status_of))
            .filter_map(|acc| {
                let h = headroom_of(&acc);
                if h > active_headroom {
                    keychain.get(&acc).ok().flatten().map(|cred| (acc, cred.secret, h))
                } else {
                    None
                }
            })
            .max_by(|(_, _, ha), (_, _, hb)| ha.partial_cmp(hb).unwrap_or(std::cmp::Ordering::Equal));
        if let Some((best_id, best_dir, _)) = best_alt {
            return CredOutcome::Resolved {
                resolved: ResolvedCredentials { account: best_id, config_dir: best_dir },
                dead: Vec::new(),
                rotated_from: None,
            };
        }
        return CredOutcome::Resolved {
            resolved: ResolvedCredentials {
                account: active,
                config_dir: active_dir,
            },
            dead: Vec::new(),
            rotated_from: None,
        };
    }

    // Active account cannot receive a sling — credential-dead, quota-limited/blocked, or both. Try
    // to rotate to another account that is slingable on BOTH axes. Accounts with a MISSING creds
    // file are skipped here (we only rotate ONTO a credential we can positively validate). The
    // skipped active is reported as `dead` so the operator is alerted; its `health` carries the
    // credential reason (Valid when the block was purely quota — the rotation message names that).
    let active_health = match &active_raw {
        None => CredentialHealth::Valid,
        Some(raw) => classify_credentials(Some(raw.as_str()), now_ms, REFRESH_SKEW_MS),
    };
    let active_dead = DeadAccount {
        account: active.clone(),
        health: active_health,
    };
    let accounts = keychain.accounts().unwrap_or_default();
    let mut dirs: Vec<(String, String)> = Vec::new(); // (account, config_dir)
    let mut candidates: Vec<Candidate> = Vec::new();
    for acc in &accounts {
        if *acc == active {
            continue;
        }
        // Quota gate first: never rotate ONTO a Limited/Blocked account (gtcore-2836bb).
        if !quota_slingable(acc, &status_of) {
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
                    "[cred-guard] set_active({account}) failed after sling rotation: {e} — \
                     stamping its config dir for this sling anyway"
                );
            }
            let reason = if !active_quota_ok && active_cred_ok {
                "quota Limited/Blocked"
            } else {
                active_dead_reason(active_health)
            };
            eprintln!(
                "[cred-guard] active account {active} not slingable ({reason}) — rotated to {account}"
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

    /// Quota-status closure that reports every account `Healthy` — the common case where only the
    /// credential axis is under test.
    fn all_healthy(_: &str) -> Option<AccountQuotaStatus> {
        Some(AccountQuotaStatus::Healthy)
    }

    #[test]
    fn no_active_pointer_is_host_default() {
        let kc = keychain(&[("a", "/dir/a")]);
        let out = resolve_for_sling_with(&kc, NOW, |_| None, all_healthy, |_| 100.0);
        assert_eq!(out, CredOutcome::HostDefault);
    }

    #[test]
    fn missing_credential_file_uses_active_as_is() {
        // Legacy liveness: a config dir without a .credentials.json (fresh/seeded) is still used —
        // this is exactly the existing dispatch test's shape and must not regress.
        let kc = keychain(&[("a", "/dir/a")]);
        kc.set_active("a").unwrap();
        let out = resolve_for_sling_with(&kc, NOW, |_| None, all_healthy, |_| 100.0);
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
        let out = resolve_for_sling_with(&kc, NOW, |_| Some(valid.clone()), all_healthy, |_| 100.0);
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
        }, all_healthy, |_| 100.0);
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
        }, all_healthy, |_| 100.0);
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

    #[test]
    fn quota_limited_active_rotates_to_a_healthy_account_even_with_valid_creds() {
        // gtcore-2836bb AC#1: the active account's credentials are perfectly VALID, but its quota
        // is Limited — a polecat slung onto it is born into the rate-limit dialog. The guard must
        // rotate to the Healthy account and flip the pointer, exactly like the credential-dead path.
        let kc = keychain(&[("limited", "/dir/limited"), ("fresh", "/dir/fresh")]);
        kc.set_active("limited").unwrap();
        let valid = creds(Some(NOW + 3_600_000), true);
        let v2 = valid.clone();
        let out = resolve_for_sling_with(
            &kc,
            NOW,
            move |_| Some(v2.clone()),
            |acc| {
                Some(match acc {
                    "limited" => AccountQuotaStatus::Limited,
                    _ => AccountQuotaStatus::Healthy,
                })
            },
            |_| 100.0,
        );
        match out {
            CredOutcome::Resolved { resolved, rotated_from, dead } => {
                assert_eq!(resolved.account, "fresh", "rotated off the Limited account");
                assert_eq!(rotated_from.as_deref(), Some("limited"));
                assert_eq!(dead.len(), 1);
                assert_eq!(dead[0].account, "limited");
            }
            other => panic!("expected Resolved, got {other:?}"),
        }
        assert_eq!(kc.active().unwrap().as_deref(), Some("fresh"), "pointer flipped");
        let _ = valid;
    }

    #[test]
    fn never_rotates_onto_a_blocked_account() {
        // Active is credential-dead; the only alternative is quota-Blocked → no valid target, so
        // the sling is blocked rather than landing on the Blocked account (gtcore-2836bb).
        let kc = keychain(&[("dead", "/dir/dead"), ("blocked", "/dir/blocked")]);
        kc.set_active("dead").unwrap();
        let dead_raw = creds(Some(NOW - 1), false);
        let blocked_raw = creds(Some(NOW + 3_600_000), true);
        let out = resolve_for_sling_with(
            &kc,
            NOW,
            move |dir| match dir {
                "/dir/dead" => Some(dead_raw.clone()),
                "/dir/blocked" => Some(blocked_raw.clone()),
                _ => None,
            },
            |acc| {
                Some(match acc {
                    "blocked" => AccountQuotaStatus::Blocked,
                    _ => AccountQuotaStatus::Healthy,
                })
            },
            |_| 100.0,
        );
        match out {
            CredOutcome::NoValidAccount { dead } => assert_eq!(dead[0].account, "dead"),
            other => panic!("expected NoValidAccount (Blocked is not a target), got {other:?}"),
        }
    }

    #[test]
    fn healthy_active_with_valid_creds_is_used_directly_under_the_quota_gate() {
        // No-regression: a Healthy + credential-valid active account is used as-is (no rotation),
        // even with the quota gate active.
        let kc = keychain(&[("a", "/dir/a"), ("b", "/dir/b")]);
        kc.set_active("a").unwrap();
        let valid = creds(Some(NOW + 3_600_000), true);
        let out = resolve_for_sling_with(&kc, NOW, move |_| Some(valid.clone()), all_healthy, |_| 100.0);
        match out {
            CredOutcome::Resolved { resolved, rotated_from, .. } => {
                assert_eq!(resolved.account, "a");
                assert!(rotated_from.is_none());
            }
            other => panic!("expected Resolved, got {other:?}"),
        }
    }

    #[test]
    fn unknown_quota_status_is_permissive() {
        // An un-probed account (status_of → None) must not be blocked on the quota axis — the first
        // probe corrects it. Active with valid creds + unknown status is used directly.
        let kc = keychain(&[("a", "/dir/a")]);
        kc.set_active("a").unwrap();
        let valid = creds(Some(NOW + 3_600_000), true);
        let out = resolve_for_sling_with(&kc, NOW, move |_| Some(valid.clone()), |_| None, |_| 100.0);
        match out {
            CredOutcome::Resolved { resolved, rotated_from, .. } => {
                assert_eq!(resolved.account, "a");
                assert!(rotated_from.is_none());
            }
            other => panic!("expected Resolved, got {other:?}"),
        }
    }

    #[test]
    fn concurrent_slings_pick_higher_headroom_account(  ) {
        // gtcore-98e14f gap #2: active `a` is slingable but `b` has strictly more headroom
        // → sling goes to `b` WITHOUT flipping the active pointer.
        let kc = keychain(&[("a", "/dir/a"), ("b", "/dir/b")]);
        kc.set_active("a").unwrap();
        let valid = creds(Some(NOW + 3_600_000), true);
        let v2 = valid.clone();
        let out = resolve_for_sling_with(
            &kc,
            NOW,
            move |_| Some(v2.clone()),
            all_healthy,
            |acc| match acc { "a" => 20.0, _ => 80.0 }, // b has more headroom
        );
        match out {
            CredOutcome::Resolved { resolved, rotated_from, dead } => {
                assert_eq!(resolved.account, "b", "sling should land on higher-headroom account");
                assert!(rotated_from.is_none(), "distribution must NOT flip the active pointer");
                assert!(dead.is_empty());
            }
            other => panic!("expected Resolved, got {other:?}"),
        }
        // Active pointer NOT moved — distribution, not rotation.
        assert_eq!(kc.active().unwrap().as_deref(), Some("a"));
        let _ = valid;
    }

    #[test]
    fn equal_headroom_uses_active() {
        // When all slingable accounts have equal headroom, use the active account (fast path).
        let kc = keychain(&[("a", "/dir/a"), ("b", "/dir/b")]);
        kc.set_active("a").unwrap();
        let valid = creds(Some(NOW + 3_600_000), true);
        let v2 = valid.clone();
        let out = resolve_for_sling_with(&kc, NOW, move |_| Some(v2.clone()), all_healthy, |_| 50.0);
        match out {
            CredOutcome::Resolved { resolved, rotated_from, .. } => {
                assert_eq!(resolved.account, "a", "equal headroom → use active");
                assert!(rotated_from.is_none());
            }
            other => panic!("expected Resolved, got {other:?}"),
        }
        let _ = valid;
    }

    #[test]
    fn blocked_account_never_receives_sling_even_with_more_headroom() {
        // A Blocked account is never a distribution target, even if headroom_of says it has room.
        let kc = keychain(&[("a", "/dir/a"), ("blocked", "/dir/blocked")]);
        kc.set_active("a").unwrap();
        let valid = creds(Some(NOW + 3_600_000), true);
        let v2 = valid.clone();
        let out = resolve_for_sling_with(
            &kc,
            NOW,
            move |_| Some(v2.clone()),
            |acc| Some(match acc { "blocked" => AccountQuotaStatus::Blocked, _ => AccountQuotaStatus::Healthy }),
            |acc| match acc { "blocked" => 99.0, _ => 10.0 }, // blocked has "more headroom" but is Blocked
        );
        match out {
            CredOutcome::Resolved { resolved, .. } => {
                assert_eq!(resolved.account, "a", "Blocked account must never receive a sling");
            }
            other => panic!("expected Resolved on a, got {other:?}"),
        }
        let _ = valid;
    }
}
