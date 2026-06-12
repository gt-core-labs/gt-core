//! Server-side usage probe + OAuth credential parsing (hq-28b063 / hq-358fd3 / hq-002cca).
//!
//! The local counters (`SampleTokens`) cannot see usage the account incurs OUTSIDE this
//! runtime (the same Claude account driven from another device), and the locally-modeled
//! rolling-5h window drifts from the provider's real one. The authoritative source is the
//! OAuth usage endpoint the Claude Code `/usage` screen reads:
//!
//! ```text
//! GET https://api.anthropic.com/api/oauth/usage
//! Authorization: Bearer <accessToken>
//! anthropic-beta: oauth-2025-04-20
//! →
//! {"five_hour":{"utilization":77.0,"resets_at":"2026-06-12T07:40:00.484148+00:00"},
//!  "seven_day":{"utilization":56.0,"resets_at":"2026-06-16T00:00:00.484168+00:00"}, ...}
//! ```
//!
//! This module is the PURE half: parse the `.credentials.json` credential file (the keychain
//! secret points at the account's `CLAUDE_CONFIG_DIR`, which contains it), parse the usage
//! body, and map a snapshot into the existing [`ProbeWindow`] command. No clock reads, no
//! I/O, no HTTP — the composition edge (`gt-composition::usage_probe`) owns the network and
//! filesystem so every function here is deterministic under replay, exactly like
//! [`crate::probe::parse_anthropic_ratelimit`].

use serde::{Deserialize, Serialize};
use time::OffsetDateTime;

use crate::commands::ProbeWindow;

/// Default refresh skew: treat a token as expired this long BEFORE its real expiry so a
/// probe never races the cutoff mid-request (5 minutes).
pub const REFRESH_SKEW_MS: u64 = 5 * 60 * 1000;

/// The OAuth credential triple stored in a Claude `CLAUDE_CONFIG_DIR/.credentials.json`
/// under the `claudeAiOauth` key. `expires_at_ms` is the file's `expiresAt` (epoch millis).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct OauthCredentials {
    pub access_token: String,
    /// Absent on credential files that never carried a refresh token; refresh is then
    /// impossible and an expired access token is a hard probe failure.
    pub refresh_token: Option<String>,
    /// Epoch milliseconds. `None` ⇒ unknown expiry: callers should attempt the probe and
    /// fall back to a refresh on a 401.
    pub expires_at_ms: Option<u64>,
}

impl OauthCredentials {
    /// True when the access token is expired (or expires within `skew_ms`) at `now_ms`.
    /// Unknown expiry is NOT expired — the edge finds out via a 401 and refreshes then.
    pub fn needs_refresh(&self, now_ms: u64, skew_ms: u64) -> bool {
        match self.expires_at_ms {
            Some(exp) => now_ms.saturating_add(skew_ms) >= exp,
            None => false,
        }
    }
}

/// Parse a `.credentials.json` body. Accepts the canonical shape
/// `{"claudeAiOauth":{"accessToken":..,"refreshToken":..,"expiresAt":..}}` and, as a
/// fallback, the same keys at the root (some older files are unwrapped).
pub fn parse_credentials_json(raw: &str) -> Result<OauthCredentials, String> {
    let v: serde_json::Value =
        serde_json::from_str(raw).map_err(|e| format!("credentials json: {e}"))?;
    let obj = v.get("claudeAiOauth").unwrap_or(&v);
    let access_token = obj
        .get("accessToken")
        .and_then(|t| t.as_str())
        .filter(|t| !t.is_empty())
        .ok_or("credentials json: missing accessToken")?
        .to_string();
    let refresh_token = obj
        .get("refreshToken")
        .and_then(|t| t.as_str())
        .filter(|t| !t.is_empty())
        .map(str::to_string);
    let expires_at_ms = obj.get("expiresAt").and_then(|t| t.as_u64());
    Ok(OauthCredentials {
        access_token,
        refresh_token,
        expires_at_ms,
    })
}

/// Re-render a `.credentials.json` body with `fresh` tokens, PRESERVING every other field
/// (scopes, subscriptionType, sibling root keys). The original wrapping is kept: a file
/// with a `claudeAiOauth` object is patched in place; an unwrapped file is patched at the
/// root. A `None` refresh token leaves the stored one untouched (the provider may omit it
/// from a refresh response — the old one stays valid).
pub fn render_credentials_json(original: &str, fresh: &OauthCredentials) -> Result<String, String> {
    let mut v: serde_json::Value =
        serde_json::from_str(original).map_err(|e| format!("credentials json: {e}"))?;
    let target = if v.get("claudeAiOauth").is_some() {
        v.get_mut("claudeAiOauth").expect("checked above")
    } else {
        &mut v
    };
    let obj = target
        .as_object_mut()
        .ok_or("credentials json: claudeAiOauth is not an object")?;
    obj.insert(
        "accessToken".into(),
        serde_json::Value::String(fresh.access_token.clone()),
    );
    if let Some(rt) = &fresh.refresh_token {
        obj.insert("refreshToken".into(), serde_json::Value::String(rt.clone()));
    }
    if let Some(exp) = fresh.expires_at_ms {
        obj.insert("expiresAt".into(), serde_json::Value::from(exp));
    }
    serde_json::to_string(&v).map_err(|e| format!("credentials json render: {e}"))
}

/// Parse the OAuth token-refresh response body
/// (`{"access_token":..,"refresh_token":..,"expires_in":<secs>}`) into a fresh credential
/// triple. `now_ms` is stamped at the edge so the parse stays clock-free.
pub fn parse_refresh_response(raw: &str, now_ms: u64) -> Result<OauthCredentials, String> {
    let v: serde_json::Value =
        serde_json::from_str(raw).map_err(|e| format!("refresh response: {e}"))?;
    let access_token = v
        .get("access_token")
        .and_then(|t| t.as_str())
        .filter(|t| !t.is_empty())
        .ok_or("refresh response: missing access_token")?
        .to_string();
    let refresh_token = v
        .get("refresh_token")
        .and_then(|t| t.as_str())
        .filter(|t| !t.is_empty())
        .map(str::to_string);
    let expires_at_ms = v
        .get("expires_in")
        .and_then(|t| t.as_u64())
        .map(|secs| now_ms.saturating_add(secs.saturating_mul(1000)));
    Ok(OauthCredentials {
        access_token,
        refresh_token,
        expires_at_ms,
    })
}

/// One window of the usage response: provider-reported utilization percent (0–100, may
/// exceed 100 on overage) + the exact reset instant.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct UsageWindow {
    pub utilization: f64,
    pub resets_at_secs: u64,
}

/// Parsed `/api/oauth/usage` body. `five_hour` is the rolling window rotation gates on;
/// `seven_day` is the weekly budget (absent on plans without one).
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct UsageSnapshot {
    pub five_hour: Option<UsageWindow>,
    pub seven_day: Option<UsageWindow>,
}

/// Parse the usage endpoint body. Unknown/extra keys are ignored; a window missing either
/// `utilization` or a parseable `resets_at` becomes `None` rather than an error so a
/// partial response still yields whatever the provider did expose.
pub fn parse_usage_response(raw: &str) -> Result<UsageSnapshot, String> {
    let v: serde_json::Value = serde_json::from_str(raw).map_err(|e| format!("usage json: {e}"))?;
    Ok(UsageSnapshot {
        five_hour: parse_window(v.get("five_hour")),
        seven_day: parse_window(v.get("seven_day")),
    })
}

fn parse_window(v: Option<&serde_json::Value>) -> Option<UsageWindow> {
    let v = v?;
    let utilization = v.get("utilization")?.as_f64()?;
    let resets_at_secs = parse_rfc3339_secs(v.get("resets_at")?.as_str()?)?;
    Some(UsageWindow {
        utilization,
        resets_at_secs,
    })
}

/// RFC 3339 (with fractional seconds, as the endpoint emits) → UTC epoch seconds.
fn parse_rfc3339_secs(raw: &str) -> Option<u64> {
    let ts = OffsetDateTime::parse(raw.trim(), &time::format_description::well_known::Rfc3339)
        .ok()?;
    u64::try_from(ts.unix_timestamp()).ok()
}

/// Map a usage snapshot into the existing [`ProbeWindow`] command. The endpoint reports
/// percent utilization, while the registry windows count cost units against `limit` — so
/// `remaining = limit × (100 − utilization)/100`, clamped to `[0, limit]` (utilization can
/// exceed 100 on overage; that clamps to 0 remaining, which flips the account out of
/// rotation). Returns `None` without a `five_hour` window — there is nothing for the
/// rolling-5h reconciliation to consume.
pub fn probe_from_usage(
    snapshot: &UsageSnapshot,
    account: impl Into<String>,
    limit: u64,
    weekly_limit: u64,
    now_secs: u64,
) -> Option<ProbeWindow> {
    let five = snapshot.five_hour.as_ref()?;
    let (weekly_remaining, weekly_resets_at_secs) = match &snapshot.seven_day {
        Some(w) => (
            Some(remaining_units(weekly_limit, w.utilization)),
            Some(w.resets_at_secs),
        ),
        None => (None, None),
    };
    Some(ProbeWindow {
        account: account.into(),
        remaining: remaining_units(limit, five.utilization),
        resets_at_secs: five.resets_at_secs,
        weekly_remaining,
        weekly_resets_at_secs,
        now_secs,
    })
}

fn remaining_units(limit: u64, utilization: f64) -> u64 {
    let frac = ((100.0 - utilization) / 100.0).clamp(0.0, 1.0);
    (limit as f64 * frac).round() as u64
}

#[cfg(test)]
mod tests {
    use super::*;

    const CREDS: &str = r#"{"claudeAiOauth":{"accessToken":"at-1","refreshToken":"rt-1","expiresAt":1781300000000,"scopes":["user:inference"],"subscriptionType":"max"}}"#;

    #[test]
    fn parse_credentials_canonical_shape() {
        let c = parse_credentials_json(CREDS).unwrap();
        assert_eq!(c.access_token, "at-1");
        assert_eq!(c.refresh_token.as_deref(), Some("rt-1"));
        assert_eq!(c.expires_at_ms, Some(1_781_300_000_000));
    }

    #[test]
    fn parse_credentials_unwrapped_fallback() {
        let c = parse_credentials_json(r#"{"accessToken":"at-2"}"#).unwrap();
        assert_eq!(c.access_token, "at-2");
        assert_eq!(c.refresh_token, None);
        assert_eq!(c.expires_at_ms, None);
    }

    #[test]
    fn parse_credentials_rejects_missing_or_empty_token() {
        assert!(parse_credentials_json(r#"{"claudeAiOauth":{}}"#).is_err());
        assert!(parse_credentials_json(r#"{"claudeAiOauth":{"accessToken":""}}"#).is_err());
        assert!(parse_credentials_json("not-json").is_err());
    }

    #[test]
    fn needs_refresh_honors_skew_and_unknown_expiry() {
        let c = parse_credentials_json(CREDS).unwrap();
        // Well before expiry-minus-skew.
        assert!(!c.needs_refresh(1_781_000_000_000, REFRESH_SKEW_MS));
        // Inside the skew margin.
        assert!(c.needs_refresh(1_781_299_900_000, REFRESH_SKEW_MS));
        // Past expiry.
        assert!(c.needs_refresh(1_781_300_000_001, 0));
        // Unknown expiry never claims refresh (the 401 path handles it).
        let unknown = OauthCredentials {
            access_token: "at".into(),
            refresh_token: None,
            expires_at_ms: None,
        };
        assert!(!unknown.needs_refresh(u64::MAX, REFRESH_SKEW_MS));
    }

    #[test]
    fn render_patches_tokens_preserving_siblings() {
        let fresh = OauthCredentials {
            access_token: "at-new".into(),
            refresh_token: Some("rt-new".into()),
            expires_at_ms: Some(1_781_400_000_000),
        };
        let out = render_credentials_json(CREDS, &fresh).unwrap();
        let v: serde_json::Value = serde_json::from_str(&out).unwrap();
        let o = &v["claudeAiOauth"];
        assert_eq!(o["accessToken"], "at-new");
        assert_eq!(o["refreshToken"], "rt-new");
        assert_eq!(o["expiresAt"], 1_781_400_000_000_u64);
        // Sibling fields preserved.
        assert_eq!(o["subscriptionType"], "max");
        assert_eq!(o["scopes"][0], "user:inference");
    }

    #[test]
    fn render_keeps_old_refresh_token_when_response_omits_it() {
        let fresh = OauthCredentials {
            access_token: "at-new".into(),
            refresh_token: None,
            expires_at_ms: None,
        };
        let out = render_credentials_json(CREDS, &fresh).unwrap();
        let v: serde_json::Value = serde_json::from_str(&out).unwrap();
        assert_eq!(v["claudeAiOauth"]["refreshToken"], "rt-1");
        assert_eq!(v["claudeAiOauth"]["expiresAt"], 1_781_300_000_000_u64);
    }

    #[test]
    fn parse_refresh_response_computes_expiry_from_expires_in() {
        let raw = r#"{"access_token":"at-9","refresh_token":"rt-9","expires_in":28800}"#;
        let c = parse_refresh_response(raw, 1_000_000).unwrap();
        assert_eq!(c.access_token, "at-9");
        assert_eq!(c.refresh_token.as_deref(), Some("rt-9"));
        assert_eq!(c.expires_at_ms, Some(1_000_000 + 28_800_000));
        assert!(parse_refresh_response(r#"{}"#, 0).is_err());
    }

    const USAGE: &str = r#"{"five_hour":{"utilization":77.0,"resets_at":"2026-06-12T07:40:00.484148+00:00"},"seven_day":{"utilization":56.0,"resets_at":"2026-06-16T00:00:00.484168+00:00"},"seven_day_opus":null,"extra_usage":{"is_enabled":false}}"#;

    #[test]
    fn parse_usage_reads_both_windows_with_fractional_seconds() {
        let s = parse_usage_response(USAGE).unwrap();
        let five = s.five_hour.as_ref().unwrap();
        assert_eq!(five.utilization, 77.0);
        // 2026-06-12T07:40:00Z
        assert_eq!(five.resets_at_secs, 1_781_250_000);
        let seven = s.seven_day.as_ref().unwrap();
        assert_eq!(seven.utilization, 56.0);
        assert!(seven.resets_at_secs > five.resets_at_secs);
    }

    #[test]
    fn parse_usage_degrades_missing_windows_to_none() {
        let s = parse_usage_response(r#"{"five_hour":{"utilization":10.0}}"#).unwrap();
        assert!(s.five_hour.is_none(), "missing resets_at ⇒ None");
        assert!(s.seven_day.is_none());
        assert!(parse_usage_response("nope").is_err());
    }

    #[test]
    fn probe_maps_percent_to_remaining_units() {
        let s = parse_usage_response(USAGE).unwrap();
        let p = probe_from_usage(&s, "acc-1", 1_000, 2_000, 42).unwrap();
        assert_eq!(p.account, "acc-1");
        assert_eq!(p.remaining, 230, "(100-77)% of 1000");
        assert_eq!(p.resets_at_secs, 1_781_250_000);
        assert_eq!(p.weekly_remaining, Some(880), "(100-56)% of 2000");
        assert!(p.weekly_resets_at_secs.is_some());
        assert_eq!(p.now_secs, 42);
    }

    #[test]
    fn probe_requires_five_hour_window_and_clamps_overage() {
        let none = UsageSnapshot {
            five_hour: None,
            seven_day: Some(UsageWindow {
                utilization: 1.0,
                resets_at_secs: 10,
            }),
        };
        assert!(probe_from_usage(&none, "a", 100, 100, 0).is_none());

        // Overage (>100%) clamps to 0 remaining — account drops out of rotation.
        let over = UsageSnapshot {
            five_hour: Some(UsageWindow {
                utilization: 130.0,
                resets_at_secs: 10,
            }),
            seven_day: None,
        };
        let p = probe_from_usage(&over, "a", 100, 100, 0).unwrap();
        assert_eq!(p.remaining, 0);
        assert_eq!(p.weekly_remaining, None);
    }
}
