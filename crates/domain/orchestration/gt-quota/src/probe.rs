//! Real probe: parses provider rate-limit headers into a [`ProbeWindow`] command.
//!
//! Per `docs/features/token-tracking-prediction.md`, the provider's `anthropic-ratelimit-*`
//! headers are the **authoritative** source of `remaining` and `reset` — local counts only
//! exist for per-session attribution and to smooth between probes. This module is the edge
//! that turns the raw HTTP response into a typed command the actor can apply; it is pure
//! (sync, no clock read), so a unit test seeded with a known header response yields the same
//! `ProbeWindow` on every replay.
//!
//! Only the `tokens` family is consumed for the consumption-side reconciliation
//! (`anthropic-ratelimit-tokens-remaining` / `…-reset`). The provider also exposes a parallel
//! `requests` family; it does not feed the EWMA rate (which is per-token), so we surface it on
//! the [`RatelimitHeaders`] snapshot for observability/dashboards but do not derive a probe
//! from it.

use std::time::{SystemTime, UNIX_EPOCH};

use time::OffsetDateTime;

use crate::commands::ProbeWindow;

/// Authoritative snapshot of every header the provider exposed. The probe surfaces the full
/// snapshot (not only the fields it consumed) so the bin/feed can log the requests-side
/// numbers without re-parsing.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct RatelimitHeaders {
    /// `anthropic-ratelimit-requests-limit`.
    pub requests_limit: Option<u64>,
    /// `anthropic-ratelimit-requests-remaining`.
    pub requests_remaining: Option<u64>,
    /// `anthropic-ratelimit-requests-reset` parsed to UTC epoch seconds.
    pub requests_reset_secs: Option<u64>,
    /// `anthropic-ratelimit-tokens-limit`.
    pub tokens_limit: Option<u64>,
    /// `anthropic-ratelimit-tokens-remaining`.
    pub tokens_remaining: Option<u64>,
    /// `anthropic-ratelimit-tokens-reset` parsed to UTC epoch seconds.
    pub tokens_reset_secs: Option<u64>,
}

impl RatelimitHeaders {
    /// Parse from the raw headers (case-insensitive lookup). Unknown / unset values become
    /// `None`; the probe only consumes the `tokens-*` triple but other consumers (panels,
    /// `gt-feed`) may want the `requests-*` triple too.
    pub fn from_headers(headers: &[(String, String)]) -> Self {
        let lookup = |name: &str| -> Option<&str> {
            headers
                .iter()
                .find(|(k, _)| k.eq_ignore_ascii_case(name))
                .map(|(_, v)| v.as_str())
        };
        Self {
            requests_limit: lookup("anthropic-ratelimit-requests-limit").and_then(parse_u64),
            requests_remaining: lookup("anthropic-ratelimit-requests-remaining")
                .and_then(parse_u64),
            requests_reset_secs: lookup("anthropic-ratelimit-requests-reset")
                .and_then(parse_reset_secs),
            tokens_limit: lookup("anthropic-ratelimit-tokens-limit").and_then(parse_u64),
            tokens_remaining: lookup("anthropic-ratelimit-tokens-remaining").and_then(parse_u64),
            tokens_reset_secs: lookup("anthropic-ratelimit-tokens-reset")
                .and_then(parse_reset_secs),
        }
    }

    /// True when both `tokens-remaining` and `tokens-reset` are present and parseable — the
    /// only shape the probe consumes.
    pub fn has_tokens_window(&self) -> bool {
        self.tokens_remaining.is_some() && self.tokens_reset_secs.is_some()
    }
}

/// Parse the provider response headers and emit a [`ProbeWindow`] command if both
/// `anthropic-ratelimit-tokens-remaining` and `…-reset` are present. The caller stamps
/// `now_secs` at the edge — the function is otherwise clock-free, so repeated calls on the
/// same header set yield bit-identical commands (idempotent under retry).
pub fn parse_anthropic_ratelimit(
    headers: &[(String, String)],
    account: impl Into<String>,
    now_secs: u64,
) -> Option<ProbeWindow> {
    let snapshot = RatelimitHeaders::from_headers(headers);
    let remaining = snapshot.tokens_remaining?;
    let resets_at_secs = snapshot.tokens_reset_secs?;
    Some(ProbeWindow {
        account: account.into(),
        remaining,
        resets_at_secs,
        now_secs,
    })
}

fn parse_u64(raw: &str) -> Option<u64> {
    raw.trim().parse().ok()
}

/// `anthropic-ratelimit-*-reset` is an RFC 3339 timestamp (UTC). The provider used to send
/// integer epoch seconds; accept both shapes so the parser keeps working if the format
/// switches back, but always normalize to UTC epoch seconds.
fn parse_reset_secs(raw: &str) -> Option<u64> {
    let raw = raw.trim();
    if let Ok(epoch) = raw.parse::<u64>() {
        return Some(epoch);
    }
    let ts = OffsetDateTime::parse(raw, &time::format_description::well_known::Rfc3339).ok()?;
    let secs = ts.unix_timestamp();
    if secs < 0 {
        return None;
    }
    Some(secs as u64)
}

/// Edge-only helper: today's wall clock in UTC epoch seconds. Lives here (not in the
/// composition root) so a callsite that already has the headers can produce a ready-to-exec
/// `ProbeWindow` in one call without pulling a clock dep. The core never calls this.
pub fn now_secs_edge() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or(0)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn h(pairs: &[(&str, &str)]) -> Vec<(String, String)> {
        pairs
            .iter()
            .map(|(k, v)| (k.to_string(), v.to_string()))
            .collect()
    }

    #[test]
    fn snapshot_parses_both_families_case_insensitive() {
        let headers = h(&[
            ("Anthropic-RateLimit-Requests-Limit", "1000"),
            ("anthropic-ratelimit-requests-remaining", "999"),
            ("anthropic-ratelimit-requests-reset", "2026-05-28T10:00:00Z"),
            ("anthropic-ratelimit-tokens-limit", "1000000"),
            ("anthropic-ratelimit-tokens-remaining", "750000"),
            ("anthropic-ratelimit-tokens-reset", "2026-05-28T11:00:00Z"),
        ]);
        let snap = RatelimitHeaders::from_headers(&headers);
        assert_eq!(snap.requests_limit, Some(1000));
        assert_eq!(snap.requests_remaining, Some(999));
        assert!(snap.requests_reset_secs.is_some());
        assert_eq!(snap.tokens_limit, Some(1_000_000));
        assert_eq!(snap.tokens_remaining, Some(750_000));
        assert!(snap.has_tokens_window());
    }

    #[test]
    fn parse_returns_probe_when_tokens_window_present() {
        let headers = h(&[
            ("anthropic-ratelimit-tokens-remaining", "250000"),
            ("anthropic-ratelimit-tokens-reset", "2026-05-28T11:00:00Z"),
        ]);
        let probe =
            parse_anthropic_ratelimit(&headers, "acc-1", 1_748_433_600).expect("probe emitted");
        assert_eq!(probe.account, "acc-1");
        assert_eq!(probe.remaining, 250_000);
        assert_eq!(probe.now_secs, 1_748_433_600);
        // 2026-05-28T11:00:00Z = 1748430000 (approximately; just sanity-check the parse).
        assert!(probe.resets_at_secs > 1_748_400_000);
    }

    #[test]
    fn parse_handles_legacy_epoch_seconds_reset() {
        let headers = h(&[
            ("anthropic-ratelimit-tokens-remaining", "100"),
            ("anthropic-ratelimit-tokens-reset", "1748430000"),
        ]);
        let probe = parse_anthropic_ratelimit(&headers, "acc-1", 1_748_429_000).unwrap();
        assert_eq!(probe.resets_at_secs, 1_748_430_000);
    }

    #[test]
    fn parse_returns_none_when_tokens_window_incomplete() {
        // Missing reset.
        let headers = h(&[("anthropic-ratelimit-tokens-remaining", "100")]);
        assert!(parse_anthropic_ratelimit(&headers, "acc-1", 0).is_none());

        // Missing remaining.
        let headers = h(&[(
            "anthropic-ratelimit-tokens-reset",
            "2026-05-28T11:00:00Z",
        )]);
        assert!(parse_anthropic_ratelimit(&headers, "acc-1", 0).is_none());

        // Both absent.
        assert!(parse_anthropic_ratelimit(&[], "acc-1", 0).is_none());
    }

    #[test]
    fn parse_returns_none_on_garbled_values() {
        let headers = h(&[
            ("anthropic-ratelimit-tokens-remaining", "not-a-number"),
            ("anthropic-ratelimit-tokens-reset", "not-a-date"),
        ]);
        assert!(parse_anthropic_ratelimit(&headers, "acc-1", 0).is_none());
    }

    #[test]
    fn parse_is_idempotent_under_retry() {
        let headers = h(&[
            ("anthropic-ratelimit-tokens-remaining", "100"),
            ("anthropic-ratelimit-tokens-reset", "1748430000"),
        ]);
        let a = parse_anthropic_ratelimit(&headers, "acc-1", 42).unwrap();
        let b = parse_anthropic_ratelimit(&headers, "acc-1", 42).unwrap();
        assert_eq!(a, b);
    }
}
