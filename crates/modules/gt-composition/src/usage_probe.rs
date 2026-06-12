//! Server-side usage prober (hq-28b063 / hq-358fd3 / hq-002cca): the edge half of
//! `gt_quota::oauth_usage`.
//!
//! Local `quota.sample` counters are blind to usage an account incurs outside this runtime
//! (same Claude account on another device) and the locally-modeled rolling-5h window drifts
//! from the provider's real one. This prober reads the authoritative source — the OAuth
//! usage endpoint behind Claude Code's `/usage` screen — and feeds it into the existing
//! probe path ([`QuotaHandle::probe`]), so rotation gates on the provider's truth:
//!
//! - exact `resets_at` (when an exhausted account reactivates — no guessing),
//! - global utilization (external consumption shows up at the next sweep),
//! - the provider's own window, not a local reconstruction.
//!
//! Credentials come from the keychain: each account's secret is its `CLAUDE_CONFIG_DIR`
//! (`hq-agent-provisioning.7`), which holds `.credentials.json`. An expired access token is
//! refreshed against the OAuth token endpoint and the rotated tokens are persisted back to
//! the same file (the `claude` CLI reads/writes it too — preserve every sibling field).
//!
//! All parsing is pure in `gt_quota::oauth_usage`; this module owns I/O only (fs + HTTP),
//! mirroring how `quota_rotation.rs` keeps the domain clock-free.

use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use gt_quota::{
    parse_credentials_json, parse_refresh_response, parse_usage_response, probe_from_usage,
    render_credentials_json, Keychain, OauthCredentials, QuotaHandle, DEFAULT_PLAN_LIMIT,
    REFRESH_SKEW_MS,
};

/// Default usage endpoint — the same one Claude Code's `/usage` screen reads.
const DEFAULT_USAGE_URL: &str = "https://api.anthropic.com/api/oauth/usage";
/// Default OAuth token endpoint for the refresh-token grant.
const DEFAULT_TOKEN_URL: &str = "https://console.anthropic.com/v1/oauth/token";
/// Claude Code's public OAuth client id — the audience `.credentials.json` tokens belong to.
const DEFAULT_CLIENT_ID: &str = "9d1c250a-e61b-44d9-88ed-5944d1962f5e";
/// Beta header the OAuth usage endpoint requires.
const OAUTH_BETA_HEADER: &str = "oauth-2025-04-20";

/// Probes every keychain account against the provider's usage endpoint and records the
/// result through the quota actor's probe path. Endpoints/client-id are env-overridable
/// (`GT_OAUTH_USAGE_URL`, `GT_OAUTH_TOKEN_URL`, `GT_OAUTH_CLIENT_ID`) so tests and
/// alternative deployments can point elsewhere.
pub struct UsageProber {
    http: reqwest::Client,
    keychain: Arc<dyn Keychain>,
    quota: QuotaHandle,
    usage_url: String,
    token_url: String,
    client_id: String,
}

impl UsageProber {
    pub fn new(keychain: Arc<dyn Keychain>, quota: QuotaHandle) -> Self {
        let http = reqwest::Client::builder()
            .timeout(Duration::from_secs(15))
            .build()
            .expect("reqwest client");
        Self {
            http,
            keychain,
            quota,
            usage_url: std::env::var("GT_OAUTH_USAGE_URL")
                .unwrap_or_else(|_| DEFAULT_USAGE_URL.to_string()),
            token_url: std::env::var("GT_OAUTH_TOKEN_URL")
                .unwrap_or_else(|_| DEFAULT_TOKEN_URL.to_string()),
            client_id: std::env::var("GT_OAUTH_CLIENT_ID")
                .unwrap_or_else(|_| DEFAULT_CLIENT_ID.to_string()),
        }
    }

    /// Probe every account the keychain knows. Per-account failures log and continue — one
    /// broken credential file must not starve the rest of the pool. Returns the number of
    /// accounts successfully probed.
    pub async fn sweep(&self) -> usize {
        let accounts = match self.keychain.accounts() {
            Ok(a) => a,
            Err(e) => {
                eprintln!("[usage-probe] keychain accounts() failed: {e}");
                return 0;
            }
        };
        let mut ok = 0;
        for account in accounts {
            match self.probe_account(&account).await {
                Ok(()) => ok += 1,
                Err(e) => eprintln!("[usage-probe] {account}: {e}"),
            }
        }
        ok
    }

    /// Probe one account: resolve its credential file via the keychain, refresh the access
    /// token if expired (or on a 401), GET the usage endpoint, and record the resulting
    /// `ProbeWindow` through the quota actor.
    pub async fn probe_account(&self, account: &str) -> Result<(), String> {
        let record = self
            .keychain
            .get(account)
            .map_err(|e| format!("keychain get: {e}"))?
            .ok_or("keychain get: unknown account")?;
        let creds_path = Path::new(&record.secret).join(".credentials.json");
        let raw = std::fs::read_to_string(&creds_path)
            .map_err(|e| format!("read {}: {e}", creds_path.display()))?;
        let mut creds = parse_credentials_json(&raw)?;

        if creds.needs_refresh(now_ms(), REFRESH_SKEW_MS) {
            creds = self.refresh(&creds, &raw, &creds_path).await?;
        }

        let mut body = self.fetch_usage(&creds.access_token).await?;
        if body.is_none() {
            // 401 with a non-expired-looking token: refresh once and retry.
            creds = self.refresh(&creds, &raw, &creds_path).await?;
            body = self.fetch_usage(&creds.access_token).await?;
        }
        let body = body.ok_or("usage endpoint: 401 after refresh")?;

        let snapshot = parse_usage_response(&body)?;
        let probe = probe_from_usage(
            &snapshot,
            account,
            DEFAULT_PLAN_LIMIT,
            DEFAULT_PLAN_LIMIT,
            now_secs(),
        );
        eprintln!(
            "[usage-probe] {account}: 5h {:.0}% (resets {}), 7d {}",
            snapshot
                .five_hour
                .as_ref()
                .map(|w| w.utilization)
                .unwrap_or(0.0),
            probe.resets_at_secs,
            snapshot
                .seven_day
                .as_ref()
                .map(|w| format!("{:.0}%", w.utilization))
                .unwrap_or_else(|| "n/a".to_string()),
        );
        self.quota
            .probe(
                probe.account,
                probe.remaining,
                probe.resets_at_secs,
                probe.weekly_remaining,
                probe.weekly_resets_at_secs,
                probe.now_secs,
            )
            .await;
        Ok(())
    }

    /// GET the usage endpoint. `Ok(Some(body))` on 200, `Ok(None)` on 401 (so the caller
    /// can refresh-and-retry once), `Err` on anything else.
    async fn fetch_usage(&self, access_token: &str) -> Result<Option<String>, String> {
        let resp = self
            .http
            .get(&self.usage_url)
            .bearer_auth(access_token)
            .header("anthropic-beta", OAUTH_BETA_HEADER)
            .send()
            .await
            .map_err(|e| format!("usage endpoint: {e}"))?;
        match resp.status() {
            s if s.is_success() => Ok(Some(
                resp.text()
                    .await
                    .map_err(|e| format!("usage endpoint body: {e}"))?,
            )),
            reqwest::StatusCode::UNAUTHORIZED => Ok(None),
            s => Err(format!("usage endpoint: HTTP {s}")),
        }
    }

    /// Refresh the access token with the refresh-token grant and persist the rotated tokens
    /// back to `.credentials.json` (patching only the token fields — the `claude` CLI owns
    /// the rest of the file).
    async fn refresh(
        &self,
        creds: &OauthCredentials,
        original_raw: &str,
        creds_path: &PathBuf,
    ) -> Result<OauthCredentials, String> {
        let refresh_token = creds
            .refresh_token
            .as_deref()
            .ok_or("access token expired and no refresh token stored")?;
        let resp = self
            .http
            .post(&self.token_url)
            .json(&serde_json::json!({
                "grant_type": "refresh_token",
                "refresh_token": refresh_token,
                "client_id": self.client_id,
            }))
            .send()
            .await
            .map_err(|e| format!("token endpoint: {e}"))?;
        let status = resp.status();
        let body = resp
            .text()
            .await
            .map_err(|e| format!("token endpoint body: {e}"))?;
        if !status.is_success() {
            return Err(format!("token endpoint: HTTP {status}"));
        }
        let fresh = parse_refresh_response(&body, now_ms())?;
        let rendered = render_credentials_json(original_raw, &fresh)?;
        std::fs::write(creds_path, rendered)
            .map_err(|e| format!("write {}: {e}", creds_path.display()))?;
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            let _ = std::fs::set_permissions(creds_path, std::fs::Permissions::from_mode(0o600));
        }
        eprintln!("[usage-probe] refreshed access token for {}", creds_path.display());
        // Carry the stored refresh token forward when the response omitted one.
        Ok(OauthCredentials {
            refresh_token: fresh
                .refresh_token
                .clone()
                .or_else(|| creds.refresh_token.clone()),
            ..fresh
        })
    }
}

fn now_secs() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or(0)
}

fn now_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis() as u64)
        .unwrap_or(0)
}

/// REST-path on-demand usage prober: the same OAuth sweep the timer-driven [`UsageProber`]
/// runs every `GT_USAGE_PROBE_SECS`, but appended through the event-sourced
/// [`WorkspaceQuota`] path so it works from `gt-mcp-server` (which has no live actor).
pub struct EventLogSyncProber {
    http: reqwest::Client,
    usage_url: String,
    token_url: String,
    client_id: String,
}

impl EventLogSyncProber {
    pub fn new() -> Self {
        let http = reqwest::Client::builder()
            .timeout(Duration::from_secs(15))
            .build()
            .expect("reqwest client");
        Self {
            http,
            usage_url: std::env::var("GT_OAUTH_USAGE_URL")
                .unwrap_or_else(|_| DEFAULT_USAGE_URL.to_string()),
            token_url: std::env::var("GT_OAUTH_TOKEN_URL")
                .unwrap_or_else(|_| DEFAULT_TOKEN_URL.to_string()),
            client_id: std::env::var("GT_OAUTH_CLIENT_ID")
                .unwrap_or_else(|_| DEFAULT_CLIENT_ID.to_string()),
        }
    }

    async fn fetch_usage(&self, access_token: &str) -> Result<Option<String>, String> {
        let resp = self
            .http
            .get(&self.usage_url)
            .bearer_auth(access_token)
            .header("anthropic-beta", OAUTH_BETA_HEADER)
            .send()
            .await
            .map_err(|e| format!("usage endpoint: {e}"))?;
        match resp.status() {
            s if s.is_success() => Ok(Some(
                resp.text()
                    .await
                    .map_err(|e| format!("usage endpoint body: {e}"))?,
            )),
            reqwest::StatusCode::UNAUTHORIZED => Ok(None),
            s => Err(format!("usage endpoint: HTTP {s}")),
        }
    }

    async fn refresh(
        &self,
        creds: &OauthCredentials,
        original_raw: &str,
        creds_path: &std::path::Path,
    ) -> Result<OauthCredentials, String> {
        let refresh_token = creds
            .refresh_token
            .as_deref()
            .ok_or("access token expired and no refresh token stored")?;
        let resp = self
            .http
            .post(&self.token_url)
            .json(&serde_json::json!({
                "grant_type": "refresh_token",
                "refresh_token": refresh_token,
                "client_id": self.client_id,
            }))
            .send()
            .await
            .map_err(|e| format!("token refresh: {e}"))?;
        if !resp.status().is_success() {
            return Err(format!("token refresh: HTTP {}", resp.status()));
        }
        let body = resp
            .text()
            .await
            .map_err(|e| format!("token refresh body: {e}"))?;
        let fresh = parse_refresh_response(&body, now_ms())?;
        let new_raw = render_credentials_json(original_raw, &fresh)?;
        std::fs::write(creds_path, &new_raw)
            .map_err(|e| format!("write {}: {e}", creds_path.display()))?;
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            let _ = std::fs::set_permissions(creds_path, std::fs::Permissions::from_mode(0o600));
        }
        // Carry the stored refresh token forward when the response omitted one.
        Ok(OauthCredentials {
            refresh_token: fresh
                .refresh_token
                .clone()
                .or_else(|| creds.refresh_token.clone()),
            ..fresh
        })
    }
}

#[async_trait::async_trait]
impl gt_quota::http::SyncProber for EventLogSyncProber {
    async fn sweep(
        &self,
        workspace: &str,
        quota: &dyn gt_quota::http::WorkspaceQuota,
        catalog: &dyn gt_quota::http::AccountCatalog,
    ) -> usize {
        use gt_events::Command;
        let mut registry = match quota.registry(workspace).await {
            Ok(r) => r,
            Err(e) => {
                eprintln!("[sync-probe] registry load for {workspace}: {e}");
                return 0;
            }
        };
        let accounts: Vec<String> = registry.accounts().map(|a| a.id.clone()).collect();
        let mut ok = 0usize;
        for account in &accounts {
            let config_dir = match catalog.config_dir(account).await {
                Ok(Some(d)) => d,
                Ok(None) => {
                    eprintln!("[sync-probe] {account}: no credentials dir in catalog");
                    continue;
                }
                Err(e) => {
                    eprintln!("[sync-probe] {account}: catalog error: {e}");
                    continue;
                }
            };
            let creds_path = std::path::Path::new(&config_dir).join(".credentials.json");
            let raw = match std::fs::read_to_string(&creds_path) {
                Ok(r) => r,
                Err(e) => {
                    eprintln!("[sync-probe] {account}: read credentials: {e}");
                    continue;
                }
            };
            let mut creds = match parse_credentials_json(&raw) {
                Ok(c) => c,
                Err(e) => {
                    eprintln!("[sync-probe] {account}: parse credentials: {e}");
                    continue;
                }
            };
            if creds.needs_refresh(now_ms(), REFRESH_SKEW_MS) {
                match self.refresh(&creds, &raw, &creds_path).await {
                    Ok(fresh) => creds = fresh,
                    Err(e) => {
                        eprintln!("[sync-probe] {account}: refresh: {e}");
                        continue;
                    }
                }
            }
            let mut body = match self.fetch_usage(&creds.access_token).await {
                Ok(b) => b,
                Err(e) => {
                    eprintln!("[sync-probe] {account}: fetch_usage: {e}");
                    continue;
                }
            };
            if body.is_none() {
                // 401 — refresh once and retry
                match self.refresh(&creds, &raw, &creds_path).await {
                    Ok(fresh) => {
                        creds = fresh;
                        match self.fetch_usage(&creds.access_token).await {
                            Ok(b) => body = b,
                            Err(e) => {
                                eprintln!("[sync-probe] {account}: fetch_usage after refresh: {e}");
                                continue;
                            }
                        }
                    }
                    Err(e) => {
                        eprintln!("[sync-probe] {account}: refresh on 401: {e}");
                        continue;
                    }
                }
            }
            let body = match body {
                Some(b) => b,
                None => {
                    eprintln!("[sync-probe] {account}: 401 after refresh");
                    continue;
                }
            };
            let snapshot = match parse_usage_response(&body) {
                Ok(s) => s,
                Err(e) => {
                    eprintln!("[sync-probe] {account}: parse_usage_response: {e}");
                    continue;
                }
            };
            let probe = probe_from_usage(
                &snapshot,
                account.as_str(),
                DEFAULT_PLAN_LIMIT,
                DEFAULT_PLAN_LIMIT,
                now_secs(),
            );
            let event = match probe.execute(&mut registry) {
                Ok(e) => e,
                Err(e) => {
                    eprintln!("[sync-probe] {account}: execute probe command: {e}");
                    continue;
                }
            };
            if let Err(e) = quota.append(workspace, event).await {
                eprintln!("[sync-probe] {account}: append event: {e}");
                continue;
            }
            eprintln!(
                "[sync-probe] {account}: probed OK ({:.0}%)",
                snapshot.five_hour.as_ref().map(|w| w.utilization).unwrap_or(0.0)
            );
            ok += 1;
        }
        ok
    }
}
