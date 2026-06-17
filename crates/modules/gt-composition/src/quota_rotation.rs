//! Predictive claude-account rotation: the edge-effect that ties the quota predictor to the
//! keychain's live credential pointer (`hq-agent-provisioning.7`).
//!
//! The quota actor already PREDICTS (`QuotaHandle::tick` → `quota.block_predicted.v1`) and the
//! keychain already stores a live-pointer credential per account. What was missing — the
//! `lib.rs` "edge-effect arms … rotate … NOT wired" gap — is the observer that ties the two:
//! when the predictor says the active account will block (or the provider already limited it),
//! flip the keychain's active pointer to a healthy account **before** the session hits the wall,
//! and record the rotation so the next polecat sling picks the new account's credentials.
//!
//! Two halves live here, both edges of the same loop:
//!
//! - [`QuotaRotationPlugin`] — the EFFECT. An observer on the daemon hub (sibling of
//!   [`crate::polecat::PolecatSupervisorPlugin`]): a `quota.block_predicted.v1` (predictive) or
//!   `quota.account_limited.v1` (reactive) names the at-risk account; it asks the quota actor for
//!   an account snapshot, picks the first HEALTHY account that is not the at-risk one, calls
//!   [`Keychain::set_active`], and tells the actor [`QuotaHandle::rotated`] (which parks the old
//!   account in `Cooldown` and emits `quota.rotated.v1`). Idempotent: if the live pointer already
//!   moved off the at-risk account, or there is no healthy target, it does nothing — a single
//!   prediction never thrashes.
//!
//! - [`run`] — the INPUT. A gt-channel feed (sibling of `gt_scheduling::dispatch::run`): an
//!   external edge — a hook reporting a claude session's `anthropic-ratelimit-*` headers + token
//!   usage, a sidecar proxy, or a manual probe — drops a [`QuotaFeedPayload`] JSON message; the
//!   loop seeds/refreshes the account's window (so the predictor has something to evaluate) and
//!   folds the per-response token sample (so the consumption-rate EWMA grows). Without this feed
//!   the predictor stays flat (rate 0 ⇒ no `BlockPredicted`); with it, `tick` projects the block.

use std::collections::HashSet;
use std::sync::{Arc, Mutex};
use std::time::{SystemTime, UNIX_EPOCH};

use async_trait::async_trait;
use serde::{Deserialize, Serialize};

use gt_channel::Channel;
use gt_eventlog::EventRecord;
use gt_events::AppError;
use gt_plugin::Plugin;
use gt_polecat::PolecatSupervisor;
use gt_quota::{
    parse_anthropic_ratelimit, Account, AccountQuotaStatus, AccountWindow, Keychain, QuotaEvent,
    QuotaHandle, RatelimitHeaders, WindowKind,
};

/// Edge-only wall clock in UTC epoch seconds. The domain never reads the clock; this observer is
/// an edge, so it stamps `now` on the messages it sends to the actor.
fn now_secs() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or(0)
}

/// Spawn a one-shot task that fires a synthetic probe at `until_secs + 5s` if the account is still
/// `Blocked` at that point. The 5-second grace margin absorbs clock skew between our clock and the
/// provider's `Retry-After` boundary. `apply_probe` already lifts `Blocked → Healthy` when
/// `remaining > 0`; no actor changes are needed.
fn schedule_unblock(quota: QuotaHandle, account: String, until_secs: u64) {
    tokio::spawn(async move {
        let delay = until_secs.saturating_sub(now_secs()) + 5;
        tokio::time::sleep(tokio::time::Duration::from_secs(delay)).await;
        let accounts = quota.accounts().await;
        let Some(acc) = accounts.iter().find(|a| a.id == account) else {
            return;
        };
        if acc.status != AccountQuotaStatus::Blocked {
            return;
        }
        // Use the existing window's reset as the anchor so the synthetic probe doesn't push it
        // further into the future than the real window. Fall back to 5h from now if no window.
        let resets_at = acc
            .window
            .as_ref()
            .map(|w| w.resets_at_secs)
            .unwrap_or_else(|| now_secs() + ROLLING_5H_SECS);
        quota
            .probe(account, plan_limit(), resets_at, None, None, now_secs())
            .await;
    });
}

/// The rolling token window (standard claude plan).
const ROLLING_5H_SECS: u64 = 5 * 3600;

/// Default budget for a header-less synthetic window, in quota cost units (`gt_quota::cost_units`).
/// Identity weights (the daemon's default) count cache-read tokens at full price, so a turn is on
/// the order of 10^5–10^6 units — the default is deliberately generous to avoid over-rotating on an
/// uncalibrated estimate. Tune via `GT_QUOTA_PLAN_LIMIT`; the predictive `learned_limit` follow-up
/// (and the authoritative `anthropic-ratelimit-*` headers via a proxy) refine it from real data.
const DEFAULT_PLAN_LIMIT: u64 = 50_000_000;

/// Configured synthetic-window budget (`GT_QUOTA_PLAN_LIMIT`, else [`DEFAULT_PLAN_LIMIT`]).
fn plan_limit() -> u64 {
    std::env::var("GT_QUOTA_PLAN_LIMIT")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(DEFAULT_PLAN_LIMIT)
}

/// Soft (draining) threshold percent (`GT_QUOTA_SOFT_PCT`, default 80): a probed account at or
/// above it stops RECEIVING work — the rotation pointer moves off it while in-flight polecats
/// finish naturally (hq-49198f). Pairs with the provider's `allowed_warning` verdict.
fn soft_pct() -> f64 {
    std::env::var("GT_QUOTA_SOFT_PCT")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(80.0)
}

/// Hard threshold percent (`GT_QUOTA_HARD_PCT`, default 90): an account at or above it is never
/// a rotation TARGET — rotating into an almost-exhausted account just moves the wall closer.
fn hard_pct() -> f64 {
    std::env::var("GT_QUOTA_HARD_PCT")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(90.0)
}

/// Utilization percent the gates consume: the WORSE of the rolling-5h and weekly windows
/// (hq-34a2f5 — an account with the weekly budget burnt must not win as rotation target on a
/// fresh 5h window alone). No window ⇒ 0 (an unverified account is a candidate — the first
/// probe corrects it within one sweep).
fn utilization_pct(acc: &Account) -> f64 {
    let pct = |w: &Option<AccountWindow>| match w {
        Some(w) if w.limit > 0 => (w.consumed / w.limit as f64) * 100.0,
        _ => 0.0,
    };
    pct(&acc.window).max(pct(&acc.weekly_window))
}

/// A fresh Rolling5h window anchored at `now`, used when no provider headers supply the real one.
fn synthetic_window(now: u64) -> AccountWindow {
    AccountWindow {
        kind: WindowKind::Rolling5h,
        limit: plan_limit(),
        started_at_secs: now,
        resets_at_secs: now + ROLLING_5H_SECS,
        consumed: 0.0,
    }
}

/// Observer that turns a block prediction (or a reactive limit) into a real credential rotation:
/// `quota.block_predicted.v1` / `quota.account_limited.v1` → [`Keychain::set_active`] +
/// [`QuotaHandle::rotated`]. Registered on the daemon hub alongside the polecat supervisor.
pub struct QuotaRotationPlugin {
    quota: QuotaHandle,
    keychain: Arc<dyn Keychain>,
    /// Optional polecat supervisor reference for in-flight session detection
    /// (`hq-quota-refinement.3`). When wired, `rotate_away_from` emits a structured warning for
    /// every supervised polecat that was backed by the rotated account so the operator can act.
    supervisor: Option<Arc<PolecatSupervisor>>,
    /// Accounts whose in-flight polecats are currently SIGSTOP'd because rotation found no healthy
    /// target — pause-on-exhaustion (gtcore-6f449f). The set gates recovery: a probe lifting one of
    /// these accounts back to `Healthy` triggers the matching resume exactly once. Interior
    /// mutability because the plugin is shared behind `Arc` on the daemon hub; the lock is never
    /// held across an `.await`.
    paused_accounts: Mutex<HashSet<String>>,
}

impl QuotaRotationPlugin {
    /// Wire the quota command handle (for the account snapshot + the `rotated` record) and the
    /// keychain (whose live pointer the rotation flips).
    pub fn new(quota: QuotaHandle, keychain: Arc<dyn Keychain>) -> Self {
        Self {
            quota,
            keychain,
            supervisor: None,
            paused_accounts: Mutex::new(HashSet::new()),
        }
    }

    /// Wire the polecat supervisor so in-flight session risk is surfaced on rotation
    /// (`hq-quota-refinement.3`).
    pub fn with_supervisor(mut self, supervisor: Arc<PolecatSupervisor>) -> Self {
        self.supervisor = Some(supervisor);
        self
    }

    /// Pick a healthy rotation target `!= at_risk` from the registry snapshot. Candidates at or
    /// above the HARD threshold are excluded (rotating into an almost-exhausted account just
    /// moves the wall, hq-49198f); among the rest the LOWEST probed utilization wins, so the
    /// pointer lands on the account with the most real headroom. Deterministic: utilization
    /// ties break on the registry's insertion order. `None` when no eligible account exists
    /// (the caller then stays put).
    fn pick_target(accounts: &[Account], at_risk: &str) -> Option<String> {
        let hard = hard_pct();
        accounts
            .iter()
            .filter(|a| {
                a.id != at_risk
                    && a.status == AccountQuotaStatus::Healthy
                    && utilization_pct(a) < hard
            })
            .min_by(|a, b| {
                utilization_pct(a)
                    .partial_cmp(&utilization_pct(b))
                    .unwrap_or(std::cmp::Ordering::Equal)
            })
            .map(|a| a.id.clone())
    }

    /// The rotation decision, shared by the predictive and reactive arms. Idempotent and
    /// best-effort: a keychain miss logs and leaves the pointer untouched (no phantom rotation
    /// event is recorded).
    async fn rotate_away_from(&self, at_risk: &str) -> Result<(), AppError> {
        // Already rotated off this account? The live pointer moved → nothing to do (a second
        // prediction for the same account must not thrash the keychain or spam rotation events).
        if let Some(active) = self.keychain.active()? {
            if active != at_risk {
                return Ok(());
            }
        }
        let accounts = self.quota.accounts().await;
        let Some(target) = Self::pick_target(&accounts, at_risk) else {
            // No healthy account anywhere (every alternative is Blocked or ≥ hard). Letting the
            // in-flight polecats keep running just feeds them 429s until they crash or spin
            // retrying. Instead, suspend them in place (SIGSTOP) so their context survives until an
            // account recovers (gtcore-6f449f); the matching resume fires from the recovery probe.
            self.pause_on_exhaustion(at_risk).await;
            return Ok(());
        };
        // Flip the live credential pointer FIRST: if the target has no stored credential the
        // keychain refuses (NotFound), and we must not record a rotation that did not take.
        self.keychain.set_active(&target)?;
        // Record it: parks `at_risk` in Cooldown + emits `quota.rotated.v1` on the hub (durable).
        self.quota
            .rotated(at_risk.to_string(), target.clone(), now_secs())
            .await;
        eprintln!("[quota-rotation] active claude account rotated {at_risk} → {target}");
        // Hot credential swap: copy the NEW account's .credentials.json into every in-flight
        // polecat's CLAUDE_CONFIG_DIR so claude CLI picks up the fresh token without restarting. The
        // polecat keeps running — only the underlying account changes.
        self.hot_swap_in_flight(at_risk, &target);
        Ok(())
    }

    /// Copy `to`'s `.credentials.json` into every in-flight polecat still backed by `from`, so the
    /// `claude` CLI there picks up the new account's token WITHOUT restarting the session (the
    /// hot-swap half of the rotation contract). Best-effort per session: a copy failure logs and
    /// leaves that polecat on the old credentials. No-op when no supervisor is wired or the target
    /// has no stored credential dir.
    fn hot_swap_in_flight(&self, from: &str, to: &str) {
        let Some(sup) = &self.supervisor else {
            return;
        };
        let Ok(Some(to_cred)) = self.keychain.get(to) else {
            return;
        };
        for (session, config_dir) in sup.config_dirs_for_account(from) {
            match crate::credential_guard::seed_credentials(&to_cred.secret, &config_dir) {
                Ok(()) => eprintln!(
                    "[quota-rotation] hot-swapped credentials for {session}: {from} → {to}"
                ),
                Err(e) => eprintln!(
                    "[quota-rotation] WARN credential swap failed for {session}: {e} \
                     — polecat stays on {from}"
                ),
            }
        }
    }

    /// Proactive soft-drain (gtcore-df3319): the ACTIVE account crossed the soft threshold while
    /// still usable. Flip the keychain pointer to the healthiest alternative so NEW slings land
    /// there, and record `quota.soft_drain.v1` (source → target). Unlike [`Self::rotate_away_from`]
    /// this does NOT hot-swap in-flight polecats' credentials: they finish naturally on the
    /// draining account — the bead's "drain, don't kill, don't rotate creds" contract. When no
    /// healthy alternative exists (every other account is at or above the hard threshold) the
    /// pointer stays put and `quota.soft_drain_stalled.v1` fires as an operator alert instead of
    /// rotating into an almost-exhausted account. Idempotent: a probe arriving once the pointer has
    /// already moved off `at_risk` is a no-op (no thrash, no duplicate record).
    async fn soft_drain_away_from(&self, at_risk: &str) -> Result<(), AppError> {
        // Already drained off this account? The live pointer moved → nothing to do.
        if let Some(active) = self.keychain.active()? {
            if active != at_risk {
                return Ok(());
            }
        }
        let accounts = self.quota.accounts().await;
        let Some(target) = Self::pick_target(&accounts, at_risk) else {
            eprintln!(
                "[quota-rotation] {at_risk} at soft threshold but every alternative is ≥ {:.0}% \
                 (hard) — staying put, alerting (quota.soft_drain_stalled.v1)",
                hard_pct()
            );
            self.quota
                .soft_drain_stalled(at_risk.to_string(), now_secs())
                .await;
            return Ok(());
        };
        // Flip the live credential pointer FIRST: if the target has no stored credential the
        // keychain refuses (NotFound), and we must not record a drain that did not take.
        self.keychain.set_active(&target)?;
        // Record it: parks `at_risk` in Cooldown + emits `quota.soft_drain.v1` on the hub (durable).
        self.quota
            .soft_drained(at_risk.to_string(), target.clone(), now_secs())
            .await;
        // No credential hot-swap here (cf. rotate_away_from): in-flight polecats keep running on
        // `at_risk` and drain it naturally; only the pointer for new slings moved to `target`.
        eprintln!(
            "[quota-rotation] soft-drain {at_risk} → {target}: new slings use {target}, \
             in-flight work drains on {at_risk}"
        );
        Ok(())
    }

    /// Pause-on-exhaustion (gtcore-6f449f): rotation found no healthy alternative for `at_risk`, so
    /// suspend its in-flight polecats in place and emit `quota.all_exhausted.v1` with the paused
    /// session list. Idempotent — once an account is recorded paused, a repeat trigger (another
    /// `BlockPredicted`/`Blocked` for the same account) is a no-op so the event is not spammed and
    /// already-frozen polecats are not re-signalled.
    async fn pause_on_exhaustion(&self, at_risk: &str) {
        // Idempotency guard: already paused for this account?
        {
            let paused = self.paused_accounts.lock().unwrap();
            if paused.contains(at_risk) {
                return;
            }
        }
        let paused_sessions = match &self.supervisor {
            Some(sup) => sup.pause_account(at_risk),
            None => Vec::new(),
        };
        // Record the account as paused so the recovery probe knows to resume it. We mark it even
        // when no sessions were paused (no supervisor / no in-flight work) so the all_exhausted →
        // account_recovered pairing stays symmetric; resume on an empty set is a cheap no-op.
        self.paused_accounts
            .lock()
            .unwrap()
            .insert(at_risk.to_string());
        eprintln!(
            "[quota-rotation] {at_risk} at risk but no healthy alternative — pausing {} in-flight \
             polecat(s) in place (quota.all_exhausted.v1)",
            paused_sessions.len()
        );
        self.quota
            .all_exhausted(at_risk.to_string(), paused_sessions, now_secs())
            .await;
    }

    /// Resume-on-recovery (gtcore-6f449f): a probe named `account`; if we had paused it under
    /// [`Self::pause_on_exhaustion`] and it is now back to `Healthy`, resume its suspended polecats
    /// and emit `quota.account_recovered.v1` with the resumed session list. No-op when the account
    /// was never paused or has not recovered yet. Runs on every `quota.usage_probed.v1`, so the
    /// account-paused gate keeps it cheap for the common (nothing-paused) case.
    async fn resume_if_recovered(&self, account: &str) {
        // Fast path: nothing paused for this account.
        {
            let paused = self.paused_accounts.lock().unwrap();
            if !paused.contains(account) {
                return;
            }
        }
        // Confirm the probe actually lifted it back to Healthy before thawing.
        let recovered = self
            .quota
            .accounts()
            .await
            .iter()
            .any(|a| a.id == account && a.status == AccountQuotaStatus::Healthy);
        if !recovered {
            return;
        }
        // Clear the gate first (so a concurrent probe does not double-resume), then thaw. Bind in
        // its own statement so the guard drops before the await below (never held across `.await`).
        let was_paused = self.paused_accounts.lock().unwrap().remove(account);
        if !was_paused {
            // Lost the race to another probe — it already handled the resume.
            return;
        }
        let resumed_sessions = match &self.supervisor {
            Some(sup) => sup.resume_account(account),
            None => Vec::new(),
        };
        eprintln!(
            "[quota-rotation] {account} recovered to Healthy — resuming {} paused polecat(s) \
             (quota.account_recovered.v1)",
            resumed_sessions.len()
        );
        self.quota
            .account_recovered(account.to_string(), resumed_sessions, now_secs())
            .await;
    }
}

#[async_trait]
impl Plugin for QuotaRotationPlugin {
    fn name(&self) -> &'static str {
        "quota-rotation"
    }

    async fn on_event(&self, record: &EventRecord) -> Result<(), AppError> {
        match record.kind.as_str() {
            // Predictive: the window will exhaust before reset → rotate early.
            "quota.block_predicted.v1" => {
                let at_risk = match record.decode::<QuotaEvent>()? {
                    QuotaEvent::BlockPredicted { account, .. } => account,
                    _ => return Ok(()),
                };
                self.rotate_away_from(&at_risk).await
            }
            // Reactive: the provider already returned 429 → rotate now.
            "quota.account_limited.v1" => {
                let at_risk = match record.decode::<QuotaEvent>()? {
                    QuotaEvent::AccountLimited { account, .. } => account,
                    _ => return Ok(()),
                };
                self.rotate_away_from(&at_risk).await
            }
            // Hard block (the proxy saw `rejected`, or a 429 with Retry-After): rotate the
            // pointer IMMEDIATELY (hq-49198f — without this, the supervisor re-slung dead
            // polecats onto the blocked account until max_restarts burned out), then schedule
            // a probe at until_secs + 5s so the account re-enters rotation as soon as the
            // provider window closes, not on the next periodic poll.
            "quota.blocked.v1" => {
                if let QuotaEvent::Blocked { account, until_secs, .. } =
                    record.decode::<QuotaEvent>()?
                {
                    self.rotate_away_from(&account).await?;
                    if let Some(until) = until_secs {
                        schedule_unblock(self.quota.clone(), account, until);
                    }
                }
                Ok(())
            }
            // A rotation was RECORDED (gtcore-bf4acd / BUG2). The MANUAL `quota.rotate` MCP tool
            // moves quota accounting and emits this, but — unlike the predictive/reactive arms above
            // — it never touched the keychain's live pointer or in-flight credentials, so orchd kept
            // slinging onto the OLD account-dir and live polecats kept their stale token. Couple the
            // two pointers here: flip the live pointer to `to_account` so the NEXT sling uses it, and
            // hot-swap creds into every in-flight polecat still backed by `from_account` so the CLI
            // re-reads the new token without a restart. Idempotent with the automatic arms (which
            // already did both before emitting): set_active to the current target is a no-op and the
            // copy is overwrite-identical.
            "quota.rotated.v1" => {
                if let QuotaEvent::Rotated { from_account, to_account, .. } =
                    record.decode::<QuotaEvent>()?
                {
                    if from_account == to_account {
                        return Ok(());
                    }
                    // Only flip when the live pointer is not already there — avoids a redundant
                    // keychain write on the automatic path that emitted this event after flipping.
                    if self.keychain.active()?.as_deref() != Some(to_account.as_str()) {
                        if let Err(e) = self.keychain.set_active(&to_account) {
                            // Unknown/credential-less target: leave the pointer rather than strand it.
                            eprintln!(
                                "[quota-rotation] rotated.v1 set_active({to_account}) failed: {e} — pointer unchanged"
                            );
                        } else {
                            eprintln!(
                                "[quota-rotation] rotated.v1 coupled live pointer {from_account} → {to_account} (manual rotate)"
                            );
                        }
                    }
                    self.hot_swap_in_flight(&from_account, &to_account);
                }
                Ok(())
            }
            // Probe-driven draining gate (hq-49198f): every probe (per-call proxy headers or
            // the /usage sweep) re-evaluates the ACTIVE account against the soft threshold.
            // At/above it the pointer moves to the account with the most headroom — in-flight
            // polecats finish on the old account (drain), new slings land on the target.
            "quota.usage_probed.v1" => {
                let account = match record.decode::<QuotaEvent>()? {
                    QuotaEvent::UsageProbed { account, .. } => account,
                    _ => return Ok(()),
                };
                // Recovery first (gtcore-6f449f): if this probe lifted a paused-on-exhaustion
                // account back to Healthy, resume its suspended polecats. This is exactly the path
                // the synthetic unblock probe (`schedule_unblock`) lands on after a Retry-After
                // expires. Runs regardless of the live pointer — the recovered account may not be
                // active yet.
                self.resume_if_recovered(&account).await;
                // Only the live pointer gates assignment; probes for parked accounts are
                // recovery signals, not draining triggers.
                if self.keychain.active()?.as_deref() != Some(account.as_str()) {
                    return Ok(());
                }
                let accounts = self.quota.accounts().await;
                let Some(acc) = accounts.iter().find(|a| a.id == account) else {
                    return Ok(());
                };
                let pct = utilization_pct(acc);
                if pct >= soft_pct() {
                    // Skip the drain if the window resets within the prediction threshold: the
                    // account will recover on its own and rotating now just creates unnecessary
                    // Cooldown → Healthy churn (hq-49198f-drain-guard).
                    let resets_in = acc
                        .window
                        .as_ref()
                        .map(|w| w.resets_at_secs.saturating_sub(now_secs()))
                        .unwrap_or(u64::MAX);
                    let threshold = std::env::var("GT_QUOTA_THRESHOLD_SECS")
                        .ok()
                        .and_then(|v| v.parse::<u64>().ok())
                        .unwrap_or(300);
                    if resets_in <= threshold {
                        eprintln!(
                            "[quota-rotation] {account} at {pct:.0}% but window resets in {resets_in}s (≤ threshold {threshold}s) — skip drain",
                        );
                    } else {
                        eprintln!(
                            "[quota-rotation] {account} probed at {pct:.0}% (soft {:.0}%) — draining",
                            soft_pct()
                        );
                        // Proactive soft-drain: move the pointer for NEW slings, record
                        // `quota.soft_drain.v1`, and (unlike a reactive rotation) leave in-flight
                        // polecats on `account` to drain — no credential hot-swap (gtcore-df3319).
                        self.soft_drain_away_from(&account).await?;
                    }
                }
                Ok(())
            }
            _ => Ok(()),
        }
    }
}

/// A per-response token usage sample (the consumption-rate input). Mirrors the provider's `usage`
/// block; the feed folds it into the account's EWMA so the predictor sees a live burn rate.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct TokenSample {
    #[serde(default)]
    pub model: String,
    #[serde(default)]
    pub input: u64,
    #[serde(default)]
    pub output: u64,
    #[serde(default)]
    pub cache_read: u64,
    #[serde(default)]
    pub cache_creation: u64,
}

/// One quota-feed message: the authoritative window (provider ratelimit headers) and/or a
/// per-response token sample, for one account. Either half may be absent — a probe-only message
/// reconciles the window, a sample-only message grows the rate.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct QuotaFeedPayload {
    /// Provider/keychain account correlative the figures belong to.
    pub account: String,
    /// Session the sample is attributable to (per-session breakdown). Defaults to `account`.
    #[serde(default)]
    pub session: Option<String>,
    /// Raw `anthropic-ratelimit-*` headers as `[name, value]` pairs. When they carry the full
    /// tokens window (`limit` + `remaining` + `reset`), the feed seeds/refreshes the account's
    /// live window so the predictor can evaluate it.
    #[serde(default)]
    pub headers: Vec<(String, String)>,
    /// A per-response token sample (drives the consumption-rate EWMA).
    #[serde(default)]
    pub sample: Option<TokenSample>,
}

/// Run the quota-feed recv loop **inline** (awaitable), so it is usable under
/// `gt_polecat::supervise_daemon` (restart + backoff), exactly like the dispatch + refinery loops.
/// Returns `Ok(())` when the channel closes; `Err` only if the initial subscription fails.
///
/// At-least-once: the `ack` happens **after** the figures reach the actor. A malformed message is
/// acked silently (no infinite redelivery / poison loop), mirroring `gt_scheduling::dispatch::run`.
pub async fn run(channel: Channel, quota: QuotaHandle) -> Result<(), gt_channel::ChannelError> {
    let mut rx = channel.subscribe(64)?;
    while let Some(msg) = rx.recv().await {
        match serde_json::from_slice::<QuotaFeedPayload>(&msg.payload) {
            Ok(p) => {
                apply_feed(&quota, p).await;
                let _ = channel.ack(&msg);
            }
            Err(_) => {
                let _ = channel.ack(&msg);
            }
        }
    }
    Ok(())
}

/// Fold one feed message into the quota actor: seed/refresh the window from the headers, then the
/// authoritative probe reconciliation, then the consumption sample. Each step is a no-op when its
/// half of the payload is absent or incomplete.
async fn apply_feed(quota: &QuotaHandle, p: QuotaFeedPayload) {
    let now = now_secs();
    // 1) Window from the headers. When the headers carry a full tokens window we seed/refresh it
    //    via `upsert_account` (preserving `started_at` across refreshes), then the authoritative
    //    `apply_probe` that follows will reconcile consumed and — if remaining > 0 — lift any
    //    Cooldown/Limited/Blocked status back to Healthy.
    if !p.headers.is_empty() {
        let snap = RatelimitHeaders::from_headers(&p.headers);
        if let (Some(limit), Some(remaining), Some(resets_at)) = (
            snap.tokens_limit,
            snap.tokens_remaining,
            snap.tokens_reset_secs,
        ) {
            let existing = quota
                .accounts()
                .await
                .into_iter()
                .find(|a| a.id == p.account);
            let (status, started_at) = match &existing {
                Some(a) => (
                    a.status,
                    a.window.as_ref().map(|w| w.started_at_secs).unwrap_or(now),
                ),
                None => (AccountQuotaStatus::Healthy, now),
            };
            quota
                .upsert_account(Account {
                    id: p.account.clone(),
                    status,
                    window: Some(AccountWindow {
                        kind: WindowKind::Rolling5h,
                        limit,
                        started_at_secs: started_at,
                        resets_at_secs: resets_at,
                        consumed: limit.saturating_sub(remaining) as f64,
                    }),
                    weekly_window: None,
                    last_probe_secs: None,
                    sampled_since_probe: 0.0,
                    probe_divergence: None,
                })
                .await;
        }
        // Authoritative reconciliation (also catches a window already seeded earlier).
        if let Some(pw) = parse_anthropic_ratelimit(&p.headers, &p.account, now) {
            quota
                .probe(
                    pw.account,
                    pw.remaining,
                    pw.resets_at_secs,
                    pw.weekly_remaining,
                    pw.weekly_resets_at_secs,
                    pw.now_secs,
                )
                .await;
        }
    }

    // 1b) Synthetic window when the headers never arrive (hq-agent-provisioning.8). A Claude Code
    //     hook reports token samples but NOT the `anthropic-ratelimit-*` window, and the predictor
    //     needs a window (limit + reset) to project a block — a sample against a windowless account
    //     grows nothing (`rate = consumed/elapsed` reads the window). When a sample arrives for an
    //     account with no window, seed a Rolling5h window from the configured plan limit so the
    //     sample-driven `consumed` + rate can fire a `BlockPredicted`. Idempotent: a real
    //     header-seeded window (above) is left untouched; a stale synthetic window past its 5h reset
    //     is recycled here because nothing else resets a header-less window.
    if p.sample.is_some() {
        let existing = quota
            .accounts()
            .await
            .into_iter()
            .find(|a| a.id == p.account);
        match existing.and_then(|a| a.window) {
            None => {
                quota
                    .upsert_account(Account {
                        id: p.account.clone(),
                        status: AccountQuotaStatus::Healthy,
                        window: Some(synthetic_window(now)),
                        weekly_window: None,
                        last_probe_secs: None,
                        sampled_since_probe: 0.0,
                        probe_divergence: None,
                    })
                    .await;
            }
            Some(w) if now >= w.resets_at_secs => {
                quota
                    .reset_window(p.account.clone(), now, now + ROLLING_5H_SECS)
                    .await;
            }
            Some(_) => {}
        }
    }

    // 2) Consumption sample → the per-account + per-session rate EWMA the predictor reads.
    if let Some(s) = p.sample {
        let session = p.session.unwrap_or_else(|| p.account.clone());
        quota
            .sample(
                p.account,
                session,
                s.model,
                s.input,
                s.output,
                s.cache_read,
                s.cache_creation,
                now,
            )
            .await;
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use gt_events::{Envelope, EventKind};
    use gt_quota::{CredentialRecord, InMemoryKeychain};
    use tokio::sync::mpsc;

    fn record<E: EventKind + serde::Serialize>(event: E) -> EventRecord {
        EventRecord::from_envelope(&Envelope::root(event)).expect("encode")
    }

    /// Spawn a quota actor seeded with two healthy accounts (`a` active, `b` standby).
    async fn quota_with_two() -> (QuotaHandle, mpsc::Receiver<Envelope<QuotaEvent>>) {
        let (tx, rx) = mpsc::channel(64);
        let q = gt_quota::actor::spawn(tx, std::collections::HashMap::new());
        q.upsert_account(Account::new("a")).await;
        q.upsert_account(Account::new("b")).await;
        (q, rx)
    }

    #[tokio::test]
    async fn block_predicted_rotates_active_to_a_healthy_standby() {
        let (quota, _rx) = quota_with_two().await;
        let keychain = Arc::new(InMemoryKeychain::seeded([("a", "/cfg/a"), ("b", "/cfg/b")]));
        keychain.set_active("a").unwrap();

        let plugin = QuotaRotationPlugin::new(quota.clone(), keychain.clone());
        plugin
            .on_event(&record(QuotaEvent::BlockPredicted {
                account: "a".into(),
                eta_to_block_secs: 120,
                consumed: 900,
                limit: 1000,
                rate_per_min: 50.0,
                now_secs: 1000,
            }))
            .await
            .unwrap();

        assert_eq!(
            keychain.active().unwrap().as_deref(),
            Some("b"),
            "the live credential pointer flipped to the healthy standby"
        );
    }

    #[tokio::test]
    async fn rotated_event_couples_pointer_and_hot_swaps_n_in_flight_polecats() {
        // gtcore-bf4acd / BUG2 + T2: a recorded rotation (e.g. a MANUAL quota.rotate) must flip the
        // live pointer AND propagate the new account's creds to EVERY in-flight polecat still on the
        // old account — N live polecats ⇒ N config dirs updated, without a restart.
        use gt_polecat::{FakeTmux, PolecatSupervisor, RestartConfig, SpawnSpec, Tmux};
        use std::path::PathBuf;

        let uniq = format!(
            "{}-{}",
            std::process::id(),
            SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_nanos()
        );
        // Source account `b` carries the fresh credential file the swap copies out.
        let b_dir = std::env::temp_dir().join(format!("gt-rot-b-{uniq}"));
        std::fs::create_dir_all(&b_dir).unwrap();
        let fresh = r#"{"claudeAiOauth":{"accessToken":"at-NEW","refreshToken":"rt-NEW"}}"#;
        std::fs::write(b_dir.join(".credentials.json"), fresh).unwrap();

        // Three in-flight polecats backed by account `a`, each with its own CLAUDE_CONFIG_DIR.
        let tmux: Arc<dyn Tmux> = Arc::new(FakeTmux::new());
        let sup = Arc::new(PolecatSupervisor::new(tmux, RestartConfig::default(), 8));
        let mut in_flight_dirs = Vec::new();
        for i in 0..3 {
            let dir = std::env::temp_dir().join(format!("gt-rot-pol{i}-{uniq}"));
            std::fs::create_dir_all(&dir).unwrap();
            std::fs::write(dir.join(".credentials.json"), "stale").unwrap();
            let session = format!("hq-pol{i}-{uniq}");
            sup.watch(SpawnSpec {
                session: session.clone(),
                rig: "hq".into(),
                polecat: session,
                crew: None,
                workdir: PathBuf::from("/tmp"),
                command: "true".into(),
                args: vec![],
                env: vec![
                    (gt_polecat::GT_HOOK_ACCOUNT.to_string(), "a".to_string()),
                    ("CLAUDE_CONFIG_DIR".to_string(), dir.to_string_lossy().to_string()),
                ],
                hook_bead: None,
                issue: None,
                heartbeat: std::env::temp_dir().join(format!("gt-rot-pol{i}-{uniq}.hb")),
            });
            in_flight_dirs.push(dir);
        }

        let (quota, _rx) = quota_with_two().await;
        let keychain = Arc::new(InMemoryKeychain::seeded([
            ("a", "/cfg/a".to_string()),
            ("b", b_dir.to_string_lossy().to_string()),
        ]));
        keychain.set_active("a").unwrap();
        let plugin = QuotaRotationPlugin::new(quota, keychain.clone()).with_supervisor(sup);

        plugin
            .on_event(&record(QuotaEvent::Rotated {
                from_account: "a".into(),
                to_account: "b".into(),
                now_secs: 1000,
            }))
            .await
            .unwrap();

        // Pointer coupled to the rotation target.
        assert_eq!(keychain.active().unwrap().as_deref(), Some("b"));
        // Every in-flight polecat's credentials now hold the NEW token.
        for dir in &in_flight_dirs {
            let got = std::fs::read_to_string(dir.join(".credentials.json")).unwrap();
            assert_eq!(got, fresh, "in-flight dir {} should hold the rotated creds", dir.display());
        }

        let _ = std::fs::remove_dir_all(&b_dir);
        for dir in &in_flight_dirs {
            let _ = std::fs::remove_dir_all(dir);
        }
    }

    #[tokio::test]
    async fn account_limited_also_rotates() {
        let (quota, _rx) = quota_with_two().await;
        let keychain = Arc::new(InMemoryKeychain::seeded([("a", "/cfg/a"), ("b", "/cfg/b")]));
        keychain.set_active("a").unwrap();
        let plugin = QuotaRotationPlugin::new(quota, keychain.clone());

        plugin
            .on_event(&record(QuotaEvent::AccountLimited {
                account: "a".into(),
                now_secs: 1000,
            }))
            .await
            .unwrap();

        assert_eq!(keychain.active().unwrap().as_deref(), Some("b"));
    }

    #[tokio::test]
    async fn already_rotated_off_is_idempotent() {
        // Active is already `b`; a stale prediction for `a` must not flip anything back.
        let (quota, _rx) = quota_with_two().await;
        let keychain = Arc::new(InMemoryKeychain::seeded([("a", "/cfg/a"), ("b", "/cfg/b")]));
        keychain.set_active("b").unwrap();
        let plugin = QuotaRotationPlugin::new(quota, keychain.clone());

        plugin
            .on_event(&record(QuotaEvent::BlockPredicted {
                account: "a".into(),
                eta_to_block_secs: 120,
                consumed: 900,
                limit: 1000,
                rate_per_min: 50.0,
                now_secs: 1000,
            }))
            .await
            .unwrap();

        assert_eq!(
            keychain.active().unwrap().as_deref(),
            Some("b"),
            "no thrash"
        );
    }

    #[tokio::test]
    async fn no_healthy_target_stays_put() {
        // Only one account: nothing to rotate to.
        let (tx, _rx) = mpsc::channel(64);
        let quota = gt_quota::actor::spawn(tx, std::collections::HashMap::new());
        quota.upsert_account(Account::new("a")).await;
        let keychain = Arc::new(InMemoryKeychain::seeded([("a", "/cfg/a")]));
        keychain.set_active("a").unwrap();
        let plugin = QuotaRotationPlugin::new(quota, keychain.clone());

        plugin
            .on_event(&record(QuotaEvent::AccountLimited {
                account: "a".into(),
                now_secs: 1000,
            }))
            .await
            .unwrap();

        assert_eq!(
            keychain.active().unwrap().as_deref(),
            Some("a"),
            "no alternative ⇒ pointer untouched"
        );
    }

    #[tokio::test]
    async fn full_chain_tick_predicts_then_rotation_plugin_flips_the_pointer() {
        // The acceptance loop end-to-end with synthetic figures (no real claude): seed `a` with a
        // near-exhausted window + a burn rate, `tick` projects the block (`quota.block_predicted.v1`),
        // and feeding that event to the plugin flips the active account to the healthy standby `b`.
        let (tx, mut rx) = mpsc::channel::<Envelope<QuotaEvent>>(64);
        let quota = gt_quota::actor::spawn(tx, std::collections::HashMap::new());
        quota.upsert_account(Account::new("b")).await; // healthy standby
        quota
            .upsert_account(Account {
                id: "a".into(),
                status: AccountQuotaStatus::Healthy,
                window: Some(AccountWindow {
                    kind: WindowKind::Rolling5h,
                    limit: 1000,
                    started_at_secs: 1000,
                    resets_at_secs: 50_000, // reset far off → the block falls within the window
                    consumed: 900.0,        // remaining 100
                }),
                weekly_window: None,
                last_probe_secs: None,
                sampled_since_probe: 0.0,
                probe_divergence: None,
            })
            .await;
        // A sample sets the burn-rate EWMA (0 tokens ⇒ consumed unchanged, rate = consumed/elapsed).
        quota.sample("a", "s1", "opus", 0, 0, 0, 0, 1000).await;
        // Predict with a generous threshold so the small ETA crosses it.
        quota.tick(1000, 900).await;
        // Sync barrier: `tick().await` only awaits the SEND; a round-trip snapshot guarantees the
        // actor has processed the sample + tick (and emitted onto the relay) before we drain it.
        let (_, predictions) = quota.snapshot().await;
        assert!(
            predictions >= 1,
            "the actor emitted a BlockPredicted on tick"
        );

        // Drain the relay until the predictive event arrives, then route it to the plugin.
        let keychain = Arc::new(InMemoryKeychain::seeded([("a", "/cfg/a"), ("b", "/cfg/b")]));
        keychain.set_active("a").unwrap();
        let plugin = QuotaRotationPlugin::new(quota.clone(), keychain.clone());
        let mut predicted = false;
        while let Ok(env) = rx.try_recv() {
            let rec = EventRecord::from_envelope(&env).unwrap();
            if rec.kind == "quota.block_predicted.v1" {
                predicted = true;
                plugin.on_event(&rec).await.unwrap();
            }
        }
        assert!(
            predicted,
            "tick projected the block for the near-exhausted account"
        );
        assert_eq!(
            keychain.active().unwrap().as_deref(),
            Some("b"),
            "the predicted block rotated the live pointer to the healthy standby"
        );
    }

    #[tokio::test]
    async fn sample_only_seeds_a_synthetic_window_then_folds_consumption() {
        // hq-agent-provisioning.8: a header-less sample (the only figure a Claude Code hook can
        // report) must still grow the predictor — so apply_feed seeds a synthetic window first.
        let (tx, _rx) = mpsc::channel(64);
        let quota = gt_quota::actor::spawn(tx, std::collections::HashMap::new());
        apply_feed(
            &quota,
            QuotaFeedPayload {
                account: "acc-1".into(),
                session: Some("s1".into()),
                headers: vec![],
                sample: Some(TokenSample {
                    model: "opus".into(),
                    input: 1000,
                    output: 500,
                    cache_read: 200,
                    cache_creation: 100,
                }),
            },
        )
        .await;
        let acc = quota
            .accounts()
            .await
            .into_iter()
            .find(|a| a.id == "acc-1")
            .expect("account seeded");
        let w = acc
            .window
            .expect("synthetic window seeded for a header-less sample");
        assert_eq!(w.kind, WindowKind::Rolling5h);
        assert_eq!(w.limit, plan_limit());
        // Identity weights ⇒ consumed == sum of the sample's tokens (1000+500+200+100).
        assert_eq!(
            w.consumed, 1800.0,
            "the sample folded into the fresh window"
        );
    }

    #[tokio::test]
    async fn stale_synthetic_window_is_recycled_on_the_next_sample() {
        // A header-less window has no provider reset; the edge recycles it once its 5h elapsed.
        let (tx, _rx) = mpsc::channel(64);
        let quota = gt_quota::actor::spawn(tx, std::collections::HashMap::new());
        quota
            .upsert_account(Account {
                id: "acc-1".into(),
                status: AccountQuotaStatus::Healthy,
                window: Some(AccountWindow {
                    kind: WindowKind::Rolling5h,
                    limit: plan_limit(),
                    started_at_secs: 1, // long ago
                    resets_at_secs: 2,  // already past → must recycle
                    consumed: 999_999.0,
                }),
                weekly_window: None,
                last_probe_secs: None,
                sampled_since_probe: 0.0,
                probe_divergence: None,
            })
            .await;
        apply_feed(
            &quota,
            QuotaFeedPayload {
                account: "acc-1".into(),
                session: None,
                headers: vec![],
                sample: Some(TokenSample {
                    model: "opus".into(),
                    input: 10,
                    output: 5,
                    cache_read: 0,
                    cache_creation: 0,
                }),
            },
        )
        .await;
        let w = quota
            .accounts()
            .await
            .into_iter()
            .find(|a| a.id == "acc-1")
            .and_then(|a| a.window)
            .expect("window");
        // The stale consumption was cleared by the recycle, then only the new sample folded in.
        assert_eq!(w.consumed, 15.0, "recycled then folded the fresh sample");
        assert!(
            w.resets_at_secs > now_secs(),
            "reset pushed 5h into the future"
        );
    }

    #[test]
    fn feed_payload_parses_headers_and_sample() {
        let p: QuotaFeedPayload = serde_json::from_str(
            r#"{"account":"acc-1","session":"s1","headers":[["anthropic-ratelimit-tokens-limit","1000000"]],"sample":{"model":"opus","input":100,"output":50}}"#,
        )
        .unwrap();
        assert_eq!(p.account, "acc-1");
        assert_eq!(p.session.as_deref(), Some("s1"));
        assert_eq!(p.headers.len(), 1);
        assert_eq!(p.sample.unwrap().input, 100);
    }

    #[test]
    fn feed_payload_defaults_optional_halves() {
        let p: QuotaFeedPayload = serde_json::from_str(r#"{"account":"acc-1"}"#).unwrap();
        assert!(p.headers.is_empty());
        assert!(p.sample.is_none());
        assert!(p.session.is_none());
    }

    // hq-quota-unblock-probe tests -----------------------------------------------------------

    #[tokio::test(start_paused = true)]
    async fn blocked_with_until_secs_probes_after_retry_after_expiry() {
        // until_secs is in the past → delay = 5s (grace margin only), so advancing 6s is enough.
        let (tx, _rx) = mpsc::channel::<Envelope<QuotaEvent>>(64);
        let quota = gt_quota::actor::spawn(tx, std::collections::HashMap::new());
        quota
            .upsert_account(Account {
                id: "a".into(),
                status: AccountQuotaStatus::Healthy,
                window: Some(AccountWindow {
                    kind: WindowKind::Rolling5h,
                    limit: plan_limit(),
                    started_at_secs: 100,
                    resets_at_secs: 20_000,
                    consumed: plan_limit() as f64,
                }),
                weekly_window: None,
                last_probe_secs: None,
                sampled_since_probe: 0.0,
                probe_divergence: None,
            })
            .await;
        // Simulate the actor receiving a 429 (sets status = Blocked).
        quota.blocked("a", Some(1u64), 100).await;
        let _ = quota.snapshot().await; // sync: ensure actor processed the Blocked msg

        let keychain = Arc::new(InMemoryKeychain::seeded([("a", "/cfg/a")]));
        let plugin = QuotaRotationPlugin::new(quota.clone(), keychain);

        // Feed the event through the plugin — this schedules the unblock timer.
        plugin
            .on_event(&record(QuotaEvent::Blocked {
                account: "a".into(),
                until_secs: Some(1u64),
                now_secs: 100,
            }))
            .await
            .unwrap();

        // Before the timer fires the account is still Blocked.
        let acc = quota.accounts().await.into_iter().find(|a| a.id == "a").unwrap();
        assert_eq!(acc.status, AccountQuotaStatus::Blocked, "still blocked before timer");

        // Advance tokio time past the 5-second grace delay and let the spawned task run.
        tokio::time::advance(tokio::time::Duration::from_secs(6)).await;
        let _ = quota.snapshot().await; // flush: spawned task's accounts() query
        let _ = quota.snapshot().await; // flush: probe message reaches actor

        let acc = quota.accounts().await.into_iter().find(|a| a.id == "a").unwrap();
        assert_eq!(acc.status, AccountQuotaStatus::Healthy, "probe lifted Blocked → Healthy");
    }

    #[tokio::test]
    async fn blocked_without_until_secs_does_not_schedule_unblock() {
        // A Blocked event with no until_secs (provider sent no Retry-After) must not schedule
        // a timer, and the account stays Blocked.
        let (tx, _rx) = mpsc::channel::<Envelope<QuotaEvent>>(64);
        let quota = gt_quota::actor::spawn(tx, std::collections::HashMap::new());
        quota.upsert_account(Account::new("a")).await;
        quota.blocked("a", None, 100).await;
        let _ = quota.snapshot().await;

        let keychain = Arc::new(InMemoryKeychain::seeded([("a", "/cfg/a")]));
        let plugin = QuotaRotationPlugin::new(quota.clone(), keychain);

        plugin
            .on_event(&record(QuotaEvent::Blocked {
                account: "a".into(),
                until_secs: None,
                now_secs: 100,
            }))
            .await
            .unwrap();

        let acc = quota.accounts().await.into_iter().find(|a| a.id == "a").unwrap();
        assert_eq!(
            acc.status,
            AccountQuotaStatus::Blocked,
            "no until_secs → no unblock scheduled, stays Blocked"
        );
    }

    #[tokio::test]
    async fn unused_credential_record_import_is_live() {
        // Keep `CredentialRecord` in scope (the bin constructs these to seed the keychain);
        // exercising it here documents the secret == CLAUDE_CONFIG_DIR convention.
        let rec = CredentialRecord {
            account: "a".into(),
            secret: "/cfg/a".into(),
        };
        assert_eq!(rec.secret, "/cfg/a");
    }

    // hq-49198f tests ---------------------------------------------------------------------

    /// A far-future reset instant used in tests that need the window to be live (not expired).
    /// Large enough that `resets_in > GT_QUOTA_THRESHOLD_SECS` is always true at test time.
    const FAR_FUTURE: u64 = 9_000_000_000;

    fn acct_with_util(id: &str, consumed: f64, limit: u64) -> Account {
        let mut a = Account::new(id);
        a.window = Some(AccountWindow {
            kind: WindowKind::Rolling5h,
            limit,
            started_at_secs: 0,
            resets_at_secs: FAR_FUTURE,
            consumed,
        });
        a
    }

    #[test]
    fn pick_target_prefers_headroom_and_excludes_hard() {
        // c at 95% ≥ hard(90) is never a target even though Healthy; between a (70%) and
        // b (10%), the most headroom wins.
        let accounts = vec![
            acct_with_util("risk", 99.0, 100),
            acct_with_util("a", 70.0, 100),
            acct_with_util("b", 10.0, 100),
            acct_with_util("c", 95.0, 100),
        ];
        assert_eq!(
            QuotaRotationPlugin::pick_target(&accounts, "risk").as_deref(),
            Some("b")
        );
        // Only hard-exhausted alternatives ⇒ stay put.
        let only_hot = vec![acct_with_util("risk", 99.0, 100), acct_with_util("c", 95.0, 100)];
        assert!(QuotaRotationPlugin::pick_target(&only_hot, "risk").is_none());
    }

    #[test]
    fn pick_target_excludes_weekly_exhausted_account() {
        // hq-34a2f5: a fresh 5h window must not make an account whose WEEKLY budget is at
        // 95% the rotation target — utilization gates on the worse of the two windows.
        let mut weekly_hot = acct_with_util("a", 10.0, 100); // 5h at 10%
        weekly_hot.weekly_window = Some(AccountWindow {
            kind: WindowKind::Weekly,
            limit: 100,
            started_at_secs: 0,
            resets_at_secs: FAR_FUTURE,
            consumed: 95.0, // weekly at 95% ≥ hard(90)
        });
        let accounts = vec![
            acct_with_util("risk", 99.0, 100),
            weekly_hot,
            acct_with_util("b", 70.0, 100),
        ];
        assert_eq!(
            QuotaRotationPlugin::pick_target(&accounts, "risk").as_deref(),
            Some("b"),
            "weekly-exhausted 'a' must lose to 'b' despite the fresher 5h window"
        );
    }

    #[tokio::test]
    async fn blocked_event_rotates_immediately() {
        // The proxy's `rejected` verdict lands as quota.blocked.v1 — the pointer must move
        // in the SAME event, not wait for a prediction (re-slings burned restarts before).
        let (quota, _rx) = quota_with_two().await;
        let keychain = Arc::new(InMemoryKeychain::seeded([("a", "/cfg/a"), ("b", "/cfg/b")]));
        keychain.set_active("a").unwrap();
        let plugin = QuotaRotationPlugin::new(quota, keychain.clone());

        plugin
            .on_event(&record(QuotaEvent::Blocked {
                account: "a".into(),
                until_secs: None,
                now_secs: 1000,
            }))
            .await
            .unwrap();

        assert_eq!(keychain.active().unwrap().as_deref(), Some("b"));
    }

    #[tokio::test]
    async fn probe_at_soft_threshold_drains_the_active_account() {
        let (tx, _rx) = mpsc::channel(64);
        let quota = gt_quota::actor::spawn(tx, std::collections::HashMap::new());
        quota.upsert_account(acct_with_util("a", 85.0, 100)).await; // 85% ≥ soft(80)
        quota.upsert_account(acct_with_util("b", 10.0, 100)).await;
        let keychain = Arc::new(InMemoryKeychain::seeded([("a", "/cfg/a"), ("b", "/cfg/b")]));
        keychain.set_active("a").unwrap();
        let plugin = QuotaRotationPlugin::new(quota, keychain.clone());

        plugin
            .on_event(&record(QuotaEvent::UsageProbed {
                account: "a".into(),
                remaining: 15,
                // FAR_FUTURE: window is not near reset, so the drain must fire.
                resets_at_secs: FAR_FUTURE,
                weekly_remaining: None,
                weekly_resets_at_secs: None,
                now_secs: 1_000,
            }))
            .await
            .unwrap();

        assert_eq!(
            keychain.active().unwrap().as_deref(),
            Some("b"),
            "soft threshold drains: pointer moved, in-flight work finishes on a"
        );
    }

    #[tokio::test]
    async fn probe_at_soft_threshold_skips_drain_when_window_resets_soon() {
        // hq-49198f-drain-guard: an account above the soft threshold must NOT be drained when
        // its window resets within the prediction threshold — it will recover on its own, and
        // draining now only creates unnecessary Cooldown → Healthy churn.
        let (tx, _rx) = mpsc::channel(64);
        let quota = gt_quota::actor::spawn(tx, std::collections::HashMap::new());
        quota.upsert_account(acct_with_util("a", 85.0, 100)).await; // 85% ≥ soft(80)
        quota.upsert_account(acct_with_util("b", 10.0, 100)).await;
        let keychain = Arc::new(InMemoryKeychain::seeded([("a", "/cfg/a"), ("b", "/cfg/b")]));
        keychain.set_active("a").unwrap();
        let plugin = QuotaRotationPlugin::new(quota.clone(), keychain.clone());

        // Probe with a reset 60 seconds away: well within the 300s threshold.
        // Send the probe to the actor first so its in-memory state reflects the near reset,
        // matching the production sequence (actor processes probe → emits event → plugin fires).
        let near_reset = now_secs() + 60;
        let now = now_secs();
        quota.probe("a", 15, near_reset, None, None, now).await;
        let _ = quota.snapshot().await; // sync barrier: actor has processed the probe

        plugin
            .on_event(&record(QuotaEvent::UsageProbed {
                account: "a".into(),
                remaining: 15,
                resets_at_secs: near_reset,
                weekly_remaining: None,
                weekly_resets_at_secs: None,
                now_secs: now,
            }))
            .await
            .unwrap();

        assert_eq!(
            keychain.active().unwrap().as_deref(),
            Some("a"),
            "drain skipped: window resets in 60s (≤ threshold 300s), no unnecessary Cooldown"
        );
    }

    #[tokio::test]
    async fn probe_below_soft_keeps_the_active_account() {
        let (tx, _rx) = mpsc::channel(64);
        let quota = gt_quota::actor::spawn(tx, std::collections::HashMap::new());
        quota.upsert_account(acct_with_util("a", 50.0, 100)).await; // 50% < soft
        quota.upsert_account(acct_with_util("b", 10.0, 100)).await;
        let keychain = Arc::new(InMemoryKeychain::seeded([("a", "/cfg/a"), ("b", "/cfg/b")]));
        keychain.set_active("a").unwrap();
        let plugin = QuotaRotationPlugin::new(quota, keychain.clone());

        plugin
            .on_event(&record(QuotaEvent::UsageProbed {
                account: "a".into(),
                remaining: 50,
                resets_at_secs: FAR_FUTURE,
                weekly_remaining: None,
                weekly_resets_at_secs: None,
                now_secs: 1_000,
            }))
            .await
            .unwrap();

        assert_eq!(keychain.active().unwrap().as_deref(), Some("a"), "no drain below soft");
    }

    // gtcore-df3319: proactive soft-drain at 80% --------------------------------------------

    #[tokio::test]
    async fn probe_at_exactly_soft_threshold_rotates() {
        // Acceptance: a probe at exactly 80% (== soft_pct) drains — the gate is `>=`, so the
        // boundary itself triggers rotation.
        let (tx, _rx) = mpsc::channel(64);
        let quota = gt_quota::actor::spawn(tx, std::collections::HashMap::new());
        quota.upsert_account(acct_with_util("a", 80.0, 100)).await; // exactly soft(80)
        quota.upsert_account(acct_with_util("b", 10.0, 100)).await;
        let keychain = Arc::new(InMemoryKeychain::seeded([("a", "/cfg/a"), ("b", "/cfg/b")]));
        keychain.set_active("a").unwrap();
        let plugin = QuotaRotationPlugin::new(quota, keychain.clone());

        plugin
            .on_event(&record(QuotaEvent::UsageProbed {
                account: "a".into(),
                remaining: 20,
                resets_at_secs: FAR_FUTURE,
                weekly_remaining: None,
                weekly_resets_at_secs: None,
                now_secs: 1_000,
            }))
            .await
            .unwrap();

        assert_eq!(
            keychain.active().unwrap().as_deref(),
            Some("b"),
            "80% (== soft) drains: pointer moved to the healthy standby"
        );
    }

    #[tokio::test]
    async fn probe_just_below_soft_does_not_rotate() {
        // Acceptance: a probe at 79% (< soft_pct) leaves the pointer put.
        let (tx, _rx) = mpsc::channel(64);
        let quota = gt_quota::actor::spawn(tx, std::collections::HashMap::new());
        quota.upsert_account(acct_with_util("a", 79.0, 100)).await; // just below soft(80)
        quota.upsert_account(acct_with_util("b", 10.0, 100)).await;
        let keychain = Arc::new(InMemoryKeychain::seeded([("a", "/cfg/a"), ("b", "/cfg/b")]));
        keychain.set_active("a").unwrap();
        let plugin = QuotaRotationPlugin::new(quota, keychain.clone());

        plugin
            .on_event(&record(QuotaEvent::UsageProbed {
                account: "a".into(),
                remaining: 21,
                resets_at_secs: FAR_FUTURE,
                weekly_remaining: None,
                weekly_resets_at_secs: None,
                now_secs: 1_000,
            }))
            .await
            .unwrap();

        assert_eq!(
            keychain.active().unwrap().as_deref(),
            Some("a"),
            "79% (< soft) does not drain"
        );
    }

    #[tokio::test]
    async fn soft_drain_emits_soft_drain_event_naming_source_and_target() {
        // Acceptance: the proactive rotation is recorded as quota.soft_drain.v1 carrying the
        // source + destination accounts (distinct from the reactive quota.rotated.v1).
        let (tx, mut rx) = mpsc::channel::<Envelope<QuotaEvent>>(64);
        let quota = gt_quota::actor::spawn(tx, std::collections::HashMap::new());
        quota.upsert_account(acct_with_util("a", 85.0, 100)).await; // ≥ soft(80)
        quota.upsert_account(acct_with_util("b", 10.0, 100)).await;
        let keychain = Arc::new(InMemoryKeychain::seeded([("a", "/cfg/a"), ("b", "/cfg/b")]));
        keychain.set_active("a").unwrap();
        let plugin = QuotaRotationPlugin::new(quota.clone(), keychain.clone());

        plugin
            .on_event(&record(QuotaEvent::UsageProbed {
                account: "a".into(),
                remaining: 15,
                resets_at_secs: FAR_FUTURE,
                weekly_remaining: None,
                weekly_resets_at_secs: None,
                now_secs: 1_000,
            }))
            .await
            .unwrap();
        // Sync barrier: the SoftDrained emit happens inside the actor tick; a snapshot round-trip
        // guarantees the actor processed it (and put it on the relay) before we drain.
        let _ = quota.snapshot().await;

        let mut found = None;
        while let Ok(env) = rx.try_recv() {
            if let QuotaEvent::SoftDrained { from_account, to_account, .. } = env.payload {
                found = Some((from_account, to_account));
            }
        }
        assert_eq!(
            found,
            Some(("a".to_string(), "b".to_string())),
            "quota.soft_drain.v1 recorded the proactive rotation a → b"
        );
    }

    #[tokio::test]
    async fn all_alternatives_at_or_above_hard_alerts_without_rotation() {
        // Acceptance: when every alternative is ≥ hard (90%) there is no healthy target — the
        // pointer stays put and quota.soft_drain_stalled.v1 fires as an alert.
        let (tx, mut rx) = mpsc::channel::<Envelope<QuotaEvent>>(64);
        let quota = gt_quota::actor::spawn(tx, std::collections::HashMap::new());
        quota.upsert_account(acct_with_util("a", 95.0, 100)).await; // active, hot
        quota.upsert_account(acct_with_util("b", 92.0, 100)).await; // ≥ hard(90) → not a target
        let keychain = Arc::new(InMemoryKeychain::seeded([("a", "/cfg/a"), ("b", "/cfg/b")]));
        keychain.set_active("a").unwrap();
        let plugin = QuotaRotationPlugin::new(quota.clone(), keychain.clone());

        plugin
            .on_event(&record(QuotaEvent::UsageProbed {
                account: "a".into(),
                remaining: 5,
                resets_at_secs: FAR_FUTURE,
                weekly_remaining: None,
                weekly_resets_at_secs: None,
                now_secs: 1_000,
            }))
            .await
            .unwrap();
        let _ = quota.snapshot().await; // sync barrier

        assert_eq!(
            keychain.active().unwrap().as_deref(),
            Some("a"),
            "no healthy target ⇒ pointer untouched"
        );
        let mut alerted = false;
        while let Ok(env) = rx.try_recv() {
            if let QuotaEvent::SoftDrainStalled { account, .. } = env.payload {
                assert_eq!(account, "a");
                alerted = true;
            }
        }
        assert!(alerted, "quota.soft_drain_stalled.v1 alert fired");
    }

    // gtcore-6f449f: pause-on-exhaustion ----------------------------------------------------

    use gt_polecat::{
        spawn_tmux, FakeTmux, PolecatSupervisor, RestartConfig, SpawnSpec, GT_HOOK_ACCOUNT,
    };

    /// A supervised, alive polecat backed by `account`, ready for the plugin to pause. Returns the
    /// supervisor (sharing `tmux`) so the test can assert pause/resume against the same fake.
    fn supervisor_with_polecat(
        tmux: Arc<FakeTmux>,
        session: &str,
        account: &str,
    ) -> Arc<PolecatSupervisor> {
        let sup = Arc::new(PolecatSupervisor::new(
            tmux.clone(),
            RestartConfig::default(),
            u32::MAX,
        ));
        let spec = SpawnSpec {
            session: session.to_string(),
            rig: "granite".to_string(),
            polecat: session.to_string(),
            crew: None,
            workdir: std::env::temp_dir(),
            command: "sleep".to_string(),
            args: vec!["30".to_string()],
            env: vec![(GT_HOOK_ACCOUNT.to_string(), account.to_string())],
            hook_bead: Some(format!("bead-{session}")),
            issue: None,
            heartbeat: std::env::temp_dir().join(format!("{session}.hb")),
        };
        spawn_tmux(tmux.as_ref(), &spec).unwrap();
        sup.watch(spec);
        sup
    }

    /// Seed quota with `a` (active, at-risk) and a Blocked `b`, so a rotation away from `a` finds no
    /// healthy target. Returns the handle + relay receiver.
    async fn quota_a_active_b_blocked() -> (QuotaHandle, mpsc::Receiver<Envelope<QuotaEvent>>) {
        let (tx, rx) = mpsc::channel(64);
        let q = gt_quota::actor::spawn(tx, std::collections::HashMap::new());
        q.upsert_account(Account::new("a")).await;
        q.upsert_account(Account::new("b")).await;
        q.blocked("b", Some(10_000), 1_000).await; // b is unavailable → no rotation target
        let _ = q.snapshot().await; // sync barrier
        (q, rx)
    }

    #[tokio::test]
    async fn all_exhausted_pauses_in_flight_polecats_and_emits_event() {
        // Acceptance: rotate_away_from finds no healthy account → the at-risk account's in-flight
        // polecats are SIGSTOP'd and quota.all_exhausted.v1 carries the paused session list.
        let (quota, mut rx) = quota_a_active_b_blocked().await;
        let keychain = Arc::new(InMemoryKeychain::seeded([("a", "/cfg/a"), ("b", "/cfg/b")]));
        keychain.set_active("a").unwrap();
        let tmux = Arc::new(FakeTmux::new());
        let sup = supervisor_with_polecat(tmux.clone(), "sess-a", "a");
        let plugin = QuotaRotationPlugin::new(quota.clone(), keychain.clone())
            .with_supervisor(sup.clone());

        // The provider limited `a`; with `b` blocked there is nowhere to rotate.
        plugin
            .on_event(&record(QuotaEvent::AccountLimited {
                account: "a".into(),
                now_secs: 2_000,
            }))
            .await
            .unwrap();

        assert!(tmux.is_paused("sess-a"), "the in-flight polecat was suspended in place");
        assert_eq!(sup.paused_sessions(), vec!["sess-a"]);
        // The pointer never moved (no target).
        assert_eq!(keychain.active().unwrap().as_deref(), Some("a"));

        let _ = quota.snapshot().await; // sync barrier for the emit
        let mut found = None;
        while let Ok(env) = rx.try_recv() {
            if let QuotaEvent::AllExhausted { account, paused_sessions, .. } = env.payload {
                found = Some((account, paused_sessions));
            }
        }
        assert_eq!(
            found,
            Some(("a".to_string(), vec!["sess-a".to_string()])),
            "quota.all_exhausted.v1 named the account and the paused session"
        );
    }

    #[tokio::test]
    async fn exhaustion_is_idempotent_no_double_pause_or_event() {
        // A second BlockPredicted for the same already-exhausted account must not re-pause or
        // re-emit (no thrash).
        let (quota, mut rx) = quota_a_active_b_blocked().await;
        let keychain = Arc::new(InMemoryKeychain::seeded([("a", "/cfg/a"), ("b", "/cfg/b")]));
        keychain.set_active("a").unwrap();
        let tmux = Arc::new(FakeTmux::new());
        let sup = supervisor_with_polecat(tmux.clone(), "sess-a", "a");
        let plugin = QuotaRotationPlugin::new(quota.clone(), keychain.clone())
            .with_supervisor(sup.clone());

        let limited = || {
            record(QuotaEvent::AccountLimited {
                account: "a".into(),
                now_secs: 2_000,
            })
        };
        plugin.on_event(&limited()).await.unwrap();
        plugin.on_event(&limited()).await.unwrap();
        let _ = quota.snapshot().await;

        let exhausted_events = {
            let mut n = 0;
            while let Ok(env) = rx.try_recv() {
                if matches!(env.payload, QuotaEvent::AllExhausted { .. }) {
                    n += 1;
                }
            }
            n
        };
        assert_eq!(exhausted_events, 1, "all_exhausted emitted exactly once");
        assert_eq!(sup.paused_sessions(), vec!["sess-a"]);
    }

    #[tokio::test]
    async fn recovery_probe_resumes_paused_polecats_and_emits_event() {
        // Acceptance end-to-end: all accounts blocked → polecat paused; an account recovers to
        // Healthy → the paused polecat is resumed and quota.account_recovered.v1 fires.
        let (quota, mut rx) = quota_a_active_b_blocked().await;
        let keychain = Arc::new(InMemoryKeychain::seeded([("a", "/cfg/a"), ("b", "/cfg/b")]));
        keychain.set_active("a").unwrap();
        let tmux = Arc::new(FakeTmux::new());
        let sup = supervisor_with_polecat(tmux.clone(), "sess-a", "a");
        let plugin = QuotaRotationPlugin::new(quota.clone(), keychain.clone())
            .with_supervisor(sup.clone());

        // Step 1: exhaustion pauses the polecat.
        plugin
            .on_event(&record(QuotaEvent::AccountLimited {
                account: "a".into(),
                now_secs: 2_000,
            }))
            .await
            .unwrap();
        assert!(tmux.is_paused("sess-a"), "paused after exhaustion");

        // Step 2: the synthetic unblock probe lifts `a` back to Healthy. Send it to the actor
        // first (so its state reflects Healthy), then route the resulting event to the plugin —
        // exactly the production order (actor probe → emit → plugin reacts).
        quota.probe("a", 1_000_000, FAR_FUTURE, None, None, 9_000).await;
        let _ = quota.snapshot().await; // sync: probe applied (a → Healthy)

        plugin
            .on_event(&record(QuotaEvent::UsageProbed {
                account: "a".into(),
                remaining: 1_000_000,
                resets_at_secs: FAR_FUTURE,
                weekly_remaining: None,
                weekly_resets_at_secs: None,
                now_secs: 9_000,
            }))
            .await
            .unwrap();

        assert!(!tmux.is_paused("sess-a"), "the recovered account's polecat was resumed");
        assert!(sup.paused_sessions().is_empty(), "pause set cleared on recovery");

        let _ = quota.snapshot().await; // sync barrier for the emit
        let mut found = None;
        while let Ok(env) = rx.try_recv() {
            if let QuotaEvent::AccountRecovered { account, resumed_sessions, .. } = env.payload {
                found = Some((account, resumed_sessions));
            }
        }
        assert_eq!(
            found,
            Some(("a".to_string(), vec!["sess-a".to_string()])),
            "quota.account_recovered.v1 named the account and the resumed session"
        );
    }

    #[tokio::test]
    async fn probe_for_a_never_paused_account_is_a_noop() {
        // A usage probe for an account we never paused must not emit account_recovered.
        let (quota, mut rx) = quota_with_two().await;
        let keychain = Arc::new(InMemoryKeychain::seeded([("a", "/cfg/a"), ("b", "/cfg/b")]));
        keychain.set_active("a").unwrap();
        let tmux = Arc::new(FakeTmux::new());
        let sup = supervisor_with_polecat(tmux.clone(), "sess-a", "a");
        let plugin = QuotaRotationPlugin::new(quota.clone(), keychain.clone())
            .with_supervisor(sup);

        quota.probe("a", 1_000_000, FAR_FUTURE, None, None, 9_000).await;
        let _ = quota.snapshot().await;
        plugin
            .on_event(&record(QuotaEvent::UsageProbed {
                account: "a".into(),
                remaining: 1_000_000,
                resets_at_secs: FAR_FUTURE,
                weekly_remaining: None,
                weekly_resets_at_secs: None,
                now_secs: 9_000,
            }))
            .await
            .unwrap();
        let _ = quota.snapshot().await;

        let recovered = {
            let mut any = false;
            while let Ok(env) = rx.try_recv() {
                if matches!(env.payload, QuotaEvent::AccountRecovered { .. }) {
                    any = true;
                }
            }
            any
        };
        assert!(!recovered, "no recovery event for an account that was never paused");
        assert!(!tmux.is_paused("sess-a"));
    }
}
