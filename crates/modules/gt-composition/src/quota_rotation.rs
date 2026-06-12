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

use std::sync::Arc;
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
}

impl QuotaRotationPlugin {
    /// Wire the quota command handle (for the account snapshot + the `rotated` record) and the
    /// keychain (whose live pointer the rotation flips).
    pub fn new(quota: QuotaHandle, keychain: Arc<dyn Keychain>) -> Self {
        Self { quota, keychain, supervisor: None }
    }

    /// Wire the polecat supervisor so in-flight session risk is surfaced on rotation
    /// (`hq-quota-refinement.3`).
    pub fn with_supervisor(mut self, supervisor: Arc<PolecatSupervisor>) -> Self {
        self.supervisor = Some(supervisor);
        self
    }

    /// Pick a healthy rotation target `!= at_risk` from the registry snapshot. Deterministic:
    /// `accounts()` preserves insertion order, so the first healthy alternative wins. `None` when
    /// no healthy account other than the at-risk one exists (the caller then stays put).
    fn pick_target(accounts: &[Account], at_risk: &str) -> Option<String> {
        accounts
            .iter()
            .find(|a| a.id != at_risk && a.status == AccountQuotaStatus::Healthy)
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
            eprintln!(
                "[quota-rotation] {at_risk} at risk but no healthy alternative — staying put"
            );
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
        // Signal risk for any polecat that was in-flight on the rotated account
        // (hq-quota-refinement.3). These sessions were slung with the old credentials; they will
        // continue consuming the exhausting account until they finish. The operator must decide
        // whether to let them complete (they may succeed before the limit) or kill and re-sling.
        // New slings after the keychain flip above will pick `target` automatically.
        if let Some(sup) = &self.supervisor {
            let at_risk_sessions = sup.sessions_for_account(at_risk);
            for session in &at_risk_sessions {
                eprintln!(
                    "[quota-rotation] WARN kind=polecat.account_rotated_while_active \
                     account={at_risk} session={session} new_account={target} \
                     — polecat still in-flight on the rotated account; \
                     new slings will use {target}; kill+re-sling to recover immediately"
                );
            }
        }
        Ok(())
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
            // Hard block with a Retry-After timestamp → schedule a probe at until_secs + 5s so
            // the account re-enters rotation as soon as the provider window closes, not on the
            // next periodic poll (which could be minutes later).
            "quota.blocked.v1" => {
                if let QuotaEvent::Blocked { account, until_secs: Some(until), .. } =
                    record.decode::<QuotaEvent>()?
                {
                    schedule_unblock(self.quota.clone(), account, until);
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
}
