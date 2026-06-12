//! Domain state + replay reducer.
//!
//! `AccountRegistry` is the mutable state the actor owns; `QuotaState` is the version
//! rebuilt from the log for the Step 3 gate (deterministic replay).

use std::collections::{BTreeMap, HashMap};

use serde::{Deserialize, Serialize};

use crate::cost::{cost_units, ModelWeights};
use crate::events::QuotaEvent;

/// Rolling-5h window length (standard claude plan), in seconds.
pub const ROLLING_5H_SECS: u64 = 5 * 3600;

/// Default synthetic-window budget, in cost units, used when a `TokensSampled` arrives for an
/// account with no window (the replay reducer has no provider headers + no env, so it cannot read
/// the real limit). Mirrors the edge's `GT_QUOTA_PLAN_LIMIT` default so the REST `Usage` column
/// renders a consumed/limit the same order of magnitude as the daemon's live predictor sees.
/// Identity weights count a turn at ~10^5–10^6 units, so this is deliberately generous.
pub const DEFAULT_PLAN_LIMIT: u64 = 50_000_000;

/// Granularity of the window the provider counts usage over.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum WindowKind {
    /// Rolling 5h window (standard plan).
    Rolling5h,
    /// Weekly quota.
    Weekly,
}

/// Operational state of an account — the signal the rotation chain consumes.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum AccountQuotaStatus {
    Healthy,
    Limited,
    Blocked,
    /// After a rotation: the account is parked until the cooldown ends.
    Cooldown,
}

/// The live window: when it started, when it releases, the budget and the cost consumed so
/// far in cost units (see `cost.rs`).
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct AccountWindow {
    pub kind: WindowKind,
    pub limit: u64,
    pub started_at_secs: u64,
    pub resets_at_secs: u64,
    /// Cost consumed in the window (may carry a fraction from per-model weights; rounded up
    /// when compared against `limit`).
    pub consumed: f64,
}

impl AccountWindow {
    pub fn remaining(&self) -> u64 {
        let used = self.consumed.ceil() as i64;
        (self.limit as i64 - used).max(0) as u64
    }
}

/// An account managed by the domain. Identity is the `id` (the provider / keychain
/// correlative).
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Account {
    pub id: String,
    pub status: AccountQuotaStatus,
    /// Rolling-5h usage window (the primary rate-limit window for all Claude plans).
    pub window: Option<AccountWindow>,
    /// Weekly usage window, present only for Claude Pro plans that surface weekly quota
    /// headers. Tracks a separate budget alongside the rolling-5h window.
    /// Serializes as `null` (not absent) so consumers can distinguish "no weekly quota"
    /// from "field not yet available" (`hq-quota-weekly.5`).
    #[serde(default)]
    pub weekly_window: Option<AccountWindow>,
    /// When the provider last CONFIRMED this account's window (probe instant, epoch secs,
    /// hq-46cdf4). `None` ⇒ everything in `window.consumed` is locally-sampled guesswork —
    /// rotation should treat the window as unverified.
    #[serde(default)]
    pub last_probe_secs: Option<u64>,
    /// Cost units sampled locally SINCE the last probe — the telemetry tail between
    /// reconciliations. Reset to 0 on every probe; never authoritative on its own.
    #[serde(default)]
    pub sampled_since_probe: f64,
    /// Divergence observed at the last probe: locally-accumulated `consumed` minus the
    /// provider-reported one, in cost units (positive = local over-counted). Exposed on
    /// `quota.list`/`quota.info` so counter drift is diagnosable instead of silent.
    #[serde(default)]
    pub probe_divergence: Option<f64>,
}

impl Account {
    pub fn new(id: impl Into<String>) -> Self {
        Self {
            id: id.into(),
            status: AccountQuotaStatus::Healthy,
            window: None,
            weekly_window: None,
            last_probe_secs: None,
            sampled_since_probe: 0.0,
            probe_divergence: None,
        }
    }

    /// Whether this account is **confirmed-blocked right now** — the freshness gate the
    /// context custodian consults before authorizing a deferral (`hq-context-custodian.3`).
    ///
    /// The quota subsystem is not yet fully reliable: a `Blocked` status can be *stale* —
    /// the account's window already lapsed (`now >= resets_at_secs`), so the rate limit has
    /// almost certainly cleared and the account is merely awaiting the re-probe that
    /// [`AccountRegistry::apply_probe`] uses to lift it back to `Healthy`. A stale block
    /// must NOT pass for a real one, or it would hand out a deferral on capacity that
    /// actually recovered.
    ///
    /// Returns `true` only when the status is `Blocked` AND a live window confirms the block
    /// has not yet expired (`now < resets_at_secs`). A non-`Blocked` status, a `Blocked`
    /// whose window already reset (stale), or a `Blocked` with no window at all (nothing to
    /// confirm freshness against) all return `false`.
    pub fn is_genuinely_blocked(&self, now_secs: u64) -> bool {
        if self.status != AccountQuotaStatus::Blocked {
            return false;
        }
        match &self.window {
            Some(w) => now_secs < w.resets_at_secs,
            None => false,
        }
    }
}

/// EWMA (exponential weighted moving average). Reacts to a trend without a single spike
/// driving it. State is `(value, last_now_secs)`; it updates when a new sample arrives.
#[derive(Debug, Clone, Copy, PartialEq, Serialize, Deserialize)]
pub struct Ewma {
    pub value: f64,
    pub last_now_secs: u64,
    /// Decay constant: the new sample's weight in each update (0 < alpha <= 1).
    pub alpha: f64,
}

impl Ewma {
    /// Default alpha for a "rate per minute" signal over a 5h window: enough to react to a
    /// session that starts burning without amplifying single-turn noise.
    pub const DEFAULT_ALPHA: f64 = 0.3;

    pub fn new(alpha: f64) -> Self {
        Self {
            value: 0.0,
            last_now_secs: 0,
            alpha,
        }
    }

    pub fn observe(&mut self, sample: f64, now_secs: u64) {
        // Standard EWMA; the time delta is only used to snap the timestamp. The producer
        // guarantees monotonicity (now never goes backwards within a single run).
        if self.last_now_secs == 0 {
            self.value = sample;
        } else {
            self.value = self.alpha * sample + (1.0 - self.alpha) * self.value;
        }
        self.last_now_secs = now_secs;
    }
}

/// Live account registry (what the actor owns).
#[derive(Debug, Default)]
pub struct AccountRegistry {
    accounts: BTreeMap<String, Account>,
    /// EWMA of the per-account rate-per-minute, in cost units.
    rate: BTreeMap<String, Ewma>,
    /// EWMA of the per-session rate (the sub-account of "which session is burning the
    /// account"). Composite key: `format!("{account}/{session}")` — stable and sortable.
    rate_by_session: BTreeMap<String, Ewma>,
    /// `BlockPredicted` already emitted in the live window (per-window idempotency).
    predicted_in_window: BTreeMap<String, u64>, // account -> window_started_at
    /// Per-model cost weights (empty -> IDENTITY fallback). Owned here so the consumption
    /// math has one home shared by the legacy actor messages and the typed `Command` path.
    weights: HashMap<String, ModelWeights>,
}

impl AccountRegistry {
    /// Install the per-model cost weights (the actor sets these at spawn).
    pub fn set_weights(&mut self, weights: HashMap<String, ModelWeights>) {
        self.weights = weights;
    }

    pub fn upsert_account(&mut self, account: Account) {
        self.accounts.insert(account.id.clone(), account);
    }

    /// Symmetric to [`Self::upsert_account`]: drop an account from the registry. Returns
    /// `true` if an account with `id` existed and was removed, `false` if it was not present.
    /// Like upsert, this is an **edge** mutation — no domain event is produced; predictions and
    /// rate EWMA naturally stop firing for the id because no more samples reach it.
    pub fn remove_account(&mut self, id: &str) -> bool {
        self.accounts.remove(id).is_some()
    }

    pub fn get(&self, id: &str) -> Option<&Account> {
        self.accounts.get(id)
    }

    pub fn get_mut(&mut self, id: &str) -> Option<&mut Account> {
        self.accounts.get_mut(id)
    }

    pub fn accounts(&self) -> impl Iterator<Item = &Account> {
        self.accounts.values()
    }

    pub fn rate(&self, account: &str) -> Option<&Ewma> {
        self.rate.get(account)
    }

    pub fn session_rate(&self, account: &str, session: &str) -> Option<&Ewma> {
        self.rate_by_session.get(&format!("{account}/{session}"))
    }

    pub fn observe_rate(&mut self, account: &str, sample_per_min: f64, now_secs: u64) {
        let e = self
            .rate
            .entry(account.to_string())
            .or_insert_with(|| Ewma::new(Ewma::DEFAULT_ALPHA));
        e.observe(sample_per_min, now_secs);
    }

    pub fn observe_session_rate(
        &mut self,
        account: &str,
        session: &str,
        sample_per_min: f64,
        now_secs: u64,
    ) {
        let key = format!("{account}/{session}");
        let e = self
            .rate_by_session
            .entry(key)
            .or_insert_with(|| Ewma::new(Ewma::DEFAULT_ALPHA));
        e.observe(sample_per_min, now_secs);
    }

    /// `true` if no block has been predicted yet in the account's live window. Closes the
    /// idempotency: the predictor can run after every sample without re-emitting
    /// `BlockPredicted`.
    pub fn mark_predicted(&mut self, account: &str, window_started_at: u64) -> bool {
        match self.predicted_in_window.get(account).copied() {
            Some(prev) if prev == window_started_at => false,
            _ => {
                self.predicted_in_window
                    .insert(account.to_string(), window_started_at);
                true
            }
        }
    }

    /// Call on a window reset: clears the idempotency flag so a new `BlockPredicted` can be
    /// emitted in the next window.
    pub fn clear_window_prediction(&mut self, account: &str) {
        self.predicted_in_window.remove(account);
    }

    /// Fold one local usage sample: add its normalized cost to the live window and update the
    /// per-account and per-session rate EWMAs. The clock enters as `now_secs` data; no wall
    /// clock is read. Shared by `QuotaMsg::Sample` and `SampleTokens` (`commands.rs`) so both
    /// the legacy and the MCP path mutate state identically.
    #[allow(clippy::too_many_arguments)]
    pub fn apply_sample(
        &mut self,
        account: &str,
        session: &str,
        model: &str,
        input: u64,
        output: u64,
        cache_read: u64,
        cache_creation: u64,
        now_secs: u64,
    ) {
        let cost = cost_units(model, input, output, cache_read, cache_creation, &self.weights);
        if let Some(acc) = self.get_mut(account) {
            if let Some(w) = acc.window.as_mut() {
                w.consumed += cost.0;
            }
            // Track the locally-sampled tail since the last provider confirmation
            // (hq-46cdf4): never authoritative, but it shows HOW MUCH of the current
            // `consumed` is guesswork vs probed truth.
            acc.sampled_since_probe += cost.0;
        }
        let rate = match self.get(account).and_then(|a| a.window.as_ref()) {
            Some(w) => {
                // `consumed / elapsed_minutes` (spec). 1-minute floor: a rate-per-minute is
                // meaningless over less than a minute and the floor avoids the blow-up right
                // when the window starts.
                let elapsed_min =
                    ((now_secs.saturating_sub(w.started_at_secs)) as f64 / 60.0).max(1.0);
                w.consumed / elapsed_min
            }
            None => 0.0,
        };
        self.observe_rate(account, rate, now_secs);
        self.observe_session_rate(account, session, cost.0, now_secs);
    }

    /// Reconcile the live window against the provider's authoritative `remaining`/`resets_at`.
    /// Creates a synthetic Rolling5h window when none exists yet (first probe on a freshly
    /// registered account). Transitions Cooldown/Limited/Blocked → Healthy when `remaining > 0`
    /// (the account recovered capacity). Shared by `QuotaMsg::Probe` and `ProbeWindow`.
    pub fn apply_probe(&mut self, account: &str, remaining: u64, resets_at_secs: u64, now_secs: u64) {
        if let Some(acc) = self.get_mut(account) {
            match acc.window.as_mut() {
                Some(w) => {
                    let probed = (w.limit.saturating_sub(remaining)) as f64;
                    // Provider truth replaces the local accumulation (hq-46cdf4); the
                    // pre-overwrite divergence is kept so drift is diagnosable.
                    acc.probe_divergence = Some(w.consumed - probed);
                    w.consumed = probed;
                    w.resets_at_secs = resets_at_secs;
                }
                None => {
                    // First probe on a registered account: seed a Rolling5h window anchored
                    // conservatively (started 5h before the reset so `consumed/elapsed` stays
                    // sane until real samples arrive).
                    acc.window = Some(AccountWindow {
                        kind: WindowKind::Rolling5h,
                        limit: DEFAULT_PLAN_LIMIT,
                        started_at_secs: resets_at_secs.saturating_sub(ROLLING_5H_SECS),
                        resets_at_secs,
                        consumed: DEFAULT_PLAN_LIMIT.saturating_sub(remaining) as f64,
                    });
                }
            }
            // The provider just confirmed the window: stamp the probe instant and zero the
            // sampled-since tail — from here the local counter is telemetry, not truth.
            acc.last_probe_secs = Some(now_secs);
            acc.sampled_since_probe = 0.0;
            // A probe with remaining > 0 means the account has real capacity: lift any
            // non-Healthy status (Cooldown after rotation, Limited/Blocked after a 429).
            if remaining > 0 {
                acc.status = AccountQuotaStatus::Healthy;
            }
        }
    }

    /// Reconcile the weekly window against the provider's authoritative weekly
    /// `remaining`/`resets_at`. Seeds the window when none exists yet. No-op on unknown account
    /// or when `weekly_remaining`/`weekly_resets_at_secs` are `None` (headers absent).
    /// Shared by the `UsageProbed` actor path and the reducer.
    pub fn apply_weekly_probe(
        &mut self,
        account: &str,
        weekly_remaining: u64,
        weekly_resets_at_secs: u64,
        now_secs: u64,
    ) {
        if let Some(acc) = self.get_mut(account) {
            match acc.weekly_window.as_mut() {
                Some(w) => {
                    w.consumed = (w.limit.saturating_sub(weekly_remaining)) as f64;
                    w.resets_at_secs = weekly_resets_at_secs;
                }
                None => {
                    acc.weekly_window = Some(AccountWindow {
                        kind: WindowKind::Weekly,
                        limit: DEFAULT_PLAN_LIMIT,
                        started_at_secs: now_secs,
                        resets_at_secs: weekly_resets_at_secs,
                        consumed: DEFAULT_PLAN_LIMIT.saturating_sub(weekly_remaining) as f64,
                    });
                }
            }
        }
    }

    /// Park the rotated-off account in `Cooldown`. No-op if unknown. Shared by
    /// `QuotaMsg::Rotated` and `RotateAccount`.
    pub fn apply_rotation(&mut self, from_account: &str) {
        if let Some(a) = self.get_mut(from_account) {
            a.status = AccountQuotaStatus::Cooldown;
        }
    }

    /// The pool-level "no fresh capacity" signal the context custodian consults
    /// (`hq-context-custodian.3` → wired in `.2`): `true` only when the account pool
    /// offers no usable capacity AND that condition is **confirmed fresh**, so a model
    /// genuinely cannot be fed and the claim-context requirement is safe to defer.
    ///
    /// Concretely, `true` requires ALL of:
    /// - the pool is non-empty (no accounts ⇒ nothing to confirm ⇒ demand context),
    /// - no account is `Healthy` (any healthy account means work *can* be fed),
    /// - at least one account is [`genuinely blocked`](Account::is_genuinely_blocked), and
    /// - no account is *stale*-blocked (a `Blocked` whose window already lapsed). A stale
    ///   block is an unconfirmed state — the account likely recovered and is awaiting a
    ///   re-probe — so its mere presence forces the safe default (require context) rather
    ///   than grant a deferral on capacity that may be back. This is the anti-loophole:
    ///   "un Blocked stale nunca otorga Deferred".
    ///
    /// Deliberately strict and clock-fed (`now_secs` enters as data; the domain reads no
    /// clock): the deferral is the exception, so anything short of a freshly-confirmed,
    /// fully-exhausted pool keeps the contract in force.
    pub fn pool_genuinely_blocked(&self, now_secs: u64) -> bool {
        let mut any = false;
        let mut any_healthy = false;
        let mut any_fresh_blocked = false;
        let mut any_stale_blocked = false;
        for acc in self.accounts() {
            any = true;
            match acc.status {
                AccountQuotaStatus::Healthy => any_healthy = true,
                AccountQuotaStatus::Blocked => {
                    if acc.is_genuinely_blocked(now_secs) {
                        any_fresh_blocked = true;
                    } else {
                        any_stale_blocked = true;
                    }
                }
                AccountQuotaStatus::Limited | AccountQuotaStatus::Cooldown => {}
            }
        }
        any && !any_healthy && any_fresh_blocked && !any_stale_blocked
    }

    /// Rebuild a live registry from the replay reducer (boot hydration, hq-8iur.1). The
    /// event log captures status transitions (Limited/Cooldown/Blocked) so they survive
    /// restart; EWMA rates and the live `consumed` re-converge from the next edge probe
    /// (`apply_probe` reconciles `consumed = limit - remaining` from the provider headers).
    /// Window initialization arrives outside the event log via `QuotaMsg::UpsertAccount` —
    /// the edge replays it on next probe.
    pub fn from_state(state: &QuotaState) -> Self {
        let mut registry = AccountRegistry::default();
        for (id, account) in &state.accounts {
            registry.accounts.insert(id.clone(), account.clone());
        }
        registry
    }
}

/// Pure reducer: rebuilds the consolidated state from the log. Used as the Step 3 gate
/// (`docs/06-observability.md`): the live state must match the rebuilt one byte-for-byte.
#[derive(Debug, Default, PartialEq)]
pub struct QuotaState {
    pub accounts: BTreeMap<String, Account>,
    /// Sorted `(account, eta_to_block_secs, now_secs)` list of the predictions emitted — the
    /// predictive-rotation decision chain is the observable output.
    pub predictions: Vec<(String, u64, u64)>,
    pub rotations: Vec<(String, String)>, // (from, to)
    pub limited: Vec<String>,
    pub blocked: Vec<String>,
    /// Onboarded claude accounts → their `config_dir` (`CLAUDE_CONFIG_DIR`), rebuilt from
    /// `AccountRegistered`/`AccountDeregistered` (`hq-quota-accounts.1`). The daemon replays this to
    /// seed its credential keychain (`.2`), so it does not depend on the `GT_CLAUDE_ACCOUNTS` env.
    pub registered: BTreeMap<String, String>,
}

impl QuotaState {
    pub fn apply(&mut self, event: &QuotaEvent) {
        match event {
            QuotaEvent::TokensSampled {
                account,
                model,
                input,
                output,
                cache_read,
                cache_creation,
                now_secs,
                ..
            } => {
                // Fold the sample's cost into the account's window so the rebuilt state (hence the
                // REST `Usage`/`Window`/`Resets` columns via `AccountRegistry::from_state`) renders
                // real consumption (hq-quota-feed-display). Mirrors the edge `apply_feed`: seed a
                // synthetic Rolling5h window on the first sample (no provider headers in replay),
                // recycle it once its 5h reset has passed, then add the IDENTITY-weighted cost (the
                // reducer carries no per-model calibration, same default the daemon uses uncalibrated).
                let entry = self
                    .accounts
                    .entry(account.clone())
                    .or_insert_with(|| Account::new(account.clone()));
                if entry.window.is_none() {
                    entry.window = Some(AccountWindow {
                        kind: WindowKind::Rolling5h,
                        limit: DEFAULT_PLAN_LIMIT,
                        started_at_secs: *now_secs,
                        resets_at_secs: now_secs + ROLLING_5H_SECS,
                        consumed: 0.0,
                    });
                }
                if let Some(w) = entry.window.as_mut() {
                    if *now_secs >= w.resets_at_secs {
                        w.started_at_secs = *now_secs;
                        w.resets_at_secs = now_secs + ROLLING_5H_SECS;
                        w.consumed = 0.0;
                    }
                    let cost = cost_units(
                        model,
                        *input,
                        *output,
                        *cache_read,
                        *cache_creation,
                        &HashMap::new(),
                    );
                    w.consumed += cost.0;
                    // Same sampled-since-probe tail the live registry tracks (hq-46cdf4) —
                    // replay must rebuild it byte-for-byte.
                    entry.sampled_since_probe += cost.0;
                }
            }
            QuotaEvent::UsageProbed {
                account,
                remaining,
                resets_at_secs,
                weekly_remaining,
                weekly_resets_at_secs,
                now_secs,
            } => {
                let entry = self
                    .accounts
                    .entry(account.clone())
                    .or_insert_with(|| Account::new(account.clone()));
                match entry.window.as_mut() {
                    Some(w) => {
                        let probed = (w.limit.saturating_sub(*remaining)) as f64;
                        entry.probe_divergence = Some(w.consumed - probed);
                        w.consumed = probed;
                        w.resets_at_secs = *resets_at_secs;
                    }
                    None => {
                        entry.window = Some(AccountWindow {
                            kind: WindowKind::Rolling5h,
                            limit: DEFAULT_PLAN_LIMIT,
                            started_at_secs: resets_at_secs.saturating_sub(ROLLING_5H_SECS),
                            resets_at_secs: *resets_at_secs,
                            consumed: DEFAULT_PLAN_LIMIT.saturating_sub(*remaining) as f64,
                        });
                    }
                }
                if let (Some(w_rem), Some(w_reset)) =
                    (weekly_remaining, weekly_resets_at_secs)
                {
                    match entry.weekly_window.as_mut() {
                        Some(w) => {
                            w.consumed = (w.limit.saturating_sub(*w_rem)) as f64;
                            w.resets_at_secs = *w_reset;
                        }
                        None => {
                            entry.weekly_window = Some(AccountWindow {
                                kind: WindowKind::Weekly,
                                limit: DEFAULT_PLAN_LIMIT,
                                started_at_secs: *now_secs,
                                resets_at_secs: *w_reset,
                                consumed: DEFAULT_PLAN_LIMIT.saturating_sub(*w_rem) as f64,
                            });
                        }
                    }
                }
                entry.last_probe_secs = Some(*now_secs);
                entry.sampled_since_probe = 0.0;
                if *remaining > 0 {
                    entry.status = AccountQuotaStatus::Healthy;
                }
            }
            QuotaEvent::WindowReset { account, kind, started_at_secs, resets_at_secs, .. } => {
                let entry = self
                    .accounts
                    .entry(account.clone())
                    .or_insert_with(|| Account::new(account.clone()));
                // Route to the right window by kind; "Weekly" targets weekly_window, everything
                // else (Rolling5h, empty from old events) targets the main rolling window.
                if kind == "Weekly" {
                    if let Some(w) = entry.weekly_window.as_mut() {
                        w.started_at_secs = *started_at_secs;
                        w.resets_at_secs = *resets_at_secs;
                        w.consumed = 0.0;
                    }
                } else if let Some(w) = entry.window.as_mut() {
                    w.started_at_secs = *started_at_secs;
                    w.resets_at_secs = *resets_at_secs;
                    w.consumed = 0.0;
                }
                // A fresh window means quota is available again: lift Cooldown so the account
                // re-enters the rotation pool automatically without waiting for a probe.
                if entry.status == AccountQuotaStatus::Cooldown {
                    entry.status = AccountQuotaStatus::Healthy;
                }
            }
            QuotaEvent::BlockPredicted { account, eta_to_block_secs, now_secs, .. } => {
                self.predictions
                    .push((account.clone(), *eta_to_block_secs, *now_secs));
            }
            QuotaEvent::AccountLimited { account, .. } => {
                self.limited.push(account.clone());
                if let Some(a) = self.accounts.get_mut(account) {
                    a.status = AccountQuotaStatus::Limited;
                }
            }
            QuotaEvent::Rotated { from_account, to_account, .. } => {
                self.rotations
                    .push((from_account.clone(), to_account.clone()));
                if let Some(a) = self.accounts.get_mut(from_account) {
                    a.status = AccountQuotaStatus::Cooldown;
                }
            }
            QuotaEvent::Blocked { account, .. } => {
                self.blocked.push(account.clone());
                if let Some(a) = self.accounts.get_mut(account) {
                    a.status = AccountQuotaStatus::Blocked;
                }
            }
            QuotaEvent::AccountRegistered { account, config_dir, .. } => {
                self.registered.insert(account.clone(), config_dir.clone());
                // Make it a rotation candidate too (Healthy with no window yet; the window arrives
                // from the first probe). Idempotent: re-register just refreshes the config_dir.
                self.accounts
                    .entry(account.clone())
                    .or_insert_with(|| Account::new(account.clone()));
            }
            QuotaEvent::AccountDeregistered { account, .. } => {
                self.registered.remove(account);
                self.accounts.remove(account);
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn account_register_then_deregister_roundtrips_through_the_reducer() {
        // hq-quota-accounts.1: AccountRegistered populates the config_dir map AND makes the account
        // a rotation candidate; AccountDeregistered drops both.
        let mut s = QuotaState::default();
        s.apply(&QuotaEvent::AccountRegistered {
            account: "acctB".into(),
            config_dir: "/vol/accounts/acctB".into(),
            now_secs: 100,
        });
        assert_eq!(
            s.registered.get("acctB").map(String::as_str),
            Some("/vol/accounts/acctB")
        );
        assert!(s.accounts.contains_key("acctB"), "registered account is a candidate");
        assert_eq!(s.accounts["acctB"].status, AccountQuotaStatus::Healthy);

        // Re-register refreshes the dir, idempotent on the candidate set.
        s.apply(&QuotaEvent::AccountRegistered {
            account: "acctB".into(),
            config_dir: "/vol/accounts/acctB-v2".into(),
            now_secs: 200,
        });
        assert_eq!(s.registered["acctB"], "/vol/accounts/acctB-v2");
        assert_eq!(s.accounts.len(), 1);

        s.apply(&QuotaEvent::AccountDeregistered {
            account: "acctB".into(),
            now_secs: 300,
        });
        assert!(s.registered.is_empty());
        assert!(!s.accounts.contains_key("acctB"));
    }

    #[test]
    fn ewma_first_sample_sets_value() {
        let mut e = Ewma::new(0.3);
        e.observe(100.0, 60);
        assert_eq!(e.value, 100.0);
        assert_eq!(e.last_now_secs, 60);
    }

    #[test]
    fn ewma_smooths_subsequent_samples() {
        let mut e = Ewma::new(0.3);
        e.observe(100.0, 60);
        e.observe(0.0, 120); // downward spike
        // new = 0.3 * 0 + 0.7 * 100 = 70
        assert!((e.value - 70.0).abs() < 1e-9);
    }

    #[test]
    fn registry_idempotency_per_window() {
        let mut r = AccountRegistry::default();
        assert!(r.mark_predicted("acc-1", 1000));
        assert!(!r.mark_predicted("acc-1", 1000), "same window -> no re-emit");
        assert!(r.mark_predicted("acc-1", 2000), "new window -> allowed");
        r.clear_window_prediction("acc-1");
        assert!(r.mark_predicted("acc-1", 2000), "after reset, allowed again");
    }

    #[test]
    fn window_remaining_clamps_at_zero() {
        let w = AccountWindow {
            kind: WindowKind::Rolling5h,
            limit: 1000,
            started_at_secs: 0,
            resets_at_secs: 18_000,
            consumed: 1500.0,
        };
        assert_eq!(w.remaining(), 0);
    }

    #[test]
    fn tokens_sampled_folds_consumption_into_a_synthetic_window() {
        // hq-quota-feed-display: replaying a sample must render usage in QuotaState (→ REST), seeding
        // a synthetic Rolling5h window on first sight and accumulating IDENTITY-weighted cost.
        let mut s = QuotaState::default();
        s.apply(&QuotaEvent::TokensSampled {
            account: "acc-1".into(),
            session: "sess".into(),
            model: "opus".into(),
            input: 1000,
            output: 500,
            cache_read: 200,
            cache_creation: 100,
            now_secs: 1_000,
        });
        let w = s.accounts["acc-1"].window.as_ref().expect("synthetic window seeded");
        assert_eq!(w.kind, WindowKind::Rolling5h);
        assert_eq!(w.limit, DEFAULT_PLAN_LIMIT);
        assert_eq!(w.started_at_secs, 1_000);
        assert_eq!(w.resets_at_secs, 1_000 + ROLLING_5H_SECS);
        assert_eq!(w.consumed, 1800.0, "IDENTITY: sum of all token categories");

        // A second sample in the same window accumulates.
        s.apply(&QuotaEvent::TokensSampled {
            account: "acc-1".into(),
            session: "sess".into(),
            model: "opus".into(),
            input: 10,
            output: 5,
            cache_read: 0,
            cache_creation: 0,
            now_secs: 1_060,
        });
        assert_eq!(s.accounts["acc-1"].window.as_ref().unwrap().consumed, 1815.0);

        // A sample past the window reset recycles it (fresh consumed).
        s.apply(&QuotaEvent::TokensSampled {
            account: "acc-1".into(),
            session: "sess".into(),
            model: "opus".into(),
            input: 7,
            output: 0,
            cache_read: 0,
            cache_creation: 0,
            now_secs: 1_000 + ROLLING_5H_SECS + 1,
        });
        let w = s.accounts["acc-1"].window.as_ref().unwrap();
        assert_eq!(w.consumed, 7.0, "recycled then folded the fresh sample");
        assert!(w.resets_at_secs > 1_000 + ROLLING_5H_SECS);
    }

    #[test]
    fn probe_on_cooldown_account_with_window_recovers_to_healthy() {
        // hq-quota-refinement.1 (AC1): a probe with remaining > 0 lifts Cooldown → Healthy.
        let mut r = AccountRegistry::default();
        r.upsert_account(Account {
            id: "acc-1".into(),
            status: AccountQuotaStatus::Cooldown,
            window: Some(AccountWindow {
                kind: WindowKind::Rolling5h,
                limit: DEFAULT_PLAN_LIMIT,
                started_at_secs: 0,
                resets_at_secs: 18_000,
                consumed: DEFAULT_PLAN_LIMIT as f64,
            }),
            weekly_window: None,
            last_probe_secs: None,
            sampled_since_probe: 0.0,
            probe_divergence: None,
        });
        r.apply_probe("acc-1", 50_000_000, 36_000, 0);
        let acc = r.get("acc-1").unwrap();
        assert_eq!(acc.status, AccountQuotaStatus::Healthy, "Cooldown lifted on remaining>0");
        let w = acc.window.as_ref().unwrap();
        assert_eq!(w.consumed, 0.0, "consumed reconciled from DEFAULT_PLAN_LIMIT - remaining");
        assert_eq!(w.resets_at_secs, 36_000);
    }

    #[test]
    fn probe_on_cooldown_with_zero_remaining_stays_cooldown() {
        // A probe with remaining=0 must NOT rehabilitate the account.
        let mut r = AccountRegistry::default();
        r.upsert_account(Account {
            id: "acc-1".into(),
            status: AccountQuotaStatus::Cooldown,
            window: Some(AccountWindow {
                kind: WindowKind::Rolling5h,
                limit: DEFAULT_PLAN_LIMIT,
                started_at_secs: 0,
                resets_at_secs: 18_000,
                consumed: DEFAULT_PLAN_LIMIT as f64,
            }),
            weekly_window: None,
            last_probe_secs: None,
            sampled_since_probe: 0.0,
            probe_divergence: None,
        });
        r.apply_probe("acc-1", 0, 18_000, 0);
        assert_eq!(
            r.get("acc-1").unwrap().status,
            AccountQuotaStatus::Cooldown,
            "zero remaining must not lift Cooldown"
        );
    }

    #[test]
    fn probe_creates_window_when_none_exists() {
        // hq-quota-refinement.1 (AC2): first probe on a freshly registered account seeds a window.
        let mut r = AccountRegistry::default();
        r.upsert_account(Account::new("acc-1")); // window = None
        r.apply_probe("acc-1", 40_000_000, 18_000, 0);
        let acc = r.get("acc-1").unwrap();
        assert_eq!(acc.status, AccountQuotaStatus::Healthy);
        let w = acc.window.as_ref().expect("window created by probe");
        assert_eq!(w.kind, WindowKind::Rolling5h);
        assert_eq!(w.resets_at_secs, 18_000);
        assert_eq!(
            w.consumed,
            (DEFAULT_PLAN_LIMIT - 40_000_000) as f64,
            "consumed = limit - remaining"
        );
    }

    #[test]
    fn usage_probed_event_recovers_cooldown_and_creates_window_in_reducer() {
        // hq-quota-refinement.1: QuotaState reducer mirrors actor behaviour for UsageProbed.
        let mut s = QuotaState::default();
        // Seed a Cooldown account with no window (e.g. registered, rotated, never probed).
        s.accounts.insert(
            "acc-1".into(),
            Account {
                id: "acc-1".into(),
                status: AccountQuotaStatus::Cooldown,
                window: None,
                weekly_window: None,
                last_probe_secs: None,
                sampled_since_probe: 0.0,
                probe_divergence: None,
            },
        );
        s.apply(&QuotaEvent::UsageProbed {
            account: "acc-1".into(),
            remaining: 30_000_000,
            resets_at_secs: 50_000,
            weekly_remaining: None,
            weekly_resets_at_secs: None,
            now_secs: 1_000,
        });
        let acc = &s.accounts["acc-1"];
        assert_eq!(acc.status, AccountQuotaStatus::Healthy, "reducer lifts Cooldown");
        let w = acc.window.as_ref().expect("reducer seeds window");
        assert_eq!(w.resets_at_secs, 50_000);
        assert_eq!(w.consumed, (DEFAULT_PLAN_LIMIT - 30_000_000) as f64);
        assert!(acc.weekly_window.is_none(), "no weekly data -> weekly_window stays None");
    }

    // hq-context-custodian.3 — fresh-block signal -------------------------------------

    /// A window helper anchored so `now < resets_at_secs` (fresh) or not (stale) is explicit.
    fn window_resetting_at(resets_at_secs: u64) -> AccountWindow {
        AccountWindow {
            kind: WindowKind::Rolling5h,
            limit: DEFAULT_PLAN_LIMIT,
            started_at_secs: resets_at_secs.saturating_sub(ROLLING_5H_SECS),
            resets_at_secs,
            consumed: DEFAULT_PLAN_LIMIT as f64,
        }
    }

    #[test]
    fn blocked_account_recovers_to_healthy_when_window_reset_on_reprobe() {
        // AC: a Blocked account whose window already reset (remaining > 0 on re-probe) returns
        // to Healthy automatically via apply_probe — no bespoke unblock logic needed.
        let mut r = AccountRegistry::default();
        r.upsert_account(Account {
            id: "acc-1".into(),
            status: AccountQuotaStatus::Blocked,
            window: Some(window_resetting_at(18_000)),
            weekly_window: None,
            last_probe_secs: None,
            sampled_since_probe: 0.0,
            probe_divergence: None,
        });
        r.apply_probe("acc-1", 40_000_000, 36_000, 0);
        assert_eq!(
            r.get("acc-1").unwrap().status,
            AccountQuotaStatus::Healthy,
            "Blocked lifted to Healthy when the re-probe shows capacity"
        );
    }

    #[test]
    fn is_genuinely_blocked_distinguishes_fresh_real_from_stale() {
        // Fresh block: status Blocked AND now < resets_at ⇒ genuinely blocked.
        let fresh = Account {
            id: "a".into(),
            status: AccountQuotaStatus::Blocked,
            window: Some(window_resetting_at(10_000)),
            weekly_window: None,
            last_probe_secs: None,
            sampled_since_probe: 0.0,
            probe_divergence: None,
        };
        assert!(fresh.is_genuinely_blocked(9_000), "fresh block confirmed");
        // Stale block: the window already lapsed ⇒ NOT genuinely blocked (awaiting re-probe).
        assert!(!fresh.is_genuinely_blocked(10_001), "stale block (window lapsed) not confirmed");
        assert!(!fresh.is_genuinely_blocked(10_000), "at the boundary the window has reset");
        // A Blocked account with no window can't confirm freshness ⇒ false.
        let no_window = Account {
            id: "a".into(),
            status: AccountQuotaStatus::Blocked,
            window: None,
            weekly_window: None,
            last_probe_secs: None,
            sampled_since_probe: 0.0,
            probe_divergence: None,
        };
        assert!(!no_window.is_genuinely_blocked(0));
        // Any non-Blocked status is never genuinely blocked, regardless of window.
        for status in [
            AccountQuotaStatus::Healthy,
            AccountQuotaStatus::Limited,
            AccountQuotaStatus::Cooldown,
        ] {
            let acc = Account {
                id: "a".into(),
                status,
                window: Some(window_resetting_at(10_000)),
                weekly_window: None,
                last_probe_secs: None,
                sampled_since_probe: 0.0,
                probe_divergence: None,
            };
            assert!(!acc.is_genuinely_blocked(9_000), "{status:?} is not a block");
        }
    }

    #[test]
    fn pool_genuinely_blocked_only_on_confirmed_fully_exhausted_pool() {
        let blocked_fresh = |id: &str| Account {
            id: id.into(),
            status: AccountQuotaStatus::Blocked,
            window: Some(window_resetting_at(10_000)),
            weekly_window: None,
            last_probe_secs: None,
            sampled_since_probe: 0.0,
            probe_divergence: None,
        };
        let now = 9_000; // before the 10_000 reset ⇒ fresh

        // Empty pool ⇒ false (nothing to confirm; demand context).
        assert!(!AccountRegistry::default().pool_genuinely_blocked(now));

        // One freshly-blocked account, none healthy ⇒ true.
        let mut r = AccountRegistry::default();
        r.upsert_account(blocked_fresh("a"));
        assert!(r.pool_genuinely_blocked(now), "fresh block, no healthy ⇒ defer authorized");

        // A single Healthy account anywhere ⇒ capacity exists ⇒ false.
        r.upsert_account(Account::new("b")); // Healthy
        assert!(!r.pool_genuinely_blocked(now), "a healthy account means work can be fed");

        // A STALE blocked account masquerading ⇒ false even with a fresh one present.
        let mut r = AccountRegistry::default();
        r.upsert_account(blocked_fresh("a"));
        r.upsert_account(Account {
            id: "stale".into(),
            status: AccountQuotaStatus::Blocked,
            window: Some(window_resetting_at(10_000)),
            weekly_window: None,
            last_probe_secs: None,
            sampled_since_probe: 0.0,
            probe_divergence: None,
        });
        assert!(
            !r.pool_genuinely_blocked(10_500),
            "a stale Blocked forces re-probe / context — never grants a deferral"
        );
    }

    // hq-46cdf4 tests -------------------------------------------------------------------

    #[test]
    fn probe_records_divergence_and_resets_sampled_tail() {
        let mut r = AccountRegistry::default();
        r.upsert_account(Account {
            id: "acc-1".into(),
            status: AccountQuotaStatus::Healthy,
            window: Some(AccountWindow {
                kind: WindowKind::Rolling5h,
                limit: 1_000,
                started_at_secs: 0,
                resets_at_secs: 18_000,
                consumed: 0.0,
            }),
            weekly_window: None,
            last_probe_secs: None,
            sampled_since_probe: 0.0,
            probe_divergence: None,
        });
        // Local sampling accumulates the unverified tail (IDENTITY weights: 100+100=200).
        r.apply_sample("acc-1", "s1", "opus", 100, 100, 0, 0, 600);
        let acc = r.get("acc-1").unwrap();
        assert_eq!(acc.sampled_since_probe, 200.0);
        assert_eq!(acc.last_probe_secs, None, "never probed yet");

        // Provider says 850 remaining of 1000 ⇒ probed consumed 150; local said 200.
        r.apply_probe("acc-1", 850, 20_000, 700);
        let acc = r.get("acc-1").unwrap();
        assert_eq!(acc.window.as_ref().unwrap().consumed, 150.0, "probe is authoritative");
        assert_eq!(acc.probe_divergence, Some(50.0), "local over-count exposed");
        assert_eq!(acc.last_probe_secs, Some(700));
        assert_eq!(acc.sampled_since_probe, 0.0, "tail reset on probe");
    }

    #[test]
    fn reducer_rebuilds_probe_provenance_byte_for_byte() {
        let mut s = QuotaState::default();
        s.apply(&QuotaEvent::TokensSampled {
            account: "acc-1".into(),
            session: "s1".into(),
            model: "opus".into(),
            input: 100,
            output: 100,
            cache_read: 0,
            cache_creation: 0,
            now_secs: 600,
        });
        assert_eq!(s.accounts["acc-1"].sampled_since_probe, 200.0);
        s.apply(&QuotaEvent::UsageProbed {
            account: "acc-1".into(),
            remaining: DEFAULT_PLAN_LIMIT - 150,
            resets_at_secs: 20_000,
            weekly_remaining: None,
            weekly_resets_at_secs: None,
            now_secs: 700,
        });
        let acc = &s.accounts["acc-1"];
        assert_eq!(acc.window.as_ref().unwrap().consumed, 150.0);
        assert_eq!(acc.probe_divergence, Some(50.0));
        assert_eq!(acc.last_probe_secs, Some(700));
        assert_eq!(acc.sampled_since_probe, 0.0);
    }

    // hq-quota-weekly.2 tests -----------------------------------------------------------

    #[test]
    fn account_serde_roundtrip_with_weekly_window() {
        // A Claude Pro account carries both windows; serde must round-trip cleanly.
        let acc = Account {
            id: "pro-acct".into(),
            status: AccountQuotaStatus::Healthy,
            window: Some(AccountWindow {
                kind: WindowKind::Rolling5h,
                limit: DEFAULT_PLAN_LIMIT,
                started_at_secs: 1_000,
                resets_at_secs: 19_000,
                consumed: 100.0,
            }),
            weekly_window: Some(AccountWindow {
                kind: WindowKind::Weekly,
                limit: DEFAULT_PLAN_LIMIT,
                started_at_secs: 0,
                resets_at_secs: 604_800,
                consumed: 5_000_000.0,
            }),
            last_probe_secs: None,
            sampled_since_probe: 0.0,
            probe_divergence: None,
        };
        let json = serde_json::to_string(&acc).unwrap();
        let back: Account = serde_json::from_str(&json).unwrap();
        assert_eq!(acc, back);
        let ww = back.weekly_window.as_ref().unwrap();
        assert_eq!(ww.kind, WindowKind::Weekly);
        assert_eq!(ww.consumed, 5_000_000.0);
    }

    #[test]
    fn account_without_weekly_window_serde_roundtrip() {
        // Existing accounts without weekly data must deserialise fine (backwards compat).
        // hq-quota-weekly.5: weekly_window must appear as null (not absent) so consumers
        // can distinguish "no weekly quota" from a missing field.
        let acc = Account::new("classic-acct");
        let json = serde_json::to_string(&acc).unwrap();
        assert!(json.contains("weekly_window"), "weekly_window must appear in wire even when None");
        let v: serde_json::Value = serde_json::from_str(&json).unwrap();
        assert_eq!(v["weekly_window"], serde_json::Value::Null, "weekly_window is null when None");
        let back: Account = serde_json::from_str(&json).unwrap();
        assert_eq!(back.weekly_window, None);
    }

    #[test]
    fn usage_probed_event_with_weekly_fields_roundtrips() {
        // The event schema must carry and round-trip weekly fields when present.
        let ev = QuotaEvent::UsageProbed {
            account: "pro-acct".into(),
            remaining: 40_000_000,
            resets_at_secs: 18_000,
            weekly_remaining: Some(10_000_000),
            weekly_resets_at_secs: Some(604_800),
            now_secs: 1_000,
        };
        let json = serde_json::to_string(&ev).unwrap();
        let back: QuotaEvent = serde_json::from_str(&json).unwrap();
        assert_eq!(ev, back);
    }

    #[test]
    fn usage_probed_event_without_weekly_fields_roundtrips() {
        // An event without weekly fields must still deserialise (None for absent fields).
        let ev = QuotaEvent::UsageProbed {
            account: "classic".into(),
            remaining: 30_000_000,
            resets_at_secs: 18_000,
            weekly_remaining: None,
            weekly_resets_at_secs: None,
            now_secs: 500,
        };
        let json = serde_json::to_string(&ev).unwrap();
        assert!(!json.contains("weekly_remaining"), "absent fields skipped in wire");
        let back: QuotaEvent = serde_json::from_str(&json).unwrap();
        assert_eq!(ev, back);
    }

    #[test]
    fn apply_weekly_probe_seeds_and_reconciles_weekly_window() {
        let mut r = AccountRegistry::default();
        r.upsert_account(Account::new("pro-acct"));

        // First probe: seeds the weekly window.
        r.apply_weekly_probe("pro-acct", 45_000_000, 604_800, 1_000);
        let acc = r.get("pro-acct").unwrap();
        let ww = acc.weekly_window.as_ref().expect("weekly_window seeded");
        assert_eq!(ww.kind, WindowKind::Weekly);
        assert_eq!(ww.resets_at_secs, 604_800);
        assert_eq!(ww.consumed, (DEFAULT_PLAN_LIMIT - 45_000_000) as f64);
        assert_eq!(ww.started_at_secs, 1_000);

        // Second probe: reconciles consumed.
        r.apply_weekly_probe("pro-acct", 30_000_000, 604_800, 2_000);
        let ww2 = r.get("pro-acct").unwrap().weekly_window.as_ref().unwrap();
        assert_eq!(ww2.consumed, (DEFAULT_PLAN_LIMIT - 30_000_000) as f64);
    }

    #[test]
    fn reducer_populates_weekly_window_when_event_carries_weekly_fields() {
        let mut s = QuotaState::default();
        s.apply(&QuotaEvent::UsageProbed {
            account: "pro-acct".into(),
            remaining: 40_000_000,
            resets_at_secs: 18_000,
            weekly_remaining: Some(10_000_000),
            weekly_resets_at_secs: Some(604_800),
            now_secs: 1_000,
        });
        let acc = &s.accounts["pro-acct"];
        let ww = acc.weekly_window.as_ref().expect("reducer sets weekly_window");
        assert_eq!(ww.kind, WindowKind::Weekly);
        assert_eq!(ww.resets_at_secs, 604_800);
        assert_eq!(ww.consumed, (DEFAULT_PLAN_LIMIT - 10_000_000) as f64);
        // Rolling5h window still populated independently.
        assert!(acc.window.is_some());
    }
}
