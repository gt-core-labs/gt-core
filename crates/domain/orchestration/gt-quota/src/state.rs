//! Domain state + replay reducer.
//!
//! `AccountRegistry` is the mutable state the actor owns; `QuotaState` is the version
//! rebuilt from the log for the Step 3 gate (deterministic replay).

use std::collections::{BTreeMap, HashMap};

use serde::{Deserialize, Serialize};

use crate::cost::{cost_units, ModelWeights};
use crate::events::QuotaEvent;

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
    pub window: Option<AccountWindow>,
}

impl Account {
    pub fn new(id: impl Into<String>) -> Self {
        Self {
            id: id.into(),
            status: AccountQuotaStatus::Healthy,
            window: None,
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
    /// No-op if the account has no live window. Shared by `QuotaMsg::Probe` and `ProbeWindow`.
    pub fn apply_probe(&mut self, account: &str, remaining: u64, resets_at_secs: u64) {
        if let Some(acc) = self.get_mut(account) {
            if let Some(w) = acc.window.as_mut() {
                w.consumed = (w.limit.saturating_sub(remaining)) as f64;
                w.resets_at_secs = resets_at_secs;
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
}

impl QuotaState {
    pub fn apply(&mut self, event: &QuotaEvent) {
        match event {
            QuotaEvent::TokensSampled { .. } => {
                // The sample feeds the rate (applied live by the actor; replay can rebuild
                // it via EWMA if needed). What matters in `QuotaState` is the observable
                // effect: the prediction and status changes.
            }
            QuotaEvent::UsageProbed { account, remaining, resets_at_secs, .. } => {
                let entry = self.accounts.entry(account.clone()).or_insert_with(|| {
                    Account {
                        id: account.clone(),
                        status: AccountQuotaStatus::Healthy,
                        window: None,
                    }
                });
                if let Some(w) = entry.window.as_mut() {
                    w.consumed = (w.limit.saturating_sub(*remaining)) as f64;
                    w.resets_at_secs = *resets_at_secs;
                }
            }
            QuotaEvent::WindowReset { account, started_at_secs, resets_at_secs } => {
                let entry = self.accounts.entry(account.clone()).or_insert_with(|| {
                    Account {
                        id: account.clone(),
                        status: AccountQuotaStatus::Healthy,
                        window: None,
                    }
                });
                if let Some(w) = entry.window.as_mut() {
                    w.started_at_secs = *started_at_secs;
                    w.resets_at_secs = *resets_at_secs;
                    w.consumed = 0.0;
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
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

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
}
