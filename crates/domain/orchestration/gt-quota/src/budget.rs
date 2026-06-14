//! Per-session **budget ledger** (B1, gtcore-ab170f): each agent session accumulates the
//! tokens it consumed and the execution-minute span it ran, attributed to a bead. This is the
//! cost-attribution sub-account `TokensSampled` (per-account window) cannot answer: "how much
//! did *this* bead's session spend, and is it over budget?".
//!
//! Pure and sync, like every reducer in this crate (`docs/06-observability.md`): the clock
//! enters as `now_secs` data, never read here, so replaying the event log rebuilds the ledger
//! deterministically. The cost is pre-normalized by the caller (`cost.rs`) so the ledger stays
//! model-agnostic — it sums cost units the same way the account window does.
//!
//! Two facts feed a budget:
//! - **tokens / cost** — folded from each [`QuotaEvent::TokensSampled`](crate::QuotaEvent),
//!   keyed by `session`. A sample auto-opens a budget (bead unknown until stamped).
//! - **bead + limit + start** — stamped by `open` (the edge calls it at session spawn with the
//!   bead from `GT_HOOK_BEAD`). The execution span runs from the first activity to the last.
//!
//! Crossing the limit is **latched once** (`exceeded`) so the alert fires a single
//! [`QuotaEvent::BudgetExceeded`](crate::QuotaEvent) rather than one per sample past the line.

use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};

/// One session's running budget: cumulative token spend, normalized cost, and the
/// execution-minute span, attributed to a bead.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct SessionBudget {
    /// Session the spend is attributed to (the per-session breakdown key).
    pub session: String,
    /// Bead this session is working — the cost-attribution dimension. `None` until `open`
    /// stamps it (a sample that arrives before the spawn-time open accumulates unattributed).
    pub bead: Option<String>,
    pub input: u64,
    pub output: u64,
    pub cache_read: u64,
    pub cache_creation: u64,
    /// Normalized cost consumed (cost units, see `cost.rs`). May carry a fraction from
    /// per-model weights.
    pub cost: f64,
    /// First activity (open or first sample), UTC epoch seconds. Anchors the execution span.
    pub started_at_secs: u64,
    /// Most recent activity (sample, open, or close), UTC epoch seconds. The span's other end.
    pub last_activity_secs: u64,
    /// Budget ceiling in cost units. `None` ⇒ untracked (samples accumulate but never alert).
    pub limit_cost: Option<f64>,
    /// Latched once the running cost first crosses `limit_cost`, so the alert fires exactly
    /// once. Stays `true` for the rest of the session.
    pub exceeded: bool,
}

impl SessionBudget {
    fn new(session: impl Into<String>, now_secs: u64) -> Self {
        Self {
            session: session.into(),
            bead: None,
            input: 0,
            output: 0,
            cache_read: 0,
            cache_creation: 0,
            cost: 0.0,
            started_at_secs: now_secs,
            last_activity_secs: now_secs,
            limit_cost: None,
            exceeded: false,
        }
    }

    /// Execution minutes: the span from the first to the last activity. A session with a single
    /// activity (or none) reads `0.0`.
    pub fn elapsed_minutes(&self) -> f64 {
        self.last_activity_secs.saturating_sub(self.started_at_secs) as f64 / 60.0
    }

    /// Total raw tokens across all categories — the headline "tokens consumed" number.
    pub fn total_tokens(&self) -> u64 {
        self.input + self.output + self.cache_read + self.cache_creation
    }

    /// Whether the running cost is over the configured limit. Always `false` when no limit is
    /// set (an untracked session can never be "over budget").
    pub fn is_over_budget(&self) -> bool {
        match self.limit_cost {
            Some(limit) => self.cost > limit,
            None => false,
        }
    }
}

/// The owned registry of per-session budgets (`BTreeMap` keyed by session id → stable, sorted
/// snapshots). Lives inside the quota actor next to the `AccountRegistry`; also rebuilt by the
/// replay reducer ([`crate::QuotaState`]).
#[derive(Debug, Default, Clone, PartialEq, Serialize, Deserialize)]
pub struct BudgetLedger {
    budgets: BTreeMap<String, SessionBudget>,
}

impl BudgetLedger {
    /// Open (or refresh) a session's budget: stamp the bead and limit the edge knows at spawn.
    /// Idempotent and additive — if samples already accumulated under this session (a sample
    /// beat the open), their totals are preserved; only the attribution metadata is set. `bead`
    /// and `limit_cost` are applied when `Some`, leaving an existing value untouched when `None`.
    pub fn open(
        &mut self,
        session: impl Into<String>,
        bead: Option<String>,
        limit_cost: Option<f64>,
        now_secs: u64,
    ) {
        let session = session.into();
        let b = self
            .budgets
            .entry(session.clone())
            .or_insert_with(|| SessionBudget::new(session, now_secs));
        if bead.is_some() {
            b.bead = bead;
        }
        if limit_cost.is_some() {
            b.limit_cost = limit_cost;
        }
        if now_secs > b.last_activity_secs {
            b.last_activity_secs = now_secs;
        }
    }

    /// Fold one usage sample into a session's budget: add the tokens + pre-normalized `cost`,
    /// advance the activity span. A sample for an unseen session auto-opens its budget (bead
    /// unknown). Returns `true` iff this sample is the one that **first** crosses the limit, so
    /// the caller emits exactly one `BudgetExceeded` alert.
    #[allow(clippy::too_many_arguments)]
    pub fn record(
        &mut self,
        session: impl Into<String>,
        cost: f64,
        input: u64,
        output: u64,
        cache_read: u64,
        cache_creation: u64,
        now_secs: u64,
    ) -> bool {
        let session = session.into();
        let b = self
            .budgets
            .entry(session.clone())
            .or_insert_with(|| SessionBudget::new(session, now_secs));
        b.input += input;
        b.output += output;
        b.cache_read += cache_read;
        b.cache_creation += cache_creation;
        b.cost += cost;
        if now_secs > b.last_activity_secs {
            b.last_activity_secs = now_secs;
        }
        let crossed_now = !b.exceeded && b.is_over_budget();
        if crossed_now {
            b.exceeded = true;
        }
        crossed_now
    }

    /// Close a session's budget: advance the activity span to `now_secs` (the session-end edge)
    /// so the final execution-minute count covers up to shutdown. No-op for an unknown session.
    pub fn close(&mut self, session: &str, now_secs: u64) {
        if let Some(b) = self.budgets.get_mut(session) {
            if now_secs > b.last_activity_secs {
                b.last_activity_secs = now_secs;
            }
        }
    }

    /// Latch a session as over budget from a replayed `BudgetExceeded` event (idempotent). Keeps
    /// the rebuilt ledger consistent without recomputing the crossing during replay.
    pub fn mark_exceeded(&mut self, session: &str, now_secs: u64) {
        if let Some(b) = self.budgets.get_mut(session) {
            b.exceeded = true;
            if now_secs > b.last_activity_secs {
                b.last_activity_secs = now_secs;
            }
        }
    }

    pub fn get(&self, session: &str) -> Option<&SessionBudget> {
        self.budgets.get(session)
    }

    pub fn len(&self) -> usize {
        self.budgets.len()
    }

    pub fn is_empty(&self) -> bool {
        self.budgets.is_empty()
    }

    /// Every session budget, sorted by session id (the `BTreeMap` order).
    pub fn snapshot(&self) -> Vec<SessionBudget> {
        self.budgets.values().cloned().collect()
    }

    /// All sessions attributed to `bead`. Order is the session-id sort.
    pub fn by_bead(&self, bead: &str) -> Vec<&SessionBudget> {
        self.budgets
            .values()
            .filter(|b| b.bead.as_deref() == Some(bead))
            .collect()
    }

    /// Total normalized cost charged to a bead across all its sessions — the cost-attribution
    /// answer.
    pub fn cost_for_bead(&self, bead: &str) -> f64 {
        self.by_bead(bead).iter().map(|b| b.cost).sum()
    }

    /// Total raw tokens charged to a bead across all its sessions.
    pub fn tokens_for_bead(&self, bead: &str) -> u64 {
        self.by_bead(bead).iter().map(|b| b.total_tokens()).sum()
    }

    /// Total execution minutes across a bead's sessions (summed per session — sessions may run
    /// concurrently, so this is aggregate compute-time, not wall-clock).
    pub fn minutes_for_bead(&self, bead: &str) -> f64 {
        self.by_bead(bead).iter().map(|b| b.elapsed_minutes()).sum()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn sample_auto_opens_and_accumulates_tokens_and_span() {
        let mut led = BudgetLedger::default();
        // First sample at t=60 auto-opens the budget; second at t=180 advances the span.
        led.record("s1", 100.0, 10, 20, 5, 1, 60);
        led.record("s1", 50.0, 4, 6, 0, 0, 180);

        let b = led.get("s1").expect("budget exists");
        assert_eq!((b.input, b.output, b.cache_read, b.cache_creation), (14, 26, 5, 1));
        assert_eq!(b.total_tokens(), 14 + 26 + 5 + 1);
        assert!((b.cost - 150.0).abs() < 1e-9);
        // span 60..180 = 120s = 2 minutes.
        assert!((b.elapsed_minutes() - 2.0).abs() < 1e-9);
        assert_eq!(b.bead, None, "no attribution until open stamps the bead");
    }

    #[test]
    fn open_stamps_bead_and_limit_without_wiping_prior_samples() {
        let mut led = BudgetLedger::default();
        // A sample beat the open (race): accumulation must survive the later open.
        led.record("s1", 70.0, 7, 0, 0, 0, 100);
        led.open("s1", Some("gtcore-ab170f".into()), Some(500.0), 120);

        let b = led.get("s1").unwrap();
        assert_eq!(b.bead.as_deref(), Some("gtcore-ab170f"));
        assert_eq!(b.limit_cost, Some(500.0));
        assert!((b.cost - 70.0).abs() < 1e-9, "prior sample preserved");
        assert_eq!(b.started_at_secs, 100, "start anchored at first activity, not the open");
    }

    #[test]
    fn open_before_any_sample_anchors_the_span_start() {
        let mut led = BudgetLedger::default();
        led.open("s1", Some("bead-x".into()), None, 1000);
        led.record("s1", 10.0, 1, 1, 0, 0, 1600); // +10 minutes later
        let b = led.get("s1").unwrap();
        assert_eq!(b.started_at_secs, 1000);
        assert!((b.elapsed_minutes() - 10.0).abs() < 1e-9);
    }

    #[test]
    fn budget_exceeded_latches_once() {
        let mut led = BudgetLedger::default();
        led.open("s1", Some("b".into()), Some(100.0), 0);
        // Under budget: no crossing.
        assert!(!led.record("s1", 60.0, 0, 0, 0, 0, 60));
        assert!(!led.get("s1").unwrap().exceeded);
        // This sample pushes cost to 130 > 100: first crossing fires.
        assert!(led.record("s1", 70.0, 0, 0, 0, 0, 120));
        assert!(led.get("s1").unwrap().exceeded);
        // Still over budget, but the alert does NOT re-fire.
        assert!(!led.record("s1", 10.0, 0, 0, 0, 0, 180));
        assert!((led.get("s1").unwrap().cost - 140.0).abs() < 1e-9);
    }

    #[test]
    fn no_limit_never_exceeds() {
        let mut led = BudgetLedger::default();
        // huge spend, but no ceiling configured.
        assert!(!led.record("s1", 1_000_000.0, 0, 0, 0, 0, 60));
        assert!(!led.get("s1").unwrap().is_over_budget());
    }

    #[test]
    fn open_can_set_limit_after_the_fact_and_crossing_fires_on_next_sample() {
        let mut led = BudgetLedger::default();
        led.record("s1", 200.0, 0, 0, 0, 0, 60); // already spent 200, no limit yet
        led.open("s1", None, Some(100.0), 70); // now impose a 100 ceiling
        // is_over_budget is already true (200 > 100), but the latch only flips on a recorded
        // sample so the alert is tied to an event. The next sample reports the crossing.
        assert!(led.record("s1", 1.0, 0, 0, 0, 0, 80));
        assert!(led.get("s1").unwrap().exceeded);
    }

    #[test]
    fn close_extends_the_execution_span() {
        let mut led = BudgetLedger::default();
        led.open("s1", Some("b".into()), None, 0);
        led.record("s1", 1.0, 1, 0, 0, 0, 120);
        led.close("s1", 600); // session ends 10 min after open
        assert!((led.get("s1").unwrap().elapsed_minutes() - 10.0).abs() < 1e-9);
        // close on an unknown session is a no-op.
        led.close("ghost", 999);
        assert!(led.get("ghost").is_none());
    }

    #[test]
    fn mark_exceeded_latches_idempotently_for_replay() {
        let mut led = BudgetLedger::default();
        led.open("s1", Some("b".into()), Some(10.0), 0);
        led.mark_exceeded("s1", 50);
        assert!(led.get("s1").unwrap().exceeded);
        // Replaying the same fact again is harmless.
        led.mark_exceeded("s1", 50);
        assert!(led.get("s1").unwrap().exceeded);
    }

    #[test]
    fn per_bead_attribution_aggregates_across_sessions() {
        let mut led = BudgetLedger::default();
        led.open("s1", Some("bead-A".into()), None, 0);
        led.open("s2", Some("bead-A".into()), None, 0);
        led.open("s3", Some("bead-B".into()), None, 0);
        led.record("s1", 100.0, 10, 0, 0, 0, 60); // bead-A, 1 min
        led.record("s2", 50.0, 5, 0, 0, 0, 120); // bead-A, 2 min
        led.record("s3", 999.0, 1, 0, 0, 0, 60); // bead-B

        assert!((led.cost_for_bead("bead-A") - 150.0).abs() < 1e-9);
        assert_eq!(led.tokens_for_bead("bead-A"), 15);
        assert!((led.minutes_for_bead("bead-A") - 3.0).abs() < 1e-9);
        assert_eq!(led.by_bead("bead-A").len(), 2);
        assert_eq!(led.by_bead("bead-B").len(), 1);
        assert_eq!(led.by_bead("unknown").len(), 0);
        assert!((led.cost_for_bead("unknown")).abs() < 1e-9);
    }

    #[test]
    fn snapshot_is_sorted_by_session() {
        let mut led = BudgetLedger::default();
        led.record("zeta", 1.0, 0, 0, 0, 0, 0);
        led.record("alpha", 1.0, 0, 0, 0, 0, 0);
        let snap = led.snapshot();
        let ids: Vec<&str> = snap.iter().map(|b| b.session.as_str()).collect();
        assert_eq!(ids, ["alpha", "zeta"]);
        assert_eq!(led.len(), 2);
        assert!(!led.is_empty());
    }
}
