//! Epoch-based self-evolution (hq-auto.9) — the apex of the autonomous loop.
//!
//! Turns the project's core competency (migration) inward. Each **epoch**, the
//! system migrates its own code into an *isolated rig* (an hq-mt workspace:
//! schema-per-ws + a per-ws `RootRegistry`), measures fitness (hq-auto.6) there
//! **without touching prod**, and either **promotes** the rig (blue-green) if it
//! is strictly better or **discards** it (reversible — the rig just disappears)
//! if it is not. The meta-loop (hq-auto.7) writes the lesson either way. The
//! shape is evolutionary: rig = generation sandbox, migration = mutation, fitness
//! = selection, promote/discard = survival.
//!
//! Self-modifying code is dangerous, so this module is the **policy core** that
//! encodes the bead's seven hard guardrails as gates a candidate must clear
//! before it can reach prod. It is pure (no rig orchestration, no deploy, no
//! store): the host runs the rig and supplies the measurements + gate results;
//! [`decide_epoch`] returns the verdict and [`Evolution`] governs when the whole
//! loop must stop. Keeping it pure is itself a safety property — the promote
//! decision is a total, auditable function, not buried in I/O.
//!
//! ## The seven guardrails (bead hq-auto.9), and where each lives
//!
//! 1. **Rig never touches prod until promoted** — structural: the host runs the
//!    epoch in an isolated workspace, and the only verdict that mutates prod is
//!    [`EpochVerdict::Promote`]. Every other verdict leaves prod untouched, so a
//!    bad mutation is discarded for free. Asserted by the tests.
//! 2. **Fitness rigorous + human-audited** — a promotion is a blue-green prod
//!    deploy (grave/irreversible); when no human is on-loop it is **parked**
//!    ([`EpochVerdict::Park`]), reusing the same [`HumanPresence`] floor as the
//!    dispatcher's park queue (hq-auto.3).
//! 3. **docs/04 invariants as policy (hq-auto.4) no migration may cross** —
//!    [`EpochGates::policy_clean`]; a violation is a hard [`Discard`], never a
//!    promote.
//! 4. **Replay-green per epoch** — [`EpochGates::replay_green`]; red replay is a
//!    hard discard.
//! 5. **Meta circuit-breaker halts on a non-improving streak** —
//!    [`Evolution`] counts consecutive non-promoting epochs and halts at
//!    `max_streak` ([`HaltReason::NonImprovingStreak`]).
//! 6. **Cheap fitness proxy before a full epoch** — [`EpochGates::proxy_promising`];
//!    an unpromising proxy short-circuits to [`EpochVerdict::SkipFull`] so the
//!    expensive full epoch is never spent on a dead-end mutation.
//! 7. **Diminishing-returns stop** — [`Evolution`] halts once a promotion's gain
//!    falls below `min_gain` ([`HaltReason::DiminishingReturns`]).
//!
//! [`Discard`]: EpochVerdict::Discard

use gt_issues::park::HumanPresence;

/// A comparable fitness sample for one variant — the prod baseline or a rig.
///
/// Mirrors the correctness-first shape of the reconciler's fitness scorecard
/// (hq-auto.6) without importing that binary: correctness is a floor (a variant
/// with any trap / false-hidden can never out-rank a clean one, however good its
/// rates), then higher delivery, then higher graph completeness. The ordering is
/// total, so "is the rig better than prod?" is always decidable.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct EpochFitness {
    /// No correctness defects (no traps, no false-hidden). The hard floor.
    pub clean: bool,
    /// Delivery-rate as a whole percentage (0..=100).
    pub delivery_pct: u32,
    /// Graph-completeness (auto-derivable fraction) as a percentage (0..=100).
    pub completeness_pct: u32,
}

impl EpochFitness {
    /// Lexicographic ranking key: clean first, then delivery, then completeness.
    fn rank(&self) -> (bool, u32, u32) {
        (self.clean, self.delivery_pct, self.completeness_pct)
    }

    /// Whether this variant is **strictly** better than `other`. Selection
    /// requires strict improvement — a tie is *not* an improvement, so a neutral
    /// mutation is discarded (it cannot justify a prod deploy or reset the
    /// non-improving streak).
    pub fn is_better_than(&self, other: &EpochFitness) -> bool {
        self.rank() > other.rank()
    }

    /// The improvement margin over `other`, in delivery percentage points, or 0
    /// when not strictly better. Used for the diminishing-returns stop. A
    /// correctness flip (dirty→clean) counts as a full-margin gain so it never
    /// reads as diminishing.
    pub fn gain_over(&self, other: &EpochFitness) -> u32 {
        if !self.is_better_than(other) {
            return 0;
        }
        if self.clean && !other.clean {
            return 100;
        }
        self.delivery_pct.saturating_sub(other.delivery_pct)
    }
}

/// The gate results the host supplies for one epoch — guardrails 3, 4, 6.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct EpochGates {
    /// Guardrail 6: a cheap fitness proxy looked promising enough to spend a full
    /// epoch on. `false` short-circuits before any rig work.
    pub proxy_promising: bool,
    /// Guardrail 3: the rig honors every docs/04 invariant (machine-checked by
    /// the hq-auto.4 policy). `false` is a hard discard — no invariant may be
    /// crossed to win on fitness.
    pub policy_clean: bool,
    /// Guardrail 4: the rig's event replay is byte-for-byte green.
    pub replay_green: bool,
}

impl EpochGates {
    /// All gates pass and the proxy is promising — the candidate may be judged on
    /// fitness.
    pub fn all_pass(&self) -> bool {
        self.proxy_promising && self.policy_clean && self.replay_green
    }
}

/// Why a candidate rig was discarded (every reason leaves prod untouched).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DiscardReason {
    /// Rig was not strictly better than the prod baseline (selection).
    NotBetter,
    /// Rig crossed a docs/04 invariant (guardrail 3) — gaming fitness by breaking
    /// a rule evolves the system *worse*, so this is refused outright.
    PolicyViolation,
    /// Rig's replay was not green (guardrail 4).
    ReplayRed,
}

/// The verdict for one epoch. Only [`Promote`](EpochVerdict::Promote) mutates
/// prod; every other outcome leaves it exactly as it was (guardrail 1).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum EpochVerdict {
    /// Promote the rig to prod (blue-green) — it cleared every gate, beat the
    /// baseline, and a human is on-loop to own the deploy.
    Promote,
    /// Hold the (better, gate-clean) rig for human approval before the grave
    /// prod deploy — guardrail 2, unattended.
    Park,
    /// Drop the rig; prod is untouched. Carries why.
    Discard(DiscardReason),
    /// The cheap proxy was unpromising; the full epoch was skipped (guardrail 6).
    SkipFull,
}

/// Decide one epoch's verdict from the prod `baseline`, the candidate `rig`
/// fitness, the `gates`, and whether a human is on-loop.
///
/// Order matters — the cheapest and hardest gates first, so an expensive or
/// grave path is never taken when a cheaper refusal applies:
/// proxy (6) → policy (3) → replay (4) → strict improvement → human floor (2).
pub fn decide_epoch(
    baseline: &EpochFitness,
    rig: &EpochFitness,
    gates: &EpochGates,
    presence: HumanPresence,
) -> EpochVerdict {
    // 6 — cheap proxy first: never spend the rest on a dead end.
    if !gates.proxy_promising {
        return EpochVerdict::SkipFull;
    }
    // 3 — invariants are inviolable; a crossing is a hard discard, not a tradeoff.
    if !gates.policy_clean {
        return EpochVerdict::Discard(DiscardReason::PolicyViolation);
    }
    // 4 — replay must be green per epoch.
    if !gates.replay_green {
        return EpochVerdict::Discard(DiscardReason::ReplayRed);
    }
    // Selection — only a strict improvement survives.
    if !rig.is_better_than(baseline) {
        return EpochVerdict::Discard(DiscardReason::NotBetter);
    }
    // 2 — promotion is a grave prod deploy: a human must own it, else park.
    match presence {
        HumanPresence::Present => EpochVerdict::Promote,
        HumanPresence::Absent => EpochVerdict::Park,
    }
}

/// Why the whole evolution loop stopped.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum HaltReason {
    /// Guardrail 5: too many consecutive epochs without a promotion. A
    /// non-improving streak means the search has stalled — stop and escalate
    /// rather than burn epochs (and risk a slowly-gaming mutation).
    NonImprovingStreak,
    /// Guardrail 7: the last promotion's gain fell below the worthwhile margin —
    /// further epochs are not worth their cost.
    DiminishingReturns,
}

/// Whether the loop should run another epoch or stop.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LoopControl {
    Continue,
    Halt(HaltReason),
}

/// The multi-epoch controller: tracks the best fitness promoted so far and the
/// run of non-improving epochs, applying the two stop criteria (guardrails 5 & 7).
///
/// Drive it by calling [`record`](Evolution::record) with each epoch's verdict
/// (and, for a promotion, the now-promoted fitness). It returns whether to keep
/// going. Pure and owned by the host loop.
#[derive(Debug, Clone)]
pub struct Evolution {
    /// Best fitness promoted to prod so far (the running baseline). `None` until
    /// the first promotion.
    best: Option<EpochFitness>,
    /// Consecutive epochs since the last promotion.
    non_improving_streak: usize,
    /// Guardrail 5: halt once the streak reaches this. Must be >= 1.
    max_streak: usize,
    /// Guardrail 7: halt when a promotion's gain (delivery pts) is below this.
    min_gain: u32,
}

impl Evolution {
    /// A fresh controller seeded with the current prod fitness as the baseline.
    ///
    /// `max_streak` is the non-improving budget (guardrail 5; clamped to >= 1),
    /// `min_gain` the diminishing-returns floor in delivery percentage points
    /// (guardrail 7).
    pub fn new(baseline: EpochFitness, max_streak: usize, min_gain: u32) -> Self {
        Evolution {
            best: Some(baseline),
            non_improving_streak: 0,
            max_streak: max_streak.max(1),
            min_gain,
        }
    }

    /// The current best (prod) fitness — the baseline the next epoch is judged
    /// against.
    pub fn best(&self) -> Option<EpochFitness> {
        self.best
    }

    /// Consecutive non-promoting epochs so far.
    pub fn streak(&self) -> usize {
        self.non_improving_streak
    }

    /// Record an epoch's outcome and decide whether to continue.
    ///
    /// On [`Promote`](EpochVerdict::Promote): compute the gain over the prior
    /// best, adopt the new fitness as the baseline, and reset the streak — then
    /// halt for diminishing returns if that gain was below `min_gain` (guardrail
    /// 7). Every non-promoting verdict (park, discard, skip) extends the streak
    /// and halts at `max_streak` (guardrail 5). A [`Park`](EpochVerdict::Park)
    /// counts as non-improving: prod did not advance, so the search has not
    /// progressed even though a human may later approve it.
    pub fn record(&mut self, verdict: EpochVerdict, promoted: Option<EpochFitness>) -> LoopControl {
        match verdict {
            EpochVerdict::Promote => {
                let fitness = promoted.expect("a Promote verdict must carry the promoted fitness");
                let gain = match self.best {
                    Some(prev) => fitness.gain_over(&prev),
                    None => u32::MAX,
                };
                self.best = Some(fitness);
                self.non_improving_streak = 0;
                if gain < self.min_gain {
                    LoopControl::Halt(HaltReason::DiminishingReturns)
                } else {
                    LoopControl::Continue
                }
            }
            EpochVerdict::Park | EpochVerdict::Discard(_) | EpochVerdict::SkipFull => {
                self.non_improving_streak += 1;
                if self.non_improving_streak >= self.max_streak {
                    LoopControl::Halt(HaltReason::NonImprovingStreak)
                } else {
                    LoopControl::Continue
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn fit(clean: bool, delivery: u32, completeness: u32) -> EpochFitness {
        EpochFitness { clean, delivery_pct: delivery, completeness_pct: completeness }
    }

    fn gates(proxy: bool, policy: bool, replay: bool) -> EpochGates {
        EpochGates { proxy_promising: proxy, policy_clean: policy, replay_green: replay }
    }

    fn all_good_gates() -> EpochGates {
        gates(true, true, true)
    }

    // ---- fitness ordering --------------------------------------------------

    #[test]
    fn clean_beats_dirty_regardless_of_rates() {
        let clean_poor = fit(true, 10, 10);
        let dirty_great = fit(false, 99, 99);
        assert!(clean_poor.is_better_than(&dirty_great), "correctness is the floor");
        assert!(!dirty_great.is_better_than(&clean_poor));
    }

    #[test]
    fn higher_delivery_then_completeness_breaks_ties() {
        assert!(fit(true, 80, 50).is_better_than(&fit(true, 70, 99)));
        assert!(fit(true, 80, 60).is_better_than(&fit(true, 80, 50)));
        assert!(!fit(true, 80, 50).is_better_than(&fit(true, 80, 50)), "a tie is not an improvement");
    }

    #[test]
    fn gain_is_zero_unless_strictly_better_and_full_on_clean_flip() {
        assert_eq!(fit(true, 80, 50).gain_over(&fit(true, 80, 50)), 0, "tie => no gain");
        assert_eq!(fit(true, 90, 50).gain_over(&fit(true, 80, 50)), 10);
        assert_eq!(fit(true, 10, 10).gain_over(&fit(false, 99, 99)), 100, "dirty->clean is a full gain");
    }

    // ---- decide_epoch: the seven gates -------------------------------------

    #[test]
    fn unpromising_proxy_skips_the_full_epoch() {
        let v = decide_epoch(&fit(true, 50, 50), &fit(true, 99, 99), &gates(false, true, true), HumanPresence::Present);
        assert_eq!(v, EpochVerdict::SkipFull, "guardrail 6: never spend a full epoch on a dead end");
    }

    #[test]
    fn policy_violation_is_a_hard_discard_even_if_fitter() {
        // A rig that is better on every rate but crosses an invariant is refused.
        let v = decide_epoch(&fit(true, 50, 50), &fit(true, 99, 99), &gates(true, false, true), HumanPresence::Present);
        assert_eq!(v, EpochVerdict::Discard(DiscardReason::PolicyViolation));
    }

    #[test]
    fn red_replay_is_a_hard_discard() {
        let v = decide_epoch(&fit(true, 50, 50), &fit(true, 99, 99), &gates(true, true, false), HumanPresence::Present);
        assert_eq!(v, EpochVerdict::Discard(DiscardReason::ReplayRed));
    }

    #[test]
    fn not_better_is_discarded() {
        let v = decide_epoch(&fit(true, 80, 80), &fit(true, 80, 80), &all_good_gates(), HumanPresence::Present);
        assert_eq!(v, EpochVerdict::Discard(DiscardReason::NotBetter), "no strict improvement => drop the rig");
    }

    #[test]
    fn better_and_clean_promotes_with_human_present() {
        let v = decide_epoch(&fit(true, 80, 80), &fit(true, 90, 80), &all_good_gates(), HumanPresence::Present);
        assert_eq!(v, EpochVerdict::Promote);
    }

    #[test]
    fn better_but_unattended_parks_the_promotion() {
        let v = decide_epoch(&fit(true, 80, 80), &fit(true, 90, 80), &all_good_gates(), HumanPresence::Absent);
        assert_eq!(v, EpochVerdict::Park, "guardrail 2: a prod deploy needs a human to own it");
    }

    #[test]
    fn proxy_gate_precedes_policy_gate() {
        // Both proxy and policy fail; the cheaper proxy refusal wins (ordering).
        let v = decide_epoch(&fit(true, 50, 50), &fit(true, 99, 99), &gates(false, false, false), HumanPresence::Present);
        assert_eq!(v, EpochVerdict::SkipFull);
    }

    #[test]
    fn no_non_promote_verdict_implies_a_prod_mutation() {
        // Guardrail 1: only Promote touches prod. Enumerate the refusal paths.
        for v in [
            decide_epoch(&fit(true, 50, 50), &fit(true, 99, 99), &gates(false, true, true), HumanPresence::Present),
            decide_epoch(&fit(true, 50, 50), &fit(true, 99, 99), &gates(true, false, true), HumanPresence::Present),
            decide_epoch(&fit(true, 50, 50), &fit(true, 99, 99), &gates(true, true, false), HumanPresence::Present),
            decide_epoch(&fit(true, 80, 80), &fit(true, 80, 80), &all_good_gates(), HumanPresence::Present),
            decide_epoch(&fit(true, 80, 80), &fit(true, 90, 80), &all_good_gates(), HumanPresence::Absent),
        ] {
            assert_ne!(v, EpochVerdict::Promote, "prod is untouched on every non-promote path");
        }
    }

    // ---- Evolution: stop criteria (guardrails 5 & 7) -----------------------

    #[test]
    fn promotion_adopts_new_baseline_and_resets_streak() {
        let mut ev = Evolution::new(fit(true, 50, 50), 3, 1);
        // Two non-improving epochs build the streak...
        assert_eq!(ev.record(EpochVerdict::Discard(DiscardReason::NotBetter), None), LoopControl::Continue);
        assert_eq!(ev.streak(), 1);
        // ...then a promotion resets it and advances the baseline.
        let better = fit(true, 70, 50);
        assert_eq!(ev.record(EpochVerdict::Promote, Some(better)), LoopControl::Continue);
        assert_eq!(ev.streak(), 0);
        assert_eq!(ev.best(), Some(better));
    }

    #[test]
    fn non_improving_streak_halts_the_loop() {
        let mut ev = Evolution::new(fit(true, 50, 50), 2, 1);
        assert_eq!(ev.record(EpochVerdict::SkipFull, None), LoopControl::Continue);
        assert_eq!(
            ev.record(EpochVerdict::Discard(DiscardReason::NotBetter), None),
            LoopControl::Halt(HaltReason::NonImprovingStreak),
            "guardrail 5: stall halts the search"
        );
    }

    #[test]
    fn park_counts_as_non_improving_for_the_streak() {
        let mut ev = Evolution::new(fit(true, 50, 50), 1, 1);
        // A single park trips the streak: prod did not advance.
        assert_eq!(
            ev.record(EpochVerdict::Park, None),
            LoopControl::Halt(HaltReason::NonImprovingStreak)
        );
    }

    #[test]
    fn small_gain_halts_for_diminishing_returns() {
        let mut ev = Evolution::new(fit(true, 80, 50), 5, 5);
        // A promotion that gains only 2 delivery pts (< min_gain 5) stops the loop.
        let marginal = fit(true, 82, 50);
        assert_eq!(
            ev.record(EpochVerdict::Promote, Some(marginal)),
            LoopControl::Halt(HaltReason::DiminishingReturns),
            "guardrail 7: a gain below the margin is not worth more epochs"
        );
        // ...but the baseline still advanced — the gain was real, just small.
        assert_eq!(ev.best(), Some(marginal));
    }

    #[test]
    fn worthwhile_gain_continues() {
        let mut ev = Evolution::new(fit(true, 80, 50), 5, 5);
        let big = fit(true, 95, 50); // +15 >= min_gain 5
        assert_eq!(ev.record(EpochVerdict::Promote, Some(big)), LoopControl::Continue);
    }

    #[test]
    fn max_streak_is_clamped_to_at_least_one() {
        let mut ev = Evolution::new(fit(true, 50, 50), 0, 1);
        // max_streak 0 would never run; clamped to 1, the first non-improver halts.
        assert_eq!(
            ev.record(EpochVerdict::SkipFull, None),
            LoopControl::Halt(HaltReason::NonImprovingStreak)
        );
    }
}
