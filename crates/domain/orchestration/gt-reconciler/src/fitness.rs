//! Fitness function + outcome tracking (hq-auto.6) — the self-improve signal.
//!
//! The circuit-breaker ([`health`](crate::health)) answers *is the graph sound
//! enough to keep running?* — a floor. Fitness answers the dual, higher question:
//! *is the autonomous loop getting better?* It folds the same single pass's
//! observations into the handful of rates that say **what is good**, so the
//! meta-loop (`hq-auto.7`) has a scalar signal to optimize and a human on-loop
//! has a one-glance scorecard.
//!
//! All inputs are counts the reconciler already computes in a pass (closed/
//! delivered tasks, present vs Cargo-implied edges, broken-provenance beads,
//! beads a sounder graph un-hides). No new query, no store dependency: like
//! [`health::evaluate`](crate::health::evaluate), [`Fitness::assess`] is a total
//! function of [`FitnessInputs`], unit-tested without Dolt.
//!
//! ## What is and isn't measured here
//!
//! Measured now, from data on hand:
//! - **delivery-rate** — closed tasks that actually carry a `delivered_sha`. A
//!   closed task with no verifiable delivery is work claimed but not banked.
//! - **graph-completeness** — recorded `depends_on` edges as a fraction of the
//!   full Cargo-implied set (present / present+missing). The "auto-derivable
//!   fraction" the bead asks for: it grows toward 1.0 as ports land and the
//!   reconciler's §A edges get applied. *Higher is better.*
//! - **traps-claimed** — closed beads whose recorded delivery sha resolves in no
//!   repo (broken provenance: a bead reported done that cannot be verified).
//!   *Zero is the target.*
//! - **false-hidden** — open beads a *sounder* graph un-hides (they were ready
//!   but an incomplete dependency model hid them from the ready frontier). The
//!   reconciler already estimates this as beads-unhidden. *Zero is the target.*
//!
//! Intentionally **omitted** rather than faked (their source has not landed —
//! same discipline as [`health::HealthMetrics`](crate::health::HealthMetrics)):
//! - **rework rate** (re-open / superseded) — needs bead *status history*, which
//!   the reconciler's [`Bead`](crate::model::Bead) projection does not carry.
//! - **replay-green** — the byte-for-byte replay gate is a separate CI signal not
//!   visible to this job.
//! - **trend** (the "growing" in growing-fraction) — a single pass is one point
//!   sample; comparing samples over time is cross-run state owned by the
//!   meta-loop (`hq-auto.7`). This module emits the sample; .7 persists the
//!   series. Add these fields here when their source arrives, and extend the
//!   scorecard in step.

/// A `numerator / denominator` rate, kept as integers so the report can show
/// both the raw fraction and a percentage, and so a zero denominator is an
/// explicit "not applicable" rather than a `NaN`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Ratio {
    pub num: usize,
    pub den: usize,
}

impl Ratio {
    /// The rate as a fraction in `0.0..=1.0`, or `None` when the denominator is
    /// zero (nothing to rate — e.g. no closed tasks yet).
    pub fn fraction(&self) -> Option<f64> {
        (self.den != 0).then(|| self.num as f64 / self.den as f64)
    }

    /// The rate as a whole-number percentage, or `None` when not applicable.
    pub fn percent(&self) -> Option<u32> {
        self.fraction().map(|f| (f * 100.0).round() as u32)
    }
}

impl std::fmt::Display for Ratio {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self.percent() {
            Some(p) => write!(f, "{}/{} ({}%)", self.num, self.den, p),
            None => write!(f, "{}/{} (n/a)", self.num, self.den),
        }
    }
}

/// Raw observations from one reconciler pass. Every field is a plain count the
/// binary already has after §A/§B; this struct just names them.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub struct FitnessInputs {
    /// Closed beads of type `task`.
    pub closed_tasks: usize,
    /// Of those, the ones carrying a non-empty `delivered_sha`.
    pub delivered_tasks: usize,
    /// `depends_on` edges currently recorded across all beads.
    pub dep_edges_present: usize,
    /// Cargo-implied edges the reconciler found *missing* this pass (§A).
    pub dep_edges_missing: usize,
    /// Closed beads whose recorded delivery sha resolves in no repo.
    pub traps_claimed: usize,
    /// Open beads a sounder graph un-hides (reconciler's beads-unhidden estimate).
    pub false_hidden: usize,
}

/// The aggregated scorecard: the self-improve signal for the meta-loop and a
/// human-readable summary for on-loop review.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Fitness {
    /// Closed tasks that actually banked a delivery. Higher is better.
    pub delivery_rate: Ratio,
    /// Recorded edges / full Cargo-implied set. The auto-derivable fraction;
    /// higher (→ 1.0) is better.
    pub graph_completeness: Ratio,
    /// Broken-provenance closed beads. Zero is the target.
    pub traps_claimed: usize,
    /// Wrongly-hidden ready beads. Zero is the target.
    pub false_hidden: usize,
}

impl Fitness {
    /// Fold raw pass counts into the scorecard.
    pub fn assess(i: &FitnessInputs) -> Self {
        Fitness {
            delivery_rate: Ratio { num: i.delivered_tasks, den: i.closed_tasks },
            graph_completeness: Ratio {
                num: i.dep_edges_present,
                // present + still-missing = the full Cargo-implied edge set.
                den: i.dep_edges_present + i.dep_edges_missing,
            },
            traps_claimed: i.traps_claimed,
            false_hidden: i.false_hidden,
        }
    }

    /// Whether the loop is banking correct work: no unverifiable deliveries and
    /// no wrongly-hidden beads. The two count-metrics are hard correctness
    /// signals (target zero); the rates are trend gauges, not pass/fail. A clean
    /// pass is the precondition the meta-loop wants before it trusts the rates.
    pub fn is_clean(&self) -> bool {
        self.traps_claimed == 0 && self.false_hidden == 0
    }

    /// One status token for logs: `CLEAN` when no correctness signal tripped,
    /// else `DIRTY`.
    pub fn label(&self) -> &'static str {
        if self.is_clean() {
            "CLEAN"
        } else {
            "DIRTY"
        }
    }

    /// The scorecard as human-readable lines (one metric per line), for the
    /// reconciler's `── fitness ──` block.
    pub fn report_lines(&self) -> Vec<String> {
        vec![
            format!("delivery-rate       : {}", self.delivery_rate),
            format!("graph-completeness  : {}", self.graph_completeness),
            format!("traps-claimed       : {} (target 0)", self.traps_claimed),
            format!("false-hidden        : {} (target 0)", self.false_hidden),
        ]
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn ratio_handles_zero_denominator() {
        let r = Ratio { num: 0, den: 0 };
        assert_eq!(r.fraction(), None, "no data => not a number, explicitly None");
        assert_eq!(r.percent(), None);
        assert_eq!(r.to_string(), "0/0 (n/a)");
    }

    #[test]
    fn ratio_formats_percentage() {
        let r = Ratio { num: 3, den: 4 };
        assert_eq!(r.fraction(), Some(0.75));
        assert_eq!(r.percent(), Some(75));
        assert_eq!(r.to_string(), "3/4 (75%)");
    }

    #[test]
    fn perfect_pass_is_clean_and_fully_rated() {
        let i = FitnessInputs {
            closed_tasks: 10,
            delivered_tasks: 10,
            dep_edges_present: 40,
            dep_edges_missing: 0,
            traps_claimed: 0,
            false_hidden: 0,
        };
        let f = Fitness::assess(&i);
        assert_eq!(f.delivery_rate.percent(), Some(100));
        assert_eq!(f.graph_completeness.percent(), Some(100), "no missing edges => complete");
        assert!(f.is_clean());
        assert_eq!(f.label(), "CLEAN");
    }

    #[test]
    fn missing_edges_lower_completeness() {
        // 40 present, 10 missing => 40/50 = 80%.
        let i = FitnessInputs {
            dep_edges_present: 40,
            dep_edges_missing: 10,
            ..Default::default()
        };
        let f = Fitness::assess(&i);
        assert_eq!(f.graph_completeness.percent(), Some(80));
    }

    #[test]
    fn empty_graph_is_complete_not_nan() {
        // No edges present and none missing: the (empty) implied graph is fully
        // realized. den would be 0 -> reported n/a, never NaN.
        let f = Fitness::assess(&FitnessInputs::default());
        assert_eq!(f.graph_completeness.fraction(), None);
        assert_eq!(f.delivery_rate.fraction(), None);
        assert!(f.is_clean(), "nothing observed yet is trivially clean");
    }

    #[test]
    fn a_trap_makes_the_pass_dirty() {
        let i = FitnessInputs { traps_claimed: 1, ..Default::default() };
        let f = Fitness::assess(&i);
        assert!(!f.is_clean());
        assert_eq!(f.label(), "DIRTY");
    }

    #[test]
    fn false_hidden_makes_the_pass_dirty() {
        let i = FitnessInputs { false_hidden: 2, ..Default::default() };
        let f = Fitness::assess(&i);
        assert!(!f.is_clean(), "a wrongly-hidden ready bead is a correctness defect");
    }

    #[test]
    fn partial_delivery_rate() {
        let i = FitnessInputs { closed_tasks: 8, delivered_tasks: 6, ..Default::default() };
        let f = Fitness::assess(&i);
        assert_eq!(f.delivery_rate.percent(), Some(75));
        assert_eq!(f.report_lines().len(), 4, "scorecard reports every metric");
    }
}
