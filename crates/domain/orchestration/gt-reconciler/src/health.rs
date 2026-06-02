//! Circuit-breakers over reconciler health metrics (hq-auto.5).
//!
//! The reconciler is the **Monitor** of the autonomous loop: in one pass it
//! already quantifies how far the tracked dependency-graph has drifted from the
//! authoritative Cargo graph + git history (cycles, crates with no producer
//! bead, recorded shas that resolve nowhere, surface paths gone from HEAD).
//!
//! On its own those are just printed counts. This module turns them into a
//! **circuit-breaker**: thresholds that classify the graph as healthy,
//! degraded, or — past a hard ceiling — bad enough that autonomous *generation*
//! must HALT and a human be escalated to, so a slightly-wrong model cannot
//! amplify itself over many unattended cycles (compounding drift).
//!
//! Pure and store-free: [`evaluate`] is a total function of
//! [`HealthMetrics`] + [`Thresholds`], unit-tested without a Dolt connection.
//! The binary collects the metrics it already computes, calls [`evaluate`], and
//! lets the [`Health`] verdict drive its exit code (see `main`).

/// Quantified graph-health signals observed in a single reconciler pass.
///
/// Every field is "lower is better"; `0` everywhere is a perfectly sound graph.
/// Fields the reconciler does not yet measure (rework rate, replay status) are
/// intentionally omitted rather than faked — add them here when their source
/// lands, and extend [`Thresholds`] in step.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub struct HealthMetrics {
    /// `depends_on` edges the Cargo graph implies but that were skipped because
    /// adding them would close a cycle — a direct contradiction between the
    /// model and the code. The most serious signal.
    pub cyclic_edges: usize,
    /// Crates consumed by some bead's surface but with no producer bead — the
    /// graph is structurally incomplete and readiness may be wrong.
    pub no_producer_crates: usize,
    /// Recorded close-shas that resolve in no configured repo — broken
    /// provenance, a closed bead whose delivery cannot be verified.
    pub missing_shas: usize,
    /// Non-`planned` surface paths absent at HEAD — beads pointing at code that
    /// no longer exists.
    pub stale_surfaces: usize,
}

/// A two-step ceiling for one metric: `value <= warn` is healthy, `warn < value
/// <= halt` is degraded, `value > halt` trips the breaker.
///
/// `warn == halt` collapses the degraded band (any exceedance halts);
/// `warn < halt` leaves room to warn before halting.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Band {
    pub warn: usize,
    pub halt: usize,
}

impl Band {
    /// Classify a metric value against this band.
    fn classify(self, value: usize) -> Level {
        debug_assert!(self.warn <= self.halt, "Band.warn must be <= halt");
        if value > self.halt {
            Level::Halt
        } else if value > self.warn {
            Level::Degraded
        } else {
            Level::Healthy
        }
    }
}

/// Severity of a single metric against its [`Band`]. Ordered so the worst level
/// across all metrics is `max`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
enum Level {
    Healthy,
    Degraded,
    Halt,
}

/// Per-metric thresholds. The defaults treat any cycle as immediately worth a
/// warning (cycles should never appear), with a small halt ceiling, and are
/// more tolerant of the incompleteness signals that legitimately churn while
/// ports land.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Thresholds {
    pub cyclic_edges: Band,
    pub no_producer_crates: Band,
    pub missing_shas: Band,
    pub stale_surfaces: Band,
}

impl Default for Thresholds {
    fn default() -> Self {
        Thresholds {
            // A cycle is a hard contradiction: warn on the first, halt past two.
            cyclic_edges: Band { warn: 0, halt: 2 },
            // No-producer crates churn while hq-core-port lands; tolerate a few.
            no_producer_crates: Band { warn: 2, halt: 8 },
            // A missing sha is broken provenance: warn early, halt past a handful.
            missing_shas: Band { warn: 0, halt: 5 },
            // Stale surfaces are usually fixable bead edits; halt only if many.
            stale_surfaces: Band { warn: 3, halt: 10 },
        }
    }
}

/// The circuit-breaker verdict. `Degraded`/`Halt` carry one human-readable
/// reason per tripped metric, worst-first by the order metrics are checked.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Health {
    Healthy,
    Degraded(Vec<String>),
    Halt(Vec<String>),
}

impl Health {
    /// Whether autonomous generation must stop. Only [`Health::Halt`] does.
    pub fn should_halt(&self) -> bool {
        matches!(self, Health::Halt(_))
    }

    /// A one-line status token for logs: `HEALTHY` / `DEGRADED` / `HALT`.
    pub fn label(&self) -> &'static str {
        match self {
            Health::Healthy => "HEALTHY",
            Health::Degraded(_) => "DEGRADED",
            Health::Halt(_) => "HALT",
        }
    }

    /// The reasons behind a non-healthy verdict (empty when healthy).
    pub fn reasons(&self) -> &[String] {
        match self {
            Health::Healthy => &[],
            Health::Degraded(r) | Health::Halt(r) => r,
        }
    }
}

/// Classify graph health by checking each metric against its band and folding
/// to the worst level. The verdict's reasons list every metric at or above the
/// degraded band (so a `Halt` verdict still surfaces the lesser degradations).
pub fn evaluate(m: &HealthMetrics, t: &Thresholds) -> Health {
    // (name, value, band) — declaration order is the reason-list order.
    let checks: [(&str, usize, Band); 4] = [
        ("cyclic depends_on edges", m.cyclic_edges, t.cyclic_edges),
        ("crates with no producer bead", m.no_producer_crates, t.no_producer_crates),
        ("recorded shas missing from all repos", m.missing_shas, t.missing_shas),
        ("stale surface paths (absent at HEAD)", m.stale_surfaces, t.stale_surfaces),
    ];

    let mut worst = Level::Healthy;
    let mut reasons: Vec<String> = Vec::new();
    for (name, value, band) in checks {
        let level = band.classify(value);
        if level > worst {
            worst = level;
        }
        match level {
            Level::Healthy => {}
            Level::Degraded => reasons.push(format!(
                "{name}: {value} (degraded, warn>{} halt>{})",
                band.warn, band.halt
            )),
            Level::Halt => reasons.push(format!(
                "{name}: {value} (HALT, exceeds ceiling {})",
                band.halt
            )),
        }
    }

    match worst {
        Level::Healthy => Health::Healthy,
        Level::Degraded => Health::Degraded(reasons),
        Level::Halt => Health::Halt(reasons),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn all_zero_is_healthy() {
        let v = evaluate(&HealthMetrics::default(), &Thresholds::default());
        assert_eq!(v, Health::Healthy);
        assert!(!v.should_halt());
        assert_eq!(v.label(), "HEALTHY");
    }

    #[test]
    fn one_cycle_degrades_but_does_not_halt() {
        let m = HealthMetrics { cyclic_edges: 1, ..Default::default() };
        let v = evaluate(&m, &Thresholds::default());
        assert!(matches!(v, Health::Degraded(_)));
        assert!(!v.should_halt());
        assert_eq!(v.reasons().len(), 1);
    }

    #[test]
    fn many_cycles_halt() {
        let m = HealthMetrics { cyclic_edges: 3, ..Default::default() };
        let v = evaluate(&m, &Thresholds::default());
        assert!(v.should_halt());
        assert_eq!(v.label(), "HALT");
    }

    #[test]
    fn halt_verdict_still_lists_lesser_degradations() {
        // cyclic halts (3 > 2); stale degrades (5 in (3,10]).
        let m = HealthMetrics {
            cyclic_edges: 3,
            stale_surfaces: 5,
            ..Default::default()
        };
        let v = evaluate(&m, &Thresholds::default());
        assert!(v.should_halt());
        assert_eq!(v.reasons().len(), 2, "both the halt and the degradation are reported");
    }

    #[test]
    fn worst_metric_decides_verdict() {
        // no-producer degrades (5 in (2,8]); nothing halts.
        let m = HealthMetrics { no_producer_crates: 5, ..Default::default() };
        assert!(matches!(
            evaluate(&m, &Thresholds::default()),
            Health::Degraded(_)
        ));
        // push it past the ceiling → halt.
        let m = HealthMetrics { no_producer_crates: 9, ..Default::default() };
        assert!(evaluate(&m, &Thresholds::default()).should_halt());
    }

    #[test]
    fn band_collapses_when_warn_equals_halt() {
        let band = Band { warn: 0, halt: 0 };
        assert_eq!(band.classify(0), Level::Healthy);
        // No degraded band: the first exceedance halts.
        assert_eq!(band.classify(1), Level::Halt);
    }

    #[test]
    fn custom_thresholds_are_honoured() {
        let m = HealthMetrics { missing_shas: 1, ..Default::default() };
        // Default: warn 0 → degraded. Relaxed: tolerate 1.
        assert!(matches!(
            evaluate(&m, &Thresholds::default()),
            Health::Degraded(_)
        ));
        let relaxed = Thresholds {
            missing_shas: Band { warn: 2, halt: 5 },
            ..Thresholds::default()
        };
        assert_eq!(evaluate(&m, &relaxed), Health::Healthy);
    }
}
