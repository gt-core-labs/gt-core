//! Pure stale-lease detector. Ages enter as data (the edge tick supplies `now_secs`);
//! this never touches the wall clock, so replay reconstructs identical decisions.
//!
//! Generalization of the SLA-as-event idea from `docs/06-observability.md`: an absence
//! (no heartbeat for N seconds) becomes a first-class event the bus can route.

use crate::state::Lease;

/// Filter the live-lease snapshot down to the ones whose `last_seen_secs` is older than
/// `timeout`. Strict `>` (so `age == timeout` is not yet expired — same convention as
/// `gt-scheduling::expectations::expired`).
///
/// Input is a snapshot (`Vec` from `LeaseTracker::snapshot`) rather than a live tracker,
/// to keep this function reusable from replay tests and from the actor without sharing
/// mutable state.
pub fn expired_leases(leases: &[Lease], now_secs: u64, timeout_secs: u64) -> Vec<Lease> {
    leases
        .iter()
        .filter(|l| now_secs.saturating_sub(l.last_seen_secs) > timeout_secs)
        .cloned()
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn lease(bead: &str, worker: &str, priority: u8, last: u64) -> Lease {
        Lease {
            bead: bead.into(),
            worker: worker.into(),
            priority,
            last_seen_secs: last,
        }
    }

    #[test]
    fn detects_stale_only() {
        let leases = vec![
            lease("fresh", "w1", 0, 90),  // age 10 < 60: alive
            lease("edge", "w2", 1, 40),   // age 60 == timeout: NOT expired (strict >)
            lease("stale", "w3", 2, 30),  // age 70 > 60: expired
        ];
        let out = expired_leases(&leases, 100, 60);
        let ids: Vec<&str> = out.iter().map(|l| l.bead.as_str()).collect();
        assert_eq!(ids, vec!["stale"]);
    }

    #[test]
    fn handles_clock_skew_safely() {
        // If now_secs < last_seen_secs (replay edge case) saturating_sub yields 0; never
        // mistakenly flags a fresh lease as expired.
        let leases = vec![lease("future", "w", 0, 200)];
        assert!(expired_leases(&leases, 100, 60).is_empty());
    }

    #[test]
    fn empty_snapshot_returns_empty() {
        assert!(expired_leases(&[], 999, 1).is_empty());
    }
}
