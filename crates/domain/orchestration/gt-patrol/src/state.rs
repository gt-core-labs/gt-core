use std::collections::HashMap;

use crate::events::PatrolEvent;

/// One live lease: dispatched bead waiting for its polecat to keep heart-beating.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Lease {
    pub bead: String,
    pub worker: String,
    pub priority: u8,
    /// Last observation that proves the worker is alive (registration or heartbeat).
    pub last_seen_secs: u64,
}

/// In-memory tracker. The dispatcher owns leases via the patrol actor; readers go
/// through snapshots, never through shared mutable state. Pure: no clock reads.
#[derive(Debug, Default)]
pub struct LeaseTracker {
    /// Keyed by bead id. A polecat (`worker`) can hold many beads in theory; the canonical
    /// identity for a lease is the bead, so a worker heartbeat refreshes every lease this
    /// worker owns.
    by_bead: HashMap<String, Lease>,
}

impl LeaseTracker {
    pub fn register(&mut self, bead: String, worker: String, priority: u8, now_secs: u64) {
        self.by_bead.insert(
            bead.clone(),
            Lease { bead, worker, priority, last_seen_secs: now_secs },
        );
    }

    /// Refresh every lease owned by `worker`. Returns how many got refreshed (useful for
    /// tests and metrics; ignored by the actor).
    pub fn heartbeat(&mut self, worker: &str, now_secs: u64) -> usize {
        let mut n = 0;
        for lease in self.by_bead.values_mut() {
            if lease.worker == worker {
                lease.last_seen_secs = now_secs;
                n += 1;
            }
        }
        n
    }

    pub fn close(&mut self, bead: &str) -> Option<Lease> {
        self.by_bead.remove(bead)
    }

    pub fn get(&self, bead: &str) -> Option<&Lease> {
        self.by_bead.get(bead)
    }

    pub fn len(&self) -> usize {
        self.by_bead.len()
    }

    pub fn is_empty(&self) -> bool {
        self.by_bead.is_empty()
    }

    /// Snapshot of live leases ordered by bead id — stable for the pure detector and for
    /// deterministic event emission order (`docs/06`: replay must be byte-identical).
    pub fn snapshot(&self) -> Vec<Lease> {
        let mut out: Vec<Lease> = self.by_bead.values().cloned().collect();
        out.sort_by(|a, b| a.bead.cmp(&b.bead));
        out
    }

    /// Rebuild a live tracker from the replay reducer's snapshot (boot hydration, hq-8iur.1).
    /// The event log is authoritative; the actor seeds itself from `replay_gt` so restart
    /// keeps live leases without re-emitting events.
    pub fn from_state(state: &PatrolState) -> Self {
        let mut tracker = LeaseTracker::default();
        for l in state.tracker.leases() {
            tracker.by_bead.insert(l.bead.clone(), l.clone());
        }
        tracker
    }
}

/// Replay reducer: rebuilds the live-lease set from the log. Pure, total.
#[derive(Debug, Default, PartialEq, Eq)]
pub struct PatrolState {
    pub tracker: LeaseTrackerSnapshot,
    pub expired: Vec<(String, String)>,
}

impl PatrolState {
    pub fn apply(&mut self, event: &PatrolEvent) {
        match event {
            PatrolEvent::LeaseRegistered { bead, worker, priority, now_secs } => {
                self.tracker.upsert(bead.clone(), worker.clone(), *priority, *now_secs);
            }
            PatrolEvent::Heartbeat { worker, now_secs } => {
                self.tracker.touch_by_worker(worker, *now_secs);
            }
            PatrolEvent::LeaseClosed { bead } => {
                self.tracker.remove(bead);
            }
            PatrolEvent::LeaseExpired { bead, worker, .. } => {
                self.expired.push((bead.clone(), worker.clone()));
                // Expiration removes the lease: the composition root re-enqueues, which
                // produces a fresh LeaseRegistered when the next dispatch happens.
                self.tracker.remove(bead);
            }
        }
    }
}

/// Serializable-friendly mirror of `LeaseTracker` used by the replay reducer. Kept as a
/// sorted `Vec` of leases so `PatrolState` derives `PartialEq` cleanly across runs.
#[derive(Debug, Default, PartialEq, Eq)]
pub struct LeaseTrackerSnapshot {
    leases: Vec<Lease>,
}

impl LeaseTrackerSnapshot {
    fn pos(&self, bead: &str) -> Option<usize> {
        self.leases.binary_search_by(|l| l.bead.as_str().cmp(bead)).ok()
    }

    fn upsert(&mut self, bead: String, worker: String, priority: u8, now_secs: u64) {
        match self.leases.binary_search_by(|l| l.bead.as_str().cmp(&bead)) {
            Ok(i) => {
                self.leases[i] = Lease { bead, worker, priority, last_seen_secs: now_secs };
            }
            Err(i) => {
                self.leases.insert(i, Lease { bead, worker, priority, last_seen_secs: now_secs });
            }
        }
    }

    fn touch_by_worker(&mut self, worker: &str, now_secs: u64) {
        for lease in self.leases.iter_mut() {
            if lease.worker == worker {
                lease.last_seen_secs = now_secs;
            }
        }
    }

    fn remove(&mut self, bead: &str) {
        if let Some(i) = self.pos(bead) {
            self.leases.remove(i);
        }
    }

    pub fn leases(&self) -> &[Lease] {
        &self.leases
    }

    pub fn len(&self) -> usize {
        self.leases.len()
    }

    pub fn is_empty(&self) -> bool {
        self.leases.is_empty()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn tracker_register_heartbeat_close() {
        let mut t = LeaseTracker::default();
        t.register("b1".into(), "w1".into(), 0, 100);
        t.register("b2".into(), "w1".into(), 1, 100);
        t.register("b3".into(), "w2".into(), 1, 100);

        assert_eq!(t.heartbeat("w1", 150), 2, "w1 owns b1 and b2");
        assert_eq!(t.get("b1").unwrap().last_seen_secs, 150);
        assert_eq!(t.get("b2").unwrap().last_seen_secs, 150);
        assert_eq!(t.get("b3").unwrap().last_seen_secs, 100, "w2 untouched");

        let closed = t.close("b2").unwrap();
        assert_eq!(closed.bead, "b2");
        assert!(t.get("b2").is_none());
        assert_eq!(t.len(), 2);
    }

    #[test]
    fn snapshot_is_sorted_by_bead() {
        let mut t = LeaseTracker::default();
        t.register("c".into(), "w".into(), 0, 1);
        t.register("a".into(), "w".into(), 0, 1);
        t.register("b".into(), "w".into(), 0, 1);
        let snap: Vec<_> = t.snapshot().into_iter().map(|l| l.bead).collect();
        assert_eq!(snap, vec!["a", "b", "c"]);
    }

    #[test]
    fn replay_reducer_rebuilds_live_set() {
        let log = vec![
            PatrolEvent::LeaseRegistered { bead: "b1".into(), worker: "w1".into(), priority: 0, now_secs: 10 },
            PatrolEvent::LeaseRegistered { bead: "b2".into(), worker: "w2".into(), priority: 1, now_secs: 20 },
            PatrolEvent::Heartbeat { worker: "w1".into(), now_secs: 50 },
            PatrolEvent::LeaseClosed { bead: "b1".into() },
            PatrolEvent::LeaseExpired { bead: "b2".into(), worker: "w2".into(), priority: 1 },
        ];
        let mut s = PatrolState::default();
        for e in &log {
            s.apply(e);
        }
        assert!(s.tracker.is_empty(), "both leases were closed/expired");
        assert_eq!(s.expired, vec![("b2".into(), "w2".into())]);
    }
}
