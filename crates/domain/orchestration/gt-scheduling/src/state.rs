use crate::events::SchedEvent;

/// Un bead esperando dispatch.
#[derive(Debug, Clone, PartialEq, Eq)]
struct Queued {
    id: String,
    priority: u8, // 0 = P0 = más alta
}

/// Cola de trabajo en memoria (la durabilidad real vive en los beads `pending` de Dolt).
/// `pop_highest` saca el de mayor prioridad (menor número), estable por orden de llegada.
#[derive(Debug, Default)]
pub struct Queue {
    items: Vec<Queued>,
}

impl Queue {
    pub fn enqueue(&mut self, id: impl Into<String>, priority: u8) {
        self.items.push(Queued { id: id.into(), priority });
    }

    pub fn pop_highest(&mut self) -> Option<String> {
        let idx = self
            .items
            .iter()
            .enumerate()
            .min_by_key(|(i, q)| (q.priority, *i))
            .map(|(i, _)| i)?;
        Some(self.items.remove(idx).id)
    }

    /// Drop a queued bead by id (it is no longer pending). Returns whether it was present.
    pub fn remove(&mut self, id: &str) -> bool {
        if let Some(idx) = self.items.iter().position(|q| q.id == id) {
            self.items.remove(idx);
            true
        } else {
            false
        }
    }

    pub fn len(&self) -> usize {
        self.items.len()
    }

    pub fn is_empty(&self) -> bool {
        self.items.is_empty()
    }
}

/// Backpressure: el dispatcher solo desencola si hay capacidad (`max_polecats`).
#[derive(Debug)]
pub struct CapacityGovernor {
    max: usize,
    in_flight: usize,
}

impl CapacityGovernor {
    pub fn new(max: usize) -> Self {
        Self { max, in_flight: 0 }
    }

    pub fn can_dispatch(&self) -> bool {
        self.in_flight < self.max
    }

    pub fn acquire(&mut self) {
        self.in_flight += 1;
    }

    pub fn release(&mut self) {
        self.in_flight = self.in_flight.saturating_sub(1);
    }

    pub fn in_flight(&self) -> usize {
        self.in_flight
    }
}

/// The dispatcher's owned in-memory state: the pending [`Queue`] plus the [`CapacityGovernor`].
/// This is the `Command::State` the frontier commands ([`crate::commands`]) validate/execute
/// against — pure and sync, no clock, no I/O. The actor owns one of these; the CAS-claim and
/// the event emission stay at the async edge (the actor), never inside the command.
#[derive(Debug)]
pub struct SchedCore {
    pub queue: Queue,
    pub gov: CapacityGovernor,
}

impl SchedCore {
    pub fn new(max: usize) -> Self {
        Self {
            queue: Queue::default(),
            gov: CapacityGovernor::new(max),
        }
    }
}

/// Reducer de replay: reconstruye qué se despachó/falló/expiró desde el log (puro, total).
#[derive(Debug, Default, PartialEq, Eq)]
pub struct SchedState {
    pub enqueued: Vec<String>,
    pub dispatched: Vec<(String, String)>,
    pub failed: Vec<String>,
    pub timed_out: Vec<String>,
}

impl SchedState {
    pub fn apply(&mut self, event: &SchedEvent) {
        match event {
            SchedEvent::Enqueue { bead, .. } => self.enqueued.push(bead.clone()),
            SchedEvent::Dispatched { bead, worker } => {
                self.dispatched.push((bead.clone(), worker.clone()))
            }
            SchedEvent::DispatchFailed { bead, .. } => self.failed.push(bead.clone()),
            SchedEvent::DispatchTimeout { bead } => self.timed_out.push(bead.clone()),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn pop_highest_respects_priority_then_arrival() {
        let mut q = Queue::default();
        q.enqueue("p2", 2);
        q.enqueue("p0", 0);
        q.enqueue("p1a", 1);
        q.enqueue("p1b", 1);
        assert_eq!(q.pop_highest().as_deref(), Some("p0")); // P0 primero
        assert_eq!(q.pop_highest().as_deref(), Some("p1a")); // empate P1 → orden llegada
        assert_eq!(q.pop_highest().as_deref(), Some("p1b"));
        assert_eq!(q.pop_highest().as_deref(), Some("p2"));
        assert!(q.is_empty());
    }

    #[test]
    fn governor_caps_in_flight() {
        let mut g = CapacityGovernor::new(2);
        assert!(g.can_dispatch());
        g.acquire();
        g.acquire();
        assert!(!g.can_dispatch());
        g.release();
        assert!(g.can_dispatch());
    }
}
