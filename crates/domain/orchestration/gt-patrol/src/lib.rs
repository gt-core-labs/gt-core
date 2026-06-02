//! `gt-patrol` — domain that turns *absences* into first-class events (Paso 6 of
//! `docs/08-getting-started.md`).
//!
//! Closes the lease loop opened by `gt-scheduling`: a `dispatched` bead whose polecat
//! stops heart-beating must come back to `pending`. Detection here is **pure** (compares
//! ages given as data, never reads the clock) so it stays replay-able under the
//! determinism rule (`docs/06-observability.md`). The actor is the SQS-style
//! visibility-timeout, expressed as the heartbeat machinery already in place.
//!
//! Cross-domain handoff: this crate does NOT call `gt-scheduling` or `gt-beads` directly.
//! It emits `PatrolEvent::LeaseExpired { bead, worker, priority }`; the composition root
//! (or a sync bus handler) reacts by calling `repo.cas_release` and re-enqueuing — same
//! integration-by-events pattern used in Paso 5 (`docs/05-queues.md`).

pub mod actor;
pub mod commands;
pub mod expectations;
mod events;
mod repo;
mod state;

pub use commands::{CloseLease, Heartbeat, PatrolCommand, RegisterLease, Tick};
pub use events::PatrolEvent;
pub use repo::{InMemoryPatrolRepo, PatrolRepository};
pub use state::{Lease, LeaseTracker, PatrolState};
