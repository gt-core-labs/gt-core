//! `gt-witness` — patrol vivo + escalation (Paso 9.D, hq-92z9).
//!
//! Tracks worker-level "stuck" state: an operator (or a cross-domain reaction at the
//! composition root) registers a `Watch` per worker; periodic `Tick`s advance
//! `last_seen_secs` and raise an escalation once `age_secs >= threshold_secs`. Cleared by
//! an explicit operator command. Same actor + commands + events + state + repo pattern as
//! `gt-sheriff`.
//!
//! Relationship to `gt-patrol`: patrol owns lease lifecycle (register, heartbeat, expire)
//! and is the producer of `PatrolEvent::LeaseExpired`. Witness sits **above** that as a
//! pure escalation observer — it does not duplicate lease detection. The cross-domain wire
//! (`Patrol::LeaseExpired -> Witness::Tick` or `Watch`) is deferred to the composition
//! root and is **not** added in this commit so the per-role fill stays isolated; the
//! reactor arm can be enabled later without touching this crate.

pub mod actor;
pub mod commands;
pub mod witness;
mod events;
mod repo;
mod state;

pub use actor::{spawn, spawn_hydrated, WitnessHandle, WitnessMsg};
pub use commands::{ClearTarget, TickTarget, WatchTarget, WitnessCommand};
pub use events::WitnessEvent;
pub use repo::{InMemoryWitnessRepo, WitnessRepository};
pub use state::{StuckTarget, WitnessState};
