//! `gt-deacon` — town shutdown / drain coordination (Paso 9.D, hq-92z9).
//!
//! Tracks an in-flight set of `DrainItem`s the operator (or a SIGTERM-driven producer)
//! wants to finish before the town exits. `BeginDrain` flips the global drain flag;
//! `TrackItem`/`FinishItem` maintain the pending set; once `draining && pending.is_empty()`
//! the actor emits `DrainComplete`. Same actor + commands + events + state + repo pattern
//! as `gt-merge` / `gt-sheriff`.
//!
//! Aislamiento: depende solo del kernel (`gt-events`). Cross-domain wiring (tracking a
//! `merge.ready` so the deacon waits for it) lives in the composition root.

pub mod actor;
pub mod commands;
pub mod deacon;
mod events;
mod repo;
mod state;

pub use actor::{spawn, spawn_hydrated, DeaconHandle, DeaconMsg};
pub use commands::{BeginDrain, DeaconCommand, FinishItem, TrackItem};
pub use events::DeaconEvent;
pub use repo::{DeaconRepository, InMemoryDeaconRepo};
pub use state::{DeaconState, DrainItem};
