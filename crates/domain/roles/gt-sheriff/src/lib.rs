//! `gt-sheriff` — watchdog domain (Paso 9.D, hq-92z9).
//!
//! Observes event-kind strings flowing through `gt-plugin`'s observer relay (hq-evks),
//! counts occurrences per registered `Watch`, and emits `Raise` once a watch crosses its
//! threshold. Cleared by an explicit operator command. Same actor + commands + events +
//! state + repo pattern as `gt-merge`.
//!
//! Plugin integration: [`SheriffPlugin`] impls `gt_plugin::Plugin` and forwards each
//! observed [`gt_eventlog::EventRecord`] into the actor — the **real** plugin that replaces
//! `gt_plugin::SheriffPlugin`'s stub per hq-evks' handoff note (composition wiring is
//! unchanged; only the registered impl changes).

pub mod actor;
pub mod commands;
pub mod plugin;
pub mod sheriff;
mod events;
mod repo;
mod state;

pub use actor::{spawn, spawn_hydrated, SheriffHandle, SheriffMsg};
pub use commands::{ClearWatch, ObserveWatch, RegisterWatch, SheriffCommand};
pub use events::SheriffEvent;
pub use plugin::SheriffPlugin;
pub use repo::{InMemorySheriffRepo, SheriffRepository};
pub use state::{SheriffState, Watch};
