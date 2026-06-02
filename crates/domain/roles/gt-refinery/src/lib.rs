//! `gt-refinery` — refinery-side projection over the merge-ready channel (Paso 9.D, hq-92z9).
//!
//! Coexists with `gt_merge::refinery`: that crate still owns the `MERGE_READY` channel
//! watcher and emits `MergeEvent::Ready` into the merge actor (its own gate test is
//! authoritative). This crate is the parallel observer that records the refinery-side
//! lifecycle for operator dashboards + replay. `Observed`/`Dispatched` are pure
//! observations — they do not drive the merge slot; merge progress remains owned by
//! `gt-merge`.
//!
//! Same actor + commands + events + state + repo pattern as `gt-merge`, `gt-sheriff`,
//! `gt-deacon`. Cross-domain wiring (turning `MergeEvent::Ready`/`Started` into refinery
//! commands) is a composition-root reaction wired in `bins/gt::root::Reactor::react`;
//! deferred for the first fill to keep the blast radius small.

pub mod actor;
pub mod commands;
pub mod refinery;
mod events;
mod repo;
mod state;

pub use actor::{spawn, spawn_hydrated, RefineryHandle, RefineryMsg};
pub use commands::{MarkDispatched, ObserveReady, RefineryCommand};
pub use events::RefineryEvent;
pub use repo::{InMemoryRefineryRepo, RefineryRepository};
pub use state::{RefineryEntry, RefineryState};
