//! Lifecycle hooks framework — modules observe and react to kernel lifecycle
//! points without knowing about the bus.
//!
//! A module declares the points it cares about (via
//! [`Capability::subscribing`](gt_module::Capability::subscribing),
//! `hq-mod-hooks.3`) and supplies a [`HookHandler`]; the [`HookRegistry`] holds
//! those handlers keyed by [`HookPoint`], and [`HookRegistry::dispatch`]
//! (`hq-mod-hooks.9`) invokes them when the kernel reaches a point, honoring a
//! [`HookOutcome::Reject`] veto at vetoable points.
//!
//! ## Landed so far
//!
//! - [`HookPoint`] — the lifecycle-point vocabulary, re-exported from `gt-module`
//!   where it lives so [`Capability`](gt_module::Capability) can name it without a
//!   dependency cycle (relocated by `hq-mod-hooks.3`; defined `.1`/`.2`).
//! - [`HookHandler`] + [`HookContext`] + [`HookOutcome`] — the observer trait and
//!   its in/out types (`hq-mod-hooks.1`).
//! - [`HookRegistry`] — per-point, registration-ordered handler collection
//!   (`hq-mod-hooks.1`); [`HookRegistry::dispatch`] invokes handlers and honors a
//!   veto at vetoable points (`hq-mod-hooks.9`).
//!
//! ## Boundary (NN#1)
//!
//! Hooks are the observer-plugin surface NN#1's exception names (docs/03:
//! "`dyn` in kernel crates except observer plugins"). This crate owns the handler
//! trait + registry; the relay and dead-letter stay in `gt-plugin` and migrate up
//! in Phase 4. See [`handler`] for the full reconciliation.

#![forbid(unsafe_code)]
#![warn(missing_docs)]

pub mod handler;
pub mod points;
pub mod registry;

pub use handler::{HookContext, HookHandler, HookOutcome};
pub use points::HookPoint;
pub use registry::HookRegistry;
