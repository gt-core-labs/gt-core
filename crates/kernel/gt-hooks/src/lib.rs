//! Lifecycle hooks framework — modules observe and react to kernel lifecycle
//! points without knowing about the bus.
//!
//! A module declares the points it cares about (via `Capability::hooks`,
//! `hq-mod-hooks.3`) and supplies a [`HookHandler`]; the [`HookRegistry`] holds
//! those handlers keyed by [`HookPoint`], and the dispatcher (also `.3`) invokes
//! them when the kernel reaches a point, honoring a [`HookOutcome::Reject`] veto
//! at vetoable points.
//!
//! ## Landed so far
//!
//! - [`HookPoint`] — lifecycle moments; `BeforeCommand` / `AfterCommand` seed the
//!   set, the full builtin list is `hq-mod-hooks.2` (`hq-mod-hooks.1`).
//! - [`HookHandler`] + [`HookContext`] + [`HookOutcome`] — the observer trait and
//!   its in/out types (`hq-mod-hooks.1`).
//! - [`HookRegistry`] — per-point, registration-ordered handler collection
//!   (`hq-mod-hooks.1`).
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
