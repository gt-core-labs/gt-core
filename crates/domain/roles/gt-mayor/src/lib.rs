//! `gt-mayor` — orchestration loop role (Paso 9.D, hq-92z9).
//!
//! Mayor delegates work and tracks each handoff through its lifecycle:
//! `Pending → Acknowledged → Resolved` (happy path) or `Pending → Withdrawn`
//! (operator cancel). Same actor + commands + events + state + repo pattern as
//! `gt-merge` / `gt-sheriff` — one in-process actor owns the `MayorState`, the
//! reducer is shared between live emit-on-apply and boot replay, and the audit log
//! is authoritative.
//!
//! The long-running orchestration **loop** (periodic convoy scan → automatic
//! `Delegate`) is a follow-up that lives at the composition-root edge; the helpers
//! in [`mayor`] are the substrate it will call into. Until then the actor is
//! exercised directly by tests and (eventually) by CLI / MCP commands.

pub mod actor;
pub mod commands;
pub mod mayor;
mod events;
mod repo;
mod state;

pub use actor::{spawn, spawn_hydrated, MayorHandle, MayorMsg};
pub use commands::{Acknowledge, Delegate, MayorCommand, Resolve, Withdraw};
pub use events::MayorEvent;
pub use repo::{InMemoryMayorRepo, MayorRepository};
pub use state::{Delegation, DelegationStatus, MayorState};
