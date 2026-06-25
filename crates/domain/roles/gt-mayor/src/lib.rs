//! `gt-mayor` — orchestration loop role (Paso 9.D, hq-92z9).
//!
//! Mayor delegates work and tracks each handoff through its lifecycle:
//! `Pending → Acknowledged → Resolved` (happy path) or `Pending → Withdrawn`
//! (operator cancel). Same actor + commands + events + state + repo pattern as
//! `gt-merge` / `gt-sheriff` — one in-process actor owns the `MayorState`, the
//! reducer is shared between live emit-on-apply and boot replay, and the audit log
//! is authoritative.
//!
//! The long-running orchestration **loop** (on each orchd wake: read the frontier →
//! prioritize → delegate one bead per polecat up to the pool cap) lives at the
//! composition-root edge. Its pure decision step is [`triage`]: frontier + free
//! capacity in, a [`TriagePlan`] (delegate / queue / decompose buckets) out. The
//! producer helpers in [`mayor`] turn that plan into the `Delegate` commands the
//! actor records — `gtcore-5c50f0`.

pub mod actor;
pub mod commands;
pub mod mayor;
pub mod triage;
mod events;
mod repo;
mod state;

pub use actor::{spawn, spawn_hydrated, MayorHandle, MayorMsg};
pub use commands::{Acknowledge, Delegate, MayorCommand, Resolve, Withdraw};
pub use events::MayorEvent;
pub use repo::{InMemoryMayorRepo, MayorRepository};
pub use state::{Delegation, DelegationStatus, MayorState};
pub use triage::{triage, FrontierBead, FrontierKind, PlannedDelegation, TriagePlan};
