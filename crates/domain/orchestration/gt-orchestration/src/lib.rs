//! `gt-orchestration` — the convoy domain (Paso 6.d of `docs/08-getting-started.md`).
//!
//! A **convoy** drives an ordered set of member beads to completion: it feeds the next
//! ready member when the current one finishes (the *handoff*) and closes when all members
//! are done. This is the orchestration layer the **mayor** and **deacon** drive and the
//! **crew** executes:
//!
//! - **mayor / deacon** — edge *producers* (in the `bins/gt` composition root, not built
//!   yet). They translate bus facts into `OrchMsg`: a deacon launches a convoy, observes
//!   member beads closing, and tells the actor (`member_done` / `member_fail`).
//! - **crew** — the polecats that run each dispatched member. The actor's `MemberDispatched`
//!   output is what the root turns into a `gt sling` for the crew.
//! - **convoy** — the aggregate owned here: state machine + sequential handoff.
//!
//! Isolation: this crate depends ONLY on the kernel (`gt-events`). It does not call
//! `gt-scheduling`, `gt-merge` or `gt-beads`; cross-domain integration (slinging the next
//! member, closing the convoy bead in Dolt) is the composition root's job, reacting to the
//! emitted events — same integration-by-events pattern as Pasos 6.a/6.b.
//!
//! Persistence (hq-03aw.8 / epic hq-bdn8): the `[Dolt]` marker in `docs/02-tree.md` is now
//! backed by [`OrchRepository`] — the actor mirrors each transition into the port (Dolt in
//! prod, in-memory otherwise). Best-effort: the audit log + [`OrchState`] reducer remain the
//! source of truth, so replay stays byte-identical.

pub mod actor;
pub mod commands;
mod events;
/// The off-by-default `axum` REST adapter (`hq-fe-api-orch.3`): the `convoy.*` HTTP routes the
/// builder mounts at `/api/v1/convoy`, dispatching to the same command layer the MCP tools use.
#[cfg(feature = "axum")]
pub mod http;
pub mod module;
mod repo;
mod state;

pub use commands::{CompleteMember, FailMember, LaunchConvoy, OrchCommand};
pub use events::OrchEvent;
#[cfg(feature = "axum")]
pub use http::{convoy_router, ApiDoc, ConvoyApiState, WorkspaceConvoy};
#[cfg(feature = "axum")]
pub use module::ConvoyHttpModule;
pub use module::ConvoyModule;
pub use repo::{InMemoryOrchRepo, OrchRepository};
pub use state::{Convoy, ConvoyBoard, ConvoyState, Member, MemberState, OrchState};
