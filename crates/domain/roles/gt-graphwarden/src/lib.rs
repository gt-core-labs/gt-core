//! `gt-graphwarden` — the per-rig knowledge-graph custodian role (`hq-graphrig.6`).
//!
//! When a repo is attached to a rig, the warden takes custody of that repo's codebase
//! knowledge graph and tracks its freshness: it registers the rig, marks the graph stale
//! when the rig's main moves (a merge landed), and records each completed re-index. Other
//! agents only *read* the graph (via the MCP surface, `hq-graphrig.10`); the warden is the
//! sole writer of freshness state.
//!
//! ## Shape
//!
//! Same actor + commands + events + state + repo pattern as [`gt-witness`], an escalation
//! observer: mutable state lives in one actor ([`spawn`]); everyone else asks for snapshots
//! over a channel; the reducer ([`WardenState::apply`]) is a pure replay fold so the
//! rebuilt state matches the live one byte-for-byte (docs/06 Step-3 gate). Events are
//! emitted in the canonical kebab `.v1` form from the start (docs/04).
//!
//! ## Boundaries
//!
//! - **What** to (re)index is this crate's concern (freshness state); **how** to run the
//!   index is not — that is the [`GraphIndexer`](../gt_graphindex/trait.GraphIndexer.html)
//!   port, invoked by the composition edge (`hq-graphrig.8`). The warden therefore depends
//!   only on `gt-events`, not on any graph tool: it names no `graphindex` type, keeping the
//!   role replay-pure and the tool fully swappable.
//! - **Who** triggers a refresh is the composition reactor: it observes the merge-complete
//!   bus event (`merge.merged.v1`) and the rig-catalog events, then drives the warden +
//!   the indexer. The warden does not subscribe to the bus itself.

pub mod actor;
pub mod commands;
mod events;
mod repo;
mod state;

pub use actor::{spawn, spawn_hydrated, WardenHandle, WardenMsg};
pub use commands::{MarkRefreshed, MarkStale, RegisterRig, Unregister, WardenCommand};
pub use events::WardenEvent;
pub use repo::{GraphWardenRepository, InMemoryGraphWardenRepo};
pub use state::{RigGraph, WardenState};
