//! `gt-graphindex` — a tool-neutral port for building and querying a codebase
//! knowledge graph, one per managed repository.
//!
//! ## Why this is a kernel crate
//!
//! Indexing a repo directory and answering questions about it is generic,
//! domain-free plumbing — it names no Gas Town concept (no bead, rig, workspace).
//! It therefore lives in the kernel tier alongside the other domain-free hosts
//! (`gt-store-pg`, `gt-store-dolt`), so a *role* such as the graph custodian
//! (`gt-graphwarden`, `hq-graphrig.6`) can depend on the **port** without a
//! forbidden domain→domain edge (docs/03 Rule 4).
//!
//! ## The two swappable axes
//!
//! The epic (`hq-graphrig`) keeps two things replaceable:
//!
//! 1. **The graph tool.** [`GraphIndexer`] is the seam. graphify is the first
//!    adapter (`GraphifyIndexer`, `hq-graphrig.2`, behind an off-by-default
//!    `graphify` feature); a better tool later is a new adapter + a config flip,
//!    with no change to the warden, the reactor, or the MCP surface that depend
//!    only on this trait.
//! 2. **The custodian agent.** Out of scope for this crate — it is a launch-time
//!    binding the composition edge resolves (`hq-graphrig.9`). This crate only
//!    runs the index; it does not decide *who* triggers a run.
//!
//! The trait is `async` because every real adapter does I/O at the edge (spawn a
//! CLI, read `graph.json`); it never runs inside the sync replay core (NN#2).
//!
//! ## Landed so far
//!
//! - [`GraphIndexer`] + its value types ([`IndexStats`], [`IndexDiff`],
//!   [`GraphAnswer`], [`IndexStatus`], [`GraphError`]) — `hq-graphrig.1`.
//! - [`InMemoryGraphIndexer`] — the dependency-free test double every consumer
//!   wires in unit tests (the InMemory half of the hexagonal port).

#![forbid(unsafe_code)]
#![warn(missing_docs)]

pub mod artifacts;
#[cfg(feature = "graphify")]
mod graphify;
mod memory;
mod port;

pub use artifacts::{patterns_for, NEUTRAL_UMBRELLA};
#[cfg(feature = "graphify")]
pub use graphify::GraphifyIndexer;
pub use memory::InMemoryGraphIndexer;
pub use port::{GraphAnswer, GraphError, GraphIndexer, IndexDiff, IndexStats, IndexStatus};
