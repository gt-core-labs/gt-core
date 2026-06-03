//! Domain events of the Graph Warden role.
//!
//! Wire vocabulary is `graphwarden.*`. Unlike the older roles (whose `kind()` still
//! returns a legacy bare/underscore string pending `hq-mod-events.*`), this is a new
//! crate, so every kind is emitted in the canonical **kebab + `.v1`** form from the
//! start (docs/04 versioned-event-kinds, the same shape `hq-mod-events.9` aligned
//! skills to).

use serde::{Deserialize, Serialize};

use gt_events::EventKind;

/// Events emitted by the graph-warden actor. The warden is the per-rig custodian of a
/// codebase knowledge graph: `RigRegistered` opens custody when a repo is attached,
/// `MarkedStale` records that the rig's main moved (so the graph needs a refresh),
/// `Refreshed` records a completed re-index at a commit, and `Unregistered` ends custody.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum WardenEvent {
    /// A rig's repo entered custody. `repo_dir` is the on-disk checkout the indexer runs
    /// against; the graph starts stale (an initial build is owed).
    RigRegistered {
        rig: String,
        repo_dir: String,
        now_secs: u64,
    },
    /// The rig's graph fell behind the code: `changed` files moved since the last index
    /// (e.g. a merge landed). Accumulates into the rig's pending count and sets stale.
    MarkedStale {
        rig: String,
        changed: u64,
        now_secs: u64,
    },
    /// A re-index completed: the graph is current as of `commit`. Clears stale and the
    /// pending count.
    Refreshed {
        rig: String,
        commit: String,
        now_secs: u64,
    },
    /// Custody ended (the rig was removed). The warden forgets the rig.
    Unregistered { rig: String, now_secs: u64 },
}

impl EventKind for WardenEvent {
    fn kind(&self) -> &'static str {
        match self {
            WardenEvent::RigRegistered { .. } => "graphwarden.rig-registered.v1",
            WardenEvent::MarkedStale { .. } => "graphwarden.marked-stale.v1",
            WardenEvent::Refreshed { .. } => "graphwarden.refreshed.v1",
            WardenEvent::Unregistered { .. } => "graphwarden.unregistered.v1",
        }
    }
}
