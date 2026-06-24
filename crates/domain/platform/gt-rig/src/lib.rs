//! `gt-rig` — rig catalog as orchestrator state (hq-mc72.12.29).
//!
//! A rig is a managed repository inside a Gas Town workspace. Go owns this model in
//! `internal/rig` and bundles **two concerns** that the Rust side splits:
//!
//! 1. **Filesystem bootstrap** — clone the repo, init beads, set up redirects, register
//!    routes, scaffold polecat/witness/refinery dirs. Classified B1 in Paso 10 and lives
//!    under `deploy/bootstrap` (hq-mc72.12.24).
//! 2. **Orchestrator state** — the registry of rigs the orchestrator knows about, plus its
//!    sync `Command` path (Add / Adopt / Remove / SetPrefix / SetDefaultBranch). This is the
//!    crate.
//!
//! The crate follows the same shape as `gt-quota` and `gt-merge`:
//!
//! - **Owned events** ([`RigEvent`]), a `Serialize`/`Deserialize` enum.
//! - **Mutable state lives inside one actor** ([`actor::spawn`]); everyone else asks for
//!   snapshots over a channel. Snapshot/validate/exec mirror gt-quota.
//! - **Pure replay reducer** ([`RigState`]): time enters as `now_secs` data so the rebuilt
//!   state matches the live one byte-for-byte (`docs/06-observability.md` Step 3 gate).
//! - **Inverted repository** ([`RigRepository`]): the domain defines the port; `gt-store-pg`
//!   will implement it later. [`InMemoryRigs`] is the test safety net.

pub mod actor;
pub mod commands;
#[cfg(feature = "axum")]
pub mod http;
pub mod module;
#[cfg(feature = "pg")]
mod pg;
pub mod repo;
pub mod rig_seed;

mod events;
mod state;

#[cfg(feature = "pg")]
pub use pg::PgRigs;

pub use actor::{spawn, spawn_hydrated, RigHandle, RigMsg};
pub use commands::{
    AddRig, AdoptRig, RemoveRig, RigCommand, SetRigConnection, SetRigDefaultBranch, SetRigPrefix,
    SetRigTags, SetRigWorktreeRoot,
};
pub use events::RigEvent;
#[cfg(feature = "axum")]
pub use http::{rig_router, ApiDoc, DynRigRepository, RigApiState, WorkspaceRigs};
#[cfg(feature = "axum")]
pub use module::RigsHttpModule;
pub use module::RigsModule;
pub use repo::{InMemoryRigs, RigRepository};
pub use rig_seed::{seed_rigs, SeedRig, SEED_JSON as RIGS_SEED_JSON};
pub use state::{
    normalize_semantic_tags, validate_prefix, validate_rig_name, validate_semantic_tags,
    validate_worktree_root, RigCatalog, RigEntry, RigReadiness, RigState, MAX_PREFIX_LEN,
    MAX_SEMANTIC_TAGS, MAX_SEMANTIC_TAG_LEN, MAX_WORKTREE_ROOT_LEN, RESERVED_RIG_NAMES,
};
