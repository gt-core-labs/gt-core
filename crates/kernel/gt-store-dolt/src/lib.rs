//! `gt-store-dolt` — Dolt-backed adapter for the gt-core issues store.
//!
//! Lifted from gastown `apps/api/crates/kernel/gt-store-dolt` (hq-core-host.1,
//! the MVP path to host the issues tracker inside gt-core and retire the gastown
//! gt-mcp bin). Only the **issues slice** is ported here: `issues_repo` depends
//! solely on `conn` + a local [`AppError`], not on the gastown domain repos
//! (beads/merge/patrol/orch/wisp/sessions) — those stay in gastown until their
//! own ports land.
//!
//! Dolt is MySQL-wire compatible but optimistic (no `SELECT ... FOR UPDATE`):
//! the claim path is a compare-and-swap (`UPDATE ... WHERE status='open'`),
//! aligned with Dolt's native grain.

mod commit;
mod conn;
mod error;
mod issues_repo;
mod pool;
mod workspace;

pub use commit::{commit, diff_summary, rollback};
pub use conn::connect;
pub use error::AppError;
pub use pool::{WorkspacePools, DEFAULT_MAX_CONNS_PER_WS};
pub use workspace::{create_workspace_dolt, dolt_db_name};
pub use issues_repo::{
    issues_default_limit, issues_max_limit, ClaimOutcome, DepFact, DoltIssues, IssueDetail,
    IssueFilter, IssuePage, IssuePatch, IssuePhase, IssueRow, IssueStatus, NewIssue,
};
