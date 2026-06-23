//! `gt-store-dolt` — Dolt-backed adapter for the gt-core issues store.
//!
//! Lifted from the upstream `apps/api/crates/kernel/gt-store-dolt` (hq-core-host.1,
//! the MVP path to host the issues tracker inside gt-core and retire the upstream
//! gt-mcp bin). Only the **issues slice** is ported here: `issues_repo` depends
//! solely on `conn` + a local [`AppError`], not on the upstream domain repos
//! (beads/merge/patrol/orch/wisp/sessions) — those stay upstream until their
//! own ports land.
//!
//! Dolt is MySQL-wire compatible but optimistic (no `SELECT ... FOR UPDATE`):
//! the claim path is a compare-and-swap (`UPDATE ... WHERE status='open'`),
//! aligned with Dolt's native grain.

mod branch;
mod commit;
mod conn;
mod domain_catalog;
mod error;
mod issues_repo;
mod lock;
mod pool;
mod workspace;

pub use branch::delete_branch;
pub use commit::{commit, diff_summary, rollback};
pub use conn::connect;
pub use domain_catalog::{
    generic_template, reserved_gap_entry, CatalogInit, DoltDomainCatalog, DomainEntry,
    GENERIC_TEMPLATE, RESERVED_GAP_KEY,
};
pub use error::AppError;
pub use lock::WorkspaceLock;
pub use pool::{WorkspacePools, DEFAULT_MAX_CONNS_PER_WS};
pub use workspace::{create_workspace_dolt, dolt_db_name};
pub use issues_repo::{
    issues_default_limit, issues_max_limit, ArchivedIssue, ClaimOutcome, DepFact, DoltIssues,
    IssueDetail, IssueFilter, IssuePage, IssuePatch, IssuePhase, IssueRelation, IssueRow,
    IssueStatus, NewIssue,
};
