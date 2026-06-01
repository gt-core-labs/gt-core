//! Read helpers for the issues MCP resources (hq-core-host.2).
//!
//! The module contributes two read resources the server's resource router
//! (`hq-core-host.3`) serves:
//!
//! - `gt://issues` — a filtered snapshot. With `?full=1` the heavy text bodies
//!   (`description`/`design`/`acceptance_criteria`/`notes`) are inlined on every
//!   row so a whole sub-epic reviews in one call; without it the cheap snapshot
//!   omits them (set [`IssueFilter::full`]).
//! - `gt://issue/{id}` — one issue WITH the heavy bodies and its `version`.
//!
//! These are transport-free passthroughs to [`DoltIssues`]: the store rows are
//! already `Serialize`, so the resource router only has to JSON-encode the return
//! value. Keeping the contract here (rather than re-deriving the filter mapping
//! in the bin) means the `?full=1` semantics live with the module that owns the
//! data.

use gt_store_dolt::{AppError, DoltIssues, IssueDetail, IssueFilter, IssueRow};

/// Snapshot the issues table under `filter` (the `gt://issues` resource). Set
/// [`IssueFilter::full`] for the `?full=1` variant that inlines the heavy bodies.
pub async fn read_issues(
    issues: &DoltIssues,
    filter: &IssueFilter,
) -> Result<Vec<IssueRow>, AppError> {
    issues.list(filter).await
}

/// Fetch one issue WITH its heavy bodies + `version` (the `gt://issue/{id}`
/// resource). `Ok(None)` when no row matches `id`, which the resource router maps
/// to an MCP `not found`.
pub async fn read_issue(issues: &DoltIssues, id: &str) -> Result<Option<IssueDetail>, AppError> {
    issues.get_detail(id).await
}
