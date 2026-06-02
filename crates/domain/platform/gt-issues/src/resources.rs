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
//! - `gt://issues?ready=true` — the same snapshot narrowed to **sound** beads via
//!   [`filter_ready`] (hq-core-mcp.11, docs/10 §S4).
//!
//! These are transport-free passthroughs to [`DoltIssues`]: the store rows are
//! already `Serialize`, so the resource router only has to JSON-encode the return
//! value. Keeping the contract here (rather than re-deriving the filter mapping
//! in the bin) means the `?full=1` / `?ready=true` semantics live with the module
//! that owns the data.

use std::collections::HashMap;

use gt_store_dolt::{
    AppError, DepFact, DoltIssues, IssueDetail, IssueFilter, IssuePage, IssuePhase, IssueRow,
};

use crate::readiness::is_ready;
use crate::surface::SurfaceTree;

/// One less-style page of the issues snapshot (the `gt://issues` resource,
/// hq-core-mcp.13). The returned [`IssuePage`] carries `total`/`next_offset`/
/// `has_more` so a consumer pages through the corpus without a full dump or a
/// silent cap. Set [`IssueFilter::full`] for the `?full=1` variant that inlines
/// the heavy bodies, and `limit`/`offset` to position the page.
pub async fn read_issues_page(
    issues: &DoltIssues,
    filter: &IssueFilter,
) -> Result<IssuePage, AppError> {
    issues.list_page(filter).await
}

/// The bare-row snapshot under `filter`, used for the unbounded `?ready=true`
/// frontier where the readiness predicate (not a page) bounds the set. The paged
/// `gt://issues` resource goes through [`read_issues_page`] instead.
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

/// Narrow a candidate snapshot to only the **sound** beads (`gt://issues?ready=true`,
/// hq-core-mcp.11/.12, docs/10 §S4 + §C): each surviving row passes all four
/// readiness clauses via [`is_ready`]. The server supplies the gathered inputs —
/// `open_phase` from the frontier, `deps` from
/// [`dep_index`](DoltIssues::dep_index) (covering the whole table so a dependency
/// outside the candidate set is still resolvable, and carrying `issue_type`/
/// `status` so the epic-dep rule applies), and the `main` git `tree`. Keeping the
/// fan-out here means the readiness contract lives with the module that owns the
/// issues data rather than in the transport bin.
pub fn filter_ready(
    rows: Vec<IssueRow>,
    open_phase: IssuePhase,
    deps: &HashMap<String, DepFact>,
    tree: &(dyn SurfaceTree + Sync),
) -> Vec<IssueRow> {
    rows.into_iter()
        .filter(|r| is_ready(r, open_phase, &|id| deps.get(id).cloned(), tree))
        .collect()
}
