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

use std::borrow::Cow;
use std::collections::HashMap;
use std::fmt::Write as _;

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

/// Header line of the [`rows_to_tsv`] table — the column order every row follows.
const TSV_HEADER: &str = "id\tstatus\tpriority\tphase\ttype\tassignee\texternal_ref\ttitle";

/// Render a bare issues snapshot as a dense TSV table (hq-mcp-output-format): one
/// header line then one tab-separated row per bead, carrying only the cheap scalar
/// columns a triage scan needs. This is the `format=tsv` shape for `gt://issues` —
/// it drops the per-row repeated JSON keys (8 keys × N rows) for a single header,
/// the largest token saving on a list-heavy read. Heavy text bodies and the
/// `*_json` arrays are intentionally omitted; a caller needing those re-reads the
/// row as JSON (`gt://issue/{id}` or `?full=1`).
pub fn rows_to_tsv(rows: &[IssueRow]) -> String {
    let mut out = String::from(TSV_HEADER);
    for r in rows {
        // `title` last so its free-text spaces never collide with a tab delimiter;
        // every string cell is still sanitised in case one carries a stray tab.
        let _ = write!(
            out,
            "\n{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}",
            cell(&r.id),
            cell(&r.status),
            r.priority,
            cell(&r.phase),
            cell(&r.issue_type),
            cell(r.assignee.as_deref().unwrap_or("")),
            cell(r.external_ref.as_deref().unwrap_or("")),
            cell(&r.title),
        );
    }
    out
}

/// The `format=tsv` shape for the paged `gt://issues` snapshot: the [`rows_to_tsv`]
/// table followed by a trailer comment line carrying the pager envelope
/// (`total`/`next_offset`/`has_more`) so a caller still walks the corpus by offset
/// without paying for the JSON envelope's whitespace + repeated row keys.
pub fn page_to_tsv(page: &IssuePage) -> String {
    let mut out = rows_to_tsv(&page.rows);
    let _ = write!(
        out,
        "\n# total={} next_offset={} has_more={}",
        page.total, page.next_offset, page.has_more
    );
    out
}

/// Make a string safe to drop into one TSV cell: collapse any tab/newline/carriage
/// return to a space so a stray control char in free text (a `title`) never breaks
/// row/column alignment. Borrows untouched when already clean.
fn cell(s: &str) -> Cow<'_, str> {
    if s.contains(['\t', '\n', '\r']) {
        Cow::Owned(s.replace(['\t', '\n', '\r'], " "))
    } else {
        Cow::Borrowed(s)
    }
}

#[cfg(test)]
mod tsv_tests {
    use super::*;

    fn row(id: &str, status: &str, title: &str) -> IssueRow {
        IssueRow {
            id: id.to_string(),
            title: title.to_string(),
            status: status.to_string(),
            priority: 2,
            issue_type: "bead".to_string(),
            assignee: None,
            owner: None,
            created_at: None,
            updated_at: None,
            closed_at: None,
            external_ref: Some("hq-epic".to_string()),
            spec_id: None,
            domain_json: "[]".to_string(),
            surface_json: "[]".to_string(),
            depends_on_json: "[]".to_string(),
            role_scope: None,
            version: 0,
            phase: "P1".to_string(),
            delivered_sha: None,
            description: None,
            design: None,
            acceptance_criteria: None,
            notes: None,
        }
    }

    #[test]
    fn header_then_one_line_per_row() {
        let tsv = rows_to_tsv(&[row("a-1", "open", "first"), row("a-2", "working", "second")]);
        let lines: Vec<&str> = tsv.lines().collect();
        assert_eq!(lines[0], TSV_HEADER);
        assert_eq!(lines.len(), 3, "header + 2 rows");
        // 8 columns, every row, with the absent assignee an empty cell.
        assert_eq!(lines[1], "a-1\topen\t2\tP1\tbead\t\thq-epic\tfirst");
        assert_eq!(lines[1].split('\t').count(), 8);
    }

    #[test]
    fn empty_snapshot_is_header_only() {
        assert_eq!(rows_to_tsv(&[]), TSV_HEADER);
    }

    #[test]
    fn control_chars_in_title_collapse_to_spaces() {
        let tsv = rows_to_tsv(&[row("a-1", "open", "tab\there\nand newline")]);
        let cells: Vec<&str> = tsv.lines().nth(1).unwrap().split('\t').collect();
        assert_eq!(cells.len(), 8, "stray tab/newline must not add columns/rows");
        assert_eq!(cells[7], "tab here and newline");
    }

    #[test]
    fn page_appends_pager_trailer() {
        let page = IssuePage {
            rows: vec![row("a-1", "open", "first")],
            total: 42,
            next_offset: 1,
            has_more: true,
        };
        let tsv = page_to_tsv(&page);
        let last = tsv.lines().last().unwrap();
        assert_eq!(last, "# total=42 next_offset=1 has_more=true");
    }
}
