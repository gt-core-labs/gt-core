//! The Kanban board projection over the issues tracker (hq-62130a, ADR hq-423a4b).
//!
//! Cards ARE beads — there is no second issue store. This module is the thin
//! projection + write surface the gt Kanban epic (hq-56b5ee) builds on:
//!
//! - [`BoardList`] / [`run_board_list`] — bucket the existing list query into
//!   status columns ordered by `board_rank` (lexorank), with optional
//!   `group_by` lanes (assignee | epic | priority).
//! - [`BoardMove`] / [`run_board_move`] — set a card's column (status) AND its
//!   rank in ONE atomic Dolt commit ([`DoltIssues::board_move`]); the
//!   state-machine legality and the (rig, workspace) scope are enforced
//!   store-side.
//! - [`BoardReorder`] / [`run_board_reorder`] — rank-only move within a column.
//! - [`rank_between`] — the lexorank midpoint; [`spread_ranks`] the evenly-spaced
//!   rebalance sequence used on collision or lazy first-touch backfill.
//!
//! Every entry point takes the full `(rig, workspace)` scope key (ADR D4): a
//! caller scoped to another workspace cannot read or move this board's cards.

use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

use gt_store_dolt::{AppError, DoltIssues, IssueFilter, IssueRow, IssueStatus};

/// The closed set of `group_by` lanes [`BoardList`] accepts.
const GROUP_BYS: &[&str] = &["assignee", "epic", "priority"];

// ---------------------------------------------------------------------------
// Lexorank (fractional indexing over base-36 strings)
// ---------------------------------------------------------------------------

/// The rank digit alphabet, in lexicographic order. Base 36 keeps ranks short;
/// the maintained invariant is that **no rank ever ends in `'0'`** (the lowest
/// digit), which is what guarantees a midpoint always exists between any two
/// distinct ranks (the standard fractional-indexing invariant).
const RANK_DIGITS: &[u8] = b"0123456789abcdefghijklmnopqrstuvwxyz";
const RANK_BASE: usize = RANK_DIGITS.len();

fn rank_digit_value(c: u8) -> usize {
    RANK_DIGITS.iter().position(|d| *d == c).unwrap_or(0)
}

/// The midpoint string strictly between `a` and `b` (`b = None` ⇒ +∞).
/// Precondition: `a < b` when `b` is `Some` — the caller ([`rank_between`])
/// guards it. The classic fractional-indexing recursion: strip the common
/// prefix (an exhausted `a` reads as `'0'`s), then either bisect the first
/// diverging digit or descend.
fn rank_midpoint(a: &str, b: Option<&str>) -> String {
    if let Some(bs) = b {
        // Common prefix: while b's digit equals a's digit (a exhausted ⇒ '0').
        let mut n = 0usize;
        let ab = a.as_bytes();
        let bb = bs.as_bytes();
        while n < bb.len() && bb[n] == *ab.get(n).unwrap_or(&b'0') {
            n += 1;
        }
        if n > 0 {
            let a_rest = if n < a.len() { &a[n..] } else { "" };
            return format!("{}{}", &bs[..n], rank_midpoint(a_rest, Some(&bs[n..])));
        }
    }
    let da = a.bytes().next().map(|c| rank_digit_value(c)).unwrap_or(0);
    let db = b
        .and_then(|bs| bs.bytes().next())
        .map(|c| rank_digit_value(c))
        .unwrap_or(RANK_BASE);
    if db - da > 1 {
        // Room at this digit: any strictly-between digit works; floor-mid is
        // ≥ da+1, so it never emits a trailing-'0' (da ≥ 0 ⇒ mid ≥ 1).
        return (RANK_DIGITS[(da + db) / 2] as char).to_string();
    }
    // Consecutive first digits.
    if let Some(bs) = b {
        if bs.len() > 1 {
            // b's own first digit is a valid midpoint: a < b[..1] < b (a proper
            // prefix sorts before its extensions, and b never ends in '0').
            return bs[..1].to_string();
        }
    }
    // b is +∞ or a single digit: follow a's first digit and recurse upward.
    let a_rest = if a.is_empty() { "" } else { &a[1..] };
    format!("{}{}", RANK_DIGITS[da] as char, rank_midpoint(a_rest, None))
}

/// A rank string strictly between `a` and `b` (exclusive), where `None` means
/// the open boundary (`a = None` ⇒ before everything, `b = None` ⇒ after
/// everything). Returns `None` when no midpoint can exist — `a >= b`, i.e.
/// duplicate/corrupted ranks — which is the caller's signal to rebalance the
/// column ([`spread_ranks`]) and retry.
pub fn rank_between(a: Option<&str>, b: Option<&str>) -> Option<String> {
    let a = a.unwrap_or("");
    if let Some(b) = b {
        if a >= b {
            return None;
        }
        return Some(rank_midpoint(a, Some(b)));
    }
    Some(rank_midpoint(a, None))
}

/// `n` evenly-spaced, strictly-increasing rank strings for a column rebalance
/// (or the lazy first-touch backfill of a column whose cards still carry the
/// `''` default). Width grows with `n` so consecutive ranks always leave room
/// for many midpoints before the next rebalance.
pub fn spread_ranks(n: usize) -> Vec<String> {
    if n == 0 {
        return Vec::new();
    }
    // Smallest width w with base^w comfortably above n (×2 headroom), floor 2.
    let mut w = 2usize;
    let mut cap: u128 = (RANK_BASE as u128).pow(2);
    while cap < (n as u128 + 1) * 2 {
        w += 1;
        cap = cap.saturating_mul(RANK_BASE as u128);
    }
    let step = cap / (n as u128 + 1); // ≥ 2 by the headroom above
    (1..=n as u128)
        .map(|i| {
            let mut x = i * step;
            let mut chars = vec![b'0'; w];
            for slot in chars.iter_mut().rev() {
                *slot = RANK_DIGITS[(x % RANK_BASE as u128) as usize];
                x /= RANK_BASE as u128;
            }
            // Invariant: a rank never ends in '0'. step ≥ 2 keeps the bump
            // collision-free with the next rank.
            if chars[w - 1] == b'0' {
                chars[w - 1] = b'1';
            }
            String::from_utf8(chars).expect("ascii")
        })
        .collect()
}

// ---------------------------------------------------------------------------
// Commands (MCP/REST wire shapes)
// ---------------------------------------------------------------------------

fn check_scope(rig: &str, workspace: &str) -> Result<(), AppError> {
    if rig.trim().is_empty() {
        return Err(AppError::Validation("rig is required (board scope key)".into()));
    }
    if workspace.trim().is_empty() {
        return Err(AppError::Validation(
            "workspace is required (board scope key)".into(),
        ));
    }
    Ok(())
}

/// Input for `board.list` — the board projection for one `(rig, workspace)`.
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
#[cfg_attr(feature = "axum", derive(utoipa::ToSchema, utoipa::IntoParams))]
pub struct BoardList {
    /// Rig half of the board scope key. Required.
    pub rig: String,
    /// Workspace half of the board scope key. Required.
    pub workspace: String,
    /// Narrow to one epic/module (`external_ref` linkage, ADR D5).
    #[serde(default)]
    pub epic: Option<String>,
    /// Optional swimlanes within each column: `assignee` | `epic` | `priority`.
    #[serde(default)]
    pub group_by: Option<String>,
    /// Filter by exact assignee.
    #[serde(default)]
    pub assignee: Option<String>,
    /// Include only cards with priority ≤ this value (0 = highest).
    #[serde(default)]
    pub priority_max: Option<u8>,
    /// Filter by issue type (`task`, `spike`, …).
    #[serde(default)]
    pub issue_type: Option<String>,
}

impl BoardList {
    /// Shape-only guard: scope key present, `group_by` in the closed set.
    pub fn validate(&self) -> Result<(), AppError> {
        check_scope(&self.rig, &self.workspace)?;
        if let Some(g) = &self.group_by {
            if !GROUP_BYS.contains(&g.as_str()) {
                return Err(AppError::Validation(format!(
                    "unknown group_by `{g}` (expected one of assignee/epic/priority)"
                )));
            }
        }
        Ok(())
    }
}

/// Input for `board.move` — column change + placement in ONE atomic commit.
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
#[cfg_attr(feature = "axum", derive(utoipa::ToSchema))]
pub struct BoardMove {
    /// The card (bead id) to move.
    pub id: String,
    /// Rig half of the board scope key. Required.
    pub rig: String,
    /// Workspace half of the board scope key. Required.
    pub workspace: String,
    /// Target column: `open` | `working` | `closed`. The same state machine as
    /// `issues.transition` applies — an illegal move is rejected store-side.
    pub column: String,
    /// Place the card AFTER this card id in the target column. Omit both
    /// `after`/`before` to append at the bottom.
    #[serde(default)]
    pub after: Option<String>,
    /// Place the card BEFORE this card id in the target column.
    #[serde(default)]
    pub before: Option<String>,
}

impl BoardMove {
    /// Shape-only guard.
    pub fn validate(&self) -> Result<(), AppError> {
        if self.id.trim().is_empty() {
            return Err(AppError::Validation("card id is required".into()));
        }
        check_scope(&self.rig, &self.workspace)?;
        if IssueStatus::parse(&self.column).is_none() {
            return Err(AppError::Validation(format!(
                "unknown column `{}` (expected open/working/closed)",
                self.column
            )));
        }
        Ok(())
    }

    /// The parsed target column. Call after [`validate`](Self::validate).
    pub fn target(&self) -> IssueStatus {
        IssueStatus::parse(&self.column).expect("validated upstream")
    }
}

/// Input for `board.reorder` — rank-only placement within the card's column.
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
#[cfg_attr(feature = "axum", derive(utoipa::ToSchema))]
pub struct BoardReorder {
    /// The card (bead id) to reorder.
    pub id: String,
    /// Rig half of the board scope key. Required.
    pub rig: String,
    /// Workspace half of the board scope key. Required.
    pub workspace: String,
    /// Place the card AFTER this card id. Omit both to append at the bottom.
    #[serde(default)]
    pub after: Option<String>,
    /// Place the card BEFORE this card id.
    #[serde(default)]
    pub before: Option<String>,
}

impl BoardReorder {
    /// Shape-only guard.
    pub fn validate(&self) -> Result<(), AppError> {
        if self.id.trim().is_empty() {
            return Err(AppError::Validation("card id is required".into()));
        }
        check_scope(&self.rig, &self.workspace)
    }
}

// ---------------------------------------------------------------------------
// Projection output
// ---------------------------------------------------------------------------

/// One swimlane within a column (present only when `group_by` is set).
#[derive(Debug, Clone, Serialize)]
#[cfg_attr(feature = "axum", derive(utoipa::ToSchema))]
pub struct BoardLane {
    /// The lane key: the assignee, the epic id, or the priority (`"0"`/`"1"`/…).
    /// `""` collects the ungrouped tail (no assignee / no epic).
    pub key: String,
    /// The lane's cards, in board order.
    #[cfg_attr(feature = "axum", schema(value_type = Vec<Object>))]
    pub cards: Vec<IssueRow>,
}

/// One status column of the board.
#[derive(Debug, Clone, Serialize)]
#[cfg_attr(feature = "axum", derive(utoipa::ToSchema))]
pub struct BoardColumn {
    /// The column's status bucket: `open` | `working` | `closed`.
    pub status: String,
    /// The cards in board order: ranked cards lexicographically by
    /// `board_rank`, the unranked (`''`) tail last by recency.
    #[cfg_attr(feature = "axum", schema(value_type = Vec<Object>))]
    pub cards: Vec<IssueRow>,
    /// Swimlanes (only when the request set `group_by`): the same cards
    /// re-bucketed per lane key, each lane in board order.
    #[serde(skip_serializing_if = "Option::is_none")]
    #[cfg_attr(feature = "axum", schema(value_type = Option<Vec<Object>>))]
    pub lanes: Option<Vec<BoardLane>>,
}

/// The full board snapshot `board.list` returns.
#[derive(Debug, Clone, Serialize)]
#[cfg_attr(feature = "axum", derive(utoipa::ToSchema))]
pub struct BoardSnapshot {
    /// Rig half of the scope key, echoed.
    pub rig: String,
    /// Workspace half of the scope key, echoed.
    pub workspace: String,
    /// The three status columns, always in `open`, `working`, `closed` order.
    pub columns: Vec<BoardColumn>,
    /// The `group_by` that produced the lanes, echoed when set.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub group_by: Option<String>,
}

/// Order cards within a column: ranked cards first (lexorank ascending), the
/// unranked `''` tail afterwards in the list's own recency order. Stable so
/// equal ranks keep their store order rather than shuffling between calls.
fn column_sort(cards: &mut [IssueRow]) {
    cards.sort_by(|x, y| match (x.board_rank.is_empty(), y.board_rank.is_empty()) {
        (false, false) => x.board_rank.cmp(&y.board_rank),
        (false, true) => std::cmp::Ordering::Less,
        (true, false) => std::cmp::Ordering::Greater,
        (true, true) => std::cmp::Ordering::Equal,
    });
}

/// The lane key of a card under `group_by`.
fn lane_key(card: &IssueRow, group_by: &str, parent_map: &std::collections::HashMap<String, String>) -> String {
    match group_by {
        "assignee" => card.assignee.clone().unwrap_or_default(),
        "epic" => parent_map.get(&card.id).cloned().unwrap_or_default(),
        "priority" => card.priority.to_string(),
        _ => String::new(),
    }
}

/// Bucket the flat row set into the board shape. Pure — unit-testable without a
/// store.
pub fn project_board(
    rig: &str,
    workspace: &str,
    rows: Vec<IssueRow>,
    group_by: Option<&str>,
    parent_map: &std::collections::HashMap<String, String>,
) -> BoardSnapshot {
    let mut columns: Vec<BoardColumn> = ["open", "working", "closed"]
        .into_iter()
        .map(|status| BoardColumn {
            status: status.to_string(),
            cards: Vec::new(),
            lanes: None,
        })
        .collect();
    for row in rows {
        if let Some(col) = columns.iter_mut().find(|c| c.status == row.status) {
            col.cards.push(row);
        }
    }
    for col in &mut columns {
        column_sort(&mut col.cards);
        if let Some(g) = group_by {
            // Lanes keep first-seen order of keys; each lane inherits the
            // column's board order.
            let mut lanes: Vec<BoardLane> = Vec::new();
            for card in &col.cards {
                let key = lane_key(card, g, parent_map);
                match lanes.iter_mut().find(|l| l.key == key) {
                    Some(lane) => lane.cards.push(card.clone()),
                    None => lanes.push(BoardLane { key, cards: vec![card.clone()] }),
                }
            }
            col.lanes = Some(lanes);
        }
    }
    BoardSnapshot {
        rig: rig.to_string(),
        workspace: workspace.to_string(),
        columns,
        group_by: group_by.map(str::to_string),
    }
}

// ---------------------------------------------------------------------------
// Handlers (transport-free, mirroring handlers.rs)
// ---------------------------------------------------------------------------

/// `board.list`: the projection read. Reuses the existing list query (no second
/// store) filtered by the (rig, workspace) scope key, then buckets into columns.
pub async fn run_board_list(
    issues: &DoltIssues,
    args: &BoardList,
    validate_only: bool,
) -> Result<Option<BoardSnapshot>, AppError> {
    args.validate()?;
    if validate_only {
        return Ok(None);
    }
    let filter = IssueFilter {
        rig: Some(args.rig.clone()),
        workspace: Some(args.workspace.clone()),
        parent_id: args.epic.clone(),
        assignee: args.assignee.clone(),
        priority_max: args.priority_max,
        issue_type: args.issue_type.clone(),
        // The board reads the whole scoped set — a Kanban renders every card;
        // the store's max-limit ceiling still bounds a pathological corpus.
        limit: Some(gt_store_dolt::issues_max_limit()),
        ..Default::default()
    };
    let rows = issues.list(&filter).await?;
    let parent_map = issues.parent_map(&args.rig, &args.workspace).await?;
    Ok(Some(project_board(
        &args.rig,
        &args.workspace,
        rows,
        args.group_by.as_deref(),
        &parent_map,
    )))
}

/// Input for `board.scopes` — no parameters; the projection is global by
/// design (it answers "which boards exist at all?", the question the scope
/// selectors ask BEFORE a (rig, workspace) is chosen).
#[derive(Debug, Clone, Default, Serialize, Deserialize, JsonSchema)]
#[cfg_attr(feature = "axum", derive(utoipa::ToSchema, utoipa::IntoParams))]
pub struct BoardScopes {}

/// One real board scope: a `(rig, workspace)` pair that actually has beads.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[cfg_attr(feature = "axum", derive(utoipa::ToSchema))]
pub struct BoardScope {
    /// Rig half of the scope key, as stored on the beads (id-prefix namespace —
    /// NOT necessarily a rig-catalog name).
    pub rig: String,
    /// Workspace half of the scope key (the workspace id/slug).
    pub workspace: String,
}

/// Dedupe the flat row set into the sorted distinct `(rig, workspace)` pairs.
/// Pure — unit-testable without a store.
pub fn project_scopes(rows: &[IssueRow]) -> Vec<BoardScope> {
    let mut pairs: Vec<(String, String)> = rows
        .iter()
        .map(|r| (r.rig.clone(), r.workspace.clone()))
        .filter(|(r, w)| !r.is_empty() && !w.is_empty())
        .collect();
    pairs.sort();
    pairs.dedup();
    pairs
        .into_iter()
        .map(|(rig, workspace)| BoardScope { rig, workspace })
        .collect()
}

/// `board.scopes`: the distinct `(rig, workspace)` pairs present in the
/// tracker — the REAL boards, against which UI selectors are contrasted
/// (hq-26d679). Reuses the same list query as `board.list` (no second store);
/// catalog entries with no beads do not appear, and bead namespaces absent
/// from the catalog (e.g. `hq`) do.
pub async fn run_board_scopes(issues: &DoltIssues) -> Result<Vec<BoardScope>, AppError> {
    let rows = issues
        .list(&IssueFilter {
            // Unscoped on purpose: None rig/workspace = no filter (back-compat
            // semantics of the list query); the ceiling still bounds the read.
            limit: Some(gt_store_dolt::issues_max_limit()),
            ..Default::default()
        })
        .await?;
    Ok(project_scopes(&rows))
}

/// Resolve the rank for placing `moving_id` in `(rig, workspace, column)`
/// between the `after`/`before` neighbor card ids (both optional — neither
/// appends at the bottom). Lazily backfills an unranked column with
/// [`spread_ranks`] (the ADR's "lazy backfill on first reorder") and rebalances
/// on midpoint exhaustion, so the returned rank is always usable.
async fn placement_rank(
    issues: &DoltIssues,
    rig: &str,
    workspace: &str,
    column: IssueStatus,
    moving_id: &str,
    after: Option<&str>,
    before: Option<&str>,
) -> Result<String, AppError> {
    let mut col = issues.column_ranks(rig, workspace, column).await?;
    col.retain(|(id, _)| id != moving_id);

    // Lazy backfill: any unranked card would make neighbor math meaningless.
    if col.iter().any(|(_, r)| r.is_empty()) {
        let ranks = spread_ranks(col.len());
        let assign: Vec<(String, String)> = col
            .iter()
            .map(|(id, _)| id.clone())
            .zip(ranks)
            .collect();
        issues.board_set_ranks(rig, workspace, &assign).await?;
        col = assign;
    }

    let neighbor = |id: &str| -> Result<usize, AppError> {
        col.iter().position(|(cid, _)| cid == id).ok_or_else(|| {
            AppError::Validation(format!(
                "neighbor card `{id}` is not in the {} column of {rig}/{workspace}",
                column.as_str()
            ))
        })
    };
    // (lower, upper) rank bounds for the new position.
    let bounds = |col: &[(String, String)]| -> Result<(Option<String>, Option<String>), AppError> {
        Ok(match (after, before) {
            (Some(a), Some(b)) => (Some(col[neighbor(a)?].1.clone()), Some(col[neighbor(b)?].1.clone())),
            (Some(a), None) => {
                let i = neighbor(a)?;
                (Some(col[i].1.clone()), col.get(i + 1).map(|(_, r)| r.clone()))
            }
            (None, Some(b)) => {
                let i = neighbor(b)?;
                (i.checked_sub(1).and_then(|p| col.get(p)).map(|(_, r)| r.clone()), Some(col[i].1.clone()))
            }
            // Append at the bottom of the column.
            (None, None) => (col.last().map(|(_, r)| r.clone()), None),
        })
    };

    let (lo, hi) = bounds(&col)?;
    if let Some(rank) = rank_between(lo.as_deref(), hi.as_deref()) {
        return Ok(rank);
    }
    // Midpoint exhausted (adjacent/duplicate ranks): rebalance the whole column
    // once, then the retry is guaranteed room.
    let ranks = spread_ranks(col.len());
    let assign: Vec<(String, String)> = col.iter().map(|(id, _)| id.clone()).zip(ranks).collect();
    issues.board_set_ranks(rig, workspace, &assign).await?;
    let (lo, hi) = bounds(&assign)?;
    rank_between(lo.as_deref(), hi.as_deref()).ok_or_else(|| {
        AppError::Other("rank space exhausted even after rebalance".into())
    })
}

/// `board.move`: column change + placement, atomically (one Dolt commit). The
/// returned string is the rank the card landed at.
pub async fn run_board_move(
    issues: &DoltIssues,
    args: &BoardMove,
    validate_only: bool,
) -> Result<Option<String>, AppError> {
    args.validate()?;
    if validate_only {
        return Ok(None);
    }
    let target = args.target();
    let rank = placement_rank(
        issues,
        &args.rig,
        &args.workspace,
        target,
        &args.id,
        args.after.as_deref(),
        args.before.as_deref(),
    )
    .await?;
    issues
        .board_move(&args.id, &args.rig, &args.workspace, Some(target), &rank)
        .await?;
    Ok(Some(rank))
}

/// `board.reorder`: rank-only placement within the card's CURRENT column.
pub async fn run_board_reorder(
    issues: &DoltIssues,
    args: &BoardReorder,
    validate_only: bool,
) -> Result<Option<String>, AppError> {
    args.validate()?;
    if validate_only {
        return Ok(None);
    }
    // The card's current column anchors the neighbor math; reading it also
    // fails fast (NotFound) on a bogus id before any rank mutation.
    let detail = issues
        .get_detail(&args.id)
        .await?
        .ok_or_else(|| AppError::NotFound(format!("issue {}", args.id)))?;
    if detail.rig != args.rig || detail.workspace != args.workspace {
        return Err(AppError::Validation(format!(
            "issue {} is outside the caller's board scope (rig={}, workspace={})",
            args.id, args.rig, args.workspace
        )));
    }
    let column = IssueStatus::parse(&detail.status).ok_or_else(|| {
        AppError::Other(format!("issue {} carries unknown status `{}`", args.id, detail.status))
    })?;
    let rank = placement_rank(
        issues,
        &args.rig,
        &args.workspace,
        column,
        &args.id,
        args.after.as_deref(),
        args.before.as_deref(),
    )
    .await?;
    issues
        .board_move(&args.id, &args.rig, &args.workspace, None, &rank)
        .await?;
    Ok(Some(rank))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn row(id: &str, status: &str, rank: &str, assignee: Option<&str>, priority: i32) -> IssueRow {
        // Round-trip through serde so the non-board fields take their defaults
        // without spelling the whole struct here.
        let mut v: IssueRow = serde_json::from_value(serde_json::json!({
            "id": id, "title": id, "status": status, "priority": priority,
            "issue_type": "task", "assignee": assignee, "owner": null,
            "created_at": null, "updated_at": null, "closed_at": null,
            "spec_id": null, "role_scope": null,
        }))
        .expect("row");
        v.board_rank = rank.to_string();
        v
    }

    // ---- lexorank ----

    #[test]
    fn midpoint_between_open_bounds_exists() {
        let m = rank_between(None, None).expect("mid");
        assert!(!m.is_empty());
        let lo = rank_between(None, Some(&m)).expect("low");
        let hi = rank_between(Some(&m), None).expect("high");
        assert!(lo < m && m < hi);
    }

    #[test]
    fn midpoint_is_strictly_between() {
        for (a, b) in [("b", "d"), ("abc", "abd"), ("a", "b"), ("az", "b"), ("n", "naa")] {
            let m = rank_between(Some(a), Some(b)).expect(a);
            assert!(a < m.as_str() && m.as_str() < b, "{a} < {m} < {b}");
        }
    }

    #[test]
    fn repeated_append_keeps_growing() {
        let mut last = rank_between(None, None).unwrap();
        for _ in 0..50 {
            let next = rank_between(Some(&last), None).unwrap();
            assert!(next > last);
            last = next;
        }
    }

    #[test]
    fn repeated_bisection_always_finds_room() {
        // Squeeze 100 midpoints into the same gap — the string just grows.
        let mut lo = "b".to_string();
        let hi = "c".to_string();
        for _ in 0..100 {
            let m = rank_between(Some(&lo), Some(&hi)).unwrap();
            assert!(lo < m && m < hi);
            lo = m;
        }
    }

    #[test]
    fn equal_or_inverted_bounds_signal_rebalance() {
        assert!(rank_between(Some("m"), Some("m")).is_none());
        assert!(rank_between(Some("z"), Some("a")).is_none());
    }

    #[test]
    fn spread_is_strictly_increasing_and_sized() {
        for n in [1usize, 2, 5, 26, 100, 1000] {
            let ranks = spread_ranks(n);
            assert_eq!(ranks.len(), n);
            assert!(ranks.windows(2).all(|w| w[0] < w[1]), "n={n}");
            // Spread ranks leave room before the first and after the last.
            assert!(rank_between(None, Some(&ranks[0])).is_some());
            assert!(rank_between(Some(&ranks[n - 1]), None).is_some());
            // And between every adjacent pair.
            assert!(ranks
                .windows(2)
                .all(|w| rank_between(Some(&w[0]), Some(&w[1])).is_some()));
        }
    }

    // ---- projection ----

    #[test]
    fn buckets_by_status_and_orders_by_rank_with_unranked_tail() {
        let rows = vec![
            row("c", "open", "", None, 2),
            row("a", "open", "m", None, 2),
            row("b", "open", "d", None, 2),
            row("w", "working", "a", None, 2),
            row("z", "closed", "", None, 2),
        ];
        let board = project_board("hq", "default", rows, None, &std::collections::HashMap::new());
        assert_eq!(board.columns.len(), 3);
        let open: Vec<&str> = board.columns[0].cards.iter().map(|c| c.id.as_str()).collect();
        // Ranked d < m first, unranked tail last.
        assert_eq!(open, ["b", "a", "c"]);
        assert_eq!(board.columns[1].cards[0].id, "w");
        assert_eq!(board.columns[2].cards[0].id, "z");
    }

    #[test]
    fn group_by_assignee_builds_lanes_in_board_order() {
        let rows = vec![
            row("a", "open", "b", Some("ana"), 2),
            row("b", "open", "c", Some("bob"), 2),
            row("c", "open", "d", Some("ana"), 2),
            row("d", "open", "", None, 2),
        ];
        let board = project_board("hq", "default", rows, Some("assignee"), &std::collections::HashMap::new());
        let lanes = board.columns[0].lanes.as_ref().expect("lanes");
        assert_eq!(lanes.len(), 3);
        assert_eq!(lanes[0].key, "ana");
        assert_eq!(
            lanes[0].cards.iter().map(|c| c.id.as_str()).collect::<Vec<_>>(),
            ["a", "c"]
        );
        assert_eq!(lanes[1].key, "bob");
        assert_eq!(lanes[2].key, ""); // unassigned tail lane
    }

    #[test]
    fn group_by_priority_keys_lanes_numerically() {
        let rows = vec![row("a", "open", "b", None, 0), row("b", "open", "c", None, 2)];
        let board = project_board("hq", "default", rows, Some("priority"), &std::collections::HashMap::new());
        let lanes = board.columns[0].lanes.as_ref().unwrap();
        assert_eq!(lanes[0].key, "0");
        assert_eq!(lanes[1].key, "2");
    }

    // ---- command validation ----

    #[test]
    fn board_list_requires_scope_and_known_group_by() {
        let mut a = BoardList {
            rig: "hq".into(),
            workspace: "default".into(),
            epic: None,
            group_by: None,
            assignee: None,
            priority_max: None,
            issue_type: None,
        };
        assert!(a.validate().is_ok());
        a.group_by = Some("priority".into());
        assert!(a.validate().is_ok());
        a.group_by = Some("bogus".into());
        assert!(a.validate().is_err());
        a.group_by = None;
        a.workspace = "".into();
        assert!(a.validate().is_err());
        a.workspace = "default".into();
        a.rig = " ".into();
        assert!(a.validate().is_err());
    }

    #[test]
    fn board_move_requires_id_scope_and_legal_column() {
        let mut a = BoardMove {
            id: "hq-1".into(),
            rig: "hq".into(),
            workspace: "default".into(),
            column: "working".into(),
            after: None,
            before: None,
        };
        assert!(a.validate().is_ok());
        assert_eq!(a.target(), IssueStatus::Working);
        a.column = "doing".into();
        assert!(a.validate().is_err());
        a.column = "open".into();
        a.id = "".into();
        assert!(a.validate().is_err());
        a.id = "hq-1".into();
        a.workspace = "".into();
        assert!(a.validate().is_err());
    }

    #[test]
    fn board_reorder_requires_id_and_scope() {
        let a = BoardReorder {
            id: "hq-1".into(),
            rig: "hq".into(),
            workspace: "default".into(),
            after: None,
            before: None,
        };
        assert!(a.validate().is_ok());
        let bad = BoardReorder { rig: "".into(), ..a };
        assert!(bad.validate().is_err());
    }

    #[test]
    fn project_scopes_dedupes_sorts_and_drops_blank_keys() {
        let scoped = |id: &str, rig: &str, ws: &str| {
            let mut r = row(id, "open", "", None, 1);
            r.rig = rig.to_string();
            r.workspace = ws.to_string();
            r
        };
        let rows = vec![
            scoped("hq-2", "hq", "default"),
            scoped("gtcore-1", "gtcore", "default"),
            scoped("hq-1", "hq", "default"),
            scoped("hq-3", "hq", "confiar"),
            scoped("x-1", "", "default"),
            scoped("x-2", "hq", ""),
        ];
        let scopes = project_scopes(&rows);
        assert_eq!(
            scopes,
            vec![
                BoardScope { rig: "gtcore".into(), workspace: "default".into() },
                BoardScope { rig: "hq".into(), workspace: "confiar".into() },
                BoardScope { rig: "hq".into(), workspace: "default".into() },
            ]
        );
    }
}
