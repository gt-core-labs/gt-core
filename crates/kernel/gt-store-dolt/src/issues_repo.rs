use mysql_async::prelude::*;
use mysql_async::Pool;

use crate::error::AppError;
use serde::{Deserialize, Serialize};

use crate::conn::map_err;

/// Status states the `issues.transition` tool (hq-mcp-issues.4) understands.
/// `bd`'s lifecycle uses additional internal labels (`hooked`, etc.) but those
/// are owned by the polecat actor — the user-facing surface stays open/working/
/// closed for predictable kanban semantics.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum IssueStatus {
    Open,
    Working,
    Closed,
}

impl IssueStatus {
    pub fn parse(s: &str) -> Option<Self> {
        match s {
            "open" => Some(Self::Open),
            "working" => Some(Self::Working),
            "closed" => Some(Self::Closed),
            _ => None,
        }
    }

    pub fn as_str(self) -> &'static str {
        match self {
            Self::Open => "open",
            Self::Working => "working",
            Self::Closed => "closed",
        }
    }

    /// Legal transitions in the issue state machine. `open ↔ working`, plus
    /// either side may close; `closed` re-opens through `open` but never jumps
    /// straight back to `working` — matches the example the bead description
    /// calls out (`closed -> working` is rejected).
    pub fn can_transition_to(self, target: Self) -> bool {
        use IssueStatus::*;
        matches!(
            (self, target),
            (Open, Working) | (Open, Closed) | (Working, Open) | (Working, Closed) | (Closed, Open)
        )
    }
}

/// Lifecycle phase a bead belongs to (docs/10 S1, hq-core-mcp.7). `P1..P4` form
/// a total order (declaration order is the ordinal): a bead is *phase-gated* when
/// its `phase` exceeds the [`open_phase`](DoltIssues::open_phase) currently open.
/// `P4` = kernel migration up from the upstream app (gated while the frontier sits at
/// `P3`); `P3` = gt-core multi-tenant work, currently open.
///
/// `Ord` is derived so `bead.phase > frontier.open_phase` is the gate predicate
/// in one comparison. serde (de)serializes the variants verbatim as the wire
/// tokens `"P1".."P4"`, matching the SQL `ENUM('P1','P2','P3','P4')` column.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
pub enum IssuePhase {
    P1,
    P2,
    P3,
    P4,
}

impl IssuePhase {
    /// Parse a wire/SQL token (`"P1".."P4"`) into the typed phase. `None` for any
    /// other string so an out-of-set value is rejected at the frontier instead of
    /// silently landing in the column.
    pub fn parse(s: &str) -> Option<Self> {
        match s {
            "P1" => Some(Self::P1),
            "P2" => Some(Self::P2),
            "P3" => Some(Self::P3),
            "P4" => Some(Self::P4),
            _ => None,
        }
    }

    /// The canonical token (`"P1".."P4"`) stored in the `ENUM` column.
    pub fn as_str(self) -> &'static str {
        match self {
            Self::P1 => "P1",
            Self::P2 => "P2",
            Self::P3 => "P3",
            Self::P4 => "P4",
        }
    }
}

/// Filters applied when listing issues for the `gt://issues` MCP resource
/// (hq-mcp-issues.1). All fields are optional and combined with `AND`; `None`
/// means "no filter on this column". `limit` caps the result set so a noisy
/// query can't dump the whole table over the MCP wire.
#[derive(Debug, Default, Clone)]
pub struct IssueFilter {
    /// Match `status` exactly against any of the values (typically
    /// `open`/`working`/`closed`). Empty vec = no filter.
    pub status: Vec<String>,
    /// Match `priority <= priority_max` (0 = highest priority).
    pub priority_max: Option<u8>,
    /// Match `assignee` exactly. `""` (empty string) matches the canonical
    /// "unassigned" value the schema stores as `''`.
    pub assignee: Option<String>,
    /// Match beads whose `issue_relations` table has a `child_of` row pointing to this epic id.
    pub parent_id: Option<String>,
    /// Match `issue_type` exactly (`epic`, `task`, `spike`, ...).
    pub issue_type: Option<String>,
    /// Page size. When `None`, [`DoltIssues::list`] falls back to
    /// `GT_ISSUES_DEFAULT_LIMIT` (env, fallback 200) and is always clamped to
    /// `GT_ISSUES_MAX_LIMIT` (env, fallback 10000) — see [`issues_default_limit`]
    /// / [`issues_max_limit`]. Pairs with [`offset`](IssueFilter::offset) for
    /// less-style paging (hq-core-mcp.13).
    pub limit: Option<u32>,
    /// Zero-based row offset into the stable-ordered result set (the `?offset=M`
    /// querystring). `None` ⇒ start at 0. Advance it by the page size to walk
    /// forward (less page-down); decrement to walk back. Ignored on the unbounded
    /// `?ready=true` frontier (hq-core-mcp.13).
    pub offset: Option<u32>,
    /// Include the heavy text bodies (`description`/`design`/
    /// `acceptance_criteria`/`notes`) inline on every row (hq-gap-issues-list-
    /// full). Default `false` keeps the snapshot cheap; `true` lets a caller
    /// review a whole sub-epic without a per-bead `gt://issue/{id}` round-trip.
    pub full: bool,
    /// Narrow the snapshot to **sound** beads only — every readiness clause holds
    /// (hq-core-mcp.11, docs/10 §S4). NOTE: [`DoltIssues::list`] does **not**
    /// honour this flag (readiness needs the phase frontier + the delivered index
    /// + the git tree, which live above the store); the server applies it after
    /// the query via `gt_issues::resources::filter_ready`. It rides on the filter
    /// purely so the querystring parser has one place to land `?ready=true`.
    pub ready: bool,
    /// Narrow to a single rig (hq-rig-isolation.1). `None` = workspace-wide (back-compat).
    pub rig: Option<String>,
    /// Narrow to one board workspace (hq-62130a). The board projection always
    /// sets BOTH this and [`rig`](IssueFilter::rig) — the (rig, workspace) scope
    /// key (ADR D4). `None` = no workspace filter (back-compat for plain lists).
    pub workspace: Option<String>,
}

/// Snapshot row returned by [`DoltIssues::list`]. Mirrors the columns dashboards
/// and `bd list` consume; the heavy text bodies (`description`/`design`/
/// `acceptance_criteria`/`notes`) live on the per-issue `issues.get` tool added
/// by the rest of the epic so listings stay cheap.
#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct IssueRow {
    pub id: String,
    pub title: String,
    pub status: String,
    pub priority: i32,
    pub issue_type: String,
    pub assignee: Option<String>,
    pub owner: Option<String>,
    pub created_at: Option<String>,
    pub updated_at: Option<String>,
    pub closed_at: Option<String>,
    pub spec_id: Option<String>,
    /// JSON array of taxonomy domains (hq-taxon.3). Serialised as a raw JSON
    /// string so consumers (`gt-mcp` resources, `bd` mirrors) re-parse without
    /// the store needing to know the closed-set `Domain` enum.
    #[serde(default = "default_json_array")]
    pub domain_json: String,
    /// JSON array of impact surfaces (crate names or repo paths).
    #[serde(default = "default_json_array")]
    pub surface_json: String,
    /// Optional `role_scope` discriminator (e.g. `sheriff`); `None` when no
    /// role owns the bead. Stored as `VARCHAR(32)` so `bd` legacy callers can
    /// keep filtering with plain string equality.
    pub role_scope: Option<String>,
    /// Optimistic-concurrency token (hq-mcp-issues.8). Monotonic per write;
    /// pass the value you read as `expected_version` to `issues.update` to make
    /// a stale edit fail instead of clobbering a concurrent one.
    #[serde(default)]
    pub version: i64,
    /// Lifecycle phase token (`"P1".."P4"`, hq-core-mcp.7). Exposed on the cheap
    /// snapshot too (hq-core-mcp.11) so `gt://issues?ready=true` can apply the
    /// phase gate without a per-bead detail fetch. Defaults to `"P1"` for legacy
    /// rows.
    #[serde(default = "default_phase")]
    pub phase: String,
    /// Full 40-hex sha that delivered this bead's code (hq-core-mcp.10, docs/10
    /// §S2). Set by `close` phase-2 when the closing commit_sha actually touches a
    /// non-`planned` surface path; `None` until then (a wontfix/no-deliverable
    /// close leaves it null). This — not `status='closed'` — is the trustworthy
    /// delivery signal dependency readiness (S4) evaluates against.
    #[serde(default)]
    pub delivered_sha: Option<String>,
    /// The rig this bead belongs to (hq-rig-isolation.1). Derived from the bead id
    /// prefix at create time and persisted for efficient filter-by-rig queries.
    #[serde(default)]
    pub rig: String,
    /// The workspace half of the board scope key (hq-62130a, ADR D3/D4). Every
    /// card belongs to BOTH a rig and a workspace; legacy rows backfill to
    /// `'default'`.
    #[serde(default = "default_workspace")]
    pub workspace: String,
    /// Lexorank ordering token WITHIN a (rig, workspace, status) column
    /// (hq-62130a). `''` (the default) sorts after every ranked card — new cards
    /// append until a move/reorder assigns a rank.
    #[serde(default)]
    pub board_rank: String,
    /// Planning estimate (mockup "Horas Est."), `None` until planned.
    #[serde(default)]
    pub estimated_hours: Option<f64>,
    /// Planned start date `YYYY-MM-DD` (mockup "Fecha Inicio").
    #[serde(default)]
    pub start_date: Option<String>,
    /// Planned end date `YYYY-MM-DD` (mockup "Fecha Fin") — drives the retrasos
    /// metric (`due_date < now AND status != closed`).
    #[serde(default)]
    pub due_date: Option<String>,
    /// Heavy text bodies, populated only when [`IssueFilter::full`] is set
    /// (hq-gap-issues-list-full). `None` in the cheap default snapshot and
    /// skipped from the JSON entirely, so back-compat consumers see the exact
    /// same shape as before.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub description: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub design: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub acceptance_criteria: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub notes: Option<String>,
}

fn default_json_array() -> String {
    "[]".to_string()
}

fn default_workspace() -> String {
    "default".to_string()
}

/// One less-style page of the `gt://issues` snapshot (hq-core-mcp.13). Carries
/// the slice plus enough metadata that a caller never mistakes a page for the
/// whole set: `total` is the full count of the filter (pre-pagination),
/// `next_offset` is where the following page begins, and `has_more` says whether
/// another page exists. A consumer walks the corpus by advancing `offset` to
/// `next_offset` until `has_more` is `false`.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct IssuePage {
    /// The rows on this page (already `ORDER BY`-stable, see [`DoltIssues::list`]).
    pub rows: Vec<IssueRow>,
    /// Full count of rows matching the filter, independent of `limit`/`offset`.
    pub total: i64,
    /// Offset of the next page = `offset + rows.len()`. Feed it back as `?offset=`
    /// to page forward.
    pub next_offset: u32,
    /// `true` while `next_offset < total` — another page remains.
    pub has_more: bool,
}

/// Default page size for [`DoltIssues::list`] when `IssueFilter::limit` is unset:
/// `GT_ISSUES_DEFAULT_LIMIT` (env), falling back to 200. The operator retunes the
/// page size without recompiling (hq-core-mcp.13).
pub fn issues_default_limit() -> u32 {
    std::env::var("GT_ISSUES_DEFAULT_LIMIT")
        .ok()
        .and_then(|v| v.trim().parse::<u32>().ok())
        .filter(|n| *n > 0)
        .unwrap_or(200)
}

/// Hard ceiling every [`DoltIssues::list`] page is clamped to:
/// `GT_ISSUES_MAX_LIMIT` (env), falling back to 10000. A `?limit=` above this is
/// honoured only up to the ceiling (hq-core-mcp.13).
pub fn issues_max_limit() -> u32 {
    std::env::var("GT_ISSUES_MAX_LIMIT")
        .ok()
        .and_then(|v| v.trim().parse::<u32>().ok())
        .filter(|n| *n > 0)
        .unwrap_or(10_000)
}

/// Result of [`DoltIssues::claim`]. `Won` means this caller now holds the bead
/// (status moved to `working`, owner stamped); `Lost` carries the current
/// `status` + `holder` so the loser can report who owns it and stand down.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", tag = "outcome")]
pub enum ClaimOutcome {
    Won,
    Lost { status: String, holder: String },
}

/// The per-dependency facts readiness needs to decide whether a `depends_on`
/// edge is satisfied (hq-core-mcp.12 §C, docs/10 §S4). Built once for the whole
/// table by [`DoltIssues::dep_index`] so `gt://issues?ready=true` resolves each
/// edge with a single one-hop lookup.
///
/// The epic-dep rule lives in the readiness predicate, not here: an `epic`
/// dependency delivers-by-close (`status == "closed"`; epics have no single
/// `delivered_sha`), a non-epic dependency by `delivered` (`delivered_sha` is
/// non-NULL).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DepFact {
    /// The dependency's `issue_type` (`epic` is treated specially).
    pub issue_type: String,
    /// The dependency's lifecycle `status` (`closed` satisfies an epic dep).
    pub status: String,
    /// Whether the dependency has a non-NULL `delivered_sha` (satisfies a
    /// non-epic dep).
    pub delivered: bool,
}

/// One issue stamped by [`DoltIssues::archive_old_closed`]: its id and `issue_type`. The type lets
/// the caller drive type-specific cleanup off the archived set (e.g. an `epic` archive soft-deletes
/// that epic's `documents`/embeddings, hq-docs-archive-sync) without a second round-trip to Dolt.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ArchivedIssue {
    /// The archived bead id.
    pub id: String,
    /// Its `issue_type` (`epic`, `task`, …).
    pub issue_type: String,
}

/// Full single-issue row returned by [`DoltIssues::get_detail`]. Superset of
/// [`IssueRow`] that also carries the heavy text bodies (`description`,
/// `design`, `acceptance_criteria`, `notes`) the list snapshot omits to stay
/// cheap. This is the read path an agent uses after claiming a bead so it can
/// see the actual spec instead of inventing it from the title.
#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct IssueDetail {
    pub id: String,
    pub title: String,
    pub status: String,
    pub priority: i32,
    pub issue_type: String,
    pub assignee: Option<String>,
    pub owner: Option<String>,
    pub created_at: Option<String>,
    pub updated_at: Option<String>,
    pub closed_at: Option<String>,
    pub spec_id: Option<String>,
    pub description: String,
    pub design: String,
    pub acceptance_criteria: String,
    pub notes: String,
    #[serde(default = "default_json_array")]
    pub domain_json: String,
    #[serde(default = "default_json_array")]
    pub surface_json: String,
    pub role_scope: Option<String>,
    /// Optimistic-concurrency token (hq-mcp-issues.8). See [`IssueRow::version`].
    #[serde(default)]
    pub version: i64,
    /// Lifecycle phase token (`"P1".."P4"`, hq-core-mcp.7). Surfaced here so the
    /// claim echo (hq-core-mcp.8 / docs/10 S5) and the `gt://issue/{id}` resource
    /// show the gate an agent must respect. Defaults to `"P1"` for legacy rows.
    #[serde(default = "default_phase")]
    pub phase: String,
    /// Delivering commit sha (hq-core-mcp.10, docs/10 §S2). See
    /// [`IssueRow::delivered_sha`].
    #[serde(default)]
    pub delivered_sha: Option<String>,
    /// The rig this bead belongs to (hq-rig-isolation.1). See [`IssueRow::rig`].
    #[serde(default)]
    pub rig: String,
    /// Board workspace scope (hq-62130a). See [`IssueRow::workspace`].
    #[serde(default = "default_workspace")]
    pub workspace: String,
    /// Lexorank column ordering token (hq-62130a). See [`IssueRow::board_rank`].
    #[serde(default)]
    pub board_rank: String,
    /// Planning estimate in hours (hq-62130a).
    #[serde(default)]
    pub estimated_hours: Option<f64>,
    /// Planned start date `YYYY-MM-DD` (hq-62130a).
    #[serde(default)]
    pub start_date: Option<String>,
    /// Planned end date `YYYY-MM-DD` (hq-62130a).
    #[serde(default)]
    pub due_date: Option<String>,
}

/// A row from the `issue_relations` table.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct IssueRelation {
    pub from_id: String,
    pub to_id: String,
    pub rel_type: String,
}

fn default_phase() -> String {
    "P1".to_string()
}

/// Maps a patch string to a SQL value where an empty string means `NULL`.
/// Used by [`DoltIssues::update`] for the nullable "clear-able" columns
/// (`assignee`/`owner`/`external_ref`) so passing `""` detaches the value
/// rather than storing a literal empty string the read side cannot distinguish
/// from "unassigned".
fn str_or_null(v: &str) -> mysql_async::Value {
    if v.is_empty() {
        mysql_async::Value::NULL
    } else {
        mysql_async::Value::from(v.to_string())
    }
}

/// Patch payload for [`DoltIssues::update`] (hq-mcp-issues.3). Every field is
/// `Option<T>`: `None` leaves the column untouched, `Some(_)` overwrites it.
/// Status changes belong to [`DoltIssues::transition`] (hq-mcp-issues.4) so they
/// are deliberately absent here — keeping write paths separable for audit /
/// scope grants ("read + edit-fields" vs "transition").
#[derive(Debug, Default, Clone)]
pub struct IssuePatch {
    /// New title. `Some("")` is rejected upstream — schema is `NOT NULL`.
    pub title: Option<String>,
    /// New description. Empty allowed.
    pub description: Option<String>,
    /// New design notes. Empty allowed.
    pub design: Option<String>,
    /// New acceptance criteria. Empty allowed.
    pub acceptance_criteria: Option<String>,
    /// New free-form notes. Empty allowed.
    pub notes: Option<String>,
    /// New priority `0..=2` (0 = P0).
    pub priority: Option<u8>,
    /// New `issue_type` (`epic`/`task`/`spike`/...).
    pub issue_type: Option<String>,
    /// New assignee. `Some(String::new())` stores `''` (canonical "unassigned"
    /// for the column when nullable-with-default is in play; schema accepts
    /// either form). `None` leaves the column alone.
    pub assignee: Option<String>,
    /// New owner. Same nullability shape as `assignee`.
    pub owner: Option<String>,
    /// New parent epic id. `Some("")` clears the `child_of` relation; `Some(id)` upserts it; `None` leaves it unchanged.
    pub parent_id: Option<String>,
    /// New `domain_json` — raw JSON array string (e.g. `["orch.merge"]`).
    /// `None` leaves the column alone; `Some(_)` overwrites verbatim. The
    /// frontier serializes typed `Vec<Domain>` so the stored form round-trips.
    pub domain_json: Option<String>,
    /// New `surface_json` — raw JSON array string of crate names / repo paths.
    /// `None` leaves the column alone; `Some(_)` overwrites verbatim.
    pub surface_json: Option<String>,
    /// New lifecycle phase token (`"P1".."P4"`, hq-core-mcp.7). `None` leaves
    /// the column untouched; `Some(_)` is a scalar overwrite the frontier
    /// validates against [`IssuePhase`] before the write.
    pub phase: Option<String>,
    /// New planning estimate in hours (hq-62130a). Negative rejected upstream.
    pub estimated_hours: Option<f64>,
    /// New planned start date `YYYY-MM-DD` (hq-62130a). Empty string clears to
    /// SQL `NULL`, mirroring the `assignee`/`external_ref` clear semantics.
    pub start_date: Option<String>,
    /// New planned end date `YYYY-MM-DD` (hq-62130a). Empty string clears.
    pub due_date: Option<String>,
    /// New board workspace (hq-62130a) — re-scopes the card to another project.
    /// Empty rejected upstream (a card always belongs to a workspace).
    pub workspace: Option<String>,
    /// Optimistic-concurrency guard (hq-mcp-issues.8). `None` = unguarded
    /// last-write-wins (back-compat). `Some(v)` makes the UPDATE match only when
    /// the row's current `version` equals `v`; a mismatch surfaces as
    /// `AppError::Conflict` so a stale edit fails instead of clobbering. This is
    /// a guard, NOT a column to set — [`IssuePatch::is_empty`] ignores it.
    pub expected_version: Option<i64>,
}

impl IssuePatch {
    /// True when no field is set — the caller has nothing to patch, which the
    /// frontier should treat as `Validation` rather than emitting a no-op
    /// `UPDATE issues SET WHERE id = ?` (parses as a syntax error).
    pub fn is_empty(&self) -> bool {
        self.title.is_none()
            && self.description.is_none()
            && self.design.is_none()
            && self.acceptance_criteria.is_none()
            && self.notes.is_none()
            && self.priority.is_none()
            && self.issue_type.is_none()
            && self.assignee.is_none()
            && self.owner.is_none()
            && self.parent_id.is_none()
            && self.domain_json.is_none()
            && self.surface_json.is_none()
            && self.phase.is_none()
            && self.estimated_hours.is_none()
            && self.start_date.is_none()
            && self.due_date.is_none()
            && self.workspace.is_none()
    }
}

/// Insert payload for [`DoltIssues::insert`] (hq-mcp-issues.2). Mirrors the
/// required columns of `hq.issues`; the optional fields fall back to schema
/// defaults so callers only have to supply what the bead's design lists as
/// required (`id`, `title`, `priority`, `issue_type`, `created_by`).
#[derive(Debug, Clone, Default)]
pub struct NewIssue {
    /// Stable bead id. Must be unique; non-empty.
    pub id: String,
    /// Display title.
    pub title: String,
    /// Free-text body. Empty string is allowed and stored verbatim — the
    /// schema marks the column `NOT NULL` so `None` here defaults to `""`.
    pub description: String,
    /// Design notes. `NOT NULL` in schema; empty allowed.
    pub design: String,
    /// Acceptance criteria. `NOT NULL` in schema; empty allowed.
    pub acceptance_criteria: String,
    /// Free-form notes. `NOT NULL` in schema; empty allowed.
    pub notes: String,
    /// Priority `0..=2` (0 = P0). Schema default is `2`.
    pub priority: u8,
    /// `epic`/`task`/`spike`/... — domain string.
    pub issue_type: String,
    /// Bead creator. Maps to `created_by`.
    pub created_by: String,
    /// Optional parent epic id. When `Some(id)`, inserts a `child_of` relation into `issue_relations` after insert.
    pub parent_id: Option<String>,
    /// Optional assignee. `None` stores `NULL`.
    pub assignee: Option<String>,
    /// Optional initial owner. `None` stores schema default `''`.
    pub owner: Option<String>,
    /// Raw JSON array of `Domain` discriminators (e.g. `["orch.merge"]`).
    /// Empty string is normalised to `[]` by [`DoltIssues::insert`] so the
    /// schema's `NOT NULL` constraint is honoured even with a default-built
    /// `NewIssue` (hq-taxon.3).
    pub domain_json: String,
    /// Raw JSON array of impact surfaces (free-form strings).
    pub surface_json: String,
    /// Optional `role_scope` discriminator. `None` stores `NULL`.
    pub role_scope: Option<String>,
    /// Lifecycle phase token (`"P1".."P4"`, hq-core-mcp.7). `None` lets the
    /// column's `DEFAULT 'P1'` apply; `Some(_)` is a scalar overwrite the
    /// frontier validates against [`IssuePhase`] before insert.
    pub phase: Option<String>,
    /// The rig this bead belongs to (hq-rig-isolation.1). Derived from the bead id
    /// prefix by the composition layer before insert; defaults to `""`.
    pub rig: String,
    /// The board workspace the card lands in (hq-62130a). Empty string is
    /// normalised to `'default'` by [`DoltIssues::insert`] so every card carries
    /// the full (rig, workspace) scope key from birth.
    pub workspace: String,
}

/// Read-only Dolt adapter for the `issues` table. The canonical bead table is
/// `issues` (~25 cols), distinct from `beads` (5 cols, dispatcher-facing). The
/// MCP `gt://issues` resource (hq-mcp-issues.1) snapshots it; the write-side
/// tools (`.2`-`.5`) layer on top once `hq-fe-api-w.1` lands the command-bus.
pub struct DoltIssues {
    pool: Pool,
}

/// The base `issues` table DDL — the schema gt-core seeds on a fresh Dolt database. Moved
/// from the gt-app deploy's `dolt-init.sql` so the schema lives with the code that owns it;
/// `ensure_schema` runs it when the table is absent, then layers the taxonomy columns on top.
const ISSUES_BASE_TABLE_SQL: &str = "CREATE TABLE IF NOT EXISTS issues (
    id                  VARCHAR(255) PRIMARY KEY,
    content_hash        VARCHAR(64),
    title               VARCHAR(500) NOT NULL,
    description         TEXT NOT NULL,
    design              TEXT NOT NULL,
    acceptance_criteria TEXT NOT NULL,
    notes               TEXT NOT NULL,
    status              VARCHAR(32) NOT NULL DEFAULT 'open',
    priority            INT NOT NULL DEFAULT 2,
    issue_type          VARCHAR(32) NOT NULL DEFAULT 'task',
    assignee            VARCHAR(255),
    estimated_minutes   INT,
    created_at          DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
    created_by          VARCHAR(255) DEFAULT '',
    owner               VARCHAR(255) DEFAULT '',
    updated_at          DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
    closed_at           DATETIME,
    closed_by_session   VARCHAR(255) DEFAULT '',
    spec_id             VARCHAR(1024)
)";

/// Split a MySQL-wire URL into `(server_url_without_db, Some(db))`, or `(url, None)` when it
/// carries no database path segment. Used by [`DoltIssues::ensure_database`] to `CREATE
/// DATABASE` over a database-less connection before the main pool binds to that database.
fn split_database(url: &str) -> (String, Option<String>) {
    let Some((scheme, rest)) = url.split_once("://") else {
        return (url.to_string(), None);
    };
    match rest.split_once('/') {
        // `path` is `<db>[?params]`; the db is the segment before any `?` or further `/`.
        Some((authority, path)) => {
            let db = path.split(['/', '?']).next().unwrap_or("");
            let db = (!db.is_empty()).then(|| db.to_string());
            (format!("{scheme}://{authority}"), db)
        }
        None => (url.to_string(), None),
    }
}

impl DoltIssues {
    pub fn new(pool: Pool) -> Self {
        Self { pool }
    }

    pub fn connect(url: &str) -> Result<Self, AppError> {
        Ok(Self::new(crate::conn::connect(url)?))
    }

    pub fn pool(&self) -> &Pool {
        &self.pool
    }

    /// Ensure the target database in `url` exists (idempotent), creating it via a
    /// database-less connection to the same server.
    ///
    /// A fresh Dolt volume ships no databases, so a pool to `…/hq` would fail to connect
    /// before [`ensure_schema`](Self::ensure_schema) could seed anything. gt-core owns this
    /// bootstrap (it knows its own schema) rather than a deploy-side init script: the server
    /// calls this once on startup, then connects normally. No-op when the database already
    /// exists, or when `url` carries no database segment.
    pub async fn ensure_database(url: &str) -> Result<(), AppError> {
        let (server_url, db) = split_database(url);
        let Some(db) = db else { return Ok(()) };
        let pool = Pool::from_url(&server_url)
            .map_err(|e| AppError::Other(format!("dolt connect (server): {e}")))?;

        // Retry the first connection: on a cold `docker compose up` the Dolt server is often
        // still starting when this runs, so a bare attempt races to "connection refused". The
        // bootstrap owns the wait rather than leaning on the orchestrator's restart policy —
        // ~30s (15 × 2s) covers a normal server start; past that the error is real.
        let mut conn = {
            let mut attempt = 0u32;
            loop {
                match pool.get_conn().await {
                    Ok(c) => break c,
                    Err(e) if attempt < 15 => {
                        attempt += 1;
                        eprintln!("[gt-store-dolt] waiting for Dolt (attempt {attempt}): {e}");
                        tokio::time::sleep(std::time::Duration::from_secs(2)).await;
                    }
                    Err(e) => return Err(map_err(e)),
                }
            }
        };
        // `db` is the path segment of an operator-supplied URL; backtick-quote it. Dolt accepts
        // CREATE DATABASE over the MySQL wire.
        conn.query_drop(format!("CREATE DATABASE IF NOT EXISTS `{db}`"))
            .await
            .map_err(map_err)?;
        drop(conn);
        pool.disconnect().await.map_err(map_err)?;
        Ok(())
    }

    /// Confirm the `issues` table exists and adds the taxonomy columns the
    /// hq-taxon family layered on top (`domain_json`, `surface_json`,
    /// `depends_on_json`, `role_scope`). The table itself is owned by `bd` and
    /// pre-existing in hq; the column adds are idempotent — second runs are
    /// no-ops once `information_schema.columns` already lists them.
    ///
    /// Adds default `'[]'` for JSON arrays so existing rows backfill without a
    /// follow-up `UPDATE`; the actual `Domain` typing lives in `gt-mcp` and is
    /// re-validated on the write path.
    pub async fn ensure_schema(&self) -> Result<(), AppError> {
        let mut conn = self.pool.get_conn().await.map_err(map_err)?;
        let present: Option<i64> = conn
            .query_first(
                "SELECT 1 FROM information_schema.tables
                 WHERE table_schema = DATABASE() AND table_name = 'issues' LIMIT 1",
            )
            .await
            .map_err(map_err)?;
        if present.is_none() {
            // Seed the base `issues` table (gt-core owns this; upstream it was `bd`-created,
            // but this deploy has no `bd`). Idempotent via IF NOT EXISTS; the taxonomy columns
            // below then layer on top. Moved here from the gt-app deploy's dolt-init.sql.
            conn.query_drop(ISSUES_BASE_TABLE_SQL).await.map_err(map_err)?;
            conn.exec_drop(
                "CALL DOLT_COMMIT('-A', '-m', :msg)",
                mysql_async::params! { "msg" => "gt-core boot seed: hq.issues base table".to_string() },
            )
            .await
            .map_err(map_err)?;
        }

        let taxonomy_columns: &[(&str, &str)] = &[
            ("domain_json", "TEXT NOT NULL DEFAULT '[]'"),
            ("surface_json", "TEXT NOT NULL DEFAULT '[]'"),
            ("role_scope", "VARCHAR(32) NULL"),
            // hq-mcp-issues.8 — optimistic-concurrency token. Bumped on every
            // write path; `issues.update` can guard on it (expected_version) so
            // a stale edit fails loud instead of clobbering a concurrent write.
            ("version", "BIGINT NOT NULL DEFAULT 0"),
            // hq-core-mcp.7 (docs/10 S1) — per-bead lifecycle phase + the
            // timestamp the phase was last ratified. A bead is gated when its
            // `phase` exceeds the `phase_frontier.open_phase` (seeded below).
            ("phase", "ENUM('P1','P2','P3','P4') NOT NULL DEFAULT 'P1'"),
            ("phase_ratified_at", "TIMESTAMP NULL"),
            // hq-core-mcp.10 (docs/10 §S2) — delivering commit sha, stamped by
            // `close` phase-2 once the closing sha is verified to touch a
            // non-planned surface. NULL until delivered; the readiness signal.
            ("delivered_sha", "CHAR(40) NULL"),
            // hq-system-config: archive sweep stamps this when a closed issue is
            // rotated out of the live tracker view. NULL = not yet archived.
            ("archived_at", "DATETIME NULL"),
            // hq-rig-isolation.1 — rig a bead belongs to, derived from its id prefix at create
            // time and persisted for efficient filter-by-rig queries. Default '' so existing rows
            // backfill below; the backfill sets this to `SUBSTRING_INDEX(id, '-', 1)`.
            ("rig", "VARCHAR(255) NOT NULL DEFAULT ''"),
        ];

        let mut added_any = false;
        for (name, ddl) in taxonomy_columns {
            let exists: Option<i64> = conn
                .exec_first(
                    "SELECT 1 FROM information_schema.columns
                     WHERE table_schema = DATABASE()
                       AND table_name = 'issues'
                       AND column_name = :col LIMIT 1",
                    mysql_async::params! { "col" => *name },
                )
                .await
                .map_err(map_err)?;
            if exists.is_none() {
                // Column-name is not a bind parameter — only the closed-set
                // string literals from `taxonomy_columns` ever reach `format!`,
                // so there's no caller-controlled SQL here.
                let sql = format!("ALTER TABLE issues ADD COLUMN {name} {ddl}");
                conn.query_drop(sql).await.map_err(map_err)?;
                added_any = true;
            }
        }

        if added_any {
            conn.exec_drop(
                "CALL DOLT_COMMIT('-A', '-m', :msg)",
                mysql_async::params! {
                    "msg" => "hq-taxon.3: add taxonomy columns to issues".to_string(),
                },
            )
            .await
            .map_err(map_err)?;
        }

        // hq-62130a (ADR hq-423a4b D3) — the ONLY schema changes the Kanban board
        // is allowed: scoping, ordering, and the planning fields. One idempotent
        // column-add pass, mirroring the taxonomy block above.
        //
        // - `workspace`: scope key half — every card belongs to BOTH a rig (above)
        //   and a workspace; board key = (rig, workspace). `NOT NULL DEFAULT
        //   'default'` backfills existing rows to the default workspace in the
        //   same ALTER (MySQL/Dolt fills prior rows with the column default).
        // - `board_rank`: lexorank string ordering WITHIN a (rig, workspace,
        //   status) column. `''` sorts last (appended); ranks are lazily assigned
        //   on the first move/reorder, never by this migration.
        // - `estimated_hours`/`start_date`/`due_date`: the planning fields backing
        //   the operator mockup (Horas Est. / Fecha Inicio / Fecha Fin) and the
        //   retrasos metric (`due_date < now AND status != closed`).
        let board_columns: &[(&str, &str)] = &[
            ("workspace", "VARCHAR(255) NOT NULL DEFAULT 'default'"),
            ("board_rank", "VARCHAR(255) NOT NULL DEFAULT ''"),
            ("estimated_hours", "DECIMAL(8,2) NULL"),
            ("start_date", "DATE NULL"),
            ("due_date", "DATE NULL"),
        ];
        let mut board_added = false;
        for (name, ddl) in board_columns {
            let exists: Option<i64> = conn
                .exec_first(
                    "SELECT 1 FROM information_schema.columns
                     WHERE table_schema = DATABASE()
                       AND table_name = 'issues'
                       AND column_name = :col LIMIT 1",
                    mysql_async::params! { "col" => *name },
                )
                .await
                .map_err(map_err)?;
            if exists.is_none() {
                // Closed-set literals from `board_columns` only — no caller input.
                let sql = format!("ALTER TABLE issues ADD COLUMN {name} {ddl}");
                conn.query_drop(sql).await.map_err(map_err)?;
                board_added = true;
            }
        }
        if board_added {
            conn.exec_drop(
                "CALL DOLT_COMMIT('-A', '-m', :msg)",
                mysql_async::params! {
                    "msg" => "hq-62130a: add kanban board columns (workspace, board_rank, \
                              estimated_hours, start_date, due_date)"
                        .to_string(),
                },
            )
            .await
            .map_err(map_err)?;
        }
        // issue_relations migration: drop legacy inline columns when they still exist
        // (idempotent — second runs skip if already dropped).
        let mut dropped_legacy = false;
        for col in &["external_ref", "depends_on_json"] {
            let exists: Option<i64> = conn
                .exec_first(
                    "SELECT 1 FROM information_schema.columns
                     WHERE table_schema = DATABASE()
                       AND table_name = 'issues'
                       AND column_name = :col LIMIT 1",
                    mysql_async::params! { "col" => *col },
                )
                .await
                .map_err(map_err)?;
            if exists.is_some() {
                let sql = format!("ALTER TABLE issues DROP COLUMN {col}");
                conn.query_drop(sql).await.map_err(map_err)?;
                dropped_legacy = true;
            }
        }
        if dropped_legacy {
            conn.exec_drop(
                "CALL DOLT_COMMIT('-A', '-m', :msg)",
                mysql_async::params! {
                    "msg" => "issue_relations: drop legacy external_ref + depends_on_json columns".to_string(),
                },
            )
            .await
            .map_err(map_err)?;
        }

        // issue_relations normalized relation table (replaces external_ref + depends_on_json).
        let ir_exists: Option<i64> = conn
            .query_first(
                "SELECT 1 FROM information_schema.tables
                 WHERE table_schema = DATABASE() AND table_name = 'issue_relations' LIMIT 1",
            )
            .await
            .map_err(map_err)?;
        if ir_exists.is_none() {
            conn.query_drop(
                "CREATE TABLE IF NOT EXISTS issue_relations (
                    from_id  VARCHAR(255) NOT NULL,
                    to_id    VARCHAR(255) NOT NULL,
                    rel_type VARCHAR(32)  NOT NULL,
                    PRIMARY KEY (from_id, to_id, rel_type),
                    INDEX idx_ir_to_id (to_id),
                    INDEX idx_ir_rel_type (rel_type)
                )",
            )
            .await
            .map_err(map_err)?;
            conn.exec_drop(
                "CALL DOLT_COMMIT('-A', '-m', :msg)",
                mysql_async::params! {
                    "msg" => "issue_relations: create normalized relation table".to_string(),
                },
            )
            .await
            .map_err(map_err)?;
        }

        // Idempotent index for the board projection's hot path: every board call
        // filters by the (rig, workspace) scope key.
        conn.query_drop(
            "CREATE INDEX IF NOT EXISTS issues_rig_workspace_idx ON issues(rig, workspace)",
        )
        .await
        .map_err(map_err)?;

        // hq-rig-isolation.1: backfill `rig` from the bead-id prefix for all rows
        // where `rig` is still the empty-string default. `SUBSTRING_INDEX(id,'-',1)` is
        // deterministic — the prefix IS the leading token, and prefix == rig name in every
        // existing workspace. Index is created after backfill so it covers the full table
        // immediately. Commits are suppressed when no rows changed (cold idempotent runs).
        let rig_backfill: Option<u64> = conn
            .query_first(
                "SELECT COUNT(*) FROM issues WHERE rig = ''",
            )
            .await
            .map_err(map_err)?;
        if rig_backfill.unwrap_or(0) > 0 {
            conn.query_drop(
                "UPDATE issues SET rig = SUBSTRING_INDEX(id, '-', 1) WHERE rig = ''",
            )
            .await
            .map_err(map_err)?;
            // Suppress "nothing to commit": can occur when the net effect of the seed +
            // backfill round-trips to the same state as HEAD (e.g. in tests that reseed
            // the same rows the previous run already backfilled). The data is correct at
            // this point so it is safe to continue.
            let commit_res = conn.exec_drop(
                "CALL DOLT_COMMIT('-A', '-m', :msg)",
                mysql_async::params! {
                    "msg" => "hq-rig-isolation.1: backfill rig from id prefix".to_string(),
                },
            )
            .await;
            if let Err(ref e) = commit_res {
                if !e.to_string().contains("nothing to commit") {
                    return Err(map_err(commit_res.unwrap_err()));
                }
            }
        }
        // Idempotent index on `rig` for filter-by-rig queries.
        conn.query_drop(
            "CREATE INDEX IF NOT EXISTS issues_rig_idx ON issues(rig)",
        )
        .await
        .map_err(map_err)?;

        // hq-bead-id-standard.3: one-time backfill — map old shorthand prefixes to
        // canonical rig names now that prefix == name is the enforced standard.
        // Idempotent: each row is only updated once (WHERE rig = '<old>').
        let prefix_to_name: &[(&str, &str)] = &[
            ("gw", "gtweb"),
            ("gm", "gtmcp"),
            ("gp", "gtproxy"),
        ];
        let mut canon_updated = false;
        for (old, new) in prefix_to_name {
            let res = conn
                .exec_iter(
                    "UPDATE issues SET rig = :new WHERE rig = :old",
                    mysql_async::params! { "new" => *new, "old" => *old },
                )
                .await
                .map_err(map_err)?;
            let affected = res.affected_rows();
            let _ = res.drop_result().await.map_err(map_err)?;
            if affected > 0 {
                canon_updated = true;
            }
        }
        if canon_updated {
            let commit_res = conn
                .exec_drop(
                    "CALL DOLT_COMMIT('-A', '-m', :msg)",
                    mysql_async::params! {
                        "msg" => "hq-bead-id-standard.3: canonicalize issues.rig (gw→gtweb, gm→gtmcp, gp→gtproxy)".to_string()
                    },
                )
                .await;
            if let Err(ref e) = commit_res {
                if !e.to_string().contains("nothing to commit") {
                    return Err(map_err(commit_res.unwrap_err()));
                }
            }
        }

        // hq-core-mcp.7 (docs/10 S1) — the singleton `phase_frontier` row that
        // governs the highest phase currently claimable. Created + seeded
        // `open_phase = 'P3'` (ratified 2026-06-01) the first time `ensure_schema`
        // runs; idempotent thereafter (the table/row guards skip a second seed).
        let frontier_table: Option<i64> = conn
            .query_first(
                "SELECT 1 FROM information_schema.tables
                 WHERE table_schema = DATABASE() AND table_name = 'phase_frontier' LIMIT 1",
            )
            .await
            .map_err(map_err)?;
        if frontier_table.is_none() {
            conn.query_drop(
                "CREATE TABLE phase_frontier (
                    id          TINYINT PRIMARY KEY DEFAULT 1 CHECK (id = 1),
                    open_phase  ENUM('P1','P2','P3','P4') NOT NULL,
                    ratified_at TIMESTAMP NOT NULL
                )",
            )
            .await
            .map_err(map_err)?;
        }
        // Seed the singleton row if absent (covers a pre-existing empty table too).
        let frontier_row: Option<i64> = conn
            .query_first("SELECT 1 FROM phase_frontier WHERE id = 1 LIMIT 1")
            .await
            .map_err(map_err)?;
        if frontier_row.is_none() {
            conn.exec_drop(
                "INSERT INTO phase_frontier (id, open_phase, ratified_at)
                 VALUES (1, 'P3', NOW())",
                mysql_async::Params::Empty,
            )
            .await
            .map_err(map_err)?;
            conn.exec_drop(
                "CALL DOLT_COMMIT('-A', '-m', :msg)",
                mysql_async::params! {
                    "msg" => "hq-core-mcp.7: seed phase_frontier open_phase=P3".to_string(),
                },
            )
            .await
            .map_err(map_err)?;
        }

        // hq-taxon.6 — backfill live root epics so the dependency-graph
        // resources have meaningful anchor points the first time they are
        // queried. Idempotent by construction: the `WHERE` clause only
        // touches rows whose `domain_json` is still the default empty array
        // (or the legacy empty-string case Dolt occasionally hands us).
        //
        // Coverage chosen per `apps/api/docs/14-bead-taxonomy.md` §8 — the
        // well-known root epics that already exist plus the hq-taxon family
        // itself (minted via the legacy tool before `domain[]` was a field).
        // Ordinary tasks continue to backfill on their next `issues.update`.
        let backfill: &[(&str, &str)] = &[
            ("hq-fe-svelte", r#"["fe.web","fe.docs"]"#),
            ("hq-fe-api-w", r#"["kernel.root","bin.gt-web"]"#),
            ("hq-fe-api-r", r#"["bin.gt-web"]"#),
            ("hq-fe-cut", r#"["fe.web","bin.gt-web"]"#),
            ("hq-fe-build", r#"["fe.web"]"#),
            ("hq-fe-view", r#"["fe.web"]"#),
            ("hq-fe-auth", r#"["fe.web","bin.gt-web"]"#),
            ("hq-fe-rbac", r#"["fe.web","bin.gt-web"]"#),
            (
                "hq-fe-skills",
                r#"["fe.web","role.sheriff","role.deacon","role.refinery","role.witness","role.mayor"]"#,
            ),
            ("hq-fe-term", r#"["fe.web","bin.gt-web"]"#),
            ("hq-mcp-issues", r#"["bin.gt-mcp","store.dolt"]"#),
            ("hq-oap5", r#"["deploy.compose","lifecycle.polecat"]"#),
            ("hq-63az", r#"["lifecycle.polecat"]"#),
            ("hq-03aw", r#"["store.dolt","store.pg"]"#),
            ("hq-mc72", r#"["bin.gt"]"#),
            ("hq-taxon", r#"["docs.spec","bin.gt-mcp","store.dolt"]"#),
            ("hq-taxon.1", r#"["bin.gt-mcp"]"#),
            ("hq-taxon.2", r#"["bin.gt-mcp"]"#),
            ("hq-taxon.3", r#"["store.dolt"]"#),
            ("hq-taxon.4", r#"["bin.gt-mcp"]"#),
            ("hq-taxon.5", r#"["bin.gt-mcp"]"#),
            ("hq-taxon.6", r#"["store.dolt"]"#),
        ];

        let mut backfilled_any = false;
        for (id, domain) in backfill {
            let result = conn
                .exec_iter(
                    "UPDATE issues
                     SET domain_json = :domain
                     WHERE id = :id
                       AND (domain_json = '[]' OR domain_json = '' OR domain_json IS NULL)",
                    mysql_async::params! {
                        "domain" => *domain,
                        "id" => *id,
                    },
                )
                .await
                .map_err(map_err)?;
            let affected = result.affected_rows();
            let _ = result.drop_result().await.map_err(map_err)?;
            if affected > 0 {
                backfilled_any = true;
            }
        }

        if backfilled_any {
            conn.exec_drop(
                "CALL DOLT_COMMIT('-A', '-m', :msg)",
                mysql_async::params! {
                    "msg" => "hq-taxon.6: backfill live root epics with domain[]".to_string(),
                },
            )
            .await
            .map_err(map_err)?;
        }

        Ok(())
    }

    /// Insert a new row into `hq.issues` and stamp it as a Dolt commit so the
    /// write is visible to downstream readers (`bd`, the dashboard, replication)
    /// without waiting for an external commit (hq-mcp-issues.2).
    ///
    /// Atomicity: the `INSERT` and the `CALL DOLT_COMMIT` run on the same
    /// connection; a failure on the `INSERT` aborts before any commit. The
    /// `DOLT_COMMIT('-A', '-m', ...)` includes every uncommitted change on the
    /// working set — mirroring the `docker exec dolt sql -q "...; CALL
    /// DOLT_COMMIT(...)"` recipe operators ran by hand pre-MCP.
    ///
    /// Returns the duplicate-key error path verbatim so the frontier can
    /// translate it to a `Validation` outcome (the caller already validated
    /// non-empty fields; only DB-level uniqueness can race here).
    pub async fn insert(&self, row: &NewIssue) -> Result<(), AppError> {
        let mut conn = self.pool.get_conn().await.map_err(map_err)?;
        // Normalise default-built `NewIssue` (Default derive leaves the JSON
        // strings as `""`) so the NOT NULL columns honour their `[]` invariant.
        let domain_json = if row.domain_json.is_empty() { "[]" } else { row.domain_json.as_str() };
        let surface_json = if row.surface_json.is_empty() { "[]" } else { row.surface_json.as_str() };
        // Phase defaults to `P1` (matching the column default) when the caller
        // omits it; a `Some(_)` is validated against `IssuePhase` upstream.
        let phase = row.phase.as_deref().unwrap_or("P1");
        // Workspace half of the board scope key (hq-62130a): a default-built
        // `NewIssue` carries `""`, normalised to the column default.
        let workspace = if row.workspace.is_empty() { "default" } else { row.workspace.as_str() };
        conn.exec_drop(
            "INSERT INTO issues
                (id, title, description, design, acceptance_criteria, notes,
                 status, priority, issue_type, assignee, owner, created_by,
                 domain_json, surface_json, role_scope, phase, rig, workspace)
             VALUES
                (:id, :title, :description, :design, :acceptance_criteria, :notes,
                 'open', :priority, :issue_type, :assignee, :owner, :created_by,
                 :domain_json, :surface_json, :role_scope, :phase, :rig, :workspace)",
            mysql_async::params! {
                "id" => &row.id,
                "title" => &row.title,
                "description" => &row.description,
                "design" => &row.design,
                "acceptance_criteria" => &row.acceptance_criteria,
                "notes" => &row.notes,
                "priority" => row.priority as i32,
                "issue_type" => &row.issue_type,
                "assignee" => row.assignee.clone(),
                "owner" => row.owner.clone().unwrap_or_default(),
                "created_by" => &row.created_by,
                "domain_json" => domain_json,
                "surface_json" => surface_json,
                "role_scope" => row.role_scope.clone(),
                "phase" => phase,
                "rig" => &row.rig,
                "workspace" => workspace,
            },
        )
        .await
        .map_err(map_err)?;

        // Atomic Dolt commit so the row lands in history immediately. Message
        // mirrors the operator's pre-MCP recipe (`docker exec dolt sql -q
        // "INSERT ...; CALL DOLT_COMMIT('-A','-m','create <id>')"`). Failure
        // here is fatal — the INSERT already landed in the working set and
        // would be picked up by the next commit silently.
        let commit_msg = format!("create {}", row.id);
        conn.exec_drop(
            "CALL DOLT_COMMIT('-A', '-m', :msg)",
            mysql_async::params! { "msg" => commit_msg },
        )
        .await
        .map_err(map_err)?;

        // If a parent epic was supplied, create the child_of relation.
        if let Some(ref pid) = row.parent_id {
            if !pid.is_empty() {
                conn.exec_drop(
                    "INSERT IGNORE INTO issue_relations (from_id, to_id, rel_type) \
                     VALUES (:from_id, :to_id, 'child_of')",
                    mysql_async::params! { "from_id" => &row.id, "to_id" => pid },
                )
                .await
                .map_err(map_err)?;
                conn.exec_drop(
                    "CALL DOLT_COMMIT('-A', '-m', :msg)",
                    mysql_async::params! {
                        "msg" => format!("relate {} child_of {}", row.id, pid),
                    },
                )
                .await
                .map_err(map_err)?;
            }
        }

        Ok(())
    }

    /// Apply a partial patch to an existing row in `hq.issues` and stamp the
    /// change as a Dolt commit (hq-mcp-issues.3). Returns `AppError::NotFound`
    /// when no row matches `id` so the frontier can translate to a clean MCP
    /// `not found`.
    ///
    /// `updated_at = NOW()` is always set so dashboards reorder the row.
    /// `IssuePatch::is_empty` is the caller's responsibility — passing an empty
    /// patch here produces an `UPDATE ... SET updated_at = NOW() WHERE id = :id`,
    /// which is wasted churn; the frontier validates before delegating.
    pub async fn update(&self, id: &str, patch: &IssuePatch) -> Result<(), AppError> {
        let mut conn = self.pool.get_conn().await.map_err(map_err)?;

        let mut set_parts: Vec<&str> = Vec::new();
        let mut params_vec: Vec<(String, mysql_async::Value)> =
            vec![("id".to_string(), mysql_async::Value::from(id.to_string()))];

        if let Some(v) = &patch.title {
            set_parts.push("title = :title");
            params_vec.push(("title".to_string(), mysql_async::Value::from(v.clone())));
        }
        if let Some(v) = &patch.description {
            set_parts.push("description = :description");
            params_vec.push(("description".to_string(), mysql_async::Value::from(v.clone())));
        }
        if let Some(v) = &patch.design {
            set_parts.push("design = :design");
            params_vec.push(("design".to_string(), mysql_async::Value::from(v.clone())));
        }
        if let Some(v) = &patch.acceptance_criteria {
            set_parts.push("acceptance_criteria = :acceptance_criteria");
            params_vec.push((
                "acceptance_criteria".to_string(),
                mysql_async::Value::from(v.clone()),
            ));
        }
        if let Some(v) = &patch.notes {
            set_parts.push("notes = :notes");
            params_vec.push(("notes".to_string(), mysql_async::Value::from(v.clone())));
        }
        if let Some(v) = patch.priority {
            set_parts.push("priority = :priority");
            params_vec.push(("priority".to_string(), mysql_async::Value::from(v as i32)));
        }
        if let Some(v) = &patch.issue_type {
            set_parts.push("issue_type = :issue_type");
            params_vec.push(("issue_type".to_string(), mysql_async::Value::from(v.clone())));
        }
        // `assignee`/`owner` carry "clear" semantics: an empty string overwrites
        // the column with SQL `NULL` (canonical "unassigned"), so the read side's
        // `take_opt` round-trips back to `None`. A non-empty string is stored verbatim.
        if let Some(v) = &patch.assignee {
            set_parts.push("assignee = :assignee");
            params_vec.push(("assignee".to_string(), str_or_null(v)));
        }
        if let Some(v) = &patch.owner {
            set_parts.push("owner = :owner");
            params_vec.push(("owner".to_string(), str_or_null(v)));
        }
        if let Some(v) = &patch.domain_json {
            set_parts.push("domain_json = :domain_json");
            params_vec.push(("domain_json".to_string(), mysql_async::Value::from(v.clone())));
        }
        if let Some(v) = &patch.surface_json {
            set_parts.push("surface_json = :surface_json");
            params_vec.push(("surface_json".to_string(), mysql_async::Value::from(v.clone())));
        }
        // hq-core-mcp.7 — a phase overwrite also stamps `phase_ratified_at` so the
        // last ratification is auditable, mirroring the frontier's `ratified_at`.
        if let Some(v) = &patch.phase {
            set_parts.push("phase = :phase");
            set_parts.push("phase_ratified_at = NOW()");
            params_vec.push(("phase".to_string(), mysql_async::Value::from(v.clone())));
        }
        // hq-62130a — the planning fields the board/planning view edits. Dates
        // carry the empty-string-clears semantics of assignee/external_ref.
        if let Some(v) = patch.estimated_hours {
            set_parts.push("estimated_hours = :estimated_hours");
            params_vec.push(("estimated_hours".to_string(), mysql_async::Value::from(v)));
        }
        if let Some(v) = &patch.start_date {
            set_parts.push("start_date = :start_date");
            params_vec.push(("start_date".to_string(), str_or_null(v)));
        }
        if let Some(v) = &patch.due_date {
            set_parts.push("due_date = :due_date");
            params_vec.push(("due_date".to_string(), str_or_null(v)));
        }
        if let Some(v) = &patch.workspace {
            set_parts.push("workspace = :workspace");
            params_vec.push(("workspace".to_string(), mysql_async::Value::from(v.clone())));
        }

        set_parts.push("updated_at = NOW()");
        // Always advance the optimistic-concurrency token so any reader that
        // held the prior value detects the write (hq-mcp-issues.8).
        set_parts.push("version = version + 1");
        // Optional OCC guard: match only when the row's version is still what
        // the caller read. `None` keeps the legacy unguarded last-write-wins.
        let where_clause = if patch.expected_version.is_some() {
            params_vec.push((
                "expected_version".to_string(),
                mysql_async::Value::from(patch.expected_version.unwrap()),
            ));
            "id = :id AND version = :expected_version"
        } else {
            "id = :id"
        };
        let sql = format!(
            "UPDATE issues SET {} WHERE {}",
            set_parts.join(", "),
            where_clause,
        );

        let result = conn
            .exec_iter(sql, mysql_async::Params::from(params_vec))
            .await
            .map_err(map_err)?;
        let affected = result.affected_rows();
        // Drain the result-set handle before issuing the commit on the same conn.
        let _ = result.drop_result().await.map_err(map_err)?;

        if affected == 0 {
            // Disambiguate missing row from a version conflict so a stale edit
            // fails loud (Validation = "version conflict") instead of silently.
            return match (patch.expected_version, self.current_version(id).await?) {
                (_, None) => Err(AppError::NotFound(format!("issue {id}"))),
                (Some(exp), Some(cur)) => Err(AppError::Validation(format!(
                    "version conflict on {id}: expected {exp}, current {cur} — re-read and retry"
                ))),
                (None, Some(_)) => Err(AppError::NotFound(format!("issue {id}"))),
            };
        }

        let commit_msg = format!("update {id}");
        conn.exec_drop(
            "CALL DOLT_COMMIT('-A', '-m', :msg)",
            mysql_async::params! { "msg" => commit_msg },
        )
        .await
        .map_err(map_err)?;

        // Handle parent_id relation change after the issues UPDATE commits.
        if let Some(ref pid) = patch.parent_id {
            // Remove any existing child_of relation for this issue.
            conn.exec_drop(
                "DELETE FROM issue_relations WHERE from_id = :id AND rel_type = 'child_of'",
                mysql_async::params! { "id" => id },
            )
            .await
            .map_err(map_err)?;
            if !pid.is_empty() {
                // Insert new parent.
                conn.exec_drop(
                    "INSERT IGNORE INTO issue_relations (from_id, to_id, rel_type) \
                     VALUES (:from_id, :to_id, 'child_of')",
                    mysql_async::params! { "from_id" => id, "to_id" => pid },
                )
                .await
                .map_err(map_err)?;
            }
            let rel_commit_res = conn
                .exec_drop(
                    "CALL DOLT_COMMIT('-A', '-m', :msg)",
                    mysql_async::params! {
                        "msg" => if pid.is_empty() {
                            format!("unrelate {id} child_of (cleared)")
                        } else {
                            format!("relate {id} child_of {pid}")
                        },
                    },
                )
                .await;
            if let Err(ref e) = rel_commit_res {
                if !e.to_string().contains("nothing to commit") {
                    return Err(map_err(rel_commit_res.unwrap_err()));
                }
            }
        }

        Ok(())
    }

    /// Atomically append `fragment` to `hq.issues.notes` for `id` (hq-fe-api-w.5).
    /// Single `UPDATE ... CONCAT(...)` statement so a concurrent comment never
    /// reads a stale `notes` and clobbers a parallel append — the SQL is the
    /// merge point. `fragment` is concatenated verbatim; the frontier owns the
    /// formatting (timestamp prefix, author tag, separator) so the schema stays
    /// agnostic to the dashboard's comment shape.
    ///
    /// Returns `AppError::NotFound` when no row matches `id`. Atomic Dolt commit
    /// on success — mirrors the same `CALL DOLT_COMMIT('-A','-m',...)` recipe
    /// the other write paths use.
    pub async fn append_notes(&self, id: &str, fragment: &str) -> Result<(), AppError> {
        let mut conn = self.pool.get_conn().await.map_err(map_err)?;
        let sql = "UPDATE issues
             SET notes = CONCAT(IFNULL(notes, ''), :fragment),
                 updated_at = NOW()
             WHERE id = :id";
        let result = conn
            .exec_iter(
                sql,
                mysql_async::params! {
                    "id" => id,
                    "fragment" => fragment,
                },
            )
            .await
            .map_err(map_err)?;
        let affected = result.affected_rows();
        let _ = result.drop_result().await.map_err(map_err)?;
        if affected == 0 {
            return Err(AppError::NotFound(format!("issue {id}")));
        }
        let commit_msg = format!("comment {id}");
        conn.exec_drop(
            "CALL DOLT_COMMIT('-A', '-m', :msg)",
            mysql_async::params! {
                "msg" => commit_msg,
            },
        )
        .await
        .map_err(map_err)?;
        Ok(())
    }

    /// Atomically claim an `open` bead for `owner` (hq-mcp-issues.7 — the
    /// server-side CAS the concurrency protocol, docs/05 §1, calls for). A
    /// single guarded UPDATE flips `status` open -> working AND stamps
    /// `owner`/`assignee` in one statement, so two agents racing the same bead
    /// cannot both win: the SQL `WHERE` is the compare-and-swap point.
    ///
    /// Guard: `status='open' AND (owner IS NULL OR owner='')`. The winner gets
    /// [`ClaimOutcome::Won`]; a row already held returns [`ClaimOutcome::Lost`]
    /// with the current holder + status so the caller can stand down against a
    /// named owner. Re-claiming a row the same `owner` already holds
    /// (`status='working'`) is idempotent success. Missing id -> `NotFound`.
    pub async fn claim(&self, id: &str, owner: &str) -> Result<ClaimOutcome, AppError> {
        let mut conn = self.pool.get_conn().await.map_err(map_err)?;
        let result = conn
            .exec_iter(
                "UPDATE issues
                 SET status = 'working',
                     owner = :owner,
                     assignee = :owner,
                     updated_at = NOW(),
                     version = version + 1
                 WHERE id = :id
                   AND status = 'open'
                   AND (owner IS NULL OR owner = '')",
                mysql_async::params! {
                    "id" => id,
                    "owner" => owner,
                },
            )
            .await
            .map_err(map_err)?;
        let affected = result.affected_rows();
        let _ = result.drop_result().await.map_err(map_err)?;

        if affected == 1 {
            let commit_msg = format!("claim {id} by {owner}");
            conn.exec_drop(
                "CALL DOLT_COMMIT('-A', '-m', :msg)",
                mysql_async::params! { "msg" => commit_msg },
            )
            .await
            .map_err(map_err)?;
            return Ok(ClaimOutcome::Won);
        }

        // CAS missed — disambiguate missing / already-mine / held-by-other.
        let row: Option<(String, Option<String>)> = conn
            .exec_first(
                "SELECT status, owner FROM issues WHERE id = :id LIMIT 1",
                mysql_async::params! { "id" => id },
            )
            .await
            .map_err(map_err)?;
        match row {
            None => Err(AppError::NotFound(format!("issue {id}"))),
            Some((status, owner_now)) => {
                let holder = owner_now.unwrap_or_default();
                if status == "working" && holder == owner {
                    // Idempotent: this owner already holds it.
                    Ok(ClaimOutcome::Won)
                } else {
                    Ok(ClaimOutcome::Lost { status, holder })
                }
            }
        }
    }

    /// Read the current status of `id`. `None` when the row does not exist.
    /// Used by [`Self::transition`] to distinguish `NotFound` from
    /// `InvalidTransition` after a status-guarded UPDATE fails to match.
    pub async fn current_status(&self, id: &str) -> Result<Option<String>, AppError> {
        let mut conn = self.pool.get_conn().await.map_err(map_err)?;
        let row: Option<String> = conn
            .exec_first(
                "SELECT status FROM issues WHERE id = :id LIMIT 1",
                mysql_async::params! { "id" => id },
            )
            .await
            .map_err(map_err)?;
        Ok(row)
    }

    /// Read the current optimistic-concurrency token of `id` (hq-mcp-issues.8).
    /// `None` when the row does not exist. Used by [`Self::update`] to tell a
    /// version conflict from a missing row after a guarded UPDATE matches 0.
    pub async fn current_version(&self, id: &str) -> Result<Option<i64>, AppError> {
        let mut conn = self.pool.get_conn().await.map_err(map_err)?;
        let row: Option<i64> = conn
            .exec_first(
                "SELECT version FROM issues WHERE id = :id LIMIT 1",
                mysql_async::params! { "id" => id },
            )
            .await
            .map_err(map_err)?;
        Ok(row)
    }

    /// Read the currently open phase from the singleton `phase_frontier`
    /// (hq-core-mcp.7, docs/10 S1). A bead is phase-gated when its own `phase`
    /// exceeds this value. Errors if the singleton row is missing (ensure_schema
    /// seeds it) or holds an unrecognised token.
    pub async fn open_phase(&self) -> Result<IssuePhase, AppError> {
        let mut conn = self.pool.get_conn().await.map_err(map_err)?;
        let raw: Option<String> = conn
            .query_first("SELECT open_phase FROM phase_frontier WHERE id = 1 LIMIT 1")
            .await
            .map_err(map_err)?;
        match raw {
            Some(s) => IssuePhase::parse(&s).ok_or_else(|| {
                AppError::Other(format!("unknown open_phase `{s}` in phase_frontier"))
            }),
            None => Err(AppError::Other(
                "phase_frontier singleton row missing (run ensure_schema)".into(),
            )),
        }
    }

    /// Advance the global `phase_frontier.open_phase` and stamp `ratified_at =
    /// NOW()` (hq-core-mcp.7, docs/10 S1). OPERATOR ONLY: the caller's RBAC scope
    /// (`issues.phase.advance`) is the gate — never an agent — and is enforced at
    /// the MCP boundary before this runs. Setting the same phase is an idempotent
    /// re-ratification (the timestamp still advances). Atomic Dolt commit on
    /// success; `NotFound` if the singleton row is absent.
    pub async fn advance_phase(&self, open_phase: IssuePhase) -> Result<(), AppError> {
        let mut conn = self.pool.get_conn().await.map_err(map_err)?;
        let present: Option<i64> = conn
            .query_first("SELECT 1 FROM phase_frontier WHERE id = 1 LIMIT 1")
            .await
            .map_err(map_err)?;
        if present.is_none() {
            return Err(AppError::NotFound("phase_frontier singleton row".into()));
        }
        conn.exec_drop(
            "UPDATE phase_frontier SET open_phase = :phase, ratified_at = NOW() WHERE id = 1",
            mysql_async::params! { "phase" => open_phase.as_str() },
        )
        .await
        .map_err(map_err)?;
        let commit_msg = format!("phase.advance open_phase -> {}", open_phase.as_str());
        conn.exec_drop(
            "CALL DOLT_COMMIT('-A', '-m', :msg)",
            mysql_async::params! { "msg" => commit_msg },
        )
        .await
        .map_err(map_err)?;
        Ok(())
    }

    /// Move an issue across the [`IssueStatus`] state machine (hq-mcp-issues.4).
    /// Uses a status-guarded `UPDATE` so a concurrent transition cannot land an
    /// illegal jump under us — the `affected_rows == 0` path then falls back to
    /// a `current_status` read to tell `NotFound` from `InvalidTransition`.
    /// Atomic Dolt commit on success.
    pub async fn transition(
        &self,
        id: &str,
        target: IssueStatus,
    ) -> Result<(), AppError> {
        let legal_sources: Vec<&'static str> = [
            IssueStatus::Open,
            IssueStatus::Working,
            IssueStatus::Closed,
        ]
        .into_iter()
        .filter(|s| s.can_transition_to(target))
        .map(|s| s.as_str())
        .collect();

        let mut conn = self.pool.get_conn().await.map_err(map_err)?;

        let placeholders: Vec<String> = legal_sources
            .iter()
            .enumerate()
            .map(|(i, _)| format!(":src_{i}"))
            .collect();
        let mut params_vec: Vec<(String, mysql_async::Value)> = vec![
            ("id".to_string(), mysql_async::Value::from(id.to_string())),
            (
                "target".to_string(),
                mysql_async::Value::from(target.as_str().to_string()),
            ),
        ];
        for (i, s) in legal_sources.iter().enumerate() {
            params_vec.push((format!("src_{i}"), mysql_async::Value::from(s.to_string())));
        }

        let closed_at_set = match target {
            IssueStatus::Closed => "closed_at = NOW(),",
            IssueStatus::Open => "closed_at = NULL,",
            IssueStatus::Working => "",
        };

        let where_status = if placeholders.is_empty() {
            // No legal source -> impossible to satisfy. Skip the UPDATE.
            String::from("1 = 0")
        } else {
            format!("status IN ({})", placeholders.join(", "))
        };

        let sql = format!(
            "UPDATE issues
             SET status = :target,
                 {closed_at_set}
                 updated_at = NOW(),
                 version = version + 1
             WHERE id = :id AND {where_status}"
        );

        let result = conn
            .exec_iter(sql, mysql_async::Params::from(params_vec))
            .await
            .map_err(map_err)?;
        let affected = result.affected_rows();
        let _ = result.drop_result().await.map_err(map_err)?;

        if affected == 0 {
            // Disambiguate NotFound vs InvalidTransition for the frontier.
            return match self.current_status(id).await? {
                None => Err(AppError::NotFound(format!("issue {id}"))),
                Some(current) => Err(AppError::Validation(format!(
                    "invalid transition: {current} -> {}",
                    target.as_str()
                ))),
            };
        }

        let commit_msg = format!("transition {id} -> {}", target.as_str());
        conn.exec_drop(
            "CALL DOLT_COMMIT('-A', '-m', :msg)",
            mysql_async::params! {
                "msg" => commit_msg,
            },
        )
        .await
        .map_err(map_err)?;

        Ok(())
    }

    /// `board.move` write half (hq-62130a): set a card's column (status) AND its
    /// lexorank in ONE status-guarded, scope-guarded UPDATE + ONE Dolt commit —
    /// the atomicity the ADR mandates (a drag-drop is a single history entry,
    /// never a transition commit followed by a rank commit).
    ///
    /// `target` is `None` for a same-column reorder (`board.reorder`): only the
    /// rank moves, no state-machine guard applies. The WHERE carries the full
    /// (rig, workspace) scope key, so a caller scoped to another workspace
    /// matches zero rows and gets a `Validation` scope error — the server-side
    /// cross-workspace rejection (ADR D4).
    pub async fn board_move(
        &self,
        id: &str,
        rig: &str,
        workspace: &str,
        target: Option<IssueStatus>,
        rank: &str,
    ) -> Result<(), AppError> {
        let mut conn = self.pool.get_conn().await.map_err(map_err)?;

        let mut params_vec: Vec<(String, mysql_async::Value)> = vec![
            ("id".to_string(), mysql_async::Value::from(id.to_string())),
            ("rig".to_string(), mysql_async::Value::from(rig.to_string())),
            ("ws".to_string(), mysql_async::Value::from(workspace.to_string())),
            ("rank".to_string(), mysql_async::Value::from(rank.to_string())),
        ];

        // Column move: reuse the transition state machine's legal-source guard so
        // board.move can never perform a transition issues.transition would reject.
        let (status_set, status_guard) = match target {
            Some(target) => {
                let legal_sources: Vec<&'static str> =
                    [IssueStatus::Open, IssueStatus::Working, IssueStatus::Closed]
                        .into_iter()
                        .filter(|s| s.can_transition_to(target))
                        .map(|s| s.as_str())
                        .collect();
                let placeholders: Vec<String> = legal_sources
                    .iter()
                    .enumerate()
                    .map(|(i, _)| format!(":src_{i}"))
                    .collect();
                for (i, s) in legal_sources.iter().enumerate() {
                    params_vec.push((format!("src_{i}"), mysql_async::Value::from(s.to_string())));
                }
                params_vec.push((
                    "target".to_string(),
                    mysql_async::Value::from(target.as_str().to_string()),
                ));
                let closed_at_set = match target {
                    IssueStatus::Closed => "closed_at = NOW(),",
                    IssueStatus::Open => "closed_at = NULL,",
                    IssueStatus::Working => "",
                };
                let guard = if placeholders.is_empty() {
                    "AND 1 = 0".to_string()
                } else {
                    format!("AND status IN ({})", placeholders.join(", "))
                };
                (format!("status = :target, {closed_at_set}"), guard)
            }
            None => (String::new(), String::new()),
        };

        let sql = format!(
            "UPDATE issues
             SET {status_set}
                 board_rank = :rank,
                 updated_at = NOW(),
                 version = version + 1
             WHERE id = :id AND rig = :rig AND workspace = :ws {status_guard}"
        );
        let result = conn
            .exec_iter(sql, mysql_async::Params::from(params_vec))
            .await
            .map_err(map_err)?;
        let affected = result.affected_rows();
        let _ = result.drop_result().await.map_err(map_err)?;

        if affected == 0 {
            // Disambiguate for the frontier: missing row, out-of-scope caller, or
            // an illegal column move — each fails loud with its own message.
            let row: Option<(String, String, String)> = conn
                .exec_first(
                    "SELECT rig, workspace, status FROM issues WHERE id = :id LIMIT 1",
                    mysql_async::params! { "id" => id },
                )
                .await
                .map_err(map_err)?;
            return match row {
                None => Err(AppError::NotFound(format!("issue {id}"))),
                Some((r, w, _)) if r != rig || w != workspace => Err(AppError::Validation(format!(
                    "issue {id} is outside the caller's board scope (rig={rig}, workspace={workspace})"
                ))),
                Some((_, _, current)) => Err(AppError::Validation(format!(
                    "invalid transition: {current} -> {}",
                    target.map(IssueStatus::as_str).unwrap_or("<same>")
                ))),
            };
        }

        let commit_msg = match target {
            Some(t) => format!("board.move {id} -> {} @ {rank}", t.as_str()),
            None => format!("board.reorder {id} @ {rank}"),
        };
        conn.exec_drop(
            "CALL DOLT_COMMIT('-A', '-m', :msg)",
            mysql_async::params! { "msg" => commit_msg },
        )
        .await
        .map_err(map_err)?;
        Ok(())
    }

    /// The (id, board_rank) pairs of one column, in board order (ranked cards
    /// lexicographically, the unranked `''` tail last by recency, mirroring the
    /// projection sort). Feeds the frontier's neighbor lookup (`rank_between`)
    /// and the rebalance pass.
    pub async fn column_ranks(
        &self,
        rig: &str,
        workspace: &str,
        status: IssueStatus,
    ) -> Result<Vec<(String, String)>, AppError> {
        let mut conn = self.pool.get_conn().await.map_err(map_err)?;
        let rows: Vec<(String, String)> = conn
            .exec(
                "SELECT id, board_rank FROM issues
                 WHERE rig = :rig AND workspace = :ws AND status = :status
                 ORDER BY (board_rank = ''), board_rank ASC, created_at DESC, id ASC",
                mysql_async::params! {
                    "rig" => rig,
                    "ws" => workspace,
                    "status" => status.as_str(),
                },
            )
            .await
            .map_err(map_err)?;
        Ok(rows)
    }

    /// Rebalance one column (hq-62130a): overwrite every card's `board_rank` in a
    /// single pass + ONE Dolt commit. Used when `rank_between` finds no room
    /// between two adjacent ranks — the frontier recomputes evenly-spaced ranks
    /// for the whole column and writes them here before retrying the move.
    pub async fn board_set_ranks(
        &self,
        rig: &str,
        workspace: &str,
        ranks: &[(String, String)],
    ) -> Result<(), AppError> {
        if ranks.is_empty() {
            return Ok(());
        }
        let mut conn = self.pool.get_conn().await.map_err(map_err)?;
        for (id, rank) in ranks {
            conn.exec_drop(
                "UPDATE issues SET board_rank = :rank, version = version + 1
                 WHERE id = :id AND rig = :rig AND workspace = :ws",
                mysql_async::params! {
                    "rank" => rank,
                    "id" => id,
                    "rig" => rig,
                    "ws" => workspace,
                },
            )
            .await
            .map_err(map_err)?;
        }
        conn.exec_drop(
            "CALL DOLT_COMMIT('-A', '-m', :msg)",
            mysql_async::params! {
                "msg" => format!("board.rebalance {rig}/{workspace} ({} cards)", ranks.len()),
            },
        )
        .await
        .map_err(map_err)?;
        Ok(())
    }

    /// Close an issue with attribution (hq-mcp-issues.5). Sets `status='closed'`,
    /// `closed_at=NOW()`, `closed_by_session=:session`, `updated_at=NOW()` in a
    /// single status-guarded UPDATE so only `open`/`working` rows actually
    /// close — a row already `closed` rejects as `InvalidTransition` rather
    /// than silently bumping the timestamp.
    ///
    /// Differs from `transition(id, IssueStatus::Closed)`: that path leaves
    /// `closed_by_session` untouched. The dedicated `close` tool exists so the
    /// attribution column gets populated atomically with the lifecycle move.
    pub async fn close(&self, id: &str, closed_by_session: &str) -> Result<(), AppError> {
        let mut conn = self.pool.get_conn().await.map_err(map_err)?;

        let result = conn
            .exec_iter(
                "UPDATE issues
                 SET status = 'closed',
                     closed_at = NOW(),
                     closed_by_session = :session,
                     updated_at = NOW(),
                     version = version + 1
                 WHERE id = :id AND status IN ('open', 'working')",
                mysql_async::params! {
                    "id" => id,
                    "session" => closed_by_session,
                },
            )
            .await
            .map_err(map_err)?;
        let affected = result.affected_rows();
        let _ = result.drop_result().await.map_err(map_err)?;

        if affected == 0 {
            // Distinguish missing row from already-closed.
            return match self.current_status(id).await? {
                None => Err(AppError::NotFound(format!("issue {id}"))),
                Some(current) => Err(AppError::Validation(format!(
                    "invalid transition: {current} -> closed"
                ))),
            };
        }

        let commit_msg = format!("close {id} by {closed_by_session}");
        conn.exec_drop(
            "CALL DOLT_COMMIT('-A', '-m', :msg)",
            mysql_async::params! {
                "msg" => commit_msg,
            },
        )
        .await
        .map_err(map_err)?;

        Ok(())
    }

    /// Stamp the verified delivering commit sha on `id` (hq-core-mcp.10, docs/10
    /// §S2). Called by `close` phase-2 only after the sha is confirmed to touch a
    /// non-`planned` surface path, so a non-NULL `delivered_sha` is a trustworthy
    /// delivery proof. Bumps `version` and stamps an atomic Dolt commit like the
    /// other write paths. `AppError::NotFound` when no row matches `id`.
    pub async fn set_delivered_sha(&self, id: &str, sha: &str) -> Result<(), AppError> {
        let mut conn = self.pool.get_conn().await.map_err(map_err)?;
        let result = conn
            .exec_iter(
                "UPDATE issues
                 SET delivered_sha = :sha,
                     updated_at = NOW(),
                     version = version + 1
                 WHERE id = :id",
                mysql_async::params! {
                    "id" => id,
                    "sha" => sha,
                },
            )
            .await
            .map_err(map_err)?;
        let affected = result.affected_rows();
        let _ = result.drop_result().await.map_err(map_err)?;
        if affected == 0 {
            return Err(AppError::NotFound(format!("issue {id}")));
        }
        let commit_msg = format!("deliver {id} @ {sha}");
        conn.exec_drop(
            "CALL DOLT_COMMIT('-A', '-m', :msg)",
            mysql_async::params! { "msg" => commit_msg },
        )
        .await
        .map_err(map_err)?;
        Ok(())
    }

    /// Archive closed issues that have been closed for longer than `older_than_days` (hq-system-config).
    /// Sets `archived_at = NOW()` on matching rows; already-archived rows are skipped via
    /// `archived_at IS NULL`. Commits to Dolt only when at least one row changed so idle ticks
    /// leave no noise in the branch history.
    ///
    /// Returns the [`ArchivedIssue`]s it stamped (id + `issue_type`) rather than a bare count, so a
    /// caller can drive type-specific cleanup off the archived set — e.g. soft-deleting an archived
    /// epic's `documents`/embeddings so it stops surfacing in `documents.search` (hq-docs-archive-sync).
    /// Dolt/MySQL has no reliable `UPDATE ... RETURNING`, so this selects the eligible rows first and
    /// re-applies the same predicate in the `UPDATE`: nothing un-closes a row between the two steps,
    /// so every selected id is archived; a row that races in afterwards is archived too and simply
    /// reported on the next sweep.
    pub async fn archive_old_closed(
        &self,
        older_than_days: u32,
    ) -> Result<Vec<ArchivedIssue>, AppError> {
        if older_than_days == 0 {
            return Ok(Vec::new());
        }
        let mut conn = self.pool.get_conn().await.map_err(map_err)?;
        let eligible: Vec<(String, String)> = conn
            .exec(
                "SELECT id, issue_type FROM issues
                 WHERE status = 'closed'
                   AND archived_at IS NULL
                   AND closed_at < DATE_SUB(NOW(), INTERVAL :days DAY)",
                mysql_async::params! { "days" => older_than_days },
            )
            .await
            .map_err(map_err)?;
        if eligible.is_empty() {
            return Ok(Vec::new());
        }
        let count = eligible.len();
        conn.exec_drop(
            "UPDATE issues
             SET archived_at = NOW(), updated_at = NOW(), version = version + 1
             WHERE status = 'closed'
               AND archived_at IS NULL
               AND closed_at < DATE_SUB(NOW(), INTERVAL :days DAY)",
            mysql_async::params! { "days" => older_than_days },
        )
        .await
        .map_err(map_err)?;
        conn.exec_drop(
            "CALL DOLT_COMMIT('-A', '-m', :msg)",
            mysql_async::params! {
                "msg" => format!("archive-sweep: archived {count} closed issues (>{older_than_days}d)")
            },
        )
        .await
        .map_err(map_err)?;
        Ok(eligible
            .into_iter()
            .map(|(id, issue_type)| ArchivedIssue { id, issue_type })
            .collect())
    }

    /// Map every bead id to the [`DepFact`] readiness needs to judge whether it
    /// satisfies a downstream dependency (hq-core-mcp.11/.12, docs/10 §S4 + §C).
    /// Unbounded (no `LIMIT`) and column-minimal so `gt://issues?ready=true` can
    /// resolve a candidate's `depends_on` in a single one-hop lookup — a dependency
    /// may itself be filtered out of the display set, so the index must cover the
    /// whole table, not just the candidates.
    ///
    /// Carries `issue_type` + `status` alongside the delivered flag so the
    /// readiness predicate can apply the epic-dep rule (hq-core-mcp.12 §C): an
    /// `epic` dependency delivers-by-close, a non-epic dependency by a non-NULL
    /// `delivered_sha`.
    pub async fn dep_index(
        &self,
    ) -> Result<std::collections::HashMap<String, DepFact>, AppError> {
        let mut conn = self.pool.get_conn().await.map_err(map_err)?;
        let rows: Vec<(String, String, String, Option<String>)> = conn
            .exec(
                "SELECT id, issue_type, status, delivered_sha FROM issues",
                mysql_async::Params::Empty,
            )
            .await
            .map_err(map_err)?;
        Ok(rows
            .into_iter()
            .map(|(id, issue_type, status, sha)| {
                (
                    id,
                    DepFact {
                        issue_type,
                        status,
                        delivered: sha.map(|s| !s.trim().is_empty()).unwrap_or(false),
                    },
                )
            })
            .collect())
    }

    /// Insert or ignore a relation into `issue_relations` and Dolt-commit it.
    pub async fn add_relation(&self, from_id: &str, to_id: &str, rel_type: &str) -> Result<(), AppError> {
        let mut conn = self.pool.get_conn().await.map_err(map_err)?;
        conn.exec_drop(
            "INSERT IGNORE INTO issue_relations (from_id, to_id, rel_type) \
             VALUES (:from_id, :to_id, :rel_type)",
            mysql_async::params! { "from_id" => from_id, "to_id" => to_id, "rel_type" => rel_type },
        )
        .await
        .map_err(map_err)?;
        let commit_res = conn
            .exec_drop(
                "CALL DOLT_COMMIT('-A', '-m', :msg)",
                mysql_async::params! { "msg" => format!("relate {from_id} {rel_type} {to_id}") },
            )
            .await;
        if let Err(ref e) = commit_res {
            if !e.to_string().contains("nothing to commit") {
                return Err(map_err(commit_res.unwrap_err()));
            }
        }
        Ok(())
    }

    /// Remove a relation from `issue_relations` and Dolt-commit it.
    pub async fn remove_relation(&self, from_id: &str, to_id: &str, rel_type: &str) -> Result<(), AppError> {
        let mut conn = self.pool.get_conn().await.map_err(map_err)?;
        conn.exec_drop(
            "DELETE FROM issue_relations WHERE from_id = :from_id AND to_id = :to_id AND rel_type = :rel_type",
            mysql_async::params! { "from_id" => from_id, "to_id" => to_id, "rel_type" => rel_type },
        )
        .await
        .map_err(map_err)?;
        let commit_res = conn
            .exec_drop(
                "CALL DOLT_COMMIT('-A', '-m', :msg)",
                mysql_async::params! { "msg" => format!("unrelate {from_id} {rel_type} {to_id}") },
            )
            .await;
        if let Err(ref e) = commit_res {
            if !e.to_string().contains("nothing to commit") {
                return Err(map_err(commit_res.unwrap_err()));
            }
        }
        Ok(())
    }

    /// Return a map of `child_id → parent_epic_id` for all beads in `(rig, ws)` that have a `child_of` relation.
    pub async fn parent_map(&self, rig: &str, ws: &str) -> Result<std::collections::HashMap<String, String>, AppError> {
        let mut conn = self.pool.get_conn().await.map_err(map_err)?;
        let rows: Vec<(String, String)> = if rig.is_empty() && ws.is_empty() {
            conn.exec(
                "SELECT from_id, to_id FROM issue_relations WHERE rel_type = 'child_of'",
                mysql_async::Params::Empty,
            )
            .await
            .map_err(map_err)?
        } else {
            let mut conds = vec!["ir.rel_type = 'child_of'".to_string()];
            let mut params_vec: Vec<(String, mysql_async::Value)> = Vec::new();
            if !rig.is_empty() {
                conds.push("i.rig = :rig".to_string());
                params_vec.push(("rig".to_string(), mysql_async::Value::from(rig.to_string())));
            }
            if !ws.is_empty() {
                conds.push("i.workspace = :ws".to_string());
                params_vec.push(("ws".to_string(), mysql_async::Value::from(ws.to_string())));
            }
            let where_str = conds.join(" AND ");
            let sql = format!(
                "SELECT ir.from_id, ir.to_id \
                 FROM issue_relations ir \
                 JOIN issues i ON i.id = ir.from_id \
                 WHERE {where_str}"
            );
            conn.exec(sql, mysql_async::Params::from(params_vec))
                .await
                .map_err(map_err)?
        };
        Ok(rows.into_iter().collect())
    }

    /// Return a map of `issue_id → Vec<dep_id>` for all `depends_on` relations scoped by the filter's rig/workspace.
    pub async fn depends_on_edges(&self, filter: &IssueFilter) -> Result<std::collections::HashMap<String, Vec<String>>, AppError> {
        let mut conn = self.pool.get_conn().await.map_err(map_err)?;
        let rows: Vec<(String, String)> = if filter.rig.is_some() || filter.workspace.is_some() {
            let mut conds = vec!["ir.rel_type = 'depends_on'".to_string()];
            let mut params_vec: Vec<(String, mysql_async::Value)> = Vec::new();
            if let Some(ref rig) = filter.rig {
                conds.push("i.rig = :rig".to_string());
                params_vec.push(("rig".to_string(), mysql_async::Value::from(rig.clone())));
            }
            if let Some(ref ws) = filter.workspace {
                conds.push("i.workspace = :ws".to_string());
                params_vec.push(("ws".to_string(), mysql_async::Value::from(ws.clone())));
            }
            let where_str = conds.join(" AND ");
            let sql = format!(
                "SELECT ir.from_id, ir.to_id \
                 FROM issue_relations ir \
                 JOIN issues i ON i.id = ir.from_id \
                 WHERE {where_str}"
            );
            conn.exec(sql, mysql_async::Params::from(params_vec))
                .await
                .map_err(map_err)?
        } else {
            conn.exec(
                "SELECT from_id, to_id FROM issue_relations WHERE rel_type = 'depends_on'",
                mysql_async::Params::Empty,
            )
            .await
            .map_err(map_err)?
        };
        let mut map: std::collections::HashMap<String, Vec<String>> = std::collections::HashMap::new();
        for (from_id, to_id) in rows {
            map.entry(from_id).or_default().push(to_id);
        }
        Ok(map)
    }

    /// Build the shared `WHERE` clause + bound params for the list/count queries
    /// from `filter`. Returns `("" | "WHERE ...", params)`. Both `list` and
    /// `count` go through this so a page's rows and its `total` are computed
    /// against an identical predicate (hq-core-mcp.13).
    fn build_where(filter: &IssueFilter) -> (String, Vec<(String, mysql_async::Value)>) {
        let mut where_parts: Vec<String> = Vec::new();
        let mut params_vec: Vec<(String, mysql_async::Value)> = Vec::new();

        if !filter.status.is_empty() {
            let placeholders: Vec<String> = filter
                .status
                .iter()
                .enumerate()
                .map(|(i, _)| format!(":status_{i}"))
                .collect();
            where_parts.push(format!("status IN ({})", placeholders.join(", ")));
            for (i, s) in filter.status.iter().enumerate() {
                params_vec.push((format!("status_{i}"), mysql_async::Value::from(s.clone())));
            }
        }
        if let Some(p) = filter.priority_max {
            where_parts.push("priority <= :priority_max".to_string());
            params_vec.push(("priority_max".to_string(), mysql_async::Value::from(p as i32)));
        }
        if let Some(a) = &filter.assignee {
            where_parts.push("assignee = :assignee".to_string());
            params_vec.push(("assignee".to_string(), mysql_async::Value::from(a.clone())));
        }
        if let Some(ref pid) = filter.parent_id {
            where_parts.push(
                "issues.id IN (SELECT from_id FROM issue_relations WHERE rel_type = 'child_of' AND to_id = :parent_id)"
                    .to_string(),
            );
            params_vec.push(("parent_id".to_string(), mysql_async::Value::from(pid.clone())));
        }
        if let Some(t) = &filter.issue_type {
            where_parts.push("issue_type = :issue_type".to_string());
            params_vec.push(("issue_type".to_string(), mysql_async::Value::from(t.clone())));
        }
        if let Some(r) = &filter.rig {
            where_parts.push("rig = :rig".to_string());
            params_vec.push(("rig".to_string(), mysql_async::Value::from(r.clone())));
        }
        if let Some(w) = &filter.workspace {
            where_parts.push("workspace = :workspace".to_string());
            params_vec.push(("workspace".to_string(), mysql_async::Value::from(w.clone())));
        }

        let where_clause = if where_parts.is_empty() {
            String::new()
        } else {
            format!("WHERE {}", where_parts.join(" AND "))
        };
        (where_clause, params_vec)
    }

    /// Full count of rows matching `filter`, independent of `limit`/`offset`.
    /// Feeds [`IssuePage::total`] so a page is self-describing (hq-core-mcp.13).
    pub async fn count(&self, filter: &IssueFilter) -> Result<i64, AppError> {
        let mut conn = self.pool.get_conn().await.map_err(map_err)?;
        let (where_clause, params_vec) = Self::build_where(filter);
        let sql = format!("SELECT COUNT(*) FROM issues {where_clause}");
        let params = if params_vec.is_empty() {
            mysql_async::Params::Empty
        } else {
            mysql_async::Params::from(params_vec)
        };
        let total: Option<i64> = conn.exec_first(sql, params).await.map_err(map_err)?;
        Ok(total.unwrap_or(0))
    }

    /// One less-style page of the snapshot (hq-core-mcp.13): the [`list`](Self::list)
    /// rows wrapped with `total` (full filter count) + `next_offset` + `has_more`,
    /// so a consumer walks the corpus by advancing `offset` until `has_more` is
    /// false — no full dump, no silent truncation. The `?ready=true` frontier is
    /// served directly off [`list`](Self::list) (unbounded) and does not page.
    pub async fn list_page(&self, filter: &IssueFilter) -> Result<IssuePage, AppError> {
        let total = self.count(filter).await?;
        let rows = self.list(filter).await?;
        let offset = filter.offset.unwrap_or(0);
        let next_offset = offset.saturating_add(rows.len() as u32);
        let has_more = (next_offset as i64) < total;
        Ok(IssuePage {
            rows,
            total,
            next_offset,
            has_more,
        })
    }

    /// List issues matching `filter` in a **stable, immutable order**
    /// (`created_at DESC, id ASC`) so a `limit`+`offset` page-walk never shuffles
    /// or repeats a row between calls (hq-core-mcp.13). The page size is
    /// `filter.limit` (or [`issues_default_limit`]) clamped to
    /// [`issues_max_limit`], skipped past `filter.offset` rows. `?ready=true`
    /// rides in unbounded (the small actionable frontier is filtered above the
    /// store). Datetime columns are formatted server-side to ISO 8601 strings —
    /// the workspace pins `mysql_async` with `minimal` features (no
    /// `time`/`chrono` integration), so converting in SQL keeps the rust
    /// deserialization to plain `String`.
    pub async fn list(&self, filter: &IssueFilter) -> Result<Vec<IssueRow>, AppError> {
        let mut conn = self.pool.get_conn().await.map_err(map_err)?;

        let (where_clause, params_vec) = Self::build_where(filter);

        // `?ready=true` is the unbounded actionable frontier (hq-core-mcp.13 §5) —
        // no LIMIT/OFFSET. Otherwise resolve the page size from the env-tunable
        // default and clamp to the env ceiling, then skip past `offset` rows.
        let limit_clause = if filter.ready {
            String::new()
        } else {
            let limit = filter
                .limit
                .unwrap_or_else(issues_default_limit)
                .min(issues_max_limit());
            let offset = filter.offset.unwrap_or(0);
            format!("LIMIT {limit} OFFSET {offset}")
        };

        // hq-gap-issues-list-full: when `full`, append the heavy text bodies
        // after the board columns (ordinals 25-28) so a sub-epic review reads in
        // one call. Kept at the END of the SELECT so the cheap-snapshot ordinals
        // 0-24 (incl. `phase` at 17, `delivered_sha` at 18, the hq-62130a board
        // columns at 20-24) are untouched by `full`.
        let body_cols = if filter.full {
            ", description, design, acceptance_criteria, notes"
        } else {
            ""
        };
        let sql = format!(
            "SELECT id, title, status, priority, issue_type, assignee, owner,
                    DATE_FORMAT(created_at, '%Y-%m-%dT%H:%i:%SZ') AS created_at,
                    DATE_FORMAT(updated_at, '%Y-%m-%dT%H:%i:%SZ') AS updated_at,
                    DATE_FORMAT(closed_at,  '%Y-%m-%dT%H:%i:%SZ') AS closed_at,
                    spec_id,
                    domain_json, surface_json, role_scope, version,
                    phase, delivered_sha, rig,
                    workspace, board_rank,
                    CAST(estimated_hours AS DOUBLE) AS estimated_hours,
                    DATE_FORMAT(start_date, '%Y-%m-%d') AS start_date,
                    DATE_FORMAT(due_date,   '%Y-%m-%d') AS due_date{body_cols}
             FROM issues
             {where_clause}
             ORDER BY created_at DESC, id ASC
             {limit_clause}"
        );

        let params = if params_vec.is_empty() {
            mysql_async::Params::Empty
        } else {
            mysql_async::Params::from(params_vec)
        };

        // 16 columns exceeds mysql_async's `FromRow` tuple impls (12), so we
        // pull each row by ordinal index — keeps the code branchless and the
        // SELECT order is the single source of truth for the field mapping.
        let rows: Vec<mysql_async::Row> = conn.exec(sql, params).await.map_err(map_err)?;

        rows.into_iter().map(|r| row_to_issue(r, filter.full)).collect()
    }

    /// Fetch one issue by id WITH the heavy text bodies (hq-mcp-issues.6 — the
    /// `issues.get` read the original `IssueRow` doc comment anticipated but the
    /// epic never shipped). Returns `None` when no row matches. This is the
    /// read path that lets an agent see `description`/`design`/
    /// `acceptance_criteria`/`notes` after claiming a bead.
    pub async fn get_detail(&self, id: &str) -> Result<Option<IssueDetail>, AppError> {
        let mut conn = self.pool.get_conn().await.map_err(map_err)?;
        let sql = "SELECT id, title, status, priority, issue_type, assignee, owner,
                    DATE_FORMAT(created_at, '%Y-%m-%dT%H:%i:%SZ') AS created_at,
                    DATE_FORMAT(updated_at, '%Y-%m-%dT%H:%i:%SZ') AS updated_at,
                    DATE_FORMAT(closed_at,  '%Y-%m-%dT%H:%i:%SZ') AS closed_at,
                    spec_id,
                    description, design, acceptance_criteria, notes,
                    domain_json, surface_json, role_scope, version,
                    phase, delivered_sha, rig,
                    workspace, board_rank,
                    CAST(estimated_hours AS DOUBLE) AS estimated_hours,
                    DATE_FORMAT(start_date, '%Y-%m-%d') AS start_date,
                    DATE_FORMAT(due_date,   '%Y-%m-%d') AS due_date
             FROM issues
             WHERE id = :id
             LIMIT 1";
        let rows: Vec<mysql_async::Row> = conn
            .exec(sql, mysql_async::params! { "id" => id })
            .await
            .map_err(map_err)?;
        rows.into_iter().next().map(row_to_detail).transpose()
    }
}

fn row_to_detail(row: mysql_async::Row) -> Result<IssueDetail, AppError> {
    let mut row = row;
    let take_string = |row: &mut mysql_async::Row, i: usize| -> Result<String, AppError> {
        row.take::<String, _>(i)
            .ok_or_else(|| AppError::Other(format!("issues row missing column {i}")))
    };
    let take_i32 = |row: &mut mysql_async::Row, i: usize| -> Result<i32, AppError> {
        row.take::<i32, _>(i)
            .ok_or_else(|| AppError::Other(format!("issues row missing column {i}")))
    };
    let take_opt =
        |row: &mut mysql_async::Row, i: usize| -> Option<String> { row.take::<Option<String>, _>(i).unwrap_or(None) };

    // Ordinals after removing external_ref (was 10) and depends_on_json (was 18):
    // 0=id 1=title 2=status 3=priority 4=issue_type 5=assignee 6=owner
    // 7=created_at 8=updated_at 9=closed_at 10=spec_id
    // 11=description 12=design 13=acceptance_criteria 14=notes
    // 15=domain_json 16=surface_json 17=role_scope 18=version
    // 19=phase 20=delivered_sha 21=rig 22=workspace 23=board_rank
    // 24=estimated_hours 25=start_date 26=due_date
    Ok(IssueDetail {
        id: take_string(&mut row, 0)?,
        title: take_string(&mut row, 1)?,
        status: take_string(&mut row, 2)?,
        priority: take_i32(&mut row, 3)?,
        issue_type: take_string(&mut row, 4)?,
        assignee: take_opt(&mut row, 5),
        owner: take_opt(&mut row, 6),
        created_at: take_opt(&mut row, 7),
        updated_at: take_opt(&mut row, 8),
        closed_at: take_opt(&mut row, 9),
        spec_id: take_opt(&mut row, 10),
        description: take_opt(&mut row, 11).unwrap_or_default(),
        design: take_opt(&mut row, 12).unwrap_or_default(),
        acceptance_criteria: take_opt(&mut row, 13).unwrap_or_default(),
        notes: take_opt(&mut row, 14).unwrap_or_default(),
        domain_json: take_string(&mut row, 15)?,
        surface_json: take_string(&mut row, 16)?,
        role_scope: take_opt(&mut row, 17),
        version: row.take::<i64, _>(18).unwrap_or(0),
        phase: take_opt(&mut row, 19).unwrap_or_else(|| "P1".to_string()),
        delivered_sha: take_opt(&mut row, 20),
        rig: take_opt(&mut row, 21).unwrap_or_default(),
        workspace: take_opt(&mut row, 22).unwrap_or_else(default_workspace),
        board_rank: take_opt(&mut row, 23).unwrap_or_default(),
        estimated_hours: row.take::<Option<f64>, _>(24).unwrap_or(None),
        start_date: take_opt(&mut row, 25),
        due_date: take_opt(&mut row, 26),
    })
}

fn row_to_issue(row: mysql_async::Row, full: bool) -> Result<IssueRow, AppError> {
    let mut row = row;
    let take_string = |row: &mut mysql_async::Row, i: usize| -> Result<String, AppError> {
        row.take::<String, _>(i)
            .ok_or_else(|| AppError::Other(format!("issues row missing column {i}")))
    };
    let take_i32 = |row: &mut mysql_async::Row, i: usize| -> Result<i32, AppError> {
        row.take::<i32, _>(i)
            .ok_or_else(|| AppError::Other(format!("issues row missing column {i}")))
    };
    // `take::<Option<String>, _>` returns `Some(None)` for SQL NULL, so the
    // outer `unwrap_or(None)` collapses both "absent column" and "NULL" into
    // the same `None` — matches the previous tuple path's semantics.
    let take_opt = |row: &mut mysql_async::Row, i: usize| -> Option<String> {
        row.take::<Option<String>, _>(i).unwrap_or(None)
    };

    // Ordinals after removing external_ref (was 10) and depends_on_json (was 14):
    // 0=id 1=title 2=status 3=priority 4=issue_type 5=assignee 6=owner
    // 7=created_at 8=updated_at 9=closed_at 10=spec_id
    // 11=domain_json 12=surface_json 13=role_scope 14=version
    // 15=phase 16=delivered_sha 17=rig 18=workspace 19=board_rank
    // 20=estimated_hours 21=start_date 22=due_date 23-26=body_cols
    Ok(IssueRow {
        id: take_string(&mut row, 0)?,
        title: take_string(&mut row, 1)?,
        status: take_string(&mut row, 2)?,
        priority: take_i32(&mut row, 3)?,
        issue_type: take_string(&mut row, 4)?,
        assignee: take_opt(&mut row, 5),
        owner: take_opt(&mut row, 6),
        created_at: take_opt(&mut row, 7),
        updated_at: take_opt(&mut row, 8),
        closed_at: take_opt(&mut row, 9),
        spec_id: take_opt(&mut row, 10),
        domain_json: take_string(&mut row, 11)?,
        surface_json: take_string(&mut row, 12)?,
        role_scope: take_opt(&mut row, 13),
        version: row.take::<i64, _>(14).unwrap_or(0),
        phase: take_opt(&mut row, 15).unwrap_or_else(|| "P1".to_string()),
        delivered_sha: take_opt(&mut row, 16),
        rig: take_opt(&mut row, 17).unwrap_or_default(),
        workspace: take_opt(&mut row, 18).unwrap_or_else(default_workspace),
        board_rank: take_opt(&mut row, 19).unwrap_or_default(),
        estimated_hours: row.take::<Option<f64>, _>(20).unwrap_or(None),
        start_date: take_opt(&mut row, 21),
        due_date: take_opt(&mut row, 22),
        // Bodies only SELECTed when `full` — ordinals 23-26.
        description: full.then(|| take_opt(&mut row, 23).unwrap_or_default()),
        design: full.then(|| take_opt(&mut row, 24).unwrap_or_default()),
        acceptance_criteria: full.then(|| take_opt(&mut row, 25).unwrap_or_default()),
        notes: full.then(|| take_opt(&mut row, 26).unwrap_or_default()),
    })
}

#[cfg(test)]
mod bootstrap_tests {
    use super::split_database;

    #[test]
    fn splits_db_from_url() {
        let (server, db) = split_database("mysql://gtapp@dolt:3307/hq");
        assert_eq!(server, "mysql://gtapp@dolt:3307");
        assert_eq!(db.as_deref(), Some("hq"));
    }

    #[test]
    fn handles_query_params_and_missing_db() {
        let (_s, db) = split_database("mysql://root@127.0.0.1:3306/hq?ssl=false");
        assert_eq!(db.as_deref(), Some("hq"));
        let (server, none) = split_database("mysql://root@127.0.0.1:3306");
        assert_eq!(server, "mysql://root@127.0.0.1:3306");
        assert!(none.is_none(), "no path segment ⇒ no db to create");
    }
}
