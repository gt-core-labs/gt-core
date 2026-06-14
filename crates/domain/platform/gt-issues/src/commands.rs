//! Tool-argument structs for the `issues.*` MCP tools (hq-core-host.2).
//!
//! Ported verbatim-in-behaviour from the upstream `gt-mcp` `service.rs`. Each struct
//! is the wire shape an MCP client sends; `validate()` is the shape-only guard
//! the `validate` tool runs and the `execute` tool runs before touching Dolt.
//! The `to_new`/`to_patch` mappers translate the wire shape onto the store's
//! [`NewIssue`]/[`IssuePatch`].
//!
//! ## Two faithful-port notes
//!
//! 1. **`domain`/`surface`/`depends_on` are `Vec<String>`, not the closed-set
//!    `Domain`/`Role` enums.** The upstream service typed `domain` as
//!    `Vec<taxonomy::Domain>` and ran a `role.allows(domain)` check; that closed
//!    set is a separate, larger port (not in this bead's scope). The store
//!    already persists these columns as raw JSON-array strings
//!    ([`NewIssue::domain_json`] et al.), so the wire form round-trips unchanged
//!    — only the compile-time enum narrowing is deferred.
//! 2. **NN-16 is enforced here**, reusing the already-ported
//!    [`gt_module_mcp::taxonomy::validate`] (the bead-taxonomy guard), so a
//!    malformed `external_ref`/id is rejected at the same boundary the upstream app
//!    rejected it.

use std::collections::HashSet;

use gt_module_mcp::taxonomy::{validate as taxonomy_validate, BeadTaxonomy};
use gt_store_dolt::{AppError, IssuePatch, IssuePhase, IssueStatus, NewIssue};
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

use crate::surface::{
    check_surface_existence, check_surface_shape, surface_to_json, SurfaceEntry, SurfaceTree,
};
use crate::taxonomy::{Dispatch, Domain, IssueType};

/// Map a [`gt_module_mcp::taxonomy::TaxonomyError`] onto the store's
/// [`AppError::Validation`] so the NN-16 rejection surfaces with the same
/// `validation failed: …` shape as every other shape-rule failure.
fn taxonomy_err(e: gt_module_mcp::taxonomy::TaxonomyError) -> AppError {
    AppError::Validation(e.to_string())
}

/// Reject a planning date that is neither empty (the clear sentinel) nor
/// `YYYY-MM-DD`-shaped (hq-62130a). Shape-only — calendar validity (e.g. a
/// month 13) is left to the DATE column, which rejects it at execute.
fn check_date(field: &str, value: &str) -> Result<(), AppError> {
    if value.is_empty() {
        return Ok(()); // clear-to-NULL sentinel, mirrors assignee/external_ref
    }
    let b = value.as_bytes();
    let shaped = b.len() == 10
        && b[4] == b'-'
        && b[7] == b'-'
        && b.iter().enumerate().all(|(i, c)| matches!(i, 4 | 7) || c.is_ascii_digit());
    if !shaped {
        return Err(AppError::Validation(format!(
            "{field} must be YYYY-MM-DD (or empty to clear), got `{value}`"
        )));
    }
    Ok(())
}

/// Reject a phase token outside the closed set `P1..P4` (hq-core-mcp.7). Returns
/// the offending value verbatim so the agent sees exactly what was rejected.
fn check_phase(phase: &str) -> Result<(), AppError> {
    if IssuePhase::parse(phase).is_none() {
        return Err(AppError::Validation(format!(
            "unknown phase `{phase}` (expected one of P1/P2/P3/P4)"
        )));
    }
    Ok(())
}

/// Reject a dispatch token outside the closed set (gtcore-1acbcf). The empty
/// string is the clear-to-inherit sentinel (mirrors `assignee`/`start_date`):
/// it nulls the own value so the bead falls back to its `child_of` parent
/// chain, bottoming out at `manual`.
fn check_dispatch(dispatch: &str) -> Result<(), AppError> {
    if !dispatch.is_empty() && Dispatch::parse(dispatch).is_none() {
        return Err(AppError::Validation(format!(
            "unknown dispatch `{dispatch}` (expected auto/manual, or empty to clear back to inherit)"
        )));
    }
    Ok(())
}

/// Reject a self-edge or a duplicate dependency in a `depends_on` list.
fn check_depends_on(id: &str, deps: &[String]) -> Result<(), AppError> {
    if deps.iter().any(|d| d == id) {
        return Err(AppError::Validation(format!(
            "depends_on contains the bead's own id ({id}) — self-cycle"
        )));
    }
    let mut seen = HashSet::new();
    for dep in deps {
        if !seen.insert(dep.as_str()) {
            return Err(AppError::Validation(format!(
                "depends_on lists `{dep}` more than once"
            )));
        }
    }
    Ok(())
}

/// JSON-array string for the closed-set [`Domain`] values (e.g.
/// `["orch.merge","store.dolt"]`). Same NOT-NULL `"[]"` fallback.
fn domain_to_json(items: &[Domain]) -> String {
    serde_json::to_string(items).unwrap_or_else(|_| "[]".to_string())
}

fn default_priority() -> u8 {
    2
}

/// Generate a server-side bead id in `{rig}-{6hex}` format (hq-bead-id-standard.1).
/// The 6-hex hash comes from the lower 24 bits of the current nanosecond timestamp;
/// the DB primary-key constraint catches the rare collision so the caller can retry.
fn generate_bead_id(rig: &str) -> String {
    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_nanos() as u32)
        .unwrap_or(0);
    format!("{rig}-{:06x}", nanos & 0x00FF_FFFF)
}

/// Input for the `issues.create` tool (hq-mcp-issues.2). Mirrors the required
/// columns of `hq.issues`; optional fields fall back to schema defaults.
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
#[cfg_attr(feature = "axum", derive(utoipa::ToSchema))]
pub struct CreateIssue {
    /// Bead id. When provided, used verbatim (backward-compat with the old
    /// `{external_ref}.{n}` format and NN-16 validation still applies). When
    /// absent, the server generates `{rig}-{6hex}` (hq-bead-id-standard.1).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub id: Option<String>,
    /// Canonical rig namespace for this bead (hq-bead-id-standard.1). Stored
    /// directly as `issues.rig` and used as the id prefix for auto-generated ids.
    /// When absent, derived from the leading token of the provided `id`
    /// (backward-compat path so existing callers that pass `id` keep working).
    #[serde(default)]
    pub rig: String,
    /// Human-readable title. Required.
    pub title: String,
    /// Free-text description. Default empty.
    #[serde(default)]
    pub description: String,
    /// Design notes. Default empty.
    #[serde(default)]
    pub design: String,
    /// Acceptance criteria. Default empty.
    #[serde(default)]
    pub acceptance_criteria: String,
    /// Free-form notes. Default empty.
    #[serde(default)]
    pub notes: String,
    /// Priority `0..=2` (0 = P0). Defaults to `2`.
    #[serde(default = "default_priority")]
    pub priority: u8,
    /// Bead type — required. Closed set ([`IssueType`]): an out-of-set value is
    /// rejected at deserialization, mirroring `domain` (hq-48ca5c).
    pub issue_type: IssueType,
    /// Bead creator (the agent or operator). Required.
    pub created_by: String,
    /// Optional parent epic id (`child_of` relation). Required for a non-epic
    /// bead by NN-16; see [`Self::validate`].
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub parent_id: Option<String>,
    /// Optional assignee.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub assignee: Option<String>,
    /// Optional initial owner.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub owner: Option<String>,
    /// Semantic domains the bead affects (doc 14 §3). At least one is required.
    /// Closed set ([`Domain`]): an out-of-set value is rejected at deserialization
    /// (hq-core-mcp.3).
    #[serde(default)]
    pub domain: Vec<Domain>,
    /// Physical impact surface — crate names or repo paths the bead touches,
    /// each carrying a `planned` intent (docs/10 §S3). A bare string is read as
    /// `planned:false` for back-compat. Empty for pure spec/process work.
    #[serde(default)]
    pub surface: Vec<SurfaceEntry>,
    /// Forward dependency edges (this bead is blocked until each listed bead
    /// closes). Self-edges and duplicates are rejected at [`Self::validate`].
    #[serde(default)]
    pub depends_on: Vec<String>,
    /// Responsible role discriminator. Free-form; persisted verbatim.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub role_scope: Option<String>,
    /// Lifecycle phase (`P1..P4`, hq-core-mcp.7 / docs/10 S1). `None` lets the
    /// column default to `P1`; `Some(_)` is a scalar overwrite validated against
    /// the closed set. Stamp `P4` on a bead gated behind the kernel migration so
    /// `?ready=true` (S4) stops surfacing it while the frontier sits at `P3`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub phase: Option<String>,
    /// Board workspace the card lands in (hq-62130a) — the second half of the
    /// (rig, workspace) scope key. Default empty ⇒ the `'default'` workspace.
    #[serde(default)]
    pub workspace: String,
    /// Dispatch policy (gtcore-1acbcf C1) — whether an agent may claim this
    /// bead. Closed set ([`Dispatch`]): an out-of-set value is rejected at
    /// deserialization, mirroring `issue_type`. `None` stores SQL `NULL` =
    /// "inherit from the `child_of` parent chain", which bottoms out at
    /// `manual` — so `auto` is always an explicit opt-in.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub dispatch: Option<Dispatch>,
}

impl CreateIssue {
    /// The effective rig namespace: explicit `rig` field when provided, else the
    /// leading token of the provided `id` (backward-compat for callers that pass
    /// `id` without `rig`).
    pub fn effective_rig(&self) -> &str {
        if !self.rig.is_empty() {
            return &self.rig;
        }
        self.id
            .as_deref()
            .and_then(|id| id.split('-').next())
            .unwrap_or("")
    }

    /// Shape-only validation. Uniqueness on `id` is enforced by the DB layer at
    /// `execute` time (the duplicate-key error surfaces as `Validation` there),
    /// so a stale `validate` over the empty key namespace never races.
    pub fn validate(&self) -> Result<(), AppError> {
        // id: when provided must be non-empty; when absent, will be server-generated.
        if let Some(id) = &self.id {
            if id.is_empty() {
                return Err(AppError::Validation("issue id is empty (omit to auto-generate)".into()));
            }
        }
        // At least one of id or rig must identify a namespace.
        if self.effective_rig().is_empty() {
            return Err(AppError::Validation(
                "rig is required when id is absent (hq-bead-id-standard.1)".into(),
            ));
        }
        if self.title.is_empty() {
            return Err(AppError::Validation("issue title is empty".into()));
        }
        if self.created_by.is_empty() {
            return Err(AppError::Validation("created_by is empty".into()));
        }
        if self.priority > 2 {
            return Err(AppError::Validation(format!(
                "priority must be 0..=2, got {}",
                self.priority
            )));
        }

        // NN-16: when a manual id is provided, enforce the full old-format rule
        // (`<parent_id>.<n>`) for backward compat. When the id will be
        // server-generated ({rig}-{6hex} format), only require parent_id for
        // non-epics — the id no longer embeds the epic linkage.
        if let Some(id) = &self.id {
            taxonomy_validate(&BeadTaxonomy {
                id,
                issue_type: self.issue_type.as_str(),
                external_ref: self.parent_id.as_deref().unwrap_or(""),
            })
            .map_err(taxonomy_err)?;
            // Self-cycle check only makes sense with a known id.
            check_depends_on(id, &self.depends_on)?;
        } else {
            // Auto-generated id: require parent_id for non-epics (NN-16 linkage
            // is preserved; the id format constraint is relaxed).
            if !self.issue_type.is_epic()
                && self.parent_id.as_deref().unwrap_or("").trim().is_empty()
            {
                return Err(AppError::Validation(
                    "parent_id is required for non-epic beads (NN-16)".into(),
                ));
            }
        }

        if self.domain.is_empty() {
            return Err(AppError::Validation(
                "issue must declare at least one domain (doc 14 §2)".into(),
            ));
        }
        check_surface_shape(&self.surface)?;
        if let Some(phase) = &self.phase {
            check_phase(phase)?;
        }
        Ok(())
    }

    /// S3 surface existence check (docs/10 §S3): every `planned:false` surface
    /// path must exist on `main`. Separate from the shape-only [`Self::validate`]
    /// because it needs the git tree the server supplies; the handler runs it on
    /// both the `validate` and `execute` paths.
    pub fn validate_surface(&self, tree: &(dyn SurfaceTree + Sync)) -> Result<(), AppError> {
        check_surface_existence(&self.surface, tree)
    }

    /// Translate onto the store's insert payload. The taxonomy columns serialize
    /// to the JSON-array form the store persists; `surface` lands as the object
    /// form `[{"path":…,"planned":…}]`.
    ///
    /// When `id` is absent, generates `{rig}-{6hex}` (hq-bead-id-standard.1).
    /// The `rig` column is set from the explicit `rig` field when provided, else
    /// derived from the id's leading token for backward compat (hq-rig-isolation.1).
    pub fn to_new(&self) -> NewIssue {
        let rig = self.effective_rig().to_string();
        let id = self.id.clone().unwrap_or_else(|| generate_bead_id(&rig));
        NewIssue {
            id,
            title: self.title.clone(),
            description: self.description.clone(),
            design: self.design.clone(),
            acceptance_criteria: self.acceptance_criteria.clone(),
            notes: self.notes.clone(),
            priority: self.priority,
            issue_type: self.issue_type.as_str().to_string(),
            created_by: self.created_by.clone(),
            parent_id: self.parent_id.clone(),
            assignee: self.assignee.clone(),
            owner: self.owner.clone(),
            domain_json: domain_to_json(&self.domain),
            surface_json: surface_to_json(&self.surface),
            role_scope: self.role_scope.clone(),
            phase: self.phase.clone(),
            rig,
            workspace: self.workspace.clone(),
            dispatch: self.dispatch.map(|d| d.as_str().to_string()),
        }
    }
}

/// Input for the `issues.update` tool (hq-mcp-issues.3). Partial patch over the
/// editable columns; status transitions live on `issues.transition` so scope
/// grants stay separable. All fields except `id` are `Option`: `None` leaves the
/// column untouched, `Some(_)` overwrites. At least one field must be set.
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
#[cfg_attr(feature = "axum", derive(utoipa::ToSchema))]
pub struct UpdateIssue {
    /// Target bead id. Non-empty; the row is matched by primary key.
    pub id: String,
    /// New title. Empty rejected — `title` is `NOT NULL`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub title: Option<String>,
    /// New description body.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub description: Option<String>,
    /// New design notes.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub design: Option<String>,
    /// New acceptance criteria.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub acceptance_criteria: Option<String>,
    /// New free-form notes.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub notes: Option<String>,
    /// New priority `0..=2` (0 = P0).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub priority: Option<u8>,
    /// New `issue_type`. Closed set ([`IssueType`]): an out-of-set value is
    /// rejected at deserialization, mirroring `domain` (hq-48ca5c).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub issue_type: Option<IssueType>,
    /// New assignee. Empty string clears to canonical "unassigned".
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub assignee: Option<String>,
    /// New owner. Empty string clears.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub owner: Option<String>,
    /// New parent epic id (`child_of` relation). `Some("")` clears; `Some(id)` upserts; `None` leaves unchanged.
    /// When set non-empty, NN-16 is re-checked against the (possibly also-updated) `issue_type`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub parent_id: Option<String>,
    /// New semantic domains (closed set [`Domain`]). `None` leaves the column
    /// untouched; an empty overwrite is rejected (a bead must keep at least one
    /// domain); an out-of-set value is rejected at deserialization.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub domain: Option<Vec<Domain>>,
    /// New impact surface (object form `[{path,planned}]`, bare string read as
    /// `planned:false`). `None` leaves the column untouched; `Some(_)` overwrites
    /// (empty allowed). This is the field that repoints stale `surface_json` paths
    /// after a crate moves, or flips a `planned:true` entry to `false` on delivery.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub surface: Option<Vec<SurfaceEntry>>,
    /// New forward dependency edges. `None` leaves the column untouched. Self-
    /// cycle and duplicate ids are rejected at `validate`; cross-bead cycle
    /// detection runs at execute in the store.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub depends_on: Option<Vec<String>>,
    /// New lifecycle phase (`P1..P4`, hq-core-mcp.7). `None` leaves the column
    /// untouched; `Some(_)` is a scalar overwrite validated against the closed
    /// set (it also re-stamps `phase_ratified_at`). This is how a prose-blocked
    /// bead gets demoted to `P4` (docs/10 §5 hybrid step 1).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub phase: Option<String>,
    /// Optimistic-concurrency guard. Pass the `version` you read from
    /// `gt://issue/{id}`; the write applies only if the row is still at that
    /// version, else it fails with a `version conflict`. Omit for last-write-wins.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub expected_version: Option<i64>,
    /// New planning estimate in hours (hq-62130a, mockup "Horas Est."). Must be
    /// finite and ≥ 0.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub estimated_hours: Option<f64>,
    /// New planned start date `YYYY-MM-DD` (hq-62130a, "Fecha Inicio"). Empty
    /// string clears.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub start_date: Option<String>,
    /// New planned end date `YYYY-MM-DD` (hq-62130a, "Fecha Fin" — drives the
    /// retrasos metric). Empty string clears.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub due_date: Option<String>,
    /// New board workspace (hq-62130a) — re-scopes the card to another project.
    /// Empty rejected (a card always belongs to a workspace).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub workspace: Option<String>,
    /// New dispatch policy (gtcore-1acbcf C1). `None` leaves the column
    /// untouched; `Some("auto"|"manual")` overwrites (validated against the
    /// closed [`Dispatch`] set); `Some("")` clears the own value back to
    /// inherit-from-parent (`child_of` chain, bottoming out at `manual`) —
    /// the same clear-to-NULL sentinel `assignee`/`start_date` use. A string
    /// (not the enum) so the clear sentinel stays expressible, mirroring
    /// `phase`'s plumbing.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub dispatch: Option<String>,
}

impl UpdateIssue {
    /// Shape-only validation. Existence of `id` is checked at execute by the
    /// `affected_rows == 0` path in the store, mirroring create's deferred
    /// uniqueness check.
    pub fn validate(&self) -> Result<(), AppError> {
        if self.id.is_empty() {
            return Err(AppError::Validation("issue id is empty".into()));
        }
        if matches!(&self.title, Some(s) if s.is_empty()) {
            return Err(AppError::Validation("title is empty".into()));
        }
        if let Some(p) = self.priority {
            if p > 2 {
                return Err(AppError::Validation(format!("priority must be 0..=2, got {p}")));
            }
        }
        // NN-16 is only checkable when the caller (re)points `parent_id`, and
        // only the shape-only part here: when the patch ALSO carries `issue_type`
        // the post-update type is known, so validate against it (epics exempt).
        // When `issue_type` is omitted, a partial patch cannot tell an epic
        // (exempt) from a bead, so this guard defers — the store-aware
        // `run_update_issue` re-checks against the EXISTING row's type.
        if let Some(issue_type) = self.issue_type {
            self.check_taxonomy(issue_type.as_str())?;
        }
        if let Some(domain) = &self.domain {
            if domain.is_empty() {
                return Err(AppError::Validation(
                    "domain overwrite is empty; a bead must declare at least one domain (doc 14 §2)"
                        .into(),
                ));
            }
        }
        if let Some(depends_on) = &self.depends_on {
            check_depends_on(&self.id, depends_on)?;
        }
        if let Some(surface) = &self.surface {
            check_surface_shape(surface)?;
        }
        if let Some(phase) = &self.phase {
            check_phase(phase)?;
        }
        // gtcore-1acbcf — dispatch overwrite must stay in the closed set ("" clears).
        if let Some(dispatch) = &self.dispatch {
            check_dispatch(dispatch)?;
        }
        // hq-62130a — planning-field shape rules.
        if let Some(h) = self.estimated_hours {
            if !h.is_finite() || h < 0.0 {
                return Err(AppError::Validation(format!(
                    "estimated_hours must be a finite non-negative number, got {h}"
                )));
            }
        }
        for (name, value) in [("start_date", &self.start_date), ("due_date", &self.due_date)] {
            if let Some(d) = value {
                check_date(name, d)?;
            }
        }
        if matches!(&self.workspace, Some(w) if w.trim().is_empty()) {
            return Err(AppError::Validation(
                "workspace overwrite is empty; a card always belongs to a workspace".into(),
            ));
        }
        if self.to_patch().is_empty() {
            return Err(AppError::Validation("no fields set; nothing to update".into()));
        }
        Ok(())
    }

    /// NN-16 re-check for a re-pointed `parent_id`, against a resolved
    /// `issue_type` — the patch's own when present, else the existing row's type
    /// (the store-aware [`run_update_issue`](crate::handlers::run_update_issue)
    /// supplies it). Epics are exempt, matching `issues.create`. A no-op when the
    /// patch does not set `parent_id` to a non-empty value.
    pub fn check_taxonomy(&self, issue_type: &str) -> Result<(), AppError> {
        if let Some(parent_id) = &self.parent_id {
            if !parent_id.is_empty() {
                taxonomy_validate(&BeadTaxonomy {
                    id: &self.id,
                    issue_type,
                    external_ref: parent_id,
                })
                .map_err(taxonomy_err)?;
            }
        }
        Ok(())
    }

    /// S3 surface existence check (docs/10 §S3): when the patch repoints
    /// `surface`, every `planned:false` path in the new set must exist on `main`.
    /// A `None` surface patch leaves the column alone and is a no-op here.
    pub fn validate_surface(&self, tree: &(dyn SurfaceTree + Sync)) -> Result<(), AppError> {
        match &self.surface {
            Some(surface) => check_surface_existence(surface, tree),
            None => Ok(()),
        }
    }

    /// Translate onto the store's patch. Typed `Some(Vec<…>)` columns serialize
    /// to the JSON-array form the store overwrites verbatim; `surface` lands as
    /// the object form `[{"path":…,"planned":…}]`.
    pub fn to_patch(&self) -> IssuePatch {
        IssuePatch {
            title: self.title.clone(),
            description: self.description.clone(),
            design: self.design.clone(),
            acceptance_criteria: self.acceptance_criteria.clone(),
            notes: self.notes.clone(),
            priority: self.priority,
            issue_type: self.issue_type.map(|t| t.as_str().to_string()),
            assignee: self.assignee.clone(),
            owner: self.owner.clone(),
            parent_id: self.parent_id.clone(),
            domain_json: self.domain.as_deref().map(domain_to_json),
            surface_json: self.surface.as_deref().map(surface_to_json),
            phase: self.phase.clone(),
            expected_version: self.expected_version,
            estimated_hours: self.estimated_hours,
            start_date: self.start_date.clone(),
            due_date: self.due_date.clone(),
            workspace: self.workspace.clone(),
            dispatch: self.dispatch.clone(),
        }
    }
}

/// Input for the `issues.transition` tool (hq-mcp-issues.4). State-machine guard
/// over `hq.issues.status`: `open ↔ working`; either side may `close`; `closed`
/// re-opens through `open` but never jumps straight to `working`.
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
pub struct TransitionIssue {
    /// Target bead id. Non-empty.
    pub id: String,
    /// Target status: one of `open`, `working`, `closed`. Validated at the
    /// frontier so a misspelled value never reaches Dolt.
    pub target: String,
    /// The claim context recorded when entering `working` (CLAUDE.md §2): the
    /// what/files/decisions another agent needs to resume. The custodian
    /// ([`guard_claim_context`](crate::policy::guard_claim_context)) makes it
    /// mandatory for a `working` move — unless the quota subsystem confirms there
    /// is no capacity, in which case the move is deferred with a debt note. A
    /// supplied context must clear [`MIN_CONTEXT_LEN`](crate::policy::MIN_CONTEXT_LEN)
    /// so a placeholder like `wip` cannot satisfy the contract. Ignored for
    /// `open`/`closed` targets.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub context: Option<String>,
}

impl TransitionIssue {
    /// Shape-only validation: non-empty id, a recognised target status, and — when
    /// a `context` is supplied for any move — that it clears the minimum length (so
    /// `wip` is rejected). Whether context is *required* depends on the actor's live
    /// quota and is decided in the handler by the custodian, mirroring how
    /// [`CloseIssue`] validates a supplied `commit_sha`'s shape here but defers the
    /// *requirement* to `run_close_issue`.
    pub fn validate(&self) -> Result<(), AppError> {
        if self.id.is_empty() {
            return Err(AppError::Validation("issue id is empty".into()));
        }
        if IssueStatus::parse(&self.target).is_none() {
            return Err(AppError::Validation(format!(
                "unknown target status `{}` (expected open/working/closed)",
                self.target
            )));
        }
        if let Some(ctx) = self.context.as_deref().map(str::trim) {
            if !ctx.is_empty() && ctx.len() < crate::policy::MIN_CONTEXT_LEN {
                return Err(AppError::Validation(format!(
                    "context is too short ({} chars); record the what/files/decisions so another \
                     agent can resume (CLAUDE.md §2)",
                    ctx.len()
                )));
            }
        }
        Ok(())
    }

    /// The parsed target status. Call only after [`Self::validate`] succeeds.
    pub fn target_status(&self) -> IssueStatus {
        IssueStatus::parse(&self.target).expect("validate guards target parse")
    }
}

/// Input for the `issues.close` tool (hq-mcp-issues.5). Sibling of
/// `issues.transition` with `target=closed` that also stamps `closed_by_session`
/// for kanban attribution and records the delivering `commit_sha`.
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
pub struct CloseIssue {
    /// Target bead id. Non-empty.
    pub id: String,
    /// Commit SHA proving the bead's code actually landed (hq-mod-mcp.11).
    /// Required (7+ hex, git short or full sha) for a bead that declares a
    /// non-planned code surface — that close still needs delivered-code proof.
    /// Optional (omit) for a metadata/process bead with NO non-planned surface:
    /// a pure-tracking mutation produces no commit, so demanding a sha forced
    /// agents to fabricate one (hq-gap-issues-close-for-metadata-only-beads). The
    /// surface-based requirement is enforced store-side in
    /// [`run_close_issue`](crate::handlers::run_close_issue); existence of the sha
    /// in the repo is verified in a later phase (S2).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub commit_sha: Option<String>,
    /// Optional explicit session id for the attribution column. When omitted,
    /// `closed_by_session` defaults to the MCP scope actor.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub closed_by_session: Option<String>,
}

impl CloseIssue {
    /// Shape-only validation. A supplied `commit_sha` must be well-formed (7+
    /// hex); whether one is *required* depends on the bead's surface and is
    /// decided store-side in [`run_close_issue`](crate::handlers::run_close_issue),
    /// which the shape-only guard cannot see.
    pub fn validate(&self) -> Result<(), AppError> {
        if self.id.is_empty() {
            return Err(AppError::Validation("issue id is empty".into()));
        }
        if let Some(sha) = self.sha() {
            if sha.len() < 7 || !sha.chars().all(|c| c.is_ascii_hexdigit()) {
                return Err(AppError::Validation(
                    "commit_sha must be 7+ hex chars (git short or full sha)".into(),
                ));
            }
        }
        if matches!(&self.closed_by_session, Some(s) if s.is_empty()) {
            return Err(AppError::Validation(
                "closed_by_session is empty (omit to default to MCP actor)".into(),
            ));
        }
        Ok(())
    }

    /// The trimmed `commit_sha`, or `None` when omitted or blank. A blank/empty
    /// string is treated the same as omission so a caller cannot slip past the
    /// code-surface requirement with whitespace.
    pub fn sha(&self) -> Option<&str> {
        self.commit_sha
            .as_deref()
            .map(str::trim)
            .filter(|s| !s.is_empty())
    }

    /// Resolve the attribution string, falling back to the scope actor when the
    /// caller did not supply one. Borrowing keeps the no-alloc fallback.
    pub fn effective_session<'a>(&'a self, scope_actor: &'a str) -> &'a str {
        self.closed_by_session.as_deref().unwrap_or(scope_actor)
    }
}

/// Input for the `issues.claim` tool (hq-mcp-issues.7 — server-side CAS claim,
/// docs/05 §1). The owner is intentionally NOT a wire field: the server injects
/// the authenticated scope actor, so a caller cannot claim on someone else's
/// behalf.
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
pub struct ClaimIssue {
    /// Target bead id. Non-empty.
    pub id: String,
}

impl ClaimIssue {
    /// Shape-only validation: non-empty id.
    pub fn validate(&self) -> Result<(), AppError> {
        if self.id.is_empty() {
            return Err(AppError::Validation("issue id is empty".into()));
        }
        Ok(())
    }
}

/// Input for the operator-only `issues.phase.advance` tool (hq-core-mcp.7,
/// docs/10 S1). Advances the global `phase_frontier.open_phase`. This is NOT an
/// agent tool: the RBAC scope (`issues.phase.advance`) is the gate, enforced at
/// the MCP boundary — an agent's allow-list never grants it. There is no
/// per-bead id: the frontier is a singleton governing the whole tracker.
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
pub struct AdvancePhase {
    /// The phase to open (`P1..P4`). Validated against the closed set; setting
    /// the same value is an idempotent re-ratification (the timestamp advances).
    pub open_phase: String,
}

impl AdvancePhase {
    /// Shape-only validation: `open_phase` must be a recognised `P1..P4` token.
    pub fn validate(&self) -> Result<(), AppError> {
        check_phase(&self.open_phase)?;
        Ok(())
    }

    /// The parsed target phase. Call only after [`Self::validate`] succeeds.
    pub fn phase(&self) -> IssuePhase {
        IssuePhase::parse(&self.open_phase).expect("validate guards open_phase parse")
    }
}

/// Input for `issues.list` — list beads with optional filters.
#[derive(Debug, Clone, Default, Serialize, Deserialize, JsonSchema)]
pub struct ListIssues {
    /// Filter by status (comma-separated: `open`, `working`, `closed`). Omit for all.
    #[serde(default)]
    pub status: Option<String>,
    /// Include only beads with priority ≤ this value (0 = highest).
    pub priority_max: Option<u8>,
    /// Filter by exact assignee.
    pub assignee: Option<String>,
    /// Filter by parent epic id (`child_of` relation).
    pub parent_id: Option<String>,
    /// Filter by issue type (`epic`, `task`, `spike`, …).
    pub issue_type: Option<String>,
    /// Page size (default and ceiling governed by server env vars).
    pub limit: Option<u32>,
    /// Zero-based row offset for less-style paging; advance to `next_offset` to walk forward.
    pub offset: Option<u32>,
    /// Include heavy text bodies (description/design/acceptance_criteria/notes) on every row.
    #[serde(default)]
    pub full: bool,
    /// Narrow to actionable beads only (every readiness clause holds).
    #[serde(default)]
    pub ready: bool,
    /// Narrow to a single rig (hq-rig-isolation.1). Absent ⇒ workspace-wide (back-compat).
    #[serde(default)]
    pub rig: Option<String>,
    /// Narrow to one board workspace (hq-62130a) — the second half of the
    /// (rig, workspace) scope key. Absent ⇒ all workspaces (back-compat).
    #[serde(default)]
    pub workspace: Option<String>,
    /// Filter by RESOLVED dispatch policy (`auto`|`manual`, gtcore-1acbcf C1):
    /// the bead's own value, else the `child_of`-inherited one, else `manual`
    /// — NOT the raw column. Validated against the closed [`Dispatch`] set at
    /// the handler. Resolution runs per row above the store (see
    /// [`crate::dispatch::filter_dispatch`] for the cost note), so a
    /// dispatch-filtered list fetches the candidate set up to the configured
    /// max-limit ceiling and pages in memory.
    #[serde(default)]
    pub dispatch: Option<String>,
}

/// Input for `issues.read` — fetch a single bead with its full detail.
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
pub struct ReadIssue {
    /// Bead id to fetch, e.g. `hq-core-host.2`.
    pub id: String,
}

#[cfg(test)]
mod tests {
    use super::*;

    fn base_create() -> CreateIssue {
        CreateIssue {
            id: Some("hq-core-host.2".into()),
            rig: String::new(),
            title: "t".into(),
            description: String::new(),
            design: String::new(),
            acceptance_criteria: String::new(),
            notes: String::new(),
            priority: 1,
            issue_type: IssueType::Task,
            created_by: "me".into(),
            parent_id: Some("hq-core-host".into()),
            assignee: None,
            owner: None,
            domain: vec![Domain::StoreDolt],
            surface: vec![],
            depends_on: vec![],
            role_scope: None,
            phase: None,
            workspace: String::new(),
            dispatch: None,
        }
    }

    /// An all-`None` patch (every field omitted) — fill in the field under test.
    fn base_update() -> UpdateIssue {
        UpdateIssue {
            id: "hq-core-host".into(),
            title: None,
            description: None,
            design: None,
            acceptance_criteria: None,
            notes: None,
            priority: None,
            issue_type: None,
            assignee: None,
            owner: None,
            parent_id: None,
            domain: None,
            surface: None,
            depends_on: None,
            phase: None,
            expected_version: None,
            estimated_hours: None,
            start_date: None,
            due_date: None,
            workspace: None,
            dispatch: None,
        }
    }

    #[test]
    fn create_accepts_well_formed_bead() {
        assert!(base_create().validate().is_ok());
    }

    #[test]
    fn create_rejects_empty_required_fields() {
        let mut c = base_create();
        c.title = String::new();
        assert!(c.validate().is_err());
        let mut c = base_create();
        c.created_by = String::new();
        assert!(c.validate().is_err());
        // Empty string id is also invalid.
        let mut c = base_create();
        c.id = Some(String::new());
        assert!(c.validate().is_err());
    }

    #[test]
    fn create_rejects_priority_above_two() {
        let mut c = base_create();
        c.priority = 3;
        assert!(c.validate().is_err());
    }

    #[test]
    fn create_requires_at_least_one_domain() {
        let mut c = base_create();
        c.domain.clear();
        assert!(c.validate().is_err());
    }

    #[test]
    fn create_enforces_nn16_parent_id() {
        // Non-epic with a mismatched sub-epic is rejected (old format, id provided).
        let mut c = base_create();
        c.parent_id = Some("hq-other".into());
        assert!(c.validate().is_err());
        // Missing parent_id on a non-epic is rejected (old format).
        let mut c = base_create();
        c.parent_id = None;
        assert!(c.validate().is_err());
        // An epic with provided id is exempt.
        let mut c = base_create();
        c.id = Some("hq-core-host".into());
        c.issue_type = IssueType::Epic;
        c.parent_id = None;
        assert!(c.validate().is_ok());
        // Auto-generated id (id=None): non-epic requires parent_id.
        let mut c = base_create();
        c.id = None;
        c.rig = "hq".into();
        c.parent_id = None;
        assert!(c.validate().is_err());
        // Auto-generated id: non-epic with parent_id passes.
        let mut c = base_create();
        c.id = None;
        c.rig = "hq".into();
        c.parent_id = Some("hq-core-host".into());
        assert!(c.validate().is_ok());
        // Auto-generated id: epic is exempt.
        let mut c = base_create();
        c.id = None;
        c.rig = "hq".into();
        c.issue_type = IssueType::Epic;
        c.parent_id = None;
        assert!(c.validate().is_ok());
    }

    #[test]
    fn create_rejects_self_cycle_dependency() {
        // Self-cycle only checked when id is explicitly provided.
        let mut c = base_create();
        c.depends_on = vec!["hq-core-host.2".into()];
        assert!(c.validate().is_err());
    }

    #[test]
    fn create_auto_id_requires_rig_or_id() {
        // Neither id nor rig → error.
        let mut c = base_create();
        c.id = None;
        c.rig = String::new();
        assert!(c.validate().is_err());
        // rig alone (no id) → ok for epic.
        c.rig = "gtweb".into();
        c.issue_type = IssueType::Epic;
        c.parent_id = None;
        assert!(c.validate().is_ok());
    }

    #[test]
    fn create_auto_id_to_new_generates_rig_prefix_id() {
        let mut c = base_create();
        c.id = None;
        c.rig = "gtweb".into();
        c.issue_type = IssueType::Epic;
        c.parent_id = None;
        let n = c.to_new();
        assert!(n.id.starts_with("gtweb-"), "id={}", n.id);
        assert_eq!(n.rig, "gtweb");
    }

    #[test]
    fn create_to_new_serializes_taxonomy_columns() {
        let c = base_create();
        let n = c.to_new();
        assert_eq!(n.id, "hq-core-host.2");
        assert_eq!(n.rig, "hq"); // derived from id prefix (backward compat)
        assert_eq!(n.domain_json, "[\"store.dolt\"]");
        assert_eq!(n.surface_json, "[]");
    }

    #[test]
    fn create_surface_serializes_object_form_and_accepts_bare_wire() {
        // A bare-string surface entry on the wire reads as planned:false …
        let mut c = base_create();
        c.surface = serde_json::from_value(serde_json::json!([
            "crates/a",
            { "path": "migrations/gt-rig", "planned": true }
        ]))
        .unwrap();
        assert!(c.validate().is_ok());
        // … and the store form is the normalized object array.
        assert_eq!(
            c.to_new().surface_json,
            r#"[{"path":"crates/a","planned":false},{"path":"migrations/gt-rig","planned":true}]"#
        );
        // Shape rules still bite: duplicate path is rejected.
        c.surface = vec![
            SurfaceEntry { path: "x".into(), planned: false },
            SurfaceEntry { path: "x".into(), planned: true },
        ];
        assert!(c.validate().is_err());
    }

    #[test]
    fn validate_surface_rejects_missing_non_planned_only() {
        use crate::surface::SurfaceTree;
        struct Empty;
        impl SurfaceTree for Empty {
            fn contains(&self, _: &str) -> bool {
                false
            }
        }
        let mut c = base_create();
        // planned:false against an empty tree → rejected.
        c.surface = vec![SurfaceEntry { path: "crates/missing".into(), planned: false }];
        assert!(c.validate_surface(&Empty).is_err());
        // planned:true → accepted even against an empty tree.
        c.surface = vec![SurfaceEntry { path: "crates/missing".into(), planned: true }];
        assert!(c.validate_surface(&Empty).is_ok());
    }

    #[test]
    fn update_rejects_empty_patch() {
        let u = UpdateIssue {
            id: "hq-core-host.2".into(),
            title: None,
            description: None,
            design: None,
            acceptance_criteria: None,
            notes: None,
            priority: None,
            issue_type: None,
            assignee: None,
            owner: None,
            parent_id: None,
            domain: None,
            surface: None,
            depends_on: None,
            phase: None,
            expected_version: None,
            estimated_hours: None,
            start_date: None,
            due_date: None,
            workspace: None,
            dispatch: None,
        };
        assert!(u.validate().is_err());
    }

    #[test]
    fn update_accepts_single_field_and_carries_version_guard() {
        let mut u = UpdateIssue {
            id: "hq-core-host.2".into(),
            title: Some("new".into()),
            description: None,
            design: None,
            acceptance_criteria: None,
            notes: None,
            priority: None,
            issue_type: None,
            assignee: None,
            owner: None,
            parent_id: None,
            domain: None,
            surface: None,
            depends_on: None,
            phase: None,
            expected_version: Some(7),
            estimated_hours: None,
            start_date: None,
            due_date: None,
            workspace: None,
            dispatch: None,
        };
        assert!(u.validate().is_ok());
        assert_eq!(u.to_patch().expected_version, Some(7));
        // Empty domain overwrite is rejected.
        u.domain = Some(vec![]);
        assert!(u.validate().is_err());
    }

    #[test]
    fn update_rejects_empty_title_overwrite() {
        let u = UpdateIssue {
            id: "x".into(),
            title: Some(String::new()),
            description: None,
            design: None,
            acceptance_criteria: None,
            notes: None,
            priority: None,
            issue_type: None,
            assignee: None,
            owner: None,
            parent_id: None,
            domain: None,
            surface: None,
            depends_on: None,
            phase: None,
            expected_version: None,
            estimated_hours: None,
            start_date: None,
            due_date: None,
            workspace: None,
            dispatch: None,
        };
        assert!(u.validate().is_err());
    }

    #[test]
    fn update_reparent_to_epic_passes_shape_validate_when_type_in_patch() {
        // Re-parenting a dash-named epic (`hq-core-host` -> parent `hq-core`)
        // is accepted when the patch declares `issue_type=epic` — epics are
        // exempt from the `<parent_id>.<n>` id rule, same as create.
        let mut u = base_update();
        u.parent_id = Some("hq-core".into());
        u.issue_type = Some(IssueType::Epic);
        assert!(u.validate().is_ok());
    }

    #[test]
    fn update_reparent_defers_taxonomy_when_type_omitted() {
        // With `issue_type` omitted the shape-only guard cannot tell an epic from
        // a bead, so it no longer assumes `task`; the store-aware handler
        // re-checks against the row's real type.
        let mut u = base_update();
        u.parent_id = Some("hq-core".into());
        assert!(u.validate().is_ok());
    }

    #[test]
    fn update_reparent_rejects_bead_with_wrong_type_in_patch() {
        // When the patch declares a non-epic type, the strict bead rule applies:
        // `hq-core-host` is not `<hq-core>.<n>`.
        let mut u = base_update();
        u.parent_id = Some("hq-core".into());
        u.issue_type = Some(IssueType::Task);
        assert!(matches!(u.validate(), Err(AppError::Validation(_))));
    }

    #[test]
    fn check_taxonomy_exempts_epic_and_rejects_mismatched_bead() {
        let mut u = base_update();
        u.id = "hq-core-host".into();
        u.parent_id = Some("hq-core".into());
        // Resolved as epic -> exempt.
        assert!(u.check_taxonomy("epic").is_ok());
        // Resolved as a bead -> id must be `<hq-core>.<n>`, which it is not.
        assert!(u.check_taxonomy("task").is_err());
        // No parent_id repoint -> no-op regardless of type.
        let u = base_update();
        assert!(u.check_taxonomy("task").is_ok());
    }

    #[test]
    fn transition_parses_known_targets_only() {
        assert!(TransitionIssue { id: "x".into(), target: "working".into(), context: None }.validate().is_ok());
        assert!(TransitionIssue { id: "x".into(), target: "frozen".into(), context: None }.validate().is_err());
        assert!(TransitionIssue { id: String::new(), target: "open".into(), context: None }.validate().is_err());
    }

    #[test]
    fn transition_rejects_too_short_context_but_accepts_a_real_one() {
        // A placeholder context fails the shape check (whether or not it is required).
        assert!(TransitionIssue {
            id: "x".into(),
            target: "working".into(),
            context: Some("wip".into()),
        }
        .validate()
        .is_err());
        // A real breadcrumb passes shape validation.
        assert!(TransitionIssue {
            id: "x".into(),
            target: "working".into(),
            context: Some("rewiring run_transition_issue; handlers.rs + policy.rs".into()),
        }
        .validate()
        .is_ok());
        // Absent context is shape-valid (the custodian decides the requirement).
        assert!(TransitionIssue { id: "x".into(), target: "working".into(), context: None }
            .validate()
            .is_ok());
    }

    #[test]
    fn close_validates_supplied_commit_sha_but_no_longer_requires_one() {
        // Well-formed sha passes.
        assert!(CloseIssue { id: "x".into(), commit_sha: Some("bbd0579".into()), closed_by_session: None }
            .validate()
            .is_ok());
        // Too short.
        assert!(CloseIssue { id: "x".into(), commit_sha: Some("abc".into()), closed_by_session: None }
            .validate()
            .is_err());
        // Non-hex.
        assert!(CloseIssue { id: "x".into(), commit_sha: Some("zzzzzzz".into()), closed_by_session: None }
            .validate()
            .is_err());
        // Omitted (None) now passes shape — the code-surface requirement is
        // enforced store-side, so a metadata bead can close without a sha
        // (hq-gap-issues-close-for-metadata-only-beads).
        assert!(CloseIssue { id: "x".into(), commit_sha: None, closed_by_session: None }
            .validate()
            .is_ok());
        // A blank/whitespace sha is treated as omitted, not as a malformed value.
        assert!(CloseIssue { id: "x".into(), commit_sha: Some("   ".into()), closed_by_session: None }
            .validate()
            .is_ok());
        assert_eq!(
            CloseIssue { id: "x".into(), commit_sha: Some("  bbd0579 ".into()), closed_by_session: None }.sha(),
            Some("bbd0579")
        );
    }

    #[test]
    fn close_effective_session_falls_back_to_actor() {
        let c = CloseIssue { id: "x".into(), commit_sha: Some("bbd0579".into()), closed_by_session: None };
        assert_eq!(c.effective_session("mcp-local"), "mcp-local");
        let c = CloseIssue {
            id: "x".into(),
            commit_sha: Some("bbd0579".into()),
            closed_by_session: Some("sess-42".into()),
        };
        assert_eq!(c.effective_session("mcp-local"), "sess-42");
    }

    #[test]
    fn claim_requires_non_empty_id() {
        assert!(ClaimIssue { id: "x".into() }.validate().is_ok());
        assert!(ClaimIssue { id: String::new() }.validate().is_err());
    }

    #[test]
    fn create_accepts_valid_phase_and_rejects_bad() {
        let mut c = base_create();
        c.phase = Some("P4".into());
        assert!(c.validate().is_ok());
        assert_eq!(c.to_new().phase.as_deref(), Some("P4"));
        let mut c = base_create();
        c.phase = Some("P5".into());
        assert!(c.validate().is_err());
        // Omitted phase round-trips as None (store applies the P1 default).
        assert_eq!(base_create().to_new().phase, None);
        // to_new sets id from the explicit field when provided.
        assert_eq!(base_create().to_new().id, "hq-core-host.2");
    }

    #[test]
    fn update_accepts_phase_overwrite_and_rejects_bad() {
        let mut u = UpdateIssue {
            id: "hq-core-mcp.7".into(),
            title: None,
            description: None,
            design: None,
            acceptance_criteria: None,
            notes: None,
            priority: None,
            issue_type: None,
            assignee: None,
            owner: None,
            parent_id: None,
            domain: None,
            surface: None,
            depends_on: None,
            phase: Some("P4".into()),
            expected_version: None,
            estimated_hours: None,
            start_date: None,
            due_date: None,
            workspace: None,
            dispatch: None,
        };
        assert!(u.validate().is_ok());
        assert_eq!(u.to_patch().phase.as_deref(), Some("P4"));
        u.phase = Some("nope".into());
        assert!(u.validate().is_err());
    }

    #[test]
    fn create_carries_dispatch_through_to_new_and_rejects_out_of_set_wire() {
        // Typed field: to_new serializes the lowercase store token.
        let mut c = base_create();
        c.dispatch = Some(Dispatch::Auto);
        assert!(c.validate().is_ok());
        assert_eq!(c.to_new().dispatch.as_deref(), Some("auto"));
        // Omitted ⇒ NULL ⇒ inherit-from-parent (gtcore-1acbcf).
        assert_eq!(base_create().to_new().dispatch, None);
        // The closed set bites at deserialization, same plumbing as issue_type.
        let mut wire = serde_json::to_value(base_create()).unwrap();
        wire["dispatch"] = serde_json::json!("always");
        assert!(serde_json::from_value::<CreateIssue>(wire.clone()).is_err());
        wire["dispatch"] = serde_json::json!("manual");
        let parsed: CreateIssue = serde_json::from_value(wire).unwrap();
        assert_eq!(parsed.dispatch, Some(Dispatch::Manual));
    }

    #[test]
    fn update_accepts_dispatch_overwrite_clear_and_rejects_bad() {
        // Overwrite with a closed-set token.
        let mut u = base_update();
        u.dispatch = Some("auto".into());
        assert!(u.validate().is_ok());
        assert_eq!(u.to_patch().dispatch.as_deref(), Some("auto"));
        // Empty string = clear back to inherit (NULL).
        u.dispatch = Some(String::new());
        assert!(u.validate().is_ok());
        assert_eq!(u.to_patch().dispatch.as_deref(), Some(""));
        // Out-of-set rejected by validate.
        u.dispatch = Some("always".into());
        assert!(matches!(u.validate(), Err(AppError::Validation(_))));
        // A dispatch-only patch is a non-empty patch.
        let mut u = base_update();
        u.dispatch = Some("manual".into());
        assert!(!u.to_patch().is_empty());
    }

    #[test]
    fn advance_phase_parses_known_tokens_only() {
        assert!(AdvancePhase { open_phase: "P3".into() }.validate().is_ok());
        assert_eq!(
            AdvancePhase { open_phase: "P4".into() }.phase(),
            IssuePhase::P4
        );
        assert!(AdvancePhase { open_phase: "p3".into() }.validate().is_err());
        assert!(AdvancePhase { open_phase: String::new() }.validate().is_err());
    }
}
