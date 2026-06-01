//! Tool-argument structs for the `issues.*` MCP tools (hq-core-host.2).
//!
//! Ported verbatim-in-behaviour from gastown `gt-mcp` `service.rs`. Each struct
//! is the wire shape an MCP client sends; `validate()` is the shape-only guard
//! the `validate` tool runs and the `execute` tool runs before touching Dolt.
//! The `to_new`/`to_patch` mappers translate the wire shape onto the store's
//! [`NewIssue`]/[`IssuePatch`].
//!
//! ## Two faithful-port notes
//!
//! 1. **`domain`/`surface`/`depends_on` are `Vec<String>`, not the closed-set
//!    `Domain`/`Role` enums.** The gastown service typed `domain` as
//!    `Vec<taxonomy::Domain>` and ran a `role.allows(domain)` check; that closed
//!    set is a separate, larger port (not in this bead's scope). The store
//!    already persists these columns as raw JSON-array strings
//!    ([`NewIssue::domain_json`] et al.), so the wire form round-trips unchanged
//!    — only the compile-time enum narrowing is deferred.
//! 2. **NN-16 is enforced here**, reusing the already-ported
//!    [`gt_module_mcp::taxonomy::validate`] (the bead-taxonomy guard), so a
//!    malformed `external_ref`/id is rejected at the same boundary gastown
//!    rejected it.

use std::collections::HashSet;

use gt_module_mcp::taxonomy::{validate as taxonomy_validate, BeadTaxonomy};
use gt_store_dolt::{AppError, IssuePatch, IssueStatus, NewIssue};
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

use crate::taxonomy::Domain;

/// Map a [`gt_module_mcp::taxonomy::TaxonomyError`] onto the store's
/// [`AppError::Validation`] so the NN-16 rejection surfaces with the same
/// `validation failed: …` shape as every other shape-rule failure.
fn taxonomy_err(e: gt_module_mcp::taxonomy::TaxonomyError) -> AppError {
    AppError::Validation(e.to_string())
}

/// Reject an empty entry or a duplicate in a free-form string list (used for
/// `surface` on update). Returns the offending value verbatim so the agent sees
/// exactly what the frontier rejected.
fn check_no_empty_or_dup(label: &str, items: &[String]) -> Result<(), AppError> {
    let mut seen = HashSet::new();
    for s in items {
        if s.trim().is_empty() {
            return Err(AppError::Validation(format!("{label} contains an empty entry")));
        }
        if !seen.insert(s.as_str()) {
            return Err(AppError::Validation(format!("{label} lists `{s}` more than once")));
        }
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

/// JSON-array string for a list of plain strings; `"[]"` on the (infallible)
/// serialize error path, matching the store's NOT-NULL default.
fn to_json_array(items: &[String]) -> String {
    serde_json::to_string(items).unwrap_or_else(|_| "[]".to_string())
}

/// JSON-array string for the closed-set [`Domain`] values (e.g.
/// `["orch.merge","store.dolt"]`). Same NOT-NULL `"[]"` fallback.
fn domain_to_json(items: &[Domain]) -> String {
    serde_json::to_string(items).unwrap_or_else(|_| "[]".to_string())
}

fn default_priority() -> u8 {
    2
}

/// Input for the `issues.create` tool (hq-mcp-issues.2). Mirrors the required
/// columns of `hq.issues`; optional fields fall back to schema defaults.
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
pub struct CreateIssue {
    /// Bead id. Must be unique in `hq.issues`; non-empty.
    pub id: String,
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
    /// `epic`/`task`/`spike`/... — required.
    pub issue_type: String,
    /// Bead creator (the agent or operator). Required.
    pub created_by: String,
    /// Optional epic linkage (the sub-epic this bead belongs to). Required for a
    /// non-epic bead by NN-16; see [`Self::validate`].
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub external_ref: Option<String>,
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
    /// Physical impact surface — crate names or repo paths the bead touches.
    /// Empty for pure spec/process work.
    #[serde(default)]
    pub surface: Vec<String>,
    /// Forward dependency edges (this bead is blocked until each listed bead
    /// closes). Self-edges and duplicates are rejected at [`Self::validate`].
    #[serde(default)]
    pub depends_on: Vec<String>,
    /// Responsible role discriminator. Free-form; persisted verbatim.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub role_scope: Option<String>,
}

impl CreateIssue {
    /// Shape-only validation. Uniqueness on `id` is enforced by the DB layer at
    /// `execute` time (the duplicate-key error surfaces as `Validation` there),
    /// so a stale `validate` over the empty key namespace never races.
    pub fn validate(&self) -> Result<(), AppError> {
        if self.id.is_empty() {
            return Err(AppError::Validation("issue id is empty".into()));
        }
        if self.title.is_empty() {
            return Err(AppError::Validation("issue title is empty".into()));
        }
        if self.issue_type.is_empty() {
            return Err(AppError::Validation("issue_type is empty".into()));
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

        // NN-16: every non-epic bead carries external_ref = its sub-epic and an
        // id of `<external_ref>.<n>`. Epics are exempt. Reuses the already-ported
        // gt-module-mcp guard so the rule matches gastown's frontier exactly.
        taxonomy_validate(&BeadTaxonomy {
            id: &self.id,
            issue_type: &self.issue_type,
            external_ref: self.external_ref.as_deref().unwrap_or(""),
        })
        .map_err(taxonomy_err)?;

        // Taxonomy shape rules. A fresh bead id cannot already appear in any
        // existing `depends_on`, so the only reachable cycle at create time is
        // the self-edge guarded below; cross-bead cycles belong to `update`.
        if self.domain.is_empty() {
            return Err(AppError::Validation(
                "issue must declare at least one domain (doc 14 §2)".into(),
            ));
        }
        check_depends_on(&self.id, &self.depends_on)?;
        check_no_empty_or_dup("surface", &self.surface)?;
        Ok(())
    }

    /// Translate onto the store's insert payload. The `Vec<String>` taxonomy
    /// columns serialize to the same JSON-array form the store persists.
    pub fn to_new(&self) -> NewIssue {
        NewIssue {
            id: self.id.clone(),
            title: self.title.clone(),
            description: self.description.clone(),
            design: self.design.clone(),
            acceptance_criteria: self.acceptance_criteria.clone(),
            notes: self.notes.clone(),
            priority: self.priority,
            issue_type: self.issue_type.clone(),
            created_by: self.created_by.clone(),
            external_ref: self.external_ref.clone(),
            assignee: self.assignee.clone(),
            owner: self.owner.clone(),
            domain_json: domain_to_json(&self.domain),
            surface_json: to_json_array(&self.surface),
            depends_on_json: to_json_array(&self.depends_on),
            role_scope: self.role_scope.clone(),
        }
    }
}

/// Input for the `issues.update` tool (hq-mcp-issues.3). Partial patch over the
/// editable columns; status transitions live on `issues.transition` so scope
/// grants stay separable. All fields except `id` are `Option`: `None` leaves the
/// column untouched, `Some(_)` overwrites. At least one field must be set.
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
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
    /// New `issue_type`. Empty rejected.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub issue_type: Option<String>,
    /// New assignee. Empty string clears to canonical "unassigned".
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub assignee: Option<String>,
    /// New owner. Empty string clears.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub owner: Option<String>,
    /// New epic linkage. Empty string clears. When set non-empty, NN-16 is
    /// re-checked against the (possibly also-updated) `issue_type`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub external_ref: Option<String>,
    /// New semantic domains (closed set [`Domain`]). `None` leaves the column
    /// untouched; an empty overwrite is rejected (a bead must keep at least one
    /// domain); an out-of-set value is rejected at deserialization.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub domain: Option<Vec<Domain>>,
    /// New impact surface. `None` leaves the column untouched; `Some(_)`
    /// overwrites (empty allowed). This is the field that repoints stale
    /// `surface_json` paths after a crate moves.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub surface: Option<Vec<String>>,
    /// New forward dependency edges. `None` leaves the column untouched. Self-
    /// cycle and duplicate ids are rejected at `validate`; cross-bead cycle
    /// detection runs at execute in the store.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub depends_on: Option<Vec<String>>,
    /// Optimistic-concurrency guard. Pass the `version` you read from
    /// `gt://issue/{id}`; the write applies only if the row is still at that
    /// version, else it fails with a `version conflict`. Omit for last-write-wins.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub expected_version: Option<i64>,
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
        if matches!(&self.issue_type, Some(s) if s.is_empty()) {
            return Err(AppError::Validation("issue_type is empty".into()));
        }
        if let Some(p) = self.priority {
            if p > 2 {
                return Err(AppError::Validation(format!("priority must be 0..=2, got {p}")));
            }
        }
        // NN-16 is only checkable when the caller (re)points `external_ref`,
        // since a partial patch otherwise lacks the row's id/external_ref pair.
        // When `issue_type` is not also being set, assume a non-epic bead
        // (`task`) — the strictest interpretation, matching gastown's guard.
        if let Some(external_ref) = &self.external_ref {
            if !external_ref.is_empty() {
                taxonomy_validate(&BeadTaxonomy {
                    id: &self.id,
                    issue_type: self.issue_type.as_deref().unwrap_or("task"),
                    external_ref,
                })
                .map_err(taxonomy_err)?;
            }
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
            check_no_empty_or_dup("surface", surface)?;
        }
        if self.to_patch().is_empty() {
            return Err(AppError::Validation("no fields set; nothing to update".into()));
        }
        Ok(())
    }

    /// Translate onto the store's patch. Typed `Some(Vec<String>)` columns
    /// serialize to the JSON-array form the store overwrites verbatim.
    pub fn to_patch(&self) -> IssuePatch {
        IssuePatch {
            title: self.title.clone(),
            description: self.description.clone(),
            design: self.design.clone(),
            acceptance_criteria: self.acceptance_criteria.clone(),
            notes: self.notes.clone(),
            priority: self.priority,
            issue_type: self.issue_type.clone(),
            assignee: self.assignee.clone(),
            owner: self.owner.clone(),
            external_ref: self.external_ref.clone(),
            domain_json: self.domain.as_deref().map(domain_to_json),
            surface_json: self.surface.as_deref().map(to_json_array),
            depends_on_json: self.depends_on.as_deref().map(to_json_array),
            expected_version: self.expected_version,
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
}

impl TransitionIssue {
    /// Shape-only validation: non-empty id and a recognised target status.
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
    /// Required and non-empty — a `closed` bead must reference a delivered
    /// commit, so a no-deliverable close is rejected by the server, not left to
    /// caller honesty. Format: 7+ hex chars (git short or full sha). Existence of
    /// the sha in the repo is NOT verified here — that is a later phase.
    pub commit_sha: String,
    /// Optional explicit session id for the attribution column. When omitted,
    /// `closed_by_session` defaults to the MCP scope actor.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub closed_by_session: Option<String>,
}

impl CloseIssue {
    /// Shape-only validation, including the required well-formed `commit_sha`.
    pub fn validate(&self) -> Result<(), AppError> {
        if self.id.is_empty() {
            return Err(AppError::Validation("issue id is empty".into()));
        }
        let sha = self.commit_sha.trim();
        if sha.is_empty() {
            return Err(AppError::Validation(
                "close requires commit_sha (delivered-code proof)".into(),
            ));
        }
        if sha.len() < 7 || !sha.chars().all(|c| c.is_ascii_hexdigit()) {
            return Err(AppError::Validation(
                "commit_sha must be 7+ hex chars (git short or full sha)".into(),
            ));
        }
        if matches!(&self.closed_by_session, Some(s) if s.is_empty()) {
            return Err(AppError::Validation(
                "closed_by_session is empty (omit to default to MCP actor)".into(),
            ));
        }
        Ok(())
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

#[cfg(test)]
mod tests {
    use super::*;

    fn base_create() -> CreateIssue {
        CreateIssue {
            id: "hq-core-host.2".into(),
            title: "t".into(),
            description: String::new(),
            design: String::new(),
            acceptance_criteria: String::new(),
            notes: String::new(),
            priority: 1,
            issue_type: "task".into(),
            created_by: "me".into(),
            external_ref: Some("hq-core-host".into()),
            assignee: None,
            owner: None,
            domain: vec![Domain::StoreDolt],
            surface: vec![],
            depends_on: vec![],
            role_scope: None,
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
    fn create_enforces_nn16_external_ref() {
        // Non-epic with a mismatched sub-epic is rejected.
        let mut c = base_create();
        c.external_ref = Some("hq-other".into());
        assert!(c.validate().is_err());
        // Missing external_ref on a non-epic is rejected.
        let mut c = base_create();
        c.external_ref = None;
        assert!(c.validate().is_err());
        // An epic is exempt.
        let mut c = base_create();
        c.id = "hq-core-host".into();
        c.issue_type = "epic".into();
        c.external_ref = None;
        assert!(c.validate().is_ok());
    }

    #[test]
    fn create_rejects_self_cycle_dependency() {
        let mut c = base_create();
        c.depends_on = vec!["hq-core-host.2".into()];
        assert!(c.validate().is_err());
    }

    #[test]
    fn create_to_new_serializes_taxonomy_columns() {
        let c = base_create();
        let n = c.to_new();
        assert_eq!(n.domain_json, "[\"store.dolt\"]");
        assert_eq!(n.surface_json, "[]");
        assert_eq!(n.depends_on_json, "[]");
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
            external_ref: None,
            domain: None,
            surface: None,
            depends_on: None,
            expected_version: None,
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
            external_ref: None,
            domain: None,
            surface: None,
            depends_on: None,
            expected_version: Some(7),
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
            external_ref: None,
            domain: None,
            surface: None,
            depends_on: None,
            expected_version: None,
        };
        assert!(u.validate().is_err());
    }

    #[test]
    fn transition_parses_known_targets_only() {
        assert!(TransitionIssue { id: "x".into(), target: "working".into() }.validate().is_ok());
        assert!(TransitionIssue { id: "x".into(), target: "frozen".into() }.validate().is_err());
        assert!(TransitionIssue { id: String::new(), target: "open".into() }.validate().is_err());
    }

    #[test]
    fn close_requires_well_formed_commit_sha() {
        assert!(CloseIssue { id: "x".into(), commit_sha: "bbd0579".into(), closed_by_session: None }
            .validate()
            .is_ok());
        // Too short.
        assert!(CloseIssue { id: "x".into(), commit_sha: "abc".into(), closed_by_session: None }
            .validate()
            .is_err());
        // Non-hex.
        assert!(CloseIssue { id: "x".into(), commit_sha: "zzzzzzz".into(), closed_by_session: None }
            .validate()
            .is_err());
        // Missing.
        assert!(CloseIssue { id: "x".into(), commit_sha: String::new(), closed_by_session: None }
            .validate()
            .is_err());
    }

    #[test]
    fn close_effective_session_falls_back_to_actor() {
        let c = CloseIssue { id: "x".into(), commit_sha: "bbd0579".into(), closed_by_session: None };
        assert_eq!(c.effective_session("mcp-local"), "mcp-local");
        let c = CloseIssue {
            id: "x".into(),
            commit_sha: "bbd0579".into(),
            closed_by_session: Some("sess-42".into()),
        };
        assert_eq!(c.effective_session("mcp-local"), "sess-42");
    }

    #[test]
    fn claim_requires_non_empty_id() {
        assert!(ClaimIssue { id: "x".into() }.validate().is_ok());
        assert!(ClaimIssue { id: String::new() }.validate().is_err());
    }
}
