//! Transport-free handlers for the `meta.*` operations (hq-fe-api-platform.4).
//!
//! The gap-minting core of `meta.report-gap` lives here, decoupled from any wire
//! shape, so the REST adapter ([`crate::http`]) dispatches to the **same** logic
//! the MCP path runs — the bead's "one source of truth, only the wire differs"
//! rule. It mirrors the gastown `meta.report_gap`: a reported gap is minted as a
//! `hq-gap-<slug>-<ts>` bead, parented under the gaps catalog sub-epic so it is
//! NN-16-sound on mint instead of orphaned, with the graph columns seeded so
//! `?ready=true` filtering and the reconciler's edge derivation pick it up without
//! a manual `issues.update`.
//!
//! Gated behind the `axum` feature alongside the adapter: the descriptor-only
//! build pulls no store and no handler.

use std::time::{SystemTime, UNIX_EPOCH};

use gt_store_dolt::{AppError, DoltIssues, NewIssue};
use serde_json::{json, Value};

use crate::commands::ReportGap;

/// The catalog sub-epic every auto-minted gap parents under when the caller
/// supplies no `external_ref`, so reported gaps satisfy NN-16 instead of landing
/// orphaned.
const GAP_CATALOG_EPIC: &str = "hq-gaps";

/// Mint a `hq-gap-<slug>-<ts>` bead from a reported gap (`meta.report-gap`).
///
/// Transport-free: the REST adapter calls this exactly as the MCP `dispatch_meta`
/// path does, so the gap-minting rules (slug id, default-to-catalog parent,
/// graph-column seeding) live in one place. `actor` is the server identity
/// stamped as `created_by`, never the request body. Returns the minted bead's
/// summary `{ bead, operation, priority, external_ref }`.
pub async fn run_report_gap(
    store: &DoltIssues,
    gap: &ReportGap,
    actor: &str,
) -> Result<Value, AppError> {
    gap.validate().map_err(AppError::Validation)?;
    let ts = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or(0);
    let id = format!("hq-gap-{}-{ts}", slugify(&gap.operation));
    let priority = gap.priority.unwrap_or(2);
    // Parent the gap under a sub-epic so it is NN-16-sound on mint instead of
    // orphaned; default to the gaps catalog sub-epic.
    let external_ref = match gap.external_ref.as_deref().map(str::trim) {
        Some(r) if !r.is_empty() => r.to_string(),
        _ => GAP_CATALOG_EPIC.to_string(),
    };
    // Ensure the catalog sub-epic exists whenever a gap parents under it — whether
    // defaulted or named explicitly — so the ref is never dangling. A custom
    // external_ref is the caller's to have created.
    if external_ref == GAP_CATALOG_EPIC {
        ensure_catalog_epic(store).await?;
    }
    let new = NewIssue {
        id: id.clone(),
        title: format!("gap: {}", gap.operation),
        issue_type: "task".into(),
        created_by: actor.into(),
        notes: gap.notes.clone().unwrap_or_default(),
        priority,
        external_ref: Some(external_ref.clone()),
        // Seed the graph columns so native-surface filtering + reconciler edge
        // derivation pick the bead up without a manual issues.update.
        domain_json: json_array(gap.domain.as_deref()),
        surface_json: json_array(gap.surface.as_deref()),
        depends_on_json: json_array(gap.depends_on.as_deref()),
        ..Default::default()
    };
    store.insert(&new).await?;
    Ok(json!({
        "bead": id,
        "operation": gap.operation,
        "priority": priority,
        "external_ref": external_ref,
    }))
}

/// Idempotently ensure the [`GAP_CATALOG_EPIC`] bead exists before a gap is
/// parented under it, so the `external_ref` is never dangling. A no-op once the
/// epic is present.
async fn ensure_catalog_epic(store: &DoltIssues) -> Result<(), AppError> {
    if store.get_detail(GAP_CATALOG_EPIC).await?.is_some() {
        return Ok(());
    }
    let epic = NewIssue {
        id: GAP_CATALOG_EPIC.into(),
        title: "Reported tooling/spec gaps (catalog)".into(),
        issue_type: "epic".into(),
        created_by: "meta.report-gap".into(),
        notes: "Auto-created home for gaps minted by meta.report-gap.execute \
                without an explicit external_ref."
            .into(),
        priority: 2,
        ..Default::default()
    };
    // A concurrent reporter may have inserted it between the check and here; treat
    // a duplicate-id insert as success (the epic now exists either way).
    match store.insert(&epic).await {
        Ok(()) => Ok(()),
        Err(_) if store.get_detail(GAP_CATALOG_EPIC).await?.is_some() => Ok(()),
        Err(e) => Err(e),
    }
}

/// Serialize an optional list of strings into a JSON-array column value. `None` or
/// an empty list yields `"[]"`, matching the `NOT NULL` `[]` invariant
/// [`DoltIssues::insert`] normalises to.
fn json_array(items: Option<&[String]>) -> String {
    match items {
        Some(v) if !v.is_empty() => serde_json::to_string(v).unwrap_or_else(|_| "[]".into()),
        _ => "[]".into(),
    }
}

/// Lowercase the operation into a kebab slug for the gap bead id: runs of
/// non-alphanumerics collapse to a single `-`, with no leading/trailing dash. An
/// all-punctuation operation degrades to `op` so the id is never empty.
fn slugify(s: &str) -> String {
    let mut out = String::with_capacity(s.len());
    let mut prev_dash = true; // suppress leading dash
    for c in s.chars() {
        if c.is_ascii_alphanumeric() {
            out.push(c.to_ascii_lowercase());
            prev_dash = false;
        } else if !prev_dash {
            out.push('-');
            prev_dash = true;
        }
    }
    while out.ends_with('-') {
        out.pop();
    }
    if out.is_empty() {
        out.push_str("op");
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn slugify_collapses_punctuation_runs() {
        assert_eq!(slugify("issues.archive.execute"), "issues-archive-execute");
        assert_eq!(slugify("  Trim..Me  "), "trim-me");
    }

    #[test]
    fn slugify_never_empty() {
        assert_eq!(slugify("..."), "op");
        assert_eq!(slugify(""), "op");
    }

    #[test]
    fn json_array_normalises_none_and_empty_to_bracket() {
        assert_eq!(json_array(None), "[]");
        assert_eq!(json_array(Some(&[])), "[]");
        assert_eq!(json_array(Some(&["a".into(), "b".into()])), r#"["a","b"]"#);
    }
}
