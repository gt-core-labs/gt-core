//! Tool dispatch + resource-querystring parsing for the gt-core MCP server.
//!
//! The server's `call_tool` routes a `<module>.<action>.<verb>` name here; this
//! module deserializes the arguments into the `gt-issues` command structs and
//! drives the transport-free `run_*_issue` handlers, then shapes the success
//! payload the way the gastown service did (so clients see the same responses,
//! including this session's DX: `{ ok, parsed }` on update.validate, post-bump
//! `version` on update/claim.execute, the `closed @ <sha>` note on close).

use gt_issues::handlers::{
    run_advance_phase, run_claim_issue, run_close_issue, run_create_issue, run_transition_issue,
    run_update_issue, ClaimResult,
};
use gt_issues::{
    AdvancePhase, ClaimIssue, CloseIssue, CommitInspector, CreateIssue, TransitionIssue,
    UpdateIssue,
};
use gt_meta::ReportGap;
use gt_module::McpTool;
use gt_store_dolt::{AppError, ClaimOutcome, DoltIssues, IssueFilter, NewIssue};
use serde_json::{json, Value};
use std::path::Path;
use std::time::{SystemTime, UNIX_EPOCH};

use crate::git_tree::{commit_inspector, surface_tree};

/// Map a serde deserialization error onto the domain error so a malformed tool
/// payload surfaces as a validation failure (not a 500).
fn parse_args<T: serde::de::DeserializeOwned>(args: Value) -> Result<T, AppError> {
    serde_json::from_value(args)
        .map_err(|e| AppError::Validation(format!("invalid arguments: {e}")))
}

/// Inject the S5 claim echo (`description`, `acceptance_criteria`, `phase`) into a
/// claim response so the agent reads any prose blocker + the phase gate before
/// working (hq-core-mcp.8 / docs/10 S5). Fields are only added when present (a
/// missing bead leaves them out rather than emitting `null`).
fn inject_claim_echo(body: &mut Value, res: &ClaimResult) {
    if let Some(d) = &res.description {
        body["description"] = json!(d);
    }
    if let Some(ac) = &res.acceptance_criteria {
        body["acceptance_criteria"] = json!(ac);
    }
    if let Some(p) = &res.phase {
        body["phase"] = json!(p);
    }
}

/// Dispatch one `issues.*` tool call. `actor` is the server-injected scope actor
/// (used as the close/claim attribution). `Ok(value)` is the success payload the
/// server wraps in a `CallToolResult`; `Err` maps to an MCP error.
pub async fn dispatch(
    store: &DoltIssues,
    tool: &str,
    args: Value,
    actor: &str,
    repo_dir: Option<&Path>,
) -> Result<Value, AppError> {
    match tool {
        "issues.create.validate" => {
            let a: CreateIssue = parse_args(args)?;
            run_create_issue(store, &a, surface_tree(repo_dir).as_ref(), true).await?;
            Ok(json!({ "ok": true }))
        }
        "issues.create.execute" => {
            let a: CreateIssue = parse_args(args)?;
            run_create_issue(store, &a, surface_tree(repo_dir).as_ref(), false).await?;
            Ok(json!({ "ok": true }))
        }
        "issues.update.validate" => {
            let a: UpdateIssue = parse_args(args.clone())?;
            run_update_issue(store, &a, surface_tree(repo_dir).as_ref(), true).await?;
            // Echo the parsed params back so a caller can confirm what the server
            // received (the DX echo shipped in hq-gap-issues-update-validate).
            Ok(json!({ "ok": true, "parsed": args }))
        }
        "issues.update.execute" => {
            let a: UpdateIssue = parse_args(args)?;
            let version = run_update_issue(store, &a, surface_tree(repo_dir).as_ref(), false).await?;
            let mut body = json!({ "ok": true });
            if let Some(v) = version {
                body["version"] = json!(v);
            }
            Ok(body)
        }
        "issues.transition.validate" => {
            let a: TransitionIssue = parse_args(args)?;
            run_transition_issue(store, &a, true).await?;
            Ok(json!({ "ok": true }))
        }
        "issues.transition.execute" => {
            let a: TransitionIssue = parse_args(args)?;
            run_transition_issue(store, &a, false).await?;
            Ok(json!({ "ok": true }))
        }
        "issues.close.validate" => {
            let a: CloseIssue = parse_args(args)?;
            // Validate is shape-only and returns before the S2 delivery check, so
            // no inspector is needed here.
            run_close_issue(store, &a, actor, None, true).await?;
            Ok(json!({ "ok": true }))
        }
        "issues.close.execute" => {
            let a: CloseIssue = parse_args(args)?;
            // S2 phase-2: the git inspector verifies the closing sha touches a
            // non-planned surface and yields the full sha to stamp. `None` (no
            // repo wired) skips verification and leaves delivered_sha NULL.
            let inspector = commit_inspector(repo_dir);
            let inspector_ref =
                inspector.as_ref().map(|i| i as &(dyn CommitInspector + Send + Sync));
            run_close_issue(store, &a, actor, inspector_ref, false).await?;
            Ok(json!({ "ok": true }))
        }
        "issues.claim.validate" => {
            let a: ClaimIssue = parse_args(args)?;
            let res = run_claim_issue(store, &a, actor, true).await?;
            let mut body = json!({ "outcome": "won" });
            inject_claim_echo(&mut body, &res);
            Ok(body)
        }
        "issues.claim.execute" => {
            let a: ClaimIssue = parse_args(args)?;
            let res = run_claim_issue(store, &a, actor, false).await?;
            match &res.outcome {
                ClaimOutcome::Won => {
                    let mut body = json!({ "outcome": "won", "owner": actor });
                    if let Some(v) = res.version {
                        body["version"] = json!(v);
                    }
                    // S5: echo description/acceptance_criteria/phase so the agent
                    // sees the prose blocker + the gate before it starts working.
                    inject_claim_echo(&mut body, &res);
                    Ok(body)
                }
                // A lost claim is a hard error so the caller cannot proceed as if
                // it owns the bead — the message names the current holder.
                ClaimOutcome::Lost { status, holder } => Err(AppError::Validation(format!(
                    "already claimed: {holder} holds it (status={status})"
                ))),
            }
        }
        // hq-core-mcp.7 (docs/10 S1) — operator-only frontier advance. The scope
        // check at the MCP boundary (server.rs) is the `never an agent` gate.
        "issues.phase.advance" => {
            let a: AdvancePhase = parse_args(args)?;
            run_advance_phase(store, &a, false).await?;
            Ok(json!({ "ok": true, "open_phase": a.open_phase }))
        }
        other => Err(AppError::Validation(format!("unknown tool `{other}`"))),
    }
}

/// Dispatch a `meta.*` tool (hq-core-host.7). `tools` is the server's full
/// descriptor set (for meta.help); `store` + `actor` back meta.report-gap, which
/// mints a `hq-gap-<slug>-<ts>` bead directly via the issues store.
pub async fn dispatch_meta(
    store: &DoltIssues,
    tool: &str,
    args: Value,
    actor: &str,
    tools: &[McpTool],
) -> Result<Value, AppError> {
    match tool {
        "meta.help.execute" => Ok(json!({ "tools": tools })),
        "meta.report-gap.execute" => {
            let g: ReportGap = parse_args(args)?;
            g.validate().map_err(AppError::Validation)?;
            let ts = SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .map(|d| d.as_secs())
                .unwrap_or(0);
            let id = format!("hq-gap-{}-{ts}", slugify(&g.operation));
            let priority = g.priority.unwrap_or(2);
            let new = NewIssue {
                id: id.clone(),
                title: format!("gap: {}", g.operation),
                issue_type: "task".into(),
                created_by: actor.into(),
                notes: g.notes.unwrap_or_default(),
                priority,
                ..Default::default()
            };
            store.insert(&new).await?;
            Ok(json!({ "bead": id, "operation": g.operation, "priority": priority }))
        }
        other => Err(AppError::Validation(format!("unknown tool `{other}`"))),
    }
}

/// Lowercase the operation into a kebab slug for the gap bead id: runs of
/// non-alphanumerics collapse to a single `-`, with no leading/trailing dash.
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

/// Parse the optional `gt://issues?...` querystring into an [`IssueFilter`].
/// Ported from the gastown service: unknown keys are rejected so a typo surfaces
/// instead of silently returning the unfiltered set. `full=1|true|yes` inlines
/// the heavy bodies (the DX shipped in hq-gap-issues-list-full).
pub fn parse_issue_filter(qs: &str) -> Result<IssueFilter, AppError> {
    let mut filter = IssueFilter::default();
    if qs.is_empty() {
        return Ok(filter);
    }
    for pair in qs.split('&') {
        if pair.is_empty() {
            continue;
        }
        let (key, value) = pair
            .split_once('=')
            .ok_or_else(|| AppError::Validation(format!("missing `=` in `{pair}`")))?;
        match key {
            "status" => {
                filter.status = value
                    .split(',')
                    .filter(|s| !s.is_empty())
                    .map(|s| s.to_string())
                    .collect();
            }
            "priority_max" => {
                filter.priority_max = Some(
                    value
                        .parse::<u8>()
                        .map_err(|e| AppError::Validation(format!("priority_max: {e}")))?,
                );
            }
            "assignee" => filter.assignee = Some(value.to_string()),
            "external_ref" => filter.external_ref = Some(value.to_string()),
            "issue_type" => filter.issue_type = Some(value.to_string()),
            "limit" => {
                filter.limit = Some(
                    value
                        .parse::<u32>()
                        .map_err(|e| AppError::Validation(format!("limit: {e}")))?,
                );
            }
            // hq-core-mcp.13 — zero-based page offset for less-style paging. Pair
            // with `limit`; advance to the page's `next_offset` to walk forward.
            "offset" => {
                filter.offset = Some(
                    value
                        .parse::<u32>()
                        .map_err(|e| AppError::Validation(format!("offset: {e}")))?,
                );
            }
            "full" => filter.full = matches!(value, "1" | "true" | "yes"),
            // hq-core-mcp.11 (docs/10 §S4) — narrow to sound beads. Applied by the
            // resource handler (needs the phase frontier + delivered index + git
            // tree), not by the store query.
            "ready" => filter.ready = matches!(value, "1" | "true" | "yes"),
            other => return Err(AppError::Validation(format!("unknown filter `{other}`"))),
        }
    }
    Ok(filter)
}

#[cfg(test)]
mod tests {
    use super::parse_issue_filter;

    #[test]
    fn parses_full_and_filters() {
        let f = parse_issue_filter("status=open,working&priority_max=1&full=1&limit=5").unwrap();
        assert_eq!(f.status, vec!["open".to_string(), "working".to_string()]);
        assert_eq!(f.priority_max, Some(1));
        assert!(f.full);
        assert_eq!(f.limit, Some(5));
    }

    #[test]
    fn parses_limit_and_offset() {
        // hq-core-mcp.13: less-style paging querystring.
        let f = parse_issue_filter("limit=50&offset=100").unwrap();
        assert_eq!(f.limit, Some(50));
        assert_eq!(f.offset, Some(100));
        // Defaults: no offset ⇒ start at 0 (None).
        assert!(parse_issue_filter("limit=10").unwrap().offset.is_none());
        // Malformed offset surfaces instead of silently defaulting.
        assert!(parse_issue_filter("offset=abc")
            .unwrap_err()
            .to_string()
            .contains("offset"));
    }

    #[test]
    fn empty_querystring_is_default() {
        let f = parse_issue_filter("").unwrap();
        assert!(f.status.is_empty() && !f.full && f.limit.is_none() && !f.ready);
    }

    #[test]
    fn parses_ready_flag() {
        assert!(parse_issue_filter("ready=1").unwrap().ready);
        assert!(parse_issue_filter("ready=true").unwrap().ready);
        // Combinable with other filters; absent ⇒ false.
        let f = parse_issue_filter("external_ref=hq-core-mcp&ready=yes").unwrap();
        assert!(f.ready && f.external_ref.as_deref() == Some("hq-core-mcp"));
        assert!(!parse_issue_filter("status=open").unwrap().ready);
    }

    #[test]
    fn rejects_unknown_key_and_malformed_pair() {
        assert!(parse_issue_filter("bogus=1").unwrap_err().to_string().contains("bogus"));
        assert!(parse_issue_filter("status").unwrap_err().to_string().contains('='));
    }
}
