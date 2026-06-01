//! Tool dispatch + resource-querystring parsing for the gt-core MCP server.
//!
//! The server's `call_tool` routes a `<module>.<action>.<verb>` name here; this
//! module deserializes the arguments into the `gt-issues` command structs and
//! drives the transport-free `run_*_issue` handlers, then shapes the success
//! payload the way the gastown service did (so clients see the same responses,
//! including this session's DX: `{ ok, parsed }` on update.validate, post-bump
//! `version` on update/claim.execute, the `closed @ <sha>` note on close).

use gt_issues::handlers::{
    run_claim_issue, run_close_issue, run_create_issue, run_transition_issue, run_update_issue,
    ClaimResult,
};
use gt_issues::{ClaimIssue, CloseIssue, CreateIssue, TransitionIssue, UpdateIssue};
use gt_store_dolt::{AppError, ClaimOutcome, DoltIssues, IssueFilter};
use serde_json::{json, Value};

/// Map a serde deserialization error onto the domain error so a malformed tool
/// payload surfaces as a validation failure (not a 500).
fn parse_args<T: serde::de::DeserializeOwned>(args: Value) -> Result<T, AppError> {
    serde_json::from_value(args)
        .map_err(|e| AppError::Validation(format!("invalid arguments: {e}")))
}

/// Dispatch one `issues.*` tool call. `actor` is the server-injected scope actor
/// (used as the close/claim attribution). `Ok(value)` is the success payload the
/// server wraps in a `CallToolResult`; `Err` maps to an MCP error.
pub async fn dispatch(
    store: &DoltIssues,
    tool: &str,
    args: Value,
    actor: &str,
) -> Result<Value, AppError> {
    match tool {
        "issues.create.validate" => {
            let a: CreateIssue = parse_args(args)?;
            run_create_issue(store, &a, true).await?;
            Ok(json!({ "ok": true }))
        }
        "issues.create.execute" => {
            let a: CreateIssue = parse_args(args)?;
            run_create_issue(store, &a, false).await?;
            Ok(json!({ "ok": true }))
        }
        "issues.update.validate" => {
            let a: UpdateIssue = parse_args(args.clone())?;
            run_update_issue(store, &a, true).await?;
            // Echo the parsed params back so a caller can confirm what the server
            // received (the DX echo shipped in hq-gap-issues-update-validate).
            Ok(json!({ "ok": true, "parsed": args }))
        }
        "issues.update.execute" => {
            let a: UpdateIssue = parse_args(args)?;
            let version = run_update_issue(store, &a, false).await?;
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
            run_close_issue(store, &a, actor, true).await?;
            Ok(json!({ "ok": true }))
        }
        "issues.close.execute" => {
            let a: CloseIssue = parse_args(args)?;
            run_close_issue(store, &a, actor, false).await?;
            Ok(json!({ "ok": true }))
        }
        "issues.claim.validate" => {
            let a: ClaimIssue = parse_args(args)?;
            let _ = run_claim_issue(store, &a, actor, true).await?;
            Ok(json!({ "outcome": "won" }))
        }
        "issues.claim.execute" => {
            let a: ClaimIssue = parse_args(args)?;
            let ClaimResult { outcome, version } = run_claim_issue(store, &a, actor, false).await?;
            match outcome {
                ClaimOutcome::Won => {
                    let mut body = json!({ "outcome": "won", "owner": actor });
                    if let Some(v) = version {
                        body["version"] = json!(v);
                    }
                    Ok(body)
                }
                // A lost claim is a hard error so the caller cannot proceed as if
                // it owns the bead — the message names the current holder.
                ClaimOutcome::Lost { status, holder } => Err(AppError::Validation(format!(
                    "already claimed: {holder} holds it (status={status})"
                ))),
            }
        }
        other => Err(AppError::Validation(format!("unknown tool `{other}`"))),
    }
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
            "full" => filter.full = matches!(value, "1" | "true" | "yes"),
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
    fn empty_querystring_is_default() {
        let f = parse_issue_filter("").unwrap();
        assert!(f.status.is_empty() && !f.full && f.limit.is_none());
    }

    #[test]
    fn rejects_unknown_key_and_malformed_pair() {
        assert!(parse_issue_filter("bogus=1").unwrap_err().to_string().contains("bogus"));
        assert!(parse_issue_filter("status").unwrap_err().to_string().contains('='));
    }
}
