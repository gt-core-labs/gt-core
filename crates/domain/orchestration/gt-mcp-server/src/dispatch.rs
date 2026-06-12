//! Tool dispatch + resource-querystring parsing for the gt-core MCP server.
//!
//! The server's `call_tool` routes a `<module>.<action>.<verb>` name here; this
//! module deserializes the arguments into the `gt-issues` command structs and
//! drives the transport-free `run_*_issue` handlers, then shapes the success
//! payload the way the upstream service did (so clients see the same responses,
//! including this session's DX: `{ ok, parsed }` on update.validate, post-bump
//! `version` on update/claim.execute, the `closed @ <sha>` note on close).

use gt_issues::handlers::{
    run_advance_phase, run_claim_issue, run_close_issue, run_create_issue, run_transition_issue,
    run_update_issue, ClaimResult,
};
use gt_issues::{
    emit_issue_event, filter_dispatch, run_board_list, run_board_move, run_board_reorder,
    run_board_scopes, AdvancePhase, BoardList,
    BoardMove, BoardReorder, ClaimIssue, CloseIssue, CommitInspector, CreateIssue, Dispatch,
    IssueEventSink, IssueVerb, ListIssues, ReadIssue, TransitionIssue, UpdateIssue,
};
use gt_issues::resources::{read_issue, read_issues_page, filter_ready, read_issues};
use gt_meta::ReportGap;
use gt_module::McpTool;
use gt_store_dolt::{
    issues_default_limit, issues_max_limit, AppError, ClaimOutcome, DoltIssues, IssueFilter,
    IssuePage, NewIssue,
};
use serde_json::{json, Value};
use std::path::Path;
use std::time::{SystemTime, UNIX_EPOCH};

use crate::git_tree::{commit_inspector, surface_tree};
use crate::prefixes::{bead_prefix, WorkspaceRigPrefixes};

/// Map a serde deserialization error onto the domain error so a malformed tool
/// payload surfaces as a validation failure (not a 500).
fn parse_args<T: serde::de::DeserializeOwned>(args: Value) -> Result<T, AppError> {
    serde_json::from_value(args)
        .map_err(|e| AppError::Validation(format!("invalid arguments: {e}")))
}

/// The freshly-confirmed-block signal the claim-context custodian consults
/// (`hq-context-custodian.2`). A transition to `working` without context is normally
/// a stop-the-line; the custodian only *defers* it (rather than reject) when the
/// quota subsystem confirms there is genuinely no capacity to feed the work. That
/// authorization MUST come from the subsystem, never from a field the agent sets —
/// otherwise the deferral is a loophole.
///
/// The composition root implements this over the workspace's quota event log:
/// replay it into an [`AccountRegistry`](gt_quota::AccountRegistry) and return
/// [`pool_genuinely_blocked`](gt_quota::AccountRegistry::pool_genuinely_blocked),
/// which is `true` only on a non-empty, no-Healthy, freshly-blocked pool. `None`
/// wired (no quota log) ⇒ the custodian sees `false`, so context stays mandatory.
#[async_trait::async_trait]
pub trait QuotaBlockSignal: Send + Sync {
    /// `true` when `workspace`'s account pool offers no fresh capacity — the only
    /// condition under which a context-less `working` claim may be deferred.
    async fn pool_blocked(&self, workspace: Option<&str>) -> bool;
}

/// Reject an `issues.create` whose rig is not routable in the caller's workspace
/// (hq-mt-rigs.6, hq-bead-id-standard.2): the `rig` field (or the prefix derived
/// from the provided `id`) must be a registered rig prefix in `ws` or a reserved/
/// infra prefix. A prefix registered only in another tenant does not satisfy the
/// check (no global prefix uniqueness). No-op when no rig-prefix policy is wired
/// or the request resolved no workspace.
async fn enforce_rig_prefix(
    prefixes: Option<&dyn WorkspaceRigPrefixes>,
    ws: Option<&str>,
    rig: &str,
) -> Result<(), AppError> {
    let (Some(prefixes), Some(ws)) = (prefixes, ws) else {
        return Ok(());
    };
    if !prefixes.is_allowed(ws, rig).await? {
        return Err(AppError::Validation(format!(
            "rig `{rig}` is not a registered rig prefix in workspace `{ws}` \
             — register a rig with this prefix/name in this workspace first \
             (hq-bead-id-standard: pass rig=<canonical-name>)"
        )));
    }
    Ok(())
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
/// (used as the close/claim attribution). `sink` is the SSE-feed event sink
/// (`hq-issues-sse`): every successful `*.execute` mutation publishes its
/// [`IssueEvent`](gt_issues::IssueEvent) to it, keyed to `ws`, so an agent moving a
/// bead over MCP shows up on `GET /stream?channel=issues` — the natural-movement
/// source the REST surface alone would miss. `None` (no event log wired) emits
/// nothing, exactly as before. `Ok(value)` is the success payload the server wraps in
/// a `CallToolResult`; `Err` maps to an MCP error.
#[allow(clippy::too_many_arguments)]
pub async fn dispatch(
    store: &DoltIssues,
    tool: &str,
    args: Value,
    actor: &str,
    repo_dir: Option<&Path>,
    ws: Option<&str>,
    // hq-rig-isolation.7: X-Rig header value; default rig when args.rig is absent.
    request_rig: Option<&str>,
    prefixes: Option<&dyn WorkspaceRigPrefixes>,
    sink: Option<&dyn IssueEventSink>,
    // hq-context-custodian.2: the freshly-confirmed-block signal the claim-context
    // custodian consults on a `working` transition. `None` ⇒ context stays mandatory.
    quota_signal: Option<&dyn QuotaBlockSignal>,
) -> Result<Value, AppError> {
    match tool {
        "issues.create.validate" => {
            let a: CreateIssue = parse_args(args)?;
            // hq-bead-id-standard.2: validate against the canonical rig name/prefix
            // (explicit `rig` field, or prefix derived from the provided `id`).
            enforce_rig_prefix(prefixes, ws, a.effective_rig()).await?;
            run_create_issue(store, &a, surface_tree(repo_dir).as_ref(), true).await?;
            // rig echoed so callers can confirm the resolved namespace.
            Ok(json!({ "ok": true, "rig": a.effective_rig() }))
        }
        "issues.create.execute" => {
            let a: CreateIssue = parse_args(args)?;
            enforce_rig_prefix(prefixes, ws, a.effective_rig()).await?;
            // run_create_issue returns the actual bead id (which may be server-generated).
            let bead_id = run_create_issue(store, &a, surface_tree(repo_dir).as_ref(), false).await?;
            emit_issue_event(sink, store, ws, IssueVerb::Created, &bead_id, actor).await;
            Ok(json!({ "ok": true, "id": bead_id }))
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
            let version =
                run_update_issue(store, &a, surface_tree(repo_dir).as_ref(), false).await?;
            emit_issue_event(sink, store, ws, IssueVerb::Updated, &a.id, actor).await;
            let mut body = json!({ "ok": true });
            if let Some(v) = version {
                body["version"] = json!(v);
            }
            Ok(body)
        }
        "issues.transition.validate" => {
            let a: TransitionIssue = parse_args(args)?;
            // Shape-only; the custodian (and its quota signal) runs on execute.
            run_transition_issue(store, &a, false, true).await?;
            Ok(json!({ "ok": true }))
        }
        "issues.transition.execute" => {
            let a: TransitionIssue = parse_args(args)?;
            // Consult the quota subsystem ONLY for a `working` claim — the one move the
            // custodian gates — and only when a signal is wired; otherwise context stays
            // mandatory. The signal (not the agent) is what may authorize a deferral.
            // Parse (don't `target_status()`, which panics on a malformed label) so a bad
            // target falls through to run_transition_issue's validate, not a panic here.
            let quota_blocked = match (gt_store_dolt::IssueStatus::parse(&a.target), quota_signal) {
                (Some(gt_store_dolt::IssueStatus::Working), Some(sig)) => sig.pool_blocked(ws).await,
                _ => false,
            };
            run_transition_issue(store, &a, quota_blocked, false).await?;
            emit_issue_event(sink, store, ws, IssueVerb::Transitioned, &a.id, actor).await;
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
            let inspector_ref = inspector
                .as_ref()
                .map(|i| i as &(dyn CommitInspector + Send + Sync));
            run_close_issue(store, &a, actor, inspector_ref, false).await?;
            emit_issue_event(sink, store, ws, IssueVerb::Closed, &a.id, actor).await;
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
                    // Only a won claim mutated the row; a lost claim emits nothing.
                    emit_issue_event(sink, store, ws, IssueVerb::Claimed, &a.id, actor).await;
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
        "issues.list.execute" => {
            let a: ListIssues = parse_args(args)?;
            let mut filter = IssueFilter {
                status: a.status.map(|s| s.split(',').filter(|v| !v.is_empty()).map(String::from).collect()).unwrap_or_default(),
                priority_max: a.priority_max,
                assignee: a.assignee,
                parent_id: a.parent_id,
                issue_type: a.issue_type,
                limit: a.limit,
                offset: a.offset,
                full: a.full,
                ready: a.ready,
                // hq-rig-isolation.7: if the agent didn't pass rig explicitly, fall back to
                // the X-Rig header the polecat's .mcp.json injects on every call — so `list`
                // is automatically scoped to the right rig without prompt engineering.
                rig: a.rig.or_else(|| request_rig.map(str::to_string)),
                // hq-62130a — optional board-workspace narrowing.
                workspace: a.workspace,
            };
            // gtcore-1acbcf (C1) — `dispatch=auto|manual` narrows on the RESOLVED
            // policy (own value, else `child_of` inheritance, else manual), never
            // the raw column. Parse against the closed set up front so a typo
            // fails loud before any query runs.
            let want_dispatch = match a.dispatch.as_deref() {
                None => None,
                Some(raw) => Some(Dispatch::parse(raw).ok_or_else(|| {
                    AppError::Validation(format!(
                        "unknown dispatch filter `{raw}` (expected auto/manual)"
                    ))
                })?),
            };
            if filter.ready {
                // ready=true needs phase frontier + dep index + git tree (same as resource path)
                let rows = read_issues(store, &filter).await?;
                let open_phase = store.open_phase().await?;
                let deps_map = store.depends_on_edges(&filter).await?;
                let deps = store.dep_index().await?;
                let tree = surface_tree(repo_dir);
                let mut rows = filter_ready(rows, &deps_map, open_phase, &deps, tree.as_ref());
                if let Some(want) = want_dispatch {
                    // Unscoped maps: an ancestor epic may sit outside the rig/ws slice.
                    let parents = store.parent_map("", "").await?;
                    let raw = store.dispatch_index().await?;
                    rows = filter_dispatch(rows, want, &parents, &raw);
                }
                filter.ready = false; // consumed
                return serde_json::to_value(&rows)
                    .map_err(|e| AppError::Other(format!("encode issues: {e}")));
            }
            if let Some(want) = want_dispatch {
                // Inheritance resolution cannot run in the SQL WHERE, so the page
                // is assembled in memory: fetch the candidate set up to the max
                // ceiling, resolve+filter per row, then slice the requested page
                // (cost documented on `gt_issues::dispatch::filter_dispatch`).
                let offset = filter.offset.unwrap_or(0);
                let limit = filter
                    .limit
                    .unwrap_or_else(issues_default_limit)
                    .min(issues_max_limit());
                let mut unpaged = filter.clone();
                unpaged.offset = None;
                unpaged.limit = Some(issues_max_limit());
                let rows = read_issues(store, &unpaged).await?;
                let parents = store.parent_map("", "").await?;
                let raw = store.dispatch_index().await?;
                let rows = filter_dispatch(rows, want, &parents, &raw);
                let total = rows.len() as i64;
                let page_rows: Vec<_> = rows
                    .into_iter()
                    .skip(offset as usize)
                    .take(limit as usize)
                    .collect();
                let next_offset = offset.saturating_add(page_rows.len() as u32);
                let page = IssuePage {
                    rows: page_rows,
                    total,
                    next_offset,
                    has_more: (next_offset as i64) < total,
                };
                return serde_json::to_value(&page)
                    .map_err(|e| AppError::Other(format!("encode issues: {e}")));
            }
            let page = read_issues_page(store, &filter).await?;
            serde_json::to_value(&page)
                .map_err(|e| AppError::Other(format!("encode issues: {e}")))
        }
        "issues.read.execute" => {
            let a: ReadIssue = parse_args(args)?;
            if a.id.is_empty() {
                return Err(AppError::Validation("issue id is empty".into()));
            }
            match read_issue(store, &a.id).await? {
                Some(detail) => serde_json::to_value(&detail)
                    .map_err(|e| AppError::Other(format!("encode issue: {e}"))),
                None => Err(AppError::NotFound(format!("issue `{}`", a.id))),
            }
        }
        // hq-62130a — the Kanban board projection (ADR hq-423a4b). Same store,
        // same event sink as the issues tools; the board is a projection, so it
        // dispatches here rather than through the domain router.
        "board.list.execute" => {
            let mut a: BoardList = parse_args(args)?;
            // X-Rig fallback, mirroring issues.list (hq-rig-isolation.7).
            if a.rig.is_empty() {
                if let Some(rig) = request_rig {
                    a.rig = rig.to_string();
                }
            }
            let snapshot = run_board_list(store, &a, false).await?;
            serde_json::to_value(&snapshot)
                .map_err(|e| AppError::Other(format!("encode board: {e}")))
        }
        "board.scopes.execute" => {
            let scopes = run_board_scopes(store).await?;
            Ok(json!({ "scopes": scopes }))
        }
        "board.move.validate" => {
            let a: BoardMove = parse_args(args)?;
            run_board_move(store, &a, true).await?;
            Ok(json!({ "ok": true }))
        }
        "board.move.execute" => {
            let a: BoardMove = parse_args(args)?;
            let rank = run_board_move(store, &a, false).await?;
            // The card changed column = a status transition; SSE consumers see
            // the same issues.transitioned.v1 frame either write path produces.
            emit_issue_event(sink, store, ws, IssueVerb::Transitioned, &a.id, actor).await;
            Ok(json!({ "ok": true, "id": a.id, "column": a.column, "rank": rank }))
        }
        "board.reorder.validate" => {
            let a: BoardReorder = parse_args(args)?;
            run_board_reorder(store, &a, true).await?;
            Ok(json!({ "ok": true }))
        }
        "board.reorder.execute" => {
            let a: BoardReorder = parse_args(args)?;
            let rank = run_board_reorder(store, &a, false).await?;
            emit_issue_event(sink, store, ws, IssueVerb::Updated, &a.id, actor).await;
            Ok(json!({ "ok": true, "id": a.id, "rank": rank }))
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
            // Parent the gap under a sub-epic so it is NN-16-sound on mint
            // instead of orphaned; default to the gaps catalog sub-epic.
            let external_ref = match g.external_ref.as_deref().map(str::trim) {
                Some(r) if !r.is_empty() => r.to_string(),
                _ => GAP_CATALOG_EPIC.to_string(),
            };
            // Ensure the catalog sub-epic exists whenever a gap parents under it —
            // whether defaulted or named explicitly — so the ref is never
            // dangling. A custom external_ref is the caller's to have created.
            if external_ref == GAP_CATALOG_EPIC {
                ensure_catalog_epic(store).await?;
            }
            let new = NewIssue {
                id: id.clone(),
                title: format!("gap: {}", g.operation),
                issue_type: "task".into(),
                created_by: actor.into(),
                notes: g.notes.unwrap_or_default(),
                priority,
                parent_id: Some(external_ref.clone()),
                // Seed the graph columns so native-surface filtering + reconciler
                // edge derivation pick the bead up without a manual issues.update.
                domain_json: json_array(g.domain.as_deref()),
                surface_json: json_array(g.surface.as_deref()),
                rig: bead_prefix(&id).to_string(),
                ..Default::default()
            };
            store.insert(&new).await?;
            // Insert depends_on relations (replaces depends_on_json column).
            if let Some(deps) = &g.depends_on {
                for dep_id in deps {
                    store.add_relation(&id, dep_id, "depends_on").await?;
                }
            }
            Ok(json!({
                "bead": id,
                "operation": g.operation,
                "priority": priority,
                "parent_id": external_ref,
            }))
        }
        other => Err(AppError::Validation(format!("unknown tool `{other}`"))),
    }
}

/// The catalog sub-epic every auto-minted gap parents under when the caller
/// supplies no `external_ref`, so reported gaps satisfy NN-16 instead of landing
/// orphaned.
const GAP_CATALOG_EPIC: &str = "hq-gaps";

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
    // A concurrent reporter may have inserted it between the check and here;
    // treat a duplicate-id insert as success (the epic now exists either way).
    match store.insert(&epic).await {
        Ok(()) => Ok(()),
        Err(_) if store.get_detail(GAP_CATALOG_EPIC).await?.is_some() => Ok(()),
        Err(e) => Err(e),
    }
}

/// Serialize an optional list of strings into a JSON-array column value. `None`
/// or an empty list yields `"[]"`, matching the `NOT NULL` `[]` invariant
/// [`DoltIssues::insert`] normalises to.
fn json_array(items: Option<&[String]>) -> String {
    match items {
        Some(v) if !v.is_empty() => serde_json::to_string(v).unwrap_or_else(|_| "[]".into()),
        _ => "[]".into(),
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
/// Ported from the upstream service: unknown keys are rejected so a typo surfaces
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
            "parent_id" => filter.parent_id = Some(value.to_string()),
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
    use super::{enforce_rig_prefix, json_array, parse_issue_filter, slugify, GAP_CATALOG_EPIC};
    use crate::prefixes::WorkspaceRigPrefixes;
    use async_trait::async_trait;
    use gt_store_dolt::AppError;

    /// Stub policy: a fixed set of allowed prefixes for workspace `acme`, mimicking
    /// the rig catalog + reserved-set the real PG adapter unions. Anything else is
    /// rejected; a different workspace has no allowed prefixes (per-workspace).
    struct StubPrefixes;
    #[async_trait]
    impl WorkspaceRigPrefixes for StubPrefixes {
        async fn is_allowed(&self, ws: &str, prefix: &str) -> Result<bool, AppError> {
            Ok(ws == "acme" && matches!(prefix, "pl" | "hq"))
        }
    }

    #[tokio::test]
    async fn enforce_accepts_registered_prefix_in_workspace() {
        // Pass the rig name directly — enforce_rig_prefix no longer strips the trailing slug.
        let p = StubPrefixes;
        assert!(enforce_rig_prefix(Some(&p), Some("acme"), "pl").await.is_ok());
    }

    #[tokio::test]
    async fn enforce_accepts_reserved_prefix() {
        // The adapter reports the reserved `hq` as allowed, so tracker beads survive.
        let p = StubPrefixes;
        assert!(enforce_rig_prefix(Some(&p), Some("acme"), "hq").await.is_ok());
    }

    #[tokio::test]
    async fn enforce_rejects_unknown_prefix() {
        let p = StubPrefixes;
        let err = enforce_rig_prefix(Some(&p), Some("acme"), "zz")
            .await
            .unwrap_err();
        assert!(matches!(err, AppError::Validation(_)), "got {err:?}");
    }

    #[tokio::test]
    async fn enforce_rejects_prefix_registered_only_in_another_workspace() {
        // `pl` is allowed in `acme` but not in `other` — no global uniqueness.
        let p = StubPrefixes;
        assert!(enforce_rig_prefix(Some(&p), Some("other"), "pl").await.is_err());
    }

    #[tokio::test]
    async fn enforce_is_noop_without_policy_or_workspace() {
        // No policy wired (single-tenant / no-PG) ⇒ accept any prefix.
        assert!(enforce_rig_prefix(None, Some("acme"), "zz-foo")
            .await
            .is_ok());
        // Policy wired but request resolved no workspace ⇒ accept (legacy default).
        let p = StubPrefixes;
        assert!(enforce_rig_prefix(Some(&p), None, "zz-foo").await.is_ok());
    }

    #[test]
    fn json_array_seeds_columns_or_empties() {
        assert_eq!(json_array(None), "[]");
        assert_eq!(json_array(Some(&[])), "[]");
        assert_eq!(
            json_array(Some(&["crates/kernel/gt-meta".to_string()])),
            "[\"crates/kernel/gt-meta\"]"
        );
        assert_eq!(
            json_array(Some(&["a".to_string(), "b".to_string()])),
            "[\"a\",\"b\"]"
        );
    }

    #[test]
    fn catalog_epic_id_is_taxonomy_shaped() {
        // The default parent must be a valid bead id (lowercase kebab).
        assert_eq!(slugify(GAP_CATALOG_EPIC), GAP_CATALOG_EPIC);
    }

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
        let f = parse_issue_filter("parent_id=hq-core-mcp&ready=yes").unwrap();
        assert!(f.ready && f.parent_id.as_deref() == Some("hq-core-mcp"));
        assert!(!parse_issue_filter("status=open").unwrap().ready);
    }

    #[test]
    fn rejects_unknown_key_and_malformed_pair() {
        assert!(parse_issue_filter("bogus=1")
            .unwrap_err()
            .to_string()
            .contains("bogus"));
        assert!(parse_issue_filter("status")
            .unwrap_err()
            .to_string()
            .contains('='));
    }
}
