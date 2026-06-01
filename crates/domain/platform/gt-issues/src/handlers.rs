//! Transport-free handlers for the `issues.*` tools (hq-core-host.2).
//!
//! Ported from gastown `gt-mcp` `service.rs` `run_*_issue`, with the rmcp
//! transport concerns (scope check, audit row, `CallToolResult`/`McpError`
//! wrapping) stripped — those belong to the server bin (`hq-core-host.3`). What
//! remains is the scope-and-audit-free core: `validate()` (always), then the
//! [`DoltIssues`] mutation on the execute path, returning the success payload
//! data (post-bump version, claim outcome) the transport formats.
//!
//! Each handler takes a borrowed [`DoltIssues`] — the module owns no store of its
//! own; the binary supplies the live adapter. `validate_only` short-circuits
//! before any state change, exactly as the `*.validate` tools do.

use gt_store_dolt::{AppError, ClaimOutcome, DoltIssues};

use crate::commands::{
    AdvancePhase, ClaimIssue, CloseIssue, CreateIssue, TransitionIssue, UpdateIssue,
};
use crate::delivery::{path_touches_surface, CommitInspector};
use crate::surface::{parse_surface_json, SurfaceTree};

/// Outcome of [`run_claim_issue`]. Carries the CAS result plus, on a won execute,
/// the post-bump row `version` so a follow-up `issues.update` can chain its
/// `expected_version` without a separate read (hq-gap-issues-claim-execute).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ClaimResult {
    /// Won / lost outcome of the compare-and-swap.
    pub outcome: ClaimOutcome,
    /// Post-bump `version` on a won execute; `None` on a validate-only call or a
    /// lost claim (no row was written).
    pub version: Option<i64>,
    /// The bead's `description`, echoed so the agent sees any prose blocker
    /// before working (hq-core-mcp.8 / docs/10 S5). `None` only when the bead
    /// does not exist.
    pub description: Option<String>,
    /// The bead's `acceptance_criteria`, echoed alongside the description (S5).
    pub acceptance_criteria: Option<String>,
    /// The bead's lifecycle `phase` (`"P1".."P4"`), echoed so the agent sees the
    /// gate it must respect before claiming (S5). `None` when the bead is absent.
    pub phase: Option<String>,
}

/// `issues.create`: validate (shape + S3 surface existence against `tree`), then
/// (execute only) insert + atomic Dolt commit. Uniqueness on `id` is enforced by
/// the store's duplicate-key path. `tree` is the server's `main` git tree; a
/// non-existent `planned:false` surface path is rejected before any write
/// (docs/10 §S3).
pub async fn run_create_issue(
    issues: &DoltIssues,
    args: &CreateIssue,
    tree: &(dyn SurfaceTree + Sync),
    validate_only: bool,
) -> Result<(), AppError> {
    args.validate()?;
    args.validate_surface(tree)?;
    if validate_only {
        return Ok(());
    }
    issues.insert(&args.to_new()).await
}

/// `issues.update`: validate, then (execute only) patch + atomic Dolt commit.
/// Returns the post-bump `version` on execute (so a follow-up edit can chain
/// `expected_version`), `None` on a validate-only call. A stale
/// `expected_version` surfaces as a `version conflict` from the store.
pub async fn run_update_issue(
    issues: &DoltIssues,
    args: &UpdateIssue,
    tree: &(dyn SurfaceTree + Sync),
    validate_only: bool,
) -> Result<Option<i64>, AppError> {
    args.validate()?;
    args.validate_surface(tree)?;
    if validate_only {
        return Ok(None);
    }
    issues.update(&args.id, &args.to_patch()).await?;
    // Post-bump version for the success payload. A read failure here does not
    // unwind the committed write — surface `None` rather than fail the update.
    Ok(issues.current_version(&args.id).await.ok().flatten())
}

/// `issues.transition`: validate the target label, then (execute only) move the
/// status across the state machine. Illegal moves are rejected by the store's
/// status-guarded UPDATE and surface as `Validation`.
pub async fn run_transition_issue(
    issues: &DoltIssues,
    args: &TransitionIssue,
    validate_only: bool,
) -> Result<(), AppError> {
    args.validate()?;
    if validate_only {
        return Ok(());
    }
    issues.transition(&args.id, args.target_status()).await
}

/// `issues.close`: validate (incl. the required `commit_sha`), then (execute
/// only) verify delivery (S2), record the delivering commit as a note, close with
/// attribution, and stamp `delivered_sha` when verified.
///
/// **Phase-2 delivery verification (hq-core-mcp.10, docs/10 §S2).** When an
/// `inspector` is supplied (the server wires one iff a repo is configured) and
/// the bead declares at least one non-`planned` surface path, the `commit_sha`
/// must resolve on `main` AND touch one of those paths — otherwise the close is
/// rejected before any write (the sha did not deliver the claimed surface). On a
/// match the resolved full sha is stamped via
/// [`set_delivered_sha`](DoltIssues::set_delivered_sha). A bead with no
/// non-`planned` surface (wontfix / pure process) closes without a stamp, leaving
/// `delivered_sha` NULL — correct, since no artifact was produced. With no
/// `inspector` (no repo wired) verification is skipped and the close behaves as
/// before, leaving `delivered_sha` NULL.
///
/// The note is appended **before** the close flips the status, so even a
/// subsequently-rejected close (e.g. already-closed) leaves the sha breadcrumb.
/// `actor` is the scope actor the transport injects, used as the attribution
/// fallback when the caller omits `closed_by_session`.
pub async fn run_close_issue(
    issues: &DoltIssues,
    args: &CloseIssue,
    actor: &str,
    inspector: Option<&(dyn CommitInspector + Send + Sync)>,
    validate_only: bool,
) -> Result<(), AppError> {
    args.validate()?;
    if validate_only {
        return Ok(());
    }
    let sha = args.commit_sha.trim();

    // S2 phase-2: confirm the sha delivered a non-planned surface before closing.
    // `delivered` carries the resolved full sha to stamp once the close lands.
    let mut delivered: Option<String> = None;
    if let Some(inspector) = inspector {
        let detail = issues
            .get_detail(&args.id)
            .await?
            .ok_or_else(|| AppError::NotFound(format!("issue {}", args.id)))?;
        let non_planned: Vec<String> = parse_surface_json(&detail.surface_json)
            .into_iter()
            .filter(|e| !e.planned)
            .map(|e| e.path)
            .collect();
        if !non_planned.is_empty() {
            let info = inspector.inspect(sha).ok_or_else(|| {
                AppError::Validation(format!(
                    "commit_sha {sha} does not resolve to a commit on main — close rejected (docs/10 §S2)"
                ))
            })?;
            let touches = non_planned.iter().any(|surface| {
                info.changed_paths
                    .iter()
                    .any(|changed| path_touches_surface(changed, surface))
            });
            if !touches {
                return Err(AppError::Validation(format!(
                    "commit {sha} does not touch any non-planned surface of {} — close rejected (docs/10 §S2)",
                    args.id
                )));
            }
            delivered = Some(info.full_sha);
        }
        // No non-planned surface → nothing to deliver; close leaves delivered_sha NULL.
    }

    let session = args.effective_session(actor);
    let note = format!("closed @ {sha} (by {session})");
    issues.append_notes(&args.id, &note).await?;
    issues.close(&args.id, session).await?;
    if let Some(full_sha) = delivered {
        issues.set_delivered_sha(&args.id, &full_sha).await?;
    }
    Ok(())
}

/// `issues.claim`: validate, then (execute only) run the atomic CAS claim for
/// `owner` (the scope actor the transport injects). Returns the claim outcome and
/// — on a won execute — the post-bump `version`.
///
/// A lost claim is a successful call carrying [`ClaimOutcome::Lost`]; the
/// transport turns it into a hard error naming the holder so the caller stands
/// down. A validate-only call reports [`ClaimOutcome::Won`] (shape passed) with
/// no version, since no row is touched.
///
/// Both paths echo the bead's `description`, `acceptance_criteria`, and `phase`
/// (hq-core-mcp.8 / docs/10 S5) so the agent reads any prose blocker + the phase
/// gate BEFORE working. The detail is read once up front; a missing bead leaves
/// the echo fields `None` (the claim itself then surfaces the `NotFound`).
pub async fn run_claim_issue(
    issues: &DoltIssues,
    args: &ClaimIssue,
    owner: &str,
    validate_only: bool,
) -> Result<ClaimResult, AppError> {
    args.validate()?;
    // S5 echo: pull the prose + phase the agent must see before working.
    let detail = issues.get_detail(&args.id).await?;
    let description = detail.as_ref().map(|d| d.description.clone());
    let acceptance_criteria = detail.as_ref().map(|d| d.acceptance_criteria.clone());
    let phase = detail.as_ref().map(|d| d.phase.clone());
    if validate_only {
        return Ok(ClaimResult {
            outcome: ClaimOutcome::Won,
            version: None,
            description,
            acceptance_criteria,
            phase,
        });
    }
    let outcome = issues.claim(&args.id, owner).await?;
    let version = match &outcome {
        ClaimOutcome::Won => issues.current_version(&args.id).await.ok().flatten(),
        ClaimOutcome::Lost { .. } => None,
    };
    Ok(ClaimResult {
        outcome,
        version,
        description,
        acceptance_criteria,
        phase,
    })
}

/// `issues.phase.advance` (OPERATOR ONLY, hq-core-mcp.7): validate the target
/// phase token, then (execute only) advance the singleton `phase_frontier`. The
/// caller's RBAC scope is the gate — never an agent — and is checked at the MCP
/// boundary before this runs; this handler carries no authorization of its own.
pub async fn run_advance_phase(
    issues: &DoltIssues,
    args: &AdvancePhase,
    validate_only: bool,
) -> Result<(), AppError> {
    args.validate()?;
    if validate_only {
        return Ok(());
    }
    issues.advance_phase(args.phase()).await
}
