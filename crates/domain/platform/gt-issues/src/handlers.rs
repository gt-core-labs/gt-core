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
use crate::surface::SurfaceTree;

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
/// only) record the delivering commit as a note and close with attribution.
///
/// The note is appended **before** the close flips the status, so even a
/// subsequently-rejected close (e.g. already-closed) leaves the sha breadcrumb.
/// `actor` is the scope actor the transport injects, used as the attribution
/// fallback when the caller omits `closed_by_session`.
pub async fn run_close_issue(
    issues: &DoltIssues,
    args: &CloseIssue,
    actor: &str,
    validate_only: bool,
) -> Result<(), AppError> {
    args.validate()?;
    if validate_only {
        return Ok(());
    }
    let session = args.effective_session(actor);
    let note = format!("closed @ {} (by {})", args.commit_sha.trim(), session);
    issues.append_notes(&args.id, &note).await?;
    issues.close(&args.id, session).await
}

/// `issues.claim`: validate, then (execute only) run the atomic CAS claim for
/// `owner` (the scope actor the transport injects). Returns the claim outcome and
/// — on a won execute — the post-bump `version`.
///
/// A lost claim is a successful call carrying [`ClaimOutcome::Lost`]; the
/// transport turns it into a hard error naming the holder so the caller stands
/// down. A validate-only call reports [`ClaimOutcome::Won`] (shape passed) with
/// no version, since no row is touched.
pub async fn run_claim_issue(
    issues: &DoltIssues,
    args: &ClaimIssue,
    owner: &str,
    validate_only: bool,
) -> Result<ClaimResult, AppError> {
    args.validate()?;
    if validate_only {
        return Ok(ClaimResult {
            outcome: ClaimOutcome::Won,
            version: None,
        });
    }
    let outcome = issues.claim(&args.id, owner).await?;
    let version = match &outcome {
        ClaimOutcome::Won => issues.current_version(&args.id).await.ok().flatten(),
        ClaimOutcome::Lost { .. } => None,
    };
    Ok(ClaimResult { outcome, version })
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
