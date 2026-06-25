//! Transport-free handlers for the `issues.*` tools (hq-core-host.2).
//!
//! Ported from the upstream `gt-mcp` `service.rs` `run_*_issue`, with the rmcp
//! transport concerns (scope check, audit row, `CallToolResult`/`McpError`
//! wrapping) stripped — those belong to the server bin (`hq-core-host.3`). What
//! remains is the scope-and-audit-free core: `validate()` (always), then the
//! [`DoltIssues`] mutation on the execute path, returning the success payload
//! data (post-bump version, claim outcome) the transport formats.
//!
//! Each handler takes a borrowed [`DoltIssues`] — the module owns no store of its
//! own; the binary supplies the live adapter. `validate_only` short-circuits
//! before any state change, exactly as the `*.validate` tools do.

use gt_store_dolt::{AppError, ClaimOutcome, DoltDomainCatalog, DoltIssues};

use crate::commands::{
    AdvancePhase, ClaimIssue, CloseIssue, CreateIssue, TransitionIssue, UpdateIssue,
};
use crate::delivery::{path_touches_surface, CommitInspector};
use crate::domain_validate::validate_domains;
use crate::policy::{guard_claim_context, PolicyVerdict, Violation};
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
///
/// Returns the actual bead id that was persisted (hq-bead-id-standard.1: may be
/// server-generated from `{rig}-{6hex}` when `args.id` is absent). On
/// `validate_only`, returns an empty string (no row written, no id assigned).
pub async fn run_create_issue(
    issues: &DoltIssues,
    args: &CreateIssue,
    tree: &(dyn SurfaceTree + Sync),
    validate_only: bool,
) -> Result<String, AppError> {
    args.validate()?;
    args.validate_surface(tree)?;
    // H2 (gtcore-d81e77): the bead's domains are validated against the workspace's
    // `domain_catalog` — reached through the issues store's own per-workspace pool,
    // the same `hq_<ws>` DB the bead lands in — not the closed `Domain` enum. Runs
    // on the `.validate` path too so the validate tool reports an out-of-catalog
    // domain, mirroring the surface-existence check above.
    validate_domains(&DoltDomainCatalog::new(issues.pool().clone()), &args.domain).await?;
    if validate_only {
        return Ok(String::new());
    }
    let new = args.to_new();
    let id = new.id.clone();
    issues.insert(&new).await?;
    Ok(id)
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
    // NN-16 epic-exemption parity (hq-gap-issues-update-external-ref): when the
    // patch re-points `external_ref` but omits `issue_type`, the shape-only
    // `validate()` cannot tell an epic (exempt) from a bead. Resolve the type
    // from the existing row so re-parenting a dash-named epic (e.g. `hq-core-host`
    // -> `hq-core`) is accepted just like `issues.create` accepts it. Runs before
    // the `validate_only` return so the `.validate` tool reports it too. A missing
    // row is left to the execute path's `affected_rows == 0` existence check.
    if args.issue_type.is_none() && args.parent_id.as_deref().is_some_and(|r| !r.is_empty()) {
        if let Some(detail) = issues.get_detail(&args.id).await? {
            args.check_taxonomy(&detail.issue_type)?;
        }
    }
    // H2 (gtcore-d81e77): when the patch repoints `domain`, validate the new set
    // against the bead workspace's `domain_catalog` (the issues store's own pool),
    // not the closed `Domain` enum. A `None` domain patch leaves the column alone
    // and skips this. Runs before the validate-only return so the validate tool
    // reports an out-of-catalog domain.
    if let Some(domains) = &args.domain {
        validate_domains(&DoltDomainCatalog::new(issues.pool().clone()), domains).await?;
    }
    if validate_only {
        return Ok(None);
    }
    issues.update(&args.id, &args.to_patch()).await?;
    // Post-bump version for the success payload. A read failure here does not
    // unwind the committed write — surface `None` rather than fail the update.
    Ok(issues.current_version(&args.id).await.ok().flatten())
}

/// `issues.transition`: validate the target label, consult the claim-context
/// custodian for a `working` move, then (execute only) move the status across the
/// state machine. Illegal moves are rejected by the store's status-guarded UPDATE
/// and surface as `Validation`.
///
/// **Claim-context custodian (hq-context-custodian).** A transition to `working`
/// must record the what/files/decisions another agent needs to resume (CLAUDE.md
/// §2). The gate calques [`run_close_issue`]'s conditional `commit_sha`: the
/// custodian ([`guard_claim_context`]) is consulted before any write, fed
/// `quota_blocked` — a signal the caller cannot forge (wired to the quota subsystem
/// in `hq-context-custodian.2`; `false` until then).
///
/// - **StopTheLine** (no context, capacity exists) → reject with the violations,
///   leaving the status untouched.
/// - **Pass** (context present) → record it as a note *before* the flip, the same
///   breadcrumb-before-write order `run_close_issue` uses for the sha.
/// - **Deferred** (no context, quota confirmed blocked) → flip, then stamp a
///   `context-deferred: <reason>` debt note (no timestamp — the row's
///   `updated_at`/Dolt commit already date it).
///
/// `open`/`closed` moves carry no context contract and pass straight through.
pub async fn run_transition_issue(
    issues: &DoltIssues,
    args: &TransitionIssue,
    quota_blocked: bool,
    validate_only: bool,
) -> Result<(), AppError> {
    args.validate()?;
    if validate_only {
        return Ok(());
    }
    // Custodian: gate the `working` claim on context (or a quota-governed deferral).
    // Runs before any state change so a StopTheLine never mutates the bead.
    let context = args.context.as_deref().map(str::trim).filter(|c| !c.is_empty());
    match guard_claim_context(args.target_status(), args.context.as_deref(), quota_blocked) {
        PolicyVerdict::StopTheLine(violations) => {
            return Err(AppError::Validation(format_violations(&violations)));
        }
        PolicyVerdict::Pass => {
            // Record the breadcrumb BEFORE the flip (mirrors the sha note in close),
            // so even a subsequently-rejected transition leaves the context trail.
            if let Some(ctx) = context {
                issues
                    .append_notes(&args.id, &format!("working context: {ctx}"))
                    .await?;
            }
        }
        PolicyVerdict::Deferred(reason) => {
            // No timestamp in the note: `append_notes` stamps `updated_at = NOW()`
            // and each append is its own Dolt commit, so an inline `@ {ts}` only
            // adds redundant noise to the planning-digest Notas column.
            issues
                .append_notes(&args.id, &format!("context-deferred: {reason}"))
                .await?;
        }
    }
    issues.transition(&args.id, args.target_status()).await
}

/// Render a custodian stop-the-line into one `Validation` message: the detail of
/// each violation, newline-joined, so the agent sees exactly why the claim was
/// blocked.
fn format_violations(violations: &[Violation]) -> String {
    violations
        .iter()
        .map(|v| v.detail.clone())
        .collect::<Vec<_>>()
        .join("; ")
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
/// `inspector` (no repo wired) verification is skipped but `delivered_sha` is
/// still populated with the caller-supplied sha for code-surface beads, so
/// consumers never see a NULL sha for beads that declare a surface and provide one.
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
    let sha = args.sha();

    // The bead's non-planned surface decides whether a `commit_sha` is mandatory:
    // a code surface still demands delivered-code proof, but a metadata/process
    // bead (no non-planned surface) closes without one — agents no longer have to
    // fabricate a sha for pure-tracking work (hq-gap-issues-close-for-metadata-only-beads).
    // Loaded once and reused for the S2 delivery check below. A missing row is
    // deferred to the `close` call's existence check, as before.
    let non_planned: Vec<String> = match issues.get_detail(&args.id).await? {
        Some(detail) => parse_surface_json(&detail.surface_json)
            .into_iter()
            .filter(|e| !e.planned)
            .map(|e| e.path)
            .collect(),
        None => Vec::new(),
    };

    if !non_planned.is_empty() && sha.is_none() {
        return Err(AppError::Validation(format!(
            "issue {} declares a non-planned code surface; close requires commit_sha \
             (delivered-code proof). Omit commit_sha only for metadata/process beads \
             with no code surface.",
            args.id
        )));
    }

    // Report the requirement on the `validate` tool too — but stop before any
    // state change.
    if validate_only {
        return Ok(());
    }

    // S2 phase-2: confirm the sha delivered a non-planned surface before closing.
    // `delivered` carries the resolved full sha to stamp once the close lands.
    // Only runs for a code-bearing bead with a sha and a repo wired; a
    // metadata-only close skips it and leaves `delivered_sha` NULL.
    let mut delivered: Option<String> = None;
    if let (Some(inspector), Some(sha)) = (inspector, sha) {
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
    }

    // When no inspector is wired (no repo configured) S2 is skipped entirely.
    // Still stamp delivered_sha with the caller-supplied sha so code-surface
    // beads never leave it NULL regardless of whether git verification ran.
    // Metadata-only beads (non_planned empty) correctly keep delivered_sha NULL.
    if delivered.is_none() {
        if let Some(s) = sha {
            if !non_planned.is_empty() {
                delivered = Some(s.to_string());
            }
        }
    }

    let session = args.effective_session(actor);
    let note = match sha {
        Some(sha) => format!("closed @ {sha} (by {session})"),
        None => format!("closed (metadata-only, no commit) (by {session})"),
    };
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
