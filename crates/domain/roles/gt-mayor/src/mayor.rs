//! Producer helpers for the Mayor role. Mayor is command-driven: edge callers (the
//! composition root's CLI / MCP frontier, future `gt mayor delegate` subcommands) push
//! [`MayorCommand`]s through the handle. The thin wrappers below let callers state intent
//! without re-constructing the command enum every time, mirroring `gt-sheriff::observe`.
//!
//! [`delegate_frontier`] is the orchestration **loop**'s per-wake step: given the frontier the
//! orchd hands the mayor and the free polecat capacity, it runs the pure [`triage`] and records
//! a `Delegate` for each chosen bead (one bead → one polecat), returning the [`TriagePlan`] so
//! the caller acts on the `queue` / `decompose` buckets and reports what it dispatched. The
//! background task that drives this on each wake — and maps the opaque `polecat` target to the
//! real dispatch (`a2a.delegate` intake / `agent.write` spawn) — lives at the composition root.

use gt_events::AppError;

use crate::actor::MayorHandle;
use crate::commands::{Acknowledge, Delegate, MayorCommand, Resolve, Withdraw};
use crate::triage::{triage, FrontierBead, TriagePlan};

/// The role every mayor delegation hands off to: one bead → one polecat. Opaque `target`
/// string on the `Delegate` command; the composition root maps it to the actual dispatch
/// (in-process `a2a.delegate` intake or `agent.write` spawn).
pub const POLECAT_TARGET: &str = "polecat";

/// Run one wake of the orchestration loop's decision step over `frontier` with `capacity`
/// free polecat slots, and record a `Delegate` for each bead the [`triage`] chose to dispatch
/// (target = [`POLECAT_TARGET`], "one bead → one polecat"). Returns the full [`TriagePlan`] so
/// the caller can act on `queue` (leave for the next wake) and `decompose` (`issues.create`)
/// and report what it dispatched.
///
/// Idempotent against an already-recorded delegation: a `Delegate` whose id is already on the
/// board is rejected by the actor (`already exists`); that error is swallowed so a re-wake
/// over the same frontier does not abort the batch. Any other error propagates.
pub async fn delegate_frontier(
    handle: &MayorHandle,
    frontier: &[FrontierBead],
    capacity: usize,
) -> Result<TriagePlan, AppError> {
    let plan = triage(frontier, capacity);
    for planned in &plan.delegate {
        match delegate(handle, &planned.id, POLECAT_TARGET, &planned.summary).await {
            Ok(()) => {}
            // Re-delegating an existing id is a no-op on a re-wake, not a failure.
            Err(AppError::Validation(msg)) if msg.contains("already exists") => {}
            Err(e) => return Err(e),
        }
    }
    Ok(plan)
}

/// Delegate work to `target` with a free-form `summary`.
pub async fn delegate(
    handle: &MayorHandle,
    id: impl Into<String>,
    target: impl Into<String>,
    summary: impl Into<String>,
) -> Result<(), AppError> {
    handle
        .exec(MayorCommand::Delegate(Delegate {
            id: id.into(),
            target: target.into(),
            summary: summary.into(),
        }))
        .await
}

/// Mark a pending delegation acknowledged.
pub async fn acknowledge(handle: &MayorHandle, id: impl Into<String>) -> Result<(), AppError> {
    handle
        .exec(MayorCommand::Acknowledge(Acknowledge { id: id.into() }))
        .await
}

/// Resolve an acknowledged delegation with an `outcome`.
pub async fn resolve(
    handle: &MayorHandle,
    id: impl Into<String>,
    outcome: impl Into<String>,
) -> Result<(), AppError> {
    handle
        .exec(MayorCommand::Resolve(Resolve {
            id: id.into(),
            outcome: outcome.into(),
        }))
        .await
}

/// Withdraw a still-pending delegation.
pub async fn withdraw(handle: &MayorHandle, id: impl Into<String>) -> Result<(), AppError> {
    handle
        .exec(MayorCommand::Withdraw(Withdraw { id: id.into() }))
        .await
}
