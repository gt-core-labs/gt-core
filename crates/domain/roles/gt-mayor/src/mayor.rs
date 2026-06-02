//! Producer helpers for the Mayor role. Mayor is command-driven: edge callers (the
//! composition root's CLI / MCP frontier, future `gt mayor delegate` subcommands) push
//! [`MayorCommand`]s through the handle. The thin wrappers below let callers state intent
//! without re-constructing the command enum every time, mirroring `gt-sheriff::observe`.
//!
//! The real orchestration **loop** — periodically scanning convoys / queues for work to
//! delegate — is a follow-up: that producer wraps these helpers in a background task and
//! lives in the composition root once the cross-domain inputs (convoy snapshots, bead
//! priorities) are wired. Until then, the actor is exercised directly by tests and the
//! eventual CLI surface.

use gt_events::AppError;

use crate::actor::MayorHandle;
use crate::commands::{Acknowledge, Delegate, MayorCommand, Resolve, Withdraw};

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
