//! REST-surface backings for the domains whose `register_routes` adapter (hq-fe-api)
//! needs a composition-tier provider (hq-fe-api-mount.1).
//!
//! The per-domain REST `ApiState`s (`gt_rig::RigApiState`, `gt_quota::QuotaApiState`)
//! own no store of their own — they delegate to a `WorkspaceRigs` / `WorkspaceQuota`
//! provider the binary supplies, exactly as the MCP handlers delegate to the shared
//! [`WsPools`] cache / [`EventLog`]. These providers are the REST mirrors of
//! [`RigHandler`](super::rig::RigHandler) and [`QuotaHandler`](super::quota::QuotaHandler):
//! they resolve the *same* per-workspace state, so a `rig.add` over MCP and a
//! `POST /api/v1/rig` see one Postgres schema, and a `quota.sample` over either edge
//! folds into one event log.
//!
//! Only `modules` may know both the composition-tier handles ([`WsPools`], [`EventLog`])
//! and the domain crates (docs/03 Rule 4), so the providers live here, not in the domain
//! crate (whose REST trait is deliberately store-agnostic).
//!
//! The two `AppError` types are distinct nominal enums with identical variants — the
//! domain-core [`gt_events::AppError`] the REST traits speak, and the
//! [`gt_store_dolt::AppError`] the [`WsPools`]/[`EventLog`] edges return — so [`lift`]
//! maps one to the other at the boundary.

use std::sync::Arc;

use async_trait::async_trait;
use gt_events::AppError;

use gt_quota::{AccountRegistry, QuotaEvent, QuotaState, WorkspaceQuota};
use gt_rig::{DynRigRepository, PgRigs, WorkspaceRigs};

use super::eventlog::EventLog;
use super::pools::WsPools;

/// The event-log kind prefix every quota event carries (`quota.*.v1`); matches
/// [`QuotaHandler`](super::quota::QuotaHandler)'s replay filter so both edges fold
/// the identical stream.
const QUOTA_NS: &str = "quota.";

/// Map the edge error ([`gt_store_dolt::AppError`]) onto the domain-core error
/// ([`gt_events::AppError`]) the REST ports return. The two enums are variant-for-variant
/// identical (the dolt one is a behaviour-preserving port of the core one), so this is a
/// total 1:1 relabel, never a lossy collapse.
fn lift(e: gt_store_dolt::AppError) -> AppError {
    use gt_store_dolt::AppError as D;
    match e {
        D::InvalidTransition(s) => AppError::InvalidTransition(s),
        D::NotFound(s) => AppError::NotFound(s),
        D::Validation(s) => AppError::Validation(s),
        D::Handler(s) => AppError::Handler(s),
        D::Other(s) => AppError::Other(s),
    }
}

/// REST backing for `rig.*` (`gt_rig::WorkspaceRigs`): resolves the caller's workspace
/// schema through the shared [`WsPools`] cache and hands back a [`PgRigs`] over it — the
/// same pool + adapter [`RigHandler`](super::rig::RigHandler) dispatches through.
pub struct WsPoolRigs {
    pools: Arc<WsPools>,
}

impl WsPoolRigs {
    /// Wrap the per-workspace pool cache (the binary's single shared instance).
    pub fn new(pools: Arc<WsPools>) -> Self {
        Self { pools }
    }
}

#[async_trait]
impl WorkspaceRigs for WsPoolRigs {
    async fn repo(&self, workspace: &str) -> Result<Box<dyn DynRigRepository>, AppError> {
        let pool = self.pools.get(Some(workspace)).await.map_err(lift)?;
        Ok(Box::new(PgRigs::new(pool.pool().clone())))
    }
}

/// REST backing for `quota.*` (`gt_quota::WorkspaceQuota`): rehydrates the account
/// registry from the caller's workspace event log and appends decided events back — the
/// same read-modify-append [`QuotaHandler`](super::quota::QuotaHandler) runs over the
/// shared [`EventLog`].
pub struct EventLogQuota {
    log: Arc<EventLog>,
}

impl EventLogQuota {
    /// Wrap the per-workspace event log (the binary's single shared instance).
    pub fn new(log: Arc<EventLog>) -> Self {
        Self { log }
    }
}

#[async_trait]
impl WorkspaceQuota for EventLogQuota {
    async fn registry(&self, workspace: &str) -> Result<AccountRegistry, AppError> {
        let state = self
            .log
            .replay_domain(Some(workspace), QUOTA_NS, QuotaState::default(), QuotaState::apply)
            .map_err(lift)?;
        Ok(AccountRegistry::from_state(&state))
    }

    async fn append(&self, workspace: &str, event: QuotaEvent) -> Result<(), AppError> {
        self.log.append(Some(workspace), event).map_err(lift)
    }
}
