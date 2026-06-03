//! [`WorkspaceStatusGate`] — the port `call_tool` consults to reject a mutating
//! tool call against a suspended / archived workspace (hq-mt-bootstrap.8).
//!
//! ## Why a port here, not a `gt-workspace` dependency
//!
//! bootstrap.2 delivered the suspend/archive *transitions*; this is their
//! *enforcement* half: once a tenant is [`Suspended`](GateStatus::Suspended) or
//! [`Archived`](GateStatus::Archived) it must stop accepting new mutations while
//! still serving reads (and `workspace.resume`, so it can recover). The workspace
//! catalog is `gt-workspace` (a `domain/platform` crate); this `orchestration`-tier
//! crate *may* depend on a platform crate, but mirrors the [`WorkspaceRigPrefixes`]
//! (crate::prefixes) seam: the lookup + any caching are the adapter's job
//! (`gt-composition`, which owns the PG catalog), so `gt-mcp-server` needs no
//! `gt-workspace` / Postgres dependency and the server never speaks a
//! `WorkspaceStatus` domain type — only this self-contained [`GateStatus`].
//!
//! Resolution is per-workspace: the same authoritative slug that selects the
//! tenant's store selects the catalog row whose status gates its mutations.

use async_trait::async_trait;
use gt_store_dolt::AppError;

/// A workspace's lifecycle state, as the gate sees it — the orchestration-tier
/// mirror of `gt_workspace::WorkspaceStatus` (the adapter maps one onto the other),
/// so the server depends on no platform-domain type.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum GateStatus {
    /// Live: routable and accepting mutations.
    Active,
    /// Reversibly disabled: reads + `workspace.resume` only, mutations rejected.
    Suspended,
    /// Terminal: retained for audit, mutations rejected.
    Archived,
}

impl GateStatus {
    /// Whether this status permits a *mutating* tool call. Only
    /// [`Active`](GateStatus::Active) does; suspended/archived tenants are
    /// read-only (plus the explicit `workspace.resume` recovery exception the
    /// server allows ahead of this check).
    pub fn allows_mutation(self) -> bool {
        matches!(self, GateStatus::Active)
    }

    /// The lowercase label for an error / audit message.
    pub fn label(self) -> &'static str {
        match self {
            GateStatus::Active => "active",
            GateStatus::Suspended => "suspended",
            GateStatus::Archived => "archived",
        }
    }
}

/// Resolver consulted by [`IssuesServer::call_tool`](crate::IssuesServer) to decide
/// whether a workspace currently accepts mutations.
///
/// Wired in by the composition root via
/// [`with_workspace_status`](crate::IssuesServer::with_workspace_status). When
/// unset, the server keeps the legacy accept-all behaviour, so single-tenant /
/// no-Postgres builds are unaffected.
#[async_trait]
pub trait WorkspaceStatusGate: Send + Sync {
    /// The [`GateStatus`] of workspace `ws`, or `None` when no such workspace is in
    /// the catalog. `None` is *not* a block — an unknown slug passes the gate (it is
    /// the legacy default tenant or a not-yet-provisioned id, neither of which this
    /// bead governs); only a known suspended/archived status rejects a mutation.
    /// Implementations may cache to keep this off the per-call hot path.
    async fn status(&self, ws: &str) -> Result<Option<GateStatus>, AppError>;
}
