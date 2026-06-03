//! [`WorkspaceRigPrefixes`] — the port `issues.create` consults to reject a bead
//! whose id prefix is not routable in the caller's workspace (hq-mt-rigs.6).
//!
//! ## Why a port here, not a `gt-rig` dependency
//!
//! A bead id `<prefix>-<slug>` must be rejected at create time when `<prefix>` is
//! neither a registered rig prefix in the caller's workspace nor a reserved/infra
//! prefix. The rig catalog is `gt-rig` (a `domain/platform` crate); `gt-issues` is
//! also `domain/platform`, so a `gt-issues -> gt-rig` edge is a forbidden
//! platform→platform dependency (docs/03 Rule 4). The check therefore lives at the
//! dispatch layer — this `orchestration`-tier crate, which *may* depend on a
//! platform crate — behind this trait. The async rig lookup and the reserved-set
//! union are the adapter's job (`gt-composition`, which has the PG rig catalog), so
//! `gt-mcp-server` needs no `gt-rig` / Postgres dependency of its own.
//!
//! Resolution is per-workspace: the same `X-Workspace` slug that selects the
//! tenant's Dolt store selects the rig catalog the prefix is checked against, so a
//! prefix registered only in another tenant does not satisfy the check (no global
//! prefix uniqueness — hq-mt-rigs.6 AC).

use async_trait::async_trait;
use gt_store_dolt::AppError;

/// Resolver consulted by `issues.create` to decide whether a bead-id prefix is
/// permitted in a workspace.
///
/// Wired into [`IssuesServer`](crate::IssuesServer) by the composition root via
/// [`with_rig_prefixes`](crate::IssuesServer::with_rig_prefixes). When unset, the
/// server keeps the legacy accept-all behaviour, so single-tenant / no-Postgres
/// builds are unaffected.
#[async_trait]
pub trait WorkspaceRigPrefixes: Send + Sync {
    /// Whether `prefix` may begin a new bead id in workspace `ws`: it is either a
    /// registered rig prefix in that tenant, or a reserved/infra prefix (so the
    /// tracker's own reserved-prefix beads — e.g. `hq-*` — are never rejected). The
    /// reserved-set union lives in the implementation, which owns the `gt-rig`
    /// dependency.
    async fn is_allowed(&self, ws: &str, prefix: &str) -> Result<bool, AppError>;
}

/// The rig prefix of a bead id: the leading token before the first `-`.
///
/// `"pl-foo" -> "pl"`, `"hq-mod-hooks.9" -> "hq"`. An id with no `-` yields the
/// whole id (it is then checked as its own prefix). Matches how a rig prefix
/// namespaces a bead id; the rest of the id is the slug.
pub fn bead_prefix(id: &str) -> &str {
    id.split('-').next().unwrap_or(id)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn prefix_is_leading_token_before_first_dash() {
        assert_eq!(bead_prefix("pl-foo"), "pl");
        assert_eq!(bead_prefix("hq-mod-hooks.9"), "hq");
        assert_eq!(bead_prefix("hq-mt-rigs.6"), "hq");
    }

    #[test]
    fn id_without_dash_is_its_own_prefix() {
        assert_eq!(bead_prefix("solo"), "solo");
        assert_eq!(bead_prefix(""), "");
    }
}
