//! `VcsModule` — the [`GtModule`] wrapper over the VCS-connection store (hq-vcs-connections.1).
//!
//! Same faithful-wrap shape as [`gt_rig::RigsModule`]: it declares what this crate offers so the
//! `RootBuilder` can harvest it instead of the composition root hand-wiring scopes / routes /
//! migrations.
//!
//! What it declares:
//! - **Identity** ([`GtModule::meta`]) — id `connection` (so the routes mount at
//!   `/api/v1/connection` and the scope guard derives `connection.read` / `connection.write`).
//! - **Capability** ([`GtModule::capability`]) — the `connection.read` / `connection.write` scopes
//!   the module owns. It emits no events (the store is a plain PG table, not event-sourced).
//! - **Migrations** ([`GtModule::migrations`]) — the `public.vcs_connections` table.
//! - **HTTP routes + OpenAPI** (on the sibling [`VcsHttpModule`]) — under the off-by-default `axum`
//!   feature, the connection CRUD the builder mounts at `/api/v1/connection` behind the scope guard.
//!
//! There are NO MCP tools: the connection surface is REST-only (a workspace admin manages it from
//! the UI), so this wrap touches no MCP tool list and leaves `mcp_http_parity` undisturbed.

use gt_module::{Capability, GtModule, Migration, ModuleId, ModuleMeta, Scope};
use semver::Version;

/// The [`GtModule`] facade over the VCS-connection store. Zero-sized: the store lives behind the
/// REST state, so the unit struct is all the migration/scope harvest path registers. The HTTP
/// variant ([`VcsHttpModule`], via [`with_http`](Self::with_http)) carries the runtime handle.
#[derive(Clone, Copy, Debug, Default)]
pub struct VcsModule;

impl VcsModule {
    /// The module's stable id (`connection`). The literal is a known-valid slug. It is `connection`
    /// (not `vcs`) so the scope namespace the acceptance criteria name — `connection.read` /
    /// `connection.write` — equals the module id, the routes mount at `/api/v1/connection`, and the
    /// guard derives the right scopes (the builder ties prefix == scope resource to the id).
    pub fn id() -> ModuleId {
        ModuleId::new("connection").expect("`connection` is a valid module id")
    }

    /// Build the HTTP-enabled module, baking the REST `state` into the router its
    /// [`register_routes`](GtModule::register_routes) returns.
    #[cfg(feature = "axum")]
    pub fn with_http(state: crate::http::ConnectionApiState) -> VcsHttpModule {
        VcsHttpModule { http: state }
    }
}

/// The HTTP-enabled VCS module: the same `GtModule` contract as [`VcsModule`] plus the
/// `connection.*` REST routes + OpenAPI spec. Identity, capability, and migrations delegate to
/// [`VcsModule`]; only [`register_routes`](GtModule::register_routes) and
/// [`openapi`](GtModule::openapi) are overridden.
#[cfg(feature = "axum")]
#[derive(Clone)]
pub struct VcsHttpModule {
    http: crate::http::ConnectionApiState,
}

#[cfg(feature = "axum")]
impl GtModule for VcsHttpModule {
    fn meta(&self) -> ModuleMeta {
        VcsModule.meta()
    }

    fn capability(&self) -> Capability {
        VcsModule.capability()
    }

    fn migrations(&self) -> Vec<Migration> {
        VcsModule.migrations()
    }

    fn register_routes(&self) -> axum::Router {
        crate::http::connection_router(self.http.clone())
    }

    fn openapi(&self) -> Option<utoipa::openapi::OpenApi> {
        use utoipa::OpenApi;
        Some(crate::http::ApiDoc::openapi())
    }
}

impl GtModule for VcsModule {
    fn meta(&self) -> ModuleMeta {
        ModuleMeta::new(
            Self::id(),
            "VCS Connections",
            Version::new(1, 0, 0),
            "Per-workspace VCS connections — register a GitHub App installation or a PAT \
             fallback so the server can clone the workspace's private repos.",
        )
    }

    fn capability(&self) -> Capability {
        // The `connection.read` / `connection.write` scopes the module owns (the same
        // `<resource>.<verb>` convention gt-rig / gt-quota follow). No emitted event kinds — the
        // store is a plain PG table, not event-sourced.
        Capability::empty().claiming_all([
            Scope::new("connection.read").expect("valid scope"),
            Scope::new("connection.write").expect("valid scope"),
        ])
    }

    fn migrations(&self) -> Vec<Migration> {
        // The `public.vcs_connections` table backing `VcsConnectionRepo`. The module owns its
        // schema (the SQL lives in `migrations/vcs/`).
        vec![Migration::new(
            1,
            "create_vcs_connections",
            crate::migrations::CREATE_VCS_CONNECTIONS,
        )]
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn meta_identity_is_connection() {
        let m = VcsModule.meta();
        assert_eq!(m.id.as_str(), "connection");
        assert_eq!(m.version, Version::new(1, 0, 0));
    }

    #[test]
    fn capability_owns_connection_scopes_and_emits_nothing() {
        let cap = VcsModule.capability();
        let scopes: Vec<&str> = cap.scopes().iter().map(Scope::as_str).collect();
        assert_eq!(scopes, ["connection.read", "connection.write"]);
        assert!(cap.emits().is_empty(), "the store is not event-sourced");
    }

    #[test]
    fn owns_the_vcs_connections_table_migration() {
        let migs = VcsModule.migrations();
        assert_eq!(migs.len(), 1);
        assert_eq!(migs[0].version, 1);
        assert_eq!(migs[0].name, "create_vcs_connections");
        assert!(migs[0]
            .sql
            .contains("CREATE TABLE IF NOT EXISTS public.vcs_connections"));
        // Mirrors oauth_providers: a `public` table with a `workspace_id` column, NOT schema-per-tenant.
        assert!(migs[0].sql.contains("workspace_id"));
    }
}
