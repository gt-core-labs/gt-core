//! `FeedModule` — the [`GtModule`] wrapper over the activity feed (`hq-mod-refactor.8`).
//!
//! Like [`RigsModule`](../../gt-rig/src/module.rs) before it, this routes the feed's
//! *existing* contribution through the kernel's module seam **without changing any
//! domain logic**. The projection still lives in [`crate::Curator`] / [`crate::FeedState`]
//! and is folded in the `gt` bin's event loop straight from the type-erased log; this
//! file only *declares* what `gt-feed` already offers so the `RootBuilder` can harvest it
//! instead of the composition root hand-wiring scopes (`docs/03` rule 3).
//!
//! What it declares:
//!
//! - **Identity** ([`GtModule::meta`]) — id `feed`, semver, description.
//! - **Capability** ([`GtModule::capability`]) — the single `feed.read` scope the module
//!   owns (the read guard on `GET /api/feed` + `GET /api/stream`).
//!
//! ## Why this wrap is thinner than the rig wrap (no logic change)
//!
//! `gt-feed` is a **pure consumer** of the event log (`lib.rs`): it folds type-erased
//! `EventRecord`s into a `FeedState` and emits nothing of its own. So unlike a write-model
//! (rig/merge/quota), it declares:
//!
//! - **no emitted event kinds** — the feed produces no events, it only reads them. The log
//!   it consumes is not a module [`HookPoint`](gt_module::HookPoint) subscription either
//!   (the curator folds the audit log directly in the bin loop), so there is nothing to
//!   declare under `subscribing` — that hook seam is for lifecycle points, not log replay.
//! - **no MCP tools** — the feed has no commands; it is a read-only projection.
//! - **no migrations** — the projection is in-memory, rebuilt by replay (`lib.rs` rules);
//!   it owns no table.
//! - **no routes here** — the `feed.read`-guarded `/api/feed` + `/api/stream` handlers are
//!   still generic over `gt-web`'s application state (they read the shared `events.jsonl`
//!   and the running root's broadcast, not `FeedState` directly). Detangling them from that
//!   state into [`register_routes`](GtModule::register_routes) is `hq-mod-routes.5`'s job
//!   (drop the hand-wired routes, harvest via `RootBuilder`); moving them here would change
//!   logic, which this faithful wrap must not. The empty-router default stands until then —
//!   the `feed.read` scope this module declares is what that guard already names.

#[cfg(feature = "axum")]
use gt_module::McpRegistry;
use gt_module::{Capability, GtModule, ModuleId, ModuleMeta, Scope};
use semver::Version;

/// The [`GtModule`] facade over the activity feed. Zero-sized: the projection lives in the
/// `FeedState` folded by the bin's event loop, so the unit struct is all the composition
/// root registers.
#[derive(Clone, Copy, Debug, Default)]
pub struct FeedModule;

impl FeedModule {
    /// The module's stable id (`feed`). Matches the existing `feed.read` scope resource and
    /// the `/api/feed` route name, so the wrap introduces zero rename/drift. The literal is a
    /// known-valid slug.
    pub fn id() -> ModuleId {
        ModuleId::new("feed").expect("`feed` is a valid module id")
    }

    /// Build the HTTP-enabled feed module (`hq-web-extras.14`), baking `state` (the per-workspace
    /// projection provider) into the router its [`register_routes`](GtModule::register_routes)
    /// returns. The binary calls this to opt the module into its read-only REST surface; the MCP
    /// harvest path keeps the plain unit [`FeedModule`]. Returns a [`FeedHttpModule`] rather than
    /// mutating `FeedModule` so the unit struct (and its existing call sites) is left untouched.
    #[cfg(feature = "axum")]
    pub fn with_http(state: crate::http::FeedApiState) -> FeedHttpModule {
        FeedHttpModule { http: state }
    }
}

/// The HTTP-enabled feed module (`hq-web-extras.14`): the same `GtModule` contract as
/// [`FeedModule`] plus the read-only `feed.read` REST route + OpenAPI spec.
///
/// Built by [`FeedModule::with_http`]. Identity, capability, and MCP tools delegate verbatim to
/// [`FeedModule`] (one source of truth for the contract); only
/// [`register_routes`](GtModule::register_routes) and [`openapi`](GtModule::openapi) are
/// overridden, carrying the per-workspace [`WorkspaceFeed`](crate::http::WorkspaceFeed) provider
/// the handler reads through.
#[cfg(feature = "axum")]
#[derive(Clone)]
pub struct FeedHttpModule {
    /// The per-workspace REST state the route reads through.
    http: crate::http::FeedApiState,
}

#[cfg(feature = "axum")]
impl GtModule for FeedHttpModule {
    fn meta(&self) -> ModuleMeta {
        FeedModule.meta()
    }

    fn capability(&self) -> Capability {
        FeedModule.capability()
    }

    fn register_mcp_tools(&self, registry: &mut McpRegistry) {
        FeedModule.register_mcp_tools(registry);
    }

    /// The read-only feed REST route (`hq-web-extras.14`), relative — the builder nests it under
    /// `/api/v1/feed` and applies the `feed.read` scope guard (the single route is a GET).
    fn register_routes(&self) -> axum::Router {
        crate::http::feed_router(self.http.clone())
    }

    /// The OpenAPI spec for the feed REST route, so the combined document documents exactly the
    /// route mounted under the HTTP-enabled module.
    fn openapi(&self) -> Option<utoipa::openapi::OpenApi> {
        use utoipa::OpenApi;
        Some(crate::http::ApiDoc::openapi())
    }
}

impl GtModule for FeedModule {
    fn meta(&self) -> ModuleMeta {
        ModuleMeta::new(
            Self::id(),
            "Feed",
            Version::new(1, 0, 0),
            "Activity feed as a pure projection of the event log — a type-erased consumer that \
             folds every record into a correlation-grouped view and derives semantic gaps. \
             Read-only (the `feed.read` surface); it emits no events and owns no schema.",
        )
    }

    fn capability(&self) -> Capability {
        // The single `feed.read` scope the module owns — the read guard `gt-web` already
        // names on `GET /api/feed` + `GET /api/stream`. No `emitting`: the feed is a pure
        // consumer (see the module-level note).
        Capability::empty().claiming_all([Scope::new("feed.read").expect("valid scope")])
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use gt_module::{EventKind, McpRegistry};

    #[test]
    fn meta_id_is_feed_singular() {
        let meta = FeedModule.meta();
        assert_eq!(meta.id.as_str(), "feed");
        assert_eq!(meta.id, FeedModule::id());
        assert_eq!(meta.version, Version::new(1, 0, 0));
    }

    #[test]
    fn capability_claims_only_feed_read() {
        let cap = FeedModule.capability();
        let scopes: Vec<&str> = cap.scopes().iter().map(Scope::as_str).collect();
        assert_eq!(scopes, ["feed.read"]);
    }

    #[test]
    fn pure_consumer_emits_nothing() {
        // The feed produces no events of its own — it only reads the log.
        let cap = FeedModule.capability();
        let kinds: Vec<&EventKind> = cap.emits().iter().collect();
        assert!(kinds.is_empty());
    }

    #[test]
    fn registers_no_mcp_tools() {
        // Read-only projection: no commands, so no tools.
        let mut reg = McpRegistry::new();
        FeedModule.register_mcp_tools(&mut reg);
        assert!(reg.tools().is_empty());
    }

    #[test]
    fn owns_no_migration() {
        // In-memory projection, rebuilt by replay — no table of its own.
        assert!(FeedModule.migrations().is_empty());
    }

    #[test]
    fn contributes_no_route_or_openapi_surface() {
        // The feed.read read surface is still gt-web-state-coupled; harvesting it is
        // hq-mod-routes.5. The default empty router + no OpenAPI stand here.
        assert!(FeedModule.openapi().is_none());
    }
}
