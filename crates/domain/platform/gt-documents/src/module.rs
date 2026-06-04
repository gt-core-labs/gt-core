//! `DocumentsModule` — the [`GtModule`] facade over the document subsystem (hq-docs-api.1).
//!
//! Declares the module identity, its RBAC scopes, and the ten `documents.*` MCP tools
//! (`{attach,update,remove,list,search}.{validate,execute}`) with the JSON input schema
//! generated from each arg struct. The per-workspace schema is applied by the catalog
//! runner via `gt_store_pg::docs_migrations` (transitional, like the `workspace`/`feature`
//! tables), so [`GtModule::migrations`] stays empty here for now.

use gt_module::{Capability, GtModule, McpRegistry, ModuleId, ModuleMeta, Scope};
use gt_module_mcp::schema_for;
use semver::Version;

use crate::commands::{AttachDoc, ListDocs, RemoveDoc, SearchDocs, UpdateDoc};

/// The [`GtModule`] facade. Without the `axum` feature it is the zero-sized unit struct the
/// MCP harvest path registers: the live work happens in the injected `DocumentsRepository` /
/// blob store / extractor, so no runtime state is needed.
#[cfg(not(feature = "axum"))]
#[derive(Clone, Copy, Debug, Default)]
pub struct DocumentsModule;

/// The [`GtModule`] facade. Under the `axum` feature it can additionally carry the REST state
/// (`hq-fe-api-platform.3`): the binary builds it with [`with_http`](DocumentsModule::with_http)
/// so [`register_routes`](GtModule::register_routes) returns the `documents.*` router. The
/// default-constructed module carries no HTTP state, keeping the empty-router / no-spec defaults
/// the MCP-only registration path uses (the trait takes `&self`, so an implementor that needs
/// runtime handles need not be zero-sized).
#[cfg(feature = "axum")]
#[derive(Clone, Default)]
pub struct DocumentsModule {
    /// The REST state, present only when the binary opts the module into its HTTP surface via
    /// [`with_http`](DocumentsModule::with_http). `None` (the default) keeps the empty-router
    /// default.
    http: Option<crate::http::DocumentsApiState>,
}

impl DocumentsModule {
    /// The module's stable id (`documents`). Matches the `documents.*` tool + scope
    /// namespaces (zero drift).
    pub fn id() -> ModuleId {
        ModuleId::new("documents").expect("`documents` is a valid module id")
    }

    /// Build a module that also serves the REST surface (`hq-fe-api-platform.3`), baking `state`
    /// (the live stores + extractor + embedder) into the router
    /// [`register_routes`](GtModule::register_routes) returns. The binary calls this; the MCP
    /// harvest path uses [`default`](Self::default).
    #[cfg(feature = "axum")]
    pub fn with_http(state: crate::http::DocumentsApiState) -> Self {
        Self { http: Some(state) }
    }
}

impl GtModule for DocumentsModule {
    fn meta(&self) -> ModuleMeta {
        ModuleMeta::new(
            Self::id(),
            "Documents",
            Version::new(1, 0, 0),
            "Per-workspace document store: .md content + binary attachments (PDF/Office/image) \
             a model reads as context when resolving a bead. Backed by Postgres (text + \
             metadata) and an S3-compatible object store (binary bytes).",
        )
    }

    fn capability(&self) -> Capability {
        // read = list/get/search; write = attach/update/remove. The store is a direct
        // projection (not event-sourced), so no event kinds are emitted.
        Capability::empty().claiming_all([
            Scope::new("documents.read").expect("valid scope"),
            Scope::new("documents.write").expect("valid scope"),
        ])
    }

    fn register_mcp_tools(&self, registry: &mut McpRegistry) {
        registry
            .tool_with_schema(
                "documents.attach.validate",
                "Validate the shape of a document attach (owner, kind=md|blob, content) \
                 without writing it.",
                schema_for::<AttachDoc>(),
            )
            .tool_with_schema(
                "documents.attach.execute",
                "Attach a document to a bead: store .md content (kind=md) or a binary \
                 (kind=blob) in the object store + extract its text, then record the row.",
                schema_for::<AttachDoc>(),
            )
            .tool_with_schema(
                "documents.update.validate",
                "Validate a document edit (filename/body_md) under an optimistic version guard.",
                schema_for::<UpdateDoc>(),
            )
            .tool_with_schema(
                "documents.update.execute",
                "Edit a document under its expected_version guard; bumps version and appends \
                 a version snapshot.",
                schema_for::<UpdateDoc>(),
            )
            .tool_with_schema(
                "documents.remove.validate",
                "Validate a document soft-delete under a version guard.",
                schema_for::<RemoveDoc>(),
            )
            .tool_with_schema(
                "documents.remove.execute",
                "Soft-delete a document (hides it; the blob is purged later by a reconciler).",
                schema_for::<RemoveDoc>(),
            )
            .tool_with_schema(
                "documents.list.validate",
                "Validate a list request for an owner's documents.",
                schema_for::<ListDocs>(),
            )
            .tool_with_schema(
                "documents.list.execute",
                "List the live documents attached to an owner bead (newest first).",
                schema_for::<ListDocs>(),
            )
            .tool_with_schema(
                "documents.search.validate",
                "Validate a full-text document search request.",
                schema_for::<SearchDocs>(),
            )
            .tool_with_schema(
                "documents.search.execute",
                "Phase-1 full-text search over document body_md/extracted_text, ranked by \
                 relevance; optionally narrowed to one owner.",
                schema_for::<SearchDocs>(),
            );
    }

    /// The documents REST routes (`hq-fe-api-platform.3`), relative — the builder nests them under
    /// `/api/v1/documents` and applies the `documents.read`/`documents.write` scope guard. Present
    /// only when the module was built with [`with_http`](DocumentsModule::with_http); otherwise the
    /// empty default (MCP-only registration), so the MCP harvest path mounts no HTTP surface.
    #[cfg(feature = "axum")]
    fn register_routes(&self) -> axum::Router {
        match &self.http {
            Some(state) => crate::http::documents_router(state.clone()),
            None => axum::Router::new(),
        }
    }

    /// The OpenAPI spec for the documents REST routes, contributed only when the module carries the
    /// HTTP state (so the combined document documents exactly the routes actually mounted).
    #[cfg(feature = "axum")]
    fn openapi(&self) -> Option<utoipa::openapi::OpenApi> {
        use utoipa::OpenApi;
        self.http.as_ref().map(|_| crate::http::ApiDoc::openapi())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn meta_identity_is_documents() {
        let m = DocumentsModule::default().meta();
        assert_eq!(m.id.as_str(), "documents");
        assert_eq!(m.version, Version::new(1, 0, 0));
    }

    #[test]
    fn capability_owns_read_write_scopes_no_events() {
        let cap = DocumentsModule::default().capability();
        let scopes: Vec<&str> = cap.scopes().iter().map(Scope::as_str).collect();
        assert_eq!(scopes, ["documents.read", "documents.write"]);
        assert!(cap.emits().is_empty(), "the store is a direct projection, not event-sourced");
    }

    #[test]
    fn registers_ten_documents_tools_in_validate_execute_pairs() {
        let mut reg = McpRegistry::new();
        DocumentsModule::default().register_mcp_tools(&mut reg);
        let names: Vec<&str> = reg.tools().iter().map(|t| t.name.as_str()).collect();
        assert_eq!(names.len(), 10, "five commands × validate/execute");
        for base in ["attach", "update", "remove", "list", "search"] {
            assert!(names.contains(&format!("documents.{base}.validate").as_str()));
            assert!(names.contains(&format!("documents.{base}.execute").as_str()));
        }
        // Every tool is owned by this module (name prefix == meta id).
        for t in reg.tools() {
            assert!(t.name.starts_with("documents."), "tool {} not namespaced", t.name);
        }
    }

    #[test]
    fn owns_no_migration_schema_applied_via_store_pg_catalog() {
        // The documents/document_versions tables are applied by the catalog runner from
        // gt_store_pg::docs_migrations (transitional, like workspace/feature). No module-owned
        // migration yet.
        assert!(DocumentsModule::default().migrations().is_empty());
    }

    /// Built with HTTP state, the module documents its REST routes and returns a non-empty
    /// OpenAPI spec the builder mounts under `/api/v1/documents`; the default module contributes
    /// neither (MCP-only). The router itself is exercised by the http module's own tests.
    #[cfg(feature = "axum")]
    #[test]
    fn with_http_contributes_routes_and_openapi() {
        use gt_docs_extract::Extractor;

        assert!(DocumentsModule::default().openapi().is_none());

        let state = crate::http::DocumentsApiState::new(
            "postgres://x@127.0.0.1:1/none",
            None,
            "test-bucket",
            Extractor::without_ocr(),
            None,
        );
        let m = DocumentsModule::with_http(state);
        assert!(m.openapi().is_some());
    }
}
