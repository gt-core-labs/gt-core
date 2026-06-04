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

/// The [`GtModule`] facade. Zero-sized: the live work happens in the injected
/// `DocumentsRepository` / blob store / extractor, so the unit struct is all the
/// composition root registers.
#[derive(Clone, Copy, Debug, Default)]
pub struct DocumentsModule;

impl DocumentsModule {
    /// The module's stable id (`documents`). Matches the `documents.*` tool + scope
    /// namespaces (zero drift).
    pub fn id() -> ModuleId {
        ModuleId::new("documents").expect("`documents` is a valid module id")
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
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn meta_identity_is_documents() {
        let m = DocumentsModule.meta();
        assert_eq!(m.id.as_str(), "documents");
        assert_eq!(m.version, Version::new(1, 0, 0));
    }

    #[test]
    fn capability_owns_read_write_scopes_no_events() {
        let cap = DocumentsModule.capability();
        let scopes: Vec<&str> = cap.scopes().iter().map(Scope::as_str).collect();
        assert_eq!(scopes, ["documents.read", "documents.write"]);
        assert!(cap.emits().is_empty(), "the store is a direct projection, not event-sourced");
    }

    #[test]
    fn registers_ten_documents_tools_in_validate_execute_pairs() {
        let mut reg = McpRegistry::new();
        DocumentsModule.register_mcp_tools(&mut reg);
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
        assert!(DocumentsModule.migrations().is_empty());
    }
}
