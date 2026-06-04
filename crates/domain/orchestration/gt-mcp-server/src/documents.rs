//! The document-reader seam for resource reads (hq-docs-api.3, docs/11).
//!
//! The `gt://issue/{id}` resource inlines a bead's attached documents, and `gt://doc/{id}`
//! resolves a single one, so a model reading an epic gets its supporting `.md`/extracted
//! text in the same call. The documents live in the per-workspace Postgres store
//! (gt-store-pg), which the orchestration tier must not depend on directly; this trait is
//! the inversion seam the composition root implements over `PgDocuments` + the per-workspace
//! pool cache.
//!
//! Payloads are plain [`serde_json::Value`] so the server stays free of the document DTO
//! types. Wired via [`with_documents`](crate::IssuesServer::with_documents); unset ⇒ the
//! resource surface is issues-only.

use async_trait::async_trait;
use gt_store_dolt::AppError;
use serde_json::Value;

/// Reader the server consults to inline documents into resource reads. `workspace` is the
/// resolved tenant slug (`None` = default), matching the multi-tenant store resolution the
/// rest of the server uses.
#[async_trait]
pub trait DocumentsResource: Send + Sync {
    /// One document by id, already JSON-shaped, or `None` when absent in this tenant.
    async fn get(&self, workspace: Option<&str>, id: &str) -> Result<Option<Value>, AppError>;

    /// The live documents attached to `owner_id` (any owner_type), newest first, JSON-shaped.
    /// Backs the `gt://issue/{id}` inline.
    async fn list_for_owner(
        &self,
        workspace: Option<&str>,
        owner_id: &str,
    ) -> Result<Vec<Value>, AppError>;
}
