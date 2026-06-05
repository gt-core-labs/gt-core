//! `gt-documents` — the document subsystem as a [`GtModule`](gt_module::GtModule)
//! (hq-docs-api.1, docs/11).
//!
//! A document is supporting material (a `.md` body, or a binary attachment — PDF / Office /
//! image) attached to a bead (epic / skill / spec) so a model reading the bead has it as
//! context. This crate is the domain facade: it owns the `documents.*` tool contract
//! ([`commands`]) and the [`DocumentsModule`] the composition root registers. The data lives
//! in the per-workspace `DocumentsRepository` (gt-store-pg), the blob store (gt-store-blob),
//! and the extractor (gt-docs-extract); the transport-free execute handlers (hq-docs-api.2)
//! and the resource reads (hq-docs-api.3) build on this contract.

#![forbid(unsafe_code)]
#![warn(missing_docs)]

pub mod commands;
/// The off-by-default `axum` REST adapter (`hq-fe-api-platform.3`): the `documents.*` HTTP
/// surface, mapping the tools to REST routes that reuse the same stores as the MCP dispatch.
#[cfg(feature = "axum")]
pub mod http;
mod module;

pub use commands::{
    AttachDoc, CreateShare, ListDocs, PatchShare, RemoveDoc, SearchDocs, UpdateDoc,
    ValidationError,
};
#[cfg(feature = "axum")]
pub use http::{
    documents_router, public_openapi, public_share_router, ApiDoc, DocumentsApiState,
    PublicApiDoc,
};
pub use module::DocumentsModule;
