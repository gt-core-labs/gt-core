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
mod module;

pub use commands::{AttachDoc, ListDocs, RemoveDoc, SearchDocs, UpdateDoc, ValidationError};
pub use module::DocumentsModule;
