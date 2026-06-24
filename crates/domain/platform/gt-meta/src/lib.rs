//! `gt-meta` — the cross-cutting `meta.*` MCP tools as a [`GtModule`]
//! (hq-core-host.7).
//!
//! Restores the meta surface that retired with the upstream gt-mcp:
//! - `meta.help.execute` — the server's `tools/list` payload (single-call
//!   discovery).
//! - `meta.report-gap.execute` — mint a `hq-gap-<slug>-<ts>` bead so a missing
//!   operation enters the routine catalog (the upstream `meta.report_gap`,
//!   renamed to kebab for the kernel's three-segment tool-name rule).
//!
//! Descriptor-only seam: this crate declares the tools + the [`ReportGap`] arg
//! schema; the server bin (`gt-mcp-server`) dispatches them — meta.help over the
//! built `Root`'s tool list, meta.report-gap over the Dolt issues store.

#![forbid(unsafe_code)]
#![warn(missing_docs)]

pub mod commands;
/// The off-by-default `axum` REST adapter (`hq-fe-api-platform.4`): maps `meta.*` to
/// REST routes that reuse the same gap-minting logic as the MCP tools.
#[cfg(feature = "axum")]
pub mod http;
/// The off-by-default `axum` REST adapter for the per-workspace domain catalog
/// (gtcore-b37400 H4): admin-scoped list/add/rename/set-enabled/remove routes that
/// reuse the same `DoltDomainCatalog` CRUD as the `domain.catalog.*` MCP tools.
#[cfg(feature = "axum")]
pub mod domain_http;
/// Transport-free handlers shared by the MCP path and the REST adapter
/// (`hq-fe-api-platform.4`). Gated with the adapter — the descriptor-only build
/// needs neither the handler nor its store.
#[cfg(feature = "axum")]
pub mod handlers;
mod module;

pub use commands::ReportGap;
#[cfg(feature = "axum")]
pub use http::{meta_router, ApiDoc, MetaApiState};
#[cfg(feature = "axum")]
pub use domain_http::{domain_router, DomainApiState};
#[cfg(feature = "axum")]
pub use module::{DomainCatalogHttpModule, MetaHttpModule};
pub use module::MetaModule;
