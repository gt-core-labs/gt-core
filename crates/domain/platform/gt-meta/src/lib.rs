//! `gt-meta` — the cross-cutting `meta.*` MCP tools as a [`GtModule`]
//! (hq-core-host.7).
//!
//! Restores the meta surface that retired with the gastown gt-mcp:
//! - `meta.help.execute` — the server's `tools/list` payload (single-call
//!   discovery).
//! - `meta.report-gap.execute` — mint a `hq-gap-<slug>-<ts>` bead so a missing
//!   operation enters the routine catalog (the gastown `meta.report_gap`,
//!   renamed to kebab for the kernel's three-segment tool-name rule).
//!
//! Descriptor-only seam: this crate declares the tools + the [`ReportGap`] arg
//! schema; the server bin (`gt-mcp-server`) dispatches them — meta.help over the
//! built `Root`'s tool list, meta.report-gap over the Dolt issues store.

#![forbid(unsafe_code)]
#![warn(missing_docs)]

pub mod commands;
mod module;

pub use commands::ReportGap;
pub use module::MetaModule;
