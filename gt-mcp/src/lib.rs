//! `gt-mcp` — client + MCP proxy for a gt-core [`gt-mcp-server`].
//!
//! This crate isolates everything that talks to the server, so the `gt` CLI can stay
//! pure UX/config and depend on this as a published crate:
//!
//! - [`Client`] — the REST surface `gt init` drives: `POST /auth/login`, the
//!   `/auth/refresh` token rotation, and the `GET /api/v1/workspace` + `/api/v1/rig`
//!   catalogs the wizard offers.
//! - [`proxy::run`] — the stdio↔HTTP MCP bridge `gt mcp` runs: it serves a stdio MCP
//!   server and forwards every request to the remote `/mcp` streamable-HTTP transport,
//!   injecting a bearer token and an `X-Workspace` header.

pub mod client;
pub mod proxy;

pub use client::{Client, Rig, Tokens, Workspace};
