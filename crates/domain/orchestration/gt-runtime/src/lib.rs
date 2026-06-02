//! Per-workspace runtime composition for multi-tenant gt-core.
//!
//! One process serves many tenants. [`RootRegistry`] resolves a
//! [`WorkspaceId`](gt_workspace::WorkspaceId) to the composed application
//! [`RootHandle`] for that tenant, hydrating it lazily on first access and
//! dropping it when idle. This is the seam every per-request entry point goes
//! through to pick the right tenant's modules, routes, and (later) actor stack.
//!
//! ## Why this is an orchestration-domain crate
//!
//! The registry must name both a [`WorkspaceId`](gt_workspace::WorkspaceId)
//! (the `gt-workspace` platform crate) and a [`Root`](gt_module::Root) (the
//! `gt-module` kernel crate), and a hydrated handle will eventually own running
//! actor tasks. The kernel may not depend on a domain crate, and kernel crates
//! may not `tokio::spawn`, so neither piece can live in `gt-module`. The
//! `domain/orchestration` tier is the lowest one allowed to depend on **both**
//! kernel and `domain/platform` (docs/03 dependency table) and is where runtime
//! coordination belongs — so it is the natural and only correct home. Resolves
//! gap `gt-core.runtime.root-registry-host-tier`.
//!
//! ## Scope (`hq-mt-routing.1`)
//!
//! The registry + a thin handle. Per-workspace event-log hydration (`.3`) and
//! idle teardown policy (`.4`) layer on top without changing these types' shape.
//! The actor lifecycle a handle owns ([`Supervisor`], `hq-mt-routing.2`) is the
//! seam idle teardown drains and the hydrate closure registers actors against.

#![forbid(unsafe_code)]

mod dispatch;
mod handle;
mod idle;
mod lifecycle;
mod registry;

pub use dispatch::{plan, BeadId, DispatchError, Dispatcher, ReadySource, Worker};
pub use handle::RootHandle;
pub use idle::{idle_from_env, DEFAULT_IDLE_SECS};
pub use lifecycle::{Phase, Supervisor};
pub use registry::RootRegistry;
