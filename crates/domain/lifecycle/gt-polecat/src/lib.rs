//! `gt-polecat` — polecat/crew/daemon lifecycle port (Paso 9.E, hq-63az).
//!
//! Where `gt-agent` *observes* one polecat's lifecycle (events + replay), this crate
//! *drives* it: it launches real processes, watches their heartbeats, restarts them with
//! backoff, and pins the hooked bead into their environment. It is the Rust port of the Go
//! `internal/polecat` + `internal/daemon` + `internal/tmux` + hooks stack.
//!
//! Layering mirrors the rest of the kernel: the pure pieces ([`restart`], [`hooks`]) are
//! synchronous and unit-testable with no I/O; the async/edge pieces ([`lifecycle`],
//! [`supervisor`], [`tmux`]) live at the boundary and emit [`gt_agent::AgentEvent`] so the
//! existing session projection/replay reducer stays the single source of truth.

pub mod hooks;
pub mod lifecycle;
pub mod module;
pub mod restart;
pub mod supervisor;
pub mod tmux;

pub use hooks::{hook_bead_for_spawn, hook_env, resolve_env_hook_bead, GT_HOOK_BEAD};
pub use module::SessionsModule;
pub use lifecycle::{
    spawn_process, spawn_tmux, PolecatLifecycle, SpawnSpec, SpawnTemplate, SpawnedPolecat,
};
pub use restart::{RestartConfig, RestartTracker};
pub use supervisor::{
    supervise_daemon, supervise_polecat, watch, PolecatSupervisor, RespawnPolicy, WatchOutcome,
};
pub use tmux::{tmux_server_name, FakeTmux, Tmux, TmuxCli};
