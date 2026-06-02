//! `gt-plugin` — the **only** kernel crate where `dyn` + `#[async_trait]` are allowed.
//!
//! Two layers live here:
//!
//! 1. **Runtime observer plugins** ([`Plugin`], [`PluginRegistry`], [`spawn_plugin_relay`]).
//!    A heterogeneous set of watchdog / sheriff implementations subscribes to the root's
//!    [`tokio::sync::broadcast::Receiver<EventRecord>`] and reacts in a separate task.
//!    Plugins are strictly *observers*: they never publish into the domain bus, so
//!    `replay_gt` is byte-identical with or without them attached — the gate documented in
//!    `apps/api/docs/11-cutover-roadmap.md` (Paso 9.B). Errors from `on_event` land in
//!    [`PluginDeadLetter`] instead of breaking the fan-out, mirroring `gt-bus::DeadLetter`.
//!
//! 2. **Plugin discovery / sync / drift** ([`descriptor`], [`scanner`], [`sync`]). Port of
//!    the Go `internal/plugin/` package (`scanner.go`, `sync.go`, frontmatter types). These
//!    surface the `plugin.md` corpus the future Sheriff / Deacon roles (Paso 9.D, 9.E) will
//!    dispatch; they have no kernel side effects on their own.
//!
//! Replay-safety: the relay only reads from the broadcast and writes to its own dead-letter.
//! Per `docs/01-architecture.md` the rest of the kernel uses static dispatch (`gt-bus`,
//! domain actors over channels); `dyn Plugin` is confined to this crate by design.

pub mod descriptor;
mod plugin;
pub mod scanner;
pub mod sheriff;
pub mod sync;

pub use plugin::{
    spawn_plugin_relay, Plugin, PluginDeadLetter, PluginDeadLetterEntry, PluginRegistry,
};
pub use sheriff::SheriffPlugin;
