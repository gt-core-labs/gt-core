//! `gt-hooks` — a GLOBAL Claude Code hook registry (`hq-hooks`).
//!
//! A **hook** is a Claude Code `settings.json` hook entry — an `event` (e.g. `PreToolUse`), a tool
//! `matcher`, and a shell `command` — plus a [`HookTarget`] selector saying which sessions it
//! applies to (by `workspace` / `rig` / `role`; an empty dimension = "all"). Unlike `gt-skills`
//! (per-workspace, per-role bindings), this registry is **global**: one set of hooks, each deciding
//! its own scope, stored at the `None` event scope.
//!
//! The terminal materialises the hooks matching a launching session's `(workspace, rig, role)` into
//! `<workdir>/.claude/settings.json`, the sibling of the role's `.claude/skills/` + `CLAUDE.md`
//! (`hq-role-skills-term`). The seed registry carries only the portable safety guards
//! ([`safety_guard_hooks`]); everything else is operator-authored.
//!
//! Shape mirrors `gt-skills`:
//! - **Owned events** ([`HookEvent`]) — replay-safe.
//! - **Pure replay reducer** ([`HooksState`]) — time enters as `now_secs` data.
//! - **Sync `Command` path** ([`RegisterHook`] / [`RetireHook`]).
//! - **Inverted store** ([`HooksStore`]) — the domain defines the port; the composition root
//!   implements it over the global event log. [`InMemoryHooks`] is the test net.
//!
//! The REST surface lives in `gt-composition::hooks` (not here): the global router needs the
//! composition root's shared authenticator + global event log, exactly like `terminal.rs`.

pub mod commands;
pub mod presets;
pub mod repo;

mod events;
mod state;

pub use commands::{RegisterHook, RetireHook};
pub use events::HookEvent;
pub use presets::safety_guard_hooks;
pub use repo::{HooksStore, InMemoryHooks};
pub use state::{
    validate_event, validate_hook_id, HookDef, HookTarget, HooksRegistry, HooksState, EVENT_TYPES,
    MAX_HOOK_ID_LEN,
};
