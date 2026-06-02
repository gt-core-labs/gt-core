//! `gt-skills` — Skills catalog + per-role bindings (`hq-fe-skills.1`).
//!
//! A **skill** is a named capability operators can toggle for a role. Enabling a skill
//! for a role expands what that role is allowed to do; `.4` will map the binding into
//! dynamic `gt-rbac` scope additions so the JWT body the gateway mints widens without
//! a config reload. This crate is the domain side only — it owns the catalog + the
//! per-role binding set + their event log; HTTP (`.2`), toggle commands (`.3`), and
//! scope resolution (`.4`) come on top.
//!
//! Shape mirrors `gt-rig` / `gt-quota` / `gt-merge`:
//!
//! - **Owned events** ([`SkillEvent`]) — `Serialize` enum, replay-safe.
//! - **Mutable state lives inside one actor** ([`actor::spawn`]); everyone else asks
//!   for snapshots over a channel.
//! - **Pure replay reducer** ([`SkillState`]): time enters as `now_secs` data so the
//!   rebuilt state matches the live one byte-for-byte.
//! - **Inverted repository** ([`SkillsRepository`]): the domain defines the port;
//!   `gt-store-pg` / `gt-store-dolt` will implement it later. [`InMemorySkills`] is
//!   the test safety net.

pub mod actor;
pub mod commands;
pub mod repo;

mod events;
mod state;

pub use actor::{spawn, spawn_hydrated, SkillHandle, SkillMsg};
pub use commands::{
    DisableSkillForRole, EnableSkillForRole, RegisterSkill, RetireSkill, SkillCommand,
};
pub use events::SkillEvent;
pub use repo::{InMemorySkills, SkillsRepository};
pub use state::{
    validate_role_name, validate_skill_id, RoleBinding, Skill, SkillCatalog, SkillState,
    MAX_SKILL_ID_LEN,
};
