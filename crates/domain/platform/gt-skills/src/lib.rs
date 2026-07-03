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
/// Seed-vs-live drift detection (`gtcore-63bb20`): compare the embedded greenfield seed against the
/// live `skills.*` catalog so a stale embedded snapshot can never rot silently.
pub mod drift;
pub mod presets;
pub mod repo;

mod events;
/// The off-by-default `axum` REST adapter (`hq-web-extras.13`): maps `skills.*` reads to
/// REST routes the composition builder mounts at `/api/v1/skills` behind the `skills.read`
/// scope guard, with a utoipa OpenAPI spec. Compiled only under the `axum` feature.
#[cfg(feature = "axum")]
pub mod http;
pub mod module;
mod state;

pub use actor::{spawn, spawn_hydrated, SkillHandle, SkillMsg};
pub use commands::{
    DisableSkillForRole, EnableSkillForRole, RegisterSkill, RetireSkill, SetRoleModel,
    SetRolePrompt, SkillCommand, UpdateSkill, EFFORT_LEVELS, PERMISSION_MODES,
};
pub use drift::{compute_drift, seed_catalog, DriftReport, DriftStatus, RoleDrift, SkillDrift};
pub use events::SkillEvent;
#[cfg(feature = "axum")]
pub use http::{skills_router, SkillWriter, SkillsApiState, WorkspaceSkills};
#[cfg(feature = "axum")]
pub use module::SkillsHttpModule;
pub use module::SkillsModule;
pub use presets::agent_least_privilege_catalog;
#[cfg(feature = "axum")]
pub use presets::seed_workspace_if_empty;
pub use repo::{InMemorySkills, SkillsRepository};
pub use state::{
    validate_role_name, validate_skill_id, ModelConfig, RoleBinding, Skill, SkillCatalog,
    SkillState, MAX_SKILL_ID_LEN,
};
