use serde::{Deserialize, Serialize};

use gt_events::EventKind;

/// Domain events for `gt-skills`. The log of these events is the source for rebuilding
/// [`crate::SkillState`] via `apply`.
///
/// Time always travels as `now_secs` (UTC epoch). The producer (the edge) reads it off
/// the clock; the core only consumes it. Scope mappings live at the resolver edge
/// (`hq-fe-skills.4`) — these events capture only what skills exist and which roles
/// have which enabled.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum SkillEvent {
    /// A new skill joined the catalog. `default_scopes` is the canonical scope set the
    /// resolver hands out to any role with the skill enabled; toggles in `.4` may layer
    /// per-role overrides on top.
    Registered {
        skill: String,
        label: String,
        description: String,
        default_scopes: Vec<String>,
        now_secs: u64,
    },
    /// A skill was retired (no longer available). Existing role bindings are dropped
    /// atomically by the reducer so the catalog stays consistent.
    Retired { skill: String, now_secs: u64 },
    /// A role gained access to a previously-registered skill.
    EnabledForRole {
        role: String,
        skill: String,
        now_secs: u64,
    },
    /// A role lost access to a previously-enabled skill.
    DisabledForRole {
        role: String,
        skill: String,
        now_secs: u64,
    },
}

impl EventKind for SkillEvent {
    fn kind(&self) -> &'static str {
        match self {
            SkillEvent::Registered { .. } => "skills.registered",
            SkillEvent::Retired { .. } => "skills.retired",
            SkillEvent::EnabledForRole { .. } => "skills.enabled_for_role",
            SkillEvent::DisabledForRole { .. } => "skills.disabled_for_role",
        }
    }
}
