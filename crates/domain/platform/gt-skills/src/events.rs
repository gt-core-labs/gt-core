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
        /// The skill's `SKILL.md` body (`hq-role-skills-term.1`). `#[serde(default)]` keeps pre-`.1`
        /// log entries replayable.
        #[serde(default)]
        body: String,
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
    /// A role's system prompt was set (`hq-role-skills-term.4`): the persona/instructions the
    /// terminal writes as `<workdir>/CLAUDE.md` so claude loads them when a session of that role
    /// opens. Empty `prompt` clears it.
    RolePromptSet {
        role: String,
        prompt: String,
        now_secs: u64,
    },
    /// A role's model config was set (`hq-role-model.1`): the model id + permission mode + effort
    /// level the terminal stamps onto the `claude` launch for a session of that role. An all-empty
    /// config clears it. `#[serde(default)]` on the binding keeps pre-`hq-role-model` logs replayable.
    RoleModelSet {
        role: String,
        /// The claude model id/alias (`claude --model <id>`); empty ⇒ account default.
        model: String,
        /// The permission mode (`claude --permission-mode <mode>`); empty ⇒ unset.
        permission_mode: String,
        /// The effort level (`claude --effort <level>`); empty ⇒ unset (the CLI's own default).
        effort: String,
        now_secs: u64,
    },
}

impl EventKind for SkillEvent {
    fn kind(&self) -> &'static str {
        match self {
            SkillEvent::Registered { .. } => "skills.registered.v1",
            SkillEvent::Retired { .. } => "skills.retired.v1",
            SkillEvent::EnabledForRole { .. } => "skills.enabled-for-role.v1",
            SkillEvent::DisabledForRole { .. } => "skills.disabled-for-role.v1",
            SkillEvent::RolePromptSet { .. } => "skills.role-prompt-set.v1",
            SkillEvent::RoleModelSet { .. } => "skills.role-model-set.v1",
        }
    }
}
