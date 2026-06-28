//! Domain state + replay reducer.
//!
//! [`SkillCatalog`] is the mutable state the actor owns; [`SkillState`] is the version
//! rebuilt from the log for the Step 3 gate (deterministic replay): the live state
//! must match the rebuilt one byte-for-byte.

use std::collections::{BTreeMap, BTreeSet};

use serde::{Deserialize, Serialize};

use crate::events::SkillEvent;

/// Upper bound on the length of a skill id. Keeps a wedged config from minting an
/// unreadable bead label / SSE payload.
pub const MAX_SKILL_ID_LEN: usize = 64;

/// One catalog entry. `id` is the stable handle the resolver (`.4`) keys off;
/// `label` + `description` exist for the UI. `default_scopes` is the canonical set
/// every role with this skill enabled inherits (`.4` may layer per-role overrides
/// on top — out of scope for `.1`).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Skill {
    pub id: String,
    pub label: String,
    pub description: String,
    /// Scope identifiers (`<domain>.<action>`) the skill grants. Order-preserving
    /// `Vec` so the registered ordering survives serde roundtrips, matching the
    /// `gt-rbac::RoleSpec::scopes` convention (first listed = primary).
    pub default_scopes: Vec<String>,
    pub registered_at_secs: u64,
    /// The skill's `SKILL.md` body (`hq-role-skills-term.1`): the definition claude loads when the
    /// skill is materialised into a session's `.claude/skills/<id>/SKILL.md`. Empty when the skill
    /// was registered without one; `#[serde(default)]` keeps pre-`.1` catalog entries replayable.
    #[serde(default)]
    pub body: String,
    /// Optional grouping label for the UI (e.g. `"design"`, `"research"`). Empty when unset;
    /// `#[serde(default)]` keeps pre-group log entries replayable.
    #[serde(default)]
    pub group: String,
}

impl Skill {
    pub fn new(
        id: impl Into<String>,
        label: impl Into<String>,
        description: impl Into<String>,
        default_scopes: Vec<String>,
        registered_at_secs: u64,
    ) -> Self {
        Self {
            id: id.into(),
            label: label.into(),
            description: description.into(),
            default_scopes,
            registered_at_secs,
            body: String::new(),
            group: String::new(),
        }
    }

    /// Builder-style setter for the `SKILL.md` body (`hq-role-skills-term.1`).
    pub fn with_body(mut self, body: impl Into<String>) -> Self {
        self.body = body.into();
        self
    }

    /// Builder-style setter for the grouping label.
    pub fn with_group(mut self, group: impl Into<String>) -> Self {
        self.group = group.into();
        self
    }
}

/// Per-role model config (`hq-role-model.1`): the claude launch levers the terminal stamps onto a
/// session of this role. All-default (empty model + empty mode + empty effort) ⇒ "unset", and the
/// terminal launches the bare `claude` with the account default. Only levers that exist for the
/// interactive `claude` CLI live here: `--model`, `--permission-mode`, and `--effort` (print/SDK-only
/// knobs like `--max-turns` are intentionally absent — they would be inert).
#[derive(Debug, Default, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ModelConfig {
    /// The model id/alias (`claude --model <id>`); empty ⇒ account default.
    #[serde(default)]
    pub model: String,
    /// The permission mode (`claude --permission-mode <mode>`); empty ⇒ unset. Validated against the
    /// closed CLI set (`default`/`acceptEdits`/`plan`/`bypassPermissions`) at the command edge.
    #[serde(default)]
    pub permission_mode: String,
    /// The effort level (`claude --effort <level>`); empty ⇒ unset. Validated against the closed CLI
    /// set (`low`/`medium`/`high`/`xhigh`/`max`) at the command edge.
    #[serde(default)]
    pub effort: String,
}

impl ModelConfig {
    /// `true` when no lever is set — the role rides the account default, so the terminal launches a
    /// bare `claude`. The `role_model` accessor uses this to collapse an all-empty config to `None`.
    pub fn is_unset(&self) -> bool {
        self.model.trim().is_empty()
            && self.permission_mode.trim().is_empty()
            && self.effort.trim().is_empty()
    }
}

/// A role's claude permission model (`gtcore-d175ec`): the `settings.json` `permissions` block every
/// launch materialises for a session of that role. A first-class per-role catalog attribute — read
/// from the DB at launch like [`ModelConfig`], never hardcoded in the launch assembler. The default
/// lives in the seed/migration ([`default_role_permissions`] / [`SkillCatalog::role_permissions_migration`]),
/// the apparatus layer, so the operator can edit it.
#[derive(Debug, Default, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RolePermissions {
    /// claude `permissions.defaultMode` (e.g. `bypassPermissions`); empty ⇒ unset.
    #[serde(default)]
    pub default_mode: String,
    /// claude `permissions.deny` rules (e.g. `Write(**/memory/**.md)`).
    #[serde(default)]
    pub deny: Vec<String>,
}

impl RolePermissions {
    /// `true` when nothing is set — the role carries no permission block, so the launch writes none.
    /// The `role_permissions` accessor collapses an all-empty value to `None`.
    pub fn is_unset(&self) -> bool {
        self.default_mode.trim().is_empty() && self.deny.is_empty()
    }

    /// Render as the claude `settings.json` `permissions` object (`{ defaultMode, deny }`), omitting
    /// an empty `defaultMode`. The single shape every launch path writes.
    pub fn to_settings_json(&self) -> serde_json::Value {
        let mut obj = serde_json::Map::new();
        if !self.default_mode.trim().is_empty() {
            obj.insert(
                "defaultMode".into(),
                serde_json::Value::String(self.default_mode.clone()),
            );
        }
        obj.insert("deny".into(), serde_json::json!(self.deny));
        serde_json::Value::Object(obj)
    }
}

/// The apparatus DEFAULT permission model (`gtcore-d175ec`) — the seed value the operator can later
/// edit from agents. `bypassPermissions` so autonomous agents never stop at an interactive prompt,
/// plus the memory-guard `deny` (the declarative backstop to the PreToolUse guard: memories are
/// saved via the `memory.save` MCP tool, never by writing the `*/memory/*.md` corpus). This lives in
/// the catalog layer — NOT in any launch assembler — and is materialised into the DB by
/// [`SkillCatalog::role_permissions_migration`] + [`crate::presets::workspace_seed_events`].
pub fn default_role_permissions() -> RolePermissions {
    RolePermissions {
        default_mode: "bypassPermissions".into(),
        deny: vec![
            "Write(**/memory/**.md)".into(),
            "Edit(**/memory/**.md)".into(),
        ],
    }
}

/// Per-role binding. `BTreeSet` so iteration is deterministic across replay.
#[derive(Debug, Default, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RoleBinding {
    pub role: String,
    pub enabled_skills: BTreeSet<String>,
    /// The role's system prompt (`hq-role-skills-term.4`): persona/instructions written as
    /// `<workdir>/CLAUDE.md` for a session of this role. Empty when unset; `#[serde(default)]` keeps
    /// pre-`.4` bindings replayable.
    #[serde(default)]
    pub prompt: String,
    /// The role's model config (`hq-role-model.1`): the `claude` launch levers for a session of this
    /// role. `#[serde(default)]` keeps pre-`hq-role-model` bindings replayable.
    #[serde(default)]
    pub model_config: ModelConfig,
    /// The role's gt-rbac scope grants (`hq-role-scopes`): a first-class, per-role permission set,
    /// symmetric to [`prompt`](Self::prompt) and [`model_config`](Self::model_config). This is the
    /// SINGLE source of a role's scopes — [`SkillCatalog::scopes_for_roles`] reads it directly, so a
    /// skill is now pure *knowledge* (its `SKILL.md` body), never a permission bucket. Empty until a
    /// `RoleScopesSet` lands (or the one-shot migration seeds it from the role's enabled-skill
    /// `default_scopes`). `#[serde(default)]` keeps pre-`hq-role-scopes` bindings replayable.
    #[serde(default)]
    pub scopes: Vec<String>,
    /// The role's claude permission model (`gtcore-d175ec`): the `settings.json` `permissions` block
    /// every launch materialises for a session of this role — the FOURTH per-role attribute,
    /// symmetric to [`prompt`](Self::prompt) / [`model_config`](Self::model_config) /
    /// [`scopes`](Self::scopes). Read from the catalog (the DB) at launch, never hardcoded. Empty
    /// until a `RolePermissionsSet` lands (or the one-shot migration seeds the apparatus default).
    /// `#[serde(default)]` keeps pre-`gtcore-d175ec` bindings replayable.
    #[serde(default)]
    pub permissions: RolePermissions,
}

impl RoleBinding {
    pub fn new(role: impl Into<String>) -> Self {
        Self {
            role: role.into(),
            enabled_skills: BTreeSet::new(),
            prompt: String::new(),
            model_config: ModelConfig::default(),
            scopes: Vec::new(),
            permissions: RolePermissions::default(),
        }
    }
}

/// Live skills catalog (what the actor owns). `BTreeMap` so iteration is sorted and
/// the snapshot is deterministic across replay / debug dumps.
#[derive(Debug, Default, Clone, PartialEq, Eq)]
pub struct SkillCatalog {
    skills: BTreeMap<String, Skill>,
    bindings: BTreeMap<String, RoleBinding>,
}

impl SkillCatalog {
    /// Number of registered skills. Diagnostics, not a load-bearing API.
    pub fn len(&self) -> usize {
        self.skills.len()
    }

    pub fn is_empty(&self) -> bool {
        self.skills.is_empty()
    }

    pub fn get(&self, id: &str) -> Option<&Skill> {
        self.skills.get(id)
    }

    pub fn contains(&self, id: &str) -> bool {
        self.skills.contains_key(id)
    }

    /// Owned snapshot of every registered skill, in sorted order.
    pub fn skills(&self) -> Vec<Skill> {
        self.skills.values().cloned().collect()
    }

    /// Owned snapshot of the per-role bindings, in sorted order.
    pub fn bindings(&self) -> Vec<RoleBinding> {
        self.bindings.values().cloned().collect()
    }

    /// `true` if `role` currently has `skill` enabled. Used by the validator + by
    /// `.4` to assemble scope deltas without copying the whole binding.
    pub fn role_has_skill(&self, role: &str, skill: &str) -> bool {
        self.bindings
            .get(role)
            .map(|b| b.enabled_skills.contains(skill))
            .unwrap_or(false)
    }

    /// The role's system prompt (`hq-role-skills-term.4`); `None` when unset/empty. The terminal
    /// writes it as `<workdir>/CLAUDE.md`.
    pub fn role_prompt(&self, role: &str) -> Option<String> {
        self.bindings
            .get(role)
            .map(|b| b.prompt.clone())
            .filter(|p| !p.trim().is_empty())
    }

    /// The role's model config (`hq-role-model.1`); `None` when unset (all-empty) so the terminal
    /// launches the bare `claude` with the account default.
    pub fn role_model(&self, role: &str) -> Option<ModelConfig> {
        self.bindings
            .get(role)
            .map(|b| b.model_config.clone())
            .filter(|m| !m.is_unset())
    }

    /// The role's gt-rbac scopes (`hq-role-scopes`): the binding's `scopes` verbatim, in registered
    /// order. Empty when the role has no binding or no scopes set. This is the accessor the role
    /// editor reads and the symmetric sibling of [`role_prompt`](Self::role_prompt) /
    /// [`role_model`](Self::role_model).
    pub fn role_scopes(&self, role: &str) -> Vec<String> {
        self.bindings
            .get(role)
            .map(|b| b.scopes.clone())
            .unwrap_or_default()
    }

    /// The role's claude permission model (`gtcore-d175ec`); `None` when unset (all-empty), so the
    /// launch writes no `permissions` block for it. The symmetric sibling of
    /// [`role_prompt`](Self::role_prompt) / [`role_model`](Self::role_model) /
    /// [`role_scopes`](Self::role_scopes) — the SINGLE source the launch reads from the DB.
    pub fn role_permissions(&self, role: &str) -> Option<RolePermissions> {
        self.bindings
            .get(role)
            .map(|b| b.permissions.clone())
            .filter(|p| !p.is_unset())
    }

    /// All skills enabled for `role`, in stable order. Empty when the role has no
    /// bindings yet.
    pub fn skills_for_role(&self, role: &str) -> Vec<String> {
        self.bindings
            .get(role)
            .map(|b| b.enabled_skills.iter().cloned().collect())
            .unwrap_or_default()
    }

    /// Flattened scope set for every role in `roles` (`hq-role-scopes`). For each role, reads its
    /// binding's `scopes` **directly** (registered order), dedup'd first-seen across roles so the
    /// gateway can union this with the static `gt_rbac::WebGrant.scopes` without changing the
    /// existing scope ordering posture. Roles with no binding contribute nothing.
    ///
    /// This is the decoupling pivot: scopes are now the role's OWN `scopes` field, no longer derived
    /// by unioning its enabled skills' `default_scopes`. The one-shot [`role_scopes_migration`]
    /// (Self::role_scopes_migration) seeds that field from the legacy union at cutover so this
    /// direct read resolves the same grants it always did.
    pub fn scopes_for_roles(&self, roles: &[String]) -> Vec<String> {
        let mut out: Vec<String> = Vec::new();
        let mut seen: BTreeSet<String> = BTreeSet::new();
        for role in roles {
            let Some(binding) = self.bindings.get(role) else {
                continue;
            };
            for scope in &binding.scopes {
                if seen.insert(scope.clone()) {
                    out.push(scope.clone());
                }
            }
        }
        out
    }

    /// The legacy union of a binding's enabled-skill `default_scopes` — the pre-`hq-role-scopes`
    /// derivation `scopes_for_roles` used to run inline. Retained ONLY as the source the one-shot
    /// migration seeds `RoleBinding::scopes` from; not on any live read path. Walks the enabled
    /// skills (BTreeSet → alphabetical), then each skill's `default_scopes` (registered order),
    /// dedup'd first-seen.
    fn legacy_skill_scope_union(&self, binding: &RoleBinding) -> Vec<String> {
        let mut out: Vec<String> = Vec::new();
        let mut seen: BTreeSet<String> = BTreeSet::new();
        for skill_id in &binding.enabled_skills {
            let Some(skill) = self.skills.get(skill_id) else {
                continue;
            };
            for scope in &skill.default_scopes {
                if seen.insert(scope.clone()) {
                    out.push(scope.clone());
                }
            }
        }
        out
    }

    /// One-shot, idempotent migration to `RoleBinding::scopes` (`hq-role-scopes`): for every role
    /// whose `scopes` are still empty, emit a `RoleScopesSet` seeding them with the union of its
    /// enabled skills' `default_scopes` (the legacy derivation) — so a `scopes_for_roles` direct read
    /// keeps resolving the same grants after the decouple, with zero loss of access.
    ///
    /// Idempotent by construction: a role with a non-empty `scopes` (already migrated, or explicitly
    /// set by an operator) is skipped, and a role with no enabled skills contributes nothing. Applying
    /// the returned events twice is a no-op on the second pass. The caller persists the events into
    /// the workspace `skills.*` log and applies them, so replay reconstructs the seeded state.
    pub fn role_scopes_migration(&self, now_secs: u64) -> Vec<SkillEvent> {
        let mut events = Vec::new();
        for binding in self.bindings.values() {
            if !binding.scopes.is_empty() {
                continue; // already migrated or operator-set — never clobber
            }
            let scopes = self.legacy_skill_scope_union(binding);
            if scopes.is_empty() {
                continue; // a role with no scope-bearing skills has nothing to seed
            }
            events.push(SkillEvent::RoleScopesSet {
                role: binding.role.clone(),
                scopes,
                now_secs,
            });
        }
        events
    }

    /// One-shot idempotent backfill (`gtcore-d175ec`) seeding the apparatus DEFAULT permission model
    /// ([`default_role_permissions`]) into every role binding that has none yet — the same cutover
    /// shape as [`role_scopes_migration`](Self::role_scopes_migration). Run at read-time on the
    /// replayed (DB) catalog, then appended, so a live catalog seeded before this attribute existed
    /// gains the default without losing it on the next replay. Never clobbers an operator-set value
    /// (a binding with permissions already set is skipped). Returns the events to append.
    pub fn role_permissions_migration(&self, now_secs: u64) -> Vec<SkillEvent> {
        let default = default_role_permissions();
        let mut events = Vec::new();
        for binding in self.bindings.values() {
            if !binding.permissions.is_unset() {
                continue; // already migrated or operator-set — never clobber
            }
            events.push(SkillEvent::RolePermissionsSet {
                role: binding.role.clone(),
                default_mode: default.default_mode.clone(),
                deny: default.deny.clone(),
                now_secs,
            });
        }
        events
    }

    // -- mutation helpers (the only writers, consulted by both `commands::execute`
    //    and `SkillState::apply` so the live state and the rebuilt state stay in
    //    lockstep).

    pub(crate) fn apply_register(&mut self, skill: Skill) {
        self.skills.insert(skill.id.clone(), skill);
    }

    pub(crate) fn apply_retire(&mut self, id: &str) {
        self.skills.remove(id);
        // Cascade: drop the skill from every binding so the catalog stays consistent.
        for b in self.bindings.values_mut() {
            b.enabled_skills.remove(id);
        }
    }

    pub(crate) fn apply_enable(&mut self, role: &str, skill: &str) {
        self.bindings
            .entry(role.to_string())
            .or_insert_with(|| RoleBinding::new(role))
            .enabled_skills
            .insert(skill.to_string());
    }

    pub(crate) fn apply_set_role_prompt(&mut self, role: &str, prompt: &str) {
        // Materialise the binding on first prompt-set so a role can carry a prompt before any skill.
        self.bindings
            .entry(role.to_string())
            .or_insert_with(|| RoleBinding::new(role))
            .prompt = prompt.to_string();
    }

    pub(crate) fn apply_set_role_model(&mut self, role: &str, config: ModelConfig) {
        // Materialise the binding on first model-set so a role can carry a model config before any
        // skill — symmetric with `apply_set_role_prompt`.
        self.bindings
            .entry(role.to_string())
            .or_insert_with(|| RoleBinding::new(role))
            .model_config = config;
    }

    pub(crate) fn apply_set_role_scopes(&mut self, role: &str, scopes: &[String]) {
        // Materialise the binding on first scopes-set so a role can carry scopes before any skill —
        // symmetric with `apply_set_role_prompt` / `apply_set_role_model`. A scalar overwrite: the
        // event carries the full intended set (the editor PUTs the whole list), not a delta.
        self.bindings
            .entry(role.to_string())
            .or_insert_with(|| RoleBinding::new(role))
            .scopes = scopes.to_vec();
    }

    pub(crate) fn apply_set_role_permissions(&mut self, role: &str, permissions: RolePermissions) {
        // Materialise the binding on first permissions-set so a role can carry permissions before any
        // skill — symmetric with the other per-role setters. A scalar overwrite (the full value).
        self.bindings
            .entry(role.to_string())
            .or_insert_with(|| RoleBinding::new(role))
            .permissions = permissions;
    }

    pub(crate) fn apply_disable(&mut self, role: &str, skill: &str) {
        if let Some(b) = self.bindings.get_mut(role) {
            b.enabled_skills.remove(skill);
            // Don't garbage-collect empty bindings: a role with no skills is a
            // distinct, valid state (it just means "stripped to defaults"). Keeping
            // the entry lets `skills_for_role` return `Some(empty)` vs `None`.
        }
    }
}

// ----------------------------------------------------------------------------
// Pure replay reducer.

/// Same data as [`SkillCatalog`] but rebuilt by replaying [`SkillEvent`]s. The Step 3
/// gate (`docs/06-observability.md`) requires the rebuilt state to match the live
/// catalog byte-for-byte after replay, so we share the inner `apply_*` helpers.
#[derive(Debug, Default, Clone, PartialEq, Eq)]
pub struct SkillState {
    pub catalog: SkillCatalog,
}

impl SkillState {
    pub fn apply(&mut self, event: &SkillEvent) {
        match event {
            SkillEvent::Registered {
                skill,
                label,
                description,
                default_scopes,
                body,
                group,
                now_secs,
            } => {
                self.catalog.apply_register(
                    Skill::new(
                        skill.clone(),
                        label.clone(),
                        description.clone(),
                        default_scopes.clone(),
                        *now_secs,
                    )
                    .with_body(body.clone())
                    .with_group(group.clone()),
                );
            }
            SkillEvent::Retired { skill, .. } => {
                self.catalog.apply_retire(skill);
            }
            SkillEvent::EnabledForRole { role, skill, .. } => {
                self.catalog.apply_enable(role, skill);
            }
            SkillEvent::DisabledForRole { role, skill, .. } => {
                self.catalog.apply_disable(role, skill);
            }
            SkillEvent::RolePromptSet { role, prompt, .. } => {
                self.catalog.apply_set_role_prompt(role, prompt);
            }
            SkillEvent::RoleModelSet {
                role,
                model,
                permission_mode,
                effort,
                ..
            } => {
                self.catalog.apply_set_role_model(
                    role,
                    ModelConfig {
                        model: model.clone(),
                        permission_mode: permission_mode.clone(),
                        effort: effort.clone(),
                    },
                );
            }
            SkillEvent::RoleScopesSet { role, scopes, .. } => {
                self.catalog.apply_set_role_scopes(role, scopes);
            }
            SkillEvent::RolePermissionsSet {
                role,
                default_mode,
                deny,
                ..
            } => {
                self.catalog.apply_set_role_permissions(
                    role,
                    RolePermissions {
                        default_mode: default_mode.clone(),
                        deny: deny.clone(),
                    },
                );
            }
        }
    }
}

// ----------------------------------------------------------------------------
// Structural validators (used by both the command validator and any future edge
// adapter that ingests external skill registrations).

/// Validate a skill id. Conservative: ASCII alphanumeric + `-` + `_`, non-empty, no
/// leading/trailing whitespace, bounded length. The id surfaces in SSE payloads, in
/// the UI, and in scope strings — keep it parseable.
pub fn validate_skill_id(id: &str) -> Result<(), String> {
    if id.is_empty() {
        return Err("skill id is empty".into());
    }
    if id.len() > MAX_SKILL_ID_LEN {
        return Err(format!("skill id is longer than {MAX_SKILL_ID_LEN} bytes"));
    }
    if id.trim() != id {
        return Err("skill id has leading or trailing whitespace".into());
    }
    if !id
        .chars()
        .all(|c| c.is_ascii_alphanumeric() || c == '-' || c == '_')
    {
        return Err(format!(
            "skill id {id:?} has invalid characters (allowed: ASCII alnum, '-', '_')"
        ));
    }
    Ok(())
}

/// Validate a role name. Same conventions as `validate_skill_id`; `gt-rbac` keys its
/// `roles` map on this string so the constraints are consistent.
pub fn validate_role_name(role: &str) -> Result<(), String> {
    if role.is_empty() {
        return Err("role name is empty".into());
    }
    if role.trim() != role {
        return Err("role name has leading or trailing whitespace".into());
    }
    if !role
        .chars()
        .all(|c| c.is_ascii_alphanumeric() || c == '-' || c == '_')
    {
        return Err(format!(
            "role name {role:?} has invalid characters (allowed: ASCII alnum, '-', '_')"
        ));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn skill_event(id: &str, now: u64) -> SkillEvent {
        SkillEvent::Registered {
            skill: id.into(),
            label: format!("{id} label"),
            description: "test skill".into(),
            default_scopes: vec![format!("{id}.read"), format!("{id}.write")],
            body: String::new(),
            group: String::new(),
            now_secs: now,
        }
    }

    #[test]
    fn apply_register_and_lookup() {
        let mut s = SkillState::default();
        s.apply(&skill_event("merge_admin", 1));
        assert_eq!(s.catalog.len(), 1);
        assert!(s.catalog.contains("merge_admin"));
        assert_eq!(
            s.catalog.get("merge_admin").unwrap().default_scopes,
            vec!["merge_admin.read", "merge_admin.write"]
        );
    }

    #[test]
    fn apply_retire_cascades_to_bindings() {
        let mut s = SkillState::default();
        s.apply(&skill_event("merge_admin", 1));
        s.apply(&SkillEvent::EnabledForRole {
            role: "deacon".into(),
            skill: "merge_admin".into(),
            now_secs: 2,
        });
        assert!(s.catalog.role_has_skill("deacon", "merge_admin"));

        s.apply(&SkillEvent::Retired {
            skill: "merge_admin".into(),
            now_secs: 3,
        });
        assert!(!s.catalog.contains("merge_admin"));
        // Cascade: the binding must drop the retired skill, not leak it.
        assert!(!s.catalog.role_has_skill("deacon", "merge_admin"));
    }

    #[test]
    fn apply_disable_keeps_empty_binding() {
        let mut s = SkillState::default();
        s.apply(&skill_event("alpha", 1));
        s.apply(&SkillEvent::EnabledForRole {
            role: "scout".into(),
            skill: "alpha".into(),
            now_secs: 2,
        });
        s.apply(&SkillEvent::DisabledForRole {
            role: "scout".into(),
            skill: "alpha".into(),
            now_secs: 3,
        });
        // Empty binding sticks around: `Some(empty)` is a distinct state from `None`
        // (skip the binding entirely).
        assert_eq!(s.catalog.skills_for_role("scout"), Vec::<String>::new());
        assert_eq!(s.catalog.bindings().len(), 1);
    }

    #[test]
    fn replay_is_deterministic() {
        let events = vec![
            skill_event("alpha", 1),
            skill_event("beta", 2),
            SkillEvent::EnabledForRole {
                role: "deacon".into(),
                skill: "alpha".into(),
                now_secs: 3,
            },
            SkillEvent::EnabledForRole {
                role: "deacon".into(),
                skill: "beta".into(),
                now_secs: 4,
            },
            SkillEvent::DisabledForRole {
                role: "deacon".into(),
                skill: "alpha".into(),
                now_secs: 5,
            },
        ];

        let mut a = SkillState::default();
        let mut b = SkillState::default();
        for e in &events {
            a.apply(e);
        }
        for e in &events {
            b.apply(e);
        }
        assert_eq!(a, b);
        assert_eq!(a.catalog.skills_for_role("deacon"), vec!["beta"]);
    }

    #[test]
    fn scopes_for_roles_unions_and_dedups_across_skills_and_roles() {
        let mut s = SkillState::default();
        // alpha grants two scopes, beta overlaps on `feed.read`.
        s.apply(&SkillEvent::Registered {
            skill: "alpha".into(),
            label: "alpha".into(),
            description: "".into(),
            default_scopes: vec!["feed.read".into(), "merge.read".into()],
            body: String::new(),
            group: String::new(),
        now_secs: 1,
        });
        s.apply(&SkillEvent::Registered {
            skill: "beta".into(),
            label: "beta".into(),
            description: "".into(),
            default_scopes: vec!["feed.read".into(), "beads.read".into()],
            body: String::new(),
            group: String::new(),
        now_secs: 2,
        });
        s.apply(&SkillEvent::EnabledForRole {
            role: "deacon".into(),
            skill: "alpha".into(),
            now_secs: 3,
        });
        s.apply(&SkillEvent::EnabledForRole {
            role: "sheriff".into(),
            skill: "beta".into(),
            now_secs: 4,
        });

        // hq-role-scopes: `scopes_for_roles` now reads `RoleBinding::scopes` directly, so the legacy
        // skill-union must be seeded onto each binding first — exactly what the boot/migration seam
        // does. Replaying the emitted `RoleScopesSet` events lands the same grants on the bindings.
        for ev in s.catalog.role_scopes_migration(5) {
            s.apply(&ev);
        }

        // Single-role lookup hits alpha only.
        assert_eq!(
            s.catalog.scopes_for_roles(&["deacon".into()]),
            vec!["feed.read".to_string(), "merge.read".to_string()]
        );
        // Cross-role union: feed.read appears once. alpha first because BTreeSet
        // iteration on the deacon binding lands before sheriff's.
        assert_eq!(
            s.catalog
                .scopes_for_roles(&["deacon".into(), "sheriff".into()]),
            vec![
                "feed.read".to_string(),
                "merge.read".to_string(),
                "beads.read".to_string(),
            ]
        );
        // Unknown role → empty contribution.
        assert!(s.catalog.scopes_for_roles(&["ghost".into()]).is_empty());
    }

    fn registered(skill: &str, scopes: &[&str], now: u64) -> SkillEvent {
        SkillEvent::Registered {
            skill: skill.into(),
            label: skill.into(),
            description: String::new(),
            default_scopes: scopes.iter().map(|s| (*s).to_string()).collect(),
            body: String::new(),
            group: String::new(),
            now_secs: now,
        }
    }

    fn enabled(role: &str, skill: &str, now: u64) -> SkillEvent {
        SkillEvent::EnabledForRole {
            role: role.into(),
            skill: skill.into(),
            now_secs: now,
        }
    }

    #[test]
    fn role_scopes_migration_seeds_legacy_union_idempotently_with_no_access_loss() {
        // hq-role-scopes: the one-shot migration seeds `RoleBinding::scopes` from each role's
        // enabled-skill `default_scopes` union, so the now-direct `scopes_for_roles` read keeps the
        // exact pre-decouple grants. The two trigger cases: the polecat's phantom `memory-access`
        // permission-bucket skill, and the mayor's operator-enabled skills.
        let mut s = SkillState::default();
        s.apply(&registered(
            "memory-access",
            &["memory.read", "memory.write"],
            1,
        ));
        s.apply(&registered("graph-view", &["graph.read"], 2));
        s.apply(&registered("notify", &["notifications.write"], 3));
        s.apply(&registered("ws-member", &["workspace.member"], 4));
        s.apply(&enabled("polecat", "memory-access", 5));
        s.apply(&enabled("mayor", "graph-view", 6));
        s.apply(&enabled("mayor", "notify", 7));
        s.apply(&enabled("mayor", "ws-member", 8));

        // Pre-migration: scopes are decoupled from skills, so a direct read is empty until seeded.
        assert!(s.catalog.scopes_for_roles(&["polecat".into()]).is_empty());

        let events = s.catalog.role_scopes_migration(10);
        assert_eq!(events.len(), 2, "one RoleScopesSet per role with scope-bearing skills");
        for ev in &events {
            s.apply(ev);
        }

        // No access loss: the polecat keeps memory.read/write; the mayor keeps its three scopes
        // (enabled_skills is a BTreeSet → graph-view, notify, ws-member alphabetical).
        assert_eq!(
            s.catalog.scopes_for_roles(&["polecat".into()]),
            vec!["memory.read".to_string(), "memory.write".to_string()]
        );
        assert_eq!(
            s.catalog.role_scopes("mayor"),
            vec![
                "graph.read".to_string(),
                "notifications.write".to_string(),
                "workspace.member".to_string(),
            ]
        );

        // Idempotent: a second pass emits nothing — already-set scopes are never clobbered.
        assert!(s.catalog.role_scopes_migration(11).is_empty());

        // Decoupled: enabling a NEW scope-bearing skill no longer widens the role's scopes, and the
        // already-migrated role is not reseeded. Knowledge ⊥ permission.
        s.apply(&registered("extra", &["merge.write"], 12));
        s.apply(&enabled("polecat", "extra", 13));
        assert!(s.catalog.role_scopes_migration(14).is_empty());
        assert_eq!(
            s.catalog.scopes_for_roles(&["polecat".into()]),
            vec!["memory.read".to_string(), "memory.write".to_string()]
        );
    }

    #[test]
    fn role_permissions_migration_seeds_apparatus_default_idempotently() {
        // gtcore-d175ec: the one-shot migration seeds the apparatus DEFAULT permission model into
        // every role binding that has none yet, so the now-direct `role_permissions` read resolves a
        // value from the catalog (the DB) instead of a hardcoded launch constant.
        let mut s = SkillState::default();
        s.apply(&registered("bead-work", &["issues.read"], 1));
        s.apply(&enabled("polecat", "bead-work", 2));
        s.apply(&enabled("mayor", "bead-work", 3));

        // Pre-migration: no permission model on disk → the launch would write none.
        assert!(s.catalog.role_permissions("polecat").is_none());

        let events = s.catalog.role_permissions_migration(10);
        assert_eq!(events.len(), 2, "one RolePermissionsSet per bound role");
        for ev in &events {
            s.apply(ev);
        }

        // Both roles now resolve the apparatus default from the catalog.
        let want = default_role_permissions();
        assert_eq!(s.catalog.role_permissions("polecat").as_ref(), Some(&want));
        assert_eq!(s.catalog.role_permissions("mayor").as_ref(), Some(&want));
        assert_eq!(want.default_mode, "bypassPermissions");
        assert!(want.deny.iter().any(|d| d == "Write(**/memory/**.md)"));
        // The rendered settings.json shape: { defaultMode, deny:[...] }, no stale MultiEdit rule.
        let json = want.to_settings_json();
        assert_eq!(json["defaultMode"], "bypassPermissions");
        assert!(!json["deny"]
            .as_array()
            .unwrap()
            .iter()
            .any(|d| d == "MultiEdit(**/memory/**.md)"));

        // Idempotent: a second pass emits nothing — an operator-set/seeded value is never clobbered.
        assert!(s.catalog.role_permissions_migration(11).is_empty());

        // Operator override survives: setting a custom value is not reseeded.
        s.apply(&SkillEvent::RolePermissionsSet {
            role: "mayor".into(),
            default_mode: "bypassPermissions".into(),
            deny: vec!["Write(**/secret/**)".into()],
            now_secs: 12,
        });
        assert!(s.catalog.role_permissions_migration(13).is_empty());
        assert_eq!(
            s.catalog.role_permissions("mayor").unwrap().deny,
            vec!["Write(**/secret/**)".to_string()]
        );
    }

    #[test]
    fn validate_skill_id_rejects_whitespace_unicode_and_oversize() {
        assert!(validate_skill_id("merge_admin").is_ok());
        assert!(validate_skill_id("merge-admin-v2").is_ok());
        assert!(validate_skill_id("").is_err());
        assert!(validate_skill_id(" leading").is_err());
        assert!(validate_skill_id("trail ").is_err());
        assert!(validate_skill_id("café").is_err()); // non-ASCII alnum
        assert!(validate_skill_id("dot.notation").is_err()); // `.` reserved for scopes
        assert!(validate_skill_id(&"x".repeat(MAX_SKILL_ID_LEN + 1)).is_err());
    }
}
