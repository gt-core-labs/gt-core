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
        }
    }
}

/// Per-role binding. `BTreeSet` so iteration is deterministic across replay.
#[derive(Debug, Default, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RoleBinding {
    pub role: String,
    pub enabled_skills: BTreeSet<String>,
}

impl RoleBinding {
    pub fn new(role: impl Into<String>) -> Self {
        Self {
            role: role.into(),
            enabled_skills: BTreeSet::new(),
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

    /// All skills enabled for `role`, in stable order. Empty when the role has no
    /// bindings yet.
    pub fn skills_for_role(&self, role: &str) -> Vec<String> {
        self.bindings
            .get(role)
            .map(|b| b.enabled_skills.iter().cloned().collect())
            .unwrap_or_default()
    }

    /// Flattened scope set for every role in `roles` (`hq-fe-skills.4`). For each role,
    /// walks its enabled skills (BTreeSet → alphabetical), then each skill's
    /// `default_scopes` (registered ordering), dedup'd first-seen so the gateway can
    /// union this with the static `gt_rbac::WebGrant.scopes` without changing the
    /// existing scope ordering posture. Roles with no binding contribute nothing.
    pub fn scopes_for_roles(&self, roles: &[String]) -> Vec<String> {
        let mut out: Vec<String> = Vec::new();
        let mut seen: BTreeSet<String> = BTreeSet::new();
        for role in roles {
            let Some(binding) = self.bindings.get(role) else {
                continue;
            };
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
        }
        out
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
                now_secs,
            } => {
                self.catalog.apply_register(Skill::new(
                    skill.clone(),
                    label.clone(),
                    description.clone(),
                    default_scopes.clone(),
                    *now_secs,
                ));
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
        return Err(format!(
            "skill id is longer than {MAX_SKILL_ID_LEN} bytes"
        ));
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
            now_secs: 1,
        });
        s.apply(&SkillEvent::Registered {
            skill: "beta".into(),
            label: "beta".into(),
            description: "".into(),
            default_scopes: vec!["feed.read".into(), "beads.read".into()],
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
        assert!(s
            .catalog
            .scopes_for_roles(&["ghost".into()])
            .is_empty());
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
