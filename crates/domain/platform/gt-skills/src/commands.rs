//! Owned `Command` structs over [`SkillCatalog`].
//!
//! Same retrofit shape as gt-quota / gt-merge / gt-rig: each mutation an external client
//! (a model via `gt-mcp`, an operator via `gt-web`) can drive is a pure, sync [`Command`].
//! `validate` inspects the catalog without mutating it; `execute` applies the mutation
//! (delegating to the shared `SkillCatalog::apply_*` methods so the actor messages and
//! this path stay in lockstep) and returns the [`SkillEvent`] for the actor to emit on
//! the relay.

use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

use gt_events::{AppError, Command};

use crate::events::SkillEvent;
use crate::state::{validate_role_name, validate_skill_id, Skill, SkillCatalog};

/// Register a new skill in the catalog. The label/description are UI metadata; the
/// `default_scopes` is the canonical scope set the resolver hands out to any role
/// with the skill enabled (`hq-fe-skills.4`).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
pub struct RegisterSkill {
    pub skill: String,
    pub label: String,
    pub description: String,
    /// Scope identifiers (`<domain>.<action>`) the skill grants. Order-preserving so
    /// the registered ordering survives serde roundtrips.
    #[serde(default)]
    pub default_scopes: Vec<String>,
    /// The skill's `SKILL.md` body (`hq-role-skills-term.1`); empty when none. What the terminal
    /// materialises into a session's `.claude/skills/<id>/SKILL.md`.
    #[serde(default)]
    pub body: String,
    pub now_secs: u64,
}

impl RegisterSkill {
    fn validate_structure(&self) -> Result<(), AppError> {
        validate_skill_id(&self.skill).map_err(AppError::Validation)?;
        if self.label.trim().is_empty() {
            return Err(AppError::Validation("label is empty".into()));
        }
        for s in &self.default_scopes {
            if s.trim().is_empty() {
                return Err(AppError::Validation("default_scopes contains an empty entry".into()));
            }
        }
        Ok(())
    }
}

impl Command for RegisterSkill {
    type Output = SkillEvent;
    type State = SkillCatalog;

    fn validate(&self, state: &Self::State) -> Result<(), AppError> {
        self.validate_structure()?;
        if state.contains(&self.skill) {
            return Err(AppError::Validation(format!(
                "skill {:?} already registered",
                self.skill
            )));
        }
        Ok(())
    }

    fn execute(&self, state: &mut Self::State) -> Result<Self::Output, AppError> {
        self.validate(state)?;
        state.apply_register(
            Skill::new(
                self.skill.clone(),
                self.label.clone(),
                self.description.clone(),
                self.default_scopes.clone(),
                self.now_secs,
            )
            .with_body(self.body.clone()),
        );
        Ok(SkillEvent::Registered {
            skill: self.skill.clone(),
            label: self.label.clone(),
            description: self.description.clone(),
            default_scopes: self.default_scopes.clone(),
            body: self.body.clone(),
            now_secs: self.now_secs,
        })
    }
}

/// Retire a skill from the catalog. Cascades to every role binding (the reducer drops
/// the skill from each binding's `enabled_skills`) so the live state stays consistent.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
pub struct RetireSkill {
    pub skill: String,
    pub now_secs: u64,
}

impl RetireSkill {
    fn validate_structure(&self) -> Result<(), AppError> {
        validate_skill_id(&self.skill).map_err(AppError::Validation)
    }
}

impl Command for RetireSkill {
    type Output = SkillEvent;
    type State = SkillCatalog;

    fn validate(&self, state: &Self::State) -> Result<(), AppError> {
        self.validate_structure()?;
        if !state.contains(&self.skill) {
            return Err(AppError::Validation(format!(
                "skill {:?} is not registered",
                self.skill
            )));
        }
        Ok(())
    }

    fn execute(&self, state: &mut Self::State) -> Result<Self::Output, AppError> {
        self.validate(state)?;
        state.apply_retire(&self.skill);
        Ok(SkillEvent::Retired {
            skill: self.skill.clone(),
            now_secs: self.now_secs,
        })
    }
}

/// Enable a registered skill for a role. The role does not need to pre-exist in the
/// catalog — the reducer materialises a fresh [`crate::RoleBinding`] on first toggle so
/// roles defined only in `gt-rbac` config can gain dynamic skills without a registration
/// dance.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
pub struct EnableSkillForRole {
    pub role: String,
    pub skill: String,
    pub now_secs: u64,
}

impl EnableSkillForRole {
    fn validate_structure(&self) -> Result<(), AppError> {
        validate_role_name(&self.role).map_err(AppError::Validation)?;
        validate_skill_id(&self.skill).map_err(AppError::Validation)
    }
}

impl Command for EnableSkillForRole {
    type Output = SkillEvent;
    type State = SkillCatalog;

    fn validate(&self, state: &Self::State) -> Result<(), AppError> {
        self.validate_structure()?;
        if !state.contains(&self.skill) {
            return Err(AppError::Validation(format!(
                "skill {:?} is not registered",
                self.skill
            )));
        }
        if state.role_has_skill(&self.role, &self.skill) {
            return Err(AppError::Validation(format!(
                "role {:?} already has skill {:?}",
                self.role, self.skill
            )));
        }
        Ok(())
    }

    fn execute(&self, state: &mut Self::State) -> Result<Self::Output, AppError> {
        self.validate(state)?;
        state.apply_enable(&self.role, &self.skill);
        Ok(SkillEvent::EnabledForRole {
            role: self.role.clone(),
            skill: self.skill.clone(),
            now_secs: self.now_secs,
        })
    }
}

/// Remove a previously-enabled skill from a role. Symmetric to [`EnableSkillForRole`].
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
pub struct DisableSkillForRole {
    pub role: String,
    pub skill: String,
    pub now_secs: u64,
}

impl DisableSkillForRole {
    fn validate_structure(&self) -> Result<(), AppError> {
        validate_role_name(&self.role).map_err(AppError::Validation)?;
        validate_skill_id(&self.skill).map_err(AppError::Validation)
    }
}

impl Command for DisableSkillForRole {
    type Output = SkillEvent;
    type State = SkillCatalog;

    fn validate(&self, state: &Self::State) -> Result<(), AppError> {
        self.validate_structure()?;
        if !state.role_has_skill(&self.role, &self.skill) {
            return Err(AppError::Validation(format!(
                "role {:?} does not have skill {:?}",
                self.role, self.skill
            )));
        }
        Ok(())
    }

    fn execute(&self, state: &mut Self::State) -> Result<Self::Output, AppError> {
        self.validate(state)?;
        state.apply_disable(&self.role, &self.skill);
        Ok(SkillEvent::DisabledForRole {
            role: self.role.clone(),
            skill: self.skill.clone(),
            now_secs: self.now_secs,
        })
    }
}

/// Set (or clear) a role's system prompt (`hq-role-skills-term.4`): the persona/instructions the
/// terminal writes as `<workdir>/CLAUDE.md`. An empty `prompt` clears it. The role need not pre-exist
/// — the reducer materialises the binding on first set, like [`EnableSkillForRole`].
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
pub struct SetRolePrompt {
    pub role: String,
    pub prompt: String,
    pub now_secs: u64,
}

impl Command for SetRolePrompt {
    type Output = SkillEvent;
    type State = SkillCatalog;

    fn validate(&self, _state: &Self::State) -> Result<(), AppError> {
        validate_role_name(&self.role).map_err(AppError::Validation)
    }

    fn execute(&self, state: &mut Self::State) -> Result<Self::Output, AppError> {
        self.validate(state)?;
        state.apply_set_role_prompt(&self.role, &self.prompt);
        Ok(SkillEvent::RolePromptSet {
            role: self.role.clone(),
            prompt: self.prompt.clone(),
            now_secs: self.now_secs,
        })
    }
}

/// Tagged union of every command. The HTTP / MCP edge passes one of these into
/// [`crate::actor::SkillHandle::exec`] without a per-variant fan-out.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(tag = "op", rename_all = "snake_case")]
pub enum SkillCommand {
    Register(RegisterSkill),
    Retire(RetireSkill),
    Enable(EnableSkillForRole),
    Disable(DisableSkillForRole),
}

impl Command for SkillCommand {
    type Output = SkillEvent;
    type State = SkillCatalog;

    fn validate(&self, state: &Self::State) -> Result<(), AppError> {
        match self {
            SkillCommand::Register(c) => c.validate(state),
            SkillCommand::Retire(c) => c.validate(state),
            SkillCommand::Enable(c) => c.validate(state),
            SkillCommand::Disable(c) => c.validate(state),
        }
    }

    fn execute(&self, state: &mut Self::State) -> Result<Self::Output, AppError> {
        match self {
            SkillCommand::Register(c) => c.execute(state),
            SkillCommand::Retire(c) => c.execute(state),
            SkillCommand::Enable(c) => c.execute(state),
            SkillCommand::Disable(c) => c.execute(state),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn register(id: &str, scopes: &[&str], now: u64) -> RegisterSkill {
        RegisterSkill {
            skill: id.into(),
            label: format!("{id} label"),
            description: format!("{id} desc"),
            default_scopes: scopes.iter().map(|s| (*s).to_string()).collect(),
            body: String::new(),
            now_secs: now,
        }
    }

    #[test]
    fn register_carries_the_skill_md_body_into_the_catalog() {
        // hq-role-skills-term.1: the SKILL.md body registered is stored on the catalog entry and
        // rides the emitted event (so replay + the terminal materialisation see it).
        let mut state = SkillCatalog::default();
        let cmd = RegisterSkill {
            skill: "graphify".into(),
            label: "Graphify".into(),
            description: "kg".into(),
            default_scopes: vec![],
            body: "# Graphify\nbuild a knowledge graph".into(),
            now_secs: 1,
        };
        let ev = cmd.execute(&mut state).unwrap();
        assert_eq!(state.get("graphify").unwrap().body, "# Graphify\nbuild a knowledge graph");
        let SkillEvent::Registered { body, .. } = ev else { panic!("expected Registered") };
        assert_eq!(body, "# Graphify\nbuild a knowledge graph");
    }

    #[test]
    fn register_validate_then_execute_emits_registered() {
        let mut state = SkillCatalog::default();
        let cmd = register("merge_admin", &["merge.submit"], 1);
        cmd.validate(&state).unwrap();
        let ev = cmd.execute(&mut state).unwrap();
        assert!(matches!(ev, SkillEvent::Registered { .. }));
        assert!(state.contains("merge_admin"));
    }

    #[test]
    fn register_rejects_duplicate() {
        let mut state = SkillCatalog::default();
        register("alpha", &[], 1).execute(&mut state).unwrap();
        let err = register("alpha", &[], 2).validate(&state).unwrap_err();
        assert!(matches!(err, AppError::Validation(_)));
    }

    #[test]
    fn set_role_prompt_stores_and_clears_the_role_prompt() {
        // hq-role-skills-term.4: a role's prompt is stored on its binding (materialised on first
        // set) and an empty prompt clears it.
        let mut state = SkillCatalog::default();
        let set = SetRolePrompt {
            role: "witness".into(),
            prompt: "You are the witness. Audit PRs.".into(),
            now_secs: 1,
        };
        let ev = set.execute(&mut state).unwrap();
        assert!(matches!(ev, SkillEvent::RolePromptSet { .. }));
        assert_eq!(state.role_prompt("witness").as_deref(), Some("You are the witness. Audit PRs."));
        // Clear it.
        let clear = SetRolePrompt { role: "witness".into(), prompt: String::new(), now_secs: 2 };
        clear.execute(&mut state).unwrap();
        assert!(state.role_prompt("witness").is_none());
    }

    #[test]
    fn register_rejects_invalid_id_and_empty_label() {
        let mut state = SkillCatalog::default();
        let bad_id = RegisterSkill {
            skill: "has spaces".into(),
            label: "x".into(),
            description: "".into(),
            default_scopes: vec![],
            body: String::new(),
            now_secs: 1,
        };
        assert!(bad_id.validate(&state).is_err());

        let bad_label = RegisterSkill {
            skill: "ok".into(),
            label: "  ".into(),
            description: "".into(),
            default_scopes: vec![],
            body: String::new(),
            now_secs: 1,
        };
        assert!(bad_label.validate(&state).is_err());
        // Neither call should have mutated state.
        assert!(state.is_empty());
    }

    #[test]
    fn enable_requires_skill_to_exist() {
        let state = SkillCatalog::default();
        let cmd = EnableSkillForRole {
            role: "deacon".into(),
            skill: "ghost".into(),
            now_secs: 1,
        };
        assert!(cmd.validate(&state).is_err());
    }

    #[test]
    fn enable_then_disable_round_trip() {
        let mut state = SkillCatalog::default();
        register("alpha", &["alpha.read"], 1).execute(&mut state).unwrap();
        EnableSkillForRole {
            role: "deacon".into(),
            skill: "alpha".into(),
            now_secs: 2,
        }
        .execute(&mut state)
        .unwrap();
        assert!(state.role_has_skill("deacon", "alpha"));

        // Double-enable rejects (validate inspects state before exec mutates).
        let dup = EnableSkillForRole {
            role: "deacon".into(),
            skill: "alpha".into(),
            now_secs: 3,
        };
        assert!(dup.validate(&state).is_err());

        DisableSkillForRole {
            role: "deacon".into(),
            skill: "alpha".into(),
            now_secs: 4,
        }
        .execute(&mut state)
        .unwrap();
        assert!(!state.role_has_skill("deacon", "alpha"));
    }

    #[test]
    fn disable_requires_role_to_have_the_skill() {
        let mut state = SkillCatalog::default();
        register("alpha", &[], 1).execute(&mut state).unwrap();
        let cmd = DisableSkillForRole {
            role: "deacon".into(),
            skill: "alpha".into(),
            now_secs: 2,
        };
        assert!(cmd.validate(&state).is_err());
    }

    #[test]
    fn retire_cascades_to_bindings() {
        let mut state = SkillCatalog::default();
        register("alpha", &["alpha.read"], 1).execute(&mut state).unwrap();
        EnableSkillForRole {
            role: "deacon".into(),
            skill: "alpha".into(),
            now_secs: 2,
        }
        .execute(&mut state)
        .unwrap();
        RetireSkill {
            skill: "alpha".into(),
            now_secs: 3,
        }
        .execute(&mut state)
        .unwrap();
        assert!(!state.contains("alpha"));
        // Binding still exists but no longer carries the retired skill.
        assert_eq!(state.skills_for_role("deacon"), Vec::<String>::new());
    }

    #[test]
    fn enum_dispatch_matches_per_variant_execute() {
        let mut state_a = SkillCatalog::default();
        let mut state_b = SkillCatalog::default();
        let cmd = register("alpha", &["alpha.read"], 1);
        // Direct command vs enum-routed: both produce the same state + event.
        let ev_a = cmd.execute(&mut state_a).unwrap();
        let ev_b = SkillCommand::Register(cmd.clone())
            .execute(&mut state_b)
            .unwrap();
        assert_eq!(state_a, state_b);
        assert_eq!(ev_a, ev_b);
    }
}
