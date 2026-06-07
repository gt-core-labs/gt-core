//! Owned `Command` structs over [`HooksRegistry`].
//!
//! Same shape as gt-skills: each mutation is a pure, sync [`Command`]. `validate` inspects the
//! registry without mutating; `execute` applies the mutation (via the shared `apply_*` helpers) and
//! returns the [`HookEvent`] to persist.

use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

use gt_events::{AppError, Command};

use crate::events::HookEvent;
use crate::state::{validate_event, validate_hook_id, HookDef, HookTarget, HooksRegistry};

/// Register (or replace) a hook in the global registry. Re-using an existing `id` upserts the
/// entry, so an edit rides this same command — symmetric to `gt-skills`'s register/update.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
pub struct RegisterHook {
    pub id: String,
    pub event: String,
    #[serde(default)]
    pub matcher: String,
    pub command: String,
    #[serde(default)]
    pub target: HookTarget,
    pub now_secs: u64,
}

impl RegisterHook {
    fn validate_structure(&self) -> Result<(), AppError> {
        validate_hook_id(&self.id).map_err(AppError::Validation)?;
        validate_event(&self.event).map_err(AppError::Validation)?;
        if self.command.trim().is_empty() {
            return Err(AppError::Validation("command is empty".into()));
        }
        Ok(())
    }
}

impl Command for RegisterHook {
    type Output = HookEvent;
    type State = HooksRegistry;

    fn validate(&self, _state: &Self::State) -> Result<(), AppError> {
        // Upsert semantics: no uniqueness check (re-registering an id replaces it).
        self.validate_structure()
    }

    fn execute(&self, state: &mut Self::State) -> Result<Self::Output, AppError> {
        self.validate(state)?;
        state.apply_register(HookDef {
            id: self.id.clone(),
            event: self.event.clone(),
            matcher: self.matcher.clone(),
            command: self.command.clone(),
            target: self.target.clone(),
            registered_at_secs: self.now_secs,
        });
        Ok(HookEvent::Registered {
            id: self.id.clone(),
            event: self.event.clone(),
            matcher: self.matcher.clone(),
            command: self.command.clone(),
            target: self.target.clone(),
            now_secs: self.now_secs,
        })
    }
}

/// Retire a hook from the registry.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
pub struct RetireHook {
    pub id: String,
    pub now_secs: u64,
}

impl Command for RetireHook {
    type Output = HookEvent;
    type State = HooksRegistry;

    fn validate(&self, state: &Self::State) -> Result<(), AppError> {
        validate_hook_id(&self.id).map_err(AppError::Validation)?;
        if !state.contains(&self.id) {
            return Err(AppError::Validation(format!("hook {:?} is not registered", self.id)));
        }
        Ok(())
    }

    fn execute(&self, state: &mut Self::State) -> Result<Self::Output, AppError> {
        self.validate(state)?;
        state.apply_retire(&self.id);
        Ok(HookEvent::Retired { id: self.id.clone(), now_secs: self.now_secs })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn register_validates_and_upserts() {
        let mut s = HooksRegistry::default();
        let cmd = RegisterHook {
            id: "guard-rm".into(),
            event: "PreToolUse".into(),
            matcher: "Bash(rm -rf /*)".into(),
            command: "echo BLOCKED && exit 2".into(),
            target: HookTarget::default(),
            now_secs: 1,
        };
        let ev = cmd.execute(&mut s).unwrap();
        assert!(matches!(ev, HookEvent::Registered { .. }));
        assert!(s.contains("guard-rm"));

        // Re-register same id with a new command → upsert (no conflict).
        let edit = RegisterHook { command: "exit 2".into(), now_secs: 2, ..cmd.clone() };
        edit.execute(&mut s).unwrap();
        assert_eq!(s.get("guard-rm").unwrap().command, "exit 2");
        assert_eq!(s.len(), 1);
    }

    #[test]
    fn register_rejects_bad_event_and_empty_command() {
        let mut s = HooksRegistry::default();
        let bad_event = RegisterHook {
            id: "x".into(),
            event: "Bogus".into(),
            matcher: String::new(),
            command: "c".into(),
            target: HookTarget::default(),
            now_secs: 1,
        };
        assert!(bad_event.validate(&s).is_err());
        let empty_cmd = RegisterHook {
            id: "x".into(),
            event: "Stop".into(),
            matcher: String::new(),
            command: "   ".into(),
            target: HookTarget::default(),
            now_secs: 1,
        };
        assert!(empty_cmd.execute(&mut s).is_err());
        assert!(s.is_empty());
    }

    #[test]
    fn retire_requires_existing_hook() {
        let mut s = HooksRegistry::default();
        assert!(RetireHook { id: "ghost".into(), now_secs: 1 }.validate(&s).is_err());
        RegisterHook {
            id: "g".into(),
            event: "Stop".into(),
            matcher: String::new(),
            command: "c".into(),
            target: HookTarget::default(),
            now_secs: 1,
        }
        .execute(&mut s)
        .unwrap();
        RetireHook { id: "g".into(), now_secs: 2 }.execute(&mut s).unwrap();
        assert!(s.is_empty());
    }
}
