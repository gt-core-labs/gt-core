//! Owned `Command` structs over [`RigCatalog`].
//!
//! Same retrofit shape as gt-quota / gt-merge: each mutation an external client (a model via
//! `gt-mcp`, an admin via the gt-cli) can drive is a pure, sync [`Command`]. `validate`
//! inspects the catalog without mutating it; `execute` applies the mutation (delegating to
//! the shared `RigCatalog::apply_*` methods so the actor messages and this path stay in
//! lockstep) and returns the [`RigEvent`] for the actor to emit on the relay.

use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

use gt_events::{AppError, Command};

use crate::events::RigEvent;
use crate::state::{validate_prefix, validate_rig_name, RigCatalog, RigEntry};

/// Register a new rig in the catalog. The on-disk clone is the bootstrap edge's job; this
/// command only records that the orchestrator now routes work for the rig.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
pub struct AddRig {
    pub name: String,
    pub prefix: String,
    pub git_url: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub push_url: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub upstream_url: Option<String>,
    pub default_branch: String,
    /// UTC epoch seconds, stamped at the edge.
    pub now_secs: u64,
}

impl AddRig {
    fn validate_structure(&self) -> Result<(), AppError> {
        validate_rig_name(&self.name).map_err(AppError::Validation)?;
        validate_prefix(&self.prefix).map_err(AppError::Validation)?;
        if self.git_url.trim().is_empty() {
            return Err(AppError::Validation("git_url is empty".into()));
        }
        if self.default_branch.trim().is_empty() {
            return Err(AppError::Validation("default_branch is empty".into()));
        }
        Ok(())
    }

    fn validate_against(&self, state: &RigCatalog) -> Result<(), AppError> {
        if state.contains(&self.name) {
            return Err(AppError::Validation(format!(
                "rig {:?} already registered",
                self.name
            )));
        }
        if let Some(owner) = state.prefix_owner(&self.prefix) {
            return Err(AppError::Validation(format!(
                "prefix {:?} already used by rig {:?}",
                self.prefix, owner
            )));
        }
        Ok(())
    }

    fn entry(&self) -> RigEntry {
        RigEntry {
            name: self.name.clone(),
            prefix: self.prefix.clone(),
            git_url: self.git_url.clone(),
            push_url: self.push_url.clone(),
            upstream_url: self.upstream_url.clone(),
            default_branch: self.default_branch.clone(),
            registered_at_secs: self.now_secs,
        }
    }
}

impl Command for AddRig {
    type Output = RigEvent;
    type State = RigCatalog;

    fn validate(&self, state: &Self::State) -> Result<(), AppError> {
        self.validate_structure()?;
        self.validate_against(state)
    }

    fn execute(&self, state: &mut Self::State) -> Result<Self::Output, AppError> {
        self.validate(state)?;
        state.apply_add(self.entry());
        Ok(RigEvent::Added {
            rig: self.name.clone(),
            prefix: self.prefix.clone(),
            git_url: self.git_url.clone(),
            push_url: self.push_url.clone(),
            upstream_url: self.upstream_url.clone(),
            default_branch: self.default_branch.clone(),
            now_secs: self.now_secs,
        })
    }
}

/// Adopt an existing on-disk rig directory into the catalog. Same structural validation as
/// [`AddRig`]; emits the [`RigEvent::Adopted`] kind so the audit log distinguishes the two
/// paths.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
pub struct AdoptRig {
    pub name: String,
    pub prefix: String,
    pub git_url: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub push_url: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub upstream_url: Option<String>,
    pub default_branch: String,
    pub now_secs: u64,
}

impl AdoptRig {
    fn as_add(&self) -> AddRig {
        AddRig {
            name: self.name.clone(),
            prefix: self.prefix.clone(),
            git_url: self.git_url.clone(),
            push_url: self.push_url.clone(),
            upstream_url: self.upstream_url.clone(),
            default_branch: self.default_branch.clone(),
            now_secs: self.now_secs,
        }
    }
}

impl Command for AdoptRig {
    type Output = RigEvent;
    type State = RigCatalog;

    fn validate(&self, state: &Self::State) -> Result<(), AppError> {
        self.as_add().validate(state)
    }

    fn execute(&self, state: &mut Self::State) -> Result<Self::Output, AppError> {
        let add = self.as_add();
        add.validate(state)?;
        state.apply_add(add.entry());
        Ok(RigEvent::Adopted {
            rig: self.name.clone(),
            prefix: self.prefix.clone(),
            git_url: self.git_url.clone(),
            push_url: self.push_url.clone(),
            upstream_url: self.upstream_url.clone(),
            default_branch: self.default_branch.clone(),
            now_secs: self.now_secs,
        })
    }
}

/// Drop a rig from the catalog.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
pub struct RemoveRig {
    pub name: String,
    pub now_secs: u64,
}

impl Command for RemoveRig {
    type Output = RigEvent;
    type State = RigCatalog;

    fn validate(&self, state: &Self::State) -> Result<(), AppError> {
        if self.name.is_empty() {
            return Err(AppError::Validation("rig name is empty".into()));
        }
        if !state.contains(&self.name) {
            return Err(AppError::NotFound(format!("rig {:?}", self.name)));
        }
        Ok(())
    }

    fn execute(&self, state: &mut Self::State) -> Result<Self::Output, AppError> {
        self.validate(state)?;
        state.apply_remove(&self.name);
        Ok(RigEvent::Removed {
            rig: self.name.clone(),
            now_secs: self.now_secs,
        })
    }
}

/// Change the beads prefix associated with a rig. The corresponding `bd config set
/// issue_prefix` is a deploy-edge side-effect; this command records the transition.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
pub struct SetRigPrefix {
    pub name: String,
    pub new_prefix: String,
    pub now_secs: u64,
}

impl Command for SetRigPrefix {
    type Output = RigEvent;
    type State = RigCatalog;

    fn validate(&self, state: &Self::State) -> Result<(), AppError> {
        if self.name.is_empty() {
            return Err(AppError::Validation("rig name is empty".into()));
        }
        validate_prefix(&self.new_prefix).map_err(AppError::Validation)?;
        let Some(current) = state.get(&self.name) else {
            return Err(AppError::NotFound(format!("rig {:?}", self.name)));
        };
        if current.prefix == self.new_prefix {
            return Err(AppError::Validation(format!(
                "prefix already {:?}",
                self.new_prefix
            )));
        }
        if let Some(owner) = state.prefix_owner(&self.new_prefix) {
            if owner != self.name {
                return Err(AppError::Validation(format!(
                    "prefix {:?} already used by rig {:?}",
                    self.new_prefix, owner
                )));
            }
        }
        Ok(())
    }

    fn execute(&self, state: &mut Self::State) -> Result<Self::Output, AppError> {
        self.validate(state)?;
        // `validate` proved the rig exists; unwrap is safe.
        let old = state.get(&self.name).expect("rig present").prefix.clone();
        state.apply_prefix_change(&self.name, &self.new_prefix);
        Ok(RigEvent::PrefixChanged {
            rig: self.name.clone(),
            old,
            new: self.new_prefix.clone(),
            now_secs: self.now_secs,
        })
    }
}

/// Change the default branch tracked for the rig.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
pub struct SetRigDefaultBranch {
    pub name: String,
    pub new_branch: String,
    pub now_secs: u64,
}

impl Command for SetRigDefaultBranch {
    type Output = RigEvent;
    type State = RigCatalog;

    fn validate(&self, state: &Self::State) -> Result<(), AppError> {
        if self.name.is_empty() {
            return Err(AppError::Validation("rig name is empty".into()));
        }
        if self.new_branch.trim().is_empty() {
            return Err(AppError::Validation("new_branch is empty".into()));
        }
        let Some(current) = state.get(&self.name) else {
            return Err(AppError::NotFound(format!("rig {:?}", self.name)));
        };
        if current.default_branch == self.new_branch {
            return Err(AppError::Validation(format!(
                "default branch already {:?}",
                self.new_branch
            )));
        }
        Ok(())
    }

    fn execute(&self, state: &mut Self::State) -> Result<Self::Output, AppError> {
        self.validate(state)?;
        let old = state
            .get(&self.name)
            .expect("rig present")
            .default_branch
            .clone();
        state.apply_default_branch_change(&self.name, &self.new_branch);
        Ok(RigEvent::DefaultBranchChanged {
            rig: self.name.clone(),
            old,
            new: self.new_branch.clone(),
            now_secs: self.now_secs,
        })
    }
}

/// Sum type so the actor routes any rig command through a single message variant.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum RigCommand {
    Add(AddRig),
    Adopt(AdoptRig),
    Remove(RemoveRig),
    SetPrefix(SetRigPrefix),
    SetDefaultBranch(SetRigDefaultBranch),
}

impl RigCommand {
    /// Stable tool base name used by `gt-mcp` to dispatch. Matches the pattern in `docs/09`.
    pub fn tool_name(&self) -> &'static str {
        match self {
            Self::Add(_) => "rig.add",
            Self::Adopt(_) => "rig.adopt",
            Self::Remove(_) => "rig.remove",
            Self::SetPrefix(_) => "rig.set-prefix",
            Self::SetDefaultBranch(_) => "rig.set-default-branch",
        }
    }
}

impl Command for RigCommand {
    type Output = RigEvent;
    type State = RigCatalog;

    fn validate(&self, state: &Self::State) -> Result<(), AppError> {
        match self {
            Self::Add(c) => c.validate(state),
            Self::Adopt(c) => c.validate(state),
            Self::Remove(c) => c.validate(state),
            Self::SetPrefix(c) => c.validate(state),
            Self::SetDefaultBranch(c) => c.validate(state),
        }
    }

    fn execute(&self, state: &mut Self::State) -> Result<Self::Output, AppError> {
        match self {
            Self::Add(c) => c.execute(state),
            Self::Adopt(c) => c.execute(state),
            Self::Remove(c) => c.execute(state),
            Self::SetPrefix(c) => c.execute(state),
            Self::SetDefaultBranch(c) => c.execute(state),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn add_cmd(name: &str, prefix: &str, now: u64) -> AddRig {
        AddRig {
            name: name.into(),
            prefix: prefix.into(),
            git_url: format!("git@github.com:o/{name}.git"),
            push_url: None,
            upstream_url: None,
            default_branch: "main".into(),
            now_secs: now,
        }
    }

    #[test]
    fn add_rejects_invalid_name_and_collisions() {
        let mut catalog = RigCatalog::default();
        let ok = add_cmd("plane", "pl", 1);
        let ev = ok.execute(&mut catalog).unwrap();
        assert!(matches!(ev, RigEvent::Added { .. }));

        let dup_name = add_cmd("plane", "pl2", 2);
        assert!(matches!(
            dup_name.validate(&catalog),
            Err(AppError::Validation(_))
        ));

        let dup_prefix = add_cmd("other", "pl", 3);
        assert!(matches!(
            dup_prefix.validate(&catalog),
            Err(AppError::Validation(_))
        ));

        let bad_name = add_cmd("hq", "hh", 4);
        assert!(matches!(
            bad_name.validate(&catalog),
            Err(AppError::Validation(_))
        ));

        let bad_prefix = add_cmd("ok", "1bad", 5);
        assert!(matches!(
            bad_prefix.validate(&catalog),
            Err(AppError::Validation(_))
        ));
    }

    #[test]
    fn validate_does_not_mutate() {
        let mut catalog = RigCatalog::default();
        add_cmd("plane", "pl", 1).execute(&mut catalog).unwrap();
        let before = catalog.snapshot();
        let cmd = add_cmd("gastown", "gt", 2);
        cmd.validate(&catalog).unwrap();
        assert_eq!(catalog.snapshot(), before, "validate must not mutate");
    }

    #[test]
    fn adopt_emits_distinct_kind() {
        let mut catalog = RigCatalog::default();
        let cmd = AdoptRig {
            name: "plane".into(),
            prefix: "pl".into(),
            git_url: "git@github.com:o/plane.git".into(),
            push_url: None,
            upstream_url: None,
            default_branch: "main".into(),
            now_secs: 10,
        };
        let ev = cmd.execute(&mut catalog).unwrap();
        assert!(matches!(ev, RigEvent::Adopted { .. }));
        assert!(catalog.contains("plane"));
    }

    #[test]
    fn remove_round_trip() {
        let mut catalog = RigCatalog::default();
        add_cmd("plane", "pl", 1).execute(&mut catalog).unwrap();
        let rm = RemoveRig {
            name: "plane".into(),
            now_secs: 9,
        };
        let ev = rm.execute(&mut catalog).unwrap();
        assert!(matches!(ev, RigEvent::Removed { .. }));
        assert!(!catalog.contains("plane"));
        assert!(matches!(rm.validate(&catalog), Err(AppError::NotFound(_))));
    }

    #[test]
    fn set_prefix_rejects_collision_and_no_op() {
        let mut catalog = RigCatalog::default();
        add_cmd("plane", "pl", 1).execute(&mut catalog).unwrap();
        add_cmd("gastown", "gt", 2)
            .execute(&mut catalog)
            .unwrap();

        let no_op = SetRigPrefix {
            name: "plane".into(),
            new_prefix: "pl".into(),
            now_secs: 3,
        };
        assert!(matches!(
            no_op.validate(&catalog),
            Err(AppError::Validation(_))
        ));

        let collide = SetRigPrefix {
            name: "plane".into(),
            new_prefix: "gt".into(),
            now_secs: 4,
        };
        assert!(matches!(
            collide.validate(&catalog),
            Err(AppError::Validation(_))
        ));

        let ok = SetRigPrefix {
            name: "plane".into(),
            new_prefix: "pln".into(),
            now_secs: 5,
        };
        let ev = ok.execute(&mut catalog).unwrap();
        match ev {
            RigEvent::PrefixChanged { old, new, .. } => {
                assert_eq!(old, "pl");
                assert_eq!(new, "pln");
            }
            _ => panic!("expected PrefixChanged"),
        }
        assert_eq!(catalog.prefix_owner("pln"), Some("plane"));
        assert!(catalog.prefix_owner("pl").is_none());
    }

    #[test]
    fn set_default_branch_round_trip() {
        let mut catalog = RigCatalog::default();
        add_cmd("plane", "pl", 1).execute(&mut catalog).unwrap();
        let cmd = SetRigDefaultBranch {
            name: "plane".into(),
            new_branch: "master".into(),
            now_secs: 2,
        };
        let ev = cmd.execute(&mut catalog).unwrap();
        assert!(matches!(ev, RigEvent::DefaultBranchChanged { .. }));
        assert_eq!(catalog.get("plane").unwrap().default_branch, "master");
    }

    #[test]
    fn command_routes_through_sum_type() {
        let mut catalog = RigCatalog::default();
        let cmd = RigCommand::Add(add_cmd("plane", "pl", 1));
        assert_eq!(cmd.tool_name(), "rig.add");
        let ev = cmd.execute(&mut catalog).unwrap();
        assert!(matches!(ev, RigEvent::Added { .. }));
    }
}
