//! Owned `Command` structs over [`RigCatalog`].
//!
//! Same retrofit shape as gt-quota / gt-merge: each mutation an external client (a model via
//! `gt-mcp`, an admin via the gt-cli) can drive is a pure, sync [`Command`]. `validate`
//! inspects the catalog without mutating it; `execute` applies the mutation (delegating to
//! the shared `RigCatalog::apply_*` methods so the actor messages and this path stay in
//! lockstep) and returns the [`RigEvent`] for the actor to emit on the relay.

use std::path::PathBuf;

use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

use gt_events::{AppError, Command};

use crate::events::RigEvent;
use crate::state::{
    validate_prefix, validate_rig_name, validate_worktree_root, RigCatalog, RigEntry,
};

/// Default tenant for a command built without an explicit workspace. The server stamps the
/// real one from the auth context (see [`RigCommand::in_workspace`]); a command parsed from
/// external tool args lands here because `workspace_id` is `skip_deserializing` (anti-spoof).
fn default_workspace() -> String {
    "default".to_string()
}

/// Register a new rig in the catalog. The on-disk clone is the bootstrap edge's job; this
/// command only records that the orchestrator now routes work for the rig.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
pub struct AddRig {
    pub name: String,
    /// Bead-id prefix for this rig's beads. When absent or empty, defaults to `name`
    /// (hq-bead-id-standard.4: canonical names carry the prefix namespace directly).
    /// Must be a valid prefix when explicitly provided (alphanumeric + hyphens, ≤20 chars).
    #[serde(default)]
    pub prefix: String,
    pub git_url: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub push_url: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub upstream_url: Option<String>,
    pub default_branch: String,
    /// Optional soft reference to the `public.vcs_connections.id` (hq-vcs-connections.1) this
    /// rig clones with. Caller-supplied (unlike `workspace_id`); `None` registers a rig with no
    /// VCS connection bound yet (hq-vcs-connections.3).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub git_connection_ref: Option<String>,
    /// UTC epoch seconds, stamped at the edge.
    pub now_secs: u64,
    /// Server-injected tenant. NOT read from the tool payload (`skip_deserializing`) and
    /// hidden from the tool schema (`schemars(skip)`) so an external caller cannot spoof
    /// which workspace a rig mutation targets; the server stamps it from the auth context
    /// via [`RigCommand::in_workspace`] (hq-mt-rigs.1).
    #[serde(skip_deserializing, default = "default_workspace")]
    #[schemars(skip)]
    pub workspace_id: String,
}

impl AddRig {
    /// Effective prefix: the caller-supplied value if non-empty, else the rig name.
    fn effective_prefix(&self) -> &str {
        if self.prefix.is_empty() { &self.name } else { &self.prefix }
    }

    fn validate_structure(&self) -> Result<(), AppError> {
        validate_rig_name(&self.name).map_err(AppError::Validation)?;
        validate_prefix(self.effective_prefix()).map_err(AppError::Validation)?;
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
        if let Some(owner) = state.prefix_owner(self.effective_prefix()) {
            return Err(AppError::Validation(format!(
                "prefix {:?} already used by rig {:?}",
                self.effective_prefix(), owner
            )));
        }
        Ok(())
    }

    fn entry(&self) -> RigEntry {
        RigEntry {
            name: self.name.clone(),
            prefix: self.effective_prefix().to_string(),
            git_url: self.git_url.clone(),
            push_url: self.push_url.clone(),
            upstream_url: self.upstream_url.clone(),
            default_branch: self.default_branch.clone(),
            registered_at_secs: self.now_secs,
            worktree_root: None,
            git_connection_ref: self.git_connection_ref.clone(),
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
            prefix: self.effective_prefix().to_string(),
            git_url: self.git_url.clone(),
            push_url: self.push_url.clone(),
            upstream_url: self.upstream_url.clone(),
            default_branch: self.default_branch.clone(),
            git_connection_ref: self.git_connection_ref.clone(),
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
    /// See [`AddRig::prefix`] — defaults to `name` when absent (hq-bead-id-standard.4).
    #[serde(default)]
    pub prefix: String,
    pub git_url: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub push_url: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub upstream_url: Option<String>,
    pub default_branch: String,
    /// See [`AddRig::git_connection_ref`] — optional soft ref to the VCS connection.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub git_connection_ref: Option<String>,
    pub now_secs: u64,
    /// Server-injected tenant — see [`AddRig::workspace_id`].
    #[serde(skip_deserializing, default = "default_workspace")]
    #[schemars(skip)]
    pub workspace_id: String,
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
            git_connection_ref: self.git_connection_ref.clone(),
            now_secs: self.now_secs,
            workspace_id: self.workspace_id.clone(),
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
        let entry = add.entry();
        let effective_prefix = entry.prefix.clone();
        state.apply_add(entry);
        Ok(RigEvent::Adopted {
            rig: self.name.clone(),
            prefix: effective_prefix,
            git_url: self.git_url.clone(),
            push_url: self.push_url.clone(),
            upstream_url: self.upstream_url.clone(),
            default_branch: self.default_branch.clone(),
            git_connection_ref: self.git_connection_ref.clone(),
            now_secs: self.now_secs,
        })
    }
}

/// Drop a rig from the catalog.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
pub struct RemoveRig {
    pub name: String,
    pub now_secs: u64,
    /// Server-injected tenant — see [`AddRig::workspace_id`].
    #[serde(skip_deserializing, default = "default_workspace")]
    #[schemars(skip)]
    pub workspace_id: String,
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
    /// Server-injected tenant — see [`AddRig::workspace_id`].
    #[serde(skip_deserializing, default = "default_workspace")]
    #[schemars(skip)]
    pub workspace_id: String,
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
    /// Server-injected tenant — see [`AddRig::workspace_id`].
    #[serde(skip_deserializing, default = "default_workspace")]
    #[schemars(skip)]
    pub workspace_id: String,
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

/// Pin the worktree root the orchestrator carves polecat checkouts under for a rig. The
/// matching filesystem move is a deploy-edge side-effect; this command records the override.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
pub struct SetRigWorktreeRoot {
    pub name: String,
    pub new_root: PathBuf,
    pub now_secs: u64,
    /// Server-injected tenant — see [`AddRig::workspace_id`].
    #[serde(skip_deserializing, default = "default_workspace")]
    #[schemars(skip)]
    pub workspace_id: String,
}

impl Command for SetRigWorktreeRoot {
    type Output = RigEvent;
    type State = RigCatalog;

    fn validate(&self, state: &Self::State) -> Result<(), AppError> {
        if self.name.is_empty() {
            return Err(AppError::Validation("rig name is empty".into()));
        }
        validate_worktree_root(&self.new_root).map_err(AppError::Validation)?;
        let Some(current) = state.get(&self.name) else {
            return Err(AppError::NotFound(format!("rig {:?}", self.name)));
        };
        if current.worktree_root.as_deref() == Some(self.new_root.as_path()) {
            return Err(AppError::Validation(format!(
                "worktree_root already {:?}",
                self.new_root.display()
            )));
        }
        Ok(())
    }

    fn execute(&self, state: &mut Self::State) -> Result<Self::Output, AppError> {
        self.validate(state)?;
        // `validate` proved the rig exists; unwrap is safe.
        let old = state
            .get(&self.name)
            .expect("rig present")
            .worktree_root
            .clone();
        state.apply_worktree_root_change(&self.name, Some(self.new_root.clone()));
        Ok(RigEvent::WorktreeRootChanged {
            rig: self.name.clone(),
            old,
            new: self.new_root.clone(),
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
    SetWorktreeRoot(SetRigWorktreeRoot),
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
            Self::SetWorktreeRoot(_) => "rig.set-worktree-root",
        }
    }

    /// The tenant this command targets. Server-injected; `"default"` until the server stamps
    /// the resolved workspace (the dispatch layer routes the catalog by this value).
    pub fn workspace_id(&self) -> &str {
        match self {
            Self::Add(c) => &c.workspace_id,
            Self::Adopt(c) => &c.workspace_id,
            Self::Remove(c) => &c.workspace_id,
            Self::SetPrefix(c) => &c.workspace_id,
            Self::SetDefaultBranch(c) => &c.workspace_id,
            Self::SetWorktreeRoot(c) => &c.workspace_id,
        }
    }

    /// Stamp the resolved tenant onto the command (builder style). The dispatch layer parses
    /// a command from the tool args — where `workspace_id` is `skip_deserializing`, so the
    /// caller cannot set it — then calls this with the workspace from the auth context. This
    /// is the only path that sets `workspace_id`, which is what makes it server-injected.
    pub fn in_workspace(mut self, workspace_id: impl Into<String>) -> Self {
        let ws = workspace_id.into();
        match &mut self {
            Self::Add(c) => c.workspace_id = ws,
            Self::Adopt(c) => c.workspace_id = ws,
            Self::Remove(c) => c.workspace_id = ws,
            Self::SetPrefix(c) => c.workspace_id = ws,
            Self::SetDefaultBranch(c) => c.workspace_id = ws,
            Self::SetWorktreeRoot(c) => c.workspace_id = ws,
        }
        self
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
            Self::SetWorktreeRoot(c) => c.validate(state),
        }
    }

    fn execute(&self, state: &mut Self::State) -> Result<Self::Output, AppError> {
        match self {
            Self::Add(c) => c.execute(state),
            Self::Adopt(c) => c.execute(state),
            Self::Remove(c) => c.execute(state),
            Self::SetPrefix(c) => c.execute(state),
            Self::SetDefaultBranch(c) => c.execute(state),
            Self::SetWorktreeRoot(c) => c.execute(state),
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
            git_connection_ref: None,
            now_secs: now,
            workspace_id: "default".into(),
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
            git_connection_ref: None,
            now_secs: 10,
            workspace_id: "default".into(),
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
            workspace_id: "default".into(),
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
        add_cmd("gastown", "gt", 2).execute(&mut catalog).unwrap();

        let no_op = SetRigPrefix {
            name: "plane".into(),
            new_prefix: "pl".into(),
            now_secs: 3,
            workspace_id: "default".into(),
        };
        assert!(matches!(
            no_op.validate(&catalog),
            Err(AppError::Validation(_))
        ));

        let collide = SetRigPrefix {
            name: "plane".into(),
            new_prefix: "gt".into(),
            now_secs: 4,
            workspace_id: "default".into(),
        };
        assert!(matches!(
            collide.validate(&catalog),
            Err(AppError::Validation(_))
        ));

        let ok = SetRigPrefix {
            name: "plane".into(),
            new_prefix: "pln".into(),
            now_secs: 5,
            workspace_id: "default".into(),
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
            workspace_id: "default".into(),
        };
        let ev = cmd.execute(&mut catalog).unwrap();
        assert!(matches!(ev, RigEvent::DefaultBranchChanged { .. }));
        assert_eq!(catalog.get("plane").unwrap().default_branch, "master");
    }

    #[test]
    fn set_worktree_root_round_trip_and_validation() {
        let mut catalog = RigCatalog::default();
        add_cmd("plane", "pl", 1).execute(&mut catalog).unwrap();

        // Relative path is rejected before the rig is even consulted.
        let relative = SetRigWorktreeRoot {
            name: "plane".into(),
            new_root: "relative/path".into(),
            now_secs: 2,
            workspace_id: "default".into(),
        };
        assert!(matches!(
            relative.validate(&catalog),
            Err(AppError::Validation(_))
        ));

        // Unknown rig is a NotFound.
        let missing = SetRigWorktreeRoot {
            name: "ghost".into(),
            new_root: "/srv/wt/ghost".into(),
            now_secs: 3,
            workspace_id: "default".into(),
        };
        assert!(matches!(
            missing.validate(&catalog),
            Err(AppError::NotFound(_))
        ));

        let ok = SetRigWorktreeRoot {
            name: "plane".into(),
            new_root: "/srv/wt/plane".into(),
            now_secs: 4,
            workspace_id: "default".into(),
        };
        let ev = ok.execute(&mut catalog).unwrap();
        match ev {
            RigEvent::WorktreeRootChanged { old, new, .. } => {
                assert_eq!(old, None);
                assert_eq!(new, PathBuf::from("/srv/wt/plane"));
            }
            _ => panic!("expected WorktreeRootChanged"),
        }
        assert_eq!(
            catalog.get("plane").unwrap().worktree_root,
            Some(PathBuf::from("/srv/wt/plane"))
        );

        // Re-pinning the same root is a no-op rejection.
        assert!(matches!(
            ok.validate(&catalog),
            Err(AppError::Validation(_))
        ));
    }

    #[test]
    fn command_routes_through_sum_type() {
        let mut catalog = RigCatalog::default();
        let cmd = RigCommand::Add(add_cmd("plane", "pl", 1));
        assert_eq!(cmd.tool_name(), "rig.add");
        let ev = cmd.execute(&mut catalog).unwrap();
        assert!(matches!(ev, RigEvent::Added { .. }));
    }

    #[test]
    fn add_defaults_prefix_to_name_when_absent(  ) {
        // hq-bead-id-standard.4: omitting `prefix` in the payload makes it default to `name`.
        let json_no_prefix = serde_json::json!({
            "name": "gtweb", "git_url": "git@g:o/gtweb.git",
            "default_branch": "main", "now_secs": 1
        });
        let cmd: AddRig = serde_json::from_value(json_no_prefix).unwrap();
        assert!(cmd.prefix.is_empty(), "deserialized prefix should be empty string");
        assert_eq!(cmd.effective_prefix(), "gtweb", "effective_prefix falls back to name");
        let mut catalog = RigCatalog::default();
        let ev = cmd.execute(&mut catalog).unwrap();
        if let RigEvent::Added { prefix, .. } = ev {
            assert_eq!(prefix, "gtweb");
        } else {
            panic!("expected Added event");
        }
    }

    #[test]
    fn workspace_id_is_not_spoofable_from_payload_and_server_stamps_it() {
        // A caller embeds `workspace_id` in the tool args trying to target another tenant.
        let spoofed = serde_json::json!({
            "name": "plane", "prefix": "pl", "git_url": "git@x:y/p.git",
            "default_branch": "main", "now_secs": 1, "workspace_id": "victim"
        });
        let cmd: AddRig = serde_json::from_value(spoofed).unwrap();
        // skip_deserializing ignored the payload value: it lands on the default tenant.
        assert_eq!(
            cmd.workspace_id, "default",
            "payload must not set workspace_id"
        );

        // The server stamps the resolved tenant via the sum-type builder.
        let stamped = RigCommand::Add(cmd).in_workspace("acme");
        assert_eq!(stamped.workspace_id(), "acme");
    }
}
