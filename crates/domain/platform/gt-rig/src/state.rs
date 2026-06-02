//! Domain state + replay reducer.
//!
//! [`RigCatalog`] is the mutable state the actor owns; [`RigState`] is the version rebuilt
//! from the log for the Step 3 gate (deterministic replay): the live state must match the
//! rebuilt one byte-for-byte.

use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};

use crate::events::RigEvent;

/// Town-level names that cannot be used for rigs because they collide with town-level
/// infrastructure. Mirrors `reservedRigNames` in `internal/rig/manager.go`.
pub const RESERVED_RIG_NAMES: &[&str] = &["hq"];

/// Maximum length of a beads prefix. Mirrors the Go validator in
/// `internal/rig/manager.go::isValidBeadsPrefix`.
pub const MAX_PREFIX_LEN: usize = 20;

/// A rig entry in the catalog. Identity is `name`; the rest is what the orchestrator needs
/// to route work and reason about the rig's git topology. Mirrors the orchestrator-relevant
/// subset of Go `RigConfig` (drops filesystem-only fields like `local_repo`, polecat pool
/// sizing, etc. — those live at the deploy edge).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RigEntry {
    pub name: String,
    pub prefix: String,
    pub git_url: String,
    pub push_url: Option<String>,
    pub upstream_url: Option<String>,
    pub default_branch: String,
    /// UTC epoch seconds the orchestrator first learned about this rig.
    pub registered_at_secs: u64,
}

impl RigEntry {
    pub fn new(
        name: impl Into<String>,
        prefix: impl Into<String>,
        git_url: impl Into<String>,
        default_branch: impl Into<String>,
        registered_at_secs: u64,
    ) -> Self {
        Self {
            name: name.into(),
            prefix: prefix.into(),
            git_url: git_url.into(),
            push_url: None,
            upstream_url: None,
            default_branch: default_branch.into(),
            registered_at_secs,
        }
    }
}

/// Live rig catalog (what the actor owns). `BTreeMap` so iteration is sorted and the
/// snapshot is deterministic across replay / debug dumps.
#[derive(Debug, Default, Clone, PartialEq, Eq)]
pub struct RigCatalog {
    rigs: BTreeMap<String, RigEntry>,
    /// Prefix → rig name reverse index. Maintained as a derived view so collision checks are
    /// O(1) without scanning the catalog. Always kept in sync with `rigs` via the `apply_*`
    /// methods below — direct mutation is not exposed.
    prefix_index: BTreeMap<String, String>,
}

impl RigCatalog {
    pub fn get(&self, name: &str) -> Option<&RigEntry> {
        self.rigs.get(name)
    }

    pub fn contains(&self, name: &str) -> bool {
        self.rigs.contains_key(name)
    }

    /// Owner of `prefix`, if any. Used by the validator to reject prefix collisions before
    /// the command reaches the actor.
    pub fn prefix_owner(&self, prefix: &str) -> Option<&str> {
        self.prefix_index.get(prefix).map(String::as_str)
    }

    pub fn rigs(&self) -> impl Iterator<Item = &RigEntry> {
        self.rigs.values()
    }

    pub fn len(&self) -> usize {
        self.rigs.len()
    }

    pub fn is_empty(&self) -> bool {
        self.rigs.is_empty()
    }

    /// Insert a new entry into the catalog. Mirrors the actor's `Add`/`Adopted` mutation;
    /// shared by [`crate::RigCommand::execute`] and the actor's direct messages so both paths
    /// touch state identically.
    pub fn apply_add(&mut self, entry: RigEntry) {
        self.prefix_index
            .insert(entry.prefix.clone(), entry.name.clone());
        self.rigs.insert(entry.name.clone(), entry);
    }

    pub fn apply_remove(&mut self, name: &str) -> bool {
        match self.rigs.remove(name) {
            Some(entry) => {
                // Only drop the prefix index entry if it still points at this rig — a
                // PrefixChanged that races with Remove could leave a stale mapping otherwise.
                if self.prefix_index.get(&entry.prefix).map(String::as_str) == Some(name) {
                    self.prefix_index.remove(&entry.prefix);
                }
                true
            }
            None => false,
        }
    }

    pub fn apply_prefix_change(&mut self, name: &str, new_prefix: &str) -> bool {
        let Some(entry) = self.rigs.get_mut(name) else {
            return false;
        };
        let old_prefix = std::mem::replace(&mut entry.prefix, new_prefix.to_string());
        if self.prefix_index.get(&old_prefix).map(String::as_str) == Some(name) {
            self.prefix_index.remove(&old_prefix);
        }
        self.prefix_index
            .insert(new_prefix.to_string(), name.to_string());
        true
    }

    pub fn apply_default_branch_change(&mut self, name: &str, new_branch: &str) -> bool {
        match self.rigs.get_mut(name) {
            Some(entry) => {
                entry.default_branch = new_branch.to_string();
                true
            }
            None => false,
        }
    }

    /// Snapshot of the catalog as a sorted vector. Cheap clone; used by the actor's
    /// `Snapshot` reply path.
    pub fn snapshot(&self) -> Vec<RigEntry> {
        self.rigs.values().cloned().collect()
    }

    /// Rebuild a live catalog from the replay reducer (boot hydration). Symmetric to
    /// `AccountRegistry::from_state` in gt-quota.
    pub fn from_state(state: &RigState) -> Self {
        let mut catalog = RigCatalog::default();
        for entry in state.rigs.values() {
            catalog.apply_add(entry.clone());
        }
        catalog
    }
}

/// Pure reducer: rebuilds the consolidated state from the log. Used as the Step 3 gate
/// (`docs/06-observability.md`).
#[derive(Debug, Default, Clone, PartialEq, Eq)]
pub struct RigState {
    pub rigs: BTreeMap<String, RigEntry>,
    /// Ordered sequence of removed rig names — observable history is part of the gate.
    pub removed: Vec<String>,
    /// Sequence of `(rig, old_prefix, new_prefix)` prefix transitions.
    pub prefix_changes: Vec<(String, String, String)>,
    /// Sequence of `(rig, old_branch, new_branch)` default-branch transitions.
    pub default_branch_changes: Vec<(String, String, String)>,
}

impl RigState {
    pub fn apply(&mut self, event: &RigEvent) {
        match event {
            RigEvent::Added {
                rig,
                prefix,
                git_url,
                push_url,
                upstream_url,
                default_branch,
                now_secs,
            }
            | RigEvent::Adopted {
                rig,
                prefix,
                git_url,
                push_url,
                upstream_url,
                default_branch,
                now_secs,
            } => {
                self.rigs.insert(
                    rig.clone(),
                    RigEntry {
                        name: rig.clone(),
                        prefix: prefix.clone(),
                        git_url: git_url.clone(),
                        push_url: push_url.clone(),
                        upstream_url: upstream_url.clone(),
                        default_branch: default_branch.clone(),
                        registered_at_secs: *now_secs,
                    },
                );
            }
            RigEvent::Removed { rig, .. } => {
                if self.rigs.remove(rig).is_some() {
                    self.removed.push(rig.clone());
                }
            }
            RigEvent::PrefixChanged { rig, old, new, .. } => {
                if let Some(entry) = self.rigs.get_mut(rig) {
                    entry.prefix = new.clone();
                    self.prefix_changes
                        .push((rig.clone(), old.clone(), new.clone()));
                }
            }
            RigEvent::DefaultBranchChanged { rig, old, new, .. } => {
                if let Some(entry) = self.rigs.get_mut(rig) {
                    entry.default_branch = new.clone();
                    self.default_branch_changes
                        .push((rig.clone(), old.clone(), new.clone()));
                }
            }
        }
    }
}

/// Validate a rig name. Mirrors Go `AddRig`: hyphens, dots, spaces, slashes and backslashes
/// are rejected because the agent-id parser uses hyphens as field separators; `hq` is
/// reserved for town-level infrastructure (`EnsureMetadata` / dolt routing).
pub fn validate_rig_name(name: &str) -> Result<(), String> {
    if name.is_empty() {
        return Err("rig name is empty".into());
    }
    if name.chars().any(|c| matches!(c, '-' | '.' | ' ' | '/' | '\\')) {
        return Err(format!(
            "rig name {name:?} contains invalid characters; hyphens, dots, spaces, and path \
             separators are not allowed"
        ));
    }
    for reserved in RESERVED_RIG_NAMES {
        if name.eq_ignore_ascii_case(reserved) {
            return Err(format!(
                "rig name {name:?} is reserved for town-level infrastructure"
            ));
        }
    }
    Ok(())
}

/// Validate a beads prefix. Mirrors Go `isValidBeadsPrefix`: alphanumeric with optional
/// hyphens, must start with a letter, max 20 chars.
pub fn validate_prefix(prefix: &str) -> Result<(), String> {
    if prefix.is_empty() {
        return Err("prefix is empty".into());
    }
    if prefix.len() > MAX_PREFIX_LEN {
        return Err(format!(
            "prefix {prefix:?} exceeds max length {MAX_PREFIX_LEN}"
        ));
    }
    let mut chars = prefix.chars();
    match chars.next() {
        Some(c) if c.is_ascii_alphabetic() => {}
        Some(_) => return Err(format!("prefix {prefix:?} must start with a letter")),
        None => unreachable!("non-empty prefix"),
    }
    for c in chars {
        if !(c.is_ascii_alphanumeric() || c == '-') {
            return Err(format!(
                "prefix {prefix:?} contains invalid character {c:?}; only alphanumerics and \
                 hyphens allowed"
            ));
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn validate_name_rejects_hyphen_and_reserved() {
        assert!(validate_rig_name("plane").is_ok());
        assert!(validate_rig_name("gas_town").is_ok());
        assert!(validate_rig_name("my-rig").is_err(), "hyphen forbidden");
        assert!(validate_rig_name("a.b").is_err(), "dot forbidden");
        assert!(validate_rig_name("hq").is_err(), "reserved");
        assert!(validate_rig_name("HQ").is_err(), "reserved case-insensitive");
        assert!(validate_rig_name("").is_err());
    }

    #[test]
    fn validate_prefix_enforces_grammar_and_length() {
        assert!(validate_prefix("gt").is_ok());
        assert!(validate_prefix("plane-fe").is_ok());
        assert!(validate_prefix("1gt").is_err(), "must start with letter");
        assert!(validate_prefix("gt_x").is_err(), "underscore not allowed");
        assert!(validate_prefix("a".repeat(MAX_PREFIX_LEN + 1).as_str()).is_err());
        assert!(validate_prefix("").is_err());
    }

    #[test]
    fn catalog_round_trips_through_state() {
        let mut catalog = RigCatalog::default();
        catalog.apply_add(RigEntry::new(
            "plane",
            "pl",
            "git@github.com:o/plane.git",
            "main",
            1,
        ));
        catalog.apply_add(RigEntry::new(
            "gastown",
            "gt",
            "git@github.com:o/gastown.git",
            "main",
            2,
        ));
        assert_eq!(catalog.prefix_owner("pl"), Some("plane"));
        assert_eq!(catalog.prefix_owner("gt"), Some("gastown"));

        let mut state = RigState::default();
        state.apply(&RigEvent::Added {
            rig: "plane".into(),
            prefix: "pl".into(),
            git_url: "git@github.com:o/plane.git".into(),
            push_url: None,
            upstream_url: None,
            default_branch: "main".into(),
            now_secs: 1,
        });
        state.apply(&RigEvent::Added {
            rig: "gastown".into(),
            prefix: "gt".into(),
            git_url: "git@github.com:o/gastown.git".into(),
            push_url: None,
            upstream_url: None,
            default_branch: "main".into(),
            now_secs: 2,
        });

        let rebuilt = RigCatalog::from_state(&state);
        assert_eq!(rebuilt, catalog);
    }

    #[test]
    fn prefix_change_keeps_index_in_sync() {
        let mut catalog = RigCatalog::default();
        catalog.apply_add(RigEntry::new(
            "plane",
            "pl",
            "git@github.com:o/plane.git",
            "main",
            1,
        ));
        assert!(catalog.apply_prefix_change("plane", "pln"));
        assert_eq!(catalog.prefix_owner("pl"), None);
        assert_eq!(catalog.prefix_owner("pln"), Some("plane"));
        assert_eq!(catalog.get("plane").unwrap().prefix, "pln");
    }

    #[test]
    fn remove_clears_prefix_index() {
        let mut catalog = RigCatalog::default();
        catalog.apply_add(RigEntry::new(
            "plane",
            "pl",
            "git@github.com:o/plane.git",
            "main",
            1,
        ));
        assert!(catalog.apply_remove("plane"));
        assert!(catalog.prefix_owner("pl").is_none());
        assert!(!catalog.apply_remove("plane"));
    }
}
