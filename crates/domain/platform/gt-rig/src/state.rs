//! Domain state + replay reducer.
//!
//! [`RigCatalog`] is the mutable state the actor owns; [`RigState`] is the version rebuilt
//! from the log for the Step 3 gate (deterministic replay): the live state must match the
//! rebuilt one byte-for-byte.

use std::collections::BTreeMap;
use std::path::{Path, PathBuf};

use serde::{Deserialize, Serialize};

use crate::events::RigEvent;

/// Town-level names that cannot be used for rigs because they collide with town-level
/// infrastructure. Mirrors `reservedRigNames` in `internal/rig/manager.go`.
pub const RESERVED_RIG_NAMES: &[&str] = &["hq"];

/// Maximum length of a beads prefix. Mirrors the Go validator in
/// `internal/rig/manager.go::isValidBeadsPrefix`.
pub const MAX_PREFIX_LEN: usize = 20;

/// Maximum length of a worktree-root override path. A guard against an unbounded string
/// reaching the deploy edge; 256 covers any sane filesystem layout.
pub const MAX_WORKTREE_ROOT_LEN: usize = 256;

/// Maximum number of semantic capability tags a rig may carry (B3). A bound on the discovery
/// document size; a rig advertising more than this is almost certainly mis-tagged.
pub const MAX_SEMANTIC_TAGS: usize = 16;

/// Maximum length of a single semantic tag (B3). Tags are short capability keywords
/// (`rust`, `frontend`, `infra`), not sentences.
pub const MAX_SEMANTIC_TAG_LEN: usize = 32;

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
    /// Explicit worktree-root override for the rig's polecat checkouts. `None` means the rig
    /// follows the convention default (resolved against `$HOME` at the edge — hq-mt-rigs.4);
    /// `Some(path)` pins an absolute root set via [`crate::SetRigWorktreeRoot`].
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub worktree_root: Option<PathBuf>,
    /// Soft reference to the `public.vcs_connections.id` (hq-vcs-connections.1) the
    /// server-side provisioner mints a clone token from. `None` means the rig has no VCS
    /// connection bound yet (legacy operator-mounted / public-repo path); `Some(id)` is the
    /// connection the refresh path resolves to clone/fetch this rig. NOT a DB FK — the
    /// connection lives in `public` while a rig is a per-tenant `ws_<slug>` row, so the link is
    /// validated at the application layer (hq-vcs-connections.3).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub git_connection_ref: Option<String>,
    /// Free-form capability tags for skill-based peer selection (B3, gtcore-1caa48). An agent
    /// surveying the catalog via `a2a.discover` filters on these to find "who knows Rust" or
    /// "who handles frontend", rather than only the rig's bead prefix. Normalised (lowercase,
    /// deduped) by [`crate::SetRigTags`] before it lands here. `#[serde(default)]` +
    /// `skip_serializing_if` keep pre-B3 entries (and their logged events) round-tripping as an
    /// empty list — backward compatible: no tags means the rig is discoverable by prefix only,
    /// exactly as before.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub semantic_tags: Vec<String>,
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
            worktree_root: None,
            git_connection_ref: None,
            semantic_tags: Vec::new(),
        }
    }

    /// Resolve the absolute worktree root the orchestrator carves this rig's polecat
    /// checkouts under, within a workspace. An explicit [`Self::worktree_root`] override wins;
    /// otherwise the convention default `<home>/gastown-wt/<ws>/<name>` is derived.
    ///
    /// `ws` is the workspace slug as `&str`, NOT a `gt-workspace::WorkspaceId`: gt-rig is a
    /// `domain/platform` crate and must not take a `gt-workspace` dep (docs/03 Rule 4 forbids
    /// `platform → platform`). The higher-tier caller passes `ws.as_str()` — the same seam
    /// gt-auth uses to stay free of the dep (`gt-auth/src/lib.rs`).
    pub fn resolved_worktree_root(&self, ws: &str, home: &Path) -> PathBuf {
        self.worktree_root
            .clone()
            .unwrap_or_else(|| home.join("gastown-wt").join(ws).join(&self.name))
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

    /// Pin (or clear) the worktree-root override for a rig. Mirrors the actor's mutation so
    /// the command path and direct messages stay in lockstep.
    pub fn apply_worktree_root_change(&mut self, name: &str, new_root: Option<PathBuf>) -> bool {
        match self.rigs.get_mut(name) {
            Some(entry) => {
                entry.worktree_root = new_root;
                true
            }
            None => false,
        }
    }

    /// Replace the semantic capability tags for a rig (B3, gtcore-1caa48). `tags` is the full
    /// new set (replace, not merge) — the caller normalises before calling. Mirrors the actor's
    /// mutation so the command path and direct messages stay in lockstep.
    pub fn apply_tags_change(&mut self, name: &str, tags: Vec<String>) -> bool {
        match self.rigs.get_mut(name) {
            Some(entry) => {
                entry.semantic_tags = tags;
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
    /// Sequence of `(rig, old_root, new_root)` worktree-root override transitions.
    pub worktree_root_changes: Vec<(String, Option<PathBuf>, PathBuf)>,
    /// Sequence of `(rig, old_tags, new_tags)` semantic-tag transitions (B3).
    pub tags_changes: Vec<(String, Vec<String>, Vec<String>)>,
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
                git_connection_ref,
                now_secs,
            }
            | RigEvent::Adopted {
                rig,
                prefix,
                git_url,
                push_url,
                upstream_url,
                default_branch,
                git_connection_ref,
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
                        worktree_root: None,
                        git_connection_ref: git_connection_ref.clone(),
                        semantic_tags: Vec::new(),
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
            RigEvent::WorktreeRootChanged { rig, old, new, .. } => {
                if let Some(entry) = self.rigs.get_mut(rig) {
                    entry.worktree_root = Some(new.clone());
                    self.worktree_root_changes
                        .push((rig.clone(), old.clone(), new.clone()));
                }
            }
            RigEvent::TagsChanged { rig, old, new, .. } => {
                if let Some(entry) = self.rigs.get_mut(rig) {
                    entry.semantic_tags = new.clone();
                    self.tags_changes
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
    if name
        .chars()
        .any(|c| matches!(c, '-' | '.' | ' ' | '/' | '\\'))
    {
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

/// Validate a worktree-root override. The orchestrator carves polecat checkouts under this
/// path, so it must be absolute (no ambiguity about the base), contain no `..` component (no
/// escaping the configured tree), and stay within [`MAX_WORKTREE_ROOT_LEN`].
pub fn validate_worktree_root(root: &Path) -> Result<(), String> {
    use std::path::Component;

    let display = root.display();
    if root.as_os_str().is_empty() {
        return Err("worktree_root is empty".into());
    }
    if !root.is_absolute() {
        return Err(format!(
            "worktree_root {display:?} must be an absolute path"
        ));
    }
    if root.as_os_str().len() > MAX_WORKTREE_ROOT_LEN {
        return Err(format!(
            "worktree_root {display:?} exceeds max length {MAX_WORKTREE_ROOT_LEN}"
        ));
    }
    if root.components().any(|c| matches!(c, Component::ParentDir)) {
        return Err(format!(
            "worktree_root {display:?} must not contain a `..` component"
        ));
    }
    Ok(())
}

/// Normalise a raw set of semantic tags (B3): trim, lowercase, drop empties, and dedupe while
/// preserving first-seen order. Run before [`validate_semantic_tags`] and before the tags land
/// in the catalog so the stored form is canonical (replay byte-for-byte deterministic, and
/// `discover`'s tag match is case-insensitive by construction). Tags are matched/displayed in
/// lowercase, so `Rust` and `rust` collapse to one entry.
pub fn normalize_semantic_tags(raw: &[String]) -> Vec<String> {
    let mut out: Vec<String> = Vec::new();
    for tag in raw {
        let norm = tag.trim().to_ascii_lowercase();
        if norm.is_empty() || out.iter().any(|t| t == &norm) {
            continue;
        }
        out.push(norm);
    }
    out
}

/// Validate an already-[`normalize_semantic_tags`]d set of capability tags (B3). Each tag must
/// be a short keyword (alphanumeric with optional internal hyphens, starting with an
/// alphanumeric), within [`MAX_SEMANTIC_TAG_LEN`], and the set within [`MAX_SEMANTIC_TAGS`]. An
/// empty set is valid — it means "discoverable by prefix only", the pre-B3 behaviour.
pub fn validate_semantic_tags(tags: &[String]) -> Result<(), String> {
    if tags.len() > MAX_SEMANTIC_TAGS {
        return Err(format!(
            "too many semantic tags: {} (max {MAX_SEMANTIC_TAGS})",
            tags.len()
        ));
    }
    for tag in tags {
        if tag.is_empty() {
            return Err("semantic tag is empty".into());
        }
        if tag.len() > MAX_SEMANTIC_TAG_LEN {
            return Err(format!(
                "semantic tag {tag:?} exceeds max length {MAX_SEMANTIC_TAG_LEN}"
            ));
        }
        let mut chars = tag.chars();
        match chars.next() {
            Some(c) if c.is_ascii_alphanumeric() => {}
            _ => {
                return Err(format!(
                    "semantic tag {tag:?} must start with an alphanumeric character"
                ))
            }
        }
        for c in chars {
            if !(c.is_ascii_alphanumeric() || c == '-') {
                return Err(format!(
                    "semantic tag {tag:?} contains invalid character {c:?}; only alphanumerics \
                     and hyphens allowed"
                ));
            }
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
        assert!(
            validate_rig_name("HQ").is_err(),
            "reserved case-insensitive"
        );
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
            git_connection_ref: None,
            now_secs: 1,
        });
        state.apply(&RigEvent::Added {
            rig: "gastown".into(),
            prefix: "gt".into(),
            git_url: "git@github.com:o/gastown.git".into(),
            push_url: None,
            upstream_url: None,
            default_branch: "main".into(),
            git_connection_ref: None,
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
    fn validate_worktree_root_enforces_absolute_no_dotdot_and_length() {
        assert!(validate_worktree_root(Path::new("/home/nixos/gastown-wt/acme/plane")).is_ok());
        assert!(
            validate_worktree_root(Path::new("relative/path")).is_err(),
            "must be absolute"
        );
        assert!(
            validate_worktree_root(Path::new("/home/../etc")).is_err(),
            "no `..` component"
        );
        assert!(validate_worktree_root(Path::new("")).is_err(), "non-empty");
        let long = format!("/{}", "a".repeat(MAX_WORKTREE_ROOT_LEN));
        assert!(
            validate_worktree_root(Path::new(&long)).is_err(),
            "over max length"
        );
    }

    #[test]
    fn resolved_worktree_root_prefers_override_then_convention() {
        let home = Path::new("/home/nixos");

        // No override → convention default `<home>/gastown-wt/<ws>/<name>`.
        let plain = RigEntry::new("plane", "pl", "git@github.com:o/plane.git", "main", 1);
        assert_eq!(
            plain.resolved_worktree_root("acme", home),
            PathBuf::from("/home/nixos/gastown-wt/acme/plane")
        );

        // Explicit override wins, ignoring ws/home.
        let mut pinned = plain.clone();
        pinned.worktree_root = Some(PathBuf::from("/srv/checkouts/plane"));
        assert_eq!(
            pinned.resolved_worktree_root("acme", home),
            PathBuf::from("/srv/checkouts/plane")
        );
    }

    #[test]
    fn worktree_root_change_round_trips_through_state() {
        let mut catalog = RigCatalog::default();
        catalog.apply_add(RigEntry::new(
            "plane",
            "pl",
            "git@github.com:o/plane.git",
            "main",
            1,
        ));
        let root = PathBuf::from("/home/nixos/gastown-wt/acme/plane");
        assert!(catalog.apply_worktree_root_change("plane", Some(root.clone())));
        assert_eq!(
            catalog.get("plane").unwrap().worktree_root.as_ref(),
            Some(&root)
        );

        let mut state = RigState::default();
        state.apply(&RigEvent::Added {
            rig: "plane".into(),
            prefix: "pl".into(),
            git_url: "git@github.com:o/plane.git".into(),
            push_url: None,
            upstream_url: None,
            default_branch: "main".into(),
            git_connection_ref: None,
            now_secs: 1,
        });
        state.apply(&RigEvent::WorktreeRootChanged {
            rig: "plane".into(),
            old: None,
            new: root.clone(),
            now_secs: 2,
        });
        assert_eq!(
            state.worktree_root_changes,
            vec![("plane".to_string(), None, root.clone())]
        );

        let rebuilt = RigCatalog::from_state(&state);
        assert_eq!(rebuilt, catalog);
    }

    #[test]
    fn normalize_lowercases_trims_and_dedupes_preserving_order() {
        let raw = vec![
            " Rust ".to_string(),
            "backend".to_string(),
            "RUST".to_string(),
            "".to_string(),
            "  ".to_string(),
            "infra".to_string(),
        ];
        assert_eq!(
            normalize_semantic_tags(&raw),
            vec!["rust".to_string(), "backend".to_string(), "infra".to_string()]
        );
    }

    #[test]
    fn validate_semantic_tags_enforces_grammar_and_bounds() {
        assert!(validate_semantic_tags(&[]).is_ok(), "empty set is valid");
        assert!(validate_semantic_tags(&["rust".into(), "web-fe".into()]).is_ok());
        assert!(
            validate_semantic_tags(&["-bad".into()]).is_err(),
            "must start alphanumeric"
        );
        assert!(
            validate_semantic_tags(&["has space".into()]).is_err(),
            "no spaces"
        );
        assert!(
            validate_semantic_tags(&["a".repeat(MAX_SEMANTIC_TAG_LEN + 1)]).is_err(),
            "over max tag length"
        );
        let too_many: Vec<String> = (0..=MAX_SEMANTIC_TAGS).map(|i| format!("t{i}")).collect();
        assert!(validate_semantic_tags(&too_many).is_err(), "over max count");
    }

    #[test]
    fn tags_change_round_trips_through_state() {
        let mut catalog = RigCatalog::default();
        catalog.apply_add(RigEntry::new(
            "plane",
            "pl",
            "git@github.com:o/plane.git",
            "main",
            1,
        ));
        let tags = vec!["rust".to_string(), "infra".to_string()];
        assert!(catalog.apply_tags_change("plane", tags.clone()));
        assert_eq!(catalog.get("plane").unwrap().semantic_tags, tags);
        // Absent rig is a no-op.
        assert!(!catalog.apply_tags_change("ghost", tags.clone()));

        let mut state = RigState::default();
        state.apply(&RigEvent::Added {
            rig: "plane".into(),
            prefix: "pl".into(),
            git_url: "git@github.com:o/plane.git".into(),
            push_url: None,
            upstream_url: None,
            default_branch: "main".into(),
            git_connection_ref: None,
            now_secs: 1,
        });
        state.apply(&RigEvent::TagsChanged {
            rig: "plane".into(),
            old: Vec::new(),
            new: tags.clone(),
            now_secs: 2,
        });
        assert_eq!(
            state.tags_changes,
            vec![("plane".to_string(), Vec::new(), tags.clone())]
        );

        // Replay gate: the reducer rebuilds the same catalog the live mutation produced.
        let rebuilt = RigCatalog::from_state(&state);
        assert_eq!(rebuilt, catalog);
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
