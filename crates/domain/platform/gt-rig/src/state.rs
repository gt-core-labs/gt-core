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

/// Per-rig dispatch mode (rig-hold H1, epic gtcore-4b7d56). The rig-level sibling of a bead's
/// `dispatch=auto|manual`: it governs only the **dispatch + agent-lifecycle** plane.
///
/// - [`Auto`](DispatchMode::Auto) (the default — every pre-feature rig resolves here, back-compat):
///   the orchestrator may delegate the rig's ready beads and the watchdogs may re-sling its
///   polecats, exactly as today.
/// - [`Hold`](DispatchMode::Hold): the operator has paused the rig so they can intervene without
///   colliding with the orchestrator. H2/H3 teach the scheduler and the witness/sheriff to respect
///   it; **this task (H1) only carries the state, the API, and the observability** — it changes no
///   scheduler or watchdog behaviour.
///
/// Out of scope by design (`rig-hold-mechanism-design`): the deploy reconciler (namespace plane,
/// already has a cronjob suspend) and the refinery/merge queue (branch plane). Mixing those into
/// the hold would muddy the semantics.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum DispatchMode {
    /// Orchestrator dispatch + watchdog re-sling are active for the rig (default, back-compat).
    #[default]
    Auto,
    /// The rig is paused: H2/H3 will stop the scheduler delegating its beads and the watchdogs
    /// re-slinging its polecats. In-flight work drains; live agents are not killed.
    Hold,
}

impl DispatchMode {
    /// Stable wire/DB token (`"auto"` / `"hold"`). Matches the lowercase serde rename so the
    /// JSON and the `dispatch_mode` TEXT column carry the same string.
    pub fn as_str(&self) -> &'static str {
        match self {
            DispatchMode::Auto => "auto",
            DispatchMode::Hold => "hold",
        }
    }

    /// Resolve the stored token back to a mode. `"hold"` ⇒ [`Hold`](DispatchMode::Hold); ANY other
    /// value — including a legacy `NULL`/empty column or an unrecognised string — resolves to
    /// [`Auto`](DispatchMode::Auto), the back-compat default a never-touched rig carries.
    pub fn from_db(s: &str) -> Self {
        if s.eq_ignore_ascii_case("hold") {
            DispatchMode::Hold
        } else {
            DispatchMode::Auto
        }
    }
}

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
    /// Orchestrator dispatch mode for the rig (rig-hold H1, epic gtcore-4b7d56). `#[serde(default)]`
    /// makes a pre-feature entry (and any event/seed logged before this field existed) read back as
    /// [`DispatchMode::Auto`] — a never-held rig behaves exactly as before. `rig.hold`/`rig.resume`
    /// flip it; the scheduler/watchdogs reading it to gate work is H2/H3, not this task.
    #[serde(default)]
    pub dispatch_mode: DispatchMode,
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
            dispatch_mode: DispatchMode::Auto,
        }
    }

    /// Resolve the absolute worktree root the orchestrator carves this rig's polecat
    /// checkouts under, within a workspace. An explicit [`Self::worktree_root`] override wins;
    /// otherwise the convention default `<home>/gt-wt/<ws>/<name>` is derived.
    ///
    /// `ws` is the workspace slug as `&str`, NOT a `gt-workspace::WorkspaceId`: gt-rig is a
    /// `domain/platform` crate and must not take a `gt-workspace` dep (docs/03 Rule 4 forbids
    /// `platform → platform`). The higher-tier caller passes `ws.as_str()` — the same seam
    /// gt-auth uses to stay free of the dep (`gt-auth/src/lib.rs`).
    pub fn resolved_worktree_root(&self, ws: &str, home: &Path) -> PathBuf {
        self.worktree_root
            .clone()
            .unwrap_or_else(|| home.join("gt-wt").join(ws).join(&self.name))
    }

    /// Assess whether this rig is provisioned for autonomous, parallel polecat operation
    /// (hq-29ea8a B2/B3). See [`RigReadiness`] for what each check means. Pure over the
    /// catalog entry — no IO — so it is cheap to run for every rig in a readiness sweep.
    pub fn readiness(&self) -> RigReadiness {
        let has_clone_url = !self.git_url.trim().is_empty();
        let has_push_url = self
            .push_url
            .as_deref()
            .map(|u| !u.trim().is_empty())
            .unwrap_or(false);
        let has_vcs_connection = self
            .git_connection_ref
            .as_deref()
            .map(|c| !c.trim().is_empty())
            .unwrap_or(false);
        let worktree_root_pinned = self.worktree_root.is_some();

        // Blocking gaps: anything that stops the autonomous deliver→push cycle from closing.
        let mut gaps = Vec::new();
        if !has_clone_url {
            gaps.push("git_url is empty: orchd cannot clone the rig to provision worktrees".into());
        }
        // Push auth is satisfied by EITHER surface (gtcore-ae4d89): an explicit push_url, or a
        // bound VCS connection — the operational path embeds the rig token into the checkout's
        // origin at clone time (gtcore-abfe8a), so a connection-bound rig pushes fine and there
        // is deliberately no push_url setter. The old check read only the push_url column and
        // reported every connection-bound rig as unable to push (a standing false alarm).
        if !has_push_url && !has_vcs_connection {
            gaps.push(
                "no push auth: neither push_url nor a bound git_connection_ref — the refinery \
                 cannot auto-push main after an ff-merge"
                    .into(),
            );
        }

        // Advisories: surfaced but not blocking — the system still works, just on a default.
        let mut advisories = Vec::new();
        if !worktree_root_pinned {
            advisories.push(
                "worktree_root not pinned: orchd falls back to the convention default \
                 <home>/gt-wt/<ws>/<name>"
                    .into(),
            );
        }

        RigReadiness {
            has_clone_url,
            has_push_url,
            has_vcs_connection,
            worktree_root_pinned,
            gaps,
            advisories,
        }
    }
}

/// Readiness of a single rig for autonomous, parallel polecat operation (hq-29ea8a B2/B3).
///
/// The epic's two rig criteria — every rig must (a) provision isolated per-polecat worktrees
/// so parallel polecats never share a checkout, and (b) let the refinery push to `main`
/// automatically after a fast-forward merge instead of leaving a "pending push to main" note —
/// were applied operationally (`rig.set-worktree-root`, catalog `push_url`). This type makes
/// that state machine-checkable rather than eyeballed: a patrol or operator can assert "every
/// rig is ready" by reading [`Self::ready`] instead of inspecting each field by hand.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RigReadiness {
    /// `git_url` is non-empty, so orchd can clone the rig to provision worktrees.
    pub has_clone_url: bool,
    /// `push_url` is non-empty, so the refinery can fast-forward `main` automatically after a
    /// merge — closing the manual-push gap the epic's B3 criterion called out.
    pub has_push_url: bool,
    /// A VCS connection (`git_connection_ref`) is bound — the operational push-auth surface
    /// (gtcore-ae4d89): the rig token rides the checkout's `origin` from clone time
    /// (gtcore-abfe8a), so a connection-bound rig auto-pushes without a `push_url`.
    /// `#[serde(default)]` keeps pre-field payloads decodable.
    #[serde(default)]
    pub has_vcs_connection: bool,
    /// An explicit `worktree_root` override is pinned. `false` is NOT a blocker — orchd falls
    /// back to the convention default — but the epic asked every rig to pin one, so it surfaces
    /// as an advisory in [`Self::advisories`].
    pub worktree_root_pinned: bool,
    /// Blocking gaps. Empty ⇔ [`Self::ready`] is true.
    pub gaps: Vec<String>,
    /// Non-blocking notes (e.g. relying on the convention worktree root).
    pub advisories: Vec<String>,
}

impl RigReadiness {
    /// True when there are no blocking [`Self::gaps`] — the rig can run the full autonomous
    /// deliver→ff-merge→push cycle without operator intervention.
    pub fn ready(&self) -> bool {
        self.gaps.is_empty()
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

    /// Name of the rig already registered against `git_url`, if any. Linear scan — the
    /// catalog is small (a handful of rigs per workspace) and this only runs on the
    /// add/adopt validate path, not per-dispatch. Used to catch a rig mis-registered
    /// against a repo another rig already owns (the authapp incident: a new rig
    /// silently pointed at gt-core.git, the boot rig's own repo, instead of its
    /// dedicated one — a polecat only surfaced it after landing in the wrong worktree).
    pub fn git_url_owner(&self, git_url: &str) -> Option<&str> {
        self.rigs
            .values()
            .find(|entry| entry.git_url == git_url)
            .map(|entry| entry.name.as_str())
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

    /// Set (or clear) the soft VCS-connection ref for a rig (gtcore-103958). `new_ref` is the
    /// `public.vcs_connections.id` to bind, or `None` to clear. Mirrors the actor's mutation so
    /// the command path and direct messages stay in lockstep.
    pub fn apply_connection_change(&mut self, name: &str, new_ref: Option<String>) -> bool {
        match self.rigs.get_mut(name) {
            Some(entry) => {
                entry.git_connection_ref = new_ref;
                true
            }
            None => false,
        }
    }

    /// Set the dispatch mode for a rig (rig-hold H1). Mirrors the actor's mutation so the command
    /// path and direct messages stay in lockstep. Idempotent at the catalog level: re-applying the
    /// current mode is a harmless overwrite (the no-event idempotency gate lives one layer up, in
    /// the command/handler, so a no-op never emits `rig.held`/`rig.resumed`).
    pub fn apply_dispatch_mode_change(&mut self, name: &str, mode: DispatchMode) -> bool {
        match self.rigs.get_mut(name) {
            Some(entry) => {
                entry.dispatch_mode = mode;
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
    /// Sequence of `(rig, old_ref, new_ref)` VCS-connection binding transitions (gtcore-103958).
    pub connection_changes: Vec<(String, Option<String>, Option<String>)>,
    /// Sequence of `(rig, mode, reason)` dispatch-mode transitions (rig-hold H1). `reason` is the
    /// operator's note carried by `rig.held.v1` (empty for a `rig.resumed.v1`). Observable history
    /// only — the catalog rebuild reads `rigs`, not this vector.
    pub dispatch_mode_changes: Vec<(String, DispatchMode, String)>,
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
                        dispatch_mode: DispatchMode::Auto,
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
            RigEvent::ConnectionChanged { rig, old, new, .. } => {
                if let Some(entry) = self.rigs.get_mut(rig) {
                    entry.git_connection_ref = new.clone();
                    self.connection_changes
                        .push((rig.clone(), old.clone(), new.clone()));
                }
            }
            RigEvent::Held { rig, reason, .. } => {
                if let Some(entry) = self.rigs.get_mut(rig) {
                    entry.dispatch_mode = DispatchMode::Hold;
                    self.dispatch_mode_changes
                        .push((rig.clone(), DispatchMode::Hold, reason.clone()));
                }
            }
            RigEvent::Resumed { rig, .. } => {
                if let Some(entry) = self.rigs.get_mut(rig) {
                    entry.dispatch_mode = DispatchMode::Auto;
                    self.dispatch_mode_changes
                        .push((rig.clone(), DispatchMode::Auto, String::new()));
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
        assert!(validate_worktree_root(Path::new("/home/nixos/gt-wt/acme/plane")).is_ok());
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

        // No override → convention default `<home>/gt-wt/<ws>/<name>`.
        let plain = RigEntry::new("plane", "pl", "git@github.com:o/plane.git", "main", 1);
        assert_eq!(
            plain.resolved_worktree_root("acme", home),
            PathBuf::from("/home/nixos/gt-wt/acme/plane")
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
        let root = PathBuf::from("/home/nixos/gt-wt/acme/plane");
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
    fn readiness_flags_missing_push_url_as_blocking_gap() {
        // A freshly-added rig has git_url but no push_url / worktree_root: clonable, but the
        // refinery cannot auto-push, so it is NOT ready and the convention-root note shows.
        let entry = RigEntry::new("plane", "pl", "git@github.com:o/plane.git", "main", 1);
        let r = entry.readiness();
        assert!(r.has_clone_url);
        assert!(!r.has_push_url);
        assert!(!r.worktree_root_pinned);
        assert!(!r.ready(), "missing push auth blocks readiness");
        assert_eq!(r.gaps.len(), 1, "only push auth is a blocking gap here");
        assert!(r.gaps[0].contains("push auth"));
        assert_eq!(r.advisories.len(), 1, "unpinned worktree_root is advisory");
        assert!(r.advisories[0].contains("worktree_root"));
    }

    #[test]
    fn readiness_is_true_with_push_url_even_without_pinned_worktree_root() {
        // push_url set + clonable ⇒ ready; an unpinned worktree_root is advisory only because
        // orchd resolves a convention default, so parallelism still works.
        let mut entry = RigEntry::new("plane", "pl", "git@github.com:o/plane.git", "main", 1);
        entry.push_url = Some("https://github.com/o/plane.git".into());
        let r = entry.readiness();
        assert!(r.ready(), "clonable + pushable is ready");
        assert!(r.gaps.is_empty());
        assert_eq!(r.advisories.len(), 1, "still notes the unpinned worktree_root");

        // Pinning the worktree root clears the advisory.
        entry.worktree_root = Some(PathBuf::from("/rig-wt/plane"));
        let r2 = entry.readiness();
        assert!(r2.ready());
        assert!(r2.worktree_root_pinned);
        assert!(r2.advisories.is_empty(), "pinned root drops the advisory");
    }

    #[test]
    fn readiness_accepts_a_bound_vcs_connection_as_push_auth() {
        // gtcore-ae4d89: a connection-bound rig pushes via the token embedded into its
        // checkout's origin at clone time (gtcore-abfe8a) — there is no push_url setter, so
        // requiring the column reported every production rig as unable to push forever.
        let mut entry = RigEntry::new("plane", "pl", "https://github.com/o/plane.git", "main", 1);
        entry.git_connection_ref = Some("gh-139659957".into());
        let r = entry.readiness();
        assert!(!r.has_push_url, "no push_url column value");
        assert!(r.has_vcs_connection);
        assert!(r.gaps.is_empty(), "bound connection satisfies push auth");
        assert!(r.ready(), "clonable + connection-bound is ready");

        // A blank connection ref is NOT auth.
        entry.git_connection_ref = Some("   ".into());
        let r2 = entry.readiness();
        assert!(!r2.has_vcs_connection);
        assert!(!r2.ready());
        assert!(r2.gaps[0].contains("push auth"));
    }

    #[test]
    fn readiness_treats_blank_urls_as_missing() {
        // A whitespace-only push_url / git_url is not a real value — both must count as gaps.
        let mut entry = RigEntry::new("plane", "pl", "   ", "main", 1);
        entry.push_url = Some("  ".into());
        let r = entry.readiness();
        assert!(!r.has_clone_url);
        assert!(!r.has_push_url);
        assert_eq!(r.gaps.len(), 2, "both blank git_url and blank push auth are gaps");
        assert!(!r.ready());
    }

    #[test]
    fn dispatch_mode_defaults_to_auto_and_round_trips_through_state() {
        // Back-compat: a freshly-added rig resolves to Auto (the never-held default).
        let mut catalog = RigCatalog::default();
        catalog.apply_add(RigEntry::new(
            "plane",
            "pl",
            "git@github.com:o/plane.git",
            "main",
            1,
        ));
        assert_eq!(
            catalog.get("plane").unwrap().dispatch_mode,
            DispatchMode::Auto,
            "default dispatch mode is auto"
        );

        // Hold then resume flips the stored mode; an absent rig is a no-op.
        assert!(catalog.apply_dispatch_mode_change("plane", DispatchMode::Hold));
        assert_eq!(catalog.get("plane").unwrap().dispatch_mode, DispatchMode::Hold);
        assert!(!catalog.apply_dispatch_mode_change("ghost", DispatchMode::Hold));

        // Replay gate: Added → Held → Resumed rebuilds a catalog whose mode matches a live
        // Add → hold → resume sequence (back to Auto).
        let mut live = RigCatalog::default();
        live.apply_add(RigEntry::new("plane", "pl", "git@github.com:o/plane.git", "main", 1));
        live.apply_dispatch_mode_change("plane", DispatchMode::Hold);
        live.apply_dispatch_mode_change("plane", DispatchMode::Auto);

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
        state.apply(&RigEvent::Held {
            rig: "plane".into(),
            reason: "operator intervention".into(),
            now_secs: 2,
        });
        state.apply(&RigEvent::Resumed {
            rig: "plane".into(),
            now_secs: 3,
        });
        assert_eq!(
            state.dispatch_mode_changes,
            vec![
                ("plane".to_string(), DispatchMode::Hold, "operator intervention".to_string()),
                ("plane".to_string(), DispatchMode::Auto, String::new()),
            ]
        );
        let rebuilt = RigCatalog::from_state(&state);
        assert_eq!(rebuilt, live);
        assert_eq!(
            rebuilt.get("plane").unwrap().dispatch_mode,
            DispatchMode::Auto
        );
    }

    #[test]
    fn dispatch_mode_serializes_as_lowercase_token() {
        assert_eq!(DispatchMode::Auto.as_str(), "auto");
        assert_eq!(DispatchMode::Hold.as_str(), "hold");
        assert_eq!(DispatchMode::from_db("hold"), DispatchMode::Hold);
        assert_eq!(DispatchMode::from_db("HOLD"), DispatchMode::Hold);
        // Any other value (legacy NULL/empty, unknown string) resolves to the Auto default.
        assert_eq!(DispatchMode::from_db("auto"), DispatchMode::Auto);
        assert_eq!(DispatchMode::from_db(""), DispatchMode::Auto);
        assert_eq!(DispatchMode::from_db("banana"), DispatchMode::Auto);
        assert_eq!(
            serde_json::to_value(DispatchMode::Hold).unwrap(),
            serde_json::json!("hold")
        );
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
