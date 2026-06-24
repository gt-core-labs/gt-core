use std::path::PathBuf;

use serde::{Deserialize, Serialize};

use gt_events::EventKind;

/// Domain events for `gt-rig`. The log of these events is the source for rebuilding
/// [`crate::RigState`] via `apply`.
///
/// Time always travels as `now_secs` (UTC epoch). The producer (the edge) reads it off the
/// clock; the core only consumes it. Filesystem side-effects (clone, bd init, redirects) are
/// **not** in this enum — they live at the deploy/bootstrap edge. The events here capture the
/// orchestrator's view of the rig registry only.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum RigEvent {
    /// A new rig joined the catalog. The bootstrap edge reports the resolved identity
    /// (prefix, remotes, default branch) once the filesystem layout is in place.
    Added {
        rig: String,
        prefix: String,
        git_url: String,
        push_url: Option<String>,
        upstream_url: Option<String>,
        default_branch: String,
        /// Soft reference to the `public.vcs_connections.id` this rig clones with
        /// (hq-vcs-connections.3). `#[serde(default)]` so events logged before this field
        /// existed replay as `None` (the log is append-only; old payloads stay valid).
        #[serde(default)]
        git_connection_ref: Option<String>,
        now_secs: u64,
    },
    /// An existing on-disk rig directory was adopted into the catalog without re-cloning.
    /// Carries the same identity payload as [`RigEvent::Added`] but is a distinct kind so the
    /// audit log can tell them apart on replay / dashboards.
    Adopted {
        rig: String,
        prefix: String,
        git_url: String,
        push_url: Option<String>,
        upstream_url: Option<String>,
        default_branch: String,
        /// See [`RigEvent::Added::git_connection_ref`]. `#[serde(default)]` for the same
        /// backward-compatible-replay reason.
        #[serde(default)]
        git_connection_ref: Option<String>,
        now_secs: u64,
    },
    /// A rig was removed from the catalog. The on-disk directory teardown is a deploy-edge
    /// concern; this event records the orchestrator's loss of authority over the rig.
    Removed { rig: String, now_secs: u64 },
    /// The beads prefix for a rig changed. Carries both sides so the audit log + reducer
    /// reconstruct the transition; `bd config set issue_prefix` is the side-effect.
    PrefixChanged {
        rig: String,
        old: String,
        new: String,
        now_secs: u64,
    },
    /// The default branch tracked by the rig changed (e.g. main → master).
    DefaultBranchChanged {
        rig: String,
        old: String,
        new: String,
        now_secs: u64,
    },
    /// The worktree root the orchestrator carves polecat checkouts under changed. `old` is
    /// the prior override (`None` if the rig was still on the convention default); `new` is
    /// the explicit root now pinned for the rig. Filesystem moves are a deploy-edge concern;
    /// this event records only the orchestrator's routing override (hq-mt-rigs.3).
    WorktreeRootChanged {
        rig: String,
        old: Option<PathBuf>,
        new: PathBuf,
        now_secs: u64,
    },
    /// The rig's semantic capability tags changed (B3, gtcore-1caa48). Carries both sides of the
    /// transition (the full normalised sets, replace not merge) so the audit log + reducer
    /// reconstruct it. Tag-based peer selection in `a2a.discover` is the consumer; there is no
    /// filesystem side-effect.
    TagsChanged {
        rig: String,
        old: Vec<String>,
        new: Vec<String>,
        now_secs: u64,
    },
    /// The rig's soft VCS-connection ref changed (gtcore-103958). `old`/`new` are the prior and
    /// new `git_connection_ref` (`None` = unbound, the legacy operator-mounted token path). The
    /// JIT installation-token mint is a runtime/deploy-edge concern; this event records only the
    /// orchestrator's binding to a `public.vcs_connections.id`.
    ConnectionChanged {
        rig: String,
        old: Option<String>,
        new: Option<String>,
        now_secs: u64,
    },
}

impl EventKind for RigEvent {
    fn kind(&self) -> &'static str {
        match self {
            RigEvent::Added { .. } => "rig.added.v1",
            RigEvent::Adopted { .. } => "rig.adopted.v1",
            RigEvent::Removed { .. } => "rig.removed.v1",
            RigEvent::PrefixChanged { .. } => "rig.prefix_changed.v1",
            RigEvent::DefaultBranchChanged { .. } => "rig.default_branch_changed.v1",
            RigEvent::WorktreeRootChanged { .. } => "rig.worktree_root_changed.v1",
            RigEvent::TagsChanged { .. } => "rig.tags_changed.v1",
            RigEvent::ConnectionChanged { .. } => "rig.connection_changed.v1",
        }
    }
}
