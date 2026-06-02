use serde::{Deserialize, Serialize};

use gt_events::EventKind;

/// Orchestration domain events. `Serialize`/`Deserialize` for the audit log + replay.
///
/// A **convoy** drives an ordered set of member beads to completion: it feeds the next
/// ready member when the current one finishes (handoff) and closes when all members are
/// done. The events split into two roles, exactly like `gt-patrol` and `gt-merge`:
///
/// - **Inputs** observed at the edge and recorded so replay can rebuild the board:
///   `ConvoyCreated`, `ConvoyLaunched`, `MemberCompleted`, `MemberFailed`.
/// - **Outputs**: domain decisions the composition root reacts to. `MemberDispatched` is
///   the *delegation/handoff* the mayor/deacon turns into a `gt sling`; `ConvoyClosed` /
///   `ConvoyFailed` close the convoy bead. They are recorded too, so replay reconstructs
///   which member is active and how the convoy ended.
///
/// The core never reads the clock here: a convoy advances on *facts* (a member finished),
/// not on elapsed time — so this domain is trivially replay-able (`docs/06-observability.md`).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum OrchEvent {
    /// A convoy was planned: an ordered list of member beads to drive to completion.
    /// Starts `Staged` — not yet feeding crew.
    ConvoyCreated { convoy: String, members: Vec<String> },
    /// Mayor/deacon released the convoy: `Staged → Launched`. The actor reacts by feeding
    /// the first member.
    ConvoyLaunched { convoy: String },
    /// Delegation / handoff: feed this member to crew now. Emitted on launch (first member)
    /// and after each completion (next ready member). The composition root reacts by
    /// slinging the member bead.
    MemberDispatched { convoy: String, member: String },
    /// A crew member finished its bead (observed when the member bead closes).
    MemberCompleted { convoy: String, member: String },
    /// A crew member's bead failed.
    MemberFailed { convoy: String, member: String, reason: String },
    /// All members done: the convoy bead can be closed. `Launched → Closed`.
    ConvoyClosed { convoy: String },
    /// A member failed and halted the convoy. `Launched → Failed`.
    ConvoyFailed { convoy: String, member: String, reason: String },
}

impl EventKind for OrchEvent {
    /// Versioned leaf kinds (`hq-mod-events.8`). events.2 backfilled `.v1` for rig/quota/merge
    /// but skipped this crate, leaving convoy on the legacy `orch.*` family prefix. Aligned to
    /// the `convoy.*.v1` leaf namespace its siblings already use (the redundant `convoy` noun is
    /// dropped now that the module segment carries it; `member` is kept to disambiguate the
    /// member sub-events). Emitted-underscore per the quota precedent; the module capability
    /// declares the kebab form. Pre-rename `events.jsonl` read back as these forward kinds via
    /// the `gt-audit` reader rename map, and replay is unaffected (reducers decode by payload,
    /// not `kind`).
    fn kind(&self) -> &'static str {
        match self {
            OrchEvent::ConvoyCreated { .. } => "convoy.created.v1",
            OrchEvent::ConvoyLaunched { .. } => "convoy.launched.v1",
            OrchEvent::MemberDispatched { .. } => "convoy.member_dispatched.v1",
            OrchEvent::MemberCompleted { .. } => "convoy.member_completed.v1",
            OrchEvent::MemberFailed { .. } => "convoy.member_failed.v1",
            OrchEvent::ConvoyClosed { .. } => "convoy.closed.v1",
            OrchEvent::ConvoyFailed { .. } => "convoy.failed.v1",
        }
    }
}
