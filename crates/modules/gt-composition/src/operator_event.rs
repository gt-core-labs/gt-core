//! Issue-operator events: which agent is operating a bead, for the FE (`hq-agent-observability.2`).
//!
//! The polecat supervisor ([`crate::polecat::PolecatSupervisorPlugin`]) slings exactly one agent
//! per bead. These two events publish that binding onto the **issues** channel so a frontend
//! subscribed to `GET /stream?channel=issues` sees, live, which agent operates each bead and what
//! it has loaded — without a new transport. The SSE feed routes purely on the `issues.` kind
//! prefix (`hq-issues-sse`), and the daemon root persists every hub record to the per-workspace
//! log the feed reads, so emitting these onto the hub is all the wiring needed.
//!
//! Distinct from [`gt_issues::IssueEvent`]: that carries the mutated **Dolt row** of a tracker
//! change; this carries **ephemeral agent state** (session/role/skills/hooks) that lives nowhere
//! in the row. Keeping it a separate type avoids forcing a Dolt read for runtime-only facts.

use serde::{Deserialize, Serialize};

use gt_events::EventKind;

/// The agent↔bead operating relationship, broadcast to the issues channel.
///
/// `Operated` is emitted when an agent is slung for a bead (carrying the manifest computed at
/// sling: which skills/hooks it has loaded); `Cleared` when that bead's work lands (merge) so the
/// FE drops the marker. One agent per bead, so a `Cleared` needs only the bead id.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum IssueOperatorEvent {
    /// An agent began operating `bead`. `role` is the session role string (`polecat`/`witness`/…);
    /// `skills`/`hooks` are the static manifest from `gt_polecat::{skills_from_worktree,
    /// hooks_from_settings}`.
    Operated {
        bead: String,
        session: String,
        role: String,
        skills: Vec<String>,
        hooks: Vec<String>,
    },
    /// The agent stopped operating `bead` (its work merged) — the FE clears the marker.
    Cleared { bead: String },
}

impl EventKind for IssueOperatorEvent {
    fn kind(&self) -> &'static str {
        // `issues.` prefix → rides `?channel=issues`; versioned + kebab per docs/04.
        match self {
            IssueOperatorEvent::Operated { .. } => "issues.operated.v1",
            IssueOperatorEvent::Cleared { .. } => "issues.operator-cleared.v1",
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn kinds_are_channel_routable_and_versioned() {
        let op = IssueOperatorEvent::Operated {
            bead: "hq-1".into(),
            session: "hq-hq-1".into(),
            role: "polecat".into(),
            skills: vec!["graphify".into()],
            hooks: vec!["Stop".into()],
        };
        assert_eq!(op.kind(), "issues.operated.v1");
        assert!(op.kind().starts_with("issues."));
        assert!(op.kind().ends_with(".v1"));

        let cl = IssueOperatorEvent::Cleared { bead: "hq-1".into() };
        assert_eq!(cl.kind(), "issues.operator-cleared.v1");
        assert!(cl.kind().starts_with("issues."));
    }

    #[test]
    fn operated_payload_roundtrips() {
        let op = IssueOperatorEvent::Operated {
            bead: "hq-1".into(),
            session: "hq-hq-1".into(),
            role: "polecat".into(),
            skills: vec!["graphify".into(), "caveman".into()],
            hooks: vec!["SessionStart".into(), "Stop".into()],
        };
        let json = serde_json::to_string(&op).unwrap();
        let back: IssueOperatorEvent = serde_json::from_str(&json).unwrap();
        assert_eq!(op, back);
    }
}
