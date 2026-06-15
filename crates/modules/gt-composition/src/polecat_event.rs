//! Polecat sling-lifecycle events for operator notification (`gtcore-7e19fe`).
//!
//! The polecat supervisor ([`crate::polecat::PolecatSupervisorPlugin`]) already supervises slings,
//! but two operationally critical outcomes only reached `eprintln` — invisible to the operator
//! without tailing daemon logs:
//!
//! - **Pool/host cap reached** — a dispatched bead is admitted against the pool and refused; the
//!   sling is skipped and the bead waits in limbo until a slot frees.
//! - **Sling failed** — `spawn_tmux` errored (dead tmux, corrupt worktree, …); the slot is
//!   released but the bead never advances.
//!
//! Neither has a pre-existing hub event (the quota/patrol failures DO — see
//! [`crate::workflow_notify`]). These two events fill that gap: the supervisor broadcasts them onto
//! the SAME hub it already emits `agent.*` onto, and [`crate::workflow_notify::WorkflowNotifyPlugin`]
//! (registered on the same relay) mirrors them into operator notifications. Best-effort, like every
//! other emission off that sender — a closed hub costs only the notification, never the sling.

use serde::{Deserialize, Serialize};

use gt_events::EventKind;

/// A polecat sling outcome worth surfacing to the operator.
///
/// `SlingSkipped` is backpressure (recoverable: capacity frees as live polecats finish);
/// `SlingFailed` is a fault (the slot was released, but the bead made no progress).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum PolecatEvent {
    /// A dispatched bead's sling was skipped because the pool/host admission cap is reached.
    /// `workspace` is the pool the claim was refused against.
    SlingSkipped { bead: String, workspace: String },
    /// A dispatched bead's sling failed at spawn (`spawn_tmux` error). The slot was released so it
    /// is not leaked; `reason` is the spawn error rendered for the operator.
    SlingFailed { bead: String, reason: String },
}

impl EventKind for PolecatEvent {
    fn kind(&self) -> &'static str {
        // `polecat.` NS, versioned + kebab per docs/04 — born versioned (no legacy bare-kind
        // records exist, these events are new in gtcore-7e19fe).
        match self {
            PolecatEvent::SlingSkipped { .. } => "polecat.sling-skipped.v1",
            PolecatEvent::SlingFailed { .. } => "polecat.sling-failed.v1",
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn kinds_are_versioned_kebab_under_polecat_ns() {
        let skipped = PolecatEvent::SlingSkipped {
            bead: "hq-1".into(),
            workspace: "default".into(),
        };
        assert_eq!(skipped.kind(), "polecat.sling-skipped.v1");
        assert!(skipped.kind().starts_with("polecat."));
        assert!(skipped.kind().ends_with(".v1"));

        let failed = PolecatEvent::SlingFailed {
            bead: "hq-1".into(),
            reason: "tmux dead".into(),
        };
        assert_eq!(failed.kind(), "polecat.sling-failed.v1");
        assert!(failed.kind().starts_with("polecat."));
    }

    #[test]
    fn payloads_roundtrip() {
        for ev in [
            PolecatEvent::SlingSkipped {
                bead: "gtweb-1".into(),
                workspace: "acme".into(),
            },
            PolecatEvent::SlingFailed {
                bead: "gtweb-1".into(),
                reason: "worktree corrupt".into(),
            },
        ] {
            let json = serde_json::to_string(&ev).unwrap();
            let back: PolecatEvent = serde_json::from_str(&json).unwrap();
            assert_eq!(ev, back);
        }
    }
}
