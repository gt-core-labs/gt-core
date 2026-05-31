//! [`HookPoint`] — the lifecycle moments a module can observe.
//!
//! `hq-mod-hooks.1` introduced the type and the command pair (`BeforeCommand` /
//! `AfterCommand`). `hq-mod-hooks.2` completes the builtin set with the event
//! pair (`BeforeEvent` / `AfterEvent`) and the bead-lifecycle points (`OnClaim`,
//! `OnRelease`, `OnTransition`), plus [`HookPoint::ALL`] for enumeration. The enum
//! stays `#[non_exhaustive]`, so a downstream `match` must keep a `_` arm and a
//! later bead can still add points without breaking handlers.

use serde::{Deserialize, Serialize};

/// A point in an entity's lifecycle where modules may observe and react.
///
/// A handler is registered against the point it cares about and invoked when the
/// kernel reaches that point. Exactly one point — [`BeforeCommand`](Self::BeforeCommand)
/// — is *vetoable*: a handler there may stop the operation via
/// [`HookOutcome::Reject`](crate::HookOutcome) before any effect occurs (this is
/// the Sheriff pre-merge watchdog seam, `hq-mod-hooks.7`). Every other point fires
/// on a decided fact and observes only; see [`is_vetoable`](Self::is_vetoable).
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[non_exhaustive]
pub enum HookPoint {
    /// Before a command's `execute`, while a veto is still possible. Vetoable.
    BeforeCommand,
    /// After a command executed and its events were appended. Observe-only.
    AfterCommand,
    /// Before an event is dispatched to subscribers. Observe-only: an event is an
    /// already-decided fact in the append-only log, so it cannot be vetoed here —
    /// gate the originating command at [`BeforeCommand`](Self::BeforeCommand)
    /// instead.
    BeforeEvent,
    /// After an event was dispatched to all subscribers. Observe-only.
    AfterEvent,
    /// A unit of work (bead / lease) was claimed by an actor. Observe-only.
    OnClaim,
    /// A previously claimed unit of work was released. Observe-only.
    OnRelease,
    /// An entity moved between states (e.g. a bead status transition).
    /// Observe-only.
    OnTransition,
}

impl HookPoint {
    /// Every builtin hook point, in lifecycle order. Used by diagnostics and
    /// tests; stays exhaustive over the variants this crate defines.
    pub const ALL: [HookPoint; 7] = [
        HookPoint::BeforeCommand,
        HookPoint::AfterCommand,
        HookPoint::BeforeEvent,
        HookPoint::AfterEvent,
        HookPoint::OnClaim,
        HookPoint::OnRelease,
        HookPoint::OnTransition,
    ];

    /// Whether a handler at this point may veto the in-flight operation.
    ///
    /// Only [`BeforeCommand`](Self::BeforeCommand) is vetoable: it is the sole
    /// point that fires before any effect, so a [`Reject`](crate::HookOutcome::Reject)
    /// there can still stop the work. Every other point — `After*`, the event
    /// pair, and the bead-lifecycle points — observes a settled fact, and the
    /// dispatcher (`hq-mod-hooks.3`) ignores a veto returned from them.
    pub fn is_vetoable(&self) -> bool {
        matches!(self, HookPoint::BeforeCommand)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn only_before_command_is_vetoable() {
        assert!(HookPoint::BeforeCommand.is_vetoable());
        for p in HookPoint::ALL {
            if p != HookPoint::BeforeCommand {
                assert!(!p.is_vetoable(), "{p:?} must observe only");
            }
        }
    }

    #[test]
    fn all_lists_every_variant_once() {
        assert_eq!(HookPoint::ALL.len(), 7);
        // No duplicates: a set built from ALL keeps the same length.
        let mut seen = HookPoint::ALL.to_vec();
        seen.sort_by_key(|p| format!("{p:?}"));
        seen.dedup();
        assert_eq!(seen.len(), 7);
    }

    #[test]
    fn serde_round_trips_every_point_as_tagged_string() {
        let cases = [
            (HookPoint::BeforeCommand, "\"BeforeCommand\""),
            (HookPoint::AfterEvent, "\"AfterEvent\""),
            (HookPoint::OnTransition, "\"OnTransition\""),
        ];
        for (p, json) in cases {
            assert_eq!(serde_json::to_string(&p).unwrap(), json);
            assert_eq!(serde_json::from_str::<HookPoint>(json).unwrap(), p);
        }
    }
}
