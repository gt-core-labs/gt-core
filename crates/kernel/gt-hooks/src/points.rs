//! [`HookPoint`] — the lifecycle moments a module can observe.
//!
//! `hq-mod-hooks.1` introduces the type and a representative pair
//! (`BeforeCommand` / `AfterCommand`) so the [`HookRegistry`](crate::HookRegistry)
//! and [`HookHandler`](crate::HookHandler) machinery can be built and tested
//! against a real variant. The full builtin set — before/after event,
//! claim / release / transition — lands in `hq-mod-hooks.2`. The enum is
//! `#[non_exhaustive]`, so that bead adds variants without breaking a handler
//! written against `.1`, and downstream `match`es must keep a `_` arm.

use serde::{Deserialize, Serialize};

/// A point in an entity's lifecycle where modules may observe and react.
///
/// A handler is registered against the point it cares about and invoked when the
/// kernel reaches that point. `BeforeCommand` handlers may veto via
/// [`HookOutcome::Reject`](crate::HookOutcome) (e.g. the Sheriff pre-merge
/// watchdog, `hq-mod-hooks.7`); `After*` handlers observe only.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[non_exhaustive]
pub enum HookPoint {
    /// Fires before a command's `execute`, while a veto is still possible.
    BeforeCommand,
    /// Fires after a command has executed and its events were appended.
    AfterCommand,
}

impl HookPoint {
    /// Whether a handler at this point may veto the in-flight operation.
    ///
    /// Only `Before*` points are vetoable; `After*` points observe a fait
    /// accompli. `hq-mod-hooks.2` keeps this in sync as it adds the builtin set.
    pub fn is_vetoable(&self) -> bool {
        matches!(self, HookPoint::BeforeCommand)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn before_command_is_vetoable_after_is_not() {
        assert!(HookPoint::BeforeCommand.is_vetoable());
        assert!(!HookPoint::AfterCommand.is_vetoable());
    }

    #[test]
    fn serde_round_trips_as_tagged_string() {
        let p = HookPoint::BeforeCommand;
        let json = serde_json::to_string(&p).unwrap();
        assert_eq!(json, "\"BeforeCommand\"");
        assert_eq!(serde_json::from_str::<HookPoint>(&json).unwrap(), p);
    }
}
