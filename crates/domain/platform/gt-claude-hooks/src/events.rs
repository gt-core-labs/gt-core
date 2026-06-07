use serde::{Deserialize, Serialize};

use gt_events::EventKind;

use crate::state::HookTarget;

/// Domain events for `gt-hooks`. The log of these events (at the GLOBAL event scope) is the source
/// for rebuilding [`crate::HooksState`] via `apply`.
///
/// Time always travels as `now_secs` (UTC epoch). The producer (the edge) reads it off the clock;
/// the core only consumes it.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum HookEvent {
    /// A hook joined (or was replaced in) the registry. Re-emitting with the same `id` upserts the
    /// entry, so an edit rides the same event — symmetric to `skills.registered.v1`.
    Registered {
        id: String,
        /// The Claude Code hook event type (`PreToolUse`, `Stop`, …). Validated against
        /// [`crate::EVENT_TYPES`].
        event: String,
        /// The tool matcher (e.g. `Bash(rm -rf /*)`); empty ⇒ matches every invocation of `event`.
        matcher: String,
        /// The shell command claude runs for the hook.
        command: String,
        /// Which sessions the hook applies to (empty dimensions ⇒ "all"). `#[serde(default)]` keeps
        /// any future log entry without a target replayable.
        #[serde(default)]
        target: HookTarget,
        now_secs: u64,
    },
    /// A hook was retired (removed from the registry).
    Retired { id: String, now_secs: u64 },
}

impl EventKind for HookEvent {
    fn kind(&self) -> &'static str {
        match self {
            HookEvent::Registered { .. } => "hooks.registered.v1",
            HookEvent::Retired { .. } => "hooks.retired.v1",
        }
    }
}
