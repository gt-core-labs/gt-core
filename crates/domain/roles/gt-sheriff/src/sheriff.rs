//! Producer for the Sheriff role. Sheriff is event-driven: the composition root subscribes
//! to its `broadcast<EventRecord>` and, for every event seen, calls [`observe`] with the
//! event's `kind` string. The actor decides whether the kind matches a registered watch.
//!
//! Kept as a thin helper here (one fn) instead of a `spawn`-style task because the bus
//! subscription lives in `bins/gt::root` — that crate already owns the broadcast and the
//! drain loop. This lets `gt-sheriff` stay independent of `gt-audit::EventRecord`.

use gt_events::AppError;

use crate::actor::SheriffHandle;
use crate::commands::{ObserveWatch, SheriffCommand};

/// Push an `Observe` for `kind` into the sheriff actor. Returns the actor's verdict — `Ok`
/// when the pattern is unwatched (no event traffic) **and** when it is watched and bumped.
pub async fn observe(handle: &SheriffHandle, kind: &str) -> Result<(), AppError> {
    handle
        .exec(SheriffCommand::Observe(ObserveWatch {
            pattern: kind.to_string(),
        }))
        .await
}
