//! Type-erased dead-letter entry consumed by `gt-feed`.
//!
//! Mirrors `gt-bus::DeadLetterEntry` but without the `EventKind` generic so the feed never
//! depends on a specific domain (or on `gt-bus`). The composition root (`bins/gt`) translates
//! its real `DeadLetterEntry<GtEvent>` into one of these.

use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum DeadEntry {
    /// `publish` ran with no subscribers for `kind`.
    Unhandled {
        kind: String,
        event_id: String,
        correlation_id: Option<String>,
    },
    /// A handler returned `Err`. `error` is the stringified `AppError`.
    HandlerError { kind: String, error: String },
}
