//! Semantic gap detection over the projected `FeedState` + dead-letter source.
//!
//! Three classes, mapped 1:1 to the kinds of bug the spec calls out (`docs/06`):
//!
//! - `UnhandledEvent` — published with zero subscribers. Comes from the dead-letter source
//!   (the bus does not write it to the log on its own).
//! - `DeadLetterDrain` — a handler returned `Err`. Same source.
//! - `TimeoutMissed` — a workflow logged an explicit "expired"/"timeout"/"failed" terminal
//!   without ever logging its matching success. The cutoff is the log's own statement
//!   (`*.dispatch_timeout`, `*.lease_expired`, `*.failed`); the feed **does not compute it**
//!   against any wall clock.

use serde::{Deserialize, Serialize};

use crate::dead::DeadEntry;
use crate::state::FeedState;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum FeedProblem {
    UnhandledEvent {
        kind: String,
        event_id: String,
        correlation_id: Option<String>,
    },
    DeadLetterDrain {
        kind: String,
        error: String,
    },
    /// A correlation logged a terminal failure-marker without a matching success.
    TimeoutMissed {
        correlation_id: String,
        terminal_kind: String,
        terminal_ts: String,
    },
}

/// Suffixes the log uses to mark an explicit "this workflow died" event. Stable strings
/// (matching the `kind()` impls across domains in `docs/02-tree.md`).
const TERMINAL_FAILURE_SUFFIXES: &[&str] = &[
    ".dispatch_timeout",
    ".dispatch_failed",
    ".lease_expired",
    ".failed",
    ".session_end",
    ".killed",
];

/// Suffixes the log uses to mark explicit success / closure. If any of these is present for
/// the same correlation, the failure marker is treated as routine recovery, not a problem.
const SUCCESS_SUFFIXES: &[&str] = &[
    ".merged",
    ".lease_closed",
    ".rotated",
    ".window_reset",
];

pub fn detect(state: &FeedState, dead: &[DeadEntry]) -> Vec<FeedProblem> {
    let mut out = Vec::new();

    for entry in dead {
        match entry {
            DeadEntry::Unhandled {
                kind,
                event_id,
                correlation_id,
            } => out.push(FeedProblem::UnhandledEvent {
                kind: kind.clone(),
                event_id: event_id.clone(),
                correlation_id: correlation_id.clone(),
            }),
            DeadEntry::HandlerError { kind, error } => {
                out.push(FeedProblem::DeadLetterDrain {
                    kind: kind.clone(),
                    error: error.clone(),
                })
            }
        }
    }

    for (cid, corr) in &state.correlations {
        let has_success = corr
            .kind_counts
            .keys()
            .any(|k| SUCCESS_SUFFIXES.iter().any(|s| k.ends_with(s)));
        if has_success {
            continue;
        }
        for ev in &corr.events {
            if TERMINAL_FAILURE_SUFFIXES.iter().any(|s| ev.kind.ends_with(s)) {
                out.push(FeedProblem::TimeoutMissed {
                    correlation_id: cid.clone(),
                    terminal_kind: ev.kind.clone(),
                    terminal_ts: ev.ts.clone(),
                });
            }
        }
    }

    out
}
