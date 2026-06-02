//! TUI projection: stable, serializable DTOs over `FeedState` + `[FeedProblem]`.
//!
//! Stable JSON shape — internal types (`Correlation`, `EventRef`, `FeedProblem`) can change
//! without breaking the TUI as long as this module keeps producing the same field names.

use serde::{Deserialize, Serialize};

use crate::problems::FeedProblem;
use crate::state::FeedState;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct FeedView {
    pub total_events: u64,
    pub correlation_count: usize,
    pub kind_totals: Vec<KindTally>,
    pub correlations: Vec<CorrelationRow>,
    pub problems: Vec<ProblemRow>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct KindTally {
    pub kind: String,
    pub count: u32,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CorrelationRow {
    pub correlation_id: String,
    pub event_count: usize,
    pub first_ts: Option<String>,
    pub last_ts: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ProblemRow {
    pub class: String,
    pub summary: String,
}

impl FeedView {
    pub fn project(state: &FeedState, problems: &[FeedProblem]) -> Self {
        let kind_totals = state
            .kind_totals
            .iter()
            .map(|(k, n)| KindTally {
                kind: k.clone(),
                count: *n,
            })
            .collect();

        let correlations = state
            .correlations
            .values()
            .map(|c| CorrelationRow {
                correlation_id: c.correlation_id.clone(),
                event_count: c.events.len(),
                first_ts: c.first_ts.clone(),
                last_ts: c.last_ts.clone(),
            })
            .collect();

        let problems = problems.iter().map(problem_row).collect();

        Self {
            total_events: state.total_events,
            correlation_count: state.correlations.len(),
            kind_totals,
            correlations,
            problems,
        }
    }
}

fn problem_row(p: &FeedProblem) -> ProblemRow {
    match p {
        FeedProblem::UnhandledEvent { kind, event_id, .. } => ProblemRow {
            class: "unhandled".into(),
            summary: format!("{kind} ({event_id})"),
        },
        FeedProblem::DeadLetterDrain { kind, error } => ProblemRow {
            class: "handler_error".into(),
            summary: format!("{kind}: {error}"),
        },
        FeedProblem::TimeoutMissed {
            correlation_id,
            terminal_kind,
            ..
        } => ProblemRow {
            class: "timeout_missed".into(),
            summary: format!("{correlation_id} ended in {terminal_kind} without success"),
        },
    }
}
