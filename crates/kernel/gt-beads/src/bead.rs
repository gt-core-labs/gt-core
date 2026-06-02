use serde::{Deserialize, Serialize};

/// Estado de un bead en la cola de trabajo (ver `docs/05-queues.md`).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum BeadStatus {
    Pending,
    Dispatched,
    Working,
    Done,
    Failed,
}

impl BeadStatus {
    pub fn as_str(self) -> &'static str {
        match self {
            BeadStatus::Pending => "pending",
            BeadStatus::Dispatched => "dispatched",
            BeadStatus::Working => "working",
            BeadStatus::Done => "done",
            BeadStatus::Failed => "failed",
        }
    }

    pub fn parse(s: &str) -> Option<Self> {
        Some(match s {
            "pending" => BeadStatus::Pending,
            "dispatched" => BeadStatus::Dispatched,
            "working" => BeadStatus::Working,
            "done" => BeadStatus::Done,
            "failed" => BeadStatus::Failed,
            _ => return None,
        })
    }
}

/// Un bead. `priority`: 0 = P0 (más alta). El bead **es** el job: no hay tabla de cola
/// aparte, la cola es una vista sobre beads en cierto estado.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Bead {
    pub id: String,
    pub title: String,
    pub status: BeadStatus,
    pub priority: u8,
    pub assignee: Option<String>,
}

impl Bead {
    pub fn new(id: impl Into<String>, title: impl Into<String>, status: BeadStatus, priority: u8) -> Self {
        Self {
            id: id.into(),
            title: title.into(),
            status,
            priority,
            assignee: None,
        }
    }
}

/// Dependencia entre issues (grafo de beads).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct IssueDep {
    pub from: String,
    pub to: String,
}
