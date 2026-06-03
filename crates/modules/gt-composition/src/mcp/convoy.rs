//! `convoy.*` domain dispatch (`hq-mcp-dispatch.6`).
//!
//! Convoys (gt-orchestration) are event-sourced like merge — no projection table —
//! so each call rehydrates the [`ConvoyBoard`] from the workspace event log,
//! executes the command, and appends the resulting [`OrchEvent`]s. A single
//! convoy command can produce **several** events (e.g. launch emits a launch plus
//! the first member-dispatched), so the handler appends the whole batch.
//!
//! Tools: `convoy.launch` / `convoy.complete-member` / `convoy.fail-member`
//! (the [`OrchCommand`] mutations), plus `convoy.list` / `convoy.info` reads.

use std::sync::Arc;

use async_trait::async_trait;
use serde::de::DeserializeOwned;
use serde_json::{json, Value};

use gt_events::{Command, EventKind};
use gt_mcp_server::{DomainCtx, DomainHandler};
use gt_orchestration::{
    CompleteMember, Convoy, ConvoyBoard, FailMember, LaunchConvoy, OrchEvent, OrchState,
};
use gt_store_dolt::AppError;

use super::eventlog::EventLog;
use super::util::{ev_err, parse, str_arg};

/// The event-log kind prefix for every convoy event (`convoy.*.v1`).
const NS: &str = "convoy.";

/// Event-sourced handler for the `convoy.*` tool namespace.
pub struct ConvoyHandler {
    log: Arc<EventLog>,
}

impl ConvoyHandler {
    /// Wrap the per-workspace event log.
    pub fn new(log: Arc<EventLog>) -> Self {
        Self { log }
    }

    /// Rebuild the board from the workspace's convoy events.
    fn board(&self, ws: Option<&str>) -> Result<ConvoyBoard, AppError> {
        let state = self
            .log
            .replay_domain(ws, NS, OrchState::default(), OrchState::apply)?;
        Ok(ConvoyBoard::from_state(&state))
    }

    /// Parse → execute against the rehydrated board → append every produced event.
    fn run<C>(&self, ws: Option<&str>, args: Value) -> Result<Value, AppError>
    where
        C: Command<State = ConvoyBoard, Output = Vec<OrchEvent>> + DeserializeOwned,
    {
        let cmd: C = parse(args)?;
        let mut board = self.board(ws)?;
        let events = cmd.execute(&mut board).map_err(ev_err)?;
        let kinds: Vec<String> = events.iter().map(|e| e.kind().to_string()).collect();
        for event in events {
            self.log.append(ws, event)?;
        }
        Ok(json!({ "ok": true, "events": kinds }))
    }
}

#[async_trait]
impl DomainHandler for ConvoyHandler {
    fn namespace(&self) -> &'static str {
        "convoy"
    }

    async fn dispatch(&self, tool: &str, ctx: DomainCtx<'_>) -> Result<Value, AppError> {
        let ws = ctx.workspace;
        match tool {
            "convoy.launch" => self.run::<LaunchConvoy>(ws, ctx.args),
            "convoy.complete-member" => self.run::<CompleteMember>(ws, ctx.args),
            "convoy.fail-member" => self.run::<FailMember>(ws, ctx.args),
            "convoy.list" => {
                let board = self.board(ws)?;
                Ok(json!({ "convoys": board.snapshot().iter().map(convoy_json).collect::<Vec<_>>() }))
            }
            "convoy.info" => {
                let id = str_arg(&ctx.args, "convoy")?;
                match self.board(ws)?.get(id) {
                    Some(convoy) => Ok(convoy_json(convoy)),
                    None => Err(AppError::NotFound(format!("convoy {id}"))),
                }
            }
            other => Err(AppError::Validation(format!("unknown tool `{other}`"))),
        }
    }
}

/// Shape one convoy (id, state, members) as the dispatch payload.
fn convoy_json(convoy: &Convoy) -> Value {
    json!({
        "id": convoy.id,
        "state": convoy.state.as_str(),
        "members": convoy
            .members
            .iter()
            .map(|m| json!({ "bead": m.bead, "state": m.state.as_str() }))
            .collect::<Vec<_>>(),
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    fn handler(dir: &TempDir) -> ConvoyHandler {
        ConvoyHandler::new(Arc::new(EventLog::new(Some(dir.path().to_path_buf()))))
    }

    fn ctx(args: Value) -> DomainCtx<'static> {
        DomainCtx { workspace: None, actor: "tester", args }
    }

    /// Launch a convoy, then drive its members to done — board rehydrated from
    /// the log each call. The first member is Active after launch (sequential
    /// handoff), the next becomes Active when the prior completes.
    #[tokio::test]
    async fn convoy_lifecycle_through_event_log() {
        let dir = TempDir::new().unwrap();
        let h = handler(&dir);

        h.dispatch("convoy.launch", ctx(json!({ "convoy": "c1", "members": ["b1", "b2"] })))
            .await
            .unwrap();

        let info = h.dispatch("convoy.info", ctx(json!({ "convoy": "c1" }))).await.unwrap();
        assert_eq!(info["state"], "launched");
        let members = info["members"].as_array().unwrap();
        assert_eq!(members[0]["state"], "active", "first member dispatched on launch");
        assert_eq!(members[1]["state"], "pending");

        h.dispatch("convoy.complete-member", ctx(json!({ "convoy": "c1", "member": "b1" })))
            .await
            .unwrap();
        let info = h.dispatch("convoy.info", ctx(json!({ "convoy": "c1" }))).await.unwrap();
        let members = info["members"].as_array().unwrap();
        assert_eq!(members[0]["state"], "done");
        assert_eq!(members[1]["state"], "active", "handoff to next member");

        h.dispatch("convoy.complete-member", ctx(json!({ "convoy": "c1", "member": "b2" })))
            .await
            .unwrap();
        let info = h.dispatch("convoy.info", ctx(json!({ "convoy": "c1" }))).await.unwrap();
        assert_eq!(info["state"], "closed", "convoy closes when all members done");

        let list = h.dispatch("convoy.list", ctx(json!({}))).await.unwrap();
        assert_eq!(list["convoys"].as_array().unwrap().len(), 1);
    }

    #[tokio::test]
    async fn missing_convoy_is_not_found() {
        let dir = TempDir::new().unwrap();
        let h = handler(&dir);
        let gone = h.dispatch("convoy.info", ctx(json!({ "convoy": "nope" }))).await.unwrap_err();
        assert!(matches!(gone, AppError::NotFound(_)));
    }
}
