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

use gt_channel::Channel;
use gt_events::{Command, EventKind};
use gt_mcp_server::{DomainCtx, DomainHandler};
use gt_module::McpTool;
use gt_orchestration::{
    CompleteMember, Convoy, ConvoyBoard, FailMember, LaunchConvoy, OrchEvent, OrchState,
};
use gt_scheduling::dispatch::DispatchPayload;
use gt_store_dolt::AppError;

use super::eventlog::EventLog;
use super::util::{descriptor, ev_err, parse, req, str_arg};

/// The event-log kind prefix for every convoy event (`convoy.*.v1`).
const NS: &str = "convoy.";

/// Queue priority for a convoy-dispatched member, matching the dispatch channel's own
/// default (`gt_scheduling::dispatch::DispatchPayload`) — convoys carry no priority.
const CONVOY_DISPATCH_PRIORITY: u8 = 1;

/// Event-sourced handler for the `convoy.*` tool namespace.
pub struct ConvoyHandler {
    log: Arc<EventLog>,
    /// Optional bridge to the orchd scheduler. A `MemberDispatched` runs in THIS process
    /// (the MCP server), but the polecat sling lives in the orchd daemon and there is no
    /// cross-process event bus — the dispatch gt-channel is the IPC. When wired, each
    /// dispatched member is dropped as a `{bead,priority}` request onto the same channel the
    /// orchd dispatch loop consumes, so a `convoy.launch` actually slings a polecat. `None`
    /// ⇒ events are still recorded but no sling fires (in-memory tests, GT_CHANNEL_ROOT
    /// unset). (hq-daemons-health-20260607.2)
    dispatch: Option<Arc<Channel>>,
}

impl ConvoyHandler {
    /// Wrap the per-workspace event log.
    pub fn new(log: Arc<EventLog>) -> Self {
        Self {
            log,
            dispatch: None,
        }
    }

    /// Wire the dispatch gt-channel so each `MemberDispatched` is bridged to the orchd
    /// scheduler. Without it the handler only records convoy events (no sling).
    pub fn with_dispatch_channel(mut self, channel: Arc<Channel>) -> Self {
        self.dispatch = Some(channel);
        self
    }

    /// Drop a `{bead,priority}` dispatch request onto the channel for one convoy member —
    /// the same shape the orchd dispatch loop already consumes. Best-effort: a failure logs
    /// but never fails the convoy call (the member is recorded; an operator can re-dispatch).
    fn bridge_to_scheduler(&self, channel: &Channel, member: String) {
        let payload = DispatchPayload {
            bead: member,
            title: None,
            priority: CONVOY_DISPATCH_PRIORITY,
        };
        match serde_json::to_vec(&payload) {
            Ok(bytes) => {
                if let Err(e) = channel.emit(&bytes) {
                    eprintln!(
                        "[convoy] dispatch bridge: emit failed for {} — {e}",
                        payload.bead
                    );
                }
            }
            Err(e) => eprintln!("[convoy] dispatch bridge: serialize failed — {e}"),
        }
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
        // Members this command feeds to crew — bridged to the orchd scheduler after the
        // events are durable (the in-process hub never sees these cross-process events).
        let dispatched: Vec<String> = events
            .iter()
            .filter_map(|e| match e {
                OrchEvent::MemberDispatched { member, .. } => Some(member.clone()),
                _ => None,
            })
            .collect();
        for event in events {
            self.log.append(ws, event)?;
        }
        if let Some(channel) = &self.dispatch {
            for member in dispatched {
                self.bridge_to_scheduler(channel, member);
            }
        }
        Ok(json!({ "ok": true, "events": kinds }))
    }
}

#[async_trait]
impl DomainHandler for ConvoyHandler {
    fn namespace(&self) -> &'static str {
        "convoy"
    }

    fn descriptors(&self) -> Vec<McpTool> {
        vec![
            descriptor(
                "convoy.launch",
                "Launch a convoy over an ordered list of member beads; dispatches the first member.",
                &[req("convoy", "string"), req("members", "array")],
            ),
            descriptor(
                "convoy.complete-member",
                "Mark a convoy member done; hands off to the next member, closing the convoy when all are done.",
                &[req("convoy", "string"), req("member", "string")],
            ),
            descriptor(
                "convoy.fail-member",
                "Mark a convoy member failed with a reason.",
                &[req("convoy", "string"), req("member", "string"), req("reason", "string")],
            ),
            descriptor("convoy.list", "List every convoy in the workspace with its state + members.", &[]),
            descriptor("convoy.info", "Show one convoy's state + members.", &[req("convoy", "string")]),
        ]
    }

    async fn dispatch(&self, tool: &str, ctx: DomainCtx<'_>) -> Result<Value, AppError> {
        let ws = ctx.workspace;
        match tool {
            "convoy.launch" => self.run::<LaunchConvoy>(ws, ctx.args),
            "convoy.complete-member" => self.run::<CompleteMember>(ws, ctx.args),
            "convoy.fail-member" => self.run::<FailMember>(ws, ctx.args),
            "convoy.list" => {
                let board = self.board(ws)?;
                Ok(
                    json!({ "convoys": board.snapshot().iter().map(convoy_json).collect::<Vec<_>>() }),
                )
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
        DomainCtx {
            workspace: None,
            actor: "tester",
            args,
        }
    }

    /// Launch a convoy, then drive its members to done — board rehydrated from
    /// the log each call. The first member is Active after launch (sequential
    /// handoff), the next becomes Active when the prior completes.
    #[tokio::test]
    async fn convoy_lifecycle_through_event_log() {
        let dir = TempDir::new().unwrap();
        let h = handler(&dir);

        h.dispatch(
            "convoy.launch",
            ctx(json!({ "convoy": "c1", "members": ["b1", "b2"] })),
        )
        .await
        .unwrap();

        let info = h
            .dispatch("convoy.info", ctx(json!({ "convoy": "c1" })))
            .await
            .unwrap();
        assert_eq!(info["state"], "launched");
        let members = info["members"].as_array().unwrap();
        assert_eq!(
            members[0]["state"], "active",
            "first member dispatched on launch"
        );
        assert_eq!(members[1]["state"], "pending");

        h.dispatch(
            "convoy.complete-member",
            ctx(json!({ "convoy": "c1", "member": "b1" })),
        )
        .await
        .unwrap();
        let info = h
            .dispatch("convoy.info", ctx(json!({ "convoy": "c1" })))
            .await
            .unwrap();
        let members = info["members"].as_array().unwrap();
        assert_eq!(members[0]["state"], "done");
        assert_eq!(members[1]["state"], "active", "handoff to next member");

        h.dispatch(
            "convoy.complete-member",
            ctx(json!({ "convoy": "c1", "member": "b2" })),
        )
        .await
        .unwrap();
        let info = h
            .dispatch("convoy.info", ctx(json!({ "convoy": "c1" })))
            .await
            .unwrap();
        assert_eq!(
            info["state"], "closed",
            "convoy closes when all members done"
        );

        let list = h.dispatch("convoy.list", ctx(json!({}))).await.unwrap();
        assert_eq!(list["convoys"].as_array().unwrap().len(), 1);
    }

    #[tokio::test]
    async fn missing_convoy_is_not_found() {
        let dir = TempDir::new().unwrap();
        let h = handler(&dir);
        let gone = h
            .dispatch("convoy.info", ctx(json!({ "convoy": "nope" })))
            .await
            .unwrap_err();
        assert!(matches!(gone, AppError::NotFound(_)));
    }

    /// Read every queued dispatch request from a channel dir, parsed as the payload the
    /// orchd dispatch loop consumes. Reads the files directly (no inotify) for determinism.
    fn drain_dispatch(dir: &std::path::Path) -> Vec<DispatchPayload> {
        let mut paths: Vec<_> = std::fs::read_dir(dir)
            .unwrap()
            .filter_map(|e| e.ok())
            .map(|e| e.path())
            .filter(|p| p.extension().is_some_and(|x| x == "event"))
            .collect();
        paths.sort();
        paths
            .iter()
            .map(|p| serde_json::from_slice(&std::fs::read(p).unwrap()).unwrap())
            .collect()
    }

    /// With the bridge wired, a `convoy.launch` drops a `{bead,priority}` dispatch request
    /// for the first member, and each handoff drops one for the next — the cross-process
    /// signal that makes orchd actually sling a polecat (hq-daemons-health-20260607.2).
    #[tokio::test]
    async fn launch_and_handoff_bridge_members_to_the_dispatch_channel() {
        let dir = TempDir::new().unwrap();
        let chan_dir = TempDir::new().unwrap();
        let channel = Channel::open(chan_dir.path(), "dispatch").unwrap();
        let h = ConvoyHandler::new(Arc::new(EventLog::new(Some(dir.path().to_path_buf()))))
            .with_dispatch_channel(Arc::new(channel));

        h.dispatch(
            "convoy.launch",
            ctx(json!({ "convoy": "c1", "members": ["b1", "b2"] })),
        )
        .await
        .unwrap();

        let reqs = drain_dispatch(&chan_dir.path().join("dispatch"));
        assert_eq!(
            reqs.len(),
            1,
            "only the first member is dispatched on launch"
        );
        assert_eq!(reqs[0].bead, "b1");
        assert_eq!(reqs[0].priority, CONVOY_DISPATCH_PRIORITY);

        h.dispatch(
            "convoy.complete-member",
            ctx(json!({ "convoy": "c1", "member": "b1" })),
        )
        .await
        .unwrap();

        let reqs = drain_dispatch(&chan_dir.path().join("dispatch"));
        let beads: Vec<&str> = reqs.iter().map(|r| r.bead.as_str()).collect();
        assert!(
            beads.contains(&"b2"),
            "handoff dispatches the next member, got {beads:?}"
        );
    }

    /// Without a wired channel the handler still records convoy events (no panic, no sling) —
    /// the in-memory / GT_CHANNEL_ROOT-unset path is unchanged.
    #[tokio::test]
    async fn launch_without_a_channel_records_but_does_not_bridge() {
        let dir = TempDir::new().unwrap();
        let h = handler(&dir);
        let out = h
            .dispatch(
                "convoy.launch",
                ctx(json!({ "convoy": "c1", "members": ["b1"] })),
            )
            .await
            .unwrap();
        assert_eq!(out["ok"], true);
    }
}
