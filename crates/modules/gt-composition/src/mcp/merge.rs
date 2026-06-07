//! `merge.*` domain dispatch (`hq-mcp-dispatch.5`).
//!
//! The merge board is event-sourced — no projection table — so each call
//! rehydrates the [`MergeBoard`] from the workspace event log, executes the
//! command, and appends the resulting [`MergeEvent`] back to the log (see
//! [`EventLog`]).
//!
//! Tools: `merge.submit` / `merge.start` / `merge.complete` / `merge.fail`
//! (the [`MergeCommand`] mutations), plus `merge.list` / `merge.info` reads.

use std::sync::Arc;

use async_trait::async_trait;
use serde::de::DeserializeOwned;
use serde_json::{json, Value};

use gt_events::Command;
use gt_graphwarden::{MarkStale, WardenCommand, WardenState};
use gt_mcp_server::prefixes::bead_prefix;
use gt_mcp_server::{DomainCtx, DomainHandler};
use gt_merge::{
    CompleteMerge, FailMerge, MergeBoard, MergeEvent, MergeSlot, MergeState, StartMerge,
    SubmitMerge,
};
use gt_module::McpTool;
use gt_rig::{PgRigs, RigRepository};
use gt_store_dolt::AppError;

use super::eventlog::EventLog;
use super::pools::WsPools;
use super::util::{descriptor, ev_err, now_secs, parse, req, str_arg};

/// The event-log kind prefix for every merge event (`merge.*.v1`).
const NS: &str = "merge.";
/// The warden event-log kind prefix, replayed to check graph custody (`hq-graphrig.7`).
const WARDEN_NS: &str = "graphwarden.";

/// Event-sourced handler for the `merge.*` tool namespace.
pub struct MergeHandler {
    log: Arc<EventLog>,
    /// Optional rig-catalog pools: when set, a completed merge marks the owning rig's
    /// graph stale so the custodian knows to refresh it (`hq-graphrig.7`). `None` keeps the
    /// handler a pure merge handler (tests, issues-only builds).
    rig_pools: Option<Arc<WsPools>>,
}

impl MergeHandler {
    /// Wrap the per-workspace event log. No graph custody side-effect.
    pub fn new(log: Arc<EventLog>) -> Self {
        Self {
            log,
            rig_pools: None,
        }
    }

    /// Also mark the owning rig's graph stale on a completed merge, resolving the rig from
    /// the merged bead's prefix via `pools`' per-workspace rig catalog.
    pub fn with_rig_pools(mut self, pools: Arc<WsPools>) -> Self {
        self.rig_pools = Some(pools);
        self
    }

    /// Best-effort: mark the graph of the rig owning `bead` stale, so the custodian's next
    /// `graph.refresh` picks it up. Silent on every miss (no pools, prefix unmapped, rig not
    /// under graph custody) — a merge must never fail because of a graph side-effect.
    async fn mark_owning_rig_stale(&self, ws: Option<&str>, bead: &str) {
        let Some(pools) = &self.rig_pools else { return };
        let Ok(pool) = pools.get(ws).await else {
            return;
        };
        let repo = PgRigs::new(pool.pool().clone());
        let Ok(Some(rig)) = repo.prefix_owner(bead_prefix(bead)).await else {
            return;
        };

        // Only mark stale if the rig is actually under graph custody.
        let state = self
            .log
            .replay_domain(ws, WARDEN_NS, WardenState::default(), |s, e| {
                let _ = s.apply(e);
            })
            .unwrap_or_default();
        if !state.rigs.contains_key(&rig) {
            return;
        }
        let cmd = WardenCommand::MarkStale(MarkStale {
            rig,
            changed: 1,
            now_secs: now_secs(),
        });
        if let Ok(events) = cmd.execute(&state) {
            for ev in events {
                let _ = self.log.append(ws, ev);
            }
        }
    }

    /// Rebuild the board from the workspace's merge events.
    fn board(&self, ws: Option<&str>) -> Result<MergeBoard, AppError> {
        let state = self
            .log
            .replay_domain(ws, NS, MergeState::default(), MergeState::apply)?;
        Ok(MergeBoard::from_state(&state))
    }

    /// Parse → execute against the rehydrated board → append the produced event.
    fn run<C>(&self, ws: Option<&str>, args: Value) -> Result<Value, AppError>
    where
        C: Command<State = MergeBoard, Output = MergeEvent> + DeserializeOwned,
    {
        let cmd: C = parse(args)?;
        let mut board = self.board(ws)?;
        let event = cmd.execute(&mut board).map_err(ev_err)?;
        let kind = gt_events::EventKind::kind(&event);
        self.log.append(ws, event)?;
        Ok(json!({ "ok": true, "event": kind }))
    }
}

#[async_trait]
impl DomainHandler for MergeHandler {
    fn namespace(&self) -> &'static str {
        "merge"
    }

    fn descriptors(&self) -> Vec<McpTool> {
        vec![
            descriptor(
                "merge.submit",
                "Submit a bead's branch into the merge queue.",
                &[
                    req("bead", "string"),
                    req("branch", "string"),
                    req("channel_msg_id", "string"),
                ],
            ),
            descriptor(
                "merge.start",
                "Start merging a queued bead.",
                &[req("bead", "string")],
            ),
            descriptor(
                "merge.complete",
                "Mark a bead's merge complete with the landed commit sha.",
                &[req("bead", "string"), req("sha", "string")],
            ),
            descriptor(
                "merge.fail",
                "Mark a bead's merge failed with a reason.",
                &[req("bead", "string"), req("reason", "string")],
            ),
            descriptor("merge.list", "List every slot on the merge board.", &[]),
            descriptor(
                "merge.info",
                "Show one bead's merge slot.",
                &[req("bead", "string")],
            ),
        ]
    }

    async fn dispatch(&self, tool: &str, ctx: DomainCtx<'_>) -> Result<Value, AppError> {
        let ws = ctx.workspace;
        match tool {
            "merge.submit" => self.run::<SubmitMerge>(ws, ctx.args),
            "merge.start" => self.run::<StartMerge>(ws, ctx.args),
            "merge.complete" => {
                // Mark the owning rig's graph stale after a successful completion —
                // best-effort, never fails the merge (hq-graphrig.7).
                let bead = str_arg(&ctx.args, "bead").ok().map(str::to_string);
                let out = self.run::<CompleteMerge>(ws, ctx.args)?;
                if let Some(bead) = bead {
                    self.mark_owning_rig_stale(ws, &bead).await;
                }
                Ok(out)
            }
            "merge.fail" => self.run::<FailMerge>(ws, ctx.args),
            "merge.list" => {
                let board = self.board(ws)?;
                Ok(json!({ "slots": board.snapshot().iter().map(slot_json).collect::<Vec<_>>() }))
            }
            "merge.info" => {
                let bead = str_arg(&ctx.args, "bead")?;
                match self.board(ws)?.get(bead) {
                    Some(slot) => Ok(slot_json(slot)),
                    None => Err(AppError::NotFound(format!("merge slot {bead}"))),
                }
            }
            other => Err(AppError::Validation(format!("unknown tool `{other}`"))),
        }
    }
}

/// Shape one merge slot as the dispatch payload.
fn slot_json(slot: &MergeSlot) -> Value {
    json!({
        "bead": slot.bead,
        "branch": slot.branch,
        "state": slot.state.as_str(),
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    fn handler(dir: &TempDir) -> MergeHandler {
        MergeHandler::new(Arc::new(EventLog::new(Some(dir.path().to_path_buf()))))
    }

    fn ctx(args: Value) -> DomainCtx<'static> {
        DomainCtx {
            workspace: None,
            actor: "tester",
            args,
        }
    }

    /// Full lifecycle over the event log: submit → start → complete, with the
    /// board rehydrated from the log on every call (no shared in-memory state).
    #[tokio::test]
    async fn merge_lifecycle_through_event_log() {
        let dir = TempDir::new().unwrap();
        let h = handler(&dir);

        h.dispatch(
            "merge.submit",
            ctx(json!({ "bead": "b1", "branch": "feat/b1", "channel_msg_id": "m1" })),
        )
        .await
        .unwrap();

        // Re-submitting the same bead is rejected against the rehydrated board.
        let dup = h
            .dispatch(
                "merge.submit",
                ctx(json!({ "bead": "b1", "branch": "feat/b1", "channel_msg_id": "m2" })),
            )
            .await
            .unwrap_err();
        assert!(matches!(dup, AppError::Validation(_)));

        let info = h
            .dispatch("merge.info", ctx(json!({ "bead": "b1" })))
            .await
            .unwrap();
        assert_eq!(info["state"], "ready");

        h.dispatch("merge.start", ctx(json!({ "bead": "b1" })))
            .await
            .unwrap();
        let info = h
            .dispatch("merge.info", ctx(json!({ "bead": "b1" })))
            .await
            .unwrap();
        assert_eq!(info["state"], "merging");

        h.dispatch(
            "merge.complete",
            ctx(json!({ "bead": "b1", "sha": "abc1234" })),
        )
        .await
        .unwrap();
        let info = h
            .dispatch("merge.info", ctx(json!({ "bead": "b1" })))
            .await
            .unwrap();
        assert_eq!(info["state"], "merged");

        let list = h.dispatch("merge.list", ctx(json!({}))).await.unwrap();
        assert_eq!(list["slots"].as_array().unwrap().len(), 1);
    }

    /// An illegal transition (complete without start) surfaces as an error, and
    /// info on an unknown bead is a not-found.
    #[tokio::test]
    async fn illegal_transition_and_missing_slot() {
        let dir = TempDir::new().unwrap();
        let h = handler(&dir);

        let gone = h
            .dispatch("merge.info", ctx(json!({ "bead": "nope" })))
            .await
            .unwrap_err();
        assert!(matches!(gone, AppError::NotFound(_)));

        h.dispatch(
            "merge.submit",
            ctx(json!({ "bead": "b1", "branch": "x", "channel_msg_id": "m1" })),
        )
        .await
        .unwrap();
        // Ready → Merged is illegal (must go through Merging).
        let bad = h
            .dispatch(
                "merge.complete",
                ctx(json!({ "bead": "b1", "sha": "deadbee" })),
            )
            .await
            .unwrap_err();
        assert!(matches!(bad, AppError::InvalidTransition(_)));
    }
}
