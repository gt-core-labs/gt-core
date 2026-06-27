//! Convoy end-to-end wiring (epic gtcore-e719c1) — the edge producers `gt-orchestration`
//! documents as "in the bins/gt composition root, not built yet".
//!
//! Two gaps kept convoys from running on their own:
//!
//! - **dispatch (gtcore-7d16f0):** `ConvoyHandler` drops each launched member onto the dispatch
//!   channel, but the supervisor's slingability gate ([`crate::polecat::bead_should_sling`])
//!   requires `dispatch=auto` — and a convoy member is always `dispatch=manual`, so every member
//!   was skipped (`sling skipped … not slingable (…manual…)`). [`active_convoy_membership`] lets
//!   the gate recognise an EXPLICIT convoy dispatch and sling it anyway (still never a
//!   closed/epic bead, still honouring rig-hold).
//! - **completion (gtcore-896a29):** nothing translated a member bead closing into
//!   `convoy.complete-member`, so a convoy never advanced past its first member.
//!   [`ConvoyCompletionPlugin`] reacts to `issues.closed.v1`, completes the member, feeds the next
//!   one (bridged onto the dispatch channel so it actually slings) and closes the convoy when all
//!   members are done.
//!
//! Both read the convoy board by replaying the `convoy.*` domain from the shared event log — the
//! same log + reducer (`OrchState::apply`) the `ConvoyHandler` writes, so the views stay coherent.

use std::sync::Arc;

use async_trait::async_trait;
use gt_channel::DispatchSink;
use gt_eventlog::EventRecord;
// `gt_events::AppError` is the error the `Plugin` trait + the orchestration commands speak; the
// `EventLog` wrapper speaks `gt_store_dolt::AppError` (fully-qualified at its one use below). They
// are distinct types, so EventLog read/write errors are handled locally, never `?`-propagated.
use gt_events::{AppError, Command};
use gt_orchestration::{CompleteMember, ConvoyBoard, ConvoyState, MemberState, OrchEvent, OrchState};
use gt_plugin::Plugin;
use gt_scheduling::dispatch::DispatchPayload;

use crate::mcp::eventlog::EventLog;

/// Convoy domain namespace prefix in the shared event log.
const NS: &str = "convoy.";
/// Same queue priority the `ConvoyHandler` launch bridge uses (convoys carry no priority).
const CONVOY_DISPATCH_PRIORITY: u8 = 1;

/// Rebuild the convoy board for `ws` by folding the `convoy.*` domain of the shared event log.
fn replay_board(
    log: &EventLog,
    ws: Option<&str>,
) -> Result<ConvoyBoard, gt_store_dolt::AppError> {
    let state = log.replay_domain(ws, NS, OrchState::default(), OrchState::apply)?;
    Ok(ConvoyBoard::from_state(&state))
}

/// The id of the `Launched` convoy in which `bead` is currently the `Active` member, if any.
///
/// The slingability gate ([`crate::polecat::bead_should_sling`]) calls this to recognise that a
/// `dispatch=manual` bead is nonetheless an EXPLICIT convoy dispatch and must be slung. A read
/// error degrades to `None` (no override ⇒ the strict `dispatch=auto` gate still applies) — the
/// same conservative degradation the rest of the sling path uses.
pub fn active_convoy_membership(log: &EventLog, ws: Option<&str>, bead: &str) -> Option<String> {
    convoy_with_active_member(&replay_board(log, ws).ok()?, bead)
}

/// The id of the `Launched` convoy whose `Active` member is `bead`, scanning a rebuilt board.
fn convoy_with_active_member(board: &ConvoyBoard, bead: &str) -> Option<String> {
    board.snapshot().into_iter().find_map(|c| {
        let active_here = c.state == ConvoyState::Launched
            && c
                .members
                .iter()
                .any(|m| m.bead == bead && m.state == MemberState::Active);
        active_here.then_some(c.id)
    })
}

/// Drop a `{bead,priority}` dispatch request onto the channel for one convoy member — the exact
/// shape the orchd dispatch loop consumes and the same bridge `ConvoyHandler::run` uses. Best
/// effort: a failure logs but never propagates (the member stays recorded; an operator can
/// re-dispatch).
fn bridge_to_scheduler(channel: &DispatchSink, member: String) {
    let payload = DispatchPayload {
        bead: member,
        title: None,
        priority: CONVOY_DISPATCH_PRIORITY,
    };
    match serde_json::to_vec(&payload) {
        Ok(bytes) => {
            if let Err(e) = channel.emit(&bytes) {
                eprintln!(
                    "[convoy-reactor] dispatch bridge: emit failed for {} — {e}",
                    payload.bead
                );
            }
        }
        Err(e) => eprintln!("[convoy-reactor] dispatch bridge: serialize failed — {e}"),
    }
}

/// Hub plugin that advances a convoy when one of its member beads closes (gtcore-896a29).
///
/// On `issues.closed.v1`, if the closed bead is the `Active` member of a `Launched` convoy, it
/// runs [`CompleteMember`] against the event-sourced board: the member flips to `Done`, the next
/// `Pending` member is fed (`MemberDispatched`, bridged onto the dispatch channel so it actually
/// slings) or — when all members are done — the convoy closes (`ConvoyClosed`). Every produced
/// event is appended to the same shared log the `ConvoyHandler` writes, so `convoy.info`/`list`
/// reflect the advance.
///
/// Best-effort by design: a foreign close, a non-member bead, an already-`Done` member or a replay
/// error is a clean no-op — nothing returns `Err` (a dead-letter would not help a convoy advance).
pub struct ConvoyCompletionPlugin {
    log: Arc<EventLog>,
    ws: Option<String>,
    dispatch: Option<Arc<DispatchSink>>,
}

impl ConvoyCompletionPlugin {
    /// Wrap the shared event log; `ws` is the workspace the convoy domain lives under (the orchd's
    /// `GT_WORKSPACE`, `None` ⇒ the default workspace).
    pub fn new(log: Arc<EventLog>, ws: Option<String>) -> Self {
        Self {
            log,
            ws,
            dispatch: None,
        }
    }

    /// Wire the dispatch sink so the next member fed by the handoff actually slings. Without it the
    /// member still flips to `Active` on the board but no polecat is launched (in-memory tests, or
    /// `GT_CHANNEL_ROOT` unset).
    pub fn with_dispatch_channel(mut self, channel: Arc<DispatchSink>) -> Self {
        self.dispatch = Some(channel);
        self
    }

    /// Core transition, factored out for unit tests: complete `bead`'s membership and return the
    /// members the handoff dispatched (already appended to the log).
    fn advance(&self, bead: &str) -> Result<Vec<String>, AppError> {
        let ws = self.ws.as_deref();
        let mut board = match replay_board(&self.log, ws) {
            Ok(b) => b,
            Err(e) => {
                // Best-effort: a transient log read must not poison the hub fan-out.
                eprintln!("[convoy-reactor] replay failed for closed bead {bead} — {e}");
                return Ok(vec![]);
            }
        };
        let Some(convoy) = convoy_with_active_member(&board, bead) else {
            return Ok(vec![]); // not an active convoy member — clean no-op
        };
        let cmd = CompleteMember {
            convoy: convoy.clone(),
            member: bead.to_string(),
        };
        // An already-Done member would surface as InvalidTransition, but the membership check above
        // already guaranteed Active — a residual error here is a genuine fault worth propagating.
        let events = cmd.execute(&mut board)?;
        let dispatched: Vec<String> = events
            .iter()
            .filter_map(|e| match e {
                OrchEvent::MemberDispatched { member, .. } => Some(member.clone()),
                _ => None,
            })
            .collect();
        // Persist first (durable), then bridge the cross-process dispatches — mirror
        // `ConvoyHandler::run`: the in-process hub never sees these convoy events. An append failure
        // is logged, not `?`-propagated (its error type differs from the trait's).
        for event in events {
            if let Err(e) = self.log.append(ws, event) {
                eprintln!("[convoy-reactor] append failed for convoy {convoy} — {e}");
                return Ok(dispatched);
            }
        }
        eprintln!(
            "[convoy-reactor] convoy {convoy}: member {bead} done (handoff dispatched {})",
            dispatched.len()
        );
        if let Some(channel) = &self.dispatch {
            for member in dispatched.iter().cloned() {
                bridge_to_scheduler(channel, member);
            }
        }
        Ok(dispatched)
    }
}

#[async_trait]
impl Plugin for ConvoyCompletionPlugin {
    fn name(&self) -> &'static str {
        "convoy-completion"
    }

    async fn on_event(&self, record: &EventRecord) -> Result<(), AppError> {
        if record.kind != "issues.closed.v1" {
            return Ok(());
        }
        let Some(bead) = record.payload.get("id").and_then(|v| v.as_str()) else {
            return Ok(());
        };
        // Swallow the error: a convoy that can't advance must not poison the hub fan-out.
        let _ = self.advance(bead);
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use gt_orchestration::LaunchConvoy;
    use serde_json::json;

    fn tmp_log() -> (Arc<EventLog>, tempfile::TempDir) {
        let dir = tempfile::tempdir().unwrap();
        (Arc::new(EventLog::new(Some(dir.path().to_path_buf()))), dir)
    }

    /// Launch a convoy over `members` into the log under the default workspace. `LaunchConvoy`
    /// creates + launches + feeds the first member, so member[0] lands `Active`.
    fn launch(log: &Arc<EventLog>, convoy: &str, members: &[&str]) {
        let cmd = LaunchConvoy {
            convoy: convoy.into(),
            members: members.iter().map(|m| m.to_string()).collect(),
        };
        let mut board = replay_board(log, None).unwrap();
        let events = cmd.execute(&mut board).unwrap();
        for ev in events {
            log.append(None, ev).unwrap();
        }
    }

    fn board(log: &Arc<EventLog>) -> ConvoyBoard {
        replay_board(log, None).unwrap()
    }

    fn member_state(log: &Arc<EventLog>, convoy: &str, bead: &str) -> Option<MemberState> {
        board(log)
            .get(convoy)
            .and_then(|c| c.members.iter().find(|m| m.bead == bead).map(|m| m.state))
    }

    #[test]
    fn handoff_feeds_next_member_and_keeps_convoy_launched() {
        let (log, _d) = tmp_log();
        launch(&log, "cv", &["a", "b"]);
        // launch dispatches the first member: a Active, b Pending.
        assert_eq!(member_state(&log, "cv", "a"), Some(MemberState::Active));
        assert_eq!(member_state(&log, "cv", "b"), Some(MemberState::Pending));

        let reactor = ConvoyCompletionPlugin::new(log.clone(), None);
        let dispatched = reactor.advance("a").unwrap();

        assert_eq!(dispatched, vec!["b".to_string()], "handoff feeds the next member");
        assert_eq!(member_state(&log, "cv", "a"), Some(MemberState::Done));
        assert_eq!(member_state(&log, "cv", "b"), Some(MemberState::Active));
        assert_eq!(board(&log).get("cv").unwrap().state, ConvoyState::Launched);
    }

    #[test]
    fn closing_last_member_closes_the_convoy() {
        let (log, _d) = tmp_log();
        launch(&log, "cv", &["a"]);
        let reactor = ConvoyCompletionPlugin::new(log.clone(), None);

        let dispatched = reactor.advance("a").unwrap();

        assert!(dispatched.is_empty(), "no next member to feed");
        assert_eq!(member_state(&log, "cv", "a"), Some(MemberState::Done));
        assert_eq!(board(&log).get("cv").unwrap().state, ConvoyState::Closed);
    }

    #[test]
    fn non_member_close_is_a_noop() {
        let (log, _d) = tmp_log();
        launch(&log, "cv", &["a", "b"]);
        let reactor = ConvoyCompletionPlugin::new(log.clone(), None);

        let dispatched = reactor.advance("zzz-not-a-member").unwrap();

        assert!(dispatched.is_empty());
        // Board untouched.
        assert_eq!(member_state(&log, "cv", "a"), Some(MemberState::Active));
        assert_eq!(member_state(&log, "cv", "b"), Some(MemberState::Pending));
    }

    #[test]
    fn second_close_of_done_member_is_idempotent_noop() {
        let (log, _d) = tmp_log();
        launch(&log, "cv", &["a", "b"]);
        let reactor = ConvoyCompletionPlugin::new(log.clone(), None);
        reactor.advance("a").unwrap();
        // a is now Done; replaying the close must not error or re-advance.
        let again = reactor.advance("a");
        assert!(again.is_err() || again.unwrap().is_empty());
        assert_eq!(member_state(&log, "cv", "b"), Some(MemberState::Active));
    }

    #[test]
    fn active_convoy_membership_finds_then_clears() {
        let (log, _d) = tmp_log();
        launch(&log, "cv", &["a", "b"]);
        assert_eq!(active_convoy_membership(&log, None, "a"), Some("cv".to_string()));
        // b is Pending, not Active → not an explicit dispatch yet.
        assert_eq!(active_convoy_membership(&log, None, "b"), None);

        let reactor = ConvoyCompletionPlugin::new(log.clone(), None);
        reactor.advance("a").unwrap();
        // After the handoff, a is Done (no longer overridable) and b is Active.
        assert_eq!(active_convoy_membership(&log, None, "a"), None);
        assert_eq!(active_convoy_membership(&log, None, "b"), Some("cv".to_string()));
    }

    fn record(kind: &str, id: &str) -> EventRecord {
        EventRecord {
            event_id: "e".into(),
            correlation_id: "c".into(),
            causation_id: None,
            ts: "1970-01-01T00:00:00Z".into(),
            kind: kind.into(),
            payload: json!({ "id": id }),
        }
    }

    #[tokio::test]
    async fn on_event_ignores_foreign_kinds() {
        let (log, _d) = tmp_log();
        launch(&log, "cv", &["a"]);
        let reactor = ConvoyCompletionPlugin::new(log.clone(), None);
        reactor.on_event(&record("merge.merged.v1", "a")).await.unwrap();
        // Untouched: a stays Active (the close kind never matched).
        assert_eq!(member_state(&log, "cv", "a"), Some(MemberState::Active));
    }

    #[tokio::test]
    async fn on_event_completes_member_on_close() {
        let (log, _d) = tmp_log();
        launch(&log, "cv", &["a", "b"]);
        let reactor = ConvoyCompletionPlugin::new(log.clone(), None);
        reactor.on_event(&record("issues.closed.v1", "a")).await.unwrap();
        assert_eq!(member_state(&log, "cv", "a"), Some(MemberState::Done));
        assert_eq!(member_state(&log, "cv", "b"), Some(MemberState::Active));
    }
}
