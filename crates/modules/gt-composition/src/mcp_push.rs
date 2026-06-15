//! Event-log → MCP push observer (gtcore-d366ff).
//!
//! The companion to [`gt_mcp_server::SessionRegistry`]: a background poll-tail of the
//! per-workspace event log that turns the facts agents used to **poll** for into a
//! **push** on their open MCP session. There is no broadcast bus on the MCP-server path
//! (see `stream.rs`), so — exactly like the `GET /stream` feed — this re-reads records
//! newer than a per-workspace cursor every tick and routes each relevant one to the
//! addressed agent's live peer through the registry.
//!
//! Three event kinds are observed:
//!
//! | event                      | addressed to        | pushed resource URI         |
//! |----------------------------|---------------------|-----------------------------|
//! | `message.sent.v1`          | the message's `to`  | `gt://inbox/{to}`           |
//! | `delegation.completed.v1`  | the delegation's `parent` | `gt://delegation/{child}` |
//! | `escalation.requested.v1`  | the requesting `session`  | `gt://escalation/{id}` |
//!
//! The same loop sees both the in-process writes (`a2a.send` runs in the MCP server) and
//! the cross-process ones (the orchd daemon appends `delegation.completed.v1`), because
//! both land in the same durable log this tails. An agent with no registered session is a
//! no-op (it keeps polling), so the push is purely additive.
//!
//! Startup does **not** replay history: existing partitions are primed to their newest
//! record so only events that occur after the observer starts are pushed. A partition
//! that first appears after startup is genuinely new, so its events are delivered.

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use gt_eventlog::EventRecord;
use gt_mcp_server::SessionRegistry;

use crate::delegation::DelegationEvent;
use crate::mcp::escalate::EscalationEvent;
use crate::mcp::a2a_msg::MessageEvent;
use crate::mcp::EventLog;

/// Most records one tick drains per workspace — bounds the read and matches the SSE
/// feed's `REPLAY_CAP`. More than this between ticks falls back to the agent's poll.
const POLL_CAP: usize = 256;

/// How often the observer re-reads the log tail (mirrors the feed's poll cadence).
const POLL_INTERVAL: Duration = Duration::from_secs(1);

/// How often idle sessions are reaped from the registry.
const SWEEP_INTERVAL: Duration = Duration::from_secs(60);

/// The partition every `None`/`default` write lands in (`EventLog::read_all` maps both
/// to this name on either backend), so the observer always polls it even before a
/// tenant-scoped partition directory exists.
const DEFAULT_WORKSPACE: &str = "default";

/// A resolved push: who to page and which resource changed. Decoupled from the registry
/// so the kind→target routing is unit-testable without a live session.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PushIntent {
    /// An A2A message arrived for `to`; page `gt://inbox/{to}`.
    Inbox { to: String },
    /// A delegated sub-task `child` terminated; page its `parent` with `gt://delegation/{child}`.
    Delegation { parent: String, child: String },
    /// `session` raised escalation `id`; page `gt://escalation/{id}`.
    Escalation { session: String, id: String },
}

/// Resolve one log record to a push, or `None` for an event the observer ignores.
/// Decode failures are `None` — a malformed or foreign payload is never a reason to act.
pub fn push_intent(record: &EventRecord) -> Option<PushIntent> {
    match record.kind.as_str() {
        "message.sent.v1" => match record.decode::<MessageEvent>().ok()? {
            MessageEvent::Sent { to, .. } => Some(PushIntent::Inbox { to }),
            _ => None,
        },
        "delegation.completed.v1" => match record.decode::<DelegationEvent>().ok()? {
            DelegationEvent::Completed { parent, child, .. } => {
                Some(PushIntent::Delegation { parent, child })
            }
            _ => None,
        },
        "escalation.requested.v1" => match record.decode::<EscalationEvent>().ok()? {
            EscalationEvent::Requested { session, id, .. } => {
                Some(PushIntent::Escalation { session, id })
            }
            _ => None,
        },
        _ => None,
    }
}

/// Deliver one intent through the registry. Returns the number of sessions paged
/// (`0` ⇒ the addressed agent has no open session, i.e. it is polling — a no-op).
async fn deliver(registry: &SessionRegistry, intent: PushIntent) -> usize {
    match intent {
        PushIntent::Inbox { to } => registry.notify_inbox(&to).await,
        PushIntent::Delegation { parent, child } => {
            registry.notify_delegation(&parent, &child).await
        }
        PushIntent::Escalation { session, id } => registry.notify_escalation(&session, &id).await,
    }
}

/// The set of partitions to poll this tick: every workspace currently on the log plus
/// the always-present `default` partition.
fn partitions(log: &EventLog) -> Vec<String> {
    let mut ws = log.workspaces();
    if !ws.iter().any(|w| w == DEFAULT_WORKSPACE) {
        ws.push(DEFAULT_WORKSPACE.to_string());
    }
    ws
}

/// Read each partition's records newer than its cursor and push the relevant ones.
/// Advances `cursors` to the newest record seen per partition. A partition absent from
/// `cursors` is read from its tail (`None`) — at startup the caller primes existing
/// partitions so only post-start events are delivered; a partition that appears later is
/// genuinely new, so its events page. Returns the number of pushes attempted.
pub async fn poll_once(
    log: &EventLog,
    registry: &SessionRegistry,
    cursors: &mut HashMap<String, String>,
    cap: usize,
) -> usize {
    let mut pushed = 0usize;
    for ws in partitions(log) {
        let since = cursors.get(&ws).cloned();
        let batch = log
            .read_since(Some(&ws), None, since.as_deref(), cap)
            .unwrap_or_default();
        for record in batch {
            cursors.insert(ws.clone(), record.ts.clone());
            if let Some(intent) = push_intent(&record) {
                pushed += deliver(registry, intent).await;
            }
        }
        // Ensure the partition has a cursor even when this tick saw nothing new, so a
        // later genuinely-new event compares against a stable marker.
        cursors.entry(ws).or_default();
    }
    pushed
}

/// Cursors seeded to each existing partition's newest record, so the observer's first
/// ticks do not re-deliver historical events. A partition with no records is seeded
/// empty (its future events are all new).
fn primed_cursors(log: &EventLog) -> HashMap<String, String> {
    let mut cursors = HashMap::new();
    for ws in partitions(log) {
        let newest = log
            .read_since(Some(&ws), None, None, 1)
            .ok()
            .and_then(|batch| batch.last().map(|r| r.ts.clone()))
            .unwrap_or_default();
        cursors.insert(ws, newest);
    }
    cursors
}

/// Spawn the push observer and the idle-session reaper. The observer poll-tails the log
/// and pushes inbox/delegation/escalation notifications to open sessions; the reaper
/// drops sessions idle past the registry's TTL. Both are best-effort background tasks —
/// a transient log read failure is skipped, never fatal.
pub fn spawn(log: Arc<EventLog>, registry: Arc<SessionRegistry>) {
    let observer_log = log.clone();
    let observer_registry = registry.clone();
    tokio::spawn(async move {
        let mut cursors = primed_cursors(&observer_log);
        loop {
            poll_once(&observer_log, &observer_registry, &mut cursors, POLL_CAP).await;
            tokio::time::sleep(POLL_INTERVAL).await;
        }
    });

    tokio::spawn(async move {
        loop {
            tokio::time::sleep(SWEEP_INTERVAL).await;
            registry.sweep();
        }
    });
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Mutex;

    use gt_mcp_server::McpNotifier;

    // ── push_intent routing ──────────────────────────────────────────────────

    fn record(kind: &str, payload: serde_json::Value, ts: &str) -> EventRecord {
        EventRecord {
            event_id: format!("ev-{ts}"),
            correlation_id: "corr".into(),
            causation_id: None,
            ts: ts.into(),
            kind: kind.into(),
            payload,
        }
    }

    #[test]
    fn message_sent_routes_to_recipient_inbox() {
        let rec = record(
            "message.sent.v1",
            serde_json::json!({"Sent": {"id": "msg-1", "from": "operator", "to": "agent-1", "body": "hi", "in_reply_to": null}}),
            "2026-06-15T00:00:01Z",
        );
        assert_eq!(
            push_intent(&rec),
            Some(PushIntent::Inbox { to: "agent-1".into() })
        );
    }

    #[test]
    fn delegation_completed_routes_to_parent_with_child() {
        let rec = record(
            "delegation.completed.v1",
            serde_json::json!({"Completed": {"child": "gtcore-c1", "parent": "agent-p", "state": "completed"}}),
            "2026-06-15T00:00:02Z",
        );
        assert_eq!(
            push_intent(&rec),
            Some(PushIntent::Delegation {
                parent: "agent-p".into(),
                child: "gtcore-c1".into()
            })
        );
    }

    #[test]
    fn escalation_requested_routes_to_session() {
        let rec = record(
            "escalation.requested.v1",
            serde_json::json!({"Requested": {"id": "esc-1", "session": "agent-x", "bead": null, "reason": "need input"}}),
            "2026-06-15T00:00:03Z",
        );
        assert_eq!(
            push_intent(&rec),
            Some(PushIntent::Escalation {
                session: "agent-x".into(),
                id: "esc-1".into()
            })
        );
    }

    #[test]
    fn unrelated_and_malformed_events_are_ignored() {
        // A kind we don't observe.
        assert_eq!(
            push_intent(&record("merge.merged.v1", serde_json::json!({}), "t")),
            None
        );
        // The acked half of the message channel is not a push.
        assert_eq!(
            push_intent(&record(
                "message.acked.v1",
                serde_json::json!({"Acked": {"id": "m", "by": "a"}}),
                "t"
            )),
            None
        );
        // Right kind, garbage payload → decode fails → ignored, never panics.
        assert_eq!(
            push_intent(&record("message.sent.v1", serde_json::json!({"junk": 1}), "t")),
            None
        );
    }

    // ── observer ↔ registry integration ──────────────────────────────────────

    /// Records the URIs the registry pushed to a given session.
    #[derive(Default)]
    struct Spy {
        uris: Mutex<Vec<String>>,
    }

    #[async_trait::async_trait]
    impl McpNotifier for Spy {
        async fn resource_updated(&self, uri: String) -> Result<(), ()> {
            self.uris.lock().unwrap().push(uri);
            Ok(())
        }
        async fn tool_list_changed(&self) -> Result<(), ()> {
            Ok(())
        }
    }

    /// The acceptance flow: an event landing in the log for a session with a live peer
    /// drives a `resource_updated` push to exactly that session.
    #[tokio::test]
    async fn event_in_log_pushes_to_the_open_session() {
        let dir = tempfile::tempdir().unwrap();
        let log = EventLog::new(Some(dir.path().to_path_buf()));
        let registry = Arc::new(SessionRegistry::new(Duration::from_secs(3600)));

        let spy = Arc::new(Spy::default());
        registry.register("sess-1", "default", "agent-1", spy.clone());

        // An operator sends agent-1 a message (the same write `a2a.send` performs).
        log.append(
            Some("default"),
            MessageEvent::Sent {
                id: "msg-1".into(),
                from: "operator".into(),
                to: "agent-1".into(),
                body: "check the logs".into(),
                in_reply_to: None,
            },
        )
        .unwrap();

        // Fresh cursors (no priming) so the just-appended event counts as new.
        let mut cursors = HashMap::new();
        let pushed = poll_once(&log, &registry, &mut cursors, POLL_CAP).await;

        assert_eq!(pushed, 1, "the open session is paged once");
        assert_eq!(&*spy.uris.lock().unwrap(), &["gt://inbox/agent-1".to_string()]);
    }

    #[tokio::test]
    async fn delegation_completion_pushes_to_the_parent() {
        let dir = tempfile::tempdir().unwrap();
        let log = EventLog::new(Some(dir.path().to_path_buf()));
        let registry = Arc::new(SessionRegistry::new(Duration::from_secs(3600)));
        let spy = Arc::new(Spy::default());
        registry.register("sess-p", "default", "parent-agent", spy.clone());

        log.append(
            Some("default"),
            DelegationEvent::Completed {
                child: "gtcore-child".into(),
                parent: "parent-agent".into(),
                state: "completed".into(),
            },
        )
        .unwrap();

        let mut cursors = HashMap::new();
        poll_once(&log, &registry, &mut cursors, POLL_CAP).await;

        assert_eq!(
            &*spy.uris.lock().unwrap(),
            &["gt://delegation/gtcore-child".to_string()]
        );
    }

    #[tokio::test]
    async fn an_already_seen_event_is_not_pushed_twice() {
        let dir = tempfile::tempdir().unwrap();
        let log = EventLog::new(Some(dir.path().to_path_buf()));
        let registry = Arc::new(SessionRegistry::new(Duration::from_secs(3600)));
        let spy = Arc::new(Spy::default());
        registry.register("sess-1", "default", "agent-1", spy.clone());

        log.append(
            Some("default"),
            MessageEvent::Sent {
                id: "msg-1".into(),
                from: "operator".into(),
                to: "agent-1".into(),
                body: "once".into(),
                in_reply_to: None,
            },
        )
        .unwrap();

        let mut cursors = HashMap::new();
        poll_once(&log, &registry, &mut cursors, POLL_CAP).await;
        // Second tick with the advanced cursor: nothing newer, no second push.
        let again = poll_once(&log, &registry, &mut cursors, POLL_CAP).await;

        assert_eq!(again, 0);
        assert_eq!(spy.uris.lock().unwrap().len(), 1, "delivered exactly once");
    }

    #[tokio::test]
    async fn priming_skips_startup_backlog() {
        let dir = tempfile::tempdir().unwrap();
        let log = EventLog::new(Some(dir.path().to_path_buf()));
        let registry = Arc::new(SessionRegistry::new(Duration::from_secs(3600)));
        let spy = Arc::new(Spy::default());
        registry.register("sess-1", "default", "agent-1", spy.clone());

        // A message that predates observer startup.
        log.append(
            Some("default"),
            MessageEvent::Sent {
                id: "old".into(),
                from: "operator".into(),
                to: "agent-1".into(),
                body: "stale".into(),
                in_reply_to: None,
            },
        )
        .unwrap();

        // Prime as the production loop does, then poll: the backlog must not be pushed.
        let mut cursors = primed_cursors(&log);
        let pushed = poll_once(&log, &registry, &mut cursors, POLL_CAP).await;
        assert_eq!(pushed, 0, "history is not replayed on startup");

        // But a message that arrives after priming is delivered.
        log.append(
            Some("default"),
            MessageEvent::Sent {
                id: "fresh".into(),
                from: "operator".into(),
                to: "agent-1".into(),
                body: "live".into(),
                in_reply_to: None,
            },
        )
        .unwrap();
        let pushed = poll_once(&log, &registry, &mut cursors, POLL_CAP).await;
        assert_eq!(pushed, 1, "post-start events are delivered");
    }

    #[tokio::test]
    async fn legacy_agent_without_session_is_a_noop() {
        let dir = tempfile::tempdir().unwrap();
        let log = EventLog::new(Some(dir.path().to_path_buf()));
        let registry = Arc::new(SessionRegistry::new(Duration::from_secs(3600)));
        // No session registered — the agent is polling, not streaming.

        log.append(
            Some("default"),
            MessageEvent::Sent {
                id: "msg-1".into(),
                from: "operator".into(),
                to: "agent-1".into(),
                body: "poll me".into(),
                in_reply_to: None,
            },
        )
        .unwrap();

        let mut cursors = HashMap::new();
        let pushed = poll_once(&log, &registry, &mut cursors, POLL_CAP).await;
        assert_eq!(pushed, 0, "no open session ⇒ no push, agent keeps polling");
    }
}
