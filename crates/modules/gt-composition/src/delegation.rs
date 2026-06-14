//! Delegation push-callbacks (B5, gtcore-1bda00 — epic gtcore-155917).
//!
//! When an agent delegates a sub-task — over the MCP `a2a.delegate` tool or the
//! HTTP A2A `tasks/send` — the child bead runs autonomously and the parent has
//! historically had only ONE way to learn the outcome: poll `a2a.status` in a
//! loop. This module replaces that poll with a **push**: the daemon observes the
//! child reaching a terminal state and emits a callback, so the parent (and the
//! operator bell) learn the moment it lands instead of on the next poll.
//!
//! Three pieces, mirroring the existing observer/ticker patterns
//! ([`BeadClosePlugin`](crate::bead_close::BeadClosePlugin),
//! [`WorkflowNotifyPlugin`](crate::workflow_notify::WorkflowNotifyPlugin), the
//! patrol/quota ticks in `gt-orch-server`):
//!
//! 1. A [`DelegationEvent`] pair on the `delegation.` namespace. `a2a.delegate`
//!    appends [`Requested`](DelegationEvent::Requested) at delegation time
//!    (carrying the parent's session, the child's rig, the start timestamp, and
//!    the per-task timeout); the callback/ticker append
//!    [`Completed`](DelegationEvent::Completed) when the child terminates or the
//!    timeout fires. Folding the two ([`DelegationRegistry`]) gives the set of
//!    still-open delegations — the same event-sourced-projection shape
//!    `escalate.*` uses.
//!
//! 2. [`DelegationCallbackPlugin`] — a hub observer. On `merge.merged.v1` /
//!    `merge.failed.v1` / `agent.killed.v1` (the terminal facts the orchd hub
//!    actually carries) it checks whether the bead is an OPEN delegation and, if
//!    so, fires the callback: a durable `delegation.completed.v1` (the signal
//!    the parent/dashboard subscribe to, channel `delegation`) plus, when a
//!    Postgres bell is wired, an operator notification. Acting only on OPEN
//!    delegations makes a replayed terminal event a no-op (idempotent).
//!
//! 3. [`DelegationTimeoutTicker`] — a periodic sweep. Any open delegation older
//!    than its `timeout_secs` is auto-escalated (an `escalation.requested.v1`
//!    reusing the `escalate.*` registry, so it surfaces on the dashboard and can
//!    be `escalate.respond`-ed) and marked `Completed { state: "timed_out" }` so
//!    it never fires twice. `timeout_secs == 0` disables the timeout for a task.
//!
//! Everything is best-effort: a PG outage or a log-append failure is logged and
//! never breaks the relay/tick — the child bead's own lifecycle is the source of
//! truth, callbacks are observational.

use std::sync::Arc;

use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use sqlx::PgPool;
use time::format_description::well_known::Rfc3339;
use time::OffsetDateTime;

use gt_agent::AgentEvent;
use gt_eventlog::EventRecord;
use gt_events::{AppError, EventKind};
use gt_merge::MergeEvent;
use gt_plugin::Plugin;
// `EventLog::replay_domain` reports the store's error type, distinct from the
// `gt_events::AppError` the `Plugin` trait returns — alias it to keep them apart.
use gt_store_dolt::AppError as StoreError;

use crate::mcp::escalate::{EscalationEvent, EscalationMode};
use crate::mcp::eventlog::EventLog;

/// The event-log namespace prefix every delegation event shares (`delegation.`).
pub const NS: &str = "delegation.";

/// The default per-delegation timeout when neither the caller nor the deploy
/// pins one (`GT_A2A_TIMEOUT_SECS`): 30 minutes.
pub const DEFAULT_TIMEOUT_SECS: u64 = 1800;

/// The `from_role` stamped on delegation bell notifications — the gateway, not a
/// human, is the sender (mirrors `workflow_notify`'s `orchd`).
const FROM_ROLE: &str = "a2a";

/// Mirrors `mcp/notify.rs::DEFAULT_WORKSPACE`: the workspace whose SSE scope is
/// the unscoped (`None`) log, so the bell's feed sees this writer like the others.
const DEFAULT_WORKSPACE: &str = "default";

// ── Events ──────────────────────────────────────────────────────────────────

/// Lifecycle of one delegated sub-task, event-sourced on the `delegation.`
/// namespace. The bead id **is** the delegation id (no mapping table), matching
/// the A2A task-identity rule in `gt-a2a`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum DelegationEvent {
    /// A delegation was created and dispatched. Appended by `a2a.delegate`.
    Requested {
        /// The minted child bead id (the delegation id).
        child: String,
        /// The delegating agent's session / actor id — the callback's audience.
        parent: String,
        /// The rig the child runs on; with `child` it names the session
        /// (`<rig>-<child>`) used to match `agent.killed.v1`.
        rig: String,
        /// RFC3339 stamp of the delegation, the timeout clock's zero.
        at: String,
        /// Seconds after `at` with no terminal state before auto-escalation.
        /// `0` disables the timeout for this delegation.
        timeout_secs: u64,
    },
    /// The callback was delivered: the child reached `state` (or timed out). The
    /// registry treats this as closing the delegation, so neither the plugin nor
    /// the ticker fires for it again.
    Completed {
        child: String,
        parent: String,
        /// `completed` | `failed` | `canceled` | `timed_out`.
        state: String,
    },
}

impl EventKind for DelegationEvent {
    fn kind(&self) -> &'static str {
        match self {
            DelegationEvent::Requested { .. } => "delegation.requested.v1",
            DelegationEvent::Completed { .. } => "delegation.completed.v1",
        }
    }
}

// ── State (replay projection) ─────────────────────────────────────────────────

/// One delegation's projected state — open until a `Completed` folds in.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Delegation {
    pub child: String,
    pub parent: String,
    pub rig: String,
    pub at: String,
    pub timeout_secs: u64,
    pub done: bool,
}

/// In-memory projection rebuilt by folding the `delegation.` event stream — the
/// same event-sourced-registry shape as `escalate.rs`.
#[derive(Debug, Default)]
pub struct DelegationRegistry {
    entries: Vec<Delegation>,
}

impl DelegationRegistry {
    /// Event-sourced reducer: fold one event into the registry.
    pub fn apply(reg: &mut Self, event: &DelegationEvent) {
        match event {
            DelegationEvent::Requested {
                child,
                parent,
                rig,
                at,
                timeout_secs,
            } => {
                // A duplicate Requested (id reuse / replay) refreshes in place
                // rather than stacking a second open entry.
                if let Some(e) = reg.entries.iter_mut().find(|e| e.child == *child) {
                    e.parent = parent.clone();
                    e.rig = rig.clone();
                    e.at = at.clone();
                    e.timeout_secs = *timeout_secs;
                    e.done = false;
                } else {
                    reg.entries.push(Delegation {
                        child: child.clone(),
                        parent: parent.clone(),
                        rig: rig.clone(),
                        at: at.clone(),
                        timeout_secs: *timeout_secs,
                        done: false,
                    });
                }
            }
            DelegationEvent::Completed { child, .. } => {
                if let Some(e) = reg.entries.iter_mut().find(|e| e.child == *child) {
                    e.done = true;
                }
            }
        }
    }

    /// The open (not-yet-completed) delegation for `child`, if any.
    pub fn open(&self, child: &str) -> Option<&Delegation> {
        self.entries.iter().find(|e| e.child == child && !e.done)
    }

    /// Every open delegation — the ticker's sweep set.
    pub fn open_all(&self) -> impl Iterator<Item = &Delegation> {
        self.entries.iter().filter(|e| !e.done)
    }
}

// ── Pure helpers ──────────────────────────────────────────────────────────────

/// The polecat sling convention: a bead's session is `<rig>-<bead id>` (mirrors
/// `a2a::session_of`).
fn session_of(rig: &str, bead: &str) -> String {
    format!("{rig}-{bead}")
}

/// RFC3339 "now" for stamping events from this clock-bearing tier.
pub fn rfc3339_now() -> String {
    OffsetDateTime::now_utc()
        .format(&Rfc3339)
        .unwrap_or_default()
}

/// Project a hub terminal record onto the delegation it completes, given the
/// open-delegation registry. Returns `(child, final_state)` for a record that
/// terminates an OPEN delegation, else `None`:
///
/// - `merge.merged.v1`  → `completed`
/// - `merge.failed.v1`  → `failed`
/// - `agent.killed.v1`  → `canceled` (matched via the `<rig>-<child>` session)
///
/// Decode failures and records for non-delegated beads yield `None` — a foreign
/// or malformed event is never a reason to act.
fn terminal_for(record: &EventRecord, reg: &DelegationRegistry) -> Option<(String, String)> {
    match record.kind.as_str() {
        "merge.merged.v1" => match record.decode::<MergeEvent>().ok()? {
            MergeEvent::Merged { bead, .. } => {
                reg.open(&bead).map(|_| (bead, "completed".to_string()))
            }
            _ => None,
        },
        "merge.failed.v1" => match record.decode::<MergeEvent>().ok()? {
            MergeEvent::Failed { bead, .. } => {
                reg.open(&bead).map(|_| (bead, "failed".to_string()))
            }
            _ => None,
        },
        "agent.killed.v1" => match record.decode::<AgentEvent>().ok()? {
            AgentEvent::Killed { session, .. } => reg
                .open_all()
                .find(|d| session_of(&d.rig, &d.child) == session)
                .map(|d| (d.child.clone(), "canceled".to_string())),
            _ => None,
        },
        _ => None,
    }
}

/// Seconds elapsed since an RFC3339 stamp, or `None` if it cannot be parsed.
fn elapsed_secs(at: &str, now: OffsetDateTime) -> Option<i64> {
    let started = OffsetDateTime::parse(at, &Rfc3339).ok()?;
    Some((now - started).whole_seconds())
}

/// The operator-bell notification kind for a terminal state: a failure/timeout
/// is an `alert`, a clean finish is `info`.
fn bell_kind(state: &str) -> &'static str {
    match state {
        "failed" | "timed_out" | "canceled" => "alert",
        _ => "info",
    }
}

// ── Operator bell (optional PG side) ──────────────────────────────────────────

/// The SSE half of a bell write, identical in shape to `mcp/notify.rs` and
/// `workflow_notify.rs` so the dashboard's `notification.created.v1` listener
/// treats every writer the same.
#[derive(Debug, Serialize)]
struct NotificationCreated {
    id: String,
    workspace: String,
    from_role: String,
    title: String,
    body: String,
    kind: String,
}

impl EventKind for NotificationCreated {
    fn kind(&self) -> &'static str {
        "notification.created.v1"
    }
}

/// The optional Postgres bell: the same two write surfaces `notify.send` uses
/// (`public.notifications` + the SSE event log). `None` ⇒ the callback still
/// emits its durable `delegation.completed.v1`, just no operator-bell row.
#[derive(Clone)]
struct Bell {
    pool: PgPool,
    log: Arc<EventLog>,
    workspace: String,
}

impl Bell {
    /// Best-effort: insert the bell row and append the SSE mirror. Never returns
    /// an error — a notification problem must not break a relay or a tick.
    async fn ring(&self, title: String, body: String, kind: &str) {
        let row: Result<(String,), _> = sqlx::query_as(
            "INSERT INTO notifications (workspace, from_role, title, body, kind) \
             VALUES ($1, $2, $3, $4, $5) RETURNING id::text",
        )
        .bind(&self.workspace)
        .bind(FROM_ROLE)
        .bind(&title)
        .bind(&body)
        .bind(kind)
        .fetch_one(&self.pool)
        .await;
        let id = match row {
            Ok((id,)) => id,
            Err(e) => {
                eprintln!("[delegation] bell insert failed for '{title}': {e}");
                return;
            }
        };
        let ev = NotificationCreated {
            id,
            workspace: self.workspace.clone(),
            from_role: FROM_ROLE.to_string(),
            title,
            body,
            kind: kind.to_string(),
        };
        let ws_opt = (self.workspace != DEFAULT_WORKSPACE).then_some(self.workspace.as_str());
        if let Err(e) = self.log.append(ws_opt, ev) {
            eprintln!("[delegation] bell SSE emit failed: {e}");
        }
    }
}

// ── Callback plugin ───────────────────────────────────────────────────────────

/// Hub observer that turns a delegated child reaching a terminal state into a
/// push callback to the parent — replacing the parent's `a2a.status` poll.
pub struct DelegationCallbackPlugin {
    log: Arc<EventLog>,
    workspace: Option<String>,
    bell: Option<Bell>,
}

impl DelegationCallbackPlugin {
    /// Wrap the per-workspace event log. `workspace` scopes the registry replay
    /// and the `Completed` append; `None` ⇒ the unscoped (`default`) log.
    pub fn new(log: Arc<EventLog>, workspace: Option<String>) -> Self {
        Self {
            log,
            workspace,
            bell: None,
        }
    }

    /// Also ring the operator bell on each callback (PG `public.notifications`
    /// + SSE). Without it the callback is durable-event-only.
    pub fn with_bell(mut self, pool: PgPool, workspace: impl Into<String>) -> Self {
        self.bell = Some(Bell {
            pool,
            log: self.log.clone(),
            workspace: workspace.into(),
        });
        self
    }

    fn registry(&self) -> Result<DelegationRegistry, StoreError> {
        self.log.replay_domain(
            self.workspace.as_deref(),
            NS,
            DelegationRegistry::default(),
            DelegationRegistry::apply,
        )
    }
}

#[async_trait]
impl Plugin for DelegationCallbackPlugin {
    fn name(&self) -> &'static str {
        "delegation-callback"
    }

    async fn on_event(&self, record: &EventRecord) -> Result<(), AppError> {
        // Cheap kind gate before the (per-event) registry replay.
        if !matches!(
            record.kind.as_str(),
            "merge.merged.v1" | "merge.failed.v1" | "agent.killed.v1"
        ) {
            return Ok(());
        }
        let reg = match self.registry() {
            Ok(r) => r,
            Err(e) => {
                eprintln!("[delegation] registry replay failed: {e}");
                return Ok(());
            }
        };
        let Some((child, state)) = terminal_for(record, &reg) else {
            return Ok(());
        };
        // open() guaranteed Some by terminal_for; re-read for the parent id.
        let parent = reg
            .open(&child)
            .map(|d| d.parent.clone())
            .unwrap_or_default();

        // 1) Durable callback — closes the delegation and feeds the `delegation`
        //    SSE channel the parent/dashboard subscribe to (no more polling).
        if let Err(e) = self.log.append(
            self.workspace.as_deref(),
            DelegationEvent::Completed {
                child: child.clone(),
                parent: parent.clone(),
                state: state.clone(),
            },
        ) {
            eprintln!("[delegation] completed append failed for {child}: {e}");
        }
        eprintln!("[delegation] {child} → {state}; callback to parent {parent}");

        // 2) Optional operator bell.
        if let Some(bell) = &self.bell {
            bell.ring(
                format!("Delegación {child}: {state}"),
                format!("El bead delegado {child} (parent {parent}) terminó en estado {state}."),
                bell_kind(&state),
            )
            .await;
        }
        Ok(())
    }
}

// ── Timeout ticker ────────────────────────────────────────────────────────────

/// Periodic sweep that auto-escalates delegations stuck past their timeout.
///
/// Driven by an interval loop in `gt-orch-server` (alongside patrol/quota), the
/// way other time-based daemons are. Holds the same log handle the callback
/// plugin does, so they share one event-sourced registry.
pub struct DelegationTimeoutTicker {
    log: Arc<EventLog>,
    workspace: Option<String>,
    bell: Option<Bell>,
}

impl DelegationTimeoutTicker {
    pub fn new(log: Arc<EventLog>, workspace: Option<String>) -> Self {
        Self {
            log,
            workspace,
            bell: None,
        }
    }

    /// Also ring the operator bell when a delegation is auto-escalated.
    pub fn with_bell(mut self, pool: PgPool, workspace: impl Into<String>) -> Self {
        self.bell = Some(Bell {
            pool,
            log: self.log.clone(),
            workspace: workspace.into(),
        });
        self
    }

    fn registry(&self) -> Result<DelegationRegistry, StoreError> {
        self.log.replay_domain(
            self.workspace.as_deref(),
            NS,
            DelegationRegistry::default(),
            DelegationRegistry::apply,
        )
    }

    /// One sweep: escalate every open delegation whose age exceeds its
    /// `timeout_secs`. Returns the number escalated (for logging/tests).
    /// Best-effort — a single append failure is logged and the sweep continues.
    pub async fn tick(&self) -> usize {
        let reg = match self.registry() {
            Ok(r) => r,
            Err(e) => {
                eprintln!("[delegation-timeout] registry replay failed: {e}");
                return 0;
            }
        };
        let now = OffsetDateTime::now_utc();
        let overdue: Vec<Delegation> = reg
            .open_all()
            .filter(|d| d.timeout_secs > 0)
            .filter(|d| {
                elapsed_secs(&d.at, now)
                    .is_some_and(|e| e >= 0 && (e as u64) >= d.timeout_secs)
            })
            .cloned()
            .collect();

        let mut escalated = 0;
        for d in overdue {
            let esc_id = format!("deleg-timeout-{}", d.child);
            let reason = format!(
                "Delegated bead {} exceeded its {}s completion timeout with no terminal state",
                d.child, d.timeout_secs
            );
            // Auto-escalation reuses the escalate.* registry so it shows up on
            // the dashboard and can be escalate.respond-ed by an operator.
            if let Err(e) = self.log.append(
                self.workspace.as_deref(),
                EscalationEvent::Requested {
                    id: esc_id,
                    session: session_of(&d.rig, &d.child),
                    bead: Some(d.child.clone()),
                    reason: reason.clone(),
                    // A delegated bead is a headless polecat: kill-and-re-sling is the right
                    // disposition (pause-in-place is for interactive sessions). (B2, gtcore-5731e9)
                    mode: EscalationMode::Kill,
                },
            ) {
                eprintln!("[delegation-timeout] escalation append failed for {}: {e}", d.child);
                continue;
            }
            // Close the delegation so the next sweep does not re-escalate it.
            if let Err(e) = self.log.append(
                self.workspace.as_deref(),
                DelegationEvent::Completed {
                    child: d.child.clone(),
                    parent: d.parent.clone(),
                    state: "timed_out".to_string(),
                },
            ) {
                eprintln!("[delegation-timeout] completed append failed for {}: {e}", d.child);
            }
            if let Some(bell) = &self.bell {
                bell.ring(
                    format!("Delegación {} agotó el tiempo", d.child),
                    reason,
                    "alert",
                )
                .await;
            }
            eprintln!("[delegation-timeout] {} auto-escalated (timeout {}s)", d.child, d.timeout_secs);
            escalated += 1;
        }
        escalated
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use gt_events::Envelope;
    use tempfile::TempDir;

    fn log(dir: &TempDir) -> Arc<EventLog> {
        Arc::new(EventLog::new(Some(dir.path().to_path_buf())))
    }

    fn request(log: &EventLog, child: &str, parent: &str, rig: &str, at: &str, timeout: u64) {
        log.append(
            None,
            DelegationEvent::Requested {
                child: child.into(),
                parent: parent.into(),
                rig: rig.into(),
                at: at.into(),
                timeout_secs: timeout,
            },
        )
        .unwrap();
    }

    fn merged_record(bead: &str, sha: &str) -> EventRecord {
        EventRecord::from_envelope(&Envelope::root(MergeEvent::Merged {
            bead: bead.into(),
            sha: sha.into(),
        }))
        .unwrap()
    }

    fn killed_record(session: &str) -> EventRecord {
        EventRecord::from_envelope(&Envelope::root(AgentEvent::Killed {
            session: session.into(),
            reason: "stale".into(),
        }))
        .unwrap()
    }

    // ── Registry ──────────────────────────────────────────────────────────────

    #[test]
    fn registry_folds_requested_then_completed() {
        let mut reg = DelegationRegistry::default();
        DelegationRegistry::apply(
            &mut reg,
            &DelegationEvent::Requested {
                child: "gtcore-a".into(),
                parent: "sess-1".into(),
                rig: "gtcore".into(),
                at: rfc3339_now(),
                timeout_secs: 60,
            },
        );
        assert!(reg.open("gtcore-a").is_some());
        DelegationRegistry::apply(
            &mut reg,
            &DelegationEvent::Completed {
                child: "gtcore-a".into(),
                parent: "sess-1".into(),
                state: "completed".into(),
            },
        );
        assert!(reg.open("gtcore-a").is_none(), "completed delegation is no longer open");
    }

    #[test]
    fn terminal_for_maps_merge_and_kill_for_open_only() {
        let mut reg = DelegationRegistry::default();
        DelegationRegistry::apply(
            &mut reg,
            &DelegationEvent::Requested {
                child: "gtcore-a".into(),
                parent: "sess-1".into(),
                rig: "gtcore".into(),
                at: rfc3339_now(),
                timeout_secs: 0,
            },
        );
        // merged → completed
        let (child, state) = terminal_for(&merged_record("gtcore-a", "deadbeef"), &reg).unwrap();
        assert_eq!((child.as_str(), state.as_str()), ("gtcore-a", "completed"));
        // killed via the <rig>-<child> session → canceled
        let (child, state) = terminal_for(&killed_record("gtcore-gtcore-a"), &reg).unwrap();
        assert_eq!((child.as_str(), state.as_str()), ("gtcore-a", "canceled"));
        // a merge for a bead we never delegated → no callback
        assert!(terminal_for(&merged_record("gtcore-z", "f00"), &reg).is_none());
    }

    // ── Callback plugin ────────────────────────────────────────────────────────

    #[tokio::test]
    async fn callback_completes_open_delegation_and_is_idempotent() {
        let dir = TempDir::new().unwrap();
        let log = log(&dir);
        request(&log, "gtcore-a", "sess-1", "gtcore", &rfc3339_now(), 0);

        let plugin = DelegationCallbackPlugin::new(log.clone(), None);
        plugin.on_event(&merged_record("gtcore-a", "abc1234")).await.unwrap();

        // The delegation is now closed in the registry.
        let reg = log
            .replay_domain(None, NS, DelegationRegistry::default(), DelegationRegistry::apply)
            .unwrap();
        assert!(reg.open("gtcore-a").is_none(), "callback closed the delegation");

        // A replay of the same terminal event is a no-op (no second Completed):
        // exactly one delegation.completed.v1 in the log.
        plugin.on_event(&merged_record("gtcore-a", "abc1234")).await.unwrap();
        let completed = log
            .read_kind(None, "delegation.completed.v1")
            .unwrap()
            .len();
        assert_eq!(completed, 1, "terminal replay must not double-fire the callback");
    }

    #[tokio::test]
    async fn callback_ignores_non_terminal_and_undelegated() {
        let dir = TempDir::new().unwrap();
        let log = log(&dir);
        request(&log, "gtcore-a", "sess-1", "gtcore", &rfc3339_now(), 0);
        let plugin = DelegationCallbackPlugin::new(log.clone(), None);

        // A merge for a bead that was never delegated.
        plugin.on_event(&merged_record("gtcore-other", "f00")).await.unwrap();
        // A non-terminal event kind.
        let started = EventRecord::from_envelope(&Envelope::root(MergeEvent::Started {
            bead: "gtcore-a".into(),
        }))
        .unwrap();
        plugin.on_event(&started).await.unwrap();

        let completed = log.read_kind(None, "delegation.completed.v1").unwrap().len();
        assert_eq!(completed, 0, "no callback for undelegated or non-terminal events");
    }

    // ── Timeout ticker ─────────────────────────────────────────────────────────

    #[tokio::test]
    async fn ticker_escalates_overdue_only_once() {
        let dir = TempDir::new().unwrap();
        let log = log(&dir);
        // Started two hours ago, 60s timeout ⇒ overdue.
        let stale = (OffsetDateTime::now_utc() - time::Duration::hours(2))
            .format(&Rfc3339)
            .unwrap();
        request(&log, "gtcore-late", "sess-1", "gtcore", &stale, 60);
        // Fresh delegation with a long timeout ⇒ not overdue.
        request(&log, "gtcore-fresh", "sess-2", "gtcore", &rfc3339_now(), 3600);

        let ticker = DelegationTimeoutTicker::new(log.clone(), None);
        assert_eq!(ticker.tick().await, 1, "only the stale delegation escalates");

        // It is now closed → a second sweep escalates nothing.
        assert_eq!(ticker.tick().await, 0, "timed-out delegation is not re-escalated");

        // The escalation is visible on the escalate.* registry.
        let escalations = log.read_kind(None, "escalation.requested.v1").unwrap();
        assert_eq!(escalations.len(), 1);
        let ev: EscalationEvent = escalations[0].decode().unwrap();
        match ev {
            EscalationEvent::Requested { bead, .. } => {
                assert_eq!(bead.as_deref(), Some("gtcore-late"));
            }
            _ => panic!("expected an escalation request"),
        }
    }

    #[tokio::test]
    async fn ticker_ignores_zero_timeout() {
        let dir = TempDir::new().unwrap();
        let log = log(&dir);
        let stale = (OffsetDateTime::now_utc() - time::Duration::hours(2))
            .format(&Rfc3339)
            .unwrap();
        // timeout_secs == 0 disables the timeout even when very old.
        request(&log, "gtcore-forever", "sess-1", "gtcore", &stale, 0);
        let ticker = DelegationTimeoutTicker::new(log.clone(), None);
        assert_eq!(ticker.tick().await, 0, "zero timeout disables auto-escalation");
    }
}
