//! Server→agent push over the live MCP session (gtcore-d366ff).
//!
//! The `/mcp` transport is streamable-HTTP (rmcp): a client POSTs tool calls and,
//! per the 2025-03-26 spec, may also hold open a GET `text/event-stream` keyed by
//! its `Mcp-Session-Id`. rmcp already mints that session id, keeps the GET stream
//! open, and routes any **server-initiated notification** sent through the session's
//! [`Peer`] onto that standing stream (`OutboundChannel::Common`). What was missing is
//! a way for an *out-of-band observer* — the event-log tail in the composition bin —
//! to reach the right session's peer. This module is that bridge.
//!
//! [`SessionRegistry`] maps an **actor identity** (the JWT `sub` the agent
//! authenticates with — the same id A2A messages and delegations are addressed to)
//! to the live [`McpNotifier`]s of its open MCP sessions. The server registers a
//! session's peer on each authorized `call_tool`; the bin's push observer resolves an
//! event's target actor and calls the matching `notify_*`, which rmcp delivers on the
//! agent's GET stream as `notifications/resources/updated`. An agent with no open
//! session is simply absent from the registry — the push is a no-op and the agent
//! keeps polling (`a2a.inbox` / `a2a.status`), so this is **not** a breaking change.
//!
//! The peer is reached through the [`McpNotifier`] port rather than a concrete
//! [`Peer`] so the registry, its TTL reaping, and the observer's routing are testable
//! without a live transport (the production adapter is [`PeerNotifier`]).

use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use rmcp::model::ResourceUpdatedNotificationParam;
use rmcp::service::{Peer, RoleServer};

/// The default idle TTL for a tracked session: a session with no `call_tool` in this
/// window is reaped by [`SessionRegistry::sweep`]. Generous enough to outlive a quiet
/// agent that is blocked on a long tool, short enough that a vanished connection's
/// entry does not linger. The bin can override it at construction.
pub const DEFAULT_SESSION_TTL: Duration = Duration::from_secs(30 * 60);

/// The one capability the push path needs from a session's peer: emit a
/// server-initiated notification. Implemented for the real rmcp [`Peer`] by
/// [`PeerNotifier`]; a test double implements it directly to assert routing without a
/// transport. `Err(())` means the channel is gone — the registry prunes the session.
#[async_trait::async_trait]
pub trait McpNotifier: Send + Sync {
    /// Push `notifications/resources/updated { uri }` to the client. The agent reacts
    /// by re-reading that resource (`gt://inbox/{actor}`, `gt://delegation/{child}`, …).
    async fn resource_updated(&self, uri: String) -> Result<(), ()>;

    /// Push `notifications/tools/list_changed` so the client refetches `tools/list`
    /// after the skill catalog changes in place.
    async fn tool_list_changed(&self) -> Result<(), ()>;
}

/// Production [`McpNotifier`]: a thin adapter over a session's rmcp [`Peer`]. A
/// notification carries no request id, so rmcp routes it to the session's common
/// (GET-stream) channel — the same path `on_initialized` already uses to emit
/// `tools/list_changed`.
pub struct PeerNotifier(Peer<RoleServer>);

impl PeerNotifier {
    /// Wrap a session's peer, captured from the `call_tool` request context.
    pub fn new(peer: Peer<RoleServer>) -> Self {
        Self(peer)
    }
}

#[async_trait::async_trait]
impl McpNotifier for PeerNotifier {
    async fn resource_updated(&self, uri: String) -> Result<(), ()> {
        self.0
            .notify_resource_updated(ResourceUpdatedNotificationParam { uri })
            .await
            .map_err(|_| ())
    }

    async fn tool_list_changed(&self) -> Result<(), ()> {
        self.0.notify_tool_list_changed().await.map_err(|_| ())
    }
}

/// One tracked MCP session: the actor it authenticated as, its tenant, the peer to
/// push through, and the last time it was seen (for idle reaping).
struct SessionEntry {
    actor: String,
    #[allow(dead_code)]
    workspace: String,
    notifier: Arc<dyn McpNotifier>,
    last_seen: Instant,
}

/// Live MCP sessions, keyed by `Mcp-Session-Id`, indexed for push by actor.
///
/// Keying by the transport session id makes [`register`](Self::register) an idempotent
/// upsert — repeated tool calls on one connection refresh a single entry rather than
/// piling up duplicates — while a reconnect (a new session id, hence a new peer) lands
/// as a distinct entry the old one's idle TTL or a failed push will clear. Pushes look
/// up by actor, so multiple concurrent sessions of one agent all receive the event.
pub struct SessionRegistry {
    sessions: Mutex<HashMap<String, SessionEntry>>,
    ttl: Duration,
}

impl SessionRegistry {
    /// Build a registry that reaps sessions idle longer than `ttl`.
    pub fn new(ttl: Duration) -> Self {
        Self {
            sessions: Mutex::new(HashMap::new()),
            ttl,
        }
    }

    /// Build a registry with [`DEFAULT_SESSION_TTL`].
    pub fn with_default_ttl() -> Self {
        Self::new(DEFAULT_SESSION_TTL)
    }

    /// Upsert the session under `session_id`, (re)binding its peer and stamping it
    /// live. Called on each authorized `call_tool`, so an active agent's entry stays
    /// fresh against the idle TTL and always holds its current peer.
    pub fn register(
        &self,
        session_id: impl Into<String>,
        workspace: impl Into<String>,
        actor: impl Into<String>,
        notifier: Arc<dyn McpNotifier>,
    ) {
        let entry = SessionEntry {
            actor: actor.into(),
            workspace: workspace.into(),
            notifier,
            last_seen: Instant::now(),
        };
        self.sessions
            .lock()
            .expect("session registry poisoned")
            .insert(session_id.into(), entry);
    }

    /// Push `notifications/resources/updated { gt://inbox/{actor} }` to every open
    /// session of `actor` — fired when an A2A `message.sent.v1` is addressed to it.
    /// Returns how many sessions received it.
    pub async fn notify_inbox(&self, actor: &str) -> usize {
        let uri = format!("gt://inbox/{actor}");
        self.push_resource(actor, uri).await
    }

    /// Push `notifications/resources/updated { gt://delegation/{child} }` to `parent`
    /// — fired when a `delegation.completed.v1` lands for a sub-task `parent` delegated.
    pub async fn notify_delegation(&self, parent: &str, child: &str) -> usize {
        let uri = format!("gt://delegation/{child}");
        self.push_resource(parent, uri).await
    }

    /// Push `notifications/resources/updated { gt://escalation/{id} }` to the session
    /// that raised the escalation — fired on `escalation.requested.v1`.
    pub async fn notify_escalation(&self, session: &str, id: &str) -> usize {
        let uri = format!("gt://escalation/{id}");
        self.push_resource(session, uri).await
    }

    /// Push `notifications/tools/list_changed` to every open session so clients
    /// refetch `tools/list` after the catalog changes in place.
    pub async fn notify_tools_changed(&self) -> usize {
        let targets = self.snapshot(|_| true);
        let mut delivered = 0usize;
        let mut dead = Vec::new();
        for (session_id, notifier) in targets {
            match notifier.tool_list_changed().await {
                Ok(()) => delivered += 1,
                Err(()) => dead.push(session_id),
            }
        }
        self.prune(&dead);
        delivered
    }

    /// Drop every session idle longer than the TTL. Returns the number reaped. Called
    /// on a timer by the bin so a connection that vanished without a clean close (no
    /// failed push to prune it) does not leak.
    pub fn sweep(&self) -> usize {
        let mut guard = self.sessions.lock().expect("session registry poisoned");
        let before = guard.len();
        guard.retain(|_, e| e.last_seen.elapsed() < self.ttl);
        before - guard.len()
    }

    /// Number of sessions currently tracked (diagnostics + tests).
    pub fn session_count(&self) -> usize {
        self.sessions
            .lock()
            .expect("session registry poisoned")
            .len()
    }

    /// Deliver one resource-updated push to all of `actor`'s sessions, pruning any whose
    /// channel is gone. The lock is never held across the `.await`: matching peers are
    /// cloned out under the lock, awaited unlocked, then dead ids removed under the lock.
    async fn push_resource(&self, actor: &str, uri: String) -> usize {
        let targets = self.snapshot(|e| e.actor == actor);
        let mut delivered = 0usize;
        let mut dead = Vec::new();
        for (session_id, notifier) in targets {
            match notifier.resource_updated(uri.clone()).await {
                Ok(()) => delivered += 1,
                Err(()) => dead.push(session_id),
            }
        }
        self.prune(&dead);
        delivered
    }

    /// Clone out `(session_id, notifier)` for every entry matching `pred`. The notifier
    /// is an `Arc`, so a push can run unlocked after the snapshot returns.
    fn snapshot(
        &self,
        pred: impl Fn(&SessionEntry) -> bool,
    ) -> Vec<(String, Arc<dyn McpNotifier>)> {
        let guard = self.sessions.lock().expect("session registry poisoned");
        guard
            .iter()
            .filter(|(_, e)| pred(e))
            .map(|(sid, e)| (sid.clone(), e.notifier.clone()))
            .collect()
    }

    /// Remove the listed sessions (those whose push failed).
    fn prune(&self, dead: &[String]) {
        if dead.is_empty() {
            return;
        }
        let mut guard = self.sessions.lock().expect("session registry poisoned");
        for session_id in dead {
            guard.remove(session_id);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicUsize, Ordering};

    /// A test [`McpNotifier`] that records every URI it was asked to push and can be
    /// flipped to fail (simulating a closed channel) to exercise pruning.
    #[derive(Default)]
    struct RecordingNotifier {
        uris: Mutex<Vec<String>>,
        tool_changes: AtomicUsize,
        fail: bool,
    }

    #[async_trait::async_trait]
    impl McpNotifier for RecordingNotifier {
        async fn resource_updated(&self, uri: String) -> Result<(), ()> {
            if self.fail {
                return Err(());
            }
            self.uris.lock().unwrap().push(uri);
            Ok(())
        }

        async fn tool_list_changed(&self) -> Result<(), ()> {
            if self.fail {
                return Err(());
            }
            self.tool_changes.fetch_add(1, Ordering::SeqCst);
            Ok(())
        }
    }

    fn live() -> SessionRegistry {
        SessionRegistry::new(Duration::from_secs(3600))
    }

    #[tokio::test]
    async fn inbox_push_targets_only_the_addressed_actor() {
        let reg = live();
        let a = Arc::new(RecordingNotifier::default());
        let b = Arc::new(RecordingNotifier::default());
        reg.register("sess-a", "default", "agent-1", a.clone());
        reg.register("sess-b", "default", "agent-2", b.clone());

        let delivered = reg.notify_inbox("agent-1").await;

        assert_eq!(delivered, 1);
        assert_eq!(&*a.uris.lock().unwrap(), &["gt://inbox/agent-1".to_string()]);
        assert!(b.uris.lock().unwrap().is_empty(), "agent-2 must not be paged");
    }

    #[tokio::test]
    async fn delegation_push_addresses_the_parent_with_the_child_uri() {
        let reg = live();
        let parent = Arc::new(RecordingNotifier::default());
        reg.register("sess-p", "default", "parent-agent", parent.clone());

        let delivered = reg.notify_delegation("parent-agent", "gtcore-abc123").await;

        assert_eq!(delivered, 1);
        assert_eq!(
            &*parent.uris.lock().unwrap(),
            &["gt://delegation/gtcore-abc123".to_string()]
        );
    }

    #[tokio::test]
    async fn all_concurrent_sessions_of_one_actor_are_paged() {
        let reg = live();
        let s1 = Arc::new(RecordingNotifier::default());
        let s2 = Arc::new(RecordingNotifier::default());
        reg.register("sess-1", "default", "agent-1", s1.clone());
        reg.register("sess-2", "default", "agent-1", s2.clone());

        assert_eq!(reg.notify_inbox("agent-1").await, 2);
        assert_eq!(s1.uris.lock().unwrap().len(), 1);
        assert_eq!(s2.uris.lock().unwrap().len(), 1);
    }

    #[tokio::test]
    async fn push_to_unknown_actor_is_a_noop() {
        // The "legacy / no open SSE" case: nobody registered, nothing pushed, no panic.
        let reg = live();
        assert_eq!(reg.notify_inbox("ghost").await, 0);
    }

    #[tokio::test]
    async fn a_failed_push_prunes_the_dead_session() {
        let reg = live();
        let dead = Arc::new(RecordingNotifier {
            fail: true,
            ..Default::default()
        });
        reg.register("sess-dead", "default", "agent-1", dead);
        assert_eq!(reg.session_count(), 1);

        let delivered = reg.notify_inbox("agent-1").await;

        assert_eq!(delivered, 0);
        assert_eq!(reg.session_count(), 0, "a closed channel is reaped on push");
    }

    #[tokio::test]
    async fn register_is_an_idempotent_upsert_per_session_id() {
        let reg = live();
        reg.register(
            "sess-x",
            "default",
            "agent-1",
            Arc::new(RecordingNotifier::default()),
        );
        reg.register(
            "sess-x",
            "default",
            "agent-1",
            Arc::new(RecordingNotifier::default()),
        );
        assert_eq!(reg.session_count(), 1, "same session id must not duplicate");
    }

    #[tokio::test]
    async fn tools_changed_pages_every_session() {
        let reg = live();
        let s1 = Arc::new(RecordingNotifier::default());
        let s2 = Arc::new(RecordingNotifier::default());
        reg.register("s1", "default", "agent-1", s1.clone());
        reg.register("s2", "default", "agent-2", s2.clone());

        assert_eq!(reg.notify_tools_changed().await, 2);
        assert_eq!(s1.tool_changes.load(Ordering::SeqCst), 1);
        assert_eq!(s2.tool_changes.load(Ordering::SeqCst), 1);
    }

    #[test]
    fn sweep_reaps_only_idle_sessions() {
        let reg = SessionRegistry::new(Duration::from_millis(20));
        reg.register(
            "sess-old",
            "default",
            "agent-1",
            Arc::new(RecordingNotifier::default()),
        );
        std::thread::sleep(Duration::from_millis(40));
        // A fresh session registered after the sleep must survive the same sweep.
        reg.register(
            "sess-new",
            "default",
            "agent-2",
            Arc::new(RecordingNotifier::default()),
        );

        assert_eq!(reg.sweep(), 1, "only the idle session is reaped");
        assert_eq!(reg.session_count(), 1);
    }
}
