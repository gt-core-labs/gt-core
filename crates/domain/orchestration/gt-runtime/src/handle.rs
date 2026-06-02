//! [`RootHandle`] — a composed application [`Root`] bound to one workspace.

use std::fmt;
use std::sync::{Arc, Mutex};

use gt_eventlog::EventRecord;
use gt_module::Root;
use gt_plugin::{spawn_plugin_relay, PluginRegistry};
use gt_workspace::WorkspaceId;
use tokio::sync::broadcast;
use tokio::task::JoinHandle;

use crate::lifecycle::Supervisor;
use crate::Phase;

/// Capacity of the per-workspace [`EventRecord`] broadcast hub. A consumer that falls this
/// far behind sees [`broadcast::error::RecvError::Lagged`] (the plugin relay records it to the
/// dead-letter rather than dropping silently) instead of stalling the publisher.
const DEFAULT_EVENT_BUFFER: usize = 1024;

/// The live application composition for a single workspace.
///
/// Wraps the kernel's read-only [`Root`] (the assembled module registry) with
/// the [`WorkspaceId`] it was built for. It lives in the orchestration tier, not
/// the kernel, because the kernel may not name a [`WorkspaceId`] (one-way dep
/// direction, docs/03) and — as this type grows — must not spawn the actor tasks
/// a live root owns (`tokio::spawn` is forbidden in kernel crates).
///
/// It holds the composed `Root`, exposes it, and owns the [`Supervisor`] that
/// runs the workspace's actor stack (`hq-mt-routing.2`). The handle binds the two
/// for their shared lifetime: when the [`RootRegistry`](crate::RootRegistry)
/// evicts a workspace, dropping the `Arc<RootHandle>` drops the supervisor, which
/// cancels the actors. A freshly [`new`](Self::new) handle is in
/// [`Phase::Built`](crate::Phase::Built) with no actors running; the registry's
/// hydrate closure registers actors and calls [`start`](Self::start) before
/// publishing it. The registry shape is unchanged — it still holds
/// `Arc<RootHandle>` per workspace.
///
/// ## Event hub + observer relay (`hq-mod-refactor.18`)
///
/// The handle also owns the workspace's **event hub**: a [`broadcast`] of every appended
/// [`EventRecord`]. The event-log append path publishes each record through
/// [`events_sender`](Self::events_sender); read-side consumers — the observer-plugin relay and
/// (later) the SSE endpoint — each take their own [`broadcast::Receiver`] via
/// [`subscribe_events`](Self::subscribe_events). [`spawn_plugins`](Self::spawn_plugins) wires a
/// [`PluginRegistry`] onto that stream: this is the seam role observers (sheriff/witness/deacon/
/// mayor/refinery) register on, replacing gastown's monolithic `Reactor` match — gt-core's
/// modular one-namespace-per-module design has no single event enum to match on, so reaction is
/// pure observer fan-out keyed on [`EventRecord::kind`].
pub struct RootHandle {
    workspace: WorkspaceId,
    root: Root,
    supervisor: Supervisor,
    /// Per-workspace broadcast of appended events; the relay + SSE subscribe to it. Kept here
    /// (not in the kernel) for the same reason as the supervisor: dropping the handle drops the
    /// sender, which closes every receiver and lets the relay task exit on its own.
    events: broadcast::Sender<EventRecord>,
    /// The observer-relay task spawned by [`spawn_plugins`](Self::spawn_plugins), kept so
    /// [`shutdown`](Self::shutdown) can abort it. `None` until plugins are wired.
    relay: Mutex<Option<JoinHandle<()>>>,
}

impl RootHandle {
    /// Bind a composed [`Root`] to the workspace it was built for, with an
    /// unstarted actor [`Supervisor`] and a fresh, unsubscribed event hub.
    pub fn new(workspace: WorkspaceId, root: Root) -> Self {
        let (events, _) = broadcast::channel(DEFAULT_EVENT_BUFFER);
        RootHandle {
            workspace,
            root,
            supervisor: Supervisor::new(),
            events,
            relay: Mutex::new(None),
        }
    }

    /// Subscribe to this workspace's live [`EventRecord`] stream. Each caller gets its own
    /// [`broadcast::Receiver`] — used by the plugin relay and, later, per-connection SSE.
    pub fn subscribe_events(&self) -> broadcast::Receiver<EventRecord> {
        self.events.subscribe()
    }

    /// The sender side of the event hub, for the event-log append path to publish appended
    /// records into. Cloneable; a record sent here fans out to every current subscriber.
    pub fn events_sender(&self) -> broadcast::Sender<EventRecord> {
        self.events.clone()
    }

    /// Wire an observer [`PluginRegistry`] onto the event hub: spawn the relay that drains the
    /// broadcast and fans each record out to the registered plugins in order (errors land in
    /// the registry's dead-letter, never aborting the chain). This is how role observers attach
    /// (`hq-mod-refactor.16`).
    ///
    /// Must run inside a Tokio runtime (it spawns the relay task). Idempotent-ish: a second
    /// call replaces the relay, aborting the previous one. The handle keeps the join handle so
    /// [`shutdown`](Self::shutdown) can stop it; dropping the handle also closes the hub, which
    /// makes the relay exit on its own.
    pub fn spawn_plugins(&self, registry: Arc<PluginRegistry>) {
        let task = spawn_plugin_relay(self.subscribe_events(), registry);
        if let Some(old) = self.relay.lock().expect("relay mutex poisoned").replace(task) {
            old.abort();
        }
    }

    /// The workspace this root serves.
    pub fn workspace(&self) -> &WorkspaceId {
        &self.workspace
    }

    /// The composed module registry for this workspace.
    pub fn root(&self) -> &Root {
        &self.root
    }

    /// The actor-stack lifecycle for this workspace — register actors and drive
    /// start/drain/shutdown through it. See [`Supervisor`].
    pub fn supervisor(&self) -> &Supervisor {
        &self.supervisor
    }

    /// Current lifecycle [`Phase`] of the actor stack. Convenience for
    /// `supervisor().phase()`.
    pub fn phase(&self) -> Phase {
        self.supervisor.phase()
    }

    /// Spawn the registered actors. Delegates to [`Supervisor::start`].
    pub async fn start(&self) -> bool {
        self.supervisor.start().await
    }

    /// Gracefully await in-flight actor work. Delegates to [`Supervisor::drain`].
    pub async fn drain(&self) {
        self.supervisor.drain().await
    }

    /// Cancel the actor stack and join, then stop the observer relay. Delegates to
    /// [`Supervisor::shutdown`] for the actors and aborts the relay task spawned by
    /// [`spawn_plugins`](Self::spawn_plugins) (if any).
    pub async fn shutdown(&self) {
        self.supervisor.shutdown().await;
        if let Some(task) = self.relay.lock().expect("relay mutex poisoned").take() {
            task.abort();
        }
    }
}

impl fmt::Debug for RootHandle {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("RootHandle")
            .field("workspace", &self.workspace)
            .field("modules", &self.root.len())
            .field("phase", &self.supervisor.phase())
            .field("event_subscribers", &self.events.receiver_count())
            .field(
                "relay_active",
                &self.relay.lock().expect("relay mutex poisoned").is_some(),
            )
            .finish()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicUsize, Ordering};

    use gt_module::RootBuilder;
    use gt_plugin::Plugin;

    fn handle() -> RootHandle {
        let ws = WorkspaceId::new("acme").expect("valid slug");
        let root = RootBuilder::new().build().expect("empty root builds");
        RootHandle::new(ws, root)
    }

    fn record(kind: &str) -> EventRecord {
        EventRecord {
            event_id: "e1".into(),
            correlation_id: "c1".into(),
            causation_id: None,
            ts: "2026-06-02T00:00:00Z".into(),
            kind: kind.into(),
            payload: serde_json::Value::Null,
        }
    }

    /// A plugin that records every kind it observes, in order.
    struct Spy {
        seen: Arc<Mutex<Vec<String>>>,
    }

    #[async_trait::async_trait]
    impl Plugin for Spy {
        fn name(&self) -> &'static str {
            "spy"
        }
        async fn on_event(&self, record: &EventRecord) -> Result<(), gt_events::AppError> {
            self.seen.lock().unwrap().push(record.kind.clone());
            Ok(())
        }
    }

    #[tokio::test]
    async fn a_plain_subscriber_receives_published_records() {
        let h = handle();
        let mut rx = h.subscribe_events();
        h.events_sender().send(record("rig.added.v1")).unwrap();
        assert_eq!(rx.recv().await.unwrap().kind, "rig.added.v1");
    }

    #[tokio::test]
    async fn spawn_plugins_fans_published_records_to_the_registry() {
        let h = handle();
        let seen = Arc::new(Mutex::new(Vec::new()));
        let registry = Arc::new(PluginRegistry::new().register(Spy { seen: seen.clone() }));
        h.spawn_plugins(registry);

        h.events_sender().send(record("merge.ready.v1")).unwrap();
        h.events_sender().send(record("merge.merged.v1")).unwrap();

        // Let the relay task drain the broadcast.
        for _ in 0..100 {
            if seen.lock().unwrap().len() == 2 {
                break;
            }
            tokio::task::yield_now().await;
        }
        assert_eq!(
            *seen.lock().unwrap(),
            vec!["merge.ready.v1".to_string(), "merge.merged.v1".to_string()]
        );
    }

    #[tokio::test]
    async fn shutdown_aborts_the_relay_task() {
        let h = handle();
        let counter = Arc::new(AtomicUsize::new(0));
        struct Counting(Arc<AtomicUsize>);
        #[async_trait::async_trait]
        impl Plugin for Counting {
            fn name(&self) -> &'static str {
                "counting"
            }
            async fn on_event(&self, _r: &EventRecord) -> Result<(), gt_events::AppError> {
                self.0.fetch_add(1, Ordering::SeqCst);
                Ok(())
            }
        }
        let registry = Arc::new(PluginRegistry::new().register(Counting(counter.clone())));
        h.spawn_plugins(registry);
        h.shutdown().await;
        assert!(
            h.relay.lock().unwrap().is_none(),
            "shutdown clears the relay handle"
        );
    }
}
