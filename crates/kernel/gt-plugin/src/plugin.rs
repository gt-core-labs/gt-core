//! Runtime observer plugin trait, registry, dead-letter and broadcast relay (hq-evks).
//!
//! The relay is the only `tokio::spawn` in this crate; it subscribes to the root's
//! [`broadcast::Receiver<EventRecord>`], fans every record out to the registered plugins **in
//! registration order**, and routes their errors to [`PluginDeadLetter`] without breaking the
//! chain. The relay never publishes back into the domain bus — that is what keeps `replay_gt`
//! byte-identical whether plugins are attached or not (the gate criterion in
//! `apps/api/docs/11-cutover-roadmap.md`, Paso 9.B).

use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use tokio::sync::broadcast;
use tokio::task::JoinHandle;

use gt_eventlog::EventRecord;
use gt_events::AppError;

/// Observer plugin contract. Implementations are strictly read-only with respect to the
/// domain: they may emit telemetry, write to their own files or push to their own channels,
/// but they MUST NOT publish events back into the kernel bus or mutate domain actors. That
/// invariant is what makes the relay replay-safe (see crate docs).
///
/// `name` returns a `'static` string so dead-letter entries (and metrics) can carry it
/// without an allocation per event. `on_event` returns [`AppError`] so a failure is recorded
/// in the dead-letter and the fan-out continues with the next plugin, mirroring the
/// `gt-bus::Bus::publish` semantics.
#[async_trait]
pub trait Plugin: Send + Sync {
    /// Stable identifier used in dead-letter entries and metrics. Conventionally lowercase
    /// kebab-case (`"sheriff"`, `"recording"`).
    fn name(&self) -> &'static str;

    /// Called for every appended [`EventRecord`] the root broadcasts. Returning `Err` does
    /// **not** stop the fan-out — the entry goes to the dead-letter and the next plugin runs.
    async fn on_event(&self, record: &EventRecord) -> Result<(), AppError>;
}

/// Read-only handler for a **filtered** cross-module subscription (see
/// [`PluginRegistry::subscribe`]). Like [`Plugin`] it must not publish back into the domain
/// bus or mutate actors — the subscription is an observer, so replay stays byte-identical
/// (guardrail Rule: cross-module communication is event-driven + read-only). The relay only
/// invokes `on_match` for records whose versioned `kind` matched the subscribed `(kind,
/// version)`, so the handler never has to re-check the kind.
#[async_trait]
pub trait Subscriber: Send + Sync {
    /// Fired once per matching [`EventRecord`]. Returning `Err` routes the failure to the
    /// relay's [`PluginDeadLetter`] under the subscriber's `module_id` and continues the
    /// fan-out, exactly like a [`Plugin`] error.
    async fn on_match(&self, record: &EventRecord) -> Result<(), AppError>;
}

/// A `(kind, version)`-filtered observer: a [`Subscriber`] adapted to the [`Plugin`] fan-out
/// so cross-module subscriptions ride the **same** relay, ordering and dead-letter machinery
/// as full observers. The kind filter is applied here — inside the relay's call path, before
/// the handler fires — so `on_match` only ever sees the records it asked for. Built by
/// [`PluginRegistry::subscribe`]; not constructed directly.
struct FilteredSubscription {
    /// Stable subscriber identity, surfaced in dead-letter entries (e.g. `"dogs"`).
    module_id: &'static str,
    /// The full versioned kind this subscription matches, e.g. `bead.created.v1`.
    want_kind: String,
    handler: Arc<dyn Subscriber>,
}

#[async_trait]
impl Plugin for FilteredSubscription {
    fn name(&self) -> &'static str {
        self.module_id
    }

    async fn on_event(&self, record: &EventRecord) -> Result<(), AppError> {
        // Filter in the relay path: non-matching kinds are a no-op, so v1 and v2 of the same
        // noun route to distinct subscriptions and unrelated kinds never reach the handler.
        if record.kind == self.want_kind {
            self.handler.on_match(record).await
        } else {
            Ok(())
        }
    }
}

/// Dead-letter entry produced by the plugin relay. Errors carry a `'static` plugin name and
/// the event kind that triggered them; `Lagged` is recorded when a slow plugin fell behind
/// the broadcast ring and skipped some events (mirrors `tokio::broadcast::RecvError::Lagged`).
#[derive(Debug)]
pub enum PluginDeadLetterEntry {
    /// A plugin's `on_event` returned `Err`. The fan-out continued; the chain is intact.
    HandlerError {
        plugin: &'static str,
        kind: String,
        error: AppError,
    },
    /// The relay's broadcast receiver fell behind by `skipped` events (the broadcast ring is
    /// finite — see `RootConfig::event_buffer`). The plugins did not see those events.
    Lagged { skipped: u64 },
}

/// In-memory dead-letter sink for the plugin relay. Synchronous and thread-safe so the relay
/// task can `record_*` without `.await`. The kernel's other dead-letter (`gt-bus::DeadLetter`)
/// uses `RefCell` because the sync bus runs on one thread; here the relay is a tokio task and
/// callers (test gates, `/health`) may read from another, so `Mutex` is correct.
#[derive(Default)]
pub struct PluginDeadLetter {
    entries: Mutex<Vec<PluginDeadLetterEntry>>,
}

impl PluginDeadLetter {
    pub fn new() -> Self {
        Self {
            entries: Mutex::new(Vec::new()),
        }
    }

    pub fn record_error(&self, plugin: &'static str, kind: &str, error: AppError) {
        self.entries
            .lock()
            .expect("plugin dead-letter mutex poisoned")
            .push(PluginDeadLetterEntry::HandlerError {
                plugin,
                kind: kind.to_string(),
                error,
            });
    }

    pub fn record_lagged(&self, skipped: u64) {
        self.entries
            .lock()
            .expect("plugin dead-letter mutex poisoned")
            .push(PluginDeadLetterEntry::Lagged { skipped });
    }

    pub fn len(&self) -> usize {
        self.entries
            .lock()
            .expect("plugin dead-letter mutex poisoned")
            .len()
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Drain and return every entry recorded so far.
    pub fn drain(&self) -> Vec<PluginDeadLetterEntry> {
        self.entries
            .lock()
            .expect("plugin dead-letter mutex poisoned")
            .drain(..)
            .collect()
    }
}

/// Ordered set of observer plugins plus a shared [`PluginDeadLetter`]. The registry is built
/// at composition time, wrapped in `Arc`, and handed to [`spawn_plugin_relay`]. Order matters:
/// the relay invokes plugins in the order they were registered, which is the contract the
/// gate test asserts.
pub struct PluginRegistry {
    plugins: Vec<Arc<dyn Plugin>>,
    deadletter: Arc<PluginDeadLetter>,
}

impl Default for PluginRegistry {
    fn default() -> Self {
        Self::new()
    }
}

impl PluginRegistry {
    pub fn new() -> Self {
        Self {
            plugins: Vec::new(),
            deadletter: Arc::new(PluginDeadLetter::new()),
        }
    }

    /// Append a plugin to the chain. Builder-style so `main` reads top-down.
    pub fn register<P: Plugin + 'static>(mut self, plugin: P) -> Self {
        self.plugins.push(Arc::new(plugin));
        self
    }

    /// Append a pre-shared plugin (when the caller already owns an `Arc` — e.g. it wants to
    /// also hold a handle for state inspection).
    pub fn register_arc(mut self, plugin: Arc<dyn Plugin>) -> Self {
        self.plugins.push(plugin);
        self
    }

    /// Register a **cross-module subscription**: `handler` fires only for records whose
    /// versioned kind equals `<kind>.v<version>` (e.g. `subscribe("dogs", "bead.created", 1,
    /// …)` matches `bead.created.v1`). The match is evaluated in the relay before the handler
    /// runs, so a subscriber to `bead.created.v1` never sees `bead.created.v2` — versions
    /// coexist as distinct subscriptions.
    ///
    /// Builder-style and order-preserving: the subscription joins the same ordered fan-out as
    /// full [`Plugin`] observers and shares the relay's [`PluginDeadLetter`], so a handler
    /// error is recorded under `module_id` without breaking the chain. Existing all-events
    /// observers (e.g. [`SheriffPlugin`](crate::SheriffPlugin) via [`register`](Self::register))
    /// are unaffected — this is purely additive.
    pub fn subscribe(
        self,
        module_id: &'static str,
        kind: &str,
        version: u32,
        handler: Arc<dyn Subscriber>,
    ) -> Self {
        self.register(FilteredSubscription {
            module_id,
            want_kind: format!("{kind}.v{version}"),
            handler,
        })
    }

    pub fn plugins(&self) -> &[Arc<dyn Plugin>] {
        &self.plugins
    }

    pub fn len(&self) -> usize {
        self.plugins.len()
    }

    pub fn is_empty(&self) -> bool {
        self.plugins.is_empty()
    }

    pub fn names(&self) -> Vec<&'static str> {
        self.plugins.iter().map(|p| p.name()).collect()
    }

    pub fn deadletter(&self) -> Arc<PluginDeadLetter> {
        self.deadletter.clone()
    }
}

/// Spawn the plugin relay: subscribe to the broadcast, drain forever, fan-out per record.
///
/// - Plugins run **sequentially** per event, in registration order (the gate criterion).
/// - A plugin returning `Err` does not abort the chain; the entry goes to the registry's
///   [`PluginDeadLetter`] and the next plugin runs on the same record.
/// - [`broadcast::error::RecvError::Lagged`] is recorded too — the relay never silently drops
///   events.
/// - When the broadcast sender is dropped (`Closed`) the task exits cleanly.
///
/// Returns the [`JoinHandle`]; the caller (composition root) keeps it for shutdown.
pub fn spawn_plugin_relay(
    mut rx: broadcast::Receiver<EventRecord>,
    registry: Arc<PluginRegistry>,
) -> JoinHandle<()> {
    tokio::spawn(async move {
        loop {
            match rx.recv().await {
                Ok(rec) => {
                    for plugin in registry.plugins() {
                        if let Err(e) = plugin.on_event(&rec).await {
                            registry
                                .deadletter
                                .record_error(plugin.name(), &rec.kind, e);
                        }
                    }
                }
                Err(broadcast::error::RecvError::Lagged(n)) => {
                    registry.deadletter.record_lagged(n);
                }
                Err(broadcast::error::RecvError::Closed) => break,
            }
        }
    })
}
