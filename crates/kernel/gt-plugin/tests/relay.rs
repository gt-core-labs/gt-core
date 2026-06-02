//! Plugin relay gate tests (hq-evks):
//!
//! 1. **Order** — plugins fire in registration order for every record.
//! 2. **Errors → dead-letter** — a failing plugin records an entry; the chain continues to
//!    the next plugin and the next record.
//! 3. **Lagged** — when the broadcast ring overflows, the relay records a `Lagged` entry
//!    instead of silently dropping events.

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use async_trait::async_trait;
use tokio::sync::broadcast;
use tokio::time::sleep;

use gt_eventlog::EventRecord;
use gt_events::AppError;
use gt_plugin::{
    spawn_plugin_relay, Plugin, PluginDeadLetterEntry, PluginRegistry, SheriffPlugin,
};

fn rec(kind: &str, n: u64) -> EventRecord {
    EventRecord {
        event_id: format!("e{n}"),
        correlation_id: format!("c{n}"),
        causation_id: None,
        ts: "1970-01-01T00:00:00Z".into(),
        kind: kind.to_string(),
        payload: serde_json::json!({"n": n}),
    }
}

/// Records the order in which it sees events into a shared log.
struct RecorderPlugin {
    name: &'static str,
    log: Arc<Mutex<Vec<(&'static str, String)>>>,
}

#[async_trait]
impl Plugin for RecorderPlugin {
    fn name(&self) -> &'static str {
        self.name
    }
    async fn on_event(&self, record: &EventRecord) -> Result<(), AppError> {
        self.log
            .lock()
            .unwrap()
            .push((self.name, record.event_id.clone()));
        Ok(())
    }
}

/// Always returns `Err` so the relay routes it to the dead-letter.
struct FailingPlugin {
    seen: Arc<AtomicUsize>,
}

#[async_trait]
impl Plugin for FailingPlugin {
    fn name(&self) -> &'static str {
        "failing"
    }
    async fn on_event(&self, _record: &EventRecord) -> Result<(), AppError> {
        self.seen.fetch_add(1, Ordering::SeqCst);
        Err(AppError::Handler("boom".into()))
    }
}

async fn drain_until<F: Fn() -> bool>(predicate: F, max: Duration) {
    let start = std::time::Instant::now();
    while !predicate() {
        if start.elapsed() > max {
            panic!("predicate did not become true within {max:?}");
        }
        sleep(Duration::from_millis(5)).await;
    }
}

#[tokio::test(flavor = "current_thread")]
async fn relay_fans_out_in_registration_order() {
    let (tx, rx) = broadcast::channel::<EventRecord>(16);
    let log = Arc::new(Mutex::new(Vec::new()));

    let sheriff = SheriffPlugin::new();
    let sheriff_counter = sheriff.counter();

    let registry = Arc::new(
        PluginRegistry::new()
            .register(RecorderPlugin {
                name: "first",
                log: log.clone(),
            })
            .register(RecorderPlugin {
                name: "second",
                log: log.clone(),
            })
            .register_arc(Arc::new(sheriff)),
    );
    let task = spawn_plugin_relay(rx, registry);

    tx.send(rec("agent.spawned", 1)).unwrap();
    tx.send(rec("quota.usage_probed", 2)).unwrap();

    drain_until(|| log.lock().unwrap().len() == 4, Duration::from_secs(2)).await;
    drain_until(
        || sheriff_counter.load(Ordering::SeqCst) == 2,
        Duration::from_secs(2),
    )
    .await;

    let observed = log.lock().unwrap().clone();
    assert_eq!(
        observed,
        vec![
            ("first", "e1".into()),
            ("second", "e1".into()),
            ("first", "e2".into()),
            ("second", "e2".into()),
        ],
        "registration order must be honored per event"
    );

    drop(tx);
    let _ = task.await;
}

#[tokio::test(flavor = "current_thread")]
async fn plugin_error_lands_in_dead_letter_and_chain_continues() {
    let (tx, rx) = broadcast::channel::<EventRecord>(16);
    let log = Arc::new(Mutex::new(Vec::new()));
    let failing_seen = Arc::new(AtomicUsize::new(0));

    let registry = Arc::new(
        PluginRegistry::new()
            .register(FailingPlugin {
                seen: failing_seen.clone(),
            })
            .register(RecorderPlugin {
                name: "after-failure",
                log: log.clone(),
            }),
    );
    let dead = registry.deadletter();
    let task = spawn_plugin_relay(rx, registry);

    tx.send(rec("agent.spawned", 1)).unwrap();
    tx.send(rec("agent.spawned", 2)).unwrap();

    drain_until(
        || log.lock().unwrap().len() == 2 && failing_seen.load(Ordering::SeqCst) == 2,
        Duration::from_secs(2),
    )
    .await;

    // Successor plugin still fired on every event.
    let observed = log.lock().unwrap().clone();
    assert_eq!(
        observed,
        vec![("after-failure", "e1".into()), ("after-failure", "e2".into())]
    );

    // Failing plugin recorded two dead-letter entries — one per event.
    let entries = dead.drain();
    assert_eq!(entries.len(), 2);
    for entry in &entries {
        match entry {
            PluginDeadLetterEntry::HandlerError {
                plugin,
                kind,
                error,
            } => {
                assert_eq!(*plugin, "failing");
                assert_eq!(kind, "agent.spawned");
                assert!(matches!(error, AppError::Handler(_)));
            }
            other => panic!("expected HandlerError, got {other:?}"),
        }
    }

    drop(tx);
    let _ = task.await;
}

#[tokio::test(flavor = "current_thread")]
async fn lagged_subscriber_is_recorded_not_dropped() {
    // Tiny ring so we can force overflow before the relay drains.
    let (tx, rx) = broadcast::channel::<EventRecord>(2);

    let sheriff = SheriffPlugin::new();
    let counter = sheriff.counter();
    let registry = Arc::new(PluginRegistry::new().register_arc(Arc::new(sheriff)));
    let dead = registry.deadletter();

    // Fill the ring past capacity BEFORE spawning the relay so its first `recv` returns
    // `Lagged`. After Lagged, subsequent recvs deliver only the records still in the ring
    // (the last `N` for ring of size N=2).
    for n in 0..6 {
        tx.send(rec("filler", n)).unwrap();
    }

    let task = spawn_plugin_relay(rx, registry);

    drain_until(
        || {
            // Lagged is recorded once; counter increments for the survivors in the ring.
            !dead.is_empty() && counter.load(Ordering::SeqCst) > 0
        },
        Duration::from_secs(2),
    )
    .await;

    let entries = dead.drain();
    assert!(
        entries
            .iter()
            .any(|e| matches!(e, PluginDeadLetterEntry::Lagged { skipped } if *skipped > 0)),
        "expected at least one Lagged entry: {entries:?}",
    );

    drop(tx);
    let _ = task.await;
}
