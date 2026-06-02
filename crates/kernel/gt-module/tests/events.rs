//! End-to-end gate for the versioned-events stack (`hq-mod-events.7`).
//!
//! This is the capstone integration test for the `hq-mod-events` epic: it exercises the
//! pieces that landed across three kernel crates as one coherent flow, rather than in
//! isolation. Concretely it proves:
//!
//! 1. **Legacy bare kind → `.v1` on read** — `gt-eventlog`'s reader upgrades a pre-versioning
//!    bare kind (`hq-mod-events.2`), so old logs replay as versioned (`.v1`).
//! 2. **v1 reducer applies** — a `demo.created.v1` record folds into state.
//! 3. **v2 reducer applies** — a `demo.created.v2` record (extra field) folds into state.
//! 4. **Mixed stream replays byte-for-byte** — a log mixing v1 and v2 of the same noun
//!    replays deterministically via `gt_eventlog::replay_dispatch` (`hq-mod-events.3`).
//! 5. **Cross-module subscribe filters correctly** — a `gt_plugin` subscription
//!    (`hq-mod-events.4`) fires only for its `(kind, version)`; a full observer still sees all.
//! 6. **Deprecation warning fires exactly once per emission** — the `Capability` deprecation
//!    API (`hq-mod-events.6`) surfaces its note once per fire at the emission boundary.
//!
//! These crates are dev-dependencies (none depends on `gt-module`, so there is no cycle);
//! the test lives here because the epic is the module system's versioned-events contract.

use std::sync::{Arc, Mutex};
use std::time::Duration;

use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use tokio::sync::broadcast;
use tokio::time::sleep;

use gt_events::AppError;
use gt_eventlog::{read_all, replay_dispatch, EventRecord};
use gt_module::{Capability, EventKind};
use gt_plugin::{spawn_plugin_relay, Plugin, PluginRegistry, Subscriber};

// --- shared fixtures -------------------------------------------------------------------

/// Two versions of the same noun `demo.created`; v2 adds a field. On the wire each travels
/// as its own `EventRecord` carrying its versioned `kind`.
#[derive(Serialize, Deserialize)]
struct CreatedV1 {
    name: String,
}
#[derive(Serialize, Deserialize)]
struct CreatedV2 {
    name: String,
    color: String,
}

fn record(kind: &str, payload: serde_json::Value) -> EventRecord {
    EventRecord {
        event_id: format!("evt-{kind}"),
        correlation_id: "corr-1".into(),
        causation_id: None,
        ts: "2026-06-02T00:00:00Z".into(),
        kind: kind.into(),
        payload,
    }
}

/// Reconstructed state: names in log order + how many carried a colour (v2 only).
#[derive(Default, PartialEq, Debug, Clone)]
struct State {
    names: Vec<String>,
    with_color: usize,
}

/// The reducer dispatch table, keyed by the versioned `kind`: v1 and v2 of the same noun
/// route to distinct arms, so both coexist in one log.
fn reduce(state: &mut State, rec: &EventRecord) -> Result<(), AppError> {
    match rec.kind.as_str() {
        "demo.created.v1" => {
            let e: CreatedV1 = rec.decode()?;
            state.names.push(e.name);
        }
        "demo.created.v2" => {
            let e: CreatedV2 = rec.decode()?;
            state.names.push(e.name);
            state.with_color += 1;
        }
        other => return Err(AppError::Other(format!("unknown kind {other:?}"))),
    }
    Ok(())
}

// --- 1. legacy bare kind -> v1 on read -------------------------------------------------

#[test]
fn legacy_bare_kind_is_upgraded_to_v1_on_read() {
    use std::io::Write;

    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("legacy.jsonl");
    let mut f = std::fs::File::create(&path).unwrap();
    // A pre-versioning bare kind (in the reader's known-legacy set) + an already-versioned one.
    let line = |kind: &str| {
        format!(
            r#"{{"event_id":"e1","correlation_id":"c1","causation_id":null,"ts":"2026-05-27T10:00:00Z","type":"{kind}","payload":{{}}}}"#
        )
    };
    writeln!(f, "{}", line("quota.rotated")).unwrap();
    writeln!(f, "{}", line("merge.merged.v1")).unwrap();
    drop(f);

    let recs = read_all(&path).unwrap();
    assert_eq!(recs[0].kind, "quota.rotated.v1", "legacy bare kind parses as .v1");
    assert_eq!(recs[1].kind, "merge.merged.v1", "already-versioned kind untouched");
}

// --- 2 & 3. v1 and v2 reducers each apply ----------------------------------------------

#[test]
fn v1_reducer_applies() {
    let log = vec![record("demo.created.v1", serde_json::json!({ "name": "alpha" }))];
    let state = replay_dispatch(&log, State::default(), reduce).unwrap();
    assert_eq!(state.names, vec!["alpha"]);
    assert_eq!(state.with_color, 0);
}

#[test]
fn v2_reducer_applies() {
    let log = vec![record(
        "demo.created.v2",
        serde_json::json!({ "name": "beta", "color": "red" }),
    )];
    let state = replay_dispatch(&log, State::default(), reduce).unwrap();
    assert_eq!(state.names, vec!["beta"]);
    assert_eq!(state.with_color, 1, "v2 carries colour");
}

// --- 4. mixed v1/v2 stream replays byte-for-byte ---------------------------------------

#[test]
fn mixed_v1_v2_stream_replays_byte_for_byte() {
    let log = vec![
        record("demo.created.v1", serde_json::json!({ "name": "alpha" })),
        record(
            "demo.created.v2",
            serde_json::json!({ "name": "beta", "color": "red" }),
        ),
        record("demo.created.v1", serde_json::json!({ "name": "gamma" })),
    ];

    // Both versions fold in order; replaying twice yields an identical state (deterministic
    // replay = the byte-for-byte gate: a pure reducer over the same log -> the same state).
    let first = replay_dispatch(&log, State::default(), reduce).unwrap();
    let second = replay_dispatch(&log, State::default(), reduce).unwrap();

    assert_eq!(first.names, vec!["alpha", "beta", "gamma"]);
    assert_eq!(first.with_color, 1);
    assert_eq!(first, second, "same log -> same state");
}

// --- 5. cross-module subscribe filters correctly ---------------------------------------

/// Records the kinds a full observer (all events) sees.
struct AllObserver {
    seen: Arc<Mutex<Vec<String>>>,
}
#[async_trait]
impl Plugin for AllObserver {
    fn name(&self) -> &'static str {
        "observer"
    }
    async fn on_event(&self, record: &EventRecord) -> Result<(), AppError> {
        self.seen.lock().unwrap().push(record.kind.clone());
        Ok(())
    }
}

/// Records the kinds the filtered subscription actually receives.
struct MatchSubscriber {
    seen: Arc<Mutex<Vec<String>>>,
}
#[async_trait]
impl Subscriber for MatchSubscriber {
    async fn on_match(&self, record: &EventRecord) -> Result<(), AppError> {
        self.seen.lock().unwrap().push(record.kind.clone());
        Ok(())
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
async fn cross_module_subscribe_is_filtered_by_kind_and_version() {
    let (tx, rx) = broadcast::channel::<EventRecord>(16);
    let observed = Arc::new(Mutex::new(Vec::new()));
    let matched = Arc::new(Mutex::new(Vec::new()));

    let registry = Arc::new(
        PluginRegistry::new()
            .register(AllObserver {
                seen: observed.clone(),
            })
            .subscribe(
                "subscriber",
                "demo.created",
                1,
                Arc::new(MatchSubscriber {
                    seen: matched.clone(),
                }),
            ),
    );
    let task = spawn_plugin_relay(rx, registry);

    tx.send(record("demo.created.v1", serde_json::json!({ "name": "a" }))).unwrap();
    tx.send(record("demo.created.v2", serde_json::json!({ "name": "b", "color": "g" }))).unwrap();
    tx.send(record("other.thing.v1", serde_json::json!({}))).unwrap();

    drain_until(|| observed.lock().unwrap().len() == 3, Duration::from_secs(2)).await;

    assert_eq!(
        *matched.lock().unwrap(),
        vec!["demo.created.v1".to_string()],
        "subscription fires only for its matched (kind, version)"
    );
    assert_eq!(
        observed.lock().unwrap().len(),
        3,
        "the full observer still sees every record"
    );

    drop(tx);
    let _ = task.await;
}

// --- 6. deprecation warning fires exactly once per emission -----------------------------

/// The emission boundary: a module that emits `kind` consults its [`Capability`] and, if the
/// kind is deprecated, surfaces the note as a warning — once per emission. `gt-module` owns
/// the *declaration* (`deprecation_note`); the firing is the emitter's responsibility, which
/// this closure models for the test.
fn emit_with_deprecation_boundary(cap: &Capability, kind: &EventKind, warnings: &mut Vec<String>) {
    if let Some(note) = cap.deprecation_note(kind) {
        warnings.push(note.to_string());
    }
}

#[test]
fn deprecation_warning_fires_exactly_once_per_emission() {
    let deprecated = EventKind::new("demo.created.v1").unwrap();
    let current = EventKind::new("demo.created.v2").unwrap();

    let cap = Capability::empty()
        .emitting_deprecated(deprecated.clone(), "use demo.created.v2; v1 removed after P5")
        .emitting(current.clone());

    let mut warnings = Vec::new();
    // Fire the deprecated kind three times + the current kind twice.
    for _ in 0..3 {
        emit_with_deprecation_boundary(&cap, &deprecated, &mut warnings);
    }
    for _ in 0..2 {
        emit_with_deprecation_boundary(&cap, &current, &mut warnings);
    }

    // Exactly one warning per emission of the deprecated kind (3), none for the current one.
    assert_eq!(warnings.len(), 3, "one warning per fire of the deprecated kind");
    assert!(
        warnings.iter().all(|w| w == "use demo.created.v2; v1 removed after P5"),
        "each warning carries the migration note"
    );
    assert!(!cap.is_deprecated(&current), "the current version is not deprecated");
}
