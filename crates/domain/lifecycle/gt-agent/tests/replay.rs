//! Gate del Paso 3 (docs/08-getting-started.md): graba 100 eventos en `.events.jsonl`, los
//! re-corre por el reducer puro, y verifica que el `SessionRegistry` reconstruido es
//! **byte-idéntico** al de la corrida en vivo. Si fallara, hay impureza filtrada al núcleo
//! (reloj/random/async); es lo que justifica el núcleo síncrono.

use gt_agent::{AgentEvent, SessionRegistry, SessionRole};
use gt_eventlog::{read_all, replay, EventRecord, EventStore, JsonlWriter};
use gt_events::Envelope;

/// 100 eventos deterministas: 50 spawns, 30 ends, 10 kills, 10 heartbeats (no-op).
fn make_events() -> Vec<AgentEvent> {
    let mut evs = Vec::with_capacity(100);
    for i in 0..50 {
        evs.push(AgentEvent::Spawned {
            session: format!("p{i:02}"),
            rig: "granite".into(),
            role: SessionRole::Polecat,
            crew: None,
            skills: Vec::new(),
            hooks: Vec::new(),
            maintains_heartbeat: true,
            tmux_socket: None,
            spawned_by: None,
        });
    }
    for i in 0..30 {
        evs.push(AgentEvent::SessionEnd { session: format!("p{i:02}"), at_secs: None });
    }
    for i in 30..40 {
        evs.push(AgentEvent::Killed { session: format!("p{i:02}"), reason: "timeout".into(), at_secs: None });
    }
    for i in 0..10 {
        evs.push(AgentEvent::Heartbeat {
            session: format!("p{i:02}"),
            timestamp_secs: Some(1_700_000_000 + i as u64),
        });
    }
    assert_eq!(evs.len(), 100);
    evs
}

#[test]
fn replay_reconstructs_byte_identical_state() {
    let events = make_events();

    // 1. Corrida en vivo: pliega los eventos por el reducer puro.
    let mut live = SessionRegistry::default();
    for ev in &events {
        live.apply(ev);
    }

    // 2. Graba los 100 eventos como EventRecord en .events.jsonl.
    let log_path = std::env::temp_dir().join(format!("gt-replay-{}.events.jsonl", std::process::id()));
    let _ = std::fs::remove_file(&log_path);
    let writer = JsonlWriter::new(&log_path);
    for ev in &events {
        let env = Envelope::root(ev.clone());
        let rec = EventRecord::from_envelope(&env).unwrap();
        writer.append(&rec).unwrap();
    }

    // 3. Relee el log y lo re-corre por el mismo reducer, desde estado vacío.
    let records = read_all(&log_path).unwrap();
    assert_eq!(records.len(), 100, "deben grabarse y releerse 100 records");
    let replayed: SessionRegistry =
        replay(&records, SessionRegistry::default(), |reg, ev: &AgentEvent| reg.apply(ev)).unwrap();

    // 4. Estado reconstruido == estado en vivo, byte a byte.
    assert_eq!(
        replayed.fingerprint(),
        live.fingerprint(),
        "replay debe reconstruir estado byte-idéntico"
    );
    // sanity: 50 spawn - 30 done - 10 killed = 10 activas
    assert_eq!(replayed.active().len(), 10);
    assert_eq!(replayed.len(), 50);

    let _ = std::fs::remove_file(&log_path);
}

#[test]
fn replay_is_idempotent_across_runs() {
    // Re-correr el mismo log dos veces da el mismo fingerprint (determinismo puro).
    let events = make_events();
    let records: Vec<EventRecord> = events
        .iter()
        .map(|ev| EventRecord::from_envelope(&Envelope::root(ev.clone())).unwrap())
        .collect();

    let a = replay(&records, SessionRegistry::default(), |r, e: &AgentEvent| r.apply(e)).unwrap();
    let b = replay(&records, SessionRegistry::default(), |r, e: &AgentEvent| r.apply(e)).unwrap();
    assert_eq!(a.fingerprint(), b.fingerprint());
}
