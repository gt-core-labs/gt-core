//! Gate del Paso 2 (docs/08-getting-started.md): levanta el actor, spawnea un fake
//! polecat, lo deja morir, y observa `SessionEnd` en un suscriptor de test. Cero SQL.

use std::cell::RefCell;
use std::rc::Rc;
use std::time::Duration;

use tokio::sync::mpsc;

use gt_agent::{actor, supervisor, AgentEvent, Session, SessionState};
use gt_bus::Bus;
use gt_events::{Ctx, Envelope};

fn tmp_hb(tag: &str) -> std::path::PathBuf {
    std::env::temp_dir().join(format!("gt-hb-{}-{}", std::process::id(), tag))
}

#[tokio::test]
async fn fake_polecat_lifecycle_emits_session_end() {
    // 1. actor arriba, con una sesión working.
    let agent = actor::spawn(16);
    agent.add(Session::new("polecat-1", "granite")).await;
    agent.transition("polecat-1", SessionState::Working).await.unwrap();

    // 2. bus + suscriptor de test.
    let bus: Bus<AgentEvent> = Bus::new();
    let seen: Rc<RefCell<Vec<String>>> = Rc::new(RefCell::new(Vec::new()));
    {
        let seen = Rc::clone(&seen);
        bus.subscribe("agent.session_end", move |_ctx, env| {
            if let AgentEvent::SessionEnd { session } = &env.payload {
                seen.borrow_mut().push(session.clone());
            }
            Ok(())
        });
    }

    // 3. spawn fake polecat que muere rápido; supervisor lo vigila.
    let (tx, mut rx) = mpsc::channel::<Envelope<AgentEvent>>(8);
    let p = supervisor::spawn_fake("polecat-1", 0.2, tmp_hb("life"))
        .await
        .unwrap();
    let sup = tokio::spawn(supervisor::supervise(p, tx, Duration::from_secs(5)));

    // 4. el proceso muere → supervisor emite SessionEnd al relay. Drenamos al bus sync.
    sup.await.unwrap();
    let ctx = Ctx::root();
    while let Some(env) = rx.recv().await {
        if let AgentEvent::SessionEnd { session } = &env.payload {
            let _ = agent.transition(session.clone(), SessionState::Killed).await;
        }
        bus.publish(&ctx, env).unwrap();
    }

    // 5. el suscriptor vio el SessionEnd; el actor marcó la sesión terminal.
    assert_eq!(*seen.borrow(), vec!["polecat-1".to_string()]);
    let snap = agent.snapshot().await;
    let s = snap.iter().find(|s| s.id == "polecat-1").unwrap();
    assert!(s.is_terminal(), "el actor debe observar el fin de la sesión");
    assert!(bus.deadletter.is_empty(), "session_end tenía suscriptor");
}

#[tokio::test]
async fn actor_owns_state_via_messages() {
    let agent = actor::spawn(16);
    agent.add(Session::new("a", "r")).await;
    agent.add(Session::new("b", "r")).await;
    assert_eq!(agent.snapshot().await.len(), 2);

    agent.transition("a", SessionState::Working).await.unwrap();
    // transición ilegal rechazada a través del actor (Spawned -> Done)
    assert!(agent.transition("b", SessionState::Done).await.is_err());
    // id desconocido
    assert!(agent.transition("zzz", SessionState::Working).await.is_err());

    agent.remove("b").await;
    assert_eq!(agent.snapshot().await.len(), 1);
}

#[tokio::test]
async fn stale_heartbeat_triggers_session_end() {
    let (tx, mut rx) = mpsc::channel::<Envelope<AgentEvent>>(8);
    // proceso largo, pero stale_after diminuto → el heartbeat fresco se vuelve stale al primer tick.
    let p = supervisor::spawn_fake("p-stale", 60.0, tmp_hb("stale"))
        .await
        .unwrap();
    let sup = tokio::spawn(supervisor::supervise(p, tx, Duration::from_millis(1)));

    let env = rx.recv().await.expect("debe emitir SessionEnd por staleness");
    assert!(matches!(env.payload, AgentEvent::SessionEnd { .. }));
    sup.await.unwrap();
}
