//! Gate de validación del Paso 1 (docs/08-getting-started.md):
//! 1. publish llega a N suscriptores en orden.
//! 2. publish sin suscriptores → `Unhandled` en dead-letter.
//! 3. handler que devuelve `Err` no rompe el fan-out; va a dead-letter.

use std::cell::RefCell;
use std::rc::Rc;

use gt_bus::{Bus, DeadLetterEntry};
use gt_events::{AppError, Ctx, Envelope, EventKind};

/// Evento de prueba: enum owned con ruteo por `kind()`.
#[derive(Debug, Clone)]
enum TestEvent {
    Ping { seq: u32 },
    Unwired,
}

impl EventKind for TestEvent {
    fn kind(&self) -> &'static str {
        match self {
            TestEvent::Ping { .. } => "test.ping",
            TestEvent::Unwired => "test.unwired",
        }
    }
}

#[test]
fn publish_reaches_all_subscribers_in_order() {
    let bus: Bus<TestEvent> = Bus::new();
    let log: Rc<RefCell<Vec<&'static str>>> = Rc::new(RefCell::new(Vec::new()));

    for tag in ["first", "second", "third"] {
        let log = Rc::clone(&log);
        bus.subscribe("test.ping", move |_ctx, _env| {
            log.borrow_mut().push(tag);
            Ok(())
        });
    }

    let ctx = Ctx::root();
    bus.publish(&ctx, Envelope::root(TestEvent::Ping { seq: 1 })).unwrap();

    assert_eq!(*log.borrow(), vec!["first", "second", "third"]);
    assert!(bus.deadletter.is_empty(), "fan-out limpio no debe dejar dead-letter");
}

#[test]
fn publish_without_subscribers_goes_to_deadletter() {
    let bus: Bus<TestEvent> = Bus::new();
    let ctx = Ctx::root();

    bus.publish(&ctx, Envelope::root(TestEvent::Unwired)).unwrap();

    let entries = bus.deadletter.drain();
    assert_eq!(entries.len(), 1);
    match &entries[0] {
        DeadLetterEntry::Unhandled(env) => assert_eq!(env.kind(), "test.unwired"),
        other => panic!("esperaba Unhandled, fue {other:?}"),
    }
}

#[test]
fn failing_handler_does_not_break_fanout() {
    let bus: Bus<TestEvent> = Bus::new();
    let reached = Rc::new(RefCell::new(0u32));

    // Handler 1: falla.
    bus.subscribe("test.ping", |_ctx, _env| {
        Err(AppError::Handler("boom".into()))
    });
    // Handler 2: tras el fallo del 1, debe ejecutarse igual.
    {
        let reached = Rc::clone(&reached);
        bus.subscribe("test.ping", move |_ctx, _env| {
            *reached.borrow_mut() += 1;
            Ok(())
        });
    }

    let ctx = Ctx::root();
    bus.publish(&ctx, Envelope::root(TestEvent::Ping { seq: 7 })).unwrap();

    assert_eq!(*reached.borrow(), 1, "el segundo handler debe correr pese al Err del primero");

    let entries = bus.deadletter.drain();
    assert_eq!(entries.len(), 1);
    match &entries[0] {
        DeadLetterEntry::HandlerError { kind, error } => {
            assert_eq!(*kind, "test.ping");
            assert_eq!(*error, AppError::Handler("boom".into()));
        }
        other => panic!("esperaba HandlerError, fue {other:?}"),
    }
}

#[test]
fn causation_chain_links_parent_to_child() {
    let parent = Envelope::root(TestEvent::Ping { seq: 1 });
    let child = Envelope::caused_by(&parent, TestEvent::Ping { seq: 2 });

    assert_eq!(child.correlation_id, parent.correlation_id, "hereda correlación");
    assert_eq!(child.causation_id, Some(parent.event_id), "apunta a su causa");
    assert_ne!(child.event_id, parent.event_id, "identidad propia");
}
