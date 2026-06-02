use std::cell::RefCell;
use std::collections::HashMap;

use gt_events::{AppError, Ctx, Envelope, EventKind};

use crate::deadletter::DeadLetter;

/// Un handler síncrono. Recibe el contexto y el envelope por referencia; devuelve
/// `Result` para que un fallo vaya a dead-letter sin romper el fan-out.
type Handler<E> = Box<dyn Fn(&Ctx, &Envelope<E>) -> Result<(), AppError>>;

/// Bus de eventos in-process, genérico sobre el enum de eventos `E`.
///
/// `subscribe`/`publish` toman `&self` (interior mutability vía `RefCell`) para encajar en
/// el cableado del composition root. **Nota:** no suscribir desde dentro de un handler
/// durante un `publish` (re-entraría a `borrow_mut` y haría panic); el cableado se hace al
/// arranque, antes de publicar.
pub struct Bus<E: EventKind> {
    subscribers: RefCell<HashMap<&'static str, Vec<Handler<E>>>>,
    /// Dead-letter del bus: eventos sin handler + handlers con `Err`.
    pub deadletter: DeadLetter<E>,
}

impl<E: EventKind> Default for Bus<E> {
    fn default() -> Self {
        Self::new()
    }
}

impl<E: EventKind> Bus<E> {
    pub fn new() -> Self {
        Self {
            subscribers: RefCell::new(HashMap::new()),
            deadletter: DeadLetter::new(),
        }
    }

    /// Registra un handler para un `kind`. Varios handlers por kind se invocan en orden
    /// de registro.
    pub fn subscribe<F>(&self, kind: &'static str, handler: F)
    where
        F: Fn(&Ctx, &Envelope<E>) -> Result<(), AppError> + 'static,
    {
        self.subscribers
            .borrow_mut()
            .entry(kind)
            .or_default()
            .push(Box::new(handler));
    }

    /// Publica un evento. Fan-out **síncrono** a todos los suscriptores del kind, en orden.
    ///
    /// - Cero suscriptores → el envelope va a dead-letter como `Unhandled` (no se pierde).
    /// - Un handler que devuelve `Err` → su error va a dead-letter; el fan-out **continúa**
    ///   con los demás handlers.
    pub fn publish(&self, ctx: &Ctx, ev: Envelope<E>) -> Result<(), AppError> {
        let kind = ev.kind();

        let has_handlers = self
            .subscribers
            .borrow()
            .get(kind)
            .is_some_and(|v| !v.is_empty());

        if !has_handlers {
            self.deadletter.record_unhandled(ev);
            return Ok(());
        }

        let subs = self.subscribers.borrow();
        for handler in &subs[kind] {
            if let Err(e) = handler(ctx, &ev) {
                self.deadletter.record_error(kind, e);
            }
        }
        Ok(())
    }
}
