use std::cell::RefCell;

use gt_events::{AppError, Envelope, EventKind};

/// Una entrada de dead-letter: o un evento sin suscriptores, o un handler que falló.
#[derive(Debug)]
pub enum DeadLetterEntry<E: EventKind> {
    /// `publish` sin ningún suscriptor para el `kind` — el bug más común (evento nuevo,
    /// handler sin cablear). Se conserva el envelope completo.
    Unhandled(Envelope<E>),
    /// Un handler devolvió `Err`. No se loguea-y-descarta: queda aquí.
    HandlerError {
        kind: &'static str,
        error: AppError,
    },
}

/// Colector de dead-letter. In-memory y síncrono (suficiente para la espina y los tests);
/// en producción el writer lo drena al audit log.
#[derive(Default)]
pub struct DeadLetter<E: EventKind> {
    entries: RefCell<Vec<DeadLetterEntry<E>>>,
}

impl<E: EventKind> DeadLetter<E> {
    pub fn new() -> Self {
        Self {
            entries: RefCell::new(Vec::new()),
        }
    }

    pub(crate) fn record_unhandled(&self, env: Envelope<E>) {
        self.entries.borrow_mut().push(DeadLetterEntry::Unhandled(env));
    }

    pub(crate) fn record_error(&self, kind: &'static str, error: AppError) {
        self.entries
            .borrow_mut()
            .push(DeadLetterEntry::HandlerError { kind, error });
    }

    /// Nº de entradas acumuladas.
    pub fn len(&self) -> usize {
        self.entries.borrow().len()
    }

    pub fn is_empty(&self) -> bool {
        self.entries.borrow().is_empty()
    }

    /// Vacía y devuelve todo lo acumulado.
    pub fn drain(&self) -> Vec<DeadLetterEntry<E>> {
        self.entries.borrow_mut().drain(..).collect()
    }
}
