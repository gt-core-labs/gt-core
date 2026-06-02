use std::sync::mpsc::{self, Receiver, Sender};

use gt_events::EventKind;

/// Puente entre un handler **síncrono** del bus y trabajo de I/O que ocurre fuera de la
/// llamada de `publish`. El handler hace `try_send` (no bloquea el bus); una task
/// long-lived drena el `Receiver` y hace la I/O async.
///
/// Aquí se usa `std::sync::mpsc` a propósito: la espina es **cero-async, cero-tokio**. El
/// adaptador real (audit → Postgres) reemplazará el receiver por un drenado async en su
/// crate de borde; el patrón de `try_send` desde el handler sync es el mismo.
pub struct Relay<E: EventKind> {
    tx: Sender<E>,
}

impl<E: EventKind> Relay<E> {
    /// Crea el relay y devuelve el extremo de lectura para la task de drenado.
    pub fn new() -> (Self, Receiver<E>) {
        let (tx, rx) = mpsc::channel();
        (Self { tx }, rx)
    }

    /// Empuja un evento al canal sin bloquear (semántica `try_send`). Devuelve `false` si
    /// el receptor se cerró — el llamador decide (en producción: spill + métrica).
    pub fn try_send(&self, event: E) -> bool {
        self.tx.send(event).is_ok()
    }
}
