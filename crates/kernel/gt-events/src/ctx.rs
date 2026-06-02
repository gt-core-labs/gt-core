use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

use ulid::Ulid;

/// Token de cancelación cooperativo y **sin async** (sin tokio): un `AtomicBool`
/// compartido. Mantiene el núcleo libre de runtime; los bordes async pueden envolverlo o
/// puentearlo a un `CancellationToken` de tokio si lo necesitan.
#[derive(Clone, Default)]
pub struct CancelToken(Arc<AtomicBool>);

impl CancelToken {
    pub fn new() -> Self {
        Self(Arc::new(AtomicBool::new(false)))
    }

    pub fn cancel(&self) {
        self.0.store(true, Ordering::SeqCst);
    }

    pub fn is_cancelled(&self) -> bool {
        self.0.load(Ordering::SeqCst)
    }
}

/// Contexto que acompaña a `publish`: identidad de correlación + cancelación.
/// Inmutable y barato de clonar.
#[derive(Clone)]
pub struct Ctx {
    pub correlation_id: Ulid,
    pub cancel: CancelToken,
}

impl Ctx {
    pub fn new(correlation_id: Ulid) -> Self {
        Self {
            correlation_id,
            cancel: CancelToken::new(),
        }
    }

    /// Contexto raíz con un `correlation_id` fresco.
    pub fn root() -> Self {
        Self::new(Ulid::new())
    }
}
