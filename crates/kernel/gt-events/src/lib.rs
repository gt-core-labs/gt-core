//! `gt-events` — contrato base de eventos del núcleo.
//!
//! **Síncrono y sin `dyn`/`#[async_trait]`** (ver `apps/api/docs/01-architecture.md` y
//! `03-events.md`). No depende de ningún runtime async: el tiempo y la identidad se
//! generan en el borde y viajan en el [`Envelope`]; el núcleo solo los consume, lo que
//! mantiene la lógica de dominio determinista respecto al stream de eventos.

mod command;
mod ctx;
mod envelope;
mod error;
mod kind;

pub use command::Command;
pub use ctx::{CancelToken, Ctx};
pub use envelope::Envelope;
pub use error::AppError;
pub use kind::EventKind;
