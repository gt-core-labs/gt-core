//! `gt-bus` — bus de eventos in-process, **fan-out síncrono** y genérico sobre `E: EventKind`.
//!
//! Como el Go original: la lógica que reacciona corre en la misma llamada que publica.
//! Cero async, cero broker. Los handlers que necesitan I/O empujan a un canal vía
//! [`relay`] y una task long-lived hace el trabajo (no en este crate de la espina).
//!
//! El [`deadletter`] es parte del diseño, no un extra: eventos sin suscriptor y handlers
//! que devuelven `Err` se capturan en vez de perderse.

mod bus;
mod deadletter;
mod relay;

pub use bus::Bus;
pub use deadletter::{DeadLetter, DeadLetterEntry};
pub use relay::Relay;
