//! `gt-feed` — Paso 6.g. CONSUMIDOR PURO del log de eventos (`gt-eventlog`).
//!
//! El feed es el lado "visor" del CQRS: lee `EventRecord` type-erased, los pliega en una
//! `FeedState` agrupada por `correlation_id`, y deriva `FeedProblem` (huecos semánticos).
//!
//! Reglas (`docs/01-architecture.md` + `docs/06-observability.md`):
//!
//! - Solo depende del kernel (`gt-eventlog`, `gt-events`); **no conoce dominios**. Toda la
//!   lógica trabaja con `kind: &str` y `payload: serde_json::Value`.
//! - El reducer (`Curator::apply`) es **sync, puro y determinista**: sin reloj, sin random,
//!   sin I/O. Replay re-corre el mismo log → misma `FeedState`, byte a byte.
//! - Append-only sobre el estado interno: cada `EventRecord` se pliega, nada se sobre-escribe.
//! - El dead-letter del bus es in-memory (ver `gt-bus::DeadLetter`) y **no se persiste al
//!   log**. Para no acoplar `gt-feed` a `gt-bus`, el dead-letter entra por un tipo local
//!   [`DeadEntry`] que el bin traduce desde su `DeadLetterEntry<GtEvent>` real.

pub mod activity;
mod curator;
mod dead;
pub mod escalation;
pub mod module;
mod problems;
mod state;
pub mod view;

pub use activity::{activity_view, ActivityInfo, ActivityRow};
pub use curator::Curator;
pub use module::FeedModule;
pub use dead::DeadEntry;
pub use escalation::{intents as escalation_intents, EscalationIntent};
pub use problems::{detect, FeedProblem};
pub use state::{Correlation, FeedState};
