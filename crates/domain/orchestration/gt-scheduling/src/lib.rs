//! `gt-scheduling` — primer flujo orquestado real (Paso 5 de `docs/08-getting-started.md`).
//!
//! El bead **es** el job; la cola es una vista sobre beads `pending`. El **dispatcher es un
//! actor único serializado** que posee el dequeue: desencola por prioridad, reclama por CAS
//! (`BeadRepository::cas_claim`), respeta capacidad (`CapacityGovernor`) y emite eventos.
//! No llama a otro dominio: integra por eventos (ver `docs/05-queues.md`).

pub mod actor;
pub mod commands;
pub mod expectations;
mod events;
mod state;

pub use commands::{Enqueue, MarkDispatched, SchedCommand};
pub use events::SchedEvent;
pub use state::{CapacityGovernor, Queue, SchedCore, SchedState};
