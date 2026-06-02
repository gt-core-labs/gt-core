//! `gt-agent` — primer vertical slice (Paso 2 de `docs/08-getting-started.md`).
//!
//! Valida la receta completa **sin BD**: enum de eventos owned + state machine pura +
//! actor dueño del estado + supervisor de I/O. La parte pura (`events`, `state`) es
//! síncrona y replay-able; la async vive en los bordes (`actor`, `supervisor`).

mod events;
mod repo;
mod state;

pub mod actor;
pub mod commands;
pub mod supervisor;

pub use commands::{AddSession, AgentCommand, RemoveSession, TransitionSession};
pub use events::AgentEvent;
pub use repo::{InMemorySessions, SessionQueries, SessionWriter};
pub use state::{DogKind, Session, SessionRegistry, SessionRole, SessionState};
