//! `gt-merge` — dominio del merge slot (Paso 6.b de `docs/08-getting-started.md`).
//!
//! Introduce `gt-channel` al flujo: la **refinery** observa el mailbox de archivo
//! `MERGE_READY` (un polecat refinador acabó su trabajo y pide merge), traduce cada
//! mensaje a un `Submit` para el actor, y acka. El actor es el dueño único del
//! `MergeBoard`; pasa cada slot por `Ready → Merging → Merged | Failed`. La I/O del merge
//! real (correr `git merge`, abrir el PR) vive en el borde del composition root: emite
//! `Complete`/`Fail` al actor cuando el efecto está listo, igual que el patrón del Paso 6.a.
//!
//! Aislamiento: este crate depende solo del kernel (`gt-events`, `gt-channel`). No conoce
//! `gt-scheduling` ni `gt-beads`: la integración cross-dominio (marcar el bead como Done
//! tras Merged, liberar capacidad) es trabajo del composition root vía eventos.

pub mod actor;
pub mod commands;
#[cfg(feature = "axum")]
pub mod http;
pub mod module;
pub mod refinery;
mod events;
mod repo;
mod state;

pub use commands::{CompleteMerge, FailMerge, MergeCommand, StartMerge, SubmitMerge};
pub use events::{MergeEvent, MergeReadyPayload};
pub use module::MergeModule;
pub use repo::{InMemoryMergeRepo, MergeRepository};
pub use state::{BoardSnapshot, MergeBoard, MergeSlot, MergeSlotState, MergeState};

// The off-by-default `axum` REST adapter (`hq-fe-api-orch.2`): re-export the surface the
// composition root wires (the per-workspace provider, its object-safe repo mirror, and the REST
// state) so the binary can build the HTTP-enabled module without naming the `http` module path.
#[cfg(feature = "axum")]
pub use http::{merge_router, DynMergeRepository, MergeApiState, WorkspaceMerges};
#[cfg(feature = "axum")]
pub use module::MergeHttpModule;
