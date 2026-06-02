//! `gt-eventlog` — persistencia de eventos type-erased + **replay determinista** (Paso 3).
//!
//! Es el seguro de vida del proyecto para errores semánticos: como el núcleo es síncrono y
//! puro, el log de eventos se re-corre por la lógica de dominio y reconstruye el estado de
//! forma byte-idéntica (ver `docs/06-observability.md`). Este crate es **sync, sin tokio**:
//! el drenado async del `mpsc` al log vive en los bins.

mod reader;
mod record;
mod replay;
mod store;
mod writer;

pub use reader::{read_all, since, tail};
pub use record::EventRecord;
pub use replay::{replay, replay_dispatch};
pub use store::EventStore;
pub use writer::{
    segment_file_name, workspace_log_dir, JsonlWriter, DEFAULT_EVENTLOG_ROOT, SEGMENT_EXT,
    SEGMENT_PREFIX,
};
