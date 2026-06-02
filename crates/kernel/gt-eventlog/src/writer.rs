use std::fs::OpenOptions;
use std::io::Write;
use std::path::{Path, PathBuf};

use fs2::FileExt;

use gt_events::AppError;

use crate::reader;
use crate::record::EventRecord;
use crate::store::EventStore;

/// Escritor de `.events.jsonl`: un record por línea, **append-only**, con lock exclusivo de
/// archivo (`fd-lock` estilo) para que escritores concurrentes no entrelacen líneas.
pub struct JsonlWriter {
    path: PathBuf,
}

impl JsonlWriter {
    pub fn new(path: impl Into<PathBuf>) -> Self {
        Self { path: path.into() }
    }

    pub fn path(&self) -> &Path {
        &self.path
    }
}

impl EventStore for JsonlWriter {
    fn append(&self, record: &EventRecord) -> Result<(), AppError> {
        let line = serde_json::to_string(record)
            .map_err(|e| AppError::Other(format!("encode record: {e}")))?;
        let mut file = OpenOptions::new()
            .create(true)
            .append(true)
            .open(&self.path)
            .map_err(|e| AppError::Other(format!("open log: {e}")))?;
        file.lock_exclusive()
            .map_err(|e| AppError::Other(format!("lock log: {e}")))?;
        let res = writeln!(file, "{line}").map_err(|e| AppError::Other(format!("write log: {e}")));
        let _ = FileExt::unlock(&file);
        res
    }

    fn read_all(&self) -> Result<Vec<EventRecord>, AppError> {
        reader::read_all(&self.path)
    }
}
