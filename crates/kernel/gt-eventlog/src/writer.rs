use std::fs::OpenOptions;
use std::io::Write;
use std::path::{Path, PathBuf};

use fs2::FileExt;

use gt_events::AppError;

use crate::reader;
use crate::record::EventRecord;
use crate::store::EventStore;

/// Volumen por defecto del log de eventos en producción (ver `docs/09-mt-storage-layout.md`).
/// Cada workspace recibe un subdirectorio `<ws>/events.jsonl` bajo esta raíz; el host monta
/// aquí el named volume `gt-eventlog`.
pub const DEFAULT_EVENTLOG_ROOT: &str = "/var/lib/gt-core";

/// Nombre del archivo de log dentro del subdirectorio de cada workspace.
pub const EVENTLOG_FILE: &str = "events.jsonl";

/// Resuelve la ruta del log de un workspace bajo `root`: `<root>/<ws>/events.jsonl`.
/// No toca el filesystem — sólo compone la ruta (la creación perezosa del directorio vive en
/// `JsonlWriter::for_workspace_in`).
pub fn workspace_log_path(root: impl AsRef<Path>, ws_id: &str) -> PathBuf {
    root.as_ref().join(ws_id).join(EVENTLOG_FILE)
}

/// Escritor de `.events.jsonl`: un record por línea, **append-only**, con lock exclusivo de
/// archivo (`fd-lock` estilo) para que escritores concurrentes no entrelacen líneas. El lock es
/// por-archivo, así que la partición `<ws>/events.jsonl` da aislamiento de lock por-workspace
/// sin estado compartido entre tenants (`docs/04 §15`, `docs/09`).
pub struct JsonlWriter {
    path: PathBuf,
}

impl JsonlWriter {
    pub fn new(path: impl Into<PathBuf>) -> Self {
        Self { path: path.into() }
    }

    /// Escritor para el log de un workspace bajo la raíz de producción
    /// (`DEFAULT_EVENTLOG_ROOT`). Crea el subdirectorio del workspace de forma perezosa e
    /// idempotente (`mkdir -p <root>/<ws>/`) en la primera resolución, igual que
    /// `gt_create_workspace_schema` aprovisiona un schema de PG bajo demanda (`docs/09`).
    pub fn for_workspace(ws_id: &str) -> Result<Self, AppError> {
        Self::for_workspace_in(DEFAULT_EVENTLOG_ROOT, ws_id)
    }

    /// Variante con raíz explícita: aísla la partición del workspace bajo `root`. Útil para
    /// tests (tempdir) y para hosts que montan el volumen en otra ruta. Crea `<root>/<ws>/`
    /// de forma perezosa e idempotente.
    pub fn for_workspace_in(root: impl AsRef<Path>, ws_id: &str) -> Result<Self, AppError> {
        let dir = root.as_ref().join(ws_id);
        std::fs::create_dir_all(&dir)
            .map_err(|e| AppError::Other(format!("create workspace log dir {dir:?}: {e}")))?;
        Ok(Self {
            path: dir.join(EVENTLOG_FILE),
        })
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

#[cfg(test)]
mod tests {
    use super::*;

    fn rec(id: &str) -> EventRecord {
        EventRecord {
            event_id: id.to_string(),
            correlation_id: id.to_string(),
            causation_id: None,
            ts: "2026-06-02T00:00:00Z".to_string(),
            kind: "test.event.v1".to_string(),
            payload: serde_json::json!({"n": id}),
        }
    }

    #[test]
    fn workspace_log_path_partitions_by_ws() {
        assert_eq!(
            workspace_log_path(DEFAULT_EVENTLOG_ROOT, "default"),
            PathBuf::from("/var/lib/gt-core/default/events.jsonl"),
        );
        assert_eq!(
            workspace_log_path(DEFAULT_EVENTLOG_ROOT, "acme"),
            PathBuf::from("/var/lib/gt-core/acme/events.jsonl"),
        );
    }

    #[test]
    fn for_workspace_in_creates_dir_lazily_and_appends() {
        let root = tempfile::tempdir().unwrap();
        // El subdirectorio del workspace no existe todavía.
        let ws_dir = root.path().join("default");
        assert!(!ws_dir.exists());

        let w = JsonlWriter::for_workspace_in(root.path(), "default").unwrap();
        // mkdir perezoso en la resolución, antes de cualquier append.
        assert!(ws_dir.is_dir());
        assert_eq!(w.path(), workspace_log_path(root.path(), "default"));

        w.append(&rec("a")).unwrap();
        w.append(&rec("b")).unwrap();
        let got = w.read_all().unwrap();
        assert_eq!(got.len(), 2);
        assert_eq!(got[0].event_id, "a");
        assert_eq!(got[1].event_id, "b");
    }

    #[test]
    fn for_workspace_in_is_idempotent() {
        let root = tempfile::tempdir().unwrap();
        let a = JsonlWriter::for_workspace_in(root.path(), "default").unwrap();
        a.append(&rec("first")).unwrap();
        // Re-resolver el mismo workspace no borra ni recrea el log (append-only).
        let b = JsonlWriter::for_workspace_in(root.path(), "default").unwrap();
        b.append(&rec("second")).unwrap();
        assert_eq!(a.path(), b.path());
        assert_eq!(b.read_all().unwrap().len(), 2);
    }

    #[test]
    fn workspaces_are_isolated() {
        let root = tempfile::tempdir().unwrap();
        let def = JsonlWriter::for_workspace_in(root.path(), "default").unwrap();
        let acme = JsonlWriter::for_workspace_in(root.path(), "acme").unwrap();
        def.append(&rec("d1")).unwrap();
        acme.append(&rec("a1")).unwrap();
        acme.append(&rec("a2")).unwrap();
        // Cada partición ve sólo sus propios records.
        assert_eq!(def.read_all().unwrap().len(), 1);
        assert_eq!(acme.read_all().unwrap().len(), 2);
        assert_ne!(def.path(), acme.path());
    }
}
