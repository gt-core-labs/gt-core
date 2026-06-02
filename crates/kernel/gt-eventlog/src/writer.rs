use std::fs::OpenOptions;
use std::io::Write;
use std::path::{Path, PathBuf};

use fs2::FileExt;
use time::format_description::well_known::Rfc3339;
use time::OffsetDateTime;

use gt_events::AppError;

use crate::reader;
use crate::record::EventRecord;
use crate::store::EventStore;

/// Volumen por defecto del log de eventos en producción (ver `docs/09-mt-storage-layout.md`).
/// Cada workspace recibe un subdirectorio `<ws>/` bajo esta raíz, con un segmento diario
/// `events-YYYY-MM-DD.jsonl` adentro; el host monta aquí el named volume `gt-eventlog`.
pub const DEFAULT_EVENTLOG_ROOT: &str = "/var/lib/gt-core";

/// Prefijo de cada segmento diario dentro del subdirectorio de un workspace.
pub const SEGMENT_PREFIX: &str = "events-";

/// Extensión de cada segmento diario.
pub const SEGMENT_EXT: &str = ".jsonl";

/// Directorio de log de un workspace bajo `root`: `<root>/<ws>`. Los segmentos diarios viven
/// adentro (`events-YYYY-MM-DD.jsonl`). No toca el filesystem — sólo compone la ruta (la
/// creación perezosa del directorio vive en `JsonlWriter::for_workspace_in`).
pub fn workspace_log_dir(root: impl AsRef<Path>, ws_id: &str) -> PathBuf {
    root.as_ref().join(ws_id)
}

/// Nombre del segmento diario para una fecha `YYYY-MM-DD`: `events-YYYY-MM-DD.jsonl`.
pub fn segment_file_name(date: &str) -> String {
    format!("{SEGMENT_PREFIX}{date}{SEGMENT_EXT}")
}

/// Extrae la fecha UTC `YYYY-MM-DD` del timestamp RFC3339 de un record. La rotación se decide
/// por el **`ts` del propio evento**, no por un reloj inyectado: así el writer queda puro y
/// determinista (el replay re-rutea byte-idéntico a los mismos segmentos).
fn segment_date(ts: &str) -> Result<String, AppError> {
    let dt = OffsetDateTime::parse(ts, &Rfc3339)
        .map_err(|e| AppError::Other(format!("parse record ts {ts:?}: {e}")))?;
    let d = dt.to_offset(time::UtcOffset::UTC).date();
    Ok(format!(
        "{:04}-{:02}-{:02}",
        d.year(),
        d.month() as u8,
        d.day()
    ))
}

/// Destino del writer: un archivo único (modo legacy, `new`) o un directorio de workspace con
/// rotación diaria de segmentos (`for_workspace*`).
enum Target {
    /// Archivo único, append-only. Usado por callers que ya resuelven su propia ruta.
    File(PathBuf),
    /// Directorio `<root>/<ws>`; cada append va al segmento `events-<fecha-del-record>.jsonl`.
    Dir(PathBuf),
}

/// Escritor de `events*.jsonl`: un record por línea, **append-only**, con lock exclusivo de
/// archivo (`fd-lock` estilo) para que escritores concurrentes no entrelacen líneas. El lock es
/// por-archivo; como cada workspace tiene su propio subdirectorio `<ws>/`, los segmentos dan
/// aislamiento de lock por-workspace sin estado compartido entre tenants (`docs/04 §15`,
/// `docs/09`).
///
/// En modo workspace los registros rotan a un segmento por día (`events-YYYY-MM-DD.jsonl`),
/// elegido por el `ts` del evento. La rotación **no** reescribe el log (sigue append-only,
/// `docs/03`): sólo abre un archivo nuevo cuando cambia el día; los segmentos viejos quedan
/// inmutables y el lector los concatena en orden.
pub struct JsonlWriter {
    target: Target,
}

impl JsonlWriter {
    /// Writer de archivo único sobre una ruta ya resuelta (modo legacy, sin rotación).
    pub fn new(path: impl Into<PathBuf>) -> Self {
        Self {
            target: Target::File(path.into()),
        }
    }

    /// Writer rotado para el log de un workspace bajo la raíz de producción
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
        let dir = workspace_log_dir(root, ws_id);
        std::fs::create_dir_all(&dir)
            .map_err(|e| AppError::Other(format!("create workspace log dir {dir:?}: {e}")))?;
        Ok(Self {
            target: Target::Dir(dir),
        })
    }

    /// Ruta base del writer: el archivo (modo legacy) o el directorio del workspace (modo
    /// rotado). En modo rotado los segmentos viven *dentro* de esta ruta.
    pub fn path(&self) -> &Path {
        match &self.target {
            Target::File(p) => p,
            Target::Dir(d) => d,
        }
    }

    /// Segmento concreto al que iría un record dado, según su `ts`. En modo legacy es siempre
    /// el archivo único.
    fn segment_for(&self, record: &EventRecord) -> Result<PathBuf, AppError> {
        match &self.target {
            Target::File(p) => Ok(p.clone()),
            Target::Dir(d) => Ok(d.join(segment_file_name(&segment_date(&record.ts)?))),
        }
    }

    /// Lista los segmentos rotados de un directorio, ordenados cronológicamente (el nombre ISO
    /// `events-YYYY-MM-DD.jsonl` ordena lexicográfico = cronológico).
    fn segments_in(dir: &Path) -> Result<Vec<PathBuf>, AppError> {
        let mut segs: Vec<PathBuf> = std::fs::read_dir(dir)
            .map_err(|e| AppError::Other(format!("read log dir {dir:?}: {e}")))?
            .filter_map(|e| e.ok().map(|e| e.path()))
            .filter(|p| {
                p.file_name()
                    .and_then(|n| n.to_str())
                    .is_some_and(|n| n.starts_with(SEGMENT_PREFIX) && n.ends_with(SEGMENT_EXT))
            })
            .collect();
        segs.sort();
        Ok(segs)
    }
}

/// Append-only de una línea a `path` con lock exclusivo de archivo.
fn append_line(path: &Path, record: &EventRecord) -> Result<(), AppError> {
    let line = serde_json::to_string(record)
        .map_err(|e| AppError::Other(format!("encode record: {e}")))?;
    let mut file = OpenOptions::new()
        .create(true)
        .append(true)
        .open(path)
        .map_err(|e| AppError::Other(format!("open log {path:?}: {e}")))?;
    file.lock_exclusive()
        .map_err(|e| AppError::Other(format!("lock log {path:?}: {e}")))?;
    let res = writeln!(file, "{line}").map_err(|e| AppError::Other(format!("write log: {e}")));
    let _ = FileExt::unlock(&file);
    res
}

impl EventStore for JsonlWriter {
    fn append(&self, record: &EventRecord) -> Result<(), AppError> {
        append_line(&self.segment_for(record)?, record)
    }

    fn read_all(&self) -> Result<Vec<EventRecord>, AppError> {
        match &self.target {
            Target::File(p) => reader::read_all(p),
            Target::Dir(d) => {
                let mut all = Vec::new();
                for seg in Self::segments_in(d)? {
                    all.extend(reader::read_all(&seg)?);
                }
                Ok(all)
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn rec_at(id: &str, ts: &str) -> EventRecord {
        EventRecord {
            event_id: id.to_string(),
            correlation_id: id.to_string(),
            causation_id: None,
            ts: ts.to_string(),
            kind: "test.event.v1".to_string(),
            payload: serde_json::json!({ "n": id }),
        }
    }

    fn rec(id: &str) -> EventRecord {
        rec_at(id, "2026-06-02T00:00:00Z")
    }

    #[test]
    fn workspace_log_dir_partitions_by_ws() {
        assert_eq!(
            workspace_log_dir(DEFAULT_EVENTLOG_ROOT, "default"),
            PathBuf::from("/var/lib/gt-core/default"),
        );
        assert_eq!(
            workspace_log_dir(DEFAULT_EVENTLOG_ROOT, "acme"),
            PathBuf::from("/var/lib/gt-core/acme"),
        );
    }

    #[test]
    fn segment_date_is_utc_day_of_record_ts() {
        assert_eq!(segment_date("2026-06-02T13:45:00Z").unwrap(), "2026-06-02");
        // Offset que cruza medianoche se normaliza a UTC: 2026-06-02T23:00-03:00 == 03T02:00Z.
        assert_eq!(segment_date("2026-06-02T23:00:00-03:00").unwrap(), "2026-06-03");
        assert_eq!(segment_file_name("2026-06-02"), "events-2026-06-02.jsonl");
    }

    #[test]
    fn for_workspace_in_creates_dir_lazily() {
        let root = tempfile::tempdir().unwrap();
        let ws_dir = root.path().join("default");
        assert!(!ws_dir.exists());
        let w = JsonlWriter::for_workspace_in(root.path(), "default").unwrap();
        // mkdir perezoso en la resolución, antes de cualquier append.
        assert!(ws_dir.is_dir());
        assert_eq!(w.path(), ws_dir);
    }

    #[test]
    fn appends_rotate_into_per_day_segments() {
        let root = tempfile::tempdir().unwrap();
        let w = JsonlWriter::for_workspace_in(root.path(), "default").unwrap();
        w.append(&rec_at("a", "2026-06-02T08:00:00Z")).unwrap();
        w.append(&rec_at("b", "2026-06-02T20:00:00Z")).unwrap();
        w.append(&rec_at("c", "2026-06-03T09:00:00Z")).unwrap();

        let dir = root.path().join("default");
        assert!(dir.join("events-2026-06-02.jsonl").is_file());
        assert!(dir.join("events-2026-06-03.jsonl").is_file());
        // Día 2 tiene dos records, día 3 uno.
        assert_eq!(reader::read_all(&dir.join("events-2026-06-02.jsonl")).unwrap().len(), 2);
        assert_eq!(reader::read_all(&dir.join("events-2026-06-03.jsonl")).unwrap().len(), 1);
    }

    #[test]
    fn read_all_concatenates_segments_in_chronological_order() {
        let root = tempfile::tempdir().unwrap();
        let w = JsonlWriter::for_workspace_in(root.path(), "default").unwrap();
        // Escribe fuera de orden de día a propósito; el lector ordena por nombre de segmento.
        w.append(&rec_at("c", "2026-06-03T09:00:00Z")).unwrap();
        w.append(&rec_at("a", "2026-06-02T08:00:00Z")).unwrap();
        w.append(&rec_at("b", "2026-06-02T20:00:00Z")).unwrap();

        let ids: Vec<_> = w.read_all().unwrap().into_iter().map(|r| r.event_id).collect();
        assert_eq!(ids, vec!["a", "b", "c"]);
    }

    #[test]
    fn read_all_empty_dir_is_empty() {
        let root = tempfile::tempdir().unwrap();
        let w = JsonlWriter::for_workspace_in(root.path(), "default").unwrap();
        assert!(w.read_all().unwrap().is_empty());
    }

    #[test]
    fn workspaces_are_isolated() {
        let root = tempfile::tempdir().unwrap();
        let def = JsonlWriter::for_workspace_in(root.path(), "default").unwrap();
        let acme = JsonlWriter::for_workspace_in(root.path(), "acme").unwrap();
        def.append(&rec("d1")).unwrap();
        acme.append(&rec("a1")).unwrap();
        acme.append(&rec("a2")).unwrap();
        assert_eq!(def.read_all().unwrap().len(), 1);
        assert_eq!(acme.read_all().unwrap().len(), 2);
        assert_ne!(def.path(), acme.path());
    }

    #[test]
    fn legacy_single_file_mode_unchanged() {
        let root = tempfile::tempdir().unwrap();
        let path = root.path().join("events.jsonl");
        let w = JsonlWriter::new(&path);
        w.append(&rec("x")).unwrap();
        w.append(&rec("y")).unwrap();
        assert!(path.is_file());
        assert_eq!(w.path(), path);
        assert_eq!(w.read_all().unwrap().len(), 2);
    }
}
