//! Supervisor — PRODUCER de I/O (async). Spawnea el proceso del polecat, vigila su
//! heartbeat por `mtime`, y al morir (exit o heartbeat stale) emite `SessionEnd`.
//!
//! No habla con el bus directamente: el `Bus` es síncrono y !Send. El supervisor empuja
//! `Envelope<AgentEvent>` a un `mpsc` (el relay); la task dueña del bus (o el test) lo
//! drena y hace `bus.publish` en su hilo. Es el patrón async-borde → canal → bus-sync.

use std::path::{Path, PathBuf};
use std::time::{Duration, SystemTime};

use tokio::sync::mpsc;

use gt_events::Envelope;

use crate::events::AgentEvent;

/// Un polecat fake en ejecución (proceso real, p. ej. `sleep`).
pub struct SpawnedPolecat {
    pub session: String,
    pub heartbeat: PathBuf,
    child: tokio::process::Child,
}

impl SpawnedPolecat {
    /// Mata el proceso (best-effort).
    pub async fn kill(&mut self) {
        let _ = self.child.start_kill();
    }
}

/// Spawnea un polecat fake: escribe heartbeat inicial y lanza `sleep <secs>`.
pub async fn spawn_fake(
    session: impl Into<String>,
    secs: f64,
    heartbeat: PathBuf,
) -> std::io::Result<SpawnedPolecat> {
    let session = session.into();
    touch_heartbeat(&heartbeat).await?;
    let child = tokio::process::Command::new("sleep")
        .arg(format!("{secs}"))
        .spawn()?;
    Ok(SpawnedPolecat {
        session,
        heartbeat,
        child,
    })
}

/// Reescribe el heartbeat (actualiza su `mtime`). Lo llamaría el polecat vivo cada tick.
pub async fn touch_heartbeat(path: &Path) -> std::io::Result<()> {
    tokio::fs::write(path, b"alive").await
}

/// ¿El heartbeat está stale? Compara `mtime` contra ahora. Síncrono y puro respecto al fs.
pub fn heartbeat_is_stale(path: &Path, max_age: Duration) -> bool {
    let Ok(meta) = std::fs::metadata(path) else {
        return true; // sin heartbeat = stale
    };
    let Ok(modified) = meta.modified() else {
        return true;
    };
    SystemTime::now()
        .duration_since(modified)
        .map(|age| age > max_age)
        .unwrap_or(false)
}

/// Vigila el polecat hasta que muere y emite `SessionEnd`.
///
/// Termina por una de dos causas (visibility-timeout estilo SQS, ver `docs/05-queues.md`):
/// - el proceso sale (exit normal o `kill`) → `child.wait()` retorna;
/// - el heartbeat queda stale más de `stale_after` → lo mata y emite igual.
pub async fn supervise(
    mut p: SpawnedPolecat,
    events: mpsc::Sender<Envelope<AgentEvent>>,
    stale_after: Duration,
) {
    let mut tick = tokio::time::interval(Duration::from_millis(50));
    loop {
        tokio::select! {
            _ = p.child.wait() => break,
            _ = tick.tick() => {
                if heartbeat_is_stale(&p.heartbeat, stale_after) {
                    let _ = p.child.start_kill();
                    break;
                }
            }
        }
    }
    let _ = tokio::fs::remove_file(&p.heartbeat).await;
    let end = Envelope::root(AgentEvent::SessionEnd { session: p.session });
    let _ = events.send(end).await;
}
