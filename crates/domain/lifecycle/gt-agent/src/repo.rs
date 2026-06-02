use std::future::Future;
use std::sync::Mutex;

use gt_events::AppError;

use crate::state::{Session, SessionState};

/// Puerto de consulta del dominio (lado lectura). Async porque modela almacenamiento
/// (en Paso 4 lo implementa `gt-store-dolt`); aquí basta una implementación in-memory.
/// Se usa por genéricos, no por `dyn` (ver `docs/01-architecture.md`). Devuelve
/// `impl Future + Send` (RPITIT) — alineado con `gt-beads::BeadRepository` — para que el
/// puerto sea utilizable desde tasks de tokio (incluyendo axum, que exige `Send`).
pub trait SessionQueries: Send + Sync {
    fn active_sessions(&self) -> impl Future<Output = Result<Vec<Session>, AppError>> + Send;
}

/// Write-side port for the sessions table (hq-8iur.2). The Rust sessions projector mirrors
/// `AgentEvent` lifecycle transitions into the canonical store so the read-side
/// (`SessionQueries`) owns the truth — replacing the Go `gt sling` writer during cutover.
/// Both methods are idempotent (`Spawned` re-upserts, a terminal event for an unseen
/// session is a no-op), because the broadcast delivers at-least-once.
pub trait SessionWriter: Send + Sync {
    /// Full-row upsert (used for `Spawned`, where rig/role/crew are known).
    fn upsert(&self, session: &Session) -> impl Future<Output = Result<(), AppError>> + Send;
    /// State-only update by id (used for `SessionEnd`/`Killed`).
    fn set_state(
        &self,
        id: &str,
        state: SessionState,
    ) -> impl Future<Output = Result<(), AppError>> + Send;
}

/// Implementación in-memory (sin Dolt). El mismo test del slice debe pasar luego contra el
/// adaptador real: si uno falla y el otro no, el puerto está mal definido (gate del Paso 4).
#[derive(Default)]
pub struct InMemorySessions {
    sessions: Mutex<Vec<Session>>,
}

impl InMemorySessions {
    pub fn new(sessions: Vec<Session>) -> Self {
        Self {
            sessions: Mutex::new(sessions),
        }
    }
}

impl SessionQueries for InMemorySessions {
    fn active_sessions(&self) -> impl Future<Output = Result<Vec<Session>, AppError>> + Send {
        let rows: Vec<Session> = {
            let all = self.sessions.lock().unwrap();
            all.iter().filter(|s| !s.is_terminal()).cloned().collect()
        };
        async move { Ok(rows) }
    }
}

impl SessionWriter for InMemorySessions {
    fn upsert(&self, session: &Session) -> impl Future<Output = Result<(), AppError>> + Send {
        {
            let mut all = self.sessions.lock().unwrap();
            match all.iter_mut().find(|s| s.id == session.id) {
                Some(existing) => *existing = session.clone(),
                None => all.push(session.clone()),
            }
        }
        async move { Ok(()) }
    }

    fn set_state(
        &self,
        id: &str,
        state: SessionState,
    ) -> impl Future<Output = Result<(), AppError>> + Send {
        {
            let mut all = self.sessions.lock().unwrap();
            if let Some(s) = all.iter_mut().find(|s| s.id == id) {
                s.state = state;
            }
        }
        async move { Ok(()) }
    }
}
