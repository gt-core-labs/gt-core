//! Actor dueño del `SessionRegistry`. El estado vive en UNA task; el resto le habla por
//! `mpsc`. Sin `Arc<Mutex>`: los datos se mueven por canales, no se comparten por referencia.

use tokio::sync::{mpsc, oneshot};

use gt_events::{AppError, Command};

use crate::commands::AgentCommand;
use crate::state::{Session, SessionRegistry, SessionState};

/// Mensajes al actor.
///
/// `Validate`/`Exec` son el camino tipado de [`Command`] (ver `docs/09-llm-integration.md`):
/// `Validate` inspecciona el estado sin mutarlo; `Exec` re-valida y aplica en la misma
/// vuelta del actor — sin `.await` entre validate y execute — para cerrar la ventana
/// TOCTOU. Los clientes externos (`gt-mcp`) entran por aquí; los productores internos
/// pueden seguir usando `Add`/`Remove`/`Transition` por compatibilidad con el Paso 2.
pub enum AgentMsg {
    Add(Session),
    Remove(String),
    Transition {
        id: String,
        to: SessionState,
        reply: oneshot::Sender<Result<(), AppError>>,
    },
    Snapshot(oneshot::Sender<Vec<Session>>),
    Validate {
        cmd: AgentCommand,
        reply: oneshot::Sender<Result<(), AppError>>,
    },
    Exec {
        cmd: AgentCommand,
        reply: oneshot::Sender<Result<(), AppError>>,
    },
}

/// Handle clonable para hablarle al actor.
#[derive(Clone)]
pub struct AgentHandle {
    tx: mpsc::Sender<AgentMsg>,
}

impl AgentHandle {
    pub async fn add(&self, session: Session) {
        let _ = self.tx.send(AgentMsg::Add(session)).await;
    }

    pub async fn remove(&self, id: impl Into<String>) {
        let _ = self.tx.send(AgentMsg::Remove(id.into())).await;
    }

    pub async fn transition(&self, id: impl Into<String>, to: SessionState) -> Result<(), AppError> {
        let (reply, rx) = oneshot::channel();
        self.tx
            .send(AgentMsg::Transition {
                id: id.into(),
                to,
                reply,
            })
            .await
            .map_err(|_| AppError::Other("actor gone".into()))?;
        rx.await.map_err(|_| AppError::Other("actor dropped reply".into()))?
    }

    pub async fn snapshot(&self) -> Vec<Session> {
        let (reply, rx) = oneshot::channel();
        if self.tx.send(AgentMsg::Snapshot(reply)).await.is_err() {
            return Vec::new();
        }
        rx.await.unwrap_or_default()
    }

    /// "Ask without doing": run `validate` against the current state snapshot.
    /// The answer is a snapshot; the actor revalidates on `exec`.
    pub async fn validate(&self, cmd: AgentCommand) -> Result<(), AppError> {
        let (reply, rx) = oneshot::channel();
        self.tx
            .send(AgentMsg::Validate { cmd, reply })
            .await
            .map_err(|_| AppError::Other("actor gone".into()))?;
        rx.await.map_err(|_| AppError::Other("actor dropped reply".into()))?
    }

    /// Apply the command. The actor re-validates inside the same tick, so the result
    /// reflects the state at execution time, not the snapshot used by a prior `validate`.
    pub async fn exec(&self, cmd: AgentCommand) -> Result<(), AppError> {
        let (reply, rx) = oneshot::channel();
        self.tx
            .send(AgentMsg::Exec { cmd, reply })
            .await
            .map_err(|_| AppError::Other("actor gone".into()))?;
        rx.await.map_err(|_| AppError::Other("actor dropped reply".into()))?
    }
}

/// Arranca el actor y devuelve su handle. El mailbox es bounded (contrapresión correcta).
pub fn spawn(buffer: usize) -> AgentHandle {
    spawn_hydrated(buffer, SessionRegistry::default())
}

/// Boot hydration (hq-8iur.1): same as [`spawn`] but seeds the actor with a pre-built
/// [`SessionRegistry`]. The composition root passes the registry reconstructed by
/// `replay_gt` so a restart restores live sessions without re-emitting events. Because the
/// replay reducer IS the live owner type for this domain, no conversion is needed.
pub fn spawn_hydrated(buffer: usize, initial: SessionRegistry) -> AgentHandle {
    let (tx, mut rx) = mpsc::channel::<AgentMsg>(buffer);
    tokio::spawn(async move {
        let mut reg = initial;
        while let Some(msg) = rx.recv().await {
            match msg {
                AgentMsg::Add(s) => reg.add(s),
                AgentMsg::Remove(id) => {
                    reg.remove(&id);
                }
                AgentMsg::Transition { id, to, reply } => {
                    let _ = reply.send(reg.transition(&id, to));
                }
                AgentMsg::Snapshot(reply) => {
                    let _ = reply.send(reg.snapshot());
                }
                AgentMsg::Validate { cmd, reply } => {
                    let _ = reply.send(cmd.validate(&reg));
                }
                AgentMsg::Exec { cmd, reply } => {
                    // execute() re-validates first → no TOCTOU within the actor tick.
                    let _ = reply.send(cmd.execute(&mut reg));
                }
            }
        }
    });
    AgentHandle { tx }
}
