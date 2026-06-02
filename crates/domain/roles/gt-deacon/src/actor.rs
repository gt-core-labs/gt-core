//! Actor del dominio deacon: dueño único del `DeaconState`. Mensajes entran por `mpsc`,
//! las observaciones salen por el relay `events` hacia el bus síncrono del composition
//! root. Mismo patrón emit-on-apply que `gt-merge::actor` / `gt-sheriff::actor`: el actor
//! aplica el evento al estado interno antes de publicarlo, así replay solo ve transiciones
//! legales y queda byte-idéntico.

use tokio::sync::{mpsc, oneshot};

use gt_events::{AppError, Envelope};

use crate::commands::DeaconCommand;
use crate::events::DeaconEvent;
use crate::repo::DeaconRepository;
use crate::state::DeaconState;

/// Messages accepted by the deacon actor. `Validate`/`Exec` are the typed Command path;
/// `Snapshot` is the read port consumed by tests and (future) `gt://deacon/drain`.
#[derive(Debug)]
pub enum DeaconMsg {
    Validate {
        cmd: DeaconCommand,
        reply: oneshot::Sender<Result<(), AppError>>,
    },
    Exec {
        cmd: DeaconCommand,
        reply: oneshot::Sender<Result<(), AppError>>,
    },
    Snapshot {
        reply: oneshot::Sender<DeaconState>,
    },
}

/// Cloneable handle to the deacon actor.
#[derive(Clone)]
pub struct DeaconHandle {
    tx: mpsc::Sender<DeaconMsg>,
}

impl DeaconHandle {
    /// Validate a command without mutating state.
    pub async fn validate(&self, cmd: DeaconCommand) -> Result<(), AppError> {
        let (reply, rx) = oneshot::channel();
        if self
            .tx
            .send(DeaconMsg::Validate { cmd, reply })
            .await
            .is_err()
        {
            return Err(AppError::Other("deacon actor gone".into()));
        }
        rx.await
            .map_err(|_| AppError::Other("deacon actor dropped reply".into()))?
    }

    /// Execute a command: re-validate, apply, emit, mirror to repo.
    pub async fn exec(&self, cmd: DeaconCommand) -> Result<(), AppError> {
        let (reply, rx) = oneshot::channel();
        if self.tx.send(DeaconMsg::Exec { cmd, reply }).await.is_err() {
            return Err(AppError::Other("deacon actor gone".into()));
        }
        rx.await
            .map_err(|_| AppError::Other("deacon actor dropped reply".into()))?
    }

    /// Snapshot the live `DeaconState`. Returns an empty state if the actor is gone.
    pub async fn snapshot(&self) -> DeaconState {
        let (reply, rx) = oneshot::channel();
        if self.tx.send(DeaconMsg::Snapshot { reply }).await.is_err() {
            return DeaconState::default();
        }
        rx.await.unwrap_or_default()
    }
}

/// Spawn the deacon actor with an empty state. Production binaries call
/// [`spawn_hydrated`] with the replay-folded `DeaconState`.
pub fn spawn<R: DeaconRepository + 'static>(
    repo: R,
    events: mpsc::Sender<Envelope<DeaconEvent>>,
) -> DeaconHandle {
    spawn_hydrated(repo, events, DeaconState::default())
}

/// Spawn the deacon actor with `initial` as the hydrated starting state (boot replay).
pub fn spawn_hydrated<R: DeaconRepository + 'static>(
    repo: R,
    events: mpsc::Sender<Envelope<DeaconEvent>>,
    initial: DeaconState,
) -> DeaconHandle {
    let (tx, mut rx) = mpsc::channel::<DeaconMsg>(64);
    let handle = DeaconHandle { tx };

    tokio::spawn(async move {
        let mut state = initial;
        while let Some(msg) = rx.recv().await {
            match msg {
                DeaconMsg::Validate { cmd, reply } => {
                    let _ = reply.send(cmd.validate(&state));
                }
                DeaconMsg::Exec { cmd, reply } => {
                    let exec_result = cmd.execute(&state);
                    let outcome = match exec_result {
                        Ok(events_to_emit) => {
                            let mut apply_err: Option<AppError> = None;
                            for ev in &events_to_emit {
                                if let Err(e) = state.apply(ev) {
                                    apply_err = Some(e);
                                    break;
                                }
                            }
                            if let Some(e) = apply_err {
                                Err(e)
                            } else {
                                // Mirror affected items to the repo (best-effort).
                                for ev in &events_to_emit {
                                    match ev {
                                        DeaconEvent::ItemBegun { id, .. } => {
                                            if let Some(item) = state.pending.get(id) {
                                                let _ = repo.upsert_item(item).await;
                                            }
                                        }
                                        DeaconEvent::ItemFinished { id } => {
                                            let _ = repo.remove_item(id).await;
                                        }
                                        DeaconEvent::DrainRequested { .. }
                                        | DeaconEvent::DrainComplete { .. } => {
                                            // No per-item repo write — the drain flag lives
                                            // on the actor snapshot, not the per-item table.
                                        }
                                    }
                                }
                                // Publish to the relay (best-effort, mirrors gt-sheriff).
                                for ev in events_to_emit {
                                    let _ = events.send(Envelope::root(ev)).await;
                                }
                                Ok(())
                            }
                        }
                        Err(e) => Err(e),
                    };
                    let _ = reply.send(outcome);
                }
                DeaconMsg::Snapshot { reply } => {
                    let _ = reply.send(state.clone());
                }
            }
        }
    });

    handle
}
