//! Actor del dominio mayor: dueño único del `MayorState`. Los mensajes entran por `mpsc`;
//! cada delegación aplicada sale por el relay `events` hacia el bus síncrono del
//! composition root.
//!
//! Los eventos emitidos al log son los que el actor ya **aplicó** al estado interno: si la
//! transición falla, no se publica nada — preserva el determinismo del replay (sólo
//! transiciones legales quedan grabadas, mismo patrón que `gt-merge::actor` /
//! `gt-sheriff::actor`).

use tokio::sync::{mpsc, oneshot};

use gt_events::{AppError, Envelope};

use crate::commands::MayorCommand;
use crate::events::MayorEvent;
use crate::repo::MayorRepository;
use crate::state::MayorState;

/// Messages accepted by the mayor actor. `Validate` / `Exec` are the typed Command path
/// (mirrors gt-merge): `Validate` inspects without mutating; `Exec` re-validates, applies,
/// emits each produced event, and mirrors affected delegations to the repo. `Snapshot` is
/// the read port consumed by tests and (eventually) gt-mcp's `gt://mayor/delegations`.
#[derive(Debug)]
pub enum MayorMsg {
    Validate {
        cmd: MayorCommand,
        reply: oneshot::Sender<Result<(), AppError>>,
    },
    Exec {
        cmd: MayorCommand,
        reply: oneshot::Sender<Result<(), AppError>>,
    },
    Snapshot {
        reply: oneshot::Sender<MayorState>,
    },
}

/// Cloneable handle to the mayor actor. Producers and the composition root send messages
/// via [`MayorHandle::exec`] / [`MayorHandle::validate`].
#[derive(Clone)]
pub struct MayorHandle {
    tx: mpsc::Sender<MayorMsg>,
}

impl MayorHandle {
    /// Validate a command without mutating state. Returns the actor's verdict.
    pub async fn validate(&self, cmd: MayorCommand) -> Result<(), AppError> {
        let (reply, rx) = oneshot::channel();
        if self
            .tx
            .send(MayorMsg::Validate { cmd, reply })
            .await
            .is_err()
        {
            return Err(AppError::Other("mayor actor gone".into()));
        }
        rx.await
            .map_err(|_| AppError::Other("mayor actor dropped reply".into()))?
    }

    /// Execute a command: re-validate, apply, emit. Errors propagate from validation.
    pub async fn exec(&self, cmd: MayorCommand) -> Result<(), AppError> {
        let (reply, rx) = oneshot::channel();
        if self.tx.send(MayorMsg::Exec { cmd, reply }).await.is_err() {
            return Err(AppError::Other("mayor actor gone".into()));
        }
        rx.await
            .map_err(|_| AppError::Other("mayor actor dropped reply".into()))?
    }

    /// Snapshot the live `MayorState`. Returns an empty state if the actor is gone.
    pub async fn snapshot(&self) -> MayorState {
        let (reply, rx) = oneshot::channel();
        if self.tx.send(MayorMsg::Snapshot { reply }).await.is_err() {
            return MayorState::default();
        }
        rx.await.unwrap_or_default()
    }
}

/// Spawn the mayor actor with an empty board. Production binaries call [`spawn_hydrated`]
/// with the replay-folded `MayorState`.
pub fn spawn<R: MayorRepository + 'static>(
    repo: R,
    events: mpsc::Sender<Envelope<MayorEvent>>,
) -> MayorHandle {
    spawn_hydrated(repo, events, MayorState::default())
}

/// Spawn the mayor actor with `initial` as the hydrated starting state (boot replay).
pub fn spawn_hydrated<R: MayorRepository + 'static>(
    repo: R,
    events: mpsc::Sender<Envelope<MayorEvent>>,
    initial: MayorState,
) -> MayorHandle {
    let (tx, mut rx) = mpsc::channel::<MayorMsg>(64);
    let handle = MayorHandle { tx };

    tokio::spawn(async move {
        let mut state = initial;
        while let Some(msg) = rx.recv().await {
            match msg {
                MayorMsg::Validate { cmd, reply } => {
                    let _ = reply.send(cmd.validate(&state));
                }
                MayorMsg::Exec { cmd, reply } => {
                    let exec_result = cmd.execute(&state);
                    let outcome = match exec_result {
                        Ok(events_to_emit) => {
                            // Apply each event to the local state first (emit-on-apply
                            // invariant — replay must see only events that actually moved
                            // the in-process board).
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
                                // Mirror touched delegations to the repo (best-effort).
                                for ev in &events_to_emit {
                                    let id = event_target_id(ev);
                                    if let Some(d) = state.delegations.get(id) {
                                        let _ = repo.upsert_delegation(d).await;
                                    }
                                }
                                // Publish to the relay. Best-effort: a closed relay does
                                // not roll the state back — the actor is the source of
                                // truth in memory and the next emit will reach a re-
                                // subscribed root.
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
                MayorMsg::Snapshot { reply } => {
                    let _ = reply.send(state.clone());
                }
            }
        }
    });

    handle
}

/// The delegation id every `MayorEvent` variant carries — used by the repo-mirror step.
fn event_target_id(ev: &MayorEvent) -> &str {
    match ev {
        MayorEvent::Delegated { id, .. }
        | MayorEvent::Acknowledged { id }
        | MayorEvent::Resolved { id, .. }
        | MayorEvent::Withdrawn { id } => id,
    }
}
