//! Actor del dominio witness: dueño único del `WitnessState`. Mensajes entran por `mpsc`,
//! las observaciones salen por el relay `events` hacia el bus síncrono del composition root.
//!
//! Los eventos emitidos al log son los que el actor ya **aplicó** al estado interno: si la
//! transición falla, no se publica nada — preserva determinismo del replay (sólo
//! transiciones legales quedan grabadas, mismo patrón que `gt-sheriff::actor`).

use tokio::sync::{mpsc, oneshot};

use gt_events::{AppError, Envelope};

use crate::commands::WitnessCommand;
use crate::events::WitnessEvent;
use crate::repo::WitnessRepository;
use crate::state::WitnessState;

/// Messages accepted by the witness actor. `Validate`/`Exec` are the typed Command path
/// (mirrors gt-sheriff): `Validate` inspects without mutating; `Exec` re-validates,
/// applies, emits each produced event, and mirrors affected targets to the repo.
/// `Snapshot` is the read port consumed by gt-mcp resources (once wired) and by tests.
#[derive(Debug)]
pub enum WitnessMsg {
    Validate {
        cmd: WitnessCommand,
        reply: oneshot::Sender<Result<(), AppError>>,
    },
    Exec {
        cmd: WitnessCommand,
        reply: oneshot::Sender<Result<(), AppError>>,
    },
    Snapshot {
        reply: oneshot::Sender<WitnessState>,
    },
}

/// Cloneable handle to the witness actor. Producers and the composition root send
/// messages via [`WitnessHandle::exec`] / [`WitnessHandle::validate`].
#[derive(Clone)]
pub struct WitnessHandle {
    tx: mpsc::Sender<WitnessMsg>,
}

impl WitnessHandle {
    /// Validate a command without mutating state. Returns the actor's verdict.
    pub async fn validate(&self, cmd: WitnessCommand) -> Result<(), AppError> {
        let (reply, rx) = oneshot::channel();
        if self
            .tx
            .send(WitnessMsg::Validate { cmd, reply })
            .await
            .is_err()
        {
            return Err(AppError::Other("witness actor gone".into()));
        }
        rx.await
            .map_err(|_| AppError::Other("witness actor dropped reply".into()))?
    }

    /// Execute a command: re-validate, apply, emit. Errors propagate from validation; a
    /// command that produces zero events (e.g. `Tick` against an already-escalated target)
    /// returns `Ok(())` with no log traffic.
    pub async fn exec(&self, cmd: WitnessCommand) -> Result<(), AppError> {
        let (reply, rx) = oneshot::channel();
        if self
            .tx
            .send(WitnessMsg::Exec { cmd, reply })
            .await
            .is_err()
        {
            return Err(AppError::Other("witness actor gone".into()));
        }
        rx.await
            .map_err(|_| AppError::Other("witness actor dropped reply".into()))?
    }

    /// Snapshot the live `WitnessState`. Returns an empty state if the actor is gone.
    pub async fn snapshot(&self) -> WitnessState {
        let (reply, rx) = oneshot::channel();
        if self.tx.send(WitnessMsg::Snapshot { reply }).await.is_err() {
            return WitnessState::default();
        }
        rx.await.unwrap_or_default()
    }
}

/// Spawn the witness actor with an empty registry. Production binaries call
/// [`spawn_hydrated`] with the replay-folded `WitnessState`.
pub fn spawn<R: WitnessRepository + 'static>(
    repo: R,
    events: mpsc::Sender<Envelope<WitnessEvent>>,
) -> WitnessHandle {
    spawn_hydrated(repo, events, WitnessState::default())
}

/// Spawn the witness actor with `initial` as the hydrated starting state (boot replay).
pub fn spawn_hydrated<R: WitnessRepository + 'static>(
    repo: R,
    events: mpsc::Sender<Envelope<WitnessEvent>>,
    initial: WitnessState,
) -> WitnessHandle {
    let (tx, mut rx) = mpsc::channel::<WitnessMsg>(64);
    let handle = WitnessHandle { tx };

    tokio::spawn(async move {
        let mut state = initial;
        while let Some(msg) = rx.recv().await {
            match msg {
                WitnessMsg::Validate { cmd, reply } => {
                    let _ = reply.send(cmd.validate(&state));
                }
                WitnessMsg::Exec { cmd, reply } => {
                    let exec_result = cmd.execute(&state);
                    let outcome = match exec_result {
                        Ok(events_to_emit) => {
                            // Apply each event to the local state first (emit-on-apply
                            // invariant — replay must see only events that actually moved
                            // the in-process registry).
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
                                // Mirror touched targets to the repo (best-effort).
                                for ev in &events_to_emit {
                                    if let Some(worker) = event_target_worker(ev) {
                                        if let Some(t) = state.targets.get(worker) {
                                            let _ = repo.upsert_target(t).await;
                                        }
                                    }
                                }
                                // Publish to the relay. Best-effort: a closed relay does
                                // not roll the state back — the actor is the source of
                                // truth in memory and the next emit will reach a
                                // re-subscribed root.
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
                WitnessMsg::Snapshot { reply } => {
                    let _ = reply.send(state.clone());
                }
            }
        }
    });

    handle
}

/// The worker id every `WitnessEvent` variant carries — used by the repo-mirror step.
fn event_target_worker(ev: &WitnessEvent) -> Option<&str> {
    match ev {
        WitnessEvent::TargetWatched { worker, .. }
        | WitnessEvent::TargetStuck { worker, .. }
        | WitnessEvent::EscalationRaised { worker, .. }
        | WitnessEvent::TargetCleared { worker } => Some(worker),
    }
}
