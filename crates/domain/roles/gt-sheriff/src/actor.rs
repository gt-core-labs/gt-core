//! Actor del dominio sheriff: dueño único del `SheriffState`. Mensajes entran por `mpsc`,
//! las observaciones salen por el relay `events` hacia el bus síncrono del composition root.
//!
//! Los eventos emitidos al log son los que el actor ya **aplicó** al estado interno: si la
//! transición falla, no se publica nada — preserva determinismo del replay (sólo
//! transiciones legales quedan grabadas, mismo patrón que `gt-merge::actor`).

use tokio::sync::{mpsc, oneshot};

use gt_events::{AppError, Envelope};

use crate::commands::SheriffCommand;
use crate::events::SheriffEvent;
use crate::repo::SheriffRepository;
use crate::state::SheriffState;

/// Messages accepted by the sheriff actor. `Validate`/`Exec` are the typed Command path
/// (mirrors gt-merge): `Validate` inspects without mutating; `Exec` re-validates, applies,
/// emits each produced event, and mirrors affected watches to the repo. `Snapshot` is the
/// read port consumed by gt-mcp's `gt://sheriff/watches` (once wired) and by tests.
#[derive(Debug)]
pub enum SheriffMsg {
    Validate {
        cmd: SheriffCommand,
        reply: oneshot::Sender<Result<(), AppError>>,
    },
    Exec {
        cmd: SheriffCommand,
        reply: oneshot::Sender<Result<(), AppError>>,
    },
    Snapshot {
        reply: oneshot::Sender<SheriffState>,
    },
}

/// Cloneable handle to the sheriff actor. Producers and the composition root send messages
/// via [`SheriffHandle::exec`] / [`SheriffHandle::validate`].
#[derive(Clone)]
pub struct SheriffHandle {
    tx: mpsc::Sender<SheriffMsg>,
}

impl SheriffHandle {
    /// Validate a command without mutating state. Returns the actor's verdict.
    pub async fn validate(&self, cmd: SheriffCommand) -> Result<(), AppError> {
        let (reply, rx) = oneshot::channel();
        if self
            .tx
            .send(SheriffMsg::Validate { cmd, reply })
            .await
            .is_err()
        {
            return Err(AppError::Other("sheriff actor gone".into()));
        }
        rx.await
            .map_err(|_| AppError::Other("sheriff actor dropped reply".into()))?
    }

    /// Execute a command: re-validate, apply, emit. Errors propagate from validation; a
    /// command that produces zero events (e.g. `Observe` for an unwatched pattern) returns
    /// `Ok(())` with no log traffic.
    pub async fn exec(&self, cmd: SheriffCommand) -> Result<(), AppError> {
        let (reply, rx) = oneshot::channel();
        if self
            .tx
            .send(SheriffMsg::Exec { cmd, reply })
            .await
            .is_err()
        {
            return Err(AppError::Other("sheriff actor gone".into()));
        }
        rx.await
            .map_err(|_| AppError::Other("sheriff actor dropped reply".into()))?
    }

    /// Snapshot the live `SheriffState`. Returns an empty state if the actor is gone.
    pub async fn snapshot(&self) -> SheriffState {
        let (reply, rx) = oneshot::channel();
        if self.tx.send(SheriffMsg::Snapshot { reply }).await.is_err() {
            return SheriffState::default();
        }
        rx.await.unwrap_or_default()
    }
}

/// Spawn the sheriff actor with an empty board. Production binaries call
/// [`spawn_hydrated`] with the replay-folded `SheriffState`.
pub fn spawn<R: SheriffRepository + 'static>(
    repo: R,
    events: mpsc::Sender<Envelope<SheriffEvent>>,
) -> SheriffHandle {
    spawn_hydrated(repo, events, SheriffState::default())
}

/// Spawn the sheriff actor with `initial` as the hydrated starting state (boot replay).
pub fn spawn_hydrated<R: SheriffRepository + 'static>(
    repo: R,
    events: mpsc::Sender<Envelope<SheriffEvent>>,
    initial: SheriffState,
) -> SheriffHandle {
    let (tx, mut rx) = mpsc::channel::<SheriffMsg>(64);
    let handle = SheriffHandle { tx };

    tokio::spawn(async move {
        let mut state = initial;
        while let Some(msg) = rx.recv().await {
            match msg {
                SheriffMsg::Validate { cmd, reply } => {
                    let _ = reply.send(cmd.validate(&state));
                }
                SheriffMsg::Exec { cmd, reply } => {
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
                                // Mirror touched watches to the repo (best-effort).
                                for ev in &events_to_emit {
                                    if let Some(id) = event_target_id(ev) {
                                        if let Some(w) = state.watches.get(id) {
                                            let _ = repo.upsert_watch(w).await;
                                        }
                                    }
                                }
                                // Publish to the relay. Best-effort: a closed relay does not
                                // roll the state back — the actor is the source of truth in
                                // memory and the next emit will reach a re-subscribed root.
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
                SheriffMsg::Snapshot { reply } => {
                    let _ = reply.send(state.clone());
                }
            }
        }
    });

    handle
}

/// The watch id every `SheriffEvent` variant carries — used by the repo-mirror step.
fn event_target_id(ev: &SheriffEvent) -> Option<&str> {
    match ev {
        SheriffEvent::Registered { id, .. }
        | SheriffEvent::Observed { id, .. }
        | SheriffEvent::Raised { id, .. }
        | SheriffEvent::Cleared { id } => Some(id),
    }
}
