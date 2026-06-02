//! Actor del dominio refinery: dueño único del `RefineryState`. Mismo patrón emit-on-apply
//! que `gt-merge::actor` / `gt-sheriff::actor` / `gt-deacon::actor`.

use tokio::sync::{mpsc, oneshot};

use gt_events::{AppError, Envelope};

use crate::commands::RefineryCommand;
use crate::events::RefineryEvent;
use crate::repo::RefineryRepository;
use crate::state::RefineryState;

#[derive(Debug)]
pub enum RefineryMsg {
    Validate {
        cmd: RefineryCommand,
        reply: oneshot::Sender<Result<(), AppError>>,
    },
    Exec {
        cmd: RefineryCommand,
        reply: oneshot::Sender<Result<(), AppError>>,
    },
    Snapshot {
        reply: oneshot::Sender<RefineryState>,
    },
}

#[derive(Clone)]
pub struct RefineryHandle {
    tx: mpsc::Sender<RefineryMsg>,
}

impl RefineryHandle {
    pub async fn validate(&self, cmd: RefineryCommand) -> Result<(), AppError> {
        let (reply, rx) = oneshot::channel();
        if self
            .tx
            .send(RefineryMsg::Validate { cmd, reply })
            .await
            .is_err()
        {
            return Err(AppError::Other("refinery actor gone".into()));
        }
        rx.await
            .map_err(|_| AppError::Other("refinery actor dropped reply".into()))?
    }

    pub async fn exec(&self, cmd: RefineryCommand) -> Result<(), AppError> {
        let (reply, rx) = oneshot::channel();
        if self
            .tx
            .send(RefineryMsg::Exec { cmd, reply })
            .await
            .is_err()
        {
            return Err(AppError::Other("refinery actor gone".into()));
        }
        rx.await
            .map_err(|_| AppError::Other("refinery actor dropped reply".into()))?
    }

    pub async fn snapshot(&self) -> RefineryState {
        let (reply, rx) = oneshot::channel();
        if self.tx.send(RefineryMsg::Snapshot { reply }).await.is_err() {
            return RefineryState::default();
        }
        rx.await.unwrap_or_default()
    }
}

pub fn spawn<R: RefineryRepository + 'static>(
    repo: R,
    events: mpsc::Sender<Envelope<RefineryEvent>>,
) -> RefineryHandle {
    spawn_hydrated(repo, events, RefineryState::default())
}

pub fn spawn_hydrated<R: RefineryRepository + 'static>(
    repo: R,
    events: mpsc::Sender<Envelope<RefineryEvent>>,
    initial: RefineryState,
) -> RefineryHandle {
    let (tx, mut rx) = mpsc::channel::<RefineryMsg>(64);
    let handle = RefineryHandle { tx };

    tokio::spawn(async move {
        let mut state = initial;
        while let Some(msg) = rx.recv().await {
            match msg {
                RefineryMsg::Validate { cmd, reply } => {
                    let _ = reply.send(cmd.validate(&state));
                }
                RefineryMsg::Exec { cmd, reply } => {
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
                                // Mirror touched entries to the repo (best-effort).
                                for ev in &events_to_emit {
                                    let bead = match ev {
                                        RefineryEvent::Observed { bead, .. }
                                        | RefineryEvent::Dispatched { bead } => bead,
                                    };
                                    if let Some(entry) = state.entries.get(bead) {
                                        let _ = repo.upsert_entry(entry).await;
                                    }
                                }
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
                RefineryMsg::Snapshot { reply } => {
                    let _ = reply.send(state.clone());
                }
            }
        }
    });

    handle
}
