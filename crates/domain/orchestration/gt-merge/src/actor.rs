//! Actor del dominio merge: dueño único del `MergeBoard`. Sin `Arc<Mutex>`: los mensajes
//! entran por `mpsc`, las observaciones salen por el relay `events` hacia el bus síncrono
//! del composition root.
//!
//! Los eventos emitidos al log son los que el actor ya *aplicó* al estado interno: si la
//! transición falla, no se publica nada (preserva la determinismo del replay — solo
//! transiciones legales quedan grabadas).
//!
//! Persistence (hq-03aw.6): on every successful transition the actor mirrors the live slot
//! into the injected [`MergeRepository`] (Dolt in prod, in-memory in tests). The event log
//! stays authoritative — the repo is best-effort, errors are logged but never block the
//! emit so replay byte-identico holds.

use tokio::sync::{mpsc, oneshot};

use gt_events::{AppError, Command, Envelope};

use crate::commands::MergeCommand;
use crate::events::MergeEvent;
use crate::repo::MergeRepository;
use crate::state::{MergeBoard, MergeSlot};

/// Mensajes al actor.
///
/// `Validate`/`Exec` son el camino tipado de [`Command`] (ver `docs/09-llm-integration.md`):
/// `Validate` inspecciona el board sin mutarlo; `Exec` re-valida, aplica la transición y
/// emite al relay el `MergeEvent` que el command produjo — todo en la misma vuelta del actor,
/// sin `.await` entre validate y execute, cerrando la ventana TOCTOU. Los clientes externos
/// (`gt-mcp`) entran por aquí; los productores internos (refinery, root) siguen usando
/// `Submit`/`Start`/`Complete`/`Fail` (fire-and-forget) por compatibilidad con el Paso 6.b.
pub enum MergeMsg {
    /// Refinery tradujo un `MERGE_READY` del channel. Crea el slot en `Ready`.
    Submit {
        bead: String,
        branch: String,
        channel_msg_id: String,
    },
    /// Composition root pide al actor que avance al merge físico (`Ready → Merging`).
    Start { bead: String },
    /// El borde reporta merge exitoso (`Merging → Merged`).
    Complete { bead: String, sha: String },
    /// El borde reporta fallo (`Merging → Failed`).
    Fail { bead: String, reason: String },
    /// Snapshot diagnóstico del board.
    Snapshot(oneshot::Sender<Vec<MergeSlot>>),
    /// "Ask without doing": corre `validate` contra el board actual, sin mutar ni emitir.
    Validate {
        cmd: MergeCommand,
        reply: oneshot::Sender<Result<(), AppError>>,
    },
    /// Aplica el command: re-valida, transiciona y emite el evento producido al relay.
    Exec {
        cmd: MergeCommand,
        reply: oneshot::Sender<Result<(), AppError>>,
    },
}

#[derive(Clone)]
pub struct MergeHandle {
    tx: mpsc::Sender<MergeMsg>,
}

impl MergeHandle {
    pub async fn submit(
        &self,
        bead: impl Into<String>,
        branch: impl Into<String>,
        channel_msg_id: impl Into<String>,
    ) {
        let _ = self
            .tx
            .send(MergeMsg::Submit {
                bead: bead.into(),
                branch: branch.into(),
                channel_msg_id: channel_msg_id.into(),
            })
            .await;
    }

    pub async fn start(&self, bead: impl Into<String>) {
        let _ = self.tx.send(MergeMsg::Start { bead: bead.into() }).await;
    }

    pub async fn complete(&self, bead: impl Into<String>, sha: impl Into<String>) {
        let _ = self
            .tx
            .send(MergeMsg::Complete {
                bead: bead.into(),
                sha: sha.into(),
            })
            .await;
    }

    pub async fn fail(&self, bead: impl Into<String>, reason: impl Into<String>) {
        let _ = self
            .tx
            .send(MergeMsg::Fail {
                bead: bead.into(),
                reason: reason.into(),
            })
            .await;
    }

    pub async fn snapshot(&self) -> Vec<MergeSlot> {
        let (reply, rx) = oneshot::channel();
        if self.tx.send(MergeMsg::Snapshot(reply)).await.is_err() {
            return Vec::new();
        }
        rx.await.unwrap_or_default()
    }

    /// "Ask without doing": run `validate` against the current board snapshot.
    /// The answer is a snapshot; the actor revalidates on `exec`.
    pub async fn validate(&self, cmd: MergeCommand) -> Result<(), AppError> {
        let (reply, rx) = oneshot::channel();
        self.tx
            .send(MergeMsg::Validate { cmd, reply })
            .await
            .map_err(|_| AppError::Other("merge actor gone".into()))?;
        rx.await
            .map_err(|_| AppError::Other("merge actor dropped reply".into()))?
    }

    /// Apply the command. The actor re-validates inside the same tick and emits the produced
    /// `MergeEvent` to the relay, so the result reflects state at execution time, not the
    /// snapshot a prior `validate` saw.
    pub async fn exec(&self, cmd: MergeCommand) -> Result<(), AppError> {
        let (reply, rx) = oneshot::channel();
        self.tx
            .send(MergeMsg::Exec { cmd, reply })
            .await
            .map_err(|_| AppError::Other("merge actor gone".into()))?;
        rx.await
            .map_err(|_| AppError::Other("merge actor dropped reply".into()))?
    }
}

/// Persist the live `slot` for `bead`. Best-effort: a Dolt outage is logged and the actor
/// keeps emitting — the event log + replay reducer remain the source of truth.
async fn persist<R: MergeRepository>(repo: &R, board: &MergeBoard, bead: &str) {
    if let Some(slot) = board.get(bead) {
        if let Err(e) = repo.upsert_slot(slot).await {
            eprintln!("[gt-merge] persist slot {bead}: {e}");
        }
    }
}

/// Arranca el actor. `events` es el relay (mpsc) hacia el bus síncrono del composition
/// root, que a su vez lo persiste en el audit log. `repo` mirrors each transition into the
/// persistence port (Dolt in prod, in-memory otherwise).
pub fn spawn<R>(repo: R, events: mpsc::Sender<Envelope<MergeEvent>>) -> MergeHandle
where
    R: MergeRepository + 'static,
{
    spawn_hydrated(repo, events, MergeBoard::default())
}

/// Boot hydration (hq-8iur.1): same as [`spawn`] but seeds the actor with a pre-built
/// [`MergeBoard`]. The composition root passes the board reconstructed by `replay_gt` so a
/// restart restores in-flight merge slots before the actor starts processing edge messages.
pub fn spawn_hydrated<R>(
    repo: R,
    events: mpsc::Sender<Envelope<MergeEvent>>,
    initial: MergeBoard,
) -> MergeHandle
where
    R: MergeRepository + 'static,
{
    let (tx, mut rx) = mpsc::channel::<MergeMsg>(64);
    tokio::spawn(async move {
        let mut board = initial;
        while let Some(msg) = rx.recv().await {
            match msg {
                MergeMsg::Submit { bead, branch, channel_msg_id } => {
                    if board.submit(bead.clone(), branch.clone()).is_ok() {
                        persist(&repo, &board, &bead).await;
                        let _ = events
                            .send(Envelope::root(MergeEvent::Ready {
                                bead,
                                branch,
                                channel_msg_id,
                            }))
                            .await;
                    }
                }
                MergeMsg::Start { bead } => {
                    if board.start(&bead).is_ok() {
                        persist(&repo, &board, &bead).await;
                        let _ = events.send(Envelope::root(MergeEvent::Started { bead })).await;
                    }
                }
                MergeMsg::Complete { bead, sha } => {
                    if board.complete(&bead).is_ok() {
                        persist(&repo, &board, &bead).await;
                        let _ = events
                            .send(Envelope::root(MergeEvent::Merged { bead, sha }))
                            .await;
                    }
                }
                MergeMsg::Fail { bead, reason } => {
                    if board.fail(&bead).is_ok() {
                        persist(&repo, &board, &bead).await;
                        let _ = events
                            .send(Envelope::root(MergeEvent::Failed { bead, reason }))
                            .await;
                    }
                }
                MergeMsg::Snapshot(reply) => {
                    let _ = reply.send(board.snapshot());
                }
                MergeMsg::Validate { cmd, reply } => {
                    let _ = reply.send(cmd.validate(&board));
                }
                MergeMsg::Exec { cmd, reply } => {
                    // execute() re-validates first → no TOCTOU within the actor tick. On
                    // success it returns the event to emit, preserving emit-on-apply: only
                    // legal transitions reach the log.
                    match cmd.execute(&mut board) {
                        Ok(event) => {
                            if let Some(bead) = event_bead(&event) {
                                persist(&repo, &board, bead).await;
                            }
                            let _ = events.send(Envelope::root(event)).await;
                            let _ = reply.send(Ok(()));
                        }
                        Err(e) => {
                            let _ = reply.send(Err(e));
                        }
                    }
                }
            }
        }
    });
    MergeHandle { tx }
}

fn event_bead(event: &MergeEvent) -> Option<&str> {
    match event {
        MergeEvent::Ready { bead, .. }
        | MergeEvent::Started { bead }
        | MergeEvent::Merged { bead, .. }
        | MergeEvent::Failed { bead, .. } => Some(bead.as_str()),
    }
}
