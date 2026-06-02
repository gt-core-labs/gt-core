//! Dispatcher serializado: actor único dueño del dequeue. Serializa el claim dentro del
//! proceso; el CAS es la red para crash/recuperación o un segundo dispatcher.
//!
//! `Validate`/`Exec` son el camino tipado de [`Command`] (ver `docs/09-llm-integration.md`):
//! `Validate` inspecciona el estado sin mutarlo; `Exec` re-valida y aplica en la misma vuelta
//! del actor — sin `.await` entre validate y execute — para cerrar la ventana TOCTOU. Tras un
//! `Exec` exitoso el actor emite el `SchedEvent` correspondiente al log (la I/O vive aquí, en
//! el borde, no en el command). Los clientes externos (`gt-mcp`) entran por aquí.

use tokio::sync::{mpsc, oneshot};

use gt_beads::{Bead, BeadRepository};
use gt_events::{AppError, Command, Envelope};

use crate::commands::SchedCommand;
use crate::events::SchedEvent;
use crate::state::SchedCore;

pub enum SchedMsg {
    Enqueue {
        bead: String,
        priority: u8,
    },
    /// Create (or replace) a bead row in the repo. A passthrough to `BeadRepository::upsert`
    /// run from the one task that owns the repo handle (hq-mc72.10) — so the MCP edge can mint
    /// the schedulable work the dispatcher's CAS-claim later needs. No `SchedEvent`: bead
    /// creation is a repo write, not a queue state change (the durable truth is the repo, as
    /// when `bd`/Dolt creates beads externally).
    CreateBead {
        bead: Bead,
        reply: oneshot::Sender<Result<(), AppError>>,
    },
    /// Un worker terminó: libera capacidad y re-bombea.
    CapacityFreed,
    /// (queued, in_flight)
    Snapshot(oneshot::Sender<(usize, usize)>),
    /// "Ask without doing": run `validate` against the current core. No mutation, no pump.
    Validate {
        cmd: SchedCommand,
        reply: oneshot::Sender<Result<(), AppError>>,
    },
    /// Apply the command. Re-validates inside the same tick, then emits the matching event.
    Exec {
        cmd: SchedCommand,
        reply: oneshot::Sender<Result<(), AppError>>,
    },
}

#[derive(Clone)]
pub struct SchedHandle {
    tx: mpsc::Sender<SchedMsg>,
}

impl SchedHandle {
    pub async fn enqueue(&self, bead: impl Into<String>, priority: u8) {
        let _ = self
            .tx
            .send(SchedMsg::Enqueue {
                bead: bead.into(),
                priority,
            })
            .await;
    }

    pub async fn capacity_freed(&self) {
        let _ = self.tx.send(SchedMsg::CapacityFreed).await;
    }

    /// Create (or replace) a bead in the repo via the owning actor. Returns the repo result so
    /// the caller (the MCP `scheduling.create_bead` tool) can surface a failure.
    pub async fn create_bead(&self, bead: Bead) -> Result<(), AppError> {
        let (reply, rx) = oneshot::channel();
        self.tx
            .send(SchedMsg::CreateBead { bead, reply })
            .await
            .map_err(|_| AppError::Other("actor gone".into()))?;
        rx.await
            .map_err(|_| AppError::Other("actor dropped reply".into()))?
    }

    pub async fn snapshot(&self) -> (usize, usize) {
        let (reply, rx) = oneshot::channel();
        if self.tx.send(SchedMsg::Snapshot(reply)).await.is_err() {
            return (0, 0);
        }
        rx.await.unwrap_or((0, 0))
    }

    /// "Ask without doing": validate against the current state snapshot. The actor revalidates
    /// on `exec`, so the answer is a snapshot, not a lock.
    pub async fn validate(&self, cmd: SchedCommand) -> Result<(), AppError> {
        let (reply, rx) = oneshot::channel();
        self.tx
            .send(SchedMsg::Validate { cmd, reply })
            .await
            .map_err(|_| AppError::Other("actor gone".into()))?;
        rx.await
            .map_err(|_| AppError::Other("actor dropped reply".into()))?
    }

    /// Apply the command. The actor re-validates inside the same tick and emits the matching
    /// `SchedEvent` on success.
    pub async fn exec(&self, cmd: SchedCommand) -> Result<(), AppError> {
        let (reply, rx) = oneshot::channel();
        self.tx
            .send(SchedMsg::Exec { cmd, reply })
            .await
            .map_err(|_| AppError::Other("actor gone".into()))?;
        rx.await
            .map_err(|_| AppError::Other("actor dropped reply".into()))?
    }
}

/// The event a successfully-executed command lands in the log. `Enqueue` mirrors the queued
/// bead; `MarkDispatched` is the manual assignment of a bead to a worker.
fn event_for(cmd: &SchedCommand) -> SchedEvent {
    match cmd {
        SchedCommand::Enqueue(e) => SchedEvent::Enqueue {
            bead: e.bead.clone(),
            priority: e.priority,
        },
        SchedCommand::MarkDispatched(m) => SchedEvent::Dispatched {
            bead: m.bead.clone(),
            worker: m.worker.clone(),
        },
    }
}

/// Arranca el dispatcher. `events` es el relay (mpsc) hacia el bus síncrono del composition
/// root. `max` = capacidad de polecats concurrentes.
pub fn spawn<R>(repo: R, events: mpsc::Sender<Envelope<SchedEvent>>, max: usize) -> SchedHandle
where
    R: BeadRepository + Send + Sync + 'static,
{
    let (tx, mut rx) = mpsc::channel::<SchedMsg>(64);
    tokio::spawn(async move {
        let mut core = SchedCore::new(max);
        let mut worker_seq: u64 = 0;

        while let Some(msg) = rx.recv().await {
            // `pump` decides whether to drain the queue after handling the message. Reads
            // (Snapshot/Validate) and the manual MarkDispatched never auto-pump.
            let mut pump = true;
            match msg {
                SchedMsg::Enqueue { bead, priority } => {
                    core.queue.enqueue(bead.clone(), priority);
                    let _ = events
                        .send(Envelope::root(SchedEvent::Enqueue { bead, priority }))
                        .await;
                }
                SchedMsg::CreateBead { bead, reply } => {
                    let _ = reply.send(repo.upsert(&bead).await);
                    pump = false;
                }
                SchedMsg::CapacityFreed => core.gov.release(),
                SchedMsg::Snapshot(reply) => {
                    let _ = reply.send((core.queue.len(), core.gov.in_flight()));
                    pump = false;
                }
                SchedMsg::Validate { cmd, reply } => {
                    let _ = reply.send(cmd.validate(&core));
                    pump = false;
                }
                SchedMsg::Exec { cmd, reply } => {
                    // execute() re-validates first → no TOCTOU within the actor tick.
                    let res = cmd.execute(&mut core);
                    if res.is_ok() {
                        let _ = events.send(Envelope::root(event_for(&cmd))).await;
                    }
                    let _ = reply.send(res);
                    // Only Enqueue can free up work for the pump; a manual MarkDispatched has
                    // already consumed its slot.
                    pump = matches!(cmd, SchedCommand::Enqueue(_));
                }
            }

            if !pump {
                continue;
            }

            // Bombeo: desencola hasta agotar capacidad.
            while core.gov.can_dispatch() {
                let Some(bead) = core.queue.pop_highest() else {
                    break;
                };
                worker_seq += 1;
                let worker = format!("w{worker_seq}");
                let ev = match repo.cas_claim(&bead, &worker).await {
                    Ok(true) => {
                        core.gov.acquire();
                        SchedEvent::Dispatched { bead, worker }
                    }
                    Ok(false) => SchedEvent::DispatchFailed {
                        bead,
                        reason: "no longer pending".into(),
                    },
                    Err(e) => SchedEvent::DispatchFailed {
                        bead,
                        reason: e.to_string(),
                    },
                };
                let _ = events.send(Envelope::root(ev)).await;
            }
        }
    });
    SchedHandle { tx }
}
