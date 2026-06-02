//! Patrol actor: single owner of the live-lease tracker. The actor itself never reads the
//! clock — the *edge* (an async tick task in the bin) reads it and passes `now_secs` on
//! every `Heartbeat` / `Tick`. That keeps detection replay-able.
//!
//! Persistence (hq-03aw.7): each register/heartbeat/close/expire is mirrored into the
//! injected [`PatrolRepository`]. Best-effort — write failures are logged, never blocking
//! the relay. The audit log + [`crate::PatrolState`] reducer stay the source of truth.

use tokio::sync::{mpsc, oneshot};

use gt_events::{AppError, Command, Envelope};

use crate::commands::PatrolCommand;
use crate::events::PatrolEvent;
use crate::expectations::expired_leases;
use crate::repo::PatrolRepository;
use crate::state::LeaseTracker;

pub enum PatrolMsg {
    /// Dispatcher just claimed `bead` for `worker`: start a lease.
    Register {
        bead: String,
        worker: String,
        priority: u8,
        now_secs: u64,
    },
    /// Worker still alive: refresh every lease it owns.
    Heartbeat {
        worker: String,
        now_secs: u64,
    },
    /// Completion/failure observed elsewhere: drop the lease without firing expired.
    Close {
        bead: String,
    },
    /// Edge tick: run the pure detector against `now_secs` and emit `LeaseExpired` for
    /// every stale lease. Removes them from the tracker so a follow-up tick at the same
    /// `now_secs` does not re-fire the same expiration.
    Tick {
        now_secs: u64,
        timeout_secs: u64,
    },
    /// Diagnostics: snapshot `(live_leases, expired_emitted_total)`.
    Snapshot(oneshot::Sender<(usize, usize)>),
    /// "Ask without doing": run `validate` against the current tracker. No mutation.
    Validate {
        cmd: PatrolCommand,
        reply: oneshot::Sender<Result<(), AppError>>,
    },
    /// Apply the command. Re-validates inside the same tick, then emits whatever events the
    /// command produced (zero or many, e.g. a `Tick` expiring several leases).
    Exec {
        cmd: PatrolCommand,
        reply: oneshot::Sender<Result<(), AppError>>,
    },
}

#[derive(Clone)]
pub struct PatrolHandle {
    tx: mpsc::Sender<PatrolMsg>,
}

impl PatrolHandle {
    pub async fn register(&self, bead: impl Into<String>, worker: impl Into<String>, priority: u8, now_secs: u64) {
        let _ = self
            .tx
            .send(PatrolMsg::Register {
                bead: bead.into(),
                worker: worker.into(),
                priority,
                now_secs,
            })
            .await;
    }

    pub async fn heartbeat(&self, worker: impl Into<String>, now_secs: u64) {
        let _ = self
            .tx
            .send(PatrolMsg::Heartbeat {
                worker: worker.into(),
                now_secs,
            })
            .await;
    }

    pub async fn close(&self, bead: impl Into<String>) {
        let _ = self.tx.send(PatrolMsg::Close { bead: bead.into() }).await;
    }

    pub async fn tick(&self, now_secs: u64, timeout_secs: u64) {
        let _ = self.tx.send(PatrolMsg::Tick { now_secs, timeout_secs }).await;
    }

    pub async fn snapshot(&self) -> (usize, usize) {
        let (reply, rx) = oneshot::channel();
        if self.tx.send(PatrolMsg::Snapshot(reply)).await.is_err() {
            return (0, 0);
        }
        rx.await.unwrap_or((0, 0))
    }

    /// "Ask without doing": validate against the current tracker snapshot. The actor
    /// revalidates on `exec`, so the answer is a snapshot, not a lock.
    pub async fn validate(&self, cmd: PatrolCommand) -> Result<(), AppError> {
        let (reply, rx) = oneshot::channel();
        self.tx
            .send(PatrolMsg::Validate { cmd, reply })
            .await
            .map_err(|_| AppError::Other("actor gone".into()))?;
        rx.await
            .map_err(|_| AppError::Other("actor dropped reply".into()))?
    }

    /// Apply the command. The actor re-validates inside the same tick and emits the events
    /// the command produced.
    pub async fn exec(&self, cmd: PatrolCommand) -> Result<(), AppError> {
        let (reply, rx) = oneshot::channel();
        self.tx
            .send(PatrolMsg::Exec { cmd, reply })
            .await
            .map_err(|_| AppError::Other("actor gone".into()))?;
        rx.await
            .map_err(|_| AppError::Other("actor dropped reply".into()))?
    }
}

async fn persist_one<R: PatrolRepository>(repo: &R, tracker: &LeaseTracker, bead: &str) {
    if let Some(lease) = tracker.get(bead) {
        if let Err(e) = repo.upsert_lease(lease).await {
            eprintln!("[gt-patrol] persist lease {bead}: {e}");
        }
    }
}

async fn delete_one<R: PatrolRepository>(repo: &R, bead: &str) {
    if let Err(e) = repo.delete_lease(bead).await {
        eprintln!("[gt-patrol] delete lease {bead}: {e}");
    }
}

/// Spawn the patrol actor. `events` is the relay (`mpsc`) into the sync bus that the
/// composition root drains. Every observation (Register/Heartbeat/Close) is mirrored to
/// the log so replay reconstructs the same tracker state. `LeaseExpired` is emitted only
/// from `Tick`, ordered by bead id for deterministic replay. `repo` mirrors the same
/// lifecycle into the persistence port (Dolt in prod, in-memory otherwise).
pub fn spawn<R>(repo: R, events: mpsc::Sender<Envelope<PatrolEvent>>) -> PatrolHandle
where
    R: PatrolRepository + 'static,
{
    spawn_hydrated(repo, events, LeaseTracker::default(), 0)
}

/// Boot hydration (hq-8iur.1): same as [`spawn`] but seeds the actor with a pre-built
/// [`LeaseTracker`] and the count of `LeaseExpired` events emitted in the prior run (so the
/// snapshot counter stays continuous across a restart). The composition root passes both
/// from the reducer rebuilt by `replay_gt`.
pub fn spawn_hydrated<R>(
    repo: R,
    events: mpsc::Sender<Envelope<PatrolEvent>>,
    initial: LeaseTracker,
    expired_seen: usize,
) -> PatrolHandle
where
    R: PatrolRepository + 'static,
{
    let (tx, mut rx) = mpsc::channel::<PatrolMsg>(64);
    tokio::spawn(async move {
        let mut tracker = initial;
        let mut expired_emitted: usize = expired_seen;

        while let Some(msg) = rx.recv().await {
            match msg {
                PatrolMsg::Register { bead, worker, priority, now_secs } => {
                    tracker.register(bead.clone(), worker.clone(), priority, now_secs);
                    persist_one(&repo, &tracker, &bead).await;
                    let _ = events
                        .send(Envelope::root(PatrolEvent::LeaseRegistered {
                            bead,
                            worker,
                            priority,
                            now_secs,
                        }))
                        .await;
                }
                PatrolMsg::Heartbeat { worker, now_secs } => {
                    tracker.heartbeat(&worker, now_secs);
                    if let Err(e) = repo.heartbeat_worker(&worker, now_secs).await {
                        eprintln!("[gt-patrol] persist heartbeat {worker}: {e}");
                    }
                    let _ = events
                        .send(Envelope::root(PatrolEvent::Heartbeat { worker, now_secs }))
                        .await;
                }
                PatrolMsg::Close { bead } => {
                    tracker.close(&bead);
                    delete_one(&repo, &bead).await;
                    let _ = events
                        .send(Envelope::root(PatrolEvent::LeaseClosed { bead }))
                        .await;
                }
                PatrolMsg::Tick { now_secs, timeout_secs } => {
                    // Pure detection against a sorted snapshot → deterministic emit order.
                    let snap = tracker.snapshot();
                    let stale = expired_leases(&snap, now_secs, timeout_secs);
                    for lease in stale {
                        tracker.close(&lease.bead);
                        delete_one(&repo, &lease.bead).await;
                        expired_emitted += 1;
                        let _ = events
                            .send(Envelope::root(PatrolEvent::LeaseExpired {
                                bead: lease.bead,
                                worker: lease.worker,
                                priority: lease.priority,
                            }))
                            .await;
                    }
                }
                PatrolMsg::Snapshot(reply) => {
                    let _ = reply.send((tracker.len(), expired_emitted));
                }
                PatrolMsg::Validate { cmd, reply } => {
                    let _ = reply.send(cmd.validate(&tracker));
                }
                PatrolMsg::Exec { cmd, reply } => {
                    // execute() re-validates first → no TOCTOU within the actor tick. It
                    // returns the events to relay (a Tick may yield several LeaseExpired).
                    match cmd.execute(&mut tracker) {
                        Ok(evs) => {
                            for ev in evs {
                                match &ev {
                                    PatrolEvent::LeaseRegistered { bead, .. } => {
                                        persist_one(&repo, &tracker, bead).await;
                                    }
                                    PatrolEvent::Heartbeat { worker, now_secs } => {
                                        if let Err(e) = repo.heartbeat_worker(worker, *now_secs).await {
                                            eprintln!("[gt-patrol] persist heartbeat {worker}: {e}");
                                        }
                                    }
                                    PatrolEvent::LeaseClosed { bead } => {
                                        delete_one(&repo, bead).await;
                                    }
                                    PatrolEvent::LeaseExpired { bead, .. } => {
                                        expired_emitted += 1;
                                        delete_one(&repo, bead).await;
                                    }
                                }
                                let _ = events.send(Envelope::root(ev)).await;
                            }
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
    PatrolHandle { tx }
}
