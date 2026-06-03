//! Actor of the graph-warden domain: sole owner of `WardenState`. Messages enter over
//! `mpsc`; the events it applies are relayed to the composition root's sync bus.
//!
//! Emitted events are the ones the actor already **applied** to its in-memory state: if a
//! transition fails nothing is published — preserving replay determinism (only legal
//! transitions reach the log, same pattern as `gt-witness::actor`).

use tokio::sync::{mpsc, oneshot};

use gt_events::{AppError, Envelope};

use crate::commands::WardenCommand;
use crate::events::WardenEvent;
use crate::repo::GraphWardenRepository;
use crate::state::WardenState;

/// Messages accepted by the warden actor. `Validate`/`Exec` are the typed Command path
/// (mirrors gt-witness); `Snapshot` is the read port consumed by the MCP graph resource
/// (`hq-graphrig.10`) and by tests.
#[derive(Debug)]
pub enum WardenMsg {
    /// Inspect whether `cmd` would apply, without mutating.
    Validate {
        cmd: WardenCommand,
        reply: oneshot::Sender<Result<(), AppError>>,
    },
    /// Apply `cmd`: re-validate, fold each produced event, emit it, mirror to the repo.
    Exec {
        cmd: WardenCommand,
        reply: oneshot::Sender<Result<(), AppError>>,
    },
    /// Snapshot the live `WardenState`.
    Snapshot { reply: oneshot::Sender<WardenState> },
}

/// Cloneable handle to the warden actor.
#[derive(Clone)]
pub struct WardenHandle {
    tx: mpsc::Sender<WardenMsg>,
}

impl WardenHandle {
    /// Validate `cmd` against the live state without mutating.
    pub async fn validate(&self, cmd: WardenCommand) -> Result<(), AppError> {
        let (reply, rx) = oneshot::channel();
        if self.tx.send(WardenMsg::Validate { cmd, reply }).await.is_err() {
            return Err(AppError::Other("graphwarden actor gone".into()));
        }
        rx.await
            .map_err(|_| AppError::Other("graphwarden actor dropped reply".into()))?
    }

    /// Apply `cmd`. A command that produces zero events still returns `Ok(())`.
    pub async fn exec(&self, cmd: WardenCommand) -> Result<(), AppError> {
        let (reply, rx) = oneshot::channel();
        if self.tx.send(WardenMsg::Exec { cmd, reply }).await.is_err() {
            return Err(AppError::Other("graphwarden actor gone".into()));
        }
        rx.await
            .map_err(|_| AppError::Other("graphwarden actor dropped reply".into()))?
    }

    /// Snapshot the live `WardenState`. Returns the empty state if the actor is gone.
    pub async fn snapshot(&self) -> WardenState {
        let (reply, rx) = oneshot::channel();
        if self.tx.send(WardenMsg::Snapshot { reply }).await.is_err() {
            return WardenState::default();
        }
        rx.await.unwrap_or_default()
    }
}

/// Spawn the warden actor with an empty registry. Production binaries call
/// [`spawn_hydrated`] with the replay-folded `WardenState`.
pub fn spawn<R: GraphWardenRepository + 'static>(
    repo: R,
    events: mpsc::Sender<Envelope<WardenEvent>>,
) -> WardenHandle {
    spawn_hydrated(repo, events, WardenState::default())
}

/// Spawn the warden actor with `initial` as the hydrated starting state (boot replay).
pub fn spawn_hydrated<R: GraphWardenRepository + 'static>(
    repo: R,
    events: mpsc::Sender<Envelope<WardenEvent>>,
    initial: WardenState,
) -> WardenHandle {
    let (tx, mut rx) = mpsc::channel::<WardenMsg>(64);
    let handle = WardenHandle { tx };

    tokio::spawn(async move {
        let mut state = initial;
        while let Some(msg) = rx.recv().await {
            match msg {
                WardenMsg::Validate { cmd, reply } => {
                    let _ = reply.send(cmd.validate(&state));
                }
                WardenMsg::Exec { cmd, reply } => {
                    let outcome = match cmd.execute(&state) {
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
                                // Mirror touched rigs to the repo (best-effort).
                                for ev in &events_to_emit {
                                    let rig = event_target_rig(ev);
                                    match ev {
                                        WardenEvent::Unregistered { .. } => {
                                            let _ = repo.remove_rig(rig).await;
                                        }
                                        _ => {
                                            if let Some(g) = state.rigs.get(rig) {
                                                let _ = repo.upsert_rig(g).await;
                                            }
                                        }
                                    }
                                }
                                // Publish to the relay (best-effort; a closed relay does
                                // not roll back in-memory state).
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
                WardenMsg::Snapshot { reply } => {
                    let _ = reply.send(state.clone());
                }
            }
        }
    });

    handle
}

/// The rig id every `WardenEvent` variant carries — used by the repo-mirror step.
fn event_target_rig(ev: &WardenEvent) -> &str {
    match ev {
        WardenEvent::RigRegistered { rig, .. }
        | WardenEvent::MarkedStale { rig, .. }
        | WardenEvent::Refreshed { rig, .. }
        | WardenEvent::Unregistered { rig, .. } => rig,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::commands::{MarkRefreshed, MarkStale, RegisterRig};
    use crate::repo::InMemoryGraphWardenRepo;

    #[tokio::test]
    async fn exec_path_registers_marks_and_refreshes() {
        let (tx, mut rx) = mpsc::channel::<Envelope<WardenEvent>>(16);
        let h = spawn(InMemoryGraphWardenRepo::default(), tx);

        h.exec(WardenCommand::Register(RegisterRig {
            rig: "alpha".into(),
            repo_dir: "/repo/alpha".into(),
            now_secs: 1,
        }))
        .await
        .unwrap();
        h.exec(WardenCommand::MarkStale(MarkStale { rig: "alpha".into(), changed: 2, now_secs: 2 }))
            .await
            .unwrap();
        h.exec(WardenCommand::MarkRefreshed(MarkRefreshed {
            rig: "alpha".into(),
            commit: "deadbee".into(),
            now_secs: 3,
        }))
        .await
        .unwrap();

        let snap = h.snapshot().await;
        let g = &snap.rigs["alpha"];
        assert!(!g.stale);
        assert_eq!(g.last_indexed_commit.as_deref(), Some("deadbee"));

        // three events reached the relay, in canonical .v1 kinds.
        use gt_events::EventKind;
        let mut kinds = Vec::new();
        while let Ok(env) = rx.try_recv() {
            kinds.push(env.payload.kind());
        }
        assert_eq!(
            kinds,
            [
                "graphwarden.rig-registered.v1",
                "graphwarden.marked-stale.v1",
                "graphwarden.refreshed.v1"
            ]
        );
    }

    #[tokio::test]
    async fn exec_rejects_unknown_rig() {
        let (tx, _rx) = mpsc::channel::<Envelope<WardenEvent>>(4);
        let h = spawn(InMemoryGraphWardenRepo::default(), tx);
        let err = h
            .exec(WardenCommand::MarkStale(MarkStale { rig: "ghost".into(), changed: 1, now_secs: 1 }))
            .await
            .unwrap_err();
        assert!(matches!(err, AppError::NotFound(_)));
    }
}
