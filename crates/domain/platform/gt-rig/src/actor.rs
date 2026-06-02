//! Rig actor: sole owner of the [`RigCatalog`]. Clock enters as data on every message
//! (`now_secs`); the actor never reads it. Mutations go out as [`RigEvent`] envelopes to the
//! relay; the composition root drains them into the sync bus and persists them.

use tokio::sync::{mpsc, oneshot};

use gt_events::{AppError, Command, Envelope};

use crate::commands::RigCommand;
use crate::events::RigEvent;
use crate::state::{RigCatalog, RigEntry};

pub enum RigMsg {
    /// Diagnostics: number of live rigs.
    Snapshot(oneshot::Sender<usize>),
    /// Owned snapshot of every registered rig.
    Rigs(oneshot::Sender<Vec<RigEntry>>),
    /// Owner of a given prefix, if any.
    PrefixOwner {
        prefix: String,
        reply: oneshot::Sender<Option<String>>,
    },
    /// "Ask without doing": run `validate` against the current catalog, no mutation/emit.
    Validate {
        cmd: RigCommand,
        reply: oneshot::Sender<Result<(), AppError>>,
    },
    /// Apply the command: mutate the catalog and emit the produced event on the relay.
    Exec {
        cmd: RigCommand,
        reply: oneshot::Sender<Result<(), AppError>>,
    },
}

#[derive(Clone)]
pub struct RigHandle {
    tx: mpsc::Sender<RigMsg>,
}

impl RigHandle {
    /// Diagnostics: number of live rigs.
    pub async fn snapshot(&self) -> usize {
        let (reply, rx) = oneshot::channel();
        if self.tx.send(RigMsg::Snapshot(reply)).await.is_err() {
            return 0;
        }
        rx.await.unwrap_or(0)
    }

    pub async fn rigs(&self) -> Vec<RigEntry> {
        let (reply, rx) = oneshot::channel();
        if self.tx.send(RigMsg::Rigs(reply)).await.is_err() {
            return Vec::new();
        }
        rx.await.unwrap_or_default()
    }

    pub async fn prefix_owner(&self, prefix: impl Into<String>) -> Option<String> {
        let (reply, rx) = oneshot::channel();
        if self
            .tx
            .send(RigMsg::PrefixOwner {
                prefix: prefix.into(),
                reply,
            })
            .await
            .is_err()
        {
            return None;
        }
        rx.await.ok().flatten()
    }

    pub async fn validate(&self, cmd: RigCommand) -> Result<(), AppError> {
        let (reply, rx) = oneshot::channel();
        self.tx
            .send(RigMsg::Validate { cmd, reply })
            .await
            .map_err(|_| AppError::Other("rig actor gone".into()))?;
        rx.await
            .map_err(|_| AppError::Other("rig actor dropped reply".into()))?
    }

    pub async fn exec(&self, cmd: RigCommand) -> Result<(), AppError> {
        let (reply, rx) = oneshot::channel();
        self.tx
            .send(RigMsg::Exec { cmd, reply })
            .await
            .map_err(|_| AppError::Other("rig actor gone".into()))?;
        rx.await
            .map_err(|_| AppError::Other("rig actor dropped reply".into()))?
    }
}

/// Spawn the actor. `events` is the relay to the bus.
pub fn spawn(events: mpsc::Sender<Envelope<RigEvent>>) -> RigHandle {
    spawn_hydrated(events, RigCatalog::default())
}

/// Boot hydration: same as [`spawn`] but seeds the actor with a pre-built [`RigCatalog`].
/// The composition root passes the rebuilt catalog from the reducer (see
/// [`crate::RigState`]).
pub fn spawn_hydrated(
    events: mpsc::Sender<Envelope<RigEvent>>,
    initial: RigCatalog,
) -> RigHandle {
    let (tx, mut rx) = mpsc::channel::<RigMsg>(64);

    tokio::spawn(async move {
        let mut catalog = initial;

        while let Some(msg) = rx.recv().await {
            match msg {
                RigMsg::Snapshot(reply) => {
                    let _ = reply.send(catalog.len());
                }
                RigMsg::Rigs(reply) => {
                    let _ = reply.send(catalog.snapshot());
                }
                RigMsg::PrefixOwner { prefix, reply } => {
                    let _ = reply.send(catalog.prefix_owner(&prefix).map(str::to_string));
                }
                RigMsg::Validate { cmd, reply } => {
                    let _ = reply.send(cmd.validate(&catalog));
                }
                RigMsg::Exec { cmd, reply } => {
                    match cmd.execute(&mut catalog) {
                        Ok(event) => {
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

    RigHandle { tx }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::commands::{AddRig, RemoveRig};
    use tokio::sync::mpsc;

    fn add(name: &str, prefix: &str, now: u64) -> RigCommand {
        RigCommand::Add(AddRig {
            name: name.into(),
            prefix: prefix.into(),
            git_url: format!("git@x:y/{name}.git"),
            push_url: None,
            upstream_url: None,
            default_branch: "main".into(),
            now_secs: now,
        })
    }

    #[tokio::test]
    async fn actor_round_trip_emits_event_and_updates_catalog() {
        let (tx, mut events) = mpsc::channel(8);
        let actor = spawn(tx);

        actor.exec(add("plane", "pl", 1)).await.unwrap();
        let env = events.recv().await.expect("event emitted");
        assert!(matches!(env.payload, RigEvent::Added { .. }));

        assert_eq!(actor.snapshot().await, 1);
        assert_eq!(actor.prefix_owner("pl").await, Some("plane".into()));

        // Re-add must fail with Validation, no event.
        let err = actor.exec(add("plane", "pl2", 2)).await.unwrap_err();
        assert!(matches!(err, AppError::Validation(_)));
        assert!(
            events.try_recv().is_err(),
            "no event on failed exec; got one"
        );

        actor
            .exec(RigCommand::Remove(RemoveRig {
                name: "plane".into(),
                now_secs: 3,
            }))
            .await
            .unwrap();
        let env = events.recv().await.expect("removed event");
        assert!(matches!(env.payload, RigEvent::Removed { .. }));
        assert_eq!(actor.snapshot().await, 0);
    }

    #[tokio::test]
    async fn validate_is_side_effect_free() {
        let (tx, mut events) = mpsc::channel(8);
        let actor = spawn(tx);

        actor.exec(add("plane", "pl", 1)).await.unwrap();
        let _ = events.recv().await.unwrap();

        actor.validate(add("gastown", "gt", 2)).await.unwrap();
        assert_eq!(actor.snapshot().await, 1, "validate must not insert");
        assert!(events.try_recv().is_err());
    }
}
