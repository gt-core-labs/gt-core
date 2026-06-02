//! Skills actor: sole owner of the [`SkillCatalog`]. Clock enters as data on every
//! message (`now_secs`); the actor never reads it. Mutations go out as [`SkillEvent`]
//! envelopes to the relay; the composition root drains them into the sync bus and
//! persists them.

use tokio::sync::{mpsc, oneshot};

use gt_events::{AppError, Command, Envelope};

use crate::commands::SkillCommand;
use crate::events::SkillEvent;
use crate::state::{RoleBinding, Skill, SkillCatalog};

pub enum SkillMsg {
    /// Diagnostics: number of registered skills.
    Snapshot(oneshot::Sender<usize>),
    /// Owned snapshot of every registered skill.
    Skills(oneshot::Sender<Vec<Skill>>),
    /// Owned snapshot of every per-role binding.
    Bindings(oneshot::Sender<Vec<RoleBinding>>),
    /// Skills currently enabled for a given role. Empty when the role has no binding yet.
    SkillsForRole {
        role: String,
        reply: oneshot::Sender<Vec<String>>,
    },
    /// Flattened scope set unioned across every role's enabled skills (`hq-fe-skills.4`).
    /// Order-preserving + dedup'd in a single actor visit so callers don't have to
    /// double-walk the catalog from the outside.
    ScopesForRoles {
        roles: Vec<String>,
        reply: oneshot::Sender<Vec<String>>,
    },
    /// "Ask without doing": run `validate` against the current catalog, no mutation/emit.
    Validate {
        cmd: SkillCommand,
        reply: oneshot::Sender<Result<(), AppError>>,
    },
    /// Apply the command: mutate the catalog and emit the produced event on the relay.
    Exec {
        cmd: SkillCommand,
        reply: oneshot::Sender<Result<(), AppError>>,
    },
}

#[derive(Clone)]
pub struct SkillHandle {
    tx: mpsc::Sender<SkillMsg>,
}

impl SkillHandle {
    /// Diagnostics: number of registered skills.
    pub async fn snapshot(&self) -> usize {
        let (reply, rx) = oneshot::channel();
        if self.tx.send(SkillMsg::Snapshot(reply)).await.is_err() {
            return 0;
        }
        rx.await.unwrap_or(0)
    }

    pub async fn skills(&self) -> Vec<Skill> {
        let (reply, rx) = oneshot::channel();
        if self.tx.send(SkillMsg::Skills(reply)).await.is_err() {
            return Vec::new();
        }
        rx.await.unwrap_or_default()
    }

    pub async fn bindings(&self) -> Vec<RoleBinding> {
        let (reply, rx) = oneshot::channel();
        if self.tx.send(SkillMsg::Bindings(reply)).await.is_err() {
            return Vec::new();
        }
        rx.await.unwrap_or_default()
    }

    pub async fn skills_for_role(&self, role: impl Into<String>) -> Vec<String> {
        let (reply, rx) = oneshot::channel();
        if self
            .tx
            .send(SkillMsg::SkillsForRole {
                role: role.into(),
                reply,
            })
            .await
            .is_err()
        {
            return Vec::new();
        }
        rx.await.unwrap_or_default()
    }

    /// Flattened scope union for a set of roles (`hq-fe-skills.4`). For every role in
    /// `roles`, look up the enabled skills, then for each skill flatten in its
    /// `default_scopes`, dedup'd first-seen so the role's primary scope stays at the
    /// front (matches the `gt_rbac::WebGrant::scopes` ordering posture). Empty input —
    /// or roles with no bindings — yields an empty vec; callers should union against
    /// the static `WebGrant.scopes` themselves.
    pub async fn scopes_for_roles(&self, roles: Vec<String>) -> Vec<String> {
        let (reply, rx) = oneshot::channel();
        if self
            .tx
            .send(SkillMsg::ScopesForRoles { roles, reply })
            .await
            .is_err()
        {
            return Vec::new();
        }
        rx.await.unwrap_or_default()
    }

    pub async fn validate(&self, cmd: SkillCommand) -> Result<(), AppError> {
        let (reply, rx) = oneshot::channel();
        self.tx
            .send(SkillMsg::Validate { cmd, reply })
            .await
            .map_err(|_| AppError::Other("skills actor gone".into()))?;
        rx.await
            .map_err(|_| AppError::Other("skills actor dropped reply".into()))?
    }

    pub async fn exec(&self, cmd: SkillCommand) -> Result<(), AppError> {
        let (reply, rx) = oneshot::channel();
        self.tx
            .send(SkillMsg::Exec { cmd, reply })
            .await
            .map_err(|_| AppError::Other("skills actor gone".into()))?;
        rx.await
            .map_err(|_| AppError::Other("skills actor dropped reply".into()))?
    }
}

/// Spawn the actor. `events` is the relay to the bus.
pub fn spawn(events: mpsc::Sender<Envelope<SkillEvent>>) -> SkillHandle {
    spawn_hydrated(events, SkillCatalog::default())
}

/// Boot hydration: same as [`spawn`] but seeds the actor with a pre-built catalog. The
/// composition root passes the rebuilt catalog from the reducer (see [`crate::SkillState`]).
pub fn spawn_hydrated(
    events: mpsc::Sender<Envelope<SkillEvent>>,
    initial: SkillCatalog,
) -> SkillHandle {
    let (tx, mut rx) = mpsc::channel::<SkillMsg>(64);

    tokio::spawn(async move {
        let mut catalog = initial;

        while let Some(msg) = rx.recv().await {
            match msg {
                SkillMsg::Snapshot(reply) => {
                    let _ = reply.send(catalog.len());
                }
                SkillMsg::Skills(reply) => {
                    let _ = reply.send(catalog.skills());
                }
                SkillMsg::Bindings(reply) => {
                    let _ = reply.send(catalog.bindings());
                }
                SkillMsg::ScopesForRoles { roles, reply } => {
                    let _ = reply.send(catalog.scopes_for_roles(&roles));
                }
                SkillMsg::SkillsForRole { role, reply } => {
                    let _ = reply.send(catalog.skills_for_role(&role));
                }
                SkillMsg::Validate { cmd, reply } => {
                    let _ = reply.send(cmd.validate(&catalog));
                }
                SkillMsg::Exec { cmd, reply } => match cmd.execute(&mut catalog) {
                    Ok(event) => {
                        let _ = events.send(Envelope::root(event)).await;
                        let _ = reply.send(Ok(()));
                    }
                    Err(e) => {
                        let _ = reply.send(Err(e));
                    }
                },
            }
        }
    });

    SkillHandle { tx }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::commands::{EnableSkillForRole, RegisterSkill};

    fn register(id: &str, now: u64) -> SkillCommand {
        SkillCommand::Register(RegisterSkill {
            skill: id.into(),
            label: format!("{id} label"),
            description: "".into(),
            default_scopes: vec![format!("{id}.read")],
            now_secs: now,
        })
    }

    #[tokio::test]
    async fn actor_round_trip_emits_event_and_updates_catalog() {
        let (tx, mut events) = mpsc::channel(8);
        let actor = spawn(tx);

        actor.exec(register("alpha", 1)).await.unwrap();
        let env = events.recv().await.expect("event emitted");
        assert!(matches!(env.payload, SkillEvent::Registered { .. }));
        assert_eq!(actor.snapshot().await, 1);

        actor
            .exec(SkillCommand::Enable(EnableSkillForRole {
                role: "deacon".into(),
                skill: "alpha".into(),
                now_secs: 2,
            }))
            .await
            .unwrap();
        let env = events.recv().await.expect("enable event");
        assert!(matches!(env.payload, SkillEvent::EnabledForRole { .. }));
        assert_eq!(actor.skills_for_role("deacon").await, vec!["alpha"]);

        // Failed exec must NOT emit on the relay.
        let dup_err = actor.exec(register("alpha", 3)).await.unwrap_err();
        assert!(matches!(dup_err, AppError::Validation(_)));
        assert!(
            events.try_recv().is_err(),
            "no event on failed exec; got one"
        );
    }

    #[tokio::test]
    async fn validate_is_side_effect_free() {
        let (tx, mut events) = mpsc::channel(8);
        let actor = spawn(tx);

        actor.exec(register("alpha", 1)).await.unwrap();
        let _ = events.recv().await.unwrap();

        actor.validate(register("beta", 2)).await.unwrap();
        assert_eq!(actor.snapshot().await, 1, "validate must not insert");
        assert!(events.try_recv().is_err());
    }
}
