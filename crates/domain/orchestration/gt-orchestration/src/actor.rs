//! Orchestration actor: single owner of the live `ConvoyBoard`. It never reads the clock —
//! a convoy advances on *facts* (a member finished), which keeps it replay-able. The actor
//! is also the one place that turns a completion into the next handoff: on `MemberDone` it
//! either feeds the next member (`MemberDispatched`) or, if all are done, closes the convoy
//! (`ConvoyClosed`). The real I/O (`gt sling` of the dispatched member, closing the convoy
//! bead) lives at the composition-root edge reacting to those emitted events — same pattern
//! as `gt-merge` (Paso 6.b) and `gt-patrol` (Paso 6.a).
//!
//! Persistence (hq-03aw.8): each state change is mirrored into the injected
//! [`OrchRepository`]. Best-effort — write failures are logged, never blocking the relay.

use tokio::sync::{mpsc, oneshot};

use gt_events::{AppError, Command, Envelope};

use crate::commands::OrchCommand;
use crate::events::OrchEvent;
use crate::repo::OrchRepository;
use crate::state::{Convoy, ConvoyBoard, ConvoyState, MemberState};

pub enum OrchMsg {
    /// Plan a convoy: an ordered set of member beads. Starts `Staged`.
    CreateConvoy {
        convoy: String,
        members: Vec<String>,
    },
    /// Release the convoy (mayor/deacon): launch it and feed the first member.
    Launch {
        convoy: String,
    },
    /// A member's bead finished (observed at the edge): advance the convoy — feed the next
    /// member, or close the convoy if this was the last one.
    MemberDone {
        convoy: String,
        member: String,
    },
    /// A member's bead failed: record it and halt the convoy.
    MemberFail {
        convoy: String,
        member: String,
        reason: String,
    },
    /// Diagnostics: deterministic snapshot of the live board (sorted by convoy id).
    Snapshot(oneshot::Sender<Vec<Convoy>>),
    /// "Ask without doing": run `validate` against the current board. No mutation.
    Validate {
        cmd: OrchCommand,
        reply: oneshot::Sender<Result<(), AppError>>,
    },
    /// Apply the command. Re-validates inside the same tick, then emits the events the
    /// command produced (a launch/complete may also hand off or close the convoy).
    Exec {
        cmd: OrchCommand,
        reply: oneshot::Sender<Result<(), AppError>>,
    },
}

#[derive(Clone)]
pub struct OrchHandle {
    tx: mpsc::Sender<OrchMsg>,
}

impl OrchHandle {
    pub async fn create_convoy(&self, convoy: impl Into<String>, members: Vec<String>) {
        let _ = self
            .tx
            .send(OrchMsg::CreateConvoy { convoy: convoy.into(), members })
            .await;
    }

    pub async fn launch(&self, convoy: impl Into<String>) {
        let _ = self.tx.send(OrchMsg::Launch { convoy: convoy.into() }).await;
    }

    pub async fn member_done(&self, convoy: impl Into<String>, member: impl Into<String>) {
        let _ = self
            .tx
            .send(OrchMsg::MemberDone { convoy: convoy.into(), member: member.into() })
            .await;
    }

    pub async fn member_fail(
        &self,
        convoy: impl Into<String>,
        member: impl Into<String>,
        reason: impl Into<String>,
    ) {
        let _ = self
            .tx
            .send(OrchMsg::MemberFail {
                convoy: convoy.into(),
                member: member.into(),
                reason: reason.into(),
            })
            .await;
    }

    pub async fn snapshot(&self) -> Vec<Convoy> {
        let (reply, rx) = oneshot::channel();
        if self.tx.send(OrchMsg::Snapshot(reply)).await.is_err() {
            return Vec::new();
        }
        rx.await.unwrap_or_default()
    }

    /// "Ask without doing": validate against the current board snapshot. The actor
    /// revalidates on `exec`, so the answer is a snapshot, not a lock.
    pub async fn validate(&self, cmd: OrchCommand) -> Result<(), AppError> {
        let (reply, rx) = oneshot::channel();
        self.tx
            .send(OrchMsg::Validate { cmd, reply })
            .await
            .map_err(|_| AppError::Other("actor gone".into()))?;
        rx.await
            .map_err(|_| AppError::Other("actor dropped reply".into()))?
    }

    /// Apply the command. The actor re-validates inside the same tick and emits the events
    /// the command produced (launch/handoff/close/fail).
    pub async fn exec(&self, cmd: OrchCommand) -> Result<(), AppError> {
        let (reply, rx) = oneshot::channel();
        self.tx
            .send(OrchMsg::Exec { cmd, reply })
            .await
            .map_err(|_| AppError::Other("actor gone".into()))?;
        rx.await
            .map_err(|_| AppError::Other("actor dropped reply".into()))?
    }
}

async fn persist_convoy<R: OrchRepository>(repo: &R, board: &ConvoyBoard, convoy: &str) {
    if let Some(c) = board.get(convoy) {
        if let Err(e) = repo.upsert_convoy(c).await {
            eprintln!("[gt-orchestration] persist convoy {convoy}: {e}");
        }
    }
}

async fn persist_event<R: OrchRepository>(repo: &R, ev: &OrchEvent) {
    let res = match ev {
        OrchEvent::ConvoyCreated { .. } => {
            // Upsert handled by caller (board read after create).
            return;
        }
        OrchEvent::ConvoyLaunched { convoy } => {
            repo.set_convoy_state(convoy, ConvoyState::Launched).await
        }
        OrchEvent::MemberDispatched { convoy, member } => {
            repo.set_member_state(convoy, member, MemberState::Active).await
        }
        OrchEvent::MemberCompleted { convoy, member } => {
            repo.set_member_state(convoy, member, MemberState::Done).await
        }
        OrchEvent::MemberFailed { convoy, member, .. } => {
            repo.set_member_state(convoy, member, MemberState::Failed).await
        }
        OrchEvent::ConvoyClosed { convoy } => {
            repo.set_convoy_state(convoy, ConvoyState::Closed).await
        }
        OrchEvent::ConvoyFailed { convoy, .. } => {
            repo.set_convoy_state(convoy, ConvoyState::Failed).await
        }
    };
    if let Err(e) = res {
        eprintln!("[gt-orchestration] persist {}: {e}", ev_kind(ev));
    }
}

fn ev_kind(ev: &OrchEvent) -> &'static str {
    match ev {
        OrchEvent::ConvoyCreated { .. } => "ConvoyCreated",
        OrchEvent::ConvoyLaunched { .. } => "ConvoyLaunched",
        OrchEvent::MemberDispatched { .. } => "MemberDispatched",
        OrchEvent::MemberCompleted { .. } => "MemberCompleted",
        OrchEvent::MemberFailed { .. } => "MemberFailed",
        OrchEvent::ConvoyClosed { .. } => "ConvoyClosed",
        OrchEvent::ConvoyFailed { .. } => "ConvoyFailed",
    }
}

/// Spawn the orchestration actor. `events` is the relay (`mpsc`) into the sync bus that the
/// composition root drains. Every state change is mirrored to the log so replay
/// reconstructs the same board; emission order is deterministic (one event per fact, in
/// message order) so the gate stays byte-identical. `repo` mirrors the same lifecycle into
/// the persistence port (Dolt in prod, in-memory otherwise).
pub fn spawn<R>(repo: R, events: mpsc::Sender<Envelope<OrchEvent>>) -> OrchHandle
where
    R: OrchRepository + 'static,
{
    spawn_hydrated(repo, events, ConvoyBoard::default())
}

/// Boot hydration (hq-8iur.1): same as [`spawn`] but seeds the actor with a pre-built
/// [`ConvoyBoard`]. The composition root passes the board reconstructed by `replay_gt` so a
/// restart preserves convoy progress (members already done, who is active) without
/// re-emitting events.
pub fn spawn_hydrated<R>(
    repo: R,
    events: mpsc::Sender<Envelope<OrchEvent>>,
    initial: ConvoyBoard,
) -> OrchHandle
where
    R: OrchRepository + 'static,
{
    let (tx, mut rx) = mpsc::channel::<OrchMsg>(64);
    tokio::spawn(async move {
        let mut board = initial;

        while let Some(msg) = rx.recv().await {
            match msg {
                OrchMsg::CreateConvoy { convoy, members } => {
                    if board.create(convoy.clone(), members.clone()).is_ok() {
                        persist_convoy(&repo, &board, &convoy).await;
                        let _ = events
                            .send(Envelope::root(OrchEvent::ConvoyCreated { convoy, members }))
                            .await;
                    }
                }
                OrchMsg::Launch { convoy } => {
                    if board.launch(&convoy).is_ok() {
                        if let Err(e) = repo.set_convoy_state(&convoy, ConvoyState::Launched).await {
                            eprintln!("[gt-orchestration] persist launch {convoy}: {e}");
                        }
                        let _ = events
                            .send(Envelope::root(OrchEvent::ConvoyLaunched {
                                convoy: convoy.clone(),
                            }))
                            .await;
                        feed_or_close(&mut board, &convoy, &events, &repo).await;
                    }
                }
                OrchMsg::MemberDone { convoy, member } => {
                    if board.complete(&convoy, &member).is_ok() {
                        if let Err(e) = repo
                            .set_member_state(&convoy, &member, MemberState::Done)
                            .await
                        {
                            eprintln!("[gt-orchestration] persist done {convoy}/{member}: {e}");
                        }
                        let _ = events
                            .send(Envelope::root(OrchEvent::MemberCompleted {
                                convoy: convoy.clone(),
                                member,
                            }))
                            .await;
                        feed_or_close(&mut board, &convoy, &events, &repo).await;
                    }
                }
                OrchMsg::MemberFail { convoy, member, reason } => {
                    if board.fail(&convoy, &member).is_ok() {
                        if let Err(e) = repo
                            .set_member_state(&convoy, &member, MemberState::Failed)
                            .await
                        {
                            eprintln!("[gt-orchestration] persist fail {convoy}/{member}: {e}");
                        }
                        let _ = events
                            .send(Envelope::root(OrchEvent::MemberFailed {
                                convoy: convoy.clone(),
                                member: member.clone(),
                                reason: reason.clone(),
                            }))
                            .await;
                        if board.mark_failed(&convoy).is_ok() {
                            if let Err(e) = repo
                                .set_convoy_state(&convoy, ConvoyState::Failed)
                                .await
                            {
                                eprintln!(
                                    "[gt-orchestration] persist convoy fail {convoy}: {e}"
                                );
                            }
                            let _ = events
                                .send(Envelope::root(OrchEvent::ConvoyFailed {
                                    convoy,
                                    member,
                                    reason,
                                }))
                                .await;
                        }
                    }
                }
                OrchMsg::Snapshot(reply) => {
                    let _ = reply.send(board.snapshot());
                }
                OrchMsg::Validate { cmd, reply } => {
                    let _ = reply.send(cmd.validate(&board));
                }
                OrchMsg::Exec { cmd, reply } => {
                    // execute() re-validates first → no TOCTOU within the actor tick. It
                    // returns the events to relay (launch + handoff, or complete + close).
                    match cmd.execute(&mut board) {
                        Ok(evs) => {
                            for ev in evs {
                                if let OrchEvent::ConvoyCreated { convoy, .. } = &ev {
                                    persist_convoy(&repo, &board, convoy).await;
                                } else {
                                    persist_event(&repo, &ev).await;
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
    OrchHandle { tx }
}

/// Advance a launched convoy: if all members are done, close it; otherwise feed the next
/// pending member (the handoff). At most one event is emitted — the convoy is sequential.
async fn feed_or_close<R: OrchRepository>(
    board: &mut ConvoyBoard,
    convoy: &str,
    events: &mpsc::Sender<Envelope<OrchEvent>>,
    repo: &R,
) {
    if board.all_done(convoy) {
        if board.close(convoy).is_ok() {
            if let Err(e) = repo.set_convoy_state(convoy, ConvoyState::Closed).await {
                eprintln!("[gt-orchestration] persist close {convoy}: {e}");
            }
            let _ = events
                .send(Envelope::root(OrchEvent::ConvoyClosed { convoy: convoy.into() }))
                .await;
        }
    } else if let Some(member) = board.dispatch_next(convoy) {
        if let Err(e) = repo
            .set_member_state(convoy, &member, MemberState::Active)
            .await
        {
            eprintln!("[gt-orchestration] persist dispatch {convoy}/{member}: {e}");
        }
        let _ = events
            .send(Envelope::root(OrchEvent::MemberDispatched {
                convoy: convoy.into(),
                member,
            }))
            .await;
    }
}
