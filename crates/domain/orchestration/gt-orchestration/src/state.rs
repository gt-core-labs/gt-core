use std::collections::BTreeMap;

use gt_events::AppError;

use crate::events::OrchEvent;

/// Lifecycle of one convoy member. State machine **explicit**: illegal moves are rejected
/// with `AppError::InvalidTransition` (rule of `docs/06-observability.md`).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MemberState {
    Pending,
    Active,
    Done,
    Failed,
}

impl MemberState {
    pub fn as_str(self) -> &'static str {
        match self {
            MemberState::Pending => "pending",
            MemberState::Active => "active",
            MemberState::Done => "done",
            MemberState::Failed => "failed",
        }
    }
}

/// Lifecycle of a convoy. `Staged → Launched → Closed | Failed`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ConvoyState {
    Staged,
    Launched,
    Closed,
    Failed,
}

impl ConvoyState {
    pub fn as_str(self) -> &'static str {
        match self {
            ConvoyState::Staged => "staged",
            ConvoyState::Launched => "launched",
            ConvoyState::Closed => "closed",
            ConvoyState::Failed => "failed",
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Member {
    pub bead: String,
    pub state: MemberState,
}

impl Member {
    fn transition(&mut self, to: MemberState) -> Result<(), AppError> {
        let ok = matches!(
            (self.state, to),
            (MemberState::Pending, MemberState::Active)
                | (MemberState::Active, MemberState::Done)
                | (MemberState::Active, MemberState::Failed)
        );
        if ok {
            self.state = to;
            Ok(())
        } else {
            Err(AppError::InvalidTransition(format!(
                "member {}: {} → {}",
                self.bead,
                self.state.as_str(),
                to.as_str()
            )))
        }
    }
}

/// One convoy: an ordered set of member beads plus the convoy's own state. Sequential by
/// design — at most one member `Active` at a time, the next is fed only after the prior
/// finishes (the handoff). Pure: no clock reads, advances on facts only.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Convoy {
    pub id: String,
    pub state: ConvoyState,
    pub members: Vec<Member>,
}

impl Convoy {
    pub fn new(id: impl Into<String>, members: Vec<String>) -> Self {
        Self {
            id: id.into(),
            state: ConvoyState::Staged,
            members: members
                .into_iter()
                .map(|bead| Member { bead, state: MemberState::Pending })
                .collect(),
        }
    }

    /// `Staged → Launched`. Any other source state is illegal.
    pub fn launch(&mut self) -> Result<(), AppError> {
        self.transition(ConvoyState::Launched)
    }

    /// `Launched → Closed`. Caller is expected to check [`Convoy::all_done`] first.
    pub fn close(&mut self) -> Result<(), AppError> {
        self.transition(ConvoyState::Closed)
    }

    /// `Staged | Launched → Failed`.
    pub fn mark_failed(&mut self) -> Result<(), AppError> {
        let ok = matches!(
            (self.state, ConvoyState::Failed),
            (ConvoyState::Staged, _) | (ConvoyState::Launched, _)
        );
        if ok {
            self.state = ConvoyState::Failed;
            Ok(())
        } else {
            Err(AppError::InvalidTransition(format!(
                "convoy {}: {} → failed",
                self.id,
                self.state.as_str()
            )))
        }
    }

    fn transition(&mut self, to: ConvoyState) -> Result<(), AppError> {
        let ok = matches!(
            (self.state, to),
            (ConvoyState::Staged, ConvoyState::Launched)
                | (ConvoyState::Launched, ConvoyState::Closed)
        );
        if ok {
            self.state = to;
            Ok(())
        } else {
            Err(AppError::InvalidTransition(format!(
                "convoy {}: {} → {}",
                self.id,
                self.state.as_str(),
                to.as_str()
            )))
        }
    }

    fn member_mut(&mut self, bead: &str) -> Result<&mut Member, AppError> {
        let id = self.id.clone();
        self.members
            .iter_mut()
            .find(|m| m.bead == bead)
            .ok_or_else(move || AppError::NotFound(format!("convoy {id}: member {bead}")))
    }

    /// Feed the next ready member: mark the first `Pending` member `Active` and return its
    /// bead id. Returns `None` if a member is already `Active` (sequential convoy) or none
    /// remain pending. The handoff is what the composition root turns into a `gt sling`.
    pub fn dispatch_next(&mut self) -> Option<String> {
        if self.members.iter().any(|m| m.state == MemberState::Active) {
            return None;
        }
        let next = self
            .members
            .iter_mut()
            .find(|m| m.state == MemberState::Pending)?;
        // Pending → Active always legal; transition() can't fail here.
        next.transition(MemberState::Active).ok()?;
        Some(next.bead.clone())
    }

    pub fn complete(&mut self, bead: &str) -> Result<(), AppError> {
        self.member_mut(bead)?.transition(MemberState::Done)
    }

    pub fn fail(&mut self, bead: &str) -> Result<(), AppError> {
        self.member_mut(bead)?.transition(MemberState::Failed)
    }

    /// Every member reached `Done`. An empty convoy is trivially done.
    pub fn all_done(&self) -> bool {
        self.members.iter().all(|m| m.state == MemberState::Done)
    }

    // --- unconditional setters used by the replay reducer (folds are total) ---

    fn set_member(&mut self, bead: &str, to: MemberState) {
        if let Some(m) = self.members.iter_mut().find(|m| m.bead == bead) {
            m.state = to;
        }
    }

    fn set_state(&mut self, to: ConvoyState) {
        self.state = to;
    }
}

/// The live set of convoys, owned by the orchestration actor. `BTreeMap` for stable
/// iteration (deterministic snapshots for tests and replay).
#[derive(Debug, Default)]
pub struct ConvoyBoard {
    convoys: BTreeMap<String, Convoy>,
}

impl ConvoyBoard {
    /// Register a new `Staged` convoy. Re-creating the same id is a `Validation` error.
    pub fn create(&mut self, id: String, members: Vec<String>) -> Result<(), AppError> {
        if self.convoys.contains_key(&id) {
            return Err(AppError::Validation(format!("convoy {id} already exists")));
        }
        self.convoys.insert(id.clone(), Convoy::new(id, members));
        Ok(())
    }

    pub fn launch(&mut self, id: &str) -> Result<(), AppError> {
        self.get_mut(id)?.launch()
    }

    pub fn dispatch_next(&mut self, id: &str) -> Option<String> {
        self.convoys.get_mut(id)?.dispatch_next()
    }

    pub fn complete(&mut self, id: &str, member: &str) -> Result<(), AppError> {
        self.get_mut(id)?.complete(member)
    }

    pub fn fail(&mut self, id: &str, member: &str) -> Result<(), AppError> {
        self.get_mut(id)?.fail(member)
    }

    pub fn all_done(&self, id: &str) -> bool {
        self.convoys.get(id).map(|c| c.all_done()).unwrap_or(false)
    }

    pub fn close(&mut self, id: &str) -> Result<(), AppError> {
        self.get_mut(id)?.close()
    }

    pub fn mark_failed(&mut self, id: &str) -> Result<(), AppError> {
        self.get_mut(id)?.mark_failed()
    }

    fn get_mut(&mut self, id: &str) -> Result<&mut Convoy, AppError> {
        self.convoys
            .get_mut(id)
            .ok_or_else(|| AppError::NotFound(format!("convoy {id}")))
    }

    pub fn get(&self, id: &str) -> Option<&Convoy> {
        self.convoys.get(id)
    }

    pub fn snapshot(&self) -> Vec<Convoy> {
        self.convoys.values().cloned().collect()
    }

    pub fn len(&self) -> usize {
        self.convoys.len()
    }

    pub fn is_empty(&self) -> bool {
        self.convoys.is_empty()
    }

    /// Rebuild a live convoy board from the replay reducer (boot hydration, hq-8iur.1). The
    /// event log is authoritative; the actor seeds itself from `replay_gt` so restart keeps
    /// in-flight convoy progress without re-emitting events.
    pub fn from_state(state: &OrchState) -> Self {
        let mut board = ConvoyBoard::default();
        for (id, convoy) in &state.convoys {
            board.convoys.insert(id.clone(), convoy.clone());
        }
        board
    }
}

/// Replay reducer: re-runs the log and rebuilds the convoy board + the handoff/outcome
/// trails. Pure and total — events in order, unconditional folds (validation lived on the
/// write path). Reuses [`Convoy`] as the deterministic snapshot type so `PartialEq` holds
/// byte-for-byte across runs (gate of Paso 3 applied to the orch subset).
#[derive(Debug, Default, PartialEq, Eq)]
pub struct OrchState {
    pub convoys: BTreeMap<String, Convoy>,
    /// `(convoy, member)` in dispatch order — the handoff trail.
    pub dispatched: Vec<(String, String)>,
    pub closed: Vec<String>,
    /// `(convoy, member, reason)` for convoys halted by a member failure.
    pub failed: Vec<(String, String, String)>,
}

impl OrchState {
    pub fn apply(&mut self, event: &OrchEvent) {
        match event {
            OrchEvent::ConvoyCreated { convoy, members } => {
                self.convoys
                    .insert(convoy.clone(), Convoy::new(convoy.clone(), members.clone()));
            }
            OrchEvent::ConvoyLaunched { convoy } => {
                self.with(convoy, |c| c.set_state(ConvoyState::Launched));
            }
            OrchEvent::MemberDispatched { convoy, member } => {
                self.with(convoy, |c| c.set_member(member, MemberState::Active));
                self.dispatched.push((convoy.clone(), member.clone()));
            }
            OrchEvent::MemberCompleted { convoy, member } => {
                self.with(convoy, |c| c.set_member(member, MemberState::Done));
            }
            OrchEvent::MemberFailed { convoy, member, .. } => {
                self.with(convoy, |c| c.set_member(member, MemberState::Failed));
            }
            OrchEvent::ConvoyClosed { convoy } => {
                self.with(convoy, |c| c.set_state(ConvoyState::Closed));
                self.closed.push(convoy.clone());
            }
            OrchEvent::ConvoyFailed { convoy, member, reason } => {
                self.with(convoy, |c| c.set_state(ConvoyState::Failed));
                self.failed
                    .push((convoy.clone(), member.clone(), reason.clone()));
            }
        }
    }

    fn with(&mut self, convoy: &str, f: impl FnOnce(&mut Convoy)) {
        if let Some(c) = self.convoys.get_mut(convoy) {
            f(c);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn members(c: &Convoy) -> Vec<(String, MemberState)> {
        c.members.iter().map(|m| (m.bead.clone(), m.state)).collect()
    }

    #[test]
    fn member_transitions_legal_only() {
        let mut m = Member { bead: "m1".into(), state: MemberState::Pending };
        m.transition(MemberState::Active).unwrap();
        m.transition(MemberState::Done).unwrap();
        // Done is terminal.
        assert!(m.transition(MemberState::Failed).is_err());
        // Pending → Done (skipping Active) is illegal.
        let mut m2 = Member { bead: "m2".into(), state: MemberState::Pending };
        assert!(m2.transition(MemberState::Done).is_err());
    }

    #[test]
    fn convoy_drives_sequentially() {
        let mut c = Convoy::new("cv", vec!["a".into(), "b".into(), "c".into()]);
        assert_eq!(c.state, ConvoyState::Staged);
        c.launch().unwrap();
        assert!(c.launch().is_err(), "double launch illegal");

        // First handoff: a goes Active. b/c stay Pending (sequential).
        assert_eq!(c.dispatch_next(), Some("a".into()));
        assert_eq!(c.dispatch_next(), None, "a still active: no second dispatch");

        c.complete("a").unwrap();
        assert!(!c.all_done());
        assert_eq!(c.dispatch_next(), Some("b".into()));
        c.complete("b").unwrap();
        assert_eq!(c.dispatch_next(), Some("c".into()));
        c.complete("c").unwrap();

        assert!(c.all_done());
        c.close().unwrap();
        assert_eq!(c.state, ConvoyState::Closed);
    }

    #[test]
    fn convoy_member_failure_halts() {
        let mut c = Convoy::new("cv", vec!["a".into(), "b".into()]);
        c.launch().unwrap();
        c.dispatch_next().unwrap();
        c.fail("a").unwrap();
        c.mark_failed().unwrap();
        assert_eq!(c.state, ConvoyState::Failed);
    }

    #[test]
    fn board_rejects_duplicate_convoy() {
        let mut b = ConvoyBoard::default();
        b.create("cv".into(), vec!["a".into()]).unwrap();
        assert!(b.create("cv".into(), vec!["a".into()]).is_err());
    }

    #[test]
    fn replay_reducer_rebuilds_state() {
        let log = vec![
            OrchEvent::ConvoyCreated { convoy: "cv".into(), members: vec!["a".into(), "b".into()] },
            OrchEvent::ConvoyLaunched { convoy: "cv".into() },
            OrchEvent::MemberDispatched { convoy: "cv".into(), member: "a".into() },
            OrchEvent::MemberCompleted { convoy: "cv".into(), member: "a".into() },
            OrchEvent::MemberDispatched { convoy: "cv".into(), member: "b".into() },
            OrchEvent::MemberCompleted { convoy: "cv".into(), member: "b".into() },
            OrchEvent::ConvoyClosed { convoy: "cv".into() },
        ];
        let mut s = OrchState::default();
        for e in &log {
            s.apply(e);
        }
        assert_eq!(s.dispatched, vec![("cv".into(), "a".into()), ("cv".into(), "b".into())]);
        assert_eq!(s.closed, vec!["cv".to_string()]);
        assert!(s.failed.is_empty());
        let c = &s.convoys["cv"];
        assert_eq!(c.state, ConvoyState::Closed);
        assert_eq!(
            members(c),
            vec![("a".into(), MemberState::Done), ("b".into(), MemberState::Done)]
        );
    }

    #[test]
    fn replay_reducer_records_failure() {
        let log = vec![
            OrchEvent::ConvoyCreated { convoy: "cv".into(), members: vec!["a".into()] },
            OrchEvent::ConvoyLaunched { convoy: "cv".into() },
            OrchEvent::MemberDispatched { convoy: "cv".into(), member: "a".into() },
            OrchEvent::MemberFailed { convoy: "cv".into(), member: "a".into(), reason: "boom".into() },
            OrchEvent::ConvoyFailed { convoy: "cv".into(), member: "a".into(), reason: "boom".into() },
        ];
        let mut s = OrchState::default();
        for e in &log {
            s.apply(e);
        }
        assert_eq!(s.failed, vec![("cv".into(), "a".into(), "boom".into())]);
        assert!(s.closed.is_empty());
        assert_eq!(s.convoys["cv"].state, ConvoyState::Failed);
    }
}
