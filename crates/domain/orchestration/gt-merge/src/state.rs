use std::collections::BTreeMap;

use gt_events::AppError;

use crate::events::MergeEvent;

/// Ciclo de vida de un slot de merge. State machine **explícita**: transiciones ilegales
/// se rechazan con `AppError::InvalidTransition` (regla de `docs/06-observability.md`).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MergeSlotState {
    Ready,
    Merging,
    Merged,
    Failed,
}

impl MergeSlotState {
    pub fn as_str(self) -> &'static str {
        match self {
            MergeSlotState::Ready => "ready",
            MergeSlotState::Merging => "merging",
            MergeSlotState::Merged => "merged",
            MergeSlotState::Failed => "failed",
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MergeSlot {
    pub bead: String,
    pub branch: String,
    pub state: MergeSlotState,
}

impl MergeSlot {
    pub fn new(bead: impl Into<String>, branch: impl Into<String>) -> Self {
        Self {
            bead: bead.into(),
            branch: branch.into(),
            state: MergeSlotState::Ready,
        }
    }

    /// `Ready → Merging`, `Merging → Merged | Failed`. Cualquier otra es ilegal.
    pub fn transition(&mut self, to: MergeSlotState) -> Result<(), AppError> {
        let ok = matches!(
            (self.state, to),
            (MergeSlotState::Ready, MergeSlotState::Merging)
                | (MergeSlotState::Merging, MergeSlotState::Merged)
                | (MergeSlotState::Merging, MergeSlotState::Failed)
        );
        if ok {
            self.state = to;
            Ok(())
        } else {
            Err(AppError::InvalidTransition(format!(
                "merge slot {}: {} → {}",
                self.bead,
                self.state.as_str(),
                to.as_str()
            )))
        }
    }
}

/// Set de slots vivos del merge, indexado por bead. `BTreeMap` para iteración estable
/// (snapshot determinista para tests y replay).
#[derive(Debug, Default)]
pub struct MergeBoard {
    slots: BTreeMap<String, MergeSlot>,
}

impl MergeBoard {
    /// Registra un nuevo slot en estado `Ready`. Re-submit del mismo bead = `Validation`
    /// (la refinery ya marcó este como pendiente; no se duplica).
    pub fn submit(&mut self, bead: String, branch: String) -> Result<(), AppError> {
        if self.slots.contains_key(&bead) {
            return Err(AppError::Validation(format!(
                "merge slot for {bead} already submitted"
            )));
        }
        self.slots.insert(bead.clone(), MergeSlot::new(bead, branch));
        Ok(())
    }

    pub fn start(&mut self, bead: &str) -> Result<(), AppError> {
        self.transition(bead, MergeSlotState::Merging)
    }

    pub fn complete(&mut self, bead: &str) -> Result<(), AppError> {
        self.transition(bead, MergeSlotState::Merged)
    }

    pub fn fail(&mut self, bead: &str) -> Result<(), AppError> {
        self.transition(bead, MergeSlotState::Failed)
    }

    fn transition(&mut self, bead: &str, to: MergeSlotState) -> Result<(), AppError> {
        let slot = self
            .slots
            .get_mut(bead)
            .ok_or_else(|| AppError::NotFound(format!("merge slot {bead}")))?;
        slot.transition(to)
    }

    pub fn get(&self, bead: &str) -> Option<&MergeSlot> {
        self.slots.get(bead)
    }

    pub fn snapshot(&self) -> Vec<MergeSlot> {
        self.slots.values().cloned().collect()
    }

    pub fn len(&self) -> usize {
        self.slots.len()
    }

    pub fn is_empty(&self) -> bool {
        self.slots.is_empty()
    }

    /// Rebuild a live board from the replay reducer's snapshot (boot hydration, hq-8iur.1).
    /// The audit log is authoritative: the actor seeds its owned state from the result of
    /// `replay_gt` so a restart restores in-flight merge slots without re-emitting events.
    pub fn from_state(state: &MergeState) -> Self {
        let mut board = MergeBoard::default();
        for slot in state.board.slots() {
            board.slots.insert(slot.bead.clone(), slot.clone());
        }
        board
    }
}

/// Replay reducer: re-corre el log y reconstruye el board + listas de terminados. Puro y
/// total — eventos en orden, pliegues incondicionales (la validación vivió en escritura).
#[derive(Debug, Default, PartialEq, Eq)]
pub struct MergeState {
    pub board: BoardSnapshot,
    pub merged: Vec<(String, String)>,
    pub failed: Vec<(String, String)>,
}

impl MergeState {
    pub fn apply(&mut self, event: &MergeEvent) {
        match event {
            MergeEvent::Ready { bead, branch, .. } => {
                self.board.upsert(bead.clone(), branch.clone(), MergeSlotState::Ready);
            }
            MergeEvent::Started { bead } => {
                self.board.set_state(bead, MergeSlotState::Merging);
            }
            MergeEvent::Merged { bead, sha } => {
                self.board.set_state(bead, MergeSlotState::Merged);
                self.merged.push((bead.clone(), sha.clone()));
            }
            MergeEvent::Failed { bead, reason } => {
                self.board.set_state(bead, MergeSlotState::Failed);
                self.failed.push((bead.clone(), reason.clone()));
            }
        }
    }
}

/// Vista determinista del board para replay/tests: `Vec` ordenado por bead, derivable
/// con `PartialEq` byte a byte.
#[derive(Debug, Default, PartialEq, Eq)]
pub struct BoardSnapshot {
    slots: Vec<MergeSlot>,
}

impl BoardSnapshot {
    fn pos(&self, bead: &str) -> Result<usize, usize> {
        self.slots.binary_search_by(|s| s.bead.as_str().cmp(bead))
    }

    fn upsert(&mut self, bead: String, branch: String, state: MergeSlotState) {
        let slot = MergeSlot { bead: bead.clone(), branch, state };
        match self.pos(&bead) {
            Ok(i) => self.slots[i] = slot,
            Err(i) => self.slots.insert(i, slot),
        }
    }

    fn set_state(&mut self, bead: &str, state: MergeSlotState) {
        if let Ok(i) = self.pos(bead) {
            self.slots[i].state = state;
        }
    }

    pub fn slots(&self) -> &[MergeSlot] {
        &self.slots
    }

    pub fn len(&self) -> usize {
        self.slots.len()
    }

    pub fn is_empty(&self) -> bool {
        self.slots.is_empty()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn slot_transitions_legal_only() {
        let mut s = MergeSlot::new("b1", "feat/x");
        s.transition(MergeSlotState::Merging).unwrap();
        s.transition(MergeSlotState::Merged).unwrap();
        let mut s2 = MergeSlot::new("b2", "feat/y");
        // Ready → Merged (saltando Merging) es ilegal.
        assert!(s2.transition(MergeSlotState::Merged).is_err());
        // Merged es terminal.
        let mut s3 = s.clone();
        assert!(s3.transition(MergeSlotState::Merging).is_err());
    }

    #[test]
    fn board_submit_then_drive_to_merged() {
        let mut b = MergeBoard::default();
        b.submit("b1".into(), "feat/x".into()).unwrap();
        assert!(b.submit("b1".into(), "feat/x".into()).is_err(), "no duplicates");
        b.start("b1").unwrap();
        b.complete("b1").unwrap();
        assert_eq!(b.get("b1").unwrap().state, MergeSlotState::Merged);
    }

    #[test]
    fn board_fail_path() {
        let mut b = MergeBoard::default();
        b.submit("b1".into(), "feat/x".into()).unwrap();
        b.start("b1").unwrap();
        b.fail("b1").unwrap();
        assert_eq!(b.get("b1").unwrap().state, MergeSlotState::Failed);
    }

    #[test]
    fn replay_reducer_rebuilds_state() {
        let log = vec![
            MergeEvent::Ready { bead: "b1".into(), branch: "feat/x".into(), channel_msg_id: "01".into() },
            MergeEvent::Started { bead: "b1".into() },
            MergeEvent::Merged { bead: "b1".into(), sha: "abc".into() },
            MergeEvent::Ready { bead: "b2".into(), branch: "feat/y".into(), channel_msg_id: "02".into() },
            MergeEvent::Started { bead: "b2".into() },
            MergeEvent::Failed { bead: "b2".into(), reason: "conflict".into() },
        ];
        let mut s = MergeState::default();
        for e in &log {
            s.apply(e);
        }
        assert_eq!(s.merged, vec![("b1".into(), "abc".into())]);
        assert_eq!(s.failed, vec![("b2".into(), "conflict".into())]);
        assert_eq!(s.board.len(), 2);
        let states: Vec<_> = s.board.slots().iter().map(|sl| (sl.bead.clone(), sl.state)).collect();
        assert_eq!(
            states,
            vec![
                ("b1".into(), MergeSlotState::Merged),
                ("b2".into(), MergeSlotState::Failed),
            ]
        );
    }
}
