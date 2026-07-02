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

/// Cap on operator/sheriff `reset`s of a slot (gtcore-4ad682, acceptance #4): the `failed →
/// ready` re-queue is bounded so a branch that keeps failing the merge cannot be reset in an
/// infinite loop. The counter accumulates across retry cycles (`reset → merging → failed →
/// reset …`) and is only zeroed by a genuine re-`submit` (a fresh branch push = a new attempt
/// lineage). Once a slot reaches this many resets, `merge.reset` is rejected and a human must
/// re-`submit` a fixed branch (or `merge.complete` it) instead of blindly retrying.
pub const MAX_MERGE_RETRIES: u32 = 3;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MergeSlot {
    pub bead: String,
    pub branch: String,
    pub state: MergeSlotState,
    /// Number of `failed → ready` resets applied to this slot since the last `submit`
    /// (gtcore-4ad682). Bounds the retry loop against [`MAX_MERGE_RETRIES`]. Derived from the
    /// event log (`merge.reset.v1` events), so it survives replay.
    pub retries: u32,
}

impl MergeSlot {
    pub fn new(bead: impl Into<String>, branch: impl Into<String>) -> Self {
        Self {
            bead: bead.into(),
            branch: branch.into(),
            state: MergeSlotState::Ready,
            retries: 0,
        }
    }

    /// `Ready → Merging`, `Merging → Merged | Failed`, `Failed → Ready`. Cualquier otra es
    /// ilegal. `Failed → Ready` (gtcore-4ad682) es la salida operador/sheriff de un slot en
    /// `failed` — antes `failed` era terminal y el trabajo LISTO no se podía re-encolar.
    pub fn transition(&mut self, to: MergeSlotState) -> Result<(), AppError> {
        let ok = matches!(
            (self.state, to),
            (MergeSlotState::Ready, MergeSlotState::Merging)
                | (MergeSlotState::Merging, MergeSlotState::Merged)
                | (MergeSlotState::Merging, MergeSlotState::Failed)
                | (MergeSlotState::Failed, MergeSlotState::Ready)
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
    ///
    /// EXCEPCIÓN (gtcore-3a1bd4): un slot ya `Failed` SÍ puede re-entrar la cola — se sobrescribe
    /// de vuelta a `Ready`. Cuando la CI del PR de un bead falla, el bead se re-despacha, el agente
    /// arregla el fallo y vuelve a llamar `merge_submit` sobre la MISMA rama; sin esta excepción ese
    /// re-submit chocaría con "already submitted" y el lazo no cerraría. La rama arreglada vuelve a
    /// pasar por la cola (`Ready → Merging → Merged`), respetando la invariante A5 (la cola es el
    /// único camino a `main`). En replay, `MergeEvent::Ready` ya hace `upsert` a `Ready`, así que
    /// la secuencia `…Failed, Ready` reconstruye un slot `Ready` sin evento nuevo. Un slot en
    /// `Ready`/`Merging` (en vuelo) o `Merged` (terminal) sigue rechazando el duplicado.
    pub fn submit(&mut self, bead: String, branch: String) -> Result<(), AppError> {
        if let Some(existing) = self.slots.get(&bead) {
            if existing.state != MergeSlotState::Failed {
                return Err(AppError::Validation(format!(
                    "merge slot for {bead} already submitted"
                )));
            }
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

    /// Operator/sheriff re-queue of a `Failed` slot: `Failed → Ready` + bump the retry counter
    /// (gtcore-4ad682). The state-machine legality (`slot` exists and is `Failed`) is enforced by
    /// [`MergeSlot::transition`]; the retry CAP is enforced one layer up in
    /// [`crate::commands::ResetMerge`] so replay (which folds already-emitted events
    /// unconditionally) never re-checks a bound the write path already passed.
    pub fn reset(&mut self, bead: &str) -> Result<(), AppError> {
        let slot = self
            .slots
            .get_mut(bead)
            .ok_or_else(|| AppError::NotFound(format!("merge slot {bead}")))?;
        slot.transition(MergeSlotState::Ready)?;
        slot.retries = slot.retries.saturating_add(1);
        Ok(())
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

    /// Rebuild a live board from a [`MergeRepository`](crate::MergeRepository) projection
    /// (the REST / dashboard read path, `hq-fe-api-orch.2`). Each persisted [`MergeSlot`]
    /// already carries its current state, so the board is reconstituted directly — the same
    /// decide-against-live-state hydration `from_state` does for replay, but seeded from the
    /// repo's `list_slots` instead of the event log. A `Command` can then validate its
    /// transition against the rehydrated board before the touched slot is upserted back.
    pub fn from_slots(slots: impl IntoIterator<Item = MergeSlot>) -> Self {
        let mut board = MergeBoard::default();
        for slot in slots {
            board.slots.insert(slot.bead.clone(), slot);
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
            MergeEvent::Reset { bead } => {
                self.board.reset(bead);
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
        // A (re-)submit is a fresh attempt lineage: the retry counter is born/reset to 0. The
        // reset counter only accumulates via `reset` (below), never across a re-`submit`.
        let slot = MergeSlot { bead: bead.clone(), branch, state, retries: 0 };
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

    /// Fold a `merge.reset.v1` event: `Failed → Ready` + bump the retry counter. Total (no
    /// cap re-check) so replay stays deterministic — the cap was enforced at write time.
    fn reset(&mut self, bead: &str) {
        if let Ok(i) = self.pos(bead) {
            self.slots[i].state = MergeSlotState::Ready;
            self.slots[i].retries = self.slots[i].retries.saturating_add(1);
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
    fn failed_slot_may_resubmit_into_the_queue() {
        // gtcore-3a1bd4: a CI-failed slot re-enters the queue when the bead is re-dispatched and the
        // agent re-submits the fixed branch. The re-submit overwrites the Failed slot back to Ready
        // (born-again through the queue), then drives Ready → Merging → Merged cleanly — so the
        // eventual `complete` is legal (it would be illegal on the terminal Failed slot).
        let mut b = MergeBoard::default();
        b.submit("b1".into(), "feat/x".into()).unwrap();
        b.start("b1").unwrap();
        b.fail("b1").unwrap();
        assert_eq!(b.get("b1").unwrap().state, MergeSlotState::Failed);

        // Re-submit of the FAILED slot is allowed and resets it to Ready.
        b.submit("b1".into(), "feat/x".into()).unwrap();
        assert_eq!(b.get("b1").unwrap().state, MergeSlotState::Ready);
        // The fixed branch goes through the queue to Merged.
        b.start("b1").unwrap();
        b.complete("b1").unwrap();
        assert_eq!(b.get("b1").unwrap().state, MergeSlotState::Merged);
    }

    #[test]
    fn in_flight_and_merged_slots_still_reject_resubmit() {
        // The Failed exception is narrow: a slot in flight (Ready/Merging) or terminal (Merged) is a
        // genuine duplicate and stays rejected, so a stray re-submit never disturbs a live merge.
        let mut b = MergeBoard::default();
        b.submit("b1".into(), "feat/x".into()).unwrap();
        assert!(b.submit("b1".into(), "feat/x".into()).is_err(), "Ready rejects re-submit");
        b.start("b1").unwrap();
        assert!(b.submit("b1".into(), "feat/x".into()).is_err(), "Merging rejects re-submit");
        b.complete("b1").unwrap();
        assert!(b.submit("b1".into(), "feat/x".into()).is_err(), "Merged rejects re-submit");
    }

    #[test]
    fn reset_cycles_failed_to_ready_to_merged_and_counts_retries() {
        // gtcore-4ad682 acceptance #3: a failed slot re-queues via `reset` and drives the full
        // failed → ready → merging → merged cycle. The retry counter accrues per reset.
        let mut b = MergeBoard::default();
        b.submit("b1".into(), "feat/x".into()).unwrap();
        b.start("b1").unwrap();
        b.fail("b1").unwrap();
        assert_eq!(b.get("b1").unwrap().state, MergeSlotState::Failed);
        assert_eq!(b.get("b1").unwrap().retries, 0);

        // Reset (operator/sheriff): Failed → Ready, retries bumped.
        b.reset("b1").unwrap();
        assert_eq!(b.get("b1").unwrap().state, MergeSlotState::Ready);
        assert_eq!(b.get("b1").unwrap().retries, 1);

        // The re-queued branch drives cleanly to Merged.
        b.start("b1").unwrap();
        b.complete("b1").unwrap();
        assert_eq!(b.get("b1").unwrap().state, MergeSlotState::Merged);
    }

    #[test]
    fn reset_rejects_non_failed_slots() {
        // `reset` is Failed → Ready only. A Ready / Merging / Merged slot rejects it (the
        // state-machine legality that keeps the queue the only path to main intact).
        let mut b = MergeBoard::default();
        b.submit("b1".into(), "feat/x".into()).unwrap();
        assert!(b.reset("b1").is_err(), "Ready slot cannot be reset");
        b.start("b1").unwrap();
        assert!(b.reset("b1").is_err(), "Merging slot cannot be reset");
        b.complete("b1").unwrap();
        assert!(b.reset("b1").is_err(), "Merged slot cannot be reset");
        assert!(b.reset("ghost").is_err(), "unknown slot cannot be reset");
    }

    #[test]
    fn resubmit_zeroes_the_retry_counter() {
        // A genuine re-submit is a fresh branch attempt: it resets the retry lineage to 0, so
        // the reset cap bounds only blind resets of the *same* failed branch.
        let mut b = MergeBoard::default();
        b.submit("b1".into(), "feat/x".into()).unwrap();
        b.start("b1").unwrap();
        b.fail("b1").unwrap();
        b.reset("b1").unwrap();
        assert_eq!(b.get("b1").unwrap().retries, 1);
        b.start("b1").unwrap();
        b.fail("b1").unwrap();
        // Re-submit the fixed branch — counter is born again at 0.
        b.submit("b1".into(), "feat/x2".into()).unwrap();
        assert_eq!(b.get("b1").unwrap().state, MergeSlotState::Ready);
        assert_eq!(b.get("b1").unwrap().retries, 0);
    }

    #[test]
    fn reset_event_replays_deterministically() {
        // The reset counter is derived from the log: a `Reset` event folds to Ready + retries+1,
        // so a replay reconstructs the same slot the live board held.
        let log = vec![
            MergeEvent::Ready { bead: "b1".into(), branch: "feat/x".into(), channel_msg_id: "01".into() },
            MergeEvent::Started { bead: "b1".into() },
            MergeEvent::Failed { bead: "b1".into(), reason: "conflict".into() },
            MergeEvent::Reset { bead: "b1".into() },
            MergeEvent::Started { bead: "b1".into() },
            MergeEvent::Failed { bead: "b1".into(), reason: "conflict again".into() },
            MergeEvent::Reset { bead: "b1".into() },
        ];
        let mut s = MergeState::default();
        for e in &log {
            s.apply(e);
        }
        let slot = &s.board.slots()[0];
        assert_eq!(slot.state, MergeSlotState::Ready);
        assert_eq!(slot.retries, 2, "two resets fold to retries=2");
    }

    #[test]
    fn from_slots_rehydrates_states_for_command_validation() {
        // A repo projection carries each slot's *current* state, not just `Ready`. The board
        // must come back with those states so a follow-up command validates correctly.
        let board = MergeBoard::from_slots([
            MergeSlot { bead: "b1".into(), branch: "feat/x".into(), state: MergeSlotState::Merging, retries: 0 },
            MergeSlot { bead: "b2".into(), branch: "feat/y".into(), state: MergeSlotState::Ready, retries: 0 },
        ]);
        assert_eq!(board.get("b1").unwrap().state, MergeSlotState::Merging);
        assert_eq!(board.get("b2").unwrap().state, MergeSlotState::Ready);
        // `complete` is legal on the rehydrated Merging slot, illegal on the Ready one.
        let mut b = MergeBoard::from_slots(board.snapshot());
        b.complete("b1").unwrap();
        assert!(b.complete("b2").is_err(), "Ready → Merged stays illegal after hydration");
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

    /// A5 (gtcore-f3a016) — **the merge queue is the only path to `main`**, codified as a slot
    /// state-machine invariant. `Merged` is the "landed on main" terminal; a branch can only
    /// reach it by passing through the queue: `submit` (→ `Ready`) → `start` (→ `Merging`) →
    /// `complete` (→ `Merged`). This test pins that there is NO transition that jumps the queue:
    /// `Merged` is reachable *exclusively* from `Merging`, `Merging` *exclusively* from `Ready`,
    /// and a slot is *born* `Ready` (so no `Merging`/`Merged` slot can be fabricated off-queue).
    #[test]
    fn merge_queue_is_the_only_path_to_main() {
        use MergeSlotState::{Failed, Merged, Merging, Ready};

        // The ONLY legal transition into `Merged` (≙ on main) is `Merging → Merged`.
        for from in [Ready, Merging, Merged, Failed] {
            let mut s = MergeSlot { bead: "b".into(), branch: "x".into(), state: from, retries: 0 };
            assert_eq!(
                s.transition(Merged).is_ok(),
                from == Merging,
                "Merged reachable only from Merging, never from {from:?}"
            );
        }
        // The ONLY legal transition into `Merging` (the queue admit) is `Ready → Merging`.
        for from in [Ready, Merging, Merged, Failed] {
            let mut s = MergeSlot { bead: "b".into(), branch: "x".into(), state: from, retries: 0 };
            assert_eq!(
                s.transition(Merging).is_ok(),
                from == Ready,
                "Merging reachable only from a Ready (submitted/queued) slot, never from {from:?}"
            );
        }

        // A slot is always born `Ready`: both the public constructor and the queue's `submit`
        // start there, so a `Merging`/`Merged` slot can never be conjured outside the queue.
        assert_eq!(MergeSlot::new("b", "x").state, Ready);
        let mut board = MergeBoard::default();
        board.submit("b".into(), "x".into()).unwrap();
        assert_eq!(board.get("b").unwrap().state, Ready);

        // You cannot start (or complete) a merge for a bead the queue never admitted — there is
        // no back door that lands an unsubmitted branch on main.
        let mut empty = MergeBoard::default();
        assert!(
            empty.start("ghost").is_err(),
            "cannot start a merge for a bead the queue never admitted"
        );
        assert!(
            empty.complete("ghost").is_err(),
            "cannot complete a merge for a bead the queue never admitted"
        );
    }
}
