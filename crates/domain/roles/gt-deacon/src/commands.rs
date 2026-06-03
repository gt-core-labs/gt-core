//! Owned `Command` structs over `DeaconState`. Same shape as gt-merge / gt-sheriff:
//! `validate` inspects without mutating; `execute` re-validates and returns the events
//! the actor must apply + emit in the same tick. The producer of `TrackItem`/`FinishItem`
//! is the composition root (cross-domain wiring: when a `merge.ready` arrives during
//! drain, that root reaction calls `TrackItem`; on `merge.merged` it calls `FinishItem`).

use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

use gt_events::AppError;

use crate::events::DeaconEvent;
use crate::state::DeaconState;

/// Flip the drain flag. Idempotent: re-issuing while already draining is a no-op (returns
/// `Ok(vec![])`) so a duplicated SIGTERM does not double-fire the event.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
pub struct BeginDrain {
    /// Identity that asked (operator name, `"sigterm"`, `"mayor"`, …).
    pub by: String,
    /// Edge-stamped epoch seconds — recorded so replay does not depend on a clock.
    pub at: u64,
}

/// Register a new in-flight item the deacon must wait for before completion.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
pub struct TrackItem {
    pub id: String,
    /// Source domain (`"merge"`, `"orch"`, `"sched"`, …). Free-form to keep the deacon
    /// decoupled from the GtEvent enum.
    pub kind: String,
}

/// Mark a tracked item complete. Removes it from the pending set; if the deacon is
/// draining and the set hits zero, the actor follows with a `DrainComplete` event.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
pub struct FinishItem {
    pub id: String,
    /// Edge-stamped epoch seconds for the `DrainComplete` event if this finish triggers
    /// it. Recorded explicitly so replay re-emits the same `at`.
    pub at: u64,
}

/// Trip the emergency stop for this workspace's deacon. Idempotent: re-issuing while already
/// stopped is a no-op (returns `Ok(vec![])`). Distinct from `BeginDrain` — an e-stop does not
/// wait for the pending set, it records an immediate kill decision the composition root acts on.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
pub struct EmergencyStop {
    /// Operator identity that pulled the cord (`"mayor"`, an operator name, …).
    pub by: String,
    /// Edge-stamped epoch seconds — recorded so replay does not depend on a clock.
    pub at: u64,
}

/// The set of mutations the deacon actor accepts.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
pub enum DeaconCommand {
    BeginDrain(BeginDrain),
    Track(TrackItem),
    Finish(FinishItem),
    EmergencyStop(EmergencyStop),
}

impl DeaconCommand {
    /// Inspect `state` and return `Ok` iff the command would apply cleanly.
    pub fn validate(&self, state: &DeaconState) -> Result<(), AppError> {
        match self {
            DeaconCommand::BeginDrain(b) => {
                if b.by.is_empty() {
                    return Err(AppError::Validation("drain `by` empty".into()));
                }
                Ok(())
            }
            DeaconCommand::Track(t) => {
                if t.id.is_empty() {
                    return Err(AppError::Validation("track `id` empty".into()));
                }
                if t.kind.is_empty() {
                    return Err(AppError::Validation("track `kind` empty".into()));
                }
                if state.pending.contains_key(&t.id) {
                    return Err(AppError::Validation(format!(
                        "item {} already tracked",
                        t.id
                    )));
                }
                Ok(())
            }
            DeaconCommand::Finish(f) => {
                if f.id.is_empty() {
                    return Err(AppError::Validation("finish `id` empty".into()));
                }
                if !state.pending.contains_key(&f.id) {
                    return Err(AppError::NotFound(format!("item {} not tracked", f.id)));
                }
                Ok(())
            }
            DeaconCommand::EmergencyStop(e) => {
                if e.by.is_empty() {
                    return Err(AppError::Validation("e-stop `by` empty".into()));
                }
                Ok(())
            }
        }
    }

    /// Re-validate and produce the events to apply + emit. `BeginDrain` while already
    /// draining returns `Ok(vec![])` (idempotent). `BeginDrain` on an **empty** pending set
    /// produces `DrainRequested` AND `DrainComplete` in the same tick (no in-flight work →
    /// drain is immediately done; this is the SIGTERM-with-idle-town case wired by
    /// `bins/gt::drain_via_deacon`, hq-mc72.12 C2). `Finish` that empties the pending set
    /// during drain produces both `ItemFinished` and `DrainComplete` in order.
    pub fn execute(&self, state: &DeaconState) -> Result<Vec<DeaconEvent>, AppError> {
        self.validate(state)?;
        match self {
            DeaconCommand::BeginDrain(b) => {
                if state.draining {
                    Ok(Vec::new())
                } else {
                    let mut out = vec![DeaconEvent::DrainRequested {
                        by: b.by.clone(),
                        at: b.at,
                    }];
                    // Empty pending at drain time → DrainComplete fires in the same tick,
                    // mirroring the Finish-empties-pending path. `state.completed` guards
                    // re-emission on replay (it is set by `DeaconState::apply` for
                    // DrainComplete).
                    if !state.completed && state.pending.is_empty() {
                        out.push(DeaconEvent::DrainComplete { at: b.at });
                    }
                    Ok(out)
                }
            }
            DeaconCommand::Track(t) => Ok(vec![DeaconEvent::ItemBegun {
                id: t.id.clone(),
                kind: t.kind.clone(),
            }]),
            DeaconCommand::Finish(f) => {
                let mut out = vec![DeaconEvent::ItemFinished { id: f.id.clone() }];
                // Detect end-of-drain: the validate above ensured `f.id` IS in pending,
                // so after applying ItemFinished the pending count drops by 1. Compute the
                // post-apply emptiness from the current snapshot.
                let will_be_empty = state
                    .pending
                    .keys()
                    .filter(|k| k.as_str() != f.id.as_str())
                    .count()
                    == 0;
                if state.draining && !state.completed && will_be_empty {
                    out.push(DeaconEvent::DrainComplete { at: f.at });
                }
                Ok(out)
            }
            DeaconCommand::EmergencyStop(e) => {
                // Idempotent: an already-stopped workspace emits nothing so a doubled
                // operator press (or replay) does not re-latch.
                if state.stopped {
                    Ok(Vec::new())
                } else {
                    Ok(vec![DeaconEvent::EmergencyStopped {
                        by: e.by.clone(),
                        at: e.at,
                    }])
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn emergency_stop_rejects_empty_by() {
        let st = DeaconState::default();
        let err = DeaconCommand::EmergencyStop(EmergencyStop { by: String::new(), at: 1 })
            .validate(&st)
            .unwrap_err();
        assert!(matches!(err, AppError::Validation(_)));
    }

    #[test]
    fn emergency_stop_emits_once_then_idempotent() {
        let mut st = DeaconState::default();
        let evs = DeaconCommand::EmergencyStop(EmergencyStop { by: "mayor".into(), at: 7 })
            .execute(&st)
            .unwrap();
        assert_eq!(evs.len(), 1);
        assert!(matches!(evs[0], DeaconEvent::EmergencyStopped { at: 7, .. }));
        for ev in &evs { st.apply(ev).unwrap(); }
        assert!(st.stopped);
        let again = DeaconCommand::EmergencyStop(EmergencyStop { by: "mayor".into(), at: 8 })
            .execute(&st)
            .unwrap();
        assert!(again.is_empty(), "already-stopped e-stop must emit nothing");
    }

    #[test]
    fn emergency_stop_preserves_pending_as_historical_record() {
        // E-stop latches `stopped` but does NOT clear pending — the in-flight set is the
        // record of what was killed; the real session kill is the cross-domain reaction.
        let mut st = DeaconState::default();
        st.apply(&DeaconEvent::ItemBegun { id: "x".into(), kind: "merge".into() }).unwrap();
        st.apply(&DeaconEvent::EmergencyStopped { by: "op".into(), at: 1 }).unwrap();
        assert!(st.stopped);
        assert!(st.pending.contains_key("x"), "pending preserved on e-stop");
    }
}
