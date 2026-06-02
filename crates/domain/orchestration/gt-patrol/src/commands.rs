//! Owned `Command` structs over [`LeaseTracker`] (see `apps/api/docs/09-llm-integration.md`).
//!
//! Each command is pure and sync: `validate` inspects the tracker without mutation; `execute`
//! re-validates, applies the change, and **returns the events to emit**. Unlike the other
//! domains, `Tick` is variadic — it can yield zero or many `LeaseExpired`s — so the command
//! `Output` is a `Vec<PatrolEvent>` the actor relays at the edge. The clock never lives in the
//! core: `now_secs` / `timeout_secs` arrive as data on the command (the determinism rule,
//! `docs/06-observability.md`), so detection stays replay-able.

use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

use gt_events::{AppError, Command};

use crate::events::PatrolEvent;
use crate::expectations::expired_leases;
use crate::state::LeaseTracker;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
pub struct RegisterLease {
    /// Bead whose lease starts now. Must be non-empty.
    pub bead: String,
    /// Worker the bead was dispatched to. Must be non-empty.
    pub worker: String,
    /// Priority to re-enqueue at if the lease later expires.
    pub priority: u8,
    /// Edge wall-clock (seconds) when the lease opened.
    pub now_secs: u64,
}

impl Command for RegisterLease {
    type Output = Vec<PatrolEvent>;
    type State = LeaseTracker;

    fn validate(&self, _state: &Self::State) -> Result<(), AppError> {
        if self.bead.is_empty() {
            return Err(AppError::Validation("bead id is empty".into()));
        }
        if self.worker.is_empty() {
            return Err(AppError::Validation("worker is empty".into()));
        }
        Ok(())
    }

    fn execute(&self, state: &mut Self::State) -> Result<Self::Output, AppError> {
        self.validate(state)?;
        state.register(self.bead.clone(), self.worker.clone(), self.priority, self.now_secs);
        Ok(vec![PatrolEvent::LeaseRegistered {
            bead: self.bead.clone(),
            worker: self.worker.clone(),
            priority: self.priority,
            now_secs: self.now_secs,
        }])
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
pub struct Heartbeat {
    /// Worker reporting alive. Refreshes every lease it owns. Must be non-empty.
    pub worker: String,
    /// Edge wall-clock (seconds) of the heartbeat.
    pub now_secs: u64,
}

impl Command for Heartbeat {
    type Output = Vec<PatrolEvent>;
    type State = LeaseTracker;

    fn validate(&self, _state: &Self::State) -> Result<(), AppError> {
        if self.worker.is_empty() {
            return Err(AppError::Validation("worker is empty".into()));
        }
        Ok(())
    }

    fn execute(&self, state: &mut Self::State) -> Result<Self::Output, AppError> {
        self.validate(state)?;
        state.heartbeat(&self.worker, self.now_secs);
        Ok(vec![PatrolEvent::Heartbeat {
            worker: self.worker.clone(),
            now_secs: self.now_secs,
        }])
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
pub struct CloseLease {
    /// Bead whose lease closed by completion/failure. Must be non-empty.
    pub bead: String,
}

impl Command for CloseLease {
    type Output = Vec<PatrolEvent>;
    type State = LeaseTracker;

    fn validate(&self, _state: &Self::State) -> Result<(), AppError> {
        if self.bead.is_empty() {
            return Err(AppError::Validation("bead id is empty".into()));
        }
        Ok(())
    }

    fn execute(&self, state: &mut Self::State) -> Result<Self::Output, AppError> {
        self.validate(state)?;
        // Closing an untracked lease is a no-op; still emit so replay stays uniform.
        state.close(&self.bead);
        Ok(vec![PatrolEvent::LeaseClosed { bead: self.bead.clone() }])
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
pub struct Tick {
    /// Edge wall-clock (seconds) the detector compares lease ages against.
    pub now_secs: u64,
    /// Lease is stale when `now_secs - last_seen > timeout_secs` (strict).
    pub timeout_secs: u64,
}

impl Command for Tick {
    type Output = Vec<PatrolEvent>;
    type State = LeaseTracker;

    fn validate(&self, _state: &Self::State) -> Result<(), AppError> {
        Ok(())
    }

    fn execute(&self, state: &mut Self::State) -> Result<Self::Output, AppError> {
        // Pure detection over a sorted snapshot → deterministic emit order. Expired leases
        // leave the tracker so a follow-up tick at the same `now_secs` does not re-fire.
        let stale = expired_leases(&state.snapshot(), self.now_secs, self.timeout_secs);
        let mut out = Vec::with_capacity(stale.len());
        for lease in stale {
            state.close(&lease.bead);
            out.push(PatrolEvent::LeaseExpired {
                bead: lease.bead,
                worker: lease.worker,
                priority: lease.priority,
            });
        }
        Ok(out)
    }
}

/// Sum type so the actor can route any patrol command through one message variant.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum PatrolCommand {
    Register(RegisterLease),
    Heartbeat(Heartbeat),
    Close(CloseLease),
    Tick(Tick),
}

impl PatrolCommand {
    /// Stable tool name used by `gt-mcp` to dispatch. Matches the pattern in `docs/09`.
    pub fn tool_name(&self) -> &'static str {
        match self {
            Self::Register(_) => "patrol.register",
            Self::Heartbeat(_) => "patrol.heartbeat",
            Self::Close(_) => "patrol.close",
            Self::Tick(_) => "patrol.tick",
        }
    }
}

impl Command for PatrolCommand {
    type Output = Vec<PatrolEvent>;
    type State = LeaseTracker;

    fn validate(&self, state: &Self::State) -> Result<(), AppError> {
        match self {
            Self::Register(c) => c.validate(state),
            Self::Heartbeat(c) => c.validate(state),
            Self::Close(c) => c.validate(state),
            Self::Tick(c) => c.validate(state),
        }
    }

    fn execute(&self, state: &mut Self::State) -> Result<Self::Output, AppError> {
        match self {
            Self::Register(c) => c.execute(state),
            Self::Heartbeat(c) => c.execute(state),
            Self::Close(c) => c.execute(state),
            Self::Tick(c) => c.execute(state),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn register_then_tick_expires_stale_lease() {
        let mut t = LeaseTracker::default();
        RegisterLease { bead: "b1".into(), worker: "w1".into(), priority: 1, now_secs: 10 }
            .execute(&mut t)
            .unwrap();
        // age 5 < 30: not expired yet.
        assert!(Tick { now_secs: 15, timeout_secs: 30 }.execute(&mut t).unwrap().is_empty());
        // age 40 > 30: expired → one LeaseExpired, lease removed.
        let evs = Tick { now_secs: 50, timeout_secs: 30 }.execute(&mut t).unwrap();
        assert_eq!(evs.len(), 1);
        assert!(matches!(&evs[0], PatrolEvent::LeaseExpired { bead, worker, .. } if bead == "b1" && worker == "w1"));
        assert!(t.is_empty(), "expired lease leaves the tracker");
    }

    #[test]
    fn heartbeat_refreshes_and_prevents_expiry() {
        let mut t = LeaseTracker::default();
        RegisterLease { bead: "b1".into(), worker: "w1".into(), priority: 0, now_secs: 0 }
            .execute(&mut t)
            .unwrap();
        Heartbeat { worker: "w1".into(), now_secs: 40 }.execute(&mut t).unwrap();
        // Without the heartbeat age would be 50 (>30); refreshed to last_seen=40 → age 10.
        assert!(Tick { now_secs: 50, timeout_secs: 30 }.execute(&mut t).unwrap().is_empty());
    }

    #[test]
    fn validate_rejects_empty_fields() {
        let t = LeaseTracker::default();
        assert!(RegisterLease { bead: "".into(), worker: "w".into(), priority: 0, now_secs: 0 }.validate(&t).is_err());
        assert!(Heartbeat { worker: "".into(), now_secs: 0 }.validate(&t).is_err());
        assert!(CloseLease { bead: "".into() }.validate(&t).is_err());
        assert!(Tick { now_secs: 0, timeout_secs: 0 }.validate(&t).is_ok());
    }
}
