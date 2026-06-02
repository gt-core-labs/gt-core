//! Owned `Command` structs over `AccountRegistry` (see `docs/09-llm-integration.md`).
//!
//! Same retrofit shape as gt-agent/gt-merge: each mutation an external client (a model via
//! `gt-mcp`) can drive is a pure, sync [`Command`]. `validate` inspects the registry without
//! mutating it; `execute` applies the registry mutation (delegating to the shared
//! `AccountRegistry::apply_*` methods so the legacy actor messages and this path stay in
//! lockstep) and **returns the `QuotaEvent` it produced** for the actor to emit on the relay.
//!
//! `validate` is deliberately lenient — it mirrors the actor's behaviour, which emits the
//! event regardless of whether the account/window exists (the consumption math just no-ops on
//! a missing window). It only rejects structurally invalid requests (empty ids, rotating an
//! account to itself). The clock travels as `now_secs` data, keeping the path replay-able.

use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

use gt_events::{AppError, Command};

use crate::events::QuotaEvent;
use crate::state::AccountRegistry;

/// A local usage sample (one model response), attributable to a session.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, JsonSchema)]
pub struct SampleTokens {
    /// Account the usage is billed to. Must be non-empty.
    pub account: String,
    /// Session the usage is attributed to (per-session breakdown). Must be non-empty.
    pub session: String,
    /// Model the call used (cost is normalized per model). Must be non-empty.
    pub model: String,
    pub input: u64,
    pub output: u64,
    pub cache_read: u64,
    pub cache_creation: u64,
    /// UTC epoch seconds, stamped at the edge.
    pub now_secs: u64,
}

impl Command for SampleTokens {
    type Output = QuotaEvent;
    type State = AccountRegistry;

    fn validate(&self, _state: &Self::State) -> Result<(), AppError> {
        if self.account.is_empty() {
            return Err(AppError::Validation("account is empty".into()));
        }
        if self.session.is_empty() {
            return Err(AppError::Validation("session is empty".into()));
        }
        if self.model.is_empty() {
            return Err(AppError::Validation("model is empty".into()));
        }
        Ok(())
    }

    fn execute(&self, state: &mut Self::State) -> Result<Self::Output, AppError> {
        self.validate(state)?;
        state.apply_sample(
            &self.account,
            &self.session,
            &self.model,
            self.input,
            self.output,
            self.cache_read,
            self.cache_creation,
            self.now_secs,
        );
        Ok(QuotaEvent::TokensSampled {
            account: self.account.clone(),
            session: self.session.clone(),
            model: self.model.clone(),
            input: self.input,
            output: self.output,
            cache_read: self.cache_read,
            cache_creation: self.cache_creation,
            now_secs: self.now_secs,
        })
    }
}

/// Reconcile against the provider's authoritative `anthropic-ratelimit-*` headers.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
pub struct ProbeWindow {
    /// Account being probed. Must be non-empty.
    pub account: String,
    /// Provider-reported remaining budget in the live window.
    pub remaining: u64,
    /// When the window resets (UTC epoch seconds).
    pub resets_at_secs: u64,
    /// UTC epoch seconds, stamped at the edge.
    pub now_secs: u64,
}

impl Command for ProbeWindow {
    type Output = QuotaEvent;
    type State = AccountRegistry;

    fn validate(&self, _state: &Self::State) -> Result<(), AppError> {
        if self.account.is_empty() {
            return Err(AppError::Validation("account is empty".into()));
        }
        Ok(())
    }

    fn execute(&self, state: &mut Self::State) -> Result<Self::Output, AppError> {
        self.validate(state)?;
        state.apply_probe(&self.account, self.remaining, self.resets_at_secs);
        Ok(QuotaEvent::UsageProbed {
            account: self.account.clone(),
            remaining: self.remaining,
            resets_at_secs: self.resets_at_secs,
            now_secs: self.now_secs,
        })
    }
}

/// Rotate off `from_account` onto `to_account`. Parks the source in `Cooldown`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
pub struct RotateAccount {
    /// Account being rotated away from. Must be non-empty.
    pub from_account: String,
    /// Healthy account taking over. Must be non-empty and different from `from_account`.
    pub to_account: String,
    /// UTC epoch seconds, stamped at the edge.
    pub now_secs: u64,
}

impl Command for RotateAccount {
    type Output = QuotaEvent;
    type State = AccountRegistry;

    fn validate(&self, _state: &Self::State) -> Result<(), AppError> {
        if self.from_account.is_empty() || self.to_account.is_empty() {
            return Err(AppError::Validation("rotation account is empty".into()));
        }
        if self.from_account == self.to_account {
            return Err(AppError::Validation(
                "cannot rotate an account onto itself".into(),
            ));
        }
        Ok(())
    }

    fn execute(&self, state: &mut Self::State) -> Result<Self::Output, AppError> {
        self.validate(state)?;
        state.apply_rotation(&self.from_account);
        Ok(QuotaEvent::Rotated {
            from_account: self.from_account.clone(),
            to_account: self.to_account.clone(),
            now_secs: self.now_secs,
        })
    }
}

/// Sum type so the actor routes any quota command through a single message variant.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, JsonSchema)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum QuotaCommand {
    Sample(SampleTokens),
    Probe(ProbeWindow),
    Rotate(RotateAccount),
}

impl QuotaCommand {
    /// Stable tool base name used by `gt-mcp` to dispatch. Matches the pattern in `docs/09`.
    pub fn tool_name(&self) -> &'static str {
        match self {
            Self::Sample(_) => "quota.sample",
            Self::Probe(_) => "quota.probe",
            Self::Rotate(_) => "quota.rotate",
        }
    }
}

impl Command for QuotaCommand {
    type Output = QuotaEvent;
    type State = AccountRegistry;

    fn validate(&self, state: &Self::State) -> Result<(), AppError> {
        match self {
            Self::Sample(c) => c.validate(state),
            Self::Probe(c) => c.validate(state),
            Self::Rotate(c) => c.validate(state),
        }
    }

    fn execute(&self, state: &mut Self::State) -> Result<Self::Output, AppError> {
        match self {
            Self::Sample(c) => c.execute(state),
            Self::Probe(c) => c.execute(state),
            Self::Rotate(c) => c.execute(state),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::state::{Account, AccountQuotaStatus, AccountWindow, WindowKind};

    fn registry_with_account() -> AccountRegistry {
        let mut r = AccountRegistry::default();
        r.upsert_account(Account {
            id: "acc-1".into(),
            status: AccountQuotaStatus::Healthy,
            window: Some(AccountWindow {
                kind: WindowKind::Rolling5h,
                limit: 1000,
                started_at_secs: 0,
                resets_at_secs: 18_000,
                consumed: 0.0,
            }),
        });
        r
    }

    #[test]
    fn sample_rejects_empty_fields_and_feeds_consumption() {
        let mut r = registry_with_account();
        let cmd = SampleTokens {
            account: "acc-1".into(),
            session: "s1".into(),
            model: "opus".into(),
            input: 100,
            output: 100,
            cache_read: 0,
            cache_creation: 0,
            now_secs: 600,
        };
        let ev = cmd.execute(&mut r).unwrap();
        assert!(matches!(ev, QuotaEvent::TokensSampled { .. }));
        // IDENTITY weights -> 200 cost units consumed.
        assert_eq!(r.get("acc-1").unwrap().window.as_ref().unwrap().consumed, 200.0);

        let bad = SampleTokens {
            account: String::new(),
            ..cmd
        };
        assert!(matches!(bad.validate(&r), Err(AppError::Validation(_))));
    }

    #[test]
    fn validate_does_not_mutate() {
        let r = registry_with_account();
        let cmd = SampleTokens {
            account: "acc-1".into(),
            session: "s1".into(),
            model: "opus".into(),
            input: 100,
            output: 0,
            cache_read: 0,
            cache_creation: 0,
            now_secs: 600,
        };
        cmd.validate(&r).unwrap();
        assert_eq!(
            r.get("acc-1").unwrap().window.as_ref().unwrap().consumed,
            0.0,
            "validate must not mutate consumption",
        );
    }

    #[test]
    fn rotate_rejects_self_and_parks_source_in_cooldown() {
        let mut r = registry_with_account();
        let cmd = RotateAccount {
            from_account: "acc-1".into(),
            to_account: "acc-2".into(),
            now_secs: 600,
        };
        let ev = cmd.execute(&mut r).unwrap();
        assert!(matches!(ev, QuotaEvent::Rotated { .. }));
        assert_eq!(r.get("acc-1").unwrap().status, AccountQuotaStatus::Cooldown);

        let to_self = RotateAccount {
            from_account: "acc-1".into(),
            to_account: "acc-1".into(),
            now_secs: 600,
        };
        assert!(matches!(to_self.validate(&r), Err(AppError::Validation(_))));
    }

    #[test]
    fn probe_reconciles_consumed_from_remaining() {
        let mut r = registry_with_account();
        let cmd = ProbeWindow {
            account: "acc-1".into(),
            remaining: 250,
            resets_at_secs: 20_000,
            now_secs: 600,
        };
        let ev = cmd.execute(&mut r).unwrap();
        assert!(matches!(ev, QuotaEvent::UsageProbed { .. }));
        let w = r.get("acc-1").unwrap().window.as_ref().unwrap();
        assert_eq!(w.consumed, 750.0, "1000 limit - 250 remaining");
        assert_eq!(w.resets_at_secs, 20_000);
    }
}
