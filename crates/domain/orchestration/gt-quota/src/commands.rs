//! Owned `Command` structs over `AccountRegistry` (see `docs/09-llm-integration.md`).
//!
//! Same retrofit shape as gt-agent/gt-merge: each mutation an external client (a model via
//! `gt-mcp`) can drive is a pure, sync [`Command`]. `validate` inspects the registry without
//! mutating it; `execute` applies the registry mutation (delegating to the shared
//! `AccountRegistry::apply_*` methods so the legacy actor messages and this path stay in
//! lockstep) and **returns the `QuotaEvent` it produced** for the actor to emit on the relay.
//!
//! `validate` rejects structurally invalid requests (empty ids, rotating an account to itself)
//! and domain-invalid ones (`RotateAccount` also checks that the destination is registered and
//! Healthy — rotating to a Cooldown/Limited/Blocked account leaves the workspace without
//! capacity). The clock travels as `now_secs` data, keeping the path replay-able.

use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

use gt_events::{AppError, Command};

use crate::events::QuotaEvent;
use crate::state::{Account, AccountQuotaStatus, AccountRegistry};

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
    /// Provider-reported remaining budget in the rolling-5h window.
    pub remaining: u64,
    /// When the rolling-5h window resets (UTC epoch seconds).
    pub resets_at_secs: u64,
    /// Weekly budget remaining, when the provider exposes it (Claude Pro plans).
    /// `None` means the response carried no `…-remaining-week` header.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub weekly_remaining: Option<u64>,
    /// When the weekly window resets (UTC epoch seconds), paired with `weekly_remaining`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub weekly_resets_at_secs: Option<u64>,
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
        state.apply_probe(&self.account, self.remaining, self.resets_at_secs, self.now_secs);
        if let (Some(w_rem), Some(w_reset)) = (self.weekly_remaining, self.weekly_resets_at_secs) {
            state.apply_weekly_probe(&self.account, w_rem, w_reset, self.now_secs);
        }
        Ok(QuotaEvent::UsageProbed {
            account: self.account.clone(),
            remaining: self.remaining,
            resets_at_secs: self.resets_at_secs,
            weekly_remaining: self.weekly_remaining,
            weekly_resets_at_secs: self.weekly_resets_at_secs,
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

    fn validate(&self, state: &Self::State) -> Result<(), AppError> {
        if self.from_account.is_empty() || self.to_account.is_empty() {
            return Err(AppError::Validation("rotation account is empty".into()));
        }
        if self.from_account == self.to_account {
            return Err(AppError::Validation(
                "cannot rotate an account onto itself".into(),
            ));
        }
        match state.get(&self.to_account) {
            None => {
                return Err(AppError::Validation(format!(
                    "to_account '{}' is not registered — only known accounts can be rotation targets",
                    self.to_account
                )));
            }
            Some(acc) if acc.status != AccountQuotaStatus::Healthy => {
                return Err(AppError::Validation(format!(
                    "to_account '{}' is {:?} — rotation target must be Healthy",
                    self.to_account, acc.status
                )));
            }
            Some(_) => {}
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

/// Onboard (or replace) a claude account with its credential dir (`hq-quota-accounts.4`). Unlike a
/// probe/sample (consumption) this binds IDENTITY: the `config_dir` is the account's
/// `CLAUDE_CONFIG_DIR`. Event-sourced — emits `AccountRegistered`, the durable replacement for the
/// `GT_CLAUDE_ACCOUNTS` env. The account becomes a rotation candidate immediately (Healthy, no
/// window); its window arrives from the first probe (the authoritative source), not from here.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
pub struct RegisterAccount {
    /// Account id (keychain/provider correlative). Must be non-empty.
    pub account: String,
    /// The account's `CLAUDE_CONFIG_DIR` (its logged-in creds dir). Must be non-empty. The edge
    /// (composition `account_dirs`) sanitizes the path before calling; the domain only checks shape.
    pub config_dir: String,
    /// UTC epoch seconds, stamped at the edge.
    pub now_secs: u64,
}

impl Command for RegisterAccount {
    type Output = QuotaEvent;
    type State = AccountRegistry;

    fn validate(&self, _state: &Self::State) -> Result<(), AppError> {
        if self.account.trim().is_empty() {
            return Err(AppError::Validation("account is empty".into()));
        }
        if self.config_dir.trim().is_empty() {
            return Err(AppError::Validation("config_dir is empty".into()));
        }
        Ok(())
    }

    fn execute(&self, state: &mut Self::State) -> Result<Self::Output, AppError> {
        self.validate(state)?;
        // Bring the account into existence as a rotation candidate (idempotent upsert). The window
        // is NOT declared here — it is observed from the first probe.
        state.upsert_account(Account::new(&self.account));
        Ok(QuotaEvent::AccountRegistered {
            account: self.account.clone(),
            config_dir: self.config_dir.clone(),
            now_secs: self.now_secs,
        })
    }
}

/// Retire an account from the rotation pool (`hq-quota-accounts.4`). Event-sourced — emits
/// `AccountDeregistered`. Idempotent: removing an absent account still emits (the reducer no-ops).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
pub struct RetireAccount {
    /// Account id to drop. Must be non-empty.
    pub account: String,
    /// UTC epoch seconds, stamped at the edge.
    pub now_secs: u64,
}

impl Command for RetireAccount {
    type Output = QuotaEvent;
    type State = AccountRegistry;

    fn validate(&self, _state: &Self::State) -> Result<(), AppError> {
        if self.account.trim().is_empty() {
            return Err(AppError::Validation("account is empty".into()));
        }
        Ok(())
    }

    fn execute(&self, state: &mut Self::State) -> Result<Self::Output, AppError> {
        self.validate(state)?;
        state.remove_account(&self.account);
        Ok(QuotaEvent::AccountDeregistered {
            account: self.account.clone(),
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
    Register(RegisterAccount),
    Retire(RetireAccount),
}

impl QuotaCommand {
    /// Stable tool base name used by `gt-mcp` to dispatch. Matches the pattern in `docs/09`.
    pub fn tool_name(&self) -> &'static str {
        match self {
            Self::Sample(_) => "quota.sample",
            Self::Probe(_) => "quota.probe",
            Self::Rotate(_) => "quota.rotate",
            Self::Register(_) => "quota.register",
            Self::Retire(_) => "quota.retire",
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
            Self::Register(c) => c.validate(state),
            Self::Retire(c) => c.validate(state),
        }
    }

    fn execute(&self, state: &mut Self::State) -> Result<Self::Output, AppError> {
        match self {
            Self::Sample(c) => c.execute(state),
            Self::Probe(c) => c.execute(state),
            Self::Rotate(c) => c.execute(state),
            Self::Register(c) => c.execute(state),
            Self::Retire(c) => c.execute(state),
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
            weekly_window: None,
            last_probe_secs: None,
            sampled_since_probe: 0.0,
            probe_divergence: None,
        });
        // acc-2: a healthy standby used by rotate tests as a valid target.
        r.upsert_account(Account::new("acc-2"));
        r
    }

    #[test]
    fn register_account_emits_event_and_adds_candidate() {
        let mut r = AccountRegistry::default();
        let cmd = RegisterAccount {
            account: "acctB".into(),
            config_dir: "/vol/accounts/acctB".into(),
            now_secs: 100,
        };
        let ev = cmd.execute(&mut r).expect("register ok");
        assert_eq!(
            ev,
            QuotaEvent::AccountRegistered {
                account: "acctB".into(),
                config_dir: "/vol/accounts/acctB".into(),
                now_secs: 100,
            }
        );
        assert!(r.get("acctB").is_some(), "account is a live candidate");
    }

    #[test]
    fn register_rejects_empty_account_or_dir() {
        let r = AccountRegistry::default();
        assert!(RegisterAccount {
            account: "  ".into(),
            config_dir: "/d".into(),
            now_secs: 0,
        }
        .validate(&r)
        .is_err());
        assert!(RegisterAccount {
            account: "a".into(),
            config_dir: "".into(),
            now_secs: 0,
        }
        .validate(&r)
        .is_err());
    }

    #[test]
    fn retire_account_emits_event_and_removes() {
        let mut r = registry_with_account();
        let cmd = RetireAccount {
            account: "acc-1".into(),
            now_secs: 200,
        };
        let ev = cmd.execute(&mut r).expect("retire ok");
        assert_eq!(
            ev,
            QuotaEvent::AccountDeregistered {
                account: "acc-1".into(),
                now_secs: 200,
            }
        );
        assert!(r.get("acc-1").is_none(), "account dropped");
        // Idempotent: retiring again still emits (reducer no-ops on absent).
        assert!(cmd.execute(&mut r).is_ok());
    }

    #[test]
    fn quota_command_routes_register_and_retire_tool_names() {
        assert_eq!(
            QuotaCommand::Register(RegisterAccount {
                account: "a".into(),
                config_dir: "/d".into(),
                now_secs: 0,
            })
            .tool_name(),
            "quota.register"
        );
        assert_eq!(
            QuotaCommand::Retire(RetireAccount {
                account: "a".into(),
                now_secs: 0,
            })
            .tool_name(),
            "quota.retire"
        );
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
            weekly_remaining: None,
            weekly_resets_at_secs: None,
            now_secs: 600,
        };
        let ev = cmd.execute(&mut r).unwrap();
        assert!(matches!(ev, QuotaEvent::UsageProbed { .. }));
        let w = r.get("acc-1").unwrap().window.as_ref().unwrap();
        assert_eq!(w.consumed, 750.0, "1000 limit - 250 remaining");
        assert_eq!(w.resets_at_secs, 20_000);
    }

    // --- hq-quota-refinement.2: RotateAccount::validate destination checks ---

    #[test]
    fn rotate_rejects_unregistered_destination() {
        let r = registry_with_account(); // only acc-1 and acc-2
        let cmd = RotateAccount {
            from_account: "acc-1".into(),
            to_account: "unknown".into(),
            now_secs: 600,
        };
        let err = cmd.validate(&r).unwrap_err();
        assert!(
            matches!(err, AppError::Validation(ref msg) if msg.contains("not registered")),
            "unregistered target must be rejected: {err:?}"
        );
    }

    #[test]
    fn rotate_rejects_cooldown_destination() {
        let mut r = registry_with_account();
        // Park acc-2 in Cooldown.
        r.upsert_account(Account {
            id: "acc-2".into(),
            status: AccountQuotaStatus::Cooldown,
            window: None,
            weekly_window: None,
            last_probe_secs: None,
            sampled_since_probe: 0.0,
            probe_divergence: None,
        });
        let cmd = RotateAccount {
            from_account: "acc-1".into(),
            to_account: "acc-2".into(),
            now_secs: 600,
        };
        let err = cmd.validate(&r).unwrap_err();
        assert!(
            matches!(err, AppError::Validation(ref msg) if msg.contains("Healthy")),
            "Cooldown target must be rejected: {err:?}"
        );
    }

    #[test]
    fn rotate_rejects_limited_destination() {
        let mut r = registry_with_account();
        r.upsert_account(Account {
            id: "acc-2".into(),
            status: AccountQuotaStatus::Limited,
            window: None,
            weekly_window: None,
            last_probe_secs: None,
            sampled_since_probe: 0.0,
            probe_divergence: None,
        });
        let cmd = RotateAccount {
            from_account: "acc-1".into(),
            to_account: "acc-2".into(),
            now_secs: 600,
        };
        assert!(matches!(cmd.validate(&r), Err(AppError::Validation(_))));
    }

    #[test]
    fn rotate_accepts_healthy_destination() {
        // acc-2 in registry_with_account() is Healthy — must pass.
        let mut r = registry_with_account();
        let cmd = RotateAccount {
            from_account: "acc-1".into(),
            to_account: "acc-2".into(),
            now_secs: 600,
        };
        assert!(cmd.validate(&r).is_ok(), "Healthy target must be accepted");
        // execute completes the rotation.
        let ev = cmd.execute(&mut r).unwrap();
        assert!(matches!(ev, QuotaEvent::Rotated { .. }));
        assert_eq!(r.get("acc-1").unwrap().status, AccountQuotaStatus::Cooldown);
    }
}
