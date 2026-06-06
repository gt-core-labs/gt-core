use serde::{Deserialize, Serialize};

use gt_events::EventKind;

/// Domain events for `gt-quota`. The log of these events is the source for rebuilding
/// [`crate::QuotaState`] via `apply`.
///
/// Time always travels as `now_secs` (UTC epoch). The producer (the async edge) reads it
/// off the clock; the core only consumes it. That keeps the predictor pure / replay-able
/// (see `docs/06-observability.md`).
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum QuotaEvent {
    /// Local usage sample: what each response's `usage` reports, attributable to the
    /// session. This is what enables the per-session breakdown (the provider headers only
    /// aggregate per account).
    TokensSampled {
        account: String,
        session: String,
        model: String,
        input: u64,
        output: u64,
        cache_read: u64,
        cache_creation: u64,
        now_secs: u64,
    },
    /// Snapshot of the provider's `anthropic-ratelimit-*` headers: the authoritative source
    /// of the real `remaining` and the next reset. The edge probe reconciles the local
    /// aggregate against this.
    UsageProbed {
        account: String,
        remaining: u64,
        resets_at_secs: u64,
        now_secs: u64,
    },
    /// Window reset (rolling-5h, weekly...): block prediction only applies within the live
    /// window; on reset the accumulated counter starts over.
    WindowReset {
        account: String,
        started_at_secs: u64,
        resets_at_secs: u64,
    },
    /// The predictor crossed the threshold: the account will block before the reset. The
    /// rotation chain reacts early (without waiting for `AccountLimited`/`Blocked`).
    BlockPredicted {
        account: String,
        eta_to_block_secs: u64,
        consumed: u64,
        limit: u64,
        rate_per_min: f64,
        now_secs: u64,
    },
    /// Reactive state: the provider returned 429 / limited.
    AccountLimited {
        account: String,
        now_secs: u64,
    },
    /// Effective rotation to another account. Closes the `BlockPredicted -> Rotated` chain
    /// or the reactive `AccountLimited -> Rotated` one.
    Rotated {
        from_account: String,
        to_account: String,
        now_secs: u64,
    },
    /// The account is fully blocked (quota exhausted or suspension).
    Blocked {
        account: String,
        until_secs: Option<u64>,
        now_secs: u64,
    },
    /// A claude account was onboarded for rotation (`hq-quota-accounts.1`): its id plus the
    /// `config_dir` (a `CLAUDE_CONFIG_DIR` holding that account's logged-in creds). The durable,
    /// event-sourced replacement for the boot-time `GT_CLAUDE_ACCOUNTS` env — the daemon rebuilds
    /// its credential keychain by replaying these, so an account added live is picked up without an
    /// env edit or a restart.
    AccountRegistered {
        account: String,
        config_dir: String,
        now_secs: u64,
    },
    /// A claude account was retired (`hq-quota-accounts.1`): the rotation pool drops it and the
    /// keychain stops pointing at its creds.
    AccountDeregistered {
        account: String,
        now_secs: u64,
    },
}

impl EventKind for QuotaEvent {
    fn kind(&self) -> &'static str {
        match self {
            QuotaEvent::TokensSampled { .. } => "quota.tokens_sampled.v1",
            QuotaEvent::UsageProbed { .. } => "quota.usage_probed.v1",
            QuotaEvent::WindowReset { .. } => "quota.window_reset.v1",
            QuotaEvent::BlockPredicted { .. } => "quota.block_predicted.v1",
            QuotaEvent::AccountLimited { .. } => "quota.account_limited.v1",
            QuotaEvent::Rotated { .. } => "quota.rotated.v1",
            QuotaEvent::Blocked { .. } => "quota.blocked.v1",
            QuotaEvent::AccountRegistered { .. } => "quota.account_registered.v1",
            QuotaEvent::AccountDeregistered { .. } => "quota.account_deregistered.v1",
        }
    }
}
