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
        /// Weekly quota remaining, when the provider surfaces it (Claude Pro plans).
        /// `None` means the response carried no weekly-window headers.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        weekly_remaining: Option<u64>,
        /// Unix epoch at which the weekly quota window resets, paired with `weekly_remaining`.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        weekly_resets_at_secs: Option<u64>,
        now_secs: u64,
    },
    /// Window reset (rolling-5h, weekly...): block prediction only applies within the live
    /// window; on reset the accumulated counter starts over. `consumed` and `kind` carry the
    /// last window's final usage — the primary source for historical token statistics.
    WindowReset {
        account: String,
        /// Window kind string ("Rolling5h" | "Weekly") — kept as `String` to avoid a
        /// circular dep with `state::WindowKind`.
        #[serde(default)]
        kind: String,
        /// Tokens consumed in the window that just expired (cost units, f64).
        #[serde(default)]
        consumed: f64,
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
    /// Proactive soft-drain rotation (gtcore-df3319): the ACTIVE account crossed the soft
    /// threshold (≥ `soft_pct`, default 80%) while still usable, so the keychain pointer moved to
    /// a healthier account **before** the predictor emitted `BlockPredicted`. In-flight polecats
    /// finish naturally on `from_account` (no credential swap, no kill — the "drain, don't kill"
    /// contract); only new slings land on `to_account`. Parks the source in `Cooldown` like
    /// [`Self::Rotated`], but records the proactive soft trigger distinctly so operators can tell a
    /// soft-drain from a reactive (`AccountLimited`/`Blocked`) or predictive (`BlockPredicted`)
    /// rotation.
    SoftDrained {
        from_account: String,
        to_account: String,
        now_secs: u64,
    },
    /// A soft-drain was warranted — the active account reached `soft_pct` — but every alternative
    /// is at or above the hard threshold (≥ `hard_pct`, default 90%), so rotating would only move
    /// the wall closer. The pointer stays put and this alert fires instead (gtcore-df3319).
    SoftDrainStalled {
        account: String,
        now_secs: u64,
    },
    /// Every account is exhausted (gtcore-6f449f): a rotation away from `account` found no healthy
    /// alternative (all others are `Blocked` or at/above the hard threshold), so the in-flight
    /// polecats backed by `account` were suspended **in place** (`SIGSTOP`) to preserve their
    /// context instead of letting them die against the rate limit. `paused_sessions` is the exact
    /// set the edge stopped. The pause is lifted by [`Self::AccountRecovered`] once a synthetic
    /// unblock probe restores an account to `Healthy`.
    AllExhausted {
        account: String,
        paused_sessions: Vec<String>,
        now_secs: u64,
    },
    /// A previously-exhausted account recovered to `Healthy` (gtcore-6f449f): the synthetic unblock
    /// probe (or a real /usage sweep) lifted `account` back, so the polecats paused under
    /// [`Self::AllExhausted`] were resumed (`SIGCONT`) and continue from exactly where they were
    /// frozen. `resumed_sessions` is the set the edge thawed (a polecat that died while paused is
    /// no longer watched and is simply absent — the supervisor recovers it normally).
    AccountRecovered {
        account: String,
        resumed_sessions: Vec<String>,
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
    /// A per-session budget was opened (B1, gtcore-ab170f): the edge stamps the `bead` the
    /// session is working and an optional cost-unit `limit` at spawn. This is what makes the
    /// accumulated `TokensSampled` spend attributable per bead, and arms the budget-exceeded
    /// alert. Additive: a session whose budget is never opened still accumulates tokens, just
    /// without a bead or a limit.
    SessionBudgetOpened {
        session: String,
        /// Bead the session is working (`GT_HOOK_BEAD` at the edge). `None` when the session is
        /// not bead-scoped (e.g. an interactive mayor/dog).
        #[serde(default, skip_serializing_if = "Option::is_none")]
        bead: Option<String>,
        /// Budget ceiling in cost units. `None` ⇒ track spend without alerting.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        limit_cost: Option<f64>,
        now_secs: u64,
    },
    /// A session's accumulated spend crossed its budget for the first time (B1). Emitted once
    /// per session (the crossing is latched), so a downstream alerting edge fires a single
    /// budget-exceeded notification rather than one per sample past the line.
    BudgetExceeded {
        session: String,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        bead: Option<String>,
        /// Running cost at the crossing, in cost units.
        consumed_cost: f64,
        /// The ceiling that was crossed, in cost units.
        limit_cost: f64,
        now_secs: u64,
    },
    /// A per-session budget was closed (B1): the session ended. Stamps the final activity instant
    /// so the execution-minute span covers up to shutdown.
    SessionBudgetClosed {
        session: String,
        now_secs: u64,
    },
    /// A session's spend crossed its HARD ceiling (A5, gtcore-f3a016): the platform hard gate
    /// tripped. Distinct from `BudgetExceeded` (B1's soft alert) — this is the *enforcement*
    /// event: from here the anthropic proxy refuses the session's further model calls, so the
    /// runaway freezes itself. Emitted once (the crossing is latched); the replay reducer marks
    /// the session `gated` so a restart restores the freeze.
    BudgetGateTripped {
        session: String,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        bead: Option<String>,
        /// Running cost at the trip, in cost units.
        consumed_cost: f64,
        /// The hard ceiling that was crossed, in cost units.
        hard_limit_cost: f64,
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
            QuotaEvent::SoftDrained { .. } => "quota.soft_drain.v1",
            QuotaEvent::SoftDrainStalled { .. } => "quota.soft_drain_stalled.v1",
            QuotaEvent::AllExhausted { .. } => "quota.all_exhausted.v1",
            QuotaEvent::AccountRecovered { .. } => "quota.account_recovered.v1",
            QuotaEvent::Blocked { .. } => "quota.blocked.v1",
            QuotaEvent::AccountRegistered { .. } => "quota.account_registered.v1",
            QuotaEvent::AccountDeregistered { .. } => "quota.account_deregistered.v1",
            // Per-session budget lifecycle (B1, gtcore-ab170f). Born versioned + kebab — no
            // legacy bare-kind records exist. Still under the `quota.` NS so they replay.
            QuotaEvent::SessionBudgetOpened { .. } => "quota.session_budget_opened.v1",
            QuotaEvent::BudgetExceeded { .. } => "quota.budget_exceeded.v1",
            QuotaEvent::SessionBudgetClosed { .. } => "quota.session_budget_closed.v1",
            QuotaEvent::BudgetGateTripped { .. } => "quota.budget_gate_tripped.v1",
        }
    }
}
