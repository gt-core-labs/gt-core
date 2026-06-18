//! `quota.*` domain dispatch (`hq-mcp-dispatch.4`).
//!
//! The quota domain (gt-quota) is event-sourced like merge and convoy — no
//! projection table — so each call rehydrates the registry from the workspace
//! event log, executes the command, and appends the resulting [`QuotaEvent`].
//!
//! The replay reducer rebuilds a [`QuotaState`] (the deterministic Step-3 gate
//! projection); the [`AccountRegistry`] the [`QuotaCommand`]s mutate is then
//! reconstituted from it via [`AccountRegistry::from_state`] — the same bridge
//! the actor's boot hydration takes. The live `consumed`/EWMA rates re-converge
//! from the next `quota.probe` (they are not in the durable projection by
//! design; see [`QuotaState`]).
//!
//! Tools: `quota.sample` / `quota.probe` / `quota.rotate` (the [`QuotaCommand`]
//! mutations), plus `quota.list` / `quota.info` reads.

use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use async_trait::async_trait;
use serde::de::DeserializeOwned;
use serde_json::{json, Value};

use gt_events::{Command, EventKind};
use gt_mcp_server::{DomainCtx, DomainHandler, QuotaBlockSignal};
use gt_module::McpTool;
use gt_quota::{
    Account, AccountRegistry, ProbeWindow, QuotaEvent, QuotaState, RegisterAccount, RetireAccount,
    RotateAccount, SampleTokens,
};
use gt_store_dolt::AppError;

use super::eventlog::EventLog;
use super::util::{descriptor, ev_err, parse_cmd, req, str_arg};

/// The event-log kind prefix for every quota event (`quota.*.v1`).
const NS: &str = "quota.";

/// The claim-context custodian's quota-block signal (`hq-context-custodian.2`),
/// backed by the same per-workspace quota event log the [`QuotaHandler`] replays.
///
/// On each `working` transition the issues server asks whether the caller's workspace
/// pool is freshly out of capacity; this guard replays that tenant's quota log into an
/// [`AccountRegistry`] and answers with
/// [`pool_genuinely_blocked`](AccountRegistry::pool_genuinely_blocked) — `true` only on
/// a non-empty, no-`Healthy`, freshly-blocked pool. A replay error answers `false`
/// (keep context mandatory): the deferral is the exception, never the failure mode.
///
/// The signal is the *only* thing that can authorize a context-less claim, and it comes
/// from the subsystem, never from the agent — closing the loophole the custodian guards.
pub struct QuotaBlockGuard {
    log: Arc<EventLog>,
}

impl QuotaBlockGuard {
    /// Wrap the shared per-workspace event log (the same one [`QuotaHandler`] uses).
    pub fn new(log: Arc<EventLog>) -> Self {
        Self { log }
    }
}

#[async_trait]
impl QuotaBlockSignal for QuotaBlockGuard {
    async fn pool_blocked(&self, workspace: Option<&str>) -> bool {
        // Edge clock: the domain reads no clock, so the freshness comparison is fed
        // `now` from here (mirrors the quota REST/feed edges).
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|d| d.as_secs())
            .unwrap_or(0);
        match self
            .log
            .replay_domain(workspace, NS, QuotaState::default(), QuotaState::apply)
        {
            Ok(state) => AccountRegistry::from_state(&state).pool_genuinely_blocked(now),
            // Can't confirm a block ⇒ keep the context requirement in force.
            Err(_) => false,
        }
    }
}

/// Event-sourced handler for the `quota.*` tool namespace.
pub struct QuotaHandler {
    log: Arc<EventLog>,
    /// The accounts root (`GT_CLAUDE_ACCOUNTS_ROOT`) backing the `quota.cred_health` read
    /// (gtcore-1fe9b4). `None` ⇒ no root wired, so `quota.cred_health` reports an empty list
    /// (the same degraded mode the catalog runs in without an accounts root).
    accounts_root: Option<std::path::PathBuf>,
}

impl QuotaHandler {
    /// Wrap the per-workspace event log. No accounts root ⇒ `quota.cred_health` is empty until one
    /// is wired with [`with_accounts_root`](Self::with_accounts_root).
    pub fn new(log: Arc<EventLog>) -> Self {
        Self {
            log,
            accounts_root: None,
        }
    }

    /// Wire the deploy-global accounts root so `quota.cred_health` can read each account dir's
    /// `.credentials.json` + `.claude.json` and classify slingability (gtcore-1fe9b4).
    pub fn with_accounts_root(mut self, root: std::path::PathBuf) -> Self {
        self.accounts_root = Some(root);
        self
    }

    /// Rebuild the account registry from the workspace's quota events.
    fn registry(&self, ws: Option<&str>) -> Result<AccountRegistry, AppError> {
        let state = self
            .log
            .replay_domain(ws, NS, QuotaState::default(), QuotaState::apply)?;
        Ok(AccountRegistry::from_state(&state))
    }

    /// Parse → execute against the rehydrated registry → append the produced event.
    ///
    /// `parse_cmd` stamps the server clock onto `now_secs` when the caller omits
    /// it — the clock is the edge's to supply, keeping the path replay-able.
    fn run<C>(&self, ws: Option<&str>, args: Value) -> Result<Value, AppError>
    where
        C: Command<State = AccountRegistry, Output = QuotaEvent> + DeserializeOwned,
    {
        let cmd: C = parse_cmd(args)?;
        let mut registry = self.registry(ws)?;
        let event = cmd.execute(&mut registry).map_err(ev_err)?;
        let kind = event.kind().to_string();
        self.log.append(ws, event)?;
        Ok(json!({ "ok": true, "event": kind }))
    }
}

#[async_trait]
impl DomainHandler for QuotaHandler {
    fn namespace(&self) -> &'static str {
        "quota"
    }

    fn descriptors(&self) -> Vec<McpTool> {
        vec![
            descriptor(
                "quota.sample",
                "Record a token-usage sample for an account/session/model.",
                &[
                    req("account", "string"),
                    req("session", "string"),
                    req("model", "string"),
                    req("input", "integer"),
                    req("output", "integer"),
                    req("cache_read", "integer"),
                    req("cache_creation", "integer"),
                ],
            ),
            descriptor(
                "quota.probe",
                "Record a rate-limit probe: remaining budget + reset time for an account.",
                &[req("account", "string"), req("remaining", "integer"), req("resets_at_secs", "integer")],
            ),
            descriptor(
                "quota.rotate",
                "Rotate active usage from one account to another.",
                &[req("from_account", "string"), req("to_account", "string")],
            ),
            descriptor("quota.list", "List every tracked account with its usage.", &[]),
            descriptor("quota.info", "Show one account's usage + window.", &[req("account", "string")]),
            descriptor(
                "quota.register",
                "Onboard (or replace) a claude account with its CLAUDE_CONFIG_DIR; becomes a rotation candidate. Emits quota.account_registered.",
                &[req("account", "string"), req("config_dir", "string")],
            ),
            descriptor(
                "quota.retire",
                "Drop an account from the rotation pool. Emits quota.account_deregistered; idempotent.",
                &[req("account", "string")],
            ),
            descriptor(
                "quota.cred_health",
                "Per-account credential health: refresh-token present, token expiry, onboarding-complete, and needs_relogin. Reads the on-disk account dirs, not the quota log.",
                &[],
            ),
        ]
    }

    async fn dispatch(&self, tool: &str, ctx: DomainCtx<'_>) -> Result<Value, AppError> {
        let ws = ctx.workspace;
        match tool {
            "quota.sample" => {
                // Per-workspace cost attribution (hq-mt-deploy.8): tally the sample's
                // total tokens onto `gt_workspace_quota_consumed`, only once the event
                // is durably appended (so a rejected sample doesn't inflate the count).
                // The label mirrors the audit "default" tenant fallback. No-op until the
                // bin's `gt_telemetry::init` registers the counter.
                let consumed = sample_tokens(&ctx.args);
                let out = self.run::<SampleTokens>(ws, ctx.args)?;
                gt_telemetry::metrics::observe_workspace_quota_consumed(
                    ws.unwrap_or("default"),
                    consumed,
                );
                Ok(out)
            }
            "quota.probe" => self.run::<ProbeWindow>(ws, ctx.args),
            "quota.rotate" => self.run::<RotateAccount>(ws, ctx.args),
            "quota.register" => self.run::<RegisterAccount>(ws, ctx.args),
            "quota.retire" => self.run::<RetireAccount>(ws, ctx.args),
            "quota.list" => {
                let registry = self.registry(ws)?;
                Ok(json!({ "accounts": registry.accounts().map(account_json).collect::<Vec<_>>() }))
            }
            "quota.info" => {
                let id = str_arg(&ctx.args, "account")?;
                match self.registry(ws)?.get(id) {
                    Some(account) => Ok(account_json(account)),
                    None => Err(AppError::NotFound(format!("account {id}"))),
                }
            }
            "quota.cred_health" => {
                // Reads the on-disk account dirs (not the quota log) and classifies each on both
                // slingability axes (credential validity + onboarding markers). The SAME report the
                // REST `GET /api/v1/quota/cred-health` serves. Empty when no accounts root is wired.
                let reports = match self.accounts_root.as_ref() {
                    Some(root) => {
                        let now_ms = SystemTime::now()
                            .duration_since(UNIX_EPOCH)
                            .map(|d| d.as_millis() as u64)
                            .unwrap_or(0);
                        crate::relogin::cred_health_in(root, now_ms)
                    }
                    None => Vec::new(),
                };
                Ok(json!({ "accounts": reports }))
            }
            other => Err(AppError::Validation(format!("unknown tool `{other}`"))),
        }
    }
}

/// Shape one account (id, status, window) as the dispatch payload. `Account`
/// derives `Serialize`, so the projection is the domain type verbatim.
fn account_json(account: &Account) -> Value {
    serde_json::to_value(account).unwrap_or_else(|_| json!({}))
}

/// Total tokens in a `quota.sample` payload — the raw sum of the four usage
/// counters (`input`/`output`/`cache_read`/`cache_creation`), each defaulting to
/// `0` when absent (hq-mt-deploy.8). Raw, unweighted: the per-tenant dashboard
/// tracks token volume, leaving per-model cost weighting to `gt-quota`'s own
/// accounting. Read here rather than off the produced [`QuotaEvent`] so the
/// generic `run::<SampleTokens>` append path stays untouched.
fn sample_tokens(args: &Value) -> u64 {
    ["input", "output", "cache_read", "cache_creation"]
        .iter()
        .map(|k| args.get(*k).and_then(Value::as_u64).unwrap_or(0))
        .sum()
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    fn handler(dir: &TempDir) -> QuotaHandler {
        QuotaHandler::new(Arc::new(EventLog::new(Some(dir.path().to_path_buf()))))
    }

    fn ctx(args: Value) -> DomainCtx<'static> {
        DomainCtx {
            workspace: None,
            actor: "tester",
            args,
        }
    }

    /// Full lifecycle over the event log: a `probe` materializes the account in
    /// the replayed projection, a `rotate` parks it in `Cooldown`, and every read
    /// rehydrates the registry from the log (no shared in-memory state).
    #[tokio::test]
    async fn quota_lifecycle_through_event_log() {
        let dir = TempDir::new().unwrap();
        let h = handler(&dir);

        // Probe creates the account entry in the projection (Healthy, no window).
        let probed = h
            .dispatch(
                "quota.probe",
                ctx(json!({ "account": "acc-1", "remaining": 250, "resets_at_secs": 20_000 })),
            )
            .await
            .unwrap();
        assert_eq!(probed["ok"], true);
        assert_eq!(probed["event"], "quota.usage_probed.v1");

        let info = h
            .dispatch("quota.info", ctx(json!({ "account": "acc-1" })))
            .await
            .unwrap();
        assert_eq!(info["id"], "acc-1");
        assert_eq!(info["status"], "Healthy");

        // Probe the rotation target first: RotateAccount only accepts a destination that is
        // registered AND Healthy (a074fc5), and a probe with remaining>0 creates exactly that.
        h.dispatch(
            "quota.probe",
            ctx(json!({ "account": "acc-2", "remaining": 250, "resets_at_secs": 20_000 })),
        )
        .await
        .unwrap();

        // Rotate parks the source account in Cooldown (replayed onto the entry).
        let rotated = h
            .dispatch(
                "quota.rotate",
                ctx(json!({ "from_account": "acc-1", "to_account": "acc-2" })),
            )
            .await
            .unwrap();
        assert_eq!(rotated["event"], "quota.rotated.v1");

        let info = h
            .dispatch("quota.info", ctx(json!({ "account": "acc-1" })))
            .await
            .unwrap();
        assert_eq!(info["status"], "Cooldown", "rotation parked the source");

        // Sample appends a usage event (its consumption math no-ops without a live
        // window, but the event is durable on the log).
        let sampled = h
            .dispatch(
                "quota.sample",
                ctx(json!({
                    "account": "acc-1", "session": "s1", "model": "opus",
                    "input": 100, "output": 100, "cache_read": 0, "cache_creation": 0
                })),
            )
            .await
            .unwrap();
        assert_eq!(sampled["event"], "quota.tokens_sampled.v1");

        let list = h.dispatch("quota.list", ctx(json!({}))).await.unwrap();
        assert!(list["accounts"]
            .as_array()
            .unwrap()
            .iter()
            .any(|a| a["id"] == "acc-1"));
    }

    // hq-quota-weekly.5 -----------------------------------------------------------

    /// AC2 (MCP path): `quota.list` and `quota.info` emit `weekly_window: null` when the
    /// account has no weekly data, and a populated object after a weekly probe.
    #[tokio::test]
    async fn quota_list_and_info_emit_weekly_window_null_then_populated() {
        let dir = TempDir::new().unwrap();
        let h = handler(&dir);

        // Probe with only rolling-5h fields (no weekly headers).
        h.dispatch(
            "quota.probe",
            ctx(json!({ "account": "pro-acct", "remaining": 250, "resets_at_secs": 20_000 })),
        )
        .await
        .unwrap();

        // quota.info: weekly_window must be null (not absent).
        let info = h
            .dispatch("quota.info", ctx(json!({ "account": "pro-acct" })))
            .await
            .unwrap();
        assert_eq!(
            info["weekly_window"],
            serde_json::Value::Null,
            "quota.info: weekly_window null when no weekly data"
        );

        // quota.list: same.
        let list = h.dispatch("quota.list", ctx(json!({}))).await.unwrap();
        let acc = list["accounts"]
            .as_array()
            .unwrap()
            .iter()
            .find(|a| a["id"] == "pro-acct")
            .expect("pro-acct in list");
        assert_eq!(
            acc["weekly_window"],
            serde_json::Value::Null,
            "quota.list: weekly_window null when no weekly data"
        );

        // Now probe with weekly fields.
        h.dispatch(
            "quota.probe",
            ctx(json!({
                "account": "pro-acct",
                "remaining": 250,
                "resets_at_secs": 20_000,
                "weekly_remaining": 10_000_000,
                "weekly_resets_at_secs": 604_800,
            })),
        )
        .await
        .unwrap();

        let info2 = h
            .dispatch("quota.info", ctx(json!({ "account": "pro-acct" })))
            .await
            .unwrap();
        let ww = &info2["weekly_window"];
        assert!(ww.is_object(), "quota.info: weekly_window must be object after weekly probe; got {ww}");
        assert_eq!(ww["kind"], "Weekly", "kind Weekly");
        assert!(ww["consumed"].as_f64().is_some(), "consumed present");
        assert!(ww["limit"].as_u64().is_some(), "limit present");
        assert!(ww["resets_at_secs"].as_u64().is_some(), "resets_at_secs present");
        assert!(ww["started_at_secs"].as_u64().is_some(), "started_at_secs present");
    }

    /// The per-workspace consumed metric sums the four usage counters and treats a
    /// missing field as zero — the raw token volume fed to `gt_workspace_quota_consumed`.
    #[test]
    fn sample_tokens_sums_usage_counters_defaulting_zero() {
        assert_eq!(
            sample_tokens(&json!({
                "input": 100, "output": 50, "cache_read": 10, "cache_creation": 5
            })),
            165
        );
        assert_eq!(
            sample_tokens(&json!({ "input": 7 })),
            7,
            "absent fields default to 0"
        );
        assert_eq!(sample_tokens(&json!({})), 0);
    }

    /// A rotation onto itself is a validation fault, and info on an unknown
    /// account is a not-found.
    #[tokio::test]
    async fn invalid_rotation_and_missing_account() {
        let dir = TempDir::new().unwrap();
        let h = handler(&dir);

        let gone = h
            .dispatch("quota.info", ctx(json!({ "account": "nope" })))
            .await
            .unwrap_err();
        assert!(matches!(gone, AppError::NotFound(_)));

        let bad = h
            .dispatch(
                "quota.rotate",
                ctx(json!({ "from_account": "acc-1", "to_account": "acc-1" })),
            )
            .await
            .unwrap_err();
        assert!(matches!(bad, AppError::Validation(_)));
    }

    /// The custodian's quota signal end-to-end (hq-context-custodian.2): the guard reads
    /// the SAME per-workspace quota log the `quota.*` handler writes, replays it into the
    /// registry, and answers `pool_genuinely_blocked`. It authorizes a context deferral
    /// ONLY on a non-empty, no-Healthy, freshly-blocked pool — and withdraws it the moment
    /// a probe shows recovered capacity. This is the transition→custodian→quota seam minus
    /// the Dolt store (the store leg is covered by the gated issues http_contract test).
    #[tokio::test]
    async fn quota_block_guard_defers_only_on_a_fresh_fully_blocked_pool() {
        let dir = TempDir::new().unwrap();
        let log = Arc::new(EventLog::new(Some(dir.path().to_path_buf())));
        let guard = QuotaBlockGuard::new(log.clone());

        // No accounts ⇒ nothing to confirm ⇒ context stays mandatory.
        assert!(!guard.pool_blocked(None).await, "empty pool never authorizes a deferral");

        // A single account, freshly blocked: a probe at remaining=0 with a far-future reset
        // seeds the window, then a Blocked event flips the status. now < resets_at ⇒ fresh.
        let future = 9_000_000_000u64;
        log.append(
            None,
            QuotaEvent::UsageProbed {
                account: "acc-1".into(),
                remaining: 0,
                resets_at_secs: future,
                weekly_remaining: None,
                weekly_resets_at_secs: None,
                now_secs: 1_000,
            },
        )
        .unwrap();
        log.append(
            None,
            QuotaEvent::Blocked { account: "acc-1".into(), until_secs: Some(future), now_secs: 1_000 },
        )
        .unwrap();
        assert!(
            guard.pool_blocked(None).await,
            "a fresh, fully-exhausted pool authorizes the context deferral"
        );

        // A probe showing recovered capacity lifts the account to Healthy ⇒ no deferral.
        log.append(
            None,
            QuotaEvent::UsageProbed {
                account: "acc-1".into(),
                remaining: 40_000_000,
                resets_at_secs: future,
                weekly_remaining: None,
                weekly_resets_at_secs: None,
                now_secs: 2_000,
            },
        )
        .unwrap();
        assert!(
            !guard.pool_blocked(None).await,
            "recovered capacity withdraws the deferral — context mandatory again"
        );
    }
}
