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

use async_trait::async_trait;
use serde::de::DeserializeOwned;
use serde_json::{json, Value};

use gt_events::{Command, EventKind};
use gt_mcp_server::{DomainCtx, DomainHandler};
use gt_quota::{
    Account, AccountRegistry, ProbeWindow, QuotaEvent, QuotaState, RotateAccount, SampleTokens,
};
use gt_store_dolt::AppError;

use super::eventlog::EventLog;
use super::util::{ev_err, parse_cmd, str_arg};

/// The event-log kind prefix for every quota event (`quota.*.v1`).
const NS: &str = "quota.";

/// Event-sourced handler for the `quota.*` tool namespace.
pub struct QuotaHandler {
    log: Arc<EventLog>,
}

impl QuotaHandler {
    /// Wrap the per-workspace event log.
    pub fn new(log: Arc<EventLog>) -> Self {
        Self { log }
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

    async fn dispatch(&self, tool: &str, ctx: DomainCtx<'_>) -> Result<Value, AppError> {
        let ws = ctx.workspace;
        match tool {
            "quota.sample" => self.run::<SampleTokens>(ws, ctx.args),
            "quota.probe" => self.run::<ProbeWindow>(ws, ctx.args),
            "quota.rotate" => self.run::<RotateAccount>(ws, ctx.args),
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
            other => Err(AppError::Validation(format!("unknown tool `{other}`"))),
        }
    }
}

/// Shape one account (id, status, window) as the dispatch payload. `Account`
/// derives `Serialize`, so the projection is the domain type verbatim.
fn account_json(account: &Account) -> Value {
    serde_json::to_value(account).unwrap_or_else(|_| json!({}))
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    fn handler(dir: &TempDir) -> QuotaHandler {
        QuotaHandler::new(Arc::new(EventLog::new(Some(dir.path().to_path_buf()))))
    }

    fn ctx(args: Value) -> DomainCtx<'static> {
        DomainCtx { workspace: None, actor: "tester", args }
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

        let info = h.dispatch("quota.info", ctx(json!({ "account": "acc-1" }))).await.unwrap();
        assert_eq!(info["id"], "acc-1");
        assert_eq!(info["status"], "Healthy");

        // Rotate parks the source account in Cooldown (replayed onto the entry).
        let rotated = h
            .dispatch(
                "quota.rotate",
                ctx(json!({ "from_account": "acc-1", "to_account": "acc-2" })),
            )
            .await
            .unwrap();
        assert_eq!(rotated["event"], "quota.rotated.v1");

        let info = h.dispatch("quota.info", ctx(json!({ "account": "acc-1" }))).await.unwrap();
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

    /// A rotation onto itself is a validation fault, and info on an unknown
    /// account is a not-found.
    #[tokio::test]
    async fn invalid_rotation_and_missing_account() {
        let dir = TempDir::new().unwrap();
        let h = handler(&dir);

        let gone = h.dispatch("quota.info", ctx(json!({ "account": "nope" }))).await.unwrap_err();
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
}
