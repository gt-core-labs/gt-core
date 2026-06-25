//! Operator notification when a rig loses its VCS connection (gtcore-406b12, epic gtcore-0e095b).
//!
//! ## The gap this closes
//!
//! After the dev data-wipe the four rigs silently lost their `git_connection_ref` (the rigs page
//! showed CONNECTION "—") and nobody noticed until a polecat `git push` failed. Nothing watched the
//! binding. This periodic sweep rings the operator bell the moment a rig is **unbound** or points at
//! a **disabled/revoked/missing** connection — so the loss surfaces immediately, not at the next
//! failed clone.
//!
//! ## Shape
//!
//! Same observer/ticker family as [`crate::escalation_notify`]: a low-cadence
//! [`RigConnectionHealthTicker`] that, each tick, snapshots the rig catalog + the visible VCS
//! connections, computes the unhealthy rigs ([`rig_issue`], pure), and rings the operator bell
//! ([`crate::escalation_notify::OperatorNotifier`]) for each rig that BECAME unhealthy since the last
//! tick. Dedup is by an in-memory "currently unhealthy" set: a rig that stays unbound is rung ONCE,
//! not every tick (a process restart re-pings once — desirable: it re-surfaces a still-broken rig).
//!
//! Best-effort throughout: a snapshot/bell failure is logged and never breaks the loop. The catalog
//! stays the source of truth — a missed bell is an observability gap, not a lost binding.
//!
//! Env-gated in the bin on `GT_RIG_CONNECTION_CHECK_SECS > 0` (and `GT_PG_URL`, for the bell), so it
//! never fires in tests or a deploy that has not opted in.

use std::collections::BTreeSet;
use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;

use gt_events::AppError;
use gt_rig::{PgRigs, RigRepository};
use gt_store_pg::WorkspacePool;
use gt_vcs::{PgVcsConnections, VcsConnectionRepo};

use crate::escalation_notify::OperatorNotifier;

/// Why a rig's connection is unhealthy. Pure data — [`draft_for`] turns it into operator prose.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ConnIssue {
    /// No `git_connection_ref` at all — the rig is cloned on the operator-mounted token path, so no
    /// JIT installation tokens are minted for it.
    Unbound,
    /// `git_connection_ref` points at a connection id no longer visible to the workspace.
    Missing(String),
    /// `git_connection_ref` points at a connection that exists but is not `active` (id, status).
    Inactive(String, String),
}

/// The connection issue for one rig, or `None` when it is bound to an ACTIVE connection — the pure
/// core of the sweep, decided against `connections` given as `(id, status)` pairs (the workspace's
/// own rows plus the globals). An empty/whitespace `git_connection_ref` is treated as unbound, so
/// `""` and `None` mean the same thing (matching `rig.set-connection`'s clear semantics).
pub fn rig_issue(git_connection_ref: Option<&str>, connections: &[(String, String)]) -> Option<ConnIssue> {
    let bound = git_connection_ref.map(str::trim).filter(|s| !s.is_empty());
    let Some(id) = bound else {
        return Some(ConnIssue::Unbound);
    };
    match connections.iter().find(|(cid, _)| cid == id) {
        None => Some(ConnIssue::Missing(id.to_string())),
        Some((_, status)) if status == "active" => None,
        Some((_, status)) => Some(ConnIssue::Inactive(id.to_string(), status.clone())),
    }
}

/// What an unhealthy rig renders to before any I/O — pure, so the wording is unit-testable.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct HealthDraft {
    pub title: String,
    pub body: String,
    /// `warning` — this informs (and asks the operator to reconnect); the bell renders it as an
    /// alert, distinct from an escalation's `decision`.
    pub kind: &'static str,
}

/// The `{public_url}/rigs` deep link to the rigs page where the operator reconnects.
fn rigs_link(public_url: &str) -> String {
    format!("{}/rigs", public_url.trim_end_matches('/'))
}

/// Render an unhealthy rig into the operator notification it warrants. `link` is appended verbatim
/// so the body is self-contained in the bell.
pub fn draft_for(rig: &str, issue: &ConnIssue, link: &str) -> HealthDraft {
    let reason = match issue {
        ConnIssue::Unbound => "no tiene una conexión VCS vinculada (git_connection_ref vacío)".to_string(),
        ConnIssue::Missing(id) => format!("apunta a una conexión inexistente ({id})"),
        ConnIssue::Inactive(id, status) => format!("su conexión {id} está {status}"),
    };
    let body = format!(
        "El rig '{rig}' {reason}. Sin conexión activa no se mintean tokens JIT, \
         así que clone/push de los polecats y el refinery dependen del token operator-mounted.\n\n\
         Reconéctalo aquí: {link}",
    );
    HealthDraft {
        title: format!("Rig '{rig}' sin conexión VCS"),
        body,
        kind: "warning",
    }
}

/// The data the ticker needs each pass: every rig's `(name, git_connection_ref)` plus the visible
/// connections as `(id, status)`. Abstracted so the tick is unit-testable without Postgres.
#[async_trait]
pub trait RigHealthSource: Send + Sync {
    async fn snapshot(&self) -> Result<(Vec<(String, Option<String>)>, Vec<(String, String)>), AppError>;
}

/// The production [`RigHealthSource`]: reads the workspace's rig catalog (`ws_<slug>.rigs`) and the
/// connections visible to it (its own + the globals) off a per-workspace pool.
pub struct PgRigHealthSource {
    pool: WorkspacePool,
    workspace: String,
}

impl PgRigHealthSource {
    /// Connect a per-workspace pool from `GT_PG_URL` and `workspace` (the same seam the dispatch
    /// routing and drift-reconcile use).
    pub async fn connect(pg_url: &str, workspace: impl Into<String>) -> Result<Self, AppError> {
        let workspace = workspace.into();
        let pool = WorkspacePool::connect(pg_url, &workspace)
            .await
            .map_err(|e| AppError::Other(format!("rig-conn-health: pool connect failed: {e}")))?;
        Ok(Self { pool, workspace })
    }
}

#[async_trait]
impl RigHealthSource for PgRigHealthSource {
    async fn snapshot(&self) -> Result<(Vec<(String, Option<String>)>, Vec<(String, String)>), AppError> {
        let rigs = PgRigs::new(self.pool.pool().clone());
        let connections = PgVcsConnections::new(self.pool.pool().clone());
        let rig_rows = rigs
            .list()
            .await?
            .into_iter()
            .map(|e| (e.name, e.git_connection_ref))
            .collect();
        let conn_rows = connections
            .list_for_workspace(&self.workspace)
            .await?
            .into_iter()
            .map(|c| (c.id, c.status.as_str().to_string()))
            .collect();
        Ok((rig_rows, conn_rows))
    }
}

/// A periodic sweep that rings the operator bell when a rig becomes unbound / its connection goes
/// inactive. Dedups in-memory so a still-broken rig is rung once per transition, not every tick.
pub struct RigConnectionHealthTicker {
    source: Arc<dyn RigHealthSource>,
    notifier: OperatorNotifier,
    interval_secs: u64,
    public_url: String,
}

impl RigConnectionHealthTicker {
    pub fn new(
        source: Arc<dyn RigHealthSource>,
        notifier: OperatorNotifier,
        interval_secs: u64,
        public_url: impl Into<String>,
    ) -> Self {
        Self {
            source,
            notifier,
            interval_secs,
            public_url: public_url.into(),
        }
    }

    /// One pass over the catalog: returns the rigs unhealthy NOW, ringing the bell for each that was
    /// NOT in `previously_unhealthy`. Split out (and returning the new set) so it is unit-testable
    /// without a clock. Best-effort: a snapshot failure logs and returns the prior set unchanged
    /// (no spurious "recovered" on a transient read error).
    async fn sweep(&self, previously_unhealthy: &BTreeSet<String>) -> BTreeSet<String> {
        let (rigs, connections) = match self.source.snapshot().await {
            Ok(snap) => snap,
            Err(e) => {
                eprintln!("[rig-conn-health] snapshot failed, keeping prior state: {e}");
                return previously_unhealthy.clone();
            }
        };
        let link = rigs_link(&self.public_url);
        let mut now_unhealthy = BTreeSet::new();
        for (rig, conn_ref) in &rigs {
            let Some(issue) = rig_issue(conn_ref.as_deref(), &connections) else {
                continue;
            };
            now_unhealthy.insert(rig.clone());
            if !previously_unhealthy.contains(rig) {
                let draft = draft_for(rig, &issue, &link);
                self.notifier.ring(&draft.title, &draft.body, draft.kind).await;
            }
        }
        now_unhealthy
    }

    /// Run the sweep forever on `interval_secs` (floored at 1s). Consumes `self`; the in-memory
    /// dedup set lives for the process lifetime.
    pub async fn run(self) {
        let mut unhealthy = BTreeSet::new();
        loop {
            unhealthy = self.sweep(&unhealthy).await;
            tokio::time::sleep(Duration::from_secs(self.interval_secs.max(1))).await;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Mutex;

    fn conns(pairs: &[(&str, &str)]) -> Vec<(String, String)> {
        pairs.iter().map(|(a, b)| (a.to_string(), b.to_string())).collect()
    }

    #[test]
    fn rig_issue_flags_unbound_missing_and_inactive_but_not_active() {
        let c = conns(&[("gh-active", "active"), ("gh-off", "disabled")]);

        // Bound to an active connection → healthy.
        assert_eq!(rig_issue(Some("gh-active"), &c), None);

        // Unbound (None or empty/whitespace) → Unbound.
        assert_eq!(rig_issue(None, &c), Some(ConnIssue::Unbound));
        assert_eq!(rig_issue(Some("   "), &c), Some(ConnIssue::Unbound));

        // Bound to a non-existent id → Missing.
        assert_eq!(
            rig_issue(Some("gh-gone"), &c),
            Some(ConnIssue::Missing("gh-gone".into()))
        );

        // Bound to an inactive connection → Inactive with the status.
        assert_eq!(
            rig_issue(Some("gh-off"), &c),
            Some(ConnIssue::Inactive("gh-off".into(), "disabled".into()))
        );

        // The bound id is trimmed before the lookup.
        assert_eq!(rig_issue(Some("  gh-active  "), &c), None);
    }

    #[test]
    fn draft_is_actionable_and_carries_the_reconnect_link() {
        let d = draft_for("gtcore", &ConnIssue::Unbound, &rigs_link("https://gt.example.com/"));
        assert!(d.title.contains("gtcore"));
        assert!(d.body.contains("https://gt.example.com/rigs"));
        assert_eq!(d.kind, "warning");
        // Missing/Inactive name the offending id.
        let dm = draft_for("gtweb", &ConnIssue::Missing("gh-x".into()), "L");
        assert!(dm.body.contains("gh-x"));
    }

    /// A source returning a fixed snapshot, recording how many times it was polled.
    struct FakeSource {
        rigs: Vec<(String, Option<String>)>,
        connections: Vec<(String, String)>,
    }
    #[async_trait]
    impl RigHealthSource for FakeSource {
        async fn snapshot(&self) -> Result<(Vec<(String, Option<String>)>, Vec<(String, String)>), AppError> {
            Ok((self.rigs.clone(), self.connections.clone()))
        }
    }

    /// A notifier-less sweep harness: re-implements `sweep`'s dedup over a recording sink so we can
    /// assert "ring once per transition" without standing up Postgres for [`OperatorNotifier`].
    async fn rung_over_two_sweeps(
        source: &FakeSource,
    ) -> Vec<String> {
        let rung = Mutex::new(Vec::new());
        let link = "L".to_string();
        let pass = |prev: &BTreeSet<String>| {
            let (rigs, connections) = (source.rigs.clone(), source.connections.clone());
            let mut now = BTreeSet::new();
            for (rig, conn_ref) in &rigs {
                if rig_issue(conn_ref.as_deref(), &connections).is_some() {
                    now.insert(rig.clone());
                    if !prev.contains(rig) {
                        rung.lock().unwrap().push(rig.clone());
                    }
                }
            }
            now
        };
        let after_first = pass(&BTreeSet::new());
        let _after_second = pass(&after_first); // unchanged snapshot → no new rings
        let _ = &link;
        rung.into_inner().unwrap()
    }

    #[tokio::test]
    async fn unhealthy_rig_is_rung_once_not_every_sweep() {
        let source = FakeSource {
            rigs: vec![
                ("gtcore".into(), None),                         // unbound → unhealthy
                ("gtweb".into(), Some("gh-active".into())),      // healthy
                ("gtdocs".into(), Some("gh-gone".into())),       // missing → unhealthy
            ],
            connections: conns(&[("gh-active", "active")]),
        };
        let rung = rung_over_two_sweeps(&source).await;
        // Both unhealthy rigs rung exactly once across two identical sweeps; the healthy one never.
        assert_eq!(rung, vec!["gtcore".to_string(), "gtdocs".to_string()]);
    }
}
