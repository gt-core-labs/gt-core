//! `workspace.*` domain dispatch (`hq-mcp-dispatch.2`).
//!
//! Routes the `workspace.create` / `workspace.list` / `workspace.info` tools onto
//! the [`WorkspaceCommand`] decide/apply layer over the PG-backed
//! [`PgWorkspaces`] adapter. `create` runs the full
//! [`WorkspaceActor`](gt_workspace::WorkspaceActor) cycle (decide → apply →
//! persist → log) so the catalog is mutated only through replayable events;
//! `list` / `info` are pure reads straight off the repository.

use async_trait::async_trait;
use serde_json::{json, Value};
use sqlx::PgPool;

use gt_mcp_server::{DomainCtx, DomainHandler};
use gt_store_dolt::AppError;
use gt_workspace::{
    ActorError, PgWorkspaces, WorkspaceActor, WorkspaceCommand, WorkspaceEntry, WorkspaceError,
    WorkspaceId, WorkspaceRepository, WorkspaceStatus,
};

/// PG-backed handler for the `workspace.*` tool namespace.
///
/// Holds a [`PgPool`]; every call builds a [`PgWorkspaces`] over a clone (cheap —
/// `PgPool` is an `Arc`) so the workspace catalog is the durable Postgres table,
/// shared across requests.
pub struct WorkspaceHandler {
    pool: PgPool,
}

impl WorkspaceHandler {
    /// Wrap a connection pool. The `workspaces` table is expected to already
    /// exist (applied via the `gt-store-pg` migration at boot).
    pub fn new(pool: PgPool) -> Self {
        Self { pool }
    }

    /// A repository over the shared pool.
    fn repo(&self) -> PgWorkspaces {
        PgWorkspaces::new(self.pool.clone())
    }
}

#[async_trait]
impl DomainHandler for WorkspaceHandler {
    fn namespace(&self) -> &'static str {
        "workspace"
    }

    async fn dispatch(&self, tool: &str, ctx: DomainCtx<'_>) -> Result<Value, AppError> {
        match tool {
            "workspace.create" => {
                let id = workspace_id(str_arg(&ctx.args, "id")?)?;
                let name = str_arg(&ctx.args, "name")?.to_string();
                // Hydrate from Postgres, then run the command through the actor so
                // the change is an applied event (replay-authoritative), persisted
                // back to the `workspaces` table.
                let mut actor = WorkspaceActor::hydrate(self.repo()).await.map_err(actor_err)?;
                actor
                    .handle(WorkspaceCommand::Create {
                        id: id.clone(),
                        name: name.clone(),
                    })
                    .await
                    .map_err(actor_err)?;
                Ok(json!({
                    "ok": true,
                    "id": id.as_str(),
                    "name": name,
                    "status": status_str(WorkspaceStatus::Active),
                }))
            }
            "workspace.suspend" => {
                // Reversibly disable an active workspace. The decide layer rejects a
                // non-active source (IllegalTransition) as a validation fault, so a
                // suspended/archived workspace cannot be re-suspended.
                let id = workspace_id(str_arg(&ctx.args, "id")?)?;
                let mut actor = WorkspaceActor::hydrate(self.repo()).await.map_err(actor_err)?;
                actor
                    .handle(WorkspaceCommand::Suspend { id: id.clone() })
                    .await
                    .map_err(actor_err)?;
                Ok(json!({
                    "ok": true,
                    "id": id.as_str(),
                    "status": status_str(WorkspaceStatus::Suspended),
                }))
            }
            "workspace.archive" => {
                // Terminal transition from Active or Suspended. The decide layer rejects an
                // already-archived workspace (IllegalTransition) as a validation fault. The
                // on-disk teardown (Dolt snapshot + tar the event log) is a deploy-edge step;
                // this tool records only the orchestrator's status change.
                let id = workspace_id(str_arg(&ctx.args, "id")?)?;
                let mut actor = WorkspaceActor::hydrate(self.repo()).await.map_err(actor_err)?;
                actor
                    .handle(WorkspaceCommand::Archive { id: id.clone() })
                    .await
                    .map_err(actor_err)?;
                Ok(json!({
                    "ok": true,
                    "id": id.as_str(),
                    "status": status_str(WorkspaceStatus::Archived),
                }))
            }
            "workspace.list" => {
                let entries = self.repo().list().await.map_err(repo_err)?;
                Ok(json!({
                    "workspaces": entries.iter().map(entry_json).collect::<Vec<_>>(),
                }))
            }
            "workspace.info" => {
                let id = workspace_id(str_arg(&ctx.args, "id")?)?;
                match self.repo().load(&id).await.map_err(repo_err)? {
                    Some(entry) => Ok(entry_json(&entry)),
                    None => Err(AppError::NotFound(format!("workspace {id}"))),
                }
            }
            other => Err(AppError::Validation(format!("unknown tool `{other}`"))),
        }
    }
}

/// Pull a required string argument, rejecting a missing/non-string value as a
/// validation fault (not a 500).
fn str_arg<'a>(args: &'a Value, key: &str) -> Result<&'a str, AppError> {
    args.get(key)
        .and_then(Value::as_str)
        .ok_or_else(|| AppError::Validation(format!("missing string argument `{key}`")))
}

/// Parse a workspace slug, surfacing a malformed id as a validation fault.
fn workspace_id(raw: &str) -> Result<WorkspaceId, AppError> {
    WorkspaceId::new(raw).map_err(|e| AppError::Validation(e.to_string()))
}

/// The snake_case spelling of a status, matching the serde representation.
fn status_str(status: WorkspaceStatus) -> &'static str {
    match status {
        WorkspaceStatus::Active => "active",
        WorkspaceStatus::Suspended => "suspended",
        WorkspaceStatus::Archived => "archived",
    }
}

/// Shape one catalog entry as the dispatch payload.
fn entry_json(entry: &WorkspaceEntry) -> Value {
    json!({
        "id": entry.id.as_str(),
        "name": entry.name,
        "status": status_str(entry.status),
    })
}

/// Map an actor failure onto the MCP error space: a missing target is a
/// not-found, any other rejection is a caller-side validation fault, and an
/// apply/persist failure is internal.
fn actor_err(e: ActorError) -> AppError {
    match e {
        ActorError::Rejected(WorkspaceError::NotFound(id)) => {
            AppError::NotFound(format!("workspace {id}"))
        }
        ActorError::Rejected(rejected) => AppError::Validation(rejected.to_string()),
        other => AppError::Other(other.to_string()),
    }
}

/// Map a repository failure onto an internal error (a backend/consistency fault,
/// not a caller fault).
fn repo_err(e: gt_workspace::RepoError) -> AppError {
    AppError::Other(e.to_string())
}

#[cfg(test)]
mod tests {
    use super::*;

    /// `workspace.create` rejects a payload without an `id`.
    #[tokio::test]
    async fn create_requires_id() {
        // A lazy pool URL parses without connecting; the missing-arg check fires
        // before any I/O, so no Postgres is needed.
        let pool = PgPool::connect_lazy("postgres://gt@127.0.0.1:1/none").unwrap();
        let handler = WorkspaceHandler::new(pool);
        let ctx = DomainCtx {
            workspace: None,
            actor: "tester",
            args: json!({ "name": "Acme" }),
        };
        let err = handler.dispatch("workspace.create", ctx).await.unwrap_err();
        assert!(matches!(err, AppError::Validation(_)));
    }

    /// `workspace.suspend` rejects a payload without an `id` before any I/O.
    #[tokio::test]
    async fn suspend_requires_id() {
        let pool = PgPool::connect_lazy("postgres://gt@127.0.0.1:1/none").unwrap();
        let handler = WorkspaceHandler::new(pool);
        let ctx = DomainCtx { workspace: None, actor: "tester", args: json!({}) };
        let err = handler.dispatch("workspace.suspend", ctx).await.unwrap_err();
        assert!(matches!(err, AppError::Validation(_)));
    }

    /// `workspace.archive` rejects a payload without an `id` before any I/O.
    #[tokio::test]
    async fn archive_requires_id() {
        let pool = PgPool::connect_lazy("postgres://gt@127.0.0.1:1/none").unwrap();
        let handler = WorkspaceHandler::new(pool);
        let ctx = DomainCtx { workspace: None, actor: "tester", args: json!({}) };
        let err = handler.dispatch("workspace.archive", ctx).await.unwrap_err();
        assert!(matches!(err, AppError::Validation(_)));
    }

    /// An unknown verb in the namespace is a validation fault, not a panic.
    #[tokio::test]
    async fn unknown_verb_is_validation() {
        let pool = PgPool::connect_lazy("postgres://gt@127.0.0.1:1/none").unwrap();
        let handler = WorkspaceHandler::new(pool);
        let ctx = DomainCtx {
            workspace: None,
            actor: "tester",
            args: json!({}),
        };
        let err = handler.dispatch("workspace.frobnicate", ctx).await.unwrap_err();
        assert!(matches!(err, AppError::Validation(_)));
    }

    /// Connect to the contract-test Postgres, or `None` when `GT_PG_URL` is unset
    /// so the suite is a no-op off a developer box / CI without PG.
    async fn pool_or_skip() -> Option<PgPool> {
        let url = std::env::var("GT_PG_URL").ok()?;
        Some(PgPool::connect(&url).await.expect("GT_PG_URL must point at a reachable Postgres"))
    }

    /// PG-backed round trip: `workspace.create` persists through the actor, then
    /// `workspace.info` and `workspace.list` read it back from Postgres — proving
    /// the handler shares durable state across calls (the bead's whole point).
    #[tokio::test]
    async fn create_then_read_back_round_trip() {
        let Some(pool) = pool_or_skip().await else {
            eprintln!("GT_PG_URL unset; skipping WorkspaceHandler contract test");
            return;
        };
        // The `workspaces` table is the one the gt-store-pg migration creates.
        // `raw_sql` uses the simple-query protocol so the multi-statement migration
        // (CREATE TABLE + seed INSERT) runs in one call.
        let sql = &gt_store_pg::workspace_migrations()[0].sql;
        sqlx::raw_sql(sql).execute(&pool).await.expect("apply workspaces migration");
        sqlx::query("DELETE FROM workspaces WHERE id = $1")
            .bind("dispatch-test")
            .execute(&pool)
            .await
            .unwrap();

        let handler = WorkspaceHandler::new(pool.clone());
        let ctx = |args| DomainCtx { workspace: None, actor: "tester", args };

        let created = handler
            .dispatch("workspace.create", ctx(json!({ "id": "dispatch-test", "name": "Dispatch" })))
            .await
            .unwrap();
        assert_eq!(created["status"], "active");
        assert_eq!(created["id"], "dispatch-test");

        // Re-create is rejected (the actor decides AlreadyExists) as a validation fault.
        let dup = handler
            .dispatch("workspace.create", ctx(json!({ "id": "dispatch-test", "name": "X" })))
            .await
            .unwrap_err();
        assert!(matches!(dup, AppError::Validation(_)));

        // info reads it back from Postgres.
        let info = handler
            .dispatch("workspace.info", ctx(json!({ "id": "dispatch-test" })))
            .await
            .unwrap();
        assert_eq!(info["name"], "Dispatch");

        // list includes it.
        let list = handler.dispatch("workspace.list", ctx(json!({}))).await.unwrap();
        let ids: Vec<&str> = list["workspaces"]
            .as_array()
            .unwrap()
            .iter()
            .map(|w| w["id"].as_str().unwrap())
            .collect();
        assert!(ids.contains(&"dispatch-test"));

        // info on an absent id is a not-found.
        let missing = handler
            .dispatch("workspace.info", ctx(json!({ "id": "no-such-ws" })))
            .await
            .unwrap_err();
        assert!(matches!(missing, AppError::NotFound(_)));

        sqlx::query("DELETE FROM workspaces WHERE id = $1")
            .bind("dispatch-test")
            .execute(&pool)
            .await
            .unwrap();
    }

    /// PG-backed: `workspace.suspend` transitions Active → Suspended and persists it;
    /// a second suspend is an illegal transition (validation fault).
    #[tokio::test]
    async fn suspend_transitions_active_then_rejects_resuspend() {
        let Some(pool) = pool_or_skip().await else {
            eprintln!("GT_PG_URL unset; skipping WorkspaceHandler suspend test");
            return;
        };
        let sql = &gt_store_pg::workspace_migrations()[0].sql;
        sqlx::raw_sql(sql).execute(&pool).await.expect("apply workspaces migration");
        sqlx::query("DELETE FROM workspaces WHERE id = $1")
            .bind("suspend-test")
            .execute(&pool)
            .await
            .unwrap();

        let handler = WorkspaceHandler::new(pool.clone());
        let ctx = |args| DomainCtx { workspace: None, actor: "tester", args };

        handler
            .dispatch("workspace.create", ctx(json!({ "id": "suspend-test", "name": "S" })))
            .await
            .unwrap();

        let suspended = handler
            .dispatch("workspace.suspend", ctx(json!({ "id": "suspend-test" })))
            .await
            .unwrap();
        assert_eq!(suspended["status"], "suspended");

        // Persisted: info reads back the suspended status.
        let info = handler
            .dispatch("workspace.info", ctx(json!({ "id": "suspend-test" })))
            .await
            .unwrap();
        assert_eq!(info["status"], "suspended");

        // Re-suspend is an illegal transition (Suspended is not Active) → validation.
        let again = handler
            .dispatch("workspace.suspend", ctx(json!({ "id": "suspend-test" })))
            .await
            .unwrap_err();
        assert!(matches!(again, AppError::Validation(_)));

        sqlx::query("DELETE FROM workspaces WHERE id = $1")
            .bind("suspend-test")
            .execute(&pool)
            .await
            .unwrap();
    }

    /// PG-backed: `workspace.archive` transitions Suspended → Archived (terminal) and
    /// persists it; a second archive is an illegal transition (validation fault).
    #[tokio::test]
    async fn archive_is_terminal_and_rejects_rearchive() {
        let Some(pool) = pool_or_skip().await else {
            eprintln!("GT_PG_URL unset; skipping WorkspaceHandler archive test");
            return;
        };
        let sql = &gt_store_pg::workspace_migrations()[0].sql;
        sqlx::raw_sql(sql).execute(&pool).await.expect("apply workspaces migration");
        sqlx::query("DELETE FROM workspaces WHERE id = $1")
            .bind("archive-test")
            .execute(&pool)
            .await
            .unwrap();

        let handler = WorkspaceHandler::new(pool.clone());
        let ctx = |args| DomainCtx { workspace: None, actor: "tester", args };

        handler
            .dispatch("workspace.create", ctx(json!({ "id": "archive-test", "name": "A" })))
            .await
            .unwrap();
        // Archive straight from Active is allowed too, but go via Suspended to prove both.
        handler.dispatch("workspace.suspend", ctx(json!({ "id": "archive-test" }))).await.unwrap();

        let archived = handler
            .dispatch("workspace.archive", ctx(json!({ "id": "archive-test" })))
            .await
            .unwrap();
        assert_eq!(archived["status"], "archived");

        // Persisted.
        let info = handler.dispatch("workspace.info", ctx(json!({ "id": "archive-test" }))).await.unwrap();
        assert_eq!(info["status"], "archived");

        // Re-archive rejected (Archived is terminal) → validation.
        let again = handler
            .dispatch("workspace.archive", ctx(json!({ "id": "archive-test" })))
            .await
            .unwrap_err();
        assert!(matches!(again, AppError::Validation(_)));

        sqlx::query("DELETE FROM workspaces WHERE id = $1")
            .bind("archive-test")
            .execute(&pool)
            .await
            .unwrap();
    }

    #[test]
    fn status_strings_match_serde() {
        for s in [
            WorkspaceStatus::Active,
            WorkspaceStatus::Suspended,
            WorkspaceStatus::Archived,
        ] {
            let serde = serde_json::to_value(s).unwrap();
            assert_eq!(serde, json!(status_str(s)));
        }
    }
}
