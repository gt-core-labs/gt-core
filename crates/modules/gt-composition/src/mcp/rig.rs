//! `rig.*` domain dispatch (`hq-mcp-dispatch.3`).
//!
//! Routes the rig catalog tools — `rig.add`, `rig.adopt`, `rig.remove`,
//! `rig.set-prefix`, `rig.set-default-branch`, plus the `rig.list` / `rig.info`
//! reads — onto the [`RigCommand`] decide/apply layer over the PG-backed
//! [`PgRigs`] adapter.
//!
//! Each mutation hydrates the [`RigCatalog`] from the tenant's `rigs` table,
//! runs the command's `execute` (validate + mutate the in-memory catalog,
//! producing the [`RigEvent`]), then persists the touched row back to Postgres —
//! an upsert for add/adopt/prefix/branch, a delete for remove. The catalog read
//! is scoped to the caller's `ws_<slug>` schema by the [`WsPools`] connection's
//! `search_path`, so two workspaces never see each other's rigs.

use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use async_trait::async_trait;
use serde::de::DeserializeOwned;
use serde_json::{json, Value};

use gt_events::Command;
use gt_mcp_server::{DomainCtx, DomainHandler};
use gt_rig::{
    AddRig, AdoptRig, PgRigs, RemoveRig, RigCatalog, RigEntry, RigRepository, SetRigDefaultBranch,
    SetRigPrefix,
};
use gt_store_dolt::AppError;

use super::pools::WsPools;

/// PG-backed handler for the `rig.*` tool namespace.
pub struct RigHandler {
    pools: Arc<WsPools>,
}

impl RigHandler {
    /// Wrap the per-workspace pool cache.
    pub fn new(pools: Arc<WsPools>) -> Self {
        Self { pools }
    }
}

#[async_trait]
impl DomainHandler for RigHandler {
    fn namespace(&self) -> &'static str {
        "rig"
    }

    async fn dispatch(&self, tool: &str, ctx: DomainCtx<'_>) -> Result<Value, AppError> {
        let pool = self.pools.get(ctx.workspace).await?;
        let repo = PgRigs::new(pool.pool().clone());

        match tool {
            "rig.add" => {
                let cmd: AddRig = parse_cmd(ctx.args)?;
                apply_and_upsert(&repo, cmd.name.clone(), &cmd).await
            }
            "rig.adopt" => {
                let cmd: AdoptRig = parse_cmd(ctx.args)?;
                apply_and_upsert(&repo, cmd.name.clone(), &cmd).await
            }
            "rig.set-prefix" => {
                let cmd: SetRigPrefix = parse_cmd(ctx.args)?;
                apply_and_upsert(&repo, cmd.name.clone(), &cmd).await
            }
            "rig.set-default-branch" => {
                let cmd: SetRigDefaultBranch = parse_cmd(ctx.args)?;
                apply_and_upsert(&repo, cmd.name.clone(), &cmd).await
            }
            "rig.remove" => {
                let cmd: RemoveRig = parse_cmd(ctx.args)?;
                // Decide against the live catalog (rejects an absent rig as
                // NotFound), then delete the row.
                let mut catalog = hydrate(&repo).await?;
                cmd.execute(&mut catalog).map_err(ev_err)?;
                repo.remove(&cmd.name).await.map_err(ev_err)?;
                Ok(json!({ "ok": true, "rig": cmd.name, "removed": true }))
            }
            "rig.list" => {
                let rigs = repo.list().await.map_err(ev_err)?;
                Ok(json!({ "rigs": rigs.iter().map(entry_json).collect::<Vec<_>>() }))
            }
            "rig.info" => {
                let name = str_arg(&ctx.args, "name")?;
                match repo.get(name).await.map_err(ev_err)? {
                    Some(entry) => Ok(entry_json(&entry)),
                    None => Err(AppError::NotFound(format!("rig {name}"))),
                }
            }
            other => Err(AppError::Validation(format!("unknown tool `{other}`"))),
        }
    }
}

/// Hydrate a [`RigCatalog`] from the tenant's `rigs` table.
async fn hydrate(repo: &PgRigs) -> Result<RigCatalog, AppError> {
    let mut catalog = RigCatalog::default();
    for entry in repo.list().await.map_err(ev_err)? {
        catalog.apply_add(entry);
    }
    Ok(catalog)
}

/// Decide a catalog-mutating command, then persist the touched entry.
///
/// `execute` validates against the hydrated catalog and mutates it in memory
/// (the same path the actor takes); the entry the command touched is then
/// upserted back to Postgres so the change is durable.
async fn apply_and_upsert<C>(repo: &PgRigs, name: String, cmd: &C) -> Result<Value, AppError>
where
    C: Command<State = RigCatalog>,
{
    let mut catalog = hydrate(repo).await?;
    cmd.execute(&mut catalog).map_err(ev_err)?;
    let entry = catalog
        .get(&name)
        .cloned()
        .ok_or_else(|| AppError::Other(format!("rig {name} missing after execute")))?;
    repo.upsert(&entry).await.map_err(ev_err)?;
    Ok(json!({ "ok": true, "rig": name }))
}

/// Deserialize a command struct from the tool args, stamping `now_secs` with the
/// server clock when the caller omits it (the clock is the edge's to supply, not
/// the model's). A malformed payload is a validation fault.
fn parse_cmd<T: DeserializeOwned>(mut args: Value) -> Result<T, AppError> {
    if let Value::Object(map) = &mut args {
        map.entry("now_secs").or_insert_with(|| json!(now_secs()));
    }
    serde_json::from_value(args)
        .map_err(|e| AppError::Validation(format!("invalid arguments: {e}")))
}

/// Pull a required string argument.
fn str_arg<'a>(args: &'a Value, key: &str) -> Result<&'a str, AppError> {
    args.get(key)
        .and_then(Value::as_str)
        .ok_or_else(|| AppError::Validation(format!("missing string argument `{key}`")))
}

/// Server-side epoch-seconds clock for command timestamps.
fn now_secs() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or(0)
}

/// Shape one rig entry as the dispatch payload.
fn entry_json(entry: &RigEntry) -> Value {
    json!({
        "name": entry.name,
        "prefix": entry.prefix,
        "git_url": entry.git_url,
        "push_url": entry.push_url,
        "upstream_url": entry.upstream_url,
        "default_branch": entry.default_branch,
        "registered_at_secs": entry.registered_at_secs,
    })
}

/// Map a `gt-events` domain error onto the MCP error space the server maps from.
fn ev_err(e: gt_events::AppError) -> AppError {
    use gt_events::AppError as E;
    match e {
        E::NotFound(s) => AppError::NotFound(s),
        E::Validation(s) => AppError::Validation(s),
        E::InvalidTransition(s) => AppError::InvalidTransition(s),
        E::Handler(s) => AppError::Handler(s),
        other => AppError::Other(other.to_string()),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_cmd_stamps_now_secs_when_absent() {
        let cmd: AddRig = parse_cmd(json!({
            "name": "plane",
            "prefix": "pl",
            "git_url": "git@x:y/plane.git",
            "default_branch": "main",
        }))
        .unwrap();
        assert!(cmd.now_secs > 0, "server stamped a clock");
        assert_eq!(cmd.name, "plane");
    }

    #[test]
    fn parse_cmd_rejects_malformed() {
        // Missing required `prefix`/`git_url`/`default_branch`.
        let err = parse_cmd::<AddRig>(json!({ "name": "plane" })).unwrap_err();
        assert!(matches!(err, AppError::Validation(_)));
    }

    /// PG-backed round trip against the default workspace's `ws_default.rigs`
    /// table: add → reject-dup → info → set-prefix → list → remove → not-found.
    /// No-op without `GT_PG_URL`.
    #[tokio::test]
    async fn rig_round_trip_pg() {
        use gt_module::GtModule;
        use sqlx::PgPool;

        let Some(url) = std::env::var("GT_PG_URL").ok() else {
            eprintln!("GT_PG_URL unset; skipping RigHandler contract test");
            return;
        };
        let pool = PgPool::connect(&url).await.expect("connect postgres");
        // The module owns the `rigs` schema (ws_default template); apply it.
        let migs = gt_rig::RigsModule.migrations();
        sqlx::raw_sql(&migs[0].sql).execute(&pool).await.expect("apply rigs migration");
        sqlx::raw_sql("DELETE FROM ws_default.rigs WHERE name = 'dispatchrig'")
            .execute(&pool)
            .await
            .ok();

        let handler = RigHandler::new(Arc::new(WsPools::new(url)));
        let ctx = |args| DomainCtx { workspace: None, actor: "tester", args };
        let add_args = json!({
            "name": "dispatchrig", "prefix": "dr",
            "git_url": "git@x:y/d.git", "default_branch": "main"
        });

        let added = handler.dispatch("rig.add", ctx(add_args.clone())).await.unwrap();
        assert_eq!(added["ok"], true);
        assert_eq!(added["rig"], "dispatchrig");

        // Re-add is rejected (name + prefix collision) as a validation fault.
        let dup = handler.dispatch("rig.add", ctx(add_args)).await.unwrap_err();
        assert!(matches!(dup, AppError::Validation(_)));

        let info = handler
            .dispatch("rig.info", ctx(json!({ "name": "dispatchrig" })))
            .await
            .unwrap();
        assert_eq!(info["prefix"], "dr");
        assert_eq!(info["default_branch"], "main");

        handler
            .dispatch("rig.set-prefix", ctx(json!({ "name": "dispatchrig", "new_prefix": "dx" })))
            .await
            .unwrap();
        let info2 = handler
            .dispatch("rig.info", ctx(json!({ "name": "dispatchrig" })))
            .await
            .unwrap();
        assert_eq!(info2["prefix"], "dx", "prefix change persisted to PG");

        let list = handler.dispatch("rig.list", ctx(json!({}))).await.unwrap();
        assert!(list["rigs"]
            .as_array()
            .unwrap()
            .iter()
            .any(|r| r["name"] == "dispatchrig"));

        handler
            .dispatch("rig.remove", ctx(json!({ "name": "dispatchrig" })))
            .await
            .unwrap();
        let gone = handler
            .dispatch("rig.info", ctx(json!({ "name": "dispatchrig" })))
            .await
            .unwrap_err();
        assert!(matches!(gone, AppError::NotFound(_)));
    }

    /// Per-workspace rig isolation (`hq-mt-rigs.2`, re-scoped to a verification
    /// bead per the docs/04 §15 conflict gap).
    ///
    /// rigs.2 originally specified a multi-tenant `RigCatalog` keyed by
    /// `WorkspaceId` — the shared-table model **rejected** by the schema-per-ws
    /// partitioning resolution. There is no such catalog: `RigHandler` hydrates a
    /// fresh single-tenant catalog per request, scoped by the `WorkspacePool`'s
    /// `search_path` to the caller's `ws_<slug>.rigs`. This test *proves* the two
    /// properties rigs.2 wanted are already delivered structurally:
    ///
    /// 1. **same prefix in distinct workspaces does not collide** — the `rigs`
    ///    `UNIQUE(prefix)` constraint is per-schema, so tenant `a` and tenant `b`
    ///    can each register a rig with the same name + prefix (a shared
    ///    `WorkspaceId`-keyed table would reject the second on the unique index);
    /// 2. **no cross-tenant leak** — a rig registered only in `a` is invisible to
    ///    `b`'s reads.
    ///
    /// No-op without `GT_PG_URL`.
    #[tokio::test]
    async fn rig_per_workspace_isolation_pg() {
        use gt_module::GtModule;
        use sqlx::PgPool;

        let Some(url) = std::env::var("GT_PG_URL").ok() else {
            eprintln!("GT_PG_URL unset; skipping per-workspace rig isolation test");
            return;
        };
        let pool = PgPool::connect(&url).await.expect("connect postgres");

        // 1. Catalog table + the `gt_create_workspace_schema` provisioning fn.
        for mig in gt_store_pg::workspace_migrations() {
            sqlx::raw_sql(&mig.sql).execute(&pool).await.expect("apply workspace migration");
        }
        // 2. The `rigs` template in `ws_default` (the provisioner clones from it).
        let migs = gt_rig::RigsModule.migrations();
        sqlx::raw_sql(&migs[0].sql).execute(&pool).await.expect("apply rigs migration");
        // 3. Provision two tenant schemas — each gets its own `rigs` table with a
        //    schema-local UNIQUE(prefix) (idempotent; safe across reruns).
        for ws in ["a", "b"] {
            sqlx::query("SELECT gt_create_workspace_schema($1)")
                .bind(ws) // bare slug; the fn prepends `ws_` (→ schema `ws_a`/`ws_b`)
                .execute(&pool)
                .await
                .unwrap_or_else(|e| panic!("provision ws_{ws}: {e}"));
            sqlx::raw_sql(&format!("DELETE FROM ws_{ws}.rigs"))
                .execute(&pool)
                .await
                .ok();
        }

        let handler = RigHandler::new(Arc::new(WsPools::new(url)));
        let ctx = |ws: &'static str, args| DomainCtx { workspace: Some(ws), actor: "tester", args };
        let shared = || json!({
            "name": "granite", "prefix": "gr",
            "git_url": "git@x:y/granite.git", "default_branch": "main"
        });

        // Property 1: the SAME name + prefix registers cleanly in BOTH tenants.
        let in_a = handler.dispatch("rig.add", ctx("a", shared())).await.unwrap();
        assert_eq!(in_a["ok"], true);
        let in_b = handler.dispatch("rig.add", ctx("b", shared())).await.unwrap();
        assert_eq!(in_b["ok"], true, "same prefix in a distinct ws must not collide");

        // A rig that lives only in tenant `a`.
        handler
            .dispatch(
                "rig.add",
                ctx("a", json!({
                    "name": "plane", "prefix": "pl",
                    "git_url": "git@x:y/plane.git", "default_branch": "main"
                })),
            )
            .await
            .unwrap();

        // Property 2: `b` sees only its own `granite`, never `a`'s `plane`.
        let list_b = handler.dispatch("rig.list", ctx("b", json!({}))).await.unwrap();
        let names_b: Vec<&str> = list_b["rigs"]
            .as_array()
            .unwrap()
            .iter()
            .filter_map(|r| r["name"].as_str())
            .collect();
        assert!(names_b.contains(&"granite"), "b keeps its own rig");
        assert!(!names_b.contains(&"plane"), "no cross-tenant leak from a into b");
        let leaked = handler
            .dispatch("rig.info", ctx("b", json!({ "name": "plane" })))
            .await
            .unwrap_err();
        assert!(matches!(leaked, AppError::NotFound(_)), "a's rig is not found in b");

        // `a` has both.
        let list_a = handler.dispatch("rig.list", ctx("a", json!({}))).await.unwrap();
        let names_a: Vec<&str> = list_a["rigs"]
            .as_array()
            .unwrap()
            .iter()
            .filter_map(|r| r["name"].as_str())
            .collect();
        assert!(names_a.contains(&"granite") && names_a.contains(&"plane"));

        // Cleanup so the test is rerunnable.
        for ws in ["a", "b"] {
            sqlx::raw_sql(&format!("DELETE FROM ws_{ws}.rigs"))
                .execute(&pool)
                .await
                .ok();
        }
    }
}
