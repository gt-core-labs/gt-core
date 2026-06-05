//! Gate for hq-gap-store-dolt-ensure-schema-per-workspace (F2): a freshly-created
//! per-workspace Dolt DB `hq_<ws>` self-heals its schema on first access.
//!
//! The multi-tenant data plane creates the tenant database (`CREATE DATABASE`) but
//! never runs `ensure_schema` per workspace, so a bare `hq_<ws>` has no `issues`
//! table and the first read/write would fail. The fix routes per-workspace access
//! through [`WorkspacePools::ensured_pool`], which ensures the schema exactly once
//! per slug per process. This test proves it: it creates a bare `hq_<ws>` with no
//! schema, then calls `ensured_pool` and confirms a read succeeds — the schema got
//! created on first access.
//!
//! Skipped unless `GT_DOLT_URL` is set (host CI without a Dolt sidecar still
//! compiles). RUN SERIALLY (`--test-threads=1`): Dolt serializes DDL and the tests
//! create databases on the shared server.
//! `cargo test -p gt-store-dolt --test ensure_schema_per_workspace -- --test-threads=1`
//! with `GT_DOLT_URL` pointed at a SANDBOX Dolt (never the live `hq`).

use mysql_async::prelude::Queryable;

use gt_store_dolt::{dolt_db_name, DoltIssues, IssueFilter, WorkspacePools};

/// Create the bare `hq_<ws>` database with **no** schema — exactly the state the
/// data plane leaves a freshly-provisioned tenant in before any `ensure_schema`.
async fn create_bare_db(base: &str, ws: &str) -> Result<(), Box<dyn std::error::Error>> {
    let db = dolt_db_name(ws);
    let pool = gt_store_dolt::connect(base)?;
    let mut conn = pool.get_conn().await?;
    // Drop first so a rerun starts from a genuinely empty database (no leftover
    // `issues` table from a prior run that would mask a regression).
    conn.query_drop(format!("DROP DATABASE IF EXISTS `{db}`")).await?;
    conn.query_drop(format!("CREATE DATABASE `{db}`")).await?;
    drop(conn);
    pool.disconnect().await?;
    Ok(())
}

#[tokio::test]
async fn ensured_pool_self_heals_a_bare_workspace_db() {
    let Ok(base) = std::env::var("GT_DOLT_URL") else {
        eprintln!("GT_DOLT_URL unset — skipping ensure_schema-per-workspace gate");
        return;
    };
    let base = base.trim_end_matches('/').to_string();
    // A slug unique to this gate so it never collides with the contract test's DB.
    let ws = "f2heal";

    // Precondition: a bare `hq_<ws>` with no `issues` table.
    create_bare_db(&base, ws).await.expect("create bare hq_<ws>");

    let pools = WorkspacePools::from_url(&format!("{base}/")).expect("pools");

    // First access self-heals: `ensured_pool` runs `ensure_schema` on the empty DB.
    let pool = pools.ensured_pool(ws).await.expect("ensured_pool self-heals schema");

    // The schema now exists — a read against the tenant store succeeds where it
    // would have errored ("table 'issues' doesn't exist") on the bare DB.
    let repo = DoltIssues::new(pool);
    let rows = repo.list(&IssueFilter::default()).await.expect("read after self-heal");
    assert!(rows.is_empty(), "a freshly-seeded tracker has no rows");

    // Confirm the table is really there (not just an empty result swallowed).
    let server = gt_store_dolt::connect(&base).expect("server conn");
    let mut conn = server.get_conn().await.expect("conn");
    let present: Option<i64> = conn
        .query_first(format!(
            "SELECT 1 FROM information_schema.tables
             WHERE table_schema = '{}' AND table_name = 'issues' LIMIT 1",
            dolt_db_name(ws)
        ))
        .await
        .expect("information_schema query");
    assert_eq!(present, Some(1), "ensured_pool created the issues table");
}

#[tokio::test]
async fn ensured_pool_is_idempotent_and_caches() {
    let Ok(base) = std::env::var("GT_DOLT_URL") else {
        eprintln!("GT_DOLT_URL unset — skipping ensure_schema-per-workspace idempotency gate");
        return;
    };
    let base = base.trim_end_matches('/').to_string();
    let ws = "f2idem";

    create_bare_db(&base, ws).await.expect("create bare hq_<ws>");

    let pools = WorkspacePools::from_url(&format!("{base}/")).expect("pools");

    // First call ensures; the second is a cached no-op (steady-state set lookup).
    let p1 = pools.ensured_pool(ws).await.expect("first ensured_pool");
    let p2 = pools.ensured_pool(ws).await.expect("second ensured_pool is a no-op");

    // Both reads succeed; the schema persists across calls.
    for pool in [p1, p2] {
        let repo = DoltIssues::new(pool);
        repo.list(&IssueFilter::default()).await.expect("read on each call");
    }
}
