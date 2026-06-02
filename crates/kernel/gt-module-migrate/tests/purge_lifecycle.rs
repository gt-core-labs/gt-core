//! PG-backed lifecycle test for the multi-source migration loader
//! (`hq-mod-migrate.5`).
//!
//! Drives the full disable-safe story against a live Postgres:
//!
//! 1. **apply** a two-module stack — both modules' tables appear.
//! 2. **disable** one module (drop it from the plan) and re-apply — its table
//!    survives and its migration is reported as [`retained`], not re-run.
//! 3. **re-enable** it — the re-apply is a no-op (already recorded).
//! 4. **purge** the (re-disabled) module — its table is dropped and its tracking
//!    history forgotten; the live module's table is untouched.
//! 5. **guard** — purging a module still in the active plan is refused.
//!
//! Gated on `GT_PG_URL`: with no database configured the test prints a skip
//! notice and passes, so the default `cargo test` on a box without Postgres stays
//! green (same convention as the `gt-store-pg` contract tests).

use gt_module::{Migration, ModuleId};
use gt_module_migrate::{apply, applied, is_in_plan, purge, retained, MigrateError};
use sqlx::{PgPool, Row};

/// Distinct, test-owned names so this never collides with real module
/// migrations sharing the database, and stays idempotent across runs.
const MOD_A: &str = "mig5-alpha";
const MOD_B: &str = "mig5-beta";
const TABLE_A: &str = "gt_mig5_alpha";
const TABLE_B: &str = "gt_mig5_beta";

fn module(id: &str) -> ModuleId {
    ModuleId::new(id).expect("valid module id")
}

/// Whether `table` exists in the connected database's search path.
async fn table_exists(pool: &PgPool, table: &str) -> bool {
    let row = sqlx::query("SELECT to_regclass($1) IS NOT NULL AS present")
        .bind(table)
        .fetch_one(pool)
        .await
        .expect("query to_regclass");
    row.try_get::<bool, _>("present").expect("present column")
}

/// Drop both test tables and forget any tracking rows the test owns, so a prior
/// aborted run cannot taint this one.
async fn reset(pool: &PgPool) {
    for table in [TABLE_A, TABLE_B] {
        sqlx::query(&format!("DROP TABLE IF EXISTS {table}"))
            .execute(pool)
            .await
            .expect("drop test table");
    }
    // The tracking table may not exist yet on a pristine database; `applied`
    // creates it, so ensure it exists before deleting our rows.
    applied(pool).await.expect("ensure tracking table");
    sqlx::query(&format!(
        "DELETE FROM {} WHERE module_id = ANY($1)",
        gt_module_migrate::MIGRATIONS_TABLE
    ))
    .bind(vec![MOD_A.to_owned(), MOD_B.to_owned()])
    .execute(pool)
    .await
    .expect("clear test tracking rows");
}

#[tokio::test]
async fn apply_disable_reactivate_purge_lifecycle() {
    let Some(url) = std::env::var("GT_PG_URL").ok() else {
        eprintln!("GT_PG_URL unset; skipping migration purge-lifecycle test");
        return;
    };
    let pool = PgPool::connect(&url).await.expect("connect postgres");

    reset(&pool).await;

    let (mod_a, mod_b) = (module(MOD_A), module(MOD_B));
    let mig_a = Migration::new(1, "create_alpha", format!("CREATE TABLE {TABLE_A} (id INT PRIMARY KEY)"));
    let mig_b = Migration::new(1, "create_beta", format!("CREATE TABLE {TABLE_B} (id INT PRIMARY KEY)"));

    let full = vec![(&mod_a, &mig_a), (&mod_b, &mig_b)];
    let only_a = vec![(&mod_a, &mig_a)];

    // 1. apply the two-module stack: both tables come into being.
    let report = apply(&pool, &full).await.expect("apply full stack");
    assert!(report.changed(), "first apply must apply both migrations");
    assert_eq!(report.applied.len(), 2);
    assert!(table_exists(&pool, TABLE_A).await, "alpha table created");
    assert!(table_exists(&pool, TABLE_B).await, "beta table created");

    // 2. disable beta (drop from plan) and re-apply: alpha unchanged, beta's
    //    table survives, beta's migration is retained (not re-run, not dropped).
    let report = apply(&pool, &only_a).await.expect("apply with beta disabled");
    assert!(!report.changed(), "alpha already applied — disabling beta is a no-op apply");
    assert!(table_exists(&pool, TABLE_B).await, "disabling beta must NOT drop its table");

    let kept = retained(&applied(&pool).await.expect("read applied"), &only_a);
    assert!(
        kept.contains(&(MOD_B.to_owned(), 1)),
        "beta/1 must be reported as retained while disabled, got {kept:?}"
    );

    // 3. re-enable beta: its migration is already recorded, so re-apply is a no-op.
    let report = apply(&pool, &full).await.expect("re-apply with beta enabled");
    assert!(!report.changed(), "re-enabling beta must not re-run its DDL");
    assert!(table_exists(&pool, TABLE_B).await, "beta table intact after reactivation");

    // 4. purge beta (disabled again): its table is dropped, its history forgotten.
    let teardown = vec![format!("DROP TABLE IF EXISTS {TABLE_B}")];
    let report = purge(&pool, &mod_b, &teardown, &only_a).await.expect("purge beta");
    assert_eq!(report.statements, 1);
    assert_eq!(report.forgotten, 1, "beta's single tracking row is forgotten");
    assert!(!table_exists(&pool, TABLE_B).await, "purge drops beta's table");
    assert!(table_exists(&pool, TABLE_A).await, "purge leaves the live module's table");

    let kept = retained(&applied(&pool).await.expect("read applied"), &only_a);
    assert!(!kept.iter().any(|(m, _)| m == MOD_B), "no beta rows remain after purge: {kept:?}");

    // 5. guard: purging a module still in the active plan is refused, no I/O.
    assert!(is_in_plan(&mod_a, &only_a));
    let err = purge(&pool, &mod_a, &[format!("DROP TABLE {TABLE_A}")], &only_a)
        .await
        .expect_err("purging a live module must be refused");
    assert!(matches!(err, MigrateError::LiveModule(m) if m == MOD_A));
    assert!(table_exists(&pool, TABLE_A).await, "refused purge left alpha's table intact");

    reset(&pool).await;
}
