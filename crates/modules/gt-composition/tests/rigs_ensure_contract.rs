//! Regression test for the `rigs` template self-heal that un-mitigates the pin to 9f2e350
//! (gtcore-2544cb, second child of the post-incident epic gtcore-74859d; the fix landed in
//! gtcore-a80f74 / commit d81ccb4).
//!
//! THE CRASHLOOP THIS GUARDS AGAINST. The migration tracking table (`public._gt_schema_migrations`)
//! survives a `DROP SCHEMA ws_default CASCADE` (the tenant re-provision / data-wipe path). After
//! such a drop the loader still sees `rig/create_rigs` recorded as applied and SKIPS it, so the
//! dropped `ws_default.rigs` is never recreated — then the next *pending* follow-on migration
//! (`ALTER TABLE ws_default.rigs ADD COLUMN dispatch_mode …`) aborts boot with
//! `relation "ws_default.rigs" does not exist`, crashlooping the whole gt-mcp-server. The
//! mitigation was to pin the server image to a commit before the pending ALTER landed (9f2e350).
//!
//! THE FIX. `apply_pg_catalog` replays [`gt_rig::RigsModule::template_ensure_sql`] UNCONDITIONALLY
//! on boot, BEFORE `gt_module_migrate::apply`, so a missing-or-partial `rigs` is converged to the
//! current schema regardless of what the tracking table claims. This test reproduces the failure
//! against a real Postgres and proves the ensure DDL averts it:
//!   1. ensure on a clean schema creates `rigs` with the full column set;
//!   2. with the table dropped, the bare follow-on ALTER (the "pending" migration) FAILS — the
//!      exact crashloop;
//!   3. re-running ensure self-heals the table, after which the same ALTER SUCCEEDS — no crashloop;
//!   4. a further ensure against the intact schema is an idempotent no-op (additive DDL only).
//!
//! To stay parallel-safe with the other contract tests (which share the real `ws_default`
//! template), the production DDL is rewritten onto a private nonce schema — the statements are all
//! `ws_default`-qualified, so the rewrite isolates the test without changing what is tested. No-op
//! without `GT_PG_URL`. Run: `cargo test -p gt-composition --test rigs_ensure_contract`.

use std::time::{SystemTime, UNIX_EPOCH};

use gt_rig::RigsModule;
use gt_store_pg::assert_ephemeral_pg_url;

fn nonce() -> u128 {
    SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_nanos()
}

/// `true` when `schema.table` exists as a base table.
async fn table_exists(pool: &sqlx::PgPool, schema: &str, table: &str) -> bool {
    sqlx::query_scalar(
        "SELECT EXISTS (SELECT 1 FROM information_schema.tables \
         WHERE table_schema = $1 AND table_name = $2)",
    )
    .bind(schema)
    .bind(table)
    .fetch_one(pool)
    .await
    .expect("probe table existence")
}

/// `true` when `schema.rigs` has a column named `column`.
async fn column_exists(pool: &sqlx::PgPool, schema: &str, column: &str) -> bool {
    sqlx::query_scalar(
        "SELECT EXISTS (SELECT 1 FROM information_schema.columns \
         WHERE table_schema = $1 AND table_name = 'rigs' AND column_name = $2)",
    )
    .bind(schema)
    .bind(column)
    .fetch_one(pool)
    .await
    .expect("probe column existence")
}

/// Every column the ensure DDL converges `rigs` to — the 0001 base columns plus every follow-on
/// `ADD COLUMN` migration. The crashloop is fundamentally a follow-on ALTER hitting a missing
/// table, so a complete restored column set is what proves the self-heal lands the full schema.
const RIGS_COLUMNS: &[&str] = &[
    "name",
    "prefix",
    "git_url",
    "push_url",
    "upstream_url",
    "default_branch",
    "registered_at",
    "worktree_root",
    "git_connection_ref",
    "semantic_tags",
    "dispatch_mode",
];

#[tokio::test]
async fn ensure_self_heals_dropped_rigs_so_a_pending_alter_never_crashloops() {
    let Ok(url) = std::env::var("GT_PG_URL") else {
        eprintln!("GT_PG_URL unset; skipping rigs ensure contract");
        return;
    };
    assert_ephemeral_pg_url(&url);
    let pool = sqlx::PgPool::connect(&url).await.expect("connect pool");

    // Private nonce schema so the DROP below never races the shared `ws_default` template that the
    // other contract tests use. The production DDL is `ws_default`-qualified end to end (the
    // `CREATE SCHEMA`, the `CREATE TABLE`, every `ALTER`), so this single replace re-targets it
    // coherently without changing the statements under test.
    let schema = format!("ws_rigs_test_{}", nonce());
    let ensure_sql = RigsModule::template_ensure_sql().replace("ws_default", &schema);
    // The "pending" follow-on migration the data-wipe leaves un-replayed (rig/0005). Its
    // `IF NOT EXISTS` guards the COLUMN, not the table — so against a missing `rigs` it raises
    // `relation "<schema>.rigs" does not exist`, which is the boot crashloop.
    let pending_alter =
        format!("ALTER TABLE {schema}.rigs ADD COLUMN IF NOT EXISTS dispatch_mode TEXT NOT NULL DEFAULT 'auto'");

    // --- Acceptance #1: clean schema → ensure creates `rigs` with the full column set. ---
    sqlx::raw_sql(&ensure_sql).execute(&pool).await.expect("first ensure creates rigs");
    assert!(table_exists(&pool, &schema, "rigs").await, "ensure must create {schema}.rigs");
    for c in RIGS_COLUMNS {
        assert!(column_exists(&pool, &schema, c).await, "ensure must add the {c} column");
    }

    // Simulate the data-wipe of the rigs table while `public._gt_schema_migrations` would still
    // claim `rig/create_rigs` (and the follow-on ALTERs) are applied.
    sqlx::query(&format!("DROP TABLE {schema}.rigs"))
        .execute(&pool)
        .await
        .expect("drop rigs to simulate the wipe");
    assert!(
        !table_exists(&pool, &schema, "rigs").await,
        "rigs must be gone after the simulated wipe",
    );

    // --- The crashloop, reproduced: the pending follow-on ALTER fails on the missing table. ---
    let crashloop = sqlx::raw_sql(&pending_alter).execute(&pool).await;
    assert!(
        crashloop.is_err(),
        "a pending ALTER on the dropped rigs table must error — this is the boot crashloop",
    );

    // --- Acceptance #1 (self-heal) + #2 (forward-only safe): re-run ensure. ---
    // The dropped table comes back with every column, so the follow-on ALTER that crashlooped now
    // succeeds: the boot self-heal makes the table present before the migration plan runs.
    sqlx::raw_sql(&ensure_sql).execute(&pool).await.expect("second ensure self-heals the drop");
    assert!(table_exists(&pool, &schema, "rigs").await, "ensure must restore {schema}.rigs");
    for c in RIGS_COLUMNS {
        assert!(column_exists(&pool, &schema, c).await, "ensure must restore the {c} column");
    }
    sqlx::raw_sql(&pending_alter)
        .execute(&pool)
        .await
        .expect("the pending ALTER must succeed once the table is self-healed — no crashloop");

    // --- Acceptance #2: a third run against the now-intact schema is a clean idempotent no-op. ---
    sqlx::raw_sql(&ensure_sql).execute(&pool).await.expect("third ensure is an idempotent no-op");
    assert!(table_exists(&pool, &schema, "rigs").await, "{schema}.rigs still present after no-op");

    // Cleanup: drop the private schema (best-effort; an ephemeral test DB is torn down anyway).
    let _ = sqlx::query(&format!("DROP SCHEMA {schema} CASCADE")).execute(&pool).await;
}
