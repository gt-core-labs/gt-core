//! Contract test for the per-workspace projection-table self-heal (gtcore-c9b292).
//!
//! The boot path replays [`projection_template_ensure_sql`] UNCONDITIONALLY so a
//! `DROP SCHEMA ws_default CASCADE` (the data-wipe path) is repaired on the next boot even
//! though the `public._gt_schema_migrations` bookkeeping still records the migrations as
//! applied. This exercises the two acceptance criteria against a real Postgres:
//!   - a missing projection table is (re)created by the ensure DDL (`ws_default.comments`,
//!     `ws_default.documents` + family, `ws_default.memories`);
//!   - running it against an intact schema is an idempotent no-op (additive DDL only).
//!
//! To stay parallel-safe with the other contract tests (which share the real `ws_default`
//! template), the production DDL is rewritten onto a private nonce schema — the statements are
//! all `ws_default`-qualified, so the rewrite isolates the test without changing what is tested.
//! No-op without `GT_PG_URL`. Run:
//! `cargo test -p gt-store-pg --features pg --test projection_ensure_contract`.

#![cfg(feature = "pg")]

use std::time::{SystemTime, UNIX_EPOCH};

use gt_store_pg::{assert_ephemeral_pg_url, projection_template_ensure_sql};

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

/// Every projection table the ensure DDL owns, by bare name.
const PROJECTION_TABLES: &[&str] = &[
    "comments",
    "documents",
    "document_versions",
    "document_shares",
    "doc_chunks",
    "memories",
];

#[tokio::test]
async fn ensure_recreates_dropped_projection_table_and_is_idempotent() {
    let Ok(url) = std::env::var("GT_PG_URL") else {
        eprintln!("GT_PG_URL unset; skipping projection ensure contract");
        return;
    };
    assert_ephemeral_pg_url(&url);
    let pool = sqlx::PgPool::connect(&url).await.expect("connect pool");

    // Private nonce schema so the DROP below never races the shared `ws_default` template that the
    // other contract tests use. The production DDL is `ws_default`-qualified end to end, so this
    // single replace re-targets it (table bodies, FK targets, index ON-clauses) coherently.
    let schema = format!("ws_ensure_test_{}", nonce());
    let ensure_sql = projection_template_ensure_sql().replace("ws_default", &schema);

    // --- Acceptance #1: tables ABSENT → ensure creates them. ---
    sqlx::raw_sql(&ensure_sql).execute(&pool).await.expect("first ensure creates the tables");
    for t in PROJECTION_TABLES {
        assert!(table_exists(&pool, &schema, t).await, "ensure must create {schema}.{t}");
    }

    // Simulate the data-wipe of a single projection table (the comments drop the bead describes),
    // while the bookkeeping would still claim `0001_comments` is applied.
    sqlx::query(&format!("DROP TABLE {schema}.comments"))
        .execute(&pool)
        .await
        .expect("drop comments to simulate the wipe");
    assert!(
        !table_exists(&pool, &schema, "comments").await,
        "comments must be gone after the simulated wipe",
    );

    // --- Acceptance #1 (self-heal) + #2 (present → no-op): re-run ensure. ---
    // The dropped table comes back; the tables that were left intact are untouched (the DDL is
    // `IF NOT EXISTS`, so it never errors on or clobbers them).
    sqlx::raw_sql(&ensure_sql).execute(&pool).await.expect("second ensure self-heals the drop");
    for t in PROJECTION_TABLES {
        assert!(table_exists(&pool, &schema, t).await, "ensure must restore {schema}.{t}");
    }

    // --- Acceptance #2: a third run against the now-intact schema is a clean idempotent no-op. ---
    sqlx::raw_sql(&ensure_sql).execute(&pool).await.expect("third ensure is an idempotent no-op");
    for t in PROJECTION_TABLES {
        assert!(table_exists(&pool, &schema, t).await, "{schema}.{t} still present after no-op");
    }

    // Cleanup: drop the private schema (best-effort; an ephemeral test DB is torn down anyway).
    let _ = sqlx::query(&format!("DROP SCHEMA {schema} CASCADE")).execute(&pool).await;
}
