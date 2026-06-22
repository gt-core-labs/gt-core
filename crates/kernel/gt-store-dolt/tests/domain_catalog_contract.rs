//! Gate for gtcore-55d5fb H1: `DoltDomainCatalog` CRUD + seed against a real
//! Dolt server.
//!
//! Skipped unless `GT_DOLT_URL` is set, so host CI without a Dolt sidecar still
//! compiles. The container/dev runs invoke it via `cargo test -p gt-store-dolt`.
//!
//! The fixture works inside a throwaway `gt_rs_domain_catalog_test` database so
//! the production `hq.domain_catalog` is never touched.
//!
//! RUN SERIALLY — the tests share the one test database's `domain_catalog`
//! table. Invoke with
//! `cargo test -p gt-store-dolt --test domain_catalog_contract -- --test-threads=1`
//! (with `GT_DOLT_URL` pointed at a SANDBOX Dolt, never the live `hq`).

use mysql_async::prelude::Queryable;

use gt_store_dolt::{generic_template, DoltDomainCatalog, DomainEntry};

const TEST_DB: &str = "gt_rs_domain_catalog_test";

/// Build a pool whose connections default to the throwaway test DB, creating the
/// database fresh (dropped first so each run starts clean).
async fn fresh_pool(base: &str) -> Result<mysql_async::Pool, Box<dyn std::error::Error>> {
    let server = gt_store_dolt::connect(base)?;
    let mut conn = server.get_conn().await?;
    conn.query_drop(format!("DROP DATABASE IF EXISTS {TEST_DB}")).await?;
    conn.query_drop(format!("CREATE DATABASE {TEST_DB}")).await?;
    drop(conn);
    // Re-derive a base URL bound to the test DB (strip any existing db path).
    let db_url = match base.split_once("://") {
        Some((scheme, rest)) => {
            let authority = rest.split('/').next().unwrap_or(rest);
            format!("{scheme}://{authority}/{TEST_DB}")
        }
        None => base.clone(),
    };
    Ok(gt_store_dolt::connect(&db_url)?)
}

#[tokio::test]
async fn ensure_schema_is_idempotent_and_seed_upserts_by_key() {
    let Ok(base) = std::env::var("GT_DOLT_URL") else {
        eprintln!("GT_DOLT_URL unset — skipping DoltDomainCatalog contract");
        return;
    };
    let pool = fresh_pool(&base).await.expect("fresh test db");
    let catalog = DoltDomainCatalog::new(pool);

    // First ensure creates the table; a second ensure is a no-op (no error).
    catalog.ensure_schema().await.expect("ensure_schema #1");
    catalog.ensure_schema().await.expect("ensure_schema #2");

    // Empty table ⇒ not seeded yet (the H3 backfill probe).
    assert!(!catalog.is_seeded().await.expect("is_seeded empty"));

    // Seed the generic template (10 placeholders + reserved meta.gap).
    let tpl = generic_template();
    let written = catalog.seed(&tpl).await.expect("seed");
    assert_eq!(written as usize, tpl.len());
    assert!(catalog.is_seeded().await.expect("is_seeded after"));

    let listed = catalog.list().await.expect("list");
    assert_eq!(listed.len(), tpl.len());

    // Re-seeding does not duplicate (PK on key) and refreshes the label.
    let again = catalog.seed(&tpl).await.expect("re-seed");
    assert_eq!(again as usize, tpl.len());
    assert_eq!(catalog.list().await.expect("list2").len(), tpl.len());
}

#[tokio::test]
async fn upsert_get_and_reserved_delete_guard() {
    let Ok(base) = std::env::var("GT_DOLT_URL") else {
        eprintln!("GT_DOLT_URL unset — skipping DoltDomainCatalog CRUD contract");
        return;
    };
    let pool = fresh_pool(&base).await.expect("fresh test db");
    let catalog = DoltDomainCatalog::new(pool);
    catalog.ensure_schema().await.expect("ensure_schema");
    catalog.seed(&generic_template()).await.expect("seed");

    // Upsert a new business domain, then read it back.
    let sales = DomainEntry::new("sales", "Ventas", None);
    catalog.upsert(&sales).await.expect("upsert sales");
    let got = catalog.get("sales").await.expect("get sales").expect("present");
    assert_eq!(got.label, "Ventas");
    assert!(got.enabled);
    assert!(!got.reserved);

    // Upsert again with a changed label — update-in-place, still one row.
    let before = catalog.list().await.unwrap().len();
    catalog
        .upsert(&DomainEntry::new("sales", "Sales", Some("revenue".into())))
        .await
        .expect("upsert sales again");
    assert_eq!(catalog.list().await.unwrap().len(), before);
    let got = catalog.get("sales").await.unwrap().unwrap();
    assert_eq!(got.label, "Sales");
    assert_eq!(got.tier.as_deref(), Some("revenue"));

    // Delete a non-reserved domain → true; second delete → false (already gone).
    assert!(catalog.delete("sales").await.expect("delete sales"));
    assert!(!catalog.delete("sales").await.expect("delete missing"));
    assert!(catalog.get("sales").await.unwrap().is_none());

    // The reserved meta.gap cannot be deleted.
    let err = catalog.delete("meta.gap").await.expect_err("reserved guard");
    assert!(err.to_string().contains("reserved"), "got: {err}");
    assert!(catalog.get("meta.gap").await.unwrap().is_some(), "meta.gap survives");
}
