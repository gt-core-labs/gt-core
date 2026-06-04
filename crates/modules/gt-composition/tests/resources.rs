//! Issues resources contract (`hq-mcp-test.3`).
//!
//! The two read-only MCP resources the server publishes:
//! - `gt://issues` — a paged, filtered snapshot ([`read_issues_page`] →
//!   `IssuePage { rows, total, next_offset, has_more }`).
//! - `gt://issue/{id}` — one issue with bodies ([`read_issue`] → `Option`, where
//!   `None` is the `not found` the router maps to a 404).
//!
//! Seeds N issues into a per-run throwaway Dolt database, then asserts paging
//! (offset/limit walk + total + has_more), a status filter narrows the count, and
//! a missing id returns `None`.
//!
//! Gated on `GT_DOLT_URL` (no-op without it). Uses its OWN unique database (never
//! the shared `gt_rs_issues_test` the serial `gt-store-dolt` suite reseeds), so it
//! is isolated + rerunnable without `--test-threads=1`.

use std::time::{SystemTime, UNIX_EPOCH};

use gt_issues::resources::{read_issue, read_issues_page};
use gt_store_dolt::{DoltIssues, IssueFilter, NewIssue};
use mysql_async::prelude::Queryable;

/// The `issues` table DDL — the same shape the `gt-store-dolt` contract harness
/// seeds. `ensure_schema` verifies this exists; it does not create it.
const ISSUES_DDL: &str = "CREATE TABLE IF NOT EXISTS issues (
    id                  VARCHAR(255) PRIMARY KEY,
    content_hash        VARCHAR(64),
    title               VARCHAR(500) NOT NULL,
    description         TEXT NOT NULL,
    design              TEXT NOT NULL,
    acceptance_criteria TEXT NOT NULL,
    notes               TEXT NOT NULL,
    status              VARCHAR(32) NOT NULL DEFAULT 'open',
    priority            INT NOT NULL DEFAULT 2,
    issue_type          VARCHAR(32) NOT NULL DEFAULT 'task',
    assignee            VARCHAR(255),
    estimated_minutes   INT,
    created_at          DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
    created_by          VARCHAR(255) DEFAULT '',
    owner               VARCHAR(255) DEFAULT '',
    updated_at          DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
    closed_at           DATETIME,
    closed_by_session   VARCHAR(255) DEFAULT '',
    external_ref        VARCHAR(255),
    spec_id             VARCHAR(1024),
    domain_json         TEXT NOT NULL DEFAULT '[]',
    surface_json        TEXT NOT NULL DEFAULT '[]',
    depends_on_json     TEXT NOT NULL DEFAULT '[]',
    role_scope          VARCHAR(32),
    version             BIGINT NOT NULL DEFAULT 0,
    phase               ENUM('P1','P2','P3','P4') NOT NULL DEFAULT 'P1',
    delivered_sha       CHAR(40)
)";

/// A database name unique to this process + invocation.
fn unique_db() -> String {
    let nanos = SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_nanos();
    format!("gt_rs_resources_test_{}_{nanos}", std::process::id())
}

/// Build a default `NewIssue`, stamping the fields the snapshot pages/filters on.
fn issue(id: &str, priority: u8) -> NewIssue {
    NewIssue {
        id: id.into(),
        title: format!("issue {id}"),
        priority,
        issue_type: "task".into(),
        created_by: "seed".into(),
        external_ref: Some("hq-res".into()),
        ..Default::default()
    }
}

#[tokio::test]
async fn issues_snapshot_pages_filters_and_resolves_by_id() {
    let Ok(base) = std::env::var("GT_DOLT_URL") else {
        eprintln!("GT_DOLT_URL unset — skipping hq-mcp-test.3 resources");
        return;
    };
    let base = base.trim_end_matches('/').to_string();
    let db = unique_db();

    // Create the throwaway database + the issues table (ensure_schema only
    // *verifies* the table is present — the schema DDL is the harness's job, as in
    // the gt-store-dolt contract suite).
    {
        let pool = gt_store_dolt::connect(&base).expect("connect base");
        let mut conn = pool.get_conn().await.expect("base conn");
        conn.query_drop(format!("CREATE DATABASE IF NOT EXISTS {db}")).await.expect("create db");
        conn.query_drop(format!("USE {db}")).await.expect("use db");
        conn.query_drop(ISSUES_DDL).await.expect("create table");
    }
    let repo = DoltIssues::connect(&format!("{base}/{db}")).expect("connect repo");
    repo.ensure_schema().await.expect("schema");

    // Seed 5 issues; priorities span the filter range.
    for n in 0..5u8 {
        repo.insert(&issue(&format!("hq-res.{n}"), n % 3)).await.expect("insert");
    }

    // --- Full count, unpaged ---
    let all = read_issues_page(&repo, &IssueFilter::default()).await.expect("page all");
    assert_eq!(all.total, 5, "all 5 seeded rows counted");
    assert_eq!(all.rows.len(), 5);
    assert!(!all.has_more, "a single page holds the whole set");

    // --- Paging: a 2-row window walks the corpus ---
    let page0 = read_issues_page(
        &repo,
        &IssueFilter { limit: Some(2), offset: Some(0), ..Default::default() },
    )
    .await
    .expect("page 0");
    assert_eq!(page0.rows.len(), 2);
    assert_eq!(page0.total, 5, "total is independent of the page window");
    assert_eq!(page0.next_offset, 2);
    assert!(page0.has_more, "more pages remain after offset 0");

    let page2 = read_issues_page(
        &repo,
        &IssueFilter { limit: Some(2), offset: Some(4), ..Default::default() },
    )
    .await
    .expect("page 2");
    assert_eq!(page2.rows.len(), 1, "the tail page has the 5th row");
    assert!(!page2.has_more, "no page after the last row");

    // --- Filter: priority<=0 narrows the count (n%3==0 → n in {0,3}) ---
    let p0 = read_issues_page(
        &repo,
        &IssueFilter { priority_max: Some(0), ..Default::default() },
    )
    .await
    .expect("page p0");
    assert_eq!(p0.total, 2, "two seeded rows have priority 0");
    assert!(p0.rows.iter().all(|r| r.priority == 0));

    // --- gt://issue/{id}: present resolves, missing is None (404) ---
    let found = read_issue(&repo, "hq-res.1").await.expect("read present");
    assert!(found.is_some(), "a seeded id resolves to a detail");
    assert_eq!(found.unwrap().id, "hq-res.1");

    let missing = read_issue(&repo, "hq-res.does-not-exist").await.expect("read missing");
    assert!(missing.is_none(), "an unknown id is None — the resource router's 404");

    // Cleanup: drop the throwaway database so reruns don't accrete schemas.
    let pool = gt_store_dolt::connect(&base).expect("connect base for drop");
    let mut conn = pool.get_conn().await.expect("drop conn");
    let _ = conn.query_drop(format!("DROP DATABASE IF EXISTS {db}")).await;
}
