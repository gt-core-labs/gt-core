//! Contract for the `gt://issues` / `gt://issue/{id}` resource reads (`hq-mcp-test.3`).
//!
//! Exercises the module's resource passthroughs against a real Dolt server:
//! the less-style PAGE envelope (`rows`/`total`/`next_offset`/`has_more`) and its
//! `limit`/`offset` walk, plus the single-bead detail and its NotFound (`Ok(None)`)
//! mapping. The `?ready` predicate itself is unit-covered in `readiness.rs`; this
//! suite is the integration proof that the pager metadata is computed correctly
//! over stored rows.
//!
//! Skipped unless `GT_DOLT_URL` is set, so host CI without a Dolt sidecar still
//! compiles. The container/CI runs invoke it via `cargo test -p gt-issues`.
//!
//! Each test uses its **own** throwaway database (`gt_rs_resources_<test>`) so it
//! never touches the live `hq`, never races the `gt-store-dolt` `issues_contract`
//! fixture, and — crucially — never races a sibling test in this file. cargo runs
//! the `#[tokio::test]`s in parallel; a single shared DB let one test's
//! `DELETE`+`INSERT` interleave with another's, producing `duplicate primary key
//! [res-1]` (Dolt 1062). A DB per test removes the shared table entirely
//! (hq-mcp-test.11).

use mysql_async::prelude::Queryable;

use gt_issues::resources::{read_issue, read_issues_page};
use gt_store_dolt::{DoltIssues, IssueFilter};

/// Create a throwaway DB named `db` + its `issues` table and seed four rows with a
/// stable order (all share the default `created_at`, so the `created_at DESC, id
/// ASC` ordering reduces to `id ASC`: `res-1`, `res-2`, `res-3`, `res-4`). Each
/// test passes its own `db` so the parallel seeds never share a table.
async fn seed(base: &str, db: &str) -> Result<(), Box<dyn std::error::Error>> {
    let pool = gt_store_dolt::connect(base)?;
    let mut conn = pool.get_conn().await?;
    conn.query_drop(format!("CREATE DATABASE IF NOT EXISTS {db}")).await?;
    conn.query_drop(format!("USE {db}")).await?;
    conn.query_drop(
        "CREATE TABLE IF NOT EXISTS issues (
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
        )",
    )
    .await?;
    conn.query_drop("DELETE FROM issues").await?;
    conn.query_drop(
        "INSERT INTO issues (id, title, description, design, acceptance_criteria, notes,
            status, priority, issue_type) VALUES
            ('res-1', 'one',   'body one', '', 'ac one', '', 'open',    0, 'task'),
            ('res-2', 'two',   '',         '', '',       '', 'working', 1, 'task'),
            ('res-3', 'three', '',         '', '',       '', 'open',    2, 'task'),
            ('res-4', 'four',  '',         '', '',       '', 'closed',  2, 'task')",
    )
    .await?;
    Ok(())
}

/// Connect a [`DoltIssues`] against the seeded throwaway DB `db`.
async fn repo(base: &str, db: &str) -> DoltIssues {
    let repo = DoltIssues::connect(&format!("{base}/{db}")).expect("connect");
    repo.ensure_schema().await.expect("schema present");
    repo
}

/// The page envelope reports the full `total` and a `has_more`/`next_offset` walk
/// that terminates exactly once every row is paged, never mistaking a page for the
/// whole corpus.
#[tokio::test]
async fn issues_page_envelope_walks_the_corpus() {
    let Ok(base) = std::env::var("GT_DOLT_URL") else {
        eprintln!("GT_DOLT_URL unset — skipping resources contract");
        return;
    };
    let base = base.trim_end_matches('/').to_string();
    let db = "gt_rs_resources_envelope";
    seed(&base, db).await.expect("seed");
    let repo = repo(&base, db).await;

    // Page 1: limit=2, offset=0 — two rows, four total, more to come.
    let p1 = read_issues_page(
        &repo,
        &IssueFilter { limit: Some(2), offset: Some(0), ..Default::default() },
    )
    .await
    .expect("page 1");
    assert_eq!(p1.rows.len(), 2, "limit caps the page");
    assert_eq!(p1.total, 4, "total is the full filter count, not the page size");
    assert_eq!(p1.next_offset, 2, "next_offset = offset + rows.len()");
    assert!(p1.has_more, "next_offset(2) < total(4)");

    // Page 2: advance to next_offset — the final two rows, no more pages.
    let p2 = read_issues_page(
        &repo,
        &IssueFilter { limit: Some(2), offset: Some(p1.next_offset), ..Default::default() },
    )
    .await
    .expect("page 2");
    assert_eq!(p2.rows.len(), 2);
    assert_eq!(p2.next_offset, 4);
    assert!(!p2.has_more, "the corpus is exhausted: next_offset(4) == total(4)");

    // The two pages partition the corpus with no overlap.
    let mut seen: Vec<&str> = p1.rows.iter().chain(&p2.rows).map(|r| r.id.as_str()).collect();
    seen.sort_unstable();
    assert_eq!(seen, ["res-1", "res-2", "res-3", "res-4"], "pages partition the corpus");
}

/// A `status` filter narrows `total` to the matching rows — the envelope counts the
/// filtered set, not the whole table.
#[tokio::test]
async fn issues_page_total_respects_the_filter() {
    let Ok(base) = std::env::var("GT_DOLT_URL") else {
        eprintln!("GT_DOLT_URL unset — skipping resources contract");
        return;
    };
    let base = base.trim_end_matches('/').to_string();
    let db = "gt_rs_resources_filter";
    seed(&base, db).await.expect("seed");
    let repo = repo(&base, db).await;

    let open = read_issues_page(
        &repo,
        &IssueFilter { status: vec!["open".into()], ..Default::default() },
    )
    .await
    .expect("open page");
    assert_eq!(open.total, 2, "two rows are open (res-1, res-3)");
    assert!(!open.has_more);
    assert!(open.rows.iter().all(|r| r.status == "open"));
}

/// `gt://issue/{id}` returns the bead WITH its heavy bodies for a present id, and
/// `Ok(None)` for a missing one (the resource router maps that to MCP NotFound).
#[tokio::test]
async fn issue_detail_present_and_not_found() {
    let Ok(base) = std::env::var("GT_DOLT_URL") else {
        eprintln!("GT_DOLT_URL unset — skipping resources contract");
        return;
    };
    let base = base.trim_end_matches('/').to_string();
    let db = "gt_rs_resources_detail";
    seed(&base, db).await.expect("seed");
    let repo = repo(&base, db).await;

    let detail = read_issue(&repo, "res-1").await.expect("read").expect("res-1 exists");
    assert_eq!(detail.id, "res-1");
    assert_eq!(detail.title, "one");
    assert_eq!(detail.description, "body one", "detail carries the heavy body");
    assert_eq!(detail.acceptance_criteria, "ac one");

    let missing = read_issue(&repo, "res-nope").await.expect("read missing");
    assert!(missing.is_none(), "an absent id is Ok(None) → MCP NotFound");
}
