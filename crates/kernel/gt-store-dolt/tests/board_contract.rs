//! Gate for hq-62130a: the board write surface (`board_move` / `board_set_ranks`
//! / `column_ranks`) and the kanban schema migration, against a real Dolt server.
//!
//! Skipped unless `GT_DOLT_URL` is set, exactly like `issues_contract.rs`. The
//! fixture writes into a throwaway `gt_rs_board_test` database so the production
//! `hq.issues` is never touched.
//!
//! RUN SERIALLY — every test reseeds the same table:
//! `cargo test -p gt-store-dolt --test board_contract -- --test-threads=1`

use mysql_async::params;
use mysql_async::prelude::Queryable;

use gt_store_dolt::{AppError, DoltIssues, IssueFilter, IssueStatus};

const TEST_DB: &str = "gt_rs_board_test";

async fn seed(base: &str) -> Result<(), Box<dyn std::error::Error>> {
    let pool = gt_store_dolt::connect(base)?;
    let mut conn = pool.get_conn().await?;
    conn.query_drop(format!("CREATE DATABASE IF NOT EXISTS {TEST_DB}"))
        .await?;
    conn.query_drop(format!("USE {TEST_DB}")).await?;
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
            delivered_sha       CHAR(40),
            rig                 VARCHAR(255) NOT NULL DEFAULT '',
            workspace           VARCHAR(255) NOT NULL DEFAULT 'default',
            board_rank          VARCHAR(255) NOT NULL DEFAULT '',
            estimated_hours     DECIMAL(8,2),
            start_date          DATE,
            due_date            DATE
        )",
    )
    .await?;
    conn.query_drop("DELETE FROM issues").await?;
    // Two workspaces on one rig: the scope-rejection cases need a foreign card.
    conn.query_drop(
        "INSERT INTO issues (id, title, description, design, acceptance_criteria, notes,
            status, priority, rig, workspace, board_rank) VALUES
            ('hq-a', 'alpha', '', '', '', '', 'open',    0, 'hq', 'wsa', 'b'),
            ('hq-b', 'beta',  '', '', '', '', 'open',    1, 'hq', 'wsa', 'd'),
            ('hq-c', 'gamma', '', '', '', '', 'open',    2, 'hq', 'wsa', ''),
            ('hq-w', 'wip',   '', '', '', '', 'working', 1, 'hq', 'wsa', 'm'),
            ('hq-z', 'done',  '', '', '', '', 'closed',  2, 'hq', 'wsa', ''),
            ('hq-f', 'forn',  '', '', '', '', 'open',    2, 'hq', 'wsb', '')",
    )
    .await?;
    // A reseed that round-trips to the same state as HEAD has nothing to
    // commit — fine, the data is correct either way.
    if let Err(e) = conn
        .query_drop("CALL DOLT_COMMIT('-A', '-m', 'board_contract seed')")
        .await
    {
        if !e.to_string().contains("nothing to commit") {
            return Err(e.into());
        }
    }
    Ok(())
}

fn db_url(base: &str) -> String {
    format!("{base}/{TEST_DB}")
}

async fn commit_count(store: &DoltIssues) -> i64 {
    let mut conn = store.pool().get_conn().await.expect("conn");
    let n: Option<i64> = conn
        .query_first("SELECT COUNT(*) FROM dolt_log")
        .await
        .expect("dolt_log");
    n.unwrap_or(0)
}

async fn row_state(store: &DoltIssues, id: &str) -> (String, String) {
    let detail = store.get_detail(id).await.expect("detail").expect("row");
    (detail.status, detail.board_rank)
}

#[tokio::test]
async fn board_move_sets_status_and_rank_in_one_commit() {
    let Ok(base) = std::env::var("GT_DOLT_URL") else {
        eprintln!("GT_DOLT_URL unset — skipping board_move contract");
        return;
    };
    seed(&base).await.expect("seed");
    let store = DoltIssues::connect(&db_url(&base)).expect("connect");

    let before = commit_count(&store).await;
    store
        .board_move("hq-a", "hq", "wsa", Some(IssueStatus::Working), "p")
        .await
        .expect("move");
    let after = commit_count(&store).await;

    assert_eq!(after, before + 1, "status + rank must land as ONE Dolt commit");
    assert_eq!(row_state(&store, "hq-a").await, ("working".into(), "p".into()));
}

#[tokio::test]
async fn board_move_rejects_cross_workspace_caller() {
    let Ok(base) = std::env::var("GT_DOLT_URL") else {
        eprintln!("GT_DOLT_URL unset — skipping board scope contract");
        return;
    };
    seed(&base).await.expect("seed");
    let store = DoltIssues::connect(&db_url(&base)).expect("connect");

    // hq-f lives in workspace `wsb`; a caller scoped to (hq, wsa) must not move it.
    let err = store
        .board_move("hq-f", "hq", "wsa", Some(IssueStatus::Working), "p")
        .await
        .expect_err("cross-workspace move must fail");
    assert!(
        matches!(&err, AppError::Validation(m) if m.contains("scope")),
        "got {err:?}"
    );
    // And the row is untouched.
    assert_eq!(row_state(&store, "hq-f").await, ("open".into(), "".into()));

    // Same for a wrong rig.
    let err = store
        .board_move("hq-f", "other", "wsb", Some(IssueStatus::Working), "p")
        .await
        .expect_err("cross-rig move must fail");
    assert!(matches!(&err, AppError::Validation(m) if m.contains("scope")), "got {err:?}");
}

#[tokio::test]
async fn board_move_honours_the_transition_state_machine() {
    let Ok(base) = std::env::var("GT_DOLT_URL") else {
        eprintln!("GT_DOLT_URL unset — skipping board transition contract");
        return;
    };
    seed(&base).await.expect("seed");
    let store = DoltIssues::connect(&db_url(&base)).expect("connect");

    // closed -> working is illegal exactly as in issues.transition.
    let err = store
        .board_move("hq-z", "hq", "wsa", Some(IssueStatus::Working), "p")
        .await
        .expect_err("closed -> working must fail");
    assert!(
        matches!(&err, AppError::Validation(m) if m.contains("invalid transition")),
        "got {err:?}"
    );
    assert_eq!(row_state(&store, "hq-z").await.0, "closed");

    // Unknown card disambiguates as NotFound.
    let err = store
        .board_move("hq-nope", "hq", "wsa", Some(IssueStatus::Working), "p")
        .await
        .expect_err("missing card");
    assert!(matches!(err, AppError::NotFound(_)));
}

#[tokio::test]
async fn reorder_changes_only_rank_and_column_ranks_orders() {
    let Ok(base) = std::env::var("GT_DOLT_URL") else {
        eprintln!("GT_DOLT_URL unset — skipping board reorder contract");
        return;
    };
    seed(&base).await.expect("seed");
    let store = DoltIssues::connect(&db_url(&base)).expect("connect");

    // Rank-only write (target None): status stays `open`.
    store
        .board_move("hq-b", "hq", "wsa", None, "a")
        .await
        .expect("reorder");
    assert_eq!(row_state(&store, "hq-b").await, ("open".into(), "a".into()));

    // column_ranks: ranked ascending first, the unranked '' tail last.
    let col = store
        .column_ranks("hq", "wsa", IssueStatus::Open)
        .await
        .expect("column");
    let ids: Vec<&str> = col.iter().map(|(id, _)| id.as_str()).collect();
    assert_eq!(ids, ["hq-b", "hq-a", "hq-c"], "a < b ranks then '' tail");

    // And the workspace filter keeps the foreign card out of list projections.
    let rows = store
        .list(&IssueFilter {
            rig: Some("hq".into()),
            workspace: Some("wsa".into()),
            ..Default::default()
        })
        .await
        .expect("list");
    assert!(rows.iter().all(|r| r.workspace == "wsa"));
    assert!(!rows.iter().any(|r| r.id == "hq-f"));
}

#[tokio::test]
async fn board_set_ranks_rebalances_in_one_commit() {
    let Ok(base) = std::env::var("GT_DOLT_URL") else {
        eprintln!("GT_DOLT_URL unset — skipping board rebalance contract");
        return;
    };
    seed(&base).await.expect("seed");
    let store = DoltIssues::connect(&db_url(&base)).expect("connect");

    let before = commit_count(&store).await;
    store
        .board_set_ranks(
            "hq",
            "wsa",
            &[
                ("hq-a".to_string(), "c1".to_string()),
                ("hq-b".to_string(), "c2".to_string()),
                ("hq-c".to_string(), "c3".to_string()),
            ],
        )
        .await
        .expect("rebalance");
    let after = commit_count(&store).await;
    assert_eq!(after, before + 1, "a rebalance is ONE commit");
    assert_eq!(row_state(&store, "hq-c").await.1, "c3");
}

#[tokio::test]
async fn ensure_schema_adds_board_columns_and_backfills_workspace() {
    let Ok(base) = std::env::var("GT_DOLT_URL") else {
        eprintln!("GT_DOLT_URL unset — skipping board migration contract");
        return;
    };
    // A separate throwaway db that starts WITHOUT the board columns, so the
    // ensure_schema pass actually exercises the ALTERs.
    const MIG_DB: &str = "gt_rs_board_mig_test";
    let pool = gt_store_dolt::connect(&base).expect("connect server");
    let mut conn = pool.get_conn().await.expect("conn");
    conn.query_drop(format!("DROP DATABASE IF EXISTS {MIG_DB}"))
        .await
        .expect("drop");
    conn.query_drop(format!("CREATE DATABASE {MIG_DB}")).await.expect("create");

    let store = DoltIssues::connect(&format!("{base}/{MIG_DB}")).expect("connect db");
    store.ensure_schema().await.expect("ensure_schema (fresh)");
    // Idempotent second run.
    store.ensure_schema().await.expect("ensure_schema (rerun)");

    let mut conn = store.pool().get_conn().await.expect("conn");
    for col in ["workspace", "board_rank", "estimated_hours", "start_date", "due_date"] {
        let present: Option<i64> = conn
            .exec_first(
                "SELECT 1 FROM information_schema.columns
                 WHERE table_schema = DATABASE() AND table_name = 'issues'
                   AND column_name = :col LIMIT 1",
                params! { "col" => col },
            )
            .await
            .expect("information_schema");
        assert!(present.is_some(), "column {col} missing after ensure_schema");
    }
    // The column default backfills every pre-existing row to 'default'.
    conn.query_drop(
        "INSERT INTO issues (id, title, description, design, acceptance_criteria, notes)
         VALUES ('hq-mig', 'm', '', '', '', '')",
    )
    .await
    .expect("insert");
    let ws: Option<String> = conn
        .query_first("SELECT workspace FROM issues WHERE id = 'hq-mig'")
        .await
        .expect("select");
    assert_eq!(ws.as_deref(), Some("default"));
}
