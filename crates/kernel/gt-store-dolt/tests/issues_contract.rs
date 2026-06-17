//! Gate for hq-mcp-issues.1: `DoltIssues::list` against a real Dolt server.
//!
//! Skipped unless `GT_DOLT_URL` is set so host CI without a Dolt sidecar still
//! compiles. The container/dev runs invoke it via `cargo test -p gt-store-dolt`.
//!
//! The fixture writes into a throwaway `gt_rs_issues_test` database so the
//! production `hq.issues` is never touched.
//!
//! RUN SERIALLY — every test shares the one `gt_rs_issues_test.issues` table and
//! reseeds it, so parallel execution races on row counts. Invoke with
//! `cargo test -p gt-store-dolt --test issues_contract -- --test-threads=1`
//! (with `GT_DOLT_URL` pointed at a SANDBOX Dolt, e.g.
//! `mysql://gastown@<dolt-host>:3307`, never the live `hq`).

use std::collections::BTreeSet;

use mysql_async::prelude::Queryable;

use gt_store_dolt::{
    ArchivedIssue, ClaimOutcome, DoltIssues, IssueFilter, IssuePatch, IssuePhase, IssueStatus,
    NewIssue,
};

const TEST_DB: &str = "gt_rs_issues_test";

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
            spec_id             VARCHAR(1024),
            domain_json         TEXT NOT NULL DEFAULT '[]',
            surface_json        TEXT NOT NULL DEFAULT '[]',
            role_scope          VARCHAR(32),
            version             BIGINT NOT NULL DEFAULT 0,
            phase               ENUM('P1','P2','P3','P4') NOT NULL DEFAULT 'P1',
            delivered_sha       CHAR(40),
            rig                 VARCHAR(255) NOT NULL DEFAULT '',
            workspace           VARCHAR(255) NOT NULL DEFAULT 'default',
            board_rank          VARCHAR(255) NOT NULL DEFAULT '',
            estimated_hours     DECIMAL(8,2),
            start_date          DATE,
            due_date            DATE,
            dispatch            VARCHAR(16)
        )",
    )
    .await?;
    // Parentage lives in issue_relations (child_of) since the external_ref refactor.
    conn.query_drop(
        "CREATE TABLE IF NOT EXISTS issue_relations (
            from_id  VARCHAR(255) NOT NULL,
            to_id    VARCHAR(255) NOT NULL,
            rel_type VARCHAR(32)  NOT NULL,
            PRIMARY KEY (from_id, to_id, rel_type)
        )",
    )
    .await?;
    conn.query_drop("DELETE FROM issues").await?;
    conn.query_drop("DELETE FROM issue_relations").await?;
    conn.query_drop(
        "INSERT INTO issues (id, title, description, design, acceptance_criteria, notes,
            status, priority, issue_type, assignee) VALUES
            ('hq-a',  'alpha',  '', '', '', '', 'open',    0, 'epic',  'alice'),
            ('hq-b',  'beta',   '', '', '', '', 'working', 1, 'task',  'bob'),
            ('hq-c',  'gamma',  '', '', '', '', 'closed',  2, 'task',  NULL),
            ('hq-d',  'delta',  '', '', '', '', 'open',    2, 'spike', 'alice')",
    )
    .await?;
    conn.query_drop(
        "INSERT INTO issue_relations (from_id, to_id, rel_type) VALUES
            ('hq-a', 'hq-root', 'child_of'),
            ('hq-c', 'hq-root', 'child_of')",
    )
    .await?;
    Ok(())
}

#[tokio::test]
async fn list_filters_combine() {
    let Ok(base) = std::env::var("GT_DOLT_URL") else {
        eprintln!("GT_DOLT_URL unset — skipping DoltIssues contract");
        return;
    };
    let base = base.trim_end_matches('/').to_string();
    seed(&base).await.expect("seed");

    let repo = DoltIssues::connect(&format!("{base}/{TEST_DB}")).expect("connect");
    repo.ensure_schema().await.expect("schema present");

    let all = repo.list(&IssueFilter::default()).await.expect("list all");
    assert_eq!(all.len(), 4, "all 4 rows visible by default");

    let open_only = repo
        .list(&IssueFilter {
            status: vec!["open".into()],
            ..Default::default()
        })
        .await
        .expect("list open");
    let ids: Vec<&str> = open_only.iter().map(|r| r.id.as_str()).collect();
    assert!(ids.contains(&"hq-a") && ids.contains(&"hq-d"));
    assert_eq!(open_only.len(), 2);

    let p_le_1 = repo
        .list(&IssueFilter {
            priority_max: Some(1),
            ..Default::default()
        })
        .await
        .expect("list priority<=1");
    let ids: Vec<&str> = p_le_1.iter().map(|r| r.id.as_str()).collect();
    assert_eq!(ids.len(), 2, "hq-a (p0) + hq-b (p1)");
    assert!(ids.contains(&"hq-a"));
    assert!(ids.contains(&"hq-b"));

    let alice = repo
        .list(&IssueFilter {
            assignee: Some("alice".into()),
            ..Default::default()
        })
        .await
        .expect("list alice");
    assert_eq!(alice.len(), 2);

    let by_parent = repo
        .list(&IssueFilter {
            parent_id: Some("hq-root".into()),
            ..Default::default()
        })
        .await
        .expect("list parent_id");
    assert_eq!(by_parent.len(), 2);

    let epics = repo
        .list(&IssueFilter {
            issue_type: Some("epic".into()),
            ..Default::default()
        })
        .await
        .expect("list epics");
    assert_eq!(epics.len(), 1);
    assert_eq!(epics[0].id, "hq-a");

    let capped = repo
        .list(&IssueFilter {
            limit: Some(2),
            ..Default::default()
        })
        .await
        .expect("list limit");
    assert_eq!(capped.len(), 2);

    let combined = repo
        .list(&IssueFilter {
            status: vec!["open".into()],
            priority_max: Some(2),
            assignee: Some("alice".into()),
            ..Default::default()
        })
        .await
        .expect("list combined");
    assert_eq!(combined.len(), 2);
}

#[tokio::test]
async fn insert_commits_atomic() {
    let Ok(base) = std::env::var("GT_DOLT_URL") else {
        eprintln!("GT_DOLT_URL unset — skipping DoltIssues.insert contract");
        return;
    };
    let base = base.trim_end_matches('/').to_string();
    seed(&base).await.expect("seed");

    let repo = DoltIssues::connect(&format!("{base}/{TEST_DB}")).expect("connect");

    let id = format!("hq-insert-{}", ulid::Ulid::new());
    let row = NewIssue {
        id: id.clone(),
        title: "insert gate".into(),
        description: "desc".into(),
        design: "design".into(),
        acceptance_criteria: "ac".into(),
        notes: "notes".into(),
        priority: 1,
        issue_type: "task".into(),
        created_by: "test".into(),
        parent_id: Some("hq-root".into()),
        assignee: Some("alice".into()),
        owner: Some("alice".into()),
        ..Default::default()
    };
    repo.insert(&row).await.expect("insert");

    // Visible to a follow-up list (proves the row + its child_of relation landed).
    let by_parent = repo
        .list(&IssueFilter {
            parent_id: Some("hq-root".into()),
            ..Default::default()
        })
        .await
        .expect("list");
    let ids: Vec<&str> = by_parent.iter().map(|r| r.id.as_str()).collect();
    assert!(ids.contains(&id.as_str()), "{id} should be visible: ids={ids:?}");

    // Duplicate insert errors with the underlying primary-key violation.
    let err = repo.insert(&row).await.expect_err("dup must reject");
    assert!(
        err.to_string().to_lowercase().contains("duplicate")
            || err.to_string().to_lowercase().contains("primary"),
        "expected duplicate-key error, got `{err}`",
    );
}

#[tokio::test]
async fn update_patches_visible_fields_and_commits() {
    let Ok(base) = std::env::var("GT_DOLT_URL") else {
        eprintln!("GT_DOLT_URL unset — skipping DoltIssues.update contract");
        return;
    };
    let base = base.trim_end_matches('/').to_string();
    seed(&base).await.expect("seed");

    let repo = DoltIssues::connect(&format!("{base}/{TEST_DB}")).expect("connect");

    let id = format!("hq-update-{}", ulid::Ulid::new());
    let row = NewIssue {
        id: id.clone(),
        title: "before".into(),
        description: "before-desc".into(),
        design: String::new(),
        acceptance_criteria: String::new(),
        notes: String::new(),
        priority: 2,
        issue_type: "task".into(),
        created_by: "test".into(),
        parent_id: None,
        assignee: None,
        owner: None,
        ..Default::default()
    };
    repo.insert(&row).await.expect("seed insert");

    let patch = IssuePatch {
        title: Some("after".into()),
        priority: Some(0),
        assignee: Some("alice".into()),
        notes: Some("patched".into()),
        domain_json: Some("[\"orch.merge\"]".into()),
        surface_json: Some("[\"crates/domain/platform/gt-web-context\"]".into()),
        ..Default::default()
    };
    repo.update(&id, &patch).await.expect("update");

    let rows = repo
        .list(&IssueFilter {
            issue_type: Some("task".into()),
            ..Default::default()
        })
        .await
        .expect("list after update");
    let row_after = rows
        .iter()
        .find(|r| r.id == id)
        .expect("row should still be present");
    assert_eq!(row_after.title, "after");
    assert_eq!(row_after.priority, 0);
    assert_eq!(row_after.assignee.as_deref(), Some("alice"));
    // JSON-array columns are patchable through issues.update now.
    assert_eq!(row_after.domain_json, "[\"orch.merge\"]");
    assert_eq!(row_after.surface_json, "[\"crates/domain/platform/gt-web-context\"]");

    // Empty-string overwrite clears the nullable columns back to NULL/None.
    let clear = IssuePatch {
        assignee: Some(String::new()),
        ..Default::default()
    };
    repo.update(&id, &clear).await.expect("clear assignee");
    let rows = repo
        .list(&IssueFilter {
            issue_type: Some("task".into()),
            ..Default::default()
        })
        .await
        .expect("list after clear");
    let cleared = rows.iter().find(|r| r.id == id).expect("row present");
    assert_eq!(cleared.assignee, None, "empty-string clear must store NULL");

    // get_detail returns the heavy text bodies the snapshot omits.
    let detail = repo
        .get_detail(&id)
        .await
        .expect("get_detail ok")
        .expect("row present");
    assert_eq!(detail.id, id);
    assert_eq!(detail.title, "after");
    assert_eq!(detail.description, "before-desc");
    assert_eq!(detail.notes, "patched");
    assert_eq!(detail.surface_json, "[\"crates/domain/platform/gt-web-context\"]");
    assert!(
        repo.get_detail("hq-missing-zzz").await.expect("ok").is_none(),
        "missing id must be None",
    );

    // Unknown id surfaces a NotFound — the row never existed.
    let err = repo
        .update("hq-missing-zzz", &patch)
        .await
        .expect_err("missing id must error");
    assert!(
        err.to_string().to_lowercase().contains("not found")
            || err.to_string().to_lowercase().contains("issue hq-missing"),
        "expected NotFound, got `{err}`",
    );
}

#[tokio::test]
async fn delivered_sha_round_trips() {
    // hq-core-mcp.10 (docs/10 §S2): set_delivered_sha stamps the column and it
    // surfaces in both the list snapshot and get_detail; absent a stamp it is None.
    let Ok(base) = std::env::var("GT_DOLT_URL") else {
        eprintln!("GT_DOLT_URL unset — skipping DoltIssues.set_delivered_sha contract");
        return;
    };
    let base = base.trim_end_matches('/').to_string();
    seed(&base).await.expect("seed");

    let repo = DoltIssues::connect(&format!("{base}/{TEST_DB}")).expect("connect");
    repo.ensure_schema().await.expect("schema present");

    let id = format!("hq-del-{}", ulid::Ulid::new());
    repo.insert(&NewIssue {
        id: id.clone(),
        title: "deliverable".into(),
        issue_type: "task".into(),
        created_by: "test".into(),
        ..Default::default()
    })
    .await
    .expect("seed insert");

    // No stamp yet → NULL on both read paths.
    let detail = repo.get_detail(&id).await.expect("ok").expect("present");
    assert_eq!(detail.delivered_sha, None, "fresh row has no delivered_sha");

    let sha = "0123456789abcdef0123456789abcdef01234567"; // 40 hex
    repo.set_delivered_sha(&id, sha).await.expect("stamp");

    let detail = repo.get_detail(&id).await.expect("ok").expect("present");
    assert_eq!(detail.delivered_sha.as_deref(), Some(sha));
    let rows = repo
        .list(&IssueFilter { issue_type: Some("task".into()), ..Default::default() })
        .await
        .expect("list");
    let row = rows.iter().find(|r| r.id == id).expect("row present");
    assert_eq!(row.delivered_sha.as_deref(), Some(sha), "delivered_sha in snapshot");

    // Missing id surfaces NotFound.
    assert!(repo.set_delivered_sha("hq-missing-zzz", sha).await.is_err());
}

#[tokio::test]
async fn transition_state_machine_round_trip() {
    let Ok(base) = std::env::var("GT_DOLT_URL") else {
        eprintln!("GT_DOLT_URL unset — skipping DoltIssues.transition contract");
        return;
    };
    let base = base.trim_end_matches('/').to_string();
    seed(&base).await.expect("seed");

    let repo = DoltIssues::connect(&format!("{base}/{TEST_DB}")).expect("connect");

    let id = format!("hq-tr-{}", ulid::Ulid::new());
    let row = NewIssue {
        id: id.clone(),
        title: "transition gate".into(),
        description: String::new(),
        design: String::new(),
        acceptance_criteria: String::new(),
        notes: String::new(),
        priority: 2,
        issue_type: "task".into(),
        created_by: "test".into(),
        assignee: None,
        owner: None,
        ..Default::default()
    };
    repo.insert(&row).await.expect("seed insert");

    // open -> working: legal
    repo.transition(&id, IssueStatus::Working).await.expect("open->working");
    assert_eq!(repo.current_status(&id).await.unwrap().as_deref(), Some("working"));

    // working -> closed: legal, stamps closed_at
    repo.transition(&id, IssueStatus::Closed).await.expect("working->closed");
    assert_eq!(repo.current_status(&id).await.unwrap().as_deref(), Some("closed"));

    // closed -> working: illegal per state machine
    let err = repo
        .transition(&id, IssueStatus::Working)
        .await
        .expect_err("closed->working must reject");
    assert!(
        err.to_string().contains("invalid transition"),
        "got `{err}`",
    );

    // closed -> open: legal (re-open)
    repo.transition(&id, IssueStatus::Open).await.expect("closed->open");
    assert_eq!(repo.current_status(&id).await.unwrap().as_deref(), Some("open"));

    // Missing id surfaces NotFound rather than InvalidTransition.
    let err = repo
        .transition("hq-no-such", IssueStatus::Working)
        .await
        .expect_err("missing id must error");
    assert!(
        err.to_string().to_lowercase().contains("not found"),
        "got `{err}`",
    );
}

#[tokio::test]
async fn close_stamps_attribution_and_rejects_double() {
    let Ok(base) = std::env::var("GT_DOLT_URL") else {
        eprintln!("GT_DOLT_URL unset — skipping DoltIssues.close contract");
        return;
    };
    let base = base.trim_end_matches('/').to_string();
    seed(&base).await.expect("seed");

    use mysql_async::prelude::*;
    let repo = DoltIssues::connect(&format!("{base}/{TEST_DB}")).expect("connect");

    let id = format!("hq-close-{}", ulid::Ulid::new());
    let row = NewIssue {
        id: id.clone(),
        title: "close gate".into(),
        description: String::new(),
        design: String::new(),
        acceptance_criteria: String::new(),
        notes: String::new(),
        priority: 2,
        issue_type: "task".into(),
        created_by: "test".into(),
        assignee: None,
        owner: None,
        ..Default::default()
    };
    repo.insert(&row).await.expect("seed insert");

    repo.close(&id, "claude-host").await.expect("close ok");
    assert_eq!(
        repo.current_status(&id).await.unwrap().as_deref(),
        Some("closed"),
    );

    // Attribution column carries the session id.
    let pool = gt_store_dolt::connect(&format!("{base}/{TEST_DB}")).expect("pool");
    let mut conn = pool.get_conn().await.expect("conn");
    let session: Option<String> = conn
        .exec_first(
            "SELECT closed_by_session FROM issues WHERE id = :id",
            mysql_async::params! { "id" => &id },
        )
        .await
        .expect("read attr");
    assert_eq!(session.as_deref(), Some("claude-host"));

    // Double-close rejects (already closed -> InvalidTransition).
    let err = repo.close(&id, "x").await.expect_err("double close must reject");
    assert!(
        err.to_string().contains("invalid transition"),
        "got `{err}`",
    );

    // Missing id surfaces NotFound.
    let err = repo
        .close("hq-no-such", "x")
        .await
        .expect_err("missing id must error");
    assert!(
        err.to_string().to_lowercase().contains("not found"),
        "got `{err}`",
    );
}

/// The DB's own `DATE(NOW())` as `YYYY-MM-DD` — the same value the store stamps
/// via `COALESCE(.., DATE(NOW()))`. Read it from Dolt (not the test host clock)
/// so the comparison never races a midnight rollover or a host/server tz skew.
async fn dolt_today(base: &str) -> String {
    use mysql_async::prelude::*;
    let pool = gt_store_dolt::connect(&format!("{base}/{TEST_DB}")).expect("pool");
    let mut conn = pool.get_conn().await.expect("conn");
    conn.exec_first::<String, _, _>("SELECT DATE_FORMAT(DATE(NOW()), '%Y-%m-%d')", ())
        .await
        .expect("query today")
        .expect("one row")
}

#[tokio::test]
async fn transition_to_working_autofills_start_date_unless_planned() {
    // gtcore-fdc175: the move to `working` derives "Fecha Inicio" from the real
    // lifecycle — an UNSET start_date is filled with the event date, an
    // already-populated (planned) start_date is RESPECTED, never overwritten.
    let Ok(base) = std::env::var("GT_DOLT_URL") else {
        eprintln!("GT_DOLT_URL unset — skipping start_date auto-fill contract");
        return;
    };
    let base = base.trim_end_matches('/').to_string();
    seed(&base).await.expect("seed");

    let repo = DoltIssues::connect(&format!("{base}/{TEST_DB}")).expect("connect");

    // --- empty start_date auto-fills with the event date -------------------
    let empty = format!("hq-sd-empty-{}", ulid::Ulid::new());
    repo.insert(&NewIssue {
        id: empty.clone(),
        title: "start auto-fill".into(),
        issue_type: "task".into(),
        created_by: "test".into(),
        ..Default::default()
    })
    .await
    .expect("seed insert");
    assert_eq!(
        repo.get_detail(&empty).await.unwrap().unwrap().start_date,
        None,
        "fresh row has no start_date",
    );

    repo.transition(&empty, IssueStatus::Working).await.expect("open->working");
    let today = dolt_today(&base).await;
    assert_eq!(
        repo.get_detail(&empty).await.unwrap().unwrap().start_date.as_deref(),
        Some(today.as_str()),
        "unset start_date filled with the event date on the working move",
    );

    // --- a populated start_date is respected -------------------------------
    let planned = format!("hq-sd-plan-{}", ulid::Ulid::new());
    repo.insert(&NewIssue {
        id: planned.clone(),
        title: "start respected".into(),
        issue_type: "task".into(),
        created_by: "test".into(),
        ..Default::default()
    })
    .await
    .expect("seed insert");
    repo.update(
        &planned,
        &IssuePatch { start_date: Some("2020-01-02".into()), ..Default::default() },
    )
    .await
    .expect("seed planned start_date");

    repo.transition(&planned, IssueStatus::Working).await.expect("open->working");
    assert_eq!(
        repo.get_detail(&planned).await.unwrap().unwrap().start_date.as_deref(),
        Some("2020-01-02"),
        "a hand-planned start_date is never overwritten by the working move",
    );
}

#[tokio::test]
async fn claim_autofills_start_date_unless_planned() {
    // gtcore-fdc175: `claim` is the other path onto `working`, so it stamps
    // "Fecha Inicio" the same way — unset is filled, planned is respected.
    let Ok(base) = std::env::var("GT_DOLT_URL") else {
        eprintln!("GT_DOLT_URL unset — skipping claim start_date auto-fill contract");
        return;
    };
    let base = base.trim_end_matches('/').to_string();
    seed(&base).await.expect("seed");

    let repo = DoltIssues::connect(&format!("{base}/{TEST_DB}")).expect("connect");

    // --- empty start_date auto-fills on claim ------------------------------
    let empty = format!("hq-cl-empty-{}", ulid::Ulid::new());
    repo.insert(&NewIssue {
        id: empty.clone(),
        title: "claim auto-fill".into(),
        issue_type: "task".into(),
        created_by: "test".into(),
        ..Default::default()
    })
    .await
    .expect("seed insert");
    assert!(matches!(
        repo.claim(&empty, "alice").await.expect("claim ok"),
        ClaimOutcome::Won,
    ));
    let today = dolt_today(&base).await;
    assert_eq!(
        repo.get_detail(&empty).await.unwrap().unwrap().start_date.as_deref(),
        Some(today.as_str()),
        "unset start_date filled with the event date on claim",
    );

    // --- a populated start_date is respected on claim ----------------------
    let planned = format!("hq-cl-plan-{}", ulid::Ulid::new());
    repo.insert(&NewIssue {
        id: planned.clone(),
        title: "claim respected".into(),
        issue_type: "task".into(),
        created_by: "test".into(),
        ..Default::default()
    })
    .await
    .expect("seed insert");
    repo.update(
        &planned,
        &IssuePatch { start_date: Some("2019-03-04".into()), ..Default::default() },
    )
    .await
    .expect("seed planned start_date");
    assert!(matches!(
        repo.claim(&planned, "bob").await.expect("claim ok"),
        ClaimOutcome::Won,
    ));
    assert_eq!(
        repo.get_detail(&planned).await.unwrap().unwrap().start_date.as_deref(),
        Some("2019-03-04"),
        "a hand-planned start_date is never overwritten by claim",
    );
}

#[tokio::test]
async fn close_autofills_due_date_unless_planned() {
    // gtcore-fdc175: closing a bead derives "Fecha Fin" from the real close
    // date — an UNSET due_date is filled with the close date, an
    // already-populated (planned) due_date is RESPECTED, never overwritten.
    let Ok(base) = std::env::var("GT_DOLT_URL") else {
        eprintln!("GT_DOLT_URL unset — skipping due_date auto-fill contract");
        return;
    };
    let base = base.trim_end_matches('/').to_string();
    seed(&base).await.expect("seed");

    let repo = DoltIssues::connect(&format!("{base}/{TEST_DB}")).expect("connect");

    // --- empty due_date auto-fills with the close date ---------------------
    let empty = format!("hq-dd-empty-{}", ulid::Ulid::new());
    repo.insert(&NewIssue {
        id: empty.clone(),
        title: "due auto-fill".into(),
        issue_type: "task".into(),
        created_by: "test".into(),
        ..Default::default()
    })
    .await
    .expect("seed insert");
    assert_eq!(
        repo.get_detail(&empty).await.unwrap().unwrap().due_date,
        None,
        "fresh row has no due_date",
    );

    repo.close(&empty, "claude-host").await.expect("close ok");
    let today = dolt_today(&base).await;
    assert_eq!(
        repo.get_detail(&empty).await.unwrap().unwrap().due_date.as_deref(),
        Some(today.as_str()),
        "unset due_date filled with the close date",
    );

    // --- a populated due_date is respected ---------------------------------
    let planned = format!("hq-dd-plan-{}", ulid::Ulid::new());
    repo.insert(&NewIssue {
        id: planned.clone(),
        title: "due respected".into(),
        issue_type: "task".into(),
        created_by: "test".into(),
        ..Default::default()
    })
    .await
    .expect("seed insert");
    repo.update(
        &planned,
        &IssuePatch { due_date: Some("2030-12-31".into()), ..Default::default() },
    )
    .await
    .expect("seed planned due_date");

    repo.close(&planned, "claude-host").await.expect("close ok");
    assert_eq!(
        repo.get_detail(&planned).await.unwrap().unwrap().due_date.as_deref(),
        Some("2030-12-31"),
        "a hand-planned due_date is never overwritten on close",
    );
}

#[tokio::test]
async fn update_optimistic_concurrency_blocks_stale_writes() {
    let Ok(base) = std::env::var("GT_DOLT_URL") else {
        eprintln!("GT_DOLT_URL unset — skipping DoltIssues OCC contract");
        return;
    };
    let base = base.trim_end_matches('/').to_string();
    seed(&base).await.expect("seed");
    let repo = DoltIssues::connect(&format!("{base}/{TEST_DB}")).expect("connect");

    let id = format!("hq-occ-{}", ulid::Ulid::new());
    repo.insert(&NewIssue {
        id: id.clone(),
        title: "occ".into(),
        issue_type: "task".into(),
        created_by: "test".into(),
        ..Default::default()
    })
    .await
    .expect("seed bead");

    // Fresh row starts at version 0.
    let v0 = repo.get_detail(&id).await.expect("d").expect("row").version;
    assert_eq!(v0, 0);

    // Guarded update at the read version succeeds and bumps the token.
    repo.update(
        &id,
        &IssuePatch { title: Some("v1".into()), expected_version: Some(v0), ..Default::default() },
    )
    .await
    .expect("guarded update at current version");
    let v1 = repo.get_detail(&id).await.expect("d").expect("row").version;
    assert_eq!(v1, v0 + 1, "version must advance");

    // A second writer still holding v0 is now stale -> version conflict.
    let err = repo
        .update(
            &id,
            &IssuePatch { title: Some("stale".into()), expected_version: Some(v0), ..Default::default() },
        )
        .await
        .expect_err("stale write must be rejected");
    assert!(err.to_string().contains("version conflict"), "got `{err}`");
    // The losing write did not land.
    assert_eq!(repo.get_detail(&id).await.expect("d").expect("row").title, "v1");

    // Re-reading the current version lets the retry succeed.
    repo.update(
        &id,
        &IssuePatch { title: Some("v2".into()), expected_version: Some(v1), ..Default::default() },
    )
    .await
    .expect("retry at current version");

    // Unguarded update (no expected_version) still works and bumps version.
    let before = repo.get_detail(&id).await.expect("d").expect("row").version;
    repo.update(&id, &IssuePatch { notes: Some("unguarded".into()), ..Default::default() })
        .await
        .expect("unguarded update");
    assert_eq!(repo.get_detail(&id).await.expect("d").expect("row").version, before + 1);
}

#[tokio::test]
async fn claim_cas_is_exclusive_and_attributed() {
    let Ok(base) = std::env::var("GT_DOLT_URL") else {
        eprintln!("GT_DOLT_URL unset — skipping DoltIssues.claim contract");
        return;
    };
    let base = base.trim_end_matches('/').to_string();
    seed(&base).await.expect("seed");
    let repo = DoltIssues::connect(&format!("{base}/{TEST_DB}")).expect("connect");

    let id = format!("hq-claim-{}", ulid::Ulid::new());
    repo.insert(&NewIssue {
        id: id.clone(),
        title: "claimable".into(),
        issue_type: "task".into(),
        created_by: "test".into(),
        ..Default::default()
    })
    .await
    .expect("seed open bead");

    // First claimer wins: status flips to working, owner stamped.
    assert_eq!(repo.claim(&id, "agent-a").await.expect("claim a"), ClaimOutcome::Won);
    let after = repo.get_detail(&id).await.expect("detail").expect("row");
    assert_eq!(after.status, "working");
    assert_eq!(after.owner.as_deref(), Some("agent-a"));
    assert_eq!(after.assignee.as_deref(), Some("agent-a"));

    // Second claimer loses with the holder named — the CAS is exclusive.
    match repo.claim(&id, "agent-b").await.expect("claim b") {
        ClaimOutcome::Lost { status, holder } => {
            assert_eq!(status, "working");
            assert_eq!(holder, "agent-a");
        }
        ClaimOutcome::Won => panic!("agent-b must not win a held bead"),
    }

    // Re-claim by the holder is idempotent success.
    assert_eq!(repo.claim(&id, "agent-a").await.expect("reclaim a"), ClaimOutcome::Won);

    // Missing id surfaces NotFound.
    let err = repo
        .claim("hq-no-claim", "agent-a")
        .await
        .expect_err("missing id must error");
    assert!(err.to_string().to_lowercase().contains("not found"), "got `{err}`");
}

/// hq-core-mcp.7 (docs/10 S1): `ensure_schema` adds the `phase` column + seeds
/// the singleton `phase_frontier` at `P3`; `advance_phase` moves the frontier;
/// `insert`/`update` carry the per-bead phase as a scalar overwrite.
#[tokio::test]
async fn phase_frontier_and_per_bead_phase() {
    let Ok(base) = std::env::var("GT_DOLT_URL") else {
        eprintln!("GT_DOLT_URL unset — skipping DoltIssues phase contract");
        return;
    };
    let base = base.trim_end_matches('/').to_string();
    seed(&base).await.expect("seed");
    let repo = DoltIssues::connect(&format!("{base}/{TEST_DB}")).expect("connect");
    // ensure_schema adds the phase columns + creates/seeds phase_frontier=P3.
    repo.ensure_schema().await.expect("ensure_schema");

    // Frontier seeded at P3, and re-running ensure_schema does not reseed it.
    assert_eq!(repo.open_phase().await.expect("open_phase"), IssuePhase::P3);
    repo.ensure_schema().await.expect("ensure_schema idempotent");
    assert_eq!(repo.open_phase().await.expect("open_phase 2"), IssuePhase::P3);

    // Operator advances the frontier P3 -> P4.
    repo.advance_phase(IssuePhase::P4).await.expect("advance");
    assert_eq!(repo.open_phase().await.expect("open_phase 3"), IssuePhase::P4);

    // Per-bead phase: default P1 on a plain insert.
    let def = format!("hq-ph-{}", ulid::Ulid::new());
    repo.insert(&NewIssue {
        id: def.clone(),
        title: "default phase".into(),
        issue_type: "task".into(),
        created_by: "test".into(),
        ..Default::default()
    })
    .await
    .expect("insert default phase");
    // get_detail surfaces phase (hq-core-mcp.8) — the claim echo reads the same.
    assert_eq!(detail_phase(&repo, &def).await, "P1");

    // Explicit phase on insert is honoured.
    let gated = format!("hq-ph-{}", ulid::Ulid::new());
    repo.insert(&NewIssue {
        id: gated.clone(),
        title: "gated".into(),
        issue_type: "task".into(),
        created_by: "test".into(),
        phase: Some("P4".into()),
        ..Default::default()
    })
    .await
    .expect("insert P4");
    assert_eq!(detail_phase(&repo, &gated).await, "P4");

    // update overwrites the phase scalar (demote-to-P4 path, docs/10 §5).
    repo.update(&def, &IssuePatch { phase: Some("P4".into()), ..Default::default() })
        .await
        .expect("phase overwrite");
    assert_eq!(detail_phase(&repo, &def).await, "P4");
}

/// The `phase` token `get_detail` now surfaces (hq-core-mcp.8) — the same field
/// the `issues.claim.*` echo returns so an agent sees the gate before working.
async fn detail_phase(repo: &DoltIssues, id: &str) -> String {
    repo.get_detail(id).await.expect("get_detail ok").expect("row").phase
}

/// Reseed `TEST_DB.issues` with exactly `n` rows parented (`child_of`) under a
/// fixed `pg-walk` epic and zero-padded ids so `ORDER BY created_at DESC, id ASC`
/// is a deterministic permutation — the page-walk gate (hq-core-mcp.13) needs a
/// stable known corpus to prove no dup/skip.
async fn seed_paged(base: &str, n: usize) -> Result<(), Box<dyn std::error::Error>> {
    let pool = gt_store_dolt::connect(base)?;
    let mut conn = pool.get_conn().await?;
    conn.query_drop(format!("USE {TEST_DB}")).await?;
    conn.query_drop("DELETE FROM issues").await?;
    conn.query_drop("DELETE FROM issue_relations").await?;
    // Batch the inserts so a 200+ row seed stays one round-trip.
    let values: Vec<String> = (0..n)
        .map(|i| format!("('pg-{i:04}', 'row {i}', '', '', '', '', 'open', 2, 'task', NULL)"))
        .collect();
    conn.query_drop(format!(
        "INSERT INTO issues (id, title, description, design, acceptance_criteria, notes,
            status, priority, issue_type, assignee) VALUES {}",
        values.join(", ")
    ))
    .await?;
    let rels: Vec<String> = (0..n)
        .map(|i| format!("('pg-{i:04}', 'pg-walk', 'child_of')"))
        .collect();
    conn.query_drop(format!(
        "INSERT INTO issue_relations (from_id, to_id, rel_type) VALUES {}",
        rels.join(", ")
    ))
    .await?;
    Ok(())
}

/// hq-core-mcp.13: the less-style pager. Walking `offset` from 0 until
/// `has_more=false` must reconstruct the whole corpus exactly once each, in a
/// stable order, with a correct `total`; the env-tunable default/ceiling must be
/// honoured and `?limit` above the ceiling clamped — never the old silent
/// `unwrap_or(200).min(1000)` cap.
#[tokio::test]
async fn pager_walks_corpus_without_dup_or_skip() {
    let Ok(base) = std::env::var("GT_DOLT_URL") else {
        eprintln!("GT_DOLT_URL unset — skipping pager contract");
        return;
    };
    let base = base.trim_end_matches('/').to_string();
    // 230 rows crosses the dead 200 cap so a regression would truncate the walk.
    const N: usize = 230;
    seed_paged(&base, N).await.expect("seed paged");

    let repo = DoltIssues::connect(&format!("{base}/{TEST_DB}")).expect("connect");

    // Clear any env from a prior test so the default-page assertions are stable.
    std::env::remove_var("GT_ISSUES_DEFAULT_LIMIT");
    std::env::remove_var("GT_ISSUES_MAX_LIMIT");

    let filter = |offset: u32| IssueFilter {
        parent_id: Some("pg-walk".into()),
        limit: Some(50),
        offset: Some(offset),
        ..Default::default()
    };

    // ── walk page by page, collecting ids in visitation order.
    let mut seen: Vec<String> = Vec::new();
    let mut offset = 0u32;
    let mut pages = 0;
    loop {
        let page = repo.list_page(&filter(offset)).await.expect("list_page");
        assert_eq!(page.total, N as i64, "total is the full filter count on every page");
        for r in &page.rows {
            seen.push(r.id.clone());
        }
        pages += 1;
        if !page.has_more {
            assert!(page.rows.len() <= 50);
            break;
        }
        assert_eq!(page.rows.len(), 50, "a non-final page is exactly the page size");
        assert_eq!(page.next_offset, offset + 50);
        offset = page.next_offset;
        assert!(pages < 100, "walk must terminate");
    }
    assert_eq!(pages, 5, "230 rows / 50 = 5 pages (50*4 + 30)");

    // ── no dup, no skip: exactly the seeded set, each once.
    assert_eq!(seen.len(), N, "reconstructed row count");
    let unique: BTreeSet<&str> = seen.iter().map(String::as_str).collect();
    assert_eq!(unique.len(), N, "every row seen exactly once (no dup)");
    for i in 0..N {
        assert!(unique.contains(format!("pg-{i:04}").as_str()), "row pg-{i:04} present (no skip)");
    }

    // ── stable order: a second full walk yields the identical sequence.
    let mut seen2: Vec<String> = Vec::new();
    let mut offset = 0u32;
    loop {
        let page = repo.list_page(&filter(offset)).await.expect("list_page 2");
        for r in &page.rows {
            seen2.push(r.id.clone());
        }
        if !page.has_more {
            break;
        }
        offset = page.next_offset;
    }
    assert_eq!(seen, seen2, "page order is stable across walks");

    // ── env override: GT_ISSUES_DEFAULT_LIMIT sets the unset-limit page size.
    std::env::set_var("GT_ISSUES_DEFAULT_LIMIT", "17");
    let dflt = repo
        .list_page(&IssueFilter {
            parent_id: Some("pg-walk".into()),
            ..Default::default()
        })
        .await
        .expect("default page");
    assert_eq!(dflt.rows.len(), 17, "default page size from GT_ISSUES_DEFAULT_LIMIT");
    assert_eq!(dflt.total, N as i64);
    assert!(dflt.has_more, "first modest page is not the whole set");
    std::env::remove_var("GT_ISSUES_DEFAULT_LIMIT");

    // ── ceiling: a limit above GT_ISSUES_MAX_LIMIT is honoured only up to it.
    std::env::set_var("GT_ISSUES_MAX_LIMIT", "40");
    let capped = repo
        .list_page(&IssueFilter {
            parent_id: Some("pg-walk".into()),
            limit: Some(9999),
            ..Default::default()
        })
        .await
        .expect("ceiling page");
    assert_eq!(capped.rows.len(), 40, "limit>ceiling clamped to GT_ISSUES_MAX_LIMIT");
    assert!(capped.has_more);
    std::env::remove_var("GT_ISSUES_MAX_LIMIT");
}

/// hq-gap-workspace-provision-full: `create_workspace_dolt` provisions a tenant's
/// `hq_<slug>` database and `WorkspacePools::pool_for(slug)` + `ensure_schema`
/// seeds its `issues` table — the Dolt half of making a fresh workspace usable.
///
/// Proves the gastown-grant fix: the live Dolt user is `gtapp` and no `gastown`
/// user exists, so the old hardcoded `GRANT ... TO 'gastown'@'%'` would ERROR and
/// abort provisioning. With the grant dropped, provisioning relies on the server
/// user's own privilege and succeeds. Idempotent: a second call is a no-op.
#[tokio::test]
async fn create_workspace_dolt_provisions_hq_db_and_issues_schema() {
    let Ok(base) = std::env::var("GT_DOLT_URL") else {
        eprintln!("GT_DOLT_URL unset — skipping create_workspace_dolt contract");
        return;
    };
    let base = base.trim_end_matches('/').to_string();
    // A throwaway tenant slug; `hq_f3-prov` must not collide with the live `hq`.
    let slug = "f3-prov";

    // Server-scoped pool (no DB) for CREATE DATABASE — what the handler builds via
    // WorkspacePools::server_pool() in the real path.
    let server = gt_store_dolt::connect(&base).expect("connect server");
    {
        // Clean slate so the idempotency check below starts from absent.
        let mut conn = server.get_conn().await.expect("conn");
        conn.query_drop("DROP DATABASE IF EXISTS `hq_f3-prov`").await.expect("drop");
    }

    // First provision: creates the DB (and does NOT fail on a missing gastown user).
    gt_store_dolt::create_workspace_dolt(&server, slug).await.expect("provision dolt db");
    // Second provision is a no-op (CREATE DATABASE IF NOT EXISTS).
    gt_store_dolt::create_workspace_dolt(&server, slug).await.expect("re-provision idempotent");

    // The per-workspace pool binds to `hq_<slug>`; ensure_schema seeds `issues`.
    let pools = gt_store_dolt::WorkspacePools::from_url(&format!("{base}/")).expect("pools");
    let ws_pool = pools.pool_for(slug).expect("pool_for slug");
    DoltIssues::new(ws_pool.clone()).ensure_schema().await.expect("ensure issues schema");

    // The issues table exists in the new tenant database.
    let mut conn = ws_pool.get_conn().await.expect("ws conn");
    let present: Option<i64> = conn
        .query_first(
            "SELECT 1 FROM information_schema.tables \
             WHERE table_schema = 'hq_f3-prov' AND table_name = 'issues' LIMIT 1",
        )
        .await
        .expect("probe issues table");
    assert_eq!(present, Some(1), "hq_f3-prov.issues must exist after provision");

    // Cleanup.
    conn.query_drop("DROP DATABASE IF EXISTS `hq_f3-prov`").await.expect("cleanup");
}

/// gtcore-1acbcf (C1): the `dispatch` column round-trips through insert/update/
/// list/get_detail/dispatch_index, the empty-string patch clears it back to NULL
/// (= inherit), and the `ensure_schema` migration that adds it is idempotent.
#[tokio::test]
async fn dispatch_column_round_trips_and_migration_is_idempotent() {
    let Ok(base) = std::env::var("GT_DOLT_URL") else {
        eprintln!("GT_DOLT_URL unset — skipping DoltIssues dispatch contract");
        return;
    };
    let base = base.trim_end_matches('/').to_string();
    seed(&base).await.expect("seed");
    let repo = DoltIssues::connect(&format!("{base}/{TEST_DB}")).expect("connect");

    // Migration idempotence: a second ensure_schema run is a no-op (the column
    // probe in information_schema skips the ALTER).
    repo.ensure_schema().await.expect("ensure_schema");
    repo.ensure_schema().await.expect("ensure_schema idempotent");

    // Omitted dispatch → NULL (= inherit-from-parent, manual at the root).
    let plain = format!("hq-disp-{}", ulid::Ulid::new());
    repo.insert(&NewIssue {
        id: plain.clone(),
        title: "inheriting".into(),
        issue_type: "task".into(),
        created_by: "test".into(),
        ..Default::default()
    })
    .await
    .expect("insert plain");
    assert_eq!(
        repo.get_detail(&plain).await.expect("d").expect("row").dispatch,
        None,
        "omitted dispatch stores NULL"
    );

    // Explicit dispatch on insert is honoured and surfaces on every read path.
    let auto = format!("hq-disp-{}", ulid::Ulid::new());
    repo.insert(&NewIssue {
        id: auto.clone(),
        title: "dispatchable".into(),
        issue_type: "epic".into(),
        created_by: "test".into(),
        dispatch: Some("auto".into()),
        ..Default::default()
    })
    .await
    .expect("insert auto");
    assert_eq!(
        repo.get_detail(&auto).await.expect("d").expect("row").dispatch.as_deref(),
        Some("auto")
    );
    let rows = repo.list(&IssueFilter::default()).await.expect("list");
    let row = rows.iter().find(|r| r.id == auto).expect("row in snapshot");
    assert_eq!(row.dispatch.as_deref(), Some("auto"), "dispatch in list snapshot");

    // dispatch_index covers the whole table with the raw values.
    let index = repo.dispatch_index().await.expect("dispatch_index");
    assert_eq!(index.get(&auto).cloned().flatten().as_deref(), Some("auto"));
    assert_eq!(index.get(&plain).cloned().flatten(), None);

    // Patch overwrites; empty string clears back to NULL (= inherit).
    repo.update(&auto, &IssuePatch { dispatch: Some("manual".into()), ..Default::default() })
        .await
        .expect("patch manual");
    assert_eq!(
        repo.get_detail(&auto).await.expect("d").expect("row").dispatch.as_deref(),
        Some("manual")
    );
    repo.update(&auto, &IssuePatch { dispatch: Some(String::new()), ..Default::default() })
        .await
        .expect("clear dispatch");
    assert_eq!(
        repo.get_detail(&auto).await.expect("d").expect("row").dispatch,
        None,
        "empty-string patch clears back to NULL (inherit)"
    );
}

/// hq-docs-archive-sync: `archive_old_closed` returns the archived set (id + issue_type), not a
/// bare count — the shape the archive interceptor reads to soft-delete an archived epic's docs.
#[tokio::test]
async fn archive_old_closed_returns_id_and_issue_type() {
    let Ok(base) = std::env::var("GT_DOLT_URL") else {
        eprintln!("GT_DOLT_URL unset — skipping DoltIssues.archive contract");
        return;
    };
    let base = base.trim_end_matches('/').to_string();
    seed(&base).await.expect("seed");

    use mysql_async::prelude::*;
    let db = format!("{base}/{TEST_DB}");
    let repo = DoltIssues::connect(&db).expect("connect");
    repo.ensure_schema().await.expect("schema present"); // adds the `archived_at` column

    // One epic + one task, both closed and backdated past the cutoff so the sweep catches them.
    let epic_id = format!("hq-arc-epic-{}", ulid::Ulid::new());
    let task_id = format!("hq-arc-task-{}", ulid::Ulid::new());
    for (id, issue_type) in [(&epic_id, "epic"), (&task_id, "task")] {
        repo.insert(&NewIssue {
            id: id.clone(),
            title: "archive subject".into(),
            issue_type: issue_type.into(),
            priority: 2,
            created_by: "test".into(),
            ..Default::default()
        })
        .await
        .expect("seed insert");
        repo.close(id, "claude-host").await.expect("close ok");
    }

    let pool = gt_store_dolt::connect(&db).expect("pool");
    let mut conn = pool.get_conn().await.expect("conn");
    // Backdate closed_at well past any sane cutoff so a 1-day sweep is eligible.
    conn.exec_drop(
        "UPDATE issues SET closed_at = DATE_SUB(NOW(), INTERVAL 10 DAY) WHERE id IN (:e, :t)",
        mysql_async::params! { "e" => &epic_id, "t" => &task_id },
    )
    .await
    .expect("backdate closed_at");

    let mut archived = repo.archive_old_closed(1).await.expect("archive sweep");
    archived.sort_by(|a, b| a.id.cmp(&b.id));
    let mut expected = vec![
        ArchivedIssue { id: epic_id.clone(), issue_type: "epic".into() },
        ArchivedIssue { id: task_id.clone(), issue_type: "task".into() },
    ];
    expected.sort_by(|a, b| a.id.cmp(&b.id));
    assert_eq!(archived, expected, "returns each archived id with its issue_type");

    // Idempotent: the rows are now stamped, so a second sweep finds nothing.
    let again = repo.archive_old_closed(1).await.expect("second sweep");
    assert!(again.is_empty(), "already-archived rows are not re-returned");

    // The stamp really landed (CAST yields the value as text; NULL would be `None`).
    let stamped: Option<String> = conn
        .exec_first(
            "SELECT CAST(archived_at AS CHAR) FROM issues WHERE id = :id",
            mysql_async::params! { "id" => &epic_id },
        )
        .await
        .expect("read archived_at");
    assert!(stamped.is_some(), "archived_at is non-NULL after the sweep");
}
