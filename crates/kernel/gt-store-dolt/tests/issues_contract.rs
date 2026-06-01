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

use mysql_async::prelude::Queryable;

use gt_store_dolt::{
    ClaimOutcome, DoltIssues, IssueFilter, IssuePatch, IssuePhase, IssueStatus, NewIssue,
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
            external_ref        VARCHAR(255),
            spec_id             VARCHAR(1024),
            domain_json         TEXT NOT NULL DEFAULT '[]',
            surface_json        TEXT NOT NULL DEFAULT '[]',
            depends_on_json     TEXT NOT NULL DEFAULT '[]',
            role_scope          VARCHAR(32),
            version             BIGINT NOT NULL DEFAULT 0
        )",
    )
    .await?;
    conn.query_drop("DELETE FROM issues").await?;
    conn.query_drop(
        "INSERT INTO issues (id, title, description, design, acceptance_criteria, notes,
            status, priority, issue_type, assignee, external_ref) VALUES
            ('hq-a',  'alpha',  '', '', '', '', 'open',    0, 'epic',  'alice', 'hq-root'),
            ('hq-b',  'beta',   '', '', '', '', 'working', 1, 'task',  'bob',   NULL),
            ('hq-c',  'gamma',  '', '', '', '', 'closed',  2, 'task',  NULL,    'hq-root'),
            ('hq-d',  'delta',  '', '', '', '', 'open',    2, 'spike', 'alice', NULL)",
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

    let by_ref = repo
        .list(&IssueFilter {
            external_ref: Some("hq-root".into()),
            ..Default::default()
        })
        .await
        .expect("list external_ref");
    assert_eq!(by_ref.len(), 2);

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
        external_ref: Some("hq-root".into()),
        assignee: Some("alice".into()),
        owner: Some("alice".into()),
        ..Default::default()
    };
    repo.insert(&row).await.expect("insert");

    // Visible to a follow-up list (proves the row landed).
    let by_ref = repo
        .list(&IssueFilter {
            external_ref: Some("hq-root".into()),
            ..Default::default()
        })
        .await
        .expect("list");
    let ids: Vec<&str> = by_ref.iter().map(|r| r.id.as_str()).collect();
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
        external_ref: None,
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
        depends_on_json: Some("[\"hq-dep-1\"]".into()),
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
    assert_eq!(row_after.depends_on_json, "[\"hq-dep-1\"]");

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

    // Cross-bead cycle guard: self-edge and a 2-node back-edge are both rejected.
    let self_cycle = IssuePatch {
        depends_on_json: Some(format!("[\"{id}\"]")),
        ..Default::default()
    };
    let err = repo
        .update(&id, &self_cycle)
        .await
        .expect_err("self-cycle must be rejected");
    assert!(err.to_string().contains("self-cycle"), "got `{err}`");

    let other = format!("hq-cyc-{}", ulid::Ulid::new());
    repo.insert(&NewIssue {
        id: other.clone(),
        title: "dependent".into(),
        issue_type: "task".into(),
        created_by: "test".into(),
        depends_on_json: format!("[\"{id}\"]"),
        ..Default::default()
    })
    .await
    .expect("seed dependent");
    let back_edge = IssuePatch {
        depends_on_json: Some(format!("[\"{other}\"]")),
        ..Default::default()
    };
    let err = repo
        .update(&id, &back_edge)
        .await
        .expect_err("2-node cycle must be rejected");
    assert!(err.to_string().contains("cycle"), "got `{err}`");

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
        external_ref: None,
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
        external_ref: None,
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
