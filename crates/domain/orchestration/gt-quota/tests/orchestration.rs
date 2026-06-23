//! Step 6.c gate (docs/08-getting-started.md and docs/features/token-tracking-prediction.md):
//! wires the `gt-quota` actor with the in-memory repo + audit log; seeds synthetic usage at a
//! known rate; verifies the predictor:
//!
//! 1. computes `eta_to_block` within tolerance,
//! 2. emits a single `BlockPredicted` per window (idempotent),
//! 3. the rotation chain fires before any `AccountLimited`,
//! 4. replaying the log rebuilds `QuotaState` byte-for-byte.

use std::collections::HashMap;
use std::sync::Arc;

use tokio::sync::mpsc;

use gt_eventlog::{read_all, replay, EventRecord, EventStore, JsonlWriter};
use gt_events::Envelope;
use gt_quota::{
    actor as quota, Account, AccountQuotaStatus, AccountWindow, InMemoryQuota, ModelWeights,
    QuotaEvent, QuotaRepository, QuotaState, UsageSample, WindowKind,
};

const W_START: u64 = 1_700_000_000;
// Budget chosen so that after the seed (6 samples × 200 = 1200 consumed) `remaining` stays
// positive and the rate projects a block within the threshold (see the math below).
const W_LIMIT: u64 = 5_000;
const W_RESETS: u64 = W_START + 18_000; // rolling-5h
const THRESHOLD_SECS: u64 = 900;

fn account_with_window() -> Account {
    Account {
        id: "acc-1".to_string(),
        status: AccountQuotaStatus::Healthy,
        window: Some(AccountWindow {
            kind: WindowKind::Rolling5h,
            limit: W_LIMIT,
            started_at_secs: W_START,
            resets_at_secs: W_RESETS,
            consumed: 0.0,
        }),
        weekly_window: None,
        last_probe_secs: None,
        sampled_since_probe: 0.0,
        probe_divergence: None,
        credential_dead: false,
    }
}

#[tokio::test]
async fn predictive_rotation_fires_before_account_limited_and_replay_matches() {
    let repo = Arc::new(InMemoryQuota::default());
    let (ev_tx, mut ev_rx) = mpsc::channel::<Envelope<QuotaEvent>>(128);
    // IDENTITY weights -> 1 token = 1 cost unit.
    let mut weights: HashMap<String, ModelWeights> = HashMap::new();
    weights.insert("opus".to_string(), ModelWeights::IDENTITY);

    let handle = quota::spawn(ev_tx, weights);
    handle.upsert_account(account_with_window()).await;
    repo.upsert_account(&account_with_window()).await.unwrap();

    let log_path = std::env::temp_dir().join(format!("gt-quota-{}.events.jsonl", std::process::id()));
    let _ = std::fs::remove_file(&log_path);
    let writer = JsonlWriter::new(&log_path);

    // Seed: two sessions burn the account at a known rate. Each minute a turn of 100 input +
    // 100 output (200 cost units) "completes". With limit=5000 and a combined rate, the
    // remaining budget projects a block well within the 15min threshold.
    for minute in 0..3u64 {
        let now = W_START + minute * 60;
        handle
            .sample("acc-1", "sess-a", "opus", 100, 100, 0, 0, now)
            .await;
        handle
            .sample("acc-1", "sess-b", "opus", 100, 100, 0, 0, now)
            .await;
        // Persist samples in the repo (what the outbox does in production).
        repo.insert_sample(&UsageSample {
            account: "acc-1".into(),
            session: "sess-a".into(),
            model: "opus".into(),
            ts_secs: now,
            input_tokens: 100,
            output_tokens: 100,
            cache_read: 0,
            cache_creation: 0,
        })
        .await
        .unwrap();
        repo.insert_sample(&UsageSample {
            account: "acc-1".into(),
            session: "sess-b".into(),
            model: "opus".into(),
            ts_secs: now,
            input_tokens: 100,
            output_tokens: 100,
            cache_read: 0,
            cache_creation: 0,
        })
        .await
        .unwrap();
    }

    // Drain the emitted TokensSampled.
    let mut sampled_in_log = 0usize;
    while sampled_in_log < 6 {
        let env = ev_rx.recv().await.expect("TokensSampled from the actor");
        writer
            .append(&EventRecord::from_envelope(&env).unwrap())
            .unwrap();
        if matches!(env.payload, QuotaEvent::TokensSampled { .. }) {
            sampled_in_log += 1;
        }
    }

    // Tick: the predictor must emit BlockPredicted (high rate, low ETA, within window).
    let tick_now = W_START + 3 * 60;
    handle.tick(tick_now, THRESHOLD_SECS).await;
    handle.tick(tick_now, THRESHOLD_SECS).await; // second tick -> idempotency.

    // Drain the BlockPredicted (only one per window).
    let env = ev_rx.recv().await.expect("BlockPredicted after tick");
    writer
        .append(&EventRecord::from_envelope(&env).unwrap())
        .unwrap();

    let (account, eta) = match env.payload {
        QuotaEvent::BlockPredicted {
            account,
            eta_to_block_secs,
            consumed,
            limit,
            rate_per_min,
            ..
        } => {
            assert!(consumed > 0, "aggregate of the 6 samples > 0");
            assert_eq!(limit, W_LIMIT);
            assert!(rate_per_min > 0.0, "rate-per-min > 0 after several samples");
            assert!(
                eta_to_block_secs <= THRESHOLD_SECS,
                "ETA must be under the threshold (got {eta_to_block_secs}s, threshold {THRESHOLD_SECS}s)",
            );
            (account, eta_to_block_secs)
        }
        other => panic!("expected BlockPredicted, got {other:?}"),
    };
    assert_eq!(account, "acc-1");

    // Predictive rotation: the composition root reacts to BlockPredicted before any
    // AccountLimited.
    handle.rotated("acc-1", "acc-2", tick_now).await;
    let env = ev_rx.recv().await.expect("Rotated");
    writer
        .append(&EventRecord::from_envelope(&env).unwrap())
        .unwrap();

    // Live snapshot: 1 managed account, 1 prediction emitted.
    let (accounts, predictions) = handle.snapshot().await;
    assert_eq!(accounts, 1);
    assert_eq!(predictions, 1, "per-window idempotency: NOT re-emitted");

    // Per-session attribution: the repo rollup breaks down by session correctly.
    let session_a = repo
        .session_window_tokens("acc-1", "sess-a", W_START, W_RESETS)
        .await
        .unwrap();
    let session_b = repo
        .session_window_tokens("acc-1", "sess-b", W_START, W_RESETS)
        .await
        .unwrap();
    let account_total = repo
        .account_window_tokens("acc-1", W_START, W_RESETS)
        .await
        .unwrap();
    assert_eq!(session_a, 600);
    assert_eq!(session_b, 600);
    assert_eq!(account_total, 1200);

    // REPLAY: rebuild QuotaState from the log and compare against the observable.
    let records = read_all(&log_path).unwrap();
    let state = replay(&records, QuotaState::default(), |s, e: &QuotaEvent| {
        s.apply(e)
    })
    .unwrap();

    assert_eq!(state.predictions.len(), 1, "a single prediction in the log");
    let (pred_account, pred_eta, pred_now) = &state.predictions[0];
    assert_eq!(pred_account, "acc-1");
    assert_eq!(*pred_eta, eta);
    assert_eq!(*pred_now, tick_now);
    assert_eq!(state.rotations, vec![("acc-1".to_string(), "acc-2".to_string())]);
    assert!(state.limited.is_empty(), "no AccountLimited (rotated earlier)");
    assert!(state.blocked.is_empty());

    let _ = std::fs::remove_file(&log_path);
}

#[tokio::test]
async fn fresh_window_after_reset_allows_another_prediction() {
    let (ev_tx, mut ev_rx) = mpsc::channel::<Envelope<QuotaEvent>>(128);
    let weights: HashMap<String, ModelWeights> = HashMap::new();
    let handle = quota::spawn(ev_tx, weights);

    handle.upsert_account(account_with_window()).await;

    // Burn enough in the first window.
    for minute in 0..3u64 {
        handle
            .sample(
                "acc-1",
                "sess-a",
                "opus",
                200,
                200,
                0,
                0,
                W_START + minute * 60,
            )
            .await;
    }
    handle.tick(W_START + 180, THRESHOLD_SECS).await;

    // Drain until the first BlockPredicted shows up.
    let mut first_pred_eta: Option<u64> = None;
    while first_pred_eta.is_none() {
        let env = ev_rx.recv().await.expect("event");
        if let QuotaEvent::BlockPredicted {
            eta_to_block_secs, ..
        } = env.payload
        {
            first_pred_eta = Some(eta_to_block_secs);
        }
    }

    // Window reset → must clear the idempotency.
    let w2_start = W_START + 18_000;
    handle
        .reset_window("acc-1", w2_start, w2_start + 18_000)
        .await;

    // In the new window, burn again and fire the predictor.
    for minute in 0..3u64 {
        handle
            .sample(
                "acc-1",
                "sess-a",
                "opus",
                200,
                200,
                0,
                0,
                w2_start + minute * 60,
            )
            .await;
    }
    handle.tick(w2_start + 180, THRESHOLD_SECS).await;

    let mut second_pred = false;
    let mut window_reset_seen = false;
    let deadline = tokio::time::Instant::now() + std::time::Duration::from_millis(500);
    while tokio::time::Instant::now() < deadline {
        let Ok(env) = tokio::time::timeout(std::time::Duration::from_millis(50), ev_rx.recv()).await
        else {
            continue;
        };
        let Some(env) = env else { break };
        match env.payload {
            QuotaEvent::WindowReset { .. } => window_reset_seen = true,
            QuotaEvent::BlockPredicted { .. } => {
                second_pred = true;
                break;
            }
            _ => {}
        }
    }

    assert!(window_reset_seen, "WindowReset must have been emitted");
    assert!(
        second_pred,
        "after reset, the new window must allow a new BlockPredicted",
    );

    let (_, predictions_total) = handle.snapshot().await;
    assert_eq!(predictions_total, 2, "one prediction per window");
}

#[tokio::test]
async fn remove_account_drops_from_registry_and_is_idempotent() {
    let (ev_tx, mut ev_rx) = mpsc::channel::<Envelope<QuotaEvent>>(16);
    let handle = quota::spawn(ev_tx, HashMap::new());

    handle.upsert_account(account_with_window()).await;
    let accounts_before = handle.accounts().await;
    assert_eq!(accounts_before.len(), 1, "upsert seeded the account");

    // First remove: id existed → true.
    assert!(handle.remove_account("acc-1").await, "registered account must be removed");

    // Registry is now empty.
    let accounts_after = handle.accounts().await;
    assert!(accounts_after.is_empty(), "registry must be empty after remove");

    // Second remove of the same id (idempotent): false, no panic.
    assert!(
        !handle.remove_account("acc-1").await,
        "removing an absent id must return false (idempotent)"
    );

    // Edge op: no domain event is produced.
    assert!(
        ev_rx.try_recv().is_err(),
        "remove_account must not emit a domain event"
    );
}
