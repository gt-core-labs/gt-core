//! End-to-end Dog lifecycle gate (`hq-mod-dogs.9`).
//!
//! Wires the whole sub-epic together as one flow, using only the crate's public API and
//! in-memory test doubles (no OS, no real MCP):
//!
//! register a sample plugin (cron gate, agent execution) → simulate a tick → the gate opens
//! (`.3`) → the dispatcher claims an idle Dog (`.2`) → the executor runs it on a fake agent
//! backend (`.4`) → a digest receipt is emitted into the in-memory sink (`.5`) → the Dog
//! returns to `Idle`.
//!
//! Asserts the ordering of the lifecycle states and the labels carried on the receipt.

use std::sync::Mutex;

use async_trait::async_trait;
use chrono::DateTime;

use gt_dog::{
    evaluate_gate, Dispatch, DogEvent, DogId, DogReport, DogStatus, ExecBackend, ExecutionType,
    GateContext, GateDecision, PluginExecutor,
};
use gt_dog::{Digest, InMemoryReceipts, Outcome, ReceiptSink, TrackingLabels};
use gt_plugin::descriptor::{Gate, GateType};

/// A fake "agent" execution backend: records every claim it was asked to run and reports
/// success. Stands in for the real session-spawning adapter (which lands with the pool, `.8`).
#[derive(Default)]
struct FakeAgent {
    ran: Mutex<Vec<String>>,
}

#[async_trait]
impl ExecBackend for FakeAgent {
    async fn run(&self, claim: &str, _execution: &ExecutionType) -> DogReport {
        self.ran.lock().unwrap().push(claim.to_string());
        DogReport::Completed
    }
}

#[tokio::test(flavor = "current_thread")]
async fn plugin_tick_drives_dog_through_claim_execute_digest_and_back_to_idle() {
    // --- the sample plugin: a cron gate firing hourly, executed as an agent. ----------
    let gate = Gate {
        kind: GateType::Cron,
        schedule: Some("0 0 * * * *".into()),
        ..Gate::default()
    };
    let claim = "premerge-42";
    let dog_id = DogId::new("sheriff").unwrap();

    // --- 1. simulate a tick: an hourly schedule, last fired at the epoch, evaluated two
    // hours later → at least one tick is due, so the gate opens. ------------------------
    let now = DateTime::from_timestamp(2 * 3600, 0).unwrap();
    let last_fire = DateTime::from_timestamp(1, 0);
    let never = |_: &str| false;
    let ctx = GateContext {
        now,
        last_fire,
        condition: &never,
        event_fired: false,
    };
    assert_eq!(
        evaluate_gate(&gate, &ctx).unwrap(),
        GateDecision::Open,
        "the hourly cron tick is due"
    );

    // --- 2. the dispatcher claims an idle Dog (Idle → Claimed). ------------------------
    let mut pool = gt_dog::DogDispatcher::with_capacity(1);
    assert!(pool.register(dog_id.clone()));
    assert_eq!(
        pool.dispatch(claim),
        Dispatch::Assigned(dog_id.clone()),
        "the only idle Dog takes the claim"
    );
    assert_eq!(pool.dog(&dog_id).unwrap().status, DogStatus::Claimed);
    assert_eq!(pool.dog(&dog_id).unwrap().claim.as_deref(), Some(claim));

    // --- 3. start running (Claimed → Running), then execute on the fake agent. ---------
    pool.apply(&DogEvent::Started {
        dog: dog_id.clone(),
    })
    .unwrap();
    assert_eq!(pool.dog(&dog_id).unwrap().status, DogStatus::Running);

    let executor = PluginExecutor::new(FakeAgent::default());
    let report = executor
        .execute(claim, &ExecutionType::Agent { persona: "sheriff".into() })
        .await
        .expect("execution validates");
    assert_eq!(report, DogReport::Completed);
    assert_eq!(
        *executor.backend().ran.lock().unwrap(),
        vec![claim.to_string()],
        "the fake agent ran exactly the dispatched claim"
    );

    // --- 4. emit the digest receipt into the in-memory sink. ---------------------------
    let receipts = InMemoryReceipts::new();
    let digest = Digest::from_report(
        dog_id.clone(),
        claim,
        &report,
        TrackingLabels::new().with("plugin", "premerge-sheriff"),
    );
    receipts.emit(&digest).await.unwrap();

    assert_eq!(receipts.len(), 1, "one receipt recorded");
    let recorded = receipts.recorded();
    let only = &recorded[0];
    assert_eq!(only.outcome, Outcome::Completed);
    let labels = only.labels();
    assert_eq!(labels.get("dog").map(String::as_str), Some("sheriff"));
    assert_eq!(labels.get("claim").map(String::as_str), Some(claim));
    assert_eq!(labels.get("outcome").map(String::as_str), Some("completed"));
    assert_eq!(
        labels.get("plugin").map(String::as_str),
        Some("premerge-sheriff"),
        "custom provenance label is carried through"
    );

    // --- 5. finish (Running → Idle): the Dog is reusable for the next tick. ------------
    pool.apply(&DogEvent::Finished {
        dog: dog_id.clone(),
    })
    .unwrap();
    let final_state = pool.dog(&dog_id).unwrap();
    assert_eq!(final_state.status, DogStatus::Idle);
    assert!(final_state.is_idle());
    assert_eq!(final_state.claim, None, "the claim is cleared on finish");
}

/// A `Manual` gate never auto-opens, so a scheduler tick must not dispatch a Dog for it —
/// the negative-space companion to the cron flow above.
#[tokio::test(flavor = "current_thread")]
async fn manual_gate_does_not_dispatch_on_tick() {
    let gate = Gate {
        kind: GateType::Manual,
        ..Gate::default()
    };
    let never = |_: &str| false;
    let ctx = GateContext {
        now: DateTime::from_timestamp(10_000, 0).unwrap(),
        last_fire: None,
        condition: &never,
        event_fired: false,
    };
    assert!(
        matches!(evaluate_gate(&gate, &ctx).unwrap(), GateDecision::Closed { .. }),
        "manual gates are caller-triggered, never opened by a tick"
    );
}
