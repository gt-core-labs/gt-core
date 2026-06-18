//! Triage → delegation integration gate (`gtcore-5c50f0`): the mayor, on wake, delegates
//! each ready bead (P0-first, up to the pool cap) to a polecat — one bead → one polecat.
//!
//! The pure prioritization invariants live in `gt-mayor::triage`'s unit tests. This test
//! covers the *producer* edge: that [`delegate_frontier`] runs the triage and actually records
//! a `Delegated` event per dispatched bead on the live actor board (the verifiable AC: "los
//! polecats los acuña/dispatcha el MAYOR"), caps at capacity, and surfaces the surplus +
//! decomposition for the loop to act on.

use tokio::sync::mpsc;

use gt_events::Envelope;
use gt_mayor::mayor::{delegate_frontier, POLECAT_TARGET};
use gt_mayor::{actor, DelegationStatus, FrontierBead, InMemoryMayorRepo, MayorEvent};

#[tokio::test]
async fn delegates_ready_beads_up_to_cap_p0_first() {
    let (relay_tx, mut relay_rx) = mpsc::channel::<Envelope<MayorEvent>>(16);
    let handle = actor::spawn(InMemoryMayorRepo::default(), relay_tx);

    // Frontier: a childless epic (must decompose, not delegate), four actionable beads of
    // mixed priority, and one blocked bead (must be skipped). Capacity = 2 free slots.
    let frontier = vec![
        FrontierBead::epic("epic-1", "umbrella", false),
        FrontierBead::actionable("p1a", "first p1", 1),
        FrontierBead::actionable("p0", "the p0", 0),
        FrontierBead::actionable("p1b", "second p1", 1),
        FrontierBead::actionable("p2", "the p2", 2),
        FrontierBead::actionable("blocked", "waiting", 0).blocked(),
    ];

    let plan = delegate_frontier(&handle, &frontier, 2)
        .await
        .expect("delegate_frontier must succeed");

    // Plan buckets: P0 + highest P1 delegated; the rest of the ready, actionable work queued;
    // the childless epic decomposed; the blocked bead nowhere.
    let del: Vec<&str> = plan.delegate.iter().map(|d| d.id.as_str()).collect();
    let q: Vec<&str> = plan.queue.iter().map(|d| d.id.as_str()).collect();
    let dec: Vec<&str> = plan.decompose.iter().map(|d| d.id.as_str()).collect();
    assert_eq!(del, ["p0", "p1a"], "cap=2 fills the two highest-priority slots");
    assert_eq!(q, ["p1b", "p2"], "surplus queued in priority order");
    assert_eq!(dec, ["epic-1"], "childless epic surfaced for decomposition");

    // The actor board reflects exactly the two delegations the mayor minted, each Pending and
    // targeted at a polecat — the verifiable AC that the MAYOR dispatches the work.
    let snap = handle.snapshot().await;
    assert_eq!(snap.delegations.len(), 2, "only the two delegated beads recorded");
    for id in ["p0", "p1a"] {
        let d = snap.delegations.get(id).expect("delegated bead on board");
        assert_eq!(d.status, DelegationStatus::Pending);
        assert_eq!(d.target, POLECAT_TARGET, "one bead → one polecat");
    }
    assert!(!snap.delegations.contains_key("blocked"), "blocked bead not delegated");
    assert!(!snap.delegations.contains_key("epic-1"), "epic not delegated");

    // The relay emitted exactly two `mayor.delegated` events.
    let mut delegated = 0;
    while let Ok(env) = relay_rx.try_recv() {
        if matches!(env.payload, MayorEvent::Delegated { .. }) {
            delegated += 1;
        }
    }
    assert_eq!(delegated, 2, "two Delegated events on the log");
}

#[tokio::test]
async fn idle_frontier_records_nothing() {
    let (relay_tx, _relay_rx) = mpsc::channel::<Envelope<MayorEvent>>(16);
    let handle = actor::spawn(InMemoryMayorRepo::default(), relay_tx);

    // Nothing ready → idle plan, no delegations recorded, no tokens spent.
    let plan = delegate_frontier(&handle, &[], 8)
        .await
        .expect("delegate_frontier over empty frontier must succeed");
    assert!(plan.is_idle());
    assert!(handle.snapshot().await.delegations.is_empty());
}

#[tokio::test]
async fn re_wake_over_same_frontier_is_idempotent() {
    let (relay_tx, _relay_rx) = mpsc::channel::<Envelope<MayorEvent>>(16);
    let handle = actor::spawn(InMemoryMayorRepo::default(), relay_tx);

    let frontier = vec![
        FrontierBead::actionable("a", "work a", 1),
        FrontierBead::actionable("b", "work b", 1),
    ];

    delegate_frontier(&handle, &frontier, 4).await.expect("first wake");
    // Second wake over the same frontier must not error on the already-recorded ids.
    delegate_frontier(&handle, &frontier, 4)
        .await
        .expect("re-wake is a no-op, not an error");

    assert_eq!(
        handle.snapshot().await.delegations.len(),
        2,
        "re-wake records no duplicates"
    );
}
