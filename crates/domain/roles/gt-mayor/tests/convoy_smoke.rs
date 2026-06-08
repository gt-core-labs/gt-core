//! Convoy smoke gate (hq-mayor-platform-report-20260608.1): verify convoy.launch dispatch path.
//!
//! Exercises convoy.launch / convoy.info from the mayor role and confirms the dispatch
//! path. Acceptance criterion: "El convoy se lanza y convoy.info reporta el bead como member."
//!
//! The test simulates the path the mayor takes when it initiates a convoy: exec the
//! `convoy.launch.execute` command (via `OrchHandle::exec`), then read the convoy board back
//! via `convoy.info` (`OrchHandle::snapshot`) to confirm the bead is listed as a member and
//! was immediately dispatched (Active) as the sequential convoy's first and only member.

use std::time::Duration;

use tokio::sync::mpsc;

use gt_events::Envelope;
use gt_orchestration::{
    actor as orch_actor, ConvoyState, InMemoryOrchRepo, LaunchConvoy, MemberState, OrchCommand,
    OrchEvent,
};

const BEAD: &str = "hq-mayor-platform-report-20260608.1";
const CONVOY: &str = "mayor-smoke-20260608";

#[tokio::test]
async fn convoy_launch_dispatch_path() {
    let (relay_tx, mut relay_rx) = mpsc::channel::<Envelope<OrchEvent>>(16);
    let orch = orch_actor::spawn(InMemoryOrchRepo::default(), relay_tx);

    // convoy.launch.execute: mayor launches a convoy with BEAD as the sole member.
    orch.exec(OrchCommand::Launch(LaunchConvoy {
        convoy: CONVOY.to_string(),
        members: vec![BEAD.to_string()],
    }))
    .await
    .expect("convoy.launch must succeed");

    // convoy.info (snapshot): read the board back and find CONVOY.
    let snap = orch.snapshot().await;
    let convoy = snap
        .iter()
        .find(|c| c.id == CONVOY)
        .expect("convoy.info: convoy not found in board");

    // Acceptance criterion: the bead is reported as a member.
    let member = convoy
        .members
        .iter()
        .find(|m| m.bead == BEAD)
        .expect("convoy.info: bead not listed as member");

    // Dispatch path confirmed: after launch the sole member is immediately Active.
    assert_eq!(
        member.state,
        MemberState::Active,
        "bead {BEAD} must be active (dispatched) immediately after convoy.launch",
    );
    assert_eq!(convoy.state, ConvoyState::Launched);

    // Verify the relay emitted the expected event sequence: Created → Launched → Dispatched.
    let timeout = Duration::from_secs(5);
    let mut kinds = Vec::new();
    for _ in 0..3 {
        match tokio::time::timeout(timeout, relay_rx.recv()).await {
            Ok(Some(env)) => kinds.push(env.payload.kind().to_string()),
            other => panic!("relay closed early or timed out: {other:?}"),
        }
    }
    assert_eq!(
        kinds,
        ["convoy.created.v1", "convoy.launched.v1", "convoy.member_dispatched.v1"],
        "dispatch path must emit created + launched + member_dispatched in order",
    );
}
