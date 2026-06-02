//! `gt-roles` — the per-workspace role-actor stack (`hq-mt-runtime.8`).
//!
//! A live workspace runs five behavioral actors — mayor, sheriff, witness, refinery,
//! deacon — each owning its own state behind an `mpsc` mailbox (see the individual role
//! crates). This crate instances all five for one workspace and binds their lifetime to the
//! workspace's [`Supervisor`](gt_runtime::Supervisor): they start when the handle starts and
//! wind down when it drains/stops.
//!
//! ## Why this lives in `domain/roles`, not `gt-runtime`
//!
//! The composer must name both [`gt_runtime::Supervisor`] (a `domain/orchestration` type)
//! and the five role crates (`domain/roles`). Per docs/03 Rule 4 the orchestration tier may
//! depend on kernel + `domain/platform` **only** — not on roles — while `domain/roles/*` may
//! depend on *all* `domain/*`. So the orchestration crate (`gt-runtime`) cannot host this
//! wiring; the roles tier is the only tier that may, and is where it belongs. The
//! `Supervisor` seam (`hq-mt-routing.2`) was deliberately built actor-agnostic for exactly
//! this reason — it takes an actor *factory*, never naming a concrete role.
//!
//! ## Lifecycle bridge
//!
//! Each role's `spawn` detaches its own Tokio task that lives as long as a [handle] to it
//! exists (the mailbox closes when the last sender drops). To tie that to the supervisor we
//! register, per role, a tiny **anchor** future via [`Supervisor::spawn_actor`]: the anchor
//! holds a clone of the role handle and parks on the [`Phase`](gt_runtime::Phase) watch until
//! the stack reaches [`Draining`](gt_runtime::Phase::Draining), then returns — dropping its
//! clone. The anchor *is* the supervisor-managed task ([`drain`]/[`shutdown`] await it); the
//! detached role task itself terminates once the last handle (the [`RoleStack`] the caller
//! holds) is also dropped, which the registry does when it evicts the workspace — so the
//! supervisor wind-down and the stack drop happen together on eviction.
//!
//! Registration must happen while the supervisor is still in [`Phase::Built`] (before
//! [`start`](gt_runtime::Supervisor::start)); [`RoleStack::register`] surfaces a late call as
//! `Err(phase)`, mirroring [`Supervisor::spawn_actor`].
//!
//! ## Event sinks are caller-supplied
//!
//! Each role emits its events into an `mpsc` sender the caller provides ([`RoleSinks`]). The
//! receiving half is drained into that workspace's bus / event log by the composition root;
//! wiring those bus-backed sinks is bins-decomposition work (`hq-mod-refactor.13`). Repos are
//! the in-memory defaults here (an empty board per workspace); replay-hydration from the
//! event log is a follow-up (`hq-mt-data.8`).
//!
//! [handle]: gt_sheriff::SheriffHandle
//! [`drain`]: gt_runtime::Supervisor::drain

#![forbid(unsafe_code)]

mod plugins;

pub use plugins::DeaconPlugin;

use gt_events::Envelope;
use gt_runtime::{Phase, Supervisor};
use tokio::sync::mpsc;

use gt_deacon::{DeaconEvent, DeaconHandle, InMemoryDeaconRepo};
use gt_mayor::{InMemoryMayorRepo, MayorEvent, MayorHandle};
use gt_refinery::{InMemoryRefineryRepo, RefineryEvent, RefineryHandle};
use gt_sheriff::{InMemorySheriffRepo, SheriffEvent, SheriffHandle};
use gt_witness::{InMemoryWitnessRepo, WitnessEvent, WitnessHandle};

/// The event sinks for one workspace's role actors — one relay sender per role.
///
/// The caller (composition root) owns the receiving halves and drains each into the
/// workspace's bus / event log. A role emits into its sink best-effort; a dropped receiver
/// loses events but never blocks or panics the actor (the role crates send with `let _ =`).
pub struct RoleSinks {
    /// Sink for [`SheriffEvent`]s (watch raises).
    pub sheriff: mpsc::Sender<Envelope<SheriffEvent>>,
    /// Sink for [`WitnessEvent`]s (worker escalations).
    pub witness: mpsc::Sender<Envelope<WitnessEvent>>,
    /// Sink for [`DeaconEvent`]s (drain progress).
    pub deacon: mpsc::Sender<Envelope<DeaconEvent>>,
    /// Sink for [`MayorEvent`]s (delegation lifecycle).
    pub mayor: mpsc::Sender<Envelope<MayorEvent>>,
    /// Sink for [`RefineryEvent`]s (merge-ready projection).
    pub refinery: mpsc::Sender<Envelope<RefineryEvent>>,
}

/// The five live role-actor handles for one workspace.
///
/// Built by [`register`](Self::register), which also wires each actor's lifecycle into the
/// supervisor. The caller holds this to send commands to the actors; dropping it (e.g. on
/// workspace eviction) releases the last handles so the detached actor tasks exit.
pub struct RoleStack {
    sheriff: SheriffHandle,
    witness: WitnessHandle,
    deacon: DeaconHandle,
    mayor: MayorHandle,
    refinery: RefineryHandle,
}

impl RoleStack {
    /// Instance all five role actors for a workspace and register their lifecycle with
    /// `supervisor`.
    ///
    /// Each actor is spawned with an in-memory repo (empty board) and the matching sink from
    /// `sinks`, then anchored to the supervisor's [`Phase`] so it winds down with the
    /// workspace. Returns the [`RoleStack`] of command handles on success.
    ///
    /// Must be called while the supervisor is in [`Phase::Built`] (before
    /// [`start`](Supervisor::start)); a later call returns `Err` with the observed phase, the
    /// same contract as [`Supervisor::spawn_actor`]. On that error the actors already spawned
    /// in this call are dropped (their handles fall out of scope), so they exit cleanly.
    pub async fn register(supervisor: &Supervisor, sinks: RoleSinks) -> Result<RoleStack, Phase> {
        let sheriff = gt_sheriff::spawn(InMemorySheriffRepo::default(), sinks.sheriff);
        let witness = gt_witness::spawn(InMemoryWitnessRepo::default(), sinks.witness);
        let deacon = gt_deacon::spawn(InMemoryDeaconRepo::default(), sinks.deacon);
        let mayor = gt_mayor::spawn(InMemoryMayorRepo::default(), sinks.mayor);
        let refinery = gt_refinery::spawn(InMemoryRefineryRepo::default(), sinks.refinery);

        // Anchor each actor to the supervisor lifecycle. A clone keeps the actor alive while
        // the supervisor runs; the original stays in the returned stack for command access.
        anchor(supervisor, sheriff.clone()).await?;
        anchor(supervisor, witness.clone()).await?;
        anchor(supervisor, deacon.clone()).await?;
        anchor(supervisor, mayor.clone()).await?;
        anchor(supervisor, refinery.clone()).await?;

        Ok(RoleStack { sheriff, witness, deacon, mayor, refinery })
    }

    /// Command handle for the sheriff (watchdog) actor.
    pub fn sheriff(&self) -> &SheriffHandle {
        &self.sheriff
    }

    /// Command handle for the witness (patrol/escalation) actor.
    pub fn witness(&self) -> &WitnessHandle {
        &self.witness
    }

    /// Command handle for the deacon (drain coordination) actor.
    pub fn deacon(&self) -> &DeaconHandle {
        &self.deacon
    }

    /// Command handle for the mayor (orchestration loop) actor.
    pub fn mayor(&self) -> &MayorHandle {
        &self.mayor
    }

    /// Command handle for the refinery (merge-ready projection) actor.
    pub fn refinery(&self) -> &RefineryHandle {
        &self.refinery
    }
}

/// Register a role handle as a supervisor lifecycle anchor.
///
/// The anchor future holds `handle` and parks on the phase watch until the stack reaches
/// [`Phase::Draining`] (or the supervisor is dropped, closing the channel), then returns —
/// releasing its clone. It is the supervisor-managed task that [`drain`](Supervisor::drain)
/// and [`shutdown`](Supervisor::shutdown) await; the role's own detached task ends once every
/// handle (including the caller's [`RoleStack`]) is dropped.
async fn anchor<H: Send + 'static>(supervisor: &Supervisor, handle: H) -> Result<(), Phase> {
    supervisor
        .spawn_actor(move |mut phase_rx| async move {
            // Hold the handle for the life of the stack; it drops when this future returns.
            let _anchor = handle;
            loop {
                if *phase_rx.borrow_and_update() >= Phase::Draining {
                    return;
                }
                if phase_rx.changed().await.is_err() {
                    return; // supervisor dropped — wind down.
                }
            }
        })
        .await
}

#[cfg(test)]
mod tests {
    use super::*;
    use gt_sheriff::{RegisterWatch, SheriffCommand};

    /// Build a `RoleSinks` whose receivers are kept alive for the test's duration, so a role
    /// emit during the test is delivered rather than dropped. Returns the sinks plus the held
    /// receivers.
    #[allow(clippy::type_complexity)]
    fn sinks() -> (
        RoleSinks,
        (
            mpsc::Receiver<Envelope<SheriffEvent>>,
            mpsc::Receiver<Envelope<WitnessEvent>>,
            mpsc::Receiver<Envelope<DeaconEvent>>,
            mpsc::Receiver<Envelope<MayorEvent>>,
            mpsc::Receiver<Envelope<RefineryEvent>>,
        ),
    ) {
        let (st, sr) = mpsc::channel(16);
        let (wt, wr) = mpsc::channel(16);
        let (dt, dr) = mpsc::channel(16);
        let (mt, mr) = mpsc::channel(16);
        let (rt, rr) = mpsc::channel(16);
        (
            RoleSinks { sheriff: st, witness: wt, deacon: dt, mayor: mt, refinery: rt },
            (sr, wr, dr, mr, rr),
        )
    }

    #[tokio::test]
    async fn register_wires_all_five_then_starts() {
        let sup = Supervisor::new();
        let (sinks, _rx) = sinks();
        let stack = RoleStack::register(&sup, sinks).await.expect("registers while Built");
        // The five anchors registered; starting spawns them and is the real transition.
        assert_eq!(sup.phase(), Phase::Built);
        assert!(sup.start().await, "start spawns the anchored actors");
        assert_eq!(sup.phase(), Phase::Started);
        // Keep the stack alive past start.
        drop(stack);
    }

    #[tokio::test]
    async fn plugin_registry_holds_the_observer_roles_in_order() {
        let sup = Supervisor::new();
        let (sinks, _rx) = sinks();
        let stack = RoleStack::register(&sup, sinks).await.unwrap();
        // sheriff + deacon + witness are broadcast observers (refinery/mayor are not plugins —
        // see plugins.rs); registration order is the relay fan-out order.
        assert_eq!(
            stack.plugin_registry().names(),
            vec!["sheriff", "deacon", "witness"]
        );
    }

    #[tokio::test]
    async fn sheriff_actor_is_live_and_wired() {
        let sup = Supervisor::new();
        let (sinks, _rx) = sinks();
        let stack = RoleStack::register(&sup, sinks).await.unwrap();
        sup.start().await;

        // Drive a command through the real actor + repo and read it back.
        stack
            .sheriff()
            .exec(SheriffCommand::Register(RegisterWatch {
                id: "w1".into(),
                pattern: "merge.failed".into(),
                threshold: 3,
            }))
            .await
            .expect("register watch accepted");
        let snap = stack.sheriff().snapshot().await;
        assert!(snap.watches.contains_key("w1"), "actor applied the command");
    }

    #[tokio::test]
    async fn registration_after_start_is_rejected() {
        let sup = Supervisor::new();
        sup.start().await;
        let (sinks, _rx) = sinks();
        // `.err()` avoids requiring `RoleStack: Debug` (the role handles aren't `Debug`).
        let err = RoleStack::register(&sup, sinks).await.err();
        assert_eq!(err, Some(Phase::Started));
    }

    #[tokio::test]
    async fn anchors_wind_down_on_shutdown() {
        let sup = Supervisor::new();
        let (sinks, _rx) = sinks();
        let _stack = RoleStack::register(&sup, sinks).await.unwrap();
        sup.start().await;
        // shutdown awaits the five anchor futures; it must return (not hang).
        sup.shutdown().await;
        assert_eq!(sup.phase(), Phase::Stopped);
    }
}
