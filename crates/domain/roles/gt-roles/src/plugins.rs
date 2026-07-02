//! Observer plugins that wire role actors onto the per-workspace event broadcast
//! (`hq-mod-refactor.16`, approach A — roles react as `gt_plugin::Plugin` observers, not as
//! arms of a central match, because gt-core's modular one-namespace-per-module design has no
//! single event enum to match on).
//!
//! Only the roles that genuinely **react to other modules' broadcast events** are observers:
//!
//! - **Sheriff** — [`gt_sheriff::SheriffPlugin`] (kind-only watchdog), reused verbatim.
//! - **Deacon** — [`DeaconPlugin`] (below): mirrors merge lifecycle into the deacon's
//!   in-flight set so a drain waits for outstanding merges.
//! - **Witness** — [`WitnessPlugin`] (below): on `patrol.lease-expired.v1` (the lease-expiry
//!   source ported up in `hq-core-port.15`) it watches the dead worker so a later tick can
//!   escalate if it stays dark.
//!
//! The other two roles are deliberately **not** plugins:
//!
//! - **refinery** is a *producer*: it reads the channel mailbox and **emits** `merge.ready`,
//!   it does not consume it. It runs as a daemon (a Supervisor actor), not an observer.
//! - **mayor** is loop-driven (a periodic convoy scan), not event-driven.
//!
//! Like [`gt_sheriff::SheriffPlugin`], these observers are an intentional carve-out of the
//! "observers are read-only" convention: each forwards an observed event into its **own**
//! role actor, which owns its state machine and emits only legal transitions — so replay over
//! the resulting log stays byte-identical, and no *other* module's state is touched.

use std::sync::Arc;

use async_trait::async_trait;
use time::format_description::well_known::Rfc3339;
use time::OffsetDateTime;

use gt_deacon::{DeaconCommand, DeaconHandle, FinishItem, TrackItem};
use gt_events::AppError;
use gt_eventlog::EventRecord;
use gt_merge::MergeEvent;
use gt_patrol::PatrolEvent;
use gt_plugin::{Plugin, PluginRegistry};
use gt_sheriff::SheriffPlugin;
use gt_witness::{WatchTarget, WitnessCommand, WitnessHandle};

use crate::RoleStack;

/// Default inactivity window (seconds) a witnessed worker may stay silent before the next
/// tick escalates. Stamped into each `Watch` the [`WitnessPlugin`] issues. A conservative
/// 5 minutes; the periodic tick that consults it is a separate Supervisor timer (out of scope
/// of the observer).
const DEFAULT_WITNESS_THRESHOLD_SECS: u64 = 300;

/// Observer that mirrors the merge lifecycle into the deacon's in-flight set: `merge.ready.v1`
/// tracks a new item, `merge.merged.v1` finishes it. Every other kind is ignored. Wraps a
/// cloneable [`DeaconHandle`]; the actor behind it owns the drain state and emits the
/// `ItemBegun` / `DrainComplete` events.
pub struct DeaconPlugin {
    handle: DeaconHandle,
}

impl DeaconPlugin {
    /// Wrap a deacon command handle as an observer plugin.
    pub fn new(handle: DeaconHandle) -> Self {
        Self { handle }
    }
}

#[async_trait]
impl Plugin for DeaconPlugin {
    fn name(&self) -> &'static str {
        "deacon"
    }

    async fn on_event(&self, record: &EventRecord) -> Result<(), AppError> {
        // Only merge events concern the deacon; skip the rest before decoding so the relay's
        // dead-letter is not spammed with irrelevant kinds.
        if !record.kind.starts_with("merge.") {
            return Ok(());
        }
        match record.decode::<MergeEvent>()? {
            // A slot became merge-ready: register it as an in-flight item to wait for. `kind`
            // is the free-form source tag the deacon stores (it stays decoupled from the merge
            // enum — only this composition crate knows the mapping).
            MergeEvent::Ready { bead, .. } => {
                self.handle
                    .exec(DeaconCommand::Track(TrackItem {
                        id: bead,
                        kind: "merge".to_string(),
                    }))
                    .await
            }
            // The merge landed: clear the in-flight item. `at` is derived from the record's own
            // timestamp (log data, not a wall-clock read) so a triggered `DrainComplete` event
            // replays deterministically.
            MergeEvent::Merged { bead, .. } => {
                self.handle
                    .exec(DeaconCommand::Finish(FinishItem {
                        id: bead,
                        at: epoch_secs(&record.ts),
                    }))
                    .await
            }
            // started / failed / reset are not drain-tracked.
            MergeEvent::Started { .. } | MergeEvent::Failed { .. } | MergeEvent::Reset { .. } => {
                Ok(())
            }
        }
    }
}

/// Parse an [`EventRecord`] RFC3339 timestamp to epoch seconds. A malformed timestamp falls
/// back to `0` rather than dropping the event (the role actors use it only to stamp a
/// follow-up event, never to gate the reaction itself).
fn epoch_secs(ts: &str) -> u64 {
    OffsetDateTime::parse(ts, &Rfc3339)
        .map(|t| t.unix_timestamp().max(0) as u64)
        .unwrap_or(0)
}

/// Observer that starts watching a worker whose lease just expired, so a later witness tick
/// can escalate if it stays silent. Reacts only to `patrol.lease-expired.v1`. Wraps a
/// cloneable [`WitnessHandle`]; the witness actor owns the watch set + escalation state.
pub struct WitnessPlugin {
    handle: WitnessHandle,
    /// Inactivity window stamped into each `Watch` (see [`DEFAULT_WITNESS_THRESHOLD_SECS`]).
    threshold_secs: u64,
}

impl WitnessPlugin {
    /// Wrap a witness command handle as an observer with the given escalation threshold.
    pub fn new(handle: WitnessHandle, threshold_secs: u64) -> Self {
        Self { handle, threshold_secs }
    }
}

#[async_trait]
impl Plugin for WitnessPlugin {
    fn name(&self) -> &'static str {
        "witness"
    }

    async fn on_event(&self, record: &EventRecord) -> Result<(), AppError> {
        // Only the lease-expiry output concerns the witness; the other patrol kinds
        // (registered/heartbeat/closed) are scheduling's business.
        if record.kind != "patrol.lease-expired.v1" {
            return Ok(());
        }
        match record.decode::<PatrolEvent>()? {
            // The worker's lease lapsed: start watching it. `since_secs` is the record's own
            // timestamp (log data, replay-safe); `threshold_secs` is this observer's config.
            // A re-watch of an already-watched worker is rejected by the actor and lands in the
            // relay dead-letter — harmless, and rare (a worker expires once per lease).
            PatrolEvent::LeaseExpired { worker, .. } => {
                self.handle
                    .exec(WitnessCommand::Watch(WatchTarget {
                        worker,
                        since_secs: epoch_secs(&record.ts),
                        threshold_secs: self.threshold_secs,
                    }))
                    .await
            }
            // The kind guard above means only LeaseExpired reaches here; the rest are defensive.
            _ => Ok(()),
        }
    }
}

impl RoleStack {
    /// Build the observer-plugin registry for this role stack: the roles that react to the
    /// broadcast (sheriff, deacon, witness — in that fixed fan-out order). See the
    /// [module docs](self) for why refinery + mayor are not plugins.
    pub fn plugin_registry(&self) -> PluginRegistry {
        PluginRegistry::new()
            .register(SheriffPlugin::new(self.sheriff().clone()))
            .register(DeaconPlugin::new(self.deacon().clone()))
            .register(WitnessPlugin::new(
                self.witness().clone(),
                DEFAULT_WITNESS_THRESHOLD_SECS,
            ))
    }

    /// Wire this stack's observer plugins onto `handle`'s per-workspace event hub
    /// (`hq-mod-refactor.18` seam): every appended [`EventRecord`] published through the hub
    /// fans out to the sheriff + deacon + witness observers. Spawns the relay task — must run
    /// inside a Tokio runtime; the handle aborts it on `shutdown`.
    pub fn register_plugins(&self, handle: &gt_runtime::RootHandle) {
        handle.spawn_plugins(Arc::new(self.plugin_registry()));
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::sync::mpsc;

    use gt_deacon::DeaconEvent;
    use gt_events::Envelope;
    use gt_witness::WitnessEvent;

    fn merge_record(kind: &str, ev: &MergeEvent) -> EventRecord {
        EventRecord {
            event_id: "e1".into(),
            correlation_id: "c1".into(),
            causation_id: None,
            ts: "2026-06-02T10:00:00Z".into(),
            kind: kind.into(),
            payload: serde_json::to_value(ev).unwrap(),
        }
    }

    /// Spawn a deacon actor + return its plugin and the sink the actor emits onto.
    fn deacon_plugin() -> (DeaconPlugin, mpsc::Receiver<Envelope<DeaconEvent>>) {
        let (tx, rx) = mpsc::channel(16);
        let handle = gt_deacon::spawn(gt_deacon::InMemoryDeaconRepo::default(), tx);
        (DeaconPlugin::new(handle), rx)
    }

    #[tokio::test]
    async fn deacon_tracks_a_merge_ready_item() {
        let (plugin, mut rx) = deacon_plugin();
        let ev = MergeEvent::Ready {
            bead: "gg-1".into(),
            branch: "wt/gg-1".into(),
            channel_msg_id: "01ABC.event".into(),
        };
        plugin
            .on_event(&merge_record("merge.ready.v1", &ev))
            .await
            .unwrap();

        let emitted = rx.recv().await.unwrap();
        assert!(
            matches!(emitted.payload, DeaconEvent::ItemBegun { ref id, .. } if id == "gg-1"),
            "merge.ready should begin tracking the bead, got {:?}",
            emitted.payload
        );
    }

    #[tokio::test]
    async fn deacon_ignores_non_merge_kinds() {
        let (plugin, _rx) = deacon_plugin();
        let rec = EventRecord {
            event_id: "e2".into(),
            correlation_id: "c2".into(),
            causation_id: None,
            ts: "2026-06-02T10:00:00Z".into(),
            kind: "rig.added.v1".into(),
            payload: serde_json::Value::Null,
        };
        // Must not error (and not try to decode a MergeEvent out of an unrelated payload).
        plugin.on_event(&rec).await.unwrap();
    }

    #[test]
    fn epoch_secs_parses_rfc3339_and_falls_back_to_zero() {
        assert_eq!(epoch_secs("1970-01-01T00:00:10Z"), 10);
        assert_eq!(epoch_secs("not-a-timestamp"), 0);
    }

    fn patrol_record(ev: &PatrolEvent) -> EventRecord {
        EventRecord {
            event_id: "e3".into(),
            correlation_id: "c3".into(),
            causation_id: None,
            ts: "1970-01-01T00:00:42Z".into(),
            kind: "patrol.lease-expired.v1".into(),
            payload: serde_json::to_value(ev).unwrap(),
        }
    }

    #[tokio::test]
    async fn witness_watches_the_worker_on_lease_expiry() {
        let (tx, mut rx) = mpsc::channel(16);
        let handle = gt_witness::spawn(gt_witness::InMemoryWitnessRepo::default(), tx);
        let plugin = WitnessPlugin::new(handle, 90);

        let ev = PatrolEvent::LeaseExpired {
            bead: "gg-7".into(),
            worker: "polecat-3".into(),
            priority: 1,
        };
        plugin.on_event(&patrol_record(&ev)).await.unwrap();

        let emitted = rx.recv().await.unwrap();
        assert!(
            matches!(
                emitted.payload,
                WitnessEvent::TargetWatched { ref worker, since_secs, threshold_secs }
                    if worker == "polecat-3" && since_secs == 42 && threshold_secs == 90
            ),
            "lease expiry should watch the worker with ts-derived since + configured threshold, got {:?}",
            emitted.payload
        );
    }

    #[tokio::test]
    async fn witness_ignores_non_lease_expired_kinds() {
        let (tx, _rx) = mpsc::channel(16);
        let handle = gt_witness::spawn(gt_witness::InMemoryWitnessRepo::default(), tx);
        let plugin = WitnessPlugin::new(handle, 90);
        let rec = EventRecord {
            event_id: "e4".into(),
            correlation_id: "c4".into(),
            causation_id: None,
            ts: "1970-01-01T00:00:42Z".into(),
            kind: "patrol.heartbeat.v1".into(),
            payload: serde_json::Value::Null,
        };
        plugin.on_event(&rec).await.unwrap();
    }
}
