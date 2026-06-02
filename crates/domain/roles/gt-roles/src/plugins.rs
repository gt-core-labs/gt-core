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
//!
//! The other three roles are deliberately **not** plugins:
//!
//! - **witness** reacts to a patrol `lease-expired` source that is not yet ported to gt-core
//!   (no `gt-patrol` crate / `patrol.*` event kind exists), so it has no broadcast source to
//!   observe. Re-add a `WitnessPlugin` here once patrol ports up.
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
use gt_plugin::{Plugin, PluginRegistry};
use gt_sheriff::SheriffPlugin;

use crate::RoleStack;

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
            // started / failed are not drain-tracked.
            MergeEvent::Started { .. } | MergeEvent::Failed { .. } => Ok(()),
        }
    }
}

/// Parse an [`EventRecord`] RFC3339 timestamp to epoch seconds. A malformed timestamp falls
/// back to `0` rather than dropping the finish (the deacon only uses it to stamp a follow-up
/// event, never to gate the finish itself).
fn epoch_secs(ts: &str) -> u64 {
    OffsetDateTime::parse(ts, &Rfc3339)
        .map(|t| t.unix_timestamp().max(0) as u64)
        .unwrap_or(0)
}

impl RoleStack {
    /// Build the observer-plugin registry for this role stack: the roles that react to the
    /// broadcast (sheriff + deacon, in that fixed order). See the [module docs](self) for why
    /// witness/refinery/mayor are not plugins.
    pub fn plugin_registry(&self) -> PluginRegistry {
        PluginRegistry::new()
            .register(SheriffPlugin::new(self.sheriff().clone()))
            .register(DeaconPlugin::new(self.deacon().clone()))
    }

    /// Wire this stack's observer plugins onto `handle`'s per-workspace event hub
    /// (`hq-mod-refactor.18` seam): every appended [`EventRecord`] published through the hub
    /// fans out to the sheriff + deacon observers. Spawns the relay task — must run inside a
    /// Tokio runtime; the handle aborts it on `shutdown`.
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
}
