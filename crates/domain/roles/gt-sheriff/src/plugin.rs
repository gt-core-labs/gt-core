//! `SheriffPlugin` — implementation of `gt_plugin::Plugin` (Paso 9.B contract) that
//! forwards every observed [`EventRecord`]'s `kind` into the sheriff actor as a
//! `SheriffCommand::Observe`. This is the role's seam with the broadcast relay
//! (`gt_plugin::spawn_plugin_relay`): per hq-evks' handoff note, 9.D **swaps the stub
//! `gt_plugin::SheriffPlugin` for this real one through the same trait, no wiring change
//! at the composition root**.
//!
//! Convention note: the trait docstring says plugins are read-only with respect to the
//! domain. Sheriff is an intentional carve-out — its whole point is reactive observation
//! that bumps a domain counter and (conditionally) emits `sheriff.raised`. The actor still
//! owns its state machine and emits only legal transitions, so replay over the resulting
//! log is byte-identical.

use async_trait::async_trait;

use gt_eventlog::EventRecord;
use gt_events::AppError;
use gt_plugin::Plugin;

use crate::actor::SheriffHandle;
use crate::commands::{ObserveWatch, SheriffCommand};

/// Wraps a cloneable [`SheriffHandle`]; registered against the plugin relay by `main`.
pub struct SheriffPlugin {
    handle: SheriffHandle,
}

impl SheriffPlugin {
    pub fn new(handle: SheriffHandle) -> Self {
        Self { handle }
    }
}

#[async_trait]
impl Plugin for SheriffPlugin {
    fn name(&self) -> &'static str {
        "sheriff"
    }

    async fn on_event(&self, record: &EventRecord) -> Result<(), AppError> {
        // Suppress self-observation: the actor's own emits flow through the same broadcast,
        // so without this guard `sheriff.observed` would bump on every `sheriff.observed`
        // forever. The actor's `Observe` also no-ops on unwatched patterns, so this guard is
        // defensive — the dead-letter cost of letting it through is one entry per emit.
        if record.kind.starts_with("sheriff.") {
            return Ok(());
        }
        self.handle
            .exec(SheriffCommand::Observe(ObserveWatch {
                pattern: record.kind.clone(),
            }))
            .await
    }
}
