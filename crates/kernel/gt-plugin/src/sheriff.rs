//! Sheriff plugin stub. Wired into the relay so the chain is testable end-to-end (hq-evks);
//! the watchdog behavior — escalation on stuck merges, dispatch-timeout patrol — lands in
//! `gt-sheriff` as part of Paso 9.D (hq-92z9), which replaces this stub via the same
//! `Plugin` trait.
//!
//! The stub observes only: it counts the events it sees so the gate test can assert the
//! Sheriff was in the chain, but it never errors and never emits anything. That keeps
//! `replay_gt` byte-identical with or without the Sheriff attached.

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

use async_trait::async_trait;

use gt_eventlog::EventRecord;
use gt_events::AppError;

use crate::Plugin;

/// Lightweight observer that just tallies events. Tests poll [`SheriffPlugin::observed`] to
/// assert the relay reached it; production wiring (9.D) swaps this for the real watchdog.
#[derive(Default)]
pub struct SheriffPlugin {
    observed: Arc<AtomicUsize>,
}

impl SheriffPlugin {
    pub fn new() -> Self {
        Self::default()
    }

    /// Number of records the relay has delivered to this plugin so far.
    pub fn observed(&self) -> usize {
        self.observed.load(Ordering::SeqCst)
    }

    /// Shared counter handle — useful when the test wants to read the count from outside the
    /// registry's `Arc<dyn Plugin>` (the trait object hides concrete-type accessors).
    pub fn counter(&self) -> Arc<AtomicUsize> {
        self.observed.clone()
    }
}

#[async_trait]
impl Plugin for SheriffPlugin {
    fn name(&self) -> &'static str {
        "sheriff"
    }

    async fn on_event(&self, _record: &EventRecord) -> Result<(), AppError> {
        self.observed.fetch_add(1, Ordering::SeqCst);
        Ok(())
    }
}
