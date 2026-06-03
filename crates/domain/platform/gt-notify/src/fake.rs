//! In-process fake notifier: captures every delivered notification for assertions.
//!
//! Lives in the port crate (not a `tests/` dir) because it has no transport dependency and is
//! reused across the workspace's gate tests — the composition-root escalation gate injects it
//! as `Arc<dyn Notifier>` and reads back what was captured.

use std::sync::{Arc, Mutex};

use crate::notification::Notification;
use crate::Notifier;

/// Records notifications in order. Clone-shares the same backing store, so a test can hold a
/// view while the `Arc<dyn Notifier>` lives inside `RootConfig`.
#[derive(Clone, Default)]
pub struct FakeNotifier {
    captured: Arc<Mutex<Vec<Notification>>>,
}

impl FakeNotifier {
    pub fn new() -> Self {
        Self::default()
    }

    /// Snapshot of everything delivered so far, in delivery order.
    pub fn captured(&self) -> Vec<Notification> {
        self.captured.lock().unwrap().clone()
    }

    /// Number of notifications delivered.
    pub fn len(&self) -> usize {
        self.captured.lock().unwrap().len()
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }
}

impl Notifier for FakeNotifier {
    fn notify(&self, notification: &Notification) {
        self.captured.lock().unwrap().push(notification.clone());
    }
}
