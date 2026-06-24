//! Rig event sink port (rig-hold H1, epic gtcore-4b7d56).
//!
//! `gt-rig` is a `domain/platform` crate and must stay infra-free (docs/03 Rule 4): it cannot
//! depend on the composition root's event log. This is the seam — exactly mirroring
//! [`gt_issues::IssueEventSink`] — that lets the rig surfaces (MCP + REST) record a decided
//! [`RigEvent`] for observability without knowing where it lands. The composition root backs it
//! with the per-workspace event log; tests back it with a recording double.
//!
//! Only the dispatch-mode transitions (`rig.held.v1` / `rig.resumed.v1`) flow through this seam
//! today — the operator-auditability the rig-hold feature asked for. The other rig mutations stay
//! projection-only (they upsert the `rigs` row without an event), unchanged by this task.

use crate::events::RigEvent;

/// Fire-and-forget sink for decided [`RigEvent`]s.
///
/// Best-effort by contract (same as [`gt_issues::IssueEventSink`]): the mutation has already
/// committed to the catalog by the time `emit` is called, so a sink failure must NOT fail the
/// request — an implementation logs and moves on. `workspace` is the tenant the event belongs to
/// (`None` ⇒ the default workspace), so a path-partitioned log writes it to the right partition.
pub trait RigEventSink: Send + Sync {
    /// Record one decided rig event for the workspace. Infallible at the call site — see the trait
    /// note on best-effort semantics.
    fn emit(&self, workspace: Option<&str>, event: &RigEvent);
}

/// A sink that drops every event. The default when no observability backend is wired (tests, or a
/// surface built without an event log) — emitting is then a silent no-op, never an error.
#[derive(Debug, Clone, Copy, Default)]
pub struct NoopRigEventSink;

impl RigEventSink for NoopRigEventSink {
    fn emit(&self, _workspace: Option<&str>, _event: &RigEvent) {}
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Mutex;

    /// A recording double used by the surface tests to assert exactly which events were emitted.
    #[derive(Default)]
    struct Recording {
        events: Mutex<Vec<(Option<String>, String)>>,
    }

    impl RigEventSink for Recording {
        fn emit(&self, workspace: Option<&str>, event: &RigEvent) {
            use gt_events::EventKind;
            self.events
                .lock()
                .unwrap()
                .push((workspace.map(str::to_string), event.kind().to_string()));
        }
    }

    #[test]
    fn noop_sink_swallows_events() {
        let sink = NoopRigEventSink;
        sink.emit(
            Some("default"),
            &RigEvent::Resumed {
                rig: "plane".into(),
                now_secs: 1,
            },
        );
        // Nothing to assert beyond "does not panic / compiles against the trait".
    }

    #[test]
    fn recording_sink_captures_kind_and_workspace() {
        let sink = Recording::default();
        sink.emit(
            Some("acme"),
            &RigEvent::Held {
                rig: "plane".into(),
                reason: "intervention".into(),
                now_secs: 1,
            },
        );
        let got = sink.events.lock().unwrap().clone();
        assert_eq!(got, vec![(Some("acme".to_string()), "rig.held.v1".to_string())]);
    }
}
