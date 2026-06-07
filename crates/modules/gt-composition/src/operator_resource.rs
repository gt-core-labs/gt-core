//! Event-log-backed [`OperatorResource`] for the `operated_by` overlay (`hq-agent-observability.3`).
//!
//! The issues REST surface stays infra-free and takes a provider for the agent operating each bead
//! (`gt_issues::OperatorResource`); this is the composition-root implementation, the read-side twin
//! of [`crate::mcp::eventlog::EventLogIssueSink`]. It folds the workspace event log's
//! `issues.operated.v1` / `issues.operator-cleared.v1` records (emitted by the polecat supervisor,
//! `.2`) into the current bead→operator map and answers per-bead lookups from it.
//!
//! The fold is point-in-time and per request: the same `read_since(ws, Some("issues"), …)` the SSE
//! feed uses, then a linear apply (Operated inserts, Cleared removes). Cheap relative to the Dolt
//! query the same request already makes, and always consistent with what `?channel=issues` streamed.

use std::collections::HashMap;
use std::sync::Arc;

use serde_json::json;

use gt_issues::OperatorResource;

use crate::mcp::eventlog::EventLog;
use crate::operator_event::IssueOperatorEvent;

/// Upper bound on the issues-channel records folded per request. The operator log is one record per
/// sling/merge, so this covers a very long-lived workspace; the read is tail-bounded anyway.
const FOLD_LIMIT: usize = 100_000;

/// Resolves the operating agent of each bead by folding the workspace event log.
pub struct EventLogOperatorResource {
    log: Arc<EventLog>,
}

impl EventLogOperatorResource {
    /// Back the provider with the shared per-workspace event log — the same handle the SSE feed and
    /// the issue sink use, so the overlay is consistent with the live `?channel=issues` stream.
    pub fn new(log: Arc<EventLog>) -> Self {
        Self { log }
    }

    /// Fold the workspace's issues channel into the current bead→operator JSON map. A best-effort
    /// read failure yields an empty map (the overlay is then simply absent — never an error on the
    /// issue read itself).
    fn fold(&self, workspace: Option<&str>) -> HashMap<String, serde_json::Value> {
        let mut operators: HashMap<String, serde_json::Value> = HashMap::new();
        let records = match self.log.read_since(workspace, Some("issues"), None, FOLD_LIMIT) {
            Ok(r) => r,
            Err(_) => return operators,
        };
        for record in &records {
            // Only the two operator kinds decode into `IssueOperatorEvent`; other `issues.*`
            // records (created/updated/transitioned/…) fail the decode and are skipped.
            match record.decode::<IssueOperatorEvent>() {
                Ok(IssueOperatorEvent::Operated {
                    bead,
                    session,
                    role,
                    skills,
                    hooks,
                }) => {
                    operators.insert(
                        bead,
                        json!({ "session": session, "role": role, "skills": skills, "hooks": hooks }),
                    );
                }
                Ok(IssueOperatorEvent::Cleared { bead }) => {
                    operators.remove(&bead);
                }
                Err(_) => {}
            }
        }
        operators
    }
}

impl OperatorResource for EventLogOperatorResource {
    fn operators_for(
        &self,
        workspace: Option<&str>,
        beads: &[String],
    ) -> HashMap<String, serde_json::Value> {
        let all = self.fold(workspace);
        beads
            .iter()
            .filter_map(|bead| all.get(bead).map(|op| (bead.clone(), op.clone())))
            .collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    fn append(log: &EventLog, ws: &str, ev: IssueOperatorEvent) {
        log.append(Some(ws), ev).unwrap();
    }

    #[test]
    fn folds_operated_then_cleared_into_the_current_operator() {
        let dir = TempDir::new().unwrap();
        let log = Arc::new(EventLog::new(Some(dir.path().to_path_buf())));
        append(
            &log,
            "acme",
            IssueOperatorEvent::Operated {
                bead: "hq-1".into(),
                session: "hq-hq-1".into(),
                role: "polecat".into(),
                skills: vec!["graphify".into()],
                hooks: vec!["Stop".into()],
            },
        );
        // A second bead operated, never cleared.
        append(
            &log,
            "acme",
            IssueOperatorEvent::Operated {
                bead: "hq-2".into(),
                session: "hq-hq-2".into(),
                role: "polecat".into(),
                skills: vec![],
                hooks: vec!["SessionStart".into()],
            },
        );
        // hq-1 merges → its operator clears.
        append(&log, "acme", IssueOperatorEvent::Cleared { bead: "hq-1".into() });

        let provider = EventLogOperatorResource::new(log);
        let ops = provider.operators_for(Some("acme"), &["hq-1".into(), "hq-2".into()]);

        assert!(!ops.contains_key("hq-1"), "cleared bead has no operator");
        let two = &ops["hq-2"];
        assert_eq!(two["session"], "hq-hq-2");
        assert_eq!(two["role"], "polecat");
        assert_eq!(two["hooks"][0], "SessionStart");
    }

    #[test]
    fn is_tenant_scoped_and_empty_for_unknown_beads() {
        let dir = TempDir::new().unwrap();
        let log = Arc::new(EventLog::new(Some(dir.path().to_path_buf())));
        append(
            &log,
            "acme",
            IssueOperatorEvent::Operated {
                bead: "hq-1".into(),
                session: "hq-hq-1".into(),
                role: "polecat".into(),
                skills: vec![],
                hooks: vec![],
            },
        );
        let provider = EventLogOperatorResource::new(log);
        // Another tenant never sees acme's operator (path-partitioned log).
        assert!(provider
            .operators_for(Some("beta"), &["hq-1".into()])
            .is_empty());
        // An unqueried bead is absent.
        assert!(provider
            .operators_for(Some("acme"), &["hq-9".into()])
            .is_empty());
    }
}
