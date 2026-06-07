//! Read-side seam for the agent operating a bead (`hq-agent-observability.3`).
//!
//! A bead in `working` is being driven by exactly one agent. Which agent — and what it has loaded
//! (skills/hooks) — is ephemeral runtime state, emitted by the polecat supervisor as
//! `issues.operated.v1` / `issues.operator-cleared.v1` (`.2`); it lives in the event log, **not**
//! in the Dolt row. So the issues REST surface stays infra-free and takes a provider rather than
//! reading the log itself: the composition root backs this with an event-log-folding
//! implementation, exactly mirroring the write-side [`IssueEventSink`](crate::events::IssueEventSink).
//!
//! The provider returns each bead's operator as opaque JSON (`{session,role,skills,hooks}`), which
//! the handlers inline as an `operated_by` field on the served row via [`attach_operated_by`] — the
//! same additive overlay pattern the MCP `gt://issue/{id}` resource uses for its `documents` array.

use std::collections::HashMap;

/// Resolves the agent currently operating each bead, for the `operated_by` overlay.
///
/// One query per served read (the handler passes every row id at once) so the implementation can
/// fold the event log a single time. A bead with no live operator is simply absent from the map —
/// the overlay then omits `operated_by` for that row.
pub trait OperatorResource: Send + Sync {
    /// The operator JSON for each of `beads` that currently has one. Keyed by bead id; absent
    /// keys mean "no operator". `workspace` scopes the lookup to the request's tenant.
    fn operators_for(
        &self,
        workspace: Option<&str>,
        beads: &[String],
    ) -> HashMap<String, serde_json::Value>;
}

/// Inline `operated_by` onto a single serialized issue row (`value`), reading its `id` and looking
/// it up in `operators`. A no-op when the row carries no operator (the field is simply omitted, so
/// a client treats presence as "an agent is on it"). Safe on any JSON: a non-object value or a row
/// without a string `id` is left untouched.
pub fn attach_operated_by(
    value: &mut serde_json::Value,
    operators: &HashMap<String, serde_json::Value>,
) {
    let Some(obj) = value.as_object_mut() else { return };
    let Some(id) = obj.get("id").and_then(|v| v.as_str()) else {
        return;
    };
    if let Some(op) = operators.get(id) {
        obj.insert("operated_by".into(), op.clone());
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn attaches_operator_when_present_for_the_rows_id() {
        let mut row = json!({ "id": "hq-1", "status": "working" });
        let ops = HashMap::from([(
            "hq-1".to_string(),
            json!({ "session": "hq-hq-1", "role": "polecat", "skills": ["graphify"], "hooks": ["Stop"] }),
        )]);
        attach_operated_by(&mut row, &ops);
        assert_eq!(row["operated_by"]["session"], "hq-hq-1");
        assert_eq!(row["operated_by"]["skills"][0], "graphify");
    }

    #[test]
    fn omits_operator_when_absent() {
        let mut row = json!({ "id": "hq-2", "status": "open" });
        attach_operated_by(&mut row, &HashMap::new());
        assert!(row.get("operated_by").is_none(), "no operator ⇒ no field");
    }

    #[test]
    fn ignores_non_object_or_idless_values() {
        let mut arr = json!(["not", "an", "object"]);
        attach_operated_by(&mut arr, &HashMap::new());
        assert!(arr.is_array());
        let mut idless = json!({ "title": "no id here" });
        let ops = HashMap::from([("x".to_string(), json!({}))]);
        attach_operated_by(&mut idless, &ops);
        assert!(idless.get("operated_by").is_none());
    }
}
