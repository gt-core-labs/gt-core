//! Issue-mutation events for the per-workspace SSE feed (`hq-issues-sse`).
//!
//! The issues tracker is Dolt-backed, not event-sourced, so its mutations never
//! reached the per-workspace event log the `GET /stream` SSE feed fans out — a
//! frontend therefore never saw the tracker *move* without polling. This module is
//! the missing emission: every successful issue mutation (over REST **or** MCP)
//! publishes a versioned [`IssueEvent`] into that same log, so the existing feed
//! carries it with no new transport.
//!
//! ## Shape
//!
//! - [`IssueVerb`] names the mutation and selects the event `kind` (`issues.<verb>.v1`).
//!   The SSE frame is named by that kind, so a browser subscribes with `?channel=issues`
//!   (the feed's channel filter matches the `issues.` prefix).
//! - [`IssueEvent`] carries the **full changed row** (`issue`) so a client patches in
//!   place instead of re-fetching the list on every event.
//! - [`IssueEventSink`] is the seam: the domain stays infra-free, and the composition
//!   root supplies the event-log-backed implementation that feeds the SSE feed.
//!
//! ## Extensibility
//!
//! More mutations will emit over time. A new event is a new [`IssueVerb`] variant plus
//! its `kind` string — nothing in the sink, the feed, or the call sites changes: they
//! all route on the opaque `IssueEvent` + its `EventKind::kind`.

use gt_events::EventKind;
use gt_store_dolt::DoltIssues;
use serde::Serialize;

use crate::resources::read_issue;

/// The mutation an [`IssueEvent`] reports. Each maps to a versioned event `kind`
/// (`issues.<verb>.v1`) the SSE feed names the frame by.
///
/// Extensible by design: a new mutation is a new variant plus its [`kind`](Self::kind)
/// — the sink and the feed route on the opaque kind, so neither changes.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum IssueVerb {
    /// A bead was created (`issues.created.v1`).
    Created,
    /// Editable fields were patched (`issues.updated.v1`).
    Updated,
    /// The status moved across the open/working/closed machine (`issues.transitioned.v1`).
    Transitioned,
    /// The bead was closed (`issues.closed.v1`).
    Closed,
    /// The bead was atomically claimed (`issues.claimed.v1`).
    Claimed,
}

impl IssueVerb {
    /// The versioned event kind this verb emits (`issues.<verb>.v1`). The `issues.`
    /// prefix is what the SSE feed's `?channel=issues` filter matches.
    pub const fn kind(self) -> &'static str {
        match self {
            IssueVerb::Created => "issues.created.v1",
            IssueVerb::Updated => "issues.updated.v1",
            IssueVerb::Transitioned => "issues.transitioned.v1",
            IssueVerb::Closed => "issues.closed.v1",
            IssueVerb::Claimed => "issues.claimed.v1",
        }
    }
}

/// One issue mutation, broadcast to the per-workspace SSE feed so a frontend sees the
/// tracker move without polling (`hq-issues-sse`).
///
/// The payload carries the full row after the mutation (`issue`) so a client patches
/// it in place rather than re-fetching; `version` lives inside that row. The
/// serialized form is exactly the SSE frame's `data:`.
#[derive(Debug, Clone, Serialize)]
pub struct IssueEvent {
    /// Which mutation happened — also selects the event `kind` (see [`IssueVerb::kind`]).
    pub verb: IssueVerb,
    /// The bead id that changed.
    pub id: String,
    /// The server-attributed actor that drove the mutation.
    pub actor: String,
    /// Rig prefix derived from the bead id (`<prefix>-<slug>`). Carried in the payload
    /// so the SSE feed's `?rig=` filter can narrow delivery to one rig's events.
    pub rig: String,
    /// The full row after the mutation, when it could be read back. `None` only on a
    /// best-effort read failure — the event still fires so the client knows to refresh.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub issue: Option<serde_json::Value>,
}

impl EventKind for IssueEvent {
    fn kind(&self) -> &'static str {
        self.verb.kind()
    }
}

/// The sink an issue mutation publishes its [`IssueEvent`] to (`hq-issues-sse`).
///
/// The seam keeps the domain infra-free: this trait lives here, and the composition
/// root supplies the event-log-backed implementation that feeds the SSE feed.
/// Emission is **best-effort** — a mutation has already succeeded by the time it
/// emits, so an implementation must never let a feed failure surface to the caller;
/// `emit` therefore returns nothing and swallows transport errors.
pub trait IssueEventSink: Send + Sync {
    /// Publish `event` to `workspace`'s feed. Infallible by contract — swallow any
    /// transport error inside the implementation.
    fn emit(&self, workspace: Option<&str>, event: &IssueEvent);
}

/// Read the mutated row back and publish an [`IssueEvent`] to `sink` (`hq-issues-sse`).
///
/// A no-op when no sink is wired. Best-effort throughout: a read failure still emits
/// the event (without the row) and is never propagated, so the mutation that already
/// succeeded is never undone by a feed problem. Shared by the REST ([`crate::http`])
/// and MCP (`gt-mcp-server` dispatch) mutation paths so both transports emit the
/// identical event — the parity guarantee that makes the SSE feed faithful regardless
/// of which surface drove the change.
pub async fn emit_issue_event(
    sink: Option<&dyn IssueEventSink>,
    store: &DoltIssues,
    workspace: Option<&str>,
    verb: IssueVerb,
    id: &str,
    actor: &str,
) {
    let Some(sink) = sink else { return };
    // Best-effort read-back of the full row; a failure leaves `issue` None rather than
    // skipping the event (the client still learns the bead changed and can refresh).
    let issue = read_issue(store, id)
        .await
        .ok()
        .flatten()
        .and_then(|detail| serde_json::to_value(detail).ok());
    let rig = id.split('-').next().unwrap_or("").to_string();
    sink.emit(
        workspace,
        &IssueEvent {
            verb,
            id: id.to_string(),
            actor: actor.to_string(),
            rig,
            issue,
        },
    );
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn each_verb_maps_to_a_versioned_issues_kind() {
        // The SSE `?channel=issues` filter matches the `issues.` prefix, and the frame
        // is named by the kind — so every verb must produce a versioned `issues.*` name.
        for verb in [
            IssueVerb::Created,
            IssueVerb::Updated,
            IssueVerb::Transitioned,
            IssueVerb::Closed,
            IssueVerb::Claimed,
        ] {
            let kind = verb.kind();
            assert!(
                kind.starts_with("issues."),
                "{kind} is not channel-routable"
            );
            assert!(kind.ends_with(".v1"), "{kind} is not version-tagged");
        }
    }

    #[test]
    fn event_kind_follows_the_verb() {
        let ev = IssueEvent {
            verb: IssueVerb::Transitioned,
            id: "hq-x.1".into(),
            actor: "mcp-local".into(),
            rig: "hq".into(),
            issue: None,
        };
        assert_eq!(ev.kind(), "issues.transitioned.v1");
    }

    #[test]
    fn serialized_payload_carries_id_actor_rig_and_row_and_omits_absent_row() {
        // The serialized form IS the SSE frame data; a missing row must be omitted, not
        // emitted as `null`, so a client can treat presence as "patch in place".
        let ev = IssueEvent {
            verb: IssueVerb::Claimed,
            id: "hq-x.1".into(),
            actor: "alice".into(),
            rig: "hq".into(),
            issue: None,
        };
        let v = serde_json::to_value(&ev).unwrap();
        assert_eq!(v["verb"], "claimed");
        assert_eq!(v["id"], "hq-x.1");
        assert_eq!(v["actor"], "alice");
        assert_eq!(v["rig"], "hq");
        assert!(v.get("issue").is_none(), "absent row must be omitted");

        let ev = IssueEvent {
            issue: Some(serde_json::json!({ "id": "hq-x.1", "version": 3 })),
            ..ev
        };
        let v = serde_json::to_value(&ev).unwrap();
        assert_eq!(v["issue"]["version"], 3);
    }
}
