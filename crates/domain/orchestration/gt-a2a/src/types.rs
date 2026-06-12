//! A2A v1.0 spec objects (B1, gtcore-fbf90e / epic gtcore-155917).
//!
//! Native serde types for the subset of the spec gt-core speaks — no `a2a-rs`
//! dependency: the surface is ~10 stable structs and an external crate would
//! cost more in version churn than it saves. Field names follow the spec's
//! camelCase JSON exactly; the conformance tests at the bottom round-trip
//! spec-shaped documents to pin that.

use serde::{Deserialize, Serialize};
use serde_json::Value;

/// The discovery document served at `/.well-known/agent.json`.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct AgentCard {
    pub name: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub description: Option<String>,
    /// Base URL of the A2A endpoint (the JSON-RPC POST target).
    pub url: String,
    pub version: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub provider: Option<AgentProvider>,
    pub capabilities: AgentCapabilities,
    /// Supported auth schemes, e.g. `["bearer"]`. The composition root guards
    /// the routes with the same PAT authenticator as the MCP surface.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub authentication: Option<AgentAuthentication>,
    #[serde(default = "text_modes")]
    pub default_input_modes: Vec<String>,
    #[serde(default = "text_modes")]
    pub default_output_modes: Vec<String>,
    /// One skill per dispatchable rig — hydrated from the rig catalog up-tier.
    pub skills: Vec<AgentSkill>,
    /// Compact RS256 JWS over the card's canonical JSON **without** this field
    /// (B5, gtcore-9039b5). The composition root mints it with the platform's
    /// existing RS256 signing key (`gt-auth`), so a client verifies the card
    /// against the same public key/JWKS that verifies the platform's bearer
    /// tokens. Optional: an unsigned card (no signing key configured) simply
    /// omits the field, and pre-B5 documents deserialize unchanged.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub signature: Option<String>,
}

fn text_modes() -> Vec<String> {
    vec!["text".into()]
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct AgentProvider {
    pub organization: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub url: Option<String>,
}

#[derive(Clone, Debug, Default, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct AgentCapabilities {
    #[serde(default)]
    pub streaming: bool,
    #[serde(default)]
    pub push_notifications: bool,
    #[serde(default)]
    pub state_transition_history: bool,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct AgentAuthentication {
    pub schemes: Vec<String>,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct AgentSkill {
    pub id: String,
    pub name: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub description: Option<String>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub tags: Vec<String>,
}

/// A2A task lifecycle states and their bead/session projection:
///
/// | A2A              | gt                                          |
/// |------------------|---------------------------------------------|
/// | `submitted`      | bead created/open, polecat not yet running   |
/// | `working`        | session running                              |
/// | `input-required` | session waiting on operator input            |
/// | `completed`      | bead closed (delivered/merged)               |
/// | `failed`         | merge failed / polecat died terminally       |
/// | `canceled`       | `tasks/cancel` killed the session            |
#[derive(Clone, Copy, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "kebab-case")]
pub enum TaskState {
    Submitted,
    Working,
    InputRequired,
    Completed,
    Failed,
    Canceled,
    Unknown,
}

impl TaskState {
    /// Project a tracker bead status onto the task state. Only the three
    /// tracker statuses exist (`open`/`working`/`closed`); `failed` and
    /// `canceled` are session/merge facts the caller layers on top.
    pub fn from_bead_status(status: &str) -> TaskState {
        match status {
            "open" => TaskState::Submitted,
            "working" => TaskState::Working,
            "closed" => TaskState::Completed,
            _ => TaskState::Unknown,
        }
    }

    /// Terminal states end the SSE stream (`final: true` on the last event).
    pub fn is_terminal(self) -> bool {
        matches!(self, TaskState::Completed | TaskState::Failed | TaskState::Canceled)
    }
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct TaskStatus {
    pub state: TaskState,
    /// RFC 3339. A `String` so the contract crate stays clock-free — the
    /// composition root stamps it.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub timestamp: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub message: Option<Message>,
}

/// The task object — `id` is the bead id verbatim.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct Task {
    pub id: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub session_id: Option<String>,
    pub status: TaskStatus,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub artifacts: Vec<Artifact>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub history: Vec<Message>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub metadata: Option<Value>,
}

#[derive(Clone, Copy, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "lowercase")]
pub enum Role {
    User,
    Agent,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct Message {
    pub role: Role,
    pub parts: Vec<Part>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub metadata: Option<Value>,
}

/// Message/artifact content. gt speaks `text` today; the enum is `#[non_exhaustive]`
/// in spirit — `data`/`file` parts deserialize once a variant is added, and
/// unknown kinds fail loudly rather than silently dropping content.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
#[serde(tag = "type", rename_all = "lowercase")]
pub enum Part {
    Text {
        text: String,
        #[serde(skip_serializing_if = "Option::is_none")]
        metadata: Option<Value>,
    },
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct Artifact {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub name: Option<String>,
    pub parts: Vec<Part>,
    #[serde(default)]
    pub index: u32,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub append: Option<bool>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub last_chunk: Option<bool>,
}

/// Params of `tasks/send` and `tasks/sendSubscribe`. The client mints the task
/// id; for gt the handler answers with the minted **bead id** as the canonical
/// id (the spec allows the server to be authoritative).
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct TaskSendParams {
    pub id: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub session_id: Option<String>,
    pub message: Message,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub history_length: Option<u32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub metadata: Option<Value>,
}

/// Params of `tasks/get` and `tasks/cancel`.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct TaskIdParams {
    pub id: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub history_length: Option<u32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub metadata: Option<Value>,
}

/// SSE event: a task changed state. `final: true` closes the stream.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct TaskStatusUpdateEvent {
    pub id: String,
    pub status: TaskStatus,
    #[serde(rename = "final", default)]
    pub is_final: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub metadata: Option<Value>,
}

/// SSE event: the task produced (a chunk of) an artifact.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct TaskArtifactUpdateEvent {
    pub id: String,
    pub artifact: Artifact,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub metadata: Option<Value>,
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Round-trip a spec-shaped document and require byte-stable JSON keys —
    /// the conformance pin for B1, gtcore-fbf90e / epic gtcore-155917.
    fn round_trip<T: Serialize + for<'de> Deserialize<'de> + PartialEq + std::fmt::Debug>(
        json: &str,
    ) -> T {
        let v: T = serde_json::from_str(json).expect("deserialize spec example");
        let back = serde_json::to_value(&v).expect("serialize");
        let orig: Value = serde_json::from_str(json).unwrap();
        assert_eq!(back, orig, "re-serialization must match the spec document");
        v
    }

    #[test]
    fn agent_card_spec_example() {
        let card: AgentCard = round_trip(
            r#"{
              "name": "gt",
              "description": "gt platform orchestrator",
              "url": "https://gt-dev.codecsrayo.com/a2a",
              "version": "0.1.0",
              "provider": {"organization": "gt-core-labs"},
              "capabilities": {"streaming": true, "pushNotifications": false, "stateTransitionHistory": false},
              "authentication": {"schemes": ["bearer"]},
              "defaultInputModes": ["text"],
              "defaultOutputModes": ["text"],
              "skills": [
                {"id": "gtcore", "name": "gt-core", "description": "Rust MCP server / platform", "tags": ["rust"]}
              ]
            }"#,
        );
        assert!(card.capabilities.streaming);
        assert_eq!(card.skills[0].id, "gtcore");
        // A document without a `signature` (pre-B5 / unsigned deploy) stays
        // byte-stable: the optional field must not appear on re-serialization.
        assert_eq!(card.signature, None);
    }

    #[test]
    fn agent_card_signature_round_trips_when_present() {
        let card: AgentCard = round_trip(
            r#"{
              "name": "gt",
              "url": "https://gt-dev.codecsrayo.com/a2a",
              "version": "0.1.0",
              "capabilities": {"streaming": true, "pushNotifications": false, "stateTransitionHistory": false},
              "defaultInputModes": ["text"],
              "defaultOutputModes": ["text"],
              "skills": [],
              "signature": "eyJhbGciOiJSUzI1NiJ9.e30.sig"
            }"#,
        );
        assert_eq!(card.signature.as_deref(), Some("eyJhbGciOiJSUzI1NiJ9.e30.sig"));
    }

    #[test]
    fn task_spec_example() {
        let task: Task = round_trip(
            r#"{
              "id": "hq-abc123",
              "sessionId": "gtcore-hq-abc123",
              "status": {"state": "working", "timestamp": "2026-06-12T00:00:00Z"},
              "artifacts": [{"parts": [{"type": "text", "text": "partial"}], "index": 0}],
              "history": [{"role": "user", "parts": [{"type": "text", "text": "do the thing"}]}]
            }"#,
        );
        assert_eq!(task.status.state, TaskState::Working);
        assert_eq!(task.id, "hq-abc123");
    }

    #[test]
    fn send_params_spec_example() {
        let p: TaskSendParams = round_trip(
            r#"{
              "id": "client-minted-1",
              "message": {"role": "user", "parts": [{"type": "text", "text": "fix the bug"}]}
            }"#,
        );
        assert_eq!(p.message.role, Role::User);
    }

    #[test]
    fn status_update_event_final_field_name() {
        let ev: TaskStatusUpdateEvent = round_trip(
            r#"{"id": "hq-x", "status": {"state": "completed"}, "final": true}"#,
        );
        assert!(ev.is_final);
        assert!(ev.status.state.is_terminal());
    }

    #[test]
    fn task_state_wire_names_are_kebab_case() {
        assert_eq!(serde_json::to_string(&TaskState::InputRequired).unwrap(), "\"input-required\"");
        assert_eq!(serde_json::to_string(&TaskState::Submitted).unwrap(), "\"submitted\"");
    }

    #[test]
    fn bead_status_projection() {
        assert_eq!(TaskState::from_bead_status("open"), TaskState::Submitted);
        assert_eq!(TaskState::from_bead_status("working"), TaskState::Working);
        assert_eq!(TaskState::from_bead_status("closed"), TaskState::Completed);
        assert_eq!(TaskState::from_bead_status("???"), TaskState::Unknown);
    }
}
