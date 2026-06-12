//! The [`A2aHandler`] port (B1, gtcore-fbf90e / epic gtcore-155917).
//!
//! The seam between this contract crate and the cross-domain integration:
//! `gt-composition` (tier `modules`, docs/03 Rule 4) implements it against
//! `gt-issues` + `gt-runtime` + `gt-polecat`; tests implement it with fakes.
//! Same split as `gt-mcp-server`'s `DomainHandler`.

use std::pin::Pin;

use async_trait::async_trait;
use futures_core::Stream;

use crate::jsonrpc::JsonRpcError;
use crate::types::{Task, TaskArtifactUpdateEvent, TaskIdParams, TaskSendParams, TaskStatusUpdateEvent};

/// One item on a `tasks/sendSubscribe` stream.
#[derive(Clone, Debug, PartialEq)]
pub enum StreamEvent {
    Status(TaskStatusUpdateEvent),
    Artifact(TaskArtifactUpdateEvent),
}

impl StreamEvent {
    /// `true` ends the SSE stream (a terminal [`Status`](StreamEvent::Status)
    /// with `final: true`).
    pub fn is_final(&self) -> bool {
        matches!(self, StreamEvent::Status(s) if s.is_final)
    }
}

/// The subscribe seam: B3's observer plugin produces this from the
/// per-workspace event hub.
pub type EventStream = Pin<Box<dyn Stream<Item = StreamEvent> + Send>>;

/// Handler outcome. Wraps the wire error so adapters map 1:1 onto the JSON-RPC
/// error table without a second error vocabulary.
#[derive(Debug, thiserror::Error)]
#[error("{0:?}")]
pub struct A2aError(pub JsonRpcError);

impl From<JsonRpcError> for A2aError {
    fn from(e: JsonRpcError) -> Self {
        A2aError(e)
    }
}

/// The A2A operations gt serves. Implementations live up-tier.
#[async_trait]
pub trait A2aHandler: Send + Sync {
    /// `tasks/send` — create the bead and dispatch. Returns the task with the
    /// minted bead id as its canonical id, state `submitted`.
    async fn send(&self, params: TaskSendParams) -> Result<Task, A2aError>;

    /// `tasks/get` — project the bead + session state onto a [`Task`].
    async fn get(&self, params: TaskIdParams) -> Result<Task, A2aError>;

    /// `tasks/cancel` — kill the session, transition the bead to canceled.
    async fn cancel(&self, params: TaskIdParams) -> Result<Task, A2aError>;

    /// `tasks/sendSubscribe` — like [`send`](A2aHandler::send) but stream
    /// status/artifact updates until a final event. Default: unsupported, so
    /// B2's handler compiles before B3 wires the observer plugin; the Agent
    /// Card must keep `capabilities.streaming = false` until then.
    async fn send_subscribe(&self, params: TaskSendParams) -> Result<EventStream, A2aError> {
        let _ = params;
        Err(JsonRpcError::unsupported_operation("streaming is not enabled on this server").into())
    }
}
