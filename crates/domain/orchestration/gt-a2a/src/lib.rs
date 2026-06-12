//! `gt-a2a` — the A2A (Agent2Agent) v1.0 protocol contract (plans/a2a-checklist.md B1).
//!
//! A2A is the Linux Foundation standard for agent-to-agent task delegation:
//! JSON-RPC 2.0 over HTTP plus SSE streaming, with capability discovery via a
//! signed Agent Card at `/.well-known/agent.json`. This crate makes gt-core a
//! **native** A2A server — no wrapper process — replacing the bespoke `.event`
//! file dispatch as the standard ingress for task delegation.
//!
//! ## Contract here, integration up-tier
//!
//! This crate owns only the protocol surface: the spec object types
//! ([`AgentCard`], [`Task`], [`Message`], …), the JSON-RPC envelope, the SSE
//! framing, the axum router (off-by-default `axum` feature), and the
//! [`A2aHandler`](handler::A2aHandler) port. The implementation that creates
//! beads (`gt-issues`), dispatches polecats (`gt-runtime`) and kills sessions
//! (`gt-polecat`) lives in `gt-composition` — only the `modules` tier may name
//! all `domain/*` crates together (docs/03 Rule 4), the same split
//! `gt-mcp-server` makes with its `DomainHandler` seam.
//!
//! ## Task identity and state
//!
//! The bead id **is** the A2A task id — no mapping table. Task state is a
//! projection of the existing bead/session events ([`TaskState`] documents the
//! mapping), so the event log stays the single source of truth.

#![forbid(unsafe_code)]

pub mod handler;
mod jsonrpc;
mod sse;
mod types;

#[cfg(feature = "axum")]
pub mod http;

pub use handler::{A2aError, A2aHandler, EventStream, StreamEvent};
pub use jsonrpc::{JsonRpcError, JsonRpcRequest, JsonRpcResponse, JSONRPC_VERSION};
pub use sse::sse_frame;
pub use types::{
    AgentCapabilities, AgentCard, AgentProvider, AgentSkill, Artifact, Message, Part, Role, Task,
    TaskArtifactUpdateEvent, TaskIdParams, TaskSendParams, TaskState, TaskStatus,
    TaskStatusUpdateEvent,
};

#[cfg(feature = "axum")]
pub use http::a2a_router;
