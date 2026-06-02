//! Dog worker abstraction.
//!
//! Closes the loop that `gt-plugin/src/descriptor.rs:7` deferred: a Dog claims a
//! Plugin, evaluates its Gate, executes per ExecutionType, emits a digest
//! receipt.
//!
//! ## Landed so far
//!
//! - [`Dog`] + [`DogReport`] — the async worker trait and its outcome
//!   (`hq-mod-dogs.1`).
//! - [`DogId`] — the validated worker identity (`hq-mod-dogs.1`).
//! - [`DogState`] + [`DogStatus`] — the per-worker lifecycle projection and its
//!   reducer primitives (`hq-mod-dogs.1`).
//! - [`DogEvent`] + [`DogDispatcher`] — the lifecycle event sum type and the
//!   pool that matches ready claims to idle Dogs within a capacity budget
//!   (`hq-mod-dogs.2`).
//! - [`ExecutionType`] + [`PluginExecutor`] + [`ExecBackend`] — the execution
//!   strategy a claim runs under and the executor that validates a request and
//!   dispatches it to a backend (`hq-mod-dogs.4`).
//! - [`Digest`] + [`TrackingLabels`] + [`ReceiptSink`] — the receipt a run
//!   leaves and the port it is injected into (`hq-mod-dogs.5`).
//! - [`Notification`] + [`Notifier`] + [`notify_failure`] — surfacing a failed
//!   run to an operator via the `gt-notify` port (`hq-mod-dogs.6`).
//!
//! - [`mcp`] — the `dog.claim.{validate,execute}`, `dog.release.execute`,
//!   `dog.status.read` MCP tools and their payload parsing (`hq-mod-dogs.7`).
//! - [`evaluate_gate`] + [`GateContext`] + [`GateDecision`] — the pure Gate
//!   evaluator deciding whether a claim may fire now, across all five gate
//!   types (`hq-mod-dogs.3`).
//!
//! Still to come on this epic: the per-workspace pool (`.8`) and the end-to-end
//! test (`.9`).

#![forbid(unsafe_code)]
#![warn(missing_docs)]

mod dispatcher;
mod dog;
mod executor;
mod gate;
pub mod mcp;
mod notify;
mod pool;
mod state;
mod tracking;

pub use dispatcher::{Dispatch, DispatchError, DogDispatcher, DogEvent};
pub use pool::WorkspaceDogPool;
pub use dog::{Dog, DogReport};
pub use executor::{ExecBackend, ExecError, ExecutionKind, ExecutionType, PluginExecutor};
pub use gate::{evaluate as evaluate_gate, GateContext, GateDecision, GateError};
pub use notify::{notify_failure, InMemoryNotifier, Notification, NotifyError, Notifier};
pub use tracking::{
    Digest, InMemoryReceipts, Outcome, ReceiptError, ReceiptSink, TrackingLabels,
};
pub use state::{DogError, DogId, DogIdError, DogState, DogStatus};
