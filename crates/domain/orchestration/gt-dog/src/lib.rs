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
//!
//! Still to come on this epic: the `Gate` evaluator (`.3`), failure notify
//! (`.6`), MCP claim tools (`.7`), the per-workspace pool (`.8`), and the
//! end-to-end test (`.9`).

#![forbid(unsafe_code)]
#![warn(missing_docs)]

mod dispatcher;
mod dog;
mod executor;
mod state;
mod tracking;

pub use dispatcher::{Dispatch, DispatchError, DogDispatcher, DogEvent};
pub use dog::{Dog, DogReport};
pub use executor::{ExecBackend, ExecError, ExecutionKind, ExecutionType, PluginExecutor};
pub use tracking::{
    Digest, InMemoryReceipts, Outcome, ReceiptError, ReceiptSink, TrackingLabels,
};
pub use state::{DogError, DogId, DogIdError, DogState, DogStatus};
