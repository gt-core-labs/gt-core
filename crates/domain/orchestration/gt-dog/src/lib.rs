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
//!
//! Still to come on this epic: the `Gate` evaluator (`.3`), the
//! `PluginExecutor` (`.4`), digest/tracking (`.5`), failure notify (`.6`), MCP
//! claim tools (`.7`), the per-workspace pool (`.8`), and the end-to-end test
//! (`.9`).

#![forbid(unsafe_code)]
#![warn(missing_docs)]

mod dispatcher;
mod dog;
mod state;

pub use dispatcher::{Dispatch, DispatchError, DogDispatcher, DogEvent};
pub use dog::{Dog, DogReport};
pub use state::{DogError, DogId, DogIdError, DogState, DogStatus};
