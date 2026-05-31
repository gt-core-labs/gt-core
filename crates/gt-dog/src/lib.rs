//! Dog worker abstraction.
//!
//! Closes the loop that `gt-plugin/src/descriptor.rs:7` deferred: a Dog claims a Plugin,
//! evaluates its Gate, executes per ExecutionType, emits a digest receipt.
//!
//! Real impl lands in `hq-mod-dogs.1` through `.9`.

#![forbid(unsafe_code)]
#![warn(missing_docs)]

#[doc(hidden)]
pub const SCAFFOLD: &str = "gt-dog — hq-mod-dogs scaffold";
