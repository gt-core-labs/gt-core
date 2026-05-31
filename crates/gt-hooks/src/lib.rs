//! Lifecycle hooks framework.
//!
//! Modules declare `Capability::hooks` to subscribe to lifecycle points without bus knowledge.
//! Real implementation lands in `hq-mod-hooks.1` through `.3`.

#![forbid(unsafe_code)]
#![warn(missing_docs)]

#[doc(hidden)]
pub const SCAFFOLD: &str = "gt-hooks — hq-mod-hooks scaffold";
