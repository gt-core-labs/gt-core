//! [`HookRegistry`] — the per-[`HookPoint`] collection of registered handlers.
//!
//! `hq-mod-hooks.1` provides registration and lookup. Wiring the registry into
//! `Capability`/`RootBuilder` (so modules declare their hooks declaratively) is
//! `hq-mod-hooks.3`; actually invoking handlers and honoring a veto is the
//! dispatcher in that same bead. This type holds only handlers and is built once
//! at composition time, then read-only during dispatch.

use std::collections::HashMap;

use crate::handler::HookHandler;
use crate::points::HookPoint;

/// Collects [`HookHandler`]s keyed by the [`HookPoint`] they observe.
///
/// Handlers register against a point and are returned in registration order on
/// lookup, giving deterministic dispatch (`hq-mod-hooks.3` relies on this order).
/// Stores boxed trait objects — the sanctioned observer-plugin use of `dyn` (see
/// [`crate::HookHandler`] docs for the NN#1 reconciliation).
#[derive(Default)]
pub struct HookRegistry {
    by_point: HashMap<HookPoint, Vec<Box<dyn HookHandler>>>,
}

impl HookRegistry {
    /// Start an empty registry.
    pub fn new() -> Self {
        HookRegistry::default()
    }

    /// Register `handler` to observe `point`. Chainable. Handlers are kept in
    /// registration order; the same handler type may register more than once and
    /// at more than one point.
    pub fn register(&mut self, point: HookPoint, handler: Box<dyn HookHandler>) -> &mut Self {
        self.by_point.entry(point).or_default().push(handler);
        self
    }

    /// The handlers registered for `point`, in registration order. Empty slice if
    /// none — callers never special-case a missing point.
    pub fn handlers(&self, point: HookPoint) -> &[Box<dyn HookHandler>] {
        self.by_point.get(&point).map(Vec::as_slice).unwrap_or(&[])
    }

    /// Total number of registered handlers across every point.
    pub fn len(&self) -> usize {
        self.by_point.values().map(Vec::len).sum()
    }

    /// Whether no handler is registered at any point.
    pub fn is_empty(&self) -> bool {
        self.by_point.values().all(Vec::is_empty)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::handler::{HookContext, HookOutcome};
    use async_trait::async_trait;

    struct Named(&'static str);
    #[async_trait]
    impl HookHandler for Named {
        fn name(&self) -> &str {
            self.0
        }
        async fn handle(&self, _ctx: &HookContext) -> HookOutcome {
            HookOutcome::Continue
        }
    }

    #[test]
    fn empty_registry_returns_empty_slices() {
        let reg = HookRegistry::new();
        assert!(reg.is_empty());
        assert_eq!(reg.len(), 0);
        assert!(reg.handlers(HookPoint::BeforeCommand).is_empty());
    }

    #[test]
    fn handlers_returned_in_registration_order_per_point() {
        let mut reg = HookRegistry::new();
        reg.register(HookPoint::BeforeCommand, Box::new(Named("first")))
            .register(HookPoint::BeforeCommand, Box::new(Named("second")))
            .register(HookPoint::AfterCommand, Box::new(Named("after")));

        let before: Vec<&str> = reg
            .handlers(HookPoint::BeforeCommand)
            .iter()
            .map(|h| h.name())
            .collect();
        assert_eq!(before, ["first", "second"]);

        let after: Vec<&str> = reg
            .handlers(HookPoint::AfterCommand)
            .iter()
            .map(|h| h.name())
            .collect();
        assert_eq!(after, ["after"]);

        assert_eq!(reg.len(), 3);
        assert!(!reg.is_empty());
    }

    #[tokio::test]
    async fn registered_handler_is_invocable_through_the_registry() {
        let mut reg = HookRegistry::new();
        reg.register(HookPoint::AfterCommand, Box::new(Named("obs")));
        let h = &reg.handlers(HookPoint::AfterCommand)[0];
        let out = h.handle(&HookContext::new(HookPoint::AfterCommand)).await;
        assert_eq!(out, HookOutcome::Continue);
    }
}
