//! [`HookRegistry`] — the per-[`HookPoint`] collection of registered handlers.
//!
//! `hq-mod-hooks.1` provides registration and lookup; `hq-mod-hooks.9` adds
//! [`dispatch`](HookRegistry::dispatch), which invokes the registered handlers
//! and honors a [`Reject`](crate::HookOutcome::Reject) veto at a vetoable point.
//! Wiring the registry into `Capability`/`RootBuilder` (so modules declare their
//! hooks declaratively) is `hq-mod-hooks.3`. The registry is built once at
//! composition time, then read-only while dispatch borrows it.

use std::collections::HashMap;

use crate::handler::{HookContext, HookHandler, HookOutcome};
use crate::points::HookPoint;

/// Collects [`HookHandler`]s keyed by the [`HookPoint`] they observe.
///
/// Handlers register against a point and are returned in registration order on
/// lookup, giving deterministic dispatch ([`dispatch`](HookRegistry::dispatch)
/// relies on this order).
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

    /// Run every handler registered at `ctx.point`, in registration order, and
    /// return the aggregate [`HookOutcome`].
    ///
    /// At a vetoable point ([`HookPoint::is_vetoable`] — only
    /// [`BeforeCommand`](HookPoint::BeforeCommand)) the first handler to
    /// [`Reject`](HookOutcome::Reject) short-circuits dispatch and its veto is
    /// returned, so no later handler runs and the in-flight operation is stopped
    /// before any effect. At an observe-only point every handler runs and a
    /// `Reject` is ignored — the point fires on a settled fact, nothing is left to
    /// veto. Returns [`Continue`](HookOutcome::Continue) when no handler vetoes.
    ///
    /// A handler that reacts to only one command matches on
    /// [`HookContext::command`] itself (set via
    /// [`with_command`](HookContext::with_command), `hq-mod-hooks.8`); the registry
    /// does not pre-filter by command. `async` because handlers live at the I/O
    /// edge — never call this from the sync replay core (non-negotiable #2).
    pub async fn dispatch(&self, ctx: &HookContext) -> HookOutcome {
        let vetoable = ctx.point.is_vetoable();
        for handler in self.handlers(ctx.point) {
            let outcome = handler.handle(ctx).await;
            if vetoable && matches!(outcome, HookOutcome::Reject(_)) {
                return outcome;
            }
        }
        HookOutcome::Continue
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
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::Arc;

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

    /// Counts its invocations and returns a fixed outcome, so a test can assert
    /// both the aggregate result and exactly how many handlers actually ran
    /// (proving short-circuit vs run-all).
    struct Recorder {
        name: &'static str,
        ran: Arc<AtomicUsize>,
        outcome: HookOutcome,
    }
    #[async_trait]
    impl HookHandler for Recorder {
        fn name(&self) -> &str {
            self.name
        }
        async fn handle(&self, _ctx: &HookContext) -> HookOutcome {
            self.ran.fetch_add(1, Ordering::SeqCst);
            self.outcome.clone()
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

    #[tokio::test]
    async fn dispatch_continues_when_no_handler_vetoes() {
        let mut reg = HookRegistry::new();
        reg.register(HookPoint::BeforeCommand, Box::new(Named("a")))
            .register(HookPoint::BeforeCommand, Box::new(Named("b")));
        let out = reg.dispatch(&HookContext::new(HookPoint::BeforeCommand)).await;
        assert_eq!(out, HookOutcome::Continue);
    }

    #[tokio::test]
    async fn dispatch_on_a_point_with_no_handlers_continues() {
        let reg = HookRegistry::new();
        let out = reg.dispatch(&HookContext::new(HookPoint::BeforeCommand)).await;
        assert_eq!(out, HookOutcome::Continue);
    }

    #[tokio::test]
    async fn dispatch_short_circuits_first_reject_at_vetoable_point() {
        let ran = Arc::new(AtomicUsize::new(0));
        let mut reg = HookRegistry::new();
        reg.register(
            HookPoint::BeforeCommand,
            Box::new(Recorder {
                name: "veto",
                ran: ran.clone(),
                outcome: HookOutcome::Reject("stop".into()),
            }),
        )
        .register(
            HookPoint::BeforeCommand,
            Box::new(Recorder {
                name: "after-veto",
                ran: ran.clone(),
                outcome: HookOutcome::Continue,
            }),
        );

        let out = reg.dispatch(&HookContext::new(HookPoint::BeforeCommand)).await;
        assert_eq!(out, HookOutcome::Reject("stop".into()));
        assert_eq!(
            ran.load(Ordering::SeqCst),
            1,
            "the handler after a veto must not run at a vetoable point"
        );
    }

    #[tokio::test]
    async fn dispatch_ignores_reject_at_observe_only_point() {
        let ran = Arc::new(AtomicUsize::new(0));
        let mut reg = HookRegistry::new();
        reg.register(
            HookPoint::AfterCommand,
            Box::new(Recorder {
                name: "obs-1",
                ran: ran.clone(),
                outcome: HookOutcome::Reject("ignored".into()),
            }),
        )
        .register(
            HookPoint::AfterCommand,
            Box::new(Recorder {
                name: "obs-2",
                ran: ran.clone(),
                outcome: HookOutcome::Continue,
            }),
        );

        let out = reg.dispatch(&HookContext::new(HookPoint::AfterCommand)).await;
        assert_eq!(
            out,
            HookOutcome::Continue,
            "a Reject at an observe-only point is not honored"
        );
        assert_eq!(
            ran.load(Ordering::SeqCst),
            2,
            "every observer runs at an observe-only point"
        );
    }

    /// The command-gated seam (`hq-mod-hooks.8` + this dispatcher): a handler
    /// registered once on `BeforeCommand` vetoes only its target command, reading
    /// [`HookContext::command`] itself — the registry does not pre-filter.
    #[tokio::test]
    async fn handler_self_filters_on_command_to_gate_one_command() {
        struct MergeGuard;
        #[async_trait]
        impl HookHandler for MergeGuard {
            fn name(&self) -> &str {
                "sheriff.premerge"
            }
            async fn handle(&self, ctx: &HookContext) -> HookOutcome {
                if ctx.command() == Some("merge.submit") {
                    HookOutcome::Reject("merge gated".into())
                } else {
                    HookOutcome::Continue
                }
            }
        }

        let mut reg = HookRegistry::new();
        reg.register(HookPoint::BeforeCommand, Box::new(MergeGuard));

        let gated = reg
            .dispatch(&HookContext::new(HookPoint::BeforeCommand).with_command("merge.submit"))
            .await;
        assert_eq!(gated, HookOutcome::Reject("merge gated".into()));

        let passed = reg
            .dispatch(&HookContext::new(HookPoint::BeforeCommand).with_command("bead.claim"))
            .await;
        assert_eq!(passed, HookOutcome::Continue);
    }
}
