//! [`HookHandler`] — the observer a module registers against a [`HookPoint`].
//!
//! ## NN#1 reconciliation
//!
//! Non-negotiable #1 confines `dyn` + `#[async_trait]` to `gt-plugin`. Hooks are
//! the observer-plugin surface that rule's exception names: docs/03 guardrail
//! states plugins are declared in `Capability::hooks`, and a hook handler is a
//! heterogeneous, side-effecting observer dispatched at runtime — exactly the
//! case `dyn` + `#[async_trait]` exist for. `gt-hooks` therefore owns the handler
//! *trait* and its registry; the relay + dead-letter machinery still lives in
//! `gt-plugin` and migrates up in Phase 4. Handlers run at the edge (before/after
//! a command), never inside the sync replay core, so NN#2 is preserved.

use async_trait::async_trait;

use crate::points::HookPoint;

/// What a [`HookHandler`] reports back after observing a [`HookPoint`].
///
/// `Continue` lets the operation proceed; `Reject` vetoes it with a reason. A
/// veto is only honored at a vetoable point (see [`HookPoint::is_vetoable`]); the
/// dispatcher that enforces this is wired in `hq-mod-hooks.3`. `#[non_exhaustive]`
/// so later beads can add outcomes (e.g. `Defer`) without breaking matches.
#[derive(Clone, Debug, PartialEq, Eq)]
#[non_exhaustive]
pub enum HookOutcome {
    /// Proceed with the operation.
    Continue,
    /// Veto the operation, carrying a human-readable reason.
    Reject(String),
}

/// Context handed to a handler when a hook point fires.
///
/// `.1` carries only the firing [`HookPoint`]; richer payload (the command name,
/// envelope, actor scope) is added by `hq-mod-hooks.2`/`.3` as the builtin points
/// and dispatcher land. `#[non_exhaustive]` keeps that growth additive.
#[derive(Clone, Debug, PartialEq, Eq)]
#[non_exhaustive]
pub struct HookContext {
    /// The lifecycle point that fired this invocation.
    pub point: HookPoint,
}

impl HookContext {
    /// Build a context for `point`. Future fields gain dedicated setters as the
    /// payload grows, so this constructor stays stable.
    pub fn new(point: HookPoint) -> Self {
        HookContext { point }
    }
}

/// An observer a module registers to react to a lifecycle [`HookPoint`].
///
/// Implementors are values held in the [`HookRegistry`](crate::HookRegistry) as
/// trait objects (see the NN#1 reconciliation in this module's docs). `name`
/// gives a stable id for diagnostics and ordering; `handle` performs the
/// reaction and reports a [`HookOutcome`]. It is `async` because handlers live at
/// the I/O edge (notify, sync, watchdog checks).
#[async_trait]
pub trait HookHandler: Send + Sync {
    /// Stable identifier for this handler, used in diagnostics and dispatch
    /// ordering. Conventionally `<module-id>.<purpose>` (e.g. `sheriff.premerge`).
    fn name(&self) -> &str;

    /// React to a fired hook point and report whether the operation may proceed.
    async fn handle(&self, ctx: &HookContext) -> HookOutcome;
}

#[cfg(test)]
mod tests {
    use super::*;

    struct Noop;
    #[async_trait]
    impl HookHandler for Noop {
        fn name(&self) -> &str {
            "test.noop"
        }
        async fn handle(&self, _ctx: &HookContext) -> HookOutcome {
            HookOutcome::Continue
        }
    }

    struct Veto;
    #[async_trait]
    impl HookHandler for Veto {
        fn name(&self) -> &str {
            "test.veto"
        }
        async fn handle(&self, ctx: &HookContext) -> HookOutcome {
            HookOutcome::Reject(format!("blocked at {:?}", ctx.point))
        }
    }

    #[tokio::test]
    async fn handler_observes_context_and_continues() {
        let h = Noop;
        let out = h.handle(&HookContext::new(HookPoint::AfterCommand)).await;
        assert_eq!(h.name(), "test.noop");
        assert_eq!(out, HookOutcome::Continue);
    }

    #[tokio::test]
    async fn handler_can_veto_with_reason() {
        let out = Veto.handle(&HookContext::new(HookPoint::BeforeCommand)).await;
        assert_eq!(out, HookOutcome::Reject("blocked at BeforeCommand".to_string()));
    }
}
