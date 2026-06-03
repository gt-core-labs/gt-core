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
/// `point` is the firing [`HookPoint`]; `command` carries the name of the command
/// in flight at the [`BeforeCommand`](HookPoint::BeforeCommand) /
/// [`AfterCommand`](HookPoint::AfterCommand) points so a handler can discriminate
/// *which* command fired (e.g. only veto `merge.submit`). It is `None` at the
/// non-command points (the event pair, claim/release, transition), which carry no
/// command. Richer payload (envelope, actor scope) can still be added later;
/// `#[non_exhaustive]` keeps that growth additive (`hq-mod-hooks.8`).
#[derive(Clone, Debug, PartialEq, Eq)]
#[non_exhaustive]
pub struct HookContext {
    /// The lifecycle point that fired this invocation.
    pub point: HookPoint,
    /// The command in flight, at the command points; `None` elsewhere. Set via
    /// [`with_command`](HookContext::with_command); read via
    /// [`command`](HookContext::command).
    pub command: Option<String>,
}

impl HookContext {
    /// Build a context for `point` with no command attached. Stays stable as the
    /// payload grows — command-scoped callers add the command with
    /// [`with_command`](HookContext::with_command).
    pub fn new(point: HookPoint) -> Self {
        HookContext { point, command: None }
    }

    /// Attach the in-flight command name, for the
    /// [`BeforeCommand`](HookPoint::BeforeCommand) /
    /// [`AfterCommand`](HookPoint::AfterCommand) points. Chainable.
    pub fn with_command(mut self, command: impl Into<String>) -> Self {
        self.command = Some(command.into());
        self
    }

    /// The command in flight, or `None` at a non-command point.
    pub fn command(&self) -> Option<&str> {
        self.command.as_deref()
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

    #[test]
    fn new_context_has_no_command() {
        let ctx = HookContext::new(HookPoint::AfterCommand);
        assert_eq!(ctx.command(), None);
    }

    #[test]
    fn with_command_attaches_and_reads_back() {
        let ctx = HookContext::new(HookPoint::BeforeCommand).with_command("merge.submit");
        assert_eq!(ctx.command(), Some("merge.submit"));
        assert_eq!(ctx.point, HookPoint::BeforeCommand);
    }

    /// A handler discriminates on the command name — the seam `hq-mod-hooks.7`
    /// (Sheriff pre-merge watchdog) needs: veto only `merge.submit`, ignore the
    /// rest.
    struct MergeGate;
    #[async_trait]
    impl HookHandler for MergeGate {
        fn name(&self) -> &str {
            "test.merge-gate"
        }
        async fn handle(&self, ctx: &HookContext) -> HookOutcome {
            match ctx.command() {
                Some("merge.submit") => HookOutcome::Reject("watchdog".to_string()),
                _ => HookOutcome::Continue,
            }
        }
    }

    #[tokio::test]
    async fn handler_discriminates_by_command() {
        let gate = MergeGate;
        let targeted = HookContext::new(HookPoint::BeforeCommand).with_command("merge.submit");
        let other = HookContext::new(HookPoint::BeforeCommand).with_command("bead.claim");
        assert_eq!(gate.handle(&targeted).await, HookOutcome::Reject("watchdog".to_string()));
        assert_eq!(gate.handle(&other).await, HookOutcome::Continue);
        assert_eq!(gate.handle(&HookContext::new(HookPoint::BeforeCommand)).await, HookOutcome::Continue);
    }
}
