//! Capability declaration — what a module contributes and what it requires.
//!
//! Seeded by `.2` (a type for [`GtModule::capability`](crate::GtModule::capability)
//! to return) and given its [`Scope`] vocabulary by `.3`. The first real field —
//! the set of authorization scopes a module *claims* (owns and enforces) — lands
//! with `hq-mod-core.6`, which the [`RootBuilder`](crate::RootBuilder) reads to
//! reject two modules claiming the same scope.
//!
//! The struct is `#[non_exhaustive]`: every later bead/epic grows it additively
//! (emitted event kinds, owned MCP tool namespaces, subscribed hooks, required
//! modules), so a module built through [`Capability::empty`] plus the fluent
//! `claiming*` chain keeps compiling as fields appear.

use serde::{Deserialize, Serialize};

use crate::event_kind::EventKind;
use crate::hook::HookPoint;
use crate::scope::Scope;

/// Declarative description of a module's contributions and requirements.
///
/// A module that contributes nothing (or has not yet been ported) returns
/// [`Capability::empty`]. Fields are populated through the fluent `claiming*`
/// builders so call sites read as a declaration.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[non_exhaustive]
pub struct Capability {
    /// Authorization scopes this module claims — i.e. owns and enforces.
    ///
    /// Each [`Scope`] here names a `<resource>.<verb>` authority the module is
    /// the single source of truth for. Two modules claiming the same scope is a
    /// wiring bug the builder rejects at `build()` time (`hq-mod-core.6`).
    scopes: Vec<Scope>,
    /// Lifecycle [`HookPoint`]s this module observes (`hq-mod-hooks.3`).
    ///
    /// This is the declaration only — which points the module reacts to. The
    /// handler itself (the async, `dyn`-dispatched behavior) lives in `gt-hooks`,
    /// which cannot be named here without a dependency cycle. The builder
    /// aggregates these declarations across the loaded modules and exposes them
    /// via [`Root::hook_subscriptions`](crate::Root::hook_subscriptions); the
    /// `gt-hooks` dispatcher binds handlers to them. Unlike [`scopes`](Self::scopes),
    /// the same point may be observed by many modules — observation is not
    /// exclusive ownership — so no conflict check applies.
    subscribed_hooks: Vec<HookPoint>,
    /// Versioned event kinds this module emits — i.e. owns as the source of truth
    /// (`hq-mod-events.1`).
    ///
    /// Each [`EventKind`] is a `<module>.<noun>.v<N>` identifier the module is the
    /// sole emitter of. v1 and v2 of the same noun are distinct kinds and may be
    /// emitted side by side during a migration (guardrail rule 5: versioned,
    /// replay-safe). Like [`scopes`](Self::scopes) this is exclusive ownership: two
    /// modules declaring the same emitted kind+version is a wiring bug the builder
    /// rejects (`hq-mod-events.5`). The append-only emission, replay reducers, and
    /// cross-module subscribe live in `gt-events`/`gt-plugin` (later beads); this
    /// is the declaration the module system reasons about.
    emits: Vec<EventKind>,
}

impl Capability {
    /// An empty capability set — contributes nothing, requires nothing.
    ///
    /// Equivalent to [`Capability::default`]; provided as a named constructor
    /// so module authors read intent at the call site.
    pub fn empty() -> Self {
        Capability::default()
    }

    /// Declare that this module claims (owns and enforces) `scope`. Chainable.
    ///
    /// Duplicate scopes within a single module are harmless and de-duplicated by
    /// the conflict check; the check only flags the *same* scope claimed by two
    /// *different* modules.
    pub fn claiming(mut self, scope: Scope) -> Self {
        self.scopes.push(scope);
        self
    }

    /// Declare a batch of claimed scopes at once. Chainable.
    pub fn claiming_all(mut self, scopes: impl IntoIterator<Item = Scope>) -> Self {
        self.scopes.extend(scopes);
        self
    }

    /// The scopes this module claims, in declaration order.
    pub fn scopes(&self) -> &[Scope] {
        &self.scopes
    }

    /// Declare that this module observes lifecycle `point`. Chainable.
    ///
    /// Observation is not exclusive: many modules may subscribe to the same
    /// point, and a module may list a point more than once (the dispatcher treats
    /// the declaration set as the points to wire). A `BeforeCommand` subscriber
    /// may later veto via the `gt-hooks` handler; every other point observes only
    /// (see [`HookPoint::is_vetoable`]).
    pub fn subscribing(mut self, point: HookPoint) -> Self {
        self.subscribed_hooks.push(point);
        self
    }

    /// Declare a batch of observed hook points at once. Chainable.
    pub fn subscribing_all(mut self, points: impl IntoIterator<Item = HookPoint>) -> Self {
        self.subscribed_hooks.extend(points);
        self
    }

    /// The lifecycle hook points this module observes, in declaration order.
    pub fn subscribed_hooks(&self) -> &[HookPoint] {
        &self.subscribed_hooks
    }

    /// Declare that this module emits (owns) event `kind`. Chainable.
    ///
    /// A module is the single source of truth for the kinds it emits. Listing the
    /// same kind twice within one module is harmless and collapses in the
    /// ownership check; that check (`hq-mod-events.5`) only flags the *same*
    /// kind+version emitted by two *different* modules.
    pub fn emitting(mut self, kind: EventKind) -> Self {
        self.emits.push(kind);
        self
    }

    /// Declare a batch of emitted event kinds at once. Chainable.
    pub fn emitting_all(mut self, kinds: impl IntoIterator<Item = EventKind>) -> Self {
        self.emits.extend(kinds);
        self
    }

    /// The event kinds this module emits, in declaration order.
    pub fn emits(&self) -> &[EventKind] {
        &self.emits
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn empty_capability_subscribes_to_nothing() {
        assert!(Capability::empty().subscribed_hooks().is_empty());
    }

    #[test]
    fn subscribing_records_points_in_declaration_order() {
        let cap = Capability::empty()
            .subscribing(HookPoint::BeforeCommand)
            .subscribing(HookPoint::OnClaim);
        assert_eq!(
            cap.subscribed_hooks(),
            [HookPoint::BeforeCommand, HookPoint::OnClaim]
        );
    }

    #[test]
    fn subscribing_all_appends_a_batch() {
        let cap = Capability::empty()
            .subscribing(HookPoint::AfterCommand)
            .subscribing_all([HookPoint::BeforeEvent, HookPoint::AfterEvent]);
        assert_eq!(
            cap.subscribed_hooks(),
            [HookPoint::AfterCommand, HookPoint::BeforeEvent, HookPoint::AfterEvent]
        );
    }

    #[test]
    fn hooks_and_scopes_are_independent() {
        // A capability can declare hooks without scopes and vice versa.
        let cap = Capability::empty().subscribing(HookPoint::OnTransition);
        assert!(cap.scopes().is_empty());
        assert_eq!(cap.subscribed_hooks(), [HookPoint::OnTransition]);
    }

    #[test]
    fn empty_capability_emits_nothing() {
        assert!(Capability::empty().emits().is_empty());
    }

    #[test]
    fn emitting_records_kinds_in_declaration_order() {
        let cap = Capability::empty()
            .emitting(EventKind::new("beads.created.v1").unwrap())
            .emitting(EventKind::new("beads.closed.v1").unwrap());
        let kinds: Vec<&str> = cap.emits().iter().map(|k| k.as_str()).collect();
        assert_eq!(kinds, ["beads.created.v1", "beads.closed.v1"]);
    }

    #[test]
    fn emitting_all_appends_a_batch() {
        let cap = Capability::empty()
            .emitting(EventKind::new("beads.created.v1").unwrap())
            .emitting_all([
                EventKind::new("beads.created.v2").unwrap(),
                EventKind::new("beads.moved.v1").unwrap(),
            ]);
        let kinds: Vec<&str> = cap.emits().iter().map(|k| k.as_str()).collect();
        // v1 and v2 of the same noun coexist as distinct kinds.
        assert_eq!(kinds, ["beads.created.v1", "beads.created.v2", "beads.moved.v1"]);
    }

    #[test]
    fn emits_is_independent_of_scopes_and_hooks() {
        let cap = Capability::empty().emitting(EventKind::new("rigs.added.v1").unwrap());
        assert!(cap.scopes().is_empty());
        assert!(cap.subscribed_hooks().is_empty());
        assert_eq!(cap.emits().len(), 1);
    }
}
