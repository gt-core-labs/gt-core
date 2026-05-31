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
}
