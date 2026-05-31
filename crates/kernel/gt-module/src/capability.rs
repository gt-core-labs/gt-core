//! Capability declaration — what a module contributes and what it requires.
//!
//! **Seam for [`hq-mod-core.3`].** This bead (`.2`) only needs a type for
//! [`GtModule::capability`](crate::GtModule::capability) to return so the trait
//! compiles and downstream crates can name it. The real shape — emitted event
//! kinds, required scopes, owned MCP tool namespaces, subscribed hooks, and the
//! capability-conflict mapping — lands in `.3`.
//!
//! Keep additions here additive: `.3` will grow fields on [`Capability`], so a
//! module that returns [`Capability::default`] today must keep compiling.

use serde::{Deserialize, Serialize};

/// Declarative description of a module's contributions and requirements.
///
/// Intentionally minimal until `hq-mod-core.3`. A module that contributes
/// nothing (or has not yet been ported) returns [`Capability::default`].
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[non_exhaustive]
pub struct Capability {}

impl Capability {
    /// An empty capability set — contributes nothing, requires nothing.
    ///
    /// Equivalent to [`Capability::default`]; provided as a named constructor
    /// so module authors read intent at the call site.
    pub fn empty() -> Self {
        Capability::default()
    }
}
