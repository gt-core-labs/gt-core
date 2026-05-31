//! [`WebhookRegistry`] — the set of [`WebhookSource`]s the router dispatches to,
//! keyed by their `<source>` path segment.
//!
//! Built once at composition time (the app registers GitHub, Linear, … sources)
//! and shared read-only behind an `Arc` by the router. A later id silently
//! overrides an earlier one, so registering the same source twice is harmless.

use std::collections::HashMap;

use crate::source::WebhookSource;

/// Holds the registered [`WebhookSource`]s by id.
#[derive(Default)]
pub struct WebhookRegistry {
    sources: HashMap<String, Box<dyn WebhookSource>>,
}

impl WebhookRegistry {
    /// Start an empty registry.
    pub fn new() -> Self {
        WebhookRegistry::default()
    }

    /// Register `source` under its [`id`](WebhookSource::id). Chainable. A source
    /// whose id collides with an existing one replaces it.
    pub fn register(&mut self, source: Box<dyn WebhookSource>) -> &mut Self {
        self.sources.insert(source.id().to_string(), source);
        self
    }

    /// Look up the source registered under `id`, if any.
    pub fn get(&self, id: &str) -> Option<&dyn WebhookSource> {
        self.sources.get(id).map(Box::as_ref)
    }

    /// Number of registered sources.
    pub fn len(&self) -> usize {
        self.sources.len()
    }

    /// Whether no source is registered.
    pub fn is_empty(&self) -> bool {
        self.sources.is_empty()
    }
}
