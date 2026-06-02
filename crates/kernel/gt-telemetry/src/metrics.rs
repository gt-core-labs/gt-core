//! Process-global Prometheus registry + the golden counters every domain shares.
//!
//! Why a global instead of injecting a registry through the actor handles: Prometheus
//! exporters scrape the registry process-wide, and the `tracing` macros in the hot path
//! cannot weave a parameter into every span emission. Threading a registry would force every
//! domain to take it as a constructor arg even though only the bin's `/metrics` handler
//! actually reads it. Keeping it on `OnceLock` makes the dependency direction match the
//! architecture (domains depend only on the kernel, not on the bin), at the cost of one
//! single global — bounded, namespaced, and discoverable here.

use std::sync::OnceLock;

use prometheus::{
    register_int_counter_vec_with_registry, IntCounterVec, Registry,
};

/// The global registry. Lazily created on first access so domains can record metrics even
/// before [`crate::init`] runs (tests, replay).
static REGISTRY: OnceLock<Registry> = OnceLock::new();
/// Counter of every domain event the bus has seen, labelled by `kind`. Built once and
/// reused — labels are bound on the hot path via `with_label_values`.
static EVENT_COUNTER: OnceLock<IntCounterVec> = OnceLock::new();
/// Counter of dead-letter entries, labelled by `kind`. Bumped by the composition root /
/// the bus on `UnhandledEvent` / handler errors.
static DEAD_LETTER_COUNTER: OnceLock<IntCounterVec> = OnceLock::new();

/// Borrow the process-wide registry (Prometheus' scrape target).
pub fn registry() -> &'static Registry {
    REGISTRY.get_or_init(Registry::new)
}

/// Idempotent: register the golden metrics on the global registry. Called by [`crate::init`]
/// at startup; safe to call again from tests.
pub fn ensure_registered() {
    let reg = registry();
    EVENT_COUNTER.get_or_init(|| {
        register_int_counter_vec_with_registry!(
            "gt_events_total",
            "Total domain events ingested by the composition root, labelled by kind",
            &["kind"],
            reg
        )
        .expect("register gt_events_total")
    });
    DEAD_LETTER_COUNTER.get_or_init(|| {
        register_int_counter_vec_with_registry!(
            "gt_dead_letter_total",
            "Total dead-letter entries (unhandled events + handler errors)",
            &["kind"],
            reg
        )
        .expect("register gt_dead_letter_total")
    });
}

/// Increment `gt_events_total{kind=…}`. Used by [`crate::record_envelope`] and by the bus on
/// every published event so the count matches the audit log.
pub fn observe_event(kind: &'static str) {
    if let Some(c) = EVENT_COUNTER.get() {
        c.with_label_values(&[kind]).inc();
    }
}

/// Increment `gt_dead_letter_total{kind=…}`. The composition root calls this whenever it
/// appends to the in-process dead-letter sink.
pub fn observe_dead_letter(kind: &'static str) {
    if let Some(c) = DEAD_LETTER_COUNTER.get() {
        c.with_label_values(&[kind]).inc();
    }
}

/// Render the registry in the Prometheus text exposition format for the `/metrics` route.
pub fn render_text() -> Result<String, prometheus::Error> {
    use prometheus::Encoder;
    let encoder = prometheus::TextEncoder::new();
    let mut buf = Vec::new();
    encoder.encode(&registry().gather(), &mut buf)?;
    String::from_utf8(buf).map_err(|e| prometheus::Error::Msg(e.to_string()))
}
