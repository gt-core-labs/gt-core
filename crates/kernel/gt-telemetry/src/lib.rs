//! `gt-telemetry` — `tracing` + OTEL + Prometheus, the trace/metric path into Grafana
//! described in `docs/06-observability.md`.
//!
//! The kernel rule (`docs/01-architecture.md`): a domain never reads the wall clock or pulls
//! in an exporter directly. The bins call [`init`] once at startup and own the returned
//! [`TelemetryGuard`] until shutdown. Domains call [`record_envelope`] / [`metrics::*`] to
//! attach `correlation_id` / `causation_id` to the current span and bump the counters —
//! whoever wired the exporters ships them to Tempo / Prometheus.
//!
//! Replay-friendly: every helper here is a no-op when called inside `replay_gt` (no global
//! state mutation, no clock read in the hot path), so a deterministic replay produces the same
//! `GtState` whether or not telemetry was on.

use std::sync::{Arc, OnceLock};

use opentelemetry::trace::TracerProvider as _;
use opentelemetry_otlp::WithExportConfig;
use opentelemetry_sdk::trace::SdkTracerProvider;
use tracing_subscriber::layer::SubscriberExt;
use tracing_subscriber::util::SubscriberInitExt;
use tracing_subscriber::EnvFilter;

use gt_events::{Envelope, EventKind};

pub mod metrics;
pub mod test_support;

/// Build-time configuration for [`init`]. Driven by env vars in production
/// (`gt-root` / `gt-web` / `gt-mcp` build it via [`TelemetryConfig::from_env`]) and by
/// in-process fakes in tests.
#[derive(Debug, Clone)]
pub struct TelemetryConfig {
    /// `tracing` filter directive (e.g. `gt=debug,info`). Defaults to `info`.
    pub log_filter: String,
    /// OTLP HTTP endpoint (e.g. `http://tempo:4318/v1/traces`). `None` disables span export.
    pub otlp_endpoint: Option<String>,
    /// Service name attached to every exported span. Bins pass their own (`gt`, `gt-web`).
    pub service_name: String,
}

impl TelemetryConfig {
    /// Read the standard envs:
    /// - `RUST_LOG`             — log filter (default `info`)
    /// - `OTEL_EXPORTER_OTLP_ENDPOINT` — collector base URL (paths appended per signal)
    /// - `OTEL_SERVICE_NAME`    — service name override (falls back to `bin_default`)
    pub fn from_env(bin_default: &str) -> Self {
        Self {
            log_filter: std::env::var("RUST_LOG").unwrap_or_else(|_| "info".to_string()),
            otlp_endpoint: std::env::var("OTEL_EXPORTER_OTLP_ENDPOINT").ok(),
            service_name: std::env::var("OTEL_SERVICE_NAME")
                .unwrap_or_else(|_| bin_default.to_string()),
        }
    }

    /// No-op config for tests: filter `off`, no OTLP, service `gt-test`. The fmt layer is
    /// still registered so any rogue `info!` does not panic when no subscriber is set.
    pub fn off() -> Self {
        Self {
            log_filter: "off".to_string(),
            otlp_endpoint: None,
            service_name: "gt-test".to_string(),
        }
    }
}

/// Errors returned by [`init`]. Wraps the subscriber/exporter setup so the bins can decide
/// whether telemetry is required (fail-fast) or best-effort (log + continue).
#[derive(Debug, thiserror::Error)]
pub enum TelemetryError {
    #[error("global tracing subscriber already set")]
    AlreadySet,
    #[error("invalid log filter: {0}")]
    BadFilter(String),
    #[error("OTLP exporter build failed: {0}")]
    Exporter(String),
}

/// RAII guard returned by [`init`]. Drop it (or call [`TelemetryGuard::shutdown`]) before
/// the process exits so the OTLP exporter flushes its batched spans.
pub struct TelemetryGuard {
    provider: Option<Arc<SdkTracerProvider>>,
}

impl TelemetryGuard {
    /// Explicit shutdown — equivalent to dropping the guard.
    pub fn shutdown(mut self) {
        self.take_and_shutdown();
    }

    fn take_and_shutdown(&mut self) {
        if let Some(provider) = self.provider.take() {
            // SdkTracerProvider::shutdown returns Result<()>; ignore here — the process is
            // already on its way out, dead-letter visibility is the bus' job, not telemetry's.
            let _ = provider.shutdown();
        }
    }
}

impl Drop for TelemetryGuard {
    fn drop(&mut self) {
        self.take_and_shutdown();
    }
}

/// Install the global tracing subscriber + OTEL pipeline + Prometheus registry. Call once
/// from a binary's `main`; calling twice returns [`TelemetryError::AlreadySet`].
///
/// Wiring:
/// - `tracing-subscriber` registers an `EnvFilter` + `fmt` layer (stderr, compact),
/// - if `cfg.otlp_endpoint` is set, a `tracing-opentelemetry` layer ships spans via OTLP/HTTP
///   to that collector (Tempo in production) — annotated with the configured `service.name`,
/// - the Prometheus registry is exposed via [`metrics::registry`] for the bin's `/metrics`
///   endpoint and for the counters defined in [`metrics`].
pub fn init(cfg: TelemetryConfig) -> Result<TelemetryGuard, TelemetryError> {
    let filter = EnvFilter::try_new(&cfg.log_filter)
        .map_err(|e| TelemetryError::BadFilter(e.to_string()))?;
    let fmt_layer = tracing_subscriber::fmt::layer()
        .with_writer(std::io::stderr)
        .with_target(true)
        .compact();

    let registry = tracing_subscriber::registry().with(filter).with(fmt_layer);

    let provider = if let Some(endpoint) = cfg.otlp_endpoint.as_ref() {
        let exporter = opentelemetry_otlp::SpanExporter::builder()
            .with_http()
            .with_endpoint(endpoint.clone())
            .build()
            .map_err(|e| TelemetryError::Exporter(e.to_string()))?;
        let provider = SdkTracerProvider::builder()
            .with_batch_exporter(exporter)
            .with_resource(
                opentelemetry_sdk::Resource::builder()
                    .with_service_name(cfg.service_name.clone())
                    .build(),
            )
            .build();
        let tracer = provider.tracer(cfg.service_name.clone());
        let otel_layer = tracing_opentelemetry::layer().with_tracer(tracer);
        registry
            .with(otel_layer)
            .try_init()
            .map_err(|_| TelemetryError::AlreadySet)?;
        Some(Arc::new(provider))
    } else {
        registry
            .try_init()
            .map_err(|_| TelemetryError::AlreadySet)?;
        None
    };

    metrics::ensure_registered();

    Ok(TelemetryGuard { provider })
}

/// Record one envelope to telemetry: bump the per-kind Prometheus counter (always) and, if a
/// tracing span is active, attach the causal triple (`correlation_id`, `causation_id`,
/// `event_id`) to it. The causal chain *is* the trace (`docs/06-observability.md`).
///
/// The metric bump is **unconditional** — it must not depend on whether the caller happens to
/// be inside an `#[instrument]` span. The composition root's reactor loop has no span, so
/// gating the counter on span presence would silently zero `gt_events_total` in production
/// (the span attributes are the only span-conditional part).
pub fn record_envelope<E: EventKind>(env: &Envelope<E>) {
    metrics::observe_event(env.payload.kind());

    let span = tracing::Span::current();
    if span.is_disabled() {
        return;
    }
    span.record("correlation_id", tracing::field::display(&env.correlation_id));
    span.record("event_id", tracing::field::display(&env.event_id));
    if let Some(cause) = env.causation_id.as_ref() {
        span.record("causation_id", tracing::field::display(cause));
    }
    span.record("event.kind", env.payload.kind());
}

/// Module-level OnceLock so tests / repeated `init_off` calls never fight over the global.
static OFF_GUARD: OnceLock<()> = OnceLock::new();

/// Test helper: install a no-op subscriber once. Re-callable from any test; idempotent.
pub fn init_off_for_tests() {
    OFF_GUARD.get_or_init(|| {
        let _ = init(TelemetryConfig::off());
    });
}
