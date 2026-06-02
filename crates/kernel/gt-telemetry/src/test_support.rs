//! In-process test helpers. Not used in production: hidden behind `cfg(test)` in dependents
//! by gating their imports, but kept public here so other crates' integration tests can
//! exercise the same subscriber wiring without re-implementing it.

use std::sync::{Arc, Mutex};

use opentelemetry::trace::TracerProvider as _;
use opentelemetry_sdk::trace::{SdkTracerProvider, SpanData, SpanExporter};
use tracing_subscriber::layer::SubscriberExt;
use tracing_subscriber::EnvFilter;

/// In-memory `SpanExporter`: every batched span is appended to a `Vec` so tests can assert on
/// the trace tree. Concurrent inserts go through a `Mutex` — fine for tests, never used in a
/// hot path.
#[derive(Debug, Clone, Default)]
pub struct InMemorySpanSink {
    inner: Arc<Mutex<Vec<SpanData>>>,
}

impl InMemorySpanSink {
    pub fn new() -> Self {
        Self::default()
    }

    /// Snapshot of every span recorded so far. The provider must be shut down (or the
    /// `BatchSpanProcessor` flushed) before reading for the assertion to be reliable.
    pub fn spans(&self) -> Vec<SpanData> {
        self.inner.lock().expect("span sink mutex").clone()
    }
}

impl SpanExporter for InMemorySpanSink {
    fn export(
        &self,
        batch: Vec<SpanData>,
    ) -> impl std::future::Future<Output = opentelemetry_sdk::error::OTelSdkResult> + Send {
        let inner = self.inner.clone();
        async move {
            inner.lock().expect("span sink mutex").extend(batch);
            Ok(())
        }
    }
}

/// Spawn a tracer provider that records spans into [`InMemorySpanSink`] and install it as the
/// default subscriber for the **current thread**. Returns the sink + an RAII guard whose
/// `Drop` flushes the provider. Re-callable from many tests without fighting the global
/// subscriber.
pub fn install_in_memory_tracer(filter: &str) -> (InMemorySpanSink, TestTracerGuard) {
    let sink = InMemorySpanSink::new();
    let provider = SdkTracerProvider::builder()
        .with_simple_exporter(sink.clone())
        .build();
    let tracer = provider.tracer("gt-test");
    let subscriber = tracing_subscriber::registry()
        .with(EnvFilter::try_new(filter).unwrap_or_else(|_| EnvFilter::new("info")))
        .with(tracing_opentelemetry::layer().with_tracer(tracer));
    let guard = TestTracerGuard {
        provider: Some(provider),
        _default: tracing::subscriber::set_default(subscriber),
    };
    crate::metrics::ensure_registered();
    (sink, guard)
}

/// Holds the OTEL provider for explicit shutdown plus the tracing default-subscriber guard.
/// Dropping the guard restores the previous subscriber.
pub struct TestTracerGuard {
    provider: Option<SdkTracerProvider>,
    _default: tracing::subscriber::DefaultGuard,
}

impl TestTracerGuard {
    /// Force-flush spans so [`InMemorySpanSink::spans`] reflects everything emitted so far.
    pub fn flush(&self) {
        if let Some(p) = self.provider.as_ref() {
            let _ = p.force_flush();
        }
    }
}

impl Drop for TestTracerGuard {
    fn drop(&mut self) {
        if let Some(p) = self.provider.take() {
            let _ = p.shutdown();
        }
    }
}
