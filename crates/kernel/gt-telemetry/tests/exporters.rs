//! Gate test (hq-0bko.1): a synthetic workflow shows a multi-span trace in a fake
//! collector + the golden counter increments. Runs against the in-memory exporter so it
//! does not require a live OTLP collector.

use gt_events::Envelope;
use gt_telemetry::test_support::install_in_memory_tracer;
use gt_telemetry::{metrics, record_envelope};
use serde::{Deserialize, Serialize};
use tracing::{info, info_span};

/// Tiny domain event for the gate. Mirrors `QuotaEvent::UsageProbed` shape — the test only
/// needs `EventKind` + `Serialize`.
#[derive(Debug, Clone, Serialize, Deserialize)]
struct FakeEvent {
    account: String,
}

impl gt_events::EventKind for FakeEvent {
    fn kind(&self) -> &'static str {
        "test.fake_event"
    }
}

/// Distinct kind so the no-span assertion is isolated from the span-based test (the
/// Prometheus counter is process-global and tests run in parallel).
#[derive(Debug, Clone, Serialize, Deserialize)]
struct NoSpanEvent;

impl gt_events::EventKind for NoSpanEvent {
    fn kind(&self) -> &'static str {
        "test.no_span_event"
    }
}

/// Regression gate: the counter must bump even with NO active tracing span. The composition
/// root's reactor loop has no `#[instrument]` span, so a span-gated counter would silently
/// zero `gt_events_total` in production (the bug this asserts against).
#[test]
fn counter_bumps_without_an_active_span() {
    metrics::ensure_registered();
    assert!(
        tracing::Span::current().is_disabled(),
        "test must run with no ambient subscriber/span"
    );
    for _ in 0..3 {
        record_envelope(&Envelope::root(NoSpanEvent));
    }
    let text = metrics::render_text().expect("render");
    assert!(
        text.contains("gt_events_total{kind=\"test.no_span_event\"} 3"),
        "counter must bump outside a span, body:\n{text}"
    );
}

#[test]
fn synthetic_workflow_emits_trace_and_metrics() {
    let (sink, guard) = install_in_memory_tracer("gt_telemetry=trace,info");

    let parent_env = Envelope::root(FakeEvent {
        account: "acc-1".into(),
    });
    let child_env = Envelope::caused_by(
        &parent_env,
        FakeEvent {
            account: "acc-1".into(),
        },
    );

    // A two-span trace: outer "rotate" causally drives inner "probe". Both spans must close
    // (drop) before the OTEL exporter ships them — keep each in its own block.
    {
        let outer = info_span!(
            "rotate",
            correlation_id = tracing::field::Empty,
            causation_id = tracing::field::Empty,
            event_id = tracing::field::Empty,
            event.kind = tracing::field::Empty
        );
        let _outer = outer.enter();
        record_envelope(&parent_env);
        info!(target: "gt_telemetry", "outer step");

        {
            let inner = info_span!(
                "probe",
                correlation_id = tracing::field::Empty,
                causation_id = tracing::field::Empty,
                event_id = tracing::field::Empty,
                event.kind = tracing::field::Empty
            );
            let _i = inner.enter();
            record_envelope(&child_env);
            info!(target: "gt_telemetry", "inner step");
        }
    }

    guard.flush();
    let spans = sink.spans();
    assert!(spans.len() >= 2, "expected ≥2 spans, got {}", spans.len());

    // The Prometheus counter saw both events.
    let text = metrics::render_text().expect("render");
    assert!(
        text.contains("gt_events_total{kind=\"test.fake_event\"} 2"),
        "expected counter line, body:\n{text}"
    );
}
