//! The audit metrics.
//!
//! Ported from `staging/src/k8s.io/apiserver/pkg/audit/metrics.go`
//! (`eventCounter`, `errorCounter`, `levelCounter`,
//! `ApiserverAuditDroppedCounter`, `ObserveEvent`, `ObservePolicyLevel`,
//! `HandlePluginError`). Call sites: `ObserveEvent` in
//! `AuditContext.ProcessEventStage` (context.go:129), `ObservePolicyLevel` in
//! `evaluatePolicyAndCreateAuditEvent` (filters/audit.go:131) and
//! `ApiserverAuditDroppedCounter` in `WithAudit` (filters/audit.go:62).

use std::sync::LazyLock;

use prometheus::{register, IntCounter, IntCounterVec, Opts};
use rusternetes_common::audit::AuditLevel;

fn opts(name: &str, help: &str) -> Opts {
    Opts::new(name, help)
        .namespace("apiserver")
        .subsystem("audit")
}

/// `eventCounter`.
static EVENT_COUNTER: LazyLock<IntCounter> = LazyLock::new(|| {
    let c = IntCounter::with_opts(opts(
        "event_total",
        "Counter of audit events generated and sent to the audit backend.",
    ))
    .expect("valid metric");
    let _ = register(Box::new(c.clone()));
    c
});

/// `errorCounter`.
static ERROR_COUNTER: LazyLock<IntCounterVec> = LazyLock::new(|| {
    let c = IntCounterVec::new(
        opts(
            "error_total",
            "Counter of audit events that failed to be audited properly. Plugin identifies the plugin affected by the error.",
        ),
        &["plugin"],
    )
    .expect("valid metric");
    let _ = register(Box::new(c.clone()));
    c
});

/// `levelCounter`.
static LEVEL_COUNTER: LazyLock<IntCounterVec> = LazyLock::new(|| {
    let c = IntCounterVec::new(
        opts(
            "level_total",
            "Counter of policy levels for audit events (1 per request).",
        ),
        &["level"],
    )
    .expect("valid metric");
    let _ = register(Box::new(c.clone()));
    c
});

/// `ApiserverAuditDroppedCounter`.
static DROPPED_COUNTER: LazyLock<IntCounter> = LazyLock::new(|| {
    let c = IntCounter::with_opts(opts(
        "requests_rejected_total",
        "Counter of apiserver requests rejected due to an error in audit logging backend.",
    ))
    .expect("valid metric");
    let _ = register(Box::new(c.clone()));
    c
});

/// Register all four, as the Go `init()` does, so they are served at zero.
pub fn register_metrics() {
    LazyLock::force(&EVENT_COUNTER);
    LazyLock::force(&ERROR_COUNTER);
    LazyLock::force(&LEVEL_COUNTER);
    LazyLock::force(&DROPPED_COUNTER);
}

/// `ObserveEvent`.
pub fn observe_event() {
    EVENT_COUNTER.inc();
}

/// `ObservePolicyLevel`: the label is `string(level)`.
pub fn observe_policy_level(level: &AuditLevel) {
    let l = match level {
        AuditLevel::None => "None",
        AuditLevel::Metadata => "Metadata",
        AuditLevel::Request => "Request",
        AuditLevel::RequestResponse => "RequestResponse",
    };
    LEVEL_COUNTER.with_label_values(&[l]).inc();
}

/// `ApiserverAuditDroppedCounter.Inc()`.
pub fn observe_request_rejected() {
    DROPPED_COUNTER.inc();
}

/// The counting half of `HandlePluginError`: `errorCounter.WithLabelValues(plugin).Add(len(impacted))`.
pub fn count_plugin_error(plugin: &str, impacted: usize) {
    ERROR_COUNTER
        .with_label_values(&[plugin])
        .inc_by(impacted as u64);
}
