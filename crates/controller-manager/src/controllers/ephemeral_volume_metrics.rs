//! Prometheus series for the generic ephemeral volume controller.
//!
//! Port of `pkg/controller/volume/ephemeral/metrics/metrics.go` (release-1.35):
//! subsystem `ephemeral_volume_controller`, counters `create_total` and
//! `create_failures_total`. Upstream registers them in `legacyregistry` once
//! (`registerMetrics sync.Once`); here a process-global [`Registry`] plays that
//! role. Upstream's quirk is kept verbatim: both counters share the help text
//! "Number of PersistentVolumeClaim creation requests".

use prometheus::{Encoder, IntCounter, Opts, Registry, TextEncoder};
use std::sync::LazyLock;

const SUBSYSTEM: &str = "ephemeral_volume_controller";
const HELP: &str = "Number of PersistentVolumeClaim creation requests";

struct Series {
    registry: Registry,
    create_attempts: IntCounter,
    create_failures: IntCounter,
}

static SERIES: LazyLock<Series> = LazyLock::new(|| {
    let registry = Registry::new();
    let create_attempts =
        IntCounter::with_opts(Opts::new("create_total", HELP).subsystem(SUBSYSTEM))
            .expect("valid opts");
    let create_failures =
        IntCounter::with_opts(Opts::new("create_failures_total", HELP).subsystem(SUBSYSTEM))
            .expect("valid opts");
    registry
        .register(Box::new(create_attempts.clone()))
        .expect("register once");
    registry
        .register(Box::new(create_failures.clone()))
        .expect("register once");
    Series {
        registry,
        create_attempts,
        create_failures,
    }
});

/// `EphemeralVolumeCreateAttempts.Inc()` (`controller.go:295`).
pub fn inc_create_attempts() {
    SERIES.create_attempts.inc();
}

/// `EphemeralVolumeCreateFailures.Inc()` (`controller.go:298`).
pub fn inc_create_failures() {
    SERIES.create_failures.inc();
}

/// `testutil.GetCounterMetricValue(EphemeralVolumeCreateAttempts)`.
pub fn create_attempts() -> u64 {
    SERIES.create_attempts.get()
}

/// `testutil.GetCounterMetricValue(EphemeralVolumeCreateFailures)`.
pub fn create_failures() -> u64 {
    SERIES.create_failures.get()
}

/// The registry in Prometheus text exposition format.
pub fn gather() -> String {
    let mut buf = Vec::new();
    TextEncoder::new()
        .encode(&SERIES.registry.gather(), &mut buf)
        .expect("text encode");
    String::from_utf8(buf).expect("utf8")
}
