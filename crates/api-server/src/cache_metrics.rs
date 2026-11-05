//! Watch-cache metrics (`storage/cacher/metrics/metrics.go`).

use prometheus::{register, IntCounterVec, Opts};
use std::sync::LazyLock;

/// `TerminatedWatchersCounter`: `apiserver_terminated_watchers_total{group,resource}`.
static TERMINATED_WATCHERS: LazyLock<IntCounterVec> = LazyLock::new(|| {
    let c = IntCounterVec::new(
        Opts::new(
            "terminated_watchers_total",
            "Counter of watchers closed due to unresponsiveness broken by resource type.",
        )
        .namespace("apiserver"),
        &["group", "resource"],
    )
    .expect("valid metric");
    let _ = register(Box::new(c.clone()));
    c
});

/// `metrics.TerminatedWatchersCounter.WithLabelValues(group, resource).Inc()`.
pub fn observe_terminated_watcher(group: &str, resource: &str) {
    TERMINATED_WATCHERS
        .with_label_values(&[group, resource])
        .inc();
}

/// Current value (tests).
pub fn terminated_watchers(group: &str, resource: &str) -> u64 {
    TERMINATED_WATCHERS
        .with_label_values(&[group, resource])
        .get()
}
