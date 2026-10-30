//! HPA pod grouping for the initial-readiness / CPU-initialization window.
//!
//! Port of the CPU branch of upstream
//! `pkg/controller/podautoscaler/replica_calculator.go::groupPods`: when
//! computing CPU utilization, pods that are not yet usefully measurable — not
//! running, missing metrics, or unready/just-started within the
//! `cpuInitializationPeriod` / `delayOfInitialReadinessStatus` windows — are
//! excluded so a cold-start CPU spike doesn't trigger a scale-up.
//!
//! Scope note: this implements the *classification* (which pods count). The
//! upstream `calcPlainMetricReplicas` value-rebalancing (assigning assumed
//! usage to missing/unready pods to bias the ratio) is a separate refinement
//! and is not done here — we simply average over the ready pods. `now` and the
//! windows are injected so the logic is unit-testable.

use std::collections::HashSet;

use chrono::{DateTime, Duration, Utc};
use rusternetes_common::resources::pod::Pod;
use rusternetes_common::types::Phase;

use super::hpa_metrics_client::PodMetricsInfo;

/// Upstream `--horizontal-pod-autoscaler-cpu-initialization-period` default.
pub fn cpu_initialization_period() -> Duration {
    Duration::minutes(5)
}

/// Upstream `--horizontal-pod-autoscaler-initial-readiness-delay` default.
pub fn initial_readiness_delay() -> Duration {
    Duration::seconds(30)
}

/// Names of the pods whose CPU metric should count toward utilization, mirroring
/// upstream `groupPods` (CPU branch): excludes deleting/Failed/Pending pods,
/// pods missing metrics, and unready/just-initializing pods.
pub fn ready_cpu_pods(
    pods: &[Pod],
    metrics: &PodMetricsInfo,
    now: DateTime<Utc>,
    cpu_init: Duration,
    readiness_delay: Duration,
) -> HashSet<String> {
    let mut ready = HashSet::new();
    for pod in pods {
        let name = &pod.metadata.name;
        // Deleting or Failed pods are ignored entirely.
        if pod.metadata.deletion_timestamp.is_some() {
            continue;
        }
        let Some(status) = pod.status.as_ref() else {
            continue;
        };
        match status.phase {
            Some(Phase::Failed) => continue,
            // Pending pods are unready.
            Some(Phase::Pending) => continue,
            _ => {}
        }
        // Pods without a metric reading are "missing", not ready.
        let Some(metric) = metrics.get(name) else {
            continue;
        };

        let ready_cond = status
            .conditions
            .as_ref()
            .and_then(|cs| cs.iter().find(|c| c.condition_type == "Ready"));

        let unready = match (ready_cond, status.start_time) {
            (Some(cond), Some(start)) => {
                if start + cpu_init > now {
                    // Still within the CPU initialization period: ignore the
                    // sample if the pod is unready, or if less than one metric
                    // window has elapsed since the last readiness transition.
                    let cond_false = cond.status == "False";
                    // replica_calculator.go:452 `groupPods`: the per-sample
                    // `metric.Window`, not a fixed constant.
                    let window =
                        Duration::from_std(metric.window).unwrap_or_else(|_| Duration::zero());
                    let window_not_elapsed = cond
                        .last_transition_time
                        .map(|lt| metric.timestamp < lt + window)
                        .unwrap_or(false);
                    cond_false || window_not_elapsed
                } else {
                    // Past the init period: ignore only if the pod is unready
                    // and has never been ready (still within the readiness delay
                    // of its start).
                    let cond_false = cond.status == "False";
                    cond_false
                        && cond
                            .last_transition_time
                            .map(|lt| start + readiness_delay > lt)
                            .unwrap_or(false)
                }
            }
            // No Ready condition or no start time → treat as unready.
            _ => true,
        };

        if !unready {
            ready.insert(name.clone());
        }
    }
    ready
}
