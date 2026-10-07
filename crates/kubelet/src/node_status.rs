//! Node status report gating: change detection and the report-frequency
//! gate. Ports `tryUpdateNodeStatus` / `nodeStatusHasChanged` /
//! `isUpdateStatusPeriodExpired` / `calculateDelay`
//! (pkg/kubelet/kubelet_node_status.go:493-541, :745-790) and the 4% loop
//! jitter (`wait.JitterUntil(kl.syncNodeStatus, kl.nodeStatusUpdateFrequency,
//! 0.04, true, wait.NeverStop)`, pkg/kubelet/kubelet.go:1852).
//!
//! The node is kept Ready between full reports by the node-heartbeat Lease
//! (see the lease task in `Kubelet::run`), exactly as upstream: status is
//! computed every `nodeStatusUpdateFrequency` but only written when it
//! changed or `nodeStatusReportFrequency` elapsed.

use rusternetes_common::resources::{NodeCondition, NodeStatus};
use serde_json::Value;
use std::time::{Duration, Instant};

/// `wait.JitterUntil` factor used for the status loop (kubelet.go:1852).
pub const NODE_STATUS_LOOP_JITTER_FACTOR: f64 = 0.04;

/// A uniform sample in [0, 1).
pub fn rand_unit() -> f64 {
    let b = uuid::Uuid::new_v4().as_u128();
    ((b >> 75) as u64 as f64) / ((1u64 << 53) as f64)
}

/// `wait.Jitter(d, factor)`: `d + rand[0,1) * factor * d`
/// (k8s.io/apimachinery/pkg/util/wait/backoff.go).
pub fn jitter(d: Duration, factor: f64, r: f64) -> Duration {
    d + d.mul_f64(r * factor)
}

/// `calculateDelay` (:539-541): `freq * (-0.5 + rand)`, in seconds, signed.
pub fn calculate_delay_secs(report_frequency: Duration, r: f64) -> f64 {
    report_frequency.as_secs_f64() * (-0.5 + r)
}

/// `isUpdateStatusPeriodExpired` (:535-537). A kubelet that never reported
/// (`last_report == None`, the zero `lastStatusReportTime`) is always expired.
pub fn is_update_status_period_expired(
    last_report: Option<Instant>,
    now: Instant,
    report_frequency: Duration,
    delay_secs: f64,
) -> bool {
    match last_report {
        None => true,
        Some(last) => {
            now.saturating_duration_since(last).as_secs_f64()
                >= report_frequency.as_secs_f64() + delay_secs
        }
    }
}

/// Drop nulls and empty arrays/objects: `apiequality.Semantic.DeepEqual`
/// treats nil and empty slices/maps as equal.
fn normalize(v: Value) -> Value {
    match v {
        Value::Object(m) => Value::Object(
            m.into_iter()
                .map(|(k, v)| (k, normalize(v)))
                .filter(|(_, v)| !is_empty(v))
                .collect(),
        ),
        Value::Array(a) => Value::Array(a.into_iter().map(normalize).collect()),
        other => other,
    }
}

fn is_empty(v: &Value) -> bool {
    match v {
        Value::Null => true,
        Value::Array(a) => a.is_empty(),
        Value::Object(m) => m.is_empty(),
        _ => false,
    }
}

/// `nodeConditionsHaveChanged` (:769-800): ignores `lastHeartbeatTime` and order.
pub fn node_conditions_have_changed(original: &[NodeCondition], current: &[NodeCondition]) -> bool {
    if original.len() != current.len() {
        return true;
    }
    let prep = |c: &[NodeCondition]| {
        let mut v: Vec<NodeCondition> = c.to_vec();
        v.sort_by(|a, b| a.condition_type.cmp(&b.condition_type));
        for c in &mut v {
            c.last_heartbeat_time = None;
        }
        serde_json::to_value(&v).unwrap_or(Value::Null)
    };
    prep(original) != prep(current)
}

/// `nodeStatusHasChanged` (:745-764).
pub fn node_status_has_changed(
    original: Option<&NodeStatus>,
    current: Option<&NodeStatus>,
) -> bool {
    let (o, c) = match (original, current) {
        (None, None) => return false,
        (Some(o), Some(c)) => (o, c),
        _ => return true,
    };
    if node_conditions_have_changed(
        o.conditions.as_deref().unwrap_or(&[]),
        c.conditions.as_deref().unwrap_or(&[]),
    ) {
        return true;
    }
    let strip = |s: &NodeStatus| {
        let mut s = s.clone();
        s.conditions = None;
        normalize(serde_json::to_value(&s).unwrap_or(Value::Null))
    };
    strip(o) != strip(c)
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::{DateTime, Duration as CDur, TimeZone, Utc};

    fn now() -> DateTime<Utc> {
        Utc.with_ymd_and_hms(2015, 1, 1, 12, 0, 0).unwrap()
    }
    fn cond(t: &str, s: &str, hb: DateTime<Utc>, tr: DateTime<Utc>) -> NodeCondition {
        NodeCondition {
            condition_type: t.into(),
            status: s.into(),
            last_heartbeat_time: Some(hb),
            last_transition_time: Some(tr),
            reason: None,
            message: None,
        }
    }
    fn st(c: Vec<NodeCondition>) -> NodeStatus {
        NodeStatus {
            conditions: Some(c),
            ..Default::default()
        }
    }

    /// Port of TestNodeStatusHasChanged
    /// (pkg/kubelet/kubelet_node_status_test.go:2785-2935).
    #[test]
    fn node_status_has_changed_cases() {
        let n = now();
        let f = n + CDur::minutes(1);
        let ready = cond("Ready", "True", n, n);
        let ready_hb = cond("Ready", "True", f, n);
        let ready_tr = cond("Ready", "True", f, f);
        let not_ready = cond("Ready", "False", n, n);
        let mem = cond("MemoryPressure", "False", n, n);
        let d = NodeStatus::default();

        assert!(!node_status_has_changed(None, None));
        assert!(!node_status_has_changed(Some(&d), Some(&d)));
        assert!(node_status_has_changed(None, Some(&d)));
        assert!(node_status_has_changed(
            None,
            Some(&st(vec![ready.clone(), mem.clone()]))
        ));
        assert!(!node_status_has_changed(
            Some(&st(vec![])),
            Some(&st(vec![]))
        ));
        let base = st(vec![ready.clone(), mem.clone()]);
        assert!(!node_status_has_changed(
            Some(&base),
            Some(&st(vec![ready.clone(), mem.clone()]))
        ));
        // heartbeat-only change
        assert!(!node_status_has_changed(
            Some(&base),
            Some(&st(vec![ready_hb.clone(), mem.clone()]))
        ));
        // order-only change
        assert!(!node_status_has_changed(
            Some(&base),
            Some(&st(vec![mem.clone(), ready_hb.clone()]))
        ));
        assert!(node_status_has_changed(
            Some(&base),
            Some(&st(vec![not_ready, mem.clone()]))
        ));
        assert!(node_status_has_changed(
            Some(&base),
            Some(&st(vec![ready_tr, mem.clone()]))
        ));
        assert!(node_status_has_changed(
            Some(&st(vec![ready.clone()])),
            Some(&st(vec![ready.clone(), mem]))
        ));
        let mut p1 = st(vec![ready.clone()]);
        let mut p2 = p1.clone();
        // (no `phase` in our NodeStatus; any non-condition field stands in)
        p1.volumes_in_use = Some(vec!["a".into()]);
        p2.volumes_in_use = Some(vec!["b".into()]);
        assert!(node_status_has_changed(Some(&p1), Some(&p2)));
    }

    #[test]
    fn non_condition_field_change_is_a_change_but_empty_equals_none() {
        let a = st(vec![]);
        let mut b = a.clone();
        b.addresses = Some(vec![]);
        assert!(!node_status_has_changed(Some(&a), Some(&b)));
        b.volumes_in_use = Some(vec!["v".into()]);
        assert!(node_status_has_changed(Some(&a), Some(&b)));
    }

    /// Port of TestIsUpdateStatusPeriodExpired
    /// (kubelet_node_status_test.go:3094-3150).
    #[test]
    fn is_update_status_period_expired_cases() {
        let freq = Duration::from_secs(300);
        let t = Instant::now();
        let ago = |s: u64| t.checked_sub(Duration::from_secs(s));
        assert!(is_update_status_period_expired(None, t, freq, 0.0));
        assert!(is_update_status_period_expired(None, t, freq, 30.0));
        assert!(!is_update_status_period_expired(ago(240), t, freq, 0.0));
        assert!(!is_update_status_period_expired(ago(300), t, freq, 60.0));
        assert!(is_update_status_period_expired(ago(240), t, freq, -120.0));
        assert!(is_update_status_period_expired(ago(300), t, freq, 0.0));
    }

    /// Port of TestCalculateDelay (kubelet_node_status_test.go:3152-3162).
    #[test]
    fn calculate_delay_is_within_half_the_report_frequency() {
        let f = Duration::from_secs(300);
        for _ in 0..100 {
            let d = calculate_delay_secs(f, rand_unit());
            assert!(d.abs() <= f.as_secs_f64() / 2.0);
        }
        assert_eq!(calculate_delay_secs(f, 0.0), -150.0);
    }

    /// `wait.Jitter(d, 0.04)` stays within [d, 1.04 d).
    #[test]
    fn jitter_is_within_four_percent() {
        let d = Duration::from_secs(10);
        for _ in 0..100 {
            let j = jitter(d, NODE_STATUS_LOOP_JITTER_FACTOR, rand_unit());
            assert!(j >= d && j < d.mul_f64(1.04));
        }
    }
}
