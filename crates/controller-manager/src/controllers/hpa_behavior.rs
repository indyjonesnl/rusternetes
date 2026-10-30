//! HPA `spec.behavior` scale rate policies.
//!
//! Port of upstream `pkg/controller/podautoscaler/horizontal.go`
//! (`convertDesiredReplicasWithBehaviorRate`,
//! `calculateScaleUpLimitWithScalingRules`,
//! `calculateScaleDownLimitWithBehaviors`, `getReplicasChangePerPeriod`,
//! `storeScaleEvent`). Given the metric-driven desired replica count, these
//! cap how fast the workload may scale per the configured `Pods`/`Percent`
//! policies over their `periodSeconds`, selecting the most/least permissive via
//! `selectPolicy` (`Max` default, `Min`, `Disabled`). A per-HPA history of
//! recent scale events feeds the period windows.
//!
//! Also implements `behavior.{scaleUp,scaleDown}.stabilizationWindowSeconds`
//! ([`stabilize_recommendation`] + [`RecommendationStore`], upstream
//! `stabilizeRecommendationWithBehaviors`). `now` is injected so the pure math
//! is unit-testable.

use std::collections::HashMap;
use std::sync::Mutex;

use chrono::{DateTime, Duration, Utc};
use rusternetes_common::resources::{HPAScalingRules, HorizontalPodAutoscalerBehavior};

/// One recorded scale change (absolute, non-negative), timestamped. Mirrors
/// upstream `timestampedScaleEvent`.
#[derive(Clone, Copy, Debug)]
pub struct ScaleEvent {
    pub change: i32,
    pub at: DateTime<Utc>,
}

/// Recent scale events for one HPA, split by direction: `(scale-up, scale-down)`.
pub type DirectionEvents = (Vec<ScaleEvent>, Vec<ScaleEvent>);

/// Sum of replica changes within `period_seconds` of `now`. Upstream
/// `getReplicasChangePerPeriod`.
pub fn change_in_period(events: &[ScaleEvent], period_seconds: i32, now: DateTime<Utc>) -> i32 {
    let cutoff = now - Duration::seconds(period_seconds as i64);
    events
        .iter()
        .filter(|e| e.at > cutoff)
        .map(|e| e.change)
        .sum()
}

/// Longest `periodSeconds` across a rule's policies (upstream
/// `getLongestPolicyPeriod`) — the window past which events can be pruned.
fn longest_period(rules: &HPAScalingRules) -> i32 {
    rules
        .policies
        .as_deref()
        .unwrap_or(&[])
        .iter()
        .map(|p| p.period_seconds)
        .max()
        .unwrap_or(0)
}

/// Upstream `calculateScaleUpLimitWithScalingRules`: the maximum replica count
/// the workload may reach this step under the scale-up rules.
fn scale_up_limit(
    current: i32,
    up: &[ScaleEvent],
    down: &[ScaleEvent],
    rules: &HPAScalingRules,
    now: DateTime<Utc>,
) -> i32 {
    let select = rules.select_policy.as_deref().unwrap_or("Max");
    if select == "Disabled" {
        return current;
    }
    let use_min = select == "Min";
    // For scale up: Min selects the lowest change, Max the highest.
    let mut result = if use_min { i32::MAX } else { i32::MIN };
    for p in rules.policies.as_deref().unwrap_or(&[]) {
        let added = change_in_period(up, p.period_seconds, now);
        let deleted = change_in_period(down, p.period_seconds, now);
        let period_start = current - added + deleted;
        let proposed = match p.policy_type.as_str() {
            "Pods" => period_start + p.value,
            "Percent" => (period_start as f64 * (1.0 + p.value as f64 / 100.0)).ceil() as i32,
            _ => continue,
        };
        result = if use_min {
            result.min(proposed)
        } else {
            result.max(proposed)
        };
    }
    result
}

/// Upstream `calculateScaleDownLimitWithBehaviors`: the minimum replica count
/// the workload may drop to this step under the scale-down rules.
fn scale_down_limit(
    current: i32,
    up: &[ScaleEvent],
    down: &[ScaleEvent],
    rules: &HPAScalingRules,
    now: DateTime<Utc>,
) -> i32 {
    let select = rules.select_policy.as_deref().unwrap_or("Max");
    if select == "Disabled" {
        return current;
    }
    let use_max = select == "Min";
    // For scale down: Min selects the lowest change (→ highest floor, use max);
    // the default selects the largest change (→ lowest floor, use min).
    let mut result = if use_max { i32::MIN } else { i32::MAX };
    for p in rules.policies.as_deref().unwrap_or(&[]) {
        let added = change_in_period(up, p.period_seconds, now);
        let deleted = change_in_period(down, p.period_seconds, now);
        let period_start = current - added + deleted;
        let proposed = match p.policy_type.as_str() {
            "Pods" => period_start - p.value,
            "Percent" => (period_start as f64 * (1.0 - p.value as f64 / 100.0)) as i32,
            _ => continue,
        };
        result = if use_max {
            result.max(proposed)
        } else {
            result.min(proposed)
        };
    }
    result
}

/// Upstream `convertDesiredReplicasWithBehaviorRate`: rate-limit `desired`
/// against `current` per the behavior policies and bound by min/max. Returns
/// the metric-driven `desired` unchanged when no rule constrains it (the caller
/// still applies the overall min/max clamp).
pub fn convert_with_behavior_rate(
    current: i32,
    desired: i32,
    min_replicas: i32,
    max_replicas: i32,
    behavior: &HorizontalPodAutoscalerBehavior,
    events: &DirectionEvents,
    now: DateTime<Utc>,
) -> i32 {
    let (up, down) = (events.0.as_slice(), events.1.as_slice());
    if desired > current {
        if let Some(rules) = behavior.scale_up.as_ref() {
            let mut limit = scale_up_limit(current, up, down, rules, now);
            // Don't scale up until the in-window events age out.
            if limit < current {
                limit = current;
            }
            let max_allowed = max_replicas.min(limit);
            if desired > max_allowed {
                return max_allowed;
            }
        }
    } else if desired < current {
        if let Some(rules) = behavior.scale_down.as_ref() {
            let mut limit = scale_down_limit(current, up, down, rules, now);
            if limit > current {
                limit = current;
            }
            let min_allowed = min_replicas.max(limit);
            if desired < min_allowed {
                return min_allowed;
            }
        }
    }
    desired
}

/// Per-HPA history of recent scale events, feeding the period windows. Mirrors
/// upstream's `scaleUpEvents` / `scaleDownEvents` maps; pruning past the longest
/// policy period replaces upstream's outdated-slot reuse with a plain retain.
#[derive(Default)]
pub struct ScaleEventStore {
    // key -> (scale-up events, scale-down events)
    inner: Mutex<HashMap<String, DirectionEvents>>,
}

impl ScaleEventStore {
    /// Snapshot of `(up, down)` events for `key` (empty if none recorded).
    pub fn snapshot(&self, key: &str) -> DirectionEvents {
        self.inner
            .lock()
            .unwrap()
            .get(key)
            .cloned()
            .unwrap_or_default()
    }

    /// Record a scale from `prev` → `new` replicas (upstream `storeScaleEvent`),
    /// pruning entries older than the relevant rule's longest policy period.
    /// No-op when `behavior` carries no rule for the direction or replicas are
    /// unchanged.
    pub fn record(
        &self,
        key: &str,
        behavior: &HorizontalPodAutoscalerBehavior,
        prev: i32,
        new: i32,
        now: DateTime<Utc>,
    ) {
        if new == prev {
            return;
        }
        let mut map = self.inner.lock().unwrap();
        let entry = map.entry(key.to_string()).or_default();
        if new > prev {
            let lp = behavior.scale_up.as_ref().map(longest_period).unwrap_or(0);
            let cutoff = now - Duration::seconds(lp as i64);
            entry.0.retain(|e| e.at > cutoff);
            entry.0.push(ScaleEvent {
                change: new - prev,
                at: now,
            });
        } else {
            let lp = behavior
                .scale_down
                .as_ref()
                .map(longest_period)
                .unwrap_or(0);
            let cutoff = now - Duration::seconds(lp as i64);
            entry.1.retain(|e| e.at > cutoff);
            entry.1.push(ScaleEvent {
                change: prev - new,
                at: now,
            });
        }
    }
}

/// Upstream `stabilizeRecommendationWithBehaviors`: clamp `desired` between the
/// *min* recommendation seen within the scale-up window and the *max* within the
/// scale-down window (each also bounded by `desired`), measured around
/// `current`. `recs` is the recent (unstabilized) recommendation history. This
/// holds the replica count at a recent high during a transient dip (downscale
/// stabilization) and at a recent low during a transient spike (upscale).
pub fn stabilize_recommendation(
    current: i32,
    desired: i32,
    up_window_s: i32,
    down_window_s: i32,
    recs: &[(DateTime<Utc>, i32)],
    now: DateTime<Utc>,
) -> i32 {
    let up_cutoff = now - Duration::seconds(up_window_s as i64);
    let down_cutoff = now - Duration::seconds(down_window_s as i64);
    let mut up_rec = desired;
    let mut down_rec = desired;
    for (t, r) in recs {
        if *t > up_cutoff {
            up_rec = up_rec.min(*r);
        }
        if *t > down_cutoff {
            down_rec = down_rec.max(*r);
        }
    }
    let mut rec = current;
    if rec < up_rec {
        rec = up_rec;
    }
    if rec > down_rec {
        rec = down_rec;
    }
    rec
}

/// A timestamped recommendation: `(observed_at, replicas)`. Mirrors upstream
/// `timestampedRecommendation`.
pub type Recommendation = (DateTime<Utc>, i32);

/// Per-HPA history of recent (unstabilized) recommendations feeding
/// [`stabilize_recommendation`]. Mirrors upstream's `recommendations` map of
/// `timestampedRecommendation`.
#[derive(Default)]
pub struct RecommendationStore {
    inner: Mutex<HashMap<String, Vec<Recommendation>>>,
}

impl RecommendationStore {
    /// Recent recommendations for `key` (empty if none).
    pub fn snapshot(&self, key: &str) -> Vec<Recommendation> {
        self.inner
            .lock()
            .unwrap()
            .get(key)
            .cloned()
            .unwrap_or_default()
    }

    /// Append a recommendation, pruning entries older than `max_window_s` (the
    /// longest of the two stabilization windows).
    pub fn record(&self, key: &str, recommendation: i32, max_window_s: i32, now: DateTime<Utc>) {
        let mut map = self.inner.lock().unwrap();
        let v = map.entry(key.to_string()).or_default();
        let cutoff = now - Duration::seconds(max_window_s as i64);
        v.retain(|(t, _)| *t >= cutoff);
        v.push((now, recommendation));
    }
}
