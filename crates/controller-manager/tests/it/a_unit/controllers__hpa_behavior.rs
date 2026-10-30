//! Unit tests moved from `src/controllers/hpa_behavior.rs`.
#![allow(unused_imports, dead_code)]

mod __parent {
    pub use chrono::{DateTime, Duration, Utc};
    pub use rusternetes_common::resources::{HPAScalingRules, HorizontalPodAutoscalerBehavior};
    pub use std::collections::HashMap;
    pub use std::sync::Mutex;
}

mod tests {
    use super::__parent::*;
    use rusternetes_common::resources::HPAScalingPolicy;
    use rusternetes_controller_manager::controllers::hpa_behavior::*;

    fn rules(select: &str, policies: Vec<HPAScalingPolicy>) -> HPAScalingRules {
        HPAScalingRules {
            stabilization_window_seconds: Some(0),
            select_policy: Some(select.to_string()),
            policies: Some(policies),
            tolerance: None,
        }
    }

    fn pods(value: i32, period: i32) -> HPAScalingPolicy {
        HPAScalingPolicy {
            policy_type: "Pods".to_string(),
            value,
            period_seconds: period,
        }
    }

    fn percent(value: i32, period: i32) -> HPAScalingPolicy {
        HPAScalingPolicy {
            policy_type: "Percent".to_string(),
            value,
            period_seconds: period,
        }
    }

    fn now() -> DateTime<Utc> {
        DateTime::from_timestamp(1_000_000, 0).unwrap()
    }

    #[test]
    fn scale_up_pods_policy_caps_jump() {
        // 2 replicas, no prior events, +2 pods/60s → limit 4 (the #1105 case).
        let b = HorizontalPodAutoscalerBehavior {
            scale_up: Some(rules("Max", vec![pods(2, 60)])),
            scale_down: None,
        };
        assert_eq!(
            convert_with_behavior_rate(2, 17, 1, 20, &b, &(vec![], vec![]), now()),
            4
        );
    }

    #[test]
    fn scale_up_percent_policy() {
        // 10 replicas, +100%/60s → limit 20; desired 50 capped to min(max=100,20)=20.
        let b = HorizontalPodAutoscalerBehavior {
            scale_up: Some(rules("Max", vec![percent(100, 60)])),
            scale_down: None,
        };
        assert_eq!(
            convert_with_behavior_rate(10, 50, 1, 100, &b, &(vec![], vec![]), now()),
            20
        );
    }

    #[test]
    fn select_policy_max_takes_most_permissive() {
        // Pods +1 vs Percent +100% on 4 replicas → max(5, 8) = 8.
        let b = HorizontalPodAutoscalerBehavior {
            scale_up: Some(rules("Max", vec![pods(1, 60), percent(100, 60)])),
            scale_down: None,
        };
        assert_eq!(
            convert_with_behavior_rate(4, 100, 1, 100, &b, &(vec![], vec![]), now()),
            8
        );
    }

    #[test]
    fn select_policy_min_takes_least_permissive() {
        let b = HorizontalPodAutoscalerBehavior {
            scale_up: Some(rules("Min", vec![pods(1, 60), percent(100, 60)])),
            scale_down: None,
        };
        assert_eq!(
            convert_with_behavior_rate(4, 100, 1, 100, &b, &(vec![], vec![]), now()),
            5
        );
    }

    #[test]
    fn disabled_policy_blocks_scale_up() {
        let b = HorizontalPodAutoscalerBehavior {
            scale_up: Some(rules("Disabled", vec![pods(10, 60)])),
            scale_down: None,
        };
        // limit == current → capped to current.
        assert_eq!(
            convert_with_behavior_rate(3, 17, 1, 20, &b, &(vec![], vec![]), now()),
            3
        );
    }

    #[test]
    fn prior_events_in_window_reduce_headroom() {
        // 4 replicas, +2/60s policy, but already +2 added this window → period
        // start was 2, limit 4 == current → no further scale up.
        let b = HorizontalPodAutoscalerBehavior {
            scale_up: Some(rules("Max", vec![pods(2, 60)])),
            scale_down: None,
        };
        let up = vec![ScaleEvent {
            change: 2,
            at: now() - Duration::seconds(10),
        }];
        assert_eq!(
            convert_with_behavior_rate(4, 17, 1, 20, &b, &(up, vec![]), now()),
            4
        );
    }

    #[test]
    fn scale_down_pods_policy_caps_drop() {
        // 10 replicas, -3 pods/60s → floor 7; desired 1 capped to max(min=1,7)=7.
        let b = HorizontalPodAutoscalerBehavior {
            scale_up: None,
            scale_down: Some(rules("Max", vec![pods(3, 60)])),
        };
        assert_eq!(
            convert_with_behavior_rate(10, 1, 1, 20, &b, &(vec![], vec![]), now()),
            7
        );
    }

    #[test]
    fn event_store_records_and_prunes() {
        let store = ScaleEventStore::default();
        let b = HorizontalPodAutoscalerBehavior {
            scale_up: Some(rules("Max", vec![pods(2, 60)])),
            scale_down: None,
        };
        // An old event (outside 60s) is pruned when a new one is recorded.
        store.record("ns/h", &b, 2, 3, now() - Duration::seconds(120));
        store.record("ns/h", &b, 3, 5, now());
        let (up, _down) = store.snapshot("ns/h");
        assert_eq!(up.len(), 1, "stale event pruned: {up:?}");
        assert_eq!(up[0].change, 2);
        assert_eq!(change_in_period(&up, 60, now()), 2);
    }

    #[test]
    fn stabilize_holds_recent_high_on_downscale() {
        // 8 replicas, metric now wants 2, but a recent recommendation of 8 is
        // within the 300s down-window → max(2,8)=8 → hold at 8.
        let recs = vec![(now() - Duration::seconds(30), 8)];
        assert_eq!(stabilize_recommendation(8, 2, 0, 300, &recs, now()), 8);
    }

    #[test]
    fn stabilize_allows_downscale_with_empty_history() {
        // No recent high → down_rec = desired → scale down proceeds.
        assert_eq!(stabilize_recommendation(8, 2, 0, 300, &[], now()), 2);
    }

    #[test]
    fn stabilize_holds_recent_low_on_upscale() {
        // 2 replicas, metric now wants 8, recent low of 2 within the up-window
        // → min(8,2)=2 → hold at 2 (don't chase a transient spike).
        let recs = vec![(now() - Duration::seconds(30), 2)];
        assert_eq!(stabilize_recommendation(2, 8, 300, 0, &recs, now()), 2);
    }

    #[test]
    fn stabilize_ignores_samples_outside_window() {
        // The high sample is older than the 300s down-window → not counted.
        let recs = vec![(now() - Duration::seconds(600), 8)];
        assert_eq!(stabilize_recommendation(8, 2, 0, 300, &recs, now()), 2);
    }

    #[test]
    fn recommendation_store_records_and_prunes() {
        let store = RecommendationStore::default();
        store.record("ns/h", 8, 300, now() - Duration::seconds(600));
        store.record("ns/h", 2, 300, now());
        let recs = store.snapshot("ns/h");
        assert_eq!(recs.len(), 1, "stale recommendation pruned: {recs:?}");
        assert_eq!(recs[0].1, 2);
    }
}
