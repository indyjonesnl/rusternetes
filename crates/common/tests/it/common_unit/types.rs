//! Unit tests moved from `src/types.rs`.
#![allow(unused_imports, dead_code)]

mod __parent {
    pub use chrono::{DateTime, Utc};
    pub use rusternetes_common::serde_helpers::go_zero_time;
    pub use rusternetes_common::serde_helpers::{
        k8s_float, k8s_micro_time, k8s_micro_time_required, k8s_time, k8s_time_map,
        k8s_time_required, k8s_time_str, sorted_opt_map,
    };
    pub use serde::{Deserialize, Deserializer, Serialize};
    pub use std::collections::HashMap;
}

mod tests {
    #[test]
    fn required_times_accept_null_and_emit_null_for_zero() {
        // Time.UnmarshalJSON time.go:100-104; MarshalJSON time.go:162-166.
        #[derive(serde::Serialize, serde::Deserialize, Debug, PartialEq)]
        struct W {
            #[serde(with = "rusternetes_common::types::k8s_time_required")]
            t: chrono::DateTime<chrono::Utc>,
            #[serde(with = "rusternetes_common::types::k8s_micro_time_required")]
            m: chrono::DateTime<chrono::Utc>,
        }
        let w: W = serde_json::from_str(r#"{"t":null,"m":null}"#).unwrap();
        assert_eq!(w.t, go_zero_time());
        assert_eq!(w.m, go_zero_time());
        assert_eq!(serde_json::to_string(&w).unwrap(), r#"{"t":null,"m":null}"#);
        let w: W = serde_json::from_str(
            r#"{"t":"2024-01-02T03:04:05Z","m":"2024-01-02T03:04:05.123456Z"}"#,
        )
        .unwrap();
        assert_eq!(
            serde_json::to_string(&w).unwrap(),
            r#"{"t":"2024-01-02T03:04:05Z","m":"2024-01-02T03:04:05.123456Z"}"#
        );
    }

    use super::__parent::*;
    use rusternetes_common::types::*;

    /// metav1.ObjectMeta: `Name string `json:"name,omitempty"``
    /// (staging/src/k8s.io/apimachinery/pkg/apis/meta/v1/types.go) — an empty
    /// name is dropped from the wire form (#2942).
    #[test]
    fn empty_object_meta_name_is_omitted_from_json() {
        let v = serde_json::to_value(ObjectMeta::default()).unwrap();
        assert!(v.get("name").is_none(), "got {v}");
        let v = serde_json::to_value(ObjectMeta::new("x")).unwrap();
        assert_eq!(v["name"], "x");
    }

    fn sel(json: serde_json::Value) -> LabelSelector {
        serde_json::from_value(json).unwrap()
    }

    fn labels(pairs: &[(&str, &str)]) -> HashMap<String, String> {
        pairs
            .iter()
            .map(|(k, v)| (k.to_string(), v.to_string()))
            .collect()
    }

    /// The three answers of `LabelSelectorAsSelector` (`helpers.go:36-42`).
    /// Absent is `labels.Nothing()`; `{}` is `labels.Everything()`. Collapsing
    /// the two made a `{}` selector match nothing, which is what #2012 was.
    #[test]
    fn an_absent_selector_matches_nothing_and_an_empty_one_matches_everything() {
        assert_eq!(label_selector_as_selector(None), Ok(Selector::Nothing));
        assert!(!Selector::Nothing.matches(Some(&labels(&[("a", "b")]))));

        for empty in [LabelSelector::default(), sel(serde_json::json!({}))] {
            assert_eq!(
                label_selector_as_selector(Some(&empty)),
                Ok(Selector::Everything)
            );
            assert!(empty.matches_labels(&labels(&[("a", "b")])));
            // Including a pod with no labels at all.
            assert!(empty.matches_labels(&HashMap::new()));
        }

        // A selector whose maps are present but empty is still `{}`.
        let explicitly_empty = sel(serde_json::json!({
            "matchLabels": {}, "matchExpressions": []
        }));
        assert_eq!(
            label_selector_as_selector(Some(&explicitly_empty)),
            Ok(Selector::Everything)
        );
    }

    /// Upstream returns `"%q is not a valid label selector operator"` rather
    /// than a requirement that never matches, and each caller decides what that
    /// means. `matches_labels` is the "does not match" reading.
    #[test]
    fn an_unknown_operator_is_an_error_not_a_silent_non_match() {
        let s = sel(serde_json::json!({
            "matchExpressions": [{"key": "a", "operator": "Greater", "values": ["1"]}]
        }));
        assert_eq!(
            label_selector_as_selector(Some(&s)),
            Err("\"Greater\" is not a valid label selector operator".to_string())
        );
        assert!(!s.matches_labels(&labels(&[("a", "b")])));
    }

    /// `TestLabelSelectorAsSelector` (`helpers_test.go:31-90`) pins that an
    /// `Exists` requirement carrying values is an error; the rest of
    /// `NewRequirement` (`labels/selector.go:185-225`) refuses an invalid key,
    /// an invalid value and an empty `In`/`NotIn` set the same way.
    #[test]
    fn a_requirement_newrequirement_refuses_is_an_error() {
        for bad in [
            // Exists with values (helpers_test.go case 6).
            serde_json::json!({"matchExpressions": [{"key": "baz", "operator": "Exists", "values": ["qux", "norf"]}]}),
            serde_json::json!({"matchExpressions": [{"key": "baz", "operator": "DoesNotExist", "values": ["x"]}]}),
            // Empty In / NotIn set.
            serde_json::json!({"matchExpressions": [{"key": "baz", "operator": "In"}]}),
            serde_json::json!({"matchExpressions": [{"key": "baz", "operator": "NotIn", "values": []}]}),
            // Invalid key.
            serde_json::json!({"matchExpressions": [{"key": "not a key!", "operator": "Exists"}]}),
            serde_json::json!({"matchLabels": {"bad key": "v"}}),
            // Invalid value.
            serde_json::json!({"matchLabels": {"app": "not valid!"}}),
            serde_json::json!({"matchExpressions": [{"key": "a", "operator": "In", "values": ["-bad"]}]}),
        ] {
            let s = sel(bad.clone());
            assert!(
                label_selector_as_selector(Some(&s)).is_err(),
                "{bad} must be refused"
            );
            // Eviction reads this as "does not match".
            assert!(!s.matches_labels(&labels(&[("app", "web"), ("baz", "x")])));
        }

        let good = sel(serde_json::json!({
            "matchLabels": {"foo": "bar"},
            "matchExpressions": [{"key": "baz", "operator": "In", "values": ["qux", "norf"]}]
        }));
        assert!(label_selector_as_selector(Some(&good)).is_ok());
    }

    /// #2923: `label_selector_as_selector` validates without rendering the
    /// selector string; the refusals must stay exactly `as_selector_string`'s.
    #[test]
    fn validate_requirements_reports_what_as_selector_string_reports() {
        for v in [
            serde_json::json!({"matchExpressions": [{"key": "baz", "operator": "Exists", "values": ["q"]}]}),
            serde_json::json!({"matchExpressions": [{"key": "baz", "operator": "In"}]}),
            serde_json::json!({"matchExpressions": [{"key": "a", "operator": "Bogus"}]}),
            serde_json::json!({"matchLabels": {"bad key": "not valid!"}}),
            serde_json::json!({"matchLabels": {"app": "web"}, "matchExpressions": [{"key": "b", "operator": "NotIn", "values": ["x"]}]}),
            serde_json::json!({}),
        ] {
            let s = sel(v.clone());
            assert_eq!(
                s.validate_requirements(),
                s.as_selector_string().map(drop),
                "{v}"
            );
        }
    }

    #[test]
    fn matches_labels_match_labels() {
        let s = sel(serde_json::json!({"matchLabels": {"app": "web"}}));
        assert!(s.matches_labels(&labels(&[("app", "web"), ("x", "y")])));
        assert!(!s.matches_labels(&labels(&[("app", "db")])));
        assert!(!s.matches_labels(&labels(&[("x", "y")])));
    }

    #[test]
    fn matches_labels_match_expressions() {
        // In
        let s = sel(serde_json::json!({
            "matchExpressions": [{"key": "tier", "operator": "In", "values": ["fe", "be"]}]
        }));
        assert!(s.matches_labels(&labels(&[("tier", "fe")])));
        assert!(!s.matches_labels(&labels(&[("tier", "cache")])));
        assert!(!s.matches_labels(&labels(&[("other", "x")])));

        // Exists / DoesNotExist
        let exists = sel(serde_json::json!({
            "matchExpressions": [{"key": "tier", "operator": "Exists"}]
        }));
        assert!(exists.matches_labels(&labels(&[("tier", "fe")])));
        assert!(!exists.matches_labels(&labels(&[("other", "x")])));

        let dne = sel(serde_json::json!({
            "matchExpressions": [{"key": "tier", "operator": "DoesNotExist"}]
        }));
        assert!(dne.matches_labels(&labels(&[("other", "x")])));
        assert!(!dne.matches_labels(&labels(&[("tier", "fe")])));

        // NotIn (also matches when key absent)
        let notin = sel(serde_json::json!({
            "matchExpressions": [{"key": "tier", "operator": "NotIn", "values": ["fe"]}]
        }));
        assert!(notin.matches_labels(&labels(&[("tier", "be")])));
        assert!(notin.matches_labels(&labels(&[("other", "x")])));
        assert!(!notin.matches_labels(&labels(&[("tier", "fe")])));
    }

    #[test]
    fn matches_labels_both_must_hold() {
        let s = sel(serde_json::json!({
            "matchLabels": {"app": "web"},
            "matchExpressions": [{"key": "tier", "operator": "Exists"}]
        }));
        assert!(s.matches_labels(&labels(&[("app", "web"), ("tier", "fe")])));
        assert!(!s.matches_labels(&labels(&[("app", "web")])));
        assert!(!s.matches_labels(&labels(&[("tier", "fe")])));
    }

    #[test]
    fn test_status_failure() {
        let status = Status::failure("pod not found", "NotFound", 404);
        assert_eq!(status.kind, "Status");
        assert_eq!(status.api_version, "v1");
        assert_eq!(status.status, Some("Failure".to_string()));
        assert_eq!(status.message, Some("pod not found".to_string()));
        assert_eq!(status.reason, Some("NotFound".to_string()));
        assert_eq!(status.code, Some(404));
        assert!(status.details.is_none());
    }

    #[test]
    fn test_status_failure_with_details() {
        let details = StatusDetails {
            name: Some("my-pod".to_string()),
            group: None,
            kind: Some("Pod".to_string()),
            uid: None,
            causes: Some(vec![StatusCause {
                reason: Some("FieldValueInvalid".to_string()),
                message: Some("spec.containers[0].image is required".to_string()),
                field: Some("spec.containers[0].image".to_string()),
            }]),
            retry_after_seconds: None,
        };

        let status =
            Status::failure_with_details("Pod \"my-pod\" is invalid", "Invalid", 422, details);

        assert_eq!(status.reason, Some("Invalid".to_string()));
        assert_eq!(status.code, Some(422));
        let d = status.details.unwrap();
        assert_eq!(d.name, Some("my-pod".to_string()));
        assert_eq!(d.kind, Some("Pod".to_string()));
        assert_eq!(d.causes.as_ref().unwrap().len(), 1);
        assert_eq!(
            d.causes.unwrap()[0].field,
            Some("spec.containers[0].image".to_string())
        );
    }

    #[test]
    fn test_status_success() {
        let status = Status::success();
        assert_eq!(status.status, Some("Success".to_string()));
        assert_eq!(status.code, Some(200));
        assert!(status.reason.is_none());
    }

    #[test]
    fn test_status_serialization() {
        let status = Status::failure("resource not found", "NotFound", 404);
        let json = serde_json::to_string(&status).unwrap();

        assert!(json.contains("\"kind\":\"Status\""));
        assert!(json.contains("\"apiVersion\":\"v1\""));
        assert!(json.contains("\"status\":\"Failure\""));
        assert!(json.contains("\"reason\":\"NotFound\""));
        assert!(json.contains("\"code\":404"));

        let deserialized: Status = serde_json::from_str(&json).unwrap();
        assert_eq!(deserialized, status);
    }

    #[test]
    fn test_status_deserialization_from_kubernetes() {
        // Simulate a Status response as returned by a real Kubernetes API
        let json = r#"{
            "kind": "Status",
            "apiVersion": "v1",
            "metadata": {},
            "status": "Failure",
            "message": "pods \"nonexistent\" not found",
            "reason": "NotFound",
            "details": {
                "name": "nonexistent",
                "kind": "pods"
            },
            "code": 404
        }"#;

        let status: Status = serde_json::from_str(json).unwrap();
        assert_eq!(status.reason, Some("NotFound".to_string()));
        assert_eq!(status.code, Some(404));
        let d = status.details.unwrap();
        assert_eq!(d.name, Some("nonexistent".to_string()));
    }

    #[test]
    fn test_condition_serialization() {
        let condition = Condition {
            condition_type: "Ready".to_string(),
            status: "True".to_string(),
            observed_generation: Some(5),
            last_transition_time: None,
            reason: Some("PodReady".to_string()),
            message: Some("Pod is ready".to_string()),
        };

        let json = serde_json::to_string(&condition).unwrap();
        assert!(json.contains("\"type\":\"Ready\""));
        assert!(json.contains("\"observedGeneration\":5"));

        let deserialized: Condition = serde_json::from_str(&json).unwrap();
        assert_eq!(deserialized.condition_type, "Ready");
        assert_eq!(deserialized.observed_generation, Some(5));
    }

    #[test]
    fn test_list_meta_with_pagination() {
        let meta = ListMeta {
            resource_version: Some("12345".to_string()),
            continue_token: Some("abc123".to_string()),
            remaining_item_count: Some(42),
        };

        let json = serde_json::to_string(&meta).unwrap();
        assert!(json.contains("\"continue\":\"abc123\""));
        assert!(json.contains("\"remainingItemCount\":42"));
    }

    #[test]
    fn unset_creation_timestamp_is_omitted_like_omitzero() {
        // meta/v1/types.go:188 `omitempty,omitzero` on a `Time` value.
        let mut meta = ObjectMeta::new("x");
        meta.creation_timestamp = None;
        let v = serde_json::to_value(&meta).unwrap();
        assert!(v.get("creationTimestamp").is_none());
        // and an explicit null on the wire decodes to None
        let m: ObjectMeta =
            serde_json::from_str(r#"{"name":"x","creationTimestamp":null}"#).unwrap();
        assert!(m.creation_timestamp.is_none());
    }

    #[test]
    fn test_creation_timestamp_serializes_at_second_precision() {
        use chrono::{DateTime, Utc};
        // Upstream metav1.Time.MarshalJSON formats at SECOND precision
        // (time.RFC3339, no fractional seconds). A client may *send* sub-second
        // precision, but we must serialize responses at second precision to
        // match k8s — emitting nanoseconds is a non-conformance and breaks
        // proto Timestamp round-tripping.
        let ts: DateTime<Utc> = "2026-03-29T21:05:27.270173921Z".parse().unwrap();
        let meta = ObjectMeta {
            name: "test".to_string(),
            creation_timestamp: Some(ts),
            ..Default::default()
        };
        let json = serde_json::to_string(&meta).unwrap();
        assert!(
            json.contains("\"2026-03-29T21:05:27Z\""),
            "creationTimestamp must serialize at second precision: {}",
            json
        );
        assert!(
            !json.contains(".270"),
            "must not emit sub-second precision: {}",
            json
        );
        // Lenient deserialize still accepts sub-second input; re-serialize is stable.
        let meta2: ObjectMeta = serde_json::from_str(&json).unwrap();
        assert_eq!(json, serde_json::to_string(&meta2).unwrap());
    }

    /// Regression: kubectl / client-go send `status: { phase: "" }` on CREATE
    /// for namespace/pod/etc. — server is meant to assign the phase. Without
    /// `#[serde(alias = "")]` on `Phase::Unknown` the api-server returned 422
    /// before any handler ran and `kubectl create ns <X>` was uniformly broken,
    /// taking hydrophone + sonobuoy down with it.
    #[test]
    fn phase_accepts_empty_string_on_input() {
        let p: Phase = serde_json::from_str(r#""""#).unwrap();
        assert_eq!(p, Phase::Unknown);
    }

    #[test]
    fn phase_named_variants_still_deserialize() {
        for (input, expected) in [
            (r#""Pending""#, Phase::Pending),
            (r#""Running""#, Phase::Running),
            (r#""Active""#, Phase::Active),
            (r#""Terminating""#, Phase::Terminating),
            (r#""Unknown""#, Phase::Unknown),
        ] {
            let parsed: Phase = serde_json::from_str(input).unwrap();
            assert_eq!(
                parsed, expected,
                "input {} should yield {:?}",
                input, expected
            );
        }
    }

    /// Output wire format must NOT change: `Phase::Unknown` still serializes as
    /// `"Unknown"`, not the empty string. The alias is an input-only synonym so
    /// upstream-shape persistence is preserved.
    #[test]
    fn phase_unknown_serializes_as_unknown_not_empty() {
        let json = serde_json::to_string(&Phase::Unknown).unwrap();
        assert_eq!(json, r#""Unknown""#);
    }

    /// Genuinely invalid variants must still fail loudly — the alias is only
    /// for the empty string, not a catch-all.
    #[test]
    fn phase_rejects_unknown_non_empty_variants() {
        let r: Result<Phase, _> = serde_json::from_str(r#""NotAPhase""#);
        assert!(r.is_err(), "unknown non-empty variant must error");
    }
}
