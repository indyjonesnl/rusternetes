//! Unit tests moved from `src/validation/hpa.rs`.
#![allow(unused_imports, dead_code)]

mod __parent {
    pub use rusternetes_common::quantity::Quantity;
    pub use rusternetes_common::resources::autoscaling::Scale;
    pub use rusternetes_common::resources::autoscaling::{
        ContainerResourceMetricSource, CrossVersionObjectReference, ExternalMetricSource,
        HPAScalingPolicy, HPAScalingRules, HorizontalPodAutoscaler,
        HorizontalPodAutoscalerBehavior, HorizontalPodAutoscalerSpec, MetricIdentifier, MetricSpec,
        MetricTarget, ObjectMetricSource, PodsMetricSource, ResourceMetricSource,
    };
    pub use rusternetes_common::validation::field::{Error, ErrorList, Path};
    pub use rusternetes_common::validation::metav1::is_dns1123_label;
    pub use rusternetes_common::validation::objectmeta::name_is_path_segment;
}

mod tests {
    use super::__parent::*;
    use rusternetes_common::resources::autoscaling::HorizontalPodAutoscalerBehavior;
    use rusternetes_common::validation::field::ErrorType;
    use rusternetes_common::validation::hpa::*;
    use serde_json::json;

    fn hpa_spec(spec: serde_json::Value) -> HorizontalPodAutoscalerSpec {
        serde_json::from_value(spec).expect("spec deserializes")
    }

    fn errs_for(spec: serde_json::Value) -> ErrorList {
        validate_hpa_spec(&hpa_spec(spec), &Path::new("spec"))
    }

    fn has(errs: &ErrorList, field: &str, ty: ErrorType) -> bool {
        errs.iter().any(|e| e.field == field && e.error_type == ty)
    }

    fn target() -> serde_json::Value {
        json!({"kind": "Deployment", "name": "web", "apiVersion": "apps/v1"})
    }

    #[test]
    fn valid_resource_metric_passes() {
        let errs = errs_for(json!({
            "scaleTargetRef": target(),
            "minReplicas": 1,
            "maxReplicas": 10,
            "metrics": [{
                "type": "Resource",
                "resource": {"name": "cpu", "target": {"type": "Utilization", "averageUtilization": 80}}
            }]
        }));
        assert!(errs.is_empty(), "unexpected: {errs:?}");
    }

    #[test]
    fn metric_type_required() {
        let errs = errs_for(json!({
            "scaleTargetRef": target(),
            "maxReplicas": 5,
            "metrics": [{"type": ""}]
        }));
        assert!(
            has(&errs, "spec.metrics[0].type", ErrorType::Required),
            "got: {errs:?}"
        );
    }

    #[test]
    fn metric_type_invalid_enum_not_supported() {
        let errs = errs_for(json!({
            "scaleTargetRef": target(),
            "maxReplicas": 5,
            "metrics": [{"type": "Bogus"}]
        }));
        assert!(
            has(&errs, "spec.metrics[0].type", ErrorType::NotSupported),
            "got: {errs:?}"
        );
    }

    #[test]
    fn metric_source_must_match_type() {
        // type=Pods but only `resource` populated. Upstream: typesPresent={resource}
        // has len 1, so the forbidden loop is skipped — only required(pods) fires.
        let errs = errs_for(json!({
            "scaleTargetRef": target(),
            "maxReplicas": 5,
            "metrics": [{
                "type": "Pods",
                "resource": {"name": "cpu", "target": {"type": "Utilization", "averageUtilization": 80}}
            }]
        }));
        assert!(
            has(&errs, "spec.metrics[0].pods", ErrorType::Required),
            "got: {errs:?}"
        );
        assert!(
            !has(&errs, "spec.metrics[0].resource", ErrorType::Forbidden),
            "single source present must not be forbidden; got: {errs:?}"
        );
    }

    #[test]
    fn two_sources_forbidden() {
        let errs = errs_for(json!({
            "scaleTargetRef": target(),
            "maxReplicas": 5,
            "metrics": [{
                "type": "Resource",
                "resource": {"name": "cpu", "target": {"type": "Utilization", "averageUtilization": 80}},
                "pods": {"metric": {"name": "qps"}, "target": {"type": "AverageValue", "averageValue": "1"}}
            }]
        }));
        // resource matches type → pods is the extra one and is forbidden.
        assert!(
            has(&errs, "spec.metrics[0].pods", ErrorType::Forbidden),
            "got: {errs:?}"
        );
    }

    #[test]
    fn resource_source_requires_value_or_utilization() {
        let errs = errs_for(json!({
            "scaleTargetRef": target(),
            "maxReplicas": 5,
            "metrics": [{
                "type": "Resource",
                "resource": {"name": "cpu", "target": {"type": "Utilization"}}
            }]
        }));
        assert!(
            has(
                &errs,
                "spec.metrics[0].resource.target.averageUtilization",
                ErrorType::Required
            ),
            "got: {errs:?}"
        );
    }

    #[test]
    fn metric_target_type_enum_checked() {
        let errs = errs_for(json!({
            "scaleTargetRef": target(),
            "maxReplicas": 5,
            "metrics": [{
                "type": "Pods",
                "pods": {"metric": {"name": "qps"}, "target": {"type": "Bogus", "averageValue": "1"}}
            }]
        }));
        assert!(
            has(
                &errs,
                "spec.metrics[0].pods.target.type",
                ErrorType::Invalid
            ),
            "got: {errs:?}"
        );
    }

    #[test]
    fn metric_target_value_must_be_positive() {
        let errs = errs_for(json!({
            "scaleTargetRef": target(),
            "maxReplicas": 5,
            "metrics": [{
                "type": "Object",
                "object": {
                    "describedObject": {"kind": "Service", "name": "svc", "apiVersion": "v1"},
                    "metric": {"name": "requests"},
                    "target": {"type": "Value", "value": "0"}
                }
            }]
        }));
        assert!(
            has(
                &errs,
                "spec.metrics[0].object.target.value",
                ErrorType::Invalid
            ),
            "got: {errs:?}"
        );
    }

    #[test]
    fn metric_target_average_utilization_must_be_at_least_one() {
        let errs = errs_for(json!({
            "scaleTargetRef": target(),
            "maxReplicas": 5,
            "metrics": [{
                "type": "Resource",
                "resource": {"name": "cpu", "target": {"type": "Utilization", "averageUtilization": 0}}
            }]
        }));
        assert!(
            has(
                &errs,
                "spec.metrics[0].resource.target.averageUtilization",
                ErrorType::Invalid
            ),
            "got: {errs:?}"
        );
    }

    #[test]
    fn metric_identifier_name_required() {
        let errs = errs_for(json!({
            "scaleTargetRef": target(),
            "maxReplicas": 5,
            "metrics": [{
                "type": "Pods",
                "pods": {"metric": {"name": ""}, "target": {"type": "AverageValue", "averageValue": "1"}}
            }]
        }));
        assert!(
            has(
                &errs,
                "spec.metrics[0].pods.metric.name",
                ErrorType::Required
            ),
            "got: {errs:?}"
        );
    }

    #[test]
    fn metric_identifier_name_path_segment() {
        let errs = errs_for(json!({
            "scaleTargetRef": target(),
            "maxReplicas": 5,
            "metrics": [{
                "type": "Pods",
                "pods": {"metric": {"name": "bad/name"}, "target": {"type": "AverageValue", "averageValue": "1"}}
            }]
        }));
        assert!(
            has(
                &errs,
                "spec.metrics[0].pods.metric.name",
                ErrorType::Invalid
            ),
            "got: {errs:?}"
        );
    }

    #[test]
    fn scale_target_ref_kind_path_segment() {
        let errs = errs_for(json!({
            "scaleTargetRef": {"kind": "bad/kind", "name": "web", "apiVersion": "apps/v1"},
            "maxReplicas": 5
        }));
        assert!(
            has(&errs, "spec.scaleTargetRef.kind", ErrorType::Invalid),
            "got: {errs:?}"
        );
    }

    #[test]
    fn scale_target_ref_requires_api_group() {
        // apiVersion "v1" → empty group; non-RC kind → must specify API group.
        let errs = errs_for(json!({
            "scaleTargetRef": {"kind": "Deployment", "name": "web", "apiVersion": "v1"},
            "maxReplicas": 5
        }));
        assert!(
            has(&errs, "spec.scaleTargetRef.apiVersion", ErrorType::Invalid),
            "got: {errs:?}"
        );
    }

    #[test]
    fn replication_controller_allows_empty_api_group() {
        let errs = errs_for(json!({
            "scaleTargetRef": {"kind": "ReplicationController", "name": "web", "apiVersion": "v1"},
            "maxReplicas": 5
        }));
        assert!(
            !has(&errs, "spec.scaleTargetRef.apiVersion", ErrorType::Invalid),
            "got: {errs:?}"
        );
    }

    #[test]
    fn malformed_api_version_rejected() {
        let errs = errs_for(json!({
            "scaleTargetRef": {"kind": "Deployment", "name": "web", "apiVersion": "a/b/c"},
            "maxReplicas": 5
        }));
        assert!(
            has(&errs, "spec.scaleTargetRef.apiVersion", ErrorType::Invalid),
            "got: {errs:?}"
        );
    }

    #[test]
    fn scale_to_zero_requires_object_or_external_metric() {
        let errs = errs_for(json!({
            "scaleTargetRef": target(),
            "minReplicas": 0,
            "maxReplicas": 5,
            "metrics": [{
                "type": "Resource",
                "resource": {"name": "cpu", "target": {"type": "Utilization", "averageUtilization": 80}}
            }]
        }));
        assert!(
            has(&errs, "spec.metrics", ErrorType::Forbidden),
            "got: {errs:?}"
        );
    }

    #[test]
    fn scale_to_zero_allowed_with_external_metric() {
        let errs = errs_for(json!({
            "scaleTargetRef": target(),
            "minReplicas": 0,
            "maxReplicas": 5,
            "metrics": [{
                "type": "External",
                "external": {"metric": {"name": "queue"}, "target": {"type": "Value", "value": "10"}}
            }]
        }));
        assert!(
            !has(&errs, "spec.metrics", ErrorType::Forbidden),
            "got: {errs:?}"
        );
        // minReplicas==0 with lower bound 1 still triggers the bound error.
        assert!(
            has(&errs, "spec.minReplicas", ErrorType::Invalid),
            "got: {errs:?}"
        );
    }

    #[test]
    fn behavior_select_policy_enum_checked() {
        let mut spec = hpa_spec(json!({
            "scaleTargetRef": target(),
            "maxReplicas": 5
        }));
        spec.behavior = Some(HorizontalPodAutoscalerBehavior {
            scale_up: Some(
                serde_json::from_value(json!({
                    "selectPolicy": "Bogus",
                    "policies": [{"type": "Pods", "value": 1, "periodSeconds": 60}]
                }))
                .unwrap(),
            ),
            scale_down: None,
        });
        let errs = validate_hpa_spec(&spec, &Path::new("spec"));
        assert!(
            has(
                &errs,
                "spec.behavior.scaleUp.selectPolicy",
                ErrorType::NotSupported
            ),
            "got: {errs:?}"
        );
    }

    #[test]
    fn behavior_requires_at_least_one_policy() {
        let mut spec = hpa_spec(json!({"scaleTargetRef": target(), "maxReplicas": 5}));
        spec.behavior = Some(HorizontalPodAutoscalerBehavior {
            scale_up: Some(serde_json::from_value(json!({"policies": []})).unwrap()),
            scale_down: None,
        });
        let errs = validate_hpa_spec(&spec, &Path::new("spec"));
        assert!(
            has(&errs, "spec.behavior.scaleUp.policies", ErrorType::Required),
            "got: {errs:?}"
        );
    }

    #[test]
    fn behavior_policy_value_and_period_checked() {
        let mut spec = hpa_spec(json!({"scaleTargetRef": target(), "maxReplicas": 5}));
        spec.behavior = Some(HorizontalPodAutoscalerBehavior {
            scale_down: Some(
                serde_json::from_value(json!({
                    "policies": [{"type": "Percent", "value": 0, "periodSeconds": 0}]
                }))
                .unwrap(),
            ),
            scale_up: None,
        });
        let errs = validate_hpa_spec(&spec, &Path::new("spec"));
        assert!(
            has(
                &errs,
                "spec.behavior.scaleDown.policies[0].value",
                ErrorType::Invalid
            ),
            "got: {errs:?}"
        );
        assert!(
            has(
                &errs,
                "spec.behavior.scaleDown.policies[0].periodSeconds",
                ErrorType::Invalid
            ),
            "got: {errs:?}"
        );
    }

    #[test]
    fn behavior_policy_period_upper_bound() {
        let mut spec = hpa_spec(json!({"scaleTargetRef": target(), "maxReplicas": 5}));
        spec.behavior = Some(HorizontalPodAutoscalerBehavior {
            scale_up: Some(
                serde_json::from_value(json!({
                    "policies": [{"type": "Pods", "value": 4, "periodSeconds": 1801}]
                }))
                .unwrap(),
            ),
            scale_down: None,
        });
        let errs = validate_hpa_spec(&spec, &Path::new("spec"));
        assert!(
            has(
                &errs,
                "spec.behavior.scaleUp.policies[0].periodSeconds",
                ErrorType::Invalid
            ),
            "got: {errs:?}"
        );
    }

    #[test]
    fn behavior_stabilization_window_bounds() {
        let mut spec = hpa_spec(json!({"scaleTargetRef": target(), "maxReplicas": 5}));
        spec.behavior = Some(HorizontalPodAutoscalerBehavior {
            scale_up: Some(
                serde_json::from_value(json!({
                    "stabilizationWindowSeconds": 3601,
                    "policies": [{"type": "Pods", "value": 4, "periodSeconds": 60}]
                }))
                .unwrap(),
            ),
            scale_down: None,
        });
        let errs = validate_hpa_spec(&spec, &Path::new("spec"));
        assert!(
            has(
                &errs,
                "spec.behavior.scaleUp.stabilizationWindowSeconds",
                ErrorType::Invalid
            ),
            "got: {errs:?}"
        );
    }

    fn hpa(spec: serde_json::Value) -> HorizontalPodAutoscaler {
        serde_json::from_value(json!({
            "apiVersion": "autoscaling/v2",
            "kind": "HorizontalPodAutoscaler",
            "metadata": {"name": "h", "namespace": "default"},
            "spec": spec,
        }))
        .expect("hpa deserializes")
    }

    fn update_errs(new: serde_json::Value, old: serde_json::Value) -> ErrorList {
        validate_horizontal_pod_autoscaler_update(&hpa(new), &hpa(old))
    }

    #[test]
    fn update_unchanged_target_skips_apiversion_validation() {
        // Old object already carries a group-less apiVersion on its target. An
        // update that leaves kind/apiVersion unchanged must not be rejected for
        // it (upstream skips apiVersion validation on unchanged kind).
        let bad_target = json!({"kind": "Foo", "name": "x", "apiVersion": "v1"});
        let spec = json!({
            "scaleTargetRef": bad_target,
            "minReplicas": 1,
            "maxReplicas": 10,
        });
        let errs = update_errs(spec.clone(), spec);
        assert!(errs.is_empty(), "unexpected: {errs:?}");
    }

    #[test]
    fn update_changed_target_revalidates_apiversion() {
        let old = json!({
            "scaleTargetRef": {"kind": "Deployment", "name": "x", "apiVersion": "apps/v1"},
            "minReplicas": 1,
            "maxReplicas": 10,
        });
        // New target switches to a group-less apiVersion — kind/apiVersion
        // changed, so apiVersion validation runs and rejects it.
        let new = json!({
            "scaleTargetRef": {"kind": "Foo", "name": "x", "apiVersion": "v1"},
            "minReplicas": 1,
            "maxReplicas": 10,
        });
        let errs = update_errs(new, old);
        assert!(
            has(&errs, "spec.scaleTargetRef.apiVersion", ErrorType::Invalid),
            "got: {errs:?}"
        );
    }

    #[test]
    fn update_keeps_zero_min_replicas_when_old_was_zero() {
        // Old already had minReplicas: 0 with an object metric — an update that
        // keeps 0 stays valid even with the scale-to-zero gate off.
        let object_metric = json!({
            "type": "Object",
            "object": {
                "describedObject": {"kind": "Service", "name": "s", "apiVersion": "v1"},
                "metric": {"name": "rps"},
                "target": {"type": "Value", "value": "100"}
            }
        });
        let old = json!({
            "scaleTargetRef": {"kind": "Deployment", "name": "x", "apiVersion": "apps/v1"},
            "minReplicas": 0,
            "maxReplicas": 10,
            "metrics": [object_metric.clone()],
        });
        let new = json!({
            "scaleTargetRef": {"kind": "Deployment", "name": "x", "apiVersion": "apps/v1"},
            "minReplicas": 0,
            "maxReplicas": 10,
            "metrics": [object_metric],
        });
        let errs = update_errs(new, old);
        assert!(errs.is_empty(), "unexpected: {errs:?}");
    }

    #[test]
    fn update_rejects_zero_min_replicas_when_old_was_nonzero() {
        let old = json!({
            "scaleTargetRef": {"kind": "Deployment", "name": "x", "apiVersion": "apps/v1"},
            "minReplicas": 1,
            "maxReplicas": 10,
        });
        let object_metric = json!({
            "type": "Object",
            "object": {
                "describedObject": {"kind": "Service", "name": "s", "apiVersion": "v1"},
                "metric": {"name": "rps"},
                "target": {"type": "Value", "value": "100"}
            }
        });
        let new = json!({
            "scaleTargetRef": {"kind": "Deployment", "name": "x", "apiVersion": "apps/v1"},
            "minReplicas": 0,
            "maxReplicas": 10,
            "metrics": [object_metric],
        });
        let errs = update_errs(new, old);
        assert!(
            has(&errs, "spec.minReplicas", ErrorType::Invalid),
            "got: {errs:?}"
        );
    }
}
