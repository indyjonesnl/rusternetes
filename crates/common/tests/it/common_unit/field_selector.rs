//! Unit tests moved from `src/field_selector.rs`.
#![allow(unused_imports, dead_code)]

mod __parent {
    pub use serde_json::Value;
    pub use std::fmt;
}

mod tests {
    use super::__parent::*;
    use rusternetes_common::field_selector::*;
    use serde_json::json;

    #[test]
    fn test_parse_single_requirement() {
        let selector = FieldSelector::parse("status.phase=Running").unwrap();
        assert_eq!(selector.requirements.len(), 1);
        assert_eq!(selector.requirements[0].field, "status.phase");
        assert_eq!(selector.requirements[0].operator, FieldOperator::Equals);
        assert_eq!(selector.requirements[0].value, "Running");
    }

    #[test]
    fn test_parse_multiple_requirements() {
        let selector = FieldSelector::parse("status.phase=Running,spec.nodeName=node-1").unwrap();
        assert_eq!(selector.requirements.len(), 2);
        assert_eq!(selector.requirements[0].field, "status.phase");
        assert_eq!(selector.requirements[1].field, "spec.nodeName");
    }

    #[test]
    fn test_parse_not_equals() {
        let selector = FieldSelector::parse("status.phase!=Failed").unwrap();
        assert_eq!(selector.requirements[0].operator, FieldOperator::NotEquals);
        assert_eq!(selector.requirements[0].value, "Failed");
    }

    #[test]
    fn test_parse_double_equals() {
        let selector = FieldSelector::parse("status.phase==Running").unwrap();
        assert_eq!(selector.requirements[0].operator, FieldOperator::Equals);
    }

    #[test]
    fn test_matches_simple() {
        let selector = FieldSelector::parse("status.phase=Running").unwrap();
        let resource = json!({
            "status": {
                "phase": "Running"
            }
        });

        assert!(selector.matches(&resource));
    }

    #[test]
    fn test_matches_nested() {
        let selector = FieldSelector::parse("spec.nodeName=node-1").unwrap();
        let resource = json!({
            "spec": {
                "nodeName": "node-1"
            }
        });

        assert!(selector.matches(&resource));
    }

    #[test]
    fn test_not_matches() {
        let selector = FieldSelector::parse("status.phase=Running").unwrap();
        let resource = json!({
            "status": {
                "phase": "Pending"
            }
        });

        assert!(!selector.matches(&resource));
    }

    #[test]
    fn test_not_equals_matches() {
        let selector = FieldSelector::parse("status.phase!=Failed").unwrap();
        let resource = json!({
            "status": {
                "phase": "Running"
            }
        });

        assert!(selector.matches(&resource));
    }

    #[test]
    fn test_not_equals_not_matches() {
        let selector = FieldSelector::parse("status.phase!=Failed").unwrap();
        let resource = json!({
            "status": {
                "phase": "Failed"
            }
        });

        assert!(!selector.matches(&resource));
    }

    #[test]
    fn test_multiple_requirements_all_match() {
        let selector = FieldSelector::parse("status.phase=Running,spec.nodeName=node-1").unwrap();
        let resource = json!({
            "status": {
                "phase": "Running"
            },
            "spec": {
                "nodeName": "node-1"
            }
        });

        assert!(selector.matches(&resource));
    }

    #[test]
    fn test_multiple_requirements_one_fails() {
        let selector = FieldSelector::parse("status.phase=Running,spec.nodeName=node-1").unwrap();
        let resource = json!({
            "status": {
                "phase": "Running"
            },
            "spec": {
                "nodeName": "node-2"
            }
        });

        assert!(!selector.matches(&resource));
    }

    #[test]
    fn test_field_not_found() {
        let selector = FieldSelector::parse("status.phase=Running").unwrap();
        let resource = json!({
            "metadata": {
                "name": "test"
            }
        });

        assert!(!selector.matches(&resource));
    }

    #[test]
    fn test_empty_selector_matches_all() {
        let selector = FieldSelector::parse("").unwrap();
        let resource = json!({"any": "value"});

        assert!(selector.matches(&resource));
        assert!(selector.is_empty());
    }

    #[test]
    fn test_helper_pod_phase() {
        let selector = FieldSelector::pod_phase("Running");
        let resource = json!({
            "status": {
                "phase": "Running"
            }
        });

        assert!(selector.matches(&resource));
    }

    #[test]
    fn test_helper_namespace() {
        let selector = FieldSelector::namespace("default");
        let resource = json!({
            "metadata": {
                "namespace": "default"
            }
        });

        assert!(selector.matches(&resource));
    }

    #[test]
    fn test_display() {
        let selector = FieldSelector::parse("status.phase=Running,spec.nodeName!=node-1").unwrap();
        let displayed = format!("{}", selector);
        assert!(displayed.contains("status.phase=Running"));
        assert!(displayed.contains("spec.nodeName!=node-1"));
    }

    #[test]
    fn test_extract_string_field() {
        let resource = json!({
            "metadata": {
                "name": "test-pod"
            }
        });

        let value = extract_field_value(&resource, "metadata.name");
        assert_eq!(value, Some("test-pod".to_string()));
    }

    #[test]
    fn test_extract_number_field() {
        let resource = json!({
            "spec": {
                "replicas": 3
            }
        });

        let value = extract_field_value(&resource, "spec.replicas");
        assert_eq!(value, Some("3".to_string()));
    }

    #[test]
    fn test_extract_bool_field() {
        let resource = json!({
            "status": {
                "ready": true
            }
        });

        let value = extract_field_value(&resource, "status.ready");
        assert_eq!(value, Some("true".to_string()));
    }

    /// strategy.go:117-121: `source` selects on the reporting controller when
    /// `source.component` is empty; the object itself is not touched.
    #[test]
    fn event_source_falls_back_to_reporting_component() {
        let ev = json!({"reportingComponent": "ctl", "source": {}});
        assert!(FieldSelector::parse("source=ctl").unwrap().matches(&ev));
        let ev = json!({"reportingComponent": "ctl"});
        assert!(FieldSelector::parse("source=ctl").unwrap().matches(&ev));
        // How the stored core Event actually serializes it.
        let ev = json!({"reportingController": "ctl"});
        assert!(FieldSelector::parse("source=ctl").unwrap().matches(&ev));
        let ev = json!({"reportingComponent": "ctl", "source": {"component": "kubelet"}});
        assert!(FieldSelector::parse("source=kubelet").unwrap().matches(&ev));
        assert!(!FieldSelector::parse("source=ctl").unwrap().matches(&ev));
        // Neither set: selects as the empty string.
        assert!(FieldSelector::parse("source=").unwrap().matches(&json!({})));
    }

    /// strategy.go:117-121 (`"reportingComponent": event.ReportingController, //
    /// use the core/v1 field name`): the selector key `reportingComponent`
    /// selects on the reporting controller, however the object serializes it.
    #[test]
    fn event_reporting_component_selects_on_the_reporting_controller() {
        let ev = json!({"reportingController": "ctl"});
        assert!(FieldSelector::parse("reportingComponent=ctl")
            .unwrap()
            .matches(&ev));
        assert!(!FieldSelector::parse("reportingComponent=other")
            .unwrap()
            .matches(&ev));
        let ev = json!({"reportingComponent": "ctl"});
        assert!(FieldSelector::parse("reportingComponent=ctl")
            .unwrap()
            .matches(&ev));
        // Absent: selects as the empty string, like the Go struct field.
        assert!(FieldSelector::parse("reportingComponent=")
            .unwrap()
            .matches(&json!({})));
    }
}
