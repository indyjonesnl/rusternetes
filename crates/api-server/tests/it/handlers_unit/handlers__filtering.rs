//! Unit tests moved from `src/handlers/filtering.rs`.
#![allow(unused_imports, dead_code)]

mod __parent {
    pub use rusternetes_common::field_selector::FieldSelector;
    pub use rusternetes_common::label_selector::LabelSelector;
    pub use serde::Serialize;
    pub use std::collections::HashMap;
}

mod tests {
    use super::__parent::*;
    use rusternetes_api_server::handlers::filtering::*;

    #[derive(Serialize)]
    struct TestResource {
        metadata: Metadata,
        status: Status,
    }

    #[derive(Serialize)]
    struct Metadata {
        name: String,
        #[serde(skip_serializing_if = "Option::is_none")]
        labels: Option<HashMap<String, String>>,
    }

    #[derive(Serialize)]
    struct Status {
        phase: String,
    }

    #[test]
    fn test_apply_field_selector_empty() {
        let mut resources = vec![
            TestResource {
                metadata: Metadata {
                    name: "test1".to_string(),
                    labels: None,
                },
                status: Status {
                    phase: "Running".to_string(),
                },
            },
            TestResource {
                metadata: Metadata {
                    name: "test2".to_string(),
                    labels: None,
                },
                status: Status {
                    phase: "Pending".to_string(),
                },
            },
        ];

        let params = HashMap::new();
        apply_field_selector(&mut resources, &params).unwrap();

        assert_eq!(resources.len(), 2);
    }

    #[test]
    fn test_apply_field_selector_filters() {
        let mut resources = vec![
            TestResource {
                metadata: Metadata {
                    name: "test1".to_string(),
                    labels: None,
                },
                status: Status {
                    phase: "Running".to_string(),
                },
            },
            TestResource {
                metadata: Metadata {
                    name: "test2".to_string(),
                    labels: None,
                },
                status: Status {
                    phase: "Pending".to_string(),
                },
            },
        ];

        let mut params = HashMap::new();
        params.insert(
            "fieldSelector".to_string(),
            "status.phase=Running".to_string(),
        );

        apply_field_selector(&mut resources, &params).unwrap();

        assert_eq!(resources.len(), 1);
        assert_eq!(resources[0].status.phase, "Running");
    }

    #[test]
    fn test_apply_label_selector_filters() {
        let mut labels1 = HashMap::new();
        labels1.insert("app".to_string(), "nginx".to_string());

        let mut labels2 = HashMap::new();
        labels2.insert("app".to_string(), "redis".to_string());

        let mut resources = vec![
            TestResource {
                metadata: Metadata {
                    name: "test1".to_string(),
                    labels: Some(labels1),
                },
                status: Status {
                    phase: "Running".to_string(),
                },
            },
            TestResource {
                metadata: Metadata {
                    name: "test2".to_string(),
                    labels: Some(labels2),
                },
                status: Status {
                    phase: "Running".to_string(),
                },
            },
        ];

        let mut params = HashMap::new();
        params.insert("labelSelector".to_string(), "app=nginx".to_string());

        apply_label_selector(&mut resources, &params).unwrap();

        assert_eq!(resources.len(), 1);
        assert_eq!(resources[0].metadata.name, "test1");
    }

    #[test]
    fn test_apply_both_selectors() {
        let mut labels1 = HashMap::new();
        labels1.insert("app".to_string(), "nginx".to_string());

        let mut labels2 = HashMap::new();
        labels2.insert("app".to_string(), "nginx".to_string());

        let mut resources = vec![
            TestResource {
                metadata: Metadata {
                    name: "test1".to_string(),
                    labels: Some(labels1),
                },
                status: Status {
                    phase: "Running".to_string(),
                },
            },
            TestResource {
                metadata: Metadata {
                    name: "test2".to_string(),
                    labels: Some(labels2),
                },
                status: Status {
                    phase: "Pending".to_string(),
                },
            },
        ];

        let mut params = HashMap::new();
        params.insert(
            "fieldSelector".to_string(),
            "status.phase=Running".to_string(),
        );
        params.insert("labelSelector".to_string(), "app=nginx".to_string());

        apply_selectors(&mut resources, &params).unwrap();

        assert_eq!(resources.len(), 1);
        assert_eq!(resources[0].metadata.name, "test1");
        assert_eq!(resources[0].status.phase, "Running");
    }

    /// #2181: a malformed labelSelector is a 400 BadRequest carrying
    /// upstream's labels.Parse message (selector.go `parse`), not a 422.
    #[test]
    fn malformed_label_selector_is_bad_request() {
        for bad in ["!!!", "app in ("] {
            let mut params = HashMap::new();
            params.insert("labelSelector".to_string(), bad.to_string());
            let mut resources: Vec<serde_json::Value> = Vec::new();
            let err = apply_label_selector(&mut resources, &params).unwrap_err();
            assert!(
                matches!(&err, rusternetes_common::Error::BadRequest(m) if m.starts_with("unable to parse requirement: ")),
                "{bad:?}: {err:?}"
            );
            let err = apply_selectors(&mut resources, &params).unwrap_err();
            assert!(
                matches!(err, rusternetes_common::Error::BadRequest(_)),
                "{bad:?}"
            );
        }
    }
}
