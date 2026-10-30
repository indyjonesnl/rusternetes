//! Unit tests moved from `src/resources/runtimeclass.rs`.
#![allow(unused_imports, dead_code)]

mod __parent {
    pub use rusternetes_common::types::{ObjectMeta, TypeMeta};
    pub use serde::{Deserialize, Serialize};
    pub use std::collections::HashMap;
}

mod tests {
    use super::__parent::*;
    use rusternetes_common::resources::runtimeclass::*;

    #[test]
    fn test_runtime_class_creation() {
        let rc = RuntimeClass::new("my-runtime", "runc");

        assert_eq!(rc.metadata.name, "my-runtime");
        assert_eq!(rc.handler, "runc");
        assert_eq!(rc.type_meta.kind, "RuntimeClass");
        assert_eq!(rc.type_meta.api_version, "node.k8s.io/v1");
    }

    #[test]
    fn test_runtime_class_with_overhead() {
        let mut pod_fixed = HashMap::new();
        pod_fixed.insert("memory".to_string(), "128Mi".to_string());
        pod_fixed.insert("cpu".to_string(), "250m".to_string());

        let overhead = Overhead {
            pod_fixed: Some(pod_fixed),
        };

        let rc = RuntimeClass::new("my-runtime", "runc").with_overhead(overhead.clone());

        assert!(rc.overhead.is_some());
        let overhead_val = rc.overhead.unwrap();
        assert!(overhead_val.pod_fixed.is_some());
        let pod_fixed = overhead_val.pod_fixed.unwrap();
        assert_eq!(pod_fixed.get("memory"), Some(&"128Mi".to_string()));
        assert_eq!(pod_fixed.get("cpu"), Some(&"250m".to_string()));
    }

    #[test]
    fn test_runtime_class_with_scheduling() {
        let mut node_selector = HashMap::new();
        node_selector.insert("runtime".to_string(), "kata".to_string());
        node_selector.insert("zone".to_string(), "us-west-1a".to_string());

        let scheduling = Scheduling {
            node_selector: Some(node_selector.clone()),
            tolerations: None,
        };

        let rc = RuntimeClass::new("kata", "kata-runtime").with_scheduling(scheduling);

        assert!(rc.scheduling.is_some());
        let sched = rc.scheduling.unwrap();
        assert!(sched.node_selector.is_some());
        let ns = sched.node_selector.unwrap();
        assert_eq!(ns.get("runtime"), Some(&"kata".to_string()));
        assert_eq!(ns.get("zone"), Some(&"us-west-1a".to_string()));
    }

    #[test]
    fn test_runtime_class_serialization() {
        let rc = RuntimeClass::new("gvisor", "runsc");

        let json = serde_json::to_string(&rc).unwrap();
        assert!(json.contains("RuntimeClass"));
        assert!(json.contains("node.k8s.io/v1"));
        assert!(json.contains("gvisor"));
        assert!(json.contains("runsc"));
    }

    #[test]
    fn test_runtime_class_complete() {
        let mut pod_fixed = HashMap::new();
        pod_fixed.insert("memory".to_string(), "256Mi".to_string());

        let overhead = Overhead {
            pod_fixed: Some(pod_fixed),
        };

        let mut node_selector = HashMap::new();
        node_selector.insert("runtime".to_string(), "gvisor".to_string());

        let tolerations = vec![rusternetes_common::resources::pod::Toleration {
            key: Some("runtime".to_string()),
            operator: Some("Equal".to_string()),
            value: Some("gvisor".to_string()),
            effect: Some("NoSchedule".to_string()),
            toleration_seconds: None,
        }];

        let scheduling = Scheduling {
            node_selector: Some(node_selector),
            tolerations: Some(tolerations),
        };

        let rc = RuntimeClass::new("gvisor", "runsc")
            .with_overhead(overhead)
            .with_scheduling(scheduling);

        assert_eq!(rc.handler, "runsc");
        assert!(rc.overhead.is_some());
        assert!(rc.scheduling.is_some());
    }
}
