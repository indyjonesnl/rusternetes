//! Unit tests moved from `src/labels.rs`.
#![allow(unused_imports, dead_code)]

mod __parent {
    pub use rusternetes_common::resources::Pod;
    pub use std::collections::HashMap;
}

mod tests {
    use super::__parent::*;
    use rusternetes_common::types::ObjectMeta;
    use rusternetes_kubelet::labels::*;

    fn pod_with(name: &str, namespace: Option<&str>, uid: &str) -> Pod {
        Pod {
            type_meta: Default::default(),
            metadata: ObjectMeta {
                name: name.to_string(),
                namespace: namespace.map(|s| s.to_string()),
                uid: uid.to_string(),
                ..Default::default()
            },
            spec: None,
            status: None,
        }
    }

    #[test]
    fn pod_container_labels_has_four_labels() {
        let pod = pod_with("web-0", Some("apps"), "uid-abc");
        let labels = pod_container_labels(&pod, "nginx");
        assert_eq!(labels.len(), 4);
        assert_eq!(labels.get(POD_NAME_LABEL).unwrap(), "web-0");
        assert_eq!(labels.get(POD_NAMESPACE_LABEL).unwrap(), "apps");
        assert_eq!(labels.get(POD_UID_LABEL).unwrap(), "uid-abc");
        assert_eq!(labels.get(CONTAINER_NAME_LABEL).unwrap(), "nginx");
    }

    #[test]
    fn pod_container_labels_defaults_empty_namespace_to_default() {
        let pod = pod_with("web-0", None, "uid-abc");
        let labels = pod_container_labels(&pod, "nginx");
        assert_eq!(labels.get(POD_NAMESPACE_LABEL).unwrap(), "default");
    }

    /// Full identity-label set for a container of pod `name`/`ns` with `uid`.
    fn full_labels(name: &str, ns: &str, uid: &str) -> HashMap<String, String> {
        let mut l = HashMap::new();
        l.insert(POD_NAME_LABEL.to_string(), name.to_string());
        l.insert(POD_NAMESPACE_LABEL.to_string(), ns.to_string());
        l.insert(POD_UID_LABEL.to_string(), uid.to_string());
        l
    }

    #[test]
    fn stale_when_same_name_same_ns_different_uid() {
        let labels = full_labels("web-0", "apps", "old-uid");
        assert!(is_stale_same_pod_incarnation(
            Some(&labels),
            "web-0",
            "apps",
            "new-uid"
        ));
    }

    #[test]
    fn not_stale_when_different_pod_name() {
        // Docker's name filter is a substring match — a container of pod
        // "myweb-0" can show up when listing for pod "web-0". The predicate
        // must reject it.
        let labels = full_labels("myweb-0", "apps", "old-uid");
        assert!(!is_stale_same_pod_incarnation(
            Some(&labels),
            "web-0",
            "apps",
            "new-uid"
        ));
    }

    #[test]
    fn not_stale_when_different_namespace() {
        // Same-named pods in different namespaces share the docker name
        // pattern; only the same-namespace pod may be swept.
        let labels = full_labels("web-0", "other-ns", "old-uid");
        assert!(!is_stale_same_pod_incarnation(
            Some(&labels),
            "web-0",
            "apps",
            "new-uid"
        ));
    }

    #[test]
    fn not_stale_when_uid_matches() {
        let labels = full_labels("web-0", "apps", "same-uid");
        assert!(!is_stale_same_pod_incarnation(
            Some(&labels),
            "web-0",
            "apps",
            "same-uid"
        ));
    }

    #[test]
    fn not_stale_when_uid_label_missing() {
        let mut labels = full_labels("web-0", "apps", "x");
        labels.remove(POD_UID_LABEL);
        assert!(!is_stale_same_pod_incarnation(
            Some(&labels),
            "web-0",
            "apps",
            "new-uid"
        ));
    }

    #[test]
    fn not_stale_when_name_label_missing() {
        let mut labels = full_labels("web-0", "apps", "old-uid");
        labels.remove(POD_NAME_LABEL);
        assert!(!is_stale_same_pod_incarnation(
            Some(&labels),
            "web-0",
            "apps",
            "new-uid"
        ));
    }

    #[test]
    fn not_stale_when_namespace_label_missing() {
        let mut labels = full_labels("web-0", "apps", "old-uid");
        labels.remove(POD_NAMESPACE_LABEL);
        assert!(!is_stale_same_pod_incarnation(
            Some(&labels),
            "web-0",
            "apps",
            "new-uid"
        ));
    }

    #[test]
    fn not_stale_when_pod_uid_empty() {
        let labels = full_labels("web-0", "apps", "old-uid");
        assert!(!is_stale_same_pod_incarnation(
            Some(&labels),
            "web-0",
            "apps",
            ""
        ));
    }

    #[test]
    fn not_stale_when_labels_map_empty() {
        let labels = HashMap::new();
        assert!(!is_stale_same_pod_incarnation(
            Some(&labels),
            "web-0",
            "apps",
            "new-uid"
        ));
    }

    #[test]
    fn not_stale_when_labels_none() {
        assert!(!is_stale_same_pod_incarnation(
            None, "web-0", "apps", "new-uid"
        ));
    }
}
