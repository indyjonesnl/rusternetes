//! Unit tests moved from `src/ops.rs`.
#![allow(unused_imports, dead_code)]

mod __parent {
    pub use anyhow::{Context, Result};
    pub use rusternetes_client::http::ApiClient;
    pub use rusternetes_kubectl::discovery::ResourceMapping;
    pub use serde_json::Value;
}

mod tests {
    use super::__parent::*;
    use rusternetes_kubectl::discovery::ResourceMapping;
    use rusternetes_kubectl::ops::*;

    #[test]
    fn label_selector_query_none_and_empty_are_blank() {
        assert_eq!(label_selector_query(None), "");
        assert_eq!(label_selector_query(Some("")), "");
    }

    #[test]
    fn label_selector_query_encodes() {
        assert_eq!(
            label_selector_query(Some("app=nginx,tier=web")),
            "?labelSelector=app=nginx,tier=web"
        );
        // spaces -> '+', parens percent-encoded (set-based selectors).
        assert_eq!(
            label_selector_query(Some("env in (a, b)")),
            "?labelSelector=env+in+%28a,+b%29"
        );
    }

    fn m(group: &str, plural: &str, namespaced: bool) -> ResourceMapping {
        ResourceMapping {
            group: group.into(),
            version: "v1".into(),
            kind: "X".into(),
            plural: plural.into(),
            singular: "x".into(),
            namespaced,
            verbs: vec![],
            short_names: vec![],
        }
    }

    #[test]
    fn core_namespaced_paths() {
        let pod = m("", "pods", true);
        assert_eq!(
            build_path(&pod, Some("kube-system"), Some("dns")),
            "/api/v1/namespaces/kube-system/pods/dns"
        );
        assert_eq!(
            build_path(&pod, None, None),
            "/api/v1/namespaces/default/pods"
        );
    }

    #[test]
    fn grouped_cluster_path() {
        let crb = m("rbac.authorization.k8s.io", "clusterrolebindings", false);
        assert_eq!(
            build_path(&crb, Some("ignored"), Some("admin")),
            "/apis/rbac.authorization.k8s.io/v1/clusterrolebindings/admin"
        );
    }

    #[test]
    fn grouped_namespaced_collection() {
        let dep = m("apps", "deployments", true);
        assert_eq!(
            build_path(&dep, Some("prod"), None),
            "/apis/apps/v1/namespaces/prod/deployments"
        );
    }

    #[test]
    fn cluster_collection_path_no_name() {
        let pv = m("", "persistentvolumes", false);
        assert_eq!(build_path(&pv, None, None), "/api/v1/persistentvolumes");
    }

    #[test]
    fn all_namespaces_cluster_wide_path() {
        // When all_namespaces is requested for a namespaced resource the path
        // must NOT contain a /namespaces/{ns} segment. This exercises the
        // actual decision in list_path (the canonical place for LIST paths).
        let pod = m("", "pods", true);
        assert_eq!(list_path(&pod, None, true), "/api/v1/pods");
        assert_eq!(
            list_path(&pod, Some("kube-system"), true),
            "/api/v1/pods" // namespace ignored under all_namespaces
        );

        let dep = m("apps", "deployments", true);
        assert_eq!(list_path(&dep, None, true), "/apis/apps/v1/deployments");
    }

    #[test]
    fn list_path_namespaced_uses_default_when_not_all_namespaces() {
        let pod = m("", "pods", true);
        assert_eq!(
            list_path(&pod, None, false),
            "/api/v1/namespaces/default/pods"
        );
        assert_eq!(
            list_path(&pod, Some("prod"), false),
            "/api/v1/namespaces/prod/pods"
        );
    }

    #[test]
    fn list_path_cluster_scoped_ignores_all_namespaces() {
        // Cluster-scoped resources have no namespace segment either way.
        let pv = m("", "persistentvolumes", false);
        assert_eq!(list_path(&pv, None, false), "/api/v1/persistentvolumes");
        assert_eq!(list_path(&pv, None, true), "/api/v1/persistentvolumes");
    }

    #[test]
    fn resource_label_core_and_grouped() {
        // The local m() helper hardcodes singular to "x", so build literals with
        // the real singular to keep both assertions meaningful.

        // Core-group resource: no group qualifier.
        let pod = ResourceMapping {
            group: "".into(),
            version: "v1".into(),
            kind: "Pod".into(),
            plural: "pods".into(),
            singular: "pod".into(),
            namespaced: true,
            verbs: vec![],
            short_names: vec![],
        };
        assert_eq!(resource_label(&pod, "nginx"), "pod/nginx");

        // Grouped resource: label must be "<singular>.<group>/name".
        let dep = ResourceMapping {
            group: "apps".into(),
            version: "v1".into(),
            kind: "Deployment".into(),
            plural: "deployments".into(),
            singular: "deployment".into(),
            namespaced: true,
            verbs: vec![],
            short_names: vec![],
        };
        assert_eq!(resource_label(&dep, "nginx"), "deployment.apps/nginx");
    }

    #[test]
    fn reads_metadata_name() {
        let v = serde_json::json!({"metadata": {"name": "foo", "namespace": "bar"}});
        assert_eq!(value_name(&v).as_deref(), Some("foo"));
        assert_eq!(value_namespace(&v).as_deref(), Some("bar"));
    }

    #[test]
    fn missing_metadata_is_none() {
        let v = serde_json::json!({"spec": {}});
        assert!(value_name(&v).is_none());
        assert!(value_namespace(&v).is_none());
    }
}
