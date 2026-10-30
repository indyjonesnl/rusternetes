//! Unit tests moved from `src/commands/diff.rs`.
#![allow(unused_imports, dead_code)]

mod __parent {
    pub use anyhow::{Context, Result};
    pub use rusternetes_client::http::{ApiClient, GetError};
    pub use rusternetes_kubectl::discovery::RestMapper;
    pub use rusternetes_kubectl::ops::build_path;
    pub use serde_json::Value;
}

mod tests {
    use rusternetes_kubectl::commands::diff::strip_server_managed;
    use rusternetes_kubectl::discovery::{ResourceMapping, RestMapper};
    use rusternetes_kubectl::ops::build_path;
    use serde_json::json;

    fn mapping(
        group: &str,
        version: &str,
        kind: &str,
        plural: &str,
        namespaced: bool,
    ) -> ResourceMapping {
        ResourceMapping {
            group: group.into(),
            version: version.into(),
            kind: kind.into(),
            plural: plural.into(),
            singular: kind.to_lowercase(),
            namespaced,
            verbs: vec![],
            short_names: vec![],
        }
    }

    fn mapper_with(mappings: Vec<ResourceMapping>) -> RestMapper {
        RestMapper::new(mappings)
    }

    // ---------------------------------------------------------------------------
    // Path-building tests: verify that build_path (used by diff_resource) produces
    // the correct API paths for a variety of resource kinds via the mapper.
    // ---------------------------------------------------------------------------

    #[test]
    fn strip_server_managed_removes_managed_fields_keeps_real_data() {
        let mut v = json!({
            "apiVersion": "v1",
            "kind": "ConfigMap",
            "metadata": {
                "name": "f2-cm",
                "namespace": "default",
                "labels": { "app": "demo" },
                "uid": "abc-123",
                "resourceVersion": "4711",
                "generation": 3,
                "creationTimestamp": "2026-05-29T00:00:00Z",
                "managedFields": [{ "manager": "kubectl" }],
                "selfLink": "/api/v1/namespaces/default/configmaps/f2-cm",
                "ownerReferences": [{ "name": "owner" }],
                "annotations": {
                    "kubectl.kubernetes.io/last-applied-configuration": "{}"
                }
            },
            "data": { "a": "1", "b": "2" },
            "status": { "phase": "Active" }
        });

        strip_server_managed(&mut v);

        let meta = v.get("metadata").unwrap().as_object().unwrap();
        for k in [
            "uid",
            "resourceVersion",
            "generation",
            "creationTimestamp",
            "managedFields",
            "selfLink",
            "ownerReferences",
        ] {
            assert!(!meta.contains_key(k), "expected {k} to be stripped");
        }
        // last-applied-configuration was the only annotation → annotations map dropped
        assert!(!meta.contains_key("annotations"));
        // status is server-populated → stripped
        assert!(v.get("status").is_none());

        // user-meaningful fields survive
        assert_eq!(meta.get("name").unwrap(), "f2-cm");
        assert_eq!(meta.get("namespace").unwrap(), "default");
        assert_eq!(meta.get("labels").unwrap(), &json!({ "app": "demo" }));
        assert_eq!(v.get("data").unwrap(), &json!({ "a": "1", "b": "2" }));
        assert_eq!(v.get("kind").unwrap(), "ConfigMap");
    }

    #[test]
    fn strip_server_managed_keeps_other_annotations() {
        let mut v = json!({
            "metadata": {
                "name": "x",
                "annotations": {
                    "kubectl.kubernetes.io/last-applied-configuration": "{}",
                    "team": "infra"
                }
            }
        });
        strip_server_managed(&mut v);
        let ann = v
            .get("metadata")
            .unwrap()
            .get("annotations")
            .unwrap()
            .as_object()
            .unwrap();
        assert!(!ann.contains_key("kubectl.kubernetes.io/last-applied-configuration"));
        assert_eq!(ann.get("team").unwrap(), "infra");
    }

    #[test]
    fn pod_path_via_mapper() {
        let m = mapper_with(vec![mapping("", "v1", "Pod", "pods", true)]);
        let pod = m.resolve("Pod").unwrap();
        assert_eq!(
            build_path(pod, Some("default"), Some("nginx")),
            "/api/v1/namespaces/default/pods/nginx"
        );
    }

    #[test]
    fn deployment_path_via_mapper() {
        let m = mapper_with(vec![mapping(
            "apps",
            "v1",
            "Deployment",
            "deployments",
            true,
        )]);
        let dep = m.resolve("Deployment").unwrap();
        assert_eq!(
            build_path(dep, Some("prod"), Some("web")),
            "/apis/apps/v1/namespaces/prod/deployments/web"
        );
    }

    #[test]
    fn persistent_volume_cluster_scoped() {
        let m = mapper_with(vec![mapping(
            "",
            "v1",
            "PersistentVolume",
            "persistentvolumes",
            false,
        )]);
        let pv = m.resolve("PersistentVolume").unwrap();
        // namespace argument is ignored for cluster-scoped resources
        assert_eq!(
            build_path(pv, Some("ignored"), Some("pv-1")),
            "/api/v1/persistentvolumes/pv-1"
        );
    }

    #[test]
    fn crd_path_via_mapper() {
        let m = mapper_with(vec![mapping(
            "apiextensions.k8s.io",
            "v1",
            "CustomResourceDefinition",
            "customresourcedefinitions",
            false,
        )]);
        let crd = m.resolve("CustomResourceDefinition").unwrap();
        assert_eq!(
            build_path(crd, Some("ignored"), Some("foos.example.com")),
            "/apis/apiextensions.k8s.io/v1/customresourcedefinitions/foos.example.com"
        );
    }

    #[test]
    fn unknown_kind_resolves_to_none() {
        let m = mapper_with(vec![mapping("", "v1", "Pod", "pods", true)]);
        assert!(m.resolve("Unknown").is_none());
    }

    #[test]
    fn service_path_via_mapper() {
        let m = mapper_with(vec![mapping("", "v1", "Service", "services", true)]);
        let svc = m.resolve("Service").unwrap();
        assert_eq!(
            build_path(svc, Some("prod"), Some("my-svc")),
            "/api/v1/namespaces/prod/services/my-svc"
        );
    }

    #[test]
    fn configmap_path_via_mapper() {
        let m = mapper_with(vec![mapping("", "v1", "ConfigMap", "configmaps", true)]);
        let cm = m.resolve("ConfigMap").unwrap();
        assert_eq!(
            build_path(cm, Some("default"), Some("cfg")),
            "/api/v1/namespaces/default/configmaps/cfg"
        );
    }

    #[test]
    fn namespace_cluster_scoped_via_mapper() {
        let m = mapper_with(vec![mapping("", "v1", "Namespace", "namespaces", false)]);
        let ns = m.resolve("Namespace").unwrap();
        assert_eq!(
            build_path(ns, Some("ignored"), Some("kube-system")),
            "/api/v1/namespaces/kube-system"
        );
    }

    #[test]
    fn statefulset_path_via_mapper() {
        let m = mapper_with(vec![mapping(
            "apps",
            "v1",
            "StatefulSet",
            "statefulsets",
            true,
        )]);
        let ss = m.resolve("StatefulSet").unwrap();
        assert_eq!(
            build_path(ss, Some("staging"), Some("mysql")),
            "/apis/apps/v1/namespaces/staging/statefulsets/mysql"
        );
    }

    #[test]
    fn node_cluster_scoped_via_mapper() {
        let m = mapper_with(vec![mapping("", "v1", "Node", "nodes", false)]);
        let node = m.resolve("Node").unwrap();
        assert_eq!(
            build_path(node, Some("ignored"), Some("worker-1")),
            "/api/v1/nodes/worker-1"
        );
    }

    #[test]
    fn rbac_resources_via_mapper() {
        let mappings = vec![
            mapping(
                "rbac.authorization.k8s.io",
                "v1",
                "ClusterRole",
                "clusterroles",
                false,
            ),
            mapping(
                "rbac.authorization.k8s.io",
                "v1",
                "ClusterRoleBinding",
                "clusterrolebindings",
                false,
            ),
            mapping("rbac.authorization.k8s.io", "v1", "Role", "roles", true),
        ];
        let m = mapper_with(mappings);

        let cr = m.resolve("ClusterRole").unwrap();
        assert_eq!(
            build_path(cr, Some("ignored"), Some("admin")),
            "/apis/rbac.authorization.k8s.io/v1/clusterroles/admin"
        );

        let crb = m.resolve("ClusterRoleBinding").unwrap();
        assert_eq!(
            build_path(crb, Some("ignored"), Some("admin-binding")),
            "/apis/rbac.authorization.k8s.io/v1/clusterrolebindings/admin-binding"
        );

        let role = m.resolve("Role").unwrap();
        assert_eq!(
            build_path(role, Some("default"), Some("pod-reader")),
            "/apis/rbac.authorization.k8s.io/v1/namespaces/default/roles/pod-reader"
        );
    }
}
