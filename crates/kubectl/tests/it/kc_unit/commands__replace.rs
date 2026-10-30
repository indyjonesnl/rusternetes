//! Unit tests moved from `src/commands/replace.rs`.
#![allow(unused_imports, dead_code)]

mod __parent {
    pub use anyhow::{Context, Result};
    pub use rusternetes_client::http::ApiClient;
    pub use serde::Deserialize;
    pub use std::fs;
    pub use std::io::{self, Read};
}

mod tests {
    use super::__parent::*;
    use rusternetes_kubectl::commands::replace::*;

    #[test]
    fn test_resource_path_core_namespaced() {
        let path = resource_path("v1", "Pod", Some("default"), "nginx").unwrap();
        assert_eq!(path, "/api/v1/namespaces/default/pods/nginx");

        let path = resource_path("v1", "Service", Some("kube-system"), "coredns").unwrap();
        assert_eq!(path, "/api/v1/namespaces/kube-system/services/coredns");

        let path = resource_path("v1", "ConfigMap", Some("default"), "my-config").unwrap();
        assert_eq!(path, "/api/v1/namespaces/default/configmaps/my-config");

        let path = resource_path("v1", "Secret", Some("default"), "my-secret").unwrap();
        assert_eq!(path, "/api/v1/namespaces/default/secrets/my-secret");

        let path = resource_path("v1", "ServiceAccount", Some("default"), "my-sa").unwrap();
        assert_eq!(path, "/api/v1/namespaces/default/serviceaccounts/my-sa");
    }

    #[test]
    fn test_resource_path_core_cluster_scoped() {
        let path = resource_path("v1", "Namespace", None, "my-ns").unwrap();
        assert_eq!(path, "/api/v1/namespaces/my-ns");

        let path = resource_path("v1", "Node", None, "node-1").unwrap();
        assert_eq!(path, "/api/v1/nodes/node-1");

        let path = resource_path("v1", "PersistentVolume", None, "pv-1").unwrap();
        assert_eq!(path, "/api/v1/persistentvolumes/pv-1");
    }

    #[test]
    fn test_resource_path_apps_v1() {
        let path = resource_path("apps/v1", "Deployment", Some("default"), "nginx").unwrap();
        assert_eq!(path, "/apis/apps/v1/namespaces/default/deployments/nginx");

        let path = resource_path("apps/v1", "StatefulSet", Some("default"), "redis").unwrap();
        assert_eq!(path, "/apis/apps/v1/namespaces/default/statefulsets/redis");

        let path = resource_path("apps/v1", "DaemonSet", Some("kube-system"), "proxy").unwrap();
        assert_eq!(
            path,
            "/apis/apps/v1/namespaces/kube-system/daemonsets/proxy"
        );
    }

    #[test]
    fn test_resource_path_batch_v1() {
        let path = resource_path("batch/v1", "Job", Some("default"), "my-job").unwrap();
        assert_eq!(path, "/apis/batch/v1/namespaces/default/jobs/my-job");

        let path = resource_path("batch/v1", "CronJob", Some("default"), "my-cron").unwrap();
        assert_eq!(path, "/apis/batch/v1/namespaces/default/cronjobs/my-cron");
    }

    #[test]
    fn test_resource_path_rbac() {
        let path = resource_path(
            "rbac.authorization.k8s.io/v1",
            "Role",
            Some("default"),
            "my-role",
        )
        .unwrap();
        assert_eq!(
            path,
            "/apis/rbac.authorization.k8s.io/v1/namespaces/default/roles/my-role"
        );

        let path =
            resource_path("rbac.authorization.k8s.io/v1", "ClusterRole", None, "admin").unwrap();
        assert_eq!(
            path,
            "/apis/rbac.authorization.k8s.io/v1/clusterroles/admin"
        );

        let path = resource_path(
            "rbac.authorization.k8s.io/v1",
            "ClusterRoleBinding",
            None,
            "admin-binding",
        )
        .unwrap();
        assert_eq!(
            path,
            "/apis/rbac.authorization.k8s.io/v1/clusterrolebindings/admin-binding"
        );
    }

    #[test]
    fn test_resource_path_cluster_scoped_special() {
        let path = resource_path("storage.k8s.io/v1", "StorageClass", None, "standard").unwrap();
        assert_eq!(path, "/apis/storage.k8s.io/v1/storageclasses/standard");

        let path = resource_path(
            "scheduling.k8s.io/v1",
            "PriorityClass",
            None,
            "high-priority",
        )
        .unwrap();
        assert_eq!(
            path,
            "/apis/scheduling.k8s.io/v1/priorityclasses/high-priority"
        );

        let path = resource_path(
            "apiextensions.k8s.io/v1",
            "CustomResourceDefinition",
            None,
            "foos.example.com",
        )
        .unwrap();
        assert_eq!(
            path,
            "/apis/apiextensions.k8s.io/v1/customresourcedefinitions/foos.example.com"
        );
    }

    #[test]
    fn test_resource_path_default_namespace() {
        // When namespace is None for a namespaced resource, defaults to "default"
        let path = resource_path("v1", "Pod", None, "nginx").unwrap();
        assert_eq!(path, "/api/v1/namespaces/default/pods/nginx");
    }

    #[test]
    fn test_resource_path_networking() {
        let path = resource_path(
            "networking.k8s.io/v1",
            "Ingress",
            Some("default"),
            "my-ingress",
        )
        .unwrap();
        assert_eq!(
            path,
            "/apis/networking.k8s.io/v1/namespaces/default/ingresses/my-ingress"
        );

        let path = resource_path(
            "networking.k8s.io/v1",
            "NetworkPolicy",
            Some("default"),
            "deny-all",
        )
        .unwrap();
        assert_eq!(
            path,
            "/apis/networking.k8s.io/v1/namespaces/default/networkpolicies/deny-all"
        );
    }

    #[test]
    fn test_resource_path_unknown_kind_with_group() {
        // Unknown kind with API group should still construct a reasonable path
        let path = resource_path(
            "custom.example.com/v1",
            "Widget",
            Some("default"),
            "my-widget",
        )
        .unwrap();
        assert_eq!(
            path,
            "/apis/custom.example.com/v1/namespaces/default/widgets/my-widget"
        );
    }

    #[test]
    fn test_kind_to_plural() {
        assert_eq!(kind_to_plural("Pod"), "pods");
        assert_eq!(kind_to_plural("Service"), "services");
        assert_eq!(kind_to_plural("Ingress"), "ingresses");
        assert_eq!(kind_to_plural("NetworkPolicy"), "networkpolicies");
        assert_eq!(kind_to_plural("Endpoints"), "endpoints");
    }

    #[test]
    fn test_kind_to_output_name() {
        assert_eq!(kind_to_output_name("Pod", "v1"), "pod");
        assert_eq!(
            kind_to_output_name("Deployment", "apps/v1"),
            "deployment.apps"
        );
        assert_eq!(
            kind_to_output_name("ClusterRole", "rbac.authorization.k8s.io/v1"),
            "clusterrole.rbac.authorization.k8s.io"
        );
        assert_eq!(kind_to_output_name("Service", "v1"), "service");
    }

    #[test]
    fn test_resource_path_pvc() {
        let path = resource_path("v1", "PersistentVolumeClaim", Some("default"), "my-pvc").unwrap();
        assert_eq!(
            path,
            "/api/v1/namespaces/default/persistentvolumeclaims/my-pvc"
        );
    }

    #[test]
    fn test_resource_path_policy() {
        let path = resource_path(
            "policy/v1",
            "PodDisruptionBudget",
            Some("default"),
            "my-pdb",
        )
        .unwrap();
        assert_eq!(
            path,
            "/apis/policy/v1/namespaces/default/poddisruptionbudgets/my-pdb"
        );
    }

    #[test]
    fn test_resource_path_hpa() {
        let path = resource_path(
            "autoscaling/v2",
            "HorizontalPodAutoscaler",
            Some("prod"),
            "web-hpa",
        )
        .unwrap();
        assert_eq!(
            path,
            "/apis/autoscaling/v2/namespaces/prod/horizontalpodautoscalers/web-hpa"
        );
    }

    #[test]
    fn test_resource_path_volume_snapshot() {
        let path = resource_path(
            "snapshot.storage.k8s.io/v1",
            "VolumeSnapshot",
            Some("default"),
            "snap-1",
        )
        .unwrap();
        assert_eq!(
            path,
            "/apis/snapshot.storage.k8s.io/v1/namespaces/default/volumesnapshots/snap-1"
        );
    }

    #[test]
    fn test_resource_path_volume_snapshot_class() {
        let path = resource_path(
            "snapshot.storage.k8s.io/v1",
            "VolumeSnapshotClass",
            None,
            "csi-snap",
        )
        .unwrap();
        assert_eq!(
            path,
            "/apis/snapshot.storage.k8s.io/v1/volumesnapshotclasses/csi-snap"
        );
    }

    #[test]
    fn test_resource_path_unknown_kind_no_group() {
        let path = resource_path("v1", "Widget", Some("default"), "w1").unwrap();
        assert_eq!(path, "/api/v1/namespaces/default/widgets/w1");
    }

    #[test]
    fn test_resource_path_unknown_cluster_scoped_no_group() {
        let path = resource_path("v1", "Widget", None, "w1").unwrap();
        assert_eq!(path, "/api/v1/widgets/w1");
    }

    #[test]
    fn test_kind_to_plural_special_endings() {
        // Ends with "ch" -> add "es"
        assert_eq!(kind_to_plural("Batch"), "batches");
        // Ends with "sh" -> add "es"
        assert_eq!(kind_to_plural("Mesh"), "meshes");
        // Ends with "x" -> add "es"
        assert_eq!(kind_to_plural("Box"), "boxes");
        // Ends with "ss" -> add "es"
        assert_eq!(kind_to_plural("Class"), "classes");
        // Ends with "y" but not "ey"/"ay"/"oy" -> "ies"
        assert_eq!(kind_to_plural("Policy"), "policies");
    }

    #[test]
    fn test_kind_to_plural_y_exceptions() {
        // "ey" ending -> just add "s"
        assert_eq!(kind_to_plural("Key"), "keys");
        // "ay" ending -> just add "s"
        assert_eq!(kind_to_plural("Gateway"), "gateways");
        // "oy" ending -> just add "s"
        assert_eq!(kind_to_plural("Decoy"), "decoys");
    }

    #[test]
    fn test_kind_to_output_name_empty_api_version() {
        assert_eq!(kind_to_output_name("Pod", ""), "pod");
    }

    #[test]
    fn test_resource_path_remaining_core_types() {
        let path = resource_path("v1", "Endpoints", Some("default"), "my-ep").unwrap();
        assert_eq!(path, "/api/v1/namespaces/default/endpoints/my-ep");

        let path = resource_path("v1", "ResourceQuota", Some("default"), "rq").unwrap();
        assert_eq!(path, "/api/v1/namespaces/default/resourcequotas/rq");

        let path = resource_path("v1", "LimitRange", Some("default"), "lr").unwrap();
        assert_eq!(path, "/api/v1/namespaces/default/limitranges/lr");

        let path = resource_path("v1", "ReplicationController", Some("default"), "rc").unwrap();
        assert_eq!(path, "/api/v1/namespaces/default/replicationcontrollers/rc");

        let path = resource_path("v1", "Event", Some("default"), "ev").unwrap();
        assert_eq!(path, "/api/v1/namespaces/default/events/ev");
    }

    #[test]
    fn test_resource_path_unknown_cluster_scoped_with_group() {
        let path = resource_path("custom.example.com/v1", "Widget", None, "w1").unwrap();
        assert_eq!(path, "/apis/custom.example.com/v1/widgets/w1");
    }
}
