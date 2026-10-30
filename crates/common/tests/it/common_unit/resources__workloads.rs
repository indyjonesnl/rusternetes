//! Unit tests moved from `src/resources/workloads.rs`.
#![allow(unused_imports, dead_code)]

mod __parent {
    pub use chrono::{DateTime, Utc};
    pub use rusternetes_common::resources::pod::PodSpec;
    pub use rusternetes_common::resources::policy::IntOrString;
    pub use rusternetes_common::types::{LabelSelector, ObjectMeta, TypeMeta};
    pub use serde::{Deserialize, Serialize};
    pub use std::collections::HashMap;
}

mod tests {
    use super::__parent::*;
    use rusternetes_common::resources::workloads::*;

    #[test]
    fn test_replication_controller_creation() {
        let pod_template_spec = PodTemplateSpec {
            metadata: None,
            spec: PodSpec {
                containers: vec![],
                init_containers: None,
                ephemeral_containers: None,
                volumes: None,
                restart_policy: Some("Always".to_string()),
                node_name: None,
                node_selector: None,
                service_account_name: None,
                service_account: None,
                hostname: None,
                subdomain: None,
                host_network: None,
                host_pid: None,
                host_ipc: None,
                affinity: None,
                tolerations: None,
                priority: None,
                priority_class_name: None,
                automount_service_account_token: None,
                topology_spread_constraints: None,
                overhead: None,
                scheduler_name: None,
                resource_claims: None,
                active_deadline_seconds: None,
                dns_policy: None,
                dns_config: None,
                security_context: None,
                image_pull_secrets: None,
                share_process_namespace: None,
                readiness_gates: None,
                runtime_class_name: None,
                enable_service_links: None,
                preemption_policy: None,
                host_users: None,
                set_hostname_as_fqdn: None,
                termination_grace_period_seconds: None,
                host_aliases: None,
                os: None,
                scheduling_gates: None,
                resources: None,
                ..Default::default()
            },
        };

        let rc_spec = ReplicationControllerSpec {
            replicas: Some(3),
            selector: Some(
                [("app".to_string(), "nginx".to_string())]
                    .iter()
                    .cloned()
                    .collect(),
            ),
            template: pod_template_spec,
            min_ready_seconds: None,
        };

        let rc = ReplicationController::new("test-rc", "default", rc_spec);

        assert_eq!(rc.metadata.name, "test-rc");
        assert_eq!(rc.metadata.namespace, Some("default".to_string()));
        assert_eq!(rc.type_meta.kind, "ReplicationController");
        assert_eq!(rc.type_meta.api_version, "v1");
        assert_eq!(rc.spec.replicas, Some(3));
    }

    #[test]
    fn test_pod_template_creation() {
        let pod_template_spec = PodTemplateSpec {
            metadata: None,
            spec: PodSpec {
                containers: vec![],
                init_containers: None,
                ephemeral_containers: None,
                volumes: None,
                restart_policy: Some("Always".to_string()),
                node_name: None,
                node_selector: None,
                service_account_name: None,
                service_account: None,
                hostname: None,
                subdomain: None,
                host_network: None,
                host_pid: None,
                host_ipc: None,
                affinity: None,
                tolerations: None,
                priority: None,
                priority_class_name: None,
                automount_service_account_token: None,
                topology_spread_constraints: None,
                overhead: None,
                scheduler_name: None,
                resource_claims: None,
                active_deadline_seconds: None,
                dns_policy: None,
                dns_config: None,
                security_context: None,
                image_pull_secrets: None,
                share_process_namespace: None,
                readiness_gates: None,
                runtime_class_name: None,
                enable_service_links: None,
                preemption_policy: None,
                host_users: None,
                set_hostname_as_fqdn: None,
                termination_grace_period_seconds: None,
                host_aliases: None,
                os: None,
                scheduling_gates: None,
                resources: None,
                ..Default::default()
            },
        };

        let pod_template = PodTemplate::new("test-template", "default", pod_template_spec);

        assert_eq!(pod_template.metadata.name, "test-template");
        assert_eq!(pod_template.metadata.namespace, Some("default".to_string()));
        assert_eq!(pod_template.type_meta.kind, "PodTemplate");
        assert_eq!(pod_template.type_meta.api_version, "v1");
    }

    #[test]
    fn test_job_creation() {
        let template = PodTemplateSpec {
            metadata: None,
            spec: PodSpec {
                containers: vec![],
                init_containers: None,
                ephemeral_containers: None,
                volumes: None,
                restart_policy: Some("Never".to_string()),
                node_name: None,
                node_selector: None,
                service_account_name: None,
                service_account: None,
                hostname: None,
                subdomain: None,
                host_network: None,
                host_pid: None,
                host_ipc: None,
                affinity: None,
                tolerations: None,
                priority: None,
                priority_class_name: None,
                automount_service_account_token: None,
                topology_spread_constraints: None,
                overhead: None,
                scheduler_name: None,
                resource_claims: None,
                active_deadline_seconds: None,
                dns_policy: None,
                dns_config: None,
                security_context: None,
                image_pull_secrets: None,
                share_process_namespace: None,
                readiness_gates: None,
                runtime_class_name: None,
                enable_service_links: None,
                preemption_policy: None,
                host_users: None,
                set_hostname_as_fqdn: None,
                termination_grace_period_seconds: None,
                host_aliases: None,
                os: None,
                scheduling_gates: None,
                resources: None,
                ..Default::default()
            },
        };

        let job_spec = JobSpec {
            template,
            completions: Some(1),
            parallelism: Some(1),
            backoff_limit: Some(3),
            active_deadline_seconds: None,
            selector: None,
            manual_selector: None,
            suspend: None,
            ttl_seconds_after_finished: None,
            completion_mode: None,
            backoff_limit_per_index: None,
            max_failed_indexes: None,
            pod_failure_policy: None,
            pod_replacement_policy: None,
            success_policy: None,
            managed_by: None,
        };

        let job = Job::new("test-job", "default", job_spec);

        assert_eq!(job.metadata.name, "test-job");
        assert_eq!(job.type_meta.api_version, "batch/v1");
    }

    /// Reproducing the "missing field `replicas`" conformance failure.
    /// A conformance test creates an RC without specifying replicas.
    /// Go's json.Unmarshal leaves *int32 at nil; we must also accept absence.
    #[test]
    fn test_rc_decode_without_replicas() {
        // Typical RC body sent by k8s conformance tests (no `replicas` field)
        let json = r#"{
            "kind": "ReplicationController",
            "apiVersion": "v1",
            "metadata": {
                "name": "e2e-test-rc",
                "namespace": "replication-controller-9005",
                "labels": {"app": "e2e-test-rc"},
                "creationTimestamp": null
            },
            "spec": {
                "selector": {"app": "e2e-test-rc"},
                "template": {
                    "metadata": {
                        "labels": {"app": "e2e-test-rc"},
                        "creationTimestamp": null
                    },
                    "spec": {
                        "containers": [
                            {
                                "name": "busybox",
                                "image": "registry.k8s.io/e2e-test-images/busybox:1.36.1-2",
                                "resources": {}
                            }
                        ],
                        "restartPolicy": "Always",
                        "terminationGracePeriodSeconds": 30,
                        "dnsPolicy": "ClusterFirst"
                    }
                }
            }
        }"#;

        let rc: ReplicationController =
            serde_json::from_str(json).expect("RC without replicas must decode");
        // replicas absent → None (Go: *int32 nil → defaults to 1 in registry)
        assert!(
            rc.spec.replicas.is_none(),
            "replicas absent in wire body should decode as None"
        );
    }

    /// ReplicationControllerStatus.replicas is `int32` in Go (no omitempty),
    /// so Go leaves it at 0 when absent — we must decode it to 0 too.
    /// This reproduces the "missing field `replicas`" 400 BadRequest that
    /// blocked ~30 conformance specs (garbage_collector, rc lifecycle, etc.)
    #[test]
    fn test_rc_status_replicas_defaults_to_zero() {
        // status present but replicas absent — the exact shape sent by
        // many conformance tests.
        let json = r#"{
            "kind": "ReplicationController",
            "apiVersion": "v1",
            "metadata": {
                "name": "test-rc",
                "namespace": "default",
                "creationTimestamp": null
            },
            "spec": {
                "selector": {"app": "test"},
                "template": {
                    "metadata": {"labels": {"app": "test"}, "creationTimestamp": null},
                    "spec": {"containers": [{"name": "c", "image": "busybox"}]}
                }
            },
            "status": {
                "readyReplicas": 0,
                "availableReplicas": 0
            }
        }"#;

        let rc: ReplicationController = serde_json::from_str(json)
            .expect("RC with status missing replicas must decode (Go zero-value parity)");
        let status = rc.status.expect("status must be present");
        assert_eq!(
            status.replicas, 0,
            "status.replicas absent on wire must decode as 0"
        );
    }

    /// ReplicaSetStatus.replicas is `int32` in Go (no omitempty).
    #[test]
    fn test_replicaset_status_replicas_defaults_to_zero() {
        let json = r#"{
            "kind": "ReplicaSet",
            "apiVersion": "apps/v1",
            "metadata": {"name": "test-rs"},
            "spec": {
                "selector": {"matchLabels": {"app": "test"}},
                "template": {
                    "metadata": {"labels": {"app": "test"}},
                    "spec": {"containers": [{"name": "c", "image": "busybox"}]}
                }
            },
            "status": {
                "readyReplicas": 0,
                "availableReplicas": 0
            }
        }"#;

        let rs: ReplicaSet = serde_json::from_str(json)
            .expect("ReplicaSet with status missing replicas must decode");
        let status = rs.status.expect("status must be present");
        assert_eq!(
            status.replicas, 0,
            "status.replicas absent on wire must decode as 0"
        );
    }

    /// StatefulSetStatus.replicas is `int32` in Go (no omitempty).
    #[test]
    fn test_statefulset_status_replicas_defaults_to_zero() {
        let json = r#"{
            "kind": "StatefulSet",
            "apiVersion": "apps/v1",
            "metadata": {"name": "test-ss"},
            "spec": {
                "selector": {"matchLabels": {"app": "test"}},
                "template": {
                    "metadata": {"labels": {"app": "test"}},
                    "spec": {"containers": [{"name": "c", "image": "busybox"}]}
                },
                "serviceName": "test"
            },
            "status": {
                "readyReplicas": 0
            }
        }"#;

        let ss: StatefulSet = serde_json::from_str(json)
            .expect("StatefulSet with status missing replicas must decode");
        let status = ss.status.expect("status must be present");
        assert_eq!(
            status.replicas, 0,
            "status.replicas absent on wire must decode as 0"
        );
    }
}
