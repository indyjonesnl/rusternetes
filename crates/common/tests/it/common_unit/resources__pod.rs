//! Unit tests moved from `src/resources/pod.rs`.
#![allow(unused_imports, dead_code)]

mod __parent {
    pub use rusternetes_common::resources::policy::IntOrString;
    pub use rusternetes_common::types::{ObjectMeta, Phase, ResourceRequirements, TypeMeta};
    pub use serde::{Deserialize, Deserializer, Serialize};
    pub use std::collections::HashMap;
}

mod tests {
    use super::__parent::*;
    use rusternetes_common::resources::pod::*;

    /// #2453/#2454: Go's struct holds several states at once and encodes
    /// them all back (core/v1/types.go:3300 `ContainerState`).
    #[test]
    fn container_state_with_several_keys_round_trips() {
        let input = serde_json::json!({
            "running": {"startedAt": "2026-10-07T00:00:00Z"},
            "waiting": {"reason": "ContainerCreating"}
        });
        let s: ContainerState = serde_json::from_value(input.clone()).unwrap();
        assert_eq!(serde_json::to_value(&s).unwrap(), input);
        let s: ContainerState =
            serde_json::from_value(serde_json::json!({"running": {}, "waiting": null})).unwrap();
        assert_eq!(
            serde_json::to_value(&s).unwrap(),
            serde_json::json!({"running": {}})
        );
        let input = serde_json::json!({
            "waiting": {"reason": "x"},
            "terminated": {"exitCode": 1}
        });
        let s: ContainerState = serde_json::from_value(input.clone()).unwrap();
        assert_eq!(serde_json::to_value(&s).unwrap(), input);
    }

    /// Pod condition timestamps MUST serialize at whole-second precision, like
    /// upstream `metav1.Time.MarshalJSON`. Sub-second precision diverges from
    /// the protobuf/typed-client representation and breaks the
    /// `[sig-node] ... resize pod via the replace endpoint` conformance test,
    /// whose `Semantic.DeepEqual` compares the `/resize` (JSON) read against the
    /// main (typed) read of the same pod (#477).
    #[test]
    fn test_pod_condition_timestamp_serializes_at_second_precision() {
        let cond = PodCondition {
            condition_type: "Ready".to_string(),
            status: "True".to_string(),
            reason: None,
            message: None,
            last_probe_time: None,
            last_transition_time: Some(
                "2026-06-05T07:21:39.569909935Z"
                    .parse::<chrono::DateTime<chrono::Utc>>()
                    .unwrap(),
            ),
            observed_generation: None,
        };
        let json = serde_json::to_string(&cond).unwrap();
        assert!(
            json.contains("\"lastTransitionTime\":\"2026-06-05T07:21:39Z\""),
            "condition timestamp must serialize at second precision: {json}"
        );
        assert!(
            !json.contains(".569909935"),
            "must not emit sub-second precision: {json}"
        );
    }

    #[test]
    fn test_pod_with_pvc_volume_serialization() {
        let json = r#"{
  "apiVersion": "v1",
  "kind": "Pod",
  "metadata": {
    "name": "test-pod",
    "namespace": "default"
  },
  "spec": {
    "containers": [
      {
        "name": "test-container",
        "image": "nginx:latest",
        "volumeMounts": [
          {
            "name": "test-volume",
            "mountPath": "/data"
          }
        ]
      }
    ],
    "volumes": [
      {
        "name": "test-volume",
        "persistentVolumeClaim": {
          "claimName": "test-pvc"
        }
      }
    ]
  }
}"#;

        // Test deserialization
        let pod: Pod = serde_json::from_str(json).expect("Failed to deserialize Pod");

        assert_eq!(pod.metadata.name, "test-pod");
        let spec = pod.spec.as_ref().unwrap();
        assert_eq!(spec.containers.len(), 1);
        assert_eq!(spec.containers[0].name, "test-container");

        // Check volumes
        assert!(spec.volumes.is_some(), "volumes should be Some");
        let volumes = spec.volumes.as_ref().unwrap();
        assert_eq!(volumes.len(), 1);
        assert_eq!(volumes[0].name, "test-volume");
        assert!(
            volumes[0].persistent_volume_claim.is_some(),
            "persistent_volume_claim should be Some"
        );
        assert_eq!(
            volumes[0]
                .persistent_volume_claim
                .as_ref()
                .unwrap()
                .claim_name,
            "test-pvc"
        );

        // Check volume mounts
        assert!(
            spec.containers[0].volume_mounts.is_some(),
            "volume_mounts should be Some"
        );
        let mounts = spec.containers[0].volume_mounts.as_ref().unwrap();
        assert_eq!(mounts.len(), 1);
        assert_eq!(mounts[0].name, "test-volume");
        assert_eq!(mounts[0].mount_path, "/data");

        // Test serialization
        let serialized = serde_json::to_string_pretty(&pod).expect("Failed to serialize Pod");

        // Verify round-trip
        let pod2: Pod =
            serde_json::from_str(&serialized).expect("Failed to deserialize serialized Pod");
        let spec2 = pod2.spec.as_ref().unwrap();
        assert!(spec2.volumes.is_some());
        assert_eq!(spec2.volumes.as_ref().unwrap()[0].name, "test-volume");
    }

    #[test]
    fn test_pod_with_init_containers() {
        let spec = PodSpec {
            init_containers: Some(vec![
                Container {
                    name: "init-myservice".to_string(),
                    image: "busybox:1.28".to_string(),
                    command: Some(vec![
                        "sh".to_string(),
                        "-c".to_string(),
                        "echo initializing".to_string(),
                    ]),
                    args: None,
                    working_dir: None,
                    ports: None,
                    env: None,
                    resources: None,
                    volume_mounts: None,
                    image_pull_policy: None,
                    liveness_probe: None,
                    readiness_probe: None,
                    startup_probe: None,
                    security_context: None,
                    restart_policy: None,
                    resize_policy: None,
                    lifecycle: None,
                    termination_message_path: None,
                    termination_message_policy: None,
                    stdin: None,
                    stdin_once: None,
                    tty: None,
                    env_from: None,
                    volume_devices: None,
                    ..Default::default()
                },
                Container {
                    name: "init-mydb".to_string(),
                    image: "busybox:1.28".to_string(),
                    command: Some(vec![
                        "sh".to_string(),
                        "-c".to_string(),
                        "echo waiting for db".to_string(),
                    ]),
                    args: None,
                    working_dir: None,
                    ports: None,
                    env: None,
                    resources: None,
                    volume_mounts: None,
                    image_pull_policy: None,
                    liveness_probe: None,
                    readiness_probe: None,
                    startup_probe: None,
                    security_context: None,
                    restart_policy: None,
                    resize_policy: None,
                    lifecycle: None,
                    termination_message_path: None,
                    termination_message_policy: None,
                    stdin: None,
                    stdin_once: None,
                    tty: None,
                    env_from: None,
                    volume_devices: None,
                    ..Default::default()
                },
            ]),
            containers: vec![Container {
                name: "app".to_string(),
                image: "nginx:latest".to_string(),
                command: None,
                args: None,
                working_dir: None,
                ports: Some(vec![ContainerPort {
                    container_port: 80,
                    name: Some("http".to_string()),
                    protocol: "TCP".to_string(),
                    host_port: None,
                    host_ip: None,
                }]),
                env: None,
                resources: None,
                volume_mounts: None,
                image_pull_policy: None,
                liveness_probe: None,
                readiness_probe: None,
                startup_probe: None,
                security_context: None,
                restart_policy: None,
                resize_policy: None,
                lifecycle: None,
                termination_message_path: None,
                termination_message_policy: None,
                stdin: None,
                stdin_once: None,
                tty: None,
                env_from: None,
                volume_devices: None,
                ..Default::default()
            }],
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
        };

        let pod = Pod::new("myapp-pod", spec);

        assert_eq!(pod.metadata.name, "myapp-pod");
        let pod_spec = pod.spec.as_ref().unwrap();

        // Verify init containers
        assert!(pod_spec.init_containers.is_some());
        let init_containers = pod_spec.init_containers.as_ref().unwrap();
        assert_eq!(init_containers.len(), 2);
        assert_eq!(init_containers[0].name, "init-myservice");
        assert_eq!(init_containers[1].name, "init-mydb");

        // Verify main containers
        assert_eq!(pod_spec.containers.len(), 1);
        assert_eq!(pod_spec.containers[0].name, "app");
    }

    #[test]
    fn test_pod_with_init_containers_serialization() {
        let json = r#"{
  "apiVersion": "v1",
  "kind": "Pod",
  "metadata": {
    "name": "myapp-pod",
    "namespace": "default"
  },
  "spec": {
    "initContainers": [
      {
        "name": "init-myservice",
        "image": "busybox:1.28",
        "command": ["sh", "-c", "until nslookup myservice; do echo waiting for myservice; sleep 2; done;"]
      },
      {
        "name": "init-mydb",
        "image": "busybox:1.28",
        "command": ["sh", "-c", "until nslookup mydb; do echo waiting for mydb; sleep 2; done;"]
      }
    ],
    "containers": [
      {
        "name": "myapp-container",
        "image": "nginx:latest",
        "ports": [
          {
            "containerPort": 80
          }
        ]
      }
    ]
  }
}"#;

        let pod: Pod = serde_json::from_str(json).unwrap();
        assert_eq!(pod.metadata.name, "myapp-pod");

        let spec = pod.spec.as_ref().unwrap();
        assert!(spec.init_containers.is_some());

        let init_containers = spec.init_containers.as_ref().unwrap();
        assert_eq!(init_containers.len(), 2);
        assert_eq!(init_containers[0].name, "init-myservice");
        assert_eq!(init_containers[0].image, "busybox:1.28");
        assert_eq!(init_containers[1].name, "init-mydb");

        assert_eq!(spec.containers.len(), 1);
        assert_eq!(spec.containers[0].name, "myapp-container");

        // Test round-trip serialization
        let serialized = serde_json::to_string(&pod).unwrap();
        let pod2: Pod = serde_json::from_str(&serialized).unwrap();
        assert!(pod2.spec.as_ref().unwrap().init_containers.is_some());
        assert_eq!(
            pod2.spec
                .as_ref()
                .unwrap()
                .init_containers
                .as_ref()
                .unwrap()
                .len(),
            2
        );
    }

    #[test]
    fn test_pod_status_with_init_container_statuses() {
        let status = PodStatus {
            phase: Some(Phase::Running),
            message: None,
            reason: None,
            host_ip: Some("192.168.1.1".to_string()),
            host_i_ps: None,
            pod_ip: Some("10.0.0.1".to_string()),
            pod_i_ps: None,
            nominated_node_name: None,
            qos_class: None,
            start_time: None,
            container_statuses: Some(vec![ContainerStatus {
                name: "app".to_string(),
                ready: true,
                restart_count: 0,
                state: Some(ContainerState {
                    running: Some(ContainerStateRunning {
                        started_at: Some("2024-01-01T00:00:00Z".to_string()),
                    }),
                    ..Default::default()
                }),
                last_state: None,
                image: Some("nginx:latest".to_string()),
                image_id: None,
                container_id: Some("containerd://abc123".to_string()),
                started: None,
                allocated_resources: None,
                allocated_resources_status: None,
                resources: None,
                user: None,
                volume_mounts: None,
                stop_signal: None,
            }]),
            init_container_statuses: Some(vec![
                ContainerStatus {
                    name: "init-myservice".to_string(),
                    ready: true,
                    restart_count: 0,
                    state: Some(ContainerState {
                        terminated: Some(ContainerStateTerminated {
                            exit_code: 0,
                            reason: Some("Completed".to_string()),
                            signal: None,
                            message: None,
                            started_at: None,
                            finished_at: None,
                            container_id: None,
                        }),
                        ..Default::default()
                    }),
                    last_state: None,
                    image: Some("busybox:1.28".to_string()),
                    image_id: None,
                    container_id: Some("containerd://def456".to_string()),
                    started: None,
                    allocated_resources: None,
                    allocated_resources_status: None,
                    resources: None,
                    user: None,
                    volume_mounts: None,
                    stop_signal: None,
                },
                ContainerStatus {
                    name: "init-mydb".to_string(),
                    ready: true,
                    restart_count: 0,
                    state: Some(ContainerState {
                        terminated: Some(ContainerStateTerminated {
                            exit_code: 0,
                            reason: Some("Completed".to_string()),
                            signal: None,
                            message: None,
                            started_at: None,
                            finished_at: None,
                            container_id: None,
                        }),
                        ..Default::default()
                    }),
                    last_state: None,
                    image: Some("busybox:1.28".to_string()),
                    image_id: None,
                    container_id: Some("containerd://ghi789".to_string()),
                    started: None,
                    allocated_resources: None,
                    allocated_resources_status: None,
                    resources: None,
                    user: None,
                    volume_mounts: None,
                    stop_signal: None,
                },
            ]),
            ephemeral_container_statuses: None,
            conditions: None,
            resize: None,
            resource_claim_statuses: None,
            observed_generation: None,
            ..Default::default()
        };

        assert_eq!(status.phase, Some(Phase::Running));
        assert!(status.init_container_statuses.is_some());

        let init_statuses = status.init_container_statuses.as_ref().unwrap();
        assert_eq!(init_statuses.len(), 2);
        assert_eq!(init_statuses[0].name, "init-myservice");
        assert_eq!(init_statuses[1].name, "init-mydb");

        // Verify both init containers completed successfully
        for init_status in init_statuses {
            if let Some(ContainerState {
                terminated: Some(ContainerStateTerminated { exit_code, .. }),
                ..
            }) = &init_status.state
            {
                assert_eq!(*exit_code, 0);
            }
        }
    }

    #[test]
    fn test_env_var_with_field_ref_serialization() {
        // Test deserialization of env var with fieldRef
        let json = r#"{"name":"NODE_NAME","valueFrom":{"fieldRef":{"fieldPath":"spec.nodeName"}}}"#;
        let env_var: EnvVar = serde_json::from_str(json).expect("Failed to deserialize EnvVar");

        assert_eq!(env_var.name, "NODE_NAME");
        assert!(env_var.value.is_none());
        assert!(env_var.value_from.is_some());

        let value_from = env_var.value_from.as_ref().unwrap();
        assert!(value_from.field_ref.is_some());
        assert_eq!(
            value_from.field_ref.as_ref().unwrap().field_path,
            "spec.nodeName"
        );

        // Test serialization - should preserve the fieldRef
        let serialized = serde_json::to_string(&env_var).expect("Failed to serialize EnvVar");
        println!("Serialized: {}", serialized);

        // Verify valueFrom is in the serialized output
        assert!(serialized.contains("valueFrom"));
        assert!(serialized.contains("fieldRef"));
        assert!(serialized.contains("spec.nodeName"));

        // Test round-trip
        let env_var2: EnvVar =
            serde_json::from_str(&serialized).expect("Failed to deserialize serialized EnvVar");
        assert!(env_var2.value_from.is_some());
        assert!(env_var2.value_from.as_ref().unwrap().field_ref.is_some());
    }

    #[test]
    fn test_env_var_with_field_ref_via_value() {
        // Test the round-trip through serde_json::Value (what the API server does)
        let json = r#"{"name":"NODE_NAME","valueFrom":{"fieldRef":{"fieldPath":"spec.nodeName"}}}"#;
        let env_var: EnvVar = serde_json::from_str(json).expect("Failed to deserialize EnvVar");

        // Convert to Value and back (simulating webhook flow)
        let value = serde_json::to_value(&env_var).expect("Failed to convert to Value");
        println!(
            "As Value: {}",
            serde_json::to_string_pretty(&value).unwrap()
        );

        let env_var2: EnvVar = serde_json::from_value(value).expect("Failed to convert from Value");

        // Verify fieldRef is still there after round-trip through Value
        assert!(
            env_var2.value_from.is_some(),
            "valueFrom should be Some after Value round-trip"
        );
        let value_from = env_var2.value_from.as_ref().unwrap();
        assert!(
            value_from.field_ref.is_some(),
            "fieldRef should be Some after Value round-trip"
        );
        assert_eq!(
            value_from.field_ref.as_ref().unwrap().field_path,
            "spec.nodeName"
        );
    }

    #[test]
    fn test_pod_spec_clone_serialization() {
        use super::*;

        // Create a PodSpec with env vars that have valueFrom.fieldRef
        let original_spec = PodSpec {
            containers: vec![Container {
                name: "test".to_string(),
                image: "busybox".to_string(),
                env: Some(vec![EnvVar {
                    name: "NODE_NAME".to_string(),
                    value: None,
                    value_from: Some(EnvVarSource {
                        field_ref: Some(ObjectFieldSelector {
                            field_path: "spec.nodeName".to_string(),
                            api_version: None,
                        }),
                        config_map_key_ref: None,
                        secret_key_ref: None,
                        resource_field_ref: None,
                        file_key_ref: None,
                    }),
                }]),
                command: None,
                args: None,
                working_dir: None,
                ports: None,
                resources: None,
                volume_mounts: None,
                image_pull_policy: None,
                liveness_probe: None,
                readiness_probe: None,
                startup_probe: None,
                security_context: None,
                restart_policy: None,
                resize_policy: None,
                lifecycle: None,
                termination_message_path: None,
                termination_message_policy: None,
                stdin: None,
                stdin_once: None,
                tty: None,
                env_from: None,
                volume_devices: None,
                ..Default::default()
            }],
            init_containers: None,
            ephemeral_containers: None,
            volumes: None,
            restart_policy: None,
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
        };

        // Clone it (like DaemonSet controller does)
        let cloned_spec = original_spec.clone();

        // Serialize and deserialize (like etcd storage does)
        let json = serde_json::to_string(&cloned_spec).unwrap();
        println!("Serialized JSON:\n{}", json);

        let deserialized: PodSpec = serde_json::from_str(&json).unwrap();

        // Check if valueFrom.fieldRef is preserved
        let env_var = &deserialized.containers[0].env.as_ref().unwrap()[0];
        assert_eq!(env_var.name, "NODE_NAME");
        assert!(env_var.value_from.is_some(), "value_from should be Some");
        assert!(
            env_var.value_from.as_ref().unwrap().field_ref.is_some(),
            "field_ref should be Some"
        );
        assert_eq!(
            env_var
                .value_from
                .as_ref()
                .unwrap()
                .field_ref
                .as_ref()
                .unwrap()
                .field_path,
            "spec.nodeName"
        );
    }

    #[test]
    fn test_pod_resource_claim_serialization() {
        // Test that PodResourceClaim with resourceClaimName serializes correctly
        let resource_claim = PodResourceClaim {
            name: "test-claim".to_string(),
            resource_claim_name: Some("my-resource-claim".to_string()),
            resource_claim_template_name: None,
        };

        let json =
            serde_json::to_string(&resource_claim).expect("Failed to serialize PodResourceClaim");
        println!("Serialized PodResourceClaim: {}", json);

        assert!(
            json.contains("resourceClaimName"),
            "Should contain resourceClaimName field"
        );
        assert!(
            json.contains("my-resource-claim"),
            "Should contain the claim name"
        );

        // Test round-trip
        let deserialized: PodResourceClaim =
            serde_json::from_str(&json).expect("Failed to deserialize PodResourceClaim");
        assert_eq!(deserialized.name, "test-claim");
        assert_eq!(
            deserialized.resource_claim_name,
            Some("my-resource-claim".to_string())
        );
        assert_eq!(deserialized.resource_claim_template_name, None);

        // Test with resourceClaimTemplateName instead
        let resource_claim2 = PodResourceClaim {
            name: "template-claim".to_string(),
            resource_claim_name: None,
            resource_claim_template_name: Some("my-template".to_string()),
        };

        let json2 = serde_json::to_string(&resource_claim2)
            .expect("Failed to serialize PodResourceClaim with template");
        assert!(
            json2.contains("resourceClaimTemplateName"),
            "Should contain resourceClaimTemplateName field"
        );
        assert!(
            json2.contains("my-template"),
            "Should contain the template name"
        );
    }

    #[test]
    fn test_downward_api_volume_roundtrip() {
        let json = r#"{
            "name": "podinfo",
            "downwardAPI": {
                "items": [
                    {
                        "path": "labels",
                        "fieldRef": {
                            "fieldPath": "metadata.labels"
                        },
                        "mode": 256
                    }
                ]
            }
        }"#;

        let vol: Volume = serde_json::from_str(json).expect("Failed to deserialize Volume");
        assert_eq!(vol.name, "podinfo");
        assert!(
            vol.downward_api.is_some(),
            "downward_api should be Some but was None"
        );
        let da = vol.downward_api.as_ref().unwrap();
        assert!(da.items.is_some(), "items should be Some");
        let items = da.items.as_ref().unwrap();
        assert_eq!(items.len(), 1);
        assert_eq!(items[0].path, "labels");
        assert_eq!(items[0].mode, Some(256));

        // Verify serialization preserves the field name as "downwardAPI"
        let serialized = serde_json::to_value(&vol).expect("Failed to serialize");
        assert!(
            serialized.get("downwardAPI").is_some(),
            "Serialized JSON should have 'downwardAPI' key, got: {}",
            serde_json::to_string_pretty(&serialized).unwrap()
        );

        // Verify round-trip through serde_json::Value
        let vol2: Volume = serde_json::from_value(serialized).expect("Failed round-trip");
        assert!(
            vol2.downward_api.is_some(),
            "downward_api should survive round-trip"
        );
    }

    #[test]
    fn test_pod_resize_status_serialization() {
        // Pod with resize="Proposed" should serialize/deserialize correctly
        let json = r#"{
            "apiVersion": "v1",
            "kind": "Pod",
            "metadata": { "name": "resize-pod", "namespace": "default" },
            "spec": {
                "containers": [{
                    "name": "app",
                    "image": "nginx:latest",
                    "resources": {
                        "requests": { "cpu": "100m", "memory": "128Mi" },
                        "limits": { "cpu": "200m", "memory": "256Mi" }
                    },
                    "resizePolicy": [
                        { "resourceName": "cpu", "restartPolicy": "NotRequired" },
                        { "resourceName": "memory", "restartPolicy": "RestartContainer" }
                    ]
                }]
            },
            "status": {
                "phase": "Running",
                "resize": "Proposed",
                "containerStatuses": [{
                    "name": "app",
                    "ready": true,
                    "restartCount": 0,
                    "state": { "running": { "startedAt": "2024-01-01T00:00:00Z" } },
                    "allocatedResources": {
                        "cpu": "100m",
                        "memory": "128Mi"
                    },
                    "resources": {
                        "requests": { "cpu": "100m", "memory": "128Mi" },
                        "limits": { "cpu": "200m", "memory": "256Mi" }
                    }
                }]
            }
        }"#;

        let pod: Pod =
            serde_json::from_str(json).expect("Failed to deserialize pod with resize fields");

        // Verify resize status field
        let status = pod.status.as_ref().unwrap();
        assert_eq!(status.resize.as_deref(), Some("Proposed"));

        // Verify allocatedResources in container status
        let cs = &status.container_statuses.as_ref().unwrap()[0];
        let alloc = cs.allocated_resources.as_ref().unwrap();
        assert_eq!(alloc.get("cpu"), Some(&"100m".to_string()));
        assert_eq!(alloc.get("memory"), Some(&"128Mi".to_string()));

        // Verify resources in container status
        let res = cs.resources.as_ref().unwrap();
        assert_eq!(
            res.requests.as_ref().unwrap().get("cpu"),
            Some(&"100m".to_string())
        );

        // Verify resizePolicy in spec container
        let spec = pod.spec.as_ref().unwrap();
        let resize_policy = spec.containers[0].resize_policy.as_ref().unwrap();
        assert_eq!(resize_policy.len(), 2);
        assert_eq!(resize_policy[0].resource_name, "cpu");
        assert_eq!(resize_policy[0].restart_policy, "NotRequired");
        assert_eq!(resize_policy[1].resource_name, "memory");
        assert_eq!(resize_policy[1].restart_policy, "RestartContainer");
    }

    #[test]
    fn test_pod_resize_status_roundtrip() {
        use std::collections::HashMap;

        let mut alloc = HashMap::new();
        alloc.insert("cpu".to_string(), "250m".to_string());
        alloc.insert("memory".to_string(), "512Mi".to_string());

        let pod = Pod {
            type_meta: TypeMeta {
                kind: "Pod".to_string(),
                api_version: "v1".to_string(),
            },
            metadata: ObjectMeta::new("resize-roundtrip").with_namespace("default"),
            spec: Some(PodSpec {
                containers: vec![Container {
                    name: "app".to_string(),
                    image: "nginx".to_string(),
                    resize_policy: Some(vec![ContainerResizePolicy {
                        resource_name: "cpu".to_string(),
                        restart_policy: "NotRequired".to_string(),
                    }]),
                    resources: Some(rusternetes_common::types::ResourceRequirements {
                        requests: Some({
                            let mut m = HashMap::new();
                            m.insert("cpu".to_string(), "500m".to_string());
                            m
                        }),
                        limits: Some({
                            let mut m = HashMap::new();
                            m.insert("cpu".to_string(), "1".to_string());
                            m
                        }),
                        claims: None,
                    }),
                    command: None,
                    args: None,
                    working_dir: None,
                    ports: None,
                    volume_mounts: None,
                    image_pull_policy: None,
                    liveness_probe: None,
                    readiness_probe: None,
                    startup_probe: None,
                    security_context: None,
                    restart_policy: None,
                    lifecycle: None,
                    termination_message_path: None,
                    termination_message_policy: None,
                    stdin: None,
                    stdin_once: None,
                    tty: None,
                    env: None,
                    env_from: None,
                    volume_devices: None,
                    ..Default::default()
                }],
                init_containers: None,
                ephemeral_containers: None,
                volumes: None,
                restart_policy: None,
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
            }),
            status: Some(PodStatus {
                phase: Some(Phase::Running),
                message: None,
                reason: None,
                host_ip: Some("10.0.0.1".to_string()),
                host_i_ps: None,
                pod_ip: Some("10.244.0.5".to_string()),
                pod_i_ps: None,
                nominated_node_name: None,
                qos_class: None,
                start_time: None,
                conditions: None,
                container_statuses: Some(vec![ContainerStatus {
                    name: "app".to_string(),
                    ready: true,
                    restart_count: 0,
                    state: Some(ContainerState {
                        running: Some(ContainerStateRunning {
                            started_at: Some("2024-01-01T00:00:00Z".to_string()),
                        }),
                        ..Default::default()
                    }),
                    last_state: None,
                    image: Some("nginx".to_string()),
                    image_id: None,
                    container_id: Some("docker://abc123".to_string()),
                    started: Some(true),
                    allocated_resources: Some(alloc.clone()),
                    allocated_resources_status: None,
                    resources: Some(rusternetes_common::types::ResourceRequirements {
                        requests: Some({
                            let mut m = HashMap::new();
                            m.insert("cpu".to_string(), "250m".to_string());
                            m
                        }),
                        limits: None,
                        claims: None,
                    }),
                    user: None,
                    volume_mounts: None,
                    stop_signal: None,
                }]),
                init_container_statuses: None,
                ephemeral_container_statuses: None,
                resize: Some("InProgress".to_string()),
                resource_claim_statuses: None,
                observed_generation: None,
                ..Default::default()
            }),
        };

        // Serialize and deserialize
        let json_str = serde_json::to_string(&pod).unwrap();
        let deserialized: Pod = serde_json::from_str(&json_str).unwrap();

        // Check resize status survived round-trip
        let status = deserialized.status.as_ref().unwrap();
        assert_eq!(status.resize.as_deref(), Some("InProgress"));

        // Check allocatedResources survived round-trip
        let cs = &status.container_statuses.as_ref().unwrap()[0];
        let ar = cs.allocated_resources.as_ref().unwrap();
        assert_eq!(ar.get("cpu"), Some(&"250m".to_string()));
        assert_eq!(ar.get("memory"), Some(&"512Mi".to_string()));

        // Check resizePolicy survived round-trip
        let spec = deserialized.spec.as_ref().unwrap();
        let rp = spec.containers[0].resize_policy.as_ref().unwrap();
        assert_eq!(rp[0].resource_name, "cpu");
        assert_eq!(rp[0].restart_policy, "NotRequired");

        // Check JSON field names are camelCase
        let json_val = serde_json::to_value(&pod).unwrap();
        let status_val = &json_val["status"];
        assert!(status_val.get("resize").is_some());
        let cs_val = &status_val["containerStatuses"][0];
        assert!(cs_val.get("allocatedResources").is_some());

        let spec_val = &json_val["spec"]["containers"][0];
        assert!(spec_val.get("resizePolicy").is_some());
    }

    #[test]
    fn test_pod_resize_empty_string_means_complete() {
        // When resize is empty string, it means resize is complete
        let json = r#"{
            "apiVersion": "v1",
            "kind": "Pod",
            "metadata": { "name": "resize-done", "namespace": "default" },
            "spec": {
                "containers": [{ "name": "app", "image": "nginx" }]
            },
            "status": {
                "phase": "Running",
                "resize": ""
            }
        }"#;

        let pod: Pod = serde_json::from_str(json).unwrap();
        let status = pod.status.as_ref().unwrap();
        assert_eq!(status.resize.as_deref(), Some(""));
    }

    #[test]
    fn test_pod_resize_omitted_when_none() {
        // When resize is None, it should not appear in serialized JSON
        let pod = Pod {
            type_meta: TypeMeta {
                kind: "Pod".to_string(),
                api_version: "v1".to_string(),
            },
            metadata: ObjectMeta::new("no-resize").with_namespace("default"),
            spec: Some(PodSpec {
                containers: vec![Container {
                    name: "app".to_string(),
                    image: "nginx".to_string(),
                    command: None,
                    args: None,
                    working_dir: None,
                    ports: None,
                    resources: None,
                    volume_mounts: None,
                    image_pull_policy: None,
                    liveness_probe: None,
                    readiness_probe: None,
                    startup_probe: None,
                    security_context: None,
                    restart_policy: None,
                    resize_policy: None,
                    lifecycle: None,
                    termination_message_path: None,
                    termination_message_policy: None,
                    stdin: None,
                    stdin_once: None,
                    tty: None,
                    env: None,
                    env_from: None,
                    volume_devices: None,
                    ..Default::default()
                }],
                init_containers: None,
                ephemeral_containers: None,
                volumes: None,
                restart_policy: None,
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
            }),
            status: Some(PodStatus {
                phase: Some(Phase::Running),
                message: None,
                reason: None,
                host_ip: None,
                host_i_ps: None,
                pod_ip: None,
                pod_i_ps: None,
                nominated_node_name: None,
                qos_class: None,
                start_time: None,
                conditions: None,
                container_statuses: None,
                init_container_statuses: None,
                ephemeral_container_statuses: None,
                resize: None,
                resource_claim_statuses: None,
                observed_generation: None,
                ..Default::default()
            }),
        };

        let json = serde_json::to_value(&pod).unwrap();
        let status_val = &json["status"];
        // resize should be omitted when None (skip_serializing_if)
        assert!(
            status_val.get("resize").is_none(),
            "resize should be omitted from JSON when None"
        );
    }

    #[test]
    fn probe_port_accepts_named_string() {
        // K8s spec: probe.httpGet.port is IntOrString — must accept a named
        // port that refers to container.ports[].name (e.g. "http").
        // Previously typed as i32, which rejected named-port YAML at parse
        // time and broke any pod using named-port probes.
        let json = r#"{"path":"/health","port":"http","scheme":"HTTP"}"#;
        let parsed: HTTPGetAction = serde_json::from_str(json).expect("named port must parse");
        assert_eq!(parsed.port, IntOrString::String("http".to_string()));

        let json2 = r#"{"port":8080}"#;
        let parsed2: HTTPGetAction = serde_json::from_str(json2).expect("int port must parse");
        assert_eq!(parsed2.port, IntOrString::Int(8080));
    }

    #[test]
    fn resolve_probe_port_int_passes_through() {
        let c = Container::default();
        assert_eq!(resolve_probe_port(&IntOrString::Int(80), &c), Some(80));
        assert_eq!(resolve_probe_port(&IntOrString::Int(-1), &c), None);
        assert_eq!(
            resolve_probe_port(&IntOrString::Int(70_000), &c),
            None,
            "ports outside u16 range must not resolve"
        );
    }

    #[test]
    fn resolve_probe_port_named_via_container_ports() {
        let mut c = Container {
            name: "web".to_string(),
            ..Default::default()
        };
        c.ports = Some(vec![
            ContainerPort {
                container_port: 8080,
                name: Some("http".to_string()),
                protocol: "TCP".to_string(),
                host_port: None,
                host_ip: None,
            },
            ContainerPort {
                container_port: 8443,
                name: Some("https".to_string()),
                protocol: "TCP".to_string(),
                host_port: None,
                host_ip: None,
            },
        ]);
        assert_eq!(
            resolve_probe_port(&IntOrString::String("http".to_string()), &c),
            Some(8080)
        );
        assert_eq!(
            resolve_probe_port(&IntOrString::String("https".to_string()), &c),
            Some(8443)
        );
        // Unknown named port: falls back to parsing the string as a number,
        // returning None when neither lookup nor parse succeeds.
        assert_eq!(
            resolve_probe_port(&IntOrString::String("missing".to_string()), &c),
            None
        );
        // A numeric string is parsed as a fallback (matches K8s relaxed behavior).
        assert_eq!(
            resolve_probe_port(&IntOrString::String("9000".to_string()), &c),
            Some(9000)
        );
    }

    /// Regression test: upstream `PodCondition.LastProbeTime` must round-trip
    /// through serde so strict-decoding Pod-status updates don't reject the
    /// field with `unknown field "status.conditions[N].lastProbeTime"`.
    #[test]
    fn pod_condition_round_trips_last_probe_time() {
        let json = r#"{
            "type": "Ready",
            "status": "True",
            "lastProbeTime": "2026-05-27T10:15:30Z",
            "lastTransitionTime": "2026-05-27T10:15:35Z"
        }"#;

        let cond: PodCondition =
            serde_json::from_str(json).expect("PodCondition must accept lastProbeTime");
        assert_eq!(cond.condition_type, "Ready");
        assert_eq!(cond.status, "True");
        assert!(
            cond.last_probe_time.is_some(),
            "lastProbeTime must deserialize into Some"
        );
        assert!(cond.last_transition_time.is_some());

        let serialized =
            serde_json::to_value(&cond).expect("PodCondition must serialize back to JSON");
        assert_eq!(
            serialized.get("lastProbeTime").and_then(|v| v.as_str()),
            Some("2026-05-27T10:15:30Z"),
            "lastProbeTime must round-trip with the upstream camelCase name"
        );

        // Absent lastProbeTime must remain absent on the wire (skip_serializing_if).
        let minimal: PodCondition = serde_json::from_str(
            r#"{"type":"Ready","status":"True","lastTransitionTime":"2026-05-27T10:15:35Z"}"#,
        )
        .expect("PodCondition without lastProbeTime must still parse");
        assert!(minimal.last_probe_time.is_none());
        let serialized = serde_json::to_value(&minimal).unwrap();
        assert!(
            serialized.get("lastProbeTime").is_none(),
            "absent lastProbeTime must not be serialized"
        );
    }

    /// A deprecated in-tree source survives decode/encode untouched (#2534).
    #[test]
    fn volume_legacy_sources_round_trip_losslessly() {
        let src = serde_json::json!({
            "name": "v",
            "gcePersistentDisk": {"pdName": "d", "fsType": "ext4", "partition": 2},
            "storageos": {"volumeName": "s"},
        });
        let v: Volume = serde_json::from_value(src.clone()).unwrap();
        assert!(v.legacy_sources.gce_persistent_disk.is_some());
        assert!(v.legacy_sources.storageos.is_some());
        assert!(v.legacy_sources.rbd.is_none());
        assert_eq!(serde_json::to_value(&v).unwrap(), src);
    }

    /// `gcePersistentDisk` / `awsElasticBlockStore` decode into the typed
    /// core/v1 sources (`GCEPersistentDiskVolumeSource`,
    /// `AWSElasticBlockStoreVolumeSource`) that csi-translation reads (#3739),
    /// and re-encode unchanged.
    #[test]
    fn volume_gce_and_ebs_sources_are_typed() {
        use rusternetes_common::resources::volume::{
            AWSElasticBlockStoreVolumeSource, GCEPersistentDiskVolumeSource,
        };
        let src = serde_json::json!({
            "name": "v",
            "gcePersistentDisk": {"pdName": "d", "fsType": "ext4", "partition": 2, "readOnly": true},
        });
        let v: Volume = serde_json::from_value(src.clone()).unwrap();
        assert_eq!(
            v.legacy_sources.gce_persistent_disk,
            Some(GCEPersistentDiskVolumeSource {
                pd_name: "d".into(),
                fs_type: Some("ext4".into()),
                partition: Some(2),
                read_only: Some(true),
            })
        );
        assert_eq!(serde_json::to_value(&v).unwrap(), src);

        let src = serde_json::json!({
            "name": "e",
            "awsElasticBlockStore": {"volumeID": "vol-1", "partition": 1},
        });
        let v: Volume = serde_json::from_value(src.clone()).unwrap();
        assert_eq!(
            v.legacy_sources.aws_elastic_block_store,
            Some(AWSElasticBlockStoreVolumeSource {
                volume_id: "vol-1".into(),
                fs_type: None,
                partition: Some(1),
                read_only: None,
            })
        );
        assert_eq!(serde_json::to_value(&v).unwrap(), src);
    }
}
