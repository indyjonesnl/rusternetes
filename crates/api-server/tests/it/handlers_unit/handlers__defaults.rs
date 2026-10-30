//! Unit tests moved from `src/handlers/defaults.rs`.
#![allow(unused_imports, dead_code)]

mod __parent {
    pub use rusternetes_common::resources::deployment::{
        DeploymentStrategy, RollingUpdateDeployment,
    };
    pub use rusternetes_common::resources::policy::IntOrString;
    pub use rusternetes_common::resources::workloads::{
        DaemonSetUpdateStrategy, RollingUpdateDaemonSet, RollingUpdateStatefulSetStrategy,
        StatefulSetUpdateStrategy,
    };
    pub use rusternetes_common::resources::PodSpec;
}

mod tests {
    use super::__parent::*;
    use rusternetes_api_server::handlers::defaults::*;
    use rusternetes_common::resources::{Container, PodSpec, PodTemplateSpec};

    #[test]
    fn defaults_env_field_ref_api_version_to_v1() {
        // K8s SetDefaults_ObjectFieldSelector: an env valueFrom.fieldRef with no
        // apiVersion defaults to "v1". Real manifests (e.g. flannel) omit it and
        // rely on this; without it the create-path validation rejects the pod
        // with "fieldRef.apiVersion: Required value".
        let mut c: Container = serde_json::from_value(serde_json::json!({
            "name": "c",
            "image": "busybox:1.36",
            "env": [{
                "name": "POD_NODE",
                "valueFrom": { "fieldRef": { "fieldPath": "spec.nodeName" } }
            }]
        }))
        .unwrap();

        apply_container_defaults(&mut c);

        let env_var = c.env.unwrap().remove(0);
        let field_ref = env_var.value_from.unwrap().field_ref.unwrap();
        assert_eq!(field_ref.api_version.as_deref(), Some("v1"));
    }

    #[test]
    fn test_apply_pod_spec_defaults() {
        let mut spec = PodSpec {
            containers: vec![Container {
                name: "test".to_string(),
                image: "nginx:1.19".to_string(),
                ..Default::default()
            }],
            ..Default::default()
        };

        apply_pod_spec_defaults(&mut spec);

        assert_eq!(spec.dns_policy.as_deref(), Some("ClusterFirst"));
        assert_eq!(spec.restart_policy.as_deref(), Some("Always"));
        assert_eq!(spec.termination_grace_period_seconds, Some(30));
        assert_eq!(spec.scheduler_name.as_deref(), Some("default-scheduler"));

        let c = &spec.containers[0];
        assert_eq!(
            c.termination_message_path.as_deref(),
            Some("/dev/termination-log")
        );
        assert_eq!(c.termination_message_policy.as_deref(), Some("File"));
        assert_eq!(c.image_pull_policy.as_deref(), Some("IfNotPresent"));
    }

    #[test]
    fn test_ephemeral_container_defaults() {
        use rusternetes_common::resources::pod::EphemeralContainer;
        let mut spec = PodSpec {
            containers: vec![Container {
                name: "app".to_string(),
                image: "nginx:1.19".to_string(),
                ..Default::default()
            }],
            ephemeral_containers: Some(vec![EphemeralContainer {
                name: "debugger".to_string(),
                image: "busybox:latest".to_string(),
                ..Default::default()
            }]),
            ..Default::default()
        };

        apply_pod_spec_defaults(&mut spec);

        let ec = &spec.ephemeral_containers.as_ref().unwrap()[0];
        assert_eq!(
            ec.termination_message_path.as_deref(),
            Some("/dev/termination-log")
        );
        assert_eq!(ec.termination_message_policy.as_deref(), Some("File"));
        // ":latest" tag → Always
        assert_eq!(ec.image_pull_policy.as_deref(), Some("Always"));
    }

    #[test]
    fn test_container_defaults_latest_tag() {
        let mut spec = PodSpec {
            containers: vec![Container {
                name: "test".to_string(),
                image: "nginx:latest".to_string(),
                ..Default::default()
            }],
            ..Default::default()
        };

        apply_pod_spec_defaults(&mut spec);
        assert_eq!(
            spec.containers[0].image_pull_policy.as_deref(),
            Some("Always")
        );
    }

    #[test]
    fn test_container_defaults_no_tag() {
        let mut spec = PodSpec {
            containers: vec![Container {
                name: "test".to_string(),
                image: "nginx".to_string(),
                ..Default::default()
            }],
            ..Default::default()
        };

        apply_pod_spec_defaults(&mut spec);
        assert_eq!(
            spec.containers[0].image_pull_policy.as_deref(),
            Some("Always")
        );
    }

    /// Regression: protobuf-encoded PodSpecs from client-go arrive with
    /// `schedulerName: ""` for an unset Go `string` field (no `omitempty`
    /// over the protobuf wire — zero values are still emitted). Upstream
    /// `SetDefaults_PodSpec` checks `if obj.SchedulerName == ""`, so an
    /// empty string is also defaulted to `v1.DefaultSchedulerName`. The
    /// rusternetes check used `Option::is_none()` only and missed
    /// `Some("")`, which made the scheduler silently skip such pods
    /// (their canonical name stayed `""` ≠ `"default-scheduler"`).
    /// Surfaced via hydrophone after PR #134.
    #[test]
    fn test_empty_scheduler_name_defaults_to_default_scheduler() {
        let mut spec = PodSpec {
            containers: vec![Container {
                name: "c".to_string(),
                image: "nginx:1.27".to_string(),
                ..Default::default()
            }],
            scheduler_name: Some(String::new()),
            ..Default::default()
        };
        apply_pod_spec_defaults(&mut spec);
        assert_eq!(spec.scheduler_name.as_deref(), Some("default-scheduler"));
    }

    #[test]
    fn test_empty_dns_policy_defaults_to_cluster_first() {
        let mut spec = PodSpec {
            containers: vec![Container {
                name: "c".to_string(),
                image: "nginx:1.27".to_string(),
                ..Default::default()
            }],
            dns_policy: Some(String::new()),
            ..Default::default()
        };
        apply_pod_spec_defaults(&mut spec);
        assert_eq!(spec.dns_policy.as_deref(), Some("ClusterFirst"));
    }

    #[test]
    fn test_empty_restart_policy_defaults_to_always() {
        let mut spec = PodSpec {
            containers: vec![Container {
                name: "c".to_string(),
                image: "nginx:1.27".to_string(),
                ..Default::default()
            }],
            restart_policy: Some(String::new()),
            ..Default::default()
        };
        apply_pod_spec_defaults(&mut spec);
        assert_eq!(spec.restart_policy.as_deref(), Some("Always"));
    }

    #[test]
    fn test_does_not_override_existing() {
        let mut spec = PodSpec {
            containers: vec![Container {
                name: "test".to_string(),
                image: "nginx:1.19".to_string(),
                image_pull_policy: Some("Never".to_string()),
                termination_message_path: Some("/custom/path".to_string()),
                ..Default::default()
            }],
            dns_policy: Some("None".to_string()),
            restart_policy: Some("OnFailure".to_string()),
            termination_grace_period_seconds: Some(60),
            scheduler_name: Some("my-scheduler".to_string()),
            ..Default::default()
        };

        apply_pod_spec_defaults(&mut spec);

        assert_eq!(spec.dns_policy.as_deref(), Some("None"));
        assert_eq!(spec.restart_policy.as_deref(), Some("OnFailure"));
        assert_eq!(spec.termination_grace_period_seconds, Some(60));
        assert_eq!(spec.scheduler_name.as_deref(), Some("my-scheduler"));
        assert_eq!(
            spec.containers[0].image_pull_policy.as_deref(),
            Some("Never")
        );
        assert_eq!(
            spec.containers[0].termination_message_path.as_deref(),
            Some("/custom/path")
        );
    }

    #[test]
    fn test_probe_defaults() {
        use rusternetes_common::resources::pod::{HTTPGetAction, Probe};

        let mut spec = PodSpec {
            containers: vec![Container {
                name: "test".to_string(),
                image: "nginx:1.19".to_string(),
                liveness_probe: Some(Probe {
                    http_get: Some(HTTPGetAction {
                        path: Some("/health".to_string()),
                        port: rusternetes_common::resources::IntOrString::Int(8080),
                        host: None,
                        scheme: None,
                        http_headers: None,
                    }),
                    tcp_socket: None,
                    exec: None,
                    grpc: None,
                    initial_delay_seconds: None,
                    timeout_seconds: None,
                    period_seconds: None,
                    success_threshold: None,
                    failure_threshold: None,
                    termination_grace_period_seconds: None,
                }),
                ..Default::default()
            }],
            ..Default::default()
        };

        apply_pod_spec_defaults(&mut spec);

        let probe = spec.containers[0].liveness_probe.as_ref().unwrap();
        assert_eq!(probe.timeout_seconds, Some(1));
        assert_eq!(probe.period_seconds, Some(10));
        assert_eq!(probe.success_threshold, Some(1));
        assert_eq!(probe.failure_threshold, Some(3));
    }

    #[test]
    fn test_pod_template_defaults_applied() {
        let mut template = PodTemplateSpec {
            metadata: None,
            spec: PodSpec {
                containers: vec![Container {
                    name: "test".to_string(),
                    image: "busybox".to_string(),
                    ..Default::default()
                }],
                ..Default::default()
            },
        };

        apply_pod_template_defaults(&mut template);

        assert_eq!(template.spec.dns_policy.as_deref(), Some("ClusterFirst"));
        assert_eq!(template.spec.restart_policy.as_deref(), Some("Always"));
        assert_eq!(
            template.spec.scheduler_name.as_deref(),
            Some("default-scheduler")
        );
        assert_eq!(template.spec.termination_grace_period_seconds, Some(30));
        assert_eq!(
            template.spec.containers[0].image_pull_policy.as_deref(),
            Some("Always")
        );
    }

    /// `+default="default"` on `ISCSIVolumeSource.iscsiInterface`
    /// (staging/src/k8s.io/api/core/v1/types.go:1510), applied by
    /// `SetObjectDefaults_Pod` (zz_generated.defaults.go:219-222) and
    /// `SetObjectDefaults_PodTemplate` (:610-613) as `== ""` -> "default".
    #[test]
    fn test_iscsi_interface_defaulted() {
        let mut spec = PodSpec {
            containers: vec![],
            volumes: Some(vec![
                serde_json::from_value(serde_json::json!({"name": "a", "iscsi": {"targetPortal": "p", "iqn": "q", "lun": 0}})).unwrap(),
                serde_json::from_value(serde_json::json!({"name": "b", "iscsi": {"targetPortal": "p", "iqn": "q", "lun": 0, "iscsiInterface": ""}})).unwrap(),
                serde_json::from_value(serde_json::json!({"name": "c", "iscsi": {"targetPortal": "p", "iqn": "q", "lun": 0, "iscsiInterface": "tcp"}})).unwrap(),
            ]),
            ..Default::default()
        };
        apply_pod_spec_defaults(&mut spec);
        let v = spec.volumes.as_deref().unwrap();
        let get = |i: usize| v[i].iscsi.as_ref().unwrap().iscsi_interface.clone();
        assert_eq!(get(0).as_deref(), Some("default"));
        assert_eq!(get(1).as_deref(), Some("default"));
        assert_eq!(get(2).as_deref(), Some("tcp"));
    }

    #[test]
    fn test_volume_default_mode_applied() {
        use rusternetes_common::resources::pod::Volume;
        let mut spec = PodSpec {
            containers: vec![],
            volumes: Some(vec![
                serde_json::from_value(serde_json::json!({"name": "s", "secret": {"secretName": "x"}})).unwrap(),
                serde_json::from_value(serde_json::json!({"name": "cm", "configMap": {"name": "y"}})).unwrap(),
                serde_json::from_value(serde_json::json!({"name": "p", "projected": {"sources": []}})).unwrap(),
                serde_json::from_value(serde_json::json!({"name": "d", "downwardAPI": {}})).unwrap(),
                // explicit mode preserved
                serde_json::from_value(serde_json::json!({"name": "s2", "secret": {"secretName": "z", "defaultMode": 0o600}})).unwrap(),
            ]),
            ..Default::default()
        };
        apply_pod_spec_defaults(&mut spec);
        let v: &[Volume] = spec.volumes.as_deref().unwrap();
        assert_eq!(v[0].secret.as_ref().unwrap().default_mode, Some(0o644));
        assert_eq!(v[1].config_map.as_ref().unwrap().default_mode, Some(0o644));
        assert_eq!(v[2].projected.as_ref().unwrap().default_mode, Some(0o644));
        assert_eq!(
            v[3].downward_api.as_ref().unwrap().default_mode,
            Some(0o644)
        );
        assert_eq!(
            v[4].secret.as_ref().unwrap().default_mode,
            Some(0o600),
            "explicit mode preserved"
        );
    }

    /// zz_generated.defaults.go:211-290 (`SetObjectDefaults_Pod` volumes): rbd,
    /// azureDisk and scaleIO default when empty/nil; explicit values are kept.
    #[test]
    fn test_pod_volume_rbd_azure_disk_scale_io_defaults() {
        let mut spec = PodSpec {
            containers: vec![],
            volumes: Some(vec![
                serde_json::from_value(serde_json::json!({"name": "r", "rbd": {"monitors": ["m"], "image": "i"}})).unwrap(),
                serde_json::from_value(serde_json::json!({"name": "a", "azureDisk": {"diskName": "d", "diskURI": "u"}})).unwrap(),
                serde_json::from_value(serde_json::json!({"name": "s", "scaleIO": {"gateway": "g", "system": "s", "secretRef": {"name": "n"}}})).unwrap(),
                serde_json::from_value(serde_json::json!({"name": "k", "azureDisk": {"diskName": "d", "diskURI": "u",
                    "cachingMode": "None", "fsType": "xfs", "readOnly": true, "kind": "Dedicated"}})).unwrap(),
            ]),
            ..Default::default()
        };
        apply_pod_spec_defaults(&mut spec);
        let v = spec.volumes.as_deref().unwrap();
        let r = v[0].legacy_sources.rbd.as_ref().unwrap();
        assert_eq!(r["pool"], "rbd");
        assert_eq!(r["user"], "admin");
        assert_eq!(r["keyring"], "/etc/ceph/keyring");
        let a = v[1].legacy_sources.azure_disk.as_ref().unwrap();
        assert_eq!(a["cachingMode"], "ReadWrite");
        assert_eq!(a["fsType"], "ext4");
        assert_eq!(a["readOnly"], false);
        assert_eq!(a["kind"], "Shared");
        let s = v[2].legacy_sources.scale_io.as_ref().unwrap();
        assert_eq!(s["storageMode"], "ThinProvisioned");
        assert_eq!(s["fsType"], "xfs");
        let k = v[3].legacy_sources.azure_disk.as_ref().unwrap();
        assert_eq!(k["cachingMode"], "None");
        assert_eq!(k["fsType"], "xfs");
        assert_eq!(k["readOnly"], true);
        assert_eq!(k["kind"], "Dedicated");
    }

    /// SetDefaults_Volume (pkg/apis/core/v1/defaults.go:66-71): a volume with
    /// no source becomes an emptyDir; one with a source is untouched.
    #[test]
    fn test_volume_without_source_defaults_to_empty_dir() {
        let mut spec = PodSpec {
            containers: vec![],
            volumes: Some(vec![
                serde_json::from_value(serde_json::json!({"name": "bare"})).unwrap(),
                serde_json::from_value(
                    serde_json::json!({"name": "gce", "gcePersistentDisk": {"pdName": "x"}}),
                )
                .unwrap(),
                serde_json::from_value(
                    serde_json::json!({"name": "hp", "hostPath": {"path": "/x"}}),
                )
                .unwrap(),
            ]),
            ..Default::default()
        };
        apply_pod_spec_defaults(&mut spec);
        let v = spec.volumes.as_deref().unwrap();
        assert!(v[0].empty_dir.is_some(), "bare volume -> emptyDir");
        assert!(v[1].empty_dir.is_none(), "legacy source preserved");
        assert!(v[2].empty_dir.is_none(), "hostPath preserved");
    }

    #[test]
    fn test_http_get_path_scheme_defaults() {
        use rusternetes_common::resources::pod::{Container, Lifecycle, LifecycleHandler, Probe};
        let mut c: Container = serde_json::from_value(serde_json::json!({
            "name": "c", "image": "busybox",
            "livenessProbe": {"httpGet": {"port": 8080}},
            "lifecycle": {"preStop": {"httpGet": {"port": 8080, "scheme": "HTTPS"}}}
        }))
        .unwrap();
        apply_container_defaults(&mut c);
        let lp: &Probe = c.liveness_probe.as_ref().unwrap();
        let hg = lp.http_get.as_ref().unwrap();
        assert_eq!(hg.path.as_deref(), Some("/"));
        assert_eq!(hg.scheme.as_deref(), Some("HTTP"));
        // lifecycle preStop httpGet: explicit scheme preserved, path defaulted.
        let lc: &Lifecycle = c.lifecycle.as_ref().unwrap();
        let ph: &LifecycleHandler = lc.pre_stop.as_ref().unwrap();
        let phg = ph.http_get.as_ref().unwrap();
        assert_eq!(
            phg.scheme.as_deref(),
            Some("HTTPS"),
            "explicit scheme preserved"
        );
        assert_eq!(phg.path.as_deref(), Some("/"));
    }

    #[test]
    fn test_job_pod_replacement_policy_default() {
        use rusternetes_common::resources::Job;
        // No podFailurePolicy -> TerminatingOrFailed.
        let mut job: Job = serde_json::from_value(serde_json::json!({
            "metadata": {"name": "j"},
            "spec": {"template": {"spec": {"containers": [{"name": "c", "image": "busybox"}], "restartPolicy": "Never"}}}
        })).unwrap();
        apply_job_defaults(&mut job);
        assert_eq!(
            job.spec.pod_replacement_policy.as_deref(),
            Some("TerminatingOrFailed")
        );

        // With podFailurePolicy -> Failed.
        let mut job2: Job = serde_json::from_value(serde_json::json!({
            "metadata": {"name": "j2"},
            "spec": {
                "podFailurePolicy": {"rules": []},
                "template": {"spec": {"containers": [{"name": "c", "image": "busybox"}], "restartPolicy": "Never"}}
            }
        })).unwrap();
        apply_job_defaults(&mut job2);
        assert_eq!(job2.spec.pod_replacement_policy.as_deref(), Some("Failed"));

        // Explicit value preserved.
        let mut job3: Job = serde_json::from_value(serde_json::json!({
            "metadata": {"name": "j3"},
            "spec": {"podReplacementPolicy": "Failed",
                "template": {"spec": {"containers": [{"name": "c", "image": "busybox"}], "restartPolicy": "Never"}}}
        })).unwrap();
        apply_job_defaults(&mut job3);
        assert_eq!(job3.spec.pod_replacement_policy.as_deref(), Some("Failed"));
    }
}
