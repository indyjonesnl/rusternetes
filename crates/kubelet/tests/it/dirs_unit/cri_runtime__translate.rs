//! Unit tests moved from `src/cri_runtime/translate.rs`.
#![allow(unused_imports, dead_code)]

mod __parent {
    pub use rusternetes_common::quantity::parse_resource_value;
    pub use rusternetes_common::resources::pod::{Container, Pod};
    pub use rusternetes_common::resources::{ConfigMap, Secret, Service};
    pub use rusternetes_cri::v1;
    pub use std::collections::HashMap;
    pub use std::path::Path;
}

mod tests {
    use super::__parent::*;
    use rusternetes_common::resources::pod::PodSpec;
    use rusternetes_kubelet::cri_runtime::translate::*;

    fn pod_with(spec: PodSpec) -> Pod {
        let mut pod = Pod::new("web", spec);
        pod.metadata.uid = "uid-123".to_string();
        pod.metadata.namespace = Some("prod".to_string());
        pod
    }

    fn svc(name: &str, cluster_ip: Option<&str>, port: u16, proto: &str) -> Service {
        use rusternetes_common::resources::service::{ServicePort, ServiceSpec};
        let mut s = Service {
            type_meta: Default::default(),
            metadata: Default::default(),
            spec: ServiceSpec {
                ports: vec![ServicePort {
                    name: None,
                    port,
                    target_port: None,
                    protocol: proto.to_string(),
                    node_port: None,
                    app_protocol: None,
                }],
                cluster_ip: cluster_ip.map(|s| s.to_string()),
                ..Default::default()
            },
            status: None,
        };
        s.metadata.name = name.to_string();
        s
    }

    #[test]
    fn service_links_emit_docker_link_vars_and_skip_headless() {
        // Upstream getServiceEnvVarMap/makeLinkVariables: each service with a
        // real ClusterIP yields the docker-link env quartet, keyed by the
        // uppercased, '-'→'_' service name. Headless ("None") and IP-less
        // services contribute nothing.
        let services = vec![
            svc("redis-master", Some("10.0.0.11"), 6379, "TCP"),
            svc("headless", Some("None"), 80, "TCP"),
            svc("pending", None, 80, "TCP"),
        ];
        let env = service_link_env_vars(&services);
        let get = |k: &str| env.iter().find(|(kk, _)| kk == k).map(|(_, v)| v.as_str());
        assert_eq!(get("REDIS_MASTER_SERVICE_HOST"), Some("10.0.0.11"));
        assert_eq!(get("REDIS_MASTER_SERVICE_PORT"), Some("6379"));
        assert_eq!(get("REDIS_MASTER_PORT"), Some("tcp://10.0.0.11:6379"));
        assert_eq!(
            get("REDIS_MASTER_PORT_6379_TCP"),
            Some("tcp://10.0.0.11:6379")
        );
        assert_eq!(get("REDIS_MASTER_PORT_6379_TCP_ADDR"), Some("10.0.0.11"));
        assert_eq!(get("REDIS_MASTER_PORT_6379_TCP_PROTO"), Some("tcp"));
        assert_eq!(get("REDIS_MASTER_PORT_6379_TCP_PORT"), Some("6379"));
        assert!(
            env.iter()
                .all(|(k, _)| !k.starts_with("HEADLESS_") && !k.starts_with("PENDING_")),
            "headless / IP-less services must contribute no env"
        );
    }

    #[test]
    fn downward_api_pod_ip_and_host_ip_are_distinct() {
        use rusternetes_common::resources::pod::{HostIP, PodIP, PodStatus};
        let mut pod = pod_with(PodSpec {
            node_name: Some("node-1".to_string()),
            ..Default::default()
        });
        pod.status = Some(PodStatus {
            pod_ip: Some("10.244.0.7".to_string()),
            host_ip: Some("172.20.0.5".to_string()),
            pod_i_ps: Some(vec![PodIP {
                ip: "10.244.0.7".to_string(),
            }]),
            host_i_ps: Some(vec![HostIP {
                ip: "172.20.0.5".to_string(),
            }]),
            ..Default::default()
        });
        // podIP is the pod's address; hostIP is the node's — never conflated.
        assert_eq!(
            pod_field_value(&pod, "status.podIP").as_deref(),
            Some("10.244.0.7")
        );
        assert_eq!(
            pod_field_value(&pod, "status.hostIP").as_deref(),
            Some("172.20.0.5")
        );
        assert_eq!(
            pod_field_value(&pod, "status.podIPs").as_deref(),
            Some("10.244.0.7")
        );
        assert_eq!(
            pod_field_value(&pod, "status.hostIPs").as_deref(),
            Some("172.20.0.5")
        );
        assert_eq!(
            pod_field_value(&pod, "spec.nodeName").as_deref(),
            Some("node-1")
        );
    }

    #[test]
    fn cpu_parsing() {
        assert_eq!(parse_cpu_millicores("500m"), Some(500));
        assert_eq!(parse_cpu_millicores("2"), Some(2000));
        assert_eq!(parse_cpu_millicores("0.5"), Some(500));
        assert_eq!(parse_cpu_millicores("garbage"), None);
    }

    #[test]
    fn memory_parsing() {
        assert_eq!(parse_memory_bytes("128Mi"), Some(128 * 1024 * 1024));
        assert_eq!(parse_memory_bytes("1Gi"), Some(1024 * 1024 * 1024));
        assert_eq!(parse_memory_bytes("1000000"), Some(1_000_000));
        assert_eq!(parse_memory_bytes("1M"), Some(1_000_000));
    }

    /// `resourceFieldRef` divisors and limits carry the full quantity
    /// grammar. `Pi`/`Ei`/`P`/`E` were missing from the suffix table and fell
    /// through to a bare `i64` parse, yielding `None` — which drops the
    /// downward-API env var entirely.
    #[test]
    fn quantity_parsing_covers_the_full_suffix_set() {
        assert_eq!(parse_memory_bytes("1Pi"), Some(1_125_899_906_842_624));
        assert_eq!(parse_memory_bytes("1Ei"), Some(1_152_921_504_606_846_976));
        assert_eq!(parse_memory_bytes("1P"), Some(1_000_000_000_000_000));
        assert_eq!(parse_memory_bytes("1E"), Some(1_000_000_000_000_000_000));
        assert_eq!(parse_memory_bytes("129e6"), Some(129_000_000));
        assert_eq!(parse_memory_bytes("0.5Gi"), Some(536_870_912));
        // Sub-millicore CPU: `MilliValue()` rounds up, so a container asking
        // for some CPU never reports none.
        assert_eq!(parse_cpu_millicores("0.5m"), Some(1));
        assert_eq!(parse_cpu_millicores("10.5m"), Some(11));
        assert_eq!(parse_cpu_millicores("1500u"), Some(2));
    }

    #[test]
    fn sandbox_carries_metadata_and_labels() {
        let pod = pod_with(PodSpec {
            host_network: Some(true),
            ..Default::default()
        });
        let cfg = sandbox_config(&pod, "/var/log/pods/web");
        let meta = cfg.metadata.unwrap();
        assert_eq!(meta.name, "web");
        assert_eq!(meta.uid, "uid-123");
        assert_eq!(meta.namespace, "prod");
        assert_eq!(cfg.labels.get(labels::POD_UID).unwrap(), "uid-123");
        // host_network -> NODE network namespace
        let ns = cfg
            .linux
            .unwrap()
            .security_context
            .unwrap()
            .namespace_options
            .unwrap();
        assert_eq!(ns.network, v1::NamespaceMode::Node as i32);
    }

    #[test]
    fn sandbox_omits_hostname_for_host_network_pods() {
        let pod = pod_with(PodSpec {
            host_network: Some(true),
            hostname: Some("custom-host".to_string()),
            ..Default::default()
        });

        let cfg = sandbox_config(&pod, "/var/log/pods/web");
        assert_eq!(cfg.hostname, "");
    }

    #[test]
    fn sandbox_truncates_default_hostname_for_kernel() {
        let long_name = "termination-message-containerb09028ac-d9a4-4be6-9165-f0df6ecd7244";
        assert!(long_name.len() > 63);

        let pod = pod_with(PodSpec::default());
        let mut pod = pod;
        pod.metadata.name = long_name.to_string();

        let cfg = sandbox_config(&pod, "/var/log/pods/web");
        assert_eq!(cfg.hostname.len(), 63);
        assert!(!cfg.hostname.ends_with('-'));
        assert!(!cfg.hostname.ends_with('.'));
    }

    #[test]
    fn container_translates_command_env_resources() {
        let mut c = Container {
            name: "app".to_string(),
            image: "busybox".to_string(),
            ..Default::default()
        };
        c.command = Some(vec!["/bin/sh".to_string()]);
        c.args = Some(vec!["-c".to_string(), "sleep 1".to_string()]);
        c.env = Some(vec![rusternetes_common::resources::pod::EnvVar {
            name: "FOO".to_string(),
            value: Some("bar".to_string()),
            value_from: None,
        }]);
        c.resources = Some(rusternetes_common::types::ResourceRequirements {
            limits: Some(HashMap::from([
                ("cpu".to_string(), "500m".to_string()),
                ("memory".to_string(), "64Mi".to_string()),
            ])),
            requests: None,
            claims: None,
        });
        let pod = pod_with(PodSpec {
            containers: vec![c.clone()],
            ..Default::default()
        });

        let cfg = container_config(
            &pod,
            &c,
            "docker.io/library/busybox@sha256:abc",
            &HashMap::new(),
            &HashMap::new(),
            &HashMap::new(),
        )
        .unwrap();
        assert_eq!(cfg.command, vec!["/bin/sh"]);
        assert_eq!(cfg.args, vec!["-c", "sleep 1"]);
        assert_eq!(cfg.envs[0].key, "FOO");
        assert_eq!(cfg.envs[0].value, "bar");
        let res = cfg.linux.unwrap().resources.unwrap();
        assert_eq!(res.cpu_quota, 50_000); // 500m -> quota 50000 @ 100ms period
                                           // No CPU request, so shares fall back to the limit: 500m -> 512 shares.
        assert_eq!(res.cpu_shares, 512);
        assert_eq!(res.memory_limit_in_bytes, 64 * 1024 * 1024);
        assert_eq!(cfg.labels.get(labels::CONTAINER_NAME).unwrap(), "app");
    }

    #[test]
    fn linux_resources_shares_quota_and_floors() {
        use rusternetes_common::types::ResourceRequirements;
        let res = |limits: Option<Vec<(&str, &str)>>, requests: Option<Vec<(&str, &str)>>| {
            let mut c = Container {
                name: "app".to_string(),
                image: "busybox".to_string(),
                ..Default::default()
            };
            let map = |v: Vec<(&str, &str)>| {
                v.into_iter()
                    .map(|(k, val)| (k.to_string(), val.to_string()))
                    .collect::<HashMap<_, _>>()
            };
            c.resources = Some(ResourceRequirements {
                limits: limits.map(map),
                requests: requests.map(map),
                claims: None,
            });
            linux_resources(&c).unwrap()
        };

        // CPU request drives shares; limit drives quota/period.
        let r = res(Some(vec![("cpu", "1")]), Some(vec![("cpu", "250m")]));
        assert_eq!(r.cpu_shares, 256); // 250m
        assert_eq!(r.cpu_quota, 100_000); // 1 CPU @ 100ms
        assert_eq!(r.cpu_period, 100_000);

        // No resources at all → MinShares, no quota.
        let r = res(None, None);
        assert_eq!(r.cpu_shares, MIN_SHARES);
        assert_eq!(r.cpu_quota, 0);

        // Tiny CPU limit floors the quota at MinQuotaPeriod (1ms).
        let r = res(Some(vec![("cpu", "5m")]), None);
        assert_eq!(r.cpu_quota, MIN_QUOTA_PERIOD); // 5m -> 500, floored to 1000
        assert_eq!(r.cpu_shares, 5); // 5m -> 5 shares (already >= MinShares)

        // 1m floors shares at MinShares (1m -> 1 share -> 2).
        let r = res(Some(vec![("cpu", "1m")]), None);
        assert_eq!(r.cpu_shares, MIN_SHARES);
    }

    #[test]
    fn volume_mounts_resolve_from_host_paths() {
        use rusternetes_common::resources::pod::VolumeMount;
        let mut c = Container {
            name: "app".to_string(),
            image: "busybox".to_string(),
            ..Default::default()
        };
        c.volume_mounts = Some(vec![
            VolumeMount {
                name: "data".to_string(),
                mount_path: "/data".to_string(),
                read_only: Some(true),
                sub_path: None,
                sub_path_expr: None,
                mount_propagation: None,
                recursive_read_only: None,
            },
            VolumeMount {
                name: "missing".to_string(),
                mount_path: "/nope".to_string(),
                read_only: None,
                sub_path: None,
                sub_path_expr: None,
                mount_propagation: None,
                recursive_read_only: None,
            },
        ]);
        let pod = pod_with(PodSpec {
            containers: vec![c.clone()],
            ..Default::default()
        });
        let host_paths = HashMap::from([("data".to_string(), "/host/data".to_string())]);
        let cfg = container_config(
            &pod,
            &c,
            "img",
            &host_paths,
            &HashMap::new(),
            &HashMap::new(),
        )
        .unwrap();
        // Only the resolvable mount is emitted; the unmapped one is skipped.
        assert_eq!(cfg.mounts.len(), 1);
        assert_eq!(cfg.mounts[0].container_path, "/data");
        assert_eq!(cfg.mounts[0].host_path, "/host/data");
        assert!(cfg.mounts[0].readonly);
        // Unset propagation defaults to Private.
        assert_eq!(
            cfg.mounts[0].propagation,
            v1::MountPropagation::PropagationPrivate as i32
        );
    }

    /// Port of the `TestMakeMounts` shape (`pkg/kubelet/kubelet_pods_linux_test.go:42`):
    /// one volume ("disk") mounted twice, one volume ("disk4") once. Upstream's
    /// `stubVolume{attributes: ...}` (`kubelet_volumes_test.go:687`) is how the
    /// attributes reach `makeMounts`; here they arrive as `MountAttrs`.
    fn two_mounts_of_one_volume() -> (Container, HashMap<String, String>) {
        use rusternetes_common::resources::pod::VolumeMount;
        let vm = |name: &str, path: &str, ro: bool| VolumeMount {
            name: name.to_string(),
            mount_path: path.to_string(),
            read_only: Some(ro),
            sub_path: None,
            sub_path_expr: None,
            mount_propagation: None,
            recursive_read_only: None,
        };
        let mut c = Container {
            name: "container1".to_string(),
            image: "busybox".to_string(),
            ..Default::default()
        };
        c.volume_mounts = Some(vec![
            vm("disk", "/etc/hosts", false),
            vm("disk", "/mnt/path3", true),
            vm("disk4", "/mnt/path4", false),
        ]);
        let host_paths = HashMap::from([
            ("disk".to_string(), "/mnt/disk".to_string()),
            ("disk4".to_string(), "/mnt/host".to_string()),
        ]);
        (c, host_paths)
    }

    #[test]
    fn mounter_read_only_attribute_forces_readonly_mount() {
        // `ReadOnly: mount.ReadOnly || mustMountRO` (kubelet_pods.go:417):
        // a read-only PVC over a hostPath PV is read-only even for a
        // `readOnly: false` volumeMount.
        let (c, host_paths) = two_mounts_of_one_volume();
        let attrs = HashMap::from([(
            "disk".to_string(),
            MountAttrs {
                read_only: true,
                selinux_relabel: false,
            },
        )]);
        let m = mounts_with(
            &c,
            &host_paths,
            &HashMap::new(),
            &attrs,
            true,
            false,
            &HashMap::new(),
        )
        .unwrap();
        assert!(m[0].readonly, "disk /etc/hosts must be forced read-only");
        assert!(m[1].readonly);
        assert!(!m[2].readonly, "disk4 has no attributes: stays read-write");
    }

    #[test]
    fn default_attributes_leave_mount_flags_alone() {
        let (c, host_paths) = two_mounts_of_one_volume();
        let m = mounts_with(
            &c,
            &host_paths,
            &HashMap::new(),
            &HashMap::new(),
            true,
            false,
            &HashMap::new(),
        )
        .unwrap();
        assert_eq!(
            m.iter().map(|m| m.readonly).collect::<Vec<_>>(),
            [false, true, false]
        );
        assert!(m.iter().all(|m| !m.selinux_relabel));
    }

    #[test]
    fn mounter_relabel_sets_selinux_relabel_only_when_selinux_enabled() {
        // `relabelVolume` -> `SELinuxRelabel` (kubelet_pods.go:296,:420), ANDed
        // with `selinux.GetEnabled()` (kuberuntime_container.go:484).
        let (c, host_paths) = two_mounts_of_one_volume();
        let attrs = HashMap::from([(
            "disk".to_string(),
            MountAttrs {
                read_only: false,
                selinux_relabel: true,
            },
        )]);
        let on = mounts_with(
            &c,
            &host_paths,
            &HashMap::new(),
            &attrs,
            true,
            false,
            &HashMap::new(),
        )
        .unwrap();
        assert!(on[0].selinux_relabel);
        // Once per volume: `vol.SELinuxLabeled = true` after the first mount.
        assert!(
            !on[1].selinux_relabel,
            "second mount of disk is already labeled"
        );
        assert!(!on[2].selinux_relabel);
        let off = mounts_with(
            &c,
            &host_paths,
            &HashMap::new(),
            &attrs,
            false,
            false,
            &HashMap::new(),
        )
        .unwrap();
        assert!(off.iter().all(|m| !m.selinux_relabel));
    }

    #[test]
    fn volume_mount_subpath_joins_onto_host_path() {
        use rusternetes_common::resources::pod::VolumeMount;
        let mut c = Container {
            name: "app".to_string(),
            image: "busybox".to_string(),
            ..Default::default()
        };
        c.volume_mounts = Some(vec![VolumeMount {
            name: "data".to_string(),
            mount_path: "/data".to_string(),
            read_only: None,
            sub_path: Some("nested/sub".to_string()),
            sub_path_expr: None,
            mount_propagation: None,
            recursive_read_only: None,
        }]);
        let pod = pod_with(PodSpec {
            containers: vec![c.clone()],
            ..Default::default()
        });
        let host_paths = HashMap::from([("data".to_string(), "/host/data".to_string())]);
        let cfg = container_config(
            &pod,
            &c,
            "img",
            &host_paths,
            &HashMap::new(),
            &HashMap::new(),
        )
        .unwrap();
        assert_eq!(cfg.mounts.len(), 1);
        assert_eq!(cfg.mounts[0].host_path, "/host/data/nested/sub");
    }

    #[test]
    fn resolve_rro_matches_upstream() {
        use rusternetes_common::resources::pod::VolumeMount;
        let vm = |ro: bool, rro: Option<&str>, prop: Option<&str>| VolumeMount {
            name: "v".into(),
            mount_path: "/v".into(),
            read_only: Some(ro),
            sub_path: None,
            sub_path_expr: None,
            mount_propagation: prop.map(str::to_string),
            recursive_read_only: rro.map(str::to_string),
        };
        // nil / Disabled -> false, no error even when not read-only.
        assert_eq!(
            resolve_recursive_read_only(&vm(false, None, None), true),
            Ok(false)
        );
        assert_eq!(
            resolve_recursive_read_only(&vm(false, Some("Disabled"), None), true),
            Ok(false)
        );
        // Enabled/IfPossible require readOnly.
        assert!(resolve_recursive_read_only(&vm(false, Some("Enabled"), None), true).is_err());
        assert!(resolve_recursive_read_only(&vm(false, Some("IfPossible"), None), true).is_err());
        // ...and propagation None (unset or "None").
        assert!(resolve_recursive_read_only(
            &vm(true, Some("Enabled"), Some("HostToContainer")),
            true
        )
        .is_err());
        assert_eq!(
            resolve_recursive_read_only(&vm(true, Some("Enabled"), Some("None")), true),
            Ok(true)
        );
        // IfPossible follows runtime support; Enabled errors without it.
        assert_eq!(
            resolve_recursive_read_only(&vm(true, Some("IfPossible"), None), true),
            Ok(true)
        );
        assert_eq!(
            resolve_recursive_read_only(&vm(true, Some("IfPossible"), None), false),
            Ok(false)
        );
        assert!(resolve_recursive_read_only(&vm(true, Some("Enabled"), None), false).is_err());
        assert!(resolve_recursive_read_only(&vm(true, Some("Bogus"), None), true).is_err());
    }

    #[test]
    fn runtime_handler_rro_support_matches_upstream() {
        let h = |name: &str, rro: bool| v1::RuntimeHandler {
            name: name.into(),
            features: Some(v1::RuntimeHandlerFeatures {
                recursive_read_only_mounts: rro,
                ..Default::default()
            }),
        };
        // Empty list (runtime cannot list handlers) -> false.
        assert!(!runtime_handler_supports_rro("", &[]));
        let hs = [h("", true), h("gvisor", false)];
        assert!(runtime_handler_supports_rro("", &hs));
        assert!(!runtime_handler_supports_rro("gvisor", &hs));
        // Unknown handler -> false.
        assert!(!runtime_handler_supports_rro("nope", &hs));
    }

    #[test]
    fn mount_carries_recursive_read_only() {
        use rusternetes_common::resources::pod::VolumeMount;
        let c = Container {
            name: "app".into(),
            image: "busybox".into(),
            volume_mounts: Some(vec![VolumeMount {
                name: "v".into(),
                mount_path: "/v".into(),
                read_only: Some(true),
                sub_path: None,
                sub_path_expr: None,
                mount_propagation: None,
                recursive_read_only: Some("Enabled".into()),
            }]),
            ..Default::default()
        };
        let hp = HashMap::from([("v".to_string(), "/host/v".to_string())]);
        let ok = mounts(
            &c,
            &hp,
            &HashMap::new(),
            &HashMap::new(),
            true,
            &HashMap::new(),
        )
        .unwrap();
        assert!(ok[0].recursive_read_only && ok[0].readonly);
        assert!(mounts(
            &c,
            &hp,
            &HashMap::new(),
            &HashMap::new(),
            false,
            &HashMap::new()
        )
        .is_err());
    }

    #[test]
    fn mount_propagation_modes() {
        use rusternetes_common::resources::pod::VolumeMount;
        use v1::MountPropagation;
        let mk = |mode: Option<&str>| VolumeMount {
            name: "v".to_string(),
            mount_path: "/v".to_string(),
            read_only: None,
            sub_path: None,
            sub_path_expr: None,
            mount_propagation: mode.map(str::to_string),
            recursive_read_only: None,
        };
        let mut c = Container {
            name: "app".to_string(),
            image: "busybox".to_string(),
            ..Default::default()
        };
        let host_paths = HashMap::from([("v".to_string(), "/host/v".to_string())]);
        let mut prop = |mode: Option<&str>| -> i32 {
            c.volume_mounts = Some(vec![mk(mode)]);
            mounts(
                &c,
                &host_paths,
                &HashMap::new(),
                &HashMap::new(),
                false,
                &HashMap::new(),
            )
            .unwrap()[0]
                .propagation
        };
        assert_eq!(
            prop(Some("HostToContainer")),
            MountPropagation::PropagationHostToContainer as i32
        );
        assert_eq!(
            prop(Some("Bidirectional")),
            MountPropagation::PropagationBidirectional as i32
        );
        assert_eq!(
            prop(Some("None")),
            MountPropagation::PropagationPrivate as i32
        );
        assert_eq!(prop(None), MountPropagation::PropagationPrivate as i32);
    }

    #[test]
    fn ports_map_with_protocol() {
        use rusternetes_common::resources::pod::ContainerPort;
        let mut c = Container {
            name: "app".to_string(),
            image: "busybox".to_string(),
            ..Default::default()
        };
        c.ports = Some(vec![
            ContainerPort {
                container_port: 53,
                name: Some("dns".to_string()),
                protocol: "UDP".to_string(),
                host_port: Some(5353),
                host_ip: None,
            },
            ContainerPort {
                container_port: 80,
                name: None,
                protocol: "TCP".to_string(), // defaults to TCP
                host_port: None,
                host_ip: None,
            },
        ]);
        let pod = pod_with(PodSpec {
            containers: vec![c],
            ..Default::default()
        });
        let cfg = sandbox_config(&pod, "/log");
        assert_eq!(cfg.port_mappings.len(), 2);
        assert_eq!(cfg.port_mappings[0].protocol, v1::Protocol::Udp as i32);
        assert_eq!(cfg.port_mappings[0].container_port, 53);
        assert_eq!(cfg.port_mappings[0].host_port, 5353);
        assert_eq!(cfg.port_mappings[1].protocol, v1::Protocol::Tcp as i32);
    }

    #[test]
    fn host_pid_and_ipc_map_to_node_namespace() {
        let pod = pod_with(PodSpec {
            host_pid: Some(true),
            host_ipc: Some(true),
            host_network: Some(false),
            ..Default::default()
        });
        let ns = sandbox_config(&pod, "/log")
            .linux
            .unwrap()
            .security_context
            .unwrap()
            .namespace_options
            .unwrap();
        assert_eq!(ns.pid, v1::NamespaceMode::Node as i32);
        assert_eq!(ns.ipc, v1::NamespaceMode::Node as i32);
        assert_eq!(ns.network, v1::NamespaceMode::Pod as i32);
    }

    #[test]
    fn pod_sysctls_map_onto_sandbox_linux_config() {
        use rusternetes_common::resources::pod::{PodSecurityContext, Sysctl};
        let pod = pod_with(PodSpec {
            security_context: Some(PodSecurityContext {
                sysctls: Some(vec![
                    Sysctl {
                        name: "net.core.somaxconn".to_string(),
                        value: "1024".to_string(),
                    },
                    Sysctl {
                        name: "kernel.shm_rmid_forced".to_string(),
                        value: "1".to_string(),
                    },
                    Sysctl {
                        name: "net/ipv4/ip_local_port_range".to_string(),
                        value: "1024 65535".to_string(),
                    },
                    Sysctl {
                        name: "net.ipv4.conf.eno2/100.rp_filter".to_string(),
                        value: "1".to_string(),
                    },
                ]),
                ..Default::default()
            }),
            ..Default::default()
        });
        let sysctls = sandbox_config(&pod, "/log").linux.unwrap().sysctls;
        assert_eq!(sysctls.len(), 4);
        assert_eq!(
            sysctls.get("net.core.somaxconn").map(String::as_str),
            Some("1024")
        );
        assert_eq!(
            sysctls.get("kernel.shm_rmid_forced").map(String::as_str),
            Some("1")
        );
        assert_eq!(
            sysctls
                .get("net.ipv4.ip_local_port_range")
                .map(String::as_str),
            Some("1024 65535")
        );
        assert_eq!(
            sysctls
                .get("net.ipv4.conf.eno2/100.rp_filter")
                .map(String::as_str),
            Some("1")
        );
        assert!(
            !sysctls.contains_key("net/ipv4/ip_local_port_range"),
            "slash-first sysctl names must be normalized before CRI"
        );
    }

    #[test]
    fn normalize_sysctl_name_matches_upstream_separator_rules() {
        let cases = [
            ("kernel.shm_rmid_forced", "kernel.shm_rmid_forced"),
            ("kernel/shm_rmid_forced", "kernel.shm_rmid_forced"),
            (
                "net.ipv4.conf.eno2/100.rp_filter",
                "net.ipv4.conf.eno2/100.rp_filter",
            ),
            (
                "net/ipv4/conf/eno2.100/rp_filter",
                "net.ipv4.conf.eno2/100.rp_filter",
            ),
            (
                "net/ipv4/ip_local_port_range",
                "net.ipv4.ip_local_port_range",
            ),
            ("kernel/msgmax", "kernel.msgmax"),
            ("kernel/sem", "kernel.sem"),
        ];

        for (input, expected) in cases {
            assert_eq!(normalize_sysctl_name(input), expected);
        }
    }

    /// Regression test for NC failure: "[sig-node] Sysctls [LinuxOnly]
    /// [NodeConformance] should support sysctls [Environment:NotInUserNS]"
    ///
    /// The test creates a pod with `spec.securityContext.sysctls:
    /// [{name: kernel.shm_rmid_forced, value: "1"}]` and verifies that
    /// `LinuxPodSandboxConfig.sysctls` (the CRI sandbox map) contains the
    /// entry. Upstream: `generatePodSandboxLinuxConfig` copies every sysctl
    /// from `pod.Spec.SecurityContext.Sysctls` into `lc.Sysctls`
    /// (`pkg/kubelet/kuberuntime/kuberuntime_sandbox.go:181-183`):
    ///
    /// ```go
    /// for _, c := range pod.Spec.SecurityContext.Sysctls {
    ///     sysctls[c.Name] = c.Value
    /// }
    /// lc.Sysctls = sysctls
    /// ```
    #[test]
    fn nc_sysctl_dot_form_lands_in_cri_sandbox_sysctls_map() {
        use rusternetes_common::resources::pod::{PodSecurityContext, Sysctl};
        // Exact sysctl the NC test uses (sysctl.go:83-89 in k8s v1.35):
        //   pod.Spec.SecurityContext.Sysctls = [{Name: "kernel.shm_rmid_forced", Value: "1"}]
        let pod = pod_with(PodSpec {
            security_context: Some(PodSecurityContext {
                sysctls: Some(vec![Sysctl {
                    name: "kernel.shm_rmid_forced".to_string(),
                    value: "1".to_string(),
                }]),
                ..Default::default()
            }),
            ..Default::default()
        });
        let linux = sandbox_config(&pod, "/log/pods/test").linux.unwrap();
        // CRI wire field is LinuxPodSandboxConfig.sysctls (map<string,string>, tag=3).
        // NOT security_context.sysctls — the security context carries namespace
        // options, seccomp, selinux etc; sysctls are a sibling field.
        assert_eq!(
            linux
                .sysctls
                .get("kernel.shm_rmid_forced")
                .map(String::as_str),
            Some("1"),
            "kernel.shm_rmid_forced must appear in LinuxPodSandboxConfig.sysctls"
        );
        // Sysctls must NOT bleed into security_context (wrong CRI field).
        assert!(
            linux.security_context.is_some(),
            "security_context must still be set"
        );
    }

    /// Regression test for NC failure: "[sig-node] Sysctls should support
    /// sysctls with slashes as separator".
    ///
    /// Kubernetes accepts `/` as an alternate separator for `.` in sysctl names.
    /// runc/crun use dot form internally; passing a slash-form name to the CRI
    /// sandbox would cause RunPodSandbox to fail with "sysctl is not in a
    /// separate kernel namespace". The kubelet normalizes before the CRI call.
    ///
    /// Upstream normalization: `ConvertPodSysctlsVariableToDotsSeparator` calls
    /// `utilsysctl.NormalizeName` from
    /// `staging/src/k8s.io/component-helpers/node/util/sysctl/sysctl.go:109-131`:
    ///
    /// ```go
    /// func NormalizeName(val string) string {
    ///     firstSepIndex := strings.IndexAny(val, "./")
    ///     if firstSepIndex == -1 || val[firstSepIndex] == '.' {
    ///         return val
    ///     }
    ///     // first separator is `/` → swap `.`↔`/` throughout
    ///     f := func(r rune) rune {
    ///         switch r {
    ///         case '.': return '/'
    ///         case '/': return '.'
    ///         }
    ///         return r
    ///     }
    ///     return strings.Map(f, val)
    /// }
    /// ```
    #[test]
    fn nc_sysctl_slash_form_is_normalized_to_dot_form_in_cri_sandbox() {
        use rusternetes_common::resources::pod::{PodSecurityContext, Sysctl};
        // Exact sysctl the NC slash test uses (sysctl.go:192-196 in k8s v1.35):
        //   pod.Spec.SecurityContext.Sysctls = [{Name: "kernel/shm_rmid_forced", Value: "1"}]
        let pod = pod_with(PodSpec {
            security_context: Some(PodSecurityContext {
                sysctls: Some(vec![Sysctl {
                    name: "kernel/shm_rmid_forced".to_string(),
                    value: "1".to_string(),
                }]),
                ..Default::default()
            }),
            ..Default::default()
        });
        let sysctls = sandbox_config(&pod, "/log/pods/test")
            .linux
            .unwrap()
            .sysctls;
        // Slash form must be normalized to dot form before the CRI call.
        // runc/crun identify sysctls by dot-form name; passing "kernel/shm_rmid_forced"
        // causes RunPodSandbox to reject the sandbox with "sysctl not in a separate
        // kernel namespace" (kuberuntime_manager.go:1529-1537 comment).
        assert_eq!(
            sysctls.get("kernel.shm_rmid_forced").map(String::as_str),
            Some("1"),
            "slash-form kernel/shm_rmid_forced must be normalized to kernel.shm_rmid_forced"
        );
        assert!(
            !sysctls.contains_key("kernel/shm_rmid_forced"),
            "slash form must not reach the CRI runtime"
        );
    }

    #[test]
    fn no_pod_sysctls_yields_empty_sandbox_map() {
        let pod = pod_with(PodSpec::default());
        assert!(sandbox_config(&pod, "/log")
            .linux
            .unwrap()
            .sysctls
            .is_empty());
    }

    #[test]
    fn dns_cluster_first_is_default_with_cluster_searches_and_ndots() {
        // No dnsPolicy => ClusterFirst (api-server default).
        let pod = pod_with(PodSpec::default());
        let dns =
            old_dns(&pod, &["10.96.0.10".to_string()]).expect("ClusterFirst yields a DnsConfig");
        assert_eq!(dns.servers, vec!["10.96.0.10".to_string()]);
        assert_eq!(
            dns.searches,
            vec![
                "prod.svc.cluster.local".to_string(), // pod_with sets namespace=prod
                "svc.cluster.local".to_string(),
                "cluster.local".to_string(),
            ]
        );
        assert_eq!(dns.options, vec!["ndots:5".to_string()]);
    }

    #[test]
    fn dns_default_policy_inherits_host() {
        let pod = pod_with(PodSpec {
            dns_policy: Some("Default".to_string()),
            ..Default::default()
        });
        // None => leave dns_config unset so the runtime copies host resolv.conf.
        assert!(old_dns(&pod, &["10.96.0.10".to_string()])
            .unwrap()
            .servers
            .is_empty());
    }

    #[test]
    fn dns_cluster_first_without_cluster_dns_falls_back_to_host() {
        let pod = pod_with(PodSpec::default());
        assert!(old_dns(&pod, &[]).unwrap().servers.is_empty());
    }

    #[test]
    fn dns_cluster_first_on_host_network_falls_back_to_host() {
        // hostNetwork pod with default (ClusterFirst) dnsPolicy: upstream
        // getPodDNSType falls back to Default (host resolv.conf), so we must
        // NOT stamp clusterDNS — else the pod can't resolve the `api-server`
        // alias before cluster DNS is up. Regression test for the in-cluster
        // control-plane static pods (scheduler / controller-manager).
        let pod = pod_with(PodSpec {
            host_network: Some(true),
            ..Default::default()
        });
        assert!(old_dns(&pod, &["10.96.0.10".to_string()])
            .unwrap()
            .servers
            .is_empty());
    }

    #[test]
    fn dns_cluster_first_with_host_net_keeps_cluster_dns_on_host_network() {
        // ClusterFirstWithHostNet explicitly opts a hostNetwork pod back into
        // clusterDNS (upstream getPodDNSType returns podDNSCluster).
        let pod = pod_with(PodSpec {
            host_network: Some(true),
            dns_policy: Some("ClusterFirstWithHostNet".to_string()),
            ..Default::default()
        });
        let dns = old_dns(&pod, &["10.96.0.10".to_string()])
            .expect("ClusterFirstWithHostNet yields a DnsConfig on hostNetwork");
        assert_eq!(dns.servers, vec!["10.96.0.10".to_string()]);
    }

    #[test]
    fn dns_none_uses_only_pod_dns_config() {
        use rusternetes_common::resources::pod::{PodDNSConfig, PodDNSConfigOption};
        let pod = pod_with(PodSpec {
            dns_policy: Some("None".to_string()),
            dns_config: Some(PodDNSConfig {
                nameservers: Some(vec!["1.2.3.4".to_string()]),
                searches: Some(vec!["custom.example".to_string()]),
                options: Some(vec![PodDNSConfigOption {
                    name: "ndots".to_string(),
                    value: Some("2".to_string()),
                }]),
            }),
            ..Default::default()
        });
        let dns = old_dns(&pod, &["10.96.0.10".to_string()]).unwrap();
        // No cluster defaults leak in for policy None.
        assert_eq!(dns.servers, vec!["1.2.3.4".to_string()]);
        assert_eq!(dns.searches, vec!["custom.example".to_string()]);
        assert_eq!(dns.options, vec!["ndots:2".to_string()]);
    }

    #[test]
    fn dns_cluster_first_merges_pod_dns_config_and_overrides_ndots() {
        use rusternetes_common::resources::pod::{PodDNSConfig, PodDNSConfigOption};
        let pod = pod_with(PodSpec {
            dns_config: Some(PodDNSConfig {
                nameservers: Some(vec!["1.1.1.1".to_string()]),
                searches: Some(vec!["extra.example".to_string()]),
                options: Some(vec![
                    PodDNSConfigOption {
                        name: "ndots".to_string(),
                        value: Some("3".to_string()),
                    },
                    PodDNSConfigOption {
                        name: "edns0".to_string(),
                        value: None,
                    },
                ]),
            }),
            ..Default::default()
        });
        let dns = old_dns(&pod, &["10.96.0.10".to_string()]).unwrap();
        // cluster server + appended pod nameserver.
        assert_eq!(
            dns.servers,
            vec!["10.96.0.10".to_string(), "1.1.1.1".to_string()]
        );
        // cluster searches + appended pod search.
        assert!(dns.searches.contains(&"prod.svc.cluster.local".to_string()));
        assert!(dns.searches.contains(&"extra.example".to_string()));
        // ndots overridden in place; edns0 appended (no duplicate ndots).
        assert_eq!(
            dns.options,
            vec!["ndots:3".to_string(), "edns0".to_string()]
        );
    }

    fn old_dns(pod: &Pod, cluster_dns: &[String]) -> Option<v1::DnsConfig> {
        Some(get_pod_dns(
            pod,
            cluster_dns,
            "cluster.local",
            &v1::DnsConfig::default(),
        ))
    }

    // --- GetPodDNS host-base tests (ported from upstream dns_test.go) ---

    fn host_dns() -> v1::DnsConfig {
        v1::DnsConfig {
            servers: vec!["127.0.0.1".into()],
            searches: vec!["host.example".into()],
            options: vec!["ndots:1".into(), "timeout:2".into()],
        }
    }

    #[test]
    fn get_pod_dns_default_policy_uses_host_base() {
        let pod = pod_with(PodSpec {
            dns_policy: Some("Default".to_string()),
            ..Default::default()
        });
        let dns = get_pod_dns(&pod, &["10.96.0.10".into()], "cluster.local", &host_dns());
        assert_eq!(dns, host_dns());
    }

    #[test]
    fn get_pod_dns_default_policy_merges_explicit_dns_config() {
        use rusternetes_common::resources::pod::{PodDNSConfig, PodDNSConfigOption};
        let pod = pod_with(PodSpec {
            dns_policy: Some("Default".to_string()),
            dns_config: Some(PodDNSConfig {
                nameservers: Some(vec!["1.2.3.4".into(), "127.0.0.1".into()]),
                searches: Some(vec!["extra.example".into()]),
                options: Some(vec![PodDNSConfigOption {
                    name: "ndots".into(),
                    value: Some("3".into()),
                }]),
            }),
            ..Default::default()
        });
        let dns = get_pod_dns(&pod, &[], "cluster.local", &host_dns());
        assert_eq!(dns.servers, vec!["127.0.0.1", "1.2.3.4"]);
        assert_eq!(dns.searches, vec!["host.example", "extra.example"]);
        assert_eq!(dns.options, vec!["ndots:3", "timeout:2"]);
    }

    #[test]
    fn get_pod_dns_cluster_first_appends_host_searches_deduped() {
        let pod = pod_with(PodSpec::default());
        let mut host = host_dns();
        host.searches = vec!["host.example".into(), "svc.cluster.local".into()];
        let dns = get_pod_dns(&pod, &["10.96.0.10".into()], "cluster.local", &host);
        assert_eq!(dns.servers, vec!["10.96.0.10"]);
        assert_eq!(
            dns.searches,
            vec![
                "prod.svc.cluster.local",
                "svc.cluster.local",
                "cluster.local",
                "host.example"
            ]
        );
        assert_eq!(dns.options, vec!["ndots:5"]);
    }

    #[test]
    fn get_pod_dns_cluster_first_without_cluster_dns_falls_back_to_host() {
        let pod = pod_with(PodSpec::default());
        assert_eq!(
            get_pod_dns(&pod, &[], "cluster.local", &host_dns()),
            host_dns()
        );
    }

    #[test]
    fn get_pod_dns_none_ignores_host() {
        let pod = pod_with(PodSpec {
            dns_policy: Some("None".to_string()),
            ..Default::default()
        });
        assert_eq!(
            get_pod_dns(&pod, &["10.96.0.10".into()], "cluster.local", &host_dns()),
            v1::DnsConfig::default()
        );
    }

    #[test]
    fn get_pod_dns_limits_nameservers_searches_and_length() {
        use rusternetes_common::resources::pod::PodDNSConfig;
        let pod = pod_with(PodSpec {
            dns_policy: Some("None".to_string()),
            dns_config: Some(PodDNSConfig {
                nameservers: Some((1..=5).map(|i| format!("1.1.1.{i}")).collect()),
                searches: Some(
                    (0..40)
                        .map(|i| format!("s{i}.example"))
                        .chain(["x".repeat(254)])
                        .collect(),
                ),
                options: None,
            }),
            ..Default::default()
        });
        let dns = get_pod_dns(&pod, &[], "cluster.local", &v1::DnsConfig::default());
        assert_eq!(dns.servers.len(), 3);
        assert_eq!(dns.searches.len(), 32);
        assert_eq!(dns.searches[0], "s0.example");
    }

    #[test]
    fn get_pod_dns_limits_search_chars_and_overlong_entries() {
        use rusternetes_common::resources::pod::PodDNSConfig;
        let long = "a".repeat(254);
        let pod = pod_with(PodSpec {
            dns_policy: Some("None".to_string()),
            dns_config: Some(PodDNSConfig {
                nameservers: None,
                searches: Some(vec![
                    "ok.example".into(),
                    long,
                    "b".repeat(250),
                    "c".repeat(250),
                    "d".repeat(250),
                    "e".repeat(250),
                    "f".repeat(250),
                    "g".repeat(250),
                    "h".repeat(250),
                    "i".repeat(250),
                    "j".repeat(250),
                ]),
                options: None,
            }),
            ..Default::default()
        });
        let dns = get_pod_dns(&pod, &[], "cluster.local", &v1::DnsConfig::default());
        assert!(!dns.searches.iter().any(|s| s.len() > 253));
        assert!(dns.searches.join(" ").len() <= 2048);
        assert_eq!(dns.searches[0], "ok.example");
        assert_eq!(dns.searches.len(), 9);
    }

    #[test]
    fn parse_resolv_conf_matches_upstream() {
        let dns = parse_resolv_conf(
            "# c\nnameserver 1.1.1.1\nnameserver 8.8.8.8 extra\nsearch a.example. . b.example\n\
             search c.example d.example.\noptions ndots:2 edns0\noptions ndots:4 timeout:1\n",
        )
        .unwrap();
        assert_eq!(dns.servers, vec!["1.1.1.1", "8.8.8.8"]);
        // last search line wins; trailing dots trimmed; "." dropped.
        assert_eq!(dns.searches, vec!["c.example", "d.example"]);
        let mut o = dns.options.clone();
        o.sort();
        assert_eq!(o, vec!["edns0", "ndots:4", "timeout:1"]);
        assert!(parse_resolv_conf("nameserver\n").is_err());
    }

    // --- #3065: resolv-conf fallback + DNS events (dns.go:431, :178, :119, :145) ---

    /// `dns.go:431`: `--resolv-conf=""` on a Default-policy pod yields the
    /// localhost nameserver and a `.` search instead of an empty config.
    #[test]
    fn empty_resolv_conf_falls_back_to_localhost() {
        let pod = pod_with(PodSpec {
            dns_policy: Some("Default".to_string()),
            ..Default::default()
        });
        let (dns, _) = get_pod_dns_with_events(
            &pod,
            &[],
            "cluster.local",
            &v1::DnsConfig::default(),
            "",
            &[],
        );
        assert_eq!(dns.servers, vec!["127.0.0.1".to_string()]);
        assert_eq!(dns.searches, vec![".".to_string()]);
        let (dns6, _) = get_pod_dns_with_events(
            &pod,
            &[],
            "cluster.local",
            &v1::DnsConfig::default(),
            "",
            &["fd00::1".to_string(), "10.0.0.1".to_string()],
        );
        assert_eq!(
            dns6.servers,
            vec!["::1".to_string(), "127.0.0.1".to_string()]
        );
    }

    /// A non-empty resolv-conf path never triggers the fallback.
    #[test]
    fn non_empty_resolv_conf_keeps_host_config() {
        let pod = pod_with(PodSpec {
            dns_policy: Some("Default".to_string()),
            ..Default::default()
        });
        let (dns, events) = get_pod_dns_with_events(
            &pod,
            &[],
            "cluster.local",
            &host_dns(),
            "/etc/resolv.conf",
            &[],
        );
        assert_eq!(dns, host_dns());
        assert!(events.is_empty());
    }

    /// `dns.go:145-146`: ClusterFirst without cluster DNS emits MissingClusterDNS
    /// on both the node and the pod.
    #[test]
    fn cluster_first_without_cluster_dns_emits_missing_cluster_dns() {
        let pod = pod_with(PodSpec::default());
        let (_, events) = get_pod_dns_with_events(
            &pod,
            &[],
            "cluster.local",
            &host_dns(),
            "/etc/resolv.conf",
            &[],
        );
        let node_msg = "kubelet does not have ClusterDNS IP configured and cannot create Pod using \"ClusterFirst\" policy. Falling back to \"Default\" policy.";
        assert_eq!(
            events,
            vec![
                DnsEvent {
                    target: DnsEventTarget::Node,
                    reason: "MissingClusterDNS",
                    message: node_msg.to_string(),
                },
                DnsEvent {
                    target: DnsEventTarget::Pod,
                    reason: "MissingClusterDNS",
                    message: format!("pod: \"web_prod(uid-123)\". {node_msg}"),
                },
            ]
        );
    }

    /// `dns.go:119`/`:145`: truncation emits DNSConfigForming on the pod.
    #[test]
    fn limit_truncation_emits_dns_config_forming() {
        use rusternetes_common::resources::pod::PodDNSConfig;
        let pod = pod_with(PodSpec {
            dns_policy: Some("None".to_string()),
            dns_config: Some(PodDNSConfig {
                nameservers: Some((1..=4).map(|i| format!("1.1.1.{i}")).collect()),
                searches: Some((0..33).map(|i| format!("s{i}.example")).collect()),
                options: None,
            }),
            ..Default::default()
        });
        let (_, events) = get_pod_dns_with_events(
            &pod,
            &[],
            "cluster.local",
            &v1::DnsConfig::default(),
            "/etc/resolv.conf",
            &[],
        );
        assert_eq!(events.len(), 2);
        assert!(events
            .iter()
            .all(|e| e.target == DnsEventTarget::Pod && e.reason == "DNSConfigForming"));
        assert_eq!(
            events[0].message,
            "Nameserver limits were exceeded, some nameservers have been omitted, the applied nameserver line is: 1.1.1.1 1.1.1.2 1.1.1.3"
        );
        assert!(events[1].message.starts_with(
            "Search Line limits were exceeded, some search paths have been omitted, the applied search line is: s0.example "
        ));
    }

    /// `dns.go:178`: CheckLimitsForResolvConf node-event messages.
    #[test]
    fn check_limits_for_resolv_conf_messages() {
        let dir = std::env::temp_dir().join(format!("rn-resolv-{}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        let ok = dir.join("ok");
        std::fs::write(&ok, "nameserver 1.1.1.1\nsearch a.example\n").unwrap();
        assert_eq!(
            check_limits_for_resolv_conf(ok.to_str().unwrap(), "cluster.local"),
            None
        );

        // 30 > 32-3 when a cluster domain is set.
        let many = dir.join("many");
        let search: Vec<String> = (0..30).map(|i| format!("s{i}.x")).collect();
        std::fs::write(&many, format!("search {}\n", search.join(" "))).unwrap();
        let p = many.to_str().unwrap();
        assert_eq!(
            check_limits_for_resolv_conf(p, "cluster.local"),
            Some(format!(
                "Resolv.conf file '{p}' contains search line consisting of more than 29 domains!"
            ))
        );
        assert_eq!(check_limits_for_resolv_conf(p, ""), None);

        let long = dir.join("long");
        std::fs::write(&long, format!("search {}\n", "a".repeat(254))).unwrap();
        let p = long.to_str().unwrap();
        assert_eq!(
            check_limits_for_resolv_conf(p, "cluster.local"),
            Some(format!(
                "Resolv.conf file \"{p}\" contains a search path which length is more than allowed 253 chars!"
            ))
        );

        let chars = dir.join("chars");
        let search: Vec<String> = (0..9).map(|_| "b".repeat(250)).collect();
        std::fs::write(&chars, format!("search {}\n", search.join(" "))).unwrap();
        let p = chars.to_str().unwrap();
        assert_eq!(
            check_limits_for_resolv_conf(p, ""),
            Some(format!(
                "Resolv.conf file '{p}' contains search line which length is more than allowed 2048 chars!"
            ))
        );

        let missing = check_limits_for_resolv_conf("/nonexistent/resolv.conf", "x").unwrap();
        assert!(missing.contains("No such file"), "{missing}");
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn security_context_maps_privileged_and_user() {
        use rusternetes_common::resources::pod::SecurityContext;
        let mut c = Container {
            name: "app".to_string(),
            image: "busybox".to_string(),
            ..Default::default()
        };
        c.security_context = Some(SecurityContext {
            privileged: Some(true),
            run_as_user: Some(1000),
            run_as_group: Some(2000),
            run_as_non_root: None,
            read_only_root_filesystem: Some(true),
            allow_privilege_escalation: None,
            proc_mount: None,
            capabilities: None,
            seccomp_profile: None,
            se_linux_options: None,
            app_armor_profile: None,
            windows_options: None,
        });
        let pod = pod_with(PodSpec {
            containers: vec![c.clone()],
            ..Default::default()
        });
        let sc = container_config(
            &pod,
            &c,
            "img",
            &HashMap::new(),
            &HashMap::new(),
            &HashMap::new(),
        )
        .unwrap()
        .linux
        .unwrap()
        .security_context
        .unwrap();
        assert!(sc.privileged);
        assert!(sc.readonly_rootfs);
        assert_eq!(sc.run_as_user.unwrap().value, 1000);
        assert_eq!(sc.run_as_group.unwrap().value, 2000);
        // allowPrivilegeEscalation unset -> no_new_privs stays false.
        assert!(!sc.no_new_privs);
    }

    #[test]
    fn run_as_user_group_inherit_from_pod_and_container_overrides() {
        use serde_json::json;
        // Returns (run_as_user, run_as_group) as seen by the container CRI SC.
        let run_as = |spec: serde_json::Value| -> (Option<i64>, Option<i64>) {
            let pod: Pod = serde_json::from_value(json!({
                "metadata": {"name": "p", "namespace": "default"}, "spec": spec
            }))
            .unwrap();
            let c = pod.spec.as_ref().unwrap().containers[0].clone();
            let sc = container_config(
                &pod,
                &c,
                "img",
                &HashMap::new(),
                &HashMap::new(),
                &HashMap::new(),
            )
            .unwrap()
            .linux
            .unwrap()
            .security_context
            .unwrap();
            (
                sc.run_as_user.map(|v| v.value),
                sc.run_as_group.map(|v| v.value),
            )
        };

        // Pod-level runAsUser/runAsGroup propagate to a container that sets none
        // of its own (upstream DetermineEffectiveSecurityContext /
        // securityContextFromPodSecurityContext).
        assert_eq!(
            run_as(json!({
                "securityContext": {"runAsUser": 1000, "runAsGroup": 2000},
                "containers": [{"name": "app", "image": "busybox"}]
            })),
            (Some(1000), Some(2000))
        );

        // Container-level values override the pod-level ones.
        assert_eq!(
            run_as(json!({
                "securityContext": {"runAsUser": 1000, "runAsGroup": 2000},
                "containers": [{"name": "app", "image": "busybox",
                    "securityContext": {"runAsUser": 4000, "runAsGroup": 5000}}]
            })),
            (Some(4000), Some(5000))
        );

        // Mixed: container overrides only runAsUser, pod fills runAsGroup.
        assert_eq!(
            run_as(json!({
                "securityContext": {"runAsUser": 1000, "runAsGroup": 2000},
                "containers": [{"name": "app", "image": "busybox",
                    "securityContext": {"runAsUser": 4000}}]
            })),
            (Some(4000), Some(2000))
        );

        // Neither set -> both None.
        assert_eq!(
            run_as(json!({
                "containers": [{"name": "app", "image": "busybox"}]
            })),
            (None, None)
        );
    }

    #[test]
    fn no_new_privs_only_when_allow_priv_esc_false() {
        use serde_json::json;
        let no_new_privs = |sec_ctx: serde_json::Value| -> bool {
            let c: Container = serde_json::from_value(json!({
                "name": "app",
                "image": "busybox",
                "securityContext": sec_ctx
            }))
            .unwrap();
            let pod = pod_with(PodSpec {
                containers: vec![c.clone()],
                ..Default::default()
            });
            container_config(
                &pod,
                &c,
                "img",
                &HashMap::new(),
                &HashMap::new(),
                &HashMap::new(),
            )
            .unwrap()
            .linux
            .unwrap()
            .security_context
            .unwrap()
            .no_new_privs
        };
        // Upstream AddNoNewPrivileges: true only when explicitly false.
        assert!(
            no_new_privs(json!({"allowPrivilegeEscalation": false})),
            "explicit false must set no_new_privs"
        );
        assert!(
            !no_new_privs(json!({"allowPrivilegeEscalation": true})),
            "explicit true must not set no_new_privs"
        );
        assert!(!no_new_privs(json!({})), "unset must not set no_new_privs");
    }

    #[test]
    fn seccomp_profile_container_pod_fallback_and_types() {
        use serde_json::json;
        use v1::security_profile::ProfileType;

        // Returns (profile_type, localhost_ref) of the container's seccomp, or
        // None if unset, from a pod spec JSON.
        let seccomp = |spec: serde_json::Value| -> Option<(i32, String)> {
            let pod: Pod = serde_json::from_value(json!({
                "metadata": {"name": "p", "namespace": "default"},
                "spec": spec
            }))
            .unwrap();
            let c = pod.spec.as_ref().unwrap().containers[0].clone();
            container_config(
                &pod,
                &c,
                "img",
                &HashMap::new(),
                &HashMap::new(),
                &HashMap::new(),
            )
            .unwrap()
            .linux
            .and_then(|l| l.security_context)
            .and_then(|s| s.seccomp)
            .map(|sp| (sp.profile_type, sp.localhost_ref))
        };

        // Container RuntimeDefault.
        assert_eq!(
            seccomp(json!({"containers": [{
                "name": "app", "image": "busybox",
                "securityContext": {"seccompProfile": {"type": "RuntimeDefault"}}
            }]})),
            Some((ProfileType::RuntimeDefault as i32, String::new()))
        );

        // Container Localhost carries the profile ref.
        assert_eq!(
            seccomp(json!({"containers": [{
                "name": "app", "image": "busybox",
                "securityContext": {"seccompProfile": {"type": "Localhost", "localhostProfile": "profiles/audit.json"}}
            }]})),
            Some((
                ProfileType::Localhost as i32,
                "profiles/audit.json".to_string()
            ))
        );

        // Pod-level profile applies when the container has none.
        assert_eq!(
            seccomp(json!({
                "securityContext": {"seccompProfile": {"type": "Unconfined"}},
                "containers": [{"name": "app", "image": "busybox"}]
            })),
            Some((ProfileType::Unconfined as i32, String::new()))
        );

        // Container profile overrides the pod's.
        assert_eq!(
            seccomp(json!({
                "securityContext": {"seccompProfile": {"type": "Unconfined"}},
                "containers": [{
                    "name": "app", "image": "busybox",
                    "securityContext": {"seccompProfile": {"type": "RuntimeDefault"}}
                }]
            })),
            Some((ProfileType::RuntimeDefault as i32, String::new()))
        );

        // No seccomp anywhere → unset.
        assert_eq!(
            seccomp(json!({"containers": [{"name": "app", "image": "busybox"}]})),
            None
        );
    }

    #[test]
    fn sandbox_security_context_carries_pod_fields() {
        use serde_json::json;
        use v1::security_profile::ProfileType;
        let pod: Pod = serde_json::from_value(json!({
            "metadata": {"name": "p", "namespace": "default"},
            "spec": {
                "securityContext": {
                    "runAsUser": 1000,
                    "runAsGroup": 3000,
                    "fsGroup": 2000,
                    "supplementalGroups": [5, 6],
                    "supplementalGroupsPolicy": "Strict",
                    "seLinuxOptions": {"level": "s0:c1,c2"}
                },
                "containers": [{
                    "name": "app", "image": "busybox",
                    "securityContext": {"privileged": true}
                }]
            }
        }))
        .unwrap();
        let sc = sandbox_config(&pod, "/log")
            .linux
            .unwrap()
            .security_context
            .unwrap();
        assert!(sc.privileged, "sandbox privileged when a container is");
        assert_eq!(sc.run_as_user.unwrap().value, 1000);
        assert_eq!(sc.run_as_group.unwrap().value, 3000);
        // fsGroup first, then supplementalGroups.
        assert_eq!(sc.supplemental_groups, vec![2000, 5, 6]);
        assert_eq!(
            sc.supplemental_groups_policy,
            v1::SupplementalGroupsPolicy::Strict as i32
        );
        assert_eq!(sc.selinux_options.unwrap().level, "s0:c1,c2");
        // Sandbox seccomp is always forced to RuntimeDefault (#84623).
        assert_eq!(
            sc.seccomp.unwrap().profile_type,
            ProfileType::RuntimeDefault as i32
        );
    }

    /// Regression: the WORKLOAD container's `LinuxContainerSecurityContext` must
    /// carry `fsGroup` (then the pod's `supplementalGroups`) in
    /// `supplemental_groups`, plus `supplementalGroupsPolicy`. In containerd the
    /// workload container is a separate OCI container from the sandbox pause
    /// container, so the process's supplementary groups come from the container
    /// SC, NOT the sandbox SC. Without fsGroup here, a non-root pod cannot read a
    /// mode-0440 secret/projected volume whose files were chowned to `:fsGroup`
    /// (`open …: permission denied`) — the two `should be consumable from pods in
    /// volume as non-root with defaultMode and fsGroup set` conformance specs.
    /// Mirrors upstream `pkg/kubelet/kuberuntime/security_context.go`
    /// (`determineEffectiveSecurityContext`): fsGroup appended first, then
    /// `supplementalGroups`, then the policy.
    #[test]
    fn container_security_context_carries_fsgroup_and_supplemental_groups() {
        use serde_json::json;
        let pod: Pod = serde_json::from_value(json!({
            "metadata": {"name": "p", "namespace": "default"},
            "spec": {
                "securityContext": {
                    "runAsUser": 1000,
                    "fsGroup": 2000,
                    "supplementalGroups": [5, 6],
                    "supplementalGroupsPolicy": "Strict"
                },
                "containers": [{"name": "app", "image": "busybox"}]
            }
        }))
        .unwrap();
        let container = pod.spec.as_ref().unwrap().containers[0].clone();
        let sc = linux_security_context(&pod, &container)
            .expect("container security context must be built");
        // fsGroup first, then the pod's supplementalGroups.
        assert_eq!(sc.supplemental_groups, vec![2000, 5, 6]);
        assert_eq!(
            sc.supplemental_groups_policy,
            v1::SupplementalGroupsPolicy::Strict as i32
        );
    }

    /// `extra_supplemental_gids` (upstream `getExtraSupplementalGID`) must
    /// de-duplicate volume GIDs and drop any GID already in the pod's
    /// `supplementalGroups`, while preserving fsGroup-equal GIDs (upstream does
    /// not consult fsGroup) and input order.
    #[test]
    fn extra_supplemental_gids_dedups_and_skips_supplemental_groups() {
        use serde_json::json;
        let pod: Pod = serde_json::from_value(json!({
            "metadata": {"name": "p", "namespace": "default"},
            "spec": {
                "securityContext": {"fsGroup": 2000, "supplementalGroups": [5678]},
                "containers": [{"name": "app", "image": "busybox"}]
            }
        }))
        .unwrap();
        // 5678 is already a supplementalGroup → dropped. 1234 is duplicated in
        // the input → kept once. 2000 equals fsGroup but is NOT a
        // supplementalGroup → kept (upstream parity).
        let extra = extra_supplemental_gids(&[1234, 5678, 1234, 2000], &pod);
        assert_eq!(extra, vec![1234, 2000]);
    }

    #[test]
    fn apparmor_profile_container_pod_fallback_and_types() {
        use serde_json::json;
        use v1::security_profile::ProfileType;
        let apparmor = |spec: serde_json::Value| -> Option<(i32, String)> {
            let pod: Pod = serde_json::from_value(json!({
                "metadata": {"name": "p", "namespace": "default"}, "spec": spec
            }))
            .unwrap();
            let c = pod.spec.as_ref().unwrap().containers[0].clone();
            container_config(
                &pod,
                &c,
                "img",
                &HashMap::new(),
                &HashMap::new(),
                &HashMap::new(),
            )
            .unwrap()
            .linux
            .and_then(|l| l.security_context)
            .and_then(|s| s.apparmor)
            .map(|p| (p.profile_type, p.localhost_ref))
        };
        assert_eq!(
            apparmor(json!({"containers": [{
                "name": "app", "image": "busybox",
                "securityContext": {"appArmorProfile": {"type": "Localhost", "localhostProfile": "k8s-audit"}}
            }]})),
            Some((ProfileType::Localhost as i32, "k8s-audit".to_string()))
        );
        // Pod-level applies when the container has none.
        assert_eq!(
            apparmor(json!({
                "securityContext": {"appArmorProfile": {"type": "RuntimeDefault"}},
                "containers": [{"name": "app", "image": "busybox"}]
            })),
            Some((ProfileType::RuntimeDefault as i32, String::new()))
        );
        assert_eq!(
            apparmor(json!({"containers": [{"name": "app", "image": "busybox"}]})),
            None
        );
    }

    #[test]
    fn selinux_options_mapped_with_pod_fallback() {
        use serde_json::json;
        let selinux = |spec: serde_json::Value| -> Option<(String, String, String, String)> {
            let pod: Pod = serde_json::from_value(json!({
                "metadata": {"name": "p", "namespace": "default"}, "spec": spec
            }))
            .unwrap();
            let c = pod.spec.as_ref().unwrap().containers[0].clone();
            container_config(
                &pod,
                &c,
                "img",
                &HashMap::new(),
                &HashMap::new(),
                &HashMap::new(),
            )
            .unwrap()
            .linux
            .and_then(|l| l.security_context)
            .and_then(|s| s.selinux_options)
            .map(|o| (o.user, o.role, o.r#type, o.level))
        };
        // Container options win.
        assert_eq!(
            selinux(json!({"containers": [{
                "name": "app", "image": "busybox",
                "securityContext": {"seLinuxOptions": {"level": "s0:c1,c2", "type": "spc_t"}}
            }]})),
            Some((
                String::new(),
                String::new(),
                "spc_t".to_string(),
                "s0:c1,c2".to_string()
            ))
        );
        // Pod-level applies when the container has none.
        assert_eq!(
            selinux(json!({
                "securityContext": {"seLinuxOptions": {"user": "system_u"}},
                "containers": [{"name": "app", "image": "busybox"}]
            })),
            Some((
                "system_u".to_string(),
                String::new(),
                String::new(),
                String::new()
            ))
        );
        assert_eq!(
            selinux(json!({"containers": [{"name": "app", "image": "busybox"}]})),
            None
        );
    }

    fn env_from(
        name: &str,
        src: rusternetes_common::resources::pod::EnvVarSource,
    ) -> rusternetes_common::resources::pod::EnvVar {
        rusternetes_common::resources::pod::EnvVar {
            name: name.to_string(),
            value: None,
            value_from: Some(src),
        }
    }

    fn empty_source() -> rusternetes_common::resources::pod::EnvVarSource {
        rusternetes_common::resources::pod::EnvVarSource {
            config_map_key_ref: None,
            secret_key_ref: None,
            field_ref: None,
            resource_field_ref: None,
            file_key_ref: None,
        }
    }

    #[test]
    fn field_ref_resolves_labels_annotations_spec_and_phase() {
        use rusternetes_common::types::Phase;
        let mut pod = pod_with(PodSpec {
            restart_policy: Some("OnFailure".to_string()),
            scheduler_name: Some("custom-sched".to_string()),
            ..Default::default()
        });
        pod.metadata.labels = Some(HashMap::from([("app".to_string(), "web".to_string())]));
        pod.metadata.annotations = Some(HashMap::from([("team".to_string(), "infra".to_string())]));
        pod.status = Some(rusternetes_common::resources::pod::PodStatus {
            phase: Some(Phase::Running),
            ..Default::default()
        });

        assert_eq!(
            pod_field_value(&pod, "metadata.labels['app']").as_deref(),
            Some("web")
        );
        assert_eq!(
            pod_field_value(&pod, "metadata.annotations['team']").as_deref(),
            Some("infra")
        );
        assert_eq!(
            pod_field_value(&pod, "spec.restartPolicy").as_deref(),
            Some("OnFailure")
        );
        assert_eq!(
            pod_field_value(&pod, "spec.schedulerName").as_deref(),
            Some("custom-sched")
        );
        assert_eq!(
            pod_field_value(&pod, "status.phase").as_deref(),
            Some("Running")
        );
        // Missing subscript key resolves to None (var omitted).
        assert_eq!(pod_field_value(&pod, "metadata.labels['nope']"), None);
    }

    /// A `resourceFieldRef` selector; env vars never carry a `containerName`
    /// (validation rejects it outside a volume).
    fn rfr(
        resource: &str,
        divisor: Option<&str>,
    ) -> rusternetes_common::resources::ResourceFieldSelector {
        rusternetes_common::resources::ResourceFieldSelector {
            container_name: None,
            resource: resource.to_string(),
            divisor: divisor.map(str::to_string),
        }
    }

    #[test]
    fn resource_field_ref_resolves_ephemeral_storage_and_hugepages() {
        use rusternetes_common::types::ResourceRequirements;
        let c = Container {
            name: "app".to_string(),
            image: "busybox".to_string(),
            resources: Some(ResourceRequirements {
                limits: Some(HashMap::from([
                    ("ephemeral-storage".to_string(), "1Gi".to_string()),
                    ("hugepages-2Mi".to_string(), "4Mi".to_string()),
                ])),
                requests: None,
                claims: None,
            }),
            ..Default::default()
        };
        // Both normalize to bytes, like memory (not raw passthrough).
        assert_eq!(
            container_resource_value(&c, &rfr("limits.ephemeral-storage", None), None).as_deref(),
            Some("1073741824")
        );
        assert_eq!(
            container_resource_value(&c, &rfr("limits.hugepages-2Mi", None), None).as_deref(),
            Some("4194304")
        );
    }

    #[test]
    fn resource_field_ref_defaults_unset_limits_to_node_allocatable() {
        use rusternetes_common::types::ResourceRequirements;
        // upstream MergeContainerResourceLimits: an unset cpu/memory LIMIT
        // defaults to the node's allocatable for resourceFieldRef extraction
        // (the "default limits.cpu/memory from node allocatable" NodeConformance
        // spec). cpu rounds up to whole cores; memory is bytes.
        let c = Container {
            name: "app".to_string(),
            image: "busybox".to_string(),
            resources: Some(ResourceRequirements {
                limits: None,
                requests: None,
                claims: None,
            }),
            ..Default::default()
        };
        let alloc = HashMap::from([
            ("cpu".to_string(), "4".to_string()),
            ("memory".to_string(), "8Gi".to_string()),
        ]);
        assert_eq!(
            container_resource_value(&c, &rfr("limits.cpu", None), Some(&alloc)).as_deref(),
            Some("4")
        );
        assert_eq!(
            container_resource_value(&c, &rfr("limits.memory", None), Some(&alloc)).as_deref(),
            Some("8589934592")
        );
        // With no node allocatable to merge in, the limit stays unset, and an
        // unset quantity is the zero Quantity upstream — `Limits.Cpu()` returns
        // it and `convertResourceCPUToString` renders "0"
        // (`pkg/api/v1/resource/helpers.go:115,146-149`). Upstream never takes
        // this branch (`getNodeAnyWay` always yields allocatable); the previous
        // copy dropped the variable entirely instead.
        assert_eq!(
            container_resource_value(&c, &rfr("limits.cpu", None), None).as_deref(),
            Some("0")
        );
    }

    #[test]
    fn config_map_and_secret_key_ref_env_resolved() {
        use rusternetes_common::resources::pod::{ConfigMapKeySelector, SecretKeySelector};
        let mut c = Container {
            name: "app".to_string(),
            image: "busybox".to_string(),
            ..Default::default()
        };
        c.env = Some(vec![
            env_from(
                "FROM_CM",
                rusternetes_common::resources::pod::EnvVarSource {
                    config_map_key_ref: Some(ConfigMapKeySelector {
                        name: "cfg".to_string(),
                        key: "color".to_string(),
                        optional: None,
                    }),
                    ..empty_source()
                },
            ),
            env_from(
                "FROM_SECRET",
                rusternetes_common::resources::pod::EnvVarSource {
                    secret_key_ref: Some(SecretKeySelector {
                        name: "creds".to_string(),
                        key: "password".to_string(),
                        optional: None,
                    }),
                    ..empty_source()
                },
            ),
        ]);
        let pod = pod_with(PodSpec {
            containers: vec![c.clone()],
            ..Default::default()
        });

        let mut cm = ConfigMap::new("cfg", "prod");
        cm.data = Some(HashMap::from([("color".to_string(), "blue".to_string())]));
        let mut secret = Secret::new("creds", "prod");
        secret.data = Some(HashMap::from([(
            "password".to_string(),
            b"s3cr3t".to_vec(),
        )]));
        let config_maps = HashMap::from([("cfg".to_string(), cm)]);
        let secrets = HashMap::from([("creds".to_string(), secret)]);

        let cfg =
            container_config(&pod, &c, "img", &HashMap::new(), &config_maps, &secrets).unwrap();
        let get = |k: &str| {
            cfg.envs
                .iter()
                .find(|e| e.key == k)
                .map(|e| e.value.as_str())
        };
        assert_eq!(get("FROM_CM"), Some("blue"));
        assert_eq!(get("FROM_SECRET"), Some("s3cr3t"));
    }

    #[test]
    fn env_from_bulk_imports_configmap_and_secret_with_prefix() {
        use rusternetes_common::resources::pod::{
            ConfigMapEnvSource, EnvFromSource, SecretEnvSource,
        };
        let mut c = Container {
            name: "app".to_string(),
            image: "busybox".to_string(),
            ..Default::default()
        };
        c.env_from = Some(vec![
            EnvFromSource {
                prefix: None,
                config_map_ref: Some(ConfigMapEnvSource {
                    name: "cfg".to_string(),
                    optional: None,
                }),
                secret_ref: None,
            },
            EnvFromSource {
                prefix: Some("SEC_".to_string()),
                config_map_ref: None,
                secret_ref: Some(SecretEnvSource {
                    name: "creds".to_string(),
                    optional: None,
                }),
            },
        ]);
        let pod = pod_with(PodSpec {
            containers: vec![c.clone()],
            ..Default::default()
        });

        let mut cm = ConfigMap::new("cfg", "prod");
        cm.data = Some(HashMap::from([
            ("COLOR".to_string(), "blue".to_string()),
            ("SIZE".to_string(), "large".to_string()),
        ]));
        let mut secret = Secret::new("creds", "prod");
        secret.data = Some(HashMap::from([("TOKEN".to_string(), b"abc".to_vec())]));
        let config_maps = HashMap::from([("cfg".to_string(), cm)]);
        let secrets = HashMap::from([("creds".to_string(), secret)]);

        let cfg =
            container_config(&pod, &c, "img", &HashMap::new(), &config_maps, &secrets).unwrap();
        let get = |k: &str| {
            cfg.envs
                .iter()
                .find(|e| e.key == k)
                .map(|e| e.value.as_str())
        };
        assert_eq!(get("COLOR"), Some("blue"));
        assert_eq!(get("SIZE"), Some("large"));
        // The secretRef prefix is prepended to every imported key.
        assert_eq!(get("SEC_TOKEN"), Some("abc"));
    }

    #[test]
    fn explicit_env_overrides_env_from_without_duplicating() {
        use rusternetes_common::resources::pod::{ConfigMapEnvSource, EnvFromSource, EnvVar};
        let mut c = Container {
            name: "app".to_string(),
            image: "busybox".to_string(),
            ..Default::default()
        };
        c.env_from = Some(vec![EnvFromSource {
            prefix: None,
            config_map_ref: Some(ConfigMapEnvSource {
                name: "cfg".to_string(),
                optional: None,
            }),
            secret_ref: None,
        }]);
        c.env = Some(vec![EnvVar {
            name: "COLOR".to_string(),
            value: Some("red".to_string()),
            value_from: None,
        }]);
        let pod = pod_with(PodSpec {
            containers: vec![c.clone()],
            ..Default::default()
        });

        let mut cm = ConfigMap::new("cfg", "prod");
        cm.data = Some(HashMap::from([("COLOR".to_string(), "blue".to_string())]));
        let config_maps = HashMap::from([("cfg".to_string(), cm)]);

        let cfg = container_config(
            &pod,
            &c,
            "img",
            &HashMap::new(),
            &config_maps,
            &HashMap::new(),
        )
        .unwrap();
        let colors: Vec<&str> = cfg
            .envs
            .iter()
            .filter(|e| e.key == "COLOR")
            .map(|e| e.value.as_str())
            .collect();
        // Explicit env wins over envFrom, and there is exactly one COLOR entry.
        assert_eq!(colors, vec!["red"]);
    }

    #[test]
    fn missing_key_ref_omits_var() {
        use rusternetes_common::resources::pod::ConfigMapKeySelector;
        let mut c = Container {
            name: "app".to_string(),
            image: "busybox".to_string(),
            ..Default::default()
        };
        c.env = Some(vec![env_from(
            "MISSING",
            rusternetes_common::resources::pod::EnvVarSource {
                config_map_key_ref: Some(ConfigMapKeySelector {
                    name: "absent".to_string(),
                    key: "k".to_string(),
                    optional: Some(true),
                }),
                ..empty_source()
            },
        )]);
        let pod = pod_with(PodSpec {
            containers: vec![c.clone()],
            ..Default::default()
        });
        let cfg = container_config(
            &pod,
            &c,
            "img",
            &HashMap::new(),
            &HashMap::new(),
            &HashMap::new(),
        )
        .unwrap();
        assert!(cfg.envs.iter().all(|e| e.key != "MISSING"));
    }

    fn pod_with_env(env: Vec<rusternetes_common::resources::pod::EnvVar>) -> (Pod, Container) {
        let mut c = Container {
            name: "app".to_string(),
            image: "busybox".to_string(),
            ..Default::default()
        };
        c.env = Some(env);
        let pod = pod_with(PodSpec {
            containers: vec![c.clone()],
            ..Default::default()
        });
        (pod, c)
    }

    #[test]
    fn validate_rejects_nonoptional_missing_key() {
        use rusternetes_common::resources::pod::ConfigMapKeySelector;
        let (pod, c) = pod_with_env(vec![env_from(
            "NEED",
            rusternetes_common::resources::pod::EnvVarSource {
                config_map_key_ref: Some(ConfigMapKeySelector {
                    name: "cfg".to_string(),
                    key: "absent".to_string(),
                    optional: None,
                }),
                ..empty_source()
            },
        )]);
        // ConfigMap exists but lacks the key.
        let mut cm = ConfigMap::new("cfg", "prod");
        cm.data = Some(HashMap::from([("present".to_string(), "x".to_string())]));
        let config_maps = HashMap::from([("cfg".to_string(), cm)]);
        let err = validate_env_key_refs(&pod, &c, &config_maps, &HashMap::new()).unwrap_err();
        assert_eq!(err, "couldn't find key absent in ConfigMap prod/cfg");
    }

    #[test]
    fn validate_rejects_nonoptional_missing_object() {
        use rusternetes_common::resources::pod::SecretKeySelector;
        let (pod, c) = pod_with_env(vec![env_from(
            "NEED",
            rusternetes_common::resources::pod::EnvVarSource {
                secret_key_ref: Some(SecretKeySelector {
                    name: "creds".to_string(),
                    key: "password".to_string(),
                    optional: None,
                }),
                ..empty_source()
            },
        )]);
        let err = validate_env_key_refs(&pod, &c, &HashMap::new(), &HashMap::new()).unwrap_err();
        assert_eq!(err, "couldn't get Secret prod/creds");
    }

    #[test]
    fn validate_allows_optional_missing_and_resolved_refs() {
        use rusternetes_common::resources::pod::{ConfigMapKeySelector, SecretKeySelector};
        let (pod, c) = pod_with_env(vec![
            // optional + missing -> ok
            env_from(
                "OPT",
                rusternetes_common::resources::pod::EnvVarSource {
                    config_map_key_ref: Some(ConfigMapKeySelector {
                        name: "gone".to_string(),
                        key: "k".to_string(),
                        optional: Some(true),
                    }),
                    ..empty_source()
                },
            ),
            // non-optional + present -> ok
            env_from(
                "OK",
                rusternetes_common::resources::pod::EnvVarSource {
                    secret_key_ref: Some(SecretKeySelector {
                        name: "creds".to_string(),
                        key: "password".to_string(),
                        optional: None,
                    }),
                    ..empty_source()
                },
            ),
        ]);
        let mut s = Secret::new("creds", "prod");
        s.data = Some(HashMap::from([("password".to_string(), b"x".to_vec())]));
        let secrets = HashMap::from([("creds".to_string(), s)]);
        assert!(validate_env_key_refs(&pod, &c, &HashMap::new(), &secrets).is_ok());
    }

    fn container_with_env_from(
        env_from: Vec<rusternetes_common::resources::pod::EnvFromSource>,
    ) -> (Pod, Container) {
        let c = Container {
            name: "app".to_string(),
            image: "busybox".to_string(),
            env_from: Some(env_from),
            ..Default::default()
        };
        let pod = pod_with(PodSpec {
            containers: vec![c.clone()],
            ..Default::default()
        });
        (pod, c)
    }

    #[test]
    fn validate_rejects_nonoptional_missing_envfrom_configmap() {
        use rusternetes_common::resources::pod::{ConfigMapEnvSource, EnvFromSource};
        let (pod, c) = container_with_env_from(vec![EnvFromSource {
            prefix: None,
            config_map_ref: Some(ConfigMapEnvSource {
                name: "bulk".to_string(),
                optional: None,
            }),
            secret_ref: None,
        }]);
        let err = validate_env_key_refs(&pod, &c, &HashMap::new(), &HashMap::new()).unwrap_err();
        assert_eq!(err, "couldn't get ConfigMap prod/bulk");
    }

    #[test]
    fn validate_rejects_nonoptional_missing_envfrom_secret() {
        use rusternetes_common::resources::pod::{EnvFromSource, SecretEnvSource};
        let (pod, c) = container_with_env_from(vec![EnvFromSource {
            prefix: None,
            config_map_ref: None,
            secret_ref: Some(SecretEnvSource {
                name: "bulk-creds".to_string(),
                optional: None,
            }),
        }]);
        let err = validate_env_key_refs(&pod, &c, &HashMap::new(), &HashMap::new()).unwrap_err();
        assert_eq!(err, "couldn't get Secret prod/bulk-creds");
    }

    #[test]
    fn validate_allows_optional_missing_and_present_envfrom() {
        use rusternetes_common::resources::pod::{
            ConfigMapEnvSource, EnvFromSource, SecretEnvSource,
        };
        let (pod, c) = container_with_env_from(vec![
            // optional + missing -> ok
            EnvFromSource {
                prefix: None,
                config_map_ref: Some(ConfigMapEnvSource {
                    name: "gone".to_string(),
                    optional: Some(true),
                }),
                secret_ref: None,
            },
            // non-optional + present -> ok
            EnvFromSource {
                prefix: None,
                config_map_ref: None,
                secret_ref: Some(SecretEnvSource {
                    name: "creds".to_string(),
                    optional: None,
                }),
            },
        ]);
        let secrets = HashMap::from([("creds".to_string(), Secret::new("creds", "prod"))]);
        assert!(validate_env_key_refs(&pod, &c, &HashMap::new(), &secrets).is_ok());
    }

    fn sec_ctx_for(sec_ctx: serde_json::Value) -> v1::LinuxContainerSecurityContext {
        let mut c: Container = serde_json::from_value(serde_json::json!({
            "name": "app",
            "image": "busybox",
        }))
        .unwrap();
        if !sec_ctx.is_null() {
            c.security_context = Some(serde_json::from_value(sec_ctx).unwrap());
        }
        let pod = pod_with(PodSpec {
            containers: vec![c.clone()],
            ..Default::default()
        });
        container_config(
            &pod,
            &c,
            "img",
            &HashMap::new(),
            &HashMap::new(),
            &HashMap::new(),
        )
        .unwrap()
        .linux
        .unwrap()
        .security_context
        .unwrap()
    }

    #[test]
    fn default_procmount_sends_default_masked_and_readonly_paths() {
        // Unset procMount → upstream default masked/readonly paths.
        let sc = sec_ctx_for(serde_json::Value::Null);
        assert_eq!(
            sc.masked_paths,
            rusternetes_kubelet::cri_runtime::host_policy::default_masked_paths()
        );
        assert_eq!(sc.readonly_paths, DEFAULT_READONLY_PATHS);
    }

    #[test]
    fn explicit_default_procmount_sends_defaults() {
        let sc = sec_ctx_for(serde_json::json!({"procMount": "Default"}));
        assert_eq!(
            sc.masked_paths,
            rusternetes_kubelet::cri_runtime::host_policy::default_masked_paths()
        );
        assert_eq!(sc.readonly_paths, DEFAULT_READONLY_PATHS);
    }

    #[test]
    fn unmasked_procmount_sends_empty_paths() {
        // procMount: Unmasked → nothing masked/readonly (upstream returns []).
        let sc = sec_ctx_for(serde_json::json!({"procMount": "Unmasked"}));
        assert!(sc.masked_paths.is_empty());
        assert!(sc.readonly_paths.is_empty());
    }

    /// Regression test for the NC "should provide container's limits.cpu/memory
    /// and requests.cpu/memory as env vars" and "should provide default
    /// limits.cpu/memory from node allocatable" conformance failures.
    ///
    /// The Go/Kubernetes client marshals an unset `resource.Quantity` divisor as
    /// `"0"` in JSON (`zeroBytes = []byte("0")` in
    /// `k8s.io/apimachinery/pkg/api/resource/amount.go:52`). When rusternetes
    /// deserialises the pod it gets `divisor: Some("0")`, which the former code
    /// treated as a zero divisor and returned `None` (skipping the env var
    /// entirely) instead of defaulting it to `"1"` — matching upstream
    /// `pkg/api/v1/resource/helpers.go` `ExtractContainerResourceValue`
    /// (lines 106-111):
    ///
    /// ```go
    /// divisor := resource.Quantity{}
    /// if divisor.Cmp(fs.Divisor) == 0 {
    ///     divisor = resource.MustParse("1")
    /// }
    /// ```
    #[test]
    fn resource_field_ref_zero_divisor_treated_as_default_one() {
        use rusternetes_common::resources::pod::{EnvVar, EnvVarSource};
        use rusternetes_common::types::ResourceRequirements;

        // Container with explicit limits/requests matching the conformance pod
        // created by testDownwardAPI in test/e2e/common/node/downwardapi.go:
        //   CPU limit=1250m, memory limit=64Mi, CPU req=250m, memory req=32Mi.
        let mut c = Container {
            name: "dapi-container".to_string(),
            image: "busybox".to_string(),
            resources: Some(ResourceRequirements {
                limits: Some(HashMap::from([
                    ("cpu".to_string(), "1250m".to_string()),
                    ("memory".to_string(), "64Mi".to_string()),
                ])),
                requests: Some(HashMap::from([
                    ("cpu".to_string(), "250m".to_string()),
                    ("memory".to_string(), "32Mi".to_string()),
                ])),
                claims: None,
            }),
            ..Default::default()
        };
        // divisor "0" — the Go client's wire representation of an unset divisor.
        let make_rfr_env = |name: &str, resource: &str| EnvVar {
            name: name.to_string(),
            value: None,
            value_from: Some(EnvVarSource {
                resource_field_ref: Some(rusternetes_common::resources::ResourceFieldSelector {
                    container_name: None,
                    resource: resource.to_string(),
                    divisor: Some("0".to_string()), // zero Quantity = unset on the wire
                }),
                config_map_key_ref: None,
                secret_key_ref: None,
                field_ref: None,
                file_key_ref: None,
            }),
        };
        c.env = Some(vec![
            make_rfr_env("CPU_LIMIT", "limits.cpu"),
            make_rfr_env("MEMORY_LIMIT", "limits.memory"),
            make_rfr_env("CPU_REQUEST", "requests.cpu"),
            make_rfr_env("MEMORY_REQUEST", "requests.memory"),
        ]);
        let pod = pod_with(PodSpec {
            containers: vec![c.clone()],
            ..Default::default()
        });

        // Exercise the full env_vars path via container_config (no allocatable
        // needed — all limits/requests are explicit here).
        let cfg = container_config(
            &pod,
            &c,
            "img",
            &HashMap::new(),
            &HashMap::new(),
            &HashMap::new(),
        )
        .unwrap();
        let get = |k: &str| {
            cfg.envs
                .iter()
                .find(|e| e.key == k)
                .map(|e| e.value.as_str())
        };
        // Conformance expectations from test/e2e/common/node/downwardapi.go:234-237:
        // CPU_LIMIT=2 (ceil(1250m / 1000m) = 2)
        assert_eq!(
            get("CPU_LIMIT"),
            Some("2"),
            "CPU_LIMIT missing or wrong — divisor '0' not defaulted to '1'"
        );
        // MEMORY_LIMIT=67108864 (64 MiB = 64*1024*1024 = 67108864 bytes)
        assert_eq!(
            get("MEMORY_LIMIT"),
            Some("67108864"),
            "MEMORY_LIMIT missing or wrong"
        );
        // CPU_REQUEST=1 (ceil(250m / 1000m) = 1)
        assert_eq!(
            get("CPU_REQUEST"),
            Some("1"),
            "CPU_REQUEST missing or wrong"
        );
        // MEMORY_REQUEST=33554432 (32 MiB = 32*1024*1024)
        assert_eq!(
            get("MEMORY_REQUEST"),
            Some("33554432"),
            "MEMORY_REQUEST missing or wrong"
        );
    }

    /// Regression test for "should provide default limits.cpu/memory from node
    /// allocatable": a container with NO explicit limits must fall back to the
    /// node allocatable, and divisor "0" must still resolve correctly.
    #[test]
    fn resource_field_ref_zero_divisor_with_allocatable_fallback() {
        use rusternetes_common::resources::pod::{EnvVar, EnvVarSource};
        use rusternetes_common::types::ResourceRequirements;

        let mut c = Container {
            name: "dapi-container".to_string(),
            image: "busybox".to_string(),
            // No limits set — must default to node allocatable.
            resources: Some(ResourceRequirements {
                limits: None,
                requests: None,
                claims: None,
            }),
            ..Default::default()
        };
        let make_rfr_env = |name: &str, resource: &str| EnvVar {
            name: name.to_string(),
            value: None,
            value_from: Some(EnvVarSource {
                resource_field_ref: Some(rusternetes_common::resources::ResourceFieldSelector {
                    container_name: None,
                    resource: resource.to_string(),
                    divisor: Some("0".to_string()),
                }),
                config_map_key_ref: None,
                secret_key_ref: None,
                field_ref: None,
                file_key_ref: None,
            }),
        };
        c.env = Some(vec![
            make_rfr_env("CPU_LIMIT", "limits.cpu"),
            make_rfr_env("MEMORY_LIMIT", "limits.memory"),
        ]);
        let pod = pod_with(PodSpec {
            containers: vec![c.clone()],
            ..Default::default()
        });

        let alloc = HashMap::from([
            ("cpu".to_string(), "4".to_string()),
            ("memory".to_string(), "8Gi".to_string()),
        ]);
        let cfg = container_config_with_allocatable(
            &pod,
            &c,
            "img",
            &HashMap::new(),
            &HashMap::new(),
            &HashMap::new(),
            Some(&alloc),
        )
        .unwrap();
        let get = |k: &str| {
            cfg.envs
                .iter()
                .find(|e| e.key == k)
                .map(|e| e.value.as_str())
        };
        // Node has 4 CPU cores → CPU_LIMIT must match /[1-9]/ (conformance line 269).
        assert_eq!(
            get("CPU_LIMIT"),
            Some("4"),
            "CPU_LIMIT missing — zero-divisor with allocatable fallback broken"
        );
        // Node has 8 GiB memory.
        assert_eq!(
            get("MEMORY_LIMIT"),
            Some("8589934592"),
            "MEMORY_LIMIT missing"
        );
    }

    #[test]
    fn bare_container_still_gets_masked_paths() {
        // A container with no securityContext must still receive the default
        // masked paths — containerd resets them to empty otherwise, leaving
        // /proc exposed.
        let c: Container = serde_json::from_value(serde_json::json!({
            "name": "app",
            "image": "busybox",
        }))
        .unwrap();
        let pod = pod_with(PodSpec {
            containers: vec![c.clone()],
            ..Default::default()
        });
        let sc = container_config(
            &pod,
            &c,
            "img",
            &HashMap::new(),
            &HashMap::new(),
            &HashMap::new(),
        )
        .unwrap()
        .linux
        .unwrap()
        .security_context
        .unwrap();
        assert_eq!(
            sc.masked_paths,
            rusternetes_kubelet::cri_runtime::host_policy::default_masked_paths()
        );
    }

    /// `makeMounts` image-volume arm (`pkg/kubelet/kubelet_pods.go:327-330`)
    /// and the CRI `Mount` the kuberuntime builds from it
    /// (`kuberuntime_container.go:487-496`): with the `ImageVolume` gate on, a
    /// volume the kubelet pulled gets `Mount.image` + `Mount.image_sub_path`
    /// and NO `host_path` (cri-api: "host_path ... Has to be empty if the
    /// image field below is provided"); the image mount is read-only because
    /// the image plugin's mounter is (`image.go:75-81`).
    fn image_volume_container(sub_path: Option<&str>) -> Container {
        use rusternetes_common::resources::pod::VolumeMount;
        let vm = |name: &str, path: &str, sub: Option<&str>| VolumeMount {
            name: name.to_string(),
            mount_path: path.to_string(),
            read_only: None,
            sub_path: sub.map(str::to_string),
            sub_path_expr: None,
            mount_propagation: None,
            recursive_read_only: None,
        };
        Container {
            name: "c".to_string(),
            image: "busybox".to_string(),
            volume_mounts: Some(vec![
                vm("img", "/data", sub_path),
                vm("plain", "/plain", None),
            ]),
            ..Default::default()
        }
    }

    fn pulled_image() -> HashMap<String, v1::ImageSpec> {
        HashMap::from([(
            "img".to_string(),
            v1::ImageSpec {
                image: "sha256:abc".to_string(),
                user_specified_image: "quay.io/a/b:1".to_string(),
                ..Default::default()
            },
        )])
    }

    fn image_host_paths() -> HashMap<String, String> {
        HashMap::from([
            ("img".to_string(), String::new()),
            ("plain".to_string(), "/host/plain".to_string()),
        ])
    }

    #[test]
    #[serial_test::serial]
    fn an_image_volume_mount_carries_the_image_not_a_host_path() {
        let _g = rusternetes_common::feature_gates::with_feature(
            rusternetes_common::feature_gates::Feature::ImageVolume,
            true,
        );
        let c = image_volume_container(Some("dir/sub"));
        let attrs = HashMap::from([(
            "img".to_string(),
            MountAttrs {
                read_only: true,
                selinux_relabel: false,
            },
        )]);
        let m = mounts_with(
            &c,
            &image_host_paths(),
            &HashMap::new(),
            &attrs,
            false,
            false,
            &pulled_image(),
        )
        .unwrap();
        assert_eq!(m.len(), 2);
        assert_eq!(m[0].container_path, "/data");
        assert_eq!(m[0].host_path, "");
        assert_eq!(m[0].image, Some(pulled_image().remove("img").unwrap()));
        assert_eq!(m[0].image_sub_path, "dir/sub");
        assert!(m[0].readonly);
        // A volume the kubelet did not pull is an ordinary host mount.
        assert_eq!(m[1].host_path, "/host/plain");
        assert_eq!(m[1].image, None);
        assert_eq!(m[1].image_sub_path, "");
    }

    #[test]
    #[serial_test::serial]
    fn an_image_volume_without_a_sub_path_has_an_empty_image_sub_path() {
        let _g = rusternetes_common::feature_gates::with_feature(
            rusternetes_common::feature_gates::Feature::ImageVolume,
            true,
        );
        let c = image_volume_container(None);
        let m = mounts_with(
            &c,
            &image_host_paths(),
            &HashMap::new(),
            &HashMap::new(),
            false,
            false,
            &pulled_image(),
        )
        .unwrap();
        assert!(m[0].image.is_some());
        assert_eq!(m[0].image_sub_path, "");
    }

    /// `makeMounts` consults `imageVolumes` only when
    /// `features.ImageVolume` is enabled (`kubelet_pods.go:327`).
    #[test]
    #[serial_test::serial]
    fn the_image_volume_gate_off_ignores_pulled_images() {
        let _g = rusternetes_common::feature_gates::with_feature(
            rusternetes_common::feature_gates::Feature::ImageVolume,
            false,
        );
        let c = image_volume_container(None);
        let m = mounts_with(
            &c,
            &image_host_paths(),
            &HashMap::new(),
            &HashMap::new(),
            false,
            false,
            &pulled_image(),
        )
        .unwrap();
        assert_eq!(m[0].image, None);
        assert_eq!(m[0].image_sub_path, "");
    }

    /// The subPath guards run before the image arm (`kubelet_pods.go:317-325`
    /// precede `:327`), so an escaping subPath still fails.
    #[test]
    #[serial_test::serial]
    fn an_image_volume_sub_path_is_still_validated() {
        let _g = rusternetes_common::feature_gates::with_feature(
            rusternetes_common::feature_gates::Feature::ImageVolume,
            true,
        );
        let c = image_volume_container(Some("../escape"));
        assert!(mounts_with(
            &c,
            &image_host_paths(),
            &HashMap::new(),
            &HashMap::new(),
            false,
            false,
            &pulled_image(),
        )
        .is_err());
    }
}
