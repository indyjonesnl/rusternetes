//! Unit tests moved from `src/downward_api.rs`.
#![allow(unused_imports, dead_code)]

mod __parent {
    pub use rusternetes_common::quantity::Quantity;
    pub use rusternetes_common::resources::{Pod, ResourceFieldSelector};
    pub use rusternetes_common::validation::metav1::is_qualified_name;
}

mod tests {
    use super::__parent::*;
    use rusternetes_common::resources::{Container, Pod, PodSpec};
    use rusternetes_common::types::{ObjectMeta, ResourceRequirements, TypeMeta};
    use rusternetes_kubelet::downward_api::*;
    use std::collections::HashMap;

    fn make_pod() -> Pod {
        Pod {
            type_meta: TypeMeta {
                kind: "Pod".to_string(),
                api_version: "v1".to_string(),
            },
            metadata: ObjectMeta::new("p").with_namespace("ns"),
            spec: Some(PodSpec {
                containers: vec![Container {
                    name: "c".to_string(),
                    image: "x".to_string(),
                    ..Default::default()
                }],
                ..Default::default()
            }),
            status: None,
        }
    }

    #[test]
    fn metadata_name_resolves() {
        let p = make_pod();
        assert_eq!(resolve_pod_field(&p, "metadata.name").unwrap(), "p");
    }

    #[test]
    fn unknown_field_returns_unsupported_error() {
        let p = make_pod();
        let err = resolve_pod_field(&p, "spec.unknownField").unwrap_err();
        assert!(matches!(err, DownwardError::UnsupportedField(_)));
    }

    /// `TestExtractFieldPathAsString` (`pkg/fieldpath/fieldpath_test.go:51`):
    /// the volume side (`downwardapi.go:85`) reaches only
    /// `ExtractFieldPathAsString`, which has no `status.*` / `spec.*` cases —
    /// no loopback invention for `status.hostIP`.
    #[test]
    fn status_and_spec_paths_are_unsupported_for_volumes() {
        let p = make_pod();
        for path in [
            "status.hostIP",
            "status.hostIPs",
            "status.podIP",
            "spec.nodeName",
            "spec.serviceAccountName",
            "metadata.whoops",
        ] {
            let err = resolve_pod_field(&p, path).unwrap_err();
            assert_eq!(err.to_string(), format!("unsupported fieldPath: {path}"));
        }
    }

    #[test]
    fn namespace_is_returned_as_is() {
        let mut p = make_pod();
        p.metadata.namespace = None;
        assert_eq!(resolve_pod_field(&p, "metadata.namespace").unwrap(), "");
    }

    #[test]
    fn invalid_label_and_annotation_subscripts() {
        let mut p = make_pod();
        p.metadata.annotations = Some(HashMap::from([("Www.k8s.io/test".into(), "1".into())]));
        assert_eq!(
            resolve_pod_field(&p, "metadata.annotations['Www.k8s.io/test']").unwrap(),
            "1"
        );
        let e = resolve_pod_field(&p, "metadata.annotations['invalid~key']").unwrap_err();
        assert!(e
            .to_string()
            .contains("invalid key subscript in metadata.annotations['invalid~key']"));
        let e = resolve_pod_field(&p, "metadata.labels['Www.k8s.io/test']").unwrap_err();
        assert!(e
            .to_string()
            .contains("invalid key subscript in metadata.labels"));
        let e = resolve_pod_field(&p, "metadata.notexisting['something']").unwrap_err();
        assert_eq!(
            e.to_string(),
            "fieldPath \"metadata.notexisting['something']\" does not support subscript"
        );
        // Only the `['key']` form is a subscript; `["key"]` and unquoted are not.
        assert!(resolve_pod_field(&p, "metadata.labels[app]").is_err());
    }

    #[test]
    fn labels_subscript_resolves_value() {
        let mut p = make_pod();
        let mut labels = HashMap::new();
        labels.insert("app".to_string(), "web".to_string());
        p.metadata.labels = Some(labels);
        assert_eq!(
            resolve_pod_field(&p, "metadata.labels['app']").unwrap(),
            "web"
        );
    }

    /// The node's advertised `status.allocatable`, as
    /// `kubelet::node_allocatable_map` builds it.
    fn allocatable() -> HashMap<String, String> {
        HashMap::from([
            ("cpu".to_string(), "4".to_string()),
            ("memory".to_string(), "8Gi".to_string()),
            ("ephemeral-storage".to_string(), "100Gi".to_string()),
        ])
    }

    fn sel(resource: &str, divisor: Option<&str>) -> ResourceFieldSelector {
        ResourceFieldSelector {
            container_name: Some("c".into()),
            resource: resource.into(),
            divisor: divisor.map(str::to_string),
        }
    }

    fn with_resources(pod: &mut Pod, limits: &[(&str, &str)], requests: &[(&str, &str)]) {
        let to_map = |kv: &[(&str, &str)]| -> Option<HashMap<String, String>> {
            if kv.is_empty() {
                None
            } else {
                Some(
                    kv.iter()
                        .map(|(k, v)| (k.to_string(), v.to_string()))
                        .collect(),
                )
            }
        };
        if let Some(ref mut spec) = pod.spec {
            spec.containers[0].resources = Some(ResourceRequirements {
                limits: to_map(limits),
                requests: to_map(requests),
                claims: None,
            });
        }
    }

    #[test]
    fn limits_cpu_default_to_node_allocatable() {
        let p = make_pod();
        // 4000 millicores ÷ 1000 (cores divisor) = 4
        assert_eq!(
            resolve_container_resource(&p, &sel("limits.cpu", None), Some(&allocatable())).unwrap(),
            "4"
        );
    }

    /// `limits.ephemeral-storage` defaults from the node's allocatable like any
    /// other mergeable limit (`MergeContainerResourceLimits`,
    /// `pkg/api/v1/resource/helpers.go:196`), which lists cpu, memory **and**
    /// ephemeral-storage.
    ///
    /// The volume-side copy of this logic hardcoded an 8 GiB fallback for every
    /// byte-valued resource, so a downwardAPI *volume* file for
    /// `limits.ephemeral-storage` on a container with no explicit limit wrote
    /// `8589934592` while the *env var* for the very same selector — resolved by
    /// the other copy, which did consult the node — wrote `107374182400`.
    #[test]
    fn limits_ephemeral_storage_defaults_to_node_allocatable_not_8gi() {
        let p = make_pod();
        assert_eq!(
            resolve_container_resource(
                &p,
                &sel("limits.ephemeral-storage", None),
                Some(&allocatable())
            )
            .unwrap(),
            (100i64 * 1024 * 1024 * 1024).to_string()
        );
    }

    /// Every `convertResource*ToString` helper rounds **up**
    /// (`math.Ceil`, `pkg/api/v1/resource/helpers.go:146-170`). The env-var copy
    /// of this logic floored byte quantities (`bytes / div_bytes`), so a limit
    /// that is not a whole multiple of the divisor reported one unit short.
    ///
    /// 100M = 100_000_000 bytes; 100_000_000 / 1Mi = 95.367…, so upstream says
    /// 96 and the flooring copy said 95.
    #[test]
    fn memory_divisor_rounds_up_not_down() {
        let mut p = make_pod();
        with_resources(&mut p, &[("memory", "100M")], &[]);
        assert_eq!(
            resolve_container_resource(
                &p,
                &sel("limits.memory", Some("1Mi")),
                Some(&allocatable())
            )
            .unwrap(),
            "96"
        );
    }

    /// `findContainerInPod` (`pkg/api/v1/resource/helpers.go:172-185`) searches
    /// `Spec.Containers` **and then** `Spec.InitContainers`. Naming a sidecar or
    /// init container in `resourceFieldRef.containerName` resolves upstream; both
    /// kubelet copies searched only `spec.containers` and failed the pod.
    #[test]
    fn container_name_resolves_an_init_container() {
        let mut p = make_pod();
        if let Some(ref mut spec) = p.spec {
            spec.init_containers = Some(vec![Container {
                name: "init".to_string(),
                image: "x".to_string(),
                resources: Some(ResourceRequirements {
                    limits: Some(HashMap::from([("memory".to_string(), "64Mi".to_string())])),
                    requests: None,
                    claims: None,
                }),
                ..Default::default()
            }]);
        }
        let mut s = sel("limits.memory", None);
        s.container_name = Some("init".into());
        assert_eq!(
            resolve_container_resource(&p, &s, Some(&allocatable())).unwrap(),
            (64 * 1024 * 1024).to_string()
        );
    }

    /// `SetDefaults_Pod` (`pkg/apis/core/v1/defaults.go:164-180`) copies limits
    /// into unset requests at admission, so `requests.memory` on a limits-only
    /// container reports the limit. The volume-side copy had no such fallback and
    /// reported its hardcoded 8 GiB instead.
    #[test]
    fn unset_request_defaults_to_the_limit() {
        let mut p = make_pod();
        with_resources(&mut p, &[("memory", "64Mi")], &[]);
        assert_eq!(
            resolve_container_resource(&p, &sel("requests.memory", None), Some(&allocatable()))
                .unwrap(),
            (64 * 1024 * 1024).to_string()
        );
    }

    /// `MergeContainerResourceLimits` fills **limits** only (helpers.go:191-202).
    /// With neither a request nor a limit set, `Requests.Memory()` is the zero
    /// Quantity, so upstream renders `"0"` — not the node's allocatable.
    #[test]
    fn unset_request_without_a_limit_is_zero_not_allocatable() {
        let p = make_pod();
        assert_eq!(
            resolve_container_resource(&p, &sel("requests.memory", None), Some(&allocatable()))
                .unwrap(),
            "0"
        );
    }

    /// An unset `resource.Quantity` marshals as `"0"` on the wire, and
    /// helpers.go:106-111 maps that to a divisor of 1. `None`, `""` and `"0"`
    /// must therefore all mean "natural unit".
    #[test]
    fn zero_empty_and_absent_divisors_all_mean_one() {
        let mut p = make_pod();
        with_resources(&mut p, &[("memory", "64Mi")], &[]);
        let bytes = (64 * 1024 * 1024).to_string();
        for divisor in [None, Some(""), Some("0")] {
            assert_eq!(
                resolve_container_resource(
                    &p,
                    &sel("limits.memory", divisor),
                    Some(&allocatable())
                )
                .unwrap(),
                bytes,
                "divisor {divisor:?}"
            );
        }
    }

    /// helpers.go:129-141 accepts `hugepages-<size>` under either prefix and
    /// errors on anything else — it does not pass the raw string through.
    #[test]
    fn hugepages_divide_as_bytes_and_unknown_resources_error() {
        let mut p = make_pod();
        with_resources(&mut p, &[("hugepages-2Mi", "4Mi")], &[]);
        assert_eq!(
            resolve_container_resource(
                &p,
                &sel("limits.hugepages-2Mi", Some("1Mi")),
                Some(&allocatable())
            )
            .unwrap(),
            "4"
        );
        assert_eq!(
            resolve_container_resource(&p, &sel("limits.nvidia.com/gpu", None), None).unwrap_err(),
            DownwardError::UnsupportedResource("limits.nvidia.com/gpu".to_string())
        );
    }

    #[test]
    fn limits_memory_explicit_returns_bytes() {
        let mut p = make_pod();
        let mut limits = HashMap::new();
        limits.insert("memory".to_string(), "64Mi".to_string());
        if let Some(ref mut spec) = p.spec {
            spec.containers[0].resources = Some(ResourceRequirements {
                limits: Some(limits),
                requests: None,
                claims: None,
            });
        }
        // 64 MiB = 67_108_864 bytes
        assert_eq!(
            resolve_container_resource(&p, &sel("limits.memory", None), Some(&allocatable()))
                .unwrap(),
            (64 * 1024 * 1024).to_string()
        );
    }
}
