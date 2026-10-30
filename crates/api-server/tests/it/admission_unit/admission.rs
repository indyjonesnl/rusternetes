//! Unit tests moved from `src/admission.rs`.
#![allow(unused_imports, dead_code)]

mod __parent {
    pub use rusternetes_common::{
        quantity::{parse_resource_value, Quantity},
        resources::{LimitRange, Pod, ServiceAccount},
        types::ResourceRequirements,
    };
    pub use rusternetes_storage::Storage;
    pub use std::collections::{BTreeMap, HashMap};
    pub use std::sync::Arc;
    pub use tracing::{info, warn};
}

mod tests {
    use super::__parent::*;
    use rusternetes_api_server::admission::*;
    use rusternetes_common::quota;
    use rusternetes_common::types::ObjectMeta;

    fn make_pod(name: &str, cpu_request: Option<&str>, cpu_limit: Option<&str>) -> Pod {
        let mut resources = serde_json::Map::new();
        if let Some(cpu) = cpu_request {
            resources.insert("requests".to_string(), serde_json::json!({"cpu": cpu}));
        }
        if let Some(cpu) = cpu_limit {
            resources.insert("limits".to_string(), serde_json::json!({"cpu": cpu}));
        }
        let resources_json = if resources.is_empty() {
            serde_json::Value::Null
        } else {
            serde_json::Value::Object(resources)
        };
        let pod_json = serde_json::json!({
            "apiVersion": "v1",
            "kind": "Pod",
            "metadata": {"name": name},
            "spec": {
                "containers": [{
                    "name": "main",
                    "image": "busybox",
                    "resources": resources_json
                }]
            }
        });
        serde_json::from_value(pod_json).unwrap()
    }

    #[test]
    fn test_parse_cpu_to_millicores_various() {
        assert_eq!(parse_cpu_to_millicores("100m").unwrap(), 100);
        assert_eq!(parse_cpu_to_millicores("1").unwrap(), 1000);
        assert_eq!(parse_cpu_to_millicores("0.5").unwrap(), 500);
        assert_eq!(parse_cpu_to_millicores("250m").unwrap(), 250);
        assert_eq!(parse_cpu_to_millicores("2").unwrap(), 2000);
    }

    #[test]
    fn test_parse_memory_to_bytes_various() {
        assert_eq!(parse_memory_to_bytes("0").unwrap(), 0);
        assert_eq!(parse_memory_to_bytes("1024").unwrap(), 1024);
        assert_eq!(parse_memory_to_bytes("1Ki").unwrap(), 1024);
        assert_eq!(parse_memory_to_bytes("1Mi").unwrap(), 1024 * 1024);
        assert_eq!(parse_memory_to_bytes("1Gi").unwrap(), 1024 * 1024 * 1024);
    }

    /// A `ResourceQuota` or `LimitRange` may carry any quantity the API
    /// accepts. These all failed the old `trim_end_matches` chain, and in the
    /// quota path a failed parse means `i64::MAX` — the dimension is simply not
    /// enforced.
    #[test]
    fn parse_quantities_covers_full_grammar() {
        for (value, expected) in [
            ("1Ti", 1_099_511_627_776i64),
            ("1Pi", 1_125_899_906_842_624),
            ("1Ei", 1_152_921_504_606_846_976),
            ("1T", 1_000_000_000_000),
            ("1P", 1_000_000_000_000_000),
            ("1E", 1_000_000_000_000_000_000),
            ("129e6", 129_000_000),
            ("0.5", 1),
        ] {
            assert_eq!(
                parse_memory_to_bytes(value).unwrap_or_else(|e| panic!("{value}: {e}")),
                expected,
                "memory {value:?}"
            );
        }
        // Sub-unit CPU suffixes, rounded up as `MilliValue()` does.
        assert_eq!(parse_cpu_to_millicores("0.5m").unwrap(), 1);
        assert_eq!(parse_cpu_to_millicores("10.5m").unwrap(), 11);
        assert_eq!(parse_cpu_to_millicores("1500u").unwrap(), 2);
    }

    /// `k` is the kilo suffix; `K` is not in the grammar at all. The chain
    /// matched only `ends_with("K")`, so it had these exactly backwards.
    #[test]
    fn parse_memory_accepts_lowercase_k_and_rejects_uppercase() {
        assert_eq!(parse_memory_to_bytes("1k").unwrap(), 1_000);
        assert!(parse_memory_to_bytes("1K").is_err());
    }

    /// `trim_end_matches` strips *every* trailing occurrence of the suffix.
    #[test]
    fn parse_memory_rejects_repeated_suffix() {
        assert!(parse_memory_to_bytes("1GiGi").is_err());
        assert!(parse_memory_to_bytes("1MiMi").is_err());
        assert!(parse_cpu_to_millicores("100mm").is_err());
    }

    /// An unparseable `spec.hard` value leaves that one dimension unenforced
    /// without taking the rest of the quota with it. The old code reached the
    /// same outcome via a per-key `unwrap_or(i64::MAX)`; now there is one place
    /// where a bad quantity is dropped, and the surviving keys still bind.
    #[test]
    fn unparseable_hard_value_drops_only_its_own_dimension() {
        let mut hard = HashMap::new();
        hard.insert("requests.cpu".to_string(), "1".to_string());
        hard.insert("requests.memory".to_string(), "nonsense".to_string());
        let parsed = quota::parse_resource_list(&hard);
        assert_eq!(quota::resource_names(&parsed), vec!["requests.cpu"]);
    }

    // ---- LimitRange admission tests ----

    fn make_limit_range(
        default_cpu: Option<&str>,
        default_request_cpu: Option<&str>,
        min_cpu: Option<&str>,
        max_cpu: Option<&str>,
    ) -> LimitRange {
        let mut default = HashMap::new();
        if let Some(v) = default_cpu {
            default.insert("cpu".to_string(), v.to_string());
        }
        let mut default_request = HashMap::new();
        if let Some(v) = default_request_cpu {
            default_request.insert("cpu".to_string(), v.to_string());
        }
        let mut min = HashMap::new();
        if let Some(v) = min_cpu {
            min.insert("cpu".to_string(), v.to_string());
        }
        let mut max = HashMap::new();
        if let Some(v) = max_cpu {
            max.insert("cpu".to_string(), v.to_string());
        }
        LimitRange {
            type_meta: rusternetes_common::types::TypeMeta {
                api_version: "v1".to_string(),
                kind: "LimitRange".to_string(),
            },
            metadata: ObjectMeta::new("test-limit-range").with_namespace("default"),
            spec: rusternetes_common::resources::LimitRangeSpec {
                limits: vec![rusternetes_common::resources::LimitRangeItem {
                    item_type: "Container".to_string(),
                    default: if default.is_empty() {
                        None
                    } else {
                        Some(default)
                    },
                    default_request: if default_request.is_empty() {
                        None
                    } else {
                        Some(default_request)
                    },
                    min: if min.is_empty() { None } else { Some(min) },
                    max: if max.is_empty() { None } else { Some(max) },
                    max_limit_request_ratio: None,
                }],
            },
        }
    }

    #[tokio::test]
    async fn test_limit_range_applies_default_request_cpu() {
        // Conformance test scenario: LimitRange with default=500m, defaultRequest=300m
        // Pod with NO resources should get requests.cpu=300m, limits.cpu=500m
        let storage = Arc::new(rusternetes_storage::MemoryStorage::new());
        let lr = make_limit_range(Some("500m"), Some("300m"), Some("100m"), Some("1"));
        let lr_key = "/registry/limitranges/default/test-limit-range";
        storage.create(lr_key, &lr).await.unwrap();

        let mut pod = make_pod("test-pod", None, None);
        let result = apply_limit_range(&storage, "default", &mut pod)
            .await
            .unwrap();
        assert!(result, "LimitRange admission should pass");

        let resources = pod.spec.as_ref().unwrap().containers[0]
            .resources
            .as_ref()
            .unwrap();
        let requests = resources.requests.as_ref().expect("requests should be set");
        let limits = resources.limits.as_ref().expect("limits should be set");

        assert_eq!(
            requests.get("cpu").unwrap(),
            "300m",
            "requests.cpu should be 300m from defaultRequest, not from default limits"
        );
        assert_eq!(
            limits.get("cpu").unwrap(),
            "500m",
            "limits.cpu should be 500m from default"
        );
    }

    #[tokio::test]
    async fn test_limit_range_requests_fallback_to_limits_when_no_default_request() {
        // When defaultRequest is NOT set but default (limits) IS set,
        // requests should default to the limit value
        let storage = Arc::new(rusternetes_storage::MemoryStorage::new());
        let lr = make_limit_range(Some("500m"), None, None, None);
        let lr_key = "/registry/limitranges/default/test-limit-range";
        storage.create(lr_key, &lr).await.unwrap();

        let mut pod = make_pod("test-pod", None, None);
        let result = apply_limit_range(&storage, "default", &mut pod)
            .await
            .unwrap();
        assert!(result);

        let resources = pod.spec.as_ref().unwrap().containers[0]
            .resources
            .as_ref()
            .unwrap();
        let requests = resources.requests.as_ref().expect("requests should be set");
        let limits = resources.limits.as_ref().expect("limits should be set");

        assert_eq!(limits.get("cpu").unwrap(), "500m");
        assert_eq!(
            requests.get("cpu").unwrap(),
            "500m",
            "requests.cpu should fall back to default limits (500m) when defaultRequest not set"
        );
    }

    #[tokio::test]
    async fn test_limit_range_does_not_override_explicit_requests() {
        // Container with explicit requests.cpu=200m should NOT be overridden by defaultRequest
        let storage = Arc::new(rusternetes_storage::MemoryStorage::new());
        let lr = make_limit_range(Some("500m"), Some("300m"), Some("100m"), Some("1"));
        let lr_key = "/registry/limitranges/default/test-limit-range";
        storage.create(lr_key, &lr).await.unwrap();

        let mut pod = make_pod("test-pod", Some("200m"), None);
        let result = apply_limit_range(&storage, "default", &mut pod)
            .await
            .unwrap();
        assert!(result);

        let resources = pod.spec.as_ref().unwrap().containers[0]
            .resources
            .as_ref()
            .unwrap();
        let requests = resources.requests.as_ref().expect("requests should be set");
        assert_eq!(
            requests.get("cpu").unwrap(),
            "200m",
            "explicit requests.cpu=200m should not be overridden by defaultRequest=300m"
        );
    }

    #[tokio::test]
    async fn test_limit_range_limits_default_to_requests_when_unset() {
        // K8s rule: if limits are set (from LimitRange default) but container has no requests,
        // requests should default to the limits value
        let storage = Arc::new(rusternetes_storage::MemoryStorage::new());
        // Only default limits, no defaultRequest
        let lr = make_limit_range(Some("400m"), None, None, None);
        let lr_key = "/registry/limitranges/default/test-limit-range";
        storage.create(lr_key, &lr).await.unwrap();

        let mut pod = make_pod("test-pod", None, None);
        let result = apply_limit_range(&storage, "default", &mut pod)
            .await
            .unwrap();
        assert!(result);

        let resources = pod.spec.as_ref().unwrap().containers[0]
            .resources
            .as_ref()
            .unwrap();
        let requests = resources.requests.as_ref().expect("requests should be set");
        let limits = resources.limits.as_ref().expect("limits should be set");

        assert_eq!(limits.get("cpu").unwrap(), "400m");
        assert_eq!(
            requests.get("cpu").unwrap(),
            "400m",
            "requests.cpu should default to limits.cpu when no defaultRequest"
        );
    }

    #[tokio::test]
    async fn test_limit_range_explicit_limits_override_default_request() {
        // Conformance scenario: pod has explicit limits.cpu=300m but no requests.cpu.
        // LimitRange has default=500m, defaultRequest=100m.
        // The pod has explicit limits.cpu=300m but no requests.cpu.
        // apply_limit_range only handles LimitRange defaults — the pod-level
        // limits→requests defaulting happens in the pod handler BEFORE this.
        // So apply_limit_range should set requests.cpu = 100m (from defaultRequest).
        let storage = Arc::new(rusternetes_storage::MemoryStorage::new());
        let lr = make_limit_range(Some("500m"), Some("100m"), Some("50m"), Some("1"));
        let lr_key = "/registry/limitranges/default/test-limit-range";
        storage.create(lr_key, &lr).await.unwrap();

        let mut pod = make_pod("test-pod", None, Some("300m"));
        let result = apply_limit_range(&storage, "default", &mut pod)
            .await
            .unwrap();
        assert!(result);

        let resources = pod.spec.as_ref().unwrap().containers[0]
            .resources
            .as_ref()
            .unwrap();
        let requests = resources.requests.as_ref().expect("requests should be set");
        let limits = resources.limits.as_ref().expect("limits should be set");

        assert_eq!(
            limits.get("cpu").unwrap(),
            "300m",
            "explicit limits.cpu=300m should be preserved"
        );
        // Note: the pod handler does limits→requests defaulting BEFORE calling
        // apply_limit_range, so in production requests.cpu=300m. But this unit
        // test only calls apply_limit_range, which applies defaultRequest=100m.
        assert_eq!(
            requests.get("cpu").unwrap(),
            "100m",
            "apply_limit_range sets requests from defaultRequest (pod handler does limits->requests)"
        );
    }

    #[test]
    fn test_validate_max_rejects_over_limit_cpu() {
        let resources = ResourceRequirements {
            limits: Some({
                let mut m = HashMap::new();
                m.insert("cpu".to_string(), "800m".to_string());
                m
            }),
            requests: None,
            claims: None,
        };
        let max = {
            let mut m = HashMap::new();
            m.insert("cpu".to_string(), "500m".to_string());
            m
        };
        let result = validate_max_resources(&resources, &max, "test").unwrap();
        assert!(!result, "800m CPU should exceed max of 500m");
    }

    #[test]
    fn test_validate_max_rejects_over_limit_memory() {
        let resources = ResourceRequirements {
            limits: Some({
                let mut m = HashMap::new();
                m.insert("memory".to_string(), "1Gi".to_string());
                m
            }),
            requests: None,
            claims: None,
        };
        let max = {
            let mut m = HashMap::new();
            m.insert("memory".to_string(), "500Mi".to_string());
            m
        };
        let result = validate_max_resources(&resources, &max, "test").unwrap();
        assert!(!result, "1Gi memory should exceed max of 500Mi");
    }

    #[test]
    fn test_validate_max_rejects_over_limit_ephemeral_storage() {
        let resources = ResourceRequirements {
            limits: Some({
                let mut m = HashMap::new();
                m.insert("ephemeral-storage".to_string(), "2Gi".to_string());
                m
            }),
            requests: None,
            claims: None,
        };
        let max = {
            let mut m = HashMap::new();
            m.insert("ephemeral-storage".to_string(), "1Gi".to_string());
            m
        };
        let result = validate_max_resources(&resources, &max, "test").unwrap();
        assert!(!result, "2Gi ephemeral-storage should exceed max of 1Gi");
    }

    #[test]
    fn test_validate_max_checks_requests_too() {
        let resources = ResourceRequirements {
            limits: None,
            requests: Some({
                let mut m = HashMap::new();
                m.insert("cpu".to_string(), "800m".to_string());
                m
            }),
            claims: None,
        };
        let max = {
            let mut m = HashMap::new();
            m.insert("cpu".to_string(), "500m".to_string());
            m
        };
        let result = validate_max_resources(&resources, &max, "test").unwrap();
        assert!(!result, "800m CPU request should exceed max of 500m");
    }

    #[test]
    fn test_validate_max_allows_within_limit() {
        let resources = ResourceRequirements {
            limits: Some({
                let mut m = HashMap::new();
                m.insert("cpu".to_string(), "400m".to_string());
                m
            }),
            requests: None,
            claims: None,
        };
        let max = {
            let mut m = HashMap::new();
            m.insert("cpu".to_string(), "500m".to_string());
            m
        };
        let result = validate_max_resources(&resources, &max, "test").unwrap();
        assert!(result, "400m CPU should be within max of 500m");
    }

    // ----- PodSecurityAdmission allow-case coverage -----
    //
    // The reject paths are pinned by the HTTP-level tests in
    // `tests/pod_security_admission_test.rs`. These unit tests cover the
    // admit (allow) paths the integration tests don't assert.

    async fn put_namespace<S: Storage>(storage: &Arc<S>, name: &str, enforce: Option<&str>) {
        let mut labels = std::collections::BTreeMap::new();
        if let Some(level) = enforce {
            labels.insert(
                "pod-security.kubernetes.io/enforce".to_string(),
                level.to_string(),
            );
        }
        let ns = serde_json::json!({
            "apiVersion": "v1",
            "kind": "Namespace",
            "metadata": { "name": name, "labels": labels },
        });
        let ns: rusternetes_common::resources::Namespace = serde_json::from_value(ns).unwrap();
        let key = rusternetes_storage::build_key("namespaces", None, name);
        storage.create(&key, &ns).await.unwrap();
    }

    fn pod_from_spec(name: &str, spec: serde_json::Value) -> Pod {
        serde_json::from_value(serde_json::json!({
            "apiVersion": "v1",
            "kind": "Pod",
            "metadata": { "name": name },
            "spec": spec,
        }))
        .unwrap()
    }

    #[tokio::test]
    async fn psa_privileged_namespace_allows_privileged_pod() {
        let storage = Arc::new(rusternetes_storage::MemoryStorage::new());
        put_namespace(&storage, "ns", Some("privileged")).await;
        let pod = pod_from_spec(
            "p",
            serde_json::json!({
                "hostPID": true,
                "containers": [{
                    "name": "main", "image": "busybox",
                    "securityContext": { "privileged": true },
                }],
            }),
        );
        PodSecurityAdmission::new()
            .admit_as(&storage, "ns", &pod, "")
            .await
            .expect("privileged namespace must admit everything");
    }

    fn psa_pod(spec: serde_json::Value) -> Pod {
        serde_json::from_value(serde_json::json!({
            "apiVersion": "v1", "kind": "Pod",
            "metadata": {"name": "p"}, "spec": spec
        }))
        .unwrap()
    }

    /// admission_test.go `TestValidatePodAndController` update cases /
    /// `isSignificantPodUpdate` (admission.go:632-666).
    #[test]
    fn psa_is_significant_pod_update() {
        let base = psa_pod(serde_json::json!({
            "containers": [{"name": "a", "image": "i1"}],
            "initContainers": [{"name": "i", "image": "j1"}]
        }));
        assert!(!is_significant_pod_update(&base, &base));
        let image = psa_pod(serde_json::json!({
            "containers": [{"name": "a", "image": "i2"}],
            "initContainers": [{"name": "i", "image": "j1"}]
        }));
        assert!(is_significant_pod_update(&image, &base));
        let init_image = psa_pod(serde_json::json!({
            "containers": [{"name": "a", "image": "i1"}],
            "initContainers": [{"name": "i", "image": "j2"}]
        }));
        assert!(is_significant_pod_update(&init_image, &base));
        let added = psa_pod(serde_json::json!({
            "containers": [{"name": "a", "image": "i1"}, {"name": "b", "image": "i1"}],
            "initContainers": [{"name": "i", "image": "j1"}]
        }));
        assert!(is_significant_pod_update(&added, &base));
        let eph = psa_pod(serde_json::json!({
            "containers": [{"name": "a", "image": "i1"}],
            "initContainers": [{"name": "i", "image": "j1"}],
            "ephemeralContainers": [{"name": "e", "image": "x"}]
        }));
        assert!(is_significant_pod_update(&eph, &base));
        assert!(!is_significant_pod_update(&eph, &eph));
        let eph_image = psa_pod(serde_json::json!({
            "containers": [{"name": "a", "image": "i1"}],
            "initContainers": [{"name": "i", "image": "j1"}],
            "ephemeralContainers": [{"name": "e", "image": "y"}]
        }));
        assert!(is_significant_pod_update(&eph_image, &eph));
    }

    /// `ignoredPodSubresources` (admission.go:316-325).
    #[test]
    fn psa_ignored_subresources_are_not_evaluated() {
        let pod = psa_pod(serde_json::json!({"containers": [{"name": "a", "image": "i"}]}));
        for s in IGNORED_POD_SUBRESOURCES {
            assert!(!PodSecurityAdmission::should_evaluate(Some(s), None, &pod));
        }
        for s in [None, Some("ephemeralcontainers"), Some("resize")] {
            assert!(PodSecurityAdmission::should_evaluate(s, None, &pod));
        }
        // An insignificant update is allowed unevaluated.
        assert!(!PodSecurityAdmission::should_evaluate(
            None,
            Some(&pod),
            &pod
        ));
    }

    #[tokio::test]
    async fn psa_missing_enforce_label_allows() {
        let storage = Arc::new(rusternetes_storage::MemoryStorage::new());
        put_namespace(&storage, "ns", None).await;
        let pod = pod_from_spec(
            "p",
            serde_json::json!({
                "containers": [{
                    "name": "main", "image": "busybox",
                    "securityContext": { "privileged": true },
                }],
            }),
        );
        PodSecurityAdmission::new()
            .admit_as(&storage, "ns", &pod, "")
            .await
            .expect("absent enforce label must admit everything");
    }

    #[tokio::test]
    async fn psa_restricted_admits_compliant_pod() {
        let storage = Arc::new(rusternetes_storage::MemoryStorage::new());
        put_namespace(&storage, "ns", Some("restricted")).await;
        let pod = pod_from_spec(
            "p",
            serde_json::json!({
                "securityContext": {
                    "runAsNonRoot": true,
                    "seccompProfile": { "type": "RuntimeDefault" },
                },
                "volumes": [{ "name": "data", "emptyDir": {} }],
                "containers": [{
                    "name": "main", "image": "busybox",
                    "securityContext": {
                        "runAsNonRoot": true,
                        "runAsUser": 1000,
                        "allowPrivilegeEscalation": false,
                        "capabilities": { "drop": ["ALL"] },
                    },
                }],
            }),
        );
        PodSecurityAdmission::new()
            .admit_as(&storage, "ns", &pod, "")
            .await
            .expect("compliant restricted pod must be admitted");
    }

    // ---- PodSecurity exemptions (admission_test.go TestValidatePodAndController
    // "exempt namespace" / "exempt user" / "exempt runtimeClass") ----

    fn exemptions() -> PodSecurityExemptions {
        PodSecurityExemptions {
            usernames: vec!["exempt-user".into()],
            namespaces: vec!["exempt-ns".into()],
            runtime_classes: vec!["exempt-rc".into()],
        }
    }

    fn privileged_pod(runtime_class: Option<&str>) -> Pod {
        let mut spec = serde_json::json!({
            "containers": [{
                "name": "main", "image": "busybox",
                "securityContext": { "privileged": true },
            }],
        });
        if let Some(rc) = runtime_class {
            spec["runtimeClassName"] = serde_json::json!(rc);
        }
        pod_from_spec("p", spec)
    }

    #[tokio::test]
    async fn psa_exempt_namespace_admits_violating_pod() {
        let storage = Arc::new(rusternetes_storage::MemoryStorage::new());
        put_namespace(&storage, "exempt-ns", Some("restricted")).await;
        put_namespace(&storage, "other-ns", Some("restricted")).await;
        let psa = PodSecurityAdmission::with_exemptions(exemptions());
        psa.admit_as(&storage, "exempt-ns", &privileged_pod(None), "alice")
            .await
            .expect("exempt namespace admits");
        psa.admit_as(&storage, "other-ns", &privileged_pod(None), "alice")
            .await
            .expect_err("non-exempt namespace still enforced");
    }

    #[tokio::test]
    async fn psa_exempt_user_admits_violating_pod() {
        let storage = Arc::new(rusternetes_storage::MemoryStorage::new());
        put_namespace(&storage, "ns", Some("restricted")).await;
        let psa = PodSecurityAdmission::with_exemptions(exemptions());
        psa.admit_as(&storage, "ns", &privileged_pod(None), "exempt-user")
            .await
            .expect("exempt user admits");
        psa.admit_as(&storage, "ns", &privileged_pod(None), "alice")
            .await
            .expect_err("non-exempt user still enforced");
        psa.admit_as(&storage, "ns", &privileged_pod(None), "")
            .await
            .expect_err("empty username is never exempt");
    }

    #[tokio::test]
    async fn psa_exempt_runtime_class_admits_violating_pod() {
        let storage = Arc::new(rusternetes_storage::MemoryStorage::new());
        put_namespace(&storage, "ns", Some("restricted")).await;
        let psa = PodSecurityAdmission::with_exemptions(exemptions());
        psa.admit_as(&storage, "ns", &privileged_pod(Some("exempt-rc")), "alice")
            .await
            .expect("exempt runtimeClass admits");
        psa.admit_as(&storage, "ns", &privileged_pod(Some("other-rc")), "alice")
            .await
            .expect_err("other runtimeClass still enforced");
    }

    /// Upstream `GetAdmissionPluginConfigurationFor`
    /// (apiserver/pkg/admission/config.go): a plugin entry with `path`
    /// reads the file, relative to the config file's directory.
    #[test]
    fn psa_exemptions_from_plugin_path_relative_toconfig_file() {
        let dir = std::env::temp_dir().join(format!("psa-path-{}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        std::fs::write(
            dir.join("psa.yaml"),
            "apiVersion: pod-security.admission.config.k8s.io/v1\nkind: PodSecurityConfiguration\nexemptions:\n  usernames: [\"u\"]\n",
        )
        .unwrap();
        let yaml = "apiVersion: apiserver.config.k8s.io/v1\nkind: AdmissionConfiguration\nplugins:\n- name: PodSecurity\n  path: psa.yaml\n";
        let e = PodSecurityExemptions::from_admission_configuration(yaml, &dir.join("adm.yaml"))
            .unwrap();
        assert_eq!(e.usernames, vec!["u"]);
        // Inline `configuration` wins over `path` (config.go:127-135).
        let both =
            format!("{yaml}  configuration:\n    exemptions:\n      usernames: [\"inline\"]\n");
        let e = PodSecurityExemptions::from_admission_configuration(&both, &dir.join("adm.yaml"))
            .unwrap();
        assert_eq!(e.usernames, vec!["inline"]);
        // An unreadable path is an error naming it.
        let missing = yaml.replace("psa.yaml", "nope.yaml");
        let err =
            PodSecurityExemptions::from_admission_configuration(&missing, &dir.join("adm.yaml"))
                .unwrap_err();
        assert!(
            err.contains("Couldn't open admission plugin configuration"),
            "{err}"
        );
    }

    #[test]
    fn psa_exemptions_parse_from_admission_configuration() {
        let yaml = r#"
apiVersion: apiserver.config.k8s.io/v1
kind: AdmissionConfiguration
plugins:
- name: PodSecurity
  configuration:
    apiVersion: pod-security.admission.config.k8s.io/v1
    kind: PodSecurityConfiguration
    defaults:
      enforce: baseline
    exemptions:
      usernames: ["u"]
      namespaces: ["n1", "n2"]
      runtimeClasses: ["rc"]
"#;
        let e = PodSecurityExemptions::from_admission_configuration(
            yaml,
            std::path::Path::new("c.yaml"),
        )
        .unwrap();
        assert_eq!(e.usernames, vec!["u"]);
        assert_eq!(e.namespaces, vec!["n1", "n2"]);
        assert_eq!(e.runtime_classes, vec!["rc"]);
        assert_eq!(
            PodSecurityExemptions::from_admission_configuration(
                "plugins: []",
                std::path::Path::new("c.yaml")
            )
            .unwrap(),
            PodSecurityExemptions::default()
        );
    }

    // ---- PodSecurity audit / warn modes and label parsing ----
    // admission_test.go `TestValidatePodAndController` cases "enforce deny",
    // "warn deny", "audit deny", "invalid namespace labels", exempt and
    // privileged short-circuits (annotation keys).

    /// `makeNs(enforce, warn, audit)` (admission_test.go).
    async fn put_ns_modes(
        storage: &Arc<rusternetes_storage::MemoryStorage>,
        name: &str,
        enforce: &str,
        warn: &str,
        audit: &str,
    ) {
        let mut labels = std::collections::BTreeMap::new();
        for (k, v) in [("enforce", enforce), ("warn", warn), ("audit", audit)] {
            if !v.is_empty() {
                labels.insert(format!("pod-security.kubernetes.io/{k}"), v.to_string());
            }
        }
        let ns: rusternetes_common::resources::Namespace =
            serde_json::from_value(serde_json::json!({"apiVersion": "v1", "kind": "Namespace",
                "metadata": {"name": name, "labels": labels}}))
            .unwrap();
        let key = rusternetes_storage::build_key("namespaces", None, name);
        storage.create(&key, &ns).await.unwrap();
    }

    fn baseline_pod() -> Pod {
        pod_from_spec(
            "p",
            serde_json::json!({"containers": [{"name": "main", "image": "busybox"}]}),
        )
    }

    #[tokio::test]
    async fn psa_warn_deny_allows_with_warning() {
        let storage = Arc::new(rusternetes_storage::MemoryStorage::new());
        put_ns_modes(&storage, "warn-ns", "", "baseline", "").await;
        let out = PodSecurityAdmission::new()
            .admit_outcome(&storage, "warn-ns", &privileged_pod(None), "alice")
            .await
            .expect("warn mode never denies");
        assert_eq!(out.warnings.len(), 1, "{:?}", out.warnings);
        assert!(
            out.warnings[0].starts_with(r#"would violate PodSecurity "baseline:latest": "#),
            "{}",
            out.warnings[0]
        );
        assert!(!out.audit_annotations.contains_key("audit-violations"));
    }

    #[tokio::test]
    async fn psa_audit_deny_allows_with_annotation() {
        let storage = Arc::new(rusternetes_storage::MemoryStorage::new());
        put_ns_modes(&storage, "audit-ns", "", "", "baseline").await;
        let out = PodSecurityAdmission::new()
            .admit_outcome(&storage, "audit-ns", &privileged_pod(None), "alice")
            .await
            .expect("audit mode never denies");
        assert!(out.warnings.is_empty());
        let v = &out.audit_annotations["audit-violations"];
        assert!(
            v.starts_with(r#"would violate PodSecurity "baseline:latest": "#),
            "{v}"
        );
    }

    /// "enforce deny": no warning on a request already rejected; the audit
    /// annotation records the enforced policy.
    #[tokio::test]
    async fn psa_enforce_deny_message_names_level_and_version() {
        let storage = Arc::new(rusternetes_storage::MemoryStorage::new());
        put_ns_modes(&storage, "r-ns", "restricted", "", "").await;
        let err = PodSecurityAdmission::new()
            .admit_outcome(&storage, "r-ns", &privileged_pod(None), "alice")
            .await
            .expect_err("enforce denies");
        assert!(
            err.to_string()
                .contains(r#"violates PodSecurity "restricted:latest": "#),
            "{err}"
        );
    }

    /// "enforce allow" records `enforce-policy`; the privileged
    /// short-circuit is `privileged:latest` (response.go init).
    #[tokio::test]
    async fn psa_enforce_policy_audit_annotation() {
        let storage = Arc::new(rusternetes_storage::MemoryStorage::new());
        put_ns_modes(&storage, "b-ns", "baseline", "", "").await;
        put_ns_modes(&storage, "p-ns", "", "", "").await;
        let psa = PodSecurityAdmission::new();
        let out = psa
            .admit_outcome(&storage, "b-ns", &baseline_pod(), "alice")
            .await
            .unwrap();
        assert_eq!(out.audit_annotations["enforce-policy"], "baseline:latest");
        assert!(out.warnings.is_empty());
        let out = psa
            .admit_outcome(&storage, "p-ns", &privileged_pod(None), "alice")
            .await
            .unwrap();
        assert_eq!(out.audit_annotations["enforce-policy"], "privileged:latest");
    }

    /// "invalid namespace labels": an unparseable enforce level is
    /// restricted:latest, so even a baseline pod is denied, and the parse
    /// error is annotated (EvaluatePod :466-470).
    #[tokio::test]
    async fn psa_invalid_enforce_label_fails_closed() {
        let storage = Arc::new(rusternetes_storage::MemoryStorage::new());
        put_ns_modes(&storage, "invalid-ns", "not-a-valid-level", "", "").await;
        let err = PodSecurityAdmission::new()
            .admit_outcome(&storage, "invalid-ns", &baseline_pod(), "alice")
            .await
            .expect_err("restricted:latest denies a pod without runAsNonRoot");
        assert!(err.to_string().contains("restricted:latest"), "{err}");
    }

    /// An unparseable audit/warn level fails open and is annotated.
    #[tokio::test]
    async fn psa_invalid_warn_label_fails_open_with_error_annotation() {
        let storage = Arc::new(rusternetes_storage::MemoryStorage::new());
        put_ns_modes(&storage, "w-ns", "", "bogus", "").await;
        let out = PodSecurityAdmission::new()
            .admit_outcome(&storage, "w-ns", &privileged_pod(None), "alice")
            .await
            .unwrap();
        assert!(out.warnings.is_empty());
        assert!(
            out.audit_annotations["error"].starts_with("Failed to parse policy: "),
            "{:?}",
            out.audit_annotations
        );
    }

    /// Enforce also defaults warn to the enforce level, but a request that is
    /// already denied carries no warning.
    #[tokio::test]
    async fn psa_enforce_defaults_warn_level() {
        let storage = Arc::new(rusternetes_storage::MemoryStorage::new());
        put_ns_modes(&storage, "e-ns", "baseline", "", "").await;
        // restricted audit sees the baseline pod's missing runAsNonRoot.
        put_ns_modes(&storage, "ea-ns", "baseline", "", "restricted").await;
        let out = PodSecurityAdmission::new()
            .admit_outcome(&storage, "ea-ns", &baseline_pod(), "alice")
            .await
            .unwrap();
        assert!(out.audit_annotations.contains_key("audit-violations"));
        assert!(out.warnings.is_empty(), "warn defaults to baseline: passes");
    }

    /// Exemptions annotate the reason (response.go init).
    #[tokio::test]
    async fn psa_exemption_audit_annotation() {
        let storage = Arc::new(rusternetes_storage::MemoryStorage::new());
        put_ns_modes(&storage, "ns", "restricted", "", "").await;
        let psa = PodSecurityAdmission::with_exemptions(exemptions());
        let by = |ns: &'static str, user: &'static str, rc: Option<&'static str>| {
            let psa = psa.clone();
            let storage = storage.clone();
            async move {
                psa.admit_outcome(&storage, ns, &privileged_pod(rc), user)
                    .await
                    .unwrap()
                    .audit_annotations["exempt"]
                    .clone()
            }
        };
        assert_eq!(by("exempt-ns", "alice", None).await, "namespace");
        assert_eq!(by("ns", "exempt-user", None).await, "user");
        assert_eq!(by("ns", "alice", Some("exempt-rc")).await, "runtimeClass");
    }

    // ---- imagePullSecrets propagation (SA admission, upstream parity) -------

    async fn put_sa<S: Storage>(storage: &Arc<S>, ns: &str, name: &str, secrets: &[&str]) {
        let sa: ServiceAccount = serde_json::from_value(serde_json::json!({
            "apiVersion": "v1",
            "kind": "ServiceAccount",
            "metadata": {"name": name, "namespace": ns},
            "imagePullSecrets": secrets.iter().map(|s| serde_json::json!({"name": s})).collect::<Vec<_>>(),
        }))
        .unwrap();
        let key = format!("/registry/serviceaccounts/{}/{}", ns, name);
        storage.create(&key, &sa).await.unwrap();
    }

    fn pull_secret_names(pod: &Pod) -> Vec<String> {
        pod.spec
            .as_ref()
            .and_then(|s| s.image_pull_secrets.as_ref())
            .map(|v| v.iter().map(|r| r.name.clone()).collect())
            .unwrap_or_default()
    }

    #[tokio::test]
    async fn imagepullsecrets_propagated_from_sa_when_pod_has_none() {
        let storage = Arc::new(rusternetes_storage::MemoryStorage::new());
        put_sa(&storage, "ns", "default", &["regcred", "ghcr"]).await;
        let mut pod = make_pod("p", None, None);
        inject_service_account_token(&storage, "ns", &mut pod)
            .await
            .unwrap();
        assert_eq!(pull_secret_names(&pod), vec!["regcred", "ghcr"]);
    }

    #[tokio::test]
    async fn imagepullsecrets_pod_list_wins_no_merge() {
        let storage = Arc::new(rusternetes_storage::MemoryStorage::new());
        put_sa(&storage, "ns", "default", &["regcred"]).await;
        let mut pod = make_pod("p", None, None);
        // Pod already declares its own secret — SA list must NOT be appended.
        pod.spec.as_mut().unwrap().image_pull_secrets = Some(vec![
            rusternetes_common::resources::pod::LocalObjectReference {
                name: "pod-own".to_string(),
            },
        ]);
        inject_service_account_token(&storage, "ns", &mut pod)
            .await
            .unwrap();
        assert_eq!(pull_secret_names(&pod), vec!["pod-own"]);
    }

    #[tokio::test]
    async fn imagepullsecrets_noop_when_sa_has_none() {
        let storage = Arc::new(rusternetes_storage::MemoryStorage::new());
        put_sa(&storage, "ns", "default", &[]).await;
        let mut pod = make_pod("p", None, None);
        inject_service_account_token(&storage, "ns", &mut pod)
            .await
            .unwrap();
        assert!(pull_secret_names(&pod).is_empty());
    }

    #[tokio::test]
    async fn imagepullsecrets_propagated_even_when_automount_disabled() {
        let storage = Arc::new(rusternetes_storage::MemoryStorage::new());
        put_sa(&storage, "ns", "default", &["regcred"]).await;
        let mut pod = make_pod("p", None, None);
        // automount off must not block imagePullSecrets propagation.
        pod.spec.as_mut().unwrap().automount_service_account_token = Some(false);
        inject_service_account_token(&storage, "ns", &mut pod)
            .await
            .unwrap();
        assert_eq!(pull_secret_names(&pod), vec!["regcred"]);
    }

    // --- #2493: versioned policy checks + namespace lookup failure ---

    async fn put_ns_labels<S: Storage>(storage: &Arc<S>, name: &str, labels: &[(&str, &str)]) {
        let labels: std::collections::BTreeMap<String, String> = labels
            .iter()
            .map(|(k, v)| (k.to_string(), v.to_string()))
            .collect();
        let ns: rusternetes_common::resources::Namespace =
            serde_json::from_value(serde_json::json!({
                "apiVersion": "v1", "kind": "Namespace",
                "metadata": { "name": name, "labels": labels },
            }))
            .unwrap();
        let key = rusternetes_storage::build_key("namespaces", None, name);
        storage.create(&key, &ns).await.unwrap();
    }

    /// policy/check_sysctls.go:70-79: `net.ipv4.tcp_rmem` joined the safe
    /// set in v1.32, so `baseline:v1.31` forbids it and `latest` allows it.
    #[tokio::test]
    async fn psa_enforce_version_label_selects_older_sysctls_check() {
        let storage = Arc::new(rusternetes_storage::MemoryStorage::new());
        put_ns_labels(
            &storage,
            "old",
            &[
                ("pod-security.kubernetes.io/enforce", "baseline"),
                ("pod-security.kubernetes.io/enforce-version", "v1.31"),
            ],
        )
        .await;
        put_ns_labels(
            &storage,
            "new",
            &[("pod-security.kubernetes.io/enforce", "baseline")],
        )
        .await;
        let pod = psa_pod(serde_json::json!({
            "securityContext": {"sysctls": [{"name": "net.ipv4.tcp_rmem", "value": "1 2 3"}]},
            "containers": [{"name": "a", "image": "i"}],
        }));
        let psa = PodSecurityAdmission::new();
        let err = psa
            .admit_outcome(&storage, "old", &pod, "alice")
            .await
            .expect_err("v1.31 forbids tcp_rmem");
        assert!(
            err.to_string().contains(
                r#"violates PodSecurity "baseline:v1.31": forbidden sysctls (net.ipv4.tcp_rmem)"#
            ),
            "{err}"
        );
        psa.admit_outcome(&storage, "new", &pod, "alice")
            .await
            .expect("latest allows tcp_rmem");
    }

    /// policy/check_seccompProfile_restricted.go:54 and
    /// check_capabilities_restricted.go:65: both restricted checks start at
    /// v1.19 / v1.22, so a pod that sets neither passes `restricted:v1.18`.
    #[tokio::test]
    async fn psa_restricted_old_version_lacks_newer_checks() {
        let storage = Arc::new(rusternetes_storage::MemoryStorage::new());
        put_ns_labels(
            &storage,
            "old",
            &[
                ("pod-security.kubernetes.io/enforce", "restricted"),
                ("pod-security.kubernetes.io/enforce-version", "v1.18"),
            ],
        )
        .await;
        let pod = psa_pod(serde_json::json!({
            "securityContext": {"runAsNonRoot": true},
            "containers": [{"name": "a", "image": "i",
                "securityContext": {"allowPrivilegeEscalation": false}}],
        }));
        PodSecurityAdmission::new()
            .admit_outcome(&storage, "old", &pod, "alice")
            .await
            .expect("restricted:v1.18 predates seccomp and capabilities checks");
    }

    /// The enforce message lists EVERY failing check, baseline first, as
    /// `AggregateCheckResult.ForbiddenDetail` does (checks.go:98-117).
    #[tokio::test]
    async fn psa_enforce_message_aggregates_all_violations() {
        let storage = Arc::new(rusternetes_storage::MemoryStorage::new());
        put_ns_labels(
            &storage,
            "ns",
            &[("pod-security.kubernetes.io/enforce", "baseline")],
        )
        .await;
        let pod = psa_pod(serde_json::json!({
            "hostNetwork": true,
            "containers": [{"name": "a", "image": "i", "securityContext": {"privileged": true},
                "ports": [{"containerPort": 80, "hostPort": 80}]}],
        }));
        let err = PodSecurityAdmission::new()
            .admit_outcome(&storage, "ns", &pod, "alice")
            .await
            .unwrap_err()
            .to_string();
        assert!(
            err.contains(r#"host namespaces (hostNetwork=true), hostPort (container "a" uses hostPort 80), privileged (container "a" must not set securityContext.privileged=true)"#),
            "{err}"
        );
    }

    /// admission.go:344-350: a namespace that cannot be read is an
    /// InternalError, not an allow.
    #[tokio::test]
    async fn psa_namespace_lookup_failure_is_internal_error() {
        let storage = Arc::new(rusternetes_storage::MemoryStorage::new());
        // An unreadable (undecodable) Namespace record: a read error that is
        // not NotFound.
        let key = rusternetes_storage::build_key("namespaces", None, "broken");
        storage
            .create(&key, &serde_json::json!("not a namespace"))
            .await
            .unwrap();
        let err = PodSecurityAdmission::new()
            .admit_outcome(&storage, "broken", &baseline_pod(), "alice")
            .await
            .expect_err("an unreadable namespace must not fail open");
        assert!(
            matches!(&err, rusternetes_common::Error::Internal(m)
                if m.contains(r#"failed to lookup namespace "broken""#)),
            "{err:?}"
        );
    }

    /// admission.go:344-350: a missing namespace is an InternalError too.
    /// Upstream never reaches it because NamespaceLifecycle answers NotFound
    /// first; here that plugin exists, so no NotFound arm remains.
    #[tokio::test]
    async fn psa_missing_namespace_is_internal_error() {
        let storage = Arc::new(rusternetes_storage::MemoryStorage::new());
        let err = PodSecurityAdmission::new()
            .admit_outcome(&storage, "missing", &privileged_pod(None), "alice")
            .await
            .expect_err("a missing namespace must not fail open");
        assert!(
            matches!(&err, rusternetes_common::Error::Internal(m)
                if m.contains(r#"failed to lookup namespace "missing""#)),
            "{err:?}"
        );
    }
}
