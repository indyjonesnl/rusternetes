//! Unit tests moved from `src/bootstrap.rs`.
#![allow(unused_imports, dead_code)]

mod __parent {
    pub use anyhow::{Context, Result};
    pub use rusternetes_api_server::registry::rbac::reconciliation::AUTOUPDATE_ANNOTATION;
    pub use rusternetes_api_server::registry::rbac::reconciliation::{
        compute_reconciled_role, compute_reconciled_role_binding, ReconcileOperation,
    };
    pub use rusternetes_common::resources::{EndpointSlice, Endpoints};
    pub use rusternetes_storage::Storage;
    pub use rusternetes_storage::StorageBackend;
    pub use std::sync::Arc;
    pub use std::time::Duration;
    pub use tracing::{info, warn};
}

mod system_priority_class_tests {
    use super::__parent::*;
    use rusternetes_api_server::bootstrap::*;
    use rusternetes_common::resources::PriorityClass;
    use rusternetes_storage::memory::MemoryStorage;

    /// `AddSystemPriorityClasses` seeds `SystemPriorityClasses()`.
    #[tokio::test]
    async fn seeds_the_two_system_priority_classes() {
        let storage = MemoryStorage::new();
        bootstrap_system_priority_classes(&storage).await.unwrap();
        let node: PriorityClass = storage
            .get("/registry/priorityclasses/system-node-critical")
            .await
            .unwrap();
        assert_eq!(node.value, 2_000_001_000);
        assert_eq!(node.global_default, None);
        assert_eq!(node.metadata.generation, Some(1));
        assert_eq!(
            node.preemption_policy.as_deref(),
            Some("PreemptLowerPriority")
        );
        let cluster: PriorityClass = storage
            .get("/registry/priorityclasses/system-cluster-critical")
            .await
            .unwrap();
        assert_eq!(cluster.value, 2_000_000_000);
    }

    /// Upstream returns the error once the 30s poll expires
    /// (storage_scheduling.go:140-143) instead of logging and finishing.
    #[tokio::test(start_paused = true)]
    async fn poll_returns_error_at_deadline() {
        let mut calls = 0;
        let r = poll_until(Duration::from_secs(30), Duration::from_secs(1), || {
            calls += 1;
            async { Err::<(), _>(anyhow::anyhow!("storage down")) }
        })
        .await;
        let msg = r.unwrap_err().to_string();
        assert!(
            msg.contains("unable to add default system priority classes: storage down"),
            "{msg}"
        );
        assert!(calls > 1);
    }

    #[tokio::test(start_paused = true)]
    async fn poll_succeeds_after_retries() {
        let mut calls = 0;
        let r = poll_until(Duration::from_secs(30), Duration::from_secs(1), || {
            calls += 1;
            let ok = calls >= 3;
            async move {
                if ok {
                    Ok(())
                } else {
                    Err(anyhow::anyhow!("not yet"))
                }
            }
        })
        .await;
        assert!(r.is_ok());
    }

    /// The hook only creates what is missing: an existing class is left alone.
    #[tokio::test]
    async fn leaves_an_existing_class_alone() {
        let storage = MemoryStorage::new();
        let mut pc = PriorityClass::new("system-node-critical", 2_000_001_000);
        pc.description = Some("custom".into());
        storage
            .create("/registry/priorityclasses/system-node-critical", &pc)
            .await
            .unwrap();
        bootstrap_system_priority_classes(&storage).await.unwrap();
        let got: PriorityClass = storage
            .get("/registry/priorityclasses/system-node-critical")
            .await
            .unwrap();
        assert_eq!(got.description.as_deref(), Some("custom"));
        assert!(storage
            .get::<PriorityClass>("/registry/priorityclasses/system-cluster-critical")
            .await
            .is_ok());
    }
}

mod tests {
    use super::__parent::*;
    use rusternetes_api_server::bootstrap::*;
    use rusternetes_common::resources::rbac::{ClusterRole, ClusterRoleBinding};
    use rusternetes_storage::memory::MemoryStorage;

    #[tokio::test]
    async fn creates_endpoints_and_slice_when_missing() {
        let storage = MemoryStorage::new();
        reconcile_kubernetes_endpoint(&storage, "10.89.0.5", 6443)
            .await
            .unwrap();

        let ep: Endpoints = storage.get(ENDPOINTS_KEY).await.unwrap();
        assert_eq!(ep.subsets[0].addresses.as_ref().unwrap()[0].ip, "10.89.0.5");
        assert_eq!(ep.subsets[0].ports.as_ref().unwrap()[0].port, 6443);
        // Mirroring is suppressed; the apiserver owns the slice.
        assert_eq!(
            ep.metadata
                .labels
                .as_ref()
                .unwrap()
                .get(SKIP_MIRROR_LABEL)
                .map(String::as_str),
            Some("true")
        );

        let es: EndpointSlice = storage.get(ENDPOINTSLICE_KEY).await.unwrap();
        assert_eq!(es.endpoints[0].addresses, vec!["10.89.0.5".to_string()]);
        assert_eq!(es.ports[0].port, Some(6443));
    }

    /// #2533: the systemnamespaces controller's pass creates default,
    /// kube-system and kube-public, and is idempotent.
    #[tokio::test]
    async fn seeds_system_namespaces() {
        let storage = Arc::new(StorageBackend::new_memory());
        bootstrap_system_namespaces(storage.as_ref()).await.unwrap();
        bootstrap_system_namespaces(storage.as_ref()).await.unwrap();
        for ns in ["default", "kube-system", "kube-public"] {
            let key = rusternetes_storage::build_key("namespaces", None, ns);
            let got: serde_json::Value = storage.get(&key).await.unwrap();
            assert_eq!(got["status"]["phase"], "Active", "{ns}");
        }
    }

    /// #1659: bootstrap seeds the cluster-admin ClusterRole + a binding to the
    /// system:masters group on an empty store, and is idempotent. This is what
    /// authorizes kubeadm's `O=system:masters` admin cert before any RBAC is
    /// applied (via a real rule, so the escalation check stays rule-based).
    #[tokio::test]
    async fn seeds_cluster_admin_for_system_masters() {
        let storage = Arc::new(StorageBackend::new_memory());

        bootstrap_default_rbac(storage.clone()).await.unwrap();
        // Idempotent: a second run must not error or duplicate.
        bootstrap_default_rbac(storage.clone()).await.unwrap();

        let role: ClusterRole = storage.get(CLUSTER_ADMIN_ROLE_KEY).await.unwrap();
        assert!(
            role.rules.iter().any(|r| r.verbs.contains(&"*".to_string())
                && r.resources
                    .as_ref()
                    .is_some_and(|x| x.contains(&"*".to_string()))),
            "cluster-admin must grant wildcard resource access"
        );

        let binding: ClusterRoleBinding = storage.get(CLUSTER_ADMIN_BINDING_KEY).await.unwrap();
        assert_eq!(binding.role_ref.name, "cluster-admin");
        assert!(
            binding
                .subjects
                .iter()
                .any(|s| s.kind == "Group" && s.name == "system:masters"),
            "binding must target the system:masters group"
        );
    }

    /// `computeReconciledRole` (reconcile_role.go:195-232): a bootstrap rule that
    /// an existing rule already `Covers` (here a wildcard) is not appended;
    /// only the uncovered atomic rules are, and labels are merged with the
    /// existing value winning.
    #[tokio::test]
    async fn reconcile_uses_covers_not_verbatim_match() {
        let storage = Arc::new(StorageBackend::new_memory());
        bootstrap_default_rbac(storage.clone()).await.unwrap();
        let key = "/registry/clusterroles/system:basic-user";
        let mut role: serde_json::Value = storage.get(key).await.unwrap();
        // Wildcard covers every bootstrap rule of the role.
        role["rules"] = serde_json::json!([{"apiGroups":["*"],"resources":["*"],"verbs":["*"]}]);
        role["metadata"]["labels"]["extra"] = "kept".into();
        role["metadata"]["labels"]["kubernetes.io/bootstrapping"] = "mine".into();
        storage.update(key, &role).await.unwrap();

        bootstrap_default_rbac(storage.clone()).await.unwrap();
        let after: serde_json::Value = storage.get(key).await.unwrap();
        assert_eq!(after["rules"].as_array().unwrap().len(), 1, "{after}");
        assert_eq!(after["metadata"]["labels"]["extra"], "kept");
        assert_eq!(
            after["metadata"]["labels"]["kubernetes.io/bootstrapping"],
            "mine"
        );

        // A rule that is NOT covered is appended as atomic rules.
        let mut role = after.clone();
        role["rules"] = serde_json::json!([]);
        storage.update(key, &role).await.unwrap();
        bootstrap_default_rbac(storage.clone()).await.unwrap();
        let after: serde_json::Value = storage.get(key).await.unwrap();
        assert!(!after["rules"].as_array().unwrap().is_empty());
    }

    /// `computeReconciledRoleBinding` (reconcile_rolebindings.go:152-160): a
    /// changed roleRef is immutable, so the binding is deleted and recreated
    /// (`ReconcileRecreate`), dropping the stale subjects.
    #[tokio::test]
    async fn reconcile_recreates_binding_when_role_ref_differs() {
        let storage = Arc::new(StorageBackend::new_memory());
        bootstrap_default_rbac(storage.clone()).await.unwrap();
        let key = CLUSTER_ADMIN_BINDING_KEY;
        let mut b: serde_json::Value = storage.get(key).await.unwrap();
        b["roleRef"]["name"] = "view".into();
        b["subjects"] =
            serde_json::json!([{"apiGroup":"rbac.authorization.k8s.io","kind":"User","name":"x"}]);
        storage.update(key, &b).await.unwrap();

        bootstrap_default_rbac(storage.clone()).await.unwrap();
        let after: serde_json::Value = storage.get(key).await.unwrap();
        assert_eq!(after["roleRef"]["name"], "cluster-admin");
        assert_eq!(after["subjects"].as_array().unwrap().len(), 1);
        assert_eq!(after["subjects"][0]["name"], "system:masters");
    }

    /// A protected (`autoupdate: "false"`) object is left alone, even with a
    /// differing roleRef (reconcile_role.go:121-123 returns before the switch).
    #[tokio::test]
    async fn reconcile_leaves_protected_binding_alone() {
        let storage = Arc::new(StorageBackend::new_memory());
        bootstrap_default_rbac(storage.clone()).await.unwrap();
        let key = CLUSTER_ADMIN_BINDING_KEY;
        let mut b: serde_json::Value = storage.get(key).await.unwrap();
        b["roleRef"]["name"] = "view".into();
        b["metadata"]["annotations"][AUTOUPDATE_ANNOTATION] = "false".into();
        storage.update(key, &b).await.unwrap();
        bootstrap_default_rbac(storage.clone()).await.unwrap();
        let after: serde_json::Value = storage.get(key).await.unwrap();
        assert_eq!(after["roleRef"]["name"], "view");
    }

    /// `ClusterRoles()` only: not the `system:controller:*` roles (`ControllerRoles()`).
    fn is_cluster_roles_item(i: &serde_json::Value) -> bool {
        i["kind"] == "ClusterRole"
            && !i["metadata"]["name"]
                .as_str()
                .unwrap()
                .starts_with("system:controller:")
    }

    /// Upstream `TestBootstrapClusterRolesWithFeatureGatesEnabled`
    /// (`bootstrappolicy/policy_test.go:180-200`): with the gates on, the
    /// ClusterRoles equal `testdata/cluster-roles-featuregates.yaml`. Upstream
    /// flips `AllAlpha`/`AllBeta`; we flip each gate that changes a ClusterRole.
    #[test]
    #[serial_test::serial]
    fn cluster_roles_with_feature_gates_enabled_match_upstream_testdata() {
        use rusternetes_common::feature_gates::{with_feature, Feature};
        let _g: Vec<_> = [
            Feature::ClusterTrustBundle,
            Feature::PodCertificateRequest,
            Feature::DRAExtendedResource,
            Feature::DRADeviceTaintRules,
            Feature::GenericWorkload,
            Feature::ComponentFlagz,
            Feature::ComponentStatusz,
        ]
        .into_iter()
        .map(|f| with_feature(f, true))
        .collect();
        let mut got: Vec<_> = bootstrap_policy_items()
            .unwrap()
            .into_iter()
            .filter(is_cluster_roles_item)
            .collect();
        got.sort_by_key(|i| i["metadata"]["name"].as_str().unwrap().to_string());
        let want = load_policy_items(include_str!(
            "../../../src/bootstrap_policy/cluster-roles-featuregates.yaml"
        ))
        .unwrap();
        assert_eq!(got.len(), want.len(), "role set differs");
        for (g, w) in got.iter().zip(&want) {
            assert_eq!(g, w, "ClusterRole {} differs", w["metadata"]["name"]);
        }
    }

    /// With the gates at their v1.35 defaults the policy is exactly the
    /// vendored `cluster-roles.yaml` (`TestBootstrapClusterRoles`).
    #[test]
    #[serial_test::serial]
    fn cluster_roles_with_default_gates_match_upstream_testdata() {
        rusternetes_common::feature_gates::reset_to_defaults();
        let mut got: Vec<_> = bootstrap_policy_items()
            .unwrap()
            .into_iter()
            .filter(is_cluster_roles_item)
            .collect();
        got.sort_by_key(|i| i["metadata"]["name"].as_str().unwrap().to_string());
        let want = load_policy_items(include_str!(
            "../../../src/bootstrap_policy/cluster-roles.yaml"
        ))
        .unwrap();
        assert_eq!(got, want);
    }

    /// `policy.go:713-715`: the trust-bundle discovery binding exists only
    /// with `ClusterTrustBundle` on.
    #[test]
    #[serial_test::serial]
    fn cluster_trust_bundle_discovery_binding_follows_the_gate() {
        use rusternetes_common::feature_gates::{with_feature, Feature};
        let has = || {
            bootstrap_policy_items().unwrap().iter().any(|i| {
                i["kind"] == "ClusterRoleBinding"
                    && i["metadata"]["name"] == "system:cluster-trust-bundle-discovery"
            })
        };
        assert!(!has());
        let _g = with_feature(Feature::ClusterTrustBundle, true);
        assert!(has());
    }

    fn find_item(kind: &str, name: &str) -> Option<serde_json::Value> {
        bootstrap_policy_items()
            .unwrap()
            .into_iter()
            .find(|i| i["kind"] == kind && i["metadata"]["name"] == name)
    }

    fn events() -> serde_json::Value {
        // `eventsRule()` (controller_policy.go:55-57)
        serde_json::json!({"apiGroups": ["", "events.k8s.io"], "resources": ["events"],
            "verbs": ["create", "patch", "update"]})
    }

    /// Asserts the role exists with exactly `rules` and that its
    /// `addControllerRole` binding (`controller_policy.go:36-52`) exists too.
    fn assert_controller_role(short: &str, rules: Vec<serde_json::Value>) {
        let name = format!("system:controller:{short}");
        let role =
            find_item("ClusterRole", &name).unwrap_or_else(|| panic!("no ClusterRole {name}"));
        assert_eq!(role["rules"], serde_json::Value::Array(rules), "{name}");
        assert_eq!(
            role["metadata"]["labels"]["kubernetes.io/bootstrapping"],
            "rbac-defaults"
        );
        let b = find_item("ClusterRoleBinding", &name)
            .unwrap_or_else(|| panic!("no ClusterRoleBinding {name}"));
        assert_eq!(b["roleRef"]["name"], name.as_str());
        assert_eq!(b["subjects"][0]["kind"], "ServiceAccount");
        assert_eq!(b["subjects"][0]["name"], short);
        assert_eq!(b["subjects"][0]["namespace"], "kube-system");
    }

    fn assert_no_controller_role(short: &str) {
        let name = format!("system:controller:{short}");
        assert!(find_item("ClusterRole", &name).is_none(), "{name} role");
        assert!(
            find_item("ClusterRoleBinding", &name).is_none(),
            "{name} binding"
        );
    }

    /// `buildControllerRoles` (`controller_policy.go:206-231`): the DRA
    /// device-taint-eviction role needs `DRADeviceTaints`; `DRADeviceTaintRules`
    /// appends the DeviceTaintRule rules.
    #[test]
    #[serial_test::serial]
    fn device_taint_eviction_controller_role_follows_dra_gates() {
        use rusternetes_common::feature_gates::{with_feature, Feature};
        const R: &str = "device-taint-eviction-controller";
        rusternetes_common::feature_gates::reset_to_defaults();
        assert_no_controller_role(R);
        let _a = with_feature(Feature::DRADeviceTaints, true);
        let mut rules = vec![
            serde_json::json!({"apiGroups": [""], "resources": ["pods"], "verbs": ["delete", "get", "list", "watch"]}),
            serde_json::json!({"apiGroups": [""], "resources": ["pods/status"], "verbs": ["patch", "update"]}),
            serde_json::json!({"apiGroups": ["resource.k8s.io"], "resources": ["resourceclaims"], "verbs": ["get", "list", "watch"]}),
            serde_json::json!({"apiGroups": ["resource.k8s.io"], "resources": ["resourceslices"], "verbs": ["get", "list", "watch"]}),
            serde_json::json!({"apiGroups": ["resource.k8s.io"], "resources": ["deviceclasses"], "verbs": ["get", "list", "watch"]}),
            events(),
        ];
        assert_controller_role(R, rules.clone());
        let _b = with_feature(Feature::DRADeviceTaintRules, true);
        rules.push(serde_json::json!({"apiGroups": ["resource.k8s.io"], "resources": ["devicetaintrules/status"], "verbs": ["patch", "update"]}));
        rules.push(serde_json::json!({"apiGroups": ["resource.k8s.io"], "resources": ["devicetaintrules"], "verbs": ["get", "list", "watch"]}));
        assert_controller_role(R, rules);
    }

    /// `controller_policy.go:440-446` (`PodCertificateRequest`) and `:490-498`
    /// (`ClusterTrustBundle`).
    #[test]
    #[serial_test::serial]
    fn certificate_controller_roles_follow_their_gates() {
        use rusternetes_common::feature_gates::{with_feature, Feature};
        rusternetes_common::feature_gates::reset_to_defaults();
        assert_no_controller_role("podcertificaterequestcleaner");
        assert_no_controller_role("kube-apiserver-serving-clustertrustbundle-publisher");
        let _a = with_feature(Feature::PodCertificateRequest, true);
        assert_controller_role(
            "podcertificaterequestcleaner",
            vec![
                serde_json::json!({"apiGroups": ["certificates.k8s.io"], "resources": ["podcertificaterequests"], "verbs": ["delete", "get", "list", "watch"]}),
            ],
        );
        assert_no_controller_role("kube-apiserver-serving-clustertrustbundle-publisher");
        let _b = with_feature(Feature::ClusterTrustBundle, true);
        assert_controller_role(
            "kube-apiserver-serving-clustertrustbundle-publisher",
            vec![
                serde_json::json!({"apiGroups": ["certificates.k8s.io"], "resourceNames": ["kubernetes.io/kube-apiserver-serving"], "resources": ["signers"], "verbs": ["attest"]}),
                serde_json::json!({"apiGroups": ["certificates.k8s.io"], "resources": ["clustertrustbundles"], "verbs": ["create", "delete", "list", "update", "watch"]}),
                events(),
            ],
        );
    }

    /// `controller_policy.go:512-523`: needs `StorageVersionAPI` AND
    /// `APIServerIdentity` (on by default).
    #[test]
    #[serial_test::serial]
    fn storage_version_gc_role_needs_both_gates() {
        use rusternetes_common::feature_gates::{with_feature, Feature};
        const R: &str = "storage-version-garbage-collector";
        rusternetes_common::feature_gates::reset_to_defaults();
        assert_no_controller_role(R);
        {
            let _a = with_feature(Feature::StorageVersionAPI, true);
            let _b = with_feature(Feature::APIServerIdentity, false);
            assert_no_controller_role(R);
        }
        let _a = with_feature(Feature::StorageVersionAPI, true);
        assert_controller_role(
            R,
            vec![
                serde_json::json!({"apiGroups": ["coordination.k8s.io"], "resources": ["leases"], "verbs": ["get", "list", "watch"]}),
                serde_json::json!({"apiGroups": ["internal.apiserver.k8s.io"], "resources": ["storageversions"], "verbs": ["delete", "get", "list", "patch", "update", "watch"]}),
                serde_json::json!({"apiGroups": ["internal.apiserver.k8s.io"], "resources": ["storageversions/status"], "verbs": ["get", "patch", "update"]}),
            ],
        );
    }

    /// `controller_policy.go:534-546`.
    #[test]
    #[serial_test::serial]
    fn storage_version_migrator_role_follows_the_gate() {
        use rusternetes_common::feature_gates::{with_feature, Feature};
        const R: &str = "storage-version-migrator-controller";
        rusternetes_common::feature_gates::reset_to_defaults();
        assert_no_controller_role(R);
        let _a = with_feature(Feature::StorageVersionMigrator, true);
        assert_controller_role(
            R,
            vec![
                serde_json::json!({"apiGroups": ["*"], "resources": ["*"], "verbs": ["create", "list", "patch"]}),
                serde_json::json!({"apiGroups": ["storagemigration.k8s.io"], "resources": ["storageversionmigrations/status"], "verbs": ["update"]}),
            ],
        );
    }

    /// Gates that are on in v1.35 (`DynamicResourceAllocation`,
    /// `MultiCIDRServiceAllocator`, `VolumeAttributesClass`, `SELinuxChangePolicy`)
    /// are already in the vendored `controller-roles.yaml`; switching one off
    /// removes its role (`controller_policy.go:206,386,463,549`).
    #[test]
    #[serial_test::serial]
    fn default_on_controller_roles_are_present() {
        rusternetes_common::feature_gates::reset_to_defaults();
        for r in [
            "resource-claim-controller",
            "service-cidrs-controller",
            "volumeattributesclass-protection-controller",
            "selinux-warning-controller",
        ] {
            assert!(
                find_item("ClusterRole", &format!("system:controller:{r}")).is_some(),
                "{r}"
            );
        }
    }

    /// Default-gate output equals the vendored `controller-roles.yaml` /
    /// `controller-role-bindings.yaml` (`TestBootstrapControllerRoles`,
    /// `policy_test.go:212-248`).
    #[test]
    #[serial_test::serial]
    fn controller_roles_with_default_gates_match_upstream_testdata() {
        rusternetes_common::feature_gates::reset_to_defaults();
        for (kind, yaml) in [
            (
                "ClusterRole",
                include_str!("../../../src/bootstrap_policy/controller-roles.yaml"),
            ),
            (
                "ClusterRoleBinding",
                include_str!("../../../src/bootstrap_policy/controller-role-bindings.yaml"),
            ),
        ] {
            let mut got: Vec<_> = bootstrap_policy_items()
                .unwrap()
                .into_iter()
                .filter(|i| {
                    i["kind"] == kind
                        && i["metadata"]["name"]
                            .as_str()
                            .unwrap()
                            .starts_with("system:controller:")
                })
                .collect();
            got.sort_by_key(|i| i["metadata"]["name"].as_str().unwrap().to_string());
            assert_eq!(got, load_policy_items(yaml).unwrap(), "{kind}");
        }
    }

    /// #1753: the whole upstream bootstrap policy is seeded, not only
    /// cluster-admin. Names are those of upstream's
    /// `bootstrappolicy/testdata/{cluster-roles,cluster-role-bindings,
    /// namespace-roles}.yaml` (release-1.35).
    #[tokio::test]
    async fn seeds_upstream_bootstrap_policy() {
        let storage = Arc::new(StorageBackend::new_memory());
        bootstrap_default_rbac(storage.clone()).await.unwrap();
        bootstrap_default_rbac(storage.clone()).await.unwrap();

        let role: ClusterRole = storage
            .get("/registry/clusterroles/system:service-account-issuer-discovery")
            .await
            .expect("issuer-discovery ClusterRole (policy.go:555-566)");
        assert_eq!(role.rules.len(), 1);
        let urls = role.rules[0].non_resource_urls.clone().unwrap();
        assert_eq!(
            urls,
            [
                "/.well-known/openid-configuration",
                "/.well-known/openid-configuration/",
                "/openid/v1/jwks",
                "/openid/v1/jwks/"
            ]
        );
        assert_eq!(role.rules[0].verbs, ["get"]);
        let labels = role.metadata.labels.clone().unwrap();
        assert_eq!(
            labels
                .get("kubernetes.io/bootstrapping")
                .map(String::as_str),
            Some("rbac-defaults")
        );
        assert_eq!(
            role.metadata
                .annotations
                .as_ref()
                .and_then(|a| a.get("rbac.authorization.kubernetes.io/autoupdate"))
                .map(String::as_str),
            Some("true")
        );

        let binding: ClusterRoleBinding = storage
            .get("/registry/clusterrolebindings/system:service-account-issuer-discovery")
            .await
            .expect("issuer-discovery binding (policy.go:709)");
        assert_eq!(binding.subjects[0].name, "system:serviceaccounts");

        for name in [
            "admin",
            "edit",
            "view",
            "system:discovery",
            "system:basic-user",
            "system:public-info-viewer",
            "system:node",
            "system:node-proxier",
            "system:kube-scheduler",
            "system:kube-controller-manager",
            "system:controller:replicaset-controller",
            "system:controller:generic-garbage-collector",
        ] {
            storage
                .get::<ClusterRole>(&format!("/registry/clusterroles/{name}"))
                .await
                .unwrap_or_else(|e| panic!("ClusterRole {name} missing: {e}"));
        }
        for name in [
            "system:discovery",
            "system:basic-user",
            "system:public-info-viewer",
            "system:node-proxier",
            "system:kube-scheduler",
            "system:kube-controller-manager",
            "system:controller:replicaset-controller",
        ] {
            storage
                .get::<ClusterRoleBinding>(&format!("/registry/clusterrolebindings/{name}"))
                .await
                .unwrap_or_else(|e| panic!("ClusterRoleBinding {name} missing: {e}"));
        }
        // namespace-roles.yaml
        storage
            .get::<serde_json::Value>(
                "/registry/roles/kube-system/system::leader-locking-kube-scheduler",
            )
            .await
            .expect("kube-system leader-locking Role");
        // admin/edit/view are seeded with an aggregationRule and no rules, as
        // upstream; the clusterroleaggregation controller fills them from the
        // aggregate-to-* roles.
        let admin: ClusterRole = storage.get("/registry/clusterroles/admin").await.unwrap();
        assert!(admin.aggregation_rule.is_some());
        assert!(admin.rules.is_empty(), "seeded without rules");
        rusternetes_controller_core::clusterrole_aggregation::ClusterRoleAggregationController::new(storage.clone())
            .sync_all()
            .await
            .unwrap();
        for name in ["admin", "edit", "view"] {
            let role: ClusterRole = storage
                .get(&format!("/registry/clusterroles/{name}"))
                .await
                .unwrap();
            assert!(
                !role.rules.is_empty(),
                "{name} aggregated by the controller"
            );
        }
    }

    /// An IPv6-primary range: the `kubernetes` Service takes the range's first
    /// address (`cp.ServiceIPRange`, options.go:378-382) and its family.
    #[tokio::test]
    async fn kubernetes_service_follows_an_ipv6_primary_range() {
        use rusternetes_common::resources::service::IPFamily;

        let storage = MemoryStorage::new();
        let ip = rusternetes_api_server::registry::core::service::ipranges::ServiceIpRanges::parse(
            "fd00:10:96::/112",
        )
        .unwrap()
        .api_server_service_ip();
        reconcile_kubernetes_service(&storage, 6443, ip)
            .await
            .unwrap();
        let svc: rusternetes_common::resources::Service =
            storage.get(SERVICE_KEY).await.expect("kubernetes Service");
        assert_eq!(svc.spec.cluster_ip.as_deref(), Some("fd00:10:96::1"));
        assert_eq!(
            svc.spec.cluster_ips.as_deref(),
            Some(["fd00:10:96::1".to_string()].as_slice())
        );
        assert_eq!(
            svc.spec.ip_families.as_deref(),
            Some([IPFamily::IPv6].as_slice())
        );
    }

    /// The api-server must own the `default/kubernetes` Service, as upstream's
    /// kubernetesservice controller does (`pkg/controlplane/instance.go:349`,
    /// which creates AND repairs it on every reconcile tick).
    ///
    /// Ours only ever reconciled the Endpoints. In the compose stack the Service
    /// comes from bootstrap-cluster.yaml, so nothing was visibly broken — but on a
    /// cluster that does not run that script (the vanilla-swap api-server leg, and
    /// any real mixed deployment) there is no Service at all, so the kubelet
    /// injects no KUBERNETES_SERVICE_HOST/PORT and every in-cluster client dies:
    ///
    /// ```text
    /// Error loading client: error creating client: unable to load in-cluster
    /// configuration, KUBERNETES_SERVICE_HOST and KUBERNETES_SERVICE_PORT must be defined
    /// ```
    ///
    /// which aborted the whole conformance suite in 1ms (#1667).
    #[tokio::test]
    async fn creates_the_kubernetes_service_when_absent() {
        let storage = MemoryStorage::new();
        reconcile_kubernetes_service(&storage, 6443, test_service_ip())
            .await
            .unwrap();

        let svc: rusternetes_common::resources::Service =
            storage.get(SERVICE_KEY).await.expect("kubernetes Service");
        assert_eq!(svc.spec.cluster_ip.as_deref(), Some(KUBERNETES_SERVICE_IP));
        let ports = &svc.spec.ports;
        assert_eq!(ports[0].port, 443);
        assert_eq!(
            ports[0].target_port.as_ref().map(|t| format!("{t:?}")),
            Some(format!(
                "{:?}",
                rusternetes_common::resources::policy::IntOrString::Int(6443)
            ))
        );
        let labels = svc.metadata.labels.as_ref().expect("labels");
        assert_eq!(
            labels.get("component").map(String::as_str),
            Some("apiserver")
        );
        assert_eq!(
            labels.get("provider").map(String::as_str),
            Some("kubernetes")
        );
    }

    /// kube-proxy resolves a Service to its EndpointSlices BY IP FAMILY, so a
    /// `kubernetes` Service without `ipFamilies` yields no usable endpoints and
    /// kube-proxy installs a REJECT rule for 10.96.0.1 with the comment
    /// "default/kubernetes:https has no endpoints" — the ClusterIP is then
    /// unroutable even though the EndpointSlice is present and ready.
    ///
    /// Upstream sets `IPFamilyPolicy: SingleStack` and `SessionAffinity: None`
    /// explicitly when it creates this Service
    /// (`pkg/controlplane/controller/kubernetesservice/controller.go:225-239`),
    /// and the registry allocator fills `ClusterIPs`/`IPFamilies` on the way in
    /// (`pkg/registry/core/service/storage/alloc.go:239`). We write straight to
    /// storage and so bypass that allocator, which is why they must be set
    /// here. Observed live: `kube-dns` had all three and worked; this Service
    /// had none of them and did not.
    #[tokio::test]
    async fn kubernetes_service_carries_the_ip_family_fields_kube_proxy_needs() {
        use rusternetes_common::resources::service::{IPFamily, IPFamilyPolicy};

        let storage = MemoryStorage::new();
        reconcile_kubernetes_service(&storage, 6443, test_service_ip())
            .await
            .unwrap();
        let svc: rusternetes_common::resources::Service =
            storage.get(SERVICE_KEY).await.expect("kubernetes Service");

        assert_eq!(
            svc.spec.cluster_ips.as_deref(),
            Some([KUBERNETES_SERVICE_IP.to_string()].as_slice()),
            "clusterIPs must mirror clusterIP — the registry allocator would have \
             set it, and kube-proxy reads it"
        );
        assert_eq!(
            svc.spec.ip_families.as_deref(),
            Some([IPFamily::IPv4].as_slice()),
            "ipFamilies must be [IPv4]; without it kube-proxy matches no \
             EndpointSlice and REJECTs the ClusterIP"
        );
        assert_eq!(
            svc.spec.ip_family_policy,
            Some(IPFamilyPolicy::SingleStack),
            "upstream sets IPFamilyPolicy: SingleStack explicitly"
        );
        assert_eq!(
            svc.spec.session_affinity.as_deref(),
            Some("None"),
            "upstream sets SessionAffinity: None explicitly"
        );
    }

    /// Reconcile runs every tick, so it must be idempotent.
    #[tokio::test]
    async fn reconciling_the_service_twice_is_stable() {
        let storage = MemoryStorage::new();
        reconcile_kubernetes_service(&storage, 6443, test_service_ip())
            .await
            .unwrap();
        let first: rusternetes_common::resources::Service = storage.get(SERVICE_KEY).await.unwrap();
        reconcile_kubernetes_service(&storage, 6443, test_service_ip())
            .await
            .unwrap();
        let second: rusternetes_common::resources::Service =
            storage.get(SERVICE_KEY).await.unwrap();
        assert_eq!(first.metadata.uid, second.metadata.uid, "must not recreate");
        assert_eq!(
            second.spec.cluster_ip.as_deref(),
            Some(KUBERNETES_SERVICE_IP)
        );
    }

    /// And it must self-heal: upstream recreates the Service if it is deleted.
    #[tokio::test]
    async fn recreates_the_service_after_deletion() {
        let storage = MemoryStorage::new();
        reconcile_kubernetes_service(&storage, 6443, test_service_ip())
            .await
            .unwrap();
        storage.delete(SERVICE_KEY).await.unwrap();
        reconcile_kubernetes_service(&storage, 6443, test_service_ip())
            .await
            .unwrap();
        let svc: rusternetes_common::resources::Service = storage
            .get(SERVICE_KEY)
            .await
            .expect("Service must be recreated after deletion");
        assert_eq!(svc.spec.cluster_ip.as_deref(), Some(KUBERNETES_SERVICE_IP));
    }

    #[tokio::test]
    async fn updates_ip_on_recreate() {
        let storage = MemoryStorage::new();
        reconcile_kubernetes_endpoint(&storage, "172.18.0.2", 6443)
            .await
            .unwrap();
        // Simulate api-server container recreate with a new bridge IP.
        reconcile_kubernetes_endpoint(&storage, "172.18.0.9", 6443)
            .await
            .unwrap();

        let ep: Endpoints = storage.get(ENDPOINTS_KEY).await.unwrap();
        assert_eq!(
            ep.subsets[0].addresses.as_ref().unwrap()[0].ip,
            "172.18.0.9"
        );
        let es: EndpointSlice = storage.get(ENDPOINTSLICE_KEY).await.unwrap();
        assert_eq!(es.endpoints[0].addresses, vec!["172.18.0.9".to_string()]);
    }

    #[tokio::test]
    async fn repairs_malformed_empty_subsets() {
        let storage = MemoryStorage::new();
        // An endpoint stuck with no subsets — the failure mode the old one-shot
        // bootstrap could not repair (its update path only touched an existing
        // first address).
        use rusternetes_common::types::{ObjectMeta, TypeMeta};
        let mut metadata = ObjectMeta::new("kubernetes");
        metadata.namespace = Some("default".to_string());
        let broken = Endpoints {
            type_meta: TypeMeta {
                kind: "Endpoints".to_string(),
                api_version: "v1".to_string(),
            },
            metadata,
            subsets: vec![],
        };
        storage.create(ENDPOINTS_KEY, &broken).await.unwrap();

        reconcile_kubernetes_endpoint(&storage, "10.1.2.3", 6443)
            .await
            .unwrap();

        let ep: Endpoints = storage.get(ENDPOINTS_KEY).await.unwrap();
        assert_eq!(ep.subsets[0].addresses.as_ref().unwrap()[0].ip, "10.1.2.3");
        assert_eq!(
            ep.metadata
                .labels
                .as_ref()
                .unwrap()
                .get(SKIP_MIRROR_LABEL)
                .map(String::as_str),
            Some("true")
        );
    }

    // --- default ServiceCIDR controller -----------------------------------

    use rusternetes_common::resources::{ServiceCIDR, ServiceCIDRCondition, ServiceCIDRStatus};

    fn default_cidrs() -> Vec<String> {
        DEFAULT_SERVICE_CIDRS
            .iter()
            .map(|c| c.to_string())
            .collect()
    }

    fn sc_key() -> String {
        rusternetes_storage::build_key("servicecidrs", None, DEFAULT_SERVICE_CIDR_NAME)
    }

    fn controller(
        storage: &Arc<MemoryStorage>,
        cidrs: Vec<String>,
    ) -> DefaultServiceCIDRController<MemoryStorage> {
        DefaultServiceCIDRController::new(Arc::clone(storage), cidrs)
    }

    async fn stored(storage: &MemoryStorage) -> ServiceCIDR {
        storage
            .get::<ServiceCIDR>(&sc_key())
            .await
            .expect("default ServiceCIDR exists")
    }

    fn ready(sc: &ServiceCIDR) -> Option<ServiceCIDRCondition> {
        sc.status
            .as_ref()?
            .conditions
            .as_ref()?
            .iter()
            .find(|c| c.condition_type == "Ready")
            .cloned()
    }

    #[tokio::test]
    async fn creates_the_default_servicecidr_and_marks_it_ready() {
        let storage = Arc::new(MemoryStorage::new());
        controller(&storage, default_cidrs()).sync().await.unwrap();

        let sc = stored(&storage).await;
        assert_eq!(sc.spec.as_ref().unwrap().cidrs, default_cidrs());
        let cond = ready(&sc).expect("Ready condition applied by syncStatus");
        assert_eq!(cond.status, "True");
        assert_eq!(cond.message, DEFAULT_SERVICE_CIDR_READY_MESSAGE);
        assert_eq!(
            cond.reason, "",
            "upstream applies Ready=True with no reason"
        );
    }

    #[tokio::test]
    async fn sync_is_idempotent() {
        let storage = Arc::new(MemoryStorage::new());
        let mut c = controller(&storage, default_cidrs());
        c.sync().await.unwrap();
        let first = ready(&stored(&storage).await).unwrap();
        c.sync().await.unwrap();
        let second = ready(&stored(&storage).await).unwrap();
        assert_eq!(first.last_transition_time, second.last_transition_time);
    }

    #[tokio::test]
    async fn upgrades_single_stack_to_dual_stack() {
        let storage = Arc::new(MemoryStorage::new());
        controller(&storage, vec!["10.96.0.0/12".into()])
            .sync()
            .await
            .unwrap();

        let dual = vec!["10.96.0.0/12".to_string(), "2001:db8::/112".to_string()];
        controller(&storage, dual.clone()).sync().await.unwrap();

        assert_eq!(stored(&storage).await.spec.unwrap().cidrs, dual);
    }

    #[tokio::test]
    async fn mismatched_cidrs_leave_the_condition_alone() {
        let storage = Arc::new(MemoryStorage::new());
        // Persisted range disagrees with this api-server's configuration.
        controller(&storage, vec!["10.0.0.0/16".into()])
            .sync()
            .await
            .unwrap();
        let mut sc = stored(&storage).await;
        sc.status = None;
        storage.update(&sc_key(), &sc).await.unwrap();

        controller(&storage, vec!["10.96.0.0/12".into()])
            .sync()
            .await
            .unwrap();

        let sc = stored(&storage).await;
        assert_eq!(
            sc.spec.as_ref().unwrap().cidrs,
            vec!["10.0.0.0/16".to_string()],
            "a mismatch is reported, never silently rewritten"
        );
        assert!(
            ready(&sc).is_none(),
            "inconsistent config must not be marked Ready"
        );
    }

    #[tokio::test]
    async fn never_touches_the_status_of_a_terminating_servicecidr() {
        let storage = Arc::new(MemoryStorage::new());
        controller(&storage, default_cidrs()).sync().await.unwrap();

        // The controller-manager owns the terminating path; clear Ready and
        // mark the object deleting the way a DELETE + finalizer would.
        let mut sc = stored(&storage).await;
        sc.status = Some(ServiceCIDRStatus { conditions: None });
        sc.metadata.deletion_timestamp = Some(chrono::Utc::now());
        sc.metadata.finalizers = Some(vec!["networking.k8s.io/service-cidr-finalizer".into()]);
        storage.update(&sc_key(), &sc).await.unwrap();

        controller(&storage, default_cidrs()).sync().await.unwrap();

        assert!(
            ready(&stored(&storage).await).is_none(),
            "a deleting ServiceCIDR must not be marked Ready again"
        );
    }

    #[tokio::test]
    async fn does_not_overwrite_a_ready_false_condition() {
        let storage = Arc::new(MemoryStorage::new());
        controller(&storage, default_cidrs()).sync().await.unwrap();

        let mut sc = stored(&storage).await;
        sc.status = Some(ServiceCIDRStatus {
            conditions: Some(vec![ServiceCIDRCondition {
                condition_type: "Ready".to_string(),
                status: "False".to_string(),
                observed_generation: None,
                last_transition_time: None,
                reason: "Terminating".to_string(),
                message: "blocked".to_string(),
            }]),
        });
        storage.update(&sc_key(), &sc).await.unwrap();

        controller(&storage, default_cidrs()).sync().await.unwrap();

        let cond = ready(&stored(&storage).await).unwrap();
        assert_eq!(
            cond.status, "False",
            "Ready=False is another component's to clear, not ours"
        );
        assert_eq!(cond.reason, "Terminating");
    }
}

mod post_start_hook_registration_tests {
    use super::__parent::*;
    use rusternetes_api_server::bootstrap::*;
    use rusternetes_api_server::post_start_hooks::global;

    async fn finished(name: &str) -> bool {
        for _ in 0..200 {
            if global().check(name) == Some(Ok(())) {
                return true;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        false
    }

    /// Each controller upstream registers as a `poststarthook/<name>` check
    /// that passes once the hook has started it (instance.go:360,
    /// apiserver.go:339/361, server.go:249).
    #[tokio::test]
    async fn controller_hooks_register_and_finish() {
        let storage = Arc::new(StorageBackend::new_memory());
        spawn_endpoint_reconciler(
            storage.clone(),
            6443,
            rusternetes_api_server::bootstrap::test_service_ip(),
            AdvertiseAddress::default(),
        );
        spawn_apiservice_availability_controller(storage.clone());
        rusternetes_api_server::registry::apiextensions::customresourcedefinition::start_controllers(
            storage.clone(),
            Default::default(),
        );
        rusternetes_api_server::registry::apiextensions::customresourcedefinition::spawn_crd_informer_synced_hook(
            storage.clone(),
        );
        spawn_cluster_authentication_trust_controller(
            storage.clone(),
            cluster_authentication_info(None, None, None),
        );
        start_default_servicecidr_controller(
            storage.clone(),
            DEFAULT_SERVICE_CIDRS
                .iter()
                .map(|c| c.to_string())
                .collect(),
        )
        .await;
        for name in [
            BOOTSTRAP_CONTROLLER_HOOK,
            APISERVICE_LOCAL_AVAILABLE_HOOK,
            APISERVICE_REMOTE_AVAILABLE_HOOK,
            APIEXTENSIONS_CONTROLLERS_HOOK,
            CRD_INFORMER_SYNCED_HOOK,
            CLUSTER_AUTHENTICATION_INFO_HOOK,
            SERVICE_CIDR_CONTROLLER_HOOK,
        ] {
            assert!(finished(name).await, "{name} not registered/finished");
        }
    }

    /// storage_rbac.go:162-179: poll 1s/30s, then fail with
    /// "unable to initialize roles: %v" -- which the runner makes fatal.
    #[tokio::test(start_paused = true)]
    async fn rbac_hook_poll_fails_with_upstream_message() {
        let r = poll_until_msg(
            "unable to initialize roles",
            Duration::from_secs(30),
            Duration::from_secs(1),
            || async { Err::<(), _>(anyhow::anyhow!("etcd down")) },
        )
        .await;
        assert_eq!(
            r.unwrap_err().to_string(),
            "unable to initialize roles: etcd down"
        );
    }

    #[tokio::test]
    async fn rbac_hook_seeds_policy_and_finishes() {
        let storage = Arc::new(StorageBackend::new_memory());
        spawn_rbac_bootstrap_roles_hook(storage.clone())
            .await
            .unwrap();
        assert_eq!(global().check(RBAC_BOOTSTRAP_ROLES_HOOK), Some(Ok(())));
        let _: serde_json::Value = storage
            .get("/registry/clusterroles/cluster-admin")
            .await
            .unwrap();
    }
}

/// Ported from `pkg/controlplane/controller/systemnamespaces/
/// system_namespaces_controller_test.go` `Test_Controller`: `sync` creates
/// exactly the missing system namespaces and leaves existing ones alone.
mod system_namespaces_tests {
    use super::__parent::*;
    use rusternetes_api_server::bootstrap::*;
    use rusternetes_common::resources::Namespace;

    async fn seed(storage: &StorageBackend, ns: &str) -> String {
        create_namespace_if_needed(storage, ns).await.unwrap();
        let key = rusternetes_storage::build_key("namespaces", None, ns);
        storage
            .get::<Namespace>(&key)
            .await
            .unwrap()
            .metadata
            .uid
            .clone()
    }

    /// options.go:131 `{kube-system, kube-public, default}` plus
    /// cmd/kube-apiserver/app/options/options.go:94 `kube-node-lease`.
    #[test]
    fn the_four_system_namespaces() {
        assert_eq!(
            SYSTEM_NAMESPACES,
            ["kube-system", "kube-public", "default", "kube-node-lease"]
        );
        assert_eq!(SYSTEM_NAMESPACES_HOOK, "start-system-namespaces-controller");
    }

    #[tokio::test]
    async fn creates_every_missing_system_namespace() {
        for pre in [
            &["foo", "bar"][..],
            &["kube-system"],
            &["kube-system", "kube-public"],
        ] {
            let storage = StorageBackend::new_memory();
            for ns in pre {
                seed(&storage, ns).await;
            }
            let errs = sync_system_namespaces(&storage, &SYSTEM_NAMESPACES).await;
            assert!(errs.is_empty(), "{errs:?}");
            for ns in SYSTEM_NAMESPACES {
                let key = rusternetes_storage::build_key("namespaces", None, ns);
                let got: Namespace = storage.get(&key).await.unwrap();
                assert_eq!(got.metadata.name, ns);
            }
        }
    }

    /// "the four namespaces" case: no create at all, so uids are unchanged.
    #[tokio::test]
    async fn existing_namespaces_are_not_recreated() {
        let storage = StorageBackend::new_memory();
        let before = seed(&storage, "kube-system").await;
        sync_system_namespaces(&storage, &SYSTEM_NAMESPACES).await;
        sync_system_namespaces(&storage, &SYSTEM_NAMESPACES).await;
        let key = rusternetes_storage::build_key("namespaces", None, "kube-system");
        let got: Namespace = storage.get(&key).await.unwrap();
        assert_eq!(got.metadata.uid, before);
    }
}

/// Ports of the `utilnet` host-address tests
/// (`apimachinery/pkg/util/net/interface_test.go` `TestGetIPFromHostInterfaces`,
/// `TestMemberOf`) plus `ResolveBindAddress` / `AdvertiseAddress` semantics.
mod advertise_address_tests {
    use super::__parent::*;
    use rusternetes_api_server::bootstrap::*;
    use rusternetes_storage::memory::MemoryStorage;
    use std::net::IpAddr;

    fn ip(s: &str) -> IpAddr {
        s.parse().unwrap()
    }

    fn addrs(list: &[&str]) -> Vec<(String, IpAddr)> {
        list.iter()
            .enumerate()
            .map(|(i, a)| (format!("eth{i}"), ip(a)))
            .collect()
    }

    #[test]
    fn prefers_requested_family_and_falls_back() {
        let both = addrs(&["10.0.0.5", "2001:db8::5"]);
        assert_eq!(
            choose_ip_from_host_interfaces(&both, PREFER_IPV4).unwrap(),
            ip("10.0.0.5")
        );
        assert_eq!(
            choose_ip_from_host_interfaces(&both, PREFER_IPV6).unwrap(),
            ip("2001:db8::5")
        );
        let v6_only = addrs(&["2001:db8::5"]);
        assert_eq!(
            choose_ip_from_host_interfaces(&v6_only, PREFER_IPV4).unwrap(),
            ip("2001:db8::5")
        );
        let v4_only = addrs(&["10.0.0.5"]);
        assert_eq!(
            choose_ip_from_host_interfaces(&v4_only, PREFER_IPV6).unwrap(),
            ip("10.0.0.5")
        );
    }

    #[test]
    fn skips_non_global_unicast_and_errors_when_none() {
        let l = addrs(&["169.254.1.1", "fe80::1", "224.0.0.1", "10.0.0.5"]);
        assert_eq!(
            choose_ip_from_host_interfaces(&l, PREFER_IPV6).unwrap(),
            ip("10.0.0.5")
        );
        let none = addrs(&["fe80::1", "169.254.1.1"]);
        assert!(choose_ip_from_host_interfaces(&none, PREFER_IPV4).is_err());
        assert!(choose_ip_from_host_interfaces(&[], PREFER_IPV4).is_err());
    }

    /// `ResolveBindAddress`: a concrete bind address is returned as is; an
    /// unspecified one resolves within ITS family.
    #[test]
    fn resolve_bind_address_follows_bind_family() {
        let host = || Ok(addrs(&["10.0.0.5", "2001:db8::5"]));
        assert_eq!(
            resolve_bind_address_with(Some(ip("192.0.2.9")), host).unwrap(),
            ip("192.0.2.9")
        );
        assert_eq!(
            resolve_bind_address_with(Some(ip("0.0.0.0")), host).unwrap(),
            ip("10.0.0.5")
        );
        assert_eq!(
            resolve_bind_address_with(Some(ip("::")), host).unwrap(),
            ip("2001:db8::5")
        );
        assert_eq!(
            resolve_bind_address_with(Some(ip("::1")), host).unwrap(),
            ip("2001:db8::5")
        );
        assert_eq!(
            resolve_bind_address_with(None, host).unwrap(),
            ip("10.0.0.5")
        );
    }

    #[test]
    fn advertise_address_wins_unless_unspecified() {
        let a = AdvertiseAddress::new(Some(ip("2001:db8::7")), "0.0.0.0:6443");
        assert_eq!(a.resolve().unwrap(), ip("2001:db8::7"));
        let a = AdvertiseAddress::new(None, "[2001:db8::9]:6443");
        assert_eq!(a.bind, Some(ip("2001:db8::9")));
        assert_eq!(a.resolve().unwrap(), ip("2001:db8::9"));
        // Unspecified --advertise-address falls through to the bind address.
        let a = AdvertiseAddress::new(Some(ip("::")), "10.1.1.1:6443");
        assert_eq!(a.resolve().unwrap(), ip("10.1.1.1"));
    }

    /// `endpointSliceFromEndpoints`: IPv6 address -> IPv6 slice; a transition
    /// replaces the slice (`EnsureEndpointSliceFromEndpoints`, :96-104).
    #[tokio::test]
    async fn endpointslice_address_type_follows_ip_and_is_replaced() {
        let storage = MemoryStorage::new();
        reconcile_kubernetes_endpoint(&storage, "2001:db8::5", 6443)
            .await
            .unwrap();
        let es: EndpointSlice = storage.get(ENDPOINTSLICE_KEY).await.unwrap();
        assert_eq!(es.address_type, "IPv6");
        assert_eq!(es.endpoints[0].addresses, vec!["2001:db8::5".to_string()]);

        reconcile_kubernetes_endpoint(&storage, "10.0.0.5", 6443)
            .await
            .unwrap();
        let es: EndpointSlice = storage.get(ENDPOINTSLICE_KEY).await.unwrap();
        assert_eq!(es.address_type, "IPv4");
        assert_eq!(es.endpoints[0].addresses, vec!["10.0.0.5".to_string()]);
    }
}

/// `ClusterAuthenticationInfo` publication (#3423). Upstream
/// `pkg/controlplane/apiserver/config.go:344-357`: `ClientCA` always, the
/// request-header keys only when `ToAuthenticationRequestHeaderConfig` is
/// non-nil, i.e. `--requestheader-client-ca-file` is set.
mod cluster_authentication_info_tests {
    use super::__parent::*;
    use rusternetes_api_server::bootstrap::*;
    use rusternetes_api_server::request_header_authentication::RequestHeaderAuthenticationArgs;

    const CA_PEM: &str = "-----BEGIN CERTIFICATE-----\nAAAA\n-----END CERTIFICATE-----\n";
    const FRONT_PROXY_PEM: &str = "-----BEGIN CERTIFICATE-----\nBBBB\n-----END CERTIFICATE-----\n";

    #[test]
    fn unset_request_header_ca_publishes_no_request_header_keys() {
        let info = cluster_authentication_info(Some(CA_PEM), None, None);
        assert_eq!(info.client_ca.as_deref(), Some(CA_PEM));
        assert_eq!(info.request_header_username_headers, None);
        assert_eq!(info.request_header_uid_headers, None);
        assert_eq!(info.request_header_group_headers, None);
        assert_eq!(info.request_header_extra_header_prefixes, None);
        assert_eq!(info.request_header_allowed_names, None);
        assert_eq!(info.request_header_ca, None);
    }

    #[test]
    fn set_request_header_ca_publishes_the_flags() {
        let dir = std::env::temp_dir().join(format!("rt-3423-{}", uuid::Uuid::new_v4()));
        std::fs::create_dir_all(&dir).unwrap();
        let ca_file = dir.join("front-proxy-ca.crt");
        std::fs::write(&ca_file, FRONT_PROXY_PEM).unwrap();
        let args = RequestHeaderAuthenticationArgs {
            username_headers: vec!["X-Remote-User".into()],
            group_headers: vec!["X-Remote-Group".into()],
            extra_headers_prefix: vec!["X-Remote-Extra-".into()],
            client_ca_file: ca_file.to_string_lossy().into_owned(),
            allowed_names: vec!["front-proxy-client".into()],
            ..Default::default()
        };
        let trust = args.resolve(None).unwrap().unwrap();
        let info = cluster_authentication_info(Some(CA_PEM), None, Some(&trust));
        std::fs::remove_dir_all(&dir).ok();
        assert_eq!(info.client_ca.as_deref(), Some(CA_PEM));
        assert_eq!(info.request_header_ca.as_deref(), Some(FRONT_PROXY_PEM));
        assert_eq!(
            info.request_header_username_headers,
            Some(vec!["X-Remote-User".to_string()])
        );
        assert_eq!(
            info.request_header_group_headers,
            Some(vec!["X-Remote-Group".to_string()])
        );
        assert_eq!(
            info.request_header_extra_header_prefixes,
            Some(vec!["X-Remote-Extra-".to_string()])
        );
        assert_eq!(
            info.request_header_allowed_names,
            Some(vec!["front-proxy-client".to_string()])
        );
    }
}

#[path = "bootstrap_policy_tests.rs"]
mod bootstrap_policy_tests {
    //! Ported from `plugin/pkg/auth/authorizer/rbac/bootstrappolicy/policy_test.go`
    //! (release-1.35): the policy-invariant tests that do not compare against a
    //! vendored fixture -- `TestEditViewRelationship` (:86-118),
    //! `TestClusterRoleLabel` (:287-312), `TestNodeRuleVerbsConsistency` (:314-322),
    //! `TestClusterRoleVerbsConsistency` (:324-334) and
    //! `TestNamespaceRoleVerbsConsistency` (:336-348).
    //!
    //! The fixture-equality tests (`TestBootstrap*Roles*`, `testObjects`) are
    //! ported in `bootstrap.rs` (`*_match_upstream_testdata`).

    use super::__parent::*;
    use rusternetes_api_server::bootstrap::*;
    use rusternetes_api_server::registry::rbac::policy_comparator::covers;
    use rusternetes_common::resources::PolicyRule;

    /// `ClusterRoles()`: every ClusterRole except `system:controller:*`
    /// (`ControllerRoles()`).
    fn cluster_roles() -> Vec<serde_json::Value> {
        bootstrap_policy_items()
            .unwrap()
            .into_iter()
            .filter(|i| {
                i["kind"] == "ClusterRole"
                    && !i["metadata"]["name"]
                        .as_str()
                        .unwrap()
                        .starts_with("system:controller:")
            })
            .collect()
    }

    fn rules_of(role: &serde_json::Value) -> Vec<PolicyRule> {
        serde_json::from_value(role["rules"].clone()).unwrap_or_default()
    }

    fn rule(groups: &[&str], resources: &[&str]) -> PolicyRule {
        PolicyRule {
            // `bootstrappolicy.Read` (policy.go:39).
            verbs: vec!["get".into(), "list".into(), "watch".into()],
            api_groups: Some(groups.iter().map(|s| s.to_string()).collect()),
            resources: Some(resources.iter().map(|s| s.to_string()).collect()),
            resource_names: None,
            non_resource_urls: None,
        }
    }

    /// `list` without `watch` in any rule of `rules` (the three
    /// `Test*VerbsConsistency` assertions).
    fn list_without_watch(rules: &[PolicyRule]) -> bool {
        rules
            .iter()
            .any(|r| r.verbs.iter().any(|v| v == "list") && !r.verbs.iter().any(|v| v == "watch"))
    }

    /// `TestEditViewRelationship`.
    #[test]
    #[serial_test::serial]
    fn view_role_has_no_escalating_or_ungettable_powers() {
        rusternetes_common::feature_gates::reset_to_defaults();
        let roles = cluster_roles();
        let view = roles
            .iter()
            .find(|r| r["metadata"]["name"] == "system:aggregate-to-view")
            .expect("system:aggregate-to-view");
        let view_rules = rules_of(view);

        // viewEscalatingNamespaceResources (policy_test.go:68-75): rules that
        // would allow privilege escalation based on the ability to view (GET) them.
        let escalating = [
            rule(&[""], &["pods/attach"]),
            rule(&[""], &["pods/proxy"]),
            rule(&[""], &["pods/exec"]),
            rule(&[""], &["pods/portforward"]),
            rule(&[""], &["secrets"]),
            rule(&[""], &["services/proxy"]),
        ];
        for r in &escalating {
            assert!(
                !covers(&view_rules, std::slice::from_ref(r)).0,
                "view has extra powers: {r:?}"
            );
        }

        // ungettableResources (policy_test.go:79-81).
        let ungettable = [rule(&["apps", "extensions"], &["deployments/rollback"])];
        for r in &ungettable {
            assert!(
                !covers(&view_rules, std::slice::from_ref(r)).0,
                "view has ungettable resource: {r:?}"
            );
        }
    }

    /// `TestClusterRoleLabel`.
    #[test]
    #[serial_test::serial]
    fn cluster_roles_and_bindings_carry_the_bootstrapping_label() {
        rusternetes_common::feature_gates::reset_to_defaults();
        for role in cluster_roles() {
            assert_eq!(
                role["metadata"]["labels"]["kubernetes.io/bootstrapping"], "rbac-defaults",
                "ClusterRole {}",
                role["metadata"]["name"]
            );
        }

        // `ClusterRoleBindings()` excludes `ControllerRoleBindings()`; the labels
        // must be exactly the one entry (reflect.DeepEqual, policy_test.go:305).
        let want = serde_json::json!({"kubernetes.io/bootstrapping": "rbac-defaults"});
        for b in bootstrap_policy_items().unwrap().into_iter().filter(|i| {
            i["kind"] == "ClusterRoleBinding"
                && !i["metadata"]["name"]
                    .as_str()
                    .unwrap()
                    .starts_with("system:controller:")
        }) {
            assert_eq!(
                b["metadata"]["labels"], want,
                "ClusterRoleBinding {}",
                b["metadata"]["name"]
            );
        }
    }

    /// `TestNodeRuleVerbsConsistency`: `NodeRules()` are the rules of the
    /// `system:node` ClusterRole (policy.go `ClusterRoles()`).
    #[test]
    #[serial_test::serial]
    fn node_rules_list_implies_watch() {
        rusternetes_common::feature_gates::reset_to_defaults();
        let roles = cluster_roles();
        let node = roles
            .iter()
            .find(|r| r["metadata"]["name"] == "system:node")
            .expect("system:node");
        assert!(!list_without_watch(&rules_of(node)));
    }

    /// `TestClusterRoleVerbsConsistency`.
    #[test]
    #[serial_test::serial]
    fn cluster_role_rules_list_implies_watch() {
        rusternetes_common::feature_gates::reset_to_defaults();
        for role in cluster_roles() {
            assert!(
                !list_without_watch(&rules_of(&role)),
                "The ClusterRole {} has Verb `List` but does not have Verb `Watch`.",
                role["metadata"]["name"]
            );
        }
    }

    /// `TestNamespaceRoleVerbsConsistency`.
    #[test]
    #[serial_test::serial]
    fn namespace_role_rules_list_implies_watch() {
        rusternetes_common::feature_gates::reset_to_defaults();
        let roles: Vec<_> = bootstrap_policy_items()
            .unwrap()
            .into_iter()
            .filter(|i| i["kind"] == "Role")
            .collect();
        assert!(!roles.is_empty(), "namespace roles must be seeded");
        for role in roles {
            assert!(
                !list_without_watch(&rules_of(&role)),
                "The Role {}/{} has Verb `List` but does not have Verb `Watch`.",
                role["metadata"]["namespace"],
                role["metadata"]["name"]
            );
        }
    }

    /// Mutation guard: the helper really flags a list-without-watch rule.
    #[test]
    fn list_without_watch_detects_the_violation() {
        let mut r = rule(&[""], &["pods"]);
        r.verbs = vec!["list".into()];
        assert!(list_without_watch(&[r]));
    }

    /// `TestBootstrapClusterRoleBindings`, `TestBootstrapNamespaceRoles` and
    /// `TestBootstrapNamespaceRoleBindings` (`policy_test.go:120-216`): with the
    /// default gates the generated objects equal the vendored fixtures.
    #[test]
    #[serial_test::serial]
    fn bindings_and_namespace_objects_match_upstream_testdata() {
        rusternetes_common::feature_gates::reset_to_defaults();
        let is_controller = |i: &serde_json::Value| {
            i["metadata"]["name"]
                .as_str()
                .unwrap()
                .starts_with("system:controller:")
        };
        for (kind, yaml) in [
            (
                "ClusterRoleBinding",
                include_str!("../../../src/bootstrap_policy/cluster-role-bindings.yaml"),
            ),
            (
                "Role",
                include_str!("../../../src/bootstrap_policy/namespace-roles.yaml"),
            ),
            (
                "RoleBinding",
                include_str!("../../../src/bootstrap_policy/namespace-role-bindings.yaml"),
            ),
        ] {
            let key = |i: &serde_json::Value| {
                (
                    i["metadata"]["namespace"]
                        .as_str()
                        .unwrap_or("")
                        .to_string(),
                    i["metadata"]["name"].as_str().unwrap().to_string(),
                )
            };
            let mut got: Vec<_> = bootstrap_policy_items()
                .unwrap()
                .into_iter()
                .filter(|i| {
                    i["kind"] == kind && !(kind == "ClusterRoleBinding" && is_controller(i))
                })
                .collect();
            got.sort_by_key(key);
            let mut want = load_policy_items(yaml).unwrap();
            want.sort_by_key(key);
            assert_eq!(got, want, "{kind}");
        }
    }
}
