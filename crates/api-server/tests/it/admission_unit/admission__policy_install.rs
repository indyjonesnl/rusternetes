//! Unit tests moved from `src/admission/policy_install.rs`.
#![allow(unused_imports, dead_code)]

mod __parent {
    pub use async_trait::async_trait;
    pub use futures::StreamExt;
    pub use rusternetes_api_server::admission::policy_dispatch::ParamScope;
    pub use rusternetes_api_server::admission::policy_matching::EquivalentResourceMapper;
    pub use rusternetes_api_server::admission::policy_mutating::{
        compile_policy, MutatingEvaluator, NamespaceObjects, ObjectDefaulter, TypeConverters,
    };
    pub use rusternetes_api_server::admission::policy_plugin::{HookSource, MutatingPolicyPlugin};
    pub use rusternetes_api_server::admission::policy_registry::{
        EquivalentResourceRegistry, StorageNamespaceLister,
    };
    pub use rusternetes_api_server::admission::policy_source::{
        Informer, ParamInformerFactory, PolicySource, RestMapper, RestMapping, StorageInformer,
        StorageParamInformerFactory,
    };
    pub use rusternetes_common::admission::{GroupVersionKind, GroupVersionResource};
    pub use rusternetes_common::authz::Authorizer;
    pub use rusternetes_common::resources::mutating_admission_policy::{
        MutatingAdmissionPolicy, MutatingAdmissionPolicyBinding,
    };
    pub use rusternetes_common::resources::Namespace;
    pub use rusternetes_common::Error;
    pub use rusternetes_discovery::restmapper::{
        new_discovery_rest_mapper, served_group_resources, Gk, PriorityRestMapper, Scope,
    };
    pub use rusternetes_storage::{build_prefix, Storage, WatchEvent};
    pub use serde_json::Value;
    pub use std::collections::BTreeMap;
    pub use std::sync::atomic::{AtomicU64, Ordering};
    pub use std::sync::{Arc, Mutex, RwLock};
}

mod tests {
    use super::__parent::*;
    use rusternetes_api_server::admission::policy_install::*;
    use rusternetes_common::resources::Namespace;
    use rusternetes_storage::MemoryStorage;

    fn gvk(g: &str, v: &str, k: &str) -> GroupVersionKind {
        GroupVersionKind {
            group: g.into(),
            version: v.into(),
            kind: k.into(),
        }
    }

    fn config_map_attributes() -> rusternetes_api_server::admission::policy_matching::Attributes {
        rusternetes_api_server::admission::policy_matching::Attributes {
            kind: gvk("", "v1", "ConfigMap"),
            resource: GroupVersionResource {
                group: String::new(),
                version: "v1".into(),
                resource: "configmaps".into(),
            },
            subresource: String::new(),
            namespace: "default".into(),
            name: "cm".into(),
            operation: rusternetes_common::admission::Operation::Create,
            object: Some(serde_json::json!({
                "apiVersion": "v1",
                "kind": "ConfigMap",
                "metadata": {"name": "cm", "namespace": "default"},
                "data": {}
            })),
            old_object: None,
            user_info: None,
            dry_run: false,
            operation_options: None,
        }
    }

    /// generic/plugin.go:139-143: a disabled plugin builds no source.
    #[tokio::test]
    #[serial_test::serial]
    async fn plugin_is_not_installed_while_the_gate_is_off() {
        let _g = rusternetes_common::feature_gates::with_feature(
            rusternetes_common::feature_gates::Feature::MutatingAdmissionPolicy,
            false,
        );
        let plugin = new_mutating_policy_plugin(
            Arc::new(MemoryStorage::new()),
            Arc::new(rusternetes_common::authz::AlwaysAllowAuthorizer),
        )
        .await;
        assert!(plugin.is_none());
    }

    /// The installed plugin runs a stored policy and binding end to end:
    /// informers -> source -> matcher -> mutating dispatcher.
    #[tokio::test]
    #[serial_test::serial]
    async fn installed_plugin_applies_a_stored_policy() {
        use rusternetes_api_server::admission::policy_mutating::ReinvocationContext;
        let _g = rusternetes_common::feature_gates::with_feature(
            rusternetes_common::feature_gates::Feature::MutatingAdmissionPolicy,
            true,
        );
        let storage = Arc::new(MemoryStorage::new());
        storage
            .create(
                "/registry/mutatingadmissionpolicies/p",
                &serde_json::json!({
                    "apiVersion": "admissionregistration.k8s.io/v1beta1",
                    "kind": "MutatingAdmissionPolicy",
                    "metadata": {"name": "p"},
                    "spec": {
                        "failurePolicy": "Fail",
                        // Stored as the registry strategy defaults them.
                        "matchConstraints": {
                            "matchPolicy": "Equivalent",
                            "namespaceSelector": {},
                            "objectSelector": {},
                            "resourceRules": [{
                            "apiGroups": [""], "apiVersions": ["v1"],
                            "operations": ["CREATE"], "resources": ["configmaps"]
                        }]},
                        "reinvocationPolicy": "Never",
                        "mutations": [{
                            "patchType": "JSONPatch",
                            "jsonPatch": {"expression":
                                "[JSONPatch{op: \"add\", path: \"/data/mutated\", value: \"yes\"}]"}
                        }]
                    }
                }),
            )
            .await
            .unwrap();
        storage
            .create(
                "/registry/mutatingadmissionpolicybindings/b",
                &serde_json::json!({
                    "apiVersion": "admissionregistration.k8s.io/v1beta1",
                    "kind": "MutatingAdmissionPolicyBinding",
                    "metadata": {"name": "b"},
                    "spec": {"policyName": "p", "matchResources": {
                        "matchPolicy": "Equivalent",
                        "namespaceSelector": {},
                        "objectSelector": {}
                    }}
                }),
            )
            .await
            .unwrap();
        storage
            .create(
                "/registry/namespaces/default",
                &serde_json::json!({
                    "apiVersion": "v1", "kind": "Namespace",
                    "metadata": {"name": "default"}
                }),
            )
            .await
            .unwrap();
        let plugin = new_mutating_policy_plugin(
            storage.clone(),
            Arc::new(rusternetes_common::authz::AlwaysAllowAuthorizer),
        )
        .await
        .expect("installed while the gate is on");

        let mut mutated = None;
        let mut last = String::new();
        for _ in 0..500 {
            let mut attr = config_map_attributes();
            // Not ready until the source has compiled its first hooks.
            let r = plugin
                .admit(&mut attr, Arc::new(ReinvocationContext::new(false)))
                .await;
            last = format!("{r:?} {:?}", attr.object);
            if r.is_ok() && attr.object.as_ref().unwrap()["data"]["mutated"] == "yes" {
                mutated = attr.object;
                break;
            }
            tokio::time::sleep(std::time::Duration::from_millis(10)).await;
        }
        assert!(
            mutated.is_some(),
            "the stored policy was not applied in 5s: {last}"
        );
    }

    #[test]
    fn param_resources_are_keyed_like_the_stores_write_them() {
        let gvr = |g: &str, r: &str| GroupVersionResource {
            group: g.into(),
            version: "v1".into(),
            resource: r.into(),
        };
        assert_eq!(param_storage_type(&gvr("", "configmaps")), "configmaps");
        assert_eq!(
            param_storage_type(&gvr("apps", "deployments")),
            "deployments"
        );
        assert_eq!(
            param_storage_type(&gvr("example.com", "widgets")),
            "example_com_widgets"
        );
    }

    #[tokio::test]
    async fn mutating_source_compiles_policies_with_compile_policy() {
        let storage = Arc::new(MemoryStorage::new());
        storage
            .create(
                "/registry/mutatingadmissionpolicies/p",
                &serde_json::json!({
                    "apiVersion": "admissionregistration.k8s.io/v1beta1",
                    "kind": "MutatingAdmissionPolicy",
                    "metadata": {"name": "p"},
                    "spec": {
                        "matchConditions": [{"name": "c", "expression": "true"}],
                        "mutations": [{
                            "patchType": "JSONPatch",
                            "jsonPatch": {"expression": "[]"}
                        }]
                    }
                }),
            )
            .await
            .unwrap();
        storage
            .create(
                "/registry/mutatingadmissionpolicybindings/b",
                &serde_json::json!({
                    "apiVersion": "admissionregistration.k8s.io/v1beta1",
                    "kind": "MutatingAdmissionPolicyBinding",
                    "metadata": {"name": "b"},
                    "spec": {"policyName": "p"}
                }),
            )
            .await
            .unwrap();
        let policies = StorageInformer::<MutatingAdmissionPolicy>::new("mutatingadmissionpolicies");
        let bindings = StorageInformer::<MutatingAdmissionPolicyBinding>::new(
            "mutatingadmissionpolicybindings",
        );
        let source = new_mutating_policy_source(
            policies.clone(),
            bindings.clone(),
            Arc::new(StorageParamInformerFactory::new(storage.clone(), |_| {
                String::new()
            })),
            Arc::new(DiscoveryRestMapper::default()),
        );
        let tasks = [
            tokio::spawn(policies.run(storage.clone())),
            tokio::spawn(bindings.run(storage.clone())),
            tokio::spawn({
                let s = source.clone();
                async move {
                    let _ = s.run().await;
                }
            }),
        ];
        let mut hooks = None;
        for _ in 0..500 {
            if source.has_synced() {
                hooks = source.hooks().filter(|h| !h.is_empty());
                if hooks.is_some() {
                    break;
                }
            }
            tokio::time::sleep(std::time::Duration::from_millis(10)).await;
        }
        for t in &tasks {
            t.abort();
        }
        let hooks = hooks.expect("hooks compiled within 5s");
        // compile_policy built the patcher and the matchConditions matcher.
        assert_eq!(hooks[0].evaluator.mutators.len(), 1);
        assert!(hooks[0].evaluator.matcher.is_some());
    }

    #[test]
    fn rest_mapper_resolves_namespaced_and_cluster_kinds() {
        let m = DiscoveryRestMapper::default();
        let cm = m.rest_mapping("", "ConfigMap", "v1").unwrap();
        assert_eq!(cm.resource.resource, "configmaps");
        assert_eq!(cm.resource.version, "v1");
        assert_eq!(cm.scope, ParamScope::Namespace);
        let ns = m.rest_mapping("", "Namespace", "v1").unwrap();
        assert_eq!(ns.resource.resource, "namespaces");
        assert_eq!(ns.scope, ParamScope::Root);
        let d = m.rest_mapping("apps", "Deployment", "v1").unwrap();
        assert_eq!(d.resource.group, "apps");
        assert_eq!(d.resource.resource, "deployments");
    }

    #[test]
    fn rest_mapper_unknown_kind_is_an_error() {
        // NoKindMatchError.
        let m = DiscoveryRestMapper::default();
        assert!(m.rest_mapping("example.com", "Widget", "v1").is_err());
        assert!(m.rest_mapping("", "Nope", "v1").is_err());
        // errors.go:106-120 NoKindMatchError, one searched version.
        assert_eq!(
            m.rest_mapping("apps", "Nope", "v1").unwrap_err(),
            r#"no matches for kind "Nope" in version "apps/v1""#
        );
        // A version the group does not serve does not match.
        assert!(m.rest_mapping("apps", "Deployment", "v9").is_err());
    }

    fn widget_crd(established: bool, scope: &str) -> Value {
        serde_json::json!({
            "metadata": {"name": "widgets.example.com"},
            "spec": {
                "group": "example.com",
                "scope": scope,
                "names": {"plural": "widgets", "singular": "widget", "kind": "Widget"},
                "versions": [{"name": "v1", "served": true, "storage": true}],
            },
            "status": {"conditions": [{
                "type": "Established",
                "status": if established { "True" } else { "False" },
            }]},
        })
    }

    // #3597: a MAP matching/param kind defined by a CRD resolves instead of
    // NoKindMatchError, and the mapper follows CRD changes.
    #[test]
    fn rest_mapper_resolves_established_crd_kinds_and_follows_changes() {
        let crds = Arc::new(CrdSnapshot::default());
        let m = DiscoveryRestMapper::new(crds.clone());
        assert!(m.rest_mapping("example.com", "Widget", "v1").is_err());

        crds.apply("widgets.example.com", widget_crd(true, "Namespaced"));
        let w = m.rest_mapping("example.com", "Widget", "v1").unwrap();
        assert_eq!(w.resource.resource, "widgets");
        assert_eq!(w.scope, ParamScope::Namespace);

        // Scope change is picked up (cache invalidated on CRD change).
        crds.apply("widgets.example.com", widget_crd(true, "Cluster"));
        assert_eq!(
            m.rest_mapping("example.com", "Widget", "v1").unwrap().scope,
            ParamScope::Root
        );

        // Not-yet-established CRDs are not served (discovery parity).
        crds.apply("widgets.example.com", widget_crd(false, "Cluster"));
        assert!(m.rest_mapping("example.com", "Widget", "v1").is_err());

        crds.apply("widgets.example.com", widget_crd(true, "Cluster"));
        crds.remove("widgets.example.com");
        assert!(m.rest_mapping("example.com", "Widget", "v1").is_err());
        // Built-ins are unaffected.
        assert!(m.rest_mapping("apps", "Deployment", "v1").is_ok());
    }

    #[tokio::test]
    async fn crd_snapshot_tracks_storage() {
        let storage = Arc::new(rusternetes_storage::MemoryStorage::new());
        let crds = CrdSnapshot::spawn(storage.clone()).await;
        let m = DiscoveryRestMapper::new(crds.clone());
        storage
            .create(
                &rusternetes_storage::build_key(
                    "customresourcedefinitions",
                    None,
                    "widgets.example.com",
                ),
                &widget_crd(true, "Namespaced"),
            )
            .await
            .unwrap();
        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(5);
        while m.rest_mapping("example.com", "Widget", "v1").is_err() {
            assert!(std::time::Instant::now() < deadline, "CRD never observed");
            tokio::time::sleep(std::time::Duration::from_millis(10)).await;
        }
    }

    #[tokio::test]
    async fn namespace_objects_returns_the_full_namespace() {
        let storage = Arc::new(MemoryStorage::new());
        let mut ns = Namespace::new("team-a");
        ns.metadata.labels = Some([("env".to_string(), "dev".to_string())].into());
        storage
            .create(
                &rusternetes_storage::build_key("namespaces", None, "team-a"),
                &ns,
            )
            .await
            .unwrap();
        let got = StorageNamespaceObjects::new(storage)
            .get_namespace("team-a")
            .await
            .unwrap();
        assert_eq!(got["metadata"]["name"], "team-a");
        assert_eq!(got["metadata"]["labels"]["env"], "dev");
    }

    #[tokio::test]
    async fn namespace_objects_missing_is_not_found() {
        let storage = Arc::new(MemoryStorage::new());
        let err = StorageNamespaceObjects::new(storage)
            .get_namespace("absent")
            .await
            .unwrap_err();
        assert!(matches!(err, Error::NotFound(_)), "{err:?}");
    }

    #[test]
    fn defaulter_defaults_a_deployment_after_patch() {
        // Scheme.Default runs SetDefaults_Deployment and the pod template's.
        let mut o = serde_json::json!({
            "apiVersion": "apps/v1", "kind": "Deployment",
            "metadata": {"name": "d"},
            "spec": {"selector": {"matchLabels": {"a": "b"}},
                "template": {"metadata": {"labels": {"a": "b"}},
                    "spec": {"containers": [{"name": "c", "image": "i", "customField": true}]}}}
        });
        SchemeObjectDefaulter.default_object(&mut o);
        assert_eq!(o["spec"]["strategy"]["type"], "RollingUpdate");
        assert_eq!(o["spec"]["template"]["spec"]["restartPolicy"], "Always");
        // Fields the typed form does not know survive.
        assert_eq!(
            o["spec"]["template"]["spec"]["containers"][0]["customField"],
            true
        );
    }

    #[test]
    fn defaulter_leaves_set_values_and_unknown_kinds_alone() {
        let mut o = serde_json::json!({
            "apiVersion": "example.com/v1", "kind": "Widget", "spec": {"a": 1}
        });
        let before = o.clone();
        SchemeObjectDefaulter.default_object(&mut o);
        assert_eq!(o, before);
        let mut p = serde_json::json!({
            "apiVersion": "v1", "kind": "Pod", "metadata": {"name": "p"},
            "spec": {"restartPolicy": "Never", "containers": [{"name": "c", "image": "i"}]}
        });
        SchemeObjectDefaulter.default_object(&mut p);
        assert_eq!(p["spec"]["restartPolicy"], "Never");
        assert_eq!(p["spec"]["enableServiceLinks"], true);
    }

    #[test]
    fn type_converters_cover_served_kinds_only() {
        let c = RegistryTypeConverters;
        assert!(c.has_type_converter(&gvk("", "v1", "ConfigMap")));
        assert!(c.has_type_converter(&gvk("apps", "v1", "Deployment")));
        assert!(!c.has_type_converter(&gvk("example.com", "v1", "Widget")));
    }

    fn equiv_crd(established: bool) -> Value {
        serde_json::json!({
            "metadata": {"name": "widgets.example.com"},
            "spec": {
                "group": "example.com",
                "names": {"plural": "widgets", "kind": "Widget"},
                "versions": [
                    {"name": "v1", "served": true, "storage": true,
                     "subresources": {"status": {}, "scale": {"specReplicasPath": ".spec.replicas"}}},
                    {"name": "v2", "served": false, "storage": false}
                ]
            },
            "status": {"conditions": [{"type": "Established",
                "status": if established { "True" } else { "False" }}]}
        })
    }

    fn equiv_gvr(g: &str, v: &str, r: &str) -> GroupVersionResource {
        GroupVersionResource {
            group: g.into(),
            version: v.into(),
            resource: r.into(),
        }
    }

    /// customresource_handler.go:762-768: every version (served or not) is
    /// registered for "", "status" when subresources.status is set, and
    /// "scale" as autoscaling/v1 Scale; built-ins still resolve.
    #[test]
    fn crd_resources_are_registered_as_equivalent() {
        let crds = Arc::new(CrdSnapshot::default());
        crds.apply("widgets.example.com", equiv_crd(true));
        let reg = CrdEquivalentRegistry::new(EquivalentResourceRegistry::from_discovery(), crds);
        let v1 = equiv_gvr("example.com", "v1", "widgets");
        let v2 = equiv_gvr("example.com", "v2", "widgets");
        assert_eq!(
            reg.equivalent_resources_for(&v1, ""),
            vec![v1.clone(), v2.clone()]
        );
        assert_eq!(
            reg.equivalent_resources_for(&v2, ""),
            vec![v1.clone(), v2.clone()]
        );
        assert_eq!(reg.kind_for(&v2, ""), gvk("example.com", "v2", "Widget"));
        assert_eq!(
            reg.kind_for(&v1, "status"),
            gvk("example.com", "v1", "Widget")
        );
        assert_eq!(
            reg.kind_for(&v1, "scale"),
            gvk("autoscaling", "v1", "Scale")
        );
        // v2 has no status subresource: only v1 registered "status", and the
        // equivalents are keyed by group-resource (mapper.go), not version.
        assert_eq!(reg.kind_for(&v2, "status"), gvk("", "", ""));
        assert_eq!(
            reg.equivalent_resources_for(&v2, "status"),
            vec![v1.clone()]
        );
        // Built-ins are untouched.
        assert!(!reg
            .equivalent_resources_for(&equiv_gvr("", "v1", "pods"), "")
            .is_empty());
    }

    /// A CRD that is not Established has no serving info, and a deleted CRD
    /// drops out (upstream drops the per-CRD registry with its storage).
    #[test]
    fn unestablished_and_deleted_crds_are_not_registered() {
        let crds = Arc::new(CrdSnapshot::default());
        let reg =
            CrdEquivalentRegistry::new(EquivalentResourceRegistry::from_discovery(), crds.clone());
        let v1 = equiv_gvr("example.com", "v1", "widgets");
        crds.apply("widgets.example.com", equiv_crd(false));
        assert!(reg.equivalent_resources_for(&v1, "").is_empty());
        crds.apply("widgets.example.com", equiv_crd(true));
        assert_eq!(reg.equivalent_resources_for(&v1, "").len(), 2);
        crds.remove("widgets.example.com");
        assert!(reg.equivalent_resources_for(&v1, "").is_empty());
        assert_eq!(reg.kind_for(&v1, ""), gvk("", "", ""));
    }
}
