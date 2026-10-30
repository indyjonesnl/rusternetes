//! Unit tests moved from `src/admission/pod_plugins.rs`.
#![allow(unused_imports, dead_code)]

mod __parent {
    pub use rusternetes_api_server::admission::builtin_plugins::new_forbidden;
    pub use rusternetes_api_server::admission::plugins::enabled;
    pub use rusternetes_common::qos::{get_pod_qos, QoSClass};
    pub use rusternetes_common::resources::pod::Toleration;
    pub use rusternetes_common::resources::{Namespace, Pod};
    pub use rusternetes_common::validation::field::Path;
    pub use rusternetes_common::validation::metav1::{is_qualified_name, is_valid_label_value};
    pub use rusternetes_common::validation::pod::validate_tolerations_with_options;
    pub use rusternetes_common::{Error, Result};
    pub use rusternetes_storage::{build_key, Storage, StorageBackend};
    pub use std::collections::{BTreeMap, HashMap};
    pub use std::sync::OnceLock;
}

mod tests {
    use super::__parent::*;
    use rusternetes_api_server::admission::pod_plugins::*;
    use rusternetes_storage::MemoryStorage;

    fn tol(key: &str, op: &str, value: &str, effect: &str, secs: Option<i64>) -> Toleration {
        let o = |s: &str| (!s.is_empty()).then(|| s.to_string());
        Toleration {
            key: o(key),
            operator: o(op),
            value: o(value),
            effect: o(effect),
            toleration_seconds: secs,
        }
    }

    // pkg/util/tolerations/tolerations_test.go TestVerifyAgainstWhitelist
    #[test]
    fn whitelist_verification() {
        let ok = |t: Vec<Toleration>, w: Vec<Toleration>| verify_against_whitelist(&t, &w);
        // empty whitelist / empty tolerations pass
        assert!(ok(
            vec![tol("foo", "Equal", "bar", "NoSchedule", None)],
            vec![]
        ));
        assert!(ok(
            vec![],
            vec![tol("foo", "Equal", "bar", "NoSchedule", None)]
        ));
        // exact match
        let t = tol("foo", "Equal", "bar", "NoSchedule", None);
        assert!(ok(vec![t.clone()], vec![t.clone()]));
        // different value
        assert!(!ok(
            vec![tol("foo", "Equal", "baz", "NoSchedule", None)],
            vec![t.clone()]
        ));
        // Exists on the whitelist covers every value of the key
        assert!(ok(
            vec![t.clone()],
            vec![tol("foo", "Exists", "", "NoSchedule", None)]
        ));
        // empty key + Exists matches all keys; empty effect matches all effects
        assert!(ok(vec![t.clone()], vec![tol("", "Exists", "", "", None)]));
        // whitelist NoExecute seconds bound the pod's
        let wl = tol("foo", "Exists", "", "NoExecute", Some(60));
        assert!(ok(
            vec![tol("foo", "Exists", "", "NoExecute", Some(30))],
            vec![wl.clone()]
        ));
        assert!(!ok(
            vec![tol("foo", "Exists", "", "NoExecute", Some(120))],
            vec![wl.clone()]
        ));
        assert!(!ok(
            vec![tol("foo", "Exists", "", "NoExecute", None)],
            vec![wl]
        ));
    }

    // TestMergeTolerations
    #[test]
    fn merge_keeps_supersets_only() {
        let narrow = tol("foo", "Equal", "bar", "NoSchedule", None);
        let wide = tol("foo", "Exists", "", "NoSchedule", None);
        let merged = merge_tolerations(std::slice::from_ref(&narrow), std::slice::from_ref(&wide));
        assert_eq!(merged, vec![wide.clone()]);
        let merged = merge_tolerations(std::slice::from_ref(&wide), &[narrow]);
        assert_eq!(merged, vec![wide.clone()]);
        // equal tolerations: the first is preferred, one copy kept
        assert_eq!(
            merge_tolerations(std::slice::from_ref(&wide), std::slice::from_ref(&wide)),
            vec![wide]
        );
    }

    #[test]
    fn selector_conversion() {
        assert_eq!(convert_selector_to_labels_map("").unwrap().len(), 0);
        let m = convert_selector_to_labels_map("a=b, c = d").unwrap();
        assert_eq!(m.get("a").unwrap(), "b");
        assert_eq!(m.get("c").unwrap(), "d");
        assert!(convert_selector_to_labels_map("a").is_err());
        assert!(convert_selector_to_labels_map("a=b=c").is_err());
        assert!(convert_selector_to_labels_map("a b=c").is_err());
    }

    fn set(pairs: &[(&str, &str)]) -> Set {
        pairs
            .iter()
            .map(|(k, v)| (k.to_string(), v.to_string()))
            .collect()
    }

    #[test]
    fn labels_set_ops() {
        assert!(conflicts(&set(&[("a", "1")]), &set(&[("a", "2")])));
        assert!(!conflicts(&set(&[("a", "1")]), &set(&[("b", "2")])));
        assert_eq!(
            merge(&set(&[("a", "1")]), &set(&[("a", "2")])),
            set(&[("a", "2")])
        );
        assert!(is_subset(&set(&[("a", "1")]), &Set::new()));
        assert!(is_subset(
            &set(&[("a", "1")]),
            &set(&[("a", "1"), ("b", "2")])
        ));
        assert!(!is_subset(&set(&[("a", "1")]), &set(&[("a", "2")])));
        assert!(!is_subset(&set(&[("c", "1")]), &set(&[("a", "2")])));
    }

    async fn store_with_ns(annotations: &[(&str, &str)]) -> StorageBackend {
        let storage = StorageBackend::Memory(MemoryStorage::new().into());
        let mut ns = Namespace::new("testns");
        ns.metadata.annotations = Some(
            annotations
                .iter()
                .map(|(k, v)| (k.to_string(), v.to_string()))
                .collect(),
        );
        storage
            .create(&build_key("namespaces", None, "testns"), &ns)
            .await
            .unwrap();
        storage
    }

    fn pod(
        node_selector: &[(&str, &str)],
        tolerations: Vec<Toleration>,
        qos_guaranteed: bool,
    ) -> Pod {
        let mut container = serde_json::json!({"name": "c", "image": "i"});
        if qos_guaranteed {
            container["resources"] = serde_json::json!({
                "requests": {"cpu": "1", "memory": "1Gi"},
                "limits": {"cpu": "1", "memory": "1Gi"}
            });
        }
        let mut pod: Pod = serde_json::from_value(serde_json::json!({
            "apiVersion": "v1", "kind": "Pod",
            "metadata": {"name": "p"},
            "spec": {"containers": [container]}
        }))
        .unwrap();
        let spec = pod.spec.as_mut().unwrap();
        if !node_selector.is_empty() {
            spec.node_selector = Some(
                node_selector
                    .iter()
                    .map(|(k, v)| (k.to_string(), v.to_string()))
                    .collect(),
            );
        }
        if !tolerations.is_empty() {
            spec.tolerations = Some(tolerations);
        }
        pod
    }

    const NS_SEL: &str = "scheduler.alpha.kubernetes.io/node-selector";

    // podnodeselector TestPodAdmission
    #[tokio::test]
    async fn node_selector_admission() {
        let cluster: NodeSelectorConfig = [(
            "clusterDefaultNodeSelector".to_string(),
            "env=dev".to_string(),
        )]
        .into_iter()
        .chain([("testns".to_string(), "env=test,infra=nginx".to_string())])
        .collect();

        // namespace annotation selector merged into the pod's
        let st = store_with_ns(&[(NS_SEL, "infra=nginx")]).await;
        let mut p = pod(&[("env", "test")], vec![], false);
        pod_node_selector_admit_with(&st, "testns", &mut p, &cluster)
            .await
            .unwrap();
        assert_eq!(
            p.spec.unwrap().node_selector.unwrap(),
            [("env", "test"), ("infra", "nginx")]
                .iter()
                .map(|(k, v)| (k.to_string(), v.to_string()))
                .collect::<HashMap<_, _>>()
        );

        // conflict with the namespace selector
        let mut p = pod(&[("infra", "apache")], vec![], false);
        let err = pod_node_selector_admit_with(&st, "testns", &mut p, &cluster)
            .await
            .unwrap_err();
        assert!(err
            .to_string()
            .contains("conflicts with its namespace node label selector"));

        // outside the namespace whitelist
        let mut p = pod(&[("env", "prod")], vec![], false);
        let err = pod_node_selector_admit_with(&st, "testns", &mut p, &cluster)
            .await
            .unwrap_err();
        assert!(err
            .to_string()
            .contains("conflict with its namespace whitelist"));

        // no annotation: the cluster default is used
        let st = store_with_ns(&[]).await;
        let mut p = pod(&[], vec![], false);
        let cluster2: NodeSelectorConfig = [(
            "clusterDefaultNodeSelector".to_string(),
            "env=dev".to_string(),
        )]
        .into();
        pod_node_selector_admit_with(&st, "testns", &mut p, &cluster2)
            .await
            .unwrap();
        assert_eq!(p.spec.unwrap().node_selector.unwrap()["env"], "dev");

        // missing namespace is NotFound
        let err =
            pod_node_selector_admit_with(&st, "nope", &mut pod(&[], vec![], false), &cluster2)
                .await
                .unwrap_err();
        assert!(matches!(err, Error::NotFound(_)));
    }

    #[test]
    fn node_selector_config_formats() {
        let c = read_node_selector_config(
            "podNodeSelectorPluginConfig:\n  clusterDefaultNodeSelector: a=b\n  ns1: c=d\n",
        )
        .unwrap();
        assert_eq!(c["clusterDefaultNodeSelector"], "a=b");
        assert_eq!(c["ns1"], "c=d");
        assert!(read_node_selector_config("").unwrap().is_empty());
        assert!(
            read_node_selector_config("{\"podNodeSelectorPluginConfig\":{\"x\":\"y=z\"}}")
                .unwrap()
                .contains_key("x")
        );
    }

    const DEF: &str = "scheduler.alpha.kubernetes.io/defaultTolerations";
    const WL: &str = "scheduler.alpha.kubernetes.io/tolerationsWhitelist";

    // podtolerationrestriction TestPodAdmission
    #[tokio::test]
    async fn toleration_admission() {
        let cfg = TolerationConfiguration::default();

        // namespace default tolerations are merged in
        let st = store_with_ns(&[(
            DEF,
            r#"[{"key":"testKey","operator":"Equal","value":"testValue","effect":"NoSchedule"}]"#,
        )])
        .await;
        let mut p = pod(&[], vec![], false);
        toleration_restriction_admit_with(&st, "testns", true, &mut p, &cfg)
            .await
            .unwrap();
        assert_eq!(
            p.spec.unwrap().tolerations.unwrap(),
            vec![tol("testKey", "Equal", "testValue", "NoSchedule", None)]
        );

        // cluster defaults when the namespace sets none
        let st = store_with_ns(&[]).await;
        let cfg2 = TolerationConfiguration {
            default: vec![tol("cluster", "Exists", "", "NoSchedule", None)],
            whitelist: vec![],
        };
        let mut p = pod(&[], vec![], false);
        toleration_restriction_admit_with(&st, "testns", true, &mut p, &cfg2)
            .await
            .unwrap();
        assert_eq!(p.spec.unwrap().tolerations.unwrap().len(), 1);

        // a non-BestEffort pod gets the memory-pressure toleration
        let mut p = pod(&[], vec![], true);
        toleration_restriction_admit_with(&st, "testns", true, &mut p, &cfg)
            .await
            .unwrap();
        assert_eq!(
            p.spec.unwrap().tolerations.unwrap(),
            vec![tol(
                TAINT_NODE_MEMORY_PRESSURE,
                "Exists",
                "",
                "NoSchedule",
                None
            )]
        );

        // namespace whitelist rejects a toleration outside it
        let st = store_with_ns(&[(
            WL,
            r#"[{"key":"allowed","operator":"Exists","effect":"NoSchedule"}]"#,
        )])
        .await;
        let mut p = pod(
            &[],
            vec![tol("other", "Exists", "", "NoSchedule", None)],
            false,
        );
        let err = toleration_restriction_admit_with(&st, "testns", true, &mut p, &cfg)
            .await
            .unwrap_err();
        assert!(err
            .to_string()
            .contains("conflict with its namespace whitelist"));
        // and accepts one inside it
        let mut p = pod(
            &[],
            vec![tol("allowed", "Equal", "x", "NoSchedule", None)],
            false,
        );
        toleration_restriction_admit_with(&st, "testns", true, &mut p, &cfg)
            .await
            .unwrap();

        // empty namespace whitelist overrides the cluster whitelist
        let st = store_with_ns(&[(WL, "")]).await;
        let cfg3 = TolerationConfiguration {
            default: vec![],
            whitelist: vec![tol("allowed", "Exists", "", "", None)],
        };
        let mut p = pod(
            &[],
            vec![tol("other", "Exists", "", "NoSchedule", None)],
            false,
        );
        toleration_restriction_admit_with(&st, "testns", true, &mut p, &cfg3)
            .await
            .unwrap();

        // cluster whitelist applies when the namespace sets none
        let st = store_with_ns(&[]).await;
        let mut p = pod(
            &[],
            vec![tol("other", "Exists", "", "NoSchedule", None)],
            false,
        );
        let err = toleration_restriction_admit_with(&st, "testns", true, &mut p, &cfg3)
            .await
            .unwrap_err();
        assert!(err
            .to_string()
            .contains("conflict with its cluster whitelist"));

        // malformed annotation is an error
        let st = store_with_ns(&[(DEF, "not json")]).await;
        assert!(toleration_restriction_admit_with(
            &st,
            "testns",
            true,
            &mut pod(&[], vec![], false),
            &cfg
        )
        .await
        .is_err());

        // on UPDATE the namespace defaults are not applied
        let st = store_with_ns(&[(
            DEF,
            r#"[{"key":"testKey","operator":"Exists","effect":"NoSchedule"}]"#,
        )])
        .await;
        let mut p = pod(&[], vec![], false);
        toleration_restriction_admit_with(&st, "testns", false, &mut p, &cfg)
            .await
            .unwrap();
        assert!(p.spec.unwrap().tolerations.is_none());
    }

    // validation_test.go TestValidateConfiguration + config.go decoding
    #[test]
    fn toleration_configuration_loading() {
        let hdr =
            "apiVersion: podtolerationrestriction.admission.k8s.io/v1alpha1\nkind: Configuration\n";
        let ok = format!(
            "{hdr}default:\n- {{key: foo, operator: Exists, effect: NoExecute, tolerationSeconds: 60}}\n- {{operator: Exists, effect: NoSchedule}}\nwhitelist:\n- {{key: foo, value: bar, effect: NoSchedule}}\n"
        );
        let c = load_toleration_configuration(&ok).unwrap();
        assert_eq!(c.default.len(), 2);
        assert_eq!(c.whitelist.len(), 1);
        // Exists with a value is invalid
        let bad = format!(
            "{hdr}whitelist:\n- {{key: foo, operator: Exists, value: bar, effect: NoSchedule}}\n"
        );
        assert!(load_toleration_configuration(&bad).is_err());
        // empty key needs Exists
        let bad = format!("{hdr}default:\n- {{operator: Equal, value: bar, effect: NoSchedule}}\n");
        assert!(load_toleration_configuration(&bad).is_err());
        // wrong kind / missing kind
        assert!(load_toleration_configuration("kind: Other\napiVersion: v1\n").is_err());
        assert!(load_toleration_configuration("default: []\n").is_err());
    }

    fn erpod(reqs: &[(&str, &str)], init: &[&str], tols: Option<Vec<Toleration>>) -> Pod {
        let v = serde_json::json!({
            "metadata": {"name": "p"},
            "spec": {
                "containers": [{"name": "c", "image": "i", "resources": {"requests":
                    reqs.iter().map(|(k, v)| (k.to_string(), serde_json::json!(v))).collect::<serde_json::Map<_, _>>()}}],
                "initContainers": init.iter().map(|k| serde_json::json!({"name": "i", "image": "i",
                    "resources": {"requests": {*k: "1"}}})).collect::<Vec<_>>(),
            }
        });
        let mut pod: Pod = serde_json::from_value(v).unwrap();
        pod.spec.as_mut().unwrap().tolerations = tols;
        pod
    }

    fn ert(key: &str, op: &str, effect: &str) -> Toleration {
        Toleration {
            key: Some(key.into()),
            operator: Some(op.into()),
            value: None,
            effect: Some(effect.into()),
            toleration_seconds: None,
        }
    }

    /// `TestAdmit` (extendedresourcetoleration/admission_test.go).
    #[test]
    fn extended_resource_toleration_cases() {
        let tols = |p: &Pod| p.spec.as_ref().unwrap().tolerations.clone();
        // no extended resources: unchanged (stays None)
        let mut p = erpod(&[("cpu", "1"), ("kubernetes.io/foo", "1")], &[], None);
        extended_resource_toleration_admit(&mut p);
        assert_eq!(tols(&p), None);
        // container + init container, sorted, existing preserved
        let mut p = erpod(
            &[("example.com/device-b", "1")],
            &["example.com/device-a"],
            Some(vec![ert("foo", "Exists", "NoSchedule")]),
        );
        extended_resource_toleration_admit(&mut p);
        assert_eq!(
            tols(&p).unwrap(),
            vec![
                ert("foo", "Exists", "NoSchedule"),
                ert("example.com/device-a", "Exists", "NoSchedule"),
                ert("example.com/device-b", "Exists", "NoSchedule"),
            ]
        );
        // already present: no duplicate
        let mut p = erpod(
            &[("example.com/device-a", "1")],
            &[],
            Some(vec![ert("example.com/device-a", "Exists", "NoSchedule")]),
        );
        extended_resource_toleration_admit(&mut p);
        assert_eq!(tols(&p).unwrap().len(), 1);
        // same key, different effect: both kept
        let mut p = erpod(
            &[("example.com/device-a", "1")],
            &[],
            Some(vec![ert("example.com/device-a", "Exists", "NoExecute")]),
        );
        extended_resource_toleration_admit(&mut p);
        assert_eq!(tols(&p).unwrap().len(), 2);
        // wildcard toleration preserved, new one added
        let mut p = erpod(
            &[("example.com/device-a", "1")],
            &[],
            Some(vec![Toleration {
                key: None,
                operator: Some("Exists".into()),
                value: None,
                effect: None,
                toleration_seconds: None,
            }]),
        );
        extended_resource_toleration_admit(&mut p);
        assert_eq!(tols(&p).unwrap().len(), 2);
    }
}
