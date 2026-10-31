//! Unit tests moved from `src/admission/builtin_plugins.rs`.
#![allow(unused_imports, dead_code)]

mod __parent {
    pub use rusternetes_common::quantity::{Format, Quantity};
    pub use rusternetes_common::resources::volume::PersistentVolumeClaimPhase;
    pub use rusternetes_common::resources::{
        Ingress, IngressClass, PersistentVolumeClaim, Pod, Service, StorageClass,
    };
    pub use rusternetes_common::types::{ObjectMeta, Status};
    pub use rusternetes_common::validation::field;
    pub use rusternetes_common::{Error, Result};
    pub use rusternetes_storage::Storage;
    pub use std::sync::Arc;
}

mod tests {
    use std::sync::Arc;

    use rusternetes_common::admission::{GroupVersionKind, GroupVersionResource, Operation};
    use rusternetes_common::auth::{TokenManager, UserInfo};
    use rusternetes_common::authz::{AlwaysAllowAuthorizer, Authorizer};
    use rusternetes_common::observability::MetricsRegistry;
    use rusternetes_common::{Error, Result};
    use rusternetes_storage::{Storage, StorageBackend};
    use serde_json::{json, Value};

    use rusternetes_api_server::admission::plugins::EnabledForTest;
    use rusternetes_api_server::endpoints::handlers::admission::Admission;
    use rusternetes_api_server::registry::rest::{Object, RequestContext};
    use rusternetes_api_server::state::ApiServerState;

    fn state() -> ApiServerState {
        ApiServerState::new(
            Arc::new(StorageBackend::new_memory()),
            Arc::new(TokenManager::new(b"test-secret")),
            Arc::new(AlwaysAllowAuthorizer) as Arc<dyn Authorizer>,
            Arc::new(MetricsRegistry::new()),
            true,
        )
    }

    /// One request through the mutating then the validating chain, as
    /// `create.rs`/`update.rs` run it. Returns the admitted object.
    async fn run<T: Object>(
        state: &ApiServerState,
        (group, kind, resource): (&str, &str, &str),
        subresource: Option<&str>,
        obj: Value,
        old: Option<Value>,
    ) -> Result<T> {
        let gvk = GroupVersionKind {
            group: group.into(),
            version: "v1".into(),
            kind: kind.into(),
        };
        let gvr = GroupVersionResource {
            group: group.into(),
            version: "v1".into(),
            resource: resource.into(),
        };
        let user = UserInfo {
            username: "alice".into(),
            uid: String::new(),
            groups: vec![],
            extra: Default::default(),
        };
        let admission = Admission {
            state,
            kind: &gvk,
            resource: &gvr,
            subresource,
            namespace: Some("default"),
            user: &user,
            dry_run: false,
            forbid_create: false,
        };
        let op = if old.is_some() {
            Operation::Update
        } else {
            Operation::Create
        };
        let old: Option<T> = old.map(|o| serde_json::from_value(o).unwrap());
        let obj: T = serde_json::from_value(obj).unwrap();
        let obj = admission.admit(op.clone(), obj, old.as_ref()).await?;
        admission
            .validate(
                &RequestContext::new(Some("default")),
                op,
                Some(&obj),
                old.as_ref(),
            )
            .await?;
        Ok(obj)
    }

    fn pod(containers: Value) -> Value {
        json!({"apiVersion": "v1", "kind": "Pod",
            "metadata": {"name": "p", "namespace": "default"},
            "spec": {"containers": containers}})
    }

    async fn run_pod(
        enabled: &[&str],
        sub: Option<&str>,
        obj: Value,
        old: Option<Value>,
    ) -> Result<rusternetes_common::resources::Pod> {
        let _g = EnabledForTest::new(enabled);
        run(&state(), ("", "Pod", "pods"), sub, obj, old).await
    }

    fn msg(e: Error) -> String {
        match e {
            Error::Forbidden(m) => m,
            Error::Status(s) => s.message.unwrap_or_default(),
            other => other.to_string(),
        }
    }

    // plugin/pkg/admission/deny/admission.go:46-54: AlwaysDeny denies every
    // request through NewForbidden.
    #[tokio::test]
    async fn always_deny_denies_everything() {
        let e = run_pod(
            &["AlwaysDeny"],
            None,
            pod(json!([{"name":"c","image":"i"}])),
            None,
        )
        .await
        .unwrap_err();
        assert!(matches!(e, Error::Forbidden(_)), "{e:?}");
        assert_eq!(
            msg(e),
            "pods \"p\" is forbidden: admission control is denying all modifications"
        );
    }

    #[tokio::test]
    async fn always_admit_and_default_off() {
        run_pod(
            &["AlwaysAdmit"],
            None,
            pod(json!([{"name":"c","image":"i"}])),
            None,
        )
        .await
        .unwrap();
        // AlwaysDeny is default-off.
        run_pod(
            &["ServiceAccount"],
            None,
            pod(json!([{"name":"c","image":"i"}])),
            None,
        )
        .await
        .unwrap();
        let e = run_pod(
            &["AlwaysAdmit", "AlwaysDeny"],
            None,
            pod(json!([{"name":"c","image":"i"}])),
            None,
        )
        .await;
        assert!(e.is_err(), "AlwaysDeny still denies beside AlwaysAdmit");
    }

    fn svc(ips: &[&str]) -> Value {
        json!({"apiVersion":"v1","kind":"Service",
            "metadata":{"name":"s","namespace":"default"},
            "spec":{"externalIPs": ips}})
    }

    // denyserviceexternalips/admission_test.go TestAdmission (all nine cases).
    #[tokio::test]
    async fn deny_service_external_ips() {
        type Case<'a> = (&'a str, &'a [&'a str], Option<&'a [&'a str]>, bool);
        let cases: &[Case] = &[
            ("create: without externalIPs", &[], None, false),
            ("create: with externalIPs", &["1.1.1.1"], None, true),
            (
                "update: same",
                &["1.1.1.1", "2.2.2.2"],
                Some(&["1.1.1.1", "2.2.2.2"]),
                false,
            ),
            (
                "update: reorder",
                &["1.1.1.1", "2.2.2.2"],
                Some(&["2.2.2.2", "1.1.1.1"]),
                false,
            ),
            (
                "update: change",
                &["1.1.1.1", "2.2.2.2"],
                Some(&["1.1.1.1", "3.3.3.3"]),
                true,
            ),
            (
                "update: add",
                &["1.1.1.1", "2.2.2.2"],
                Some(&["1.1.1.1"]),
                true,
            ),
            ("update: erase", &[], Some(&["1.1.1.1", "2.2.2.2"]), false),
            (
                "update: reduce from back",
                &["1.1.1.1"],
                Some(&["1.1.1.1", "2.2.2.2"]),
                false,
            ),
            (
                "update: reduce from front",
                &["2.2.2.2"],
                Some(&["1.1.1.1", "2.2.2.2"]),
                false,
            ),
        ];
        for (name, new, old, fail) in cases {
            let _g = EnabledForTest::new(&["DenyServiceExternalIPs"]);
            let r = run::<rusternetes_common::resources::Service>(
                &state(),
                ("", "Service", "services"),
                None,
                svc(new),
                old.map(svc),
            )
            .await;
            assert_eq!(r.is_err(), *fail, "{name}: {r:?}");
            if let Err(e) = r {
                assert_eq!(
                    msg(e),
                    "services \"s\" is forbidden: Use of external IPs is denied by admission control",
                    "{name}"
                );
            }
        }
        // Default-off: the same request passes.
        let _g = EnabledForTest::new(&[]);
        run::<rusternetes_common::resources::Service>(
            &state(),
            ("", "Service", "services"),
            None,
            svc(&["1.1.1.1"]),
            None,
        )
        .await
        .unwrap();
    }

    // alwayspullimages/admission_test.go TestAdmission: every init, regular
    // and ephemeral container (and image volume, KEP-4639) is set to Always.
    #[tokio::test]
    async fn always_pull_images_admit_sets_always() {
        let p = json!({"apiVersion":"v1","kind":"Pod",
            "metadata":{"name":"p","namespace":"default"},
            "spec":{
                "initContainers":[{"name":"i","image":"i","imagePullPolicy":"IfNotPresent"}],
                "containers":[{"name":"c","image":"i","imagePullPolicy":"Never"},{"name":"d","image":"i"}],
                "ephemeralContainers":[{"name":"e","image":"i","imagePullPolicy":"Never"}],
                "volumes":[{"name":"v","image":{"reference":"r","pullPolicy":"Never"}}]}});
        let got = run_pod(&["AlwaysPullImages"], None, p, None).await.unwrap();
        let spec = got.spec.unwrap();
        let always = Some("Always".to_string());
        assert_eq!(spec.init_containers.unwrap()[0].image_pull_policy, always);
        assert_eq!(spec.containers[0].image_pull_policy, always);
        assert_eq!(spec.containers[1].image_pull_policy, always);
        assert_eq!(
            spec.ephemeral_containers.unwrap()[0].image_pull_policy,
            always
        );
        assert_eq!(
            spec.volumes.unwrap()[0].image.as_ref().unwrap().pull_policy,
            always
        );
    }

    // alwayspullimages/admission_test.go TestOtherResources + TestUpdatePod.
    #[tokio::test]
    async fn always_pull_images_ignores_subresources_and_unchanged_images() {
        let never = pod(json!([{"name":"c","image":"image","imagePullPolicy":"Never"}]));
        let got = run_pod(&["AlwaysPullImages"], Some("exec"), never, None)
            .await
            .unwrap();
        assert_eq!(
            got.spec.unwrap().containers[0].image_pull_policy.as_deref(),
            Some("Never"),
            "pod subresource"
        );
        // An update that adds no image is left alone.
        let old = pod(json!([{"name":"c","image":"image","imagePullPolicy":"IfNotPresent"}]));
        let got = run_pod(&["AlwaysPullImages"], None, old.clone(), Some(old.clone()))
            .await
            .unwrap();
        assert_eq!(
            got.spec.unwrap().containers[0].image_pull_policy.as_deref(),
            Some("IfNotPresent")
        );
        // ... one that changes an image is mutated.
        let new = pod(json!([{"name":"c","image":"image2","imagePullPolicy":"IfNotPresent"}]));
        let got = run_pod(&["AlwaysPullImages"], None, new, Some(old))
            .await
            .unwrap();
        assert_eq!(
            got.spec.unwrap().containers[0].image_pull_policy.as_deref(),
            Some("Always")
        );
    }

    // Validate (alwayspullimages/admission.go:92-124): the aggregate of
    // Forbidden/NotSupported errors is not an APIStatus, so it is a 500 with
    // the aggregate message (`Error()` of a multi-error aggregate is
    // `[a, b]`). Unreachable through the chain (Admit has just run), so the
    // validator is called directly.
    #[test]
    fn always_pull_images_validate_message() {
        let pod: rusternetes_common::resources::Pod = serde_json::from_value(json!({
            "apiVersion":"v1","kind":"Pod","metadata":{"name":"p"},
            "spec":{"initContainers":[{"name":"i","image":"i","imagePullPolicy":"Never"}],
                    "containers":[{"name":"c","image":"i","imagePullPolicy":"IfNotPresent"}],
                    "volumes":[{"name":"v","image":{"reference":"r"}}]}}))
        .unwrap();
        let e = rusternetes_api_server::admission::builtin_plugins::always_pull_images_validate(
            "pods", &pod,
        )
        .unwrap_err();
        assert_eq!(
            msg(e),
            "[pods \"p\" is forbidden: spec.initContainers[0].imagePullPolicy: Unsupported value: \"Never\": supported values: \"Always\", \
pods \"p\" is forbidden: spec.containers[0].imagePullPolicy: Unsupported value: \"IfNotPresent\": supported values: \"Always\", \
pods \"p\" is forbidden: spec.volumes[0].image.pullPolicy: Unsupported value: \"\": supported values: \"Always\"]"
        );
    }

    // antiaffinity/admission_test.go TestInterPodAffinityAdmission.
    #[tokio::test]
    async fn limit_pod_hard_anti_affinity_topology() {
        let with = |key: &str| {
            json!({"apiVersion":"v1","kind":"Pod","metadata":{"name":"p","namespace":"default"},
                "spec":{"containers":[{"name":"c","image":"i"}],
                "affinity":{"podAntiAffinity":{"requiredDuringSchedulingIgnoredDuringExecution":
                    [{"labelSelector":{"matchLabels":{"a":"b"}},"topologyKey":key}]}}}})
        };
        let on = ["LimitPodHardAntiAffinityTopology"];
        run_pod(&on, None, with("kubernetes.io/hostname"), None)
            .await
            .unwrap();
        let e = run_pod(&on, None, with("zone"), None).await.unwrap_err();
        assert_eq!(
            msg(e),
            "pods \"p\" is forbidden: affinity.PodAntiAffinity.RequiredDuringScheduling has TopologyKey zone but only key kubernetes.io/hostname is allowed"
        );
        // Subresources are ignored; default-off.
        run_pod(&on, Some("status"), with("zone"), Some(with("zone")))
            .await
            .unwrap();
        run_pod(&[], None, with("zone"), None).await.unwrap();
        // No anti-affinity at all.
        run_pod(&on, None, pod(json!([{"name":"c","image":"i"}])), None)
            .await
            .unwrap();
    }

    fn pvc(size: &str, class: Option<&str>, phase: &str) -> Value {
        let mut spec = json!({"resources":{"requests":{"storage": size}}});
        if let Some(c) = class {
            spec["storageClassName"] = json!(c);
        }
        json!({"apiVersion":"v1","kind":"PersistentVolumeClaim",
            "metadata":{"name":"c","namespace":"default"},
            "spec":spec,"status":{"phase":phase}})
    }

    async fn storage_class(state: &ApiServerState, name: &str, expand: Option<bool>) {
        let mut sc = json!({"apiVersion":"storage.k8s.io/v1","kind":"StorageClass",
            "metadata":{"name":name},"provisioner":"kubernetes.io/glusterfs"});
        if let Some(e) = expand {
            sc["allowVolumeExpansion"] = json!(e);
        }
        state
            .storage
            .create(&format!("/registry/storageclasses/{name}"), &sc)
            .await
            .unwrap();
    }

    // resize/admission_test.go TestPVCResizeAdmission.
    #[tokio::test]
    async fn pvc_resize() {
        let _g = EnabledForTest::new(&["PersistentVolumeClaimResize"]);
        let state = state();
        storage_class(&state, "gold", Some(true)).await;
        storage_class(&state, "silver", Some(false)).await;
        storage_class(&state, "nil", None).await;
        let id = ("", "PersistentVolumeClaim", "persistentvolumeclaims");
        type P = rusternetes_common::resources::PersistentVolumeClaim;
        // gold allows expansion.
        run::<P>(
            &state,
            id,
            None,
            pvc("2Gi", Some("gold"), "Bound"),
            Some(pvc("1Gi", Some("gold"), "Bound")),
        )
        .await
        .unwrap();
        // silver and an unset flag do not.
        for class in ["silver", "nil"] {
            let e = run::<P>(
                &state,
                id,
                None,
                pvc("2Gi", Some(class), "Bound"),
                Some(pvc("1Gi", Some(class), "Bound")),
            )
            .await
            .unwrap_err();
            assert_eq!(
                msg(e),
                "persistentvolumeclaims \"c\" is forbidden: only dynamically provisioned pvc can be resized and the storageclass that provisions the pvc must support resize"
            );
        }
        // No class at all.
        let e = run::<P>(
            &state,
            id,
            None,
            pvc("2Gi", None, "Bound"),
            Some(pvc("1Gi", None, "Bound")),
        )
        .await
        .unwrap_err();
        assert!(msg(e).contains("only dynamically provisioned pvc"));
        // No change in size.
        run::<P>(
            &state,
            id,
            None,
            pvc("1Gi", Some("silver"), "Bound"),
            Some(pvc("1Gi", Some("silver"), "Pending")),
        )
        .await
        .unwrap();
        // Shrinking is not this plugin's business.
        run::<P>(
            &state,
            id,
            None,
            pvc("1Gi", Some("silver"), "Bound"),
            Some(pvc("2Gi", Some("silver"), "Bound")),
        )
        .await
        .unwrap();
        // Pending claim.
        let e = run::<P>(
            &state,
            id,
            None,
            pvc("2Gi", Some("silver"), "Pending"),
            Some(pvc("1Gi", Some("silver"), "Pending")),
        )
        .await
        .unwrap_err();
        assert!(msg(e).contains("Only bound persistent volume claims can be expanded"));
        // Subresource: ignored.
        run::<P>(
            &state,
            id,
            Some("status"),
            pvc("2Gi", Some("silver"), "Pending"),
            Some(pvc("1Gi", Some("silver"), "Pending")),
        )
        .await
        .unwrap();
    }

    // defaultingressclass/admission_test.go TestAdmission.
    #[tokio::test]
    async fn default_ingress_class() {
        let _g = EnabledForTest::new(&["DefaultIngressClass"]);
        let state = state();
        let ing = |spec: Value, ann: Option<Value>| {
            let mut v = json!({"apiVersion":"networking.k8s.io/v1","kind":"Ingress",
                "metadata":{"name":"i","namespace":"default"},"spec":spec});
            if let Some(a) = ann {
                v["metadata"]["annotations"] = a;
            }
            v
        };
        let id = ("networking.k8s.io", "Ingress", "ingresses");
        type I = rusternetes_common::resources::Ingress;
        let class = |name: &str, default: Option<&str>, created: Option<&str>| {
            let mut c = json!({"apiVersion":"networking.k8s.io/v1","kind":"IngressClass",
                "metadata":{"name":name},"spec":{"controller":"example.com/c"}});
            if let Some(d) = default {
                c["metadata"]["annotations"] =
                    json!({"ingressclass.kubernetes.io/is-default-class": d});
            }
            if let Some(t) = created {
                c["metadata"]["creationTimestamp"] = json!(t);
            }
            c
        };
        let put = |name: &str, c: Value| {
            let st = state.storage.clone();
            let key = format!("/registry/ingressclasses/{name}");
            async move { st.create(&key, &c).await.unwrap() }
        };
        // No default: untouched.
        put("nondefault1", class("nondefault1", Some("false"), None)).await;
        put("nodefault", class("nodefault", None, None)).await;
        put("emptydefault", class("emptydefault", Some(""), None)).await;
        let got: I = run(&state, id, None, ing(json!({}), None), None)
            .await
            .unwrap();
        assert_eq!(got.spec.unwrap().ingress_class_name, None);
        // One default.
        // Both defaults carry an explicit, equal creation time: storage stamps
        // `creationTimestamp` at whole-second precision, so leaving it to the
        // clock lets the two creates straddle a second boundary and the later
        // class wins on "newest first" instead of the name tie-break.
        let same = Some("2000-01-01T00:00:00Z");
        put("default1", class("default1", Some("true"), same)).await;
        let got: I = run(&state, id, None, ing(json!({}), None), None)
            .await
            .unwrap();
        assert_eq!(
            got.spec.unwrap().ingress_class_name.as_deref(),
            Some("default1")
        );
        // Explicit "" and "foo" are kept.
        for v in ["", "foo"] {
            let got: I = run(
                &state,
                id,
                None,
                ing(json!({"ingressClassName": v}), None),
                None,
            )
            .await
            .unwrap();
            assert_eq!(got.spec.unwrap().ingress_class_name.as_deref(), Some(v));
        }
        // The annotation counts as a class.
        let got: I = run(
            &state,
            id,
            None,
            ing(
                json!({}),
                Some(json!({"kubernetes.io/ingress.class":"foo"})),
            ),
            None,
        )
        .await
        .unwrap();
        assert_eq!(got.spec.unwrap().ingress_class_name, None);
        // Two defaults, same creation time: the lower name.
        put("default2", class("default2", Some("true"), same)).await;
        let got: I = run(&state, id, None, ing(json!({}), None), None)
            .await
            .unwrap();
        assert_eq!(
            got.spec.unwrap().ingress_class_name.as_deref(),
            Some("default1")
        );
        // Newer creation time wins.
        put(
            "newer",
            class("newer", Some("true"), Some("2099-01-01T00:00:00Z")),
        )
        .await;
        let got: I = run(&state, id, None, ing(json!({}), None), None)
            .await
            .unwrap();
        assert_eq!(
            got.spec.unwrap().ingress_class_name.as_deref(),
            Some("newer")
        );
        // Update (the plugin handles Create only).
        let got: I = run(
            &state,
            id,
            None,
            ing(json!({}), None),
            Some(ing(json!({}), None)),
        )
        .await
        .unwrap();
        assert_eq!(got.spec.unwrap().ingress_class_name, None);
    }

    // nodetaint/admission_test.go Test_nodeTaints: the three cases, plus the
    // plugin's `NewHandler(Create)` and the gate.
    async fn run_node(enabled: &[&str], obj: Value, old: Option<Value>) -> Value {
        let _g = EnabledForTest::new(enabled);
        let node: rusternetes_common::resources::Node =
            run(&state(), ("", "Node", "nodes"), None, obj, old)
                .await
                .unwrap();
        serde_json::to_value(node).unwrap()["spec"]["taints"].clone()
    }

    fn not_ready_taint() -> Value {
        json!({"key": "node.kubernetes.io/not-ready", "value": null, "effect": "NoSchedule"})
    }

    #[tokio::test]
    async fn taint_nodes_by_condition_adds_not_ready_on_create() {
        let node = json!({"apiVersion": "v1", "kind": "Node", "metadata": {"name": "mynode"}});
        let on = ["TaintNodesByCondition"];
        // "notReady taint is added on creation"
        assert_eq!(
            run_node(&on, node.clone(), None).await,
            json!([not_ready_taint()])
        );
        // "already tainted node is not tainted again"
        let mut tainted = node.clone();
        tainted["spec"] =
            json!({"taints": [{"key": "node.kubernetes.io/not-ready", "effect": "NoSchedule"}]});
        let got = run_node(&on, tainted, None).await;
        assert_eq!(got.as_array().unwrap().len(), 1, "{got}");
        // "NotReady taint is added to an unready node as well"
        let mut unready = node.clone();
        unready["status"] = json!({"conditions": [{"type": "Ready", "status": "False"}]});
        assert_eq!(
            run_node(&on, unready, None).await,
            json!([not_ready_taint()])
        );
        // A taint with another effect is a different taint (MatchTaint).
        let mut other = node.clone();
        other["spec"] =
            json!({"taints": [{"key": "node.kubernetes.io/not-ready", "effect": "NoExecute"}]});
        assert_eq!(
            run_node(&on, other, None).await.as_array().unwrap().len(),
            2
        );
        // Update is not handled (NewHandler(Create)); a disabled plugin does nothing.
        assert!(run_node(&on, node.clone(), Some(node.clone()))
            .await
            .is_null());
        assert!(run_node(&["ServiceAccount"], node, None).await.is_null());
    }
}
