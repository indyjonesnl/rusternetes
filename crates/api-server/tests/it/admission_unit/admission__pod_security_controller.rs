//! Unit tests moved from `src/admission/pod_security_controller.rs`.
#![allow(unused_imports, dead_code)]

mod __parent {
    pub use rusternetes_api_server::admission::pod_security_api::{
        self as api, Level, LevelVersion,
    };
    pub use rusternetes_api_server::admission::pod_security_policy::aggregate_check_results;
    pub use rusternetes_api_server::admission::{
        pod_security_registry, PodSecurityAdmission, PodSecurityOutcome,
    };
    pub use rusternetes_common::resources::pod::PodSpec;
    pub use rusternetes_common::types::ObjectMeta;
    pub use rusternetes_storage::Storage;
    pub use serde_json::Value;
    pub use std::collections::{BTreeMap, HashMap};
    pub use std::sync::Arc;
}

mod tests {
    use super::__parent::*;
    use rusternetes_api_server::admission::pod_security_controller::*;
    use rusternetes_api_server::admission::PodSecurityExemptions;
    use serde_json::json;

    async fn ns(storage: &Arc<rusternetes_storage::MemoryStorage>, name: &str, l: &[(&str, &str)]) {
        let labels: BTreeMap<String, String> = l
            .iter()
            .map(|(k, v)| (format!("pod-security.kubernetes.io/{k}"), v.to_string()))
            .collect();
        let n: rusternetes_common::resources::Namespace = serde_json::from_value(json!({
            "apiVersion": "v1", "kind": "Namespace",
            "metadata": {"name": name, "labels": labels}}))
        .unwrap();
        storage
            .create(
                &rusternetes_storage::build_key("namespaces", None, name),
                &n,
            )
            .await
            .unwrap();
    }

    fn spec(privileged: bool) -> Value {
        json!({"containers": [{"name": "c", "image": "i",
            "securityContext": {"privileged": privileged}}]})
    }
    fn deployment(privileged: bool) -> Value {
        json!({"apiVersion": "apps/v1", "kind": "Deployment", "metadata": {"name": "d"},
            "spec": {"template": {"metadata": {"labels": {"a": "b"}}, "spec": spec(privileged)}}})
    }

    // admission_test.go `TestValidatePodAndController` runs every
    // `PodSpecResources` entry through the controller path.
    #[test]
    fn extractor_covers_every_default_resource() {
        let tpl = json!({"metadata": {"labels": {"a": "b"}}, "spec": spec(true)});
        let cases = [
            (
                "",
                "replicationcontrollers",
                json!({"spec": {"template": tpl}}),
            ),
            ("", "podtemplates", json!({"template": tpl})),
            ("apps", "replicasets", json!({"spec": {"template": tpl}})),
            ("apps", "deployments", json!({"spec": {"template": tpl}})),
            ("apps", "statefulsets", json!({"spec": {"template": tpl}})),
            ("apps", "daemonsets", json!({"spec": {"template": tpl}})),
            ("batch", "jobs", json!({"spec": {"template": tpl}})),
            (
                "batch",
                "cronjobs",
                json!({"spec": {"jobTemplate": {"spec": {"template": tpl}}}}),
            ),
        ];
        for (g, r, obj) in cases {
            assert!(has_pod_spec(g, r), "{g}/{r}");
            let (meta, s) = extract_pod_spec(g, r, &obj).unwrap().unwrap();
            assert_eq!(meta.labels.unwrap()["a"], "b", "{g}/{r}");
            assert_eq!(s.containers.len(), 1, "{g}/{r}");
        }
        assert!(!has_pod_spec("apps", "controllerrevisions"));
        // no template (an RC's template is optional): nil, nil, nil
        assert!(
            extract_pod_spec("", "replicationcontrollers", &json!({"spec": {}}))
                .unwrap()
                .is_none()
        );
        assert!(extract_pod_spec("apps", "nope", &json!({})).is_err());
    }

    #[tokio::test]
    async fn warn_and_audit_never_enforce() {
        let storage = Arc::new(rusternetes_storage::MemoryStorage::new());
        ns(
            &storage,
            "n",
            &[
                ("enforce", "restricted"),
                ("warn", "baseline"),
                ("audit", "baseline"),
            ],
        )
        .await;
        let out = PodSecurityAdmission::new()
            .validate_pod_controller(
                &storage,
                "n",
                None,
                "apps",
                "deployments",
                &deployment(true),
                "alice",
            )
            .await;
        assert_eq!(out.warnings.len(), 1, "{out:?}");
        assert!(out.warnings[0].starts_with(r#"would violate PodSecurity "baseline:latest": "#));
        assert!(out.audit_annotations["audit-violations"].starts_with("would violate PodSecurity"));
        assert!(!out.audit_annotations.contains_key("enforce-policy"));
    }

    #[tokio::test]
    async fn privileged_namespace_is_silent() {
        let storage = Arc::new(rusternetes_storage::MemoryStorage::new());
        ns(&storage, "n", &[("enforce", "privileged")]).await;
        let out = PodSecurityAdmission::new()
            .validate_pod_controller(
                &storage,
                "n",
                None,
                "apps",
                "deployments",
                &deployment(true),
                "alice",
            )
            .await;
        assert_eq!(out, PodSecurityOutcome::default());
    }

    #[tokio::test]
    async fn compliant_template_subresource_and_missing_namespace() {
        let storage = Arc::new(rusternetes_storage::MemoryStorage::new());
        ns(&storage, "n", &[("warn", "baseline")]).await;
        let psa = PodSecurityAdmission::new();
        let ok = psa
            .validate_pod_controller(
                &storage,
                "n",
                None,
                "apps",
                "deployments",
                &deployment(false),
                "a",
            )
            .await;
        assert!(
            ok.warnings.is_empty() && ok.audit_annotations.is_empty(),
            "{ok:?}"
        );
        let sub = psa
            .validate_pod_controller(
                &storage,
                "n",
                Some("scale"),
                "apps",
                "deployments",
                &deployment(true),
                "a",
            )
            .await;
        assert_eq!(sub, PodSecurityOutcome::default());
        // namespace lookup failure: allowed with an `error` annotation (:411-416)
        let gone = psa
            .validate_pod_controller(
                &storage,
                "missing",
                None,
                "apps",
                "deployments",
                &deployment(true),
                "a",
            )
            .await;
        assert!(gone.warnings.is_empty());
        assert!(
            gone.audit_annotations["error"].starts_with(r#"failed to lookup namespace "missing""#)
        );
    }

    #[tokio::test]
    async fn exemptions_short_circuit() {
        let storage = Arc::new(rusternetes_storage::MemoryStorage::new());
        ns(&storage, "n", &[("warn", "baseline")]).await;
        let psa = PodSecurityAdmission::with_exemptions(PodSecurityExemptions {
            usernames: vec!["bob".into()],
            namespaces: vec![],
            runtime_classes: vec!["rc".into()],
        });
        let job = |s: Value| json!({"spec": {"template": {"spec": s}}});
        let user = psa
            .validate_pod_controller(
                &storage,
                "n",
                None,
                "batch",
                "jobs",
                &job(spec(true)),
                "bob",
            )
            .await;
        assert_eq!(user.audit_annotations["exempt"], "user");
        let mut s = spec(true);
        s["runtimeClassName"] = json!("rc");
        let rc = psa
            .validate_pod_controller(&storage, "n", None, "batch", "jobs", &job(s), "alice")
            .await;
        assert_eq!(rc.audit_annotations["exempt"], "runtimeClass");
        assert!(rc.warnings.is_empty());
    }
}
