//! Unit tests moved from `src/admission/pod_security_namespace.rs`.
#![allow(unused_imports, dead_code)]

mod __parent {
    pub use rusternetes_api_server::admission::pod_security_api::{
        self as api, Level, LevelVersion, Policy,
    };
    pub use rusternetes_api_server::admission::pod_security_policy::aggregate_check_results;
    pub use rusternetes_api_server::admission::{pod_security_registry, PodSecurityAdmission};
    pub use rusternetes_common::resources::{Namespace, Pod};
    pub use rusternetes_common::{Error, Result};
    pub use rusternetes_storage::Storage;
    pub use std::collections::{HashMap, HashSet};
    pub use std::sync::Arc;
    pub use std::time::{Duration, Instant};
}

mod tests {
    use super::__parent::*;
    use rusternetes_api_server::admission::pod_security_namespace::*;
    use rusternetes_api_server::admission::PodSecurityExemptions;
    use rusternetes_storage::MemoryStorage;

    fn ns(name: &str, labels: &[(&str, &str)]) -> Namespace {
        let labels: HashMap<String, String> = labels
            .iter()
            .map(|(k, v)| (format!("pod-security.kubernetes.io/{k}"), v.to_string()))
            .collect();
        serde_json::from_value(serde_json::json!({
            "apiVersion": "v1", "kind": "Namespace",
            "metadata": {"name": name, "labels": labels},
        }))
        .unwrap()
    }

    fn is_invalid(e: &Error) -> bool {
        matches!(e, Error::Status(s) if s.reason.as_deref() == Some("Invalid"))
    }

    fn pod(name: &str, privileged: bool, owner: Option<&str>) -> Pod {
        let mut v = serde_json::json!({
            "apiVersion": "v1", "kind": "Pod",
            "metadata": {"name": name, "namespace": "ns"},
            "spec": {"containers": [{"name": "c", "image": "busybox",
                "securityContext": {"privileged": privileged}}]},
        });
        if let Some(uid) = owner {
            v["metadata"]["ownerReferences"] = serde_json::json!([{
                "apiVersion": "apps/v1", "kind": "ReplicaSet", "name": "rs",
                "uid": uid, "controller": true}]);
        }
        serde_json::from_value(v).unwrap()
    }

    async fn store(pods: Vec<Pod>) -> Arc<MemoryStorage> {
        let s = Arc::new(MemoryStorage::new());
        for p in pods {
            let key = rusternetes_storage::build_key("pods", Some("ns"), &p.metadata.name);
            s.create(&key, &p).await.unwrap();
        }
        s
    }

    // admission_test.go TestValidateNamespace "invalid labels on create".
    #[tokio::test]
    async fn create_with_invalid_label_is_invalid() {
        let s = store(vec![]).await;
        let err = PodSecurityAdmission::new()
            .validate_namespace(&s, &ns("ns", &[("enforce", "bogus")]), None)
            .await
            .unwrap_err();
        assert!(is_invalid(&err), "{err:?}");
    }

    // admission.go:279-281: invalid labels that did not change are allowed.
    #[tokio::test]
    async fn update_with_unchanged_invalid_label_is_allowed() {
        let s = store(vec![]).await;
        let n = ns("ns", &[("enforce", "bogus")]);
        let w = PodSecurityAdmission::new()
            .validate_namespace(&s, &n, Some(&n))
            .await
            .unwrap();
        assert!(w.is_empty());
        // a changed invalid label is refused
        let err = PodSecurityAdmission::new()
            .validate_namespace(&s, &ns("ns", &[("enforce", "bogus2")]), Some(&n))
            .await
            .unwrap_err();
        assert!(is_invalid(&err), "{err:?}");
    }

    // admission.go:574-605: tightening enforce warns about existing pods.
    #[tokio::test]
    async fn tightening_enforce_warns_about_existing_pods() {
        let s = store(vec![
            pod("a", true, None),
            pod("b", true, None),
            pod("ok", false, None),
        ])
        .await;
        let w = PodSecurityAdmission::new()
            .validate_namespace(
                &s,
                &ns("ns", &[("enforce", "baseline")]),
                Some(&ns("ns", &[])),
            )
            .await
            .unwrap();
        assert_eq!(
            w,
            vec![
                "existing pods in namespace \"ns\" violate the new PodSecurity enforce level \"baseline:latest\"".to_string(),
                "a (and 1 other pod): privileged".to_string(),
            ]
        );
    }

    // admission.go:306-318: unchanged / privileged / relaxed skip the dry-run.
    #[tokio::test]
    async fn relaxing_or_unchanged_enforce_does_not_evaluate_pods() {
        let s = store(vec![pod("a", true, None)]).await;
        let psa = PodSecurityAdmission::new();
        let restricted = ns("ns", &[("enforce", "restricted")]);
        let baseline = ns("ns", &[("enforce", "baseline")]);
        for (new, old) in [
            (&restricted, &restricted),
            (&ns("ns", &[("enforce", "privileged")]), &restricted),
            (&baseline, &restricted),
        ] {
            assert!(psa
                .validate_namespace(&s, new, Some(old))
                .await
                .unwrap()
                .is_empty());
        }
    }

    // admission.go:319-326 + exemptNamespaceWarning.
    #[tokio::test]
    async fn exempt_namespace_warns_instead_of_evaluating() {
        let s = store(vec![pod("a", true, None)]).await;
        let psa = PodSecurityAdmission::with_exemptions(PodSecurityExemptions {
            namespaces: vec!["ns".into()],
            ..Default::default()
        });
        let n = ns("ns", &[("enforce", "baseline"), ("warn", "restricted")]);
        let expect = vec![
            "namespace \"ns\" is exempt from Pod Security, and the policy (enforce=baseline:latest, warn=restricted:latest) will be ignored".to_string(),
        ];
        assert_eq!(psa.validate_namespace(&s, &n, None).await.unwrap(), expect);
        assert_eq!(
            psa.validate_namespace(&s, &n, Some(&ns("ns", &[])))
                .await
                .unwrap(),
            expect
        );
    }

    // admission.go:697-722: one pod per controller first, replicas last.
    #[test]
    fn prioritize_pods_puts_replicas_last_and_drops_exempt_runtime_classes() {
        let mut exempt = pod("exempt", false, None);
        exempt.spec.as_mut().unwrap().runtime_class_name = Some("rc".into());
        let out = prioritize_pods(
            vec![
                pod("r1", false, Some("u1")),
                pod("r2", false, Some("u1")),
                exempt,
                pod("solo", false, None),
            ],
            |rc| rc == Some("rc"),
        );
        let names: Vec<_> = out.iter().map(|p| p.metadata.name.as_str()).collect();
        assert_eq!(names, ["r1", "solo", "r2"]);
    }

    // admission.go:566-569, :592-594: only the first N pods are checked.
    #[tokio::test]
    async fn max_pods_to_check_is_reported() {
        let s = store(vec![
            pod("a", true, None),
            pod("b", true, None),
            pod("c", true, None),
        ])
        .await;
        let enforce = LevelVersion::new(Level::Baseline, api::Version::LATEST);
        let w = PodSecurityAdmission::new()
            .evaluate_pods_in_namespace(&s, "ns", enforce, 2, Duration::from_secs(5))
            .await;
        assert_eq!(
            w[0],
            "new PodSecurity enforce level only checked against the first 2 of 3 existing pods"
        );
    }
}
