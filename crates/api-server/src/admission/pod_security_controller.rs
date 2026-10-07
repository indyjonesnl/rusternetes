//! PodSecurity's handling of a pod controller (Deployment, Job, ...): the
//! warn and audit evaluation of the pod template it embeds. Never enforcing.
//!
//! Ported from `Admission.ValidatePodController`
//! (staging/src/k8s.io/pod-security-admission/admission/admission.go:393-453),
//! `EvaluatePod` with `enforce=false` (:455-528) and
//! `DefaultPodSpecExtractor` (:83-150). `Validate` routes every resource
//! other than pods and namespaces to `ValidatePodController` (:211-222), and
//! the plugin only runs it for resources with `HasPodSpec`
//! (plugin/pkg/admission/security/podsecurity/admission.go:193-197).

use super::pod_security_api::{self as api, Level, LevelVersion};
use super::pod_security_policy::aggregate_check_results;
use super::{pod_security_registry, PodSecurityAdmission, PodSecurityOutcome};
use rusternetes_common::resources::pod::PodSpec;
use rusternetes_common::types::ObjectMeta;
use rusternetes_storage::Storage;
use serde_json::Value;
use std::collections::{BTreeMap, HashMap};
use std::sync::Arc;

/// `defaultPodSpecResources` (admission.go:94-104), `(group, resource)`.
const POD_SPEC_RESOURCES: &[(&str, &str)] = &[
    ("", "pods"),
    ("", "replicationcontrollers"),
    ("", "podtemplates"),
    ("apps", "replicasets"),
    ("apps", "deployments"),
    ("apps", "statefulsets"),
    ("apps", "daemonsets"),
    ("batch", "jobs"),
    ("batch", "cronjobs"),
];

/// `DefaultPodSpecExtractor.HasPodSpec` (admission.go:108-110).
pub fn has_pod_spec(group: &str, resource: &str) -> bool {
    POD_SPEC_RESOURCES.contains(&(group, resource))
}

/// `extractPodSpecFromTemplate` (admission.go:145-150): a missing template
/// is `nil, nil, nil`.
fn from_template(template: Option<&Value>) -> Result<Option<(ObjectMeta, PodSpec)>, String> {
    let Some(t) = template.filter(|t| !t.is_null()) else {
        return Ok(None);
    };
    let meta = match t.get("metadata").filter(|m| !m.is_null()) {
        Some(m) => serde_json::from_value(m.clone()).map_err(|e| e.to_string())?,
        None => ObjectMeta::default(),
    };
    let spec = match t.get("spec").filter(|s| !s.is_null()) {
        Some(s) => serde_json::from_value(s.clone()).map_err(|e| e.to_string())?,
        None => PodSpec::default(),
    };
    Ok(Some((meta, spec)))
}

/// `DefaultPodSpecExtractor.ExtractPodSpec` (admission.go:112-135) over the
/// decoded object. `Ok(None)` is upstream's `nil, nil, nil`; `Err` is the
/// "unexpected object type" or a decode failure, which never blocks
/// admission.
pub fn extract_pod_spec(
    group: &str,
    resource: &str,
    obj: &Value,
) -> Result<Option<(ObjectMeta, PodSpec)>, String> {
    let at = |path: &[&str]| path.iter().try_fold(obj, |v, k| v.get(*k));
    match (group, resource) {
        ("", "pods") => from_template(Some(obj)),
        ("", "podtemplates") => from_template(at(&["template"])),
        ("", "replicationcontrollers")
        | ("apps", "replicasets" | "deployments" | "daemonsets" | "statefulsets")
        | ("batch", "jobs") => from_template(at(&["spec", "template"])),
        ("batch", "cronjobs") => from_template(at(&["spec", "jobTemplate", "spec", "template"])),
        _ => Err(format!("unexpected object type: {group}/{resource}")),
    }
}

impl PodSecurityAdmission {
    /// `Admission.ValidatePodController` (admission.go:393-453) for a
    /// CREATE or UPDATE of `group`/`resource`. Never denies: the outcome
    /// carries the warn warnings and the audit annotations. Failures to
    /// look up the namespace, decode the object or extract the template
    /// are an `error` audit annotation (:411-416, :428-433, :437-442).
    #[allow(clippy::too_many_arguments)]
    pub async fn validate_pod_controller<S: Storage>(
        &self,
        storage: &Arc<S>,
        namespace: &str,
        subresource: Option<&str>,
        group: &str,
        resource: &str,
        obj: &Value,
        username: &str,
    ) -> PodSecurityOutcome {
        let annotated = |k: &str, v: String| PodSecurityOutcome {
            warnings: Vec::new(),
            audit_annotations: BTreeMap::from([(k.to_string(), v)]),
        };
        // short-circuit on subresources (:395-397)
        if subresource.is_some_and(|s| !s.is_empty()) {
            return PodSecurityOutcome::default();
        }
        // exempt namespaces and users (:399-408)
        if self.exemptions.exempt_namespace(namespace) {
            return annotated("exempt", "namespace".into());
        }
        if self.exemptions.exempt_user(username) {
            return annotated("exempt", "user".into());
        }
        let ns_key = rusternetes_storage::build_key("namespaces", None, namespace);
        let labels = match storage
            .get::<rusternetes_common::resources::Namespace>(&ns_key)
            .await
        {
            Ok(ns) => ns.metadata.labels,
            Err(e) => {
                return annotated(
                    "error",
                    format!("failed to lookup namespace {namespace:?}: {e}"),
                )
            }
        };
        let (policy, policy_errs) =
            api::policy_to_evaluate(labels.as_ref(), api::Policy::PRIVILEGED);
        // short-circuit on privileged audit+warn namespaces (:420-423)
        if policy_errs.is_empty()
            && policy.warn.level == Level::Privileged
            && policy.audit.level == Level::Privileged
        {
            return PodSecurityOutcome::default();
        }
        let (meta, spec) = match extract_pod_spec(group, resource, obj) {
            Ok(Some(t)) => t,
            // a controller with an optional pod spec and none: skip (:444-447)
            Ok(None) => return PodSecurityOutcome::default(),
            Err(e) => return annotated("error", format!("failed to extract pod template: {e}")),
        };
        // EvaluatePod with enforce=false (:455-528)
        if self
            .exemptions
            .exempt_runtime_class(spec.runtime_class_name.as_deref())
        {
            return annotated("exempt", "runtimeClass".into());
        }
        let mut annotations = BTreeMap::new();
        if !policy_errs.is_empty() {
            let joined = policy_errs
                .iter()
                .map(|e| e.to_string())
                .collect::<Vec<_>>()
                .join(", ");
            annotations.insert(
                "error".to_string(),
                format!("Failed to parse policy: [{joined}]"),
            );
        }
        let mut cache: HashMap<LevelVersion, Option<String>> = HashMap::new();
        let mut eval = |lv: LevelVersion| {
            cache
                .entry(lv)
                .or_insert_with(|| {
                    let r = aggregate_check_results(
                        &pod_security_registry().evaluate_pod(lv, &meta, &spec),
                    );
                    (!r.allowed).then(|| r.forbidden_detail())
                })
                .clone()
        };
        if let Some(detail) = eval(policy.audit) {
            annotations.insert(
                "audit-violations".to_string(),
                format!("would violate PodSecurity \"{}\": {detail}", policy.audit),
            );
        }
        let warnings = eval(policy.warn)
            .map(|detail| format!("would violate PodSecurity \"{}\": {detail}", policy.warn))
            .into_iter()
            .collect();
        PodSecurityOutcome {
            warnings,
            audit_annotations: annotations,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::admission::PodSecurityExemptions;
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
