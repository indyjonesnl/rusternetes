//! PodSecurity's handling of a Namespace create/update: label validation
//! and the dry-run of existing pods against a new enforce level.
//!
//! Ported from `Admission.ValidateNamespace`
//! (staging/src/k8s.io/pod-security-admission/admission/admission.go:229-327),
//! `EvaluatePodsInNamespace` (:539-606), `decoratePodWarnings` (:608-626),
//! `prioritizePods` (:697-722) and `exemptNamespaceWarning` (:733-775).

use super::pod_security_api::{self as api, Level, LevelVersion, Policy};
use super::pod_security_policy::aggregate_check_results;
use super::{pod_security_registry, PodSecurityAdmission};
use rusternetes_common::resources::{Namespace, Pod};
use rusternetes_common::{Error, Result};
use rusternetes_storage::Storage;
use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::time::{Duration, Instant};

/// `defaultNamespaceMaxPodsToCheck` (admission.go:47).
pub const DEFAULT_NAMESPACE_MAX_PODS_TO_CHECK: usize = 3000;
/// `defaultNamespacePodCheckTimeout` (admission.go:48).
pub const DEFAULT_NAMESPACE_POD_CHECK_TIMEOUT: Duration = Duration::from_secs(1);

/// `Admission.prioritizePods` (admission.go:697-722): drop pods of an
/// exempt runtime class, then put the first pod of each controller ahead of
/// the replicas that follow it.
pub fn prioritize_pods(
    pods: Vec<Pod>,
    exempt_runtime_class: impl Fn(Option<&str>) -> bool,
) -> Vec<Pod> {
    let mut prioritized = Vec::with_capacity(pods.len());
    let mut duplicate_replicated = Vec::new();
    let mut evaluated_controllers: HashSet<String> = HashSet::new();
    for pod in pods {
        if exempt_runtime_class(
            pod.spec
                .as_ref()
                .and_then(|s| s.runtime_class_name.as_deref()),
        ) {
            continue;
        }
        // metav1.GetControllerOfNoCopy: the owner reference with controller=true.
        let controller_uid = pod
            .metadata
            .owner_references
            .iter()
            .flatten()
            .find(|r| r.controller == Some(true))
            .map(|r| r.uid.clone());
        match controller_uid {
            None => prioritized.push(pod),
            Some(uid) if evaluated_controllers.contains(&uid) => duplicate_replicated.push(pod),
            Some(uid) => {
                evaluated_controllers.insert(uid);
                prioritized.push(pod);
            }
        }
    }
    prioritized.extend(duplicate_replicated);
    prioritized
}

/// `podCount` (admission.go): a warning's first pod name and pod count.
struct PodCount {
    pod_name: String,
    pod_count: usize,
}

/// `decoratePodWarnings` (admission.go:608-626): prefix each warning with the
/// pod names that produced it.
fn decorate_pod_warnings(counts: &HashMap<String, PodCount>, warnings: &mut [String]) {
    for warning in warnings.iter_mut() {
        let Some(c) = counts.get(warning.as_str()) else {
            continue;
        };
        *warning = match c.pod_count {
            0 => continue, // unexpected, leave the warning alone
            1 => format!("{}: {warning}", c.pod_name),
            2 => format!("{} (and 1 other pod): {warning}", c.pod_name),
            n => format!("{} (and {} other pods): {warning}", c.pod_name, n - 1),
        };
    }
}

/// `exemptNamespaceWarning` (admission.go:733-775): a warning when an exempt
/// namespace sets Pod Security labels that will be ignored, else `None`.
pub fn exempt_namespace_warning(
    exempt_namespace: &str,
    policy: &Policy,
    labels: Option<&HashMap<String, String>>,
    default_policy: &Policy,
) -> Option<String> {
    if policy.fully_privileged() || policy.equivalent(default_policy) {
        return None;
    }
    let has = |k: &str| labels.is_some_and(|l| l.contains_key(k));
    let mut parts: Vec<String> = Vec::new();
    for (mode, lv, level_label, version_label) in [
        (
            "enforce",
            &policy.enforce,
            api::ENFORCE_LEVEL_LABEL,
            api::ENFORCE_VERSION_LABEL,
        ),
        (
            "audit",
            &policy.audit,
            api::AUDIT_LEVEL_LABEL,
            api::AUDIT_VERSION_LABEL,
        ),
        (
            "warn",
            &policy.warn,
            api::WARN_LEVEL_LABEL,
            api::WARN_VERSION_LABEL,
        ),
    ] {
        if lv.level != Level::Privileged && (has(level_label) || has(version_label)) {
            parts.push(format!("{mode}={lv}"));
        }
    }
    Some(format!(
        "namespace {exempt_namespace:?} is exempt from Pod Security, and the policy ({}) will be ignored",
        parts.join(", ")
    ))
}

const LIST_FAILED: &str = "failed to list pods while checking new PodSecurity enforce level";

impl PodSecurityAdmission {
    /// `Admission.ValidateNamespace` (admission.go:229-327) for a namespace
    /// create (`old` is `None`) or update. Returns the response warnings;
    /// invalid labels are `NewInvalid` (`invalidResponse`, response.go:60-65).
    /// The caller skips subresource requests (:231-234).
    pub async fn validate_namespace<S: Storage>(
        &self,
        storage: &Arc<S>,
        namespace: &Namespace,
        old: Option<&Namespace>,
    ) -> Result<Vec<String>> {
        let name = &namespace.metadata.name;
        let default = Policy::PRIVILEGED;
        let (new_policy, new_errs) =
            api::policy_to_evaluate(namespace.metadata.labels.as_ref(), default);
        let invalid = |errs| Error::new_invalid("", "Namespace", name, errs);
        let exempt_warning = || {
            exempt_namespace_warning(
                name,
                &new_policy,
                namespace.metadata.labels.as_ref(),
                &default,
            )
        };

        let Some(old) = old else {
            // require valid labels on create
            if !new_errs.is_empty() {
                return Err(invalid(new_errs));
            }
            if self.exemptions.exempt_namespace(name) {
                return Ok(exempt_warning().into_iter().collect());
            }
            return Ok(Vec::new());
        };

        let (old_policy, old_errs) = api::policy_to_evaluate(old.metadata.labels.as_ref(), default);
        // require valid labels on update if they have changed
        if !new_errs.is_empty() && (old_errs.is_empty() || new_errs != old_errs) {
            return Err(invalid(new_errs));
        }

        // Skip dry-running pods:
        // * if the enforce policy is unchanged
        // * if the new enforce policy is privileged
        // * if the new enforce is the same version and level was relaxed
        // * for exempt namespaces
        if new_policy.enforce == old_policy.enforce
            || new_policy.enforce.level == Level::Privileged
            || (new_policy.enforce.version == old_policy.enforce.version
                && api::compare_levels(new_policy.enforce.level, old_policy.enforce.level) < 1)
        {
            return Ok(Vec::new());
        }
        if self.exemptions.exempt_namespace(name) {
            return Ok(exempt_warning().into_iter().collect());
        }
        Ok(self
            .evaluate_pods_in_namespace(
                storage,
                name,
                new_policy.enforce,
                DEFAULT_NAMESPACE_MAX_PODS_TO_CHECK,
                DEFAULT_NAMESPACE_POD_CHECK_TIMEOUT,
            )
            .await)
    }

    /// `Admission.EvaluatePodsInNamespace` (admission.go:539-606): warnings
    /// for the existing pods that would violate `enforce`.
    pub async fn evaluate_pods_in_namespace<S: Storage>(
        &self,
        storage: &Arc<S>,
        namespace: &str,
        enforce: LevelVersion,
        max_pods: usize,
        timeout: Duration,
    ) -> Vec<String> {
        let deadline = Instant::now() + timeout;
        let prefix = rusternetes_storage::build_prefix("pods", Some(namespace));
        let pods = match tokio::time::timeout(timeout, storage.list::<Pod>(&prefix)).await {
            Ok(Ok(pods)) => pods,
            _ => {
                tracing::error!("PodSecurity: failed to list pods in {namespace:?}");
                return vec![LIST_FAILED.to_string()];
            }
        };

        let mut prioritized = prioritize_pods(pods, |rc| self.exemptions.exempt_runtime_class(rc));
        let total_pods = prioritized.len();
        prioritized.truncate(max_pods);

        let mut pod_warnings: Vec<String> = Vec::new();
        let mut counts: HashMap<String, PodCount> = HashMap::new();
        let default_spec = rusternetes_common::resources::pod::PodSpec::default();
        let mut checked_pods = prioritized.len();
        for (i, pod) in prioritized.iter().enumerate() {
            let spec = pod.spec.as_ref().unwrap_or(&default_spec);
            let r = aggregate_check_results(&pod_security_registry().evaluate_pod(
                enforce,
                &pod.metadata,
                spec,
            ));
            if !r.allowed {
                let warning = r.forbidden_reason();
                let name = &pod.metadata.name;
                match counts.get_mut(&warning) {
                    None => {
                        pod_warnings.push(warning.clone());
                        counts.insert(
                            warning,
                            PodCount {
                                pod_name: name.clone(),
                                pod_count: 1,
                            },
                        );
                    }
                    Some(c) => {
                        if *name < c.pod_name {
                            c.pod_name = name.clone();
                        }
                        c.pod_count += 1;
                    }
                }
            }
            if Instant::now() >= deadline {
                // deadline exceeded
                checked_pods = i + 1;
                break;
            }
        }

        let mut warnings = Vec::new();
        if checked_pods < total_pods {
            warnings.push(format!(
                "new PodSecurity enforce level only checked against the first {checked_pods} of {total_pods} existing pods"
            ));
        }
        if !pod_warnings.is_empty() {
            warnings.push(format!(
                "existing pods in namespace {namespace:?} violate the new PodSecurity enforce level \"{enforce}\""
            ));
        }
        decorate_pod_warnings(&counts, &mut pod_warnings);
        pod_warnings.sort();
        warnings.extend(pod_warnings);
        warnings
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::admission::PodSecurityExemptions;
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
