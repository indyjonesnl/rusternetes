//! Unit tests moved from `src/validation/apps.rs`.
#![allow(unused_imports, dead_code)]

mod __parent {
    pub use rusternetes_common::resources::deployment::{
        Deployment, DeploymentStatus, DeploymentStrategy, RollingUpdateDeployment,
    };
    pub use rusternetes_common::resources::policy::IntOrString;
    pub use rusternetes_common::resources::workloads::{
        DaemonSet, DaemonSetSpec, DaemonSetStatus, ReplicaSet, ReplicaSetSpec, ReplicaSetStatus,
        RollingUpdateStatefulSetStrategy, StatefulSet,
        StatefulSetPersistentVolumeClaimRetentionPolicy, StatefulSetSpec, StatefulSetStatus,
    };
    pub use rusternetes_common::types::LabelSelector;
    pub use rusternetes_common::validation::field::{BadValue, Error, ErrorList, Path};
    pub use rusternetes_common::validation::metav1::label_selector_matches_labels as selector_matches_labels;
    pub use rusternetes_common::validation::metav1::{
        is_dns1123_label, validate_label_selector, validate_labels, LabelSelectorValidationOptions,
    };
    pub use rusternetes_common::validation::objectmeta::{
        name_is_dns_label, name_is_dns_subdomain, validate_annotations, validate_immutable_field,
        validate_nonnegative_field, validate_object_meta, validate_object_meta_update,
    };
    pub use rusternetes_common::validation::podtemplate::validate_pod_template_spec;
    pub use rusternetes_common::validation::pvc::validate_persistent_volume_claim_spec;
}

mod selector_match_tests {
    use super::__parent::*;
    use rusternetes_common::validation::apps::*;

    fn labels(pairs: &[(&str, &str)]) -> std::collections::HashMap<String, String> {
        pairs
            .iter()
            .map(|(k, v)| (k.to_string(), v.to_string()))
            .collect()
    }

    fn sel(json: serde_json::Value) -> LabelSelector {
        serde_json::from_value(json).unwrap()
    }

    #[test]
    fn match_labels_still_enforced() {
        let s = sel(serde_json::json!({"matchLabels": {"app": "x"}}));
        assert!(selector_matches_labels(&s, &labels(&[("app", "x")])));
        assert!(!selector_matches_labels(&s, &labels(&[("app", "y")])));
        assert!(!selector_matches_labels(&s, &labels(&[("other", "x")])));
    }

    #[test]
    fn match_expressions_in_notin() {
        let in_sel = sel(serde_json::json!({"matchExpressions": [
            {"key": "tier", "operator": "In", "values": ["fe", "be"]}
        ]}));
        assert!(selector_matches_labels(&in_sel, &labels(&[("tier", "fe")])));
        assert!(!selector_matches_labels(
            &in_sel,
            &labels(&[("tier", "db")])
        ));
        assert!(!selector_matches_labels(&in_sel, &labels(&[("x", "y")]))); // key absent

        let notin = sel(serde_json::json!({"matchExpressions": [
            {"key": "tier", "operator": "NotIn", "values": ["db"]}
        ]}));
        assert!(selector_matches_labels(&notin, &labels(&[("tier", "fe")])));
        assert!(selector_matches_labels(&notin, &labels(&[("x", "y")]))); // absent => NotIn matches
        assert!(!selector_matches_labels(&notin, &labels(&[("tier", "db")])));
    }

    #[test]
    fn match_expressions_exists_doesnotexist() {
        let exists = sel(serde_json::json!({"matchExpressions": [
            {"key": "tier", "operator": "Exists"}
        ]}));
        assert!(selector_matches_labels(
            &exists,
            &labels(&[("tier", "anything")])
        ));
        assert!(!selector_matches_labels(&exists, &labels(&[("x", "y")])));

        let dne = sel(serde_json::json!({"matchExpressions": [
            {"key": "tier", "operator": "DoesNotExist"}
        ]}));
        assert!(selector_matches_labels(&dne, &labels(&[("x", "y")])));
        assert!(!selector_matches_labels(&dne, &labels(&[("tier", "fe")])));
    }

    #[test]
    fn template_mismatch_on_expressions_reports_error() {
        // selector requires tier In [fe]; template labels have tier=db -> mismatch
        let s = sel(serde_json::json!({"matchExpressions": [
            {"key": "tier", "operator": "In", "values": ["fe"]}
        ]}));
        let errs = template_labels_match_selector(
            &s,
            &labels(&[("tier", "db")]),
            &Path::new("spec").child("template"),
        );
        assert!(
            errs.iter()
                .any(|e| e.to_string().contains("does not match template")),
            "{errs:?}"
        );
        // matching template -> no error
        assert!(
            template_labels_match_selector(&s, &labels(&[("tier", "fe")]), &Path::new("spec"))
                .is_empty()
        );
    }
}

mod workload_parity_tests {
    use super::__parent::*;
    use rusternetes_common::validation::apps::*;

    fn agg(errs: &ErrorList) -> String {
        errs.iter()
            .map(|e| e.to_string())
            .collect::<Vec<_>>()
            .join(" | ")
    }

    fn deployment(json: serde_json::Value) -> Deployment {
        let mut d: Deployment = serde_json::from_value(json).unwrap();
        // `ValidateObjectMeta` requires the namespace of a namespaced object.
        d.metadata
            .namespace
            .get_or_insert_with(|| "default".to_string());
        d
    }
    fn statefulset(json: serde_json::Value) -> StatefulSet {
        serde_json::from_value(json).unwrap()
    }
    fn daemonset(json: serde_json::Value) -> DaemonSet {
        serde_json::from_value(json).unwrap()
    }
    fn replicaset(json: serde_json::Value) -> ReplicaSet {
        serde_json::from_value(json).unwrap()
    }

    /// A pod template whose labels match `{app: x}` and (optionally) carries an
    /// explicit restartPolicy / activeDeadlineSeconds.
    fn template(restart_policy: Option<&str>, ads: Option<i64>) -> serde_json::Value {
        let mut spec = serde_json::json!({
            "containers": [{"name": "c", "image": "nginx"}]
        });
        if let Some(rp) = restart_policy {
            spec["restartPolicy"] = serde_json::json!(rp);
        }
        if let Some(a) = ads {
            spec["activeDeadlineSeconds"] = serde_json::json!(a);
        }
        serde_json::json!({
            "metadata": {"labels": {"app": "x"}},
            "spec": spec,
        })
    }

    fn matching_selector() -> serde_json::Value {
        serde_json::json!({"matchLabels": {"app": "x"}})
    }

    // --- maxSurge > 100% is valid for a Deployment (upstream line 603 only
    //     bounds maxUnavailable) ----------------------------------------------

    #[test]
    fn deployment_max_surge_over_100_percent_is_valid() {
        let d = deployment(serde_json::json!({
            "metadata": {"name": "d"},
            "spec": {
                "replicas": 3,
                "selector": matching_selector(),
                "strategy": {
                    "type": "RollingUpdate",
                    "rollingUpdate": {"maxUnavailable": "25%", "maxSurge": "200%"}
                },
                "template": template(Some("Always"), None),
            }
        }));
        let errs = validate_deployment(&d);
        assert!(
            errs.is_empty(),
            "maxSurge: 200% must be accepted for a Deployment, got: {}",
            agg(&errs)
        );
    }

    #[test]
    fn deployment_max_unavailable_over_100_percent_rejected() {
        let d = deployment(serde_json::json!({
            "metadata": {"name": "d"},
            "spec": {
                "replicas": 3,
                "selector": matching_selector(),
                "strategy": {
                    "type": "RollingUpdate",
                    "rollingUpdate": {"maxUnavailable": "200%", "maxSurge": "25%"}
                },
                "template": template(Some("Always"), None),
            }
        }));
        let errs = validate_deployment(&d);
        let a = agg(&errs);
        assert!(
            a.contains("maxUnavailable") && a.contains("100%"),
            "maxUnavailable: 200% must be rejected, got: {a}"
        );
    }

    // --- Deployment empty-selector wording -----------------------------------

    #[test]
    fn deployment_empty_selector_wording() {
        let d = deployment(serde_json::json!({
            "metadata": {"name": "d"},
            "spec": {
                "selector": {},
                "template": template(Some("Always"), None),
            }
        }));
        let errs = validate_deployment(&d);
        assert!(
            agg(&errs).contains("empty selector is invalid for deployment"),
            "got: {}",
            agg(&errs)
        );
    }

    // --- StatefulSet restartPolicy / activeDeadlineSeconds -------------------

    fn base_statefulset(template: serde_json::Value) -> serde_json::Value {
        serde_json::json!({
            "metadata": {"name": "s", "namespace": "default"},
            "spec": {
                "serviceName": "svc",
                "selector": matching_selector(),
                "podManagementPolicy": "OrderedReady",
                "updateStrategy": {"type": "RollingUpdate"},
                "template": template,
            }
        })
    }

    #[test]
    fn statefulset_restart_policy_always_ok() {
        let s = statefulset(base_statefulset(template(Some("Always"), None)));
        assert!(
            validate_statefulset(&s).is_empty(),
            "got: {}",
            agg(&validate_statefulset(&s))
        );
        // unset restartPolicy also OK (defaulted to Always)
        let s2 = statefulset(base_statefulset(template(None, None)));
        assert!(validate_statefulset(&s2).is_empty());
    }

    #[test]
    fn statefulset_restart_policy_never_rejected() {
        let s = statefulset(base_statefulset(template(Some("Never"), None)));
        let a = agg(&validate_statefulset(&s));
        assert!(
            a.contains("template.spec.restartPolicy") && a.contains("supported values"),
            "got: {a}"
        );
    }

    #[test]
    fn statefulset_active_deadline_seconds_forbidden() {
        let s = statefulset(base_statefulset(template(Some("Always"), Some(30))));
        let a = agg(&validate_statefulset(&s));
        assert!(
            a.contains("template.spec.activeDeadlineSeconds")
                && a.contains("activeDeadlineSeconds in StatefulSet is not Supported"),
            "got: {a}"
        );
    }

    // --- #2000: the rest of ValidateStatefulSetSpec ---------------------------

    fn ss_errs(mutate: impl FnOnce(&mut serde_json::Value)) -> String {
        let mut s = base_statefulset(template(Some("Always"), None));
        mutate(&mut s);
        agg(&validate_statefulset(&statefulset(s)))
    }

    #[test]
    fn statefulset_rolling_update_max_unavailable_rules() {
        // validateRollingUpdateStatefulSet (validation.go:499-516).
        let ru = |mu: serde_json::Value| {
            ss_errs(|s| {
                s["spec"]["updateStrategy"]["rollingUpdate"] =
                    serde_json::json!({"maxUnavailable": mu})
            })
        };
        assert!(ru(serde_json::json!(1)).is_empty());
        assert!(ru(serde_json::json!("50%")).is_empty());
        let a = ru(serde_json::json!(0));
        assert!(
            a.contains(
                "spec.updateStrategy.rollingUpdate.maxUnavailable: Invalid value: 0: cannot be 0"
            ),
            "{a}"
        );
        let a = ru(serde_json::json!(-1));
        assert!(
            a.contains("maxUnavailable: Invalid value: -1: must be greater than or equal to 0"),
            "{a}"
        );
        let a = ru(serde_json::json!("101%"));
        assert!(
            a.contains("maxUnavailable: Invalid value: \"101%\": must not be greater than 100%"),
            "{a}"
        );
    }

    #[test]
    fn statefulset_retention_policy_must_be_retain_or_delete() {
        // ValidatePersistentVolumeClaimRetentionPolicy (validation.go:93-100).
        let a = ss_errs(|s| {
            s["spec"]["persistentVolumeClaimRetentionPolicy"] =
                serde_json::json!({"whenDeleted": "Bogus", "whenScaled": "Delete"})
        });
        assert!(a.contains("spec.persistentVolumeClaimRetentionPolicy.whenDeleted: Unsupported value: \"Bogus\": supported values: \"Retain\", \"Delete\""), "{a}");
        assert!(!a.contains("whenScaled"), "{a}");
    }

    fn vct(name: &str, spec: serde_json::Value) -> serde_json::Value {
        serde_json::json!([{"metadata": {"name": name}, "spec": spec}])
    }

    #[test]
    fn statefulset_volume_claim_templates_are_validated() {
        // validateVolumeClaimTemplates (validation.go:118-125).
        let a = ss_errs(|s| s["spec"]["volumeClaimTemplates"] = vct("data", serde_json::json!({})));
        assert!(a.contains("spec.volumeClaimTemplates[0].spec.accessModes: Required value: at least 1 access mode is required"), "{a}");
        let ok = ss_errs(|s| {
            s["spec"]["volumeClaimTemplates"] = vct(
                "data",
                serde_json::json!({"accessModes": ["ReadWriteOnce"], "resources": {"requests": {"storage": "1Gi"}}}),
            )
        });
        assert!(ok.is_empty(), "{ok}");
    }

    #[test]
    fn statefulset_template_may_mount_a_volume_claim_template_volume() {
        // validation.go:193-213: the VCT volumes are merged into the template
        // before pod validation, and subdomain/hostname are cleared.
        let a = ss_errs(|s| {
            s["spec"]["volumeClaimTemplates"] = vct(
                "data",
                serde_json::json!({"accessModes": ["ReadWriteOnce"], "resources": {"requests": {"storage": "1Gi"}}}),
            );
            let t = &mut s["spec"]["template"]["spec"];
            t["containers"][0]["volumeMounts"] =
                serde_json::json!([{"name": "data", "mountPath": "/d"}]);
            t["subdomain"] = serde_json::json!("Not_Valid");
            t["hostname"] = serde_json::json!("Not_Valid");
        });
        assert!(a.is_empty(), "{a}");
    }

    #[test]
    fn statefulset_empty_selector_wording() {
        let s = statefulset(serde_json::json!({
            "metadata": {"name": "s", "namespace": "default"},
            "spec": {
                "serviceName": "svc",
                "selector": {},
                "podManagementPolicy": "OrderedReady",
                "updateStrategy": {"type": "RollingUpdate"},
                "template": template(Some("Always"), None),
            }
        }));
        assert!(
            agg(&validate_statefulset(&s)).contains("empty selector is invalid for statefulset"),
            "got: {}",
            agg(&validate_statefulset(&s))
        );
    }

    // --- ReplicaSet restartPolicy / activeDeadlineSeconds --------------------

    fn base_replicaset(template: serde_json::Value) -> serde_json::Value {
        serde_json::json!({
            "metadata": {"name": "r", "namespace": "default"},
            "spec": {"replicas": 1, "selector": matching_selector(), "template": template}
        })
    }

    #[test]
    fn replicaset_restart_policy_always_ok() {
        let r = replicaset(base_replicaset(template(Some("Always"), None)));
        assert!(
            validate_replicaset(&r).is_empty(),
            "got: {}",
            agg(&validate_replicaset(&r))
        );
    }

    #[test]
    fn replicaset_restart_policy_onfailure_rejected() {
        let r = replicaset(base_replicaset(template(Some("OnFailure"), None)));
        let a = agg(&validate_replicaset(&r));
        assert!(
            a.contains("template.spec.restartPolicy") && a.contains("supported values"),
            "got: {a}"
        );
    }

    #[test]
    fn replicaset_active_deadline_seconds_forbidden() {
        let r = replicaset(base_replicaset(template(Some("Always"), Some(10))));
        let a = agg(&validate_replicaset(&r));
        assert!(
            a.contains("activeDeadlineSeconds in ReplicaSet is not Supported"),
            "got: {a}"
        );
    }

    #[test]
    fn replicaset_empty_selector_wording() {
        let r = replicaset(serde_json::json!({
            "metadata": {"name": "r"},
            "spec": {"replicas": 1, "selector": {}, "template": template(Some("Always"), None)}
        }));
        assert!(
            agg(&validate_replicaset(&r)).contains("empty selector is invalid for deployment"),
            "got: {}",
            agg(&validate_replicaset(&r))
        );
    }

    // --- DaemonSet restartPolicy / ADS / rollingUpdate mutual-exclusion ------

    fn daemonset_with(
        template: serde_json::Value,
        rolling_update: serde_json::Value,
    ) -> serde_json::Value {
        serde_json::json!({
            "metadata": {"name": "ds", "namespace": "default"},
            "spec": {
                "selector": matching_selector(),
                "updateStrategy": {"type": "RollingUpdate", "rollingUpdate": rolling_update},
                "template": template,
            }
        })
    }

    #[test]
    fn daemonset_restart_policy_never_rejected_ads_forbidden() {
        let ds = daemonset(daemonset_with(
            template(Some("Never"), Some(5)),
            serde_json::json!({"maxUnavailable": 1, "maxSurge": 0}),
        ));
        let a = agg(&validate_daemonset(&ds));
        assert!(a.contains("template.spec.restartPolicy"), "got: {a}");
        assert!(
            a.contains("activeDeadlineSeconds in DaemonSet is not Supported"),
            "got: {a}"
        );
    }

    #[test]
    fn daemonset_rolling_update_both_nonzero_rejected() {
        let ds = daemonset(daemonset_with(
            template(Some("Always"), None),
            serde_json::json!({"maxUnavailable": 1, "maxSurge": 1}),
        ));
        let a = agg(&validate_daemonset(&ds));
        assert!(
            a.contains("maxSurge") && a.contains("may not be set when maxUnavailable is non-zero"),
            "got: {a}"
        );
    }

    #[test]
    fn daemonset_rolling_update_both_zero_rejected() {
        let ds = daemonset(daemonset_with(
            template(Some("Always"), None),
            serde_json::json!({"maxUnavailable": 0, "maxSurge": 0}),
        ));
        let a = agg(&validate_daemonset(&ds));
        assert!(
            a.contains("maxUnavailable") && a.contains("cannot be 0 when maxSurge is 0"),
            "got: {a}"
        );
    }

    #[test]
    fn daemonset_rolling_update_surge_only_ok() {
        let ds = daemonset(daemonset_with(
            template(Some("Always"), None),
            serde_json::json!({"maxUnavailable": 0, "maxSurge": 1}),
        ));
        assert!(
            validate_daemonset(&ds).is_empty(),
            "got: {}",
            agg(&validate_daemonset(&ds))
        );
    }

    #[test]
    fn daemonset_rolling_update_surge_over_100_percent_rejected() {
        // unlike Deployment, DaemonSet bounds BOTH fields to 100% (upstream 483)
        let ds = daemonset(daemonset_with(
            template(Some("Always"), None),
            serde_json::json!({"maxUnavailable": 0, "maxSurge": "200%"}),
        ));
        let a = agg(&validate_daemonset(&ds));
        assert!(
            a.contains("maxSurge") && a.contains("must not be greater than 100%"),
            "got: {a}"
        );
    }

    #[test]
    fn daemonset_empty_selector_wording() {
        let ds = daemonset(serde_json::json!({
            "metadata": {"name": "ds"},
            "spec": {
                "selector": {},
                "updateStrategy": {"type": "OnDelete"},
                "template": template(Some("Always"), None),
            }
        }));
        assert!(
            agg(&validate_daemonset(&ds)).contains("empty selector is invalid for daemonset"),
            "got: {}",
            agg(&validate_daemonset(&ds))
        );
    }

    // --- an invalid selector (#2034) ------------------------------------------
    //
    // When `metav1.LabelSelectorAsSelector` errors, StatefulSet, Deployment and
    // ReplicaSet push `field.Invalid(fldPath.Child("selector"), spec.Selector,
    // <detail>)` (`pkg/apis/apps/validation/validation.go:189-191`, `:652-654`,
    // `:823-825`); DaemonSet only gates its template check on `err == nil`
    // (`:446-449`) and adds nothing.

    /// A selector whose one expression uses an operator upstream does not know,
    /// so `LabelSelectorAsSelector` fails on it.
    fn unknown_operator_selector() -> serde_json::Value {
        serde_json::json!({
            "matchLabels": {"app": "x"},
            "matchExpressions": [{"key": "app", "operator": "Bogus", "values": ["x"]}]
        })
    }

    /// The `Invalid` errors on exactly `spec.selector` (not its children).
    fn selector_invalids(errs: &ErrorList) -> Vec<(String, BadValue)> {
        errs.iter()
            .filter(|e| {
                e.error_type == rusternetes_common::validation::field::ErrorType::Invalid
                    && e.field == "spec.selector"
            })
            .map(|e| (e.detail.clone(), e.bad_value.clone()))
            .collect()
    }

    fn expected_selector_invalid(detail: &str) -> Vec<(String, BadValue)> {
        let selector: LabelSelector = serde_json::from_value(unknown_operator_selector()).unwrap();
        vec![(detail.to_string(), BadValue::marshal(&selector))]
    }

    /// #2190: the selector renders matchLabels before matchExpressions
    /// (Go declaration order), not alphabetically.
    #[test]
    fn invalid_selector_renders_in_declaration_order() {
        let mut s = base_statefulset(template(Some("Always"), None));
        s["spec"]["selector"] = unknown_operator_selector();
        let errs = validate_statefulset(&statefulset(s));
        let e = errs.iter().find(|e| e.field == "spec.selector").unwrap();
        assert_eq!(
            e.error_body(),
            r#"Invalid value: {"matchLabels":{"app":"x"},"matchExpressions":[{"key":"app","operator":"Bogus","values":["x"]}]}"#
        );
    }

    #[test]
    fn statefulset_invalid_selector_is_field_invalid() {
        let mut s = base_statefulset(template(Some("Always"), None));
        s["spec"]["selector"] = unknown_operator_selector();
        let errs = validate_statefulset(&statefulset(s));
        assert_eq!(
            selector_invalids(&errs),
            expected_selector_invalid(""),
            "got: {}",
            agg(&errs)
        );
    }

    #[test]
    fn deployment_invalid_selector_is_field_invalid() {
        let d = deployment(serde_json::json!({
            "metadata": {"name": "d"},
            "spec": {
                "replicas": 1,
                "selector": unknown_operator_selector(),
                "template": template(Some("Always"), None),
            }
        }));
        let errs = validate_deployment(&d);
        assert_eq!(
            selector_invalids(&errs),
            expected_selector_invalid("invalid label selector"),
            "got: {}",
            agg(&errs)
        );
    }

    #[test]
    fn replicaset_invalid_selector_is_field_invalid() {
        let mut r = base_replicaset(template(Some("Always"), None));
        r["spec"]["selector"] = unknown_operator_selector();
        let errs = validate_replicaset(&replicaset(r));
        assert_eq!(
            selector_invalids(&errs),
            expected_selector_invalid("invalid label selector"),
            "got: {}",
            agg(&errs)
        );
    }

    #[test]
    fn daemonset_invalid_selector_only_skips_the_template_check() {
        let mut ds = daemonset_with(
            template(Some("Always"), None),
            serde_json::json!({"maxUnavailable": 1}),
        );
        ds["spec"]["selector"] = unknown_operator_selector();
        let errs = validate_daemonset(&daemonset(ds));
        assert_eq!(selector_invalids(&errs), vec![], "got: {}", agg(&errs));
        assert!(
            !agg(&errs).contains("does not match template"),
            "got: {}",
            agg(&errs)
        );
    }
}

/// Ports of upstream `TestValidateDaemonSetUpdate` and
/// `TestValidateDaemonSetStatusUpdate`
/// (pkg/apis/apps/validation/validation_test.go:1504-1878, 1879-2341).
mod daemonset_update_tests {
    use super::__parent::*;
    use rusternetes_common::validation::apps::*;

    fn ds(template_generation: i64, image: &str, app: &str) -> DaemonSet {
        let mut ds: DaemonSet = serde_json::from_value(serde_json::json!({
            "metadata": {"name": "abc", "namespace": "default", "resourceVersion": "1"},
            "spec": {
                "selector": {"matchLabels": {"a": app}},
                "updateStrategy": {"type": "OnDelete"},
                "template": {
                    "metadata": {"labels": {"a": app}},
                    "spec": {
                        "restartPolicy": "Always",
                        "dnsPolicy": "ClusterFirst",
                        "containers": [{
                            "name": "c", "image": image,
                            "imagePullPolicy": "IfNotPresent",
                            "terminationMessagePolicy": "File"
                        }]
                    }
                }
            }
        }))
        .unwrap();
        ds.set_template_generation(template_generation);
        ds
    }

    fn fields(errs: &ErrorList) -> Vec<String> {
        errs.iter().map(|e| e.field.to_string()).collect()
    }

    #[test]
    fn accepted_updates() {
        // "no change"
        assert_eq!(
            fields(&validate_daemonset_update(
                &ds(1, "i", "b"),
                &ds(1, "i", "b")
            )),
            Vec::<String>::new()
        );
        // "change template": the generation moves with it
        assert_eq!(
            fields(&validate_daemonset_update(
                &ds(4, "j", "b"),
                &ds(3, "i", "b")
            )),
            Vec::<String>::new()
        );
        // "change update strategy": a non-template spec change keeps it
        let mut rolling = ds(4, "i", "b");
        rolling.spec.update_strategy = serde_json::from_value(serde_json::json!({
            "type": "RollingUpdate", "rollingUpdate": {"maxUnavailable": 1}
        }))
        .unwrap();
        assert_eq!(
            fields(&validate_daemonset_update(&rolling, &ds(4, "i", "b"))),
            Vec::<String>::new()
        );
    }

    #[test]
    fn rejected_updates() {
        for (name, new, old, field) in [
            (
                "change selector",
                ds(2, "i", "c"),
                ds(1, "i", "b"),
                "spec.selector",
            ),
            (
                "negative templateGeneration",
                ds(-1, "i", "b"),
                ds(-1, "i", "b"),
                "spec.templateGeneration",
            ),
            (
                "decreased templateGeneration",
                ds(1, "j", "b"),
                ds(2, "i", "b"),
                "spec.templateGeneration",
            ),
            (
                "unchanged templateGeneration upon template update",
                ds(2, "j", "b"),
                ds(2, "i", "b"),
                "spec.templateGeneration",
            ),
            (
                "incremented templateGeneration without template update",
                ds(3, "i", "b"),
                ds(2, "i", "b"),
                "spec.templateGeneration",
            ),
        ] {
            let errs = validate_daemonset_update(&new, &old);
            assert!(
                fields(&errs).iter().any(|f| f == field),
                "{name}: want an error on {field}, got {errs:?}"
            );
        }
    }

    fn status(json: serde_json::Value) -> DaemonSet {
        let mut ds = ds(1, "i", "b");
        ds.status = Some(serde_json::from_value(json).unwrap());
        ds
    }

    #[test]
    fn status_update() {
        let old = status(serde_json::json!({
            "currentNumberScheduled": 1, "numberMisscheduled": 2,
            "desiredNumberScheduled": 3, "numberReady": 1, "observedGeneration": 3,
            "updatedNumberScheduled": 1, "numberAvailable": 1, "numberUnavailable": 2,
            "collisionCount": 2
        }));
        let ok = status(serde_json::json!({
            "currentNumberScheduled": 1, "numberMisscheduled": 1,
            "desiredNumberScheduled": 3, "numberReady": 1,
            "updatedNumberScheduled": 1, "numberAvailable": 1, "numberUnavailable": 2,
            "collisionCount": 2
        }));
        assert_eq!(
            fields(&validate_daemonset_status_update(&ok, &old)),
            Vec::<String>::new()
        );

        // "negative values"
        let negative = status(serde_json::json!({
            "currentNumberScheduled": -1, "numberMisscheduled": -1,
            "desiredNumberScheduled": -3, "numberReady": -1, "observedGeneration": -3,
            "updatedNumberScheduled": -1, "numberAvailable": -1, "numberUnavailable": -2,
            "collisionCount": -1
        }));
        let got = fields(&validate_daemonset_status_update(&negative, &old));
        for field in [
            "status.currentNumberScheduled",
            "status.numberMisscheduled",
            "status.desiredNumberScheduled",
            "status.numberReady",
            "status.observedGeneration",
            "status.updatedNumberScheduled",
            "status.numberAvailable",
            "status.numberUnavailable",
            "status.collisionCount",
        ] {
            assert!(got.iter().any(|f| f == field), "{field} missing: {got:?}");
        }

        // A decremented collisionCount (validation.go:429-435).
        let decremented = status(serde_json::json!({"collisionCount": 1}));
        let errs = validate_daemonset_status_update(&decremented, &old);
        assert!(
            errs.iter().any(|e| e.field == "status.collisionCount"
                && e.to_string().contains("cannot be decremented")),
            "{errs:?}"
        );
    }

    #[test]
    fn a_daemonset_name_is_a_dns_subdomain() {
        let mut named = ds(1, "i", "b");
        named.metadata.name = "a.b".to_string();
        assert!(validate_daemonset(&named).is_empty());
        named.metadata.name = "A_b".to_string();
        assert!(fields(&validate_daemonset(&named))
            .iter()
            .any(|f| f == "metadata.name"));
    }
}
