//! Unit tests moved from `src/validation/pod.rs`.
#![allow(unused_imports, dead_code)]

mod __parent {
    pub use once_cell::sync::Lazy;
    pub use regex::Regex;
    pub use rusternetes_common::resources::pod::{
        AppArmorProfile, ConfigMapVolumeSource, Container, ContainerPort, ContainerResizePolicy,
        ContainerRestartRule, DownwardAPIVolumeFile, DownwardAPIVolumeSource, EnvFromSource,
        EnvVar, EphemeralVolumeSource, ExecAction, FileKeySelector, GRPCAction, HTTPGetAction,
        HostPathVolumeSource, KeyToPath, Lifecycle, LifecycleHandler, NodeAffinity,
        NodeSelectorTerm, Pod, PodAffinityTerm, PodDNSConfig, PodOS, PodReadinessGate,
        PodResourceClaim, PodSchedulingGate, PodSecurityContext, PodSpec, PreferredSchedulingTerm,
        Probe, ProjectedVolumeSource, ResourceFieldSelector, SeccompProfile, SecretVolumeSource,
        SecurityContext, SleepAction, TCPSocketAction, Toleration, TopologySpreadConstraint,
        Volume, VolumeDevice, VolumeMount, WeightedPodAffinityTerm, WorkloadReference,
    };
    pub use rusternetes_common::resources::policy::IntOrString;
    pub use rusternetes_common::validation::field::{BadValue, Error, ErrorList, Path};
    pub use rusternetes_common::validation::metav1::{
        is_dns1123_label, is_dns1123_subdomain, is_dns1123_subdomain_with_underscore,
        is_qualified_name, is_valid_label_value, validate_label_name, validate_label_selector,
        LabelSelectorValidationOptions,
    };
    pub use rusternetes_common::validation::pod_options::{
        get_validation_options_from_pod_spec_and_meta, PodValidationOptions,
    };
    pub use rusternetes_common::validation::pod_status::is_valid_id;
    pub use std::collections::{HashMap, HashSet};
}

mod tests {
    use super::__parent::*;
    use rusternetes_common::resources::pod::Sysctl;
    use rusternetes_common::validation::pod::*;

    /// validation.go:1396 `sets.List(supportedHostPathTypes)` sorts, so the
    /// supported list in the NotSupported detail is lexicographic (#2499).
    #[test]
    fn host_path_type_not_supported_lists_sorted() {
        let hp = HostPathVolumeSource {
            path: "/x".to_string(),
            type_: Some("bogus".to_string()),
        };
        let errs = validate_host_path_volume_source(&hp, &Path::new("hostPath"));
        assert_eq!(errs.len(), 1, "{errs:?}");
        assert!(
            errs[0].to_string().contains(
                "\"\", \"BlockDevice\", \"CharDevice\", \"Directory\", \"DirectoryOrCreate\", \"File\", \"FileOrCreate\", \"Socket\""
            ),
            "{}",
            errs[0]
        );
    }

    #[test]
    fn sysctl_name_validity_matches_upstream() {
        // Valid: dot- and slash-separated lowercase-alnum segments, inner -/_.
        assert!(is_valid_sysctl_name("kernel.shmmax"));
        assert!(is_valid_sysctl_name("kernel/shm_rmid_forced")); // slash separator
        assert!(is_valid_sysctl_name("safe-and-unsafe")); // single segment, inner '-'
        assert!(is_valid_sysctl_name("net.ipv4.tcp_keepalive_time"));

        // Invalid: trailing '-', empty segments, leading/trailing separators.
        assert!(!is_valid_sysctl_name("foo-"));
        assert!(!is_valid_sysctl_name("bar.."));
        assert!(!is_valid_sysctl_name(""));
        assert!(!is_valid_sysctl_name(".leading"));
        assert!(!is_valid_sysctl_name("trailing."));
        assert!(!is_valid_sysctl_name("Upper.Case"));
        assert!(!is_valid_sysctl_name(&"a".repeat(SYSCTL_MAX_LENGTH + 1)));
    }

    fn sc_with_sysctls(names: &[&str]) -> PodSecurityContext {
        PodSecurityContext {
            sysctls: Some(
                names
                    .iter()
                    .map(|n| Sysctl {
                        name: n.to_string(),
                        value: "1".to_string(),
                    })
                    .collect(),
            ),
            ..Default::default()
        }
    }

    #[test]
    fn validate_sysctls_rejects_invalid_names_only() {
        // Mirrors the [sig-node] "should reject invalid sysctls" conformance
        // case: foo- and bar.. are rejected; kernel.shmmax and safe-and-unsafe
        // are accepted.
        let sc = sc_with_sysctls(&["foo-", "kernel.shmmax", "safe-and-unsafe", "bar.."]);
        let errs = validate_sysctls(
            &sc,
            false,
            false,
            &PodValidationOptions::default(),
            &Path::new("spec").child("securityContext").child("sysctls"),
        );
        let bad: Vec<&str> = errs
            .iter()
            .filter_map(|e| match &e.bad_value {
                rusternetes_common::validation::field::BadValue::String(s) => Some(s.as_str()),
                _ => None,
            })
            .collect();
        assert!(bad.contains(&"foo-"), "foo- must be rejected: {errs:?}");
        assert!(bad.contains(&"bar.."), "bar.. must be rejected: {errs:?}");
        assert!(!bad.contains(&"kernel.shmmax"));
        assert!(!bad.contains(&"safe-and-unsafe"));
        assert_eq!(errs.len(), 2, "exactly the two invalid names: {errs:?}");
    }

    #[test]
    fn validate_sysctls_flags_duplicates_and_empty() {
        let sc = sc_with_sysctls(&["kernel.shmmax", "kernel.shmmax", ""]);
        let errs = validate_sysctls(
            &sc,
            false,
            false,
            &PodValidationOptions::default(),
            &Path::new("spec").child("securityContext").child("sysctls"),
        );
        assert_eq!(errs.len(), 2, "one duplicate + one required: {errs:?}");
    }

    fn t(key: &str) -> Toleration {
        Toleration {
            key: Some(key.to_string()),
            operator: Some("Exists".to_string()),
            value: None,
            effect: None,
            toleration_seconds: None,
        }
    }

    fn g(name: &str) -> PodSchedulingGate {
        PodSchedulingGate {
            name: name.to_string(),
        }
    }

    fn pod_spec_with_active_deadline(active_deadline_seconds: Option<i64>) -> PodSpec {
        PodSpec {
            containers: vec![Container {
                name: "c".to_string(),
                image: "pause".to_string(),
                ..Default::default()
            }],
            active_deadline_seconds,
            ..Default::default()
        }
    }

    #[test]
    fn tolerations_additions_only_allows_add() {
        let p = Path::new("spec").child("tolerations");
        let old = vec![t("a")];
        let new = vec![t("a"), t("b")];
        assert!(validate_only_added_tolerations(&old, &new, &p).is_empty());
    }

    #[test]
    fn tolerations_additions_only_rejects_remove() {
        let p = Path::new("spec").child("tolerations");
        let old = vec![t("a"), t("b")];
        let new = vec![t("a")];
        let errs = validate_only_added_tolerations(&old, &new, &p);
        assert_eq!(errs.len(), 1);
        assert!(errs[0]
            .to_string()
            .contains("existing toleration can not be modified except its tolerationSeconds"));
    }

    // Upstream: validation.go:4303-4324 (release-1.35) - tolerationSeconds may
    // change on an existing toleration.
    #[test]
    fn tolerations_update_allows_toleration_seconds_change() {
        let p = Path::new("spec").child("tolerations");
        let mk = |secs: Option<i64>| Toleration {
            key: Some("key1".into()),
            operator: Some("Equal".into()),
            value: Some("value1".into()),
            effect: Some("NoExecute".into()),
            toleration_seconds: secs,
        };
        assert!(validate_only_added_tolerations(&[mk(Some(10))], &[mk(Some(20))], &p).is_empty());
        assert!(validate_only_added_tolerations(&[mk(None)], &[mk(Some(20))], &p).is_empty());
        let mut changed = mk(Some(10));
        changed.value = Some("other".into());
        let errs = validate_only_added_tolerations(&[mk(Some(10))], &[changed], &p);
        assert_eq!(errs.len(), 1);
        assert!(errs[0]
            .to_string()
            .contains("existing toleration can not be modified except its tolerationSeconds"));
    }

    fn tol(
        key: &str,
        op: Option<&str>,
        value: &str,
        effect: Option<&str>,
        secs: Option<i64>,
    ) -> Toleration {
        Toleration {
            key: (!key.is_empty()).then(|| key.to_string()),
            operator: op.map(String::from),
            value: (!value.is_empty()).then(|| value.to_string()),
            effect: effect.map(String::from),
            toleration_seconds: secs,
        }
    }

    // validation.go:4360-4414 ValidateTolerations.
    #[test]
    fn tolerations_validate_key_value_and_empty_key_rules() {
        let p = Path::new("spec").child("tolerations");
        // invalid key -> ValidateLabelName at tolerations[0].key
        let errs = validate_tolerations(
            &[tol(
                "nospecialchars^=@",
                Some("Equal"),
                "bar",
                Some("NoSchedule"),
                None,
            )],
            &p,
        );
        assert!(errs.iter().any(|e| e.field == "spec.tolerations[0].key"));
        // empty key requires Exists (operator "" / Equal both rejected)
        for op in [Some("Equal"), None] {
            let errs = validate_tolerations(&[tol("", op, "bar", Some("NoSchedule"), None)], &p);
            assert!(errs.iter().any(|e| e.field == "spec.tolerations[0].operator"
                && e.to_string().contains(
                    "operator must be Exists when `key` is empty, which means \"match all values and all keys\""
                )));
        }
        assert!(
            validate_tolerations(&[tol("", Some("Exists"), "", Some("NoSchedule"), None)], &p)
                .is_empty()
        );
        // Equal value is label-value validated, reported on the operator path.
        let errs = validate_tolerations(&[tol("foo", Some("Equal"), "bad value!", None, None)], &p);
        assert_eq!(errs.len(), 1);
        assert_eq!(errs[0].field, "spec.tolerations[0].operator");
        // Exists with value: exact wording.
        let errs = validate_tolerations(&[tol("foo", Some("Exists"), "bar", None, None)], &p);
        assert_eq!(errs.len(), 1);
        assert!(errs[0]
            .to_string()
            .contains("value must be empty when `operator` is 'Exists'"));
        // tolerationSeconds with non-NoExecute (incl. empty effect) -> effect path.
        for eff in [Some("NoSchedule"), None] {
            let errs = validate_tolerations(&[tol("foo", Some("Exists"), "", eff, Some(20))], &p);
            assert_eq!(errs.len(), 1, "{errs:?}");
            assert_eq!(errs[0].field, "spec.tolerations[0].effect");
        }
    }

    #[test]
    fn gates_deletions_only_allows_remove() {
        let p = Path::new("spec").child("schedulingGates");
        let old = vec![g("a"), g("b")];
        let new = vec![g("a")];
        assert!(validate_only_deleted_scheduling_gates(&old, &new, &p).is_empty());
    }

    #[test]
    fn gates_deletions_only_rejects_add() {
        let p = Path::new("spec").child("schedulingGates");
        let old = vec![g("a")];
        let new = vec![g("a"), g("b")];
        let errs = validate_only_deleted_scheduling_gates(&old, &new, &p);
        assert_eq!(errs.len(), 1);
        assert!(errs[0].to_string().contains("only deletion is allowed"));
    }

    #[test]
    fn tgps_unchanged_is_allowed() {
        let p = Path::new("spec").child("terminationGracePeriodSeconds");
        assert!(validate_termination_grace_period_immutable(Some(30), Some(30), &p).is_empty());
        assert!(validate_termination_grace_period_immutable(None, None, &p).is_empty());
    }

    #[test]
    fn tgps_negative_to_one_is_allowed() {
        let p = Path::new("spec").child("terminationGracePeriodSeconds");
        assert!(validate_termination_grace_period_immutable(Some(-5), Some(1), &p).is_empty());
    }

    #[test]
    fn tgps_arbitrary_change_is_rejected() {
        let p = Path::new("spec").child("terminationGracePeriodSeconds");
        let errs = validate_termination_grace_period_immutable(Some(30), Some(60), &p);
        assert_eq!(errs.len(), 1);
        assert!(errs[0].to_string().contains("field is immutable"));
    }

    /// validation.go:5830-5834: the forbidden error appends
    /// `diff.Diff(oldPod.Spec, mungedPodSpec)` — a unified diff (context 3) of
    /// `json.MarshalIndent` of the INTERNAL `core.PodSpec` (Go field names,
    /// declaration order, nil pointers/slices as `null`, `SecurityContext`
    /// always an object because the v1 conversion allocates it).
    #[test]
    fn pod_spec_update_forbidden_carries_diff_text() {
        let spec = |node: &str| -> PodSpec {
            serde_json::from_value(serde_json::json!({
                "containers": [{"name": "c", "image": "i"}],
                "restartPolicy": "Always",
                "nodeName": node
            }))
            .unwrap()
        };
        let errs = validate_pod_spec_update(&spec("a"), &spec("b"), false);
        let e = errs
            .iter()
            .find(|e| {
                e.detail
                    .starts_with("pod updates may not change fields other than")
            })
            .unwrap_or_else(|| panic!("{errs:?}"));
        let want = format!(
            "pod updates may not change fields other than {}\n{}",
            UPDATABLE_POD_SPEC_FIELDS.join(","),
            concat!(
                "@@ -42,7 +42,7 @@\n",
                "  \"NodeSelector\": null,\n",
                "  \"ServiceAccountName\": \"\",\n",
                "  \"AutomountServiceAccountToken\": null,\n",
                "- \"NodeName\": \"a\",\n",
                "+ \"NodeName\": \"b\",\n",
                "  \"SecurityContext\": {\n",
                "   \"HostNetwork\": false,\n",
                "   \"HostPID\": false,\n",
            )
        );
        assert_eq!(e.detail, want);
    }

    /// validation.go:5833 diffs the typed specs, so an empty (non-nil)
    /// `nodeSelector` context line renders `{}`, not `null`.
    #[test]
    fn pod_spec_update_diff_keeps_empty_node_selector() {
        let spec = |node: &str| -> PodSpec {
            serde_json::from_value(serde_json::json!({
                "containers": [{"name": "c", "image": "i"}],
                "restartPolicy": "Always",
                "nodeSelector": {},
                "nodeName": node
            }))
            .unwrap()
        };
        let errs = validate_pod_spec_update(&spec("a"), &spec("b"), false);
        let e = errs
            .iter()
            .find(|e| e.detail.starts_with("pod updates may not change"))
            .unwrap_or_else(|| panic!("{errs:?}"));
        assert!(e.detail.contains("\"NodeSelector\": {},\n"), "{}", e.detail);
    }

    #[test]
    fn active_deadline_update_allows_nil_to_positive() {
        let old = pod_spec_with_active_deadline(None);
        let new = pod_spec_with_active_deadline(Some(5));

        assert!(validate_pod_spec_update(&old, &new, false).is_empty());
    }

    #[test]
    fn active_deadline_update_allows_decrease() {
        let old = pod_spec_with_active_deadline(Some(10));
        let new = pod_spec_with_active_deadline(Some(5));

        assert!(validate_pod_spec_update(&old, &new, false).is_empty());
    }

    #[test]
    fn active_deadline_update_rejects_increase() {
        let old = pod_spec_with_active_deadline(Some(5));
        let new = pod_spec_with_active_deadline(Some(10));
        let err = validate_pod_spec_update(&old, &new, false)
            .iter()
            .map(|e| e.to_string())
            .collect::<Vec<_>>()
            .join("; ");

        assert!(err.contains("spec.activeDeadlineSeconds"));
        assert!(err.contains("must be less than or equal to previous value"));
    }

    #[test]
    fn active_deadline_update_rejects_unset_zero_and_negative() {
        let path = Path::new("spec").child("activeDeadlineSeconds");

        let unset_errs = validate_active_deadline_seconds_update(Some(5), None, &path);
        assert_eq!(unset_errs.len(), 1);
        assert!(unset_errs[0]
            .to_string()
            .contains("must not update from a positive integer to nil value"));

        // validation.go:5721: the update range is [0, MaxInt32]; a zero is
        // refused by the create-time rule in validatePodMetadataAndSpec.
        assert!(validate_active_deadline_seconds_update(None, Some(0), &path).is_empty());

        let negative_errs = validate_active_deadline_seconds_update(None, Some(-1), &path);
        assert_eq!(negative_errs.len(), 1);
        assert!(negative_errs[0]
            .to_string()
            .contains("must be between 0 and 2147483647, inclusive"));
    }

    fn dns_cfg(searches: &[&str]) -> PodDNSConfig {
        PodDNSConfig {
            nameservers: None,
            searches: Some(searches.iter().map(|s| s.to_string()).collect()),
            options: None,
        }
    }

    #[test]
    fn dns_search_underscore_rejected_when_gate_off() {
        let p = Path::new("spec").child("dnsConfig");
        let cfg = dns_cfg(&["_sip._tcp.abc_d.example.com"]);
        let errs = validate_pod_dns_config(Some(&cfg), None, false, &p);
        assert!(
            !errs.is_empty(),
            "underscore search must be rejected with relaxed gate disabled"
        );
        assert!(errs[0].to_string().contains("spec.dnsConfig.searches[0]"));
    }

    #[test]
    fn dns_search_underscore_accepted_when_gate_on() {
        let p = Path::new("spec").child("dnsConfig");
        let cfg = dns_cfg(&["_sip._tcp.abc_d.example.com"]);
        let errs = validate_pod_dns_config(Some(&cfg), None, true, &p);
        assert!(
            errs.is_empty(),
            "underscore search must be accepted with relaxed gate enabled: {:?}",
            errs
        );
    }

    #[test]
    fn dns_search_lone_dot_rejected_when_gate_off() {
        let p = Path::new("spec").child("dnsConfig");
        // Strict mode trims trailing `.` and then runs IsDNS1123Subdomain on
        // the empty string, which the upstream regex rejects.
        let cfg = dns_cfg(&["."]);
        let errs = validate_pod_dns_config(Some(&cfg), None, false, &p);
        assert!(
            !errs.is_empty(),
            "lone-dot search must be rejected with relaxed gate disabled"
        );
    }

    #[test]
    fn dns_search_lone_dot_accepted_when_gate_on() {
        let p = Path::new("spec").child("dnsConfig");
        let cfg = dns_cfg(&["."]);
        let errs = validate_pod_dns_config(Some(&cfg), None, true, &p);
        assert!(
            errs.is_empty(),
            "lone-dot search must be accepted with relaxed gate enabled: {:?}",
            errs
        );
    }

    #[test]
    fn dns_search_plain_subdomain_accepted_both_modes() {
        let p = Path::new("spec").child("dnsConfig");
        let cfg = dns_cfg(&["example.com"]);
        assert!(validate_pod_dns_config(Some(&cfg), None, true, &p).is_empty());
        assert!(validate_pod_dns_config(Some(&cfg), None, false, &p).is_empty());
    }

    #[test]
    fn dns_policy_none_without_config_is_rejected() {
        let p = Path::new("spec").child("dnsConfig");
        let errs = validate_pod_dns_config(None, Some("None"), true, &p);
        assert_eq!(errs.len(), 1);
        assert!(errs[0]
            .to_string()
            .contains("must provide `dnsConfig` when `dnsPolicy` is None"));
    }

    #[test]
    fn dns_policy_none_with_empty_nameservers_is_rejected() {
        let p = Path::new("spec").child("dnsConfig");
        let cfg = PodDNSConfig {
            nameservers: Some(vec![]),
            searches: None,
            options: None,
        };
        let errs = validate_pod_dns_config(Some(&cfg), Some("None"), true, &p);
        assert_eq!(errs.len(), 1);
        assert!(errs[0]
            .to_string()
            .contains("must provide at least one DNS nameserver"));
    }

    #[test]
    fn dns_search_too_many_paths_rejected() {
        let p = Path::new("spec").child("dnsConfig");
        let too_many: Vec<&str> = std::iter::repeat_n("a.com", MAX_DNS_SEARCH_PATHS + 1).collect();
        let cfg = dns_cfg(&too_many);
        let errs = validate_pod_dns_config(Some(&cfg), None, true, &p);
        assert!(errs.iter().any(|e| e
            .to_string()
            .contains("must not have more than 32 search paths")));
    }

    #[test]
    fn dns_option_empty_name_rejected() {
        let p = Path::new("spec").child("dnsConfig");
        let cfg = PodDNSConfig {
            nameservers: None,
            searches: None,
            options: Some(vec![
                rusternetes_common::resources::pod::PodDNSConfigOption {
                    name: String::new(),
                    value: None,
                },
            ]),
        };
        let errs = validate_pod_dns_config(Some(&cfg), None, true, &p);
        assert!(
            errs.iter()
                .any(|e| e.to_string().contains("must not be empty")),
            "got: {:?}",
            errs
        );
    }

    #[test]
    fn tgps_positive_to_nil_treated_as_partial_update() {
        // Partial-update semantics: client omitted the field, so the server
        // backfills from old. The fence treats `None` on the new side as
        // "unchanged", matching K8s' defaulting + admission re-run on each
        // request.
        let p = Path::new("spec").child("terminationGracePeriodSeconds");
        let errs = validate_termination_grace_period_immutable(Some(30), None, &p);
        assert!(errs.is_empty(), "client-omitted TGPS must not be rejected");
    }

    // ----- gated-pod nodeSelector / nodeAffinity relaxation (KEP-3521) -----

    use rusternetes_common::resources::pod::{
        NodeAffinity as NA, NodeSelector as NS, NodeSelectorRequirement as NSR,
        NodeSelectorTerm as NST,
    };

    fn req(key: &str, op: &str, vals: &[&str]) -> NSR {
        NSR {
            key: key.to_string(),
            operator: op.to_string(),
            values: if vals.is_empty() {
                None
            } else {
                Some(vals.iter().map(|s| s.to_string()).collect())
            },
        }
    }

    fn term(me: Vec<NSR>) -> NST {
        NST {
            match_expressions: if me.is_empty() { None } else { Some(me) },
            match_fields: None,
        }
    }

    fn na_required(terms: Vec<NST>) -> NA {
        NA {
            required_during_scheduling_ignored_during_execution: Some(NS {
                node_selector_terms: terms,
            }),
            preferred_during_scheduling_ignored_during_execution: None,
        }
    }

    #[test]
    fn node_selector_addition_is_allowed() {
        let p = Path::new("spec").child("nodeSelector");
        let mut old = HashMap::new();
        old.insert("foo".to_string(), "bar".to_string());
        let mut new = old.clone();
        new.insert("baz".to_string(), "qux".to_string());
        assert!(validate_node_selector_mutation(&p, Some(&new), Some(&old)).is_empty());
    }

    #[test]
    fn node_selector_addition_from_empty_is_allowed() {
        let p = Path::new("spec").child("nodeSelector");
        let mut new = HashMap::new();
        new.insert("foo".to_string(), "bar".to_string());
        assert!(validate_node_selector_mutation(&p, Some(&new), None).is_empty());
    }

    #[test]
    fn node_selector_deletion_is_rejected() {
        let p = Path::new("spec").child("nodeSelector");
        let mut old = HashMap::new();
        old.insert("foo".to_string(), "bar".to_string());
        old.insert("a".to_string(), "b".to_string());
        let mut new = HashMap::new();
        new.insert("foo".to_string(), "bar".to_string());
        let errs = validate_node_selector_mutation(&p, Some(&new), Some(&old));
        assert_eq!(errs.len(), 1);
        assert!(errs[0]
            .to_string()
            .contains("only additions to spec.nodeSelector are allowed"));
    }

    #[test]
    fn node_selector_value_mutation_is_rejected() {
        let p = Path::new("spec").child("nodeSelector");
        let mut old = HashMap::new();
        old.insert("foo".to_string(), "bar".to_string());
        let mut new = HashMap::new();
        new.insert("foo".to_string(), "baz".to_string());
        let errs = validate_node_selector_mutation(&p, Some(&new), Some(&old));
        assert_eq!(errs.len(), 1);
    }

    #[test]
    fn node_affinity_nil_old_allows_anything() {
        let p = Path::new("spec").child("affinity").child("nodeAffinity");
        let new = na_required(vec![term(vec![req("a", "In", &["1"])])]);
        assert!(validate_node_affinity_mutation(&p, Some(&new), None).is_empty());
    }

    #[test]
    fn node_affinity_term_addition_inside_term_is_allowed() {
        // Same number of terms; existing matchExpressions preserved as prefix.
        let p = Path::new("spec").child("affinity").child("nodeAffinity");
        let old = na_required(vec![term(vec![req("a", "In", &["1"])])]);
        let new = na_required(vec![term(vec![
            req("a", "In", &["1"]),
            req("b", "In", &["2"]),
        ])]);
        assert!(validate_node_affinity_mutation(&p, Some(&new), Some(&old)).is_empty());
    }

    #[test]
    fn node_affinity_term_count_change_is_rejected() {
        let p = Path::new("spec").child("affinity").child("nodeAffinity");
        let old = na_required(vec![term(vec![req("a", "In", &["1"])])]);
        let new = na_required(vec![
            term(vec![req("a", "In", &["1"])]),
            term(vec![req("b", "In", &["2"])]),
        ]);
        let errs = validate_node_affinity_mutation(&p, Some(&new), Some(&old));
        assert_eq!(errs.len(), 1);
        assert!(errs[0]
            .to_string()
            .contains("no additions/deletions to non-empty NodeSelectorTerms list"));
    }

    #[test]
    fn node_affinity_existing_expression_mutation_is_rejected() {
        let p = Path::new("spec").child("affinity").child("nodeAffinity");
        let old = na_required(vec![term(vec![req("a", "In", &["1"])])]);
        let new = na_required(vec![term(vec![req("a", "In", &["2"])])]);
        let errs = validate_node_affinity_mutation(&p, Some(&new), Some(&old));
        assert_eq!(errs.len(), 1);
        assert!(errs[0].to_string().contains("only additions are allowed"));
    }

    fn mounts_from(json: serde_json::Value) -> Vec<VolumeMount> {
        serde_json::from_value(json).unwrap()
    }

    fn bare_container() -> Container {
        serde_json::from_value(serde_json::json!({"name": "c", "image": "busybox"})).unwrap()
    }

    fn container_from(json: serde_json::Value) -> Container {
        serde_json::from_value(json).unwrap()
    }

    #[test]
    fn volume_mount_must_reference_declared_volume() {
        let vols: HashSet<&str> = ["data"].into_iter().collect();
        let mounts = mounts_from(serde_json::json!([
            {"name": "data", "mountPath": "/data"},
            {"name": "missing", "mountPath": "/other"}
        ]));
        let errs = validate_volume_mounts(
            &mounts,
            &vols,
            &bare_container(),
            &Path::new("volumeMounts"),
        );
        assert_eq!(errs.len(), 1, "{errs:?}");
        assert!(errs[0].to_string().contains("missing"));
    }

    #[test]
    fn duplicate_mount_path_rejected() {
        let vols: HashSet<&str> = ["a", "b"].into_iter().collect();
        let mounts = mounts_from(serde_json::json!([
            {"name": "a", "mountPath": "/x"},
            {"name": "b", "mountPath": "/x"}
        ]));
        let errs = validate_volume_mounts(
            &mounts,
            &vols,
            &bare_container(),
            &Path::new("volumeMounts"),
        );
        assert!(
            errs.iter()
                .any(|e| e.to_string().contains("must be unique")),
            "{errs:?}"
        );
    }

    #[test]
    fn empty_name_and_mount_path_required() {
        let vols: HashSet<&str> = HashSet::new();
        let mounts = mounts_from(serde_json::json!([{"name": "", "mountPath": ""}]));
        let errs = validate_volume_mounts(
            &mounts,
            &vols,
            &bare_container(),
            &Path::new("volumeMounts"),
        );
        assert!(errs.iter().any(|e| e.field.ends_with("name")), "{errs:?}");
        assert!(
            errs.iter().any(|e| e.field.ends_with("mountPath")),
            "{errs:?}"
        );
    }

    #[test]
    fn subpath_must_be_relative_and_no_backsteps() {
        assert!(validate_local_descending_path("data/sub", &Path::new("subPath")).is_empty());
        let abs = validate_local_descending_path("/etc", &Path::new("subPath"));
        assert!(
            abs.iter().any(|e| e.to_string().contains("relative path")),
            "{abs:?}"
        );
        let back = validate_local_descending_path("../../etc", &Path::new("subPath"));
        assert!(
            back.iter()
                .any(|e| e.to_string().contains("must not contain '..'")),
            "{back:?}"
        );
    }

    #[test]
    fn subpath_and_subpathexpr_mutually_exclusive() {
        let vols: HashSet<&str> = ["v"].into_iter().collect();
        let mounts = mounts_from(serde_json::json!([
            {"name": "v", "mountPath": "/m", "subPath": "a", "subPathExpr": "$(POD_NAME)"}
        ]));
        let errs = validate_volume_mounts(
            &mounts,
            &vols,
            &bare_container(),
            &Path::new("volumeMounts"),
        );
        assert!(
            errs.iter()
                .any(|e| e.to_string().contains("mutually exclusive")),
            "{errs:?}"
        );
    }

    #[test]
    fn bidirectional_propagation_requires_privileged() {
        let vols: HashSet<&str> = ["v"].into_iter().collect();
        let mounts = mounts_from(
            serde_json::json!([{"name": "v", "mountPath": "/m", "mountPropagation": "Bidirectional"}]),
        );
        // Non-privileged → forbidden.
        let errs = validate_volume_mounts(
            &mounts,
            &vols,
            &bare_container(),
            &Path::new("volumeMounts"),
        );
        assert!(
            errs.iter()
                .any(|e| e.to_string().contains("privileged containers")),
            "{errs:?}"
        );
        // Privileged → allowed.
        let priv_c = container_from(
            serde_json::json!({"name": "c", "image": "busybox", "securityContext": {"privileged": true}}),
        );
        assert!(
            validate_volume_mounts(&mounts, &vols, &priv_c, &Path::new("volumeMounts")).is_empty()
        );
    }

    #[test]
    fn unsupported_mount_propagation_rejected() {
        let vols: HashSet<&str> = ["v"].into_iter().collect();
        let mounts = mounts_from(
            serde_json::json!([{"name": "v", "mountPath": "/m", "mountPropagation": "Sideways"}]),
        );
        let errs = validate_volume_mounts(
            &mounts,
            &vols,
            &bare_container(),
            &Path::new("volumeMounts"),
        );
        assert!(
            errs.iter().any(|e| e.field.ends_with("mountPropagation")),
            "{errs:?}"
        );
    }

    #[test]
    fn recursive_read_only_requires_read_only() {
        let vols: HashSet<&str> = ["v"].into_iter().collect();
        // Enabled without readOnly → forbidden.
        let mounts = mounts_from(
            serde_json::json!([{"name": "v", "mountPath": "/m", "recursiveReadOnly": "Enabled"}]),
        );
        let errs = validate_volume_mounts(
            &mounts,
            &vols,
            &bare_container(),
            &Path::new("volumeMounts"),
        );
        assert!(
            errs.iter()
                .any(|e| e.to_string().contains("readOnly is true")),
            "{errs:?}"
        );
        // Enabled with readOnly → ok.
        let ok = mounts_from(
            serde_json::json!([{"name": "v", "mountPath": "/m", "readOnly": true, "recursiveReadOnly": "Enabled"}]),
        );
        assert!(
            validate_volume_mounts(&ok, &vols, &bare_container(), &Path::new("volumeMounts"))
                .is_empty()
        );
    }

    #[test]
    fn mount_overlapping_volume_device_rejected() {
        let vols: HashSet<&str> = ["v"].into_iter().collect();
        let c = container_from(serde_json::json!({
            "name": "c", "image": "busybox",
            "volumeDevices": [{"name": "v", "devicePath": "/dev/xvda"}]
        }));
        let mounts = mounts_from(serde_json::json!([{"name": "v", "mountPath": "/dev/xvda"}]));
        let errs = validate_volume_mounts(&mounts, &vols, &c, &Path::new("volumeMounts"));
        assert!(
            errs.iter()
                .any(|e| e.to_string().contains("already exist in volumeDevices")),
            "{errs:?}"
        );
        assert!(
            errs.iter().any(|e| e
                .to_string()
                .contains("already exist as a path in volumeDevices")),
            "{errs:?}"
        );
    }

    #[test]
    fn env_name_must_be_valid() {
        let env: Vec<EnvVar> = serde_json::from_value(serde_json::json!([
            {"name": "MY_ENV.name-1", "value": "ok"},
            {"name": "BAD=NAME", "value": "x"},
            {"name": "", "value": "y"}
        ]))
        .unwrap();
        let errs = validate_env(&env, &Path::new("env"), &test_opts());
        assert!(
            errs.iter().any(|e| e.field.ends_with("[1].name")),
            "{errs:?}"
        );
        assert!(
            errs.iter().any(|e| e.field.ends_with("[2].name")),
            "{errs:?}"
        );
        // index 0 is valid.
        assert!(!errs.iter().any(|e| e.field.contains("[0]")), "{errs:?}");
    }

    /// `RelaxedEnvironmentVariableValidation` is GA and locked on since 1.34
    /// (`pkg/features/kube_features.go`, `LockToDefault: true`), so
    /// `ValidateEnv` and `ValidateEnvFrom` always take their
    /// `IsRelaxedEnvVarName` branch
    /// (`pkg/apis/core/validation/validation.go:2769`, `:2924`): any printable
    /// ASCII other than `=`. The conformance specs "should be consumable as
    /// environment variable names with various prefixes" (ConfigMap and
    /// Secret) send exactly these prefixes.
    /// The v1.35 defaults (both env gates on).
    fn test_opts() -> PodValidationOptions {
        get_validation_options_from_pod_spec_and_meta(None, None)
    }

    fn env_pod(env: serde_json::Value) -> Pod {
        serde_json::from_value(serde_json::json!({
            "metadata": {"name": "p", "namespace": "default"},
            "spec": {"containers": [{"name": "c", "image": "i", "env": env}]}
        }))
        .unwrap()
    }

    fn env_only(errs: ErrorList) -> Vec<String> {
        errs.into_iter()
            .map(|e| e.to_string())
            .filter(|e| e.starts_with("spec.containers[0].env"))
            .collect()
    }

    const STRICT_ENV_NAME_ERR: &str = "a valid environment variable name must consist of alphabetic characters, digits, '_', '-', or '.', and must not start with a digit (e.g. 'my.env-name',  or 'MY_ENV.NAME',  or 'MyEnvName1', regex used for validation is '[-._a-zA-Z][-._a-zA-Z0-9]*')";

    /// `ValidateEnv`, `AllowRelaxedEnvironmentVariableValidation` unset
    /// (validation.go:2769-2778): the strict `IsEnvVarName` applies.
    #[test]
    #[serial_test::serial]
    fn strict_env_name_when_the_relaxed_gate_is_off() {
        let _g = rusternetes_common::feature_gates::with_feature(
            rusternetes_common::feature_gates::Feature::RelaxedEnvironmentVariableValidation,
            false,
        );
        let errs = env_only(validate_pod_create(
            &env_pod(serde_json::json!([{"name": "1bad", "value": "x"}])),
            false,
        ));
        assert_eq!(
            errs,
            vec![format!(
                "spec.containers[0].env[0].name: Invalid value: \"1bad\": {STRICT_ENV_NAME_ERR}"
            )]
        );
    }

    /// `useRelaxedEnvironmentVariableValidation` / `relaxedEnvVarUsed`
    /// (util.go:497, :635): a relaxed name already on the old pod stays valid.
    #[test]
    #[serial_test::serial]
    fn update_keeps_a_relaxed_env_name_the_old_pod_already_used() {
        let _g = rusternetes_common::feature_gates::with_feature(
            rusternetes_common::feature_gates::Feature::RelaxedEnvironmentVariableValidation,
            false,
        );
        let old = env_pod(serde_json::json!([{"name": "1bad", "value": "x"}]));
        let new = env_pod(serde_json::json!([{"name": "1bad", "value": "y"}]));
        let kept = env_only(validate_pod_update(&new, &old, false, false));
        assert!(kept.is_empty(), "{kept:?}");
        // A relaxed name the old pod did not use is not grandfathered, but the
        // option is pod-wide once any name is.
        let other = env_pod(serde_json::json!([{"name": "2bad", "value": "y"}]));
        let errs = env_only(validate_pod_update(&other, &old, false, false));
        assert_eq!(errs.len(), 1, "{errs:?}");
        assert!(errs[0].contains(STRICT_ENV_NAME_ERR), "{errs:?}");
    }

    /// `validateEnvVarValueFrom` `numSources == 0` (validation.go:2843-2847).
    #[test]
    #[serial_test::serial]
    fn no_source_error_names_file_key_ref_only_when_env_files_validation_allowed() {
        let pod = env_pod(serde_json::json!([{"name": "a", "valueFrom": {}}]));
        let on = env_only(validate_pod_create(&pod, false));
        assert_eq!(
            on,
            vec!["spec.containers[0].env[0].valueFrom: Invalid value: \"\": must specify one of: `fieldRef`, `resourceFieldRef`, `configMapKeyRef`, `secretKeyRef` or `fileKeyRef`".to_string()]
        );
        let _g = rusternetes_common::feature_gates::with_feature(
            rusternetes_common::feature_gates::Feature::EnvFiles,
            false,
        );
        let off = env_only(validate_pod_create(&pod, false));
        assert_eq!(
            off,
            vec!["spec.containers[0].env[0].valueFrom: Invalid value: \"\": must specify one of: `fieldRef`, `resourceFieldRef`, `configMapKeyRef` or `secretKeyRef`".to_string()]
        );
        // An old pod that already uses fileKeyRef re-enables it
        // (`useAllowEnvFilesValidation`, util.go:561).
        let old = env_pod(serde_json::json!([{"name": "f", "valueFrom": {"fileKeyRef":
            {"volumeName": "v", "path": "p", "key": "K"}}}]));
        let upd = env_only(validate_pod_update(&pod, &old, false, false));
        assert!(upd.iter().any(|e| e.contains("or `fileKeyRef`")), "{upd:?}");
    }

    /// `TestValidateEnvVarValueFromFileKeyRef` (validation_test.go:7011) via
    /// `validateFileKeySelector` (validation.go:3056).
    #[test]
    fn file_key_ref_is_validated() {
        let errs = |fkr: serde_json::Value| {
            env_only(validate_pod_create(
                &env_pod(serde_json::json!([{"name": "foo", "valueFrom": {"fileKeyRef": fkr}}])),
                false,
            ))
        };
        let p = "spec.containers[0].env[0].valueFrom.fileKeyRef";
        assert!(errs(serde_json::json!({"volumeName": "volume-name", "path": "path/to/file.env", "key": "MY_VAR"})).is_empty());
        assert_eq!(
            errs(
                serde_json::json!({"volumeName": "volume-name", "path": "path/to/file.env", "key": ""})
            ),
            vec![format!("{p}.key: Required value")]
        );
        assert_eq!(
            errs(
                serde_json::json!({"volumeName": "", "path": "path/to/file.env", "key": "MY_VAR"})
            ),
            vec![format!("{p}.volumeName: Required value")]
        );
        assert_eq!(
            errs(serde_json::json!({"volumeName": "volume-name", "path": "", "key": "MY_VAR"})),
            vec![format!("{p}.path: Required value")]
        );
        assert_eq!(
            errs(
                serde_json::json!({"volumeName": "volume-name", "path": "../etc/passwd", "key": "MY_VAR"})
            ),
            vec![format!(
                "{p}.path: Invalid value: \"../etc/passwd\": must not contain '..'"
            )]
        );
        let multi = errs(
            serde_json::json!({"volumeName": "!badname", "path": "../badpath", "key": "bad=key"}),
        );
        assert_eq!(multi.len(), 3, "{multi:?}");
        assert!(
            multi[0].starts_with(&format!("{p}.key: Invalid value: \"bad=key\": ")),
            "{multi:?}"
        );
        assert!(
            multi[1].starts_with(&format!("{p}.volumeName: Invalid value: \"!badname\": ")),
            "{multi:?}"
        );
        assert!(
            multi[2].starts_with(&format!(
                "{p}.path: Invalid value: \"../badpath\": must not contain '..'"
            )),
            "{multi:?}"
        );
    }

    #[test]
    fn env_names_and_prefixes_are_relaxed() {
        let env: Vec<EnvVar> = serde_json::from_value(serde_json::json!([
            {"name": "1bad", "value": "x"},
            {"name": "..", "value": "x"},
            {"name": "#@!", "value": "x"}
        ]))
        .unwrap();
        let errs = validate_env(&env, &Path::new("env"), &test_opts());
        assert!(errs.is_empty(), "{errs:?}");

        let env_from: Vec<EnvFromSource> = serde_json::from_value(serde_json::json!([
            {"prefix": "p-", "configMapRef": {"name": "cm"}},
            {"prefix": "1-", "configMapRef": {"name": "cm"}},
            {"prefix": "$_-", "configMapRef": {"name": "cm"}},
            {"prefix": "p.", "configMapRef": {"name": "cm"}},
            {"prefix": "#@!", "configMapRef": {"name": "cm"}}
        ]))
        .unwrap();
        let errs = validate_env_from(&env_from, &Path::new("envFrom"), &test_opts());
        assert!(errs.is_empty(), "{errs:?}");
    }

    #[test]
    fn an_env_prefix_with_an_equals_sign_is_invalid() {
        let env_from: Vec<EnvFromSource> = serde_json::from_value(serde_json::json!([
            {"prefix": "a=", "configMapRef": {"name": "cm"}}
        ]))
        .unwrap();
        let errs = validate_env_from(&env_from, &Path::new("envFrom"), &test_opts());
        assert_eq!(
            errs.iter().map(|e| e.to_string()).collect::<Vec<_>>(),
            vec!["envFrom[0].prefix: Invalid value: \"a=\": a valid environment variable name must consist only of printable ASCII characters other than '='".to_string()],
        );
    }

    #[test]
    fn env_value_and_valuefrom_mutually_exclusive() {
        let env: Vec<EnvVar> = serde_json::from_value(serde_json::json!([
            {"name": "A", "value": "v", "valueFrom": {"configMapKeyRef": {"name": "cm", "key": "k"}}}
        ])).unwrap();
        let errs = validate_env(&env, &Path::new("env"), &test_opts());
        assert!(
            errs.iter().any(|e| e
                .to_string()
                .contains("may not be specified when `value` is not empty")),
            "{errs:?}"
        );
    }

    #[test]
    fn env_valuefrom_requires_exactly_one_source() {
        // zero sources
        let none: Vec<EnvVar> = serde_json::from_value(serde_json::json!([
            {"name": "A", "valueFrom": {}}
        ]))
        .unwrap();
        assert!(validate_env(&none, &Path::new("env"), &test_opts())
            .iter()
            .any(|e| e.to_string().contains("must specify one of")));
        // two sources
        let two: Vec<EnvVar> = serde_json::from_value(serde_json::json!([
            {"name": "A", "valueFrom": {"configMapKeyRef": {"name": "cm", "key": "k"}, "secretKeyRef": {"name": "s", "key": "k"}}}
        ])).unwrap();
        assert!(validate_env(&two, &Path::new("env"), &test_opts())
            .iter()
            .any(|e| e.to_string().contains("more than one field")));
    }

    #[test]
    fn env_configmap_keyref_requires_key() {
        let env: Vec<EnvVar> = serde_json::from_value(serde_json::json!([
            {"name": "A", "valueFrom": {"configMapKeyRef": {"name": "cm", "key": ""}}}
        ]))
        .unwrap();
        let errs = validate_env(&env, &Path::new("env"), &test_opts());
        assert!(
            errs.iter()
                .any(|e| e.field.ends_with("configMapKeyRef.key")),
            "{errs:?}"
        );
    }

    /// `TestValidateSecurityContext` (validation_test.go:23214-23323).
    fn sc_errs(sc: serde_json::Value, host_users: bool) -> Vec<Error> {
        let sc: SecurityContext = serde_json::from_value(sc).unwrap();
        validate_security_context(&sc, &Path::new("field"), host_users)
    }

    #[test]
    fn security_context_success_cases() {
        let base = || {
            serde_json::json!({"privileged": false, "runAsUser": 1,
            "capabilities": {"add": ["foo"], "drop": ["bar"]}})
        };
        assert!(sc_errs(base(), false).is_empty());
        let mut pm = base();
        pm["procMount"] = "Default".into();
        assert!(sc_errs(pm, true).is_empty());
        let mut um = base();
        um["procMount"] = "Unmasked".into();
        assert!(sc_errs(um, false).is_empty());
    }

    #[test]
    fn security_context_error_cases() {
        let e = sc_errs(serde_json::json!({"runAsUser": -1}), true);
        assert!(e[0].detail.contains("must be between"), "{e:?}");
        let e = sc_errs(serde_json::json!({"runAsGroup": -1}), true);
        assert!(e[0].field == "field.runAsGroup", "{e:?}");
        let e = sc_errs(
            serde_json::json!({"allowPrivilegeEscalation": false, "capabilities": {"add": ["CAP_SYS_ADMIN"]}}),
            true,
        );
        assert!(e[0].detail.contains(
            "cannot set `allowPrivilegeEscalation` to false and `capabilities.Add` CAP_SYS_ADMIN"
        ));
        let e = sc_errs(
            serde_json::json!({"allowPrivilegeEscalation": false, "privileged": true}),
            true,
        );
        assert!(e[0]
            .detail
            .contains("cannot set `allowPrivilegeEscalation` to false and `privileged` to true"));
        // validation.go:8387-8389
        let e = sc_errs(serde_json::json!({"procMount": "Unmasked"}), true);
        assert!(e[0]
            .detail
            .contains("`hostUsers` must be false to use `Unmasked`"));
        assert_eq!(e[0].field, "field.procMount");
        // ValidateProcMountType (:8728)
        let e = sc_errs(serde_json::json!({"procMount": "Bogus"}), false);
        assert_eq!(
            e[0].error_type,
            rusternetes_common::validation::field::ErrorType::NotSupported
        );
    }

    #[test]
    fn pod_spec_unmasked_procmount_needs_host_users_false() {
        // fixtures_procMount.go:93 `failRequiresError`: a container with
        // procMount Unmasked and hostUsers unset fails API validation.
        let mut spec = serde_json::json!({"containers": [{"name": "c", "image": "i",
            "securityContext": {"procMount": "Unmasked"}}]});
        let pod = |spec: &serde_json::Value| -> Pod {
            serde_json::from_value(serde_json::json!({"apiVersion": "v1", "kind": "Pod",
                "metadata": {"name": "p", "namespace": "d"}, "spec": spec}))
            .unwrap()
        };
        let errs = validate_pod_create(&pod(&spec), false);
        assert!(
            errs.iter()
                .any(|e| e.field == "spec.containers[0].securityContext.procMount"),
            "{errs:?}"
        );
        spec["hostUsers"] = false.into();
        let errs = validate_pod_create(&pod(&spec), false);
        assert!(
            !errs.iter().any(|e| e.field.ends_with("procMount")),
            "{errs:?}"
        );
    }

    fn cerrs(json: serde_json::Value) -> Vec<String> {
        let vols: HashSet<&str> = HashSet::new();
        // Gate `PodLifecycleSleepActionAllowZero` is on by default; these tests
        // exercise the strict rule (sleep 0 rejected).
        let mut opts = test_opts();
        opts.allow_pod_lifecycle_sleep_action_zero_value = false;
        validate_container(
            &container_from(json),
            false,
            &vols,
            Some(30),
            None,
            None,
            true,
            &Path::new("spec").child("containers").index(0),
            &opts,
        )
        .into_iter()
        .map(|e| e.to_string())
        .collect()
    }

    fn tsc_errs(json: serde_json::Value) -> Vec<String> {
        let tscs: Vec<TopologySpreadConstraint> = serde_json::from_value(json).unwrap();
        validate_topology_spread_constraints(
            &tscs,
            &Path::new("spec").child("topologySpreadConstraints"),
            &PodValidationOptions::default(),
        )
        .into_iter()
        .map(|e| e.to_string())
        .collect()
    }

    #[test]
    fn image_pull_policy_enum() {
        assert!(cerrs(
            serde_json::json!({"name": "c", "image": "i", "imagePullPolicy": "Sometimes"})
        )
        .iter()
        .any(|e| e.contains("imagePullPolicy")));
        // valid + unset both pass.
        assert!(!cerrs(
            serde_json::json!({"name": "c", "image": "i", "imagePullPolicy": "IfNotPresent"})
        )
        .iter()
        .any(|e| e.contains("imagePullPolicy")));
        assert!(!cerrs(serde_json::json!({"name": "c", "image": "i"}))
            .iter()
            .any(|e| e.contains("imagePullPolicy")));
    }

    #[test]
    fn termination_message_policy_enum() {
        assert!(cerrs(
            serde_json::json!({"name": "c", "image": "i", "terminationMessagePolicy": "Bogus"})
        )
        .iter()
        .any(|e| e.contains("terminationMessagePolicy")));
        assert!(!cerrs(serde_json::json!({"name": "c", "image": "i", "terminationMessagePolicy": "FallbackToLogsOnError"}))
            .iter().any(|e| e.contains("terminationMessagePolicy")));
    }

    #[test]
    fn lifecycle_handler_requires_exactly_one() {
        // empty handler -> required
        let none = cerrs(serde_json::json!({
            "name": "c", "image": "i", "lifecycle": {"preStop": {}}
        }));
        assert!(
            none.iter()
                .any(|e| e.contains("must specify a handler type")),
            "{none:?}"
        );
        // two handlers -> forbidden
        let two = cerrs(serde_json::json!({
            "name": "c", "image": "i",
            "lifecycle": {"postStart": {"exec": {"command": ["x"]}, "sleep": {"seconds": 1}}}
        }));
        assert!(
            two.iter()
                .any(|e| e.contains("may not specify more than 1 handler type")),
            "{two:?}"
        );
        // single handler -> ok
        let ok = cerrs(serde_json::json!({
            "name": "c", "image": "i", "lifecycle": {"preStop": {"exec": {"command": ["x"]}}}
        }));
        assert!(!ok.iter().any(|e| e.contains("handler type")), "{ok:?}");
    }

    #[test]
    fn topology_spread_valid_passes() {
        assert!(tsc_errs(serde_json::json!([
            {"maxSkew": 1, "topologyKey": "zone", "whenUnsatisfiable": "DoNotSchedule",
             "labelSelector": {"matchLabels": {"app": "x"}}}
        ]))
        .is_empty());
    }

    #[test]
    fn topology_spread_field_checks() {
        let e = tsc_errs(serde_json::json!([
            {"maxSkew": 0, "topologyKey": "", "whenUnsatisfiable": "Maybe"}
        ]));
        assert!(e.iter().any(|m| m.contains("maxSkew")), "{e:?}");
        assert!(e.iter().any(|m| m.contains("topologyKey")), "{e:?}");
        assert!(e.iter().any(|m| m.contains("whenUnsatisfiable")), "{e:?}");
    }

    #[test]
    fn topology_spread_duplicate_tuple_rejected() {
        let e = tsc_errs(serde_json::json!([
            {"maxSkew": 1, "topologyKey": "zone", "whenUnsatisfiable": "DoNotSchedule"},
            {"maxSkew": 2, "topologyKey": "zone", "whenUnsatisfiable": "DoNotSchedule"}
        ]));
        assert!(
            e.iter()
                .any(|m| m.contains("topologyKey, whenUnsatisfiable")),
            "{e:?}"
        );
    }

    #[test]
    fn topology_spread_min_domains_requires_donotschedule() {
        let e = tsc_errs(serde_json::json!([
            {"maxSkew": 1, "topologyKey": "zone", "whenUnsatisfiable": "ScheduleAnyway", "minDomains": 2}
        ]));
        assert!(
            e.iter()
                .any(|m| m.contains("can only use minDomains if whenUnsatisfiable=DoNotSchedule")),
            "{e:?}"
        );
    }

    #[test]
    fn topology_spread_node_policy_and_match_label_keys() {
        let bad_policy = tsc_errs(serde_json::json!([
            {"maxSkew": 1, "topologyKey": "z", "whenUnsatisfiable": "DoNotSchedule", "nodeAffinityPolicy": "Bogus"}
        ]));
        assert!(
            bad_policy.iter().any(|m| m.contains("nodeAffinityPolicy")),
            "{bad_policy:?}"
        );
        // matchLabelKeys overlapping the selector is rejected.
        let overlap = tsc_errs(serde_json::json!([
            {"maxSkew": 1, "topologyKey": "z", "whenUnsatisfiable": "DoNotSchedule",
             "labelSelector": {"matchLabels": {"app": "x"}}, "matchLabelKeys": ["app"]}
        ]));
        assert!(
            overlap
                .iter()
                .any(|m| m.contains("exists in both matchLabelKeys and labelSelector")),
            "{overlap:?}"
        );
    }

    fn env_errs(json: serde_json::Value) -> Vec<String> {
        let env: Vec<EnvVar> = serde_json::from_value(json).unwrap();
        validate_env(&env, &Path::new("env"), &test_opts())
            .into_iter()
            .map(|e| e.to_string())
            .collect()
    }

    #[test]
    fn fieldref_allowlist_and_subscripts() {
        // allowed path passes
        assert!(env_errs(serde_json::json!([
            {"name": "A", "valueFrom": {"fieldRef": {"apiVersion": "v1", "fieldPath": "metadata.name"}}}
        ])).is_empty());
        // disallowed path -> NotSupported
        assert!(env_errs(serde_json::json!([
            {"name": "A", "valueFrom": {"fieldRef": {"apiVersion": "v1", "fieldPath": "spec.containers"}}}
        ])).iter().any(|e| e.contains("fieldPath")));
        // annotations subscript with a valid key passes
        assert!(env_errs(serde_json::json!([
            {"name": "A", "valueFrom": {"fieldRef": {"apiVersion": "v1", "fieldPath": "metadata.annotations['my.key/x']"}}}
        ])).is_empty());
        // subscript on a non-subscriptable path -> rejected
        assert!(env_errs(serde_json::json!([
            {"name": "A", "valueFrom": {"fieldRef": {"apiVersion": "v1", "fieldPath": "metadata.name['x']"}}}
        ])).iter().any(|e| e.contains("does not support subscript")));
    }

    #[test]
    fn fieldref_requires_apiversion() {
        assert!(env_errs(serde_json::json!([
            {"name": "A", "valueFrom": {"fieldRef": {"fieldPath": "metadata.name"}}}
        ]))
        .iter()
        .any(|e| e.contains("apiVersion")));
    }

    #[test]
    fn configmap_keyref_name_and_key_format() {
        // invalid name (uppercase not a DNS subdomain) + invalid key char
        let e = env_errs(serde_json::json!([
            {"name": "A", "valueFrom": {"configMapKeyRef": {"name": "Bad_Name", "key": "good.key"}}}
        ]));
        assert!(e.iter().any(|m| m.contains("name")), "{e:?}");
        // valid passes
        assert!(env_errs(serde_json::json!([
            {"name": "A", "valueFrom": {"configMapKeyRef": {"name": "cm", "key": "good.key-1"}}}
        ]))
        .is_empty());
        // backstep key rejected
        assert!(env_errs(serde_json::json!([
            {"name": "A", "valueFrom": {"secretKeyRef": {"name": "s", "key": ".."}}}
        ]))
        .iter()
        .any(|m| m.contains("'..'")));
    }

    #[test]
    fn resourcefieldref_divisor_must_be_quantity() {
        // Go's `resource.Quantity.UnmarshalJSON` rejects it at decode, before
        // validation can see it (#3146).
        assert!(serde_json::from_value::<Vec<EnvVar>>(serde_json::json!([
            {"name": "A", "valueFrom": {"resourceFieldRef": {"resource": "limits.cpu", "divisor": "notaqty"}}}
        ]))
        .is_err());
    }

    // ---- container ports -------------------------------------------------

    fn cports_errs(json: serde_json::Value) -> Vec<String> {
        let ports: Vec<ContainerPort> = serde_json::from_value(json).unwrap();
        validate_container_ports(
            &ports,
            &Path::new("spec")
                .child("containers")
                .index(0)
                .child("ports"),
        )
        .into_iter()
        .map(|e| e.to_string())
        .collect()
    }

    #[test]
    fn container_port_protocol_required_and_enum() {
        // explicit empty protocol -> Required (a *missing* protocol is defaulted
        // to TCP by serde before validation, so "" is the only Required case).
        let missing = cports_errs(serde_json::json!([{"containerPort": 80, "protocol": ""}]));
        assert!(
            missing
                .iter()
                .any(|m| m.contains("ports[0].protocol") && m.contains("Required")),
            "{missing:?}"
        );
        // unsupported protocol
        let bad = cports_errs(serde_json::json!([{"containerPort": 80, "protocol": "QUIC"}]));
        assert!(bad.iter().any(|m| m.contains("protocol")), "{bad:?}");
        // valid
        let ok = cports_errs(serde_json::json!([{"containerPort": 80, "protocol": "TCP"}]));
        assert!(ok.is_empty(), "{ok:?}");
    }

    #[test]
    fn stop_signal_forbidden_when_os_empty() {
        let p = Path::new("lifecycle").child("stopSignal");
        // os = None
        let e = validate_stop_signal("SIGTERM", None, &p);
        assert!(
            e.iter().any(
                |x| x.to_string().contains("spec.os.name") && x.to_string().contains("may not")
            ),
            "{e:?}"
        );
        // os.name = ""
        let os = PodOS {
            name: String::new(),
        };
        let e = validate_stop_signal("SIGTERM", Some(&os), &p);
        assert!(!e.is_empty(), "empty os.name must forbid stopSignal: {e:?}");
    }

    #[test]
    fn stop_signal_windows_enum() {
        let p = Path::new("lifecycle").child("stopSignal");
        let win = PodOS {
            name: "windows".to_string(),
        };
        // Windows allows only SIGKILL / SIGTERM.
        assert!(validate_stop_signal("SIGTERM", Some(&win), &p).is_empty());
        assert!(validate_stop_signal("SIGKILL", Some(&win), &p).is_empty());
        // A Linux-only signal is rejected on Windows.
        let e = validate_stop_signal("SIGUSR1", Some(&win), &p);
        assert!(
            e.iter().any(|x| x.field.ends_with("stopSignal")),
            "SIGUSR1 must be NotSupported on windows: {e:?}"
        );
    }

    #[test]
    fn stop_signal_linux_enum() {
        let p = Path::new("lifecycle").child("stopSignal");
        let lin = PodOS {
            name: "linux".to_string(),
        };
        // A representative spread of the Linux set, incl. realtime-range names.
        for sig in ["SIGTERM", "SIGUSR1", "SIGRTMIN+3", "SIGRTMAX-1", "SIGRTMAX"] {
            assert!(
                validate_stop_signal(sig, Some(&lin), &p).is_empty(),
                "{sig} must be allowed on linux"
            );
        }
        // Not a real signal name.
        let e = validate_stop_signal("SIGNOTASIGNAL", Some(&lin), &p);
        assert!(
            e.iter().any(|x| x.field.ends_with("stopSignal")),
            "bogus signal must be NotSupported on linux: {e:?}"
        );
    }

    #[test]
    fn container_port_zero_is_required_not_invalid() {
        let e = cports_errs(serde_json::json!([{"containerPort": 0, "protocol": "TCP"}]));
        assert!(
            e.iter()
                .any(|m| m.contains("containerPort") && m.contains("Required")),
            "{e:?}"
        );
        assert!(
            !e.iter().any(|m| m.contains("between 1 and 65535")),
            "zero should be Required, not Invalid range: {e:?}"
        );
    }

    #[test]
    fn container_port_name_is_valid_port_name() {
        // uppercase / too long / leading hyphen / consecutive hyphens / no letter
        let bad_upper = cports_errs(
            serde_json::json!([{"containerPort": 80, "protocol": "TCP", "name": "Http"}]),
        );
        assert!(
            bad_upper.iter().any(|m| m.contains("name")),
            "{bad_upper:?}"
        );
        let no_letter = cports_errs(
            serde_json::json!([{"containerPort": 80, "protocol": "TCP", "name": "123"}]),
        );
        assert!(
            no_letter.iter().any(|m| m.contains("at least one letter")),
            "{no_letter:?}"
        );
        let lead = cports_errs(
            serde_json::json!([{"containerPort": 80, "protocol": "TCP", "name": "-ab"}]),
        );
        assert!(
            lead.iter()
                .any(|m| m.contains("begin or end with a hyphen")),
            "{lead:?}"
        );
        let dbl = cports_errs(
            serde_json::json!([{"containerPort": 80, "protocol": "TCP", "name": "a--b"}]),
        );
        assert!(
            dbl.iter().any(|m| m.contains("consecutive hyphens")),
            "{dbl:?}"
        );
        let long = cports_errs(
            serde_json::json!([{"containerPort": 80, "protocol": "TCP", "name": "abcdefghijklmnop"}]),
        );
        assert!(long.iter().any(|m| m.contains("15 characters")), "{long:?}");
        // valid name passes
        let ok = cports_errs(
            serde_json::json!([{"containerPort": 80, "protocol": "TCP", "name": "http-1"}]),
        );
        assert!(ok.is_empty(), "{ok:?}");
    }

    #[test]
    fn container_port_name_duplicate_rejected() {
        let e = cports_errs(serde_json::json!([
            {"containerPort": 80, "protocol": "TCP", "name": "web"},
            {"containerPort": 81, "protocol": "TCP", "name": "web"}
        ]));
        assert!(
            e.iter()
                .any(|m| m.contains("ports[1].name") && m.contains("Duplicate")),
            "{e:?}"
        );
    }

    #[test]
    fn host_port_conflict_across_containers() {
        // Same proto/hostIP/port on two containers -> Duplicate on the second.
        let pod: Pod = serde_json::from_value(serde_json::json!({
            "apiVersion": "v1", "kind": "Pod",
            "metadata": {"name": "p"},
            "spec": {"containers": [
                {"name": "a", "image": "i", "ports": [{"containerPort": 80, "protocol": "TCP", "hostPort": 8080}]},
                {"name": "b", "image": "i", "ports": [{"containerPort": 81, "protocol": "TCP", "hostPort": 8080}]}
            ]}
        }))
        .unwrap();
        let errs: Vec<String> = validate_pod_create(&pod, false)
            .into_iter()
            .map(|e| e.to_string())
            .collect();
        assert!(
            errs.iter()
                .any(|m| m.contains("hostPort") && m.contains("Duplicate")),
            "{errs:?}"
        );
        // Different protocol -> no conflict.
        let pod2: Pod = serde_json::from_value(serde_json::json!({
            "apiVersion": "v1", "kind": "Pod",
            "metadata": {"name": "p"},
            "spec": {"containers": [
                {"name": "a", "image": "i", "ports": [{"containerPort": 80, "protocol": "TCP", "hostPort": 8080}]},
                {"name": "b", "image": "i", "ports": [{"containerPort": 81, "protocol": "UDP", "hostPort": 8080}]}
            ]}
        }))
        .unwrap();
        let errs2: Vec<String> = validate_pod_create(&pod2, false)
            .into_iter()
            .map(|e| e.to_string())
            .collect();
        assert!(!errs2.iter().any(|m| m.contains("Duplicate")), "{errs2:?}");
    }

    // ---- probe / handler bodies -----------------------------------------

    #[test]
    fn probe_http_get_path_required_and_scheme_enum() {
        // missing path + bad scheme
        let e = cerrs(serde_json::json!({
            "name": "c", "image": "i",
            "livenessProbe": {"httpGet": {"port": 8080, "scheme": "FTP"}}
        }));
        assert!(
            e.iter()
                .any(|m| m.contains("httpGet.path") && m.contains("Required")),
            "{e:?}"
        );
        assert!(e.iter().any(|m| m.contains("httpGet.scheme")), "{e:?}");
        // valid passes
        let ok = cerrs(serde_json::json!({
            "name": "c", "image": "i",
            "livenessProbe": {"httpGet": {"path": "/healthz", "port": 8080, "scheme": "HTTPS"}}
        }));
        assert!(!ok.iter().any(|m| m.contains("httpGet")), "{ok:?}");
    }

    #[test]
    fn probe_http_header_name_validated() {
        let e = cerrs(serde_json::json!({
            "name": "c", "image": "i",
            "livenessProbe": {"httpGet": {"path": "/", "port": 80,
                "httpHeaders": [{"name": "Bad Header", "value": "v"}]}}
        }));
        assert!(e.iter().any(|m| m.contains("httpHeaders")), "{e:?}");
    }

    #[test]
    fn probe_port_num_or_name_validated() {
        // out-of-range int port on tcpSocket
        let e = cerrs(serde_json::json!({
            "name": "c", "image": "i",
            "livenessProbe": {"tcpSocket": {"port": 99999}}
        }));
        assert!(
            e.iter()
                .any(|m| m.contains("tcpSocket.port") && m.contains("65535")),
            "{e:?}"
        );
        // bad named port on grpc
        let g = cerrs(serde_json::json!({
            "name": "c", "image": "i",
            "livenessProbe": {"grpc": {"port": -1}}
        }));
        assert!(g.iter().any(|m| m.contains("grpc.port")), "{g:?}");
    }

    #[test]
    fn probe_exec_command_required() {
        let e = cerrs(serde_json::json!({
            "name": "c", "image": "i",
            "livenessProbe": {"exec": {"command": []}}
        }));
        assert!(
            e.iter()
                .any(|m| m.contains("exec.command") && m.contains("Required")),
            "{e:?}"
        );
    }

    #[test]
    fn probe_numeric_fields_nonnegative() {
        let e = cerrs(serde_json::json!({
            "name": "c", "image": "i",
            "livenessProbe": {"tcpSocket": {"port": 80}, "periodSeconds": -5}
        }));
        assert!(
            e.iter()
                .any(|m| m.contains("periodSeconds") && m.contains("greater than or equal to 0")),
            "{e:?}"
        );
    }

    #[test]
    fn liveness_success_threshold_must_be_one() {
        let e = cerrs(serde_json::json!({
            "name": "c", "image": "i",
            "livenessProbe": {"tcpSocket": {"port": 80}, "successThreshold": 2}
        }));
        assert!(
            e.iter()
                .any(|m| m.contains("livenessProbe.successThreshold") && m.contains("must be 1")),
            "{e:?}"
        );
        // startup probe same rule
        let s = cerrs(serde_json::json!({
            "name": "c", "image": "i",
            "startupProbe": {"tcpSocket": {"port": 80}, "successThreshold": 3}
        }));
        assert!(
            s.iter()
                .any(|m| m.contains("startupProbe.successThreshold") && m.contains("must be 1")),
            "{s:?}"
        );
    }

    #[test]
    fn readiness_termination_grace_period_forbidden() {
        let e = cerrs(serde_json::json!({
            "name": "c", "image": "i",
            "readinessProbe": {"tcpSocket": {"port": 80}, "terminationGracePeriodSeconds": 5}
        }));
        assert!(
            e.iter().any(
                |m| m.contains("readinessProbe.terminationGracePeriodSeconds")
                    && m.contains("must not be set for readinessProbes")
            ),
            "{e:?}"
        );
    }

    #[test]
    fn probe_termination_grace_period_must_be_positive() {
        let e = cerrs(serde_json::json!({
            "name": "c", "image": "i",
            "livenessProbe": {"tcpSocket": {"port": 80}, "terminationGracePeriodSeconds": 0}
        }));
        assert!(
            e.iter().any(
                |m| m.contains("livenessProbe.terminationGracePeriodSeconds")
                    && m.contains("must be greater than 0")
            ),
            "{e:?}"
        );
    }

    #[test]
    fn lifecycle_sleep_duration_bounded_by_grace_period() {
        // cerrs uses grace_period = Some(30); sleep 0 and >30 both rejected.
        let zero = cerrs(serde_json::json!({
            "name": "c", "image": "i",
            "lifecycle": {"preStop": {"sleep": {"seconds": 0}}}
        }));
        assert!(
            zero.iter()
                .any(|m| m.contains("preStop.sleep") && m.contains("greater than 0")),
            "{zero:?}"
        );
        let over = cerrs(serde_json::json!({
            "name": "c", "image": "i",
            "lifecycle": {"postStart": {"sleep": {"seconds": 31}}}
        }));
        assert!(
            over.iter().any(|m| m.contains("postStart.sleep")
                && m.contains("terminationGracePeriodSeconds (30)")),
            "{over:?}"
        );
        // within range passes
        let ok = cerrs(serde_json::json!({
            "name": "c", "image": "i",
            "lifecycle": {"preStop": {"sleep": {"seconds": 10}}}
        }));
        assert!(!ok.iter().any(|m| m.contains("sleep")), "{ok:?}");
    }

    #[test]
    fn lifecycle_handler_action_body_validated() {
        // httpGet handler in a lifecycle hook with missing path is rejected.
        let e = cerrs(serde_json::json!({
            "name": "c", "image": "i",
            "lifecycle": {"postStart": {"httpGet": {"port": 8080}}}
        }));
        assert!(
            e.iter()
                .any(|m| m.contains("postStart.httpGet.path") && m.contains("Required")),
            "{e:?}"
        );
    }

    #[test]
    fn env_from_requires_exactly_one_source() {
        let none = cerrs(serde_json::json!({
            "name": "c", "image": "i", "envFrom": [{}]
        }));
        assert!(
            none.iter()
                .any(|m| m.contains("must specify one of: `configMapRef` or `secretRef`")),
            "{none:?}"
        );

        let both = cerrs(serde_json::json!({
            "name": "c", "image": "i",
            "envFrom": [{"configMapRef": {"name": "cm"}, "secretRef": {"name": "s"}}]
        }));
        assert!(
            both.iter()
                .any(|m| m.contains("may not have more than one field specified at a time")),
            "{both:?}"
        );
    }

    #[test]
    fn volume_devices_names_and_paths_are_unique() {
        let vols: HashSet<&str> = ["v", "w"].into_iter().collect();
        let c = container_from(serde_json::json!({
            "name": "c", "image": "i",
            "volumeDevices": [
                {"name": "v", "devicePath": "/dev/a"},
                {"name": "v", "devicePath": "/dev/a"},
            ]
        }));
        let e: Vec<String> = validate_container(
            &c,
            false,
            &vols,
            Some(30),
            None,
            None,
            true,
            &Path::new("spec").child("containers").index(0),
            &test_opts(),
        )
        .into_iter()
        .map(|x| x.to_string())
        .collect();
        assert!(
            e.iter()
                .any(|m| m.contains("volumeDevices[1].name") && m.contains("must be unique")),
            "{e:?}"
        );
        assert!(
            e.iter()
                .any(|m| m.contains("volumeDevices[1].devicePath") && m.contains("must be unique")),
            "{e:?}"
        );
    }

    #[test]
    fn volume_device_must_name_a_declared_volume() {
        let e = cerrs(serde_json::json!({
            "name": "c", "image": "i",
            "volumeDevices": [{"name": "nope", "devicePath": "/dev/a"}]
        }));
        assert!(
            e.iter()
                .any(|m| m.contains("volumeDevices[0].name") && m.contains("Not found")),
            "{e:?}"
        );
    }

    #[test]
    fn resize_policy_enums_are_not_supported_rather_than_ignored() {
        let e = cerrs(serde_json::json!({
            "name": "c", "image": "i",
            "resizePolicy": [{"resourceName": "storage", "restartPolicy": "Reboot"}]
        }));
        assert!(
            e.iter()
                .any(|m| m.contains("resizePolicy") && m.contains("\"storage\"")),
            "{e:?}"
        );
        assert!(
            e.iter()
                .any(|m| m.contains("resizePolicy") && m.contains("\"Reboot\"")),
            "{e:?}"
        );
    }

    #[test]
    fn resize_policy_must_be_not_required_when_the_pod_never_restarts() {
        let vols: HashSet<&str> = HashSet::new();
        let c = container_from(serde_json::json!({
            "name": "c", "image": "i",
            "resizePolicy": [{"resourceName": "cpu", "restartPolicy": "RestartContainer"}]
        }));
        let e: Vec<String> = validate_container(
            &c,
            false,
            &vols,
            Some(30),
            None,
            Some("Never"),
            true,
            &Path::new("spec").child("containers").index(0),
            &test_opts(),
        )
        .into_iter()
        .map(|x| x.to_string())
        .collect();
        assert!(
            e.iter()
                .any(|m| m.contains("must be 'NotRequired' when `restartPolicy` is 'Never'")),
            "{e:?}"
        );
    }

    #[test]
    fn a_restart_rule_requires_exit_codes() {
        let e = restart_errs(
            serde_json::json!({
                "restartPolicy": "Never",
                "restartPolicyRules": [{"action": "Restart"}]
            }),
            false,
            true,
            false,
        );
        assert!(
            e.iter()
                .any(|m| m.contains("restartPolicyRules[0].exitCodes")
                    && m.contains("must be specified")),
            "{e:?}"
        );
    }

    #[test]
    fn seccomp_localhost_requires_a_localhost_profile() {
        let missing = cerrs(serde_json::json!({
            "name": "c", "image": "i",
            "securityContext": {"seccompProfile": {"type": "Localhost"}}
        }));
        assert!(
            missing
                .iter()
                .any(|m| m.contains("seccompProfile.localhostProfile")
                    && m.contains("must be set when seccomp type is Localhost")),
            "{missing:?}"
        );

        let stray = cerrs(serde_json::json!({
            "name": "c", "image": "i",
            "securityContext": {
                "seccompProfile": {"type": "RuntimeDefault", "localhostProfile": "p"}
            }
        }));
        assert!(
            stray
                .iter()
                .any(|m| m.contains("seccompProfile.localhostProfile")
                    && m.contains("can only be set when seccomp type is Localhost")),
            "{stray:?}"
        );
    }

    #[test]
    fn app_armor_profile_type_must_be_supported() {
        let e = cerrs(serde_json::json!({
            "name": "c", "image": "i",
            "securityContext": {"appArmorProfile": {"type": "Enforce"}}
        }));
        assert!(
            e.iter()
                .any(|m| m.contains("appArmorProfile.type") && m.contains("Unsupported value")),
            "{e:?}"
        );
    }

    #[test]
    fn app_armor_localhost_profile_must_not_be_padded() {
        let e = cerrs(serde_json::json!({
            "name": "c", "image": "i",
            "securityContext": {
                "appArmorProfile": {"type": "Localhost", "localhostProfile": " p "}
            }
        }));
        assert!(
            e.iter()
                .any(|m| m.contains("appArmorProfile.localhostProfile")
                    && m.contains("must not be padded with whitespace")),
            "{e:?}"
        );
    }

    /// With `PodLifecycleSleepActionAllowZero` (GA, locked on in 1.35),
    /// `GetValidationOptionsFromPodSpecAndMeta` sets
    /// `AllowPodLifecycleSleepActionZeroValue`, so a zero sleep is valid and a
    /// negative one reports the zero-allowed message (validation.go:3381-3386).
    #[test]
    fn create_allows_zero_sleep_via_gate_options() {
        let mk = |secs: i64| -> Pod {
            serde_json::from_value(serde_json::json!({
                "metadata": {"name": "p", "namespace": "default"},
                "spec": {
                    "terminationGracePeriodSeconds": 30,
                    "containers": [{"name": "c", "image": "i",
                        "lifecycle": {"preStop": {"sleep": {"seconds": secs}}}}]
                }
            }))
            .unwrap()
        };
        let zero: Vec<String> = validate_pod_create(&mk(0), false)
            .into_iter()
            .map(|e| e.to_string())
            .collect();
        assert!(!zero.iter().any(|m| m.contains("sleep")), "{zero:?}");
        let neg: Vec<String> = validate_pod_create(&mk(-1), false)
            .into_iter()
            .map(|e| e.to_string())
            .collect();
        assert!(
            neg.iter().any(|m| m.contains("preStop.sleep")
                && m.contains(
                    "must be non-negative and less than terminationGracePeriodSeconds (30)"
                )),
            "{neg:?}"
        );
    }

    /// `TestValidateSleepAction` (validation_test.go:27232).
    #[test]
    fn validate_sleep_action_cases() {
        let strict = |gp: i64| {
            format!("must be greater than 0 and less than terminationGracePeriodSeconds ({gp}). Enable AllowPodLifecycleSleepActionZeroValue feature gate for zero sleep.")
        };
        let relaxed = |gp: i64| {
            format!("must be non-negative and less than terminationGracePeriodSeconds ({gp})")
        };
        // (seconds, gracePeriod, zeroEnabled, expected error)
        let cases: [(i64, Option<i64>, bool, Option<String>); 10] = [
            (5, Some(30), false, None),
            (-1, Some(30), false, Some(strict(30))),
            (5, Some(3), false, Some(strict(3))),
            (0, Some(30), false, Some(strict(30))),
            (0, Some(30), true, None),
            (-1, Some(30), false, Some(strict(30))),
            (-1, Some(30), true, Some(relaxed(30))),
            (0, Some(0), true, None),
            // A nil grace period is not flagged here.
            (5, None, false, None),
            (0, None, true, None),
        ];
        let fld = Path::new("root");
        for (secs, gp, zero, want) in cases {
            let opts = PodValidationOptions {
                allow_pod_lifecycle_sleep_action_zero_value: zero,
                ..Default::default()
            };
            let got = validate_sleep_action(&SleepAction { seconds: secs }, gp, &fld, &opts);
            match want {
                None => assert!(got.is_empty(), "{secs} {gp:?} {zero}: {got:?}"),
                Some(msg) => {
                    assert_eq!(got.len(), 1, "{secs} {gp:?} {zero}: {got:?}");
                    assert_eq!(
                        got[0].to_string(),
                        format!("root: Invalid value: {secs}: {msg}")
                    );
                }
            }
        }
    }

    /// `validateResourceQuantityHugePageValue` (`validation.go:7955`), reached
    /// from `validateResourceRequirements` (`:7846`, `:7877`) with
    /// `opts.AllowIndivisibleHugePagesValues` from
    /// `GetValidationOptionsFromPodSpecAndMeta` (`util.go:448`). Upstream has no
    /// unit test for the rule itself, so these are behavioural.
    fn huge_pod(res: serde_json::Value) -> Pod {
        serde_json::from_value(serde_json::json!({
            "apiVersion": "v1", "kind": "Pod",
            "metadata": {"name": "p", "namespace": "default"},
            "spec": {"containers": [{"name": "c", "image": "i", "resources": res}]}
        }))
        .unwrap()
    }

    fn huge_errs(errs: ErrorList) -> Vec<String> {
        errs.into_iter()
            .map(|e| e.to_string())
            .filter(|m| m.contains("not positive integer multiple"))
            .collect()
    }

    #[test]
    fn indivisible_hugepages_rejected_on_create() {
        let pod = huge_pod(serde_json::json!({
            "requests": {"memory": "1Gi", "hugepages-2Mi": "3Mi"},
            "limits": {"memory": "1Gi", "hugepages-2Mi": "3Mi"}
        }));
        let m = huge_errs(validate_pod_create(&pod, false));
        assert_eq!(m.len(), 2, "{m:?}");
        assert!(
            m.iter()
                .all(|x| x.contains("3Mi is not positive integer multiple of hugepages-2Mi")),
            "{m:?}"
        );
        assert!(
            m[0].contains("spec.containers[0].resources.limits[hugepages-2Mi]"),
            "{m:?}"
        );
        assert!(
            m[1].contains("spec.containers[0].resources.requests[hugepages-2Mi]"),
            "{m:?}"
        );
    }

    /// Validate one container's restart fields under explicit options
    /// (`TestValidateContainers` restart-policy cases, validation_test.go:29295
    /// and `TestValidateInitContainers`, :29480).
    fn restart_errs(
        container: serde_json::Value,
        is_init: bool,
        rules_on: bool,
        restart_all: bool,
    ) -> Vec<String> {
        let mut base = serde_json::json!({"name": "name", "image": "image",
            "imagePullPolicy": "IfNotPresent", "terminationMessagePolicy": "File"});
        for (k, v) in container.as_object().unwrap() {
            base[k] = v.clone();
        }
        let c = container_from(base);
        let opts = PodValidationOptions {
            allow_container_restart_policy_rules: rules_on,
            allow_restart_all_containers: restart_all,
            ..PodValidationOptions::default()
        };
        let root = Path::new(if is_init {
            "initContainers"
        } else {
            "containers"
        })
        .index(0);
        validate_container(
            &c,
            is_init,
            &HashSet::new(),
            Some(30),
            None,
            Some("Always"),
            true,
            &root,
            &opts,
        )
        .into_iter()
        .map(|e| e.to_string())
        .collect()
    }

    fn rule(action: &str, op: &str) -> serde_json::Value {
        serde_json::json!({"action": action, "exitCodes": {"operator": op, "values": [42]}})
    }

    #[test]
    fn container_restart_policy_accepts_valid_cases() {
        for policy in [None, Some("Always"), Some("Never"), Some("OnFailure")] {
            let c = match policy {
                Some(p) => serde_json::json!({"restartPolicy": p}),
                None => serde_json::json!({}),
            };
            assert_eq!(restart_errs(c, false, true, false), Vec::<String>::new());
        }
        let with_rule = serde_json::json!({"restartPolicy": "Never",
            "restartPolicyRules": [rule("Restart", "In")]});
        assert!(restart_errs(with_rule, false, true, false).is_empty());
        let restart_all = serde_json::json!({"restartPolicy": "Never",
            "restartPolicyRules": [rule("RestartAllContainers", "In")]});
        assert!(restart_errs(restart_all.clone(), false, true, true).is_empty());
        // Without AllowRestartAllContainers the action is not supported.
        let e = restart_errs(restart_all, false, true, false);
        assert!(
            e.iter().any(|m| m.starts_with("containers[0].restartPolicyRules[0].action: Unsupported value: \"RestartAllContainers\"")),
            "{e:?}"
        );
    }

    #[test]
    fn divisible_hugepages_accepted_on_create() {
        let pod = huge_pod(serde_json::json!({
            "requests": {"memory": "1Gi", "hugepages-2Mi": "4Mi"},
            "limits": {"memory": "1Gi", "hugepages-2Mi": "4Mi"}
        }));
        assert!(huge_errs(validate_pod_create(&pod, false)).is_empty());
    }

    #[test]
    fn indivisible_hugepages_allowed_on_update_when_old_spec_has_them() {
        let pod = huge_pod(serde_json::json!({
            "requests": {"memory": "1Gi", "hugepages-2Mi": "3Mi"},
            "limits": {"memory": "1Gi", "hugepages-2Mi": "3Mi"}
        }));
        assert!(huge_errs(validate_pod_update(&pod, &pod, false, false)).is_empty());
        // An old spec that is divisible does not relax the rule.
        let old = huge_pod(serde_json::json!({
            "requests": {"memory": "1Gi", "hugepages-2Mi": "4Mi"},
            "limits": {"memory": "1Gi", "hugepages-2Mi": "4Mi"}
        }));
        assert_eq!(
            huge_errs(validate_pod_update(&pod, &old, false, false)).len(),
            2
        );
    }

    #[test]
    fn container_restart_policy_rejects_invalid_cases() {
        let one = |c: serde_json::Value| restart_errs(c, false, true, false);
        let e = one(serde_json::json!({"restartPolicy": "invalid"}));
        assert_eq!(e.len(), 1, "{e:?}");
        assert!(
            e[0].starts_with("containers[0].restartPolicy: Unsupported value: \"invalid\""),
            "{e:?}"
        );
        let e = one(serde_json::json!({"restartPolicy": ""}));
        assert!(
            e[0].starts_with("containers[0].restartPolicy: Unsupported value: \"\""),
            "{e:?}"
        );
        let e = one(serde_json::json!({"restartPolicyRules": [rule("Restart", "In")]}));
        assert_eq!(e.len(), 1, "{e:?}");
        assert!(
            e[0].starts_with("containers[0].restartPolicy: Required value"),
            "{e:?}"
        );
        for action in ["invalid", ""] {
            let e = one(serde_json::json!({"restartPolicy": "Never",
                "restartPolicyRules": [rule(action, "In")]}));
            assert_eq!(e.len(), 1, "{e:?}");
            assert!(
                e[0].starts_with(&format!(
                    "containers[0].restartPolicyRules[0].action: Unsupported value: \"{action}\""
                )),
                "{e:?}"
            );
        }
        let e = one(serde_json::json!({"restartPolicy": "Never",
            "restartPolicyRules": [{"action": "Restart"}]}));
        assert_eq!(e.len(), 1, "{e:?}");
        assert!(
            e[0].starts_with("containers[0].restartPolicyRules[0].exitCodes: Required value"),
            "{e:?}"
        );
        for op in ["invalid", ""] {
            let e = one(serde_json::json!({"restartPolicy": "Never",
                "restartPolicyRules": [rule("Restart", op)]}));
            assert_eq!(e.len(), 1, "{e:?}");
            assert!(e[0].starts_with(&format!("containers[0].restartPolicyRules[0].exitCodes.operator: Unsupported value: \"{op}\"")), "{e:?}");
        }
        let many: Vec<_> = (0..21).map(|_| rule("Restart", "In")).collect();
        let e = one(serde_json::json!({"restartPolicy": "Never", "restartPolicyRules": many}));
        assert!(
            e.iter()
                .any(|m| m.starts_with("containers[0].restartPolicyRules: Too long")),
            "{e:?}"
        );
        let values: Vec<i32> = (0..256).collect();
        let e = one(
            serde_json::json!({"restartPolicy": "Never", "restartPolicyRules": [
            {"action": "Restart", "exitCodes": {"operator": "In", "values": values}}]}),
        );
        assert!(
            e.iter()
                .any(|m| m
                    .starts_with("containers[0].restartPolicyRules[0].exitCodes.values: Too long")),
            "{e:?}"
        );
    }

    /// Without `AllowContainerRestartPolicyRules` a regular container may not
    /// carry a restartPolicy (validation.go:4059-4063).
    #[test]
    fn container_restart_policy_forbidden_without_option() {
        let e = restart_errs(
            serde_json::json!({"restartPolicy": "Never"}),
            false,
            false,
            false,
        );
        assert_eq!(e.len(), 1, "{e:?}");
        assert!(
            e[0].starts_with(
                "containers[0].restartPolicy: Forbidden: may not be set for non-init containers"
            ),
            "{e:?}"
        );
        assert!(restart_errs(serde_json::json!({}), false, false, false).is_empty());
    }

    /// `TestValidateInitContainers` restart-policy cases (validation_test.go:29480+).
    #[test]
    fn init_container_restart_policy_rules() {
        let sidecar_rules = serde_json::json!({"restartPolicy": "Always",
            "restartPolicyRules": [rule("RestartAllContainers", "In")]});
        assert!(restart_errs(
            serde_json::json!({"restartPolicy": "Always"}),
            true,
            true,
            false
        )
        .is_empty());
        // Sidecar rules are allowed with AllowRestartAllContainers.
        assert!(restart_errs(sidecar_rules.clone(), true, true, true).is_empty());
        // ... and Forbidden without it.
        let e = restart_errs(sidecar_rules, true, true, false);
        assert_eq!(e.len(), 1, "{e:?}");
        assert!(
            e[0].starts_with("initContainers[0].restartPolicyRules: Forbidden"),
            "{e:?}"
        );
        // Rules with no restartPolicy: Required.
        let e = restart_errs(
            serde_json::json!({"restartPolicyRules": [rule("RestartAllContainers", "In")]}),
            true,
            true,
            true,
        );
        assert_eq!(e.len(), 1, "{e:?}");
        assert!(
            e[0].starts_with("initContainers[0].restartPolicy: Required value"),
            "{e:?}"
        );
        // A non-sidecar init container may use a policy once the option is on.
        assert!(restart_errs(
            serde_json::json!({"restartPolicy": "Never",
            "restartPolicyRules": [rule("Restart", "In")]}),
            true,
            true,
            false
        )
        .is_empty());
        // Option off: only `Always` is supported.
        let e = restart_errs(
            serde_json::json!({"restartPolicy": "Never"}),
            true,
            false,
            false,
        );
        assert_eq!(e.len(), 1, "{e:?}");
        assert!(e[0].starts_with("initContainers[0].restartPolicy: Unsupported value: \"Never\": supported values: \"Always\""), "{e:?}");
        assert!(restart_errs(
            serde_json::json!({"restartPolicy": "Always"}),
            true,
            false,
            false
        )
        .is_empty());
    }
}
