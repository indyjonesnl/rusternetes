//! Unit tests moved from `src/scheduler_config.rs`.
#![allow(unused_imports, dead_code)]

mod __parent {
    pub use rusternetes_common::go_duration::format_go_duration;
    pub use rusternetes_common::label_selector::{LabelOperator, LabelRequirement};
    pub use rusternetes_common::quota::is_extended_resource_name;
    pub use rusternetes_common::resources::pod::{
        NodeAffinity, NodeSelectorRequirement, NodeSelectorTerm, TopologySpreadConstraint,
    };
    pub use rusternetes_common::validation::field::{self, BadValue, ErrorList, Path};
    pub use rusternetes_common::validation::metav1::{is_qualified_name, validate_label_name};
    pub use rusternetes_scheduler::framework_runtime::{
        default_plugins, merge_plugins, PluginSet, Plugins,
    };
    pub use rusternetes_scheduler::volume_binding::{
        FunctionShapePoint, MAX_CUSTOM_PRIORITY_SCORE, MAX_NODE_SCORE,
    };
    pub use serde::{Deserialize, Serialize};
}

mod tests {
    use super::__parent::*;
    use rusternetes_common::validation::field::ErrorType;
    use rusternetes_scheduler::scheduler_config::*;

    fn pt(utilization: i32, score: i32) -> UtilizationShapePoint {
        UtilizationShapePoint { utilization, score }
    }

    fn args(timeout: i64, shape: Option<Vec<UtilizationShapePoint>>) -> VolumeBindingArgs {
        VolumeBindingArgs {
            bind_timeout_seconds: Some(timeout),
            shape,
        }
    }

    fn summary(errs: &ErrorList) -> Vec<(ErrorType, String)> {
        errs.iter()
            .map(|e| (e.error_type, e.field.clone()))
            .collect()
    }

    // validation_pluginargs_test.go TestValidateVolumeBindingArgs
    #[test]
    fn zero_and_positive_timeout_valid() {
        for t in [0, 10] {
            assert!(args(t, None).validate(&Path::default(), false).is_empty());
        }
    }

    #[test]
    fn negative_timeout_invalid() {
        let errs = args(-10, None).validate(&Path::default(), false);
        assert_eq!(
            summary(&errs),
            vec![(ErrorType::Invalid, "bindTimeoutSeconds".to_string())]
        );
        assert_eq!(
            errs[0].detail,
            "invalid BindTimeoutSeconds, should not be a negative value"
        );
    }

    #[test]
    fn feature_off_shape_nil_ok() {
        assert!(args(10, None).validate(&Path::default(), false).is_empty());
    }

    #[test]
    fn feature_off_shape_set_is_invalid() {
        let errs = args(10, Some(vec![pt(1, 1), pt(3, 3)])).validate(&Path::default(), false);
        assert_eq!(
            summary(&errs),
            vec![(ErrorType::Invalid, "shape".to_string())]
        );
        assert_eq!(
            errs[0].detail,
            "unexpected field `shape`, remove it or turn on the feature gate StorageCapacityScoring"
        );
    }

    #[test]
    fn feature_on_empty_shape_required() {
        let errs = args(10, Some(vec![])).validate(&Path::default(), true);
        assert_eq!(
            summary(&errs),
            vec![(ErrorType::Required, "shape".to_string())]
        );
        // A nil shape with the gate on is also "at least one point".
        let errs = args(10, None).validate(&Path::default(), true);
        assert_eq!(
            summary(&errs),
            vec![(ErrorType::Required, "shape".to_string())]
        );
    }

    #[test]
    fn feature_on_unsorted_points() {
        let errs = args(10, Some(vec![pt(3, 3), pt(1, 1)])).validate(&Path::default(), true);
        assert_eq!(
            summary(&errs),
            vec![(ErrorType::Invalid, "shape[1].utilization".to_string())]
        );
        assert_eq!(errs[0].detail, "values must be sorted in increasing order");
    }

    #[test]
    fn feature_on_out_of_range_points() {
        let errs = args(
            10,
            Some(vec![pt(-1, 1), pt(10, -1), pt(20, 11), pt(101, 1)]),
        )
        .validate(&Path::default(), true);
        assert_eq!(
            summary(&errs),
            vec![
                (ErrorType::Invalid, "shape[0].utilization".to_string()),
                (ErrorType::Invalid, "shape[1].score".to_string()),
                (ErrorType::Invalid, "shape[2].score".to_string()),
                (ErrorType::Invalid, "shape[3].utilization".to_string()),
            ]
        );
        assert_eq!(errs[0].detail, "not in valid range [0, 100]");
        assert_eq!(errs[1].detail, "not in valid range [0, 10]");
    }

    #[test]
    fn nested_path_prefix() {
        let p = Path::new("profiles").index(0).child("args");
        let errs = args(-1, None).validate(&p, false);
        assert_eq!(errs[0].field, "profiles[0].args.bindTimeoutSeconds");
    }

    #[test]
    fn config_doc_custom_shape_gate_on() {
        let doc = r#"
apiVersion: kubescheduler.config.k8s.io/v1
kind: KubeSchedulerConfiguration
profiles:
- schedulerName: default-scheduler
  pluginConfig:
  - name: VolumeBinding
    args:
      bindTimeoutSeconds: 30
      shape:
      - utilization: 0
        score: 0
      - utilization: 100
        score: 10
"#;
        let a = volume_binding_args_from_config(doc, true).unwrap();
        assert_eq!(a, args(30, Some(vec![pt(0, 0), pt(100, 10)])));
    }

    #[test]
    fn config_doc_shape_with_gate_off_rejected() {
        let doc = "profiles:\n- pluginConfig:\n  - name: VolumeBinding\n    args:\n      shape:\n      - {utilization: 0, score: 1}\n";
        let err = volume_binding_args_from_config(doc, false).unwrap_err();
        assert!(
            err.contains("profiles[0].pluginConfig[0].args.shape"),
            "{err}"
        );
        assert!(err.contains("StorageCapacityScoring"), "{err}");
    }

    #[test]
    fn config_doc_without_volume_binding_gets_defaults() {
        let a = volume_binding_args_from_config("profiles: []\n", true).unwrap();
        assert_eq!(a, args(600, Some(vec![pt(0, 10), pt(100, 0)])));
    }

    // v1/defaults_test.go TestPluginArgsDefaults, VolumeBindingArgs cases.
    #[test]
    fn defaults_gate_off() {
        let mut a = VolumeBindingArgs::default();
        a.set_defaults(false);
        assert_eq!(a, args(600, None));
    }

    #[test]
    fn defaults_gate_on() {
        let mut a = VolumeBindingArgs::default();
        a.set_defaults(true);
        assert_eq!(a, args(600, Some(vec![pt(0, 10), pt(100, 0)])));
    }

    #[test]
    fn defaults_keep_explicit_values() {
        let mut a = args(0, Some(vec![pt(50, 5)]));
        a.set_defaults(true);
        assert_eq!(a, args(0, Some(vec![pt(50, 5)])));
    }

    #[test]
    fn scorer_shape_scales_to_max_node_score() {
        let a = args(10, Some(vec![pt(0, 1), pt(100, 10)]));
        let got: Vec<_> = a
            .scorer_shape()
            .iter()
            .map(|p| (p.utilization, p.score))
            .collect();
        assert_eq!(got, vec![(0, 10), (100, 100)]);
    }

    #[test]
    fn default_args_scorer_shape_equals_default_shape() {
        let mut a = VolumeBindingArgs::default();
        a.set_defaults(true);
        let got: Vec<_> = a
            .scorer_shape()
            .iter()
            .map(|p| (p.utilization, p.score))
            .collect();
        let want: Vec<_> = rusternetes_scheduler::volume_binding::default_shape()
            .iter()
            .map(|p| (p.utilization, p.score))
            .collect();
        assert_eq!(got, want);
    }

    // #3346: every profile becomes a runtime profile (profile.NewMap), each
    // with its own VolumeBinding args; an empty config yields the default one.
    #[test]
    fn profiles_from_config_builds_one_runtime_profile_each() {
        let m = profiles_from_config("", false).unwrap();
        assert_eq!(m.names(), vec!["default-scheduler"]);
        let doc = "profiles:\n\
            - schedulerName: a\n  pluginConfig:\n  - name: VolumeBinding\n    args:\n      bindTimeoutSeconds: 7\n\
            - schedulerName: b\n";
        let m = profiles_from_config(doc, false).unwrap();
        assert_eq!(m.names(), vec!["a", "b"]);
        assert_eq!(
            m.get("a").unwrap().volume_bind_timeout,
            std::time::Duration::from_secs(7)
        );
        assert_eq!(
            m.get("b").unwrap().volume_bind_timeout,
            std::time::Duration::from_secs(600)
        );
        assert_eq!(m.get("a").unwrap().queue_sort, "PrioritySort");
        // Per-profile VolumeBinding validation reports the profile's own path.
        let bad = "profiles:\n\
            - schedulerName: a\n\
            - schedulerName: b\n  pluginConfig:\n  - name: VolumeBinding\n    args:\n      bindTimeoutSeconds: -1\n";
        let err = profiles_from_config(bad, false).unwrap_err();
        assert!(
            err.contains("profiles[1].pluginConfig[0].args.bindTimeoutSeconds"),
            "{err}"
        );
    }

    // #3202: file loader shared by the scheduler binary and the all-in-one
    // binary (--scheduler-config).
    #[test]
    fn load_from_file_reads_defaults_and_validates() {
        let dir = std::env::temp_dir().join(format!("sched-cfg-{}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        let ok = dir.join("ok.yaml");
        std::fs::write(
            &ok,
            "profiles:\n- pluginConfig:\n  - name: VolumeBinding\n    args:\n      bindTimeoutSeconds: 42\n",
        )
        .unwrap();
        let a = load_volume_binding_args(ok.to_str().unwrap()).unwrap();
        assert_eq!(a.bind_timeout_seconds, Some(42));
        let bad = dir.join("bad.yaml");
        std::fs::write(
            &bad,
            "profiles:\n- pluginConfig:\n  - name: VolumeBinding\n    args:\n      bindTimeoutSeconds: -1\n",
        )
        .unwrap();
        let err = load_volume_binding_args(bad.to_str().unwrap()).unwrap_err();
        assert!(err.contains("bindTimeoutSeconds"), "{err}");
        let err = load_volume_binding_args(dir.join("missing.yaml").to_str().unwrap()).unwrap_err();
        assert!(err.contains("missing.yaml"), "{err}");
        let _ = std::fs::remove_dir_all(&dir);
    }

    // #3207: validatePluginConfig seenPluginConfig (validation.go:189-197);
    // validation_test.go "duplicated-plugin-config" -> profiles[0].pluginConfig[1].
    #[test]
    fn duplicate_plugin_config_rejected() {
        let doc = "profiles:\n- pluginConfig:\n  - name: VolumeBinding\n  - name: VolumeBinding\n";
        let err = volume_binding_args_from_config(doc, false).unwrap_err();
        assert!(
            err.contains("profiles[0].pluginConfig[1]: Duplicate value"),
            "{err}"
        );
        // Same name in different profiles is fine.
        let ok = "profiles:\n- schedulerName: a\n  pluginConfig:\n  - name: X\n- schedulerName: b\n  pluginConfig:\n  - name: X\n";
        assert!(volume_binding_args_from_config(ok, false).is_ok());
    }

    // #3345: validateExtenders (validation.go:256-286). validation_test.go
    // "extender-negative-weight" -> extenders[0].weight.
    #[test]
    fn extender_negative_weight_rejected() {
        let doc = "extenders:\n- urlPrefix: http://x\n  prioritizeVerb: prioritize\n  weight: -1\n";
        let err = volume_binding_args_from_config(doc, false).unwrap_err();
        assert_eq!(
            err,
            "extenders[0].weight: Invalid value: -1: must have a positive weight applied to it"
        );
        // Weight is only checked when a prioritizeVerb is set (validation.go:263).
        let ok = "extenders:\n- urlPrefix: http://x\n  weight: -1\n";
        assert!(volume_binding_args_from_config(ok, false).is_ok());
        let ok = "extenders:\n- urlPrefix: http://x\n  prioritizeVerb: p\n  weight: 1\n";
        assert!(volume_binding_args_from_config(ok, false).is_ok());
    }

    // "extender-duplicate-managed-resources" -> extenders[0].managedResources[1].name.
    #[test]
    fn extender_duplicate_managed_resource_rejected() {
        let doc = "extenders:\n- urlPrefix: http://x\n  managedResources:\n  - name: example.com/foo\n  - name: example.com/foo\n";
        let err = volume_binding_args_from_config(doc, false).unwrap_err();
        assert_eq!(
            err,
            "extenders[0].managedResources[1].name: Invalid value: \"example.com/foo\": duplicate extender managed resource name"
        );
    }

    // "extender-duplicate-bind" -> extenders (validation.go:281-283).
    #[test]
    fn extender_duplicate_bind_rejected() {
        let doc = "extenders:\n- urlPrefix: http://x\n  bindVerb: foo\n- urlPrefix: http://y\n  bindVerb: bar\n";
        let err = volume_binding_args_from_config(doc, false).unwrap_err();
        assert_eq!(
            err,
            "extenders: Invalid value: \"found 2 extenders implementing bind\": only one extender can implement bind"
        );
    }

    // validateExtendedResourceName (validation.go:290-301): qualified-name
    // errors short-circuit; native names are not extended resources.
    #[test]
    fn extender_managed_resource_name_must_be_extended() {
        let doc = "extenders:\n- urlPrefix: http://x\n  managedResources:\n  - name: cpu\n  - name: kubernetes.io/foo\n  - name: requests.example.com/foo\n  - name: \"bad name/x\"\n";
        let err = volume_binding_args_from_config(doc, false).unwrap_err();
        assert!(
            err.contains(
                "extenders[0].managedResources[0].name: Invalid value: \"cpu\": is an invalid extended resource name"
            ),
            "{err}"
        );
        assert!(
            err.contains(
                "extenders[0].managedResources[1].name: Invalid value: \"kubernetes.io/foo\": is an invalid extended resource name"
            ),
            "{err}"
        );
        assert!(
            err.contains(
                "extenders[0].managedResources[2].name: Invalid value: \"requests.example.com/foo\": is an invalid extended resource name"
            ),
            "{err}"
        );
        assert!(
            err.contains(
                "extenders[0].managedResources[3].name: Invalid value: \"bad name/x\": prefix part"
            ),
            "{err}"
        );
        assert!(
            !err.contains("managedResources[3].name: Invalid value: \"bad name/x\": is an invalid"),
            "{err}"
        );
    }

    // validation.go:58-67: duplicate schedulerName across profiles.
    #[test]
    fn duplicate_scheduler_name_rejected() {
        let doc = "profiles:\n- schedulerName: a\n- schedulerName: a\n";
        let err = volume_binding_args_from_config(doc, false).unwrap_err();
        assert!(
            err.contains("profiles[1].schedulerName: Duplicate value"),
            "{err}"
        );
    }

    // validation.go:58-67 -> field.Duplicate(path.Child("schedulerName"),
    // profilesPath.Index(idx).Child("schedulerName")): the bad value is a
    // *field.Path, which json.Marshal renders as `{}` (errors.go:92-97).
    #[test]
    fn duplicate_scheduler_name_value_renders_like_go() {
        let doc = "profiles:\n- schedulerName: a\n- schedulerName: a\n";
        let err = volume_binding_args_from_config(doc, false).unwrap_err();
        assert_eq!(err, "profiles[1].schedulerName: Duplicate value: {}");
    }

    // v1/defaults.go:116-119: the default name is only applied to a single
    // profile; validation.go:141-144 then requires a name on the rest
    // (validation_test.go "scheduler-name-not-set" -> profiles[1].schedulerName).
    #[test]
    fn unnamed_profile_required_when_multiple() {
        let doc = "profiles:\n- schedulerName: a\n- pluginConfig: []\n";
        let err = volume_binding_args_from_config(doc, false).unwrap_err();
        assert_eq!(err, "profiles[1].schedulerName: Required value");
        let one = "profiles:\n- pluginConfig: []\n";
        assert!(volume_binding_args_from_config(one, false).is_ok());
    }

    // validation.go:86-92 validatePercentageOfNodesToScore, top level and
    // per profile (validation_test.go "bad-percentage-of-nodes-to-score",
    // "greater-than-100-profile-percentage-of-nodes-to-score").
    #[test]
    fn percentage_of_nodes_to_score_range() {
        let err =
            volume_binding_args_from_config("percentageOfNodesToScore: 101\n", false).unwrap_err();
        assert_eq!(
            err,
            "percentageOfNodesToScore: Invalid value: 101: not in valid range [0-100]"
        );
        let err = volume_binding_args_from_config(
            "profiles:\n- schedulerName: a\n- schedulerName: b\n  percentageOfNodesToScore: -1\n",
            false,
        )
        .unwrap_err();
        assert_eq!(
            err,
            "profiles[1].percentageOfNodesToScore: Invalid value: -1: not in valid range [0-100]"
        );
        assert!(volume_binding_args_from_config("percentageOfNodesToScore: 100\n", false).is_ok());
    }

    fn dp(pct: i32, abs: i32) -> DefaultPreemptionArgs {
        DefaultPreemptionArgs {
            min_candidate_nodes_percentage: Some(pct),
            min_candidate_nodes_absolute: Some(abs),
        }
    }

    // validation_pluginargs_test.go TestValidateDefaultPreemptionArgs
    #[test]
    fn default_preemption_valid_default() {
        assert!(dp(10, 100).validate(&Path::default()).is_empty());
    }

    #[test]
    fn default_preemption_percentage_out_of_range() {
        for pct in [-1, 900] {
            let errs = dp(pct, 100).validate(&Path::default());
            assert_eq!(
                summary(&errs),
                vec![(
                    ErrorType::Invalid,
                    "minCandidateNodesPercentage".to_string()
                )]
            );
            assert_eq!(errs[0].detail, "not in valid range [0, 100]");
        }
    }

    #[test]
    fn default_preemption_negative_absolute() {
        let errs = dp(20, -1).validate(&Path::default());
        assert_eq!(
            summary(&errs),
            vec![(ErrorType::Invalid, "minCandidateNodesAbsolute".to_string())]
        );
        assert_eq!(errs[0].detail, "not in valid range [0, inf)");
    }

    #[test]
    fn default_preemption_all_zero() {
        let errs = dp(0, 0).validate(&Path::default());
        assert_eq!(
            summary(&errs),
            vec![
                (
                    ErrorType::Invalid,
                    "minCandidateNodesPercentage".to_string()
                ),
                (ErrorType::Invalid, "minCandidateNodesAbsolute".to_string()),
            ]
        );
        assert_eq!(
            errs[0].detail,
            "cannot be zero at the same time as minCandidateNodesAbsolute"
        );
        assert_eq!(
            errs[1].detail,
            "cannot be zero at the same time as minCandidateNodesPercentage"
        );
    }

    #[test]
    fn default_preemption_one_zero_ok() {
        assert!(dp(0, 100).validate(&Path::default()).is_empty());
        assert!(dp(10, 0).validate(&Path::default()).is_empty());
    }

    // v1/defaults.go SetDefaults_DefaultPreemptionArgs
    #[test]
    fn default_preemption_defaults() {
        let mut a = DefaultPreemptionArgs::default();
        a.set_defaults();
        assert_eq!(a, dp(10, 100));
        let mut b = dp(0, 5);
        b.set_defaults();
        assert_eq!(b, dp(0, 5));
    }

    // validation_pluginargs_test.go TestValidateInterPodAffinityArgs
    #[test]
    fn inter_pod_affinity_weight_range() {
        let ok = |w| {
            InterPodAffinityArgs {
                hard_pod_affinity_weight: Some(w),
                ..Default::default()
            }
            .validate(&Path::default())
        };
        assert!(ok(10).is_empty());
        assert!(ok(0).is_empty());
        assert!(ok(100).is_empty());
        for w in [-1, 101] {
            let errs = ok(w);
            assert_eq!(
                summary(&errs),
                vec![(ErrorType::Invalid, "hardPodAffinityWeight".to_string())]
            );
            assert_eq!(errs[0].detail, "not in valid range [0, 100]");
        }
    }

    // v1/defaults.go SetDefaults_InterPodAffinityArgs
    #[test]
    fn inter_pod_affinity_defaults() {
        let mut a = InterPodAffinityArgs::default();
        a.set_defaults();
        assert_eq!(a.hard_pod_affinity_weight, Some(1));
        let mut b = InterPodAffinityArgs {
            hard_pod_affinity_weight: Some(0),
            ..Default::default()
        };
        b.set_defaults();
        assert_eq!(b.hard_pod_affinity_weight, Some(0));
    }

    // validation.go:107-135 isPluginInvalid + validatePluginConfig
    #[test]
    fn removed_plugin_config_rejected() {
        let doc = "profiles:\n- pluginConfig:\n  - name: AzureDiskLimits\n";
        let err = volume_binding_args_from_config(doc, false).unwrap_err();
        assert_eq!(
            err,
            "profiles[0].pluginConfig[0]: Invalid value: \"AzureDiskLimits\": was invalid in version \"kubescheduler.config.k8s.io/v1\" (KubeSchedulerConfiguration is version \"kubescheduler.config.k8s.io/v1\")"
        );
    }

    #[test]
    fn config_doc_default_preemption_and_inter_pod_affinity_validated() {
        let doc = "profiles:\n- pluginConfig:\n  - name: DefaultPreemption\n    args:\n      minCandidateNodesPercentage: 0\n      minCandidateNodesAbsolute: 0\n  - name: InterPodAffinity\n    args:\n      hardPodAffinityWeight: 101\n";
        let err = volume_binding_args_from_config(doc, false).unwrap_err();
        assert!(err.contains(
            "profiles[0].pluginConfig[0].args.minCandidateNodesPercentage: Invalid value: 0: cannot be zero at the same time as minCandidateNodesAbsolute"
        ), "{err}");
        assert!(err.contains(
            "profiles[0].pluginConfig[1].args.hardPodAffinityWeight: Invalid value: 101: not in valid range [0, 100]"
        ), "{err}");
        let ok = "profiles:\n- pluginConfig:\n  - name: DefaultPreemption\n    args:\n      minCandidateNodesAbsolute: 0\n  - name: InterPodAffinity\n";
        assert!(volume_binding_args_from_config(ok, false).is_ok());
    }

    // validation.go:155-160 validatePluginConfig -> ValidateNodeAffinityArgs,
    // ValidateNodeResourcesBalancedAllocationArgs, ValidatePodTopologySpreadArgs.
    #[test]
    fn config_doc_node_affinity_balanced_allocation_pts_validated() {
        let doc = "profiles:
- pluginConfig:
  - name: NodeAffinity
    args:
      addedAffinity:
        requiredDuringSchedulingIgnoredDuringExecution:
          nodeSelectorTerms:
          - matchExpressions:
            - {key: invalid/label/key, operator: In, values: [v]}
  - name: NodeResourcesBalancedAllocation
    args:
      resources:
      - {name: cpu, weight: 2}
  - name: PodTopologySpread
    args:
      defaultingType: System
      defaultConstraints:
      - {maxSkew: 1, topologyKey: key, whenUnsatisfiable: DoNotSchedule}
";
        let err = volume_binding_args_from_config(doc, false).unwrap_err();
        assert!(err.contains("profiles[0].pluginConfig[0].args.addedAffinity.requiredDuringSchedulingIgnoredDuringExecution.nodeSelectorTerms[0].matchExpressions[0].key: Invalid value: \"invalid/label/key\""), "{err}");
        assert!(
            err.contains(
                "profiles[0].pluginConfig[1].args.resources[0].weight: Invalid value: 2: must be 1"
            ),
            "{err}"
        );
        assert!(
            err.contains("profiles[0].pluginConfig[2].args.defaultingType: Invalid value: \"System\": when .defaultConstraints are not empty"),
            "{err}"
        );
    }

    // NewFit (noderesources/fit.go:193) validates with a nil path; the
    // validation.go:158 map key is "NodeResourcesFitArgs", so the profile
    // pass never reaches it.
    #[test]
    fn config_doc_node_resources_fit_validated_at_plugin_init() {
        let doc = "profiles:\n- pluginConfig:\n  - name: NodeResourcesFit\n    args:\n      scoringStrategy:\n        type: Invalid\n";
        let err = volume_binding_args_from_config(doc, false).unwrap_err();
        assert!(
            err.contains("scoringStrategy.type: Unsupported value: \"Invalid\": supported values: \"LeastAllocated\", \"MostAllocated\", \"RequestedToCapacityRatio\""),
            "{err}"
        );
    }

    fn yaml<T: serde::de::DeserializeOwned>(s: &str) -> T {
        serde_yaml::from_str(s).unwrap()
    }

    fn fs(errs: &ErrorList) -> Vec<(ErrorType, String)> {
        summary(errs)
    }

    fn et(t: ErrorType, f: &str) -> (ErrorType, String) {
        (t, f.to_string())
    }

    // validation_pluginargs_test.go TestValidatePodTopologySpreadArgs
    #[test]
    fn pts_args_cases() {
        let c = |extra: &str| {
            format!("defaultingType: List\ndefaultConstraints:\n- {{maxSkew: 1, topologyKey: node, whenUnsatisfiable: DoNotSchedule{extra}}}\n")
        };
        let run = |doc: &str| yaml::<PodTopologySpreadArgs>(doc).validate(&Path::default());
        assert!(run("defaultingType: List\ndefaultConstraints:\n- {maxSkew: 1, topologyKey: node, whenUnsatisfiable: DoNotSchedule}\n- {maxSkew: 2, topologyKey: zone, whenUnsatisfiable: ScheduleAnyway}\n").is_empty());
        let e = run("defaultingType: List\ndefaultConstraints:\n- {maxSkew: -1, topologyKey: node, whenUnsatisfiable: DoNotSchedule}\n");
        assert_eq!(
            fs(&e),
            vec![et(ErrorType::Invalid, "defaultConstraints[0].maxSkew")]
        );
        let e = run("defaultingType: List\ndefaultConstraints:\n- {maxSkew: 1, topologyKey: '', whenUnsatisfiable: DoNotSchedule}\n");
        assert_eq!(
            fs(&e),
            vec![et(ErrorType::Required, "defaultConstraints[0].topologyKey")]
        );
        let e = run("defaultingType: List\ndefaultConstraints:\n- {maxSkew: 1, topologyKey: node, whenUnsatisfiable: ''}\n");
        assert_eq!(
            fs(&e),
            vec![et(
                ErrorType::Required,
                "defaultConstraints[0].whenUnsatisfiable"
            )]
        );
        let e = run("defaultingType: List\ndefaultConstraints:\n- {maxSkew: 1, topologyKey: node, whenUnsatisfiable: unknown action}\n");
        assert_eq!(
            fs(&e),
            vec![et(
                ErrorType::NotSupported,
                "defaultConstraints[0].whenUnsatisfiable"
            )]
        );
        let e = run("defaultingType: List\ndefaultConstraints:\n- {maxSkew: 1, topologyKey: node, whenUnsatisfiable: DoNotSchedule}\n- {maxSkew: 2, topologyKey: node, whenUnsatisfiable: DoNotSchedule}\n");
        assert_eq!(
            fs(&e),
            vec![et(ErrorType::Duplicate, "defaultConstraints[1]")]
        );
        assert_eq!(
            e[0].error_body(),
            "Duplicate value: \"{node, DoNotSchedule}\""
        );
        let e = run(&c(", labelSelector: {matchLabels: {a: b}}"));
        assert_eq!(
            fs(&e),
            vec![et(
                ErrorType::Forbidden,
                "defaultConstraints[0].labelSelector"
            )]
        );
        assert!(run("defaultingType: List\n").is_empty());
        assert!(run("defaultingType: System\n").is_empty());
        let e = run("defaultingType: unknown\n");
        assert_eq!(fs(&e), vec![et(ErrorType::NotSupported, "defaultingType")]);
        let e = run("defaultingType: System\ndefaultConstraints:\n- {maxSkew: 1, topologyKey: key, whenUnsatisfiable: DoNotSchedule}\n");
        assert_eq!(fs(&e), vec![et(ErrorType::Invalid, "defaultingType")]);
    }

    // defaults_test.go TestPluginArgsDefaults "PodTopologySpreadArgs ..."
    #[test]
    fn pts_args_defaults() {
        let mut a: PodTopologySpreadArgs = yaml("{}");
        a.set_defaults();
        assert_eq!(a.defaulting_type, "System");
        assert!(a.default_constraints.is_empty());
        let mut a: PodTopologySpreadArgs = yaml(
            "defaultConstraints:\n- {topologyKey: planet, whenUnsatisfiable: DoNotSchedule, maxSkew: 2}\n",
        );
        a.set_defaults();
        assert_eq!(a.defaulting_type, "System");
        assert_eq!(a.default_constraints.len(), 1);
        assert_eq!(a.default_constraints[0].max_skew, 2);
    }

    // validation_pluginargs_test.go TestValidateNodeResourcesBalancedAllocationArgs
    #[test]
    fn balanced_allocation_args_cases() {
        let run =
            |doc: &str| yaml::<NodeResourcesBalancedAllocationArgs>(doc).validate(&Path::default());
        assert!(run("resources: [{name: cpu, weight: 1}, {name: memory, weight: 1}]").is_empty());
        let e = run("resources: [{name: cpu, weight: 2}, {name: memory, weight: 1}]");
        assert_eq!(fs(&e), vec![et(ErrorType::Invalid, "resources[0].weight")]);
        let e = run("resources: [{name: cpu, weight: 1}, {name: cpu, weight: 1}]");
        assert_eq!(fs(&e), vec![et(ErrorType::Duplicate, "resources[1].name")]);
    }

    // defaults_test.go TestPluginArgsDefaults "NodeResourcesBalancedAllocationArgs ..."
    #[test]
    fn balanced_allocation_args_defaults() {
        let mut a: NodeResourcesBalancedAllocationArgs = yaml("{}");
        a.set_defaults();
        assert_eq!(a.resources, default_resource_spec());
        let mut a: NodeResourcesBalancedAllocationArgs = yaml(
            "resources: [{name: cpu}, {name: scalar.io/scalar0}, {name: scalar.io/scalar1, weight: 1}]",
        );
        a.set_defaults();
        assert!(a.resources.iter().all(|r| r.weight == 1));
        assert_eq!(a.resources.len(), 3);
    }

    // validation_pluginargs_test.go TestValidateNodeAffinityArgs
    #[test]
    fn node_affinity_args_cases() {
        let run = |doc: &str| yaml::<NodeAffinityArgs>(doc).validate(&Path::default());
        assert!(run("{}").is_empty());
        let valid = "addedAffinity:
  requiredDuringSchedulingIgnoredDuringExecution:
    nodeSelectorTerms:
    - matchExpressions: [{key: label-1, operator: In, values: [label-1-val]}]
  preferredDuringSchedulingIgnoredDuringExecution:
  - weight: 1
    preference:
      matchFields: [{key: metadata.name, operator: In, values: [node-1]}]
";
        assert!(run(valid).is_empty());
        let invalid = "addedAffinity:
  requiredDuringSchedulingIgnoredDuringExecution:
    nodeSelectorTerms:
    - matchExpressions: [{key: invalid/label/key, operator: In, values: [label-1-val]}]
  preferredDuringSchedulingIgnoredDuringExecution:
  - weight: 1
    preference:
      matchFields: [{key: metadata.name, operator: In, values: [node-1, node-2]}]
";
        let e = run(invalid);
        assert_eq!(
            fs(&e),
            vec![
                et(ErrorType::Invalid, "addedAffinity.requiredDuringSchedulingIgnoredDuringExecution.nodeSelectorTerms[0].matchExpressions[0].key"),
                et(ErrorType::Invalid, "addedAffinity.preferredDuringSchedulingIgnoredDuringExecution[0].matchFields[0].values"),
            ]
        );
        assert_eq!(
            e[1].error_body(),
            "Invalid value: [\"node-1\",\"node-2\"]: must have one element"
        );
        // nodeSelectorRequirementsAsSelector: unknown operator, Gt value, fields operator.
        let e = run("addedAffinity:\n  requiredDuringSchedulingIgnoredDuringExecution:\n    nodeSelectorTerms:\n    - matchExpressions: [{key: a, operator: Bogus}, {key: b, operator: Gt, values: [x]}]\n      matchFields: [{key: metadata.name, operator: Exists}]\n");
        assert_eq!(
            fs(&e),
            vec![
                et(ErrorType::NotSupported, "addedAffinity.requiredDuringSchedulingIgnoredDuringExecution.nodeSelectorTerms[0].matchExpressions[0].operator"),
                et(ErrorType::Invalid, "addedAffinity.requiredDuringSchedulingIgnoredDuringExecution.nodeSelectorTerms[0].matchExpressions[1].values[0]"),
                et(ErrorType::NotSupported, "addedAffinity.requiredDuringSchedulingIgnoredDuringExecution.nodeSelectorTerms[0].matchFields[0].operator"),
            ]
        );
        // nodeaffinity.go:119: zero weight / empty terms are not parsed.
        assert!(run("addedAffinity:\n  preferredDuringSchedulingIgnoredDuringExecution:\n  - weight: 0\n    preference:\n      matchExpressions: [{key: bad/key/x, operator: In, values: [v]}]\n  - weight: 1\n    preference: {}\n").is_empty());
    }

    // validation_pluginargs_test.go TestValidateFitArgs
    #[test]
    fn fit_args_cases() {
        let default = "scoringStrategy: {type: LeastAllocated, resources: [{name: cpu, weight: 1}, {name: memory, weight: 1}]}\n";
        let run = |extra: &str| {
            yaml::<NodeResourcesFitArgs>(&format!("{extra}{default}")).validate(&Path::default())
        };
        let detail = |e: &ErrorList| {
            e.iter()
                .map(|x| x.to_string())
                .collect::<Vec<_>>()
                .join("|")
        };
        let long = "a".repeat(64);
        assert!(
            detail(&run(&format!("ignoredResources: [longvalue{long}]\n")))
                .contains("name part must be no more than 63 bytes")
        );
        assert!(detail(&run("ignoredResources: ['example.com/']\n"))
            .contains("name part must be non-empty"));
        assert!(detail(&run("ignoredResources: [example.com/aaa/bbb]\n"))
            .contains("a valid label key must consist of"));
        assert!(run("ignoredResources: [example.com]\n").is_empty());
        assert!(run("ignoredResourceGroups: [example.com]\n").is_empty());
        assert!(detail(&run("ignoredResourceGroups: ['example.com/']\n"))
            .contains("name part must be non-empty"));
        assert!(detail(&run(&format!("ignoredResourceGroups: [{long}]\n")))
            .contains("name part must be no more than 63 bytes"));
        assert!(detail(&run("ignoredResourceGroups: [example.com/aa]\n"))
            .contains("resource group name can't contain '/'"));
        let e = yaml::<NodeResourcesFitArgs>("scoringStrategy: {type: Invalid}\n")
            .validate(&Path::default());
        assert!(
            detail(&e).contains("Unsupported value: \"Invalid\""),
            "{}",
            detail(&e)
        );
    }

    // validation_pluginargs_test.go TestValidate{Least,Most}AllocatedScoringStrategy
    // and TestValidateRequestedToCapacityRatioScoringStrategy
    #[test]
    fn fit_scoring_strategy_cases() {
        let run = |doc: &str| yaml::<NodeResourcesFitArgs>(doc).validate(&Path::default());
        for ty in ["LeastAllocated", "MostAllocated"] {
            assert!(run(&format!("scoringStrategy: {{type: {ty}}}")).is_empty());
            assert!(run(&format!("scoringStrategy: {{type: {ty}, resources: [{{name: cpu, weight: 1}}, {{name: memory, weight: 10}}]}}")).is_empty());
            for w in [0, 101, -1] {
                let e = run(&format!(
                    "scoringStrategy: {{type: {ty}, resources: [{{name: cpu, weight: {w}}}]}}"
                ));
                assert_eq!(
                    fs(&e),
                    vec![et(
                        ErrorType::Invalid,
                        "scoringStrategy.resources[0].weight"
                    )]
                );
                assert_eq!(
                    e[0].detail,
                    "resource weight of cpu not in valid range (0, 100]"
                );
            }
        }
        let rt = "RequestedToCapacityRatio";
        let e = run(&format!("scoringStrategy: {{type: {rt}}}\n"));
        // validateFunctionShape is only run when requestedToCapacityRatio is set.
        assert!(e.is_empty());
        let e = run(&format!(
            "scoringStrategy: {{type: {rt}, requestedToCapacityRatio: {{}}}}\n"
        ));
        assert_eq!(
            fs(&e),
            vec![et(ErrorType::Required, "scoringStrategy.shape")]
        );
        let shaped = |shape: &str, res: &str| {
            run(&format!("scoringStrategy: {{type: {rt}, resources: [{res}], requestedToCapacityRatio: {{shape: [{shape}]}}}}"))
        };
        let ok = "{utilization: 30, score: 3}";
        let e = shaped(ok, "{name: cpu, weight: 101}");
        assert_eq!(
            fs(&e),
            vec![et(
                ErrorType::Invalid,
                "scoringStrategy.resources[0].weight"
            )]
        );
        let e = shaped("{utilization: -1, score: 3}", "");
        assert_eq!(
            fs(&e),
            vec![et(
                ErrorType::Invalid,
                "scoringStrategy.shape[0].utilization"
            )]
        );
        let e = shaped("{utilization: 101, score: 3}", "");
        assert_eq!(
            fs(&e),
            vec![et(
                ErrorType::Invalid,
                "scoringStrategy.shape[0].utilization"
            )]
        );
        let e = shaped(
            "{utilization: 10, score: 1}, {utilization: 10, score: 2}",
            "",
        );
        assert_eq!(
            fs(&e),
            vec![et(
                ErrorType::Invalid,
                "scoringStrategy.shape[1].utilization"
            )]
        );
        let e = shaped("{utilization: 30, score: -1}", "");
        assert_eq!(
            fs(&e),
            vec![et(ErrorType::Invalid, "scoringStrategy.shape[0].score")]
        );
        let e = shaped("{utilization: 30, score: 11}", "");
        assert_eq!(
            fs(&e),
            vec![et(ErrorType::Invalid, "scoringStrategy.shape[0].score")]
        );
    }

    // defaults_test.go TestPluginArgsDefaults "NodeResourcesFitArgs ..."
    #[test]
    fn fit_args_defaults() {
        let mut a: NodeResourcesFitArgs = yaml("{}");
        a.set_defaults();
        assert_eq!(
            a.scoring_strategy,
            Some(ScoringStrategy {
                strategy_type: "LeastAllocated".into(),
                resources: default_resource_spec(),
                requested_to_capacity_ratio: None,
            })
        );
        let mut a: NodeResourcesFitArgs = yaml("scoringStrategy: {type: MostAllocated}");
        a.set_defaults();
        let s = a.scoring_strategy.unwrap();
        assert_eq!(s.strategy_type, "MostAllocated");
        assert_eq!(s.resources, default_resource_spec());
        // The type is NOT defaulted when a strategy is given without one.
        let mut a: NodeResourcesFitArgs = yaml("scoringStrategy: {resources: [{name: cpu}]}");
        a.set_defaults();
        let s = a.scoring_strategy.unwrap();
        assert_eq!(s.strategy_type, "");
        assert_eq!(s.resources[0].weight, 1);
    }

    // validation.go:158 keys the Fit validator "NodeResourcesFitArgs": a
    // pluginConfig of that name always fails the type check (validation.go:199-203).
    #[test]
    fn plugin_config_named_node_resources_fit_args_has_to_match() {
        let doc = "profiles:\n- pluginConfig:\n  - name: NodeResourcesFitArgs\n";
        let err = volume_binding_args_from_config(doc, false).unwrap_err();
        assert_eq!(
            err,
            "profiles[0].pluginConfig[0].args: Invalid value: null: has to match plugin args"
        );
    }

    // validation_test.go TestValidateKubeSchedulerConfiguration
    // "bad-parallelism-invalid-value": parallelism 0 -> Invalid.
    #[test]
    fn parallelism_must_be_positive() {
        let err = volume_binding_args_from_config("parallelism: 0\n", false).unwrap_err();
        assert_eq!(
            err,
            "parallelism: Invalid value: 0: should be an integer value greater than zero"
        );
        assert!(volume_binding_args_from_config("parallelism: 8\n", false).is_ok());
        // Unset defaults to 16 (v1/defaults.go:108-110).
        assert!(volume_binding_args_from_config("{}\n", false).is_ok());
    }

    // validation.go:70-77
    #[test]
    fn backoff_seconds_validated() {
        let err =
            volume_binding_args_from_config("podInitialBackoffSeconds: 0\n", false).unwrap_err();
        assert!(
            err.starts_with("podInitialBackoffSeconds: Invalid value: 0: must be greater than 0")
        );
        let err = volume_binding_args_from_config(
            "podInitialBackoffSeconds: 5\npodMaxBackoffSeconds: 2\n",
            false,
        )
        .unwrap_err();
        assert_eq!(
            err,
            "podMaxBackoffSeconds: Invalid value: 2: must be greater than or equal to PodInitialBackoffSeconds"
        );
        // Defaults 1 / 10; a lone max below the default initial is rejected.
        let err = volume_binding_args_from_config("podMaxBackoffSeconds: 0\n", false).unwrap_err();
        assert!(err.starts_with("podMaxBackoffSeconds: Invalid value: 0:"));
    }

    // "invalid-plugins": removed plugins in plugins.<stage>.enabled.
    #[test]
    fn removed_plugins_in_plugin_sets_rejected() {
        let doc = "profiles:\n- plugins:\n    score:\n      enabled:\n      - name: AzureDiskLimits\n      - name: PodTopologySpread\n      - name: GCEPDLimits\n";
        let err = volume_binding_args_from_config(doc, false).unwrap_err();
        let v = "was invalid in version \"kubescheduler.config.k8s.io/v1\" (KubeSchedulerConfiguration is version \"kubescheduler.config.k8s.io/v1\")";
        assert_eq!(
            err,
            format!(
                "profiles[0].plugins.score.enabled[0]: Invalid value: \"AzureDiskLimits\": {v}, profiles[0].plugins.score.enabled[2]: Invalid value: \"GCEPDLimits\": {v}"
            )
        );
        // "valid-plugins"
        let ok = "profiles:\n- plugins:\n    score:\n      enabled:\n      - {name: PodTopologySpread, weight: 2}\n";
        assert!(volume_binding_args_from_config(ok, false).is_ok());
        // Disabled entries and multiPoint are not checked (validation.go:166-182).
        let ok = "profiles:\n- plugins:\n    score:\n      disabled:\n      - name: AzureDiskLimits\n    multiPoint:\n      enabled:\n      - name: EBSLimits\n";
        assert!(volume_binding_args_from_config(ok, false).is_ok());
    }

    // "different-queue-sort"
    #[test]
    fn queue_sort_must_match_across_profiles() {
        let doc = "profiles:\n- schedulerName: a\n  plugins: {queueSort: {enabled: [{name: PrioritySort}]}}\n- schedulerName: b\n  plugins: {queueSort: {enabled: [{name: AnotherSort}]}}\n";
        let err = volume_binding_args_from_config(doc, false).unwrap_err();
        assert_eq!(
            err,
            "profiles[1].plugins.queueSort: Invalid value: {\"Enabled\":[{\"Name\":\"AnotherSort\",\"Weight\":0}],\"Disabled\":null}: must be the same for all profiles"
        );
        // "one-empty-queue-sort": second profile has no plugins at all.
        let doc = "profiles:\n- schedulerName: a\n  plugins: {queueSort: {enabled: [{name: PrioritySort}]}}\n- schedulerName: b\n";
        let err = volume_binding_args_from_config(doc, false).unwrap_err();
        assert!(err.starts_with("profiles[1].plugins.queueSort: Invalid value:"));
        // Same queue sort everywhere is fine.
        let doc = "profiles:\n- schedulerName: a\n  plugins: {queueSort: {enabled: [{name: PrioritySort}]}}\n- schedulerName: b\n  plugins: {queueSort: {enabled: [{name: PrioritySort}]}}\n";
        assert!(volume_binding_args_from_config(doc, false).is_ok());
    }

    // validation.go:221-228: only one queue sort plugin may be enabled.
    #[test]
    fn only_one_queue_sort_enabled() {
        let doc = "profiles:\n- plugins: {queueSort: {enabled: [{name: A}, {name: B}]}}\n";
        let err = volume_binding_args_from_config(doc, false).unwrap_err();
        assert_eq!(
            err,
            "profiles[0].plugins.queueSort.Enabled: Invalid value: 2: only one queue sort plugin can be enabled"
        );
    }

    // "mismatch-queue-sort": queueSort pluginConfig args must match too
    // (validation.go:243-247).
    #[test]
    fn queue_sort_plugin_config_must_match() {
        let doc = "profiles:\n- schedulerName: a\n  plugins: {queueSort: {enabled: [{name: S}]}}\n  pluginConfig: [{name: S, args: {x: 1}}]\n- schedulerName: b\n  plugins: {queueSort: {enabled: [{name: S}]}}\n  pluginConfig: [{name: S, args: {x: 2}}]\n";
        let err = volume_binding_args_from_config(doc, false).unwrap_err();
        assert_eq!(
            err,
            "profiles[1].pluginConfig.args: Invalid value: {\"x\":2}: queueSort must be the same for all profiles"
        );
    }

    // validation_pluginargs_test.go TestValidateDynamicResourcesArgs
    // (:1159-1275). Detail is ignored upstream (ignoreBadValueDetail), so
    // type and field are pinned.
    mod dynamic_resources {
        use super::*;

        const SEC: i64 = 1_000_000_000;

        fn fts(filter: bool, binding_cond: bool, device_status: bool) -> DynamicResourcesFeatures {
            DynamicResourcesFeatures {
                dra_scheduler_filter_timeout: filter,
                dra_device_binding_conditions: binding_cond,
                dra_resource_claim_device_status: device_status,
            }
        }
        const ALL: fn() -> DynamicResourcesFeatures = || fts(true, true, true);

        fn a(filter: Option<i64>, binding: Option<i64>) -> DynamicResourcesArgs {
            DynamicResourcesArgs {
                filter_timeout: filter,
                binding_timeout: binding,
            }
        }

        fn check(
            args: DynamicResourcesArgs,
            f: DynamicResourcesFeatures,
            want: &[(ErrorType, &str)],
        ) {
            let errs = args.validate(&Path::default(), f);
            let got: Vec<(ErrorType, String)> = summary(&errs);
            let want: Vec<(ErrorType, String)> =
                want.iter().map(|(t, p)| (*t, p.to_string())).collect();
            assert_eq!(got, want, "{errs:?}");
        }

        #[test]
        fn valid_default_and_disabled() {
            check(a(Some(10 * SEC), Some(600 * SEC)), ALL(), &[]);
            check(a(None, None), ALL(), &[]);
        }

        #[test]
        fn negative_filter_timeout() {
            let errs = a(Some(-SEC), None).validate(&Path::default(), ALL());
            assert_eq!(errs[0].detail, "must be zero or positive");
            check(
                a(Some(-SEC), None),
                ALL(),
                &[(ErrorType::Invalid, "filterTimeout")],
            );
        }

        #[test]
        fn negative_filter_timeout_gate_disabled() {
            let errs = a(Some(-SEC), None).validate(&Path::default(), fts(false, true, true));
            assert_eq!(
                errs[0].detail,
                "DRASchedulingFilterTimeout feature gate is disabled"
            );
            check(
                a(Some(-SEC), None),
                fts(false, true, true),
                &[(ErrorType::Forbidden, "filterTimeout")],
            );
        }

        #[test]
        fn binding_timeout_cases() {
            check(a(None, Some(30 * SEC)), ALL(), &[]);
            check(
                a(None, Some(0)),
                ALL(),
                &[(ErrorType::Invalid, "bindingTimeout")],
            );
            check(
                a(None, Some(-SEC)),
                ALL(),
                &[(ErrorType::Invalid, "bindingTimeout")],
            );
            let errs = a(None, Some(0)).validate(&Path::default(), ALL());
            assert_eq!(errs[0].detail, "must be at least 1 second");
            for f in [fts(true, false, true), fts(true, true, false)] {
                check(
                    a(None, Some(SEC)),
                    f,
                    &[(ErrorType::Forbidden, "bindingTimeout")],
                );
            }
        }

        // v1/defaults.go:247-257
        #[test]
        fn defaults_follow_gates() {
            let mut x = DynamicResourcesArgs::default();
            x.set_defaults(ALL());
            assert_eq!(x, a(Some(10 * SEC), Some(600 * SEC)));
            let mut x = DynamicResourcesArgs::default();
            x.set_defaults(fts(false, true, true));
            assert_eq!(x, a(None, Some(600 * SEC)));
            for f in [fts(true, false, true), fts(true, true, false)] {
                let mut x = DynamicResourcesArgs::default();
                x.set_defaults(f);
                assert_eq!(x, a(Some(10 * SEC), None));
            }
            // Explicit values are kept.
            let mut x = a(Some(0), Some(SEC));
            x.set_defaults(ALL());
            assert_eq!(x, a(Some(0), Some(SEC)));
        }

        // validation.go:161-163 wiring: a negative filterTimeout in
        // pluginConfig is rejected end to end; absent args validate clean
        // with the default gates (defaults then validate).
        #[test]
        #[serial_test::serial]
        fn wired_into_validate_profiles() {
            rusternetes_common::feature_gates::reset_to_defaults();
            let bad = "profiles:\n- pluginConfig:\n  - name: DynamicResources\n    args:\n      filterTimeout: -1s\n";
            let err = volume_binding_args_from_config(bad, false).unwrap_err();
            assert!(
                err.contains("profiles[0].pluginConfig[0].args.filterTimeout: Invalid value"),
                "{err}"
            );
            assert!(err.contains("must be zero or positive"), "{err}");
            let bad = "profiles:\n- pluginConfig:\n  - name: DynamicResources\n    args:\n      bindingTimeout: 1s\n";
            let err = volume_binding_args_from_config(bad, false).unwrap_err();
            assert!(
                err.contains("profiles[0].pluginConfig[0].args.bindingTimeout: Forbidden"),
                "{err}"
            );
            let ok = "profiles:\n- pluginConfig:\n  - name: DynamicResources\n";
            assert!(volume_binding_args_from_config(ok, false).is_ok());
        }
    }
}
