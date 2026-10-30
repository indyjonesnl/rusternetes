//! Unit tests moved from `src/feature_gates.rs`.
#![allow(unused_imports, dead_code)]

mod __parent {
    pub use rusternetes_common::feature_gates::upstream_table::UPSTREAM_GATES;
    pub use std::sync::atomic::{AtomicBool, Ordering};
}

mod tests {
    use super::__parent::*;
    use rusternetes_common::feature_gates::*;
    use serial_test::serial;

    #[test]
    #[serial]
    fn default_is_upstream_default() {
        reset_to_defaults();
        assert!(
            enabled(Feature::RelaxedDNSSearchValidation),
            "RelaxedDNSSearchValidation is GA-locked-default-true since v1.34"
        );
        assert!(
            enabled(Feature::PodTopologyLabelsAdmission),
            "PodTopologyLabelsAdmission is Beta+enabled in v1.35; default must be true"
        );
    }

    /// Pin that `STATES` is seeded from `default_enabled()` — without a
    /// `reset_to_defaults()` call. Catches a maintainer adding a future
    /// `Feature` variant whose `default_enabled()` is `false` but whose
    /// `STATES` slot is left as `AtomicBool::new(true)` (the obvious
    /// copy-paste from the existing slot).
    #[test]
    #[serial]
    fn states_match_default_enabled_at_process_start() {
        // First, capture each gate's current value (set may have been called
        // by other tests in the suite).
        let snapshot: Vec<(Feature, bool)> =
            ALL_FEATURES.iter().map(|&f| (f, enabled(f))).collect();
        // Force every slot back to its default and verify equality with the
        // declared `default_enabled()`. This is the property the static
        // initializer is supposed to give us at process start.
        reset_to_defaults();
        for &f in ALL_FEATURES {
            assert_eq!(
                enabled(f),
                f.default_enabled(),
                "{:?} default mismatch — STATES initializer drifted from default_enabled()",
                f,
            );
        }
        // Restore prior values so neighbouring tests aren't surprised.
        for (f, v) in snapshot {
            set(f, v);
        }
    }

    #[test]
    #[serial]
    fn set_swaps_value() {
        reset_to_defaults();
        let prev = set(Feature::RelaxedDNSSearchValidation, false);
        assert!(prev);
        assert!(!enabled(Feature::RelaxedDNSSearchValidation));
        set(Feature::RelaxedDNSSearchValidation, true);
    }

    #[test]
    #[serial]
    fn guard_restores_on_drop() {
        reset_to_defaults();
        {
            let _g = with_feature(Feature::RelaxedDNSSearchValidation, false);
            assert!(!enabled(Feature::RelaxedDNSSearchValidation));
        }
        assert!(enabled(Feature::RelaxedDNSSearchValidation));
    }

    #[test]
    #[serial]
    fn pod_topology_labels_admission_guard_round_trip() {
        reset_to_defaults();
        assert!(enabled(Feature::PodTopologyLabelsAdmission));
        {
            let _g = with_feature(Feature::PodTopologyLabelsAdmission, false);
            assert!(!enabled(Feature::PodTopologyLabelsAdmission));
        }
        assert!(enabled(Feature::PodTopologyLabelsAdmission));
    }

    #[test]
    #[serial]
    fn set_from_str_applies_and_skips_empty() {
        reset_to_defaults();
        set_from_str("StorageCapacityScoring=true,,PodDeletionCost=false").unwrap();
        assert!(enabled(Feature::StorageCapacityScoring));
        assert!(!enabled(Feature::PodDeletionCost));
        set_from_str("").unwrap();
        reset_to_defaults();
    }

    #[test]
    #[serial]
    fn set_from_str_errors_match_upstream_and_apply_nothing() {
        reset_to_defaults();
        // feature_gate.go:348-350
        assert_eq!(
            set_from_str("StorageCapacityScoring").unwrap_err(),
            "missing bool value for StorageCapacityScoring"
        );
        // feature_gate.go:353-355 (strconv.ParseBool error text)
        assert_eq!(
            set_from_str("StorageCapacityScoring=banana").unwrap_err(),
            "invalid value of StorageCapacityScoring=banana, err: strconv.ParseBool: parsing \"banana\": invalid syntax"
        );
        // feature_gate.go:446-453: enabledRaw keeps the rejected key, so every
        // later Set would fail on it too (upstream tests build a fresh gate per
        // case); reset between error cases.
        reset_to_defaults();
        // feature_gate.go:387
        assert_eq!(
            set_from_str("fooBarBaz=true").unwrap_err(),
            "unrecognized feature gate: fooBarBaz"
        );
        reset_to_defaults();
        // feature_gate.go:392 (LockToDefault)
        assert_eq!(
            set_from_str("RelaxedDNSSearchValidation=false").unwrap_err(),
            "cannot set feature gate RelaxedDNSSearchValidation to false, feature is locked to true"
        );
        reset_to_defaults();
        // feature_gate.go:463-466: nothing persisted when any error occurred.
        assert!(set_from_str("StorageCapacityScoring=true,fooBarBaz=true").is_err());
        assert!(!enabled(Feature::StorageCapacityScoring));
        reset_to_defaults();
    }

    #[test]
    #[serial]
    fn set_from_str_parse_bool_and_whitespace() {
        reset_to_defaults();
        set_from_str(" StorageCapacityScoring = T ").unwrap();
        assert!(enabled(Feature::StorageCapacityScoring));
        set_from_str("StorageCapacityScoring=0").unwrap();
        assert!(!enabled(Feature::StorageCapacityScoring));
        reset_to_defaults();
    }

    // feature_gate_test.go TestFeatureGateFlag "AllAlpha=true" / "AllBeta=true":
    // the special gates flip every Alpha (resp. Beta) gate not set explicitly.
    #[test]
    #[serial]
    fn all_alpha_and_all_beta_toggle_by_prerelease() {
        reset_to_defaults();
        // StorageCapacityScoring: Alpha, default off. PodDeletionCost: Beta, default on.
        // MaxUnavailableStatefulSet: Beta, default off.
        set_from_str("AllAlpha=true").unwrap();
        assert!(enabled(Feature::StorageCapacityScoring));
        assert!(enabled(Feature::DRADeviceTaints));
        assert!(
            !enabled(Feature::MaxUnavailableStatefulSet),
            "Beta untouched"
        );
        reset_to_defaults();
        set_from_str("AllBeta=true").unwrap();
        assert!(enabled(Feature::MaxUnavailableStatefulSet));
        assert!(!enabled(Feature::StorageCapacityScoring), "Alpha untouched");
        reset_to_defaults();
        set_from_str("AllBeta=false").unwrap();
        assert!(!enabled(Feature::PodDeletionCost));
        reset_to_defaults();
    }

    // "TestAlpha=false,AllAlpha=true" and "AllAlpha=true,TestAlpha=false":
    // explicit wins regardless of order.
    #[test]
    #[serial]
    fn explicit_gate_overrides_all_alpha_in_either_order() {
        for arg in [
            "StorageCapacityScoring=false,AllAlpha=true",
            "AllAlpha=true,StorageCapacityScoring=false",
        ] {
            reset_to_defaults();
            set_from_str(arg).unwrap();
            assert!(!enabled(Feature::StorageCapacityScoring), "{arg}");
            assert!(enabled(Feature::DRADeviceTaints), "{arg}");
        }
        reset_to_defaults();
    }

    fn synthetic() -> Vec<KnownGate> {
        let g = |n: &str, pre, locked| KnownGate {
            name: n.into(),
            specs: if pre == PreRelease::PreAlpha {
                vec![]
            } else {
                vec![FeatureSpec {
                    default: false,
                    lock_to_default: locked,
                    pre_release: pre,
                    version: Version::major_minor(0, 0),
                    min_compatibility_version: None,
                }]
            },
        };
        vec![
            g("TestAlpha", PreRelease::Alpha, false),
            g("TestBeta", PreRelease::Beta, false),
            g("TestPreAlpha", PreRelease::PreAlpha, false),
            g("TestLockedFalse", PreRelease::Ga, true),
        ]
    }

    fn m(args: &[(&str, bool)]) -> Vec<(String, bool)> {
        args.iter().map(|(k, v)| (k.to_string(), *v)).collect()
    }

    // feature_gate_test.go:806-807 / 824-825: PreAlpha gates are rejected,
    // and AllAlpha never enables them.
    #[test]
    fn pre_alpha_rejected_and_untouched_by_all_alpha() {
        let k = synthetic();
        assert_eq!(
            resolve_gates(&m(&[("TestPreAlpha", true)]), &k, EMULATION_VERSION, MIN_COMPATIBILITY_VERSION).unwrap_err(),
            "cannot set feature gate TestPreAlpha to true, feature is PreAlpha at emulated version 1.35, min compatibility version 1.34"
        );
        let mut got = resolve_gates(
            &m(&[("AllAlpha", true)]),
            &k,
            EMULATION_VERSION,
            MIN_COMPATIBILITY_VERSION,
        )
        .unwrap();
        got.sort();
        assert_eq!(got, vec![("TestAlpha".to_string(), true)]);
    }

    // feature_gate_test.go:1684-1745 TestFeatureSpecAtEmulationAndMinCompatVersion.
    #[test]
    fn feature_spec_at_emulation_and_min_compat_version_table() {
        let v = Version::major_minor;
        let spec = |ver, default, pre_release, min| FeatureSpec {
            default,
            lock_to_default: false,
            pre_release,
            version: ver,
            min_compatibility_version: min,
        };
        let alpha = spec(v(1, 25), false, PreRelease::Alpha, None);
        let beta = spec(v(1, 25), false, PreRelease::Beta, Some(v(1, 25)));
        let ga = spec(v(1, 29), true, PreRelease::Ga, Some(v(1, 25)));
        let pre = spec(v(0, 0), false, PreRelease::PreAlpha, None);
        let specs = [alpha, beta, ga];
        let cases = [
            (v(1, 30), None, ga),
            (v(1, 29), None, ga),
            (v(1, 28), None, beta),
            (v(1, 26), None, beta),
            (v(1, 25), Some(v(1, 25)), beta),
            (v(1, 25), None, alpha),
            (v(1, 24), None, pre),
            (v(1, 26), Some(v(1, 23)), alpha),
            (v(1, 29), Some(v(1, 23)), alpha),
        ];
        for (i, (emu, min, want)) in cases.into_iter().enumerate() {
            assert_eq!(
                feature_spec_at_emulation_and_min_compat_version(&specs, emu, min),
                want,
                "case {i}: emulation {emu}"
            );
        }
    }

    // pkg/features/kube_features.go defaultVersionedKubernetesFeatureGates:
    // real gates resolve through their version history.
    #[test]
    fn real_gate_specs_resolve_by_emulation_version() {
        let v = Version::major_minor;
        // GenericWorkload: {1.35, false, Alpha} only.
        assert_eq!(
            Feature::GenericWorkload.spec_at(v(1, 34)).pre_release,
            PreRelease::PreAlpha
        );
        assert_eq!(
            Feature::GenericWorkload.spec_at(v(1, 35)).pre_release,
            PreRelease::Alpha
        );
        // DRAAdminAccess: {1.32 Alpha off}, {1.34 Beta on}.
        let s = Feature::DRAAdminAccess.spec_at(v(1, 33));
        assert_eq!((s.pre_release, s.default), (PreRelease::Alpha, false));
        let s = Feature::DRAAdminAccess.spec_at(v(1, 35));
        assert_eq!((s.pre_release, s.default), (PreRelease::Beta, true));
        // RelaxedDNSSearchValidation: GA + LockToDefault only from 1.34.
        assert!(
            !Feature::RelaxedDNSSearchValidation
                .spec_at(v(1, 33))
                .lock_to_default
        );
        assert!(
            Feature::RelaxedDNSSearchValidation
                .spec_at(v(1, 34))
                .lock_to_default
        );
    }

    // The version history is the source of truth; the per-gate v1.35
    // summaries (`prerelease`/`default_enabled`/`locked_to_default`) must
    // equal the spec resolved at the target emulation version, and the
    // resolution must not depend on the min-compatibility version (no 1.35
    // spec sets MinCompatibilityVersion).
    #[test]
    fn version_history_agrees_with_v1_35_summaries() {
        for f in ALL_FEATURES {
            let s = f.spec_at(EMULATION_VERSION);
            assert_eq!(s.pre_release, f.prerelease(), "{f:?}");
            assert_eq!(s.default, f.default_enabled(), "{f:?}");
            assert_eq!(s.lock_to_default, f.locked_to_default(), "{f:?}");
            assert!(f
                .specs()
                .iter()
                .all(|x| x.min_compatibility_version.is_none()));
            assert!(f.specs().windows(2).all(|w| w[0].version < w[1].version));
        }
    }

    // version.go:259-276: SubtractMinor saturates at 0.
    #[test]
    fn subtract_minor_saturates() {
        assert_eq!(
            Version::major_minor(1, 35).subtract_minor(1).to_string(),
            "1.34"
        );
        assert_eq!(
            Version::major_minor(1, 0).subtract_minor(1).to_string(),
            "1.0"
        );
    }

    // "TestBeta=true,AllAlpha=false" and "TestAlpha=true,AllBeta=false".
    #[test]
    fn specials_do_not_cross_prerelease() {
        let k = synthetic();
        let mut got = resolve_gates(
            &m(&[("TestBeta", true), ("AllAlpha", false)]),
            &k,
            EMULATION_VERSION,
            MIN_COMPATIBILITY_VERSION,
        )
        .unwrap();
        got.sort();
        assert_eq!(
            got,
            vec![
                ("TestAlpha".to_string(), false),
                ("TestBeta".to_string(), true)
            ]
        );
        assert!(resolve_gates(
            &m(&[("TestLockedFalse", true)]),
            &k,
            EMULATION_VERSION,
            MIN_COMPATIBILITY_VERSION
        )
        .unwrap_err()
        .contains("locked to false"));
    }

    // "AllAlpha=banana" -> parse error, nothing applied.
    #[test]
    #[serial]
    fn all_alpha_bad_value_errors() {
        reset_to_defaults();
        assert!(set_from_str("AllAlpha=banana")
            .unwrap_err()
            .starts_with("invalid value of AllAlpha"));
        reset_to_defaults();
    }

    // kube_features.go: InPlacePodVerticalScaling at 1.35 is
    // `Default: true, PreRelease: GA, LockToDefault: true`.
    #[test]
    #[serial]
    fn in_place_pod_vertical_scaling_is_locked_to_default() {
        reset_to_defaults();
        assert_eq!(
            set_from_str("InPlacePodVerticalScaling=false").unwrap_err(),
            "cannot set feature gate InPlacePodVerticalScaling to false, feature is locked to true"
        );
        assert!(enabled(Feature::InPlacePodVerticalScaling));
    }

    // A gate that is locked must be GA (or Deprecated) in upstream; guard the
    // per-gate audit against drifting from the prerelease table.
    #[test]
    fn locked_gates_are_ga_and_default_on() {
        for f in ALL_FEATURES {
            if f.locked_to_default() {
                assert_eq!(f.prerelease(), PreRelease::Ga, "{f:?}");
                assert!(f.default_enabled(), "{f:?}");
            }
        }
    }

    // feature_gate.go:418-434 (AddDependencies check). kube_features.go:2166+:
    // MatchLabelKeysInPodTopologySpreadSelectorMerge: {MatchLabelKeysInPodTopologySpread}.
    #[test]
    #[serial]
    fn dependencies_checked_after_set() {
        reset_to_defaults();
        assert_eq!(
            set_from_str("MatchLabelKeysInPodTopologySpread=false").unwrap_err(),
            "MatchLabelKeysInPodTopologySpreadSelectorMerge is enabled, but depends on features that are disabled: [MatchLabelKeysInPodTopologySpread]"
        );
        assert!(enabled(Feature::MatchLabelKeysInPodTopologySpread));
        // Disabling both together is fine.
        set_from_str(
            "MatchLabelKeysInPodTopologySpread=false,MatchLabelKeysInPodTopologySpreadSelectorMerge=false",
        )
        .unwrap();
        // DRADeviceTaintRules -> DRADeviceTaints; ClusterTrustBundleProjection -> ClusterTrustBundle.
        assert_eq!(
            set_from_str("DRADeviceTaintRules=true").unwrap_err(),
            "DRADeviceTaintRules is enabled, but depends on features that are disabled: [DRADeviceTaints]"
        );
        assert!(set_from_str("DRADeviceTaintRules=true,DRADeviceTaints=true").is_ok());
        reset_to_defaults();
    }

    // kube_features.go:2166+ edges whose target was previously unmodelled
    // (#3222): GangScheduling -> GenericWorkload,
    // GracefulNodeShutdownBasedOnPodPriority -> GracefulNodeShutdown,
    // RestartAllContainersOnContainerExits -> {ContainerRestartRules,
    // NodeDeclaredFeatures}.
    #[test]
    #[serial]
    fn dependencies_on_formerly_unmodelled_gates() {
        reset_to_defaults();
        assert_eq!(
            set_from_str("GangScheduling=true").unwrap_err(),
            "GangScheduling is enabled, but depends on features that are disabled: [GenericWorkload]"
        );
        assert!(set_from_str("GangScheduling=true,GenericWorkload=true").is_ok());
        reset_to_defaults();
        assert_eq!(
            set_from_str("GracefulNodeShutdown=false").unwrap_err(),
            "[GracefulNodeShutdownBasedOnPodPriority is enabled, but depends on features that are disabled: [GracefulNodeShutdown], WindowsGracefulNodeShutdown is enabled, but depends on features that are disabled: [GracefulNodeShutdown]]"
        );
        reset_to_defaults();
        assert_eq!(
            set_from_str("RestartAllContainersOnContainerExits=true").unwrap_err(),
            "RestartAllContainersOnContainerExits is enabled, but depends on features that are disabled: [NodeDeclaredFeatures]"
        );
        assert!(set_from_str(
            "RestartAllContainersOnContainerExits=true,NodeDeclaredFeatures=true"
        )
        .is_ok());
        reset_to_defaults();
    }

    // DynamicResourceAllocation is GA + LockToDefault in 1.35
    // (kube_features.go:1293-1296), so it can not be switched off.
    #[test]
    #[serial]
    fn dynamic_resource_allocation_is_locked_on() {
        reset_to_defaults();
        assert_eq!(
            set_from_str("DynamicResourceAllocation=false").unwrap_err(),
            "cannot set feature gate DynamicResourceAllocation to false, feature is locked to true"
        );
        reset_to_defaults();
    }

    // kube_features.go:2166+ edges from #3231 whose source gate was unmodelled:
    // UserNamespacesHostNetworkSupport -> UserNamespacesSupport,
    // WindowsCPUAndMemoryAffinity -> MemoryManager,
    // WindowsGracefulNodeShutdown -> GracefulNodeShutdown,
    // ServiceAccountTokenNodeBinding -> ServiceAccountTokenNodeBindingValidation,
    // DeclarativeValidationTakeover -> DeclarativeValidation,
    // StructuredAuthenticationConfiguration{EgressSelector,JWKSMetrics} ->
    // StructuredAuthenticationConfiguration,
    // UnknownVersionInteroperabilityProxy -> APIServerIdentity.
    #[test]
    #[serial]
    fn dependency_edges_with_newly_modelled_sources() {
        reset_to_defaults();
        for (src, dep) in [
            ("UserNamespacesHostNetworkSupport", "UserNamespacesSupport"),
            ("UnknownVersionInteroperabilityProxy", "APIServerIdentity"),
            ("DeclarativeValidationTakeover", "DeclarativeValidation"),
            ("WindowsCPUAndMemoryAffinity", "MemoryManager"),
        ] {
            reset_to_defaults();
            // Enable the source and disable its dependency; MemoryManager is
            // locked on so it is covered by the enabled-dependency case below.
            if dep == "MemoryManager" {
                assert!(set_from_str(&format!("{src}=true")).is_ok(), "{src}");
                continue;
            }
            let err = set_from_str(&format!("{src}=true,{dep}=false")).unwrap_err();
            assert!(
                err.contains(&format!(
                    "{src} is enabled, but depends on features that are disabled: [{dep}]"
                )),
                "{err}"
            );
        }
        reset_to_defaults();
        assert_eq!(
            set_from_str("GracefulNodeShutdown=false").unwrap_err(),
            "[GracefulNodeShutdownBasedOnPodPriority is enabled, but depends on features that are disabled: [GracefulNodeShutdown], WindowsGracefulNodeShutdown is enabled, but depends on features that are disabled: [GracefulNodeShutdown]]"
        );
        reset_to_defaults();
        // GA + LockToDefault gates (kube_features.go:1784-1794,
        // apiserver kube_features.go:461-465).
        assert_eq!(
            set_from_str("ServiceAccountTokenNodeBindingValidation=false").unwrap_err(),
            "cannot set feature gate ServiceAccountTokenNodeBindingValidation to false, feature is locked to true"
        );
        reset_to_defaults(); // enabledRaw keeps the rejected key (feature_gate.go:446-453)
        assert_eq!(
            set_from_str("StructuredAuthenticationConfiguration=false").unwrap_err(),
            "cannot set feature gate StructuredAuthenticationConfiguration to false, feature is locked to true"
        );
        reset_to_defaults();
    }

    // ---- --emulated-version (feature_gate_test.go TestVersionedFeatureGateFlag /
    // SetEmulationVersionAndMinCompatibilityVersion) ----

    const JWKS: Feature = Feature::StructuredAuthenticationConfigurationJWKSMetrics;

    #[test]
    #[serial]
    fn emulation_version_re_resolves_every_default() {
        reset_to_defaults();
        for minor in 31..=35 {
            let emu = Version::major_minor(1, minor);
            set_emulation_version(emu).unwrap();
            assert_eq!(emulation_version(), emu);
            assert_eq!(min_compatibility_version(), emu.subtract_minor(1));
            for &f in ALL_FEATURES {
                assert_eq!(enabled(f), f.spec_at(emu).default, "{f:?} at {emu}");
            }
        }
        reset_to_defaults();
        // JWKSMetrics is introduced (Beta, on) in 1.35, so it is PreAlpha/off at 1.34.
        assert!(enabled(JWKS));
        set_emulation_version(Version::major_minor(1, 34)).unwrap();
        assert!(!enabled(JWKS));
        reset_to_defaults();
    }

    #[test]
    #[serial]
    fn prealpha_error_quotes_emulated_and_min_compat_versions() {
        reset_to_defaults();
        set_emulation_version(Version::major_minor(1, 34)).unwrap();
        assert_eq!(
            set_from_str("StructuredAuthenticationConfigurationJWKSMetrics=true").unwrap_err(),
            "cannot set feature gate StructuredAuthenticationConfigurationJWKSMetrics to true, feature is PreAlpha at emulated version 1.34, min compatibility version 1.33"
        );
        reset_to_defaults();
    }

    #[test]
    #[serial]
    fn explicit_gate_survives_emulation_version_change() {
        reset_to_defaults();
        let f = Feature::PodTopologyLabelsAdmission;
        assert!(f.spec_at(Version::major_minor(1, 34)).pre_release != PreRelease::PreAlpha);
        set_from_str("PodTopologyLabelsAdmission=false").unwrap();
        set_emulation_version(Version::major_minor(1, 34)).unwrap();
        assert!(!enabled(f));
        set_emulation_version(Version::major_minor(1, 35)).unwrap();
        assert!(!enabled(f));
        reset_to_defaults();
    }

    // feature_gate.go:821-824: nothing is persisted when the explicit
    // gates are invalid at the new version.
    #[test]
    #[serial]
    fn emulation_version_change_rejected_when_explicit_gate_becomes_prealpha() {
        reset_to_defaults();
        set_from_str("StructuredAuthenticationConfigurationJWKSMetrics=true").unwrap();
        let err = set_emulation_version(Version::major_minor(1, 34)).unwrap_err();
        assert!(
            err.contains(
                "feature is PreAlpha at emulated version 1.34, min compatibility version 1.33"
            ),
            "{err}"
        );
        assert_eq!(emulation_version(), Version::major_minor(1, 35));
        assert!(enabled(JWKS));
        reset_to_defaults();
    }

    // feature_gate.go:446-453: enabledRaw accumulates, and `enabled` is
    // recomputed from all of it, so an earlier explicit value beats a later
    // AllBeta.
    #[test]
    #[serial]
    fn raw_gates_accumulate_across_set_calls() {
        reset_to_defaults();
        set_from_str("PodTopologyLabelsAdmission=false").unwrap();
        set_from_str("AllBeta=true").unwrap();
        assert!(!enabled(Feature::PodTopologyLabelsAdmission));
        reset_to_defaults();
    }

    #[test]
    #[serial]
    fn all_alpha_is_re_evaluated_at_the_emulated_version() {
        reset_to_defaults();
        set_from_str("AllAlpha=true").unwrap();
        let emu = Version::major_minor(1, 34);
        set_emulation_version(emu).unwrap();
        for &f in ALL_FEATURES {
            let s = f.spec_at(emu);
            let want = if s.pre_release == PreRelease::Alpha {
                true
            } else {
                s.default
            };
            assert_eq!(enabled(f), want, "{f:?}");
        }
        reset_to_defaults();
    }

    #[test]
    #[serial]
    fn reset_to_defaults_restores_versions() {
        set_emulation_version_and_min_compatibility_version(
            Version::major_minor(1, 33),
            Version::major_minor(1, 33),
        )
        .unwrap();
        assert_eq!(min_compatibility_version(), Version::major_minor(1, 33));
        reset_to_defaults();
        assert_eq!(emulation_version(), Version::major_minor(1, 35));
        assert_eq!(min_compatibility_version(), Version::major_minor(1, 34));
    }

    #[test]
    #[serial]
    fn validate_reports_gates_invalid_at_the_current_version() {
        reset_to_defaults();
        assert!(validate().is_empty());
        set_from_str("PodTopologyLabelsAdmission=false").unwrap();
        assert!(validate().is_empty());
        reset_to_defaults();
    }

    // #3321: gates registered upstream but consumed by no rusternetes code
    // are still known to `--feature-gates` (feature_gate.go:385-389 only
    // rejects names absent from `known`).
    #[test]
    #[serial]
    fn unconsumed_upstream_gates_are_recognised() {
        reset_to_defaults();
        // pkg/features/kube_features.go SidecarContainers: 1.33 GA + LockToDefault(true).
        set_from_str("SidecarContainers=true").unwrap();
        // staging/src/k8s.io/apiextensions-apiserver/pkg/features/kube_features.go
        set_from_str("CRDValidationRatcheting=true").unwrap();
        // staging/src/k8s.io/component-base/logs/api/v1/kube_features.go
        set_from_str("ContextualLogging=false").unwrap();
        reset_to_defaults();
    }

    // Structural guard for the generated table (scripts/gen-feature-gate-table.sh
    // --check is the drift guard against ../kubernetes itself).
    #[test]
    fn upstream_table_is_well_formed() {
        // 218 gates in GetAllVersioned() minus the AllAlpha/AllBeta specials.
        assert_eq!(UPSTREAM_GATES.len(), 216);
        assert!(
            UPSTREAM_GATES.windows(2).all(|w| w[0].name < w[1].name),
            "sorted and unique"
        );
        for g in UPSTREAM_GATES {
            assert!(!g.specs.is_empty(), "{}", g.name);
            assert!(
                g.specs.windows(2).all(|w| w[0].version < w[1].version),
                "{} specs ascend",
                g.name
            );
            for d in g.deps {
                assert!(upstream_gate(d).is_some(), "{} -> unknown {d}", g.name);
            }
        }
        for &f in ALL_FEATURES {
            assert!(upstream_gate(&format!("{f:?}")).is_some(), "{f:?}");
        }
    }

    // `Enabled` for a gate no `Feature` variant models, across emulation versions
    // (kube_features.go WatchList: 1.32 Beta on, 1.33 Beta off, 1.34 Beta on).
    #[test]
    #[serial]
    fn unmodelled_gate_state_follows_emulation_version() {
        reset_to_defaults();
        assert_eq!(gate_enabled("WatchList"), Some(true));
        assert_eq!(gate_enabled("fooBarBaz"), None);
        set_emulation_version(Version::major_minor(1, 33)).unwrap();
        assert_eq!(gate_enabled("WatchList"), Some(false));
        set_from_str("WatchList=true").unwrap();
        assert_eq!(gate_enabled("WatchList"), Some(true));
        reset_to_defaults();
    }

    // AllAlpha / AllBeta reach unmodelled gates too (feature_gate.go:270-296).
    #[test]
    #[serial]
    fn all_alpha_toggles_unmodelled_alpha_gates() {
        reset_to_defaults();
        let alpha = UPSTREAM_GATES
            .iter()
            .find(|g| {
                feature_by_name(g.name).is_none()
                    && g.deps.is_empty()
                    && feature_spec_at_emulation_and_min_compat_version(
                        g.specs,
                        EMULATION_VERSION,
                        Some(MIN_COMPATIBILITY_VERSION),
                    )
                    .pre_release
                        == PreRelease::Alpha
            })
            .expect("an unmodelled alpha gate")
            .name;
        assert_eq!(gate_enabled(alpha), Some(false));
        assert!(!enabled_alpha_features().iter().any(|n| n == alpha));
        set_from_str("AllAlpha=true").unwrap();
        assert_eq!(gate_enabled(alpha), Some(true));
        assert!(enabled_alpha_features().iter().any(|n| n == alpha));
        reset_to_defaults();
    }

    #[test]
    #[serial]
    fn unconsumed_upstream_gates_honour_lock_to_default() {
        reset_to_defaults();
        assert_eq!(
            set_from_str("SidecarContainers=false").unwrap_err(),
            "cannot set feature gate SidecarContainers to false, feature is locked to true"
        );
        reset_to_defaults();
    }
}
