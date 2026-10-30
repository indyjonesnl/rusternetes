//! Unit tests moved from `src/compatibility.rs`.
#![allow(unused_imports, dead_code)]

mod __parent {
    pub use rusternetes_common::feature_gates::{self, Version};
    pub use std::collections::BTreeMap;
}

mod tests {
    use super::__parent::*;
    use rusternetes_common::compatibility::*;
    use rusternetes_common::feature_gates::{self, Feature};
    use serial_test::serial;

    fn v(major: u32, minor: u32) -> Version {
        Version::major_minor(major, minor)
    }

    fn strs(a: &[&str]) -> Vec<String> {
        a.iter().map(|s| s.to_string()).collect()
    }

    // version_test.go TestValidate
    #[test]
    fn effective_version_validate() {
        // (name, binary, emulation, minCompat, emulationFloor, minCompatFloor, expectErrors)
        type Case = (
            &'static str,
            &'static str,
            &'static str,
            &'static str,
            &'static str,
            &'static str,
            bool,
        );
        let cases: [Case; 6] = [
            (
                "patch version diff ok",
                "v1.32.1",
                "v1.32.2",
                "v1.32.5",
                "",
                "",
                false,
            ),
            (
                "emulation greater than binary",
                "v1.32.2",
                "v1.33.0",
                "v1.31.0",
                "",
                "",
                true,
            ),
            (
                "min compat greater than emulation",
                "v1.32.2",
                "v1.31.0",
                "v1.32.0",
                "",
                "",
                true,
            ),
            (
                "between floor and binary ok",
                "v1.32.1",
                "v1.31.0",
                "v1.30.0",
                "v1.31.0",
                "v1.30.0",
                false,
            ),
            (
                "emulation less than floor",
                "v1.32.1",
                "v1.30.0",
                "v1.30.0",
                "v1.31.0",
                "v1.30.0",
                true,
            ),
            (
                "min compat less than floor",
                "v1.32.1",
                "v1.31.0",
                "v1.29.0",
                "v1.31.0",
                "v1.30.0",
                true,
            ),
        ];
        for (name, binary, emu, min, emu_floor, min_floor, expect_errors) in cases {
            let mut e = EffectiveVersion::from_string(binary, emu_floor, min_floor).unwrap();
            e.set_emulation_version(parse_generic(emu).unwrap().major_minor());
            e.set_min_compatibility_version(parse_generic(min).unwrap().major_minor());
            let errs = e.validate();
            assert_eq!(!errs.is_empty(), expect_errors, "{name}: {errs:?}");
        }
    }

    // version_test.go TestSetEmulationVersion
    #[test]
    fn set_emulation_version_defaults_min_compat() {
        for (emu, expect) in [("v1.32", "1.31"), ("v1.31", "1.31")] {
            let mut e = EffectiveVersion::from_string("v1.34", "v1.31", "v1.31").unwrap();
            e.set_emulation_version(parse_generic(emu).unwrap().major_minor());
            assert!(e.validate().is_empty());
            assert_eq!(e.min_compatibility_version().to_string(), expect, "{emu}");
        }
    }

    // version_test.go TestSetMinCompatibilityVersion
    #[test]
    fn set_min_compatibility_version_then_reset_by_emulation() {
        for (emu, min, emu_floor, min_floor) in [
            ("v1.32", "v1.30", "v1.31", "v1.30"),
            ("v1.34", "v1.34", "v1.31", "v1.31"),
        ] {
            let mut e = EffectiveVersion::from_string("v1.34", emu_floor, min_floor).unwrap();
            let emu = parse_generic(emu).unwrap().major_minor();
            let min = parse_generic(min).unwrap().major_minor();
            e.set_emulation_version(emu);
            assert_eq!(e.emulation_version(), emu);
            e.set_min_compatibility_version(min);
            assert_eq!(e.min_compatibility_version(), min);
            assert_eq!(e.emulation_version(), emu);
            assert!(e.validate().is_empty());
            e.set_emulation_version(emu);
            assert_eq!(e.min_compatibility_version(), emu.subtract_minor(1));
        }
    }

    // DefaultBuildEffectiveVersion + kubeEffectiveVersionFloors (version.go:31-55):
    // binary 1.35, both floors binary-3 = 1.32.
    #[test]
    fn default_build_effective_version_floors() {
        let e = default_build_effective_version();
        assert_eq!(e.binary_version(), v(1, 35));
        assert_eq!(e.emulation_version(), v(1, 35));
        assert_eq!(e.min_compatibility_version(), v(1, 34));
        assert_eq!(
            e.allowed_emulation_version_range(),
            "1.32..1.35(default:1.35)"
        );
        assert_eq!(
            e.allowed_min_compatibility_version_range(),
            "1.32..1.35(default:1.34)"
        );
        assert_eq!(
            e.to_string(),
            "{BinaryVersion: 1.35, EmulationVersion: 1.35, MinCompatibilityVersion: 1.34}"
        );
    }

    // registry_test.go TestVersionFlagOptions (kube row).
    #[test]
    fn registry_version_flag_options() {
        let r = ComponentGlobalsRegistry::new_default_kube();
        assert_eq!(
            r.version_flag_options(true),
            ["kube=1.32..1.35(default:1.35)"]
        );
        assert_eq!(
            r.version_flag_options(false),
            ["kube=1.32..1.35(default:1.34)"]
        );
    }

    #[test]
    fn parse_version_forms() {
        assert_eq!(parse_version("v1.30").unwrap().to_string(), "1.30");
        assert_eq!(parse_version(" 1.30.2 ").unwrap().to_string(), "1.30.2");
        assert_eq!(
            parse_version("v1.34.0-alpha.1+abc123").unwrap().to_string(),
            "1.34.0-alpha.1+abc123"
        );
        // semantic fails (extra data) -> generic keeps the numeric components.
        assert_eq!(parse_version("1.30.2abc").unwrap().to_string(), "1.30.2");
        assert_eq!(
            parse_version("1.foo").unwrap_err(),
            "illegal version string \"1.foo\""
        );
        assert_eq!(
            parse_version("foo").unwrap_err(),
            "could not parse \"foo\" as version"
        );
        assert_eq!(
            parse_generic("01.2").unwrap_err(),
            "illegal zero-prefixed version component \"01\" in \"01.2\""
        );
    }

    // registry_test.go TestFlags cases that exercise the kube component.
    fn run(
        emulated: &[&str],
        min: &[&str],
        gates: &[&str],
    ) -> Result<ComponentGlobalsRegistry, String> {
        let mut r = ComponentGlobalsRegistry::new_default_kube();
        r.set_flag_values(&strs(emulated), &strs(min), &strs(gates))?;
        r.set()?;
        Ok(r)
    }

    #[test]
    #[serial]
    fn flags_set_kube_versions() {
        feature_gates::reset_to_defaults();
        let r = run(&["kube=1.33"], &["kube=1.32"], &[]).unwrap();
        let e = r.effective_version_for("kube").unwrap();
        assert_eq!(e.emulation_version().to_string(), "1.33");
        assert_eq!(e.min_compatibility_version().to_string(), "1.32");
        assert_eq!(feature_gates::emulation_version(), v(1, 33));
        assert_eq!(feature_gates::min_compatibility_version(), v(1, 32));
        // prefix v and missing component default to kube.
        let r = run(&["v1.33"], &["v1.32"], &[]).unwrap();
        assert_eq!(
            r.effective_version_for("kube").unwrap().emulation_version(),
            v(1, 33)
        );
        feature_gates::reset_to_defaults();
    }

    #[test]
    #[serial]
    fn flags_error_cases() {
        feature_gates::reset_to_defaults();
        let err = |emu: &[&str], min: &[&str], gates: &[&str]| run(emu, min, gates).unwrap_err();
        assert!(err(&["kube=1.33", "kube=1.34"], &[], &[])
            .contains("duplicate version flag, kube=1.33 and kube=1.34"));
        assert!(err(&["1.33", "kube=1.33"], &[], &[])
            .contains("duplicate version flag, kube=1.33 and kube=1.33"));
        assert!(err(&[], &["kube=1.33", "kube=1.32"], &[])
            .contains("duplicate version flag, kube=1.33 and kube=1.32"));
        assert!(
            err(&["kube=1.33.2"], &[], &[]).contains("patch version not allowed, got: kube=1.33.2")
        );
        assert!(
            err(&[], &["kube=1.33.2"], &[]).contains("patch version not allowed, got: kube=1.33.2")
        );
        assert!(err(&["test3=1.33"], &[], &[]).contains("component not registered: test3"));
        assert!(err(&[], &["test3=1.33"], &[]).contains("component not registered: test3"));
        assert!(err(&["kube=1.foo"], &[], &[]).contains("illegal version string \"1.foo\""));
        assert!(err(&[], &[], &["test3:Foo=true"]).contains("component not registered: test3"));
        assert!(err(
            &[],
            &[],
            &["kube:PodTopologyLabelsAdmission=false", "PodTopologyLabelsAdmission=true"]
        )
        .contains(
            "set kube feature gates with default empty prefix or kube: prefix consistently, do not mix use"
        ));
        feature_gates::reset_to_defaults();
    }

    // The order enforced by Set(): emulation version, then min compatibility,
    // then feature gates (registry.go:344-428) — so a gate introduced in 1.35
    // is PreAlpha when `--emulated-version=1.34` accompanies it.
    #[test]
    #[serial]
    fn flags_gates_evaluated_at_emulated_version() {
        feature_gates::reset_to_defaults();
        let e = run(
            &["1.34"],
            &[],
            &["StructuredAuthenticationConfigurationJWKSMetrics=true"],
        )
        .unwrap_err();
        assert!(
            e.contains("cannot set feature gate StructuredAuthenticationConfigurationJWKSMetrics to true, feature is PreAlpha at emulated version 1.34, min compatibility version 1.33"),
            "{e}"
        );
        feature_gates::reset_to_defaults();
        // Same flags at the binary version are fine.
        run(
            &[],
            &[],
            &["kube:StructuredAuthenticationConfigurationJWKSMetrics=true"],
        )
        .unwrap();
        assert!(feature_gates::enabled(
            Feature::StructuredAuthenticationConfigurationJWKSMetrics
        ));
        feature_gates::reset_to_defaults();
    }

    #[test]
    fn colon_separated_multimap_allow_default_empty_key() {
        let m = parse_colon_separated_multimap(&strs(&["a=1,kube:b=2", "c=3,kube:d=4"])).unwrap();
        assert_eq!(m[""], ["a=1", "c=3"]);
        assert_eq!(m["kube"], ["b=2", "d=4"]);
    }

    // Registry.Validate: emulation outside [floor, binary] fails.
    #[test]
    #[serial]
    fn apply_flags_validates_floor() {
        feature_gates::reset_to_defaults();
        let e = apply_flags(&strs(&["1.30"]), &[], &[]).unwrap_err();
        assert!(
            e.contains("emulation version 1.30 is not between [1.32, 1.35]"),
            "{e}"
        );
        feature_gates::reset_to_defaults();
        let e = apply_flags(&strs(&["1.33"]), &strs(&["1.34"]), &[]).unwrap_err();
        assert!(
            e.contains("minCompatibilityVersion version 1.34 is not between [1.32, 1.33]"),
            "{e}"
        );
        feature_gates::reset_to_defaults();
        let e = apply_flags(&strs(&["1.33"]), &[], &[]).unwrap();
        assert_eq!(e.emulation_version(), v(1, 33));
        feature_gates::reset_to_defaults();
    }

    // version_test.go TestInfo
    #[test]
    fn effective_version_info() {
        // (name, binary, emulation, minCompat, [major, minor, emuMaj, emuMin, mcMaj, mcMin])
        type Case = (
            &'static str,
            &'static str,
            &'static str,
            &'static str,
            [&'static str; 6],
        );
        let cases: [Case; 5] = [
            (
                "normal case",
                "v1.34.0",
                "v1.32.0",
                "v1.31.0",
                ["1", "34", "1", "32", "1", "31"],
            ),
            (
                "default min compatibility version is emulation version - 1",
                "v1.34.0",
                "v1.32.0",
                "",
                ["1", "34", "1", "32", "1", "31"],
            ),
            (
                "emulation version same as binary version",
                "v1.34.0",
                "v1.34.0",
                "",
                ["1", "34", "1", "34", "1", "33"],
            ),
            (
                "with pre-release and build metadata",
                "v1.34.0-alpha.1+abc123",
                "v1.32.0",
                "",
                ["1", "34", "1", "32", "1", "31"],
            ),
            (
                "override default min compatibility version",
                "v1.34.0",
                "v1.32.0",
                "v1.32.0",
                ["1", "34", "1", "32", "1", "32"],
            ),
        ];
        for (name, bin, emu, mc, want) in cases {
            let mut e = EffectiveVersion::from_string(bin, "", "").unwrap();
            if !emu.is_empty() {
                e.set_emulation_version(parse_version(emu).unwrap().major_minor());
            }
            if !mc.is_empty() {
                e.set_min_compatibility_version(parse_version(mc).unwrap().major_minor());
            }
            let info = e.info();
            let got = [
                info.major.as_str(),
                info.minor.as_str(),
                info.emulation_major.as_str(),
                info.emulation_minor.as_str(),
                info.min_compatibility_major.as_str(),
                info.min_compatibility_minor.as_str(),
            ];
            assert_eq!(got, want, "{name}");
        }
    }

    // apimachinery version/types.go:27-34: the four emulation fields are `omitempty`.
    #[test]
    fn version_info_json_field_names() {
        let v = serde_json::to_value(default_build_effective_version().info()).unwrap();
        assert_eq!(v["emulationMajor"], "1");
        assert_eq!(v["emulationMinor"], "35");
        assert_eq!(v["minCompatibilityMajor"], "1");
        assert_eq!(v["minCompatibilityMinor"], "34");
        let v = serde_json::to_value(VersionInfo::default()).unwrap();
        assert!(v.get("emulationMajor").is_none());
        assert!(v.get("minCompatibilityMinor").is_none());
        assert_eq!(v["major"], "");
    }
}
