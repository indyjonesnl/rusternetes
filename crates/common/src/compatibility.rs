//! Port of `k8s.io/component-base/compatibility` (`version.go` effectiveVersion,
//! `registry.go` componentGlobalsRegistry) reduced to the single `kube`
//! component, plus the `k8s.io/apimachinery/pkg/util/version` parsing it uses.
//!
//! Upstream sources (release-1.35):
//! * `staging/src/k8s.io/component-base/compatibility/version.go`
//! * `staging/src/k8s.io/component-base/compatibility/registry.go`
//! * `staging/src/k8s.io/apimachinery/pkg/util/version/version.go`
//! * `staging/src/k8s.io/apiserver/pkg/util/compatibility/version.go`

use crate::feature_gates::Version;
use std::collections::BTreeMap;

/// `DefaultKubeComponent` (registry.go:34).
pub const DEFAULT_KUBE_COMPONENT: &str = "kube";

/// A parsed `k8s.io/apimachinery/pkg/util/version.Version`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ParsedVersion {
    pub components: Vec<u64>,
    pub pre_release: String,
    pub build_metadata: String,
}

impl ParsedVersion {
    pub fn major(&self) -> u32 {
        todo!()
    }
    pub fn minor(&self) -> u32 {
        todo!()
    }
    pub fn patch(&self) -> u32 {
        todo!()
    }
    pub fn major_minor(&self) -> Version {
        todo!()
    }
}

impl std::fmt::Display for ParsedVersion {
    fn fmt(&self, _f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        todo!()
    }
}

/// `version.Parse` (version.go:150): semantic first, then generic.
pub fn parse_version(_s: &str) -> Result<ParsedVersion, String> {
    todo!()
}

/// `version.ParseGeneric` (version.go:137).
pub fn parse_generic(_s: &str) -> Result<ParsedVersion, String> {
    todo!()
}

/// `compatibility.effectiveVersion` (version.go:57-72).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct EffectiveVersion {
    binary_version: Version,
    emulation_version: Version,
    min_compatibility_version: Version,
    emulation_version_floor: Version,
    min_compatibility_version_floor: Version,
}

impl EffectiveVersion {
    /// `NewEffectiveVersion` (version.go:188-199).
    pub fn new(
        _binary_version: Version,
        _emulation_version_floor: Version,
        _min_compatibility_version_floor: Version,
    ) -> Self {
        todo!()
    }
    /// `NewEffectiveVersionFromString` (version.go:202-216).
    pub fn from_string(
        _binary_ver: &str,
        _emulation_ver_floor: &str,
        _min_compatibility_ver_floor: &str,
    ) -> Result<Self, String> {
        todo!()
    }
    pub fn binary_version(&self) -> Version {
        todo!()
    }
    pub fn emulation_version(&self) -> Version {
        todo!()
    }
    pub fn min_compatibility_version(&self) -> Version {
        todo!()
    }
    pub fn set_emulation_version(&mut self, _emulation_version: Version) {
        todo!()
    }
    pub fn set_min_compatibility_version(&mut self, _v: Version) {
        todo!()
    }
    pub fn validate(&self) -> Vec<String> {
        todo!()
    }
    pub fn allowed_emulation_version_range(&self) -> String {
        todo!()
    }
    pub fn allowed_min_compatibility_version_range(&self) -> String {
        todo!()
    }
}

impl std::fmt::Display for EffectiveVersion {
    fn fmt(&self, _f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        todo!()
    }
}

/// `DefaultBuildEffectiveVersion` (apiserver/pkg/util/compatibility/version.go:31-42).
pub fn default_build_effective_version() -> EffectiveVersion {
    todo!()
}

/// `toVersionMap` (registry.go:299-326).
pub fn to_version_map(
    _version_config: &[String],
) -> Result<BTreeMap<String, ParsedVersion>, String> {
    todo!()
}

/// `ColonSeparatedMultimapStringString.Set` with `allowDefaultEmptyKey`
/// (cli/flag/colon_separated_multimap_string_string.go:55-79), folded over
/// every flag occurrence in order.
pub fn parse_colon_separated_multimap(
    _values: &[String],
) -> Result<BTreeMap<String, Vec<String>>, String> {
    todo!()
}

/// The `kube` slice of `componentGlobalsRegistry` (registry.go:76-111).
#[derive(Debug)]
pub struct ComponentGlobalsRegistry {
    effective_version: EffectiveVersion,
    emulation_version_config: Vec<String>,
    min_compatibility_version_config: Vec<String>,
    feature_gates_config: BTreeMap<String, Vec<String>>,
}

impl ComponentGlobalsRegistry {
    /// Registry holding `DefaultBuildEffectiveVersion()` under `kube`
    /// (apiserver/pkg/util/compatibility/registry.go:51-53).
    pub fn new_default_kube() -> Self {
        todo!()
    }
    pub fn effective_version_for(&self, _component: &str) -> Option<&EffectiveVersion> {
        todo!()
    }
    /// The flag values of `--emulated-version`, `--min-compatibility-version`
    /// and `--feature-gates` (`AddFlags`, registry.go:226-262).
    pub fn set_flag_values(
        &mut self,
        _emulated_version: &[String],
        _min_compatibility_version: &[String],
        _feature_gates: &[String],
    ) -> Result<(), String> {
        todo!()
    }
    pub fn version_flag_options(&self, _is_emulation: bool) -> Vec<String> {
        todo!()
    }
    /// `Set` (registry.go:344-428).
    pub fn set(&mut self) -> Result<(), String> {
        todo!()
    }
    /// `Validate` (registry.go:430-451).
    pub fn validate(&self) -> Vec<String> {
        todo!()
    }
}

/// Parse the three flags, `Set()`, then `Validate()` — the sequence
/// kube-apiserver / kube-controller-manager / kube-scheduler run
/// (`PersistentPreRunE: ComponentGlobalsRegistry.Set()`, then options `Validate`).
pub fn apply_flags(
    _emulated_version: &[String],
    _min_compatibility_version: &[String],
    _feature_gates: &[String],
) -> Result<EffectiveVersion, String> {
    todo!()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::feature_gates::{self, Feature};
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
}
