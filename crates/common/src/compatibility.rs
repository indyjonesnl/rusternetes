//! Port of `k8s.io/component-base/compatibility` (`version.go` effectiveVersion,
//! `registry.go` componentGlobalsRegistry) reduced to the single `kube`
//! component, plus the `k8s.io/apimachinery/pkg/util/version` parsing it uses.
//!
//! Upstream sources (release-1.35):
//! * `staging/src/k8s.io/component-base/compatibility/version.go`
//! * `staging/src/k8s.io/component-base/compatibility/registry.go`
//! * `staging/src/k8s.io/apimachinery/pkg/util/version/version.go`
//! * `staging/src/k8s.io/apiserver/pkg/util/compatibility/version.go`
//!
//! Reduction: rusternetes registers exactly one component (`kube`), so
//! `SetVersionMapping` / `dependentComponentVersion` (aggregated apiservers
//! such as `wardle`) are not ported; every other component name fails with
//! `component not registered: <name>` exactly as an unregistered one does
//! upstream.

use crate::feature_gates::{self, Version};
use std::collections::BTreeMap;

/// `DefaultKubeComponent` (registry.go:34).
pub const DEFAULT_KUBE_COMPONENT: &str = "kube";

/// `DefaultKubeBinaryVersion` (component-base/version/base.go:60-63): the
/// hard-coded binary version. Upstream falls back to it whenever the build has
/// no git tag (apiserver/pkg/util/compatibility/version.go:34-39); rusternetes
/// builds never carry a Kubernetes git tag, so it is always the binary version.
pub const DEFAULT_KUBE_BINARY_VERSION: Version = Version::major_minor(1, 35);

/// `minimumKubeEmulationVersion` (apiserver/pkg/util/compatibility/version.go:26):
/// "the first release emulation version is introduced".
const MINIMUM_KUBE_EMULATION_VERSION: Version = Version::major_minor(1, 31);

/// A parsed `k8s.io/apimachinery/pkg/util/version.Version`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ParsedVersion {
    pub components: Vec<u64>,
    pub pre_release: String,
    pub build_metadata: String,
}

impl ParsedVersion {
    pub fn major(&self) -> u32 {
        u32::try_from(self.components[0]).unwrap_or(u32::MAX)
    }

    pub fn minor(&self) -> u32 {
        u32::try_from(self.components[1]).unwrap_or(u32::MAX)
    }

    /// `Patch` (version.go:224-229): 0 when there is no third component.
    pub fn patch(&self) -> u32 {
        self.components
            .get(2)
            .map_or(0, |p| u32::try_from(*p).unwrap_or(u32::MAX))
    }

    /// `MajorMinor(v.Major(), v.Minor())`.
    pub fn major_minor(&self) -> Version {
        Version::major_minor(self.major(), self.minor())
    }
}

/// `Version.String` (version.go:312-332).
impl std::fmt::Display for ParsedVersion {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let comps: Vec<String> = self.components.iter().map(u64::to_string).collect();
        write!(f, "{}", comps.join("."))?;
        if !self.pre_release.is_empty() {
            write!(f, "-{}", self.pre_release)?;
        }
        if !self.build_metadata.is_empty() {
            write!(f, "+{}", self.build_metadata)?;
        }
        Ok(())
    }
}

/// Go regexp `\s`.
fn is_go_space(c: char) -> bool {
    matches!(c, ' ' | '\t' | '\n' | '\x0c' | '\r')
}

/// `^(?:-(ident(\.ident)*))?(?:\+(ident(\.ident)*))?\s*$` (`extraMatchRE`,
/// version.go:42). Returns (pre-release, build metadata).
fn match_extra(extra: &str) -> Option<(String, String)> {
    fn dotted(s: &str) -> (&str, &str) {
        let end = s
            .find(|c: char| !(c.is_ascii_alphanumeric() || c == '-' || c == '.'))
            .unwrap_or(s.len());
        s.split_at(end)
    }
    fn valid(ident: &str) -> bool {
        !ident.is_empty() && ident.split('.').all(|p| !p.is_empty())
    }
    let mut rest = extra;
    let mut pre = String::new();
    let mut build = String::new();
    if let Some(r) = rest.strip_prefix('-') {
        let (ident, r) = dotted(r);
        if !valid(ident) {
            return None;
        }
        pre = ident.to_string();
        rest = r;
    }
    if let Some(r) = rest.strip_prefix('+') {
        let (ident, r) = dotted(r);
        if !valid(ident) {
            return None;
        }
        build = ident.to_string();
        rest = r;
    }
    rest.chars().all(is_go_space).then_some((pre, build))
}

/// `parse` (version.go:45-90).
fn parse(s: &str, semver: bool) -> Result<ParsedVersion, String> {
    // `^\s*v?([0-9]+(?:\.[0-9]+)*)(.*)*$`
    let t = s.trim_start_matches(is_go_space);
    let t = t.strip_prefix('v').unwrap_or(t);
    let bytes = t.as_bytes();
    let mut i = 0;
    while i < bytes.len() && bytes[i].is_ascii_digit() {
        i += 1;
    }
    if i == 0 || t.contains('\n') {
        return Err(format!("could not parse {s:?} as version"));
    }
    loop {
        if i + 1 < bytes.len() && bytes[i] == b'.' && bytes[i + 1].is_ascii_digit() {
            i += 1;
            while i < bytes.len() && bytes[i].is_ascii_digit() {
                i += 1;
            }
        } else {
            break;
        }
    }
    let (numbers, extra) = t.split_at(i);
    let comps: Vec<&str> = numbers.split('.').collect();
    if (semver && comps.len() != 3) || (!semver && comps.len() < 2) {
        return Err(format!("illegal version string {s:?}"));
    }
    let mut components = Vec::with_capacity(comps.len());
    for (idx, comp) in comps.iter().enumerate() {
        if (idx == 0 || semver) && comp.starts_with('0') && *comp != "0" {
            return Err(format!(
                "illegal zero-prefixed version component {comp:?} in {s:?}"
            ));
        }
        let num: u64 = comp.parse().map_err(|_| {
            format!(
                "illegal non-numeric version component {comp:?} in {s:?}: strconv.ParseUint: parsing {comp:?}: value out of range"
            )
        })?;
        components.push(num);
    }
    let mut v = ParsedVersion {
        components,
        pre_release: String::new(),
        build_metadata: String::new(),
    };
    if semver && !extra.is_empty() {
        let (pre, build) = match_extra(extra).ok_or_else(|| {
            format!("could not parse pre-release/metadata ({extra}) in version {s:?}")
        })?;
        for comp in pre.split('.') {
            if comp.parse::<u64>().is_ok() && comp.starts_with('0') && comp != "0" {
                return Err(format!(
                    "illegal zero-prefixed version component {comp:?} in {s:?}"
                ));
            }
        }
        v.pre_release = pre;
        v.build_metadata = build;
    }
    Ok(v)
}

/// `version.Parse` (version.go:150-156): "tries to do ParseSemantic first to
/// keep more information. If ParseSemantic fails, it would just do ParseGeneric."
pub fn parse_version(s: &str) -> Result<ParsedVersion, String> {
    parse(s, true).or_else(|_| parse(s, false))
}

/// `version.ParseGeneric` (version.go:137-139).
pub fn parse_generic(s: &str) -> Result<ParsedVersion, String> {
    parse(s, false)
}

/// `utilerrors.NewAggregate(errs).Error()` (a single error prints bare,
/// several print as `[a, b]`).
fn aggregate(mut errs: Vec<String>) -> Option<String> {
    match errs.len() {
        0 => None,
        1 => Some(errs.remove(0)),
        _ => Some(format!("[{}]", errs.join(", "))),
    }
}

/// `compatibility.effectiveVersion` (version.go:57-72). Versions are kept at
/// major.minor: upstream stores emulation/min-compatibility as `majorMinor`
/// (version.go:106-111) and `Validate` compares the binary version
/// `WithPatch(0)` (version.go:152), so the binary patch/pre-release never
/// influence behaviour here (only `Info()`, deferred).
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
        binary_version: Version,
        emulation_version_floor: Version,
        min_compatibility_version_floor: Version,
    ) -> Self {
        let mut effective = Self {
            binary_version,
            emulation_version: binary_version,
            min_compatibility_version: binary_version,
            emulation_version_floor,
            min_compatibility_version_floor,
        };
        let compat_version = binary_version.subtract_minor(1);
        effective.set_emulation_version(binary_version);
        effective.set_min_compatibility_version(compat_version);
        effective
    }

    /// `NewEffectiveVersionFromString` (version.go:202-216). An empty binary
    /// version yields an empty `effectiveVersion{}` upstream (nil versions
    /// everywhere); there is no such state here, so it is an error.
    pub fn from_string(
        binary_ver: &str,
        emulation_ver_floor: &str,
        min_compatibility_ver_floor: &str,
    ) -> Result<Self, String> {
        if binary_ver.is_empty() {
            return Err("binary version must not be empty".to_string());
        }
        let binary_version = parse_version(binary_ver)?.major_minor();
        let floor = |s: &str| -> Result<Version, String> {
            if s.is_empty() {
                Ok(Version::major_minor(0, 0))
            } else {
                Ok(parse_version(s)?.major_minor())
            }
        };
        Ok(Self::new(
            binary_version,
            floor(emulation_ver_floor)?,
            floor(min_compatibility_ver_floor)?,
        ))
    }

    pub fn binary_version(&self) -> Version {
        self.binary_version
    }

    pub fn emulation_version(&self) -> Version {
        self.emulation_version
    }

    pub fn min_compatibility_version(&self) -> Version {
        self.min_compatibility_version
    }

    /// `SetEmulationVersion` (version.go:105-114): "set the default
    /// minCompatibilityVersion to be emulationVersion - 1 if possible".
    pub fn set_emulation_version(&mut self, emulation_version: Version) {
        self.emulation_version = emulation_version;
        let mut min_compatibility_version = emulation_version.subtract_minor(1);
        if min_compatibility_version < self.min_compatibility_version_floor {
            min_compatibility_version = self.min_compatibility_version_floor;
        }
        self.min_compatibility_version = min_compatibility_version;
    }

    /// `SetMinCompatibilityVersion` (version.go:116-119): "should be called
    /// after SetEmulationVersion".
    pub fn set_min_compatibility_version(&mut self, min_compatibility_version: Version) {
        self.min_compatibility_version = min_compatibility_version;
    }

    /// `AllowedEmulationVersionRange` (version.go:121-134).
    pub fn allowed_emulation_version_range(&self) -> String {
        format!(
            "{}..{}(default:{})",
            self.emulation_version_floor, self.binary_version, self.emulation_version
        )
    }

    /// `AllowedMinCompatibilityVersionRange` (version.go:136-149).
    pub fn allowed_min_compatibility_version_range(&self) -> String {
        format!(
            "{}..{}(default:{})",
            self.min_compatibility_version_floor,
            self.binary_version,
            self.min_compatibility_version
        )
    }

    /// `Validate` (version.go:151-166).
    pub fn validate(&self) -> Vec<String> {
        let mut errs = Vec::new();
        // emulationVersion can only be between emulationVersionFloor and binaryVersion
        if self.emulation_version > self.binary_version
            || self.emulation_version < self.emulation_version_floor
        {
            errs.push(format!(
                "emulation version {} is not between [{}, {}]",
                self.emulation_version, self.emulation_version_floor, self.binary_version
            ));
        }
        // minCompatibilityVersion can only be between minCompatibilityVersionFloor and emulationVersion
        if self.min_compatibility_version > self.emulation_version
            || self.min_compatibility_version < self.min_compatibility_version_floor
        {
            errs.push(format!(
                "minCompatibilityVersion version {} is not between [{}, {}]",
                self.min_compatibility_version,
                self.min_compatibility_version_floor,
                self.emulation_version
            ));
        }
        errs
    }
}

/// `String` (version.go:90-97).
impl std::fmt::Display for EffectiveVersion {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "{{BinaryVersion: {}, EmulationVersion: {}, MinCompatibilityVersion: {}}}",
            self.binary_version, self.emulation_version, self.min_compatibility_version
        )
    }
}

/// `DefaultBuildEffectiveVersion` + `kubeEffectiveVersionFloors`
/// (apiserver/pkg/util/compatibility/version.go:31-55): "both emulationVersion
/// and minCompatibilityVersion can be set to binaryVersion - 3", never below
/// `minimumKubeEmulationVersion`.
pub fn default_build_effective_version() -> EffectiveVersion {
    let binary_version = DEFAULT_KUBE_BINARY_VERSION;
    let mut version_floor = binary_version.subtract_minor(3);
    if version_floor < MINIMUM_KUBE_EMULATION_VERSION {
        version_floor = MINIMUM_KUBE_EMULATION_VERSION;
    }
    EffectiveVersion::new(binary_version, version_floor, version_floor)
}

/// `toVersionMap` (registry.go:299-326).
pub fn to_version_map(
    version_config: &[String],
) -> Result<BTreeMap<String, ParsedVersion>, String> {
    let mut m = BTreeMap::new();
    for comp_ver in version_config {
        // default to "kube" if component is not specified
        let (k, v) = match comp_ver.split_once('=') {
            Some((k, v)) => (k.trim(), v.trim()),
            None => (DEFAULT_KUBE_COMPONENT, comp_ver.as_str()),
        };
        let ver = parse_version(v)?;
        if ver.patch() != 0 {
            return Err(format!("patch version not allowed, got: {k}={ver}"));
        }
        if let Some(existing) = m.get(k) {
            return Err(format!(
                "duplicate version flag, {k}={existing} and {k}={ver}"
            ));
        }
        m.insert(k.to_string(), ver);
    }
    Ok(m)
}

/// `ColonSeparatedMultimapStringString.Set` with `allowDefaultEmptyKey`
/// (cli/flag/colon_separated_multimap_string_string.go:55-79), folded over
/// every flag occurrence in order: `a:x,b:y,b:z` -> `{a:[x], b:[y, z]}`; an
/// entry without `:` lands under the empty key.
pub fn parse_colon_separated_multimap(
    values: &[String],
) -> Result<BTreeMap<String, Vec<String>>, String> {
    let mut m: BTreeMap<String, Vec<String>> = BTreeMap::new();
    for value in values {
        for pair in value.split(',') {
            if pair.is_empty() {
                continue;
            }
            let (k, v) = match pair.split_once(':') {
                Some((k, v)) => (k.trim(), v.trim()),
                None => ("", pair.trim()),
            };
            m.entry(k.to_string()).or_default().push(v.to_string());
        }
    }
    Ok(m)
}

/// The `kube` slice of `componentGlobalsRegistry` (registry.go:76-111).
/// The `kube` feature gate is the process-wide gate in [`feature_gates`]
/// (`utilfeature.DefaultMutableFeatureGate`, registered under `kube` at
/// apiserver/pkg/util/compatibility/registry.go:51-53).
#[derive(Debug)]
pub struct ComponentGlobalsRegistry {
    effective_version: EffectiveVersion,
    /// `emulationVersionConfig`: flag values, applied by [`Self::set`].
    emulation_version_config: Vec<String>,
    /// `minCompatibilityVersionConfig`.
    min_compatibility_version_config: Vec<String>,
    /// `featureGatesConfig`: component name -> `Gate=bool` values.
    feature_gates_config: BTreeMap<String, Vec<String>>,
}

impl ComponentGlobalsRegistry {
    /// A registry holding `DefaultBuildEffectiveVersion()` under `kube`
    /// (apiserver/pkg/util/compatibility/registry.go:51-53). The gate's
    /// versions are synchronised from it by [`Self::set`].
    pub fn new_default_kube() -> Self {
        Self {
            effective_version: default_build_effective_version(),
            emulation_version_config: Vec::new(),
            min_compatibility_version_config: Vec::new(),
            feature_gates_config: BTreeMap::new(),
        }
    }

    /// `EffectiveVersionFor` (registry.go:150-158): `None` if not registered.
    pub fn effective_version_for(&self, component: &str) -> Option<&EffectiveVersion> {
        (component == DEFAULT_KUBE_COMPONENT).then_some(&self.effective_version)
    }

    /// Record the values of `--emulated-version`, `--min-compatibility-version`
    /// and `--feature-gates` (`AddFlags`, registry.go:226-262). Like repeated
    /// flags, values accumulate; nothing takes effect until [`Self::set`].
    pub fn set_flag_values(
        &mut self,
        emulated_version: &[String],
        min_compatibility_version: &[String],
        feature_gates: &[String],
    ) -> Result<(), String> {
        self.emulation_version_config
            .extend(emulated_version.iter().cloned());
        self.min_compatibility_version_config
            .extend(min_compatibility_version.iter().cloned());
        for (comp, values) in parse_colon_separated_multimap(feature_gates)? {
            self.feature_gates_config
                .entry(comp)
                .or_default()
                .extend(values);
        }
        Ok(())
    }

    /// `unsafeVersionFlagOptions` (registry.go:209-224).
    pub fn version_flag_options(&self, is_emulation: bool) -> Vec<String> {
        let range = if is_emulation {
            self.effective_version.allowed_emulation_version_range()
        } else {
            self.effective_version
                .allowed_min_compatibility_version_range()
        };
        vec![format!("{DEFAULT_KUBE_COMPONENT}={range}")]
    }

    /// `Set` (registry.go:344-428): "we have to enforce [the] order of setting
    /// the emulation version first, then min compatibility version, and last
    /// the feature gate."
    pub fn set(&mut self) -> Result<(), String> {
        let not_registered = |comp: &str| format!("component not registered: {comp}");
        let emulation_version_config_map = to_version_map(&self.emulation_version_config)?;
        for comp in emulation_version_config_map.keys() {
            if comp != DEFAULT_KUBE_COMPONENT {
                return Err(not_registered(comp));
            }
        }
        for ver in emulation_version_config_map.values() {
            self.effective_version
                .set_emulation_version(ver.major_minor());
        }

        let min_compatibility_version_config_map =
            to_version_map(&self.min_compatibility_version_config)?;
        for comp in min_compatibility_version_config_map.keys() {
            if comp != DEFAULT_KUBE_COMPONENT {
                return Err(not_registered(comp));
            }
        }
        for ver in min_compatibility_version_config_map.values() {
            self.effective_version
                .set_min_compatibility_version(ver.major_minor());
        }

        // Set feature gate emulation version before setting feature gate flag values.
        tracing::debug!(
            "setting {DEFAULT_KUBE_COMPONENT}:feature gate emulation version to {}, min compatibility version to {}",
            self.effective_version.emulation_version(),
            self.effective_version.min_compatibility_version()
        );
        feature_gates::set_emulation_version_and_min_compatibility_version(
            self.effective_version.emulation_version(),
            self.effective_version.min_compatibility_version(),
        )?;
        for (comp, fg) in &self.feature_gates_config {
            let mut comp = comp.as_str();
            if comp.is_empty() {
                if self
                    .feature_gates_config
                    .contains_key(DEFAULT_KUBE_COMPONENT)
                {
                    return Err("set kube feature gates with default empty prefix or kube: prefix consistently, do not mix use".to_string());
                }
                comp = DEFAULT_KUBE_COMPONENT;
            }
            if comp != DEFAULT_KUBE_COMPONENT {
                return Err(not_registered(comp));
            }
            let flag_val = fg.join(",");
            tracing::debug!("setting {comp}:feature-gates={flag_val}");
            feature_gates::set_from_str(&flag_val)?;
        }
        Ok(())
    }

    /// `Validate` (registry.go:430-451).
    pub fn validate(&self) -> Vec<String> {
        let mut errs = self.effective_version.validate();
        errs.extend(feature_gates::validate());
        let binary_version = self.effective_version.binary_version();
        let emulated_version = self.effective_version.emulation_version();
        if binary_version > emulated_version {
            let enabled = feature_gates::enabled_alpha_features();
            if !enabled.is_empty() {
                tracing::warn!(
                    "component has alpha features enabled in emulated version, this is unsupported: features={enabled:?}"
                );
            }
        }
        errs
    }
}

/// Parse the three flags, `Set()`, then `Validate()` — the sequence
/// kube-apiserver / kube-controller-manager / kube-scheduler run
/// (`PersistentPreRunE: ComponentGlobalsRegistry.Set()`
/// cmd/kube-apiserver/app/server.go:84-87, then options `Validate` which calls
/// `ComponentGlobalsRegistry.Validate()`).
pub fn apply_flags(
    emulated_version: &[String],
    min_compatibility_version: &[String],
    feature_gates: &[String],
) -> Result<EffectiveVersion, String> {
    let mut registry = ComponentGlobalsRegistry::new_default_kube();
    registry.set_flag_values(emulated_version, min_compatibility_version, feature_gates)?;
    registry.set()?;
    if let Some(e) = aggregate(registry.validate()) {
        return Err(e);
    }
    Ok(registry.effective_version)
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
