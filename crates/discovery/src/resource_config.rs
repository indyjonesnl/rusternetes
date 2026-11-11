//! Port of `--runtime-config` handling:
//! - `staging/src/k8s.io/apiserver/pkg/server/storage/resource_config.go`
//!   (`ResourceConfig`, the `APIResourceConfigSource` the
//!   resource-expiration evaluator reads), and
//! - `staging/src/k8s.io/apiserver/pkg/server/resourceconfig/helpers.go`
//!   (`MergeAPIResourceConfigs`, `ParseGroups`,
//!   `EmulationForwardCompatibleResourceConfig`), plus
//! - `staging/src/k8s.io/apiserver/pkg/server/options/api_enablement.go`
//!   (`APIEnablementOptions.Validate`) and
//!   `staging/src/k8s.io/component-base/cli/flag/configuration_map.go`
//!   (`ConfigurationMap.Set`).
//!
//! Tests are ports of `TestParseRuntimeConfig`,
//! `TestEmulationForwardCompatibleResourceConfig` (helpers_test.go) and
//! `TestDisabledVersion` / `TestDisabledResource` (resource_config_test.go).

use crate::deleted_kinds;
use std::collections::BTreeMap;

/// `schema.GroupVersion` as `(group, version)`.
pub type Gv = (String, String);
/// `schema.GroupVersionResource` as `(group, version, resource)`.
pub type Gvr = (String, String, String);

pub const API_ALL: &str = "api/all";
pub const API_GA: &str = "api/ga";
pub const API_BETA: &str = "api/beta";
pub const API_ALPHA: &str = "api/alpha";
/// `groupVersionMatchersOrder`.
const MATCHERS_ORDER: [&str; 4] = [API_ALL, API_GA, API_BETA, API_ALPHA];

/// `cliflag.ConfigurationMap`.
pub type ConfigurationMap = BTreeMap<String, String>;

/// `ConfigurationMap.Set`: comma separated `key=value`; a bare key has an
/// empty value. Repeated flags accumulate by calling this again.
pub fn parse_configuration_map(value: &str, into: &mut ConfigurationMap) {
    for s in value.split(',') {
        if s.is_empty() {
            continue;
        }
        match s.split_once('=') {
            Some((k, v)) => into.insert(k.trim().to_string(), v.trim().to_string()),
            None => into.insert(s.trim().to_string(), String::new()),
        };
    }
}

fn is_ga(version: &str) -> bool {
    version
        .strip_prefix('v')
        .is_some_and(|d| !d.is_empty() && d.bytes().all(|b| b.is_ascii_digit()))
}

fn is_stage(version: &str, stage: &str) -> bool {
    let Some((major, minor)) = version.strip_prefix('v').and_then(|r| r.split_once(stage)) else {
        return false;
    };
    let digits = |s: &str| !s.is_empty() && s.bytes().all(|b| b.is_ascii_digit());
    digits(major) && digits(minor)
}

pub fn is_alpha(version: &str) -> bool {
    is_stage(version, "alpha")
}

/// `groupVersionMatchers`.
fn matches(flag: &str, version: &str) -> bool {
    match flag {
        API_ALL => true,
        API_GA => is_ga(version),
        API_BETA => is_stage(version, "beta"),
        API_ALPHA => is_alpha(version),
        _ => false,
    }
}

/// `serverstore.ResourceConfig`.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct ResourceConfig {
    pub group_version_configs: BTreeMap<Gv, bool>,
    pub resource_configs: BTreeMap<Gvr, bool>,
    pub explicit_group_version_configs: BTreeMap<Gv, bool>,
    pub explicit_resource_configs: BTreeMap<Gvr, bool>,
}

fn gv(group: &str, version: &str) -> Gv {
    (group.to_string(), version.to_string())
}

impl ResourceConfig {
    /// `NewResourceConfig`.
    pub fn new() -> Self {
        Self::default()
    }

    fn remove_matching_resource_preferences(&mut self, version: &Gv) {
        let matcher = |k: &Gvr| k.0 == version.0 && k.1 == version.1;
        self.resource_configs.retain(|k, _| !matcher(k));
        self.explicit_resource_configs.retain(|k, _| !matcher(k));
    }

    /// `DisableMatchingVersions` (matcher: a `groupVersionMatchers` key).
    pub fn disable_matching_versions(&mut self, flag: &str) {
        self.set_matching(flag, false);
    }

    /// `EnableMatchingVersions`.
    pub fn enable_matching_versions(&mut self, flag: &str) {
        self.set_matching(flag, true);
    }

    fn set_matching(&mut self, flag: &str, enabled: bool) {
        let versions: Vec<Gv> = self
            .group_version_configs
            .keys()
            .filter(|v| matches(flag, &v.1))
            .cloned()
            .collect();
        for v in versions {
            self.group_version_configs.insert(v.clone(), enabled);
            self.remove_matching_resource_preferences(&v);
        }
    }

    /// `DisableVersions`: also drops preferences on the version's resources.
    pub fn disable_versions(&mut self, versions: &[Gv]) {
        for v in versions {
            self.group_version_configs.insert(v.clone(), false);
            self.remove_matching_resource_preferences(v);
        }
    }

    pub fn explicitly_disable_versions(&mut self, versions: &[Gv]) {
        for v in versions {
            self.explicit_group_version_configs.insert(v.clone(), false);
        }
        self.disable_versions(versions);
    }

    /// `EnableVersions`.
    pub fn enable_versions(&mut self, versions: &[Gv]) {
        for v in versions {
            self.group_version_configs.insert(v.clone(), true);
            self.remove_matching_resource_preferences(v);
        }
    }

    pub fn explicitly_enable_versions(&mut self, versions: &[Gv]) {
        for v in versions {
            self.explicit_group_version_configs.insert(v.clone(), true);
        }
        self.enable_versions(versions);
    }

    pub fn disable_resources(&mut self, resources: &[Gvr]) {
        for r in resources {
            self.resource_configs.insert(r.clone(), false);
        }
    }

    pub fn explicitly_disable_resources(&mut self, resources: &[Gvr]) {
        for r in resources {
            self.explicit_resource_configs.insert(r.clone(), false);
        }
        self.disable_resources(resources);
    }

    pub fn enable_resources(&mut self, resources: &[Gvr]) {
        for r in resources {
            self.resource_configs.insert(r.clone(), true);
        }
    }

    pub fn explicitly_enable_resources(&mut self, resources: &[Gvr]) {
        for r in resources {
            self.explicit_resource_configs.insert(r.clone(), true);
        }
        self.enable_resources(resources);
    }

    fn version_enabled(&self, version: &Gv) -> bool {
        self.group_version_configs
            .get(version)
            .copied()
            .unwrap_or(false)
    }

    /// `ResourceEnabled`: an explicit resource setting beats its version's.
    pub fn resource_enabled(&self, group: &str, version: &str, resource: &str) -> bool {
        let key = (group.to_string(), version.to_string(), resource.to_string());
        if let Some(enabled) = self.resource_configs.get(&key) {
            return *enabled;
        }
        self.version_enabled(&gv(group, version))
    }

    /// `AnyResourceForGroupEnabled`.
    pub fn any_resource_for_group_enabled(&self, group: &str) -> bool {
        self.group_version_configs
            .keys()
            .any(|v| v.0 == group && self.version_enabled(v))
            || self
                .resource_configs
                .keys()
                .any(|r| r.0 == group && self.resource_enabled(&r.0, &r.1, &r.2))
    }
}

impl deleted_kinds::ResourceConfig for ResourceConfig {
    /// `ResourceExplicitlyEnabled`.
    fn resource_explicitly_enabled(&self, group: &str, version: &str, resource: &str) -> bool {
        let key = (group.to_string(), version.to_string(), resource.to_string());
        self.explicit_resource_configs
            .get(&key)
            .copied()
            .unwrap_or(false)
    }

    /// `VersionExplicitlyEnabled`.
    fn version_explicitly_enabled(&self, group: &str, version: &str) -> bool {
        self.explicit_group_version_configs
            .get(&gv(group, version))
            .copied()
            .unwrap_or(false)
    }
}

/// `resourceconfig.GroupVersionRegistry`.
pub trait GroupVersionRegistry {
    fn is_group_registered(&self, group: &str) -> bool;
    fn is_version_registered(&self, group: &str, version: &str) -> bool;
    fn prioritized_versions_all_groups(&self) -> Vec<Gv>;
    fn prioritized_versions_for_group(&self, group: &str) -> Vec<Gv>;
}

impl GroupVersionRegistry for k3rs_common::resource_encoding::Scheme {
    fn is_group_registered(&self, group: &str) -> bool {
        Self::is_group_registered(self, group)
    }
    fn is_version_registered(&self, group: &str, version: &str) -> bool {
        Self::is_version_registered(self, group, version)
    }
    fn prioritized_versions_all_groups(&self) -> Vec<Gv> {
        Self::prioritized_versions_all_groups(self)
            .into_iter()
            .map(|g| (g.group, g.version))
            .collect()
    }
    fn prioritized_versions_for_group(&self, group: &str) -> Vec<Gv> {
        Self::prioritized_versions_for_group(self, group)
            .into_iter()
            .map(|g| (g.group, g.version))
            .collect()
    }
}

/// `schema.ParseGroupVersion`.
fn parse_group_version(s: &str) -> Result<Gv, String> {
    if s.is_empty() || s == "/" {
        return Ok((String::new(), String::new()));
    }
    match s.matches('/').count() {
        0 => Ok((String::new(), s.to_string())),
        1 => {
            let (g, v) = s.split_once('/').expect("one slash");
            Ok((g.to_string(), v.to_string()))
        }
        _ => Err(format!("unexpected GroupVersion string: {s}")),
    }
}

/// `getRuntimeConfigValue(overrides, key, false)`.
fn runtime_config_value(overrides: &ConfigurationMap, key: &str) -> Result<bool, String> {
    match overrides.get(key) {
        None => Ok(false),
        Some(v) if v.is_empty() => Ok(true),
        // strconv.ParseBool
        Some(v) => match v.as_str() {
            "1" | "t" | "T" | "TRUE" | "true" | "True" => Ok(true),
            "0" | "f" | "F" | "FALSE" | "false" | "False" => Ok(false),
            _ => Err(format!(
                "invalid value of {key}: {v}, err: strconv.ParseBool: parsing {v:?}: invalid syntax"
            )),
        },
    }
}

/// `MergeAPIResourceConfigs`: apply `api/{all,ga,beta,alpha}` in that order,
/// then the per-version and per-resource preferences (resources last, so they
/// have the highest priority).
pub fn merge_api_resource_configs(
    mut resource_config: ResourceConfig,
    overrides: &ConfigurationMap,
    registry: &dyn GroupVersionRegistry,
) -> Result<ResourceConfig, String> {
    for flag in MATCHERS_ORDER {
        if let Some(value) = overrides.get(flag) {
            match value.as_str() {
                "false" => resource_config.disable_matching_versions(flag),
                "true" => resource_config.enable_matching_versions(flag),
                _ => return Err(format!("invalid value {flag}={value}")),
            }
        }
    }
    apply_version_and_resource_preferences(&mut resource_config, overrides, registry)?;
    Ok(resource_config)
}

/// `applyVersionAndResourcePreferences`.
fn apply_version_and_resource_preferences(
    resource_config: &mut ResourceConfig,
    overrides: &ConfigurationMap,
    registry: &dyn GroupVersionRegistry,
) -> Result<(), String> {
    let mut version_preferences: Vec<(Gv, bool)> = Vec::new();
    let mut resource_preferences: Vec<(Gvr, bool)> = Vec::new();
    for key in overrides.keys() {
        if MATCHERS_ORDER.contains(&key.as_str()) {
            continue;
        }
        let tokens: Vec<&str> = key.split('/').collect();
        if tokens.len() < 2 || tokens.len() > 3 {
            continue;
        }
        let group_version = parse_group_version(&format!("{}/{}", tokens[0], tokens[1]))
            .map_err(|_| format!("invalid key {key}"))?;
        if !registry.is_group_registered(&group_version.0) {
            continue;
        }
        if !registry.is_version_registered(&group_version.0, &group_version.1) {
            return Err(format!(
                "group version {}/{} that has not been registered",
                group_version.0, group_version.1
            ));
        }
        let enabled = runtime_config_value(overrides, key)?;
        if tokens.len() == 2 {
            version_preferences.push((group_version, enabled));
        } else {
            if tokens[2].to_lowercase() != tokens[2] {
                return Err(format!(
                    "invalid key {key}: group/version/resource and resource is always lowercase plural, not {:?}",
                    tokens[2]
                ));
            }
            resource_preferences.push((
                (group_version.0, group_version.1, tokens[2].to_string()),
                enabled,
            ));
        }
    }
    // Version preferences first, so hardcoded resource preferences they
    // override are removed; resource preferences last.
    for (v, enabled) in version_preferences {
        if enabled {
            resource_config.explicitly_enable_versions(&[v]);
        } else {
            resource_config.explicitly_disable_versions(&[v]);
        }
    }
    for (r, enabled) in resource_preferences {
        if enabled {
            resource_config.explicitly_enable_resources(&[r]);
        } else {
            resource_config.explicitly_disable_resources(&[r]);
        }
    }
    Ok(())
}

/// `ParseGroups`.
pub fn parse_groups(resource_config: &ConfigurationMap) -> Result<Vec<String>, String> {
    let mut groups = Vec::new();
    for key in resource_config.keys() {
        if MATCHERS_ORDER.contains(&key.as_str()) {
            continue;
        }
        let tokens: Vec<&str> = key.split('/').collect();
        if tokens.len() != 2 && tokens.len() != 3 {
            return Err(format!("runtime-config invalid key {key}"));
        }
        let group_version = parse_group_version(&format!("{}/{}", tokens[0], tokens[1]))
            .map_err(|_| format!("runtime-config invalid key {key}"))?;
        groups.push(group_version.0);
    }
    Ok(groups)
}

/// `APIEnablementOptions.Validate` for a single registry: refuse `api/all=false`
/// alone and any group the registry does not know.
pub fn validate_runtime_config(
    runtime_config: &ConfigurationMap,
    registry: &dyn GroupVersionRegistry,
) -> Vec<String> {
    if runtime_config.get(API_ALL).map(String::as_str) == Some("false") && runtime_config.len() == 1
    {
        return vec![format!("invalid key with only {API_ALL}=false")];
    }
    let groups = match parse_groups(runtime_config) {
        Ok(g) => g,
        Err(e) => return vec![e],
    };
    let unknown: Vec<String> = groups
        .into_iter()
        .filter(|g| !registry.is_group_registered(g))
        .collect();
    if unknown.is_empty() {
        vec![]
    } else {
        vec![format!("unknown api groups {}", unknown.join(","))]
    }
}

/// `EmulationForwardCompatibleResourceConfig`: besides everything enabled in
/// `resource_config`, enable every higher-priority version of an enabled
/// version/resource (alpha excluded), then re-apply the overrides.
pub fn emulation_forward_compatible_resource_config(
    resource_config: &ResourceConfig,
    overrides: &ConfigurationMap,
    registry: &dyn GroupVersionRegistry,
) -> Result<ResourceConfig, String> {
    let mut ret = ResourceConfig::new();
    let higher_priority = |group: &str, version: &str| -> Vec<Gv> {
        let mut prioritized = registry.prioritized_versions_for_group(group);
        crate::kube_version::sort_by_kube_aware_version_desc(&mut prioritized, |p| &p.1);
        prioritized
            .into_iter()
            .take_while(|p| p.1 != version)
            .collect()
    };
    for (v, enabled) in &resource_config.group_version_configs {
        ret.group_version_configs.insert(v.clone(), *enabled);
        if !*enabled || is_alpha(&v.1) {
            continue;
        }
        ret.enable_versions(&higher_priority(&v.0, &v.1));
    }
    for (r, enabled) in &resource_config.resource_configs {
        ret.resource_configs.insert(r.clone(), *enabled);
        if !*enabled || is_alpha(&r.1) {
            continue;
        }
        let rs: Vec<Gvr> = higher_priority(&r.0, &r.1)
            .into_iter()
            .map(|p| (p.0, p.1, r.2.clone()))
            .collect();
        ret.enable_resources(&rs);
    }
    apply_version_and_resource_preferences(&mut ret, overrides, registry)?;
    Ok(ret)
}

#[cfg(test)]
mod tests {
    use super::*;
    use k3rs_common::resource_encoding::Scheme;

    fn g(group: &str, version: &str) -> Gv {
        gv(group, version)
    }
    fn r(group: &str, version: &str, resource: &str) -> Gvr {
        (group.into(), version.into(), resource.into())
    }
    fn apiv1() -> Gv {
        g("", "v1")
    }
    fn appsv1() -> Gv {
        g("apps", "v1")
    }
    fn ext() -> Gv {
        g("extensions", "v1beta1")
    }

    /// `newFakeScheme`.
    fn fake_scheme() -> Scheme {
        let mut s = Scheme::new();
        s.add_known_type_with_name("", "v1", "Pod", None);
        s.add_known_type_with_name("apps", "v1", "Deployment", None);
        s.add_known_type_with_name("extensions", "v1beta1", "Ingress", None);
        s
    }

    /// `addTestGVs`: priority v2, v1, v2beta1, v2beta2 (v2alpha1 unlisted).
    fn test_scheme() -> Scheme {
        let mut s = fake_scheme();
        for v in ["v2", "v1", "v2beta1", "v2beta2"] {
            s.add_known_type_with_name("test", v, "TestType1", None);
        }
        s.add_known_type_with_name("test", "v2alpha1", "TestType1", None);
        s
    }

    /// `newFakeAPIResourceConfigSource`.
    fn fake_config() -> ResourceConfig {
        let mut c = ResourceConfig::new();
        c.enable_versions(&[apiv1(), appsv1(), ext()]);
        c.enable_resources(&[r("extensions", "v1beta1", "ingresses")]);
        c.disable_resources(&[
            r("extensions", "v1beta1", "deployments"),
            r("extensions", "v1beta1", "replicasets"),
            r("extensions", "v1beta1", "daemonsets"),
        ]);
        c
    }

    fn default_enabled() -> Vec<(Gvr, bool)> {
        vec![
            (r("extensions", "v1beta1", "ingresses"), true),
            (r("extensions", "v1beta1", "deployments"), false),
            (r("extensions", "v1beta1", "replicasets"), false),
            (r("extensions", "v1beta1", "daemonsets"), false),
            (r("apps", "v1", "deployments"), true),
            (r("", "v1", "pods"), true),
        ]
    }

    fn map(pairs: &[(&str, &str)]) -> ConfigurationMap {
        pairs
            .iter()
            .map(|(k, v)| (k.to_string(), v.to_string()))
            .collect()
    }

    fn check(
        name: &str,
        overrides: &[(&str, &str)],
        default: ResourceConfig,
        expected: Option<ResourceConfig>,
        enabled: Vec<(Gvr, bool)>,
    ) {
        let got = merge_api_resource_configs(default, &map(overrides), &fake_scheme());
        let Some(expected) = expected else {
            assert!(got.is_err(), "{name}: expected error");
            return;
        };
        let got = got.unwrap_or_else(|e| panic!("{name}: unexpected error {e}"));
        assert_eq!(got, expected, "{name}");
        for ((gr, v, res), want) in enabled {
            assert_eq!(
                got.resource_enabled(&gr, &v, &res),
                want,
                "{name}: {gr}/{v}/{res}"
            );
        }
    }

    #[test]
    fn parse_runtime_config_cases() {
        // using-kind: a non-lowercase resource token is an error.
        check(
            "using-kind",
            &[("apps/v1/Deployment", "false")],
            fake_config(),
            None,
            vec![],
        );
        check(
            "everything-default-value",
            &[],
            fake_config(),
            Some(fake_config()),
            default_enabled(),
        );

        // version-enabled-by-runtimeConfig-override
        let mut want = fake_config();
        want.explicitly_enable_versions(&[appsv1()]);
        check(
            "version-enabled",
            &[("apps/v1", "")],
            fake_config(),
            Some(want),
            default_enabled(),
        );

        // disable-v1
        let mut want = fake_config();
        want.explicitly_disable_versions(&[apiv1()]);
        let mut en = default_enabled();
        en[5].1 = false;
        check(
            "disable-v1",
            &[("/v1", "false")],
            fake_config(),
            Some(want),
            en,
        );

        // invalid-runtime-config: an unregistered group is skipped.
        check(
            "invalid-runtime-config",
            &[("invalidgroup/version", "false")],
            fake_config(),
            Some(fake_config()),
            default_enabled(),
        );

        // enable-all
        let mut want = fake_config();
        want.enable_versions(
            &fake_scheme()
                .prioritized_versions_all_groups()
                .into_iter()
                .map(|v| (v.group, v.version))
                .collect::<Vec<_>>(),
        );
        let en = default_enabled()
            .into_iter()
            .map(|(k, _)| (k, true))
            .collect();
        check(
            "enable-all",
            &[("api/all", "true")],
            fake_config(),
            Some(want),
            en,
        );

        // an unregistered version of a registered group is an error
        check(
            "unregistered-version",
            &[("apps/v9", "true")],
            fake_config(),
            None,
            vec![],
        );
        // a bad api/ value is an error; a bad bool for a version is an error
        check(
            "bad-api-all",
            &[("api/all", "maybe")],
            fake_config(),
            None,
            vec![],
        );
        check(
            "bad-bool",
            &[("apps/v1", "maybe")],
            fake_config(),
            None,
            vec![],
        );

        // user-explicit-disable-resource-over-user-version-enable
        let mut want = fake_config();
        want.explicitly_enable_versions(&[appsv1()]);
        want.explicitly_disable_resources(&[r("apps", "v1", "deployments")]);
        let mut en = default_enabled();
        en[4].1 = false;
        check(
            "resource-over-version",
            &[("apps/v1", "true"), ("apps/v1/deployments", "false")],
            fake_config(),
            Some(want),
            en,
        );

        // user-explicit-enable-resource-over-user-version-disable-over-user-stability-disable
        let mut want = fake_config();
        want.disable_versions(&[apiv1()]);
        want.explicitly_disable_versions(&[appsv1()]);
        want.explicitly_enable_resources(&[r("apps", "v1", "deployments")]);
        let en = vec![
            (r("extensions", "v1beta1", "ingresses"), true),
            (r("apps", "v1", "deployments"), true),
            (r("", "v1", "pods"), false),
        ];
        check(
            "resource-enable-over-version-disable-over-stability-disable",
            &[
                ("api/ga", "false"),
                ("apps/v1", "false"),
                ("apps/v1/deployments", "true"),
            ],
            fake_config(),
            Some(want),
            en,
        );
    }

    #[test]
    fn explicit_flags_feed_the_evaluator() {
        use crate::deleted_kinds::ResourceConfig as _;
        let c = merge_api_resource_configs(
            fake_config(),
            &map(&[
                ("apps/v1", "true"),
                ("extensions/v1beta1/ingresses", "true"),
            ]),
            &fake_scheme(),
        )
        .unwrap();
        assert!(c.version_explicitly_enabled("apps", "v1"));
        assert!(c.resource_explicitly_enabled("extensions", "v1beta1", "ingresses"));
        assert!(!c.version_explicitly_enabled("", "v1"));
        // The defaults alone are never "explicit".
        assert!(!fake_config().version_explicitly_enabled("apps", "v1"));
    }

    #[test]
    fn emulation_forward_compatible_cases() {
        let s = test_scheme();
        let tv = |v: &str| g("test", v);
        let tr = |v: &str| r("test", v, "testtype1");

        let run = |cfg: ResourceConfig, o: &[(&str, &str)]| {
            emulation_forward_compatible_resource_config(&cfg, &map(o), &s).unwrap()
        };

        // no higher priority
        let mut c = ResourceConfig::new();
        c.enable_versions(&[tv("v2")]);
        assert_eq!(run(c.clone(), &[]), c);
        // alpha is never expanded
        let mut c = ResourceConfig::new();
        c.enable_versions(&[tv("v2alpha1")]);
        assert_eq!(run(c.clone(), &[]), c);
        // higher priority GA
        let mut c = ResourceConfig::new();
        c.enable_versions(&[tv("v1")]);
        let mut want = ResourceConfig::new();
        want.enable_versions(&[tv("v1"), tv("v2")]);
        assert_eq!(run(c, &[]), want);
        // higher priority from beta
        let mut c = ResourceConfig::new();
        c.enable_versions(&[tv("v2beta1")]);
        let mut want = ResourceConfig::new();
        want.enable_versions(&[tv("v2beta1"), tv("v2beta2"), tv("v1"), tv("v2")]);
        assert_eq!(run(c, &[]), want);
        // ... with an override re-applied
        let mut c = ResourceConfig::new();
        c.enable_versions(&[tv("v2beta1")]);
        let mut want = ResourceConfig::new();
        want.enable_versions(&[tv("v2beta1"), tv("v1"), tv("v2")]);
        want.explicitly_disable_versions(&[tv("v2beta2")]);
        assert_eq!(run(c, &[("test/v2beta2", "false")]), want);
        // resources: higher priority and alpha
        let mut c = ResourceConfig::new();
        c.enable_resources(&[tr("v2beta1")]);
        let mut want = ResourceConfig::new();
        want.enable_resources(&[tr("v2beta1"), tr("v2beta2"), tr("v1"), tr("v2")]);
        assert_eq!(run(c, &[]), want);
        let mut c = ResourceConfig::new();
        c.enable_resources(&[tr("v2alpha1")]);
        assert_eq!(run(c.clone(), &[]), c);
        // resources with an override
        let mut c = ResourceConfig::new();
        c.enable_resources(&[tr("v2beta1")]);
        let mut want = ResourceConfig::new();
        want.enable_resources(&[tr("v2beta1"), tr("v1"), tr("v2")]);
        want.explicitly_disable_resources(&[tr("v2beta2")]);
        assert_eq!(run(c, &[("test/v2beta2/testtype1", "false")]), want);
    }

    /// resource_config_test.go `TestDisabledVersion` / `TestDisabledResource`.
    #[test]
    fn disabled_version_and_resource() {
        let mut c = ResourceConfig::new();
        c.enable_versions(&[g("g", "v1"), g("g", "v2")]);
        c.disable_versions(&[g("g", "v2")]);
        assert!(c.resource_enabled("g", "v1", "x"));
        assert!(!c.resource_enabled("g", "v2", "x"));
        assert!(c.any_resource_for_group_enabled("g"));
        c.enable_resources(&[r("g", "v2", "x")]);
        assert!(c.resource_enabled("g", "v2", "x"));
        // A version preference drops earlier resource preferences.
        c.disable_versions(&[g("g", "v2")]);
        assert!(!c.resource_enabled("g", "v2", "x"));
    }

    #[test]
    fn configuration_map_and_validation() {
        let mut m = ConfigurationMap::new();
        parse_configuration_map("apps/v1=true, api/beta=false,,extensions/v1beta1", &mut m);
        assert_eq!(m["apps/v1"], "true");
        assert_eq!(m["api/beta"], "false");
        assert_eq!(m["extensions/v1beta1"], "");
        let s = fake_scheme();
        assert_eq!(
            validate_runtime_config(&map(&[("api/all", "false")]), &s),
            vec!["invalid key with only api/all=false"]
        );
        assert_eq!(
            validate_runtime_config(&map(&[("nope/v1", "true")]), &s),
            vec!["unknown api groups nope"]
        );
        assert!(
            validate_runtime_config(&map(&[("apps/v1", "true"), ("api/ga", "false")]), &s)
                .is_empty()
        );
        assert!(!validate_runtime_config(&map(&[("apps", "true")]), &s).is_empty());
    }
}
