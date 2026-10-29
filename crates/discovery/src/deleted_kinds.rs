//! Port of `resourceExpirationEvaluator`
//! (`staging/src/k8s.io/apiserver/pkg/server/deleted_kinds.go`, release-1.35):
//! which resources a server at an effective (emulation) version serves, given
//! each kind's `APILifecycleIntroduced` / `APILifecycleRemoved`.
//!
//! Upstream mutates `APIGroupInfo.VersionedResourcesStorageMap`
//! (`version -> resource -> rest.Storage`) in `RemoveUnavailableKinds`, called
//! from `InstallAPIGroups` with the current version of the generic server's
//! `EffectiveVersion` (`genericapiserver.go` `InstallAPIGroups`; the
//! evaluator is built in `config.go` `NewResourceExpirationEvaluatorFromOptions`
//! from `c.EffectiveVersion.EmulationVersion()`).
//!
//! Reduction: there is no `rest.Storage` or `runtime.ObjectVersioner` to ask
//! for the versioned object, so the map carries the two lifecycle facts the
//! evaluator reads from it (`introducedInterface` / `removedInterface`) as
//! data: [`Lifecycle`]. `APIResourceConfigSource` is the [`ResourceConfig`]
//! trait. Tests are ports of `Test_newResourceExpirationEvaluator`,
//! `Test_removeDeletedKinds`, `Test_removeUnIntroducedKinds`,
//! `Test_RemoveUnavailableKinds` and `Test_shouldRemoveResource`.

use crate::kube_version::compare_kube_aware_version_strings;
use rusternetes_common::feature_gates::Version;
use std::collections::{BTreeMap, BTreeSet};

/// The lifecycle facts upstream reads off a versioned object. `None` means the
/// type has no such method (`versionedPtr.(introducedInterface)` fails).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct Lifecycle {
    pub introduced: Option<(u32, u32)>,
    pub removed: Option<(u32, u32)>,
}

/// `version -> resource -> lifecycle`, mirroring `APIGroupInfo`'s
/// `VersionedResourcesStorageMap`. Subresources are `parent/sub` keys.
pub type VersionedResources = BTreeMap<String, BTreeMap<String, Lifecycle>>;

/// The two `APIResourceConfigSource` methods the evaluator calls
/// (`storage/resource_config.go`).
pub trait ResourceConfig {
    fn resource_explicitly_enabled(&self, group: &str, version: &str, resource: &str) -> bool;
    fn version_explicitly_enabled(&self, group: &str, version: &str) -> bool;
}

/// `ResourceExpirationEvaluatorOptions` (deleted_kinds.go:73-86).
#[derive(Debug, Clone, Default)]
pub struct Options {
    pub current_version: Option<Version>,
    pub prerelease: String,
    pub emulation_forward_compatible: bool,
    pub runtime_config_emulation_forward_compatible: bool,
}

#[derive(Debug, Clone)]
pub struct ResourceExpirationEvaluator {
    pub current_version: Version,
    pub emulation_forward_compatible: bool,
    pub runtime_config_emulation_forward_compatible: bool,
    pub is_alpha: bool,
    pub is_alpha_zero: bool,
    pub strict_removed_handling_in_alpha: bool,
    pub serve_removed_apis_one_more_release: bool,
}

fn env_bool(name: &str) -> Result<Option<bool>, String> {
    match std::env::var(name) {
        Err(_) => Ok(None),
        Ok(s) => match s.as_str() {
            "1" | "t" | "T" | "true" | "TRUE" | "True" => Ok(Some(true)),
            "0" | "f" | "F" | "false" | "FALSE" | "False" => Ok(Some(false)),
            _ => Err(format!("strconv.ParseBool: parsing {s:?}: invalid syntax")),
        },
    }
}

impl ResourceExpirationEvaluator {
    /// `NewResourceExpirationEvaluator` (:88-95).
    pub fn new(current_version: Version, prerelease: &str) -> Result<Self, String> {
        Self::from_options(Options {
            current_version: Some(current_version),
            prerelease: prerelease.to_string(),
            ..Options::default()
        })
    }

    /// `NewResourceExpirationEvaluatorFromOptions` (:97-137).
    pub fn from_options(opts: Options) -> Result<Self, String> {
        let current_version = opts
            .current_version
            .ok_or("empty NewResourceExpirationEvaluator currentVersion")?;
        let mut ret = Self {
            current_version,
            emulation_forward_compatible: opts.emulation_forward_compatible,
            runtime_config_emulation_forward_compatible: opts
                .runtime_config_emulation_forward_compatible,
            is_alpha: opts.prerelease.contains("alpha"),
            is_alpha_zero: opts.prerelease.contains("alpha.0"),
            strict_removed_handling_in_alpha: false,
            serve_removed_apis_one_more_release: false,
        };
        if let Some(b) = env_bool("KUBE_APISERVER_STRICT_REMOVED_API_HANDLING_IN_ALPHA")? {
            ret.strict_removed_handling_in_alpha = b;
        }
        if let Some(b) = env_bool("KUBE_APISERVER_SERVE_REMOVED_APIS_FOR_ONE_RELEASE")? {
            ret.serve_removed_apis_one_more_release = b;
        }
        Ok(ret)
    }

    /// `ShouldServeForVersion` (:170-190).
    pub fn should_serve_for_version(&self, major_removed: u32, minor_removed: u32) -> bool {
        let removed = Version::major_minor(major_removed, minor_removed);
        if removed > self.current_version {
            return true;
        }
        if removed < self.current_version {
            return false;
        }
        // major and minor are equal: removed when the current release GAs.
        if self.is_alpha && !self.is_alpha_zero && self.strict_removed_handling_in_alpha {
            return false;
        }
        if self.is_alpha {
            return true;
        }
        self.serve_removed_apis_one_more_release
    }

    /// `isNotRemoved` (:140-168).
    fn is_not_removed(&self, lifecycle: &Lifecycle) -> bool {
        match lifecycle.removed {
            None => true,
            Some((major, minor)) => self.should_serve_for_version(major, minor),
        }
    }

    /// `removeDeletedKinds` (:197-233).
    pub fn remove_deleted_kinds(&self, resources: &mut VersionedResources) {
        let mut versions_to_remove = BTreeSet::new();
        for (api_version, version_to_resource) in resources.iter_mut() {
            let to_remove: BTreeSet<String> = version_to_resource
                .iter()
                .filter(|(_, lc)| !self.is_not_removed(lc))
                .map(|(name, _)| name.clone())
                .collect();
            version_to_resource
                .retain(|name, _| !should_remove_resource_and_subresources(&to_remove, name));
            if version_to_resource.is_empty() {
                versions_to_remove.insert(api_version.clone());
            }
        }
        for v in versions_to_remove {
            resources.remove(&v);
        }
    }

    /// `RemoveUnavailableKinds` (:235-238).
    pub fn remove_unavailable_kinds(
        &self,
        group: &str,
        prioritized_versions: &[String],
        resources: &mut VersionedResources,
        config: Option<&dyn ResourceConfig>,
    ) -> Result<(), String> {
        self.remove_deleted_kinds(resources);
        self.remove_unintroduced_kinds(group, prioritized_versions, resources, config)
    }

    /// `removeUnintroducedKinds` (:240-318).
    pub fn remove_unintroduced_kinds(
        &self,
        group: &str,
        prioritized_versions: &[String],
        resources: &mut VersionedResources,
        config: Option<&dyn ResourceConfig>,
    ) -> Result<(), String> {
        let mut versions_to_remove = BTreeSet::new();
        let mut prioritized: Vec<&String> = prioritized_versions.iter().collect();
        prioritized.sort_by(|a, b| compare_kube_aware_version_strings(b, a).cmp(&0));
        let mut enabled_resources: BTreeSet<String> = BTreeSet::new();

        // Iterate from the end to the front, so the lower priority versions
        // are removed first.
        for api_version in prioritized.iter().rev() {
            let Some(version_to_resource) = resources.get_mut(api_version.as_str()) else {
                continue;
            };
            if version_to_resource.is_empty() {
                continue;
            }
            let mut to_remove = BTreeSet::new();
            for (name, lifecycle) in version_to_resource.iter() {
                // Whether the same resource at a lower priority version has
                // already been kept: emulation forward compatibility for this
                // version is decided from that.
                let lower_priority_enabled = enabled_resources.contains(name);
                let keep = self.should_serve_based_on_version_introduced(
                    group,
                    api_version,
                    name,
                    lifecycle,
                    config,
                    lower_priority_enabled,
                )?;
                if !keep {
                    to_remove.insert(name.clone());
                } else if !is_alpha_version(api_version) {
                    // Emulation forward compatibility is not applicable to
                    // alpha APIs, so they never enable other versions.
                    enabled_resources.insert(name.clone());
                }
            }
            version_to_resource
                .retain(|name, _| !should_remove_resource_and_subresources(&to_remove, name));
            if version_to_resource.is_empty() {
                versions_to_remove.insert((*api_version).clone());
            }
        }

        for api_version in versions_to_remove {
            if config.is_some_and(|c| c.version_explicitly_enabled(group, &api_version)) {
                return Err(format!(
                    "cannot enable version {group}/{api_version} in runtime-config because all the resources have been introduced after the current version {}. Consider setting --runtime-config-emulation-forward-compatible=true",
                    self.current_version
                ));
            }
            resources.remove(&api_version);
        }
        Ok(())
    }

    /// `shouldServeBasedOnVersionIntroduced` (:320-378).
    fn should_serve_based_on_version_introduced(
        &self,
        group: &str,
        version: &str,
        resource: &str,
        lifecycle: &Lifecycle,
        config: Option<&dyn ResourceConfig>,
        lower_priority_enabled: bool,
    ) -> Result<bool, String> {
        let ver_introduced = lifecycle
            .introduced
            .map_or(Version::major_minor(0, 0), |(major, minor)| {
                Version::major_minor(major, minor)
            });
        // Serve resources introduced at or before the current version.
        if self.current_version >= ver_introduced {
            return Ok(true);
        }
        // The rest decides whether a resource introduced after the current
        // version is served (emulation mode only).
        if self.emulation_forward_compatible && lower_priority_enabled {
            return Ok(true);
        }
        let Some(config) = config else {
            return Ok(false);
        };
        // Future resources may be explicitly enabled in runtime-config in
        // forward compatible mode.
        if self.runtime_config_emulation_forward_compatible
            && (config.resource_explicitly_enabled(group, version, resource)
                || config.version_explicitly_enabled(group, version))
        {
            return Ok(true);
        }
        // A future resource explicitly enabled in runtime-config without
        // runtimeConfigEmulationForwardCompatible is an error.
        if config.resource_explicitly_enabled(group, version, resource) {
            return Err(format!(
                "cannot enable resource {group}/{version}, Resource={resource} in runtime-config because it is introduced at {ver_introduced} after the current version {}. Consider setting --runtime-config-emulation-forward-compatible=true",
                self.current_version
            ));
        }
        Ok(false)
    }
}

/// `alphaPattern` (`^v\d+alpha\d+$`).
fn is_alpha_version(version: &str) -> bool {
    let Some(rest) = version.strip_prefix('v') else {
        return false;
    };
    let Some((major, minor)) = rest.split_once("alpha") else {
        return false;
    };
    let digits = |s: &str| !s.is_empty() && s.bytes().all(|b| b.is_ascii_digit());
    digits(major) && digits(minor)
}

/// `shouldRemoveResourceAndSubresources` (:338-350).
pub fn should_remove_resource_and_subresources(
    resources_to_remove: &BTreeSet<String>,
    resource_name: &str,
) -> bool {
    resources_to_remove.iter().any(|r| {
        resource_name == r
            || resource_name
                .strip_prefix(r.as_str())
                .is_some_and(|rest| rest.starts_with('/'))
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::BTreeSet;

    const GROUP: &str = "group.name";

    fn lc(i: (u32, u32), r: Option<(u32, u32)>) -> Lifecycle {
        Lifecycle {
            introduced: Some(i),
            removed: r,
        }
    }

    fn ev(minor: u32) -> ResourceExpirationEvaluator {
        ResourceExpirationEvaluator {
            current_version: Version::major_minor(1, minor),
            emulation_forward_compatible: false,
            runtime_config_emulation_forward_compatible: false,
            is_alpha: false,
            is_alpha_zero: false,
            strict_removed_handling_in_alpha: false,
            serve_removed_apis_one_more_release: false,
        }
    }

    /// `runtimeConfig` map of the upstream tests: `group/version` and
    /// `group/version/resource` keys.
    struct Rc(BTreeSet<String>);
    impl ResourceConfig for Rc {
        fn resource_explicitly_enabled(&self, g: &str, v: &str, r: &str) -> bool {
            self.0.contains(&format!("{g}/{v}/{r}"))
        }
        fn version_explicitly_enabled(&self, g: &str, v: &str) -> bool {
            self.0.contains(&format!("{g}/{v}"))
        }
    }
    fn rc(keys: &[&str]) -> Rc {
        Rc(keys.iter().map(|k| k.to_string()).collect())
    }

    fn map(entries: &[(&str, &[(&str, Lifecycle)])]) -> VersionedResources {
        entries
            .iter()
            .map(|(v, rs)| {
                (
                    v.to_string(),
                    rs.iter().map(|(n, l)| (n.to_string(), *l)).collect(),
                )
            })
            .collect()
    }

    fn versions() -> Vec<String> {
        ["v1", "v2alpha1", "v2beta1", "v2beta2", "v2"]
            .map(String::from)
            .to_vec()
    }

    /// The input shared by most `Test_removeUnIntroducedKinds` cases.
    fn full_map(v1_introduced: (u32, u32)) -> VersionedResources {
        map(&[
            ("v1", &[("resource1", lc(v1_introduced, None))]),
            ("v2alpha1", &[("resource1", lc((1, 20), Some((1, 21))))]),
            ("v2beta1", &[("resource1", lc((1, 21), Some((1, 22))))]),
            (
                "v2beta2",
                &[
                    ("resource1", lc((1, 22), Some((1, 23)))),
                    ("resource2", lc((1, 22), Some((1, 23)))),
                ],
            ),
            (
                "v2",
                &[
                    ("resource1", lc((1, 23), None)),
                    ("resource2", lc((1, 23), None)),
                ],
            ),
        ])
    }

    fn served(m: &VersionedResources) -> Vec<String> {
        m.iter()
            .flat_map(|(v, rs)| rs.keys().map(move |r| format!("{v}/{r}")))
            .collect()
    }

    #[test]
    fn remove_future_version() {
        let mut m = full_map((1, 18));
        ev(21)
            .remove_unintroduced_kinds(
                GROUP,
                &versions(),
                &mut m,
                Some(&rc(&["group.name/v2beta1"])),
            )
            .unwrap();
        assert_eq!(
            served(&m),
            ["v1/resource1", "v2alpha1/resource1", "v2beta1/resource1"]
        );
    }

    #[test]
    fn missing_introduced_version_is_served() {
        let removed_23 = Lifecycle {
            introduced: None,
            removed: Some((1, 23)),
        };
        let mut m = map(&[
            ("v1", &[("resource1", removed_23)]),
            (
                "v2",
                &[
                    ("resource1", lc((1, 23), None)),
                    ("resource2", lc((1, 23), None)),
                ],
            ),
        ]);
        ev(20)
            .remove_unintroduced_kinds(GROUP, &versions(), &mut m, None)
            .unwrap();
        assert_eq!(served(&m), ["v1/resource1"]);
    }

    #[test]
    fn emulation_forward_compatible_ga_api() {
        let mut e = ev(19);
        e.emulation_forward_compatible = true;
        let mut m = full_map((1, 18));
        e.remove_unintroduced_kinds(GROUP, &versions(), &mut m, None)
            .unwrap();
        assert_eq!(served(&m), ["v1/resource1", "v2/resource1"]);
    }

    #[test]
    fn emulation_forward_compatible_alpha_api() {
        let mut e = ev(20);
        e.emulation_forward_compatible = true;
        let mut m = full_map((1, 18));
        e.remove_unintroduced_kinds(GROUP, &versions(), &mut m, None)
            .unwrap();
        assert_eq!(
            served(&m),
            ["v1/resource1", "v2/resource1", "v2alpha1/resource1"]
        );
    }

    #[test]
    fn emulation_forward_compatible_new_resource_is_not_served() {
        let mut e = ev(21);
        e.emulation_forward_compatible = true;
        let mut m = full_map((1, 20));
        e.remove_unintroduced_kinds(GROUP, &versions(), &mut m, None)
            .unwrap();
        // resource2 has no lower-priority served version, so it stays out.
        assert!(!served(&m).iter().any(|s| s.ends_with("resource2")));
        assert!(served(&m).contains(&"v2/resource1".to_string()));
    }

    #[test]
    fn runtime_config_enabling_a_future_version_or_resource_errors() {
        for key in ["group.name/v2beta2", "group.name/v2beta2/resource2"] {
            let mut m = full_map((1, 20));
            let err = ev(20)
                .remove_unintroduced_kinds(GROUP, &versions(), &mut m, Some(&rc(&[key])))
                .unwrap_err();
            assert!(
                err.contains("--runtime-config-emulation-forward-compatible"),
                "{err}"
            );
        }
    }

    #[test]
    fn runtime_config_emulation_forward_compatible_enables_version() {
        let mut e = ev(20);
        e.runtime_config_emulation_forward_compatible = true;
        let mut m = full_map((1, 20));
        e.remove_unintroduced_kinds(
            GROUP,
            &versions(),
            &mut m,
            Some(&rc(&["group.name/v2beta2"])),
        )
        .unwrap();
        assert_eq!(
            served(&m),
            [
                "v1/resource1",
                "v2alpha1/resource1",
                "v2beta2/resource1",
                "v2beta2/resource2"
            ]
        );
    }

    #[test]
    fn runtime_config_emulation_forward_compatible_enables_resource() {
        let mut e = ev(20);
        e.runtime_config_emulation_forward_compatible = true;
        let mut m = full_map((1, 20));
        e.remove_unintroduced_kinds(
            GROUP,
            &versions(),
            &mut m,
            Some(&rc(&["group.name/v2beta2/resource2"])),
        )
        .unwrap();
        assert_eq!(
            served(&m),
            ["v1/resource1", "v2alpha1/resource1", "v2beta2/resource2"]
        );
    }

    #[test]
    fn subresources_follow_their_parent() {
        let mut m = map(&[(
            "v1",
            &[
                ("things", lc((1, 36), None)),
                ("things/status", lc((1, 36), None)),
                ("things/scale", lc((1, 2), None)),
                ("other", lc((1, 2), None)),
            ],
        )]);
        ev(35)
            .remove_unintroduced_kinds(GROUP, &["v1".to_string()], &mut m, None)
            .unwrap();
        assert_eq!(served(&m), ["v1/other"]);
    }

    #[test]
    fn remove_deleted_kinds_drops_removed_and_empty_versions() {
        let mut m = full_map((1, 18));
        m.get_mut("v1")
            .unwrap()
            .insert("resource1".into(), lc((1, 18), Some((1, 20))));
        ev(21).remove_deleted_kinds(&mut m);
        // removed at or before 1.21 is gone; v1 and v2alpha1/v2beta1 (removed
        // 1.21 / 1.22) follow `ShouldServeForVersion`.
        assert_eq!(
            served(&m),
            [
                "v2/resource1",
                "v2/resource2",
                "v2beta1/resource1",
                "v2beta2/resource1",
                "v2beta2/resource2"
            ]
        );
        assert!(!m.contains_key("v1") && !m.contains_key("v2alpha1"));
    }

    #[test]
    fn should_serve_for_version_alpha_and_env_handling() {
        let mut e = ev(21);
        assert!(e.should_serve_for_version(1, 22));
        assert!(!e.should_serve_for_version(1, 20));
        assert!(!e.should_serve_for_version(1, 21));
        e.is_alpha = true;
        assert!(e.should_serve_for_version(1, 21));
        e.strict_removed_handling_in_alpha = true;
        assert!(!e.should_serve_for_version(1, 21));
        e.is_alpha_zero = true;
        assert!(e.should_serve_for_version(1, 21));
        let mut e = ev(21);
        e.serve_removed_apis_one_more_release = true;
        assert!(e.should_serve_for_version(1, 21));
        assert!(ResourceExpirationEvaluator::from_options(Options::default()).is_err());
    }

    #[test]
    fn should_remove_resource_and_subresources_matches_upstream() {
        let foo: BTreeSet<String> = ["foo".to_string()].into();
        assert!(should_remove_resource_and_subresources(&foo, "foo/scale"));
        assert!(should_remove_resource_and_subresources(&foo, "foo"));
        assert!(!should_remove_resource_and_subresources(&foo, "bar"));
        assert!(!should_remove_resource_and_subresources(&foo, "foobar"));
    }

    #[test]
    fn alpha_pattern() {
        assert!(is_alpha_version("v2alpha1"));
        assert!(!is_alpha_version("v2beta1"));
        assert!(!is_alpha_version("v1"));
    }
}
