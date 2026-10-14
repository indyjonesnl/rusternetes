//! Port of `DefaultResourceEncodingConfig` and the storage-version selection
//! that hangs off it (#3408).
//!
//! Upstream sources (release-1.35):
//! * `staging/src/k8s.io/apiserver/pkg/server/storage/resource_encoding_config.go`
//!   (`NewDefaultResourceEncodingConfigForEffectiveVersion` :61-63,
//!   `StorageEncodingFor` :76-88, `BackwardCompatibileStorageEncodingFor`
//!   :90-106, `InMemoryEncodingFor` :108-118, `emulatedStorageVersion`
//!   :125-205)
//! * `staging/src/k8s.io/apiserver/pkg/server/storage/storage_factory.go`
//!   (`DefaultStorageFactory.NewConfig` :229-271, the caller that picks
//!   `BackwardCompatibileStorageEncodingFor` over `StorageEncodingFor`)
//! * `staging/src/k8s.io/apiserver/pkg/server/resourceconfig/helpers.go:45-56`
//!   (`MergeResourceEncodingConfigs`)
//! * `pkg/kubeapiserver/default_storage_factory_builder.go:71-100`
//!   (`NewStorageFactoryConfigEffectiveVersion`, the `ResourceEncodingOverrides`)
//! * `staging/src/k8s.io/apiserver/pkg/server/storage/storage_factory_test.go`
//!   (`TestStorageFactoryCompatibilityVersion`)
//!
//! Reduction: upstream asks a `runtime.Scheme` about registered kinds and the
//! `APILifecycleIntroduced()` method generated into each versioned type by
//! `prerelease-lifecycle-gen`. Rusternetes has no Go scheme, so [`Scheme`]
//! carries the same facts as data: the group version priority order from each
//! `pkg/apis/<group>/install/install.go` `SetVersionPriority`, the kinds each
//! `staging/src/k8s.io/api/<group>/<version>/register.go` registers, and the
//! `introduced` version from each `zz_generated.prerelease-lifecycle.go`
//! (see [`scheme_table`]).

mod scheme_table;

use crate::compatibility::{current_effective_version, EffectiveVersion};
use crate::feature_gates::Version;

use std::collections::BTreeMap;

/// `runtime.APIVersionInternal`.
pub const API_VERSION_INTERNAL: &str = "__internal";

/// `schema.GroupVersion`.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Default)]
pub struct GroupVersion {
    pub group: String,
    pub version: String,
}

impl GroupVersion {
    pub fn new(group: &str, version: &str) -> Self {
        Self {
            group: group.to_string(),
            version: version.to_string(),
        }
    }
}

/// `schema.GroupResource`.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Default)]
pub struct GroupResource {
    pub group: String,
    pub resource: String,
}

impl GroupResource {
    pub fn new(group: &str, resource: &str) -> Self {
        Self {
            group: group.to_string(),
            resource: resource.to_string(),
        }
    }
}

/// `schema.GroupVersionResource`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GroupVersionResource {
    pub group: String,
    pub version: String,
    pub resource: String,
}

impl GroupVersionResource {
    pub fn new(group: &str, version: &str, resource: &str) -> Self {
        Self {
            group: group.to_string(),
            version: version.to_string(),
            resource: resource.to_string(),
        }
    }

    pub fn group_resource(&self) -> GroupResource {
        GroupResource::new(&self.group, &self.resource)
    }

    pub fn group_version(&self) -> GroupVersion {
        GroupVersion::new(&self.group, &self.version)
    }
}

/// The `runtime.Object` example handed to `emulatedStorageVersion`: its Go
/// type is identified here by the kind the scheme knows it under.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Example {
    pub group: String,
    pub kind: String,
}

impl Example {
    pub fn new(group: &str, kind: &str) -> Self {
        Self {
            group: group.to_string(),
            kind: kind.to_string(),
        }
    }
}

#[derive(Debug, Clone, Default)]
struct VersionEntry {
    version: String,
    /// kind -> `APILifecycleIntroduced()` (`None` = the type has no such
    /// method, i.e. `exampleOfGVK.(introducedInterface)` fails).
    kinds: Vec<(String, Option<(u32, u32)>)>,
}

#[derive(Debug, Clone, Default)]
struct GroupEntry {
    /// Priority order (`PrioritizedVersionsForGroup`); the internal version is
    /// registered but never listed here, as `SetVersionPriority` leaves it out.
    versions: Vec<VersionEntry>,
    /// Kinds registered in the internal version.
    internal_kinds: Vec<String>,
}

/// `runtime.Scheme`, reduced to the queries `DefaultResourceEncodingConfig`
/// makes: `IsGroupRegistered`, `PrioritizedVersionsForGroup`, `ObjectKinds`,
/// `VersionsForGroupKind` and `New`.
#[derive(Debug, Clone, Default)]
pub struct Scheme {
    groups: BTreeMap<String, GroupEntry>,
}

impl Scheme {
    pub fn new() -> Self {
        Self::default()
    }

    /// `legacyscheme.Scheme` as populated by `pkg/apis/*/install` in
    /// release-1.35.
    pub fn legacy() -> Self {
        let mut scheme = Self::new();
        for group in scheme_table::SCHEME_TABLE {
            for version in group.versions {
                for (kind, introduced) in version.kinds {
                    scheme.add_known_type_with_name(
                        group.group,
                        version.version,
                        kind,
                        *introduced,
                    );
                }
            }
        }
        scheme
    }

    /// `AddKnownTypeWithName` plus the internal-version registration done by
    /// `registerFakeLifecycle` (storage_factory_test.go:395-405). Calls are in
    /// priority order, as the upstream test's "Order dictates priority order".
    pub fn add_known_type_with_name(
        &mut self,
        group: &str,
        version: &str,
        kind: &str,
        introduced: Option<(u32, u32)>,
    ) {
        let entry = self.groups.entry(group.to_string()).or_default();
        match entry.versions.iter_mut().find(|v| v.version == version) {
            Some(v) => v.kinds.push((kind.to_string(), introduced)),
            None => entry.versions.push(VersionEntry {
                version: version.to_string(),
                kinds: vec![(kind.to_string(), introduced)],
            }),
        }
        if !entry.internal_kinds.iter().any(|k| k == kind) {
            entry.internal_kinds.push(kind.to_string());
        }
    }

    /// `IsGroupRegistered`.
    pub fn is_group_registered(&self, group: &str) -> bool {
        self.groups.contains_key(group)
    }

    /// `PrioritizedVersionsForGroup`.
    pub fn prioritized_versions_for_group(&self, group: &str) -> Vec<GroupVersion> {
        self.groups
            .get(group)
            .map(|g| {
                g.versions
                    .iter()
                    .map(|v| GroupVersion::new(group, &v.version))
                    .collect()
            })
            .unwrap_or_default()
    }

    /// `ObjectKinds` for an internal-version example: the GVKs the example's
    /// type is registered under.
    fn object_kinds(&self, example: &Example) -> Result<Vec<(String, String)>, String> {
        let registered = self
            .groups
            .get(&example.group)
            .is_some_and(|g| g.internal_kinds.contains(&example.kind));
        if registered {
            Ok(vec![(example.group.clone(), example.kind.clone())])
        } else {
            Err(format!(
                "no kind is registered for the type {}.{} in scheme",
                example.group, example.kind
            ))
        }
    }

    /// `VersionsForGroupKind`: versions in priority order, internal included.
    fn versions_for_group_kind(&self, group: &str, kind: &str) -> Vec<String> {
        let Some(entry) = self.groups.get(group) else {
            return Vec::new();
        };
        let mut out = Vec::new();
        if entry.internal_kinds.iter().any(|k| k == kind) {
            out.push(API_VERSION_INTERNAL.to_string());
        }
        for v in &entry.versions {
            if v.kinds.iter().any(|(k, _)| k == kind) {
                out.push(v.version.clone());
            }
        }
        out
    }

    /// `New(gvk)` reduced to the `introducedInterface` assertion the caller
    /// makes on the result: `Ok(Some(..))` = has `APILifecycleIntroduced`.
    fn new_introduced(
        &self,
        group: &str,
        version: &str,
        kind: &str,
    ) -> Result<Option<(u32, u32)>, String> {
        self.groups
            .get(group)
            .and_then(|g| g.versions.iter().find(|v| v.version == version))
            .and_then(|v| v.kinds.iter().find(|(k, _)| k == kind))
            .map(|(_, introduced)| *introduced)
            .ok_or_else(|| {
                format!(
                    "no kind {kind:?} is registered for version \"{group}/{version}\" in scheme"
                )
            })
    }
}

/// `ResourceEncodingConfig` (resource_encoding_config.go:29-37).
pub trait ResourceEncodingConfig {
    fn storage_encoding_for(&self, resource: &GroupResource) -> Result<GroupVersion, String>;
    fn in_memory_encoding_for(&self, resource: &GroupResource) -> Result<GroupVersion, String>;
    /// `CompatibilityResourceEncodingConfig` (:39-41): `None` when the config
    /// does not implement the optional interface.
    fn backward_compatible_storage_encoding_for(
        &self,
        _resource: &GroupResource,
        _example: Option<&Example>,
    ) -> Option<Result<GroupVersion, String>> {
        None
    }
}

/// `OverridingResourceEncoding` (:50-53).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OverridingResourceEncoding {
    pub external_resource_encoding: GroupVersion,
    pub internal_resource_encoding: GroupVersion,
}

/// `DefaultResourceEncodingConfig` (:43-48).
#[derive(Debug, Clone)]
pub struct DefaultResourceEncodingConfig {
    resources: BTreeMap<GroupResource, OverridingResourceEncoding>,
    scheme: Scheme,
    effective_version: Option<EffectiveVersion>,
}

impl DefaultResourceEncodingConfig {
    /// `NewDefaultResourceEncodingConfig` (:57-59): the effective version is
    /// `DefaultComponentGlobalsRegistry.EffectiveVersionFor(kube)`.
    pub fn new(scheme: Scheme) -> Self {
        Self::new_for_effective_version(scheme, Some(current_effective_version()))
    }

    /// `NewDefaultResourceEncodingConfigForEffectiveVersion` (:61-63).
    pub fn new_for_effective_version(
        scheme: Scheme,
        effective_version: Option<EffectiveVersion>,
    ) -> Self {
        Self {
            resources: BTreeMap::new(),
            scheme,
            effective_version,
        }
    }

    /// `SetResourceEncoding` (:65-70).
    pub fn set_resource_encoding(
        &mut self,
        resource_being_stored: GroupResource,
        external_encoding_version: GroupVersion,
        internal_version: GroupVersion,
    ) {
        self.resources.insert(
            resource_being_stored,
            OverridingResourceEncoding {
                external_resource_encoding: external_encoding_version,
                internal_resource_encoding: internal_version,
            },
        );
    }

    /// `SetEffectiveVersion` (:72-74).
    pub fn set_effective_version(&mut self, effective_version: Option<EffectiveVersion>) {
        self.effective_version = effective_version;
    }

    fn check_group(&self, group: &str) -> Result<(), String> {
        if !self.scheme.is_group_registered(group) {
            return Err(format!("group {group:?} is not registered in scheme"));
        }
        Ok(())
    }

    /// `BackwardCompatibileStorageEncodingFor` (:90-106).
    pub fn backward_compatibile_storage_encoding_for(
        &self,
        resource: &GroupResource,
        example: Option<&Example>,
    ) -> Result<GroupVersion, String> {
        self.check_group(&resource.group)?;

        // Always respect overrides
        if let Some(over) = self.resources.get(resource) {
            return Ok(over.external_resource_encoding.clone());
        }

        emulated_storage_version(
            self.scheme.prioritized_versions_for_group(&resource.group)[0].clone(),
            example,
            self.effective_version.as_ref(),
            &self.scheme,
        )
    }
}

impl ResourceEncodingConfig for DefaultResourceEncodingConfig {
    /// `StorageEncodingFor` (:76-88).
    fn storage_encoding_for(&self, resource: &GroupResource) -> Result<GroupVersion, String> {
        self.check_group(&resource.group)?;

        if let Some(over) = self.resources.get(resource) {
            return Ok(over.external_resource_encoding.clone());
        }

        // return the most preferred external version for the group
        Ok(self.scheme.prioritized_versions_for_group(&resource.group)[0].clone())
    }

    /// `InMemoryEncodingFor` (:108-118).
    fn in_memory_encoding_for(&self, resource: &GroupResource) -> Result<GroupVersion, String> {
        self.check_group(&resource.group)?;

        if let Some(over) = self.resources.get(resource) {
            return Ok(over.internal_resource_encoding.clone());
        }
        Ok(GroupVersion::new(&resource.group, API_VERSION_INTERNAL))
    }

    fn backward_compatible_storage_encoding_for(
        &self,
        resource: &GroupResource,
        example: Option<&Example>,
    ) -> Option<Result<GroupVersion, String>> {
        Some(self.backward_compatibile_storage_encoding_for(resource, example))
    }
}

/// `emulatedStorageVersion` (:125-205).
fn emulated_storage_version(
    binary_version_of_resource: GroupVersion,
    example: Option<&Example>,
    effective_version: Option<&EffectiveVersion>,
    scheme: &Scheme,
) -> Result<GroupVersion, String> {
    let (Some(example), Some(effective_version)) = (example, effective_version) else {
        return Ok(binary_version_of_resource);
    };

    // Look up example in scheme to find all objects of the same Group-Kind
    // Use the highest priority version for that group-kind whose lifecycle window
    // includes the current emulation version.
    // If no version is found, use the binary version
    // (in this case the API should be disabled anyway)
    let gvks = scheme.object_kinds(example)?;

    let Some((group, kind)) = gvks
        .into_iter()
        .find(|(group, _)| *group == binary_version_of_resource.group)
    else {
        return Err(format!(
            "object {}.{} has no GVKs registered in scheme",
            example.group, example.kind
        ));
    };

    // VersionsForGroupKind returns versions in priority order
    let compatibility_version = effective_version.min_compatibility_version();

    for version in scheme.versions_for_group_kind(&group, &kind) {
        if version == API_VERSION_INTERNAL {
            continue;
        }

        let introduced = scheme.new_introduced(&group, &version, &kind)?;

        // If it was introduced after current compatibility version, don't use it
        // skip the introduced check for test when current compatibility version is 0.0 to test all apis
        if let Some((major, minor)) = introduced {
            if compatibility_version.major > 0 || compatibility_version.minor > 0 {
                // API resource lifecycles should be relative to k8s api version
                let introduced_ver = Version::major_minor(major, minor);
                if introduced_ver > compatibility_version {
                    continue;
                }
            }
        }

        // versions is returned in priority order, so just use first result
        return Ok(GroupVersion { group, version });
    }

    // Getting here means we're serving a version that is unknown to the
    // min-compatibility-version server.
    //
    // This is only expected to happen when serving an alpha API type due
    // to missing pre-release lifecycle information
    // (which doesn't happen by default), or when emulation-version and
    // min-compatibility-version are several versions apart so a beta or GA API
    // was being served which didn't exist at all in min-compatibility-version.
    //
    // In the alpha case - we do not support compatibility versioning of
    // alpha types and recommend users do not mix the two.
    // In the skip-level case - The version of apiserver we are retaining
    // compatibility with has no knowledge of the type,
    // so storing it in another type is no issue.
    Ok(binary_version_of_resource)
}

/// The version half of `DefaultStorageFactory.NewConfig`
/// (storage_factory.go:247-263): which version a resource is stored in and
/// which it is held as in memory.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StorageCodecVersions {
    pub storage_version: GroupVersion,
    pub memory_version: GroupVersion,
}

pub fn storage_codec_versions(
    config: &dyn ResourceEncodingConfig,
    chosen_storage_resource: &GroupResource,
    group_resource: &GroupResource,
    example: Option<&Example>,
) -> Result<StorageCodecVersions, String> {
    let storage_version =
        match config.backward_compatible_storage_encoding_for(chosen_storage_resource, example) {
            Some(r) => r?,
            None => config.storage_encoding_for(chosen_storage_resource)?,
        };
    let memory_version = config.in_memory_encoding_for(group_resource)?;
    Ok(StorageCodecVersions {
        storage_version,
        memory_version,
    })
}

/// `MergeResourceEncodingConfigs` (resourceconfig/helpers.go:45-56).
pub fn merge_resource_encoding_configs(
    mut default_resource_encoding: DefaultResourceEncodingConfig,
    resource_encoding_overrides: &[GroupVersionResource],
) -> DefaultResourceEncodingConfig {
    for gvr in resource_encoding_overrides {
        default_resource_encoding.set_resource_encoding(
            gvr.group_resource(),
            gvr.group_version(),
            GroupVersion::new(&gvr.group, API_VERSION_INTERNAL),
        );
    }
    default_resource_encoding
}

/// `StorageFactoryConfig` (default_storage_factory_builder.go:103-110),
/// reduced to the encoding half.
#[derive(Debug, Clone)]
pub struct StorageFactoryConfig {
    pub default_resource_encoding: DefaultResourceEncodingConfig,
    pub resource_encoding_overrides: Vec<GroupVersionResource>,
}

impl StorageFactoryConfig {
    /// `NewStorageFactoryConfig` (:66-69).
    pub fn new() -> Self {
        Self::new_effective_version(current_effective_version())
    }

    /// `NewStorageFactoryConfigEffectiveVersion` (:71-100).
    pub fn new_effective_version(effective_version: EffectiveVersion) -> Self {
        let resources = vec![
            // If a resource has to be stored in a version that is not the
            // latest, then it can be listed here. Usually this is the case
            // when a new version for a resource gets introduced and a
            // downgrade to an older apiserver that doesn't know the new
            // version still needs to be supported for one release.
            GroupVersionResource::new("coordination.k8s.io", "v1beta1", "leasecandidates"),
            GroupVersionResource::new(
                "admissionregistration.k8s.io",
                "v1beta1",
                "mutatingadmissionpolicies",
            ),
            GroupVersionResource::new(
                "admissionregistration.k8s.io",
                "v1beta1",
                "mutatingadmissionpolicybindings",
            ),
            GroupVersionResource::new("certificates.k8s.io", "v1beta1", "clustertrustbundles"),
            GroupVersionResource::new("certificates.k8s.io", "v1beta1", "podcertificaterequests"),
            GroupVersionResource::new("storage.k8s.io", "v1beta1", "volumeattributesclasses"),
            GroupVersionResource::new("storagemigration.k8s.io", "v1beta1", "storagemigrations"),
            GroupVersionResource::new("resource.k8s.io", "v1alpha3", "devicetaintrules"),
            GroupVersionResource::new("scheduling.k8s.io", "v1alpha1", "workloads"),
        ];
        Self {
            default_resource_encoding: DefaultResourceEncodingConfig::new_for_effective_version(
                Scheme::legacy(),
                Some(effective_version),
            ),
            resource_encoding_overrides: resources,
        }
    }

    /// The `MergeResourceEncodingConfigs` call in `completedStorageFactoryConfig.New`
    /// (default_storage_factory_builder.go:132).
    pub fn resource_encoding_config(&self) -> DefaultResourceEncodingConfig {
        merge_resource_encoding_configs(
            self.default_resource_encoding.clone(),
            &self.resource_encoding_overrides,
        )
    }
}

impl Default for StorageFactoryConfig {
    fn default() -> Self {
        Self::new()
    }
}

#[doc(hidden)]
pub struct VersionTable {
    pub version: &'static str,
    pub kinds: &'static [(&'static str, Option<(u32, u32)>)],
}

#[doc(hidden)]
pub struct GroupTable {
    pub group: &'static str,
    pub versions: &'static [VersionTable],
}

#[cfg(test)]
mod tests;
