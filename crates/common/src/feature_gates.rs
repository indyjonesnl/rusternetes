//! Process-wide feature gates — analogue of upstream
//! `k8s.io/component-base/featuregate`.
//!
//! Upstream Kubernetes wires feature gates through `utilfeature.DefaultFeatureGate`,
//! a global registry consulted by validators and admission controllers. We mirror
//! that surface with a small AtomicBool-backed registry so call sites can ask
//! `feature_gates::enabled(Feature::RelaxedDNSSearchValidation)` without
//! threading flags through every function signature.
//!
//! Each [`Feature`] variant carries its rusternetes default, which mirrors the
//! upstream default at the version rusternetes targets (currently v1.35).
//! Tests that need to flip a gate should use [`with_feature`] (the RAII guard
//! restores the previous value on drop) and pair the test with
//! `#[serial_test::serial]` because the registry is process-wide.

use std::sync::atomic::{AtomicBool, Ordering};

/// Enum of all rusternetes feature gates. Adding a gate is two lines: a variant
/// here plus a slot in [`STATES`]; the `idx` <-> default mapping in
/// [`Feature::idx`] and [`Feature::default_enabled`] keeps them in sync.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Feature {
    /// When enabled, pod `dnsConfig.searches` entries may contain a single
    /// underscore per label and the lone `.` domain, matching upstream
    /// `IsDNS1123SubdomainWithUnderScore`. When disabled, falls back to the
    /// strict RFC1123-subdomain validator.
    ///
    /// Upstream: `pkg/features/kube_features.go::RelaxedDNSSearchValidation`.
    /// GA + locked-to-default in v1.34; rusternetes targets v1.35 so the
    /// default is `true`.
    RelaxedDNSSearchValidation,

    /// When enabled, the api-server's pod-binding handler copies
    /// `topology.kubernetes.io/zone` and `topology.kubernetes.io/region`
    /// labels from the bound Node onto the Pod. When disabled, the
    /// binding handler does not touch the pod's labels at all — matching
    /// upstream's `Plugin.Admit` no-op behaviour when `p.enabled == false`.
    ///
    /// Upstream: `plugin/pkg/admission/podtopologylabels/admission.go`.
    /// Beta in v1.35, default `true`.
    PodTopologyLabelsAdmission,

    /// When enabled, the api-server consults `node.status.declaredFeatures`
    /// during pod admission so that node-side features (such as
    /// `GuaranteedQoSPodCPUResize`) can be required by the API plane before a
    /// mutation is admitted to a node that has not declared support.
    ///
    /// Upstream: `pkg/features/kube_features.go::NodeDeclaredFeatures` —
    /// KEP-5328. Alpha in v1.34; rusternetes targets v1.35 where the gate is
    /// still off-by-default.
    NodeDeclaredFeatures,

    /// When enabled, pods may have their `spec.containers[*].resources` mutated
    /// in place via the `/resize` subresource (KEP-1287). Required for the
    /// node-declared-feature admission to reject a CPU resize on a node that
    /// has not declared `GuaranteedQoSPodCPUResize`.
    ///
    /// Upstream: `pkg/features/kube_features.go::InPlacePodVerticalScaling`.
    /// Beta + default-on in v1.33; rusternetes targets v1.35 so the default
    /// is `true`.
    InPlacePodVerticalScaling,

    /// When enabled, the kubelet computes an SELinux mount label for
    /// `ReadWriteOncePod` volumes and mounts them with `-o context=`, and the
    /// volume-manager caches treat two volumes with the same name but
    /// different SELinux contexts as different volumes.
    ///
    /// Upstream: `pkg/features/kube_features.go::SELinuxMountReadWriteOncePod`
    /// — Beta + default-on since v1.28 (`kube_features.go:1743-1747`), so the
    /// v1.35 default is `true`.
    SELinuxMountReadWriteOncePod,

    /// When enabled, the `-o context=` mount is extended from `ReadWriteOncePod`
    /// volumes to every access mode.
    ///
    /// Upstream: `pkg/features/kube_features.go::SELinuxMount` — Beta but
    /// off-by-default in v1.33 (`kube_features.go:1738-1741`), unchanged in
    /// v1.35, so the default is `false`.
    SELinuxMount,

    /// When enabled, a pod may opt out of `-o context=` mounting via
    /// `spec.securityContext.seLinuxChangePolicy: Recursive`.
    ///
    /// Upstream: `pkg/features/kube_features.go::SELinuxChangePolicy` — Beta +
    /// default-on since v1.33 (`kube_features.go:1733-1736`), so the v1.35
    /// default is `true`.
    SELinuxChangePolicy,

    /// When enabled, a StatefulSet's `spec.updateStrategy.rollingUpdate.
    /// maxUnavailable` is honoured; when disabled the StatefulSet strategy
    /// drops it unless the stored object already uses it
    /// (`dropStatefulSetDisabledFields`,
    /// pkg/registry/apps/statefulset/strategy.go).
    ///
    /// Upstream: `pkg/features/kube_features.go::MaxUnavailableStatefulSet` —
    /// Beta but off-by-default in v1.35 (`kube_features.go:1515-1518`), so the
    /// default is `false`.
    MaxUnavailableStatefulSet,

    /// Upstream: `pkg/features/kube_features.go::DRADeviceTaints` (1.33 Alpha, off in v1.35).
    DRADeviceTaints,

    /// Upstream: `pkg/features/kube_features.go::DRAPartitionableDevices` (1.33 Alpha, off in v1.35).
    DRAPartitionableDevices,

    /// Upstream: `pkg/features/kube_features.go::DRADeviceBindingConditions` (1.34 Alpha, off in v1.35).
    DRADeviceBindingConditions,

    /// Upstream: `pkg/features/kube_features.go::DRAConsumableCapacity` (1.34 Alpha, off in v1.35).
    DRAConsumableCapacity,

    /// Upstream: `pkg/features/kube_features.go::DRAResourceClaimDeviceStatus` (1.33 Beta, on in v1.35).
    DRAResourceClaimDeviceStatus,

    /// Upstream: `pkg/features/kube_features.go::InPlacePodLevelResourcesVerticalScaling` (1.35 Alpha, off in v1.35; kube_features.go:1372).
    InPlacePodLevelResourcesVerticalScaling,

    /// Upstream: `pkg/features/kube_features.go::DRAExtendedResource` (1.34 Alpha, off in v1.35; kube_features.go:1248).
    DRAExtendedResource,

    /// Upstream: `pkg/features/kube_features.go::ClusterTrustBundleProjection` (1.33 Beta, but off (no `Default: true`) in v1.35; kube_features.go:1204).
    ClusterTrustBundleProjection,

    /// Upstream: `pkg/features/kube_features.go::PodCertificateRequest` (1.35 Beta, but off in v1.35; kube_features.go:1602).
    PodCertificateRequest,

    /// Upstream: `pkg/features/kube_features.go::ContainerStopSignals` (1.33 Alpha, off in v1.35; kube_features.go:1219).
    ContainerStopSignals,

    /// Upstream: `pkg/features/kube_features.go::ResourceHealthStatus` (1.31 Alpha, off in v1.35; kube_features.go:1716).
    ResourceHealthStatus,

    /// Upstream: `pkg/features/kube_features.go::GenericWorkload` (1.35 Alpha, off in v1.35; kube_features.go:1321).
    GenericWorkload,

    /// Upstream: `pkg/features/kube_features.go::TaintTolerationComparisonOperators` (1.35 Alpha, off in v1.35; kube_features.go:1859).
    TaintTolerationComparisonOperators,

    /// Upstream: `pkg/features/kube_features.go::PodLevelResources` (1.34 Beta, on in v1.35; kube_features.go:1612).
    PodLevelResources,

    /// Upstream: `pkg/features/kube_features.go::MatchLabelKeysInPodTopologySpread` (1.27 Beta, on in v1.35; kube_features.go:1506).
    MatchLabelKeysInPodTopologySpread,

    /// Upstream: `pkg/features/kube_features.go::MatchLabelKeysInPodTopologySpreadSelectorMerge` (1.34 Beta, on in v1.35; kube_features.go:1511).
    MatchLabelKeysInPodTopologySpreadSelectorMerge,

    /// Upstream: `pkg/features/kube_features.go::PodDeletionCost` (1.22 Beta, on in v1.35; kube_features.go:1607).
    PodDeletionCost,

    /// Upstream gate `ClusterTrustBundle` (1.33 Beta, but off (no `Default: true`) (kube_features.go:1199-1202)).
    ClusterTrustBundle,

    /// Upstream gate `DRADeviceTaintRules` (1.35 Alpha, off (kube_features.go:1240-1242)).
    DRADeviceTaintRules,

    /// Upstream gate `ComponentFlagz` (1.32 Alpha, off (k8s.io/component-base/zpages/features/kube_features.go:40-42)).
    ComponentFlagz,

    /// Upstream gate `ComponentStatusz` (1.32 Alpha, off (k8s.io/component-base/zpages/features/kube_features.go:43-45)).
    ComponentStatusz,

    /// Upstream gate `StorageVersionAPI` (1.20 Alpha, off; `pkg/features/kube_features.go:2084-2086`).
    StorageVersionAPI,

    /// Upstream gate `APIServerIdentity` (1.26 Beta, on; `pkg/features/kube_features.go:1957-1960`).
    APIServerIdentity,

    /// Upstream gate `StorageVersionMigrator` (1.35 Beta, still off; `pkg/features/kube_features.go:1829-1832`).
    StorageVersionMigrator,

    /// Upstream gate `KubeletCrashLoopBackOffMax` (1.32 Alpha off, 1.35 Beta on; `pkg/features/kube_features.go:1429-1432`).
    KubeletCrashLoopBackOffMax,

    /// Upstream gate `ReduceDefaultCrashLoopBackOffDecay` (1.33 Alpha, off; `pkg/features/kube_features.go:1692-1694`).
    ReduceDefaultCrashLoopBackOffDecay,

    /// Upstream gate `WatchCacheInitializationPostStartHook` (1.31 Beta, still off; `staging/src/k8s.io/apiserver/pkg/features/kube_features.go:494-496`).
    WatchCacheInitializationPostStartHook,
    /// Upstream gate `MutableCSINodeAllocatableCount` (1.35 Beta, on; `pkg/features/kube_features.go:1537-1541`).
    /// Gates the kubelet's `csiNodeUpdater` (`pkg/volume/csi/csi_plugin.go:417`).
    MutableCSINodeAllocatableCount,

    /// Upstream gate `MutatingAdmissionPolicy` (1.32 Alpha, 1.34 Beta, still off;
    /// `staging/src/k8s.io/apiserver/pkg/features/kube_features.go:407-410`,
    /// `pkg/features/kube_features.go:2049-2052`). Upstream the gate only enables
    /// the admission plugin (`admission/plugin/policy/mutating/plugin.go:124`);
    /// the REST resource is gated by the `admissionregistration.k8s.io/v1beta1`
    /// API group, which is in `betaAPIGroupVersionsDisabledByDefault`
    /// (`pkg/controlplane/instance.go`). Rusternetes has no `--runtime-config`,
    /// so this gate stands in for both, as `ClusterTrustBundle` does for
    /// `certificates.k8s.io/v1beta1`.
    MutatingAdmissionPolicy,

    /// Upstream gate `DeploymentReplicaSetTerminatingReplicas` (1.33 Alpha,
    /// 1.35 Beta, on; `pkg/features/kube_features.go:1270-1273`). When off, the
    /// Deployment/ReplicaSet status strategies drop `status.terminatingReplicas`
    /// unless the stored object already has it (`dropDisabledStatusFields`).
    DeploymentReplicaSetTerminatingReplicas,

    /// Upstream gate `StorageCapacityScoring` (1.33 Alpha, off; `pkg/features/kube_features.go:1820-1822`).
    /// Enables the scheduler VolumeBinding Score extension point.
    StorageCapacityScoring,

    /// Upstream gate `CrossNamespaceVolumeDataSource` (1.26 Alpha, off;
    /// `pkg/features/kube_features.go:1223-1225`). When off, the PVC strategy
    /// drops a cross-namespace `spec.dataSourceRef` unless the old claim already
    /// had a `dataSourceRef` (`pkg/api/persistentvolumeclaim/util.go:37-58`).
    CrossNamespaceVolumeDataSource,

    /// Upstream gate `RuntimeClassInImageCriApi` (1.29 Alpha, off;
    /// `pkg/features/kube_features.go:1729-1731`; name at `:858`). When on, the
    /// kubelet passes the pod RuntimeClass handler in `ImageSpec.runtime_handler`
    /// (`kuberuntime_container.go:177-190` `getPodRuntimeHandler`).
    RuntimeClassInImageCriApi,

    /// Upstream gate `DynamicResourceAllocation`: pkg/features/kube_features.go:1293-1296 (1.34 GA, 1.35 GA + LockToDefault). Modelled so `DEPENDENCIES` can reference it (#3222).
    DynamicResourceAllocation,

    /// Upstream gate `AuthorizeWithSelectors`: staging/src/k8s.io/apiserver/pkg/features/kube_features.go:350-354 (1.34 GA + LockToDefault). Modelled so `DEPENDENCIES` can reference it (#3222).
    AuthorizeWithSelectors,

    /// Upstream gate `AuthorizeNodeWithSelectors`: pkg/features/kube_features.go:1147-1149 (1.34 GA + LockToDefault). Modelled so `DEPENDENCIES` can reference it (#3222).
    AuthorizeNodeWithSelectors,

    /// Upstream gate `MultiCIDRServiceAllocator`: pkg/features/kube_features.go:1531-1534 (1.34 GA + LockToDefault). Modelled so `DEPENDENCIES` can reference it (#3222).
    MultiCIDRServiceAllocator,

    /// Upstream gate `DisableAllocatorDualWrite`: pkg/features/kube_features.go:1276-1279 (1.35 GA + LockToDefault). Modelled so `DEPENDENCIES` can reference it (#3222).
    DisableAllocatorDualWrite,

    /// Upstream gate `GangScheduling`: pkg/features/kube_features.go:1318 (1.35 Alpha, off). Modelled so `DEPENDENCIES` can reference it (#3222).
    GangScheduling,

    /// Upstream gate `GracefulNodeShutdown`: pkg/features/kube_features.go:1331-1332 (1.21 Beta, on). Modelled so `DEPENDENCIES` can reference it (#3222).
    GracefulNodeShutdown,

    /// Upstream gate `GracefulNodeShutdownBasedOnPodPriority`: pkg/features/kube_features.go:1336-1337 (1.24 Beta, on). Modelled so `DEPENDENCIES` can reference it (#3222).
    GracefulNodeShutdownBasedOnPodPriority,

    /// Upstream gate `InPlacePodVerticalScalingAllocatedStatus`: pkg/features/kube_features.go:1383-1384 (1.33 Deprecated, off). Modelled so `DEPENDENCIES` can reference it (#3222).
    InPlacePodVerticalScalingAllocatedStatus,

    /// Upstream gate `InPlacePodVerticalScalingExclusiveCPUs`: pkg/features/kube_features.go:1388 (1.32 Alpha, off). Modelled so `DEPENDENCIES` can reference it (#3222).
    InPlacePodVerticalScalingExclusiveCPUs,

    /// Upstream gate `InPlacePodVerticalScalingExclusiveMemory`: pkg/features/kube_features.go:1392 (1.34 Alpha, off). Modelled so `DEPENDENCIES` can reference it (#3222).
    InPlacePodVerticalScalingExclusiveMemory,

    /// Upstream gate `MemoryManager`: pkg/features/kube_features.go:1521-1523 (1.32 GA + LockToDefault). Modelled so `DEPENDENCIES` can reference it (#3222).
    MemoryManager,

    /// Upstream gate `PodLifecycleSleepAction`: pkg/features/kube_features.go:1618-1620 (1.34 GA + LockToDefault). Modelled so `DEPENDENCIES` can reference it (#3222).
    PodLifecycleSleepAction,

    /// Upstream gate `PodLifecycleSleepActionAllowZero`: pkg/features/kube_features.go:1624-1626 (1.34 GA + LockToDefault). Modelled so `DEPENDENCIES` can reference it (#3222).
    PodLifecycleSleepActionAllowZero,

    /// Upstream gate `ProcMountType`: pkg/features/kube_features.go:1670-1673 (1.33 Beta, on). Modelled so `DEPENDENCIES` can reference it (#3222).
    ProcMountType,

    /// Upstream gate `UserNamespacesSupport`: pkg/features/kube_features.go:1894-1897 (1.33 Beta, on). Modelled so `DEPENDENCIES` can reference it (#3222).
    UserNamespacesSupport,

    /// Upstream gate `DRAAdminAccess`: pkg/features/kube_features.go:1227-1230 (1.34 Beta, on). Modelled so `DEPENDENCIES` can reference it (#3222).
    DRAAdminAccess,

    /// Upstream gate `DRAPrioritizedList`: pkg/features/kube_features.go:1256-1259 (1.34 Beta, on). Modelled so `DEPENDENCIES` can reference it (#3222).
    DRAPrioritizedList,

    /// Upstream gate `DRASchedulerFilterTimeout`: pkg/features/kube_features.go:1266-1268 (1.34 Beta, on). Modelled so `DEPENDENCIES` can reference it (#3222).
    DRASchedulerFilterTimeout,

    /// Upstream gate `ContainerRestartRules`: pkg/features/kube_features.go:1214-1217 (1.35 Beta, on). Modelled so `DEPENDENCIES` can reference it (#3222).
    ContainerRestartRules,

    /// Upstream gate `RestartAllContainersOnContainerExits`: pkg/features/kube_features.go:1720-1722 (1.35 Alpha, off). Modelled so `DEPENDENCIES` can reference it (#3222).
    RestartAllContainersOnContainerExits,

    /// Upstream gate `ServiceAccountTokenNodeBinding`: pkg/features/kube_features.go:1784-1788 (1.33 GA + LockToDefault). Modelled so `DEPENDENCIES` can reference it (#3231).
    ServiceAccountTokenNodeBinding,

    /// Upstream gate `ServiceAccountTokenNodeBindingValidation`: pkg/features/kube_features.go:1790-1794 (1.32 GA + LockToDefault). Modelled so `DEPENDENCIES` can reference it (#3231).
    ServiceAccountTokenNodeBindingValidation,

    /// Upstream gate `UserNamespacesHostNetworkSupport`: pkg/features/kube_features.go:1890-1892 (1.35 Alpha, off). Modelled so `DEPENDENCIES` can reference it (#3231).
    UserNamespacesHostNetworkSupport,

    /// Upstream gate `WindowsCPUAndMemoryAffinity`: pkg/features/kube_features.go:1922-1924 (1.32 Alpha, off). Modelled so `DEPENDENCIES` can reference it (#3231).
    WindowsCPUAndMemoryAffinity,

    /// Upstream gate `WindowsGracefulNodeShutdown`: pkg/features/kube_features.go:1926-1929 (1.34 Beta, on). Modelled so `DEPENDENCIES` can reference it (#3231).
    WindowsGracefulNodeShutdown,

    /// Upstream gate `UnknownVersionInteroperabilityProxy`: staging/src/k8s.io/apiserver/pkg/features/kube_features.go:490-492 (1.28 Alpha, off). Modelled so `DEPENDENCIES` can reference it (#3231).
    UnknownVersionInteroperabilityProxy,

    /// Upstream gate `DeclarativeValidation`: staging/src/k8s.io/apiserver/pkg/features/kube_features.go:384-386 (1.33 Beta, on). Modelled so `DEPENDENCIES` can reference it (#3231).
    DeclarativeValidation,

    /// Upstream gate `DeclarativeValidationTakeover`: staging/src/k8s.io/apiserver/pkg/features/kube_features.go:388-390 (1.33 Beta, off). Modelled so `DEPENDENCIES` can reference it (#3231).
    DeclarativeValidationTakeover,

    /// Upstream gate `StructuredAuthenticationConfiguration`: staging/src/k8s.io/apiserver/pkg/features/kube_features.go:461-465 (1.34 GA + LockToDefault). Modelled so `DEPENDENCIES` can reference it (#3231).
    StructuredAuthenticationConfiguration,

    /// Upstream gate `StructuredAuthenticationConfigurationEgressSelector`: staging/src/k8s.io/apiserver/pkg/features/kube_features.go:467-469 (1.34 Beta, on). Modelled so `DEPENDENCIES` can reference it (#3231).
    StructuredAuthenticationConfigurationEgressSelector,

    /// Upstream gate `StructuredAuthenticationConfigurationJWKSMetrics`: staging/src/k8s.io/apiserver/pkg/features/kube_features.go:471-473 (1.35 Beta, on). Modelled so `DEPENDENCIES` can reference it (#3231).
    StructuredAuthenticationConfigurationJWKSMetrics,

    /// Upstream gate `ImageVolume` (1.31 Alpha, 1.33 Beta, 1.35 Beta on;
    /// `pkg/features/kube_features.go:1366-1370`, name at `:389`). When on, the
    /// kubelet pulls each `image` volume and hands the runtime a CRI
    /// `Mount.image` (`kuberuntime_manager.go:1813-1850` `getImageVolumes`,
    /// `kubelet_pods.go:327-330` `makeMounts`).
    ImageVolume,

    /// Upstream gate `LogarithmicScaleDown` (1.21 Alpha, 1.22 Beta on, 1.31 GA
    /// locked to default; `pkg/features/kube_features.go:1494-1498`, name at
    /// `:565`). Read by `ActivePodsWithRanks.Less`
    /// (`pkg/controller/controller_utils.go:875,895`).
    LogarithmicScaleDown,

    /// Upstream gate `ServiceAccountNodeAudienceRestriction`: pkg/features/kube_features.go:1773-1776 (1.33 Beta, on). Read by NodeRestriction (`plugin/pkg/admission/noderestriction/admission.go:111`).
    ServiceAccountNodeAudienceRestriction,
    /// Upstream gate `RelaxedEnvironmentVariableValidation` (1.30 Alpha, 1.32
    /// Beta on, 1.34 GA locked to default;
    /// `pkg/features/kube_features.go:1702-1706`). Read by
    /// `useRelaxedEnvironmentVariableValidation` (`pkg/api/pod/util.go:497`).
    RelaxedEnvironmentVariableValidation,

    /// Upstream gate `EnvFiles` (1.34 Alpha, 1.35 Beta on;
    /// `pkg/features/kube_features.go:1299-1302`). Read by
    /// `useAllowEnvFilesValidation` (`pkg/api/pod/util.go:561`).
    EnvFiles,
}

impl Feature {
    /// Stable index into [`STATES`]. Must be kept in sync with the enum order.
    const fn idx(self) -> usize {
        match self {
            Feature::RelaxedDNSSearchValidation => 0,
            Feature::PodTopologyLabelsAdmission => 1,
            Feature::NodeDeclaredFeatures => 2,
            Feature::InPlacePodVerticalScaling => 3,
            Feature::SELinuxMountReadWriteOncePod => 4,
            Feature::SELinuxMount => 5,
            Feature::SELinuxChangePolicy => 6,
            Feature::MaxUnavailableStatefulSet => 7,
            Feature::DRADeviceTaints => 8,
            Feature::DRAPartitionableDevices => 9,
            Feature::DRADeviceBindingConditions => 10,
            Feature::DRAConsumableCapacity => 11,
            Feature::DRAResourceClaimDeviceStatus => 12,
            Feature::InPlacePodLevelResourcesVerticalScaling => 13,
            Feature::DRAExtendedResource => 14,
            Feature::ClusterTrustBundleProjection => 15,
            Feature::PodCertificateRequest => 16,
            Feature::ContainerStopSignals => 17,
            Feature::ResourceHealthStatus => 18,
            Feature::GenericWorkload => 19,
            Feature::TaintTolerationComparisonOperators => 20,
            Feature::PodLevelResources => 21,
            Feature::MatchLabelKeysInPodTopologySpread => 22,
            Feature::MatchLabelKeysInPodTopologySpreadSelectorMerge => 23,
            Feature::PodDeletionCost => 24,
            Feature::ClusterTrustBundle => 25,
            Feature::DRADeviceTaintRules => 26,
            Feature::ComponentFlagz => 27,
            Feature::ComponentStatusz => 28,
            Feature::StorageVersionAPI => 29,
            Feature::APIServerIdentity => 30,
            Feature::StorageVersionMigrator => 31,
            Feature::KubeletCrashLoopBackOffMax => 32,
            Feature::ReduceDefaultCrashLoopBackOffDecay => 33,
            Feature::WatchCacheInitializationPostStartHook => 34,
            Feature::MutableCSINodeAllocatableCount => 35,
            Feature::MutatingAdmissionPolicy => 36,
            Feature::DeploymentReplicaSetTerminatingReplicas => 37,
            Feature::StorageCapacityScoring => 38,
            Feature::CrossNamespaceVolumeDataSource => 39,
            Feature::RuntimeClassInImageCriApi => 40,
            Feature::DynamicResourceAllocation => 41,
            Feature::AuthorizeWithSelectors => 42,
            Feature::AuthorizeNodeWithSelectors => 43,
            Feature::MultiCIDRServiceAllocator => 44,
            Feature::DisableAllocatorDualWrite => 45,
            Feature::GangScheduling => 46,
            Feature::GracefulNodeShutdown => 47,
            Feature::GracefulNodeShutdownBasedOnPodPriority => 48,
            Feature::InPlacePodVerticalScalingAllocatedStatus => 49,
            Feature::InPlacePodVerticalScalingExclusiveCPUs => 50,
            Feature::InPlacePodVerticalScalingExclusiveMemory => 51,
            Feature::MemoryManager => 52,
            Feature::PodLifecycleSleepAction => 53,
            Feature::PodLifecycleSleepActionAllowZero => 54,
            Feature::ProcMountType => 55,
            Feature::UserNamespacesSupport => 56,
            Feature::DRAAdminAccess => 57,
            Feature::DRAPrioritizedList => 58,
            Feature::DRASchedulerFilterTimeout => 59,
            Feature::ContainerRestartRules => 60,
            Feature::RestartAllContainersOnContainerExits => 61,
            Feature::ServiceAccountTokenNodeBinding => 62,
            Feature::ServiceAccountTokenNodeBindingValidation => 63,
            Feature::UserNamespacesHostNetworkSupport => 64,
            Feature::WindowsCPUAndMemoryAffinity => 65,
            Feature::WindowsGracefulNodeShutdown => 66,
            Feature::UnknownVersionInteroperabilityProxy => 67,
            Feature::DeclarativeValidation => 68,
            Feature::DeclarativeValidationTakeover => 69,
            Feature::StructuredAuthenticationConfiguration => 70,
            Feature::StructuredAuthenticationConfigurationEgressSelector => 71,
            Feature::StructuredAuthenticationConfigurationJWKSMetrics => 72,
            Feature::ImageVolume => 73,
            Feature::LogarithmicScaleDown => 74,
            Feature::ServiceAccountNodeAudienceRestriction => 75,
            Feature::RelaxedEnvironmentVariableValidation => 76,
            Feature::EnvFiles => 77,
        }
    }

    /// Upstream `PreRelease` of the spec at the v1.35 target, i.e. the last
    /// entry of the gate's versioned spec list (`pkg/features/kube_features.go`,
    /// `staging/src/k8s.io/apiserver/pkg/features/kube_features.go`,
    /// `staging/src/k8s.io/component-base/zpages/features/kube_features.go`).
    /// Only the cross-check in `version_history_agrees_with_v1_35_summaries`
    /// reads it: the specials and PreAlpha rejection resolve the spec at the
    /// emulated version via [`feature_spec_at_emulation_and_min_compat_version`].
    pub const fn prerelease(self) -> PreRelease {
        match self {
            Feature::RelaxedDNSSearchValidation
            | Feature::InPlacePodVerticalScaling
            | Feature::DynamicResourceAllocation
            | Feature::AuthorizeWithSelectors
            | Feature::AuthorizeNodeWithSelectors
            | Feature::MultiCIDRServiceAllocator
            | Feature::DisableAllocatorDualWrite
            | Feature::MemoryManager
            | Feature::PodLifecycleSleepAction
            | Feature::PodLifecycleSleepActionAllowZero
            | Feature::ServiceAccountTokenNodeBinding
            | Feature::ServiceAccountTokenNodeBindingValidation
            | Feature::StructuredAuthenticationConfiguration
            | Feature::LogarithmicScaleDown
            | Feature::RelaxedEnvironmentVariableValidation => PreRelease::Ga,
            Feature::InPlacePodVerticalScalingAllocatedStatus => PreRelease::Deprecated,
            Feature::PodTopologyLabelsAdmission
            | Feature::SELinuxMountReadWriteOncePod
            | Feature::SELinuxMount
            | Feature::SELinuxChangePolicy
            | Feature::MaxUnavailableStatefulSet
            | Feature::DRAResourceClaimDeviceStatus
            | Feature::ClusterTrustBundleProjection
            | Feature::PodCertificateRequest
            | Feature::PodLevelResources
            | Feature::MatchLabelKeysInPodTopologySpread
            | Feature::MatchLabelKeysInPodTopologySpreadSelectorMerge
            | Feature::PodDeletionCost
            | Feature::ClusterTrustBundle
            | Feature::APIServerIdentity
            | Feature::StorageVersionMigrator
            | Feature::KubeletCrashLoopBackOffMax
            | Feature::WatchCacheInitializationPostStartHook
            | Feature::MutableCSINodeAllocatableCount
            | Feature::MutatingAdmissionPolicy
            | Feature::DeploymentReplicaSetTerminatingReplicas
            | Feature::GracefulNodeShutdown
            | Feature::GracefulNodeShutdownBasedOnPodPriority
            | Feature::ProcMountType
            | Feature::UserNamespacesSupport
            | Feature::DRAAdminAccess
            | Feature::DRAPrioritizedList
            | Feature::DRASchedulerFilterTimeout
            | Feature::ContainerRestartRules
            | Feature::WindowsGracefulNodeShutdown
            | Feature::DeclarativeValidation
            | Feature::DeclarativeValidationTakeover
            | Feature::StructuredAuthenticationConfigurationEgressSelector
            | Feature::StructuredAuthenticationConfigurationJWKSMetrics
            | Feature::ServiceAccountNodeAudienceRestriction
            | Feature::ImageVolume
            | Feature::EnvFiles => PreRelease::Beta,
            Feature::NodeDeclaredFeatures
            | Feature::DRADeviceTaints
            | Feature::DRAPartitionableDevices
            | Feature::DRADeviceBindingConditions
            | Feature::DRAConsumableCapacity
            | Feature::InPlacePodLevelResourcesVerticalScaling
            | Feature::DRAExtendedResource
            | Feature::ContainerStopSignals
            | Feature::ResourceHealthStatus
            | Feature::GenericWorkload
            | Feature::TaintTolerationComparisonOperators
            | Feature::DRADeviceTaintRules
            | Feature::ComponentFlagz
            | Feature::ComponentStatusz
            | Feature::StorageVersionAPI
            | Feature::ReduceDefaultCrashLoopBackOffDecay
            | Feature::StorageCapacityScoring
            | Feature::CrossNamespaceVolumeDataSource
            | Feature::RuntimeClassInImageCriApi
            | Feature::GangScheduling
            | Feature::InPlacePodVerticalScalingExclusiveCPUs
            | Feature::InPlacePodVerticalScalingExclusiveMemory
            | Feature::RestartAllContainersOnContainerExits
            | Feature::UserNamespacesHostNetworkSupport
            | Feature::WindowsCPUAndMemoryAffinity
            | Feature::UnknownVersionInteroperabilityProxy => PreRelease::Alpha,
        }
    }

    /// `LockToDefault` in the gate's v1.35 spec. Audited against every
    /// modelled gate in `pkg/features/kube_features.go`: only these two carry
    /// `LockToDefault: true`
    /// (`RelaxedDNSSearchValidation` {1.34, true, GA, LockToDefault: true},
    /// `InPlacePodVerticalScaling` {1.35, true, GA, LockToDefault: true});
    /// the zpages / apiserver gates are not locked.
    pub const fn locked_to_default(self) -> bool {
        matches!(
            self,
            Feature::RelaxedDNSSearchValidation
                | Feature::InPlacePodVerticalScaling
                | Feature::DynamicResourceAllocation
                | Feature::AuthorizeWithSelectors
                | Feature::AuthorizeNodeWithSelectors
                | Feature::MultiCIDRServiceAllocator
                | Feature::DisableAllocatorDualWrite
                | Feature::MemoryManager
                | Feature::PodLifecycleSleepAction
                | Feature::PodLifecycleSleepActionAllowZero
                | Feature::ServiceAccountTokenNodeBinding
                | Feature::ServiceAccountTokenNodeBindingValidation
                | Feature::StructuredAuthenticationConfiguration
                | Feature::LogarithmicScaleDown
                | Feature::RelaxedEnvironmentVariableValidation
        )
    }

    /// Upstream-derived default for the v1.35 target.
    pub const fn default_enabled(self) -> bool {
        match self {
            // GA + LockToDefault since v1.34.
            Feature::RelaxedDNSSearchValidation => true,
            // Beta in v1.35 — defaults to true.
            Feature::PodTopologyLabelsAdmission => true,
            // Alpha (off-by-default) in v1.34/v1.35.
            Feature::NodeDeclaredFeatures => false,
            // Beta + default-on since v1.33.
            Feature::InPlacePodVerticalScaling => true,
            // Beta + default-on since v1.28.
            Feature::SELinuxMountReadWriteOncePod => true,
            // Beta but off-by-default since v1.33.
            Feature::SELinuxMount => false,
            // Beta + default-on since v1.33.
            Feature::SELinuxChangePolicy => true,
            // Beta but off-by-default in v1.35.
            Feature::MaxUnavailableStatefulSet => false,
            // 1.33 Alpha, off (kube_features.go:1227-1266).
            Feature::DRADeviceTaints => false,
            // 1.33 Alpha, off (kube_features.go:1227-1266).
            Feature::DRAPartitionableDevices => false,
            // 1.34 Alpha, off (kube_features.go:1227-1266).
            Feature::DRADeviceBindingConditions => false,
            // 1.34 Alpha, off (kube_features.go:1227-1266).
            Feature::DRAConsumableCapacity => false,
            // 1.33 Beta, on (kube_features.go:1227-1266).
            Feature::DRAResourceClaimDeviceStatus => true,
            // 1.35 Alpha, off (kube_features.go:1372).
            Feature::InPlacePodLevelResourcesVerticalScaling => false,
            // 1.34 Alpha, off (kube_features.go:1248).
            Feature::DRAExtendedResource => false,
            // 1.33 Beta, but off (no `Default: true`) (kube_features.go:1204).
            Feature::ClusterTrustBundleProjection => false,
            // 1.35 Beta, but off (kube_features.go:1602).
            Feature::PodCertificateRequest => false,
            // 1.33 Alpha, off (kube_features.go:1219).
            Feature::ContainerStopSignals => false,
            // 1.31 Alpha, off (kube_features.go:1716).
            Feature::ResourceHealthStatus => false,
            // 1.35 Alpha, off (kube_features.go:1321).
            Feature::GenericWorkload => false,
            // 1.35 Alpha, off (kube_features.go:1859).
            Feature::TaintTolerationComparisonOperators => false,
            // 1.34 Beta, on (kube_features.go:1612).
            Feature::PodLevelResources => true,
            // 1.27 Beta, on (kube_features.go:1506).
            Feature::MatchLabelKeysInPodTopologySpread => true,
            // 1.34 Beta, on (kube_features.go:1511).
            Feature::MatchLabelKeysInPodTopologySpreadSelectorMerge => true,
            // 1.22 Beta, on (kube_features.go:1607).
            Feature::PodDeletionCost => true,
            // 1.33 Beta, but off (no `Default: true`) (kube_features.go:1199-1202)
            Feature::ClusterTrustBundle => false,
            // 1.35 Alpha, off (kube_features.go:1240-1242)
            Feature::DRADeviceTaintRules => false,
            // 1.32 Alpha, off (k8s.io/component-base/zpages/features/kube_features.go:40-42)
            Feature::ComponentFlagz => false,
            // 1.32 Alpha, off (k8s.io/component-base/zpages/features/kube_features.go:43-45)
            Feature::ComponentStatusz => false,
            // 1.20 Alpha, off (pkg/features/kube_features.go:2084-2086)
            Feature::StorageVersionAPI => false,
            // 1.26 Beta, on (pkg/features/kube_features.go:1957-1960)
            Feature::APIServerIdentity => true,
            // 1.35 Beta, still off (pkg/features/kube_features.go:1829-1832)
            Feature::StorageVersionMigrator => false,
            Feature::KubeletCrashLoopBackOffMax => true,
            Feature::ReduceDefaultCrashLoopBackOffDecay => false,
            // 1.31 Beta, no `Default: true` (apiserver kube_features.go:494-496)
            Feature::WatchCacheInitializationPostStartHook => false,
            // 1.35 Beta, on (pkg/features/kube_features.go:1537-1541)
            Feature::MutableCSINodeAllocatableCount => true,
            // 1.34 Beta, no `Default: true` (apiserver kube_features.go:407-410)
            Feature::MutatingAdmissionPolicy => false,
            // 1.35 Beta, on (pkg/features/kube_features.go:1270-1273)
            Feature::DeploymentReplicaSetTerminatingReplicas => true,
            // 1.33 Alpha, off (pkg/features/kube_features.go:1820-1822)
            Feature::StorageCapacityScoring => false,
            // 1.26 Alpha, off (pkg/features/kube_features.go:1223-1225)
            Feature::CrossNamespaceVolumeDataSource => false,
            // 1.29 Alpha, off (kube_features.go:1729-1731).
            Feature::RuntimeClassInImageCriApi => false,
            // pkg/features/kube_features.go:1293-1296 (1.34 GA, 1.35 GA + LockToDefault)
            Feature::DynamicResourceAllocation => true,
            // staging/src/k8s.io/apiserver/pkg/features/kube_features.go:350-354 (1.34 GA + LockToDefault)
            Feature::AuthorizeWithSelectors => true,
            // pkg/features/kube_features.go:1147-1149 (1.34 GA + LockToDefault)
            Feature::AuthorizeNodeWithSelectors => true,
            // pkg/features/kube_features.go:1531-1534 (1.34 GA + LockToDefault)
            Feature::MultiCIDRServiceAllocator => true,
            // pkg/features/kube_features.go:1276-1279 (1.35 GA + LockToDefault)
            Feature::DisableAllocatorDualWrite => true,
            // pkg/features/kube_features.go:1318 (1.35 Alpha, off)
            Feature::GangScheduling => false,
            // pkg/features/kube_features.go:1331-1332 (1.21 Beta, on)
            Feature::GracefulNodeShutdown => true,
            // pkg/features/kube_features.go:1336-1337 (1.24 Beta, on)
            Feature::GracefulNodeShutdownBasedOnPodPriority => true,
            // pkg/features/kube_features.go:1383-1384 (1.33 Deprecated, off)
            Feature::InPlacePodVerticalScalingAllocatedStatus => false,
            // pkg/features/kube_features.go:1388 (1.32 Alpha, off)
            Feature::InPlacePodVerticalScalingExclusiveCPUs => false,
            // pkg/features/kube_features.go:1392 (1.34 Alpha, off)
            Feature::InPlacePodVerticalScalingExclusiveMemory => false,
            // pkg/features/kube_features.go:1521-1523 (1.32 GA + LockToDefault)
            Feature::MemoryManager => true,
            // pkg/features/kube_features.go:1618-1620 (1.34 GA + LockToDefault)
            Feature::PodLifecycleSleepAction => true,
            // pkg/features/kube_features.go:1624-1626 (1.34 GA + LockToDefault)
            Feature::PodLifecycleSleepActionAllowZero => true,
            // pkg/features/kube_features.go:1670-1673 (1.33 Beta, on)
            Feature::ProcMountType => true,
            // pkg/features/kube_features.go:1894-1897 (1.33 Beta, on)
            Feature::UserNamespacesSupport => true,
            // pkg/features/kube_features.go:1227-1230 (1.34 Beta, on)
            Feature::DRAAdminAccess => true,
            // pkg/features/kube_features.go:1256-1259 (1.34 Beta, on)
            Feature::DRAPrioritizedList => true,
            // pkg/features/kube_features.go:1266-1268 (1.34 Beta, on)
            Feature::DRASchedulerFilterTimeout => true,
            // pkg/features/kube_features.go:1214-1217 (1.35 Beta, on)
            Feature::ContainerRestartRules => true,
            // pkg/features/kube_features.go:1720-1722 (1.35 Alpha, off)
            Feature::RestartAllContainersOnContainerExits => false,
            // pkg/features/kube_features.go:1784-1788 (1.33 GA + LockToDefault)
            Feature::ServiceAccountTokenNodeBinding => true,
            // pkg/features/kube_features.go:1790-1794 (1.32 GA + LockToDefault)
            Feature::ServiceAccountTokenNodeBindingValidation => true,
            // pkg/features/kube_features.go:1890-1892 (1.35 Alpha, off)
            Feature::UserNamespacesHostNetworkSupport => false,
            // pkg/features/kube_features.go:1922-1924 (1.32 Alpha, off)
            Feature::WindowsCPUAndMemoryAffinity => false,
            // pkg/features/kube_features.go:1926-1929 (1.34 Beta, on)
            Feature::WindowsGracefulNodeShutdown => true,
            // staging/src/k8s.io/apiserver/pkg/features/kube_features.go:490-492 (1.28 Alpha, off)
            Feature::UnknownVersionInteroperabilityProxy => false,
            // staging/src/k8s.io/apiserver/pkg/features/kube_features.go:384-386 (1.33 Beta, on)
            Feature::DeclarativeValidation => true,
            // staging/src/k8s.io/apiserver/pkg/features/kube_features.go:388-390 (1.33 Beta, off)
            Feature::DeclarativeValidationTakeover => false,
            // staging/src/k8s.io/apiserver/pkg/features/kube_features.go:461-465 (1.34 GA + LockToDefault)
            Feature::StructuredAuthenticationConfiguration => true,
            // staging/src/k8s.io/apiserver/pkg/features/kube_features.go:467-469 (1.34 Beta, on)
            Feature::StructuredAuthenticationConfigurationEgressSelector => true,
            // staging/src/k8s.io/apiserver/pkg/features/kube_features.go:471-473 (1.35 Beta, on)
            Feature::StructuredAuthenticationConfigurationJWKSMetrics => true,
            // 1.35 Beta, on (pkg/features/kube_features.go:1366-1370)
            Feature::ImageVolume => true,
            // pkg/features/kube_features.go:1494-1498 (1.31 GA + LockToDefault)
            Feature::LogarithmicScaleDown => true,
            // pkg/features/kube_features.go:1773-1776 (1.33 Beta, on)
            Feature::ServiceAccountNodeAudienceRestriction => true,
            Feature::RelaxedEnvironmentVariableValidation => true,
            Feature::EnvFiles => true,
        }
    }
}

/// Every [`Feature`] variant, in `Feature::idx` order. Adding a new gate
/// requires one variant on the enum and one entry here; everything else
/// derives from this single list:
/// * [`NUM_FEATURES`] is `ALL_FEATURES.len()`,
/// * [`STATES`] sizes itself from [`NUM_FEATURES`] and seeds each slot with
///   that feature's `default_enabled()`,
/// * [`reset_to_defaults`] iterates this array.
///
/// Skipping a gate here is therefore impossible without also changing every
/// derived definition — the previous hand-maintained mirror in
/// `reset_to_defaults` would silently miss new gates.
pub const ALL_FEATURES: &[Feature] = &[
    Feature::RelaxedDNSSearchValidation,
    Feature::PodTopologyLabelsAdmission,
    Feature::NodeDeclaredFeatures,
    Feature::InPlacePodVerticalScaling,
    Feature::SELinuxMountReadWriteOncePod,
    Feature::SELinuxMount,
    Feature::SELinuxChangePolicy,
    Feature::MaxUnavailableStatefulSet,
    Feature::DRADeviceTaints,
    Feature::DRAPartitionableDevices,
    Feature::DRADeviceBindingConditions,
    Feature::DRAConsumableCapacity,
    Feature::DRAResourceClaimDeviceStatus,
    Feature::InPlacePodLevelResourcesVerticalScaling,
    Feature::DRAExtendedResource,
    Feature::ClusterTrustBundleProjection,
    Feature::PodCertificateRequest,
    Feature::ContainerStopSignals,
    Feature::ResourceHealthStatus,
    Feature::GenericWorkload,
    Feature::TaintTolerationComparisonOperators,
    Feature::PodLevelResources,
    Feature::MatchLabelKeysInPodTopologySpread,
    Feature::MatchLabelKeysInPodTopologySpreadSelectorMerge,
    Feature::PodDeletionCost,
    Feature::ClusterTrustBundle,
    Feature::DRADeviceTaintRules,
    Feature::ComponentFlagz,
    Feature::ComponentStatusz,
    Feature::StorageVersionAPI,
    Feature::APIServerIdentity,
    Feature::StorageVersionMigrator,
    Feature::KubeletCrashLoopBackOffMax,
    Feature::ReduceDefaultCrashLoopBackOffDecay,
    Feature::WatchCacheInitializationPostStartHook,
    Feature::MutableCSINodeAllocatableCount,
    Feature::MutatingAdmissionPolicy,
    Feature::DeploymentReplicaSetTerminatingReplicas,
    Feature::StorageCapacityScoring,
    Feature::CrossNamespaceVolumeDataSource,
    Feature::RuntimeClassInImageCriApi,
    Feature::DynamicResourceAllocation,
    Feature::AuthorizeWithSelectors,
    Feature::AuthorizeNodeWithSelectors,
    Feature::MultiCIDRServiceAllocator,
    Feature::DisableAllocatorDualWrite,
    Feature::GangScheduling,
    Feature::GracefulNodeShutdown,
    Feature::GracefulNodeShutdownBasedOnPodPriority,
    Feature::InPlacePodVerticalScalingAllocatedStatus,
    Feature::InPlacePodVerticalScalingExclusiveCPUs,
    Feature::InPlacePodVerticalScalingExclusiveMemory,
    Feature::MemoryManager,
    Feature::PodLifecycleSleepAction,
    Feature::PodLifecycleSleepActionAllowZero,
    Feature::ProcMountType,
    Feature::UserNamespacesSupport,
    Feature::DRAAdminAccess,
    Feature::DRAPrioritizedList,
    Feature::DRASchedulerFilterTimeout,
    Feature::ContainerRestartRules,
    Feature::RestartAllContainersOnContainerExits,
    Feature::ServiceAccountTokenNodeBinding,
    Feature::ServiceAccountTokenNodeBindingValidation,
    Feature::UserNamespacesHostNetworkSupport,
    Feature::WindowsCPUAndMemoryAffinity,
    Feature::WindowsGracefulNodeShutdown,
    Feature::UnknownVersionInteroperabilityProxy,
    Feature::DeclarativeValidation,
    Feature::DeclarativeValidationTakeover,
    Feature::StructuredAuthenticationConfiguration,
    Feature::StructuredAuthenticationConfigurationEgressSelector,
    Feature::StructuredAuthenticationConfigurationJWKSMetrics,
    Feature::ImageVolume,
    Feature::LogarithmicScaleDown,
    Feature::ServiceAccountNodeAudienceRestriction,
    Feature::RelaxedEnvironmentVariableValidation,
    Feature::EnvFiles,
];

/// Total number of feature gates. Derived from [`ALL_FEATURES`].
const NUM_FEATURES: usize = ALL_FEATURES.len();

/// One AtomicBool per [`Feature`], pre-seeded with that feature's
/// `default_enabled()` value. Keep this initializer in lockstep with the
/// [`Feature`] enum — the index of each slot MUST match `Feature::idx`.
///
/// Using `default_enabled()` here (instead of a hardcoded `true`) guarantees
/// that even a brand-new process — before any caller has invoked
/// [`reset_to_defaults`] — sees the upstream default for every gate, not just
/// `RelaxedDNSSearchValidation`.
static STATES: [AtomicBool; NUM_FEATURES] = [
    AtomicBool::new(Feature::RelaxedDNSSearchValidation.default_enabled()),
    AtomicBool::new(Feature::PodTopologyLabelsAdmission.default_enabled()),
    AtomicBool::new(Feature::NodeDeclaredFeatures.default_enabled()),
    AtomicBool::new(Feature::InPlacePodVerticalScaling.default_enabled()),
    AtomicBool::new(Feature::SELinuxMountReadWriteOncePod.default_enabled()),
    AtomicBool::new(Feature::SELinuxMount.default_enabled()),
    AtomicBool::new(Feature::SELinuxChangePolicy.default_enabled()),
    AtomicBool::new(Feature::MaxUnavailableStatefulSet.default_enabled()),
    AtomicBool::new(Feature::DRADeviceTaints.default_enabled()),
    AtomicBool::new(Feature::DRAPartitionableDevices.default_enabled()),
    AtomicBool::new(Feature::DRADeviceBindingConditions.default_enabled()),
    AtomicBool::new(Feature::DRAConsumableCapacity.default_enabled()),
    AtomicBool::new(Feature::DRAResourceClaimDeviceStatus.default_enabled()),
    AtomicBool::new(Feature::InPlacePodLevelResourcesVerticalScaling.default_enabled()),
    AtomicBool::new(Feature::DRAExtendedResource.default_enabled()),
    AtomicBool::new(Feature::ClusterTrustBundleProjection.default_enabled()),
    AtomicBool::new(Feature::PodCertificateRequest.default_enabled()),
    AtomicBool::new(Feature::ContainerStopSignals.default_enabled()),
    AtomicBool::new(Feature::ResourceHealthStatus.default_enabled()),
    AtomicBool::new(Feature::GenericWorkload.default_enabled()),
    AtomicBool::new(Feature::TaintTolerationComparisonOperators.default_enabled()),
    AtomicBool::new(Feature::PodLevelResources.default_enabled()),
    AtomicBool::new(Feature::MatchLabelKeysInPodTopologySpread.default_enabled()),
    AtomicBool::new(Feature::MatchLabelKeysInPodTopologySpreadSelectorMerge.default_enabled()),
    AtomicBool::new(Feature::PodDeletionCost.default_enabled()),
    AtomicBool::new(Feature::ClusterTrustBundle.default_enabled()),
    AtomicBool::new(Feature::DRADeviceTaintRules.default_enabled()),
    AtomicBool::new(Feature::ComponentFlagz.default_enabled()),
    AtomicBool::new(Feature::ComponentStatusz.default_enabled()),
    AtomicBool::new(Feature::StorageVersionAPI.default_enabled()),
    AtomicBool::new(Feature::APIServerIdentity.default_enabled()),
    AtomicBool::new(Feature::StorageVersionMigrator.default_enabled()),
    AtomicBool::new(Feature::KubeletCrashLoopBackOffMax.default_enabled()),
    AtomicBool::new(Feature::ReduceDefaultCrashLoopBackOffDecay.default_enabled()),
    AtomicBool::new(Feature::WatchCacheInitializationPostStartHook.default_enabled()),
    AtomicBool::new(Feature::MutableCSINodeAllocatableCount.default_enabled()),
    AtomicBool::new(Feature::MutatingAdmissionPolicy.default_enabled()),
    AtomicBool::new(Feature::DeploymentReplicaSetTerminatingReplicas.default_enabled()),
    AtomicBool::new(Feature::StorageCapacityScoring.default_enabled()),
    AtomicBool::new(Feature::CrossNamespaceVolumeDataSource.default_enabled()),
    AtomicBool::new(Feature::RuntimeClassInImageCriApi.default_enabled()),
    AtomicBool::new(Feature::DynamicResourceAllocation.default_enabled()),
    AtomicBool::new(Feature::AuthorizeWithSelectors.default_enabled()),
    AtomicBool::new(Feature::AuthorizeNodeWithSelectors.default_enabled()),
    AtomicBool::new(Feature::MultiCIDRServiceAllocator.default_enabled()),
    AtomicBool::new(Feature::DisableAllocatorDualWrite.default_enabled()),
    AtomicBool::new(Feature::GangScheduling.default_enabled()),
    AtomicBool::new(Feature::GracefulNodeShutdown.default_enabled()),
    AtomicBool::new(Feature::GracefulNodeShutdownBasedOnPodPriority.default_enabled()),
    AtomicBool::new(Feature::InPlacePodVerticalScalingAllocatedStatus.default_enabled()),
    AtomicBool::new(Feature::InPlacePodVerticalScalingExclusiveCPUs.default_enabled()),
    AtomicBool::new(Feature::InPlacePodVerticalScalingExclusiveMemory.default_enabled()),
    AtomicBool::new(Feature::MemoryManager.default_enabled()),
    AtomicBool::new(Feature::PodLifecycleSleepAction.default_enabled()),
    AtomicBool::new(Feature::PodLifecycleSleepActionAllowZero.default_enabled()),
    AtomicBool::new(Feature::ProcMountType.default_enabled()),
    AtomicBool::new(Feature::UserNamespacesSupport.default_enabled()),
    AtomicBool::new(Feature::DRAAdminAccess.default_enabled()),
    AtomicBool::new(Feature::DRAPrioritizedList.default_enabled()),
    AtomicBool::new(Feature::DRASchedulerFilterTimeout.default_enabled()),
    AtomicBool::new(Feature::ContainerRestartRules.default_enabled()),
    AtomicBool::new(Feature::RestartAllContainersOnContainerExits.default_enabled()),
    AtomicBool::new(Feature::ServiceAccountTokenNodeBinding.default_enabled()),
    AtomicBool::new(Feature::ServiceAccountTokenNodeBindingValidation.default_enabled()),
    AtomicBool::new(Feature::UserNamespacesHostNetworkSupport.default_enabled()),
    AtomicBool::new(Feature::WindowsCPUAndMemoryAffinity.default_enabled()),
    AtomicBool::new(Feature::WindowsGracefulNodeShutdown.default_enabled()),
    AtomicBool::new(Feature::UnknownVersionInteroperabilityProxy.default_enabled()),
    AtomicBool::new(Feature::DeclarativeValidation.default_enabled()),
    AtomicBool::new(Feature::DeclarativeValidationTakeover.default_enabled()),
    AtomicBool::new(Feature::StructuredAuthenticationConfiguration.default_enabled()),
    AtomicBool::new(Feature::StructuredAuthenticationConfigurationEgressSelector.default_enabled()),
    AtomicBool::new(Feature::StructuredAuthenticationConfigurationJWKSMetrics.default_enabled()),
    AtomicBool::new(Feature::ImageVolume.default_enabled()),
    AtomicBool::new(Feature::LogarithmicScaleDown.default_enabled()),
    AtomicBool::new(Feature::ServiceAccountNodeAudienceRestriction.default_enabled()),
    AtomicBool::new(Feature::RelaxedEnvironmentVariableValidation.default_enabled()),
    AtomicBool::new(Feature::EnvFiles.default_enabled()),
];

/// Returns whether `feature` is currently enabled in this process.
pub fn enabled(feature: Feature) -> bool {
    // `unsafeRecordQueried` (feature_gate.go:846-855).
    let queried = &QUERIED[feature.idx()];
    if !queried.load(Ordering::Relaxed) {
        queried.store(true, Ordering::Relaxed);
    }
    STATES[feature.idx()].load(Ordering::Relaxed)
}

/// Force-set `feature` to `value`. Returns the previous value so callers (or
/// the [`FeatureGuard`] RAII helper) can restore it.
///
/// Intended for production wiring at startup (parse `--feature-gates=...`) and
/// for tests. Tests that touch this MUST be marked `#[serial_test::serial]`
/// because every gate is process-wide.
pub fn set(feature: Feature, value: bool) -> bool {
    STATES[feature.idx()].swap(value, Ordering::Relaxed)
}
/// The mutable half of upstream's `featureGate` that is not a per-gate
/// atomic: `emulationVersion`, `minCompatibilityVersion` and `enabledRaw`
/// (feature_gate.go:216-240). Process-wide, like the gates themselves.
struct GateConfig {
    emulation_version: Version,
    min_compatibility_version: Version,
    /// `enabledRaw`: every key/value ever passed to `Set`/`SetFromMap`, in
    /// first-set order; a later value for the same key replaces the earlier one.
    enabled_raw: Vec<(String, bool)>,
}

static CONFIG: std::sync::Mutex<GateConfig> = std::sync::Mutex::new(GateConfig {
    emulation_version: EMULATION_VERSION,
    min_compatibility_version: MIN_COMPATIBILITY_VERSION,
    enabled_raw: Vec::new(),
});

/// `f.queriedFeatures` (feature_gate.go:846-855): gates read through
/// [`enabled`] since the last emulation-version change.
static QUERIED: [AtomicBool; NUM_FEATURES] = [const { AtomicBool::new(false) }; NUM_FEATURES];

fn config() -> std::sync::MutexGuard<'static, GateConfig> {
    CONFIG.lock().unwrap_or_else(|e| e.into_inner())
}

/// `featureGate.EmulationVersion` (feature_gate.go:828).
pub fn emulation_version() -> Version {
    config().emulation_version
}

/// `featureGate.MinCompatibilityVersion` (feature_gate.go:832).
pub fn min_compatibility_version() -> Version {
    config().min_compatibility_version
}

/// `featureEnabled` (feature_gate.go:857-869) for one gate: an explicit value
/// from `enabled`, else the spec default at the versions.
fn feature_enabled(
    f: Feature,
    enabled: &[(String, bool)],
    emulation: Version,
    min_compat: Version,
) -> bool {
    named_gate_enabled(&format!("{f:?}"), enabled, emulation, min_compat)
}

pub fn feature_by_name(name: &str) -> Option<Feature> {
    ALL_FEATURES
        .iter()
        .copied()
        .find(|f| format!("{f:?}") == name)
}

/// [`feature_enabled`] for any known gate by name, modelled by a [`Feature`]
/// variant or not. Upstream panics on an unknown key (:868); callers only pass
/// names from [`UPSTREAM_GATES`].
fn named_gate_enabled(
    name: &str,
    enabled: &[(String, bool)],
    emulation: Version,
    min_compat: Version,
) -> bool {
    if let Some((_, v)) = enabled.iter().find(|(n, _)| n == name) {
        return *v;
    }
    let gate = upstream_gate(name).unwrap_or_else(|| panic!("feature {name} is not known"));
    feature_spec_at_emulation_and_min_compat_version(gate.specs, emulation, Some(min_compat))
        .default
}

/// Current state of any gate kube-apiserver registers, by name (`Enabled`,
/// feature_gate.go:871, for gates no [`Feature`] variant models). `None` for a
/// name that is not a known gate.
pub fn gate_enabled(name: &str) -> Option<bool> {
    upstream_gate(name)?;
    let cfg = config();
    let enabled = unsafe_set_from_map(
        &cfg.enabled_raw,
        cfg.emulation_version,
        cfg.min_compatibility_version,
    )
    .ok()?;
    Some(named_gate_enabled(
        name,
        &enabled,
        cfg.emulation_version,
        cfg.min_compatibility_version,
    ))
}

/// Store the result of `unsafeSetFromMap`: explicit values win, every other
/// gate takes its spec default at the versions (`enabled` is a sparse map in
/// upstream; here the per-gate atomics hold the resolved view).
fn store_states(enabled: &[(String, bool)], emulation: Version, min_compat: Version) {
    for &f in ALL_FEATURES {
        STATES[f.idx()].store(
            feature_enabled(f, enabled, emulation, min_compat),
            Ordering::Relaxed,
        );
    }
}

/// `unsafeSetFromMap` over `raw` at the given versions, including the
/// dependency check (feature_gate.go:374-436); returns the sparse `enabled` map.
fn unsafe_set_from_map(
    raw: &[(String, bool)],
    emulation: Version,
    min_compat: Version,
) -> Result<Vec<(String, bool)>, String> {
    let resolved = resolve_gates(raw, &known_gates(), emulation, min_compat)?;
    check_dependencies(|n| named_gate_enabled(n, &resolved, emulation, min_compat))?;
    Ok(resolved)
}

/// `enabledAlphaFeatures` (compatibility/registry.go:453-461): gates that are
/// Alpha at the emulated version and currently enabled.
pub fn enabled_alpha_features() -> Vec<String> {
    let (emulation, min_compat) = {
        let cfg = config();
        (cfg.emulation_version, cfg.min_compatibility_version)
    };
    let cfg = config();
    // The post-set view over every known gate, modelled or not.
    let Ok(resolved) = unsafe_set_from_map(&cfg.enabled_raw, emulation, min_compat) else {
        return Vec::new();
    };
    UPSTREAM_GATES
        .iter()
        .filter(|g| {
            feature_spec_at_emulation_and_min_compat_version(g.specs, emulation, Some(min_compat))
                .pre_release
                == PreRelease::Alpha
                && match feature_by_name(g.name) {
                    // Modelled gates carry the live (possibly `set`) value.
                    Some(f) => STATES[f.idx()].load(Ordering::Relaxed),
                    None => named_gate_enabled(g.name, &resolved, emulation, min_compat),
                }
        })
        .map(|g| g.name.to_string())
        .collect()
}

/// `SetEmulationVersionAndMinCompatibilityVersion` (feature_gate.go:791-826).
pub fn set_emulation_version_and_min_compatibility_version(
    emulation_version: Version,
    min_compatibility_version: Version,
) -> Result<(), String> {
    let mut cfg = config();
    if emulation_version == cfg.emulation_version
        && min_compatibility_version == cfg.min_compatibility_version
    {
        return Ok(());
    }
    tracing::debug!(
        "set feature gate emulationVersion to {emulation_version}, minCompatibilityVersion to {min_compatibility_version}"
    );
    // The enabled map is reset whenever emulationVersion is changed (:809-811).
    let enabled = unsafe_set_from_map(
        &cfg.enabled_raw,
        emulation_version,
        min_compatibility_version,
    )?;
    for &f in ALL_FEATURES {
        if !QUERIED[f.idx()].load(Ordering::Relaxed) {
            continue;
        }
        let new_val = feature_enabled(f, &enabled, emulation_version, min_compatibility_version);
        let old_val = STATES[f.idx()].load(Ordering::Relaxed);
        if new_val != old_val {
            tracing::warn!(
                "SetEmulationVersionAndMinCompatibilityVersion will change already queried feature:{f:?} from {old_val} to {new_val}"
            );
        }
    }
    store_states(&enabled, emulation_version, min_compatibility_version);
    cfg.emulation_version = emulation_version;
    cfg.min_compatibility_version = min_compatibility_version;
    for q in &QUERIED {
        q.store(false, Ordering::Relaxed);
    }
    Ok(())
}

/// `featureGate.SetEmulationVersion` (feature_gate.go:787-789).
pub fn set_emulation_version(emulation_version: Version) -> Result<(), String> {
    set_emulation_version_and_min_compatibility_version(
        emulation_version,
        emulation_version.subtract_minor(1),
    )
}

/// `featureGate.Validate` (feature_gate.go:362-372): the flag gates checked at
/// the current emulated version.
pub fn validate() -> Vec<String> {
    let cfg = config();
    match unsafe_set_from_map(
        &cfg.enabled_raw,
        cfg.emulation_version,
        cfg.min_compatibility_version,
    ) {
        Ok(_) => Vec::new(),
        Err(e) => vec![e],
    }
}

/// Reset every feature gate to its rusternetes default. Useful between tests
/// that flip gates without going through [`FeatureGuard`].
///
/// Iterates [`ALL_FEATURES`] so adding a new gate requires only the enum +
/// `ALL_FEATURES` entry — no per-call-site fix-ups.
pub fn reset_to_defaults() {
    let mut cfg = config();
    cfg.emulation_version = EMULATION_VERSION;
    cfg.min_compatibility_version = MIN_COMPATIBILITY_VERSION;
    cfg.enabled_raw.clear();
    for &f in ALL_FEATURES {
        STATES[f.idx()].store(f.default_enabled(), Ordering::Relaxed);
        QUERIED[f.idx()].store(false, Ordering::Relaxed);
    }
}

/// `featuregate.prerelease` (feature_gate.go:130-141).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PreRelease {
    PreAlpha,
    Alpha,
    Beta,
    Ga,
    Deprecated,
}

/// `k8s.io/apimachinery/pkg/util/version.Version` reduced to major.minor
/// (`MajorMinor`; patch/pre-release are never used by gate specs).
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub struct Version {
    pub major: u32,
    pub minor: u32,
}

impl Version {
    pub const fn major_minor(major: u32, minor: u32) -> Self {
        Self { major, minor }
    }

    /// `SubtractMinor` / `OffsetMinor` (apimachinery/pkg/util/version/version.go:259-276):
    /// the minor saturates at 0.
    pub fn subtract_minor(self, diff: u32) -> Self {
        Self::major_minor(self.major, self.minor.saturating_sub(diff))
    }
}

impl std::fmt::Display for Version {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}.{}", self.major, self.minor)
    }
}

/// `FeatureSpec` (feature_gate.go:74-110) reduced to the fields read by
/// [`feature_spec_at_emulation_and_min_compat_version`] and `unsafeSetFromMap`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct FeatureSpec {
    pub default: bool,
    pub lock_to_default: bool,
    pub pre_release: PreRelease,
    pub version: Version,
    pub min_compatibility_version: Option<Version>,
}

/// `featureSpecAtEmulationAndMinCompatVersion` (feature_gate.go:882-902).
/// `specs` must be sorted ascending by `version` (`sort.Sort(VersionedSpecs)`).
pub fn feature_spec_at_emulation_and_min_compat_version(
    specs: &[FeatureSpec],
    emulation_version: Version,
    min_compatibility_version: Option<Version>,
) -> FeatureSpec {
    let min_compatibility_version =
        min_compatibility_version.unwrap_or_else(|| emulation_version.subtract_minor(1));
    for spec in specs.iter().rev() {
        if spec.version > emulation_version {
            continue;
        }
        if let Some(spec_min) = spec.min_compatibility_version {
            if min_compatibility_version < spec_min {
                continue;
            }
        }
        return *spec;
    }
    FeatureSpec {
        default: false,
        lock_to_default: false,
        pre_release: PreRelease::PreAlpha,
        version: Version::major_minor(0, 0),
        min_compatibility_version: None,
    }
}

/// Name of the special "all alpha gates" toggle (feature_gate.go:51).
const ALL_ALPHA_GATE: &str = "AllAlpha";
/// Name of the special "all beta gates" toggle (feature_gate.go:57).
const ALL_BETA_GATE: &str = "AllBeta";
/// Default emulation version (`DefaultKubeBinaryVersion`, component-base/version/base.go:63)
/// and its min-compatibility version (emulation - 1 minor,
/// `NewVersionedFeatureGate` feature_gate.go:305-308).
pub const EMULATION_VERSION: Version = Version::major_minor(1, 35);
pub const MIN_COMPATIBILITY_VERSION: Version = Version::major_minor(1, 34);

/// One known gate as seen by [`resolve_gates`]; the model of upstream's
/// `known` map entry reduced to what `unsafeSetFromMap` reads.
pub struct KnownGate {
    pub name: String,
    /// `VersionedSpecs`, ascending by version. Empty = no spec at or below any
    /// emulation version, i.e. PreAlpha.
    pub specs: Vec<FeatureSpec>,
}

impl KnownGate {
    /// `featureSpecAtEmulationAndMinCompatVersion(known[key], emulationVersion,
    /// minCompatibilityVersion)` (feature_gate.go:387).
    fn spec(&self, emulation: Version, min_compat: Version) -> FeatureSpec {
        feature_spec_at_emulation_and_min_compat_version(&self.specs, emulation, Some(min_compat))
    }
}

/// One `{Version: 1.<minor>, Default, PreRelease, LockToDefault}` row of a
/// gate's `VersionedSpecs`.
const fn spec(
    minor: u32,
    default: bool,
    pre_release: PreRelease,
    lock_to_default: bool,
) -> FeatureSpec {
    FeatureSpec {
        default,
        lock_to_default,
        pre_release,
        version: Version::major_minor(1, minor),
        min_compatibility_version: None,
    }
}

impl Feature {
    /// The gate's `VersionedSpecs`, ascending by version, read from the
    /// generated [`upstream_table`] (the spec list of the gate in
    /// `defaultVersionedKubernetesFeatureGates`, release-1.35:
    /// `pkg/features/kube_features.go:1123`,
    /// `staging/src/k8s.io/apiserver/pkg/features/kube_features.go:309`,
    /// `staging/src/k8s.io/component-base/zpages/features/kube_features.go:38`
    /// `featureGates()`). No upstream 1.35 spec sets `MinCompatibilityVersion`.
    pub fn specs(self) -> Vec<FeatureSpec> {
        let name = format!("{self:?}");
        upstream_gate(&name)
            .unwrap_or_else(|| panic!("{name} missing from upstream_table.rs"))
            .specs
            .to_vec()
    }

    /// `featureSpecAtEmulationAndMinCompatVersion(known[f], emulation, nil)`.
    pub fn spec_at(self, emulation: Version) -> FeatureSpec {
        feature_spec_at_emulation_and_min_compat_version(&self.specs(), emulation, None)
    }
}

/// One gate of [`upstream_table::UPSTREAM_GATES`]: `known[name]` plus the
/// gate's entry in `defaultKubernetesFeatureGateDependencies`.
pub struct UpstreamGate {
    pub name: &'static str,
    pub specs: &'static [FeatureSpec],
    pub deps: &'static [&'static str],
}

pub mod upstream_table;
use upstream_table::UPSTREAM_GATES;

pub fn upstream_gate(name: &str) -> Option<&'static UpstreamGate> {
    UPSTREAM_GATES.iter().find(|g| g.name == name)
}

/// `known` (feature_gate.go:229): every gate kube-apiserver registers, not
/// only the ones a [`Feature`] variant models.
fn known_gates() -> Vec<KnownGate> {
    UPSTREAM_GATES
        .iter()
        .map(|g| KnownGate {
            name: g.name.to_string(),
            specs: g.specs.to_vec(),
        })
        .collect()
}

/// Parse a `--feature-gates` value and apply it. Port of
/// `featureGate.Set` / `SetFromMap`
/// (staging/src/k8s.io/component-base/featuregate/feature_gate.go:340-359, 440-469).
pub fn set_from_str(value: &str) -> Result<(), String> {
    let mut m: Vec<(String, bool)> = Vec::new();
    for s in value.split(',') {
        if s.is_empty() {
            continue;
        }
        let (k, v) = match s.split_once('=') {
            Some((k, v)) => (k.trim(), v.trim()),
            None => return Err(format!("missing bool value for {}", s.trim())),
        };
        let b = match v {
            "1" | "t" | "T" | "TRUE" | "true" | "True" => true,
            "0" | "f" | "F" | "FALSE" | "false" | "False" => false,
            _ => {
                return Err(format!(
                "invalid value of {k}={v}, err: strconv.ParseBool: parsing {v:?}: invalid syntax"
            ))
            }
        };
        m.push((k.to_string(), b));
    }
    set_from_map(&m)
}

/// `featureGate.SetFromMap` + `unsafeSetFromMap` (feature_gate.go:374-469):
/// an unknown gate returns immediately (:385-389); a gate locked to its
/// default accumulates an error (:391-394); changes are persisted only when
/// no error occurred (:463-466). Multiple errors aggregate as `[a, b]`
/// (`utilerrors.NewAggregate`).
///
/// After the per-gate pass, the dependency check (:418-434) runs against the
/// resulting state (see [`check_dependencies`]).
///
fn set_from_map(m: &[(String, bool)]) -> Result<(), String> {
    let mut cfg = config();
    // Update enabledRaw first (feature_gate.go:446-453): it is saved even
    // when the values turn out to be invalid, because the emulation version
    // may not be final yet.
    for (k, v) in m {
        match cfg.enabled_raw.iter_mut().find(|(n, _)| n == k) {
            Some(slot) => slot.1 = *v,
            None => cfg.enabled_raw.push((k.clone(), *v)),
        }
    }
    let (emulation, min_compat) = (cfg.emulation_version, cfg.min_compatibility_version);
    // `unsafeSetFromMap(enabled, enabledRaw, ...)` (:456): over ALL raw values.
    let resolved = unsafe_set_from_map(&cfg.enabled_raw, emulation, min_compat)?;
    // feature_gate.go:407-411
    for (name, v) in &cfg.enabled_raw {
        if let Some(g) = upstream_gate(name) {
            match feature_spec_at_emulation_and_min_compat_version(
                g.specs,
                emulation,
                Some(min_compat),
            )
            .pre_release
            {
                PreRelease::Deprecated => tracing::warn!(
                    "Setting deprecated feature gate {name}={v}. It will be removed in a future release."
                ),
                PreRelease::Ga => tracing::warn!(
                    "Setting GA feature gate {name}={v}. It will be removed in a future release."
                ),
                _ => {}
            }
        }
    }
    store_states(&resolved, emulation, min_compat);
    Ok(())
}

/// Dependency validation of `unsafeSetFromMap` (feature_gate.go:418-436): every
/// enabled gate must have all its dependencies enabled. The edges are
/// `defaultKubernetesFeatureGateDependencies` (pkg/features/kube_features.go:2166),
/// carried per gate in [`upstream_table`] (`Dependencies()`, sorted and
/// compacted by `AddDependencies` feature_gate.go:604-621). Errors aggregate
/// like `utilerrors.NewAggregate`. `is_enabled` yields the post-set state.
fn check_dependencies(is_enabled: impl Fn(&str) -> bool) -> Result<(), String> {
    let mut errs = Vec::new();
    for gate in UPSTREAM_GATES {
        if gate.deps.is_empty() || !is_enabled(gate.name) {
            continue;
        }
        let disabled: Vec<&str> = gate
            .deps
            .iter()
            .copied()
            .filter(|d| !is_enabled(d))
            .collect();
        if !disabled.is_empty() {
            errs.push(format!(
                "{} is enabled, but depends on features that are disabled: [{}]",
                gate.name,
                disabled.join(" ")
            ));
        }
    }
    match errs.len() {
        0 => Ok(()),
        1 => Err(errs.remove(0)),
        _ => Err(format!("[{}]", errs.join(", "))),
    }
}

/// The pure body of `unsafeSetFromMap` (feature_gate.go:381-418) including the
/// `special` handling of `AllAlpha`/`AllBeta` (:396-400, `setUnsetAlphaGates` /
/// `setUnsetBetaGates` :270-296) and PreAlpha rejection (:401-404). Returns the
/// gates to set; special toggles only fill gates the caller did not set
/// explicitly, so an explicit value wins whatever the order in `m`.
pub fn resolve_gates(
    m: &[(String, bool)],
    known: &[KnownGate],
    emulation: Version,
    min_compat: Version,
) -> Result<Vec<(String, bool)>, String> {
    let mut errs: Vec<String> = Vec::new();
    let mut enabled: Vec<(String, bool)> = Vec::new();
    let mut specials: Vec<(PreRelease, bool)> = Vec::new();
    for (k, v) in m {
        let special = match k.as_str() {
            ALL_ALPHA_GATE => Some(PreRelease::Alpha),
            ALL_BETA_GATE => Some(PreRelease::Beta),
            _ => None,
        };
        let gate = known.iter().find(|g| g.name == *k);
        if gate.is_none() && special.is_none() {
            return Err(format!("unrecognized feature gate: {k}"));
        }
        // The specials are never locked (default false, Alpha/Beta spec).
        if let Some(g) = gate {
            let spec = g.spec(emulation, min_compat);
            if spec.lock_to_default && spec.default != *v {
                errs.push(format!(
                    "cannot set feature gate {k} to {v}, feature is locked to {}",
                    spec.default
                ));
                continue;
            }
        }
        if let Some(pre) = special {
            specials.push((pre, *v));
            continue;
        }
        if gate.is_some_and(|g| g.spec(emulation, min_compat).pre_release == PreRelease::PreAlpha) {
            errs.push(format!(
                "cannot set feature gate {k} to {v}, feature is PreAlpha at emulated version {emulation}, min compatibility version {min_compat}"
            ));
            continue;
        }
        enabled.push((k.clone(), *v));
    }
    match errs.len() {
        0 => {}
        1 => return Err(errs.remove(0)),
        _ => return Err(format!("[{}]", errs.join(", "))),
    }
    for (pre, val) in specials {
        for g in known {
            if g.spec(emulation, min_compat).pre_release == pre
                && !enabled.iter().any(|(n, _)| *n == g.name)
            {
                enabled.push((g.name.clone(), val));
            }
        }
    }
    Ok(enabled)
}

/// RAII guard that restores `feature` to its prior value on drop. Use in tests
/// to scope a gate flip to a single test body.
///
/// Created via [`with_feature`].
#[must_use = "the guard restores the previous gate value when dropped"]
pub struct FeatureGuard {
    feature: Feature,
    previous: bool,
}

impl Drop for FeatureGuard {
    fn drop(&mut self) {
        set(self.feature, self.previous);
    }
}

/// Set `feature` to `value` and return a guard that restores the previous
/// value on drop. Mirrors upstream `featuregatetesting.SetFeatureGateDuringTest`.
pub fn with_feature(feature: Feature, value: bool) -> FeatureGuard {
    let previous = set(feature, value);
    FeatureGuard { feature, previous }
}
