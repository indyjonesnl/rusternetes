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
        }
    }

    /// Upstream `PreRelease` of the spec at the v1.35 target, i.e. the last
    /// entry of the gate's versioned spec list (`pkg/features/kube_features.go`,
    /// `staging/src/k8s.io/apiserver/pkg/features/kube_features.go`,
    /// `staging/src/k8s.io/component-base/zpages/features/kube_features.go`).
    /// Consumed by the `AllAlpha`/`AllBeta` specials and PreAlpha rejection in
    /// [`set_from_str`].
    const fn prerelease(self) -> PreRelease {
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
            | Feature::StructuredAuthenticationConfiguration => PreRelease::Ga,
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
            | Feature::StructuredAuthenticationConfigurationJWKSMetrics => PreRelease::Beta,
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
    const fn locked_to_default(self) -> bool {
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
        )
    }

    /// Upstream-derived default for the v1.35 target.
    const fn default_enabled(self) -> bool {
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
];

/// Returns whether `feature` is currently enabled in this process.
pub fn enabled(feature: Feature) -> bool {
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

/// Reset every feature gate to its rusternetes default. Useful between tests
/// that flip gates without going through [`FeatureGuard`].
///
/// Iterates [`ALL_FEATURES`] so adding a new gate requires only the enum +
/// `ALL_FEATURES` entry — no per-call-site fix-ups.
pub fn reset_to_defaults() {
    for &f in ALL_FEATURES {
        STATES[f.idx()].store(f.default_enabled(), Ordering::Relaxed);
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
        let minor = if diff < self.minor {
            self.minor - diff
        } else {
            0
        };
        Self::major_minor(self.major, minor)
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
    let _ = (specs, emulation_version, min_compatibility_version);
    todo!()
}

/// Name of the special "all alpha gates" toggle (feature_gate.go:51).
const ALL_ALPHA_GATE: &str = "AllAlpha";
/// Name of the special "all beta gates" toggle (feature_gate.go:57).
const ALL_BETA_GATE: &str = "AllBeta";
/// Emulation version and its min-compatibility version (emulation - 1 minor,
/// `NewVersionedFeatureGate` feature_gate.go:305-308) quoted by the PreAlpha error.
const EMULATION_VERSION: &str = "1.35";
const MIN_COMPATIBILITY_VERSION: &str = "1.34";

/// One known gate as seen by [`resolve_gates`]; the model of upstream's
/// `known` map entry reduced to what `unsafeSetFromMap` reads.
struct KnownGate {
    name: String,
    pre_release: PreRelease,
    default: bool,
    locked_to_default: bool,
}

fn known_gates() -> Vec<KnownGate> {
    ALL_FEATURES
        .iter()
        .map(|f| KnownGate {
            name: format!("{f:?}"),
            pre_release: f.prerelease(),
            default: f.default_enabled(),
            locked_to_default: f.locked_to_default(),
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
/// Gap: emulation versions are not modelled (tracked separately).
fn set_from_map(m: &[(String, bool)]) -> Result<(), String> {
    let resolved = resolve_gates(m, &known_gates())?;
    check_dependencies(|f| {
        let name = format!("{f:?}");
        resolved
            .iter()
            .find(|(n, _)| *n == name)
            .map_or_else(|| enabled(f), |(_, v)| *v)
    })?;
    for (name, v) in resolved {
        if let Some(&f) = ALL_FEATURES.iter().find(|f| format!("{f:?}") == name) {
            // feature_gate.go:407-411
            match f.prerelease() {
                PreRelease::Deprecated => tracing::warn!(
                    "Setting deprecated feature gate {name}={v}. It will be removed in a future release."
                ),
                PreRelease::Ga => tracing::warn!(
                    "Setting GA feature gate {name}={v}. It will be removed in a future release."
                ),
                _ => {}
            }
            set(f, v);
        }
    }
    Ok(())
}

/// `defaultKubernetesFeatureGateDependencies` (pkg/features/kube_features.go:2166-2420)
/// restricted to non-empty entries whose gates are all modelled in
/// [`Feature`] (#3222); the remainder of the map has no modelled sources (#3231). `AuthorizeNodeWithSelectors: {genericfeatures.AuthorizeWithSelectors}`
/// comes from the same map.
const DEPENDENCIES: &[(Feature, &[Feature])] = &[
    (
        Feature::AuthorizeNodeWithSelectors,
        &[Feature::AuthorizeWithSelectors],
    ),
    (
        Feature::ClusterTrustBundleProjection,
        &[Feature::ClusterTrustBundle],
    ),
    (
        Feature::DRAAdminAccess,
        &[Feature::DynamicResourceAllocation],
    ),
    (
        Feature::DRAConsumableCapacity,
        &[Feature::DynamicResourceAllocation],
    ),
    (
        Feature::DRADeviceBindingConditions,
        &[
            Feature::DynamicResourceAllocation,
            Feature::DRAResourceClaimDeviceStatus,
        ],
    ),
    (Feature::DRADeviceTaintRules, &[Feature::DRADeviceTaints]),
    (
        Feature::DRADeviceTaints,
        &[Feature::DynamicResourceAllocation],
    ),
    (
        Feature::DRAExtendedResource,
        &[Feature::DynamicResourceAllocation],
    ),
    (
        Feature::DRAPartitionableDevices,
        &[Feature::DynamicResourceAllocation],
    ),
    (
        Feature::DRAPrioritizedList,
        &[Feature::DynamicResourceAllocation],
    ),
    (
        Feature::DRASchedulerFilterTimeout,
        &[Feature::DynamicResourceAllocation],
    ),
    (
        Feature::DisableAllocatorDualWrite,
        &[Feature::MultiCIDRServiceAllocator],
    ),
    (Feature::GangScheduling, &[Feature::GenericWorkload]),
    (
        Feature::GracefulNodeShutdownBasedOnPodPriority,
        &[Feature::GracefulNodeShutdown],
    ),
    (
        Feature::InPlacePodLevelResourcesVerticalScaling,
        &[
            Feature::InPlacePodVerticalScaling,
            Feature::PodLevelResources,
            Feature::NodeDeclaredFeatures,
        ],
    ),
    (
        Feature::InPlacePodVerticalScalingAllocatedStatus,
        &[Feature::InPlacePodVerticalScaling],
    ),
    (
        Feature::InPlacePodVerticalScalingExclusiveCPUs,
        &[Feature::InPlacePodVerticalScaling],
    ),
    (
        Feature::InPlacePodVerticalScalingExclusiveMemory,
        &[Feature::InPlacePodVerticalScaling, Feature::MemoryManager],
    ),
    (
        Feature::MatchLabelKeysInPodTopologySpreadSelectorMerge,
        &[Feature::MatchLabelKeysInPodTopologySpread],
    ),
    (
        Feature::PodCertificateRequest,
        &[Feature::AuthorizeNodeWithSelectors],
    ),
    (
        Feature::PodLifecycleSleepActionAllowZero,
        &[Feature::PodLifecycleSleepAction],
    ),
    (Feature::ProcMountType, &[Feature::UserNamespacesSupport]),
    (
        Feature::ResourceHealthStatus,
        &[Feature::DynamicResourceAllocation],
    ),
    (Feature::StorageVersionAPI, &[Feature::APIServerIdentity]),
    (
        Feature::RestartAllContainersOnContainerExits,
        &[
            Feature::ContainerRestartRules,
            Feature::NodeDeclaredFeatures,
        ],
    ),
    (
        Feature::DeclarativeValidationTakeover,
        &[Feature::DeclarativeValidation],
    ),
    (
        Feature::ServiceAccountTokenNodeBinding,
        &[Feature::ServiceAccountTokenNodeBindingValidation],
    ),
    (
        Feature::StructuredAuthenticationConfigurationEgressSelector,
        &[Feature::StructuredAuthenticationConfiguration],
    ),
    (
        Feature::StructuredAuthenticationConfigurationJWKSMetrics,
        &[Feature::StructuredAuthenticationConfiguration],
    ),
    (
        Feature::UnknownVersionInteroperabilityProxy,
        &[Feature::APIServerIdentity],
    ),
    (
        Feature::UserNamespacesHostNetworkSupport,
        &[Feature::UserNamespacesSupport],
    ),
    (
        Feature::WindowsCPUAndMemoryAffinity,
        &[Feature::MemoryManager],
    ),
    (
        Feature::WindowsGracefulNodeShutdown,
        &[Feature::GracefulNodeShutdown],
    ),
];

/// Dependency validation of `unsafeSetFromMap` (feature_gate.go:418-436): every
/// enabled gate must have all its dependencies enabled. Errors aggregate like
/// `utilerrors.NewAggregate`. `is_enabled` yields the post-set state.
fn check_dependencies(is_enabled: impl Fn(Feature) -> bool) -> Result<(), String> {
    let mut errs = Vec::new();
    for (feature, deps) in DEPENDENCIES {
        if !is_enabled(*feature) {
            continue;
        }
        let disabled: Vec<String> = deps
            .iter()
            .filter(|d| !is_enabled(**d))
            .map(|d| format!("{d:?}"))
            .collect();
        if !disabled.is_empty() {
            errs.push(format!(
                "{feature:?} is enabled, but depends on features that are disabled: [{}]",
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
fn resolve_gates(m: &[(String, bool)], known: &[KnownGate]) -> Result<Vec<(String, bool)>, String> {
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
            if g.locked_to_default && g.default != *v {
                errs.push(format!(
                    "cannot set feature gate {k} to {v}, feature is locked to {}",
                    g.default
                ));
                continue;
            }
        }
        if let Some(pre) = special {
            specials.push((pre, *v));
            continue;
        }
        if gate.is_some_and(|g| g.pre_release == PreRelease::PreAlpha) {
            errs.push(format!(
                "cannot set feature gate {k} to {v}, feature is PreAlpha at emulated version {EMULATION_VERSION}, min compatibility version {MIN_COMPATIBILITY_VERSION}"
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
            if g.pre_release == pre && !enabled.iter().any(|(n, _)| *n == g.name) {
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

#[cfg(test)]
mod tests {
    use super::*;
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
        // feature_gate.go:387
        assert_eq!(
            set_from_str("fooBarBaz=true").unwrap_err(),
            "unrecognized feature gate: fooBarBaz"
        );
        // feature_gate.go:392 (LockToDefault)
        assert_eq!(
            set_from_str("RelaxedDNSSearchValidation=false").unwrap_err(),
            "cannot set feature gate RelaxedDNSSearchValidation to false, feature is locked to true"
        );
        // feature_gate.go:463-466: nothing persisted when any error occurred.
        assert!(set_from_str("StorageCapacityScoring=true,fooBarBaz=true").is_err());
        assert!(!enabled(Feature::StorageCapacityScoring));
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
            pre_release: pre,
            default: false,
            locked_to_default: locked,
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
            resolve_gates(&m(&[("TestPreAlpha", true)]), &k).unwrap_err(),
            "cannot set feature gate TestPreAlpha to true, feature is PreAlpha at emulated version 1.35, min compatibility version 1.34"
        );
        let mut got = resolve_gates(&m(&[("AllAlpha", true)]), &k).unwrap();
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
        let mut got = resolve_gates(&m(&[("TestBeta", true), ("AllAlpha", false)]), &k).unwrap();
        got.sort();
        assert_eq!(
            got,
            vec![
                ("TestAlpha".to_string(), false),
                ("TestBeta".to_string(), true)
            ]
        );
        assert!(resolve_gates(&m(&[("TestLockedFalse", true)]), &k)
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
        assert_eq!(
            set_from_str("StructuredAuthenticationConfiguration=false").unwrap_err(),
            "cannot set feature gate StructuredAuthenticationConfiguration to false, feature is locked to true"
        );
        reset_to_defaults();
    }
}
