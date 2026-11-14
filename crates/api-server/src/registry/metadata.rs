//! `HasMetadata`: the one accessor trait every stored resource implements.
//!
//! Upstream gets this from `meta.Accessor`
//! (`staging/src/k8s.io/apimachinery/pkg/api/meta/meta.go`), which every
//! `runtime.Object` satisfies. The registry `Object` bound, the finalizer and
//! delete helpers and the watch paths all read and write `ObjectMeta` through
//! this single trait, re-exported as `registry::rest::HasMetadata`.

/// Trait for resources that have metadata with finalizers.
/// This allows the generic registry and watch helpers to work with any
/// Kubernetes resource type.
pub trait HasMetadata {
    /// Get an immutable reference to the resource's metadata
    fn metadata(&self) -> &k3rs_common::types::ObjectMeta;

    /// Get a mutable reference to the resource's metadata
    fn metadata_mut(&mut self) -> &mut k3rs_common::types::ObjectMeta;
}

// Implement HasMetadata for common resource types
// Note: This can be extended with a macro if needed for many types

impl HasMetadata for k3rs_common::resources::Namespace {
    fn metadata(&self) -> &k3rs_common::types::ObjectMeta {
        &self.metadata
    }

    fn metadata_mut(&mut self) -> &mut k3rs_common::types::ObjectMeta {
        &mut self.metadata
    }
}

impl HasMetadata for k3rs_common::resources::Binding {
    fn metadata(&self) -> &k3rs_common::types::ObjectMeta {
        &self.metadata
    }

    fn metadata_mut(&mut self) -> &mut k3rs_common::types::ObjectMeta {
        &mut self.metadata
    }
}

impl HasMetadata for k3rs_common::resources::Pod {
    fn metadata(&self) -> &k3rs_common::types::ObjectMeta {
        &self.metadata
    }

    fn metadata_mut(&mut self) -> &mut k3rs_common::types::ObjectMeta {
        &mut self.metadata
    }
}

impl HasMetadata for k3rs_common::resources::Scale {
    fn metadata(&self) -> &k3rs_common::types::ObjectMeta {
        &self.metadata
    }

    fn metadata_mut(&mut self) -> &mut k3rs_common::types::ObjectMeta {
        &mut self.metadata
    }
}

impl HasMetadata for k3rs_common::resources::Deployment {
    fn metadata(&self) -> &k3rs_common::types::ObjectMeta {
        &self.metadata
    }

    fn metadata_mut(&mut self) -> &mut k3rs_common::types::ObjectMeta {
        &mut self.metadata
    }
}

impl HasMetadata for k3rs_common::resources::Service {
    fn metadata(&self) -> &k3rs_common::types::ObjectMeta {
        &self.metadata
    }

    fn metadata_mut(&mut self) -> &mut k3rs_common::types::ObjectMeta {
        &mut self.metadata
    }
}

impl HasMetadata for k3rs_common::resources::ConfigMap {
    fn metadata(&self) -> &k3rs_common::types::ObjectMeta {
        &self.metadata
    }

    fn metadata_mut(&mut self) -> &mut k3rs_common::types::ObjectMeta {
        &mut self.metadata
    }
}

impl HasMetadata for k3rs_common::resources::Secret {
    fn metadata(&self) -> &k3rs_common::types::ObjectMeta {
        &self.metadata
    }

    fn metadata_mut(&mut self) -> &mut k3rs_common::types::ObjectMeta {
        &mut self.metadata
    }
}

impl HasMetadata for k3rs_common::resources::ServiceAccount {
    fn metadata(&self) -> &k3rs_common::types::ObjectMeta {
        &self.metadata
    }

    fn metadata_mut(&mut self) -> &mut k3rs_common::types::ObjectMeta {
        &mut self.metadata
    }
}

impl HasMetadata for k3rs_common::resources::ReplicaSet {
    fn metadata(&self) -> &k3rs_common::types::ObjectMeta {
        &self.metadata
    }

    fn metadata_mut(&mut self) -> &mut k3rs_common::types::ObjectMeta {
        &mut self.metadata
    }
}

impl HasMetadata for k3rs_common::resources::DaemonSet {
    fn metadata(&self) -> &k3rs_common::types::ObjectMeta {
        &self.metadata
    }

    fn metadata_mut(&mut self) -> &mut k3rs_common::types::ObjectMeta {
        &mut self.metadata
    }
}

impl HasMetadata for k3rs_common::resources::StatefulSet {
    fn metadata(&self) -> &k3rs_common::types::ObjectMeta {
        &self.metadata
    }

    fn metadata_mut(&mut self) -> &mut k3rs_common::types::ObjectMeta {
        &mut self.metadata
    }
}

impl HasMetadata for k3rs_common::resources::Job {
    fn metadata(&self) -> &k3rs_common::types::ObjectMeta {
        &self.metadata
    }

    fn metadata_mut(&mut self) -> &mut k3rs_common::types::ObjectMeta {
        &mut self.metadata
    }
}

impl HasMetadata for k3rs_common::resources::CronJob {
    fn metadata(&self) -> &k3rs_common::types::ObjectMeta {
        &self.metadata
    }

    fn metadata_mut(&mut self) -> &mut k3rs_common::types::ObjectMeta {
        &mut self.metadata
    }
}

impl HasMetadata for k3rs_common::resources::PersistentVolume {
    fn metadata(&self) -> &k3rs_common::types::ObjectMeta {
        &self.metadata
    }

    fn metadata_mut(&mut self) -> &mut k3rs_common::types::ObjectMeta {
        &mut self.metadata
    }
}

impl HasMetadata for k3rs_common::resources::PersistentVolumeClaim {
    fn metadata(&self) -> &k3rs_common::types::ObjectMeta {
        &self.metadata
    }

    fn metadata_mut(&mut self) -> &mut k3rs_common::types::ObjectMeta {
        &mut self.metadata
    }
}

impl HasMetadata for k3rs_common::resources::StorageClass {
    fn metadata(&self) -> &k3rs_common::types::ObjectMeta {
        &self.metadata
    }

    fn metadata_mut(&mut self) -> &mut k3rs_common::types::ObjectMeta {
        &mut self.metadata
    }
}

impl HasMetadata for k3rs_common::resources::Ingress {
    fn metadata(&self) -> &k3rs_common::types::ObjectMeta {
        &self.metadata
    }

    fn metadata_mut(&mut self) -> &mut k3rs_common::types::ObjectMeta {
        &mut self.metadata
    }
}

impl HasMetadata for k3rs_common::resources::IngressClass {
    fn metadata(&self) -> &k3rs_common::types::ObjectMeta {
        &self.metadata
    }

    fn metadata_mut(&mut self) -> &mut k3rs_common::types::ObjectMeta {
        &mut self.metadata
    }
}

impl HasMetadata for k3rs_common::resources::NetworkPolicy {
    fn metadata(&self) -> &k3rs_common::types::ObjectMeta {
        &self.metadata
    }

    fn metadata_mut(&mut self) -> &mut k3rs_common::types::ObjectMeta {
        &mut self.metadata
    }
}

impl HasMetadata for k3rs_common::resources::ResourceQuota {
    fn metadata(&self) -> &k3rs_common::types::ObjectMeta {
        &self.metadata
    }

    fn metadata_mut(&mut self) -> &mut k3rs_common::types::ObjectMeta {
        &mut self.metadata
    }
}

impl HasMetadata for k3rs_common::resources::LimitRange {
    fn metadata(&self) -> &k3rs_common::types::ObjectMeta {
        &self.metadata
    }

    fn metadata_mut(&mut self) -> &mut k3rs_common::types::ObjectMeta {
        &mut self.metadata
    }
}

impl HasMetadata for k3rs_common::resources::Eviction {
    fn metadata(&self) -> &k3rs_common::types::ObjectMeta {
        &self.metadata
    }

    fn metadata_mut(&mut self) -> &mut k3rs_common::types::ObjectMeta {
        &mut self.metadata
    }
}

impl HasMetadata for k3rs_common::resources::PodDisruptionBudget {
    fn metadata(&self) -> &k3rs_common::types::ObjectMeta {
        &self.metadata
    }

    fn metadata_mut(&mut self) -> &mut k3rs_common::types::ObjectMeta {
        &mut self.metadata
    }
}

impl HasMetadata for k3rs_common::resources::HorizontalPodAutoscaler {
    fn metadata(&self) -> &k3rs_common::types::ObjectMeta {
        &self.metadata
    }

    fn metadata_mut(&mut self) -> &mut k3rs_common::types::ObjectMeta {
        &mut self.metadata
    }
}

impl HasMetadata for k3rs_common::resources::Node {
    fn metadata(&self) -> &k3rs_common::types::ObjectMeta {
        &self.metadata
    }

    fn metadata_mut(&mut self) -> &mut k3rs_common::types::ObjectMeta {
        &mut self.metadata
    }
}

impl HasMetadata for k3rs_common::resources::VolumeSnapshot {
    fn metadata(&self) -> &k3rs_common::types::ObjectMeta {
        &self.metadata
    }

    fn metadata_mut(&mut self) -> &mut k3rs_common::types::ObjectMeta {
        &mut self.metadata
    }
}

impl HasMetadata for k3rs_common::resources::VolumeSnapshotClass {
    fn metadata(&self) -> &k3rs_common::types::ObjectMeta {
        &self.metadata
    }

    fn metadata_mut(&mut self) -> &mut k3rs_common::types::ObjectMeta {
        &mut self.metadata
    }
}

impl HasMetadata for k3rs_common::resources::VolumeSnapshotContent {
    fn metadata(&self) -> &k3rs_common::types::ObjectMeta {
        &self.metadata
    }

    fn metadata_mut(&mut self) -> &mut k3rs_common::types::ObjectMeta {
        &mut self.metadata
    }
}

impl HasMetadata for k3rs_common::resources::CSIDriver {
    fn metadata(&self) -> &k3rs_common::types::ObjectMeta {
        &self.metadata
    }

    fn metadata_mut(&mut self) -> &mut k3rs_common::types::ObjectMeta {
        &mut self.metadata
    }
}

impl HasMetadata for k3rs_common::resources::CSINode {
    fn metadata(&self) -> &k3rs_common::types::ObjectMeta {
        &self.metadata
    }

    fn metadata_mut(&mut self) -> &mut k3rs_common::types::ObjectMeta {
        &mut self.metadata
    }
}

impl HasMetadata for k3rs_common::resources::CSIStorageCapacity {
    fn metadata(&self) -> &k3rs_common::types::ObjectMeta {
        &self.metadata
    }

    fn metadata_mut(&mut self) -> &mut k3rs_common::types::ObjectMeta {
        &mut self.metadata
    }
}

impl HasMetadata for k3rs_common::resources::VolumeAttachment {
    fn metadata(&self) -> &k3rs_common::types::ObjectMeta {
        &self.metadata
    }

    fn metadata_mut(&mut self) -> &mut k3rs_common::types::ObjectMeta {
        &mut self.metadata
    }
}

impl HasMetadata for k3rs_common::resources::VolumeAttributesClass {
    fn metadata(&self) -> &k3rs_common::types::ObjectMeta {
        &self.metadata
    }

    fn metadata_mut(&mut self) -> &mut k3rs_common::types::ObjectMeta {
        &mut self.metadata
    }
}

impl HasMetadata for k3rs_common::resources::ValidatingWebhookConfiguration {
    fn metadata(&self) -> &k3rs_common::types::ObjectMeta {
        &self.metadata
    }

    fn metadata_mut(&mut self) -> &mut k3rs_common::types::ObjectMeta {
        &mut self.metadata
    }
}

impl HasMetadata for k3rs_common::resources::MutatingWebhookConfiguration {
    fn metadata(&self) -> &k3rs_common::types::ObjectMeta {
        &self.metadata
    }

    fn metadata_mut(&mut self) -> &mut k3rs_common::types::ObjectMeta {
        &mut self.metadata
    }
}

impl HasMetadata for k3rs_common::resources::ValidatingAdmissionPolicy {
    fn metadata(&self) -> &k3rs_common::types::ObjectMeta {
        &self.metadata
    }

    fn metadata_mut(&mut self) -> &mut k3rs_common::types::ObjectMeta {
        &mut self.metadata
    }
}

impl HasMetadata for k3rs_common::resources::ValidatingAdmissionPolicyBinding {
    fn metadata(&self) -> &k3rs_common::types::ObjectMeta {
        &self.metadata
    }

    fn metadata_mut(&mut self) -> &mut k3rs_common::types::ObjectMeta {
        &mut self.metadata
    }
}

impl HasMetadata for k3rs_common::resources::MutatingAdmissionPolicyBinding {
    fn metadata(&self) -> &k3rs_common::types::ObjectMeta {
        &self.metadata
    }

    fn metadata_mut(&mut self) -> &mut k3rs_common::types::ObjectMeta {
        &mut self.metadata
    }
}

impl HasMetadata for k3rs_common::resources::MutatingAdmissionPolicy {
    fn metadata(&self) -> &k3rs_common::types::ObjectMeta {
        &self.metadata
    }

    fn metadata_mut(&mut self) -> &mut k3rs_common::types::ObjectMeta {
        &mut self.metadata
    }
}

impl HasMetadata for k3rs_common::resources::ClusterTrustBundle {
    fn metadata(&self) -> &k3rs_common::types::ObjectMeta {
        &self.metadata
    }

    fn metadata_mut(&mut self) -> &mut k3rs_common::types::ObjectMeta {
        &mut self.metadata
    }
}

impl HasMetadata for k3rs_common::resources::podcertificaterequest::PodCertificateRequest {
    fn metadata(&self) -> &k3rs_common::types::ObjectMeta {
        &self.metadata
    }

    fn metadata_mut(&mut self) -> &mut k3rs_common::types::ObjectMeta {
        &mut self.metadata
    }
}

impl HasMetadata for k3rs_common::resources::CertificateSigningRequest {
    fn metadata(&self) -> &k3rs_common::types::ObjectMeta {
        &self.metadata
    }

    fn metadata_mut(&mut self) -> &mut k3rs_common::types::ObjectMeta {
        &mut self.metadata
    }
}

impl HasMetadata for k3rs_common::resources::FlowSchema {
    fn metadata(&self) -> &k3rs_common::types::ObjectMeta {
        &self.metadata
    }

    fn metadata_mut(&mut self) -> &mut k3rs_common::types::ObjectMeta {
        &mut self.metadata
    }
}

impl HasMetadata for k3rs_common::resources::PriorityLevelConfiguration {
    fn metadata(&self) -> &k3rs_common::types::ObjectMeta {
        &self.metadata
    }

    fn metadata_mut(&mut self) -> &mut k3rs_common::types::ObjectMeta {
        &mut self.metadata
    }
}

// DRA resources. These used to be excluded here — they declared their own
// `dra::ObjectMeta`, so neither `HasMetadata` nor the shared finalizer and
// metadata-inheritance helpers typechecked against them, and DRA silently had
// no finalizer support and no server-owned-field reinstatement (#1895). They
// now use `types::ObjectMeta` like every other resource, so the generic rules
// reach them by construction rather than by a per-resource copy.

impl HasMetadata for k3rs_common::resources::dra::ResourceClaim {
    fn metadata(&self) -> &k3rs_common::types::ObjectMeta {
        &self.metadata
    }

    fn metadata_mut(&mut self) -> &mut k3rs_common::types::ObjectMeta {
        &mut self.metadata
    }
}

impl HasMetadata for k3rs_common::resources::dra::ResourceClaimTemplate {
    fn metadata(&self) -> &k3rs_common::types::ObjectMeta {
        &self.metadata
    }

    fn metadata_mut(&mut self) -> &mut k3rs_common::types::ObjectMeta {
        &mut self.metadata
    }
}

impl HasMetadata for k3rs_common::resources::dra::DeviceClass {
    fn metadata(&self) -> &k3rs_common::types::ObjectMeta {
        &self.metadata
    }

    fn metadata_mut(&mut self) -> &mut k3rs_common::types::ObjectMeta {
        &mut self.metadata
    }
}

impl HasMetadata for k3rs_common::resources::dra::ResourceSlice {
    fn metadata(&self) -> &k3rs_common::types::ObjectMeta {
        &self.metadata
    }

    fn metadata_mut(&mut self) -> &mut k3rs_common::types::ObjectMeta {
        &mut self.metadata
    }
}

impl HasMetadata for k3rs_common::resources::Role {
    fn metadata(&self) -> &k3rs_common::types::ObjectMeta {
        &self.metadata
    }

    fn metadata_mut(&mut self) -> &mut k3rs_common::types::ObjectMeta {
        &mut self.metadata
    }
}

impl HasMetadata for k3rs_common::resources::RoleBinding {
    fn metadata(&self) -> &k3rs_common::types::ObjectMeta {
        &self.metadata
    }

    fn metadata_mut(&mut self) -> &mut k3rs_common::types::ObjectMeta {
        &mut self.metadata
    }
}

impl HasMetadata for k3rs_common::resources::ClusterRole {
    fn metadata(&self) -> &k3rs_common::types::ObjectMeta {
        &self.metadata
    }

    fn metadata_mut(&mut self) -> &mut k3rs_common::types::ObjectMeta {
        &mut self.metadata
    }
}

impl HasMetadata for k3rs_common::resources::ClusterRoleBinding {
    fn metadata(&self) -> &k3rs_common::types::ObjectMeta {
        &self.metadata
    }

    fn metadata_mut(&mut self) -> &mut k3rs_common::types::ObjectMeta {
        &mut self.metadata
    }
}

impl HasMetadata for k3rs_common::resources::PodTemplate {
    fn metadata(&self) -> &k3rs_common::types::ObjectMeta {
        &self.metadata
    }

    fn metadata_mut(&mut self) -> &mut k3rs_common::types::ObjectMeta {
        &mut self.metadata
    }
}

impl HasMetadata for k3rs_common::resources::storageversion::StorageVersion {
    fn metadata(&self) -> &k3rs_common::types::ObjectMeta {
        &self.metadata
    }

    fn metadata_mut(&mut self) -> &mut k3rs_common::types::ObjectMeta {
        &mut self.metadata
    }
}

impl HasMetadata for k3rs_common::resources::ControllerRevision {
    fn metadata(&self) -> &k3rs_common::types::ObjectMeta {
        &self.metadata
    }

    fn metadata_mut(&mut self) -> &mut k3rs_common::types::ObjectMeta {
        &mut self.metadata
    }
}

impl HasMetadata for k3rs_common::resources::storageversionmigration::StorageVersionMigration {
    fn metadata(&self) -> &k3rs_common::types::ObjectMeta {
        &self.metadata
    }

    fn metadata_mut(&mut self) -> &mut k3rs_common::types::ObjectMeta {
        &mut self.metadata
    }
}

impl HasMetadata for k3rs_common::resources::ServiceCIDR {
    fn metadata(&self) -> &k3rs_common::types::ObjectMeta {
        &self.metadata
    }

    fn metadata_mut(&mut self) -> &mut k3rs_common::types::ObjectMeta {
        &mut self.metadata
    }
}

impl HasMetadata for k3rs_common::resources::IPAddress {
    fn metadata(&self) -> &k3rs_common::types::ObjectMeta {
        &self.metadata
    }

    fn metadata_mut(&mut self) -> &mut k3rs_common::types::ObjectMeta {
        &mut self.metadata
    }
}

impl HasMetadata for k3rs_common::resources::CustomResourceDefinition {
    fn metadata(&self) -> &k3rs_common::types::ObjectMeta {
        &self.metadata
    }

    fn metadata_mut(&mut self) -> &mut k3rs_common::types::ObjectMeta {
        &mut self.metadata
    }
}

impl HasMetadata for k3rs_common::resources::Event {
    fn metadata(&self) -> &k3rs_common::types::ObjectMeta {
        &self.metadata
    }

    fn metadata_mut(&mut self) -> &mut k3rs_common::types::ObjectMeta {
        &mut self.metadata
    }
}

impl HasMetadata for k3rs_common::resources::APIService {
    fn metadata(&self) -> &k3rs_common::types::ObjectMeta {
        &self.metadata
    }

    fn metadata_mut(&mut self) -> &mut k3rs_common::types::ObjectMeta {
        &mut self.metadata
    }
}

impl HasMetadata for k3rs_common::resources::PriorityClass {
    fn metadata(&self) -> &k3rs_common::types::ObjectMeta {
        &self.metadata
    }

    fn metadata_mut(&mut self) -> &mut k3rs_common::types::ObjectMeta {
        &mut self.metadata
    }
}

impl HasMetadata for k3rs_common::resources::Lease {
    fn metadata(&self) -> &k3rs_common::types::ObjectMeta {
        &self.metadata
    }

    fn metadata_mut(&mut self) -> &mut k3rs_common::types::ObjectMeta {
        &mut self.metadata
    }
}

impl HasMetadata for k3rs_common::resources::RuntimeClass {
    fn metadata(&self) -> &k3rs_common::types::ObjectMeta {
        &self.metadata
    }

    fn metadata_mut(&mut self) -> &mut k3rs_common::types::ObjectMeta {
        &mut self.metadata
    }
}

impl HasMetadata for k3rs_common::resources::EndpointSlice {
    fn metadata(&self) -> &k3rs_common::types::ObjectMeta {
        &self.metadata
    }

    fn metadata_mut(&mut self) -> &mut k3rs_common::types::ObjectMeta {
        &mut self.metadata
    }
}

impl HasMetadata for k3rs_common::resources::Endpoints {
    fn metadata(&self) -> &k3rs_common::types::ObjectMeta {
        &self.metadata
    }

    fn metadata_mut(&mut self) -> &mut k3rs_common::types::ObjectMeta {
        &mut self.metadata
    }
}

impl HasMetadata for k3rs_common::resources::ReplicationController {
    fn metadata(&self) -> &k3rs_common::types::ObjectMeta {
        &self.metadata
    }

    fn metadata_mut(&mut self) -> &mut k3rs_common::types::ObjectMeta {
        &mut self.metadata
    }
}

impl HasMetadata for k3rs_common::resources::CustomResource {
    fn metadata(&self) -> &k3rs_common::types::ObjectMeta {
        &self.metadata
    }

    fn metadata_mut(&mut self) -> &mut k3rs_common::types::ObjectMeta {
        &mut self.metadata
    }
}

// Was only in the watch module's duplicate trait before the two were collapsed.
impl HasMetadata for k3rs_common::resources::EventV1 {
    fn metadata(&self) -> &k3rs_common::types::ObjectMeta {
        &self.metadata
    }

    fn metadata_mut(&mut self) -> &mut k3rs_common::types::ObjectMeta {
        &mut self.metadata
    }
}

impl HasMetadata for k3rs_common::resources::devicetaintrule::DeviceTaintRule {
    fn metadata(&self) -> &k3rs_common::types::ObjectMeta {
        &self.metadata
    }

    fn metadata_mut(&mut self) -> &mut k3rs_common::types::ObjectMeta {
        &mut self.metadata
    }
}
